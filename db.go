package pogreb

import (
	"bytes"
	"context"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/akrylysov/pogreb/fs"
	"github.com/akrylysov/pogreb/internal/hash"
)

const (
	// MaxKeyLength is the maximum size of a key in bytes.
	MaxKeyLength = math.MaxUint16

	// MaxValueLength is the maximum size of a value in bytes.
	MaxValueLength = math.MaxUint32 - MaxKeyLength

	// MaxKeys is the maximum numbers of keys in the DB.
	MaxKeys = math.MaxUint32

	formatVersion = 2 // file format version
	metaExt       = ".meta"
	dbMetaName    = "db" + metaExt
)

// DB represents the key-value storage.
// All DB methods are safe for concurrent use by multiple goroutines.
type DB struct {
	mu                sync.RWMutex
	opts              *Options
	index             *index
	datalog           *datalog
	lock              fs.LockFile
	hashSeed          uint32
	metrics           Metrics
	cancelSyncer      context.CancelFunc
	syncWrites        bool
	compactionRunning int32
}

type dbMeta struct {
	HashSeed uint32
}

// Open opens or creates a new DB.
// The DB must be closed after use, by calling Close method.
func Open(path string, opts *Options) (*DB, error) {
	opts = opts.copyWithDefaults(path)
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}
	lock, acquiredExistingLock, err := createLockFile(opts)
	if err != nil {
		if err == os.ErrExist {
			err = errLocked
		}
		return nil, err
	}
	clean := lock.Unlock
	defer func() {
		if clean != nil {
			_ = clean()
		}
	}()
	if acquiredExistingLock {
		if err := backupNondataFiles(path); err != nil {
			return nil, err
		}
	}
	index, err := openIndex(opts)
	if err != nil {
		return nil, err
	}
	datalog, err := openDatalog(opts)
	if err != nil {
		return nil, err
	}
	db := &DB{
		opts:    opts,
		index:   index,
		datalog: datalog,
		lock:    lock,
		metrics: newMetrics(),
	}
	if index.count() == 0 {
		seed, err := hash.RandSeed()
		if err != nil {
			return nil, err
		}
		db.hashSeed = seed
	} else {
		if err := db.readMeta(); err != nil {
			return nil, err
		}
	}
	// Lock already exists - database wasn't closed properly.
	if acquiredExistingLock {
		if err := db.recover(); err != nil {
			return nil, err
		}
	}
	if opts.BackgroundSyncInterval > 0 {
		db.startSyncer(opts.BackgroundSyncInterval)
	} else if opts.BackgroundSyncInterval == -1 {
		db.syncWrites = true
	}
	clean = nil
	return db, nil
}

func cloneBytes(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func (db *DB) writeManifest() error {
	m := dbMeta{
		HashSeed: db.hashSeed,
	}
	return writeGobFile(db.opts.FileSystem, filepath.Join(db.opts.path, dbMetaName), m)
}

func (db *DB) readMeta() error {
	m := dbMeta{}
	if err := readGobFile(db.opts.FileSystem, filepath.Join(db.opts.path, dbMetaName), &m); err != nil {
		return err
	}
	db.hashSeed = m.HashSeed
	return nil
}

func (db *DB) hash(data []byte) uint32 {
	return hash.Sum32WithSeed(data, db.hashSeed)
}

func (db *DB) startSyncer(interval time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())
	db.cancelSyncer = cancel
	go func() {
		var lastModifications int64
		for {
			select {
			case <-ctx.Done():
				return
			default:
				modifications := db.metrics.Puts.Value() + db.metrics.Dels.Value()
				if modifications != lastModifications {
					if err := db.Sync(); err != nil {
						logger.Printf("error synchronizing databse: %v", err)
					}
					lastModifications = modifications
				}
				time.Sleep(interval)
			}
		}
	}()
}

// Get returns the value for the given key stored in the DB or nil if the key doesn't exist.
func (db *DB) Get(key []byte) ([]byte, error) {
	h := db.hash(key)
	db.metrics.Gets.Add(1)
	db.mu.RLock()
	defer db.mu.RUnlock()
	var retValue []byte
	err := db.index.get(h, func(sl slot) (bool, error) {
		slKey, value, err := db.datalog.readKeyValue(sl)
		if err != nil {
			return true, err
		}
		if bytes.Equal(key, slKey) {
			retValue = cloneBytes(value)
			return true, nil
		}
		db.metrics.HashCollisions.Add(1)
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return retValue, nil
}

// Has returns true if the DB contains the given key.
func (db *DB) Has(key []byte) (bool, error) {
	h := db.hash(key)
	found := false
	db.mu.RLock()
	defer db.mu.RUnlock()
	err := db.index.get(h, func(sl slot) (bool, error) {
		if uint16(len(key)) != sl.keySize {
			return false, nil
		}
		slKey, err := db.datalog.readKey(sl)
		if err != nil {
			return true, err
		}
		if bytes.Equal(key, slKey) {
			found = true
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return false, err
	}
	return found, nil
}

func (db *DB) put(sl slot, key []byte) error {
	return db.index.put(sl, func(cursl slot) (bool, error) {
		if uint16(len(key)) != cursl.keySize {
			return false, nil
		}
		slKey, err := db.datalog.readKey(cursl)
		if err != nil {
			return true, err
		}
		if bytes.Equal(key, slKey) {
			db.datalog.del(cursl) // Overwriting existing key
			return true, nil
		}
		return false, nil
	})
}

// Put sets the value for the given key. It updates the value for the existing key.
func (db *DB) Put(key []byte, value []byte) error {
	if len(key) > MaxKeyLength {
		return errKeyTooLarge
	}
	if len(value) > MaxValueLength {
		return errValueTooLarge
	}
	h := db.hash(key)
	db.metrics.Puts.Add(1)
	db.mu.Lock()
	defer db.mu.Unlock()
	fileID, offset, err := db.datalog.writeKeyValue(key, value)
	if err != nil {
		return err
	}
	sl := slot{
		hash:      h,
		fileID:    fileID,
		keySize:   uint16(len(key)),
		valueSize: uint32(len(value)),
		offset:    offset,
	}

	if err := db.put(sl, key); err != nil {
		return err
	}

	if db.syncWrites {
		return db.sync()
	}
	return nil
}

// Delete deletes the given key from the DB.
func (db *DB) Delete(key []byte) error {
	h := db.hash(key)
	db.metrics.Dels.Add(1)
	db.mu.Lock()
	defer db.mu.Unlock()
	err := db.index.delete(h, func(sl slot) (b bool, e error) {
		if uint16(len(key)) != sl.keySize {
			return false, nil
		}
		slKey, err := db.datalog.readKey(sl)
		if err != nil {
			return true, err
		}
		if bytes.Equal(key, slKey) {
			db.datalog.del(sl)
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return err
	}
	if db.syncWrites {
		return db.sync()
	}
	return nil
}

// Close closes the DB.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if atomic.LoadInt32(&db.compactionRunning) == 1 {
		return errBusy
	}
	if db.cancelSyncer != nil {
		db.cancelSyncer()
	}
	if err := db.writeManifest(); err != nil {
		return err
	}
	if err := db.datalog.close(); err != nil {
		return err
	}
	if err := db.index.close(); err != nil {
		return err
	}
	if err := db.lock.Unlock(); err != nil {
		return err
	}
	return nil
}

func (db *DB) sync() error {
	if err := db.datalog.sync(); err != nil {
		return err
	}
	if err := db.index.sync(); err != nil {
		return err
	}
	return nil
}

// Items returns a new ItemIterator.
func (db *DB) Items() *ItemIterator {
	return &ItemIterator{db: db}
}

// Sync commits the contents of the database to the backing FileSystem.
func (db *DB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.sync()
}

// Count returns the number of keys in the DB.
func (db *DB) Count() uint32 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.index.count()
}

// Metrics returns the DB metrics.
func (db *DB) Metrics() Metrics {
	return db.metrics
}

// FileSize returns the total size of the disk storage used by the DB.
func (db *DB) FileSize() (int64, error) {
	var size int64
	infos, err := ioutil.ReadDir(db.opts.path)
	if err != nil {
		return 0, err
	}
	for _, info := range infos {
		size += info.Size()
	}
	return size, nil
}
