package pogreb

import (
	"sync/atomic"
)

const (
	compactionMaxFiles = 2
)

func (db *DB) moveRecord(rec datafileRecord) (bool, error) {
	hash := db.hash(rec.key)
	reclaimed := true
	err := db.index.forEachBucket(db.index.bucketIndex(hash), func(b bucketHandle) (bool, error) {
		for i, sl := range b.slots {
			if sl.offset == 0 {
				return b.next == 0, nil
			}
			if hash == sl.hash && rec.offset == sl.offset && rec.fileID == sl.fileID {
				fileID, offset, err := db.datalog.writeRecord(rec.data) // TODO: batch writes
				if err != nil {
					return true, err
				}
				// Update index.
				b.slots[i].fileID = fileID
				b.slots[i].offset = offset
				reclaimed = false
				return true, b.write()
			}
		}
		return false, nil
	})
	return reclaimed, err
}

type CompactionMetrics struct {
	CompactedFiles int
	ReclaimedItems int
	ReclaimedBytes int
}

func (db *DB) compact(f *datafile) (CompactionMetrics, error) {
	cm := CompactionMetrics{}
	dl := db.datalog

	db.mu.Lock()
	f.meta.Full = true // Prevent writes to the compacted file.
	db.mu.Unlock()

	// Move records from f to the current data file.
	it, err := newDatafileIterator(f)
	if err != nil {
		return cm, err
	}
	for {
		err := func() error {
			db.mu.Lock()
			defer db.mu.Unlock()
			rec, err := it.next()
			if err != nil {
				return err
			}
			reclaimed, err := db.moveRecord(rec)
			if reclaimed {
				cm.ReclaimedItems++
				cm.ReclaimedBytes += len(rec.data)
			}
			return err
		}()
		if err == ErrIterationDone {
			break
		}
		if err != nil {
			return cm, err
		}
	}

	db.mu.Lock()
	err = dl.removeFile(f)
	db.mu.Unlock()
	if err != nil {
		return cm, err
	}

	return cm, nil
}

// Compact compacts the DB. Deleted and overwritten items are discarded.
func (db *DB) Compact() (CompactionMetrics, error) {
	cm := CompactionMetrics{}
	if !atomic.CompareAndSwapInt32(&db.compactionRunning, 0, 1) {
		return cm, errBusy
	}
	defer func() {
		atomic.StoreInt32(&db.compactionRunning, 0)
	}()
	for {
		db.mu.RLock()
		f, err := db.datalog.pickForCompaction()
		db.mu.RUnlock()
		if err != nil {
			return cm, err
		}
		if f == nil {
			break
		}
		fcm, err := db.compact(f)
		if err != nil {
			return cm, err
		}
		cm.CompactedFiles++
		cm.ReclaimedItems += fcm.ReclaimedItems
		cm.ReclaimedBytes += fcm.ReclaimedBytes
		if cm.CompactedFiles == compactionMaxFiles {
			return cm, nil
		}
	}
	return cm, nil
}
