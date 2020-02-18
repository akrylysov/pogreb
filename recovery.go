package pogreb

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

const (
	recoveryBackupExt = ".bac"
)

func backupNonsegmentFiles(path string) error {
	logger.Println("moving non-segment files...")

	files, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}

	for _, file := range files {
		name := file.Name()
		ext := filepath.Ext(name)
		if ext == segmentExt || name == lockName {
			continue
		}
		src := filepath.Join(path, name)
		dst := src + recoveryBackupExt
		if err := os.Rename(src, dst); err != nil {
			return err
		}
		logger.Printf("moved %s to %s", src, dst)
	}

	return nil
}

func removeRecoveryBackupFiles(path string) error {
	logger.Println("removing recovery backup files...")

	files, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}

	for _, file := range files {
		name := file.Name()
		ext := filepath.Ext(name)
		if ext != recoveryBackupExt {
			continue
		}
		src := filepath.Join(path, name)
		if err := os.Remove(src); err != nil {
			return err
		}
		logger.Printf("removed %s", src)
	}

	return nil
}

// recoveryIterator iterates over records of all datalog files in insertion order.
// Corrupted files are truncated to the last valid record.
type recoveryIterator struct {
	files []*segment
	dit   *segmentIterator
}

func newRecoveryIterator(dl *datalog) (*recoveryIterator, error) {
	files, err := dl.segmentsByModification()
	if err != nil {
		return nil, err
	}
	return &recoveryIterator{
		files: files,
	}, nil
}

func (it *recoveryIterator) next() (record, error) {
	for {
		if it.dit == nil {
			if len(it.files) == 0 {
				return record{}, ErrIterationDone
			}
			var err error
			it.dit, err = newSegmentIterator(it.files[0])
			if err != nil {
				return record{}, err
			}
			it.files = it.files[1:]
		}
		rec, err := it.dit.next()
		if err == io.EOF || err == io.ErrUnexpectedEOF || err == errCorrupted {
			// Truncate file to the last valid offset.
			if err := it.dit.f.truncate(it.dit.offset); err != nil {
				return record{}, err
			}
			fi, fierr := it.dit.f.Stat()
			if fierr != nil {
				return record{}, fierr
			}
			logger.Printf("truncated data file %s to offset %d", fi.Name(), it.dit.offset)
			err = ErrIterationDone
		}
		if err == ErrIterationDone {
			it.dit = nil
			continue
		}
		if err != nil {
			return record{}, err
		}
		return rec, nil
	}
}

func (db *DB) recover() error {
	logger.Println("started recovery")

	logger.Println("rebuilding index...")
	it, err := newRecoveryIterator(db.datalog)
	if err != nil {
		return err
	}
	for {
		rec, err := it.next()
		if err == ErrIterationDone {
			break
		}
		if err != nil {
			return err
		}

		h := db.hash(rec.key)
		meta := db.datalog.segments[rec.segmentID].meta
		if rec.rtype == recordTypePut {
			sl := slot{
				hash:      h,
				segmentID: rec.segmentID,
				keySize:   uint16(len(rec.key)),
				valueSize: uint32(len(rec.value)),
				offset:    rec.offset,
			}
			if err := db.put(sl, rec.key); err != nil {
				return err
			}
			meta.PutRecords++
		} else {
			if err := db.del(h, rec.key); err != nil {
				return err
			}
			meta.DeleteRecords++
		}
	}

	if err := removeRecoveryBackupFiles(db.opts.path); err != nil {
		logger.Printf("error removing recovery backups files: %v", err)
	}

	logger.Println("successfully recovered database")

	return nil
}
