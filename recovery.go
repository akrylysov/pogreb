package pogreb

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

func backupNondataFiles(path string) error {
	logger.Println("moving non-data files...")

	// move all index and meta files to tmp
	tmpDir, err := ioutil.TempDir("", "pogreb_recovery")
	if err != nil {
		return err
	}

	names, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}

	for _, name := range names {
		ext := filepath.Ext(name.Name())
		if ext != metaExt && ext != indexExt {
			continue
		}
		oldpath := filepath.Join(path, name.Name())
		newpath := filepath.Join(tmpDir, name.Name())
		if err := os.Rename(oldpath, newpath); err != nil {
			return err
		}
		logger.Printf("moved %s to %s", oldpath, newpath)
	}

	return nil
}

// recoveryIterator iterates over records of all datalog files in insertion order.
// Corrupted files are truncated to the last valid record.
type recoveryIterator struct {
	files []*datafile
	dit   *datafileIterator
}

func newRecoveryIterator(dl *datalog) (*recoveryIterator, error) {
	files, err := dl.filesByModification()
	if err != nil {
		return nil, err
	}
	return &recoveryIterator{
		files: files,
	}, nil
}

func (it *recoveryIterator) next() (datafileRecord, error) {
	for {
		if it.dit == nil {
			if len(it.files) == 0 {
				return datafileRecord{}, ErrIterationDone
			}
			var err error
			it.dit, err = newDatafileIterator(it.files[0])
			if err != nil {
				return datafileRecord{}, err
			}
			it.files = it.files[1:]
		}
		rec, err := it.dit.next()
		if err == io.EOF || err == io.ErrUnexpectedEOF || err == errCorrupted {
			// Truncate file to the last valid offset.
			if err := it.dit.f.truncate(it.dit.offset); err != nil {
				return datafileRecord{}, err
			}
			fi, fierr := it.dit.f.Stat()
			if fierr != nil {
				return datafileRecord{}, fierr
			}
			logger.Printf("truncated data file %s to offset %d", fi.Name(), it.dit.offset)
			err = ErrIterationDone
		}
		if err == ErrIterationDone {
			it.dit = nil
			continue
		}
		if err != nil {
			return datafileRecord{}, err
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
		if rec.rtype == recordTypePut {
			sl := slot{
				hash:      h,
				fileID:    rec.fileID,
				keySize:   uint16(len(rec.key)),
				valueSize: uint32(len(rec.value)),
				offset:    rec.offset,
			}
			if err := db.put(sl, rec.key); err != nil {
				return err
			}
			db.datalog.files[rec.fileID].meta.TotalRecords++
		} else {
			if err := db.del(h, rec.key); err != nil {
				return err
			}
		}
	}

	logger.Println("successfully recovered database")

	return nil
}
