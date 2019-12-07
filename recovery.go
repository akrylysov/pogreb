package pogreb

import (
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

func (db *DB) recover() error {
	logger.Println("started recovery")

	logger.Println("rebuilding index...")
	it, err := newDatalogIterator(db.datalog.files)
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
		db.datalog.files[rec.fileID].meta.TotalKeys++
	}

	logger.Println("successfully recovered database")

	return nil
}
