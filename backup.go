package pogreb

import (
	"io"
	"os"

	"github.com/akrylysov/pogreb/fs"
)

func touchFile(fsys fs.FileSystem, path string) error {
	f, err := fsys.OpenFile(path, os.O_CREATE|os.O_TRUNC, os.FileMode(0640))
	if err != nil {
		return err
	}
	return f.Close()
}

// Backup creates a database backup at the specified path.
func (db *DB) Backup(path string) error {
	// Make sure the compaction is not running during backup.
	db.maintenanceMu.Lock()
	defer db.maintenanceMu.Unlock()

	if err := db.opts.rootFS.MkdirAll(path, 0755); err != nil {
		return err
	}

	db.mu.RLock()
	var segments []*segment
	activeSegmentSizes := make(map[uint16]int64)
	for _, seg := range db.datalog.segmentsBySequenceID() {
		segments = append(segments, seg)
		if !seg.meta.Full {
			// Save the size of the active segments to copy only the data persisted up to the point
			// of when the backup started.
			activeSegmentSizes[seg.id] = seg.size
		}
	}
	db.mu.RUnlock()

	srcFS := db.opts.FileSystem
	dstFS := fs.Sub(db.opts.rootFS, path)

	for _, seg := range segments {
		name := segmentName(seg.id, seg.sequenceID)
		mode := os.FileMode(0640)
		srcFile, err := srcFS.OpenFile(name, os.O_RDONLY, mode)
		if err != nil {
			return err
		}

		dstFile, err := dstFS.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_TRUNC, mode)
		if err != nil {
			return err
		}

		if srcSize, ok := activeSegmentSizes[seg.id]; ok {
			if _, err := io.CopyN(dstFile, srcFile, srcSize); err != nil {
				return err
			}
		} else {
			if _, err := io.Copy(dstFile, srcFile); err != nil {
				return err
			}
		}

		if err := srcFile.Close(); err != nil {
			return err
		}
		if err := dstFile.Close(); err != nil {
			return err
		}
	}

	if err := touchFile(dstFS, lockName); err != nil {
		return err
	}

	return nil
}
