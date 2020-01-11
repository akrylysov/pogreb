package pogreb

import (
	"sync/atomic"
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

// CompactionResult holds the compaction result.
type CompactionResult struct {
	CompactedFiles   int
	ReclaimedRecords int
	ReclaimedBytes   int
}

func (db *DB) compact(f *datafile) (CompactionResult, error) {
	cr := CompactionResult{}

	db.mu.Lock()
	f.meta.Full = true // Prevent writes to the compacted file.
	db.mu.Unlock()

	// Move records from f to the current data file.
	it, err := newDatafileIterator(f)
	if err != nil {
		return cr, err
	}
	for {
		err := func() error {
			db.mu.Lock()
			defer db.mu.Unlock()
			rec, err := it.next()
			if err != nil {
				return err
			}
			if rec.rtype == recordTypeDelete {
				cr.ReclaimedRecords++
				cr.ReclaimedBytes += len(rec.data)
				return nil
			}
			reclaimed, err := db.moveRecord(rec)
			if reclaimed {
				cr.ReclaimedRecords++
				cr.ReclaimedBytes += len(rec.data)
			}
			return err
		}()
		if err == ErrIterationDone {
			break
		}
		if err != nil {
			return cr, err
		}
	}

	db.mu.Lock()
	err = db.datalog.removeFile(f)
	db.mu.Unlock()
	if err != nil {
		return cr, err
	}

	return cr, nil
}

func (db *DB) pickForCompaction() ([]*datafile, error) {
	files, err := db.datalog.filesByModification()
	if err != nil {
		return nil, err
	}
	for i := len(files) - 1; i >= 0; i-- {
		f := files[i]
		if uint32(f.size) < db.opts.compactionMinDatafileSize {
			continue
		}
		fragmentation := float32(f.meta.DeletedBytes) / float32(f.size)
		if fragmentation < db.opts.compactionMinFragmentation {
			continue
		}
		// All files older than the file eligible for compaction have to be compacted.
		// Delete records can be discarded only when older files contain no put records for the corresponding keys.
		return files[:i+1], nil
	}
	return nil, nil
}

// Compact compacts the DB. Deleted and overwritten items are discarded.
func (db *DB) Compact() (CompactionResult, error) {
	cr := CompactionResult{}

	// Run only a single compaction at a time.
	if !atomic.CompareAndSwapInt32(&db.compactionRunning, 0, 1) {
		return cr, errBusy
	}
	defer func() {
		atomic.StoreInt32(&db.compactionRunning, 0)
	}()

	db.mu.RLock()
	files, err := db.pickForCompaction()
	db.mu.RUnlock()
	if err != nil {
		return cr, err
	}

	for _, f := range files {
		fcr, err := db.compact(f)
		if err != nil {
			return cr, err
		}
		cr.CompactedFiles++
		cr.ReclaimedRecords += fcr.ReclaimedRecords
		cr.ReclaimedBytes += fcr.ReclaimedBytes
	}

	return cr, nil
}
