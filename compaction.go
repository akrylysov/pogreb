package pogreb

import (
	"sync/atomic"
)

func (db *DB) moveRecord(rec record) (bool, error) {
	hash := db.hash(rec.key)
	reclaimed := true
	err := db.index.forEachBucket(db.index.bucketIndex(hash), func(b bucketHandle) (bool, error) {
		for i, sl := range b.slots {
			if sl.offset == 0 {
				return b.next == 0, nil
			}

			// Slot points to a different record.
			if hash != sl.hash || rec.offset != sl.offset || rec.segmentID != sl.segmentID {
				continue
			}

			segmentID, offset, err := db.datalog.writeRecord(rec.data, rec.rtype) // TODO: batch writes
			if err != nil {
				return true, err
			}
			// Update index.
			b.slots[i].segmentID = segmentID
			b.slots[i].offset = offset
			reclaimed = false
			return true, b.write()
		}
		return false, nil
	})
	return reclaimed, err
}

// CompactionResult holds the compaction result.
type CompactionResult struct {
	CompactedSegments int
	ReclaimedRecords  int
	ReclaimedBytes    int
}

func (db *DB) compact(f *segment) (CompactionResult, error) {
	cr := CompactionResult{}

	db.mu.Lock()
	f.meta.Full = true // Prevent writes to the compacted file.
	db.mu.Unlock()

	it, err := newSegmentIterator(f)
	if err != nil {
		return cr, err
	}
	// Move records from f to the current segment.
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
	defer db.mu.Unlock()
	err = db.datalog.removeSegment(f)
	return cr, err
}

func (db *DB) pickForCompaction() ([]*segment, error) {
	segments, err := db.datalog.segmentsBySequenceID()
	if err != nil {
		return nil, err
	}
	var picked []*segment
	for i := len(segments) - 1; i >= 0; i-- {
		seg := segments[i]

		if uint32(seg.size) < db.opts.compactionMinSegmentSize {
			continue
		}

		fragmentation := float32(seg.meta.DeletedBytes) / float32(seg.size)
		if fragmentation < db.opts.compactionMinFragmentation {
			continue
		}

		if seg.meta.DeleteRecords > 0 {
			// Delete records can be discarded only when older files contain no put records for the corresponding keys.
			// All files older than the file eligible for compaction have to be compacted.
			return append(segments[:i+1], picked...), nil
		}

		picked = append([]*segment{seg}, picked...)
	}
	return picked, nil
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
	segments, err := db.pickForCompaction()
	db.mu.RUnlock()
	if err != nil {
		return cr, err
	}

	for _, f := range segments {
		fcr, err := db.compact(f)
		if err != nil {
			return cr, err
		}
		cr.CompactedSegments++
		cr.ReclaimedRecords += fcr.ReclaimedRecords
		cr.ReclaimedBytes += fcr.ReclaimedBytes
	}

	return cr, nil
}
