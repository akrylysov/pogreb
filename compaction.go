package pogreb

import (
	"sync/atomic"
)

func (db *DB) updateIndexRecord(dstF *datafile, rec datafileRecord) (bool, error) {
	hash := db.hash(rec.key)
	reclaimed := true
	db.mu.Lock()
	defer db.mu.Unlock()
	err := db.index.forEachBucket(db.index.bucketIndex(hash), func(b bucketHandle) (bool, error) {
		for i := 0; i < slotsPerBucket; i++ {
			sl := b.slots[i]
			if sl.offset == 0 {
				return b.next == 0, nil
			}
			if hash == sl.hash && rec.offset == sl.offset && rec.fileID == sl.fileID {
				// Index points to the record, copy the record to dstF.
				dstOffset, err := dstF.append(rec.data) // TODO: batch writes
				if err != nil {
					return true, err
				}
				dstF.meta.TotalKeys++

				// Update index.
				b.slots[i].fileID = dstF.id
				b.slots[i].offset = uint32(dstOffset)
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
	dstF, err := dl.nextFreeFile()
	db.mu.Unlock()
	if err != nil {
		return cm, err
	}

	// Write items from f to dstF.
	it, err := newDatafileIterator(f)
	if err != nil {
		return cm, err
	}
	for {
		rec, err := it.next()
		if err == ErrIterationDone {
			break
		}
		if err != nil {
			return cm, err
		}
		reclaimed, err := db.updateIndexRecord(dstF, rec)
		if err != nil {
			return cm, err
		}
		if reclaimed {
			cm.ReclaimedItems++
			cm.ReclaimedBytes += len(rec.data)
		}
	}

	db.mu.Lock()
	dstF.meta.Full = false // Enable writes to the new file.

	// Update current file if was compacted.
	if f == dl.curFile {
		dl.curFile = dstF
	}

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
	}
	return cm, nil
}
