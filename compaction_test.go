package pogreb

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func fileExists(name string) bool {
	_, err := os.Stat(name)
	return !os.IsNotExist(err)
}

func countSegments(t *testing.T, db *DB) int {
	t.Helper()
	db.mu.RLock()
	defer db.mu.RUnlock()
	var c int
	for _, f := range db.datalog.segments {
		if f != nil {
			c++
			if f.meta == nil {
				t.Fatal()
			}
		}
	}
	return c
}

func TestCompaction(t *testing.T) {
	run := func(name string, f func(t *testing.T, db *DB)) bool {
		opts := &Options{
			maxSegmentSize:             1024,
			compactionMinSegmentSize:   512,
			compactionMinFragmentation: 0.02,
		}
		return t.Run(name, func(t *testing.T) {
			db, err := createTestDB(opts)
			assertNil(t, err)
			f(t, db)
			assertNil(t, db.Close())
		})
	}

	run("empty", func(t *testing.T, db *DB) {
		assertEqual(t, 1, countSegments(t, db))
		cr, err := db.Compact()
		assertNil(t, err)
		assertEqual(t, CompactionResult{}, cr)
		assertEqual(t, 1, countSegments(t, db))
	})

	// A single segment file can fit 42 items (12 bytes per item, 1 byte key, 1 byte value).
	const maxItemsPerFile byte = 42

	run("compact only segment", func(t *testing.T, db *DB) {
		// Write items and then overwrite them on the second iteration.
		for j := 0; j < 10; j++ {
			assertNil(t, db.Put([]byte{0}, []byte{0}))
		}
		assertEqual(t, 1, countSegments(t, db))
		assertEqual(t, &segmentMeta{Full: false, PutRecords: 10, DeletedKeys: 9, DeletedBytes: 108}, db.datalog.segments[0].meta)
		cr, err := db.Compact()
		assertNil(t, err)
		assertEqual(t, CompactionResult{CompactedSegments: 1, ReclaimedRecords: 9, ReclaimedBytes: 108}, cr)
		assertEqual(t, 1, countSegments(t, db))
		assertNil(t, db.datalog.segments[0])
		assertEqual(t, &segmentMeta{PutRecords: 1}, db.datalog.segments[1].meta)
		// Compacted file was removed.
		assertEqual(t, false, fileExists(filepath.Join(db.opts.path, segmentName(0))))
		assertEqual(t, false, fileExists(filepath.Join(db.opts.path, segmentMetaName(0))))
	})

	run("compact entire segment", func(t *testing.T, db *DB) {
		// Write items and then overwrite them on the second iteration.
		for i := 0; i < 2; i++ {
			for j := byte(0); j < maxItemsPerFile; j++ {
				assertNil(t, db.Put([]byte{j}, []byte{j}))
			}
		}
		assertEqual(t, 2, countSegments(t, db))
		assertEqual(t, &segmentMeta{Full: true, PutRecords: 42, DeletedKeys: 42, DeletedBytes: 504}, db.datalog.segments[0].meta)
		assertEqual(t, &segmentMeta{PutRecords: 42}, db.datalog.segments[1].meta)
		cr, err := db.Compact()
		assertNil(t, err)
		assertEqual(t, CompactionResult{CompactedSegments: 1, ReclaimedRecords: 42, ReclaimedBytes: 504}, cr)
		assertEqual(t, 1, countSegments(t, db))
		assertNil(t, db.datalog.segments[0])
		assertEqual(t, &segmentMeta{PutRecords: 42}, db.datalog.segments[1].meta)
	})

	run("compact part of segment", func(t *testing.T, db *DB) {
		for j := byte(0); j < maxItemsPerFile; j++ {
			assertNil(t, db.Put([]byte{j}, []byte{j}))
		}
		for i := byte(0); i < 40; i++ {
			assertNil(t, db.Put([]byte{i}, []byte{i}))
		}
		assertEqual(t, 2, countSegments(t, db))
		assertEqual(t, &segmentMeta{Full: true, PutRecords: 42, DeletedKeys: 40, DeletedBytes: 480}, db.datalog.segments[0].meta)
		assertEqual(t, &segmentMeta{PutRecords: 40}, db.datalog.segments[1].meta)
		cr, err := db.Compact()
		assertNil(t, err)
		assertEqual(t, CompactionResult{CompactedSegments: 1, ReclaimedRecords: 40, ReclaimedBytes: 480}, cr)
		assertEqual(t, 1, countSegments(t, db))
		assertNil(t, db.datalog.segments[0])
		assertEqual(t, &segmentMeta{PutRecords: 42}, db.datalog.segments[1].meta)
	})

	run("compact multiple segments", func(t *testing.T, db *DB) {
		for i := 0; i < 4; i++ {
			for j := byte(0); j < maxItemsPerFile; j++ {
				assertNil(t, db.Put([]byte{j}, []byte{j}))
			}
		}
		assertEqual(t, 4, countSegments(t, db))
		cr, err := db.Compact()
		assertNil(t, err)
		assertEqual(t, CompactionResult{CompactedSegments: 3, ReclaimedRecords: 126, ReclaimedBytes: 1512}, cr)
		assertEqual(t, 1, countSegments(t, db))
	})

	run("zero deleted bytes", func(t *testing.T, db *DB) {
		for i := byte(0); i < maxItemsPerFile; i++ {
			assertNil(t, db.Put([]byte{i}, []byte{i}))
		}
		assertEqual(t, 1, countSegments(t, db))
		assertEqual(t, &segmentMeta{PutRecords: 42}, db.datalog.segments[0].meta)
		cr, err := db.Compact()
		assertNil(t, err)
		assertEqual(t, CompactionResult{}, cr)
		assertEqual(t, 1, countSegments(t, db))
		assertEqual(t, &segmentMeta{PutRecords: 42}, db.datalog.segments[0].meta)
	})

	run("below threshold", func(t *testing.T, db *DB) {
		for j := byte(0); j < maxItemsPerFile; j++ {
			assertNil(t, db.Put([]byte{j}, []byte{j}))
		}
		assertNil(t, db.Put([]byte{0}, []byte{0}))
		assertEqual(t, 2, countSegments(t, db))
		assertEqual(t, &segmentMeta{Full: true, PutRecords: 42, DeletedKeys: 1, DeletedBytes: 12}, db.datalog.segments[0].meta)
		assertEqual(t, &segmentMeta{PutRecords: 1}, db.datalog.segments[1].meta)
		cr, err := db.Compact()
		assertNil(t, err)
		assertEqual(t, CompactionResult{}, cr)
		assertEqual(t, 2, countSegments(t, db))
	})

	run("above threshold", func(t *testing.T, db *DB) {
		for j := byte(0); j < maxItemsPerFile; j++ {
			assertNil(t, db.Put([]byte{j}, []byte{j}))
		}
		assertNil(t, db.Put([]byte{0}, []byte{0}))
		assertNil(t, db.Put([]byte{1}, []byte{1}))
		assertEqual(t, 2, countSegments(t, db))
		assertEqual(t, &segmentMeta{Full: true, PutRecords: 42, DeletedKeys: 2, DeletedBytes: 24}, db.datalog.segments[0].meta)
		assertEqual(t, &segmentMeta{PutRecords: 2}, db.datalog.segments[1].meta)
		cr, err := db.Compact()
		assertNil(t, err)
		assertEqual(t, CompactionResult{CompactedSegments: 1, ReclaimedRecords: 2, ReclaimedBytes: 24}, cr)
		assertEqual(t, 1, countSegments(t, db))
	})

	run("compact single segment in the middle: puts", func(t *testing.T, db *DB) {
		// Write two segments.
		for j := byte(0); j < maxItemsPerFile*2; j++ {
			assertNil(t, db.Put([]byte{j}, []byte{j}))
		}
		// Overwrite second segment items.
		for j := maxItemsPerFile; j < maxItemsPerFile*2; j++ {
			assertNil(t, db.Put([]byte{j}, []byte{j}))
		}
		assertEqual(t, 3, countSegments(t, db))
		cr, err := db.Compact()
		assertNil(t, err)
		assertEqual(t, CompactionResult{CompactedSegments: 1, ReclaimedRecords: 42, ReclaimedBytes: 504}, cr)
		assertEqual(t, 2, countSegments(t, db))
	})

	run("compact single segment in the middle: deletes", func(t *testing.T, db *DB) {
		for j := byte(0); j < (maxItemsPerFile*2)-1; j++ {
			assertNil(t, db.Put([]byte{j}, []byte{j}))
		}
		assertNil(t, db.Delete([]byte{maxItemsPerFile}))
		assertNil(t, db.Put([]byte{maxItemsPerFile}, []byte{0}))
		assertNil(t, db.Put([]byte{maxItemsPerFile + 1}, []byte{0}))

		assertEqual(t, 3, countSegments(t, db))
		assertEqual(t, &segmentMeta{Full: true, PutRecords: 42}, db.datalog.segments[0].meta)
		assertEqual(t, &segmentMeta{Full: true, PutRecords: 41, DeleteRecords: 1, DeletedKeys: 2, DeletedBytes: 35}, db.datalog.segments[1].meta)
		assertEqual(t, &segmentMeta{Full: false, PutRecords: 2}, db.datalog.segments[2].meta)

		cr, err := db.Compact()
		assertNil(t, err)
		assertEqual(t, CompactionResult{CompactedSegments: 2, ReclaimedRecords: 3, ReclaimedBytes: 35}, cr)
		assertEqual(t, 2, countSegments(t, db))
	})

	run("delete and compact all segments", func(t *testing.T, db *DB) {
		// Write items.
		for i := byte(0); i < maxItemsPerFile; i++ {
			assertNil(t, db.Put([]byte{i}, []byte{i}))
		}
		// Delete items.
		for i := byte(0); i < maxItemsPerFile; i++ {
			assertNil(t, db.Delete([]byte{i}))
		}
		assertEqual(t, 2, countSegments(t, db))
		assertEqual(t, &segmentMeta{Full: true, PutRecords: 42, DeletedKeys: 42, DeletedBytes: 504}, db.datalog.segments[0].meta)
		assertEqual(t, &segmentMeta{DeleteRecords: 42, DeletedKeys: 0, DeletedBytes: 462}, db.datalog.segments[1].meta)
		cr, err := db.Compact()
		assertNil(t, err)
		assertEqual(t, CompactionResult{CompactedSegments: 2, ReclaimedRecords: 84, ReclaimedBytes: 966}, cr)
		assertEqual(t, 0, countSegments(t, db))
		assertNil(t, db.datalog.segments[0])
		assertNil(t, db.datalog.segments[1])
	})
}

func TestBackgroundCompaction(t *testing.T) {
	opts := &Options{
		BackgroundCompactionInterval: time.Millisecond,
		maxSegmentSize:               1024,
		compactionMinSegmentSize:     512,
		compactionMinFragmentation:   0.2,
	}

	db, err := createTestDB(opts)
	assertNil(t, err)

	for i := 0; i < 128; i++ {
		if err := db.Put([]byte{1}, []byte{1}); err != nil {
			t.Fatal(err)
		}
	}

	completeWithin(t, time.Minute, func() bool {
		return countSegments(t, db) == 1
	})

	assertNil(t, db.Close())
}
