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
	opts := &Options{
		maxSegmentSize:             1024,
		compactionMinSegmentSize:   512,
		compactionMinFragmentation: 0.2,
	}

	db, err := createTestDB(opts)
	assertNil(t, err)

	// A single segment file can fit 42 items (12 bytes per item, 1 byte key, 1 byte value).
	const maxItemsPerFile = 42

	numSegments := func() int {
		return countSegments(t, db)
	}

	t.Run("empty", func(t *testing.T) {
		assertEqual(t, 1, numSegments())
		cr, err := db.Compact()
		assertNil(t, err)
		assertEqual(t, CompactionResult{}, cr)
		assertEqual(t, 1, numSegments())
	})

	var i byte
	var n uint8 = 255

	t.Run("no compaction", func(t *testing.T) {
		for i = 0; i < maxItemsPerFile; i++ {
			if err := db.Put([]byte{i}, []byte{i}); err != nil {
				t.Fatal(err)
			}
		}
		assertEqual(t, 1, numSegments())
		assertEqual(t, &segmentMeta{TotalRecords: 42}, db.datalog.segments[0].meta)
		cr, err := db.Compact()
		assertNil(t, err)
		assertEqual(t, CompactionResult{}, cr)
		assertEqual(t, 1, numSegments())
		assertEqual(t, &segmentMeta{TotalRecords: 42}, db.datalog.segments[0].meta)
	})

	t.Run("compact full", func(t *testing.T) {
		for i = 0; i < maxItemsPerFile; i++ {
			if err := db.Put([]byte{i}, []byte{i}); err != nil {
				t.Fatal(err)
			}
		}
		assertEqual(t, 2, numSegments())
		assertEqual(t, &segmentMeta{Full: true, TotalRecords: 42, DeletedKeys: 42, DeletedBytes: 504}, db.datalog.segments[0].meta)
		assertEqual(t, &segmentMeta{TotalRecords: 42}, db.datalog.segments[1].meta)
		cr, err := db.Compact()
		assertNil(t, err)
		assertEqual(t, CompactionResult{CompactedSegments: 1, ReclaimedRecords: 42, ReclaimedBytes: 504}, cr)
		assertEqual(t, 1, numSegments())
		assertNil(t, db.datalog.segments[0])
		assertEqual(t, &segmentMeta{TotalRecords: 42}, db.datalog.segments[1].meta)
		// Compacted file was removed.
		assertEqual(t, false, fileExists(filepath.Join(db.opts.path, segmentName(0))))
		assertEqual(t, false, fileExists(filepath.Join(db.opts.path, segmentMetaName(0))))
	})

	t.Run("delete all", func(t *testing.T) {
		for i = 0; i < n; i++ {
			if err := db.Delete([]byte{i}); err != nil {
				t.Fatal(err)
			}
		}
		assertEqual(t, 2, numSegments())
		assertEqual(t, &segmentMeta{TotalRecords: 42, DeletedKeys: 0, DeletedBytes: 462}, db.datalog.segments[0].meta)
		assertEqual(t, &segmentMeta{Full: true, TotalRecords: 42, DeletedKeys: 42, DeletedBytes: 504}, db.datalog.segments[1].meta)
		cr, err := db.Compact()
		assertNil(t, err)
		assertEqual(t, CompactionResult{CompactedSegments: 2, ReclaimedRecords: 84, ReclaimedBytes: 966}, cr)
		assertEqual(t, 0, numSegments())
		assertNil(t, db.datalog.segments[0])
		assertNil(t, db.datalog.segments[1])
	})

	t.Run("no reclaimed", func(t *testing.T) {
		for i = 0; i < n; i++ {
			if err := db.Put([]byte{i}, []byte{i}); err != nil {
				t.Fatal(err)
			}
		}
		assertEqual(t, 7, numSegments())

		cr, err := db.Compact()
		assertNil(t, err)
		assertEqual(t, CompactionResult{}, cr)
		assertEqual(t, 7, numSegments())
	})

	t.Run("compact single file", func(t *testing.T) {
		for i = 0; i < 40; i++ {
			if err := db.Put([]byte{i}, []byte{i}); err != nil {
				t.Fatal(err)
			}
		}
		cr, err := db.Compact()
		assertNil(t, err)
		assertEqual(t, CompactionResult{CompactedSegments: 1, ReclaimedRecords: 40, ReclaimedBytes: 480}, cr)
		assertEqual(t, 7, numSegments())
	})

	t.Run("compact multiple files", func(t *testing.T) {
		for i = 42; i < 126; i++ {
			if err := db.Put([]byte{i}, []byte{i}); err != nil {
				t.Fatal(err)
			}
		}
		cr, err := db.Compact()
		assertNil(t, err)
		assertEqual(t, CompactionResult{CompactedSegments: 2, ReclaimedRecords: 84, ReclaimedBytes: 1008}, cr)
		assertEqual(t, 7, numSegments())
	})

	for i = 0; i < n; i++ {
		if has, err := db.Has([]byte{i}); !has || err != nil {
			t.Fatal(has, err)
		}
		v, err := db.Get([]byte{i})
		assertNil(t, err)
		assertEqual(t, []byte{i}, v)
	}

	assertNil(t, db.Close())
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
