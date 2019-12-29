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

func countDatafiles(t *testing.T, db *DB) int {
	t.Helper()
	db.mu.RLock()
	defer db.mu.RUnlock()
	var c int
	for _, f := range db.datalog.files {
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
		maxDatafileSize:            1024,
		compactionMinDatafileSize:  512,
		compactionMinFragmentation: 0.2,
	}

	db, err := createTestDB(opts)
	assertNil(t, err)

	// A single data file can fit 42 items (12 bytes per item, 1 byte key, 1 byte value).
	numFiles := func() int {
		return countDatafiles(t, db)
	}

	t.Run("empty", func(t *testing.T) {
		assertEqual(t, 1, numFiles())
		cr, err := db.Compact()
		assertNil(t, err)
		assertEqual(t, CompactionResult{}, cr)
		assertEqual(t, 1, numFiles())
	})

	var i byte
	var n uint8 = 255

	t.Run("no compaction", func(t *testing.T) {
		for i = 0; i < 42; i++ {
			if err := db.Put([]byte{i}, []byte{i}); err != nil {
				t.Fatal(err)
			}
		}
		assertEqual(t, 1, numFiles())
		cr, err := db.Compact()
		assertNil(t, err)
		assertEqual(t, CompactionResult{}, cr)
		assertEqual(t, 1, numFiles())
	})

	t.Run("compact full current", func(t *testing.T) {
		for i = 0; i < 42; i++ {
			if err := db.Put([]byte{i}, []byte{i}); err != nil {
				t.Fatal(err)
			}
		}
		assertEqual(t, 2, numFiles())
		cr, err := db.Compact()
		assertNil(t, err)
		assertEqual(t, CompactionResult{CompactedFiles: 1, ReclaimedItems: 42, ReclaimedBytes: 504}, cr)
		assertEqual(t, 1, numFiles())
		assertNil(t, db.datalog.files[0])
		assertEqual(t, &datafileMeta{TotalKeys: 42}, db.datalog.files[1].meta)
		// Compacted file was removed.
		assertEqual(t, false, fileExists(filepath.Join(db.opts.path, datafileName(0))))
		assertEqual(t, false, fileExists(filepath.Join(db.opts.path, datafileName(0))+metaExt))
	})

	t.Run("delete all", func(t *testing.T) {
		for i = 0; i < n; i++ {
			if err := db.Delete([]byte{i}); err != nil {
				t.Fatal(err)
			}
		}
		assertEqual(t, 1, numFiles())
		assertEqual(t, &datafileMeta{TotalKeys: 42, DeletedKeys: 42, DeletedBytes: 504}, db.datalog.files[1].meta)
		cr, err := db.Compact()
		assertNil(t, err)
		assertEqual(t, CompactionResult{CompactedFiles: 1, ReclaimedItems: 42, ReclaimedBytes: 504}, cr)
		assertEqual(t, 0, numFiles())
		assertNil(t, db.datalog.files[0])
		assertNil(t, db.datalog.files[1])
	})

	t.Run("no reclaimed", func(t *testing.T) {
		for i = 0; i < n; i++ {
			if err := db.Put([]byte{i}, []byte{i}); err != nil {
				t.Fatal(err)
			}
		}
		assertEqual(t, 7, numFiles())

		cr, err := db.Compact()
		assertNil(t, err)
		assertEqual(t, CompactionResult{}, cr)
		assertEqual(t, 7, numFiles())
	})

	t.Run("compact single file", func(t *testing.T) {
		for i = 0; i < 40; i++ {
			if err := db.Put([]byte{i}, []byte{i}); err != nil {
				t.Fatal(err)
			}
		}
		cr, err := db.Compact()
		assertNil(t, err)
		assertEqual(t, CompactionResult{CompactedFiles: 1, ReclaimedItems: 40, ReclaimedBytes: 480}, cr)
		assertEqual(t, 7, numFiles())
	})

	t.Run("compact multiple files", func(t *testing.T) {
		for i = 42; i < 126; i++ {
			if err := db.Put([]byte{i}, []byte{i}); err != nil {
				t.Fatal(err)
			}
		}
		cr, err := db.Compact()
		assertNil(t, err)
		assertEqual(t, CompactionResult{CompactedFiles: 2, ReclaimedItems: 84, ReclaimedBytes: 1008}, cr)
		assertEqual(t, 7, numFiles())
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
		maxDatafileSize:              1024,
		compactionMinDatafileSize:    512,
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
		return countDatafiles(t, db) == 1
	})

	assertNil(t, db.Close())
}
