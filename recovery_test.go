package pogreb

import (
	"path/filepath"
	"testing"
)

func TestRecoveryCompaction(t *testing.T) {
	opts := &Options{
		maxDatafileSize:            1024,
		compactionMinDatafileSize:  512,
		compactionMinFragmentation: 0.2,
	}

	db, err := createTestDB(opts)
	assertNil(t, err)

	// Fill file 0.
	for i := 0; i < 41; i++ {
		assertNil(t, db.Put([]byte{0}, []byte{0}))
	}
	assertNil(t, db.Put([]byte{1}, []byte{1}))

	// Write to file 1.
	assertNil(t, db.Put([]byte{0}, []byte{0}))
	assertNil(t, db.Put([]byte{0}, []byte{0}))

	assertEqual(t, &datafileMeta{Full: true, TotalKeys: 42, DeletedKeys: 41, DeletedBytes: 492}, db.datalog.files[0].meta)
	assertEqual(t, &datafileMeta{TotalKeys: 2, DeletedKeys: 1, DeletedBytes: 12}, db.datalog.files[1].meta)

	cm, err := db.Compact()
	assertNil(t, err)
	assertEqual(t, CompactionMetrics{CompactedFiles: 1, ReclaimedItems: 41, ReclaimedBytes: 492}, cm)
	assertNil(t, db.datalog.files[0]) // Items were moved from file 0 to file 1.
	assertEqual(t, &datafileMeta{TotalKeys: 3, DeletedKeys: 1, DeletedBytes: 12}, db.datalog.files[1].meta)

	// Fill file 1.
	for i := 0; i < 40; i++ {
		assertNil(t, db.Put([]byte{1}, []byte{2}))
	}

	// Fill file 0.
	for i := 0; i < 42; i++ {
		assertNil(t, db.Put([]byte{1}, []byte{2}))
	}
	// Write to file 2.
	assertNil(t, db.Put([]byte{0}, []byte{0}))

	assertEqual(t, &datafileMeta{Full: true, TotalKeys: 42, DeletedKeys: 42, DeletedBytes: 504}, db.datalog.files[0].meta)
	assertEqual(t, &datafileMeta{Full: true, TotalKeys: 42, DeletedKeys: 42, DeletedBytes: 504}, db.datalog.files[1].meta)
	assertEqual(t, &datafileMeta{TotalKeys: 2}, db.datalog.files[2].meta)

	v, err := db.Get([]byte{1})
	assertNil(t, err)
	assertEqual(t, []byte{2}, v)

	assertEqual(t, uint32(2), db.Count())

	assertNil(t, db.Close())

	// Simulate crash.
	assertNil(t, touchFile(filepath.Join("test.db", lockName)))

	db, err = Open("test.db", nil)
	assertNil(t, err)

	assertEqual(t, uint32(2), db.Count())

	v, err = db.Get([]byte{1})
	assertNil(t, err)
	assertEqual(t, []byte{2}, v)

	assertNil(t, db.Close())
}
