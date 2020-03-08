package pogreb

import (
	"fmt"
	"path/filepath"
	"testing"
)

func TestRecovery(t *testing.T) {
	dfPath := filepath.Join("test.db", segmentName(0))
	testCases := []struct {
		name string
		fn   func() error
	}{
		{
			name: "all zeroes",
			fn: func() error {
				return appendFile(dfPath, make([]byte, 128))
			},
		},
		{
			name: "partial kv size",
			fn: func() error {
				return appendFile(dfPath, []byte{1})
			},
		},
		{
			name: "only kv size",
			fn: func() error {
				return appendFile(dfPath, []byte{1, 0, 1, 0, 0, 0})
			},
		},
		{
			name: "kv size and key",
			fn: func() error {
				return appendFile(dfPath, []byte{1, 0, 1, 0, 0, 0, 1})
			},
		},
		{
			name: "kv size, key, value",
			fn: func() error {
				return appendFile(dfPath, []byte{1, 0, 1, 0, 0, 0, 1, 1})
			},
		},
		{
			name: "kv size, key, value, partial crc32",
			fn: func() error {
				return appendFile(dfPath, []byte{1, 0, 1, 0, 0, 0, 1, 1, 40})
			},
		},
		{
			name: "kv size, key, value, invalid crc32",
			fn: func() error {
				return appendFile(dfPath, []byte{1, 0, 1, 0, 0, 0, 1, 1, 40, 19, 197, 0})
			},
		},
		{
			name: "corrupted and not corrupted record",
			fn: func() error {
				if err := appendFile(dfPath, []byte{1, 0, 1, 0, 0, 0, 1, 1, 40, 19, 197, 0}); err != nil {
					return err
				}
				return appendFile(dfPath, []byte{1, 0, 1, 0, 0, 0, 1, 1, 133, 13, 200, 12})
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("case %s", testCase.name), func(t *testing.T) {
			db, err := createTestDB(nil)
			assertNil(t, err)
			// Fill file 0.
			var i uint8
			for i = 0; i < 128; i++ {
				assertNil(t, db.Put([]byte{i}, []byte{i}))
			}
			assertEqual(t, uint32(128), db.Count())
			assertNil(t, db.Close())

			// Simulate crash.
			assertNil(t, touchFile(filepath.Join("test.db", lockName)))

			assertNil(t, testCase.fn())

			db, err = Open("test.db", nil)
			assertNil(t, err)
			assertEqual(t, uint32(128), db.Count())
			assertNil(t, db.Close())

			db, err = Open("test.db", nil)
			assertNil(t, err)
			assertEqual(t, uint32(128), db.Count())
			for i = 0; i < 128; i++ {
				v, err := db.Get([]byte{i})
				assertNil(t, err)
				assertEqual(t, []byte{i}, v)
			}
			assertNil(t, db.Close())
		})
	}
}

func TestRecoveryDelete(t *testing.T) {
	db, err := createTestDB(nil)
	assertNil(t, err)
	assertNil(t, db.Put([]byte{1}, []byte{1}))
	assertNil(t, db.Put([]byte{2}, []byte{2}))
	assertNil(t, db.Delete([]byte{1}))
	assertEqual(t, uint32(1), db.Count())
	assertNil(t, db.Close())

	// Simulate crash.
	assertNil(t, touchFile(filepath.Join("test.db", lockName)))

	db, err = Open("test.db", nil)
	assertNil(t, err)

	assertEqual(t, uint32(1), db.Count())

	assertNil(t, db.Close())
}

func TestRecoveryCompaction(t *testing.T) {
	opts := &Options{
		maxSegmentSize:             1024,
		compactionMinSegmentSize:   512,
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

	assertEqual(t, &segmentMeta{Full: true, PutRecords: 42, DeletedKeys: 41, DeletedBytes: 492}, db.datalog.segments[0].meta)
	assertEqual(t, &segmentMeta{PutRecords: 2, DeletedKeys: 1, DeletedBytes: 12}, db.datalog.segments[1].meta)

	cm, err := db.Compact()
	assertNil(t, err)
	assertEqual(t, CompactionResult{CompactedSegments: 1, ReclaimedRecords: 41, ReclaimedBytes: 492}, cm)
	assertNil(t, db.datalog.segments[0]) // Items were moved from file 0 to file 1.
	assertEqual(t, &segmentMeta{PutRecords: 3, DeletedKeys: 1, DeletedBytes: 12}, db.datalog.segments[1].meta)

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

	assertEqual(t, &segmentMeta{Full: true, PutRecords: 42, DeletedKeys: 42, DeletedBytes: 504}, db.datalog.segments[0].meta)
	assertEqual(t, &segmentMeta{Full: true, PutRecords: 42, DeletedKeys: 42, DeletedBytes: 504}, db.datalog.segments[1].meta)
	assertEqual(t, &segmentMeta{PutRecords: 2}, db.datalog.segments[2].meta)

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

func TestRecoveryIterator(t *testing.T) {
	db, err := createTestDB(nil)
	assertNil(t, err)

	listRecords := func() []record {
		var records []record
		it, err := newRecoveryIterator(db.datalog)
		assertNil(t, err)
		for {
			rec, err := it.next()
			if err == ErrIterationDone {
				break
			}
			assertNil(t, err)
			records = append(records, rec)
		}
		return records
	}

	if len(listRecords()) != 0 {
		t.Fatal()
	}

	if err := db.Put([]byte{1}, []byte{1}); err != nil {
		t.Fatal(err)
	}
	assertEqual(t,
		[]record{
			{recordTypePut, 0, 512, []byte{1, 0, 1, 0, 0, 0, 1, 1, 133, 13, 200, 12}, []byte{1}, []byte{1}},
		},
		listRecords(),
	)

	if err := db.Put([]byte{1}, []byte{1}); err != nil {
		t.Fatal(err)
	}
	assertEqual(t,
		[]record{
			{recordTypePut, 0, 512, []byte{1, 0, 1, 0, 0, 0, 1, 1, 133, 13, 200, 12}, []byte{1}, []byte{1}},
			{recordTypePut, 0, 524, []byte{1, 0, 1, 0, 0, 0, 1, 1, 133, 13, 200, 12}, []byte{1}, []byte{1}},
		},
		listRecords(),
	)

	if err := db.Put([]byte{2}, []byte{2}); err != nil {
		t.Fatal(err)
	}
	assertEqual(t,
		[]record{
			{recordTypePut, 0, 512, []byte{1, 0, 1, 0, 0, 0, 1, 1, 133, 13, 200, 12}, []byte{1}, []byte{1}},
			{recordTypePut, 0, 524, []byte{1, 0, 1, 0, 0, 0, 1, 1, 133, 13, 200, 12}, []byte{1}, []byte{1}},
			{recordTypePut, 0, 536, []byte{1, 0, 1, 0, 0, 0, 2, 2, 252, 15, 236, 190}, []byte{2}, []byte{2}},
		},
		listRecords(),
	)

	assertNil(t, db.Close())
}
