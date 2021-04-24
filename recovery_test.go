package pogreb

import (
	"path/filepath"
	"testing"

	"github.com/akrylysov/pogreb/internal/assert"
)

func TestRecovery(t *testing.T) {
	segPath := filepath.Join(testDBName, segmentName(0, 1))
	testCases := []struct {
		name string
		fn   func() error
	}{
		{
			name: "all zeroes",
			fn: func() error {
				return appendFile(segPath, make([]byte, 128))
			},
		},
		{
			name: "partial kv size",
			fn: func() error {
				return appendFile(segPath, []byte{1})
			},
		},
		{
			name: "only kv size",
			fn: func() error {
				return appendFile(segPath, []byte{1, 0, 1, 0, 0, 0})
			},
		},
		{
			name: "kv size and key",
			fn: func() error {
				return appendFile(segPath, []byte{1, 0, 1, 0, 0, 0, 1})
			},
		},
		{
			name: "kv size, key, value",
			fn: func() error {
				return appendFile(segPath, []byte{1, 0, 1, 0, 0, 0, 1, 1})
			},
		},
		{
			name: "kv size, key, value, partial crc32",
			fn: func() error {
				return appendFile(segPath, []byte{1, 0, 1, 0, 0, 0, 1, 1, 40})
			},
		},
		{
			name: "kv size, key, value, invalid crc32",
			fn: func() error {
				return appendFile(segPath, []byte{1, 0, 1, 0, 0, 0, 1, 1, 40, 19, 197, 0})
			},
		},
		{
			name: "corrupted and not corrupted record",
			fn: func() error {
				if err := appendFile(segPath, []byte{1, 0, 1, 0, 0, 0, 1, 1, 40, 19, 197, 0}); err != nil {
					return err
				}
				return appendFile(segPath, []byte{1, 0, 1, 0, 0, 0, 1, 1, 133, 13, 200, 12})
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			opts := &Options{FileSystem: testFS}
			db, err := createTestDB(opts)
			assert.Nil(t, err)
			// Fill segment 0.
			var i uint8
			for i = 0; i < 128; i++ {
				assert.Nil(t, db.Put([]byte{i}, []byte{i}))
			}
			assert.Equal(t, uint32(128), db.Count())
			assert.Nil(t, db.Close())

			// Simulate crash.
			assert.Nil(t, touchFile(testFS, filepath.Join(testDBName, lockName)))

			assert.Nil(t, testCase.fn())

			db, err = Open(testDBName, opts)
			assert.Nil(t, err)
			assert.Equal(t, uint32(128), db.Count())
			assert.Nil(t, db.Close())

			db, err = Open(testDBName, opts)
			assert.Nil(t, err)
			assert.Equal(t, uint32(128), db.Count())
			for i = 0; i < 128; i++ {
				v, err := db.Get([]byte{i})
				assert.Nil(t, err)
				assert.Equal(t, []byte{i}, v)
			}
			assert.Nil(t, db.Close())
		})
	}
}

func TestRecoveryDelete(t *testing.T) {
	opts := &Options{FileSystem: testFS}
	db, err := createTestDB(opts)
	assert.Nil(t, err)
	assert.Nil(t, db.Put([]byte{1}, []byte{1}))
	assert.Nil(t, db.Put([]byte{2}, []byte{2}))
	assert.Nil(t, db.Delete([]byte{1}))
	assert.Equal(t, uint32(1), db.Count())
	assert.Nil(t, db.Close())

	// Simulate crash.
	assert.Nil(t, touchFile(testFS, filepath.Join(testDBName, lockName)))

	db, err = Open(testDBName, opts)
	assert.Nil(t, err)

	assert.Equal(t, uint32(1), db.Count())

	assert.Nil(t, db.Close())
}

func TestRecoveryCompaction(t *testing.T) {
	opts := &Options{
		FileSystem:                 testFS,
		maxSegmentSize:             1024,
		compactionMinSegmentSize:   512,
		compactionMinFragmentation: 0.2,
	}

	db, err := createTestDB(opts)
	assert.Nil(t, err)

	// Fill file 0.
	for i := 0; i < 41; i++ {
		assert.Nil(t, db.Put([]byte{0}, []byte{0}))
	}
	assert.Nil(t, db.Put([]byte{1}, []byte{1}))

	// Write to file 1.
	assert.Nil(t, db.Put([]byte{0}, []byte{0}))
	assert.Nil(t, db.Put([]byte{0}, []byte{0}))

	assert.Equal(t, &segmentMeta{Full: true, PutRecords: 42, DeletedKeys: 41, DeletedBytes: 492}, db.datalog.segments[0].meta)
	assert.Equal(t, &segmentMeta{PutRecords: 2, DeletedKeys: 1, DeletedBytes: 12}, db.datalog.segments[1].meta)

	cm, err := db.Compact()
	assert.Nil(t, err)
	assert.Equal(t, CompactionResult{CompactedSegments: 1, ReclaimedRecords: 41, ReclaimedBytes: 492}, cm)
	assert.Nil(t, db.datalog.segments[0]) // Items were moved from file 0 to file 1.
	assert.Equal(t, &segmentMeta{PutRecords: 3, DeletedKeys: 1, DeletedBytes: 12}, db.datalog.segments[1].meta)

	// Fill file 1.
	for i := 0; i < 40; i++ {
		assert.Nil(t, db.Put([]byte{1}, []byte{2}))
	}

	// Fill file 0.
	for i := 0; i < 42; i++ {
		assert.Nil(t, db.Put([]byte{1}, []byte{2}))
	}
	// Write to file 2.
	assert.Nil(t, db.Put([]byte{0}, []byte{0}))

	assert.Equal(t, &segmentMeta{Full: true, PutRecords: 42, DeletedKeys: 42, DeletedBytes: 504}, db.datalog.segments[0].meta)
	assert.Equal(t, &segmentMeta{Full: true, PutRecords: 42, DeletedKeys: 42, DeletedBytes: 504}, db.datalog.segments[1].meta)
	assert.Equal(t, &segmentMeta{PutRecords: 2}, db.datalog.segments[2].meta)

	v, err := db.Get([]byte{1})
	assert.Nil(t, err)
	assert.Equal(t, []byte{2}, v)

	assert.Equal(t, uint32(2), db.Count())

	assert.Nil(t, db.Close())

	// Simulate crash.
	assert.Nil(t, touchFile(testFS, filepath.Join(testDBName, lockName)))

	db, err = Open(testDBName, opts)
	assert.Nil(t, err)

	assert.Equal(t, uint32(2), db.Count())

	v, err = db.Get([]byte{1})
	assert.Nil(t, err)
	assert.Equal(t, []byte{2}, v)

	assert.Nil(t, db.Close())
}

func TestRecoveryIterator(t *testing.T) {
	db, err := createTestDB(nil)
	assert.Nil(t, err)

	listRecords := func() []record {
		var records []record
		it := newRecoveryIterator(db.datalog.segmentsBySequenceID())
		for {
			rec, err := it.next()
			if err == ErrIterationDone {
				break
			}
			assert.Nil(t, err)
			records = append(records, rec)
		}
		return records
	}

	assert.Equal(t, 0, len(listRecords()))

	if err := db.Put([]byte{1}, []byte{1}); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t,
		[]record{
			{recordTypePut, 0, 512, []byte{1, 0, 1, 0, 0, 0, 1, 1, 133, 13, 200, 12}, []byte{1}, []byte{1}},
		},
		listRecords(),
	)

	if err := db.Put([]byte{1}, []byte{1}); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t,
		[]record{
			{recordTypePut, 0, 512, []byte{1, 0, 1, 0, 0, 0, 1, 1, 133, 13, 200, 12}, []byte{1}, []byte{1}},
			{recordTypePut, 0, 524, []byte{1, 0, 1, 0, 0, 0, 1, 1, 133, 13, 200, 12}, []byte{1}, []byte{1}},
		},
		listRecords(),
	)

	if err := db.Put([]byte{2}, []byte{2}); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t,
		[]record{
			{recordTypePut, 0, 512, []byte{1, 0, 1, 0, 0, 0, 1, 1, 133, 13, 200, 12}, []byte{1}, []byte{1}},
			{recordTypePut, 0, 524, []byte{1, 0, 1, 0, 0, 0, 1, 1, 133, 13, 200, 12}, []byte{1}, []byte{1}},
			{recordTypePut, 0, 536, []byte{1, 0, 1, 0, 0, 0, 2, 2, 252, 15, 236, 190}, []byte{2}, []byte{2}},
		},
		listRecords(),
	)

	assert.Nil(t, db.Close())
}
