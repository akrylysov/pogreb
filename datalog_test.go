package pogreb

import (
	"testing"
)

func TestDatalog(t *testing.T) {
	db, err := createTestDB(nil)
	assertNil(t, err)

	_, _, err = db.datalog.writeKeyValue([]byte{'1'}, []byte{'1'})
	assertNil(t, err)
	assertEqual(t, &datafileMeta{TotalKeys: 1}, db.datalog.files[0].meta)
	assertNil(t, db.datalog.files[1])

	// Writing to a full file swaps it.
	db.datalog.files[0].meta.Full = true
	_, _, err = db.datalog.writeKeyValue([]byte{'1'}, []byte{'1'})
	assertNil(t, err)
	assertEqual(t, &datafileMeta{TotalKeys: 1, Full: true}, db.datalog.files[0].meta)
	assertEqual(t, &datafileMeta{TotalKeys: 1}, db.datalog.files[1].meta)

	_, _, err = db.datalog.writeKeyValue([]byte{'1'}, []byte{'1'})
	assertNil(t, err)
	assertEqual(t, &datafileMeta{TotalKeys: 1, Full: true}, db.datalog.files[0].meta)
	assertEqual(t, &datafileMeta{TotalKeys: 2}, db.datalog.files[1].meta)

	assertNil(t, db.Close())
}

func TestDatalogIterator(t *testing.T) {
	db, err := createTestDB(nil)
	assertNil(t, err)

	listRecords := func() []datafileRecord {
		var records []datafileRecord
		it, err := newDatalogIterator(db.datalog.files)
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
		[]datafileRecord{
			{0, 512, []byte{1}, []byte{1}, []byte{1, 0, 1, 0, 0, 0, 1, 1, 40, 19, 197, 47}},
		},
		listRecords(),
	)

	if err := db.Put([]byte{1}, []byte{1}); err != nil {
		t.Fatal(err)
	}
	assertEqual(t,
		[]datafileRecord{
			{0, 512, []byte{1}, []byte{1}, []byte{1, 0, 1, 0, 0, 0, 1, 1, 40, 19, 197, 47}},
			{0, 524, []byte{1}, []byte{1}, []byte{1, 0, 1, 0, 0, 0, 1, 1, 40, 19, 197, 47}},
		},
		listRecords(),
	)

	if err := db.Put([]byte{2}, []byte{2}); err != nil {
		t.Fatal(err)
	}
	assertEqual(t,
		[]datafileRecord{
			{0, 512, []byte{1}, []byte{1}, []byte{1, 0, 1, 0, 0, 0, 1, 1, 40, 19, 197, 47}},
			{0, 524, []byte{1}, []byte{1}, []byte{1, 0, 1, 0, 0, 0, 1, 1, 40, 19, 197, 47}},
			{0, 536, []byte{2}, []byte{2}, []byte{1, 0, 1, 0, 0, 0, 2, 2, 81, 17, 225, 157}},
		},
		listRecords(),
	)

	assertNil(t, db.Close())
}
