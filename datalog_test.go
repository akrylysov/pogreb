package pogreb

import (
	"testing"
)

func TestDatalog(t *testing.T) {
	db, err := createTestDB(nil)
	assertNil(t, err)

	_, _, err = db.datalog.writeKeyValue([]byte{'1'}, []byte{'1'})
	assertNil(t, err)
	assertEqual(t, &datafileMeta{TotalRecords: 1}, db.datalog.files[0].meta)
	assertNil(t, db.datalog.files[1])

	// Writing to a full file swaps it.
	db.datalog.files[0].meta.Full = true
	_, _, err = db.datalog.writeKeyValue([]byte{'1'}, []byte{'1'})
	assertNil(t, err)
	assertEqual(t, &datafileMeta{TotalRecords: 1, Full: true}, db.datalog.files[0].meta)
	assertEqual(t, &datafileMeta{TotalRecords: 1}, db.datalog.files[1].meta)

	_, _, err = db.datalog.writeKeyValue([]byte{'1'}, []byte{'1'})
	assertNil(t, err)
	assertEqual(t, &datafileMeta{TotalRecords: 1, Full: true}, db.datalog.files[0].meta)
	assertEqual(t, &datafileMeta{TotalRecords: 2}, db.datalog.files[1].meta)

	assertNil(t, db.Close())
}
