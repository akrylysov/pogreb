package pogreb

import (
	"testing"
)

func TestDatalog(t *testing.T) {
	db, err := createTestDB(nil)
	assertNil(t, err)

	_, _, err = db.datalog.writeKeyValue([]byte{'1'}, []byte{'1'})
	assertNil(t, err)
	assertEqual(t, &segmentMeta{TotalRecords: 1}, db.datalog.segments[0].meta)
	assertNil(t, db.datalog.segments[1])

	// Writing to a full file swaps it.
	db.datalog.segments[0].meta.Full = true
	_, _, err = db.datalog.writeKeyValue([]byte{'1'}, []byte{'1'})
	assertNil(t, err)
	assertEqual(t, &segmentMeta{TotalRecords: 1, Full: true}, db.datalog.segments[0].meta)
	assertEqual(t, &segmentMeta{TotalRecords: 1}, db.datalog.segments[1].meta)

	_, _, err = db.datalog.writeKeyValue([]byte{'1'}, []byte{'1'})
	assertNil(t, err)
	assertEqual(t, &segmentMeta{TotalRecords: 1, Full: true}, db.datalog.segments[0].meta)
	assertEqual(t, &segmentMeta{TotalRecords: 2}, db.datalog.segments[1].meta)

	assertNil(t, db.Close())
}
