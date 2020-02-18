package pogreb

import (
	"testing"
)

func TestDatalog(t *testing.T) {
	db, err := createTestDB(nil)
	assertNil(t, err)

	_, _, err = db.datalog.writeKeyValue([]byte{'1'}, []byte{'1'})
	assertNil(t, err)
	assertEqual(t, &segmentMeta{PutRecords: 1}, db.datalog.segments[0].meta)
	assertNil(t, db.datalog.segments[1])

	sm, err := db.datalog.segmentsByModification()
	assertNil(t, err)
	assertEqual(t, []*segment{db.datalog.segments[0]}, sm)

	// Writing to a full file swaps it.
	db.datalog.segments[0].meta.Full = true
	_, _, err = db.datalog.writeKeyValue([]byte{'1'}, []byte{'1'})
	assertNil(t, err)
	assertEqual(t, &segmentMeta{PutRecords: 1, Full: true}, db.datalog.segments[0].meta)
	assertEqual(t, &segmentMeta{PutRecords: 1}, db.datalog.segments[1].meta)

	sm, err = db.datalog.segmentsByModification()
	assertNil(t, err)
	assertEqual(t, []*segment{db.datalog.segments[0], db.datalog.segments[1]}, sm)

	_, _, err = db.datalog.writeKeyValue([]byte{'1'}, []byte{'1'})
	assertNil(t, err)
	assertEqual(t, &segmentMeta{PutRecords: 1, Full: true}, db.datalog.segments[0].meta)
	assertEqual(t, &segmentMeta{PutRecords: 2}, db.datalog.segments[1].meta)

	assertNil(t, db.Close())
}
