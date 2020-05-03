package pogreb

import (
	"testing"

	"github.com/akrylysov/pogreb/internal/assert"
)

func TestDatalog(t *testing.T) {
	db, err := createTestDB(nil)
	assert.Nil(t, err)

	_, _, err = db.datalog.writeKeyValue([]byte{'1'}, []byte{'1'})
	assert.Nil(t, err)
	assert.Equal(t, &segmentMeta{PutRecords: 1}, db.datalog.segments[0].meta)
	assert.Nil(t, db.datalog.segments[1])

	sm, err := db.datalog.segmentsByModification()
	assert.Nil(t, err)
	assert.Equal(t, []*segment{db.datalog.segments[0]}, sm)

	// Writing to a full file swaps it.
	db.datalog.segments[0].meta.Full = true
	_, _, err = db.datalog.writeKeyValue([]byte{'1'}, []byte{'1'})
	assert.Nil(t, err)
	assert.Equal(t, &segmentMeta{PutRecords: 1, Full: true}, db.datalog.segments[0].meta)
	assert.Equal(t, &segmentMeta{PutRecords: 1}, db.datalog.segments[1].meta)

	sm, err = db.datalog.segmentsByModification()
	assert.Nil(t, err)
	assert.Equal(t, []*segment{db.datalog.segments[0], db.datalog.segments[1]}, sm)

	_, _, err = db.datalog.writeKeyValue([]byte{'1'}, []byte{'1'})
	assert.Nil(t, err)
	assert.Equal(t, &segmentMeta{PutRecords: 1, Full: true}, db.datalog.segments[0].meta)
	assert.Equal(t, &segmentMeta{PutRecords: 2}, db.datalog.segments[1].meta)

	assert.Nil(t, db.Close())
}
