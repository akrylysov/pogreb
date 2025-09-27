package pogreb

import (
	"testing"

	"github.com/akrylysov/pogreb/internal/assert"
)

var testDBBackupName string

func TestBackup(t *testing.T) {
	opts := &Options{
		maxSegmentSize:             1024,
		compactionMinSegmentSize:   520,
		compactionMinFragmentation: 0.02,
	}

	testDBBackupName = testDBName + "db.backup"

	run := func(name string, f func(t *testing.T, db *DB)) bool {
		return t.Run(name, func(t *testing.T) {
			db, err := createTestDB(opts)
			assert.Nil(t, err)
			f(t, db)
			assert.Nil(t, db.Close())
			_ = cleanDir(testDBBackupName)
		})
	}

	run("empty", func(t *testing.T, db *DB) {
		assert.Nil(t, db.Backup(testDBBackupName))
		db2, err := Open(testDBBackupName, opts)
		assert.Nil(t, err)
		assert.Nil(t, db2.Close())
	})

	run("single segment", func(t *testing.T, db *DB) {
		assert.Nil(t, db.Put([]byte{0}, []byte{0}))
		assert.Equal(t, 1, countSegments(t, db))
		assert.Nil(t, db.Backup(testDBBackupName))
		db2, err := Open(testDBBackupName, opts)
		assert.Nil(t, err)
		v, err := db2.Get([]byte{0})
		assert.Equal(t, []byte{0}, v)
		assert.Nil(t, err)
		assert.Nil(t, db2.Close())
	})

	run("multiple segments", func(t *testing.T, db *DB) {
		for i := byte(0); i < 100; i++ {
			assert.Nil(t, db.Put([]byte{i}, []byte{i}))
		}
		assert.Equal(t, 3, countSegments(t, db))
		assert.Nil(t, db.Backup(testDBBackupName))
		db2, err := Open(testDBBackupName, opts)
		assert.Equal(t, 3, countSegments(t, db2))
		assert.Nil(t, err)
		for i := byte(0); i < 100; i++ {
			v, err := db2.Get([]byte{i})
			assert.Nil(t, err)
			assert.Equal(t, []byte{i}, v)
		}
		assert.Nil(t, db2.Close())
	})
}
