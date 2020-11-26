package pogreb

import (
	"bufio"
	"encoding/binary"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/akrylysov/pogreb/fs"
	"github.com/akrylysov/pogreb/internal/assert"
)

func touchFile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	return f.Close()
}

func appendFile(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0640)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(data)
	return err
}

func TestMain(m *testing.M) {
	flag.Parse()
	if !testing.Verbose() {
		SetLogger(log.New(ioutil.Discard, "", 0))
	}
	os.Exit(m.Run())
}

func align512(n uint32) uint32 {
	return (n + 511) &^ 511
}

func TestBucketSize(t *testing.T) {
	if bucketSize != align512(uint32(binary.Size(bucket{}))) {
		t.Fatal("wrong bucketSize value", bucketSize)
	}
	if bucketSize-uint32(binary.Size(bucket{})) > 32 {
		t.Fatal("bucket is wasting too much space", bucketSize)
	}
}

func TestHeaderSize(t *testing.T) {
	if headerSize != align512(uint32(binary.Size(header{}))) || headerSize != 512 {
		t.Fatal("wrong headerSize value", headerSize)
	}
}

func createTestDB(opts *Options) (*DB, error) {
	path := "test.db"
	files, err := ioutil.ReadDir(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	for _, file := range files {
		_ = os.Remove(filepath.Join(path, file.Name()))
	}
	return Open(path, opts)
}

func TestEmpty(t *testing.T) {
	db, err := createTestDB(nil)
	assert.Nil(t, err)
	assert.Nil(t, db.Close())
	db, err = Open("test.db", nil)
	assert.Nil(t, err)
	assert.Nil(t, db.Close())
}

func TestSimple(t *testing.T) {
	db, err := createTestDB(&Options{maxSegmentSize: 1024, BackgroundSyncInterval: -1})
	assert.Nil(t, err)
	var i byte
	var n uint8 = 255
	assert.Equal(t, uint32(0), db.Count())
	for i = 0; i < n; i++ {
		if has, err := db.Has([]byte{i}); has || err != nil {
			t.Fatal(has, err)
		}
	}
	assert.Nil(t, db.Delete([]byte{128}))
	assert.Equal(t, uint32(0), db.Count())
	for i = 0; i < n; i++ {
		assert.Nil(t, db.Put([]byte{i}, []byte{i}))
	}
	assert.Equal(t, uint32(255), db.Count())
	assert.Equal(t, int64(n), db.Metrics().Puts.Value())
	assert.Nil(t, db.Sync())

	sz, err := db.FileSize()
	assert.Nil(t, err)
	if sz <= 0 {
		t.Fatal()
	}

	assert.Nil(t, db.Delete([]byte{128}))
	assert.Equal(t, uint32(254), db.Count())
	if has, err := db.Has([]byte{128}); has || err != nil {
		t.Fatal(has, err)
	}
	assert.Nil(t, db.Put([]byte{128}, []byte{128}))
	assert.Equal(t, uint32(255), db.Count())

	verifyKeysAndClose := func(valueOffset uint8) {
		t.Helper()
		assert.Equal(t, uint32(255), db.Count())
		for i = 0; i < n; i++ {
			if has, err := db.Has([]byte{i}); !has || err != nil {
				t.Fatal(has, err)
			}
			if has, err := db.Has([]byte{0, i}); has || err != nil {
				t.Fatal(has, err)
			}
			v, err := db.Get([]byte{i})
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, []byte{i + valueOffset}, v)
		}
		assert.Nil(t, db.Close())
	}

	verifyKeysAndClose(0)

	// Simulate crash.
	assert.Nil(t, touchFile(filepath.Join("test.db", lockName)))
	assert.Nil(t, os.Remove(filepath.Join("test.db", segmentMetaName(0, 1))))
	assert.Nil(t, os.Remove(filepath.Join("test.db", indexMetaName)))

	// Open and check again
	db, err = Open("test.db", nil)
	assert.Nil(t, err)
	verifyKeysAndClose(0)

	assert.Equal(t, segmentMeta{PutRecords: 42, DeleteRecords: 1, DeletedBytes: 11}, *db.datalog.segments[0].meta)
	assert.Equal(t, segmentMeta{PutRecords: 42}, *db.datalog.segments[1].meta)

	// Update all items
	db, err = Open("test.db", nil)
	assert.Nil(t, err)
	for i = 0; i < n; i++ {
		assert.Nil(t, db.Put([]byte{i}, []byte{i + 6}))
	}
	verifyKeysAndClose(6)

	// Delete all items
	db, err = Open("test.db", &Options{BackgroundSyncInterval: time.Millisecond})
	assert.Nil(t, err)
	for i = 0; i < n; i++ {
		assert.Nil(t, db.Delete([]byte{i}))
	}
	for i = 0; i < n; i++ {
		if has, err := db.Has([]byte{i}); has || err != nil {
			t.Fatal(has, err)
		}
	}
	assert.Equal(t, uint32(0), db.Count())
	assert.Nil(t, db.Close())
}

func TestLock(t *testing.T) {
	db, err := createTestDB(nil)
	assert.Nil(t, err)

	// Opening already opened database returns an error.
	db2, err2 := Open("test.db", nil)
	assert.Nil(t, db2)
	assert.Equal(t, errLocked, err2)

	assert.Nil(t, db.Close())
}

func TestEmptyKey(t *testing.T) {
	db, err := createTestDB(nil)
	assert.Nil(t, err)
	if err := db.Put([]byte{}, []byte{1}); err != nil {
		t.Fatal(err)
	}
	v, err := db.Get([]byte{})
	assert.Nil(t, err)
	assert.Equal(t, []byte{1}, v)
	assert.Nil(t, db.Close())
}

func TestEmptyValue(t *testing.T) {
	db, err := createTestDB(nil)
	assert.Nil(t, err)
	// Returns a nil value if key not found.
	if v, err := db.Get([]byte{1}); err != nil || v != nil {
		t.Fatal(err)
	}
	err = db.Put([]byte{1}, []byte{})
	assert.Nil(t, err)
	// Returns an empty slice if value is empty.
	if v, err := db.Get([]byte{1}); err != nil || v == nil || len(v) != 0 {
		t.Fatal(err)
	}
	assert.Nil(t, db.Close())
}

func TestEmptyKeyValue(t *testing.T) {
	db, err := createTestDB(nil)
	assert.Nil(t, err)
	assert.Nil(t, db.Put([]byte{}, []byte{}))
	v, err := db.Get([]byte{})
	assert.Nil(t, err)
	assert.Equal(t, []byte{}, v)
	assert.Nil(t, db.Close())
}

func TestDataRecycle(t *testing.T) {
	db, err := createTestDB(nil)
	assert.Nil(t, err)
	assert.Nil(t, db.Put([]byte{1}, []byte{8}))
	v, err := db.Get([]byte{1})
	assert.Nil(t, err)
	assert.Equal(t, []byte{8}, v)
	err = db.Delete([]byte{1})
	assert.Nil(t, err)
	err = db.Put([]byte{1}, []byte{9})
	assert.Nil(t, err)
	assert.Equal(t, []byte{8}, v)
	assert.Nil(t, db.Close())
}

func TestClose(t *testing.T) {
	db, err := createTestDB(nil)
	assert.Nil(t, err)
	assert.Nil(t, db.Close())
	if _, err := db.Get([]byte{1}); err == nil {
		t.Fatal()
	}
	if err := db.Close(); err == nil {
		t.Fatal()
	}
}

func TestCorruptedIndex(t *testing.T) {
	db, err := createTestDB(nil)
	assert.Nil(t, err)
	assert.Nil(t, db.Close())

	f, err := os.OpenFile(filepath.Join("test.db", indexMetaName), os.O_WRONLY, 0)
	assert.Nil(t, err)
	_, err = f.WriteString("corrupted")
	assert.Nil(t, err)
	assert.Nil(t, f.Close())

	db, err = Open("test.db", nil)
	assert.Nil(t, db)
	assert.Equal(t, errCorrupted, err)
}

func TestFileError(t *testing.T) {
	db, err := createTestDB(nil)
	assert.Nil(t, err)
	assert.Nil(t, db.Put(nil, nil))

	errf := &errfile{}

	testDB := func(t *testing.T) {
		v, err := db.Get(nil)
		assert.Nil(t, v)
		assert.Equal(t, errfileError, err)

		assert.Equal(t, errfileError, db.Put(nil, nil))
		assert.Equal(t, errfileError, db.Delete(nil))

		has, err := db.Has(nil)
		assert.Equal(t, false, has)
		assert.Equal(t, errfileError, err)

		it := db.Items()
		k, v, err := it.Next()
		assert.Nil(t, k)
		assert.Nil(t, v)
		assert.Equal(t, errfileError, err)
	}

	t.Run("segment error", func(t *testing.T) {
		oldf := db.datalog.segments[0].MmapFile
		db.datalog.segments[0].MmapFile = errf

		testDB(t)

		assert.Equal(t, errfileError, db.Close())

		db.datalog.segments[0].MmapFile = oldf
	})

	t.Run("index error", func(t *testing.T) {
		oldf := db.index.main.MmapFile
		db.index.main.MmapFile = errf

		testDB(t)
		assert.Equal(t, errfileError, db.index.close())

		db.index.main.MmapFile = oldf
	})

	errfs := &errfs{}
	oldfs := db.opts.FileSystem
	db.opts.FileSystem = errfs
	assert.Equal(t, errfileError, db.Close())
	assert.Equal(t, errfileError, db.index.close())
	db.opts.FileSystem = oldfs

	assert.Nil(t, db.Close())
}

func TestFSError(t *testing.T) {
	db, err := createTestDB(&Options{FileSystem: &errfs{}})
	assert.Nil(t, db)
	assert.Equal(t, errfileError, err)
}

func TestWordsDict(t *testing.T) {
	fwords, err := os.Open("/usr/share/dict/words")
	if err != nil {
		t.Skip("words file is not found")
	}
	defer fwords.Close()
	db, err := createTestDB(&Options{FileSystem: fs.Mem})
	assert.Nil(t, err)
	scanner := bufio.NewScanner(fwords)
	items := make(map[string]string)
	for scanner.Scan() {
		k := scanner.Text()
		v := strings.ToUpper(k)
		items[k] = v
		assert.Nil(t, db.Put([]byte(k), []byte(v)))
	}
	assert.Nil(t, scanner.Err())
	for k, v := range items {
		v2, err := db.Get([]byte(k))
		if string(v2) != v {
			t.Fatalf("expected %v; got value=%v, err=%v for key %v", v, string(v2), err, k)
		}
	}
	assert.Nil(t, db.Close())
}

func BenchmarkPut(b *testing.B) {
	db, err := createTestDB(nil)
	assert.Nil(b, err)
	b.ResetTimer()
	k := []byte{1}
	for i := 0; i < b.N; i++ {
		if err := db.Put(k, k); err != nil {
			b.Fail()
		}
	}
	assert.Nil(b, db.Close())
}

func BenchmarkGet(b *testing.B) {
	db, err := createTestDB(nil)
	assert.Nil(b, err)
	k := []byte{1}
	if err := db.Put(k, k); err != nil {
		b.Fail()
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Get(k); err != nil {
			b.Fatal()
		}
	}
	assert.Nil(b, db.Close())
}

func BenchmarkBucket_UnmarshalBinary(b *testing.B) {
	testBucket := bucket{
		slots: [slotsPerBucket]slot{},
	}
	for i := 0; i < slotsPerBucket; i++ {
		testBucket.slots[i].hash = uint32(i)
		testBucket.slots[i].keySize = uint16(i + 1)
		testBucket.slots[i].valueSize = uint32(i + 17)
	}
	data, _ := testBucket.MarshalBinary()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tmp := bucket{}
		err := tmp.UnmarshalBinary(data)
		if err != nil {
			b.Fatal()
		}
	}
}
