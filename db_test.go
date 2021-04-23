package pogreb

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
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

const (
	testDBName = "test.db"
)

var (
	// File system used for all tests.
	testFS fs.FileSystem
)

func TestMain(m *testing.M) {
	flag.Parse()
	if !testing.Verbose() {
		SetLogger(log.New(ioutil.Discard, "", 0))
	}
	// Run tests against all file systems.
	for _, fsys := range []fs.FileSystem{fs.Mem, fs.OSMMap, fs.OS} {
		testFS = fsys
		if testing.Verbose() {
			fmt.Printf("=== SET\tFS=%T\n", fsys)
		}
		if exitCode := m.Run(); exitCode != 0 {
			fmt.Printf("DEBUG\tFS=%T\n", fsys)
			os.Exit(exitCode)
		}
	}
	os.Exit(0)
}

func touchFile(fsys fs.FileSystem, path string) error {
	f, err := fsys.OpenFile(path, os.O_CREATE|os.O_TRUNC, os.FileMode(0640))
	if err != nil {
		return err
	}
	return f.Close()
}

func appendFile(path string, data []byte) error {
	f, err := testFS.OpenFile(path, os.O_RDWR, os.FileMode(0640))
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err = f.Seek(0, os.SEEK_END); err != nil {
		return err
	}
	_, err = f.Write(data)
	return err
}

func align512(n uint32) uint32 {
	return (n + 511) &^ 511
}

func TestBucketSize(t *testing.T) {
	serializedSize := uint32(binary.Size(bucket{}))
	if bucketSize != align512(serializedSize) {
		t.Fatal("wrong bucketSize value", bucketSize)
	}
	if bucketSize-serializedSize > 32 {
		t.Fatal("bucket is wasting too much space", bucketSize, serializedSize)
	}
}

func TestHeaderSize(t *testing.T) {
	if headerSize != align512(uint32(binary.Size(header{}))) || headerSize != 512 {
		t.Fatal("wrong headerSize value", headerSize)
	}
}

func createTestDB(opts *Options) (*DB, error) {
	if opts == nil {
		opts = &Options{FileSystem: testFS}
	} else {
		if opts.FileSystem == nil {
			opts.FileSystem = testFS
		}
	}
	path := testDBName
	files, err := testFS.ReadDir(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	for _, file := range files {
		_ = testFS.Remove(filepath.Join(path, file.Name()))
	}
	return Open(path, opts)
}

func TestEmpty(t *testing.T) {
	opts := &Options{FileSystem: testFS}
	db, err := createTestDB(opts)
	assert.Nil(t, err)
	assert.Nil(t, db.Close())
	db, err = Open(testDBName, opts)
	assert.Nil(t, err)
	assert.Nil(t, db.Close())
}

func TestFull(t *testing.T) {
	opts := &Options{
		BackgroundSyncInterval: -1,
		FileSystem:             testFS,
		maxSegmentSize:         1024,
	}
	db, err := createTestDB(opts)
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
		t.Fatal(sz)
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

	expectedSegMetas := db.datalog.segmentMetas()
	verifyKeysAndClose(0)

	// Open and check again
	db, err = Open(testDBName, opts)
	assert.Nil(t, err)
	verifyKeysAndClose(0)

	// Simulate crash.
	assert.Nil(t, touchFile(testFS, filepath.Join(testDBName, lockName)))
	assert.Nil(t, testFS.Remove(filepath.Join(testDBName, segmentMetaName(0, 1))))
	assert.Nil(t, testFS.Remove(filepath.Join(testDBName, indexMetaName)))

	// Open and check again
	db, err = Open(testDBName, opts)
	assert.Nil(t, err)
	verifyKeysAndClose(0)

	assert.Equal(t, expectedSegMetas, db.datalog.segmentMetas())

	// Update all items
	db, err = Open(testDBName, opts)
	assert.Nil(t, err)
	for i = 0; i < n; i++ {
		assert.Nil(t, db.Put([]byte{i}, []byte{i + 6}))
	}
	verifyKeysAndClose(6)

	// Delete all items
	db, err = Open(testDBName, &Options{BackgroundSyncInterval: time.Millisecond})
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
	opts := &Options{FileSystem: testFS}
	db, err := createTestDB(opts)
	assert.Nil(t, err)

	// Opening already opened database returns an error.
	db2, err2 := Open(testDBName, opts)
	assert.Nil(t, db2)
	assert.NotNil(t, err2)

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
	_, err = db.Get([]byte{1})
	assert.NotNil(t, err)
	assert.NotNil(t, db.Close())
}

func TestCorruptedIndex(t *testing.T) {
	opts := &Options{FileSystem: testFS}
	db, err := createTestDB(opts)
	assert.Nil(t, err)
	assert.Nil(t, db.Close())

	f, err := testFS.OpenFile(filepath.Join(testDBName, indexMetaName), os.O_RDWR, 0)
	assert.Nil(t, err)
	_, err = f.Write([]byte("corrupted"))
	assert.Nil(t, err)
	assert.Nil(t, f.Close())

	db, err = Open(testDBName, opts)
	assert.Nil(t, db)
	assert.NotNil(t, err)
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
		oldf := db.datalog.segments[0].File
		db.datalog.segments[0].File = errf

		testDB(t)

		assert.Equal(t, errfileError, db.Close())

		db.datalog.segments[0].File = oldf
	})

	t.Run("index error", func(t *testing.T) {
		oldf := db.index.main.File
		db.index.main.File = errf

		testDB(t)
		assert.Equal(t, errfileError, db.index.close())

		db.index.main.File = oldf
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
	assert.NotNil(t, err)
}

func TestWordsDict(t *testing.T) {
	if testFS != fs.Mem {
		t.Skip()
	}
	fwords, err := os.Open("/usr/share/dict/words")
	if err != nil {
		t.Skip("words file not found")
	}
	defer fwords.Close()
	db, err := createTestDB(nil)
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
