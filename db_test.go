package pogreb

import (
	"bufio"
	"encoding/binary"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/akrylysov/pogreb/fs"
)

func assertEqual(t testing.TB, expected interface{}, actual interface{}) {
	t.Helper()
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("expected %v; got %v", expected, actual)
	}
}

func assertNil(t testing.TB, actual interface{}) {
	t.Helper()
	if actual != nil && !reflect.ValueOf(actual).IsNil() {
		t.Fatalf("expected nil; got %v", actual)
	}
}

func completeWithin(t testing.TB, waitDur time.Duration, cond func() bool) {
	t.Helper()
	start := time.Now()
	for time.Since(start) < waitDur {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond * 10)
	}
	t.Fatalf("expected to complete withing %v", waitDur)
}

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
	assertNil(t, err)
	assertNil(t, db.Close())
	db, err = Open("test.db", nil)
	assertNil(t, err)
	assertNil(t, db.Close())
}

func TestSimple(t *testing.T) {
	db, err := createTestDB(&Options{maxSegmentSize: 1024})
	assertNil(t, err)
	var i byte
	var n uint8 = 255
	if db.Count() != 0 {
		t.Fatal()
	}
	for i = 0; i < n; i++ {
		if has, err := db.Has([]byte{i}); has || err != nil {
			t.Fatal(has, err)
		}
	}
	if err := db.Delete([]byte{128}); err != nil {
		t.Fatal(err)
	}
	if db.Count() != 0 {
		t.Fatal()
	}
	for i = 0; i < n; i++ {
		if err := db.Put([]byte{i}, []byte{i}); err != nil {
			t.Fatal(err)
		}
	}
	if db.Count() != 255 {
		t.Fatal()
	}
	if err := db.Delete([]byte{128}); err != nil {
		t.Fatal(err)
	}
	if db.Count() != 254 {
		t.Fatal()
	}
	if has, err := db.Has([]byte{128}); has || err != nil {
		t.Fatal(has, err)
	}
	if err := db.Put([]byte{128}, []byte{128}); err != nil {
		t.Fatal(err)
	}
	if db.Count() != 255 {
		t.Fatal()
	}

	verifyKeysAndClose := func(valueOffset uint8) {
		t.Helper()
		assertEqual(t, uint32(255), db.Count())
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
			assertEqual(t, []byte{i + valueOffset}, v)
		}
		assertNil(t, db.Close())
	}

	verifyKeysAndClose(0)

	// Simulate crash.
	assertNil(t, touchFile(filepath.Join("test.db", lockName)))
	assertNil(t, os.Remove(filepath.Join("test.db", segmentMetaName(0))))
	assertNil(t, os.Remove(filepath.Join("test.db", indexMetaName)))

	// Open and check again
	db, err = Open("test.db", nil)
	assertNil(t, err)
	verifyKeysAndClose(0)

	assertEqual(t, segmentMeta{TotalRecords: 43, DeletedBytes: 11}, *db.datalog.segments[0].meta)
	assertEqual(t, segmentMeta{TotalRecords: 42}, *db.datalog.segments[1].meta)

	// Update all items
	db, err = Open("test.db", nil)
	assertNil(t, err)
	for i = 0; i < n; i++ {
		if err := db.Put([]byte{i}, []byte{i + 6}); err != nil {
			t.Fatal(err)
		}
	}
	verifyKeysAndClose(6)

	// Delete all items
	db, err = Open("test.db", nil)
	assertNil(t, err)
	for i = 0; i < n; i++ {
		if err := db.Delete([]byte{i}); err != nil {
			t.Fatal(err)
		}
	}
	for i = 0; i < n; i++ {
		if has, err := db.Has([]byte{i}); has || err != nil {
			t.Fatal(has, err)
		}
	}
	if db.Count() != 0 {
		t.Fatal()
	}
	assertNil(t, db.Close())
}

func TestEmptyKey(t *testing.T) {
	db, err := createTestDB(nil)
	assertNil(t, err)
	if err := db.Put([]byte{}, []byte{1}); err != nil {
		t.Fatal(err)
	}
	v, err := db.Get([]byte{})
	assertNil(t, err)
	assertEqual(t, []byte{1}, v)
	assertNil(t, db.Close())
}

func TestEmptyValue(t *testing.T) {
	db, err := createTestDB(nil)
	assertNil(t, err)
	// Returns a nil value if key not found.
	if v, err := db.Get([]byte{1}); err != nil || v != nil {
		t.Fatal(err)
	}
	err = db.Put([]byte{1}, []byte{})
	assertNil(t, err)
	// Returns an empty slice if value is empty.
	if v, err := db.Get([]byte{1}); err != nil || v == nil || len(v) != 0 {
		t.Fatal(err)
	}
	assertNil(t, db.Close())
}

func TestEmptyKeyValue(t *testing.T) {
	db, err := createTestDB(nil)
	assertNil(t, err)
	if err := db.Put([]byte{}, []byte{}); err != nil {
		t.Fatal(err)
	}
	v, err := db.Get([]byte{})
	assertNil(t, err)
	assertEqual(t, []byte{}, v)
	assertNil(t, db.Close())
}

func TestDataRecycle(t *testing.T) {
	db, err := createTestDB(nil)
	assertNil(t, err)
	if err := db.Put([]byte{1}, []byte{8}); err != nil {
		t.Fatal(err)
	}
	v, err := db.Get([]byte{1})
	assertNil(t, err)
	assertEqual(t, []byte{8}, v)
	err = db.Delete([]byte{1})
	assertNil(t, err)
	err = db.Put([]byte{1}, []byte{9})
	assertNil(t, err)
	assertEqual(t, []byte{8}, v)
	assertNil(t, db.Close())
}

func TestClose(t *testing.T) {
	db, err := createTestDB(nil)
	assertNil(t, err)
	assertNil(t, db.Close())
	if _, err := db.Get([]byte{1}); err == nil {
		t.Fatal()
	}
	if err := db.Close(); err == nil {
		t.Fatal()
	}
}

func TestCorruptedIndex(t *testing.T) {
	db, err := createTestDB(nil)
	assertNil(t, err)
	assertNil(t, db.Close())

	f, err := os.OpenFile(filepath.Join("test.db", indexMetaName), os.O_WRONLY, 0)
	assertNil(t, err)
	_, err = f.WriteString("corrupted")
	assertNil(t, err)
	assertNil(t, f.Close())

	db, err = Open("test.db", nil)
	assertNil(t, db)
	assertEqual(t, errCorrupted, err)
}

func TestWordsDict(t *testing.T) {
	fwords, err := os.Open("/usr/share/dict/words")
	if err != nil {
		t.Skip("words file is not found")
	}
	defer fwords.Close()
	db, err := createTestDB(&Options{FileSystem: fs.Mem})
	assertNil(t, err)
	scanner := bufio.NewScanner(fwords)
	items := make(map[string]string)
	for scanner.Scan() {
		k := scanner.Text()
		v := strings.ToUpper(k)
		items[k] = v
		if err := db.Put([]byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	for k, v := range items {
		v2, err := db.Get([]byte(k))
		if string(v2) != v {
			t.Fatalf("expected %v; got value=%v, err=%v for key %v", v, string(v2), err, k)
		}
	}
	assertNil(t, db.Close())
}

func BenchmarkPut(b *testing.B) {
	db, err := createTestDB(nil)
	assertNil(b, err)
	b.ResetTimer()
	k := []byte{1}
	for i := 0; i < b.N; i++ {
		if err := db.Put(k, k); err != nil {
			b.Fail()
		}
	}
	assertNil(b, db.Close())
}

func BenchmarkGet(b *testing.B) {
	db, err := createTestDB(nil)
	assertNil(b, err)
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
	assertNil(b, db.Close())
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
