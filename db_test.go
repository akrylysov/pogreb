package pogreb

import (
	"bufio"
	"encoding/binary"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/akrylysov/pogreb/fs"
)

func assertDeepEqual(t *testing.T, expected interface{}, actual interface{}) {
	t.Helper()
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("expected %v; got %v", expected, actual)
	}
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

func removeAndOpen(path string, opts *Options) (*DB, error) {
	os.Remove(path)
	os.Remove(path + indexPostfix)
	os.Remove(path + lockPostfix)
	return Open(path, opts)
}

func TestEmpty(t *testing.T) {
	db, err := removeAndOpen("test.db", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open("test.db", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSimple(t *testing.T) {
	db, err := removeAndOpen("test.db", nil)
	if err != nil {
		t.Fatal(err)
	}
	var i byte
	var n uint8 = 255
	if db.Count() != 0 {
		t.Fatal()
	}
	if len(db.data.fl.blocks) != 0 {
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
	if len(db.data.fl.blocks) == 0 {
		t.Fatal()
	}
	if db.Count() != 254 {
		t.Fatal()
	}
	if has, err := db.Has([]byte{128}); has || err != nil {
		t.Fatal(has, err)
	}
	prevFlLen := len(db.data.fl.blocks)
	if err := db.Put([]byte{128}, []byte{128}); err != nil {
		t.Fatal(err)
	}
	if db.Count() != 255 {
		t.Fatal()
	}
	if len(db.data.fl.blocks) != prevFlLen-1 {
		t.Fatal()
	}

	verifyKeysAndClose := func() {
		t.Helper()
		if db.Count() != 255 {
			t.Fatal()
		}
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
			assertDeepEqual(t, []byte{i}, v)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}

	verifyKeysAndClose()

	// Open and check again
	db, err = Open("test.db", nil)
	if err != nil {
		t.Fatal(err)
	}
	verifyKeysAndClose()
}

func TestEmptyKey(t *testing.T) {
	db, err := removeAndOpen("test.db", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte{}, []byte{1}); err != errKeyEmpty {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestEmptyValue(t *testing.T) {
	db, err := removeAndOpen("test.db", nil)
	if err != nil {
		t.Fatal(err)
	}
	// Returns a nil value if key not found.
	if v, err := db.Get([]byte{1}); err != nil || v != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte{1}, []byte{}); err != nil {
		t.Fatal(err)
	}
	// Returns an empty slice if value is empty.
	if v, err := db.Get([]byte{1}); err != nil || v == nil || len(v) != 0 {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestClose(t *testing.T) {
	db, err := removeAndOpen("test.db", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// FIXME: currently it panics with SIGSEGV
	/*if _, err := db.Get([]byte{1}); err == nil {
		t.Fatal()
	}*/
	if err := db.Close(); err == nil {
		t.Fatal()
	}
}

func TestWordsDict(t *testing.T) {
	fwords, err := os.Open("/usr/share/dict/words")
	if err != nil {
		t.Skip("words file is not found")
	}
	defer fwords.Close()
	db, err := removeAndOpen("test.db", &Options{FileSystem: fs.Mem})
	if err != nil {
		t.Fatal(err)
	}
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
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkPut(b *testing.B) {
	db, err := removeAndOpen("test.db", nil)
	if err != nil {
		b.Fatal(err)
	}
	k := []byte{1}
	for i := 0; i < b.N; i++ {
		if err := db.Put(k, k); err != nil {
			b.Fail()
		}
	}
	if err := db.Close(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkGet(b *testing.B) {
	db, err := removeAndOpen("test.db", nil)
	if err != nil {
		b.Fatal(err)
	}
	k := []byte{1}
	if err := db.Put(k, k); err != nil {
		b.Fail()
	}
	for i := 0; i < b.N; i++ {
		if _, err := db.Get(k); err != nil {
			b.Fatal()
		}
	}
	if err := db.Close(); err != nil {
		b.Fatal(err)
	}
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
		tmp.UnmarshalBinary(data)
	}
}
