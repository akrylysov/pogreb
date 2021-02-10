package fs

import (
	"io"
	"os"
	"testing"

	"github.com/akrylysov/pogreb/internal/assert"
)

const (
	lockTestPath = "test.lock"
)

var (
	lockTestMode = os.FileMode(0666)
)

func testLockFile(t *testing.T, fs FileSystem) {
	_ = fs.Remove(lockTestPath)
	lock, acquiredExisting, err := fs.CreateLockFile(lockTestPath, lockTestMode)
	if lock == nil || acquiredExisting || err != nil {
		t.Fatal(lock, err, acquiredExisting)
	}
	lock2, acquiredExisting2, err2 := fs.CreateLockFile(lockTestPath, lockTestMode)
	if lock2 != nil || acquiredExisting2 || err2 != os.ErrExist {
		t.Fatal(lock2, acquiredExisting2, err2)
	}

	err = lock.Unlock()
	assert.Nil(t, err)

	_, err = fs.Stat(lockTestPath)
	assert.NotNil(t, err)
}

func touchFile(fs FileSystem, path string) error {
	f, err := fs.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.FileMode(0666))
	if err != nil {
		return err
	}
	return f.Close()
}

func testLockFileAcquireExisting(t *testing.T, fs FileSystem) {
	err := touchFile(fs, lockTestPath)
	assert.Nil(t, err)

	lock, acquiredExisting, err := fs.CreateLockFile(lockTestPath, lockTestMode)
	if lock == nil || !acquiredExisting || err != nil {
		t.Fatal(lock, err, acquiredExisting)
	}

	err = lock.Unlock()
	assert.Nil(t, err)

	_, err = fs.Stat(lockTestPath)
	assert.NotNil(t, err)
}

func testFS(t *testing.T, fsys FileSystem) {
	f, err := fsys.OpenFile("test", os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.FileMode(0666))
	assert.Nil(t, err)

	buf := make([]byte, 10)

	t.Run("Empty file", func(t *testing.T) {
		off, err := f.Seek(0, io.SeekCurrent)
		assert.Nil(t, err)
		assert.Equal(t, int64(0), off)

		n, err := f.Read(buf)
		assert.Equal(t, 0, n)
		assert.Equal(t, io.EOF, err)

		n, err = f.ReadAt(buf, 0)
		assert.Equal(t, 0, n)
		assert.Equal(t, io.EOF, err)

		n, err = f.ReadAt(buf, 10)
		assert.Equal(t, 0, n)
		assert.Equal(t, io.EOF, err)

		b, err := f.Slice(1, 10)
		assert.Equal(t, io.EOF, err)
		assert.Nil(t, b)
	})

	testData := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

	t.Run("Write", func(t *testing.T) {
		n, err := f.Write(testData[:9])
		assert.Nil(t, err)
		assert.Equal(t, 9, n)

		off, err := f.Seek(0, io.SeekCurrent)
		assert.Nil(t, err)
		assert.Equal(t, int64(9), off)
	})

	t.Run("Write beyond EOF", func(t *testing.T) {
		off, err := f.Seek(2, io.SeekStart)
		assert.Nil(t, err)
		assert.Equal(t, int64(2), off)

		n, err := f.Write(testData[2:])
		assert.Nil(t, err)
		assert.Equal(t, 8, n)

		off, err = f.Seek(0, io.SeekCurrent)
		assert.Nil(t, err)
		assert.Equal(t, int64(10), off)
	})

	t.Run("Slice", func(t *testing.T) {
		b, err := f.Slice(1, 9)
		assert.Nil(t, err)
		assert.Equal(t, testData[1:9], b)

		b, err = f.Slice(0, 10)
		assert.Nil(t, err)
		assert.Equal(t, testData, b)

		// Offset larger than mapping.
		b, err = f.Slice(0, 12)
		assert.Equal(t, io.EOF, err)
		assert.Nil(t, b)
	})

	t.Run("WriteAt", func(t *testing.T) {
		n, err := f.WriteAt(testData[1:4], 1)
		assert.Nil(t, err)
		assert.Equal(t, 3, n)

		// WriteAt doesn't move offset.
		off, err := f.Seek(0, io.SeekCurrent)
		assert.Nil(t, err)
		assert.Equal(t, int64(10), off)
	})

	t.Run("Sync", func(t *testing.T) {
		// Not tested yet, just make sure it doesn't return an error.
		assert.Nil(t, f.Sync())
	})

	t.Run("Stat", func(t *testing.T) {
		fi, err := f.Stat()
		assert.Nil(t, err)
		assert.Equal(t, "test", fi.Name())
		assert.Equal(t, int64(len(testData)), fi.Size())
		assert.Equal(t, false, fi.IsDir())
		//  FIXME: not implemented for all file systems.
		// assert.Equal(t, os.FileMode(0666), fi.Mode())
		_ = fi.Mode()
		_ = fi.ModTime()
		_ = fi.Sys()

		// File doesn't exist.
		_, err = fsys.Stat("foobar")
		assert.NotNil(t, err)
	})

	t.Run("ReadAt", func(t *testing.T) {
		n, err := f.ReadAt(buf, 0)
		assert.Nil(t, err)
		assert.Equal(t, len(testData), n)
		assert.Equal(t, testData, buf)
	})

	t.Run("Read EOF", func(t *testing.T) {
		n, err := f.Read(buf)
		assert.Equal(t, io.EOF, err)
		assert.Equal(t, 0, n)
	})

	t.Run("Read", func(t *testing.T) {
		// SeekEnd and Read
		off, err := f.Seek(0, io.SeekEnd)
		assert.Nil(t, err)
		assert.Equal(t, int64(len(testData)), off)

		n, err := f.Read(buf)
		assert.Equal(t, io.EOF, err)
		assert.Equal(t, 0, n)

		// SeekStart and Read
		off, err = f.Seek(0, io.SeekStart)
		assert.Nil(t, err)
		assert.Equal(t, int64(0), off)

		n, err = f.Read(buf)
		assert.Nil(t, err)
		assert.Equal(t, len(testData), n)
		assert.Equal(t, testData, buf)

		off, err = f.Seek(0, io.SeekCurrent)
		assert.Equal(t, int64(n), off)
		assert.Nil(t, err)

		// SeekStart 2 and Read
		testOff := int64(2)
		lbuf := make([]byte, 8)
		off, err = f.Seek(testOff, io.SeekStart)
		assert.Nil(t, err)
		assert.Equal(t, testOff, off)

		n, err = f.Read(lbuf)
		assert.Nil(t, err)
		assert.Equal(t, len(testData)-int(testOff), n)
		assert.Equal(t, testData[testOff:], lbuf)
	})

	t.Run("Read larger than file", func(t *testing.T) {
		off, err := f.Seek(0, io.SeekStart)
		assert.Nil(t, err)
		assert.Equal(t, int64(0), off)

		lbuf := make([]byte, 4096)
		n, err := f.Read(lbuf)
		assert.Nil(t, err)
		assert.Equal(t, len(testData), n)
		assert.Equal(t, testData, lbuf[:n])

		n, err = f.Read(lbuf)
		assert.Equal(t, io.EOF, err)
		assert.Equal(t, 0, n)
	})

	t.Run("Close and Open again", func(t *testing.T) {
		assert.Nil(t, f.Close())

		f, err = fsys.OpenFile("test", os.O_RDWR, os.FileMode(0666))
		assert.Nil(t, err)

		b, err := f.Slice(1, 10)
		assert.Nil(t, err)
		assert.Equal(t, testData[1:], b)
	})

	t.Run("Truncate extend", func(t *testing.T) {
		err := f.Truncate(11)
		assert.Nil(t, err)

		lbuf := make([]byte, 11)
		n, err := f.ReadAt(lbuf, 0)
		assert.Nil(t, err)
		assert.Equal(t, 11, n)
		assert.Equal(t, testData, lbuf[:10])

		b, err := f.Slice(0, 11)
		assert.Nil(t, err)
		assert.Equal(t, testData, b[:10])

		fi, err := f.Stat()
		assert.Nil(t, err)
		assert.Equal(t, int64(11), fi.Size())
	})

	t.Run("Truncate shrink", func(t *testing.T) {
		err := f.Truncate(1)
		assert.Nil(t, err)

		lbuf := make([]byte, 1)
		n, err := f.ReadAt(lbuf, 0)
		assert.Nil(t, err)
		assert.Equal(t, 1, n)
		assert.Equal(t, testData[:1], lbuf)

		b, err := f.Slice(0, 1)
		assert.Nil(t, err)
		assert.Equal(t, testData[:1], b)

		b, err = f.Slice(0, 10)
		assert.Equal(t, io.EOF, err)
		assert.Nil(t, b)

		fi, err := f.Stat()
		assert.Nil(t, err)
		assert.Equal(t, int64(1), fi.Size())
	})

	t.Run("Truncate shrink to zero", func(t *testing.T) {
		err := f.Truncate(0)
		assert.Nil(t, err)

		n, err := f.ReadAt(buf, 0)
		assert.Equal(t, io.EOF, err)
		assert.Equal(t, 0, n)

		b, err := f.Slice(0, 1)
		assert.Equal(t, io.EOF, err)
		assert.Nil(t, b)

		fi, err := f.Stat()
		assert.Nil(t, err)
		assert.Equal(t, int64(0), fi.Size())
	})

	t.Run("Close", func(t *testing.T) {
		assert.Nil(t, f.Close())

		err := f.Close()
		assert.NotNil(t, err)

		_, err = f.Seek(1, io.SeekStart)
		assert.NotNil(t, err)
	})

	t.Run("Rename", func(t *testing.T) {
		err := fsys.Rename("foobar", "baz")
		assert.NotNil(t, err)

		assert.Nil(t, fsys.Rename("test", "test2"))
		fi, err := fsys.Stat("test2")
		assert.Nil(t, err)
		assert.Equal(t, int64(0), fi.Size())
		assert.Equal(t, "test2", fi.Name())
	})

	t.Run("ReadDir", func(t *testing.T) {
		fis, err := fsys.ReadDir(".")
		assert.Nil(t, err)

		var hasTestFile bool
		for _, fi := range fis {
			if fi.Name() == "test2" {
				hasTestFile = true
			}
		}
		assert.Equal(t, true, hasTestFile)
	})

	t.Run("Remove", func(t *testing.T) {
		err := fsys.Remove("test2")
		assert.Nil(t, err)

		_, err = fsys.Stat("test2")
		assert.NotNil(t, err)
	})
}
