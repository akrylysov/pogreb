package fs

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/akrylysov/pogreb/internal/assert"
)

func TestMemfile(t *testing.T) {
	f, err := Mem.OpenFile("test", os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.FileMode(0666))
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 10)

	t.Run("Read empty file", func(t *testing.T) {
		n, err := f.Read(buf)
		if n != 0 || err != io.EOF {
			t.Fatal()
		}

		n, err = f.ReadAt(buf, 0)
		if n != 0 || err != io.EOF {
			t.Fatal()
		}

		n, err = f.ReadAt(buf, 10)
		if n != 0 || err != io.EOF {
			t.Fatal()
		}
	})

	testData := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

	t.Run("Write", func(t *testing.T) {
		off, err := f.Seek(0, io.SeekCurrent)
		if off != 0 || err != nil {
			t.Fatal(off, err)
		}

		n, err := f.Write(testData)
		if n != len(testData) || err != nil {
			t.Fatal(n, err)
		}

		b, err := f.Slice(1, 10)
		assert.Nil(t, err)
		assert.Equal(t, testData[1:], b)

		writeatN, err := f.WriteAt(testData[1:], 1)
		if writeatN != len(testData)-1 || err != nil {
			t.Fatal(writeatN, err)
		}

		b, err = f.Slice(0, 10)
		assert.Nil(t, err)
		assert.Equal(t, testData, b)

		off, err = f.Seek(0, io.SeekCurrent)
		if off != int64(n) || err != nil {
			t.Fatal(off, err)
		}

		assert.Nil(t, f.Sync())

		assert.Panic(t, "trying to write past EOF - undefined behavior", func() {
			_, _ = f.WriteAt(testData, 100)
		})
	})

	t.Run("Stat", func(t *testing.T) {
		fi, err := f.Stat()
		assert.Nil(t, err)
		assert.Equal(t, "test", fi.Name())
		assert.Equal(t, int64(len(testData)), fi.Size())
		assert.Equal(t, os.FileMode(0), fi.Mode()) // Not implemented
		fi.ModTime()
		assert.Equal(t, false, fi.IsDir())
		assert.Nil(t, fi.Sys())
	})

	t.Run("ReadAt", func(t *testing.T) {
		n, err := f.ReadAt(buf, 0)
		if n != len(testData) || err != nil || !bytes.Equal(testData, buf) {
			t.Fatal(err, n, buf)
		}
	})

	t.Run("Read end of file", func(t *testing.T) {
		n, err := f.Read(buf)
		if n != 0 || err != io.EOF {
			t.Fatal()
		}
	})

	t.Run("Seek(0, SeekEnd) and Read", func(t *testing.T) {
		off, err := f.Seek(0, io.SeekEnd)
		if off != int64(len(testData)) || err != nil {
			t.Fatal()
		}

		n, err := f.Read(buf)
		if n != 0 || err != io.EOF {
			t.Fatal()
		}
	})

	t.Run("Seek(0, SeekStart) and Read", func(t *testing.T) {
		off, err := f.Seek(0, io.SeekStart)
		if off != 0 || err != nil {
			t.Fatal()
		}

		n, err := f.Read(buf)
		if n != len(testData) || err != nil || !bytes.Equal(testData, buf) {
			t.Fatal()
		}

		off, err = f.Seek(0, io.SeekCurrent)
		if off != int64(n) || err != nil {
			t.Fatal()
		}
	})

	t.Run("Seek(0, SeekStart) and Read larger than file", func(t *testing.T) {
		off, err := f.Seek(0, io.SeekStart)
		if off != 0 || err != nil {
			t.Fatal()
		}

		bl := make([]byte, 4096)
		n, err := f.Read(bl)
		if n != len(testData) || err != io.EOF || !bytes.Equal(testData, bl[:n]) {
			t.Fatal()
		}
	})

	t.Run("Seek(2, SeekStart) and Read", func(t *testing.T) {
		testOff := int64(2)
		lbuf := make([]byte, 8)
		off, err := f.Seek(testOff, io.SeekStart)
		if off != testOff || err != nil {
			t.Fatal()
		}

		n, err := f.Read(lbuf)
		if n != len(testData)-int(testOff) || err != nil || !bytes.Equal(testData[testOff:], lbuf) {
			t.Fatal(err, n, lbuf)
		}
	})

	t.Run("Truncate(1), ReadAt and Size", func(t *testing.T) {
		lbuf := make([]byte, 1)
		err := f.Truncate(1)
		if err != nil {
			t.Fatal()
		}

		n, err := f.ReadAt(lbuf, 0)
		if n != 1 || err != nil || lbuf[0] != 0 {
			t.Fatal(err, n, lbuf)
		}

		fi, err := f.Stat()
		if fi.Size() != 1 || err != nil {
			t.Fatal()
		}
	})

	t.Run("Truncate(2), ReadAt and Size", func(t *testing.T) {
		lbuf := make([]byte, 2)
		err := f.Truncate(2)
		if err != nil {
			t.Fatal()
		}

		n, err := f.ReadAt(lbuf, 0)
		if n != 2 || err != nil || lbuf[0] != 0 {
			t.Fatal(err, n, lbuf)
		}

		fi, err := f.Stat()
		if fi.Size() != 2 || err != nil {
			t.Fatal()
		}
	})

	t.Run("Truncate(0), ReadAt and Size", func(t *testing.T) {
		err := f.Truncate(0)
		if err != nil {
			t.Fatal()
		}

		n, err := f.ReadAt(buf, 0)
		if n != 0 || err != io.EOF {
			t.Fatal()
		}

		fi, err := f.Stat()
		if fi.Size() != 0 || err != nil {
			t.Fatal()
		}
	})

	t.Run("Close", func(t *testing.T) {
		assert.Nil(t, f.Close())

		assert.Equal(t, os.ErrClosed, f.Close())
		assert.Equal(t, os.ErrClosed, f.Sync())
		assert.Equal(t, os.ErrClosed, f.Truncate(0))

		_, err := f.Stat()
		assert.Equal(t, os.ErrClosed, err)

		_, err = f.Read(nil)
		assert.Equal(t, os.ErrClosed, err)

		_, err = f.ReadAt(nil, 0)
		assert.Equal(t, os.ErrClosed, err)

		_, err = f.Write(nil)
		assert.Equal(t, os.ErrClosed, err)

		_, err = f.WriteAt(nil, 0)
		assert.Equal(t, os.ErrClosed, err)

		_, err = f.Seek(0, 0)
		assert.Equal(t, os.ErrClosed, err)

		_, err = f.Slice(0, 0)
		assert.Equal(t, os.ErrClosed, err)

		assert.Nil(t, f.Mmap(0)) // Mmap does nothing
	})
}

func TestMemLockFile(t *testing.T) {
	testLockFile(t, Mem)
}

func TestMemLockAcquireExisting(t *testing.T) {
	testLockFileAcquireExisting(t, Mem)
}
