package pogreb

import (
	"io"
	"os"

	"github.com/akrylysov/pogreb/fs"
)

type file struct {
	fs.MmapFile
	size int64
}

func openFile(fsyst fs.FileSystem, name string, truncate bool) (*file, error) {
	flag := os.O_CREATE | os.O_RDWR
	if truncate {
		flag |= os.O_TRUNC
	}
	fi, err := fsyst.OpenFile(name, flag, os.FileMode(0640))
	f := &file{}
	if err != nil {
		return f, err
	}
	clean := fi.Close
	defer func() {
		if clean != nil {
			_ = clean()
		}
	}()
	f.MmapFile = fi
	stat, err := fi.Stat()
	if err != nil {
		return f, err
	}
	f.size = stat.Size()
	if f.size == 0 {
		// It's a new file - write header.
		if err := f.writeHeader(); err != nil {
			return nil, err
		}
	} else {
		if err := f.readHeader(); err != nil {
			return nil, err
		}
	}
	if _, err := f.Seek(int64(headerSize), io.SeekStart); err != nil {
		return nil, err
	}
	clean = nil
	return f, err
}

func (f *file) writeHeader() error {
	h := newHeader()
	data, err := h.MarshalBinary()
	if err != nil {
		return err
	}
	if _, err = f.append(data); err != nil {
		return err
	}
	return nil
}

func (f *file) readHeader() error {
	h := &header{}
	buf := make([]byte, headerSize)
	if _, err := io.ReadFull(f, buf); err != nil {
		return err
	}
	return h.UnmarshalBinary(buf)
}

func (f *file) empty() bool {
	return f.size == int64(headerSize)
}

func (f *file) extend(size uint32) (int64, error) {
	off := f.size
	if err := f.Truncate(off + int64(size)); err != nil {
		return 0, err
	}
	f.size += int64(size)
	return off, f.Mmap(f.size)
}

func (f *file) append(data []byte) (int64, error) {
	off := f.size
	if _, err := f.WriteAt(data, off); err != nil {
		return 0, err
	}
	f.size += int64(len(data))
	return off, f.Mmap(f.size)
}

func (f *file) truncate(size uint32) error {
	// Truncating memory-mapped file will fail on Windows. Unmap it first.
	if err := f.Mmap(0); err != nil {
		return err
	}
	if err := f.Truncate(int64(size)); err != nil {
		return err
	}
	f.size = int64(size)
	return f.Mmap(f.size)
}
