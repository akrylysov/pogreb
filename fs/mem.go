package fs

import (
	"io"
	"os"
	"path/filepath"
	"time"
)

type memFS struct {
	files map[string]*memFile
}

// Mem is a file system backed by memory.
// It should be used for testing only.
var Mem FileSystem = &memFS{files: map[string]*memFile{}}

func (fs *memFS) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	if flag&os.O_APPEND != 0 {
		// memFS doesn't support opening files in append-only mode.
		// The database doesn't currently use O_APPEND.
		return nil, errAppendModeNotSupported
	}
	f := fs.files[name]
	if f == nil {
		// The file doesn't exist.
		if (flag & os.O_CREATE) == 0 {
			return nil, os.ErrNotExist
		}
		f = &memFile{
			name: name,
			perm: perm, // Perm is saved to return it in Mode, but don't do anything else with it yet.
			refs: 1,
		}
		fs.files[name] = f
	} else {
		if (flag & os.O_TRUNC) != 0 {
			f.size = 0
			f.buf = nil
		}
		f.refs += 1
	}
	return &seekableMemFile{memFile: f}, nil
}

func (fs *memFS) CreateLockFile(name string, perm os.FileMode) (LockFile, bool, error) {
	f, exists := fs.files[name]
	if f != nil && f.refs > 0 {
		return nil, false, os.ErrExist
	}
	_, err := fs.OpenFile(name, os.O_CREATE, perm)
	if err != nil {
		return nil, false, err
	}
	return fs.files[name], exists, nil
}

func (fs *memFS) Stat(name string) (os.FileInfo, error) {
	if f, ok := fs.files[name]; ok {
		return f, nil
	}
	return nil, os.ErrNotExist
}

func (fs *memFS) Remove(name string) error {
	if _, ok := fs.files[name]; ok {
		delete(fs.files, name)
		return nil
	}
	return os.ErrNotExist
}

func (fs *memFS) Rename(oldpath, newpath string) error {
	if f, ok := fs.files[oldpath]; ok {
		delete(fs.files, oldpath)
		fs.files[newpath] = f
		f.name = newpath
		return nil
	}
	return os.ErrNotExist
}

func (fs *memFS) ReadDir(dir string) ([]os.DirEntry, error) {
	dir = filepath.Clean(dir)
	var entries []os.DirEntry
	for name, f := range fs.files {
		if filepath.Dir(name) == dir {
			entries = append(entries, f)
		}
	}
	return entries, nil
}

func (fs *memFS) MkdirAll(path string, perm os.FileMode) error {
	// FIXME: the implementation is incomplete.
	// memFS lets create a file even when the parent directory doesn't exist.
	return nil
}

type memFile struct {
	name string
	perm os.FileMode
	buf  []byte
	size int64
	refs int
}

func (f *memFile) Close() error {
	if f.refs == 0 {
		return os.ErrClosed
	}
	f.refs -= 1
	return nil
}

func (f *memFile) Unlock() error {
	if err := f.Close(); err != nil {
		return err
	}
	return Mem.Remove(f.name)
}

func (f *memFile) ReadAt(p []byte, off int64) (int, error) {
	if f.refs == 0 {
		return 0, os.ErrClosed
	}
	if off >= f.size {
		return 0, io.EOF
	}
	n := int64(len(p))
	if n > f.size-off {
		copy(p, f.buf[off:])
		return int(f.size - off), nil
	}
	copy(p, f.buf[off:off+n])
	return int(n), nil
}

func (f *memFile) WriteAt(p []byte, off int64) (int, error) {
	if f.refs == 0 {
		return 0, os.ErrClosed
	}
	n := int64(len(p))
	if off+n > f.size {
		f.truncate(off + n)
	}
	copy(f.buf[off:off+n], p)
	return int(n), nil
}

func (f *memFile) Stat() (os.FileInfo, error) {
	if f.refs == 0 {
		return f, os.ErrClosed
	}
	return f, nil
}

func (f *memFile) Sync() error {
	if f.refs == 0 {
		return os.ErrClosed
	}
	return nil
}

func (f *memFile) truncate(size int64) {
	if size > f.size {
		diff := int(size - f.size)
		f.buf = append(f.buf, make([]byte, diff)...)
	} else {
		f.buf = f.buf[:size]
	}
	f.size = size
}

func (f *memFile) Truncate(size int64) error {
	if f.refs == 0 {
		return os.ErrClosed
	}
	f.truncate(size)
	return nil
}

func (f *memFile) Name() string {
	_, name := filepath.Split(f.name)
	return name
}

func (f *memFile) Size() int64 {
	return f.size
}

func (f *memFile) Mode() os.FileMode {
	return f.perm
}

func (f *memFile) ModTime() time.Time {
	return time.Now()
}

func (f *memFile) IsDir() bool {
	return false
}

func (f *memFile) Sys() interface{} {
	return nil
}

func (f *memFile) Type() os.FileMode {
	return f.perm
}

func (f *memFile) Info() (os.FileInfo, error) {
	return f.Stat()
}

func (f *memFile) Slice(start int64, end int64) ([]byte, error) {
	if f.refs == 0 {
		return nil, os.ErrClosed
	}
	if end > f.size {
		return nil, io.EOF
	}
	return f.buf[start:end], nil
}

type seekableMemFile struct {
	*memFile
	offset int64
}

func (f *seekableMemFile) Read(p []byte) (int, error) {
	n, err := f.ReadAt(p, f.offset)
	if err != nil {
		return n, err
	}
	f.offset += int64(n)
	return n, err
}

func (f *seekableMemFile) Write(p []byte) (int, error) {
	n, err := f.WriteAt(p, f.offset)
	if err != nil {
		return n, err
	}
	f.offset += int64(n)
	return n, err
}

func (f *seekableMemFile) Seek(offset int64, whence int) (int64, error) {
	if f.refs == 0 {
		return 0, os.ErrClosed
	}
	switch whence {
	case io.SeekEnd:
		f.offset = f.size + offset
	case io.SeekStart:
		f.offset = offset
	case io.SeekCurrent:
		f.offset += offset
	}
	return f.offset, nil
}
