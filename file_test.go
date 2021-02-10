package pogreb

import (
	"errors"
	"os"
	"time"

	"github.com/akrylysov/pogreb/fs"
)

type errfs struct{}

func (fs *errfs) OpenFile(name string, flag int, perm os.FileMode) (fs.File, error) {
	return &errfile{}, nil
}

func (fs *errfs) CreateLockFile(name string, perm os.FileMode) (fs.LockFile, bool, error) {
	return &errfile{}, false, nil
}

func (fs *errfs) Stat(name string) (os.FileInfo, error) {
	return nil, errfileError
}

func (fs *errfs) Remove(name string) error {
	return errfileError
}

func (fs *errfs) Rename(oldpath, newpath string) error {
	return errfileError
}

func (fs *errfs) ReadDir(name string) ([]os.FileInfo, error) {
	return nil, errfileError
}

type errfile struct{}

var errfileError = errors.New("errfile error")

func (m *errfile) Close() error {
	return errfileError
}

func (m *errfile) Unlock() error {
	return errfileError
}

func (m *errfile) ReadAt(p []byte, off int64) (int, error) {
	return 0, errfileError
}

func (m *errfile) Read(p []byte) (int, error) {
	return 0, errfileError
}

func (m *errfile) WriteAt(p []byte, off int64) (int, error) {
	return 0, errfileError
}

func (m *errfile) Write(p []byte) (int, error) {
	return 0, errfileError
}

func (m *errfile) Seek(offset int64, whence int) (int64, error) {
	return 0, errfileError
}

func (m *errfile) Stat() (os.FileInfo, error) {
	return nil, errfileError
}

func (m *errfile) Sync() error {
	return errfileError
}

func (m *errfile) Truncate(size int64) error {
	return errfileError
}

func (m *errfile) Name() string {
	return "errfile"
}

func (m *errfile) Size() int64 {
	return 0
}

func (m *errfile) Mode() os.FileMode {
	return os.FileMode(0)
}

func (m *errfile) ModTime() time.Time {
	return time.Now()
}

func (m *errfile) IsDir() bool {
	return false
}

func (m *errfile) Sys() interface{} {
	return errfileError
}

func (m *errfile) Slice(start int64, end int64) ([]byte, error) {
	return nil, errfileError
}

func (m *errfile) Mmap(fileSize int64, mappingSize int64) error {
	return errfileError
}

func (m *errfile) Munmap() error {
	return errfileError
}

// Compile time interface assertion.
var _ fs.File = &errfile{}
