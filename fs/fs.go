/*
Package fs provides a file system interface.
*/
package fs

import (
	"errors"
	"io"
	"os"
)

var (
	errAppendModeNotSupported = errors.New("append mode is not supported")
)

// File is the interface compatible with os.File.
type File interface {
	io.Closer
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Writer
	io.WriterAt

	Stat() (os.FileInfo, error)
	Sync() error
	Truncate(size int64) error

	Slice(start int64, end int64) ([]byte, error)
}

// LockFile represents a lock file.
type LockFile interface {
	Unlock() error
}

// FileSystem represents a file system.
type FileSystem interface {
	OpenFile(name string, flag int, perm os.FileMode) (File, error)
	Stat(name string) (os.FileInfo, error)
	Remove(name string) error
	Rename(oldpath, newpath string) error
	ReadDir(name string) ([]os.FileInfo, error)

	CreateLockFile(name string, perm os.FileMode) (LockFile, bool, error)
}
