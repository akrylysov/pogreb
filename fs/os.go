package fs

import (
	"os"
	"syscall"
)

type osfile struct {
	*os.File
	data []byte
}

type osfs struct{}

// OS is a file system backed by the os package.
var OS = &osfs{}

func (fs *osfs) OpenFile(name string, flag int, perm os.FileMode) (MmapFile, error) {
	f, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	mf := &osfile{f, nil}
	if stat.Size() > 0 {
		if err := mf.Mmap(stat.Size()); err != nil {
			return nil, err
		}
	}
	return mf, err
}

func (fs *osfs) CreateLockFile(name string, perm os.FileMode) (LockFile, bool, error) {
	acquiredExisting := false
	if _, err := os.Stat(name); err == nil {
		acquiredExisting = true
	}
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, perm)
	if err != nil {
		return nil, false, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if err == syscall.EWOULDBLOCK {
			err = os.ErrExist
		}
		return nil, false, err
	}
	return &oslockfile{f, name}, acquiredExisting, nil
}

func (fs *osfs) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

func (fs *osfs) Remove(name string) error {
	return os.Remove(name)
}

type oslockfile struct {
	File
	path string
}

func (f *oslockfile) Unlock() error {
	if err := os.Remove(f.path); err != nil {
		return err
	}
	return f.Close()
}

func (f *osfile) Slice(start int64, end int64) []byte {
	return f.data[start:end]
}

func (f *osfile) Close() error {
	if err := munmap(f.data); err != nil {
		return nil
	}
	f.data = nil
	return f.File.Close()
}

func (f *osfile) Mmap(size int64) error {
	if f.data != nil {
		if err := munmap(f.data); err != nil {
			return err
		}
	}
	// TODO: align to the OS page size?
	data, err := mmap(f.File, size)
	if err != nil {
		return err
	}
	madviceRandom(data)
	f.data = data
	return nil
}
