//go:build plan9
// +build plan9

package fs

import (
	"os"
	"syscall"
)

func createLockFile(name string, perm os.FileMode) (LockFile, bool, error) {
	acquiredExisting := false
	if _, err := os.Stat(name); err == nil {
		acquiredExisting = true
	}
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, syscall.DMEXCL|perm)
	if err != nil {
		if acquiredExisting {
			// Assume a recognizable error.
			err = os.ErrExist
		}
		return nil, false, err
	}
	return &osLockFile{f, name}, acquiredExisting, nil
}

func (f *osLockFile) Unlock() (err error) {
	if err = f.Close(); err == nil {
		 err = os.Remove(f.path)
	}
	return
}

// Return a default FileSystem for this platform.
func DefaultFileSystem() FileSystem {
	return OS
}
