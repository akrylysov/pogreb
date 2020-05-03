package fs

import (
	"os"
	"testing"
)

const lockTestPath = "test.lock"

var lockTestMode = os.FileMode(0666)

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
	if err := lock.Unlock(); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.Stat(lockTestPath); err == nil {
		t.Fatal()
	}
}

func touchFile(fs FileSystem, path string) error {
	f, err := fs.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.FileMode(0666))
	if err != nil {
		return err
	}
	return f.Close()
}

func testLockFileAcquireExisting(t *testing.T, fs FileSystem) {
	if err := touchFile(fs, lockTestPath); err != nil {
		t.Fatal(err)
	}
	lock, acquiredExisting, err := fs.CreateLockFile(lockTestPath, lockTestMode)
	if lock == nil || !acquiredExisting || err != nil {
		t.Fatal(lock, err, acquiredExisting)
	}
	if err := lock.Unlock(); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.Stat(lockTestPath); err == nil {
		t.Fatal()
	}
}
