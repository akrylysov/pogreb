package fs

import (
	"io/ioutil"
	"os"
	"testing"
)

const lockTestPath = "test.lock"

var lockTestMode = os.FileMode(0666)

func testLockFile(fs FileSystem, t *testing.T) {
	fs.Remove(lockTestPath)
	lock, needRecovery, err := fs.CreateLockFile(lockTestPath, lockTestMode)
	if lock == nil || needRecovery || err != nil {
		t.Fatal(lock, err, needRecovery)
	}
	lock2, needRecovery2, err2 := fs.CreateLockFile(lockTestPath, lockTestMode)
	if lock2 != nil || needRecovery2 || err2 != os.ErrExist {
		t.Fatal(lock2, needRecovery2, err2)
	}
	if err := lock.Unlock(); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.Stat(lockTestPath); err == nil {
		t.Fatal()
	}
}

func testLockFileNeedsRecovery(fs FileSystem, t *testing.T) {
	ioutil.WriteFile(lockTestPath, []byte{}, lockTestMode)
	lock, needRecovery, err := fs.CreateLockFile(lockTestPath, lockTestMode)
	if lock == nil || !needRecovery || err != nil {
		t.Fatal(lock, err, needRecovery)
	}
	if err := lock.Unlock(); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.Stat(lockTestPath); err == nil {
		t.Fatal()
	}
}
