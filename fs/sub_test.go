package fs

import (
	"testing"
)

func TestSubFS(t *testing.T) {
	testFS(t, Sub(Mem, "test"))
}

func TestSubFSLockFile(t *testing.T) {
	testLockFile(t, Sub(Mem, "test"))
}

func TestSubFSLockAcquireExisting(t *testing.T) {
	testLockFileAcquireExisting(t, Sub(Mem, "test"))
}
