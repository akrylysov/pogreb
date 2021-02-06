package fs

import (
	"testing"
)

func TestOSFS(t *testing.T) {
	testFS(t, OS)
}

func TestOSLockFile(t *testing.T) {
	testLockFile(t, OS)
}

func TestOSLockAcquireExisting(t *testing.T) {
	testLockFileAcquireExisting(t, OS)
}
