package fs

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/akrylysov/pogreb/internal/assert"
)

func TestOSLockFile(t *testing.T) {
	testLockFile(t, OS)
}

func TestOSLockAcquireExisting(t *testing.T) {
	testLockFileAcquireExisting(t, OS)
}

func TestOsfile(t *testing.T) {
	const (
		name = "test"
		perm = os.FileMode(0666)
	)

	testData := []byte{1, 2, 3}

	assert.Nil(t, ioutil.WriteFile(name, testData, perm))

	f, err := OS.OpenFile(name, os.O_CREATE, perm)
	assert.Nil(t, err)

	b, err := f.Slice(0, 3)
	assert.Nil(t, err)
	assert.Equal(t, testData[0:3], b)

	// Unmap file.
	err = f.Mmap(0)
	assert.Nil(t, err)

	b, err = f.Slice(1, 2)
	assert.Equal(t, os.ErrClosed, err)
	assert.Nil(t, b)

	// Mmap file again.
	err = f.Mmap(1)
	assert.Nil(t, err)

	err = f.Mmap(3)
	assert.Nil(t, err)

	b, err = f.Slice(1, 2)
	assert.Nil(t, err)
	assert.Equal(t, testData[1:2], b)

	assert.Nil(t, f.Close())

	b, err = f.Slice(1, 2)
	assert.Equal(t, os.ErrClosed, err)
	assert.Nil(t, b)
}
