package pogreb

import (
	"os"
	"path/filepath"

	"github.com/akrylysov/pogreb/fs"
)

const (
	lockName = "lock"
)

func createLockFile(opts *Options) (fs.LockFile, bool, error) {
	return opts.FileSystem.CreateLockFile(filepath.Join(opts.path, lockName), os.FileMode(0644))
}
