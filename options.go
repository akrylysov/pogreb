package pogreb

import (
	"time"

	"github.com/akrylysov/pogreb/fs"
)

// Options holds the optional DB parameters.
type Options struct {
	// BackgroundSyncInterval sets the amount of time between background fsync() calls.
	//
	// Setting the value to 0 disables the automatic background synchronization.
	BackgroundSyncInterval time.Duration

	FileSystem fs.FileSystem
}

func (src *Options) copyWithDefaults() *Options {
	opts := Options{}
	if src != nil {
		opts = *src
	}
	if opts.FileSystem == nil {
		opts.FileSystem = fs.OS
	}
	return &opts
}
