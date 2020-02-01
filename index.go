package pogreb

import (
	"path/filepath"
)

const (
	indexExt          = ".pix"
	indexMainName     = "main" + indexExt
	indexOverflowName = "overflow" + indexExt
	indexMetaName     = "index" + metaExt
	loadFactor        = 0.7
)

// index is an on-disk linear hashing hash table.
type index struct {
	opts           *Options
	main           *file
	overflow       *file
	freeBucketOffs []int64
	level          uint8
	numKeys        uint32
	numBuckets     uint32
	splitBucketIdx uint32
}

type indexMeta struct {
	Level               uint8
	NumKeys             uint32
	NumBuckets          uint32
	SplitBucketIndex    uint32
	FreeOverflowBuckets []int64
}

func openIndex(opts *Options) (*index, error) {
	main, err := openFile(opts.FileSystem, filepath.Join(opts.path, indexMainName), false)
	if err != nil {
		return nil, err
	}
	overflow, err := openFile(opts.FileSystem, filepath.Join(opts.path, indexOverflowName), false)
	if err != nil {
		_ = main.Close()
		return nil, err
	}
	idx := &index{
		opts:       opts,
		main:       main,
		overflow:   overflow,
		numBuckets: 1,
	}
	if main.empty() {
		if _, err = idx.main.extend(bucketSize); err != nil {
			_ = main.Close()
			_ = overflow.Close()
			return nil, err
		}
	} else if err := idx.readMeta(); err != nil {
		_ = main.Close()
		_ = overflow.Close()
		return nil, err
	}
	return idx, nil
}

func (idx *index) writeMeta() error {
	m := indexMeta{
		Level:               idx.level,
		NumKeys:             idx.numKeys,
		NumBuckets:          idx.numBuckets,
		SplitBucketIndex:    idx.splitBucketIdx,
		FreeOverflowBuckets: idx.freeBucketOffs,
	}
	return writeGobFile(idx.opts.FileSystem, filepath.Join(idx.opts.path, indexMetaName), m)
}

func (idx *index) readMeta() error {
	m := indexMeta{}
	if err := readGobFile(idx.opts.FileSystem, filepath.Join(idx.opts.path, indexMetaName), &m); err != nil {
		return err
	}
	idx.level = m.Level
	idx.numKeys = m.NumKeys
	idx.numBuckets = m.NumBuckets
	idx.splitBucketIdx = m.SplitBucketIndex
	idx.freeBucketOffs = m.FreeOverflowBuckets
	return nil
}

func bucketOffset(idx uint32) int64 {
	return int64(headerSize) + (int64(bucketSize) * int64(idx))
}

func (idx *index) bucketIndex(hash uint32) uint32 {
	bidx := hash & ((1 << idx.level) - 1)
	if bidx < idx.splitBucketIdx {
		return hash & ((1 << (idx.level + 1)) - 1)
	}
	return bidx
}

func (idx *index) forEachBucket(startBucketIdx uint32, cb func(bucketHandle) (bool, error)) error {
	off := bucketOffset(startBucketIdx)
	f := idx.main.MmapFile
	for {
		b := bucketHandle{file: f, offset: off}
		if err := b.read(); err != nil {
			return err
		}
		if stop, err := cb(b); stop || err != nil {
			return err
		}
		if b.next == 0 {
			return nil
		}
		off = b.next
		f = idx.overflow.MmapFile
	}
}

func (idx *index) get(hash uint32, cb func(slot) (bool, error)) error {
	return idx.forEachBucket(idx.bucketIndex(hash), func(b bucketHandle) (bool, error) {
		for i := 0; i < slotsPerBucket; i++ {
			sl := b.slots[i]
			if sl.offset == 0 {
				return b.next == 0, nil
			}
			if hash == sl.hash {
				if found, err := cb(sl); found || err != nil {
					return found, err
				}
			}
		}
		return false, nil
	})
}

func (idx *index) put(sl slot, cb func(slot) (bool, error)) error {
	var b *bucketHandle
	var originalB *bucketHandle
	slotIdx := 0
	err := idx.forEachBucket(idx.bucketIndex(sl.hash), func(curb bucketHandle) (bool, error) {
		b = &curb
		for i := 0; i < slotsPerBucket; i++ {
			cursl := b.slots[i]
			slotIdx = i
			if cursl.offset == 0 {
				// Found an empty slot.
				return true, nil
			}
			if sl.hash == cursl.hash {
				if found, err := cb(cursl); err != nil || found {
					// Key already exists.
					return found, err
				}
			}
		}
		if b.next == 0 {
			// Couldn't find free space in the current bucket, creating a new overflow bucket.
			nextBucket, err := idx.createOverflowBucket()
			if err != nil {
				return false, err
			}
			b.next = nextBucket.offset
			originalB = b
			b = nextBucket
			slotIdx = 0
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return err
	}

	// Inserting a new item.
	if b.slots[slotIdx].offset == 0 {
		if idx.numKeys == MaxKeys {
			return errFull
		}
		idx.numKeys++
	}

	b.slots[slotIdx] = sl
	if err := b.write(); err != nil {
		return err
	}
	if originalB != nil {
		return originalB.write()
	}

	if float64(idx.numKeys)/float64(idx.numBuckets*slotsPerBucket) > loadFactor {
		if err := idx.split(); err != nil {
			return err
		}
	}
	return nil
}

func (idx *index) delete(hash uint32, cb func(slot) (bool, error)) error {
	b := bucketHandle{}
	slotIdx := -1
	err := idx.forEachBucket(idx.bucketIndex(hash), func(curb bucketHandle) (bool, error) {
		b = curb
		for i := 0; i < slotsPerBucket; i++ {
			sl := b.slots[i]
			if sl.offset == 0 {
				return b.next == 0, nil
			}
			if hash == sl.hash {
				found, err := cb(sl)
				if err != nil {
					return true, err
				}
				if found {
					slotIdx = i
					return true, nil
				}
			}
		}
		return false, nil
	})
	if slotIdx == -1 || err != nil {
		return err
	}
	b.del(slotIdx)
	if err := b.write(); err != nil {
		return err
	}
	idx.numKeys--
	return nil
}

func (idx *index) createOverflowBucket() (*bucketHandle, error) {
	var off int64
	if len(idx.freeBucketOffs) > 0 {
		off = idx.freeBucketOffs[0]
		idx.freeBucketOffs = idx.freeBucketOffs[1:]
	} else {
		var err error
		off, err = idx.overflow.extend(bucketSize)
		if err != nil {
			return nil, err
		}
	}
	return &bucketHandle{file: idx.overflow, offset: off}, nil
}

func (idx *index) freeOverflowBucket(offsets ...int64) {
	idx.freeBucketOffs = append(idx.freeBucketOffs, offsets...)
}

func (idx *index) split() error {
	updatedBucketIdx := idx.splitBucketIdx
	updatedBucketOff := bucketOffset(updatedBucketIdx)
	updatedBucket := slotWriter{
		bucket: &bucketHandle{file: idx.main, offset: updatedBucketOff},
	}

	newBucketOff, err := idx.main.extend(bucketSize)
	if err != nil {
		return err
	}

	newBucket := slotWriter{
		bucket: &bucketHandle{file: idx.main, offset: newBucketOff},
	}

	idx.splitBucketIdx++
	if idx.splitBucketIdx == 1<<idx.level {
		idx.level++
		idx.splitBucketIdx = 0
	}

	var overflowBuckets []int64
	if err := idx.forEachBucket(updatedBucketIdx, func(curb bucketHandle) (bool, error) {
		for j := 0; j < slotsPerBucket; j++ {
			sl := curb.slots[j]
			if sl.offset == 0 {
				break
			}
			if idx.bucketIndex(sl.hash) == updatedBucketIdx {
				if err := updatedBucket.insert(sl, idx); err != nil {
					return true, err
				}
			} else {
				if err := newBucket.insert(sl, idx); err != nil {
					return true, err
				}
			}
		}
		if curb.next != 0 {
			overflowBuckets = append(overflowBuckets, curb.next)
		}
		return false, nil
	}); err != nil {
		return err
	}

	idx.freeOverflowBucket(overflowBuckets...)

	if err := newBucket.write(); err != nil {
		return err
	}
	if err := updatedBucket.write(); err != nil {
		return err
	}

	idx.numBuckets++
	return nil
}

func (idx *index) sync() error {
	if err := idx.main.Sync(); err != nil {
		return err
	}
	if err := idx.overflow.Sync(); err != nil {
		return err
	}
	return nil
}

func (idx *index) close() error {
	if err := idx.writeMeta(); err != nil {
		return err
	}
	if err := idx.main.Close(); err != nil {
		return err
	}
	if err := idx.overflow.Close(); err != nil {
		return err
	}
	return nil
}

func (idx *index) count() uint32 {
	return idx.numKeys
}
