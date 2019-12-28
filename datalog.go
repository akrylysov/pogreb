package pogreb

import (
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	maxDatafiles = math.MaxInt16
	dataPrefix   = "data_"
)

type datalog struct {
	opts    *Options
	curFile *datafile
	files   [maxDatafiles]*datafile
}

func openDatalog(opts *Options) (*datalog, error) {
	names, err := ioutil.ReadDir(opts.path)
	if err != nil {
		return nil, err
	}

	dl := &datalog{
		opts: opts,
	}

	for _, name := range names {
		ext := filepath.Ext(name.Name())
		if !strings.HasPrefix(name.Name(), dataPrefix) || ext != "" {
			continue
		}
		parts := strings.SplitN(name.Name(), "_", 2)
		if len(parts) != 2 {
			continue
		}
		id, err := strconv.ParseInt(parts[1], 10, 16)
		if err != nil {
			return nil, err
		}
		_, err = dl.openDatafile(filepath.Join(opts.path, name.Name()), uint16(id))
		if err != nil {
			return nil, err
		}
	}

	if err := dl.swapDatafile(); err != nil {
		return nil, err
	}

	return dl, nil
}

func (dl *datalog) openDatafile(path string, id uint16) (*datafile, error) {
	f, err := openFile(dl.opts.FileSystem, path, false)
	if err != nil {
		return nil, err
	}
	meta := &datafileMeta{}
	if !f.empty() {
		if err := readGobFile(dl.opts.FileSystem, path+metaExt, &meta); err != nil {
			logger.Printf("error reading datafile meta %d: %v", id, err)
			// TODO: rebuild meta?
		}
	}
	df := &datafile{file: f, id: id, meta: meta}
	dl.files[id] = df
	return df, nil
}

func (dl *datalog) nextWritableFileID() (uint16, error) {
	for i, file := range dl.files {
		if file == nil || !file.meta.Full {
			return uint16(i), nil
		}
	}
	return 0, fmt.Errorf("number of data files exceeds %d", maxDatafiles)
}

func datafileName(id uint16) string {
	return fmt.Sprintf("%s%05d", dataPrefix, id)
}

func (dl *datalog) swapDatafile() error {
	id, err := dl.nextWritableFileID()
	if err != nil {
		return err
	}
	var f *datafile
	if dl.files[id] != nil {
		f = dl.files[id]
	} else {
		name := datafileName(id)
		f, err = dl.openDatafile(filepath.Join(dl.opts.path, name), id)
		if err != nil {
			return err
		}
	}
	dl.curFile = f
	return nil
}

func (dl *datalog) removeFile(f *datafile) error {
	dl.files[f.id] = nil

	if err := f.Close(); err != nil {
		return err
	}

	// Remove file.
	filePath := filepath.Join(dl.opts.path, datafileName(f.id))
	if err := os.Remove(filePath); err != nil {
		return err
	}

	// Remove file meta.
	metaPath := filePath + metaExt
	if err := os.Remove(metaPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (dl *datalog) readKeyValue(sl slot) ([]byte, []byte, error) {
	off := int64(sl.offset) + 6 // Skip key size and value size.
	f := dl.files[sl.fileID]
	keyValue, err := f.Slice(off, off+int64(sl.kvSize()))
	if err != nil {
		return nil, nil, err
	}
	/*keyValue := make([]byte, sl.kvSize())
	_, err := f.ReadAt(keyValue, sl.kvOffset)
	if err != nil {
		return nil, nil, err
	}*/
	return keyValue[:sl.keySize], keyValue[sl.keySize:], nil
}

func (dl *datalog) readKey(sl slot) ([]byte, error) {
	off := int64(sl.offset) + 6
	f := dl.files[sl.fileID]
	return f.Slice(off, off+int64(sl.keySize))
	/*key := make([]byte, sl.keySize)
	_, err := f.ReadAt(key, sl.kvOffset)
	if err != nil {
		return nil, err
	}
	return key, nil*/
}

func (dl *datalog) del(sl slot) {
	meta := dl.files[sl.fileID].meta
	meta.DeletedKeys++
	meta.DeletedBytes += encodedKeyValueSize(sl.kvSize())
}

func (dl *datalog) writeRecord(data []byte) (uint16, uint32, error) {
	if dl.curFile.meta.Full || uint32(dl.curFile.size)+uint32(len(data)) > dl.opts.maxDatafileSize {
		dl.curFile.meta.Full = true
		if err := dl.swapDatafile(); err != nil {
			return 0, 0, err
		}
	}

	off, err := dl.curFile.append(data)
	if err != nil {
		return 0, 0, err
	}
	dl.curFile.meta.TotalKeys++
	return dl.curFile.id, uint32(off), nil
}

func (dl *datalog) writeKeyValue(key []byte, value []byte) (uint16, uint32, error) {
	return dl.writeRecord(encodeKeyValue(key, value))
}

func (dl *datalog) sync() error {
	return dl.curFile.Sync()
}

func (dl *datalog) close() error {
	for id, f := range dl.files {
		if f == nil {
			continue
		}
		if err := f.Close(); err != nil {
			return err
		}
		metaPath := filepath.Join(dl.opts.path, datafileName(uint16(id))+metaExt)
		if err := writeGobFile(dl.opts.FileSystem, metaPath, f.meta); err != nil {
			return err
		}
	}
	return nil
}

func (dl *datalog) pickForCompaction() *datafile {
	for _, f := range dl.files {
		if f == nil {
			continue
		}
		if uint32(f.size) < dl.opts.compactionMinDatafileSize {
			continue
		}
		fragmentation := float32(f.meta.DeletedBytes) / float32(f.size)
		if fragmentation < dl.opts.compactionMinFragmentation {
			continue
		}
		return f
	}
	return nil
}

type datalogIterator struct {
	files []*datafile
	dit   *datafileIterator
}

func newDatalogIterator(files [maxDatafiles]*datafile) (*datalogIterator, error) {
	// Sort data file by last modified time.
	var dfs []struct {
		f       *datafile
		modTime time.Time
	}
	for _, f := range files {
		if f == nil {
			continue
		}
		stat, err := f.MmapFile.Stat()
		if err != nil {
			return nil, err
		}
		dfs = append(dfs, struct {
			f       *datafile
			modTime time.Time
		}{f: f, modTime: stat.ModTime()})
	}

	sort.Slice(dfs, func(i, j int) bool {
		return dfs[i].modTime.Nanosecond() < dfs[j].modTime.Nanosecond()
	})

	iterFiles := make([]*datafile, 0, len(dfs))
	for _, df := range dfs {
		iterFiles = append(iterFiles, df.f)
	}

	return &datalogIterator{
		files: iterFiles,
	}, nil
}

func (it *datalogIterator) next() (datafileRecord, error) {
	for {
		if it.dit == nil {
			if len(it.files) == 0 {
				return datafileRecord{}, ErrIterationDone
			}
			var err error
			it.dit, err = newDatafileIterator(it.files[0])
			if err != nil {
				return datafileRecord{}, err
			}
			it.files = it.files[1:]
		}
		rec, err := it.dit.next()
		if err == ErrIterationDone {
			it.dit = nil
			continue
		}
		if err != nil {
			return datafileRecord{}, err
		}
		return rec, nil
	}
}
