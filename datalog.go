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
	dataExt      = ".psg"
)

// datalog is a write-ahead log.
type datalog struct {
	opts    *Options
	curFile *datafile
	files   [maxDatafiles]*datafile
	modTime int64
}

func openDatalog(opts *Options) (*datalog, error) {
	names, err := ioutil.ReadDir(opts.path)
	if err != nil {
		return nil, err
	}

	dl := &datalog{
		opts:    opts,
		modTime: time.Now().UnixNano(),
	}

	for _, name := range names {
		ext := filepath.Ext(name.Name())
		if ext != dataExt {
			continue
		}
		id, err := strconv.ParseInt(strings.TrimSuffix(name.Name(), ext), 10, 16)
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
	var modTime int64
	meta := &datafileMeta{}
	if !f.empty() {
		metaPath := filepath.Join(dl.opts.path, datafileMetaName(id))
		if err := readGobFile(dl.opts.FileSystem, metaPath, &meta); err != nil {
			logger.Printf("error reading datafile meta %d: %v", id, err)
			// TODO: rebuild meta?
		}
		stat, err := f.MmapFile.Stat()
		if err != nil {
			return nil, err
		}
		modTime = stat.ModTime().UnixNano()
	} else {
		dl.modTime++
		modTime = dl.modTime
	}

	df := &datafile{file: f, id: id, meta: meta, modTime: modTime}
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
	return fmt.Sprintf("%05d%s", id, dataExt)
}

func datafileMetaName(id uint16) string {
	return fmt.Sprintf("%05d%s%s", id, dataExt, metaExt)
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
	metaPath := filepath.Join(dl.opts.path, datafileMetaName(f.id))
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
	_, err := f.ReadAt(keyValue, off)
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
	_, err := f.ReadAt(key, off)
	if err != nil {
		return nil, err
	}
	return key, nil*/
}

func (dl *datalog) trackOverwrite(sl slot) {
	meta := dl.files[sl.fileID].meta
	meta.DeletedKeys++
	meta.DeletedBytes += encodedRecordSize(sl.kvSize())
}

func (dl *datalog) del(key []byte, sl slot) error {
	dl.trackOverwrite(sl)
	delRecord := encodeDeleteRecord(key)
	_, _, err := dl.writeRecord(delRecord)
	if err != nil {
		return err
	}
	dl.curFile.meta.DeletedBytes += uint32(len(delRecord))
	return nil
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
	dl.curFile.meta.TotalRecords++
	return dl.curFile.id, uint32(off), nil
}

func (dl *datalog) writeKeyValue(key []byte, value []byte) (uint16, uint32, error) {
	return dl.writeRecord(encodePutRecord(key, value))
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
		metaPath := filepath.Join(dl.opts.path, datafileMetaName(uint16(id)))
		if err := writeGobFile(dl.opts.FileSystem, metaPath, f.meta); err != nil {
			return err
		}
	}
	return nil
}

func (dl *datalog) filesByModification() ([]*datafile, error) {
	// Sort data file in ascending order by last modified time.
	var files []*datafile

	for _, f := range dl.files {
		if f == nil {
			continue
		}
		files = append(files, f)
	}

	sort.SliceStable(files, func(i, j int) bool {
		return files[i].modTime < files[j].modTime
	})

	return files, nil
}
