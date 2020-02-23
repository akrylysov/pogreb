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
	maxSegments = math.MaxInt16
)

// datalog is a write-ahead log.
type datalog struct {
	opts     *Options
	curSeg   *segment
	segments [maxSegments]*segment
	modTime  int64
}

func openDatalog(opts *Options) (*datalog, error) {
	files, err := ioutil.ReadDir(opts.path)
	if err != nil {
		return nil, err
	}

	dl := &datalog{
		opts:    opts,
		modTime: time.Now().UnixNano(),
	}

	for _, file := range files {
		name := file.Name()
		ext := filepath.Ext(name)
		if ext != segmentExt {
			continue
		}
		id, err := strconv.ParseInt(strings.TrimSuffix(name, ext), 10, 16)
		if err != nil {
			return nil, err
		}
		_, err = dl.openSegment(filepath.Join(opts.path, name), uint16(id))
		if err != nil {
			return nil, err
		}
	}

	if err := dl.swapSegment(); err != nil {
		return nil, err
	}

	return dl, nil
}

func (dl *datalog) openSegment(path string, id uint16) (*segment, error) {
	f, err := openFile(dl.opts.FileSystem, path, false)
	if err != nil {
		return nil, err
	}
	var modTime int64
	meta := &segmentMeta{}
	if !f.empty() {
		metaPath := filepath.Join(dl.opts.path, segmentMetaName(id))
		if err := readGobFile(dl.opts.FileSystem, metaPath, &meta); err != nil {
			logger.Printf("error reading segment meta %d: %v", id, err)
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

	df := &segment{file: f, id: id, meta: meta, modTime: modTime}
	dl.segments[id] = df
	return df, nil
}

func (dl *datalog) nextWritableSegmentID() (uint16, error) {
	for i, file := range dl.segments {
		if file == nil || !file.meta.Full {
			return uint16(i), nil
		}
	}
	return 0, fmt.Errorf("number of segments exceeds %d", maxSegments)
}

func (dl *datalog) swapSegment() error {
	id, err := dl.nextWritableSegmentID()
	if err != nil {
		return err
	}
	var f *segment
	if dl.segments[id] != nil {
		f = dl.segments[id]
	} else {
		name := segmentName(id)
		f, err = dl.openSegment(filepath.Join(dl.opts.path, name), id)
		if err != nil {
			return err
		}
	}
	dl.curSeg = f
	return nil
}

func (dl *datalog) removeSegment(f *segment) error {
	dl.segments[f.id] = nil

	if err := f.Close(); err != nil {
		return err
	}

	// Remove segment.
	filePath := filepath.Join(dl.opts.path, segmentName(f.id))
	if err := os.Remove(filePath); err != nil {
		return err
	}

	// Remove segment meta.
	metaPath := filepath.Join(dl.opts.path, segmentMetaName(f.id))
	if err := os.Remove(metaPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (dl *datalog) readKeyValue(sl slot) ([]byte, []byte, error) {
	off := int64(sl.offset) + 6 // Skip key size and value size.
	f := dl.segments[sl.segmentID]
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
	f := dl.segments[sl.segmentID]
	return f.Slice(off, off+int64(sl.keySize))
	/*key := make([]byte, sl.keySize)
	_, err := f.ReadAt(key, off)
	if err != nil {
		return nil, err
	}
	return key, nil*/
}

func (dl *datalog) trackOverwrite(sl slot) {
	meta := dl.segments[sl.segmentID].meta
	meta.DeletedKeys++
	meta.DeletedBytes += encodedRecordSize(sl.kvSize())
}

func (dl *datalog) del(key []byte, sl slot) error {
	dl.trackOverwrite(sl)
	delRecord := encodeDeleteRecord(key)
	_, _, err := dl.writeRecord(delRecord, recordTypeDelete)
	if err != nil {
		return err
	}
	dl.curSeg.meta.DeletedBytes += uint32(len(delRecord))
	return nil
}

func (dl *datalog) writeRecord(data []byte, rt recordType) (uint16, uint32, error) {
	if dl.curSeg.meta.Full || dl.curSeg.size+int64(len(data)) > int64(dl.opts.maxSegmentSize) {
		dl.curSeg.meta.Full = true
		if err := dl.swapSegment(); err != nil {
			return 0, 0, err
		}
	}
	off, err := dl.curSeg.append(data)
	if err != nil {
		return 0, 0, err
	}
	switch rt {
	case recordTypePut:
		dl.curSeg.meta.PutRecords++
	case recordTypeDelete:
		dl.curSeg.meta.DeleteRecords++
	}
	return dl.curSeg.id, uint32(off), nil
}

func (dl *datalog) writeKeyValue(key []byte, value []byte) (uint16, uint32, error) {
	return dl.writeRecord(encodePutRecord(key, value), recordTypePut)
}

func (dl *datalog) sync() error {
	return dl.curSeg.Sync()
}

func (dl *datalog) close() error {
	for id, f := range dl.segments {
		if f == nil {
			continue
		}
		if err := f.Close(); err != nil {
			return err
		}
		metaPath := filepath.Join(dl.opts.path, segmentMetaName(uint16(id)))
		if err := writeGobFile(dl.opts.FileSystem, metaPath, f.meta); err != nil {
			return err
		}
	}
	return nil
}

func (dl *datalog) segmentsByModification() ([]*segment, error) {
	// Sort segments in ascending order by last modified time.
	var segments []*segment

	for _, f := range dl.segments {
		if f == nil {
			continue
		}
		segments = append(segments, f)
	}

	sort.SliceStable(segments, func(i, j int) bool {
		return segments[i].modTime < segments[j].modTime
	})

	return segments, nil
}
