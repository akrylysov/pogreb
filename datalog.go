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
)

const (
	maxSegments = math.MaxInt16
)

// datalog is a write-ahead log.
type datalog struct {
	opts          *Options
	curSeg        *segment
	segments      [maxSegments]*segment
	maxSequenceID uint64
}

func openDatalog(opts *Options) (*datalog, error) {
	files, err := ioutil.ReadDir(opts.path)
	if err != nil {
		return nil, err
	}

	dl := &datalog{
		opts: opts,
	}

	for _, file := range files {
		name := file.Name()
		ext := filepath.Ext(name)
		if ext != segmentExt {
			continue
		}
		seg, err := dl.openSegment(name)
		if err != nil {
			return nil, err
		}
		if seg.sequenceID > dl.maxSequenceID {
			dl.maxSequenceID = seg.sequenceID
		}
	}

	if err := dl.swapSegment(); err != nil {
		return nil, err
	}

	return dl, nil
}

func parseSegmentName(name string) (uint16, uint64, error) {
	parts := strings.SplitN(strings.TrimSuffix(name, segmentExt), "-", 2)
	id, err := strconv.ParseUint(parts[0], 10, 16)
	if err != nil {
		return 0, 0, err
	}
	var seqID uint64
	if len(parts) == 2 {
		seqID, err = strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return 0, 0, err
		}
	}
	return uint16(id), seqID, nil
}

func (dl *datalog) openSegment(name string) (*segment, error) {
	id, seqID, err := parseSegmentName(name)
	if err != nil {
		return nil, err
	}
	f, err := openFile(dl.opts.FileSystem, filepath.Join(dl.opts.path, name), false)
	if err != nil {
		return nil, err
	}
	meta := &segmentMeta{}
	if !f.empty() {
		metaPath := filepath.Join(dl.opts.path, name+metaExt)
		if err := readGobFile(dl.opts.FileSystem, metaPath, &meta); err != nil {
			logger.Printf("error reading segment meta %d: %v", id, err)
			// TODO: rebuild meta?
		}
	} else {
		dl.maxSequenceID++
		seqID = dl.maxSequenceID
	}

	df := &segment{
		file:       f,
		id:         id,
		sequenceID: seqID,
		name:       name,
		meta:       meta,
	}
	dl.segments[id] = df
	return df, nil
}

func (dl *datalog) nextWritableSegmentID() (uint16, uint64, error) {
	for id, seg := range dl.segments {
		// Pick unfilled segment.
		if seg != nil && !seg.meta.Full {
			dl.maxSequenceID++
			return uint16(id), dl.maxSequenceID, nil
		}
	}
	for id, seg := range dl.segments {
		// Pick empty segment.
		if seg == nil {
			dl.maxSequenceID++
			return uint16(id), dl.maxSequenceID, nil
		}
	}
	return 0, 0, fmt.Errorf("number of segments exceeds %d", maxSegments)
}

func (dl *datalog) swapSegment() error {
	id, seqID, err := dl.nextWritableSegmentID()
	if err != nil {
		return err
	}
	var seg *segment
	if dl.segments[id] != nil {
		seg = dl.segments[id]
	} else {
		name := segmentName(id, seqID)
		seg, err = dl.openSegment(name)
		if err != nil {
			return err
		}
	}
	dl.curSeg = seg
	return nil
}

func (dl *datalog) removeSegment(seg *segment) error {
	dl.segments[seg.id] = nil

	if err := seg.Close(); err != nil {
		return err
	}

	// Remove segment meta from FS.
	metaPath := filepath.Join(dl.opts.path, seg.name+segmentExt)
	if err := dl.opts.FileSystem.Remove(metaPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	// Remove segment from FS.
	filePath := filepath.Join(dl.opts.path, seg.name)
	if err := dl.opts.FileSystem.Remove(filePath); err != nil {
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
	for _, seg := range dl.segments {
		if seg == nil {
			continue
		}
		if err := seg.Close(); err != nil {
			return err
		}
		metaPath := filepath.Join(dl.opts.path, seg.name+metaExt)
		if err := writeGobFile(dl.opts.FileSystem, metaPath, seg.meta); err != nil {
			return err
		}
	}
	return nil
}

func (dl *datalog) segmentsBySequenceID() ([]*segment, error) {
	// Sort segments in ascending order by sequence ID.
	var segments []*segment

	for _, f := range dl.segments {
		if f == nil {
			continue
		}
		segments = append(segments, f)
	}

	sort.SliceStable(segments, func(i, j int) bool {
		return segments[i].sequenceID < segments[j].sequenceID
	})

	return segments, nil
}
