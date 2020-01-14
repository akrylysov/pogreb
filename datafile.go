package pogreb

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"
)

type recordType int

const (
	recordTypePut recordType = iota
	recordTypeDelete
)

// datafile is a write-ahead log segment.
// It consists of a sequence of binary-encoded variable length records.
type datafile struct {
	*file
	id      uint16
	meta    *datafileMeta
	modTime int64
}

type datafileMeta struct {
	Full         bool
	TotalRecords uint32
	DeletedKeys  uint32
	DeletedBytes uint32
}

// Binary representation of a datafile record:
// +---------------+------------------+------------------+-...-+--...--+----------+
// | Key Size (2B) | Record Type (1b) | Value Size (31b) | Key | Value | CRC (4B) |
// +---------------+------------------+------------------+-...-+--...--+----------+
type datafileRecord struct {
	rtype  recordType
	fileID uint16
	offset uint32
	data   []byte
	key    []byte
	value  []byte
}

func encodedRecordSize(kvSize uint32) uint32 {
	// key size, value size, key, value, crc32
	return 2 + 4 + kvSize + 4
}

func encodeRecord(key []byte, value []byte, rt recordType) []byte {
	size := encodedRecordSize(uint32(len(key) + len(value)))
	data := make([]byte, size)
	binary.LittleEndian.PutUint16(data[:2], uint16(len(key)))

	valLen := uint32(len(value))
	if rt == recordTypeDelete { // Set delete bit.
		valLen |= 1 << 31
	}
	binary.LittleEndian.PutUint32(data[2:], valLen)

	copy(data[6:], key)
	copy(data[6+len(key):], value)
	checksum := crc32.ChecksumIEEE(data[:6+len(key)+len(value)])
	binary.LittleEndian.PutUint32(data[size-4:size], checksum)
	return data
}

func encodePutRecord(key []byte, value []byte) []byte {
	return encodeRecord(key, value, recordTypePut)
}

func encodeDeleteRecord(key []byte) []byte {
	return encodeRecord(key, nil, recordTypeDelete)
}

// datafileIterator iterates over datafile records.
type datafileIterator struct {
	f      *datafile
	offset uint32
	r      *bufio.Reader
	buf    []byte // kv size and crc32 reusable buffer.
}

func newDatafileIterator(f *datafile) (*datafileIterator, error) {
	if _, err := f.Seek(int64(headerSize), io.SeekStart); err != nil {
		return nil, err
	}
	return &datafileIterator{
		f:      f,
		offset: headerSize,
		r:      bufio.NewReader(f),
		buf:    make([]byte, 6),
	}, nil
}

func (it *datafileIterator) next() (datafileRecord, error) {
	// Read key and value size.
	kvSizeBuf := it.buf
	if _, err := io.ReadFull(it.r, kvSizeBuf); err != nil {
		if err == io.EOF {
			return datafileRecord{}, ErrIterationDone
		}
		return datafileRecord{}, err
	}

	// Decode key size.
	keySize := uint32(binary.LittleEndian.Uint16(kvSizeBuf[:2]))

	// Decode value size and record type.
	rt := recordTypePut
	valueSize := binary.LittleEndian.Uint32(kvSizeBuf[2:])
	if valueSize&(1<<31) != 0 {
		rt = recordTypeDelete
		valueSize &^= 1 << 31
	}

	// Read key, value and checksum.
	recordSize := encodedRecordSize(keySize + valueSize)
	data := make([]byte, recordSize)
	copy(data, kvSizeBuf)
	if _, err := io.ReadFull(it.r, data[6:]); err != nil {
		return datafileRecord{}, err
	}

	// Verify checksum.
	checksum := binary.LittleEndian.Uint32(data[len(data)-4:])
	if checksum != crc32.ChecksumIEEE(data[:len(data)-4]) {
		return datafileRecord{}, errCorrupted
	}

	offset := it.offset
	it.offset += recordSize
	rec := datafileRecord{
		rtype:  rt,
		fileID: it.f.id,
		offset: offset,
		data:   data,
		key:    data[6 : 6+keySize],
		value:  data[6+keySize : 6+keySize+valueSize],
	}
	return rec, nil
}
