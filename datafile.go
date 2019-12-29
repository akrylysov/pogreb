package pogreb

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"
)

type datafile struct {
	*file
	id   uint16
	meta *datafileMeta
}

type datafileMeta struct {
	Full         bool
	TotalKeys    uint32
	DeletedKeys  uint32
	DeletedBytes uint32
}

type datafileRecord struct {
	fileID uint16
	offset uint32
	key    []byte
	value  []byte
	data   []byte
}

func encodedKeyValueSize(kvSize uint32) uint32 {
	// key size, value size, key, value, crc32
	return 2 + 4 + kvSize + 4
}

func encodeKeyValue(key []byte, value []byte) []byte {
	// key size, values size, key, value, crc32
	size := encodedKeyValueSize(uint32(len(key) + len(value)))
	data := make([]byte, size)
	binary.LittleEndian.PutUint16(data[:2], uint16(len(key)))
	binary.LittleEndian.PutUint32(data[2:], uint32(len(value)))
	copy(data[6:], key)
	copy(data[6+len(key):], value)
	checksum := crc32.ChecksumIEEE(data[:6+len(key)+len(value)])
	binary.LittleEndian.PutUint32(data[size-4:size], checksum)
	return data
}

type datafileIterator struct {
	f      *datafile
	offset uint32
	r      *bufio.Reader
	buf    []byte // kv size and crc32 reusable buffer
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
	kvSizeBuf := it.buf
	if _, err := io.ReadFull(it.r, kvSizeBuf); err != nil {
		if err == io.EOF {
			return datafileRecord{}, ErrIterationDone
		}
		return datafileRecord{}, err
	}
	keySize := uint32(binary.LittleEndian.Uint16(kvSizeBuf[:2]))
	valueSize := binary.LittleEndian.Uint32(kvSizeBuf[2:])
	recordSize := encodedKeyValueSize(keySize + valueSize)
	data := make([]byte, recordSize)
	copy(data, kvSizeBuf)
	if _, err := io.ReadFull(it.r, data[6:]); err != nil {
		return datafileRecord{}, err
	}
	checksum := binary.LittleEndian.Uint32(data[len(data)-4:])
	if checksum != crc32.ChecksumIEEE(data[:len(data)-4]) {
		return datafileRecord{}, errCorrupted
	}
	offset := it.offset
	it.offset += recordSize
	record := datafileRecord{
		fileID: it.f.id,
		offset: offset,
		key:    data[6 : 6+keySize],
		value:  data[6+keySize : 6+keySize+valueSize],
		data:   data,
	}
	return record, nil
}
