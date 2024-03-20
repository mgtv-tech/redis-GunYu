package types

import (
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/mgtv-tech/redis-GunYu/pkg/util"
)

// redis : ziplist.c
type Ziplist struct {
	buf    *util.SliceBuffer
	length int64
	pos    int64
	end    bool
}

func NewZiplist(data []byte) *Ziplist {
	zl := &Ziplist{
		buf: util.NewSliceBuffer(data),
	}
	zl.length = ReadZiplistLength(zl.buf)
	return zl
}

func (zl *Ziplist) Next() []byte {
	if zl.end {
		return nil
	}

	/* <uint16_t zllen> is the number of entries. When there are more than
	* 2^16-2 entries, this value is set to 2^16-1 and we need to traverse the
	* entire list to know how many items it holds.*/
	if zl.length == 65535 {
		firstByte := zl.buf.ReadByte()
		if firstByte != 0xFE {
			return ReadZiplistEntry2(zl.buf, firstByte)
		}
	} else {
		if zl.pos < zl.length {
			firstByte := zl.buf.ReadByte()
			zl.pos++
			return ReadZiplistEntry2(zl.buf, firstByte)
		} else {
			lastByte := zl.buf.ReadByte()
			if lastByte != 0xFF {
				panic(fmt.Errorf("invalid zipList lastByte encoding: %x", lastByte))
			}
		}
	}

	zl.end = true
	return nil
}

func (zl *Ziplist) Length() int64 {
	return zl.length
}

const (
	rdbZiplist6bitlenString  = 0
	rdbZiplist14bitlenString = 1
	rdbZiplist32bitlenString = 2

	rdbZiplistInt16 = 0xc0
	rdbZiplistInt32 = 0xd0
	rdbZiplistInt64 = 0xe0
	rdbZiplistInt24 = 0xf0
	rdbZiplistInt8  = 0xfe
	rdbZiplistInt4  = 0x0f
)

func ReadZiplistEntry2(buf *util.SliceBuffer, firstByte byte) []byte {
	// if prevLen < 254, it is represented by one byte, else 5 bytes
	if firstByte == 0xFE {
		buf.Seek(4, 1)
	}

	// encoding
	firstByte = buf.ReadByte()
	first2bits := (firstByte & 0xc0) >> 6 // first 2 bits of encoding

	switch first2bits {
	case rdbZiplist6bitlenString:
		length := int(firstByte & 0x3f) // 0x3f = 00111111
		return buf.Slice(length)
	case rdbZiplist14bitlenString:
		second := buf.ReadByte()
		length := (int(firstByte&0x3f) << 8) | int(second)
		return buf.Slice(length)
	case rdbZiplist32bitlenString:
		lenBytes := buf.Slice(4)
		length := binary.BigEndian.Uint32(lenBytes)
		return buf.Slice(int(length))
	}

	switch firstByte {
	case rdbZiplistInt8:
		v := int8(buf.ReadByte())
		return []byte(strconv.FormatInt(int64(v), 10))
	case rdbZiplistInt16:
		v := int16(buf.ReadUint16())
		return []byte(strconv.FormatInt(int64(v), 10))
	case rdbZiplistInt24:
		v := buf.ReadUint24()
		return []byte(strconv.FormatInt(int64(v), 10))
	case rdbZiplistInt32:
		v := int32(buf.ReadUint32())
		return []byte(strconv.FormatInt(int64(v), 10))
	case rdbZiplistInt64:
		v := int64(buf.ReadUint64())
		return []byte(strconv.FormatInt((v), 10))
	}
	if (firstByte >> 4) == rdbZiplistInt4 {
		v := int64(firstByte & 0x0f) // 0x0f = 00001111
		v = v - 1                    // 1-13 -> 0-12
		if v < 0 || v > 12 {
			util.PanicIfErr(fmt.Errorf("invalid zipInt04B encoding: %d", v))
		}
		return []byte(strconv.FormatInt(v, 10))
	}

	util.PanicIfErr((fmt.Errorf("rdb: unknown ziplist header byte: %d", firstByte)))
	return nil
}

// <zlbytes><zltail><zllen><entry>...<entry><zlend>
func ReadZiplistLength(buf *util.SliceBuffer) int64 {
	buf.Seek(8, 0) // skip the zlbytes and zltail
	lenBytes := buf.Slice(2)
	return int64(binary.LittleEndian.Uint16(lenBytes))
}
