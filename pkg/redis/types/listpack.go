package types

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
)

const (
	lpEncoding7BitUintMask = 0x80 // 10000000 LP_ENCODING_7BIT_UINT_MASK
	lpEncoding7BitUint     = 0x00 // 00000000 LP_ENCODING_7BIT_UINT

	lpEncoding6BitStrMask = 0xC0 // 11000000 LP_ENCODING_6BIT_STR_MASK
	lpEncoding6BitStr     = 0x80 // 10000000 LP_ENCODING_6BIT_STR

	lpEncoding13BitIntMask = 0xE0 // 11100000 LP_ENCODING_13BIT_INT_MASK
	lpEncoding13BitInt     = 0xC0 // 11000000 LP_ENCODING_13BIT_INT

	lpEncoding12BitStrMask = 0xF0 // 11110000 LP_ENCODING_12BIT_STR_MASK
	lpEncoding12BitStr     = 0xE0 // 11100000 LP_ENCODING_12BIT_STR

	lpEncoding16BitIntMask = 0xFF // 11111111 LP_ENCODING_16BIT_INT_MASK
	lpEncoding16BitInt     = 0xF1 // 11110001 LP_ENCODING_16BIT_INT

	lpEncoding24BitIntMask = 0xFF // 11111111 LP_ENCODING_24BIT_INT_MASK
	lpEncoding24BitInt     = 0xF2 // 11110010 LP_ENCODING_24BIT_INT

	lpEncoding32BitIntMask = 0xFF // 11111111 LP_ENCODING_32BIT_INT_MASK
	lpEncoding32BitInt     = 0xF3 // 11110011 LP_ENCODING_32BIT_INT

	lpEncoding64BitIntMask = 0xFF // 11111111 LP_ENCODING_64BIT_INT_MASK
	lpEncoding64BitInt     = 0xF4 // 11110100 LP_ENCODING_64BIT_INT

	lpEncoding32BitStrMask = 0xFF // 11111111 LP_ENCODING_32BIT_STR_MASK
	lpEncoding32BitStr     = 0xF0 // 11110000 LP_ENCODING_32BIT_STR
)

type Listpack struct {
	data        []byte //
	p           uint32 //
	numBytes    uint32 // 4 byte, the number of bytes
	numElements uint16 // 2 byte, the number of Elements
}

func NewListpack(data []byte) *Listpack {
	lp := new(Listpack)

	lp.data = data
	lp.numBytes = binary.LittleEndian.Uint32(data[:4])
	lp.numElements = binary.LittleEndian.Uint16(data[4:6])
	lp.p = 4 + 2

	return lp
}

// redis, Listpack.c:lpGet
func (lp *Listpack) Next() []byte {
	inx := lp.p
	data := lp.data

	var val int64
	var uval, negstart, negmax uint64
	fireByte := data[inx]

	if (fireByte & lpEncoding7BitUintMask) == lpEncoding7BitUint { // 7bit uint
		uval = uint64(data[inx] & 0x7f) // 7bit  to int64
		negmax = 0
		negstart = math.MaxUint64 /* 7 bit int is always positive. */
		lp.p += lpEncodeBacklen(1)
	} else if (fireByte & lpEncoding6BitStrMask) == lpEncoding6BitStr { // 6bit str
		length := uint32(data[inx] & 0x3f) // 6bit
		lp.p += lpEncodeBacklen(1 + length)
		return (lp.data[inx+1 : inx+1+length])
	} else if (fireByte & lpEncoding13BitIntMask) == lpEncoding13BitInt { // 13bit int
		uval = (uint64(data[inx]&0x1f) << 8) + uint64(data[inx+1]) // 5bit + 8bit
		negstart = uint64(1) << 12
		negmax = 8191 // uint13_max
		lp.p += lpEncodeBacklen(2)
	} else if (fireByte & lpEncoding12BitStrMask) == lpEncoding12BitStr { // 12bit str
		length := (uint32(data[inx]&0x0f) << 8) + uint32(lp.data[inx+1]) // 4bit + 8bit
		lp.p += lpEncodeBacklen(2 + length)
		return lp.data[inx+2 : inx+2+length]
	} else if (fireByte & lpEncoding32BitStrMask) == lpEncoding32BitStr { // 32bit str
		length := (uint32(data[inx+1]) << 0) + (uint32(data[inx+2]) << 8) + (uint32(data[inx+3]) << 16) + (uint32(data[inx+4]) << 24)
		lp.p += lpEncodeBacklen(1 + 4 + length)
		return lp.data[inx+1+4 : inx+1+4+length] // encode: 1byte, str length: 4byte
	} else if (fireByte & lpEncoding16BitIntMask) == lpEncoding16BitInt { // 16bit int
		uval = (uint64(data[inx+1]) << 0) + (uint64(data[inx+2]) << 8)
		negstart = uint64(1) << 15
		negmax = 65535                 // uint16_max
		lp.p += lpEncodeBacklen(1 + 2) // encode: 1byte, int: 2byte
	} else if (fireByte & lpEncoding24BitIntMask) == lpEncoding24BitInt { // 24bit int
		uval = (uint64(data[inx+1]) << 0) + (uint64(data[inx+2]) << 8) + (uint64(data[inx+3]) << 16)
		negstart = uint64(1) << 23
		negmax = math.MaxUint32 >> 8   // uint24_max
		lp.p += lpEncodeBacklen(1 + 3) // encode: 1byte, int: 3byte
	} else if (fireByte & lpEncoding32BitIntMask) == lpEncoding32BitInt { // 32bit int
		uval = (uint64(data[inx+1]) << 0) + (uint64(data[inx+2]) << 8) + (uint64(data[inx+3]) << 16) + (uint64(data[inx+4]) << 24)
		negstart = uint64(1) << 31
		negmax = math.MaxUint32        // uint32_max
		lp.p += lpEncodeBacklen(1 + 4) // encode: 1byte, int: 4byte
	} else if (fireByte & lpEncoding64BitIntMask) == lpEncoding64BitInt { // 64bit int
		uval = (uint64(data[inx+1]) << 0) + (uint64(data[inx+2]) << 8) + (uint64(data[inx+3]) << 16) + (uint64(data[inx+4]) << 24) +
			(uint64(data[inx+5]) << 32) + (uint64(data[inx+6]) << 40) + (uint64(data[inx+7]) << 48) + (uint64(data[inx+8]) << 56)
		negstart = uint64(1) << 63
		negmax = math.MaxUint64 // uint64_max
		lp.p += lpEncodeBacklen(1 + 8)
	} else {
		uval = 12345678900000000 + uint64(fireByte)
		negstart = math.MaxUint64
		negmax = 0
	}

	/* We reach this code path only for integer encodings.
	 * Convert the unsigned value to the signed one using two's complement
	 * rule. */
	if uval >= negstart {
		/* This three steps conversion should avoid undefined behaviors
		 * in the unsigned -> signed conversion. */

		uval = negmax - uval
		val = int64(uval)
		val = -val - 1
	} else {
		val = int64(uval)
	}

	/* Return the string representation of the integer */
	return []byte(strconv.FormatInt(val, 10))
}

func (lp *Listpack) End() {
	last := lp.data[lp.p]
	if last != 0xFF {
		panic(fmt.Errorf("list pack, last byte is not 0xFF : %v", last))
	}
}

func (lp *Listpack) NextInteger() int64 {
	b := lp.Next()
	ret, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		panic(fmt.Errorf("invalid integer : data(%v), err(%w)", b, err))
	}
	return ret
}

func (lp *Listpack) NumElements() uint16 {
	return lp.numElements
}

/* the function just returns the length(byte) of `backlen`. */
func lpEncodeBacklen(len uint32) uint32 {
	if len <= 127 {
		return len + 1
	} else if len < 16383 {
		return len + 2
	} else if len < 2097151 {
		return len + 3
	} else if len < 268435455 {
		return len + 4
	} else {
		return len + 5
	}
}
