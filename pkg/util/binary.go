package util

type littleEndian struct{}

// LittleEndian is the little-endian implementation of [ByteOrder] and [AppendByteOrder].
var LittleEndian littleEndian

func (littleEndian) Uint24(b []byte) uint32 {
	_ = b[2] // bounds check hint to compiler; see golang.org/issue/14808
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16
}
