package util

import (
	"encoding/binary"
	"errors"
	"io"
)

type SliceBuffer struct {
	s []byte
	i int
}

func NewSliceBuffer(s []byte) *SliceBuffer {
	return &SliceBuffer{s, 0}
}

func (s *SliceBuffer) Slice1(n int) ([]byte, error) {
	if s.i+n > len(s.s) {
		return nil, io.EOF
	}
	b := s.s[s.i : s.i+n]
	s.i += n
	return b, nil
}

func (s *SliceBuffer) Slice(n int) []byte {
	if s.i+n > len(s.s) {
		panic(io.EOF)
	}
	b := s.s[s.i : s.i+n]
	s.i += n
	return b
}

func (s *SliceBuffer) ReadByte() byte {
	b, e := s.ReadByte1()
	if e != nil {
		panic(e)
	}
	return b
}

func (s *SliceBuffer) ReadByte1() (byte, error) {
	if s.i >= len(s.s) {
		return 0, io.EOF
	}
	b := s.s[s.i]
	s.i++
	return b, nil
}

func (s *SliceBuffer) Read(b []byte) int {
	n, err := s.Read1(b)
	if err != nil {
		panic(err)
	}
	return n
}

func (s *SliceBuffer) Read1(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	if s.i >= len(s.s) {
		return 0, io.EOF
	}
	n := copy(b, s.s[s.i:])
	s.i += n
	return n, nil
}

func (s *SliceBuffer) Seek(offset int64, whence int) int64 {
	n, err := s.seek(offset, whence)
	if err != nil {
		panic(err)
	}
	return n
}

func (s *SliceBuffer) seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case 0:
		abs = offset
	case 1:
		abs = int64(s.i) + offset
	case 2:
		abs = int64(len(s.s)) + offset
	default:
		return 0, errors.New("invalid whence")
	}
	if abs < 0 {
		return 0, errors.New("negative position")
	}
	if abs >= 1<<31 {
		return 0, errors.New("position out of range")
	}
	s.i = int(abs)
	return abs, nil
}

func (s *SliceBuffer) ReadUint64() uint64 {
	bb := s.Slice(8)
	return binary.LittleEndian.Uint64(bb)
}

func (s *SliceBuffer) ReadUint32() uint32 {
	bb := s.Slice(4)
	return binary.LittleEndian.Uint32(bb)
}

func (s *SliceBuffer) ReadUint16() uint16 {
	bb := s.Slice(2)
	return binary.LittleEndian.Uint16(bb)
}

func (s *SliceBuffer) ReadUint24() uint32 {
	bb := s.Slice(3)
	return LittleEndian.Uint24(bb)
}
