package rdb

import (
	"encoding/binary"
	"io"
	"math"
	"strconv"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/errors"
	"github.com/mgtv-tech/redis-GunYu/pkg/log"
	"github.com/mgtv-tech/redis-GunYu/pkg/util"
)

type RdbReader struct {
	raw          io.Reader
	buf          [16]byte
	logger       log.Logger
	totalEntries uint32
	readEntries  uint32
}

func NewRdbReader(r io.Reader) *RdbReader {
	return &RdbReader{raw: r, logger: log.WithLogger(config.LogModuleName("[RdbReader] "))}
}

func (r *RdbReader) Read(p []byte) (int, error) {
	n, err := r.raw.Read(p)
	return n, errors.WithStack(err)
}

func (r *RdbReader) newParser(t byte, l *Loader) Parser {
	p, err := NewParser(t, l.targetRedisVersion, l.rdbVersion)
	panicIfErr(err)
	p.ReadBuffer(l)
	return p
}

func (r *RdbReader) ReadStringP() []byte {
	b, err := r.ReadString()
	panicIfErr(err)
	return b
}

func (r *RdbReader) ReadString() ([]byte, error) {
	length, encoded, err := r.readEncodedLength()
	if err != nil {
		return nil, err
	}
	if !encoded {
		return r.ReadBytes(int(length))
	}
	switch t := uint8(length); t {
	default:
		return nil, errors.Errorf("invalid encoded-string : %03x", t)
	case rdbEncInt8:
		i, err := r.ReadInt8()
		return []byte(strconv.FormatInt(int64(i), 10)), err
	case rdbEncInt16:
		i, err := r.ReadInt16()
		return []byte(strconv.FormatInt(int64(i), 10)), err
	case rdbEncInt32:
		i, err := r.ReadInt32()
		return []byte(strconv.FormatInt(int64(i), 10)), err
	case rdbEncLZF:
		var inlen, outlen uint32
		if inlen, err = r.ReadLength(); err != nil {
			return nil, err
		}
		if outlen, err = r.ReadLength(); err != nil {
			return nil, err
		}
		if in, err := r.ReadBytes(int(inlen)); err != nil {
			return nil, err
		} else {
			return lzfDecompress(in, int(outlen))
		}
	}
}

func (r *RdbReader) readEncodedLength() (length uint64, encoded bool, err error) {
	u, err := r.ReadUint8()
	if err != nil {
		return
	}

	var length32 uint32
	t := (u & 0xC0) >> 6
	switch t {
	case rdb6bitLen:
		length = uint64(u & 0x3f)
	case rdb14bitLen:
		var u2 uint8
		u2, err = r.ReadUint8()
		length = (uint64(u&0x3f) << 8) + uint64(u2)
	case rdbEncVal:
		encoded = true
		length = uint64(u & 0x3f)
	default:
		switch u {
		case rdb32bitLen:
			length32, err = r.ReadUint32BigEndian()
			length = uint64(length32)
		case rdb64bitLen:
			length, err = r.ReadUint64BigEndian()
		default:
			length, err = 0, errors.Errorf("unknown encoding type : %03x", u)
		}
	}
	return
}

func (r *RdbReader) ReadLengthP() uint32 {
	l, e := r.ReadLength()
	panicIfErr(e)
	return l
}

func (r *RdbReader) ReadLength() (uint32, error) {
	length, encoded, err := r.readEncodedLength()
	if err == nil && encoded {
		err = errors.Errorf("encoded-length")
	}
	return uint32(length), err
}

func (r *RdbReader) ReadLength64P() uint64 {
	length, encoded, err := r.readEncodedLength()
	if err == nil && encoded {
		panicIfErr(errors.Errorf("encoded-length"))
	}
	panicIfErr(err)
	return length
}

func (r *RdbReader) ReadLength64() (uint64, error) {
	length, encoded, err := r.readEncodedLength()
	if err == nil && encoded {
		err = errors.Errorf("encoded-length")
	}
	return length, err
}

func (r *RdbReader) ReadDoubleP() float64 {
	f, e := r.ReadDouble()
	panicIfErr(e)
	return f
}

func (r *RdbReader) ReadDouble() (float64, error) {
	var buf = make([]byte, 8)
	err := r.readFull(buf)
	if err != nil {
		return 0, err
	}
	bits := binary.LittleEndian.Uint64(buf)
	return float64(math.Float64frombits(bits)), nil
}

func (r *RdbReader) ReadFloatP() float64 {
	f, e := r.ReadFloat()
	panicIfErr(e)
	return f
}

func (r *RdbReader) ReadFloat() (float64, error) {
	u, err := r.ReadUint8()
	if err != nil {
		return 0, err
	}
	switch u {
	case 253:
		return math.NaN(), nil
	case 254:
		return math.Inf(0), nil
	case 255:
		return math.Inf(-1), nil
	default:
		if b, err := r.ReadBytes(int(u)); err != nil {
			return 0, err
		} else {
			v, err := strconv.ParseFloat(string(b), 64)
			return v, errors.WithStack(err)
		}
	}
}

func (r *RdbReader) ReadByteP() byte {
	b := r.buf[:1]
	_, err := io.ReadFull(r, b)
	panicIfErr(err)
	return b[0]
}

func (r *RdbReader) ReadByte() (byte, error) {
	b := r.buf[:1]
	_, err := io.ReadFull(r, b)
	return b[0], errors.WithStack(err)
}

func (r *RdbReader) readFull(p []byte) error {
	_, err := io.ReadFull(r, p)
	return errors.WithStack(err)
}

func (r *RdbReader) ReadBytesP(n int) []byte {
	p := make([]byte, n)
	err := r.readFull(p)
	panicIfErr(err)
	return p
}

func (r *RdbReader) ReadBytes(n int) ([]byte, error) {
	p := make([]byte, n)
	return p, r.readFull(p)
}

func (r *RdbReader) ReadUint8P() uint8 {
	b, err := r.ReadByte()
	panicIfErr(err)
	return uint8(b)
}

func (r *RdbReader) ReadUint8() (uint8, error) {
	b, err := r.ReadByte()
	return uint8(b), err
}

func (r *RdbReader) ReadUint16() (uint16, error) {
	b := r.buf[:2]
	err := r.readFull(b)
	return binary.LittleEndian.Uint16(b), err
}

func (r *RdbReader) ReadUint32P() uint32 {
	b := r.buf[:4]
	err := r.readFull(b)
	panicIfErr(err)
	return binary.LittleEndian.Uint32(b)
}

func (r *RdbReader) ReadUint32() (uint32, error) {
	b := r.buf[:4]
	err := r.readFull(b)
	return binary.LittleEndian.Uint32(b), err
}

func (r *RdbReader) ReadUint64P() uint64 {
	b := r.buf[:8]
	err := r.readFull(b)
	panicIfErr(err)
	return binary.LittleEndian.Uint64(b)
}

func (r *RdbReader) ReadUint64() (uint64, error) {
	b := r.buf[:8]
	err := r.readFull(b)
	return binary.LittleEndian.Uint64(b), err
}

func (r *RdbReader) Read8ByteP() []byte {
	b := r.buf[:8]
	err := r.readFull(b)
	panicIfErr(err)
	return b
}

func (r *RdbReader) Read16ByteP() []byte {
	b := r.buf[:16]
	err := r.readFull(b)
	panicIfErr(err)
	return b
}

func (r *RdbReader) ReadUint32BigEndian() (uint32, error) {
	b := r.buf[:4]
	err := r.readFull(b)
	return binary.BigEndian.Uint32(b), err
}

func (r *RdbReader) ReadUint64BigEndian() (uint64, error) {
	b := r.buf[:8]
	err := r.readFull(b)
	return binary.BigEndian.Uint64(b), err
}

func (r *RdbReader) ReadInt8() (int8, error) {
	u, err := r.ReadUint8()
	return int8(u), err
}

func (r *RdbReader) ReadInt16() (int16, error) {
	u, err := r.ReadUint16()
	return int16(u), err
}

func (r *RdbReader) ReadInt32() (int32, error) {
	u, err := r.ReadUint32()
	return int32(u), err
}

func lzfDecompress(in []byte, outlen int) (out []byte, err error) {
	defer func() {
		if x := recover(); x != nil {
			err = errors.Errorf("decompress exception: %v", x)
		}
	}()
	out = make([]byte, outlen)
	i, o := 0, 0
	for i < len(in) {
		ctrl := int(in[i])
		i++
		if ctrl < 32 {
			for x := 0; x <= ctrl; x++ {
				out[o] = in[i]
				i++
				o++
			}
		} else {
			length := ctrl >> 5
			if length == 7 {
				length = length + int(in[i])
				i++
			}
			ref := o - ((ctrl & 0x1f) << 8) - int(in[i]) - 1
			i++
			for x := 0; x <= length+1; x++ {
				out[o] = out[ref]
				ref++
				o++
			}
		}
	}
	if o != outlen {
		return nil, errors.Errorf("decompress length is %d != expected %d", o, outlen)
	}
	return out, nil
}

func (r *RdbReader) ReadZipmapItemP(buf *util.SliceBuffer, readFree bool) []byte {
	b := r.ReadZipmapItem(buf, readFree)
	return b
}

func (r *RdbReader) ReadZipmapItem(buf *util.SliceBuffer, readFree bool) []byte {
	length, free := readZipmapItemLength(buf, readFree)
	if length == -1 {
		return nil
	}
	value := buf.Slice(length)
	buf.Seek(int64(free), 1)
	return value
}

func readZipmapItemLength(buf *util.SliceBuffer, readFree bool) (int, int) {
	b := buf.ReadByte()
	switch b {
	case 253:
		s := buf.Slice(5)
		return int(binary.BigEndian.Uint32(s)), int(s[4])
	case 254:
		panic(errors.Errorf("rdb: invalid zipmap item length"))
	case 255:
		return -1, 0
	}
	var free byte
	if readFree {
		free = buf.ReadByte()
	}
	return int(b), int(free)
}

func (r *RdbReader) CountZipmapItemsP(buf *util.SliceBuffer) int {
	i := r.CountZipmapItems(buf)
	return i
}

func (r *RdbReader) CountZipmapItems(buf *util.SliceBuffer) int {
	n := 0
	for {
		strLen, free := readZipmapItemLength(buf, n%2 != 0)
		if strLen == -1 {
			break
		}
		buf.Seek(int64(strLen)+int64(free), 1)
		n++
	}
	buf.Seek(0, 0)
	return n
}

func moduleTypeNameByID(moduleId uint64) string {
	nameList := make([]byte, 9)
	moduleId >>= 10
	for i := 8; i >= 0; i-- {
		nameList[i] = moduleTypeNameCharSet[moduleId&63]
		moduleId >>= 6
	}
	return string(nameList)
}

func (r *RdbReader) moduleLoadUnsigned(rdbType byte) (uint64, error) {
	if rdbType == RdbTypeModule2 {
		// ver == 2
		if opCode, err := r.ReadLength(); err != nil {
			return 0, err
		} else if opCode != rdbModuleOpcodeUint {
			return 0, errors.Errorf("opcode(%v) != rdbModuleOpcodeUint(%v)", opCode, rdbModuleOpcodeUint)
		}
	}

	val, err := r.ReadLength()
	return uint64(val), err
}

func (r *RdbReader) moduleLoadDouble(rdbType byte) (float64, error) {
	if rdbType == RdbTypeModule2 {
		// ver == 2
		if opCode, err := r.ReadLength(); err != nil {
			return 0, err
		} else if opCode != rdbModuleOpcodeDouble {
			return 0, errors.Errorf("opcode(%v) != rdbModuleOpcodeDouble(%v)", opCode,
				rdbModuleOpcodeDouble)
		}
	}

	return r.ReadDouble()
}

func (r *RdbReader) moduleLoadString(rdbType byte) (string, error) {
	if rdbType == RdbTypeModule2 {
		if opCode, err := r.ReadLength(); err != nil {
			return "", err
		} else if opCode != rdbModuleOpcodeString {
			return "", errors.Errorf("opcode(%v) != rdbModuleOpcodeString(%v)", opCode,
				rdbModuleOpcodeString)
		}
	}
	ret, err := r.ReadString()
	return string(ret), err
}
