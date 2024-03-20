package rdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/digest"
	rerror "github.com/mgtv-tech/redis-GunYu/pkg/errors"
	"github.com/mgtv-tech/redis-GunYu/pkg/log"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/types"
	"github.com/mgtv-tech/redis-GunYu/pkg/util"
)

var (
	maxBinEntryBuffer = 16 * 1024 * 1024
)

func panicIfErr(err error) {
	if err != nil {
		err = rerror.WithStack(err)
		panic(err)
	}
}

type RdbObjExecutor func(cmd string, args ...interface{}) error

// Parser parse rdb to redis commands
// throw panic if something is wrong
// redis : rdb.c:rdbLoadObject, rdbSaveRio
type Parser interface {
	Type() int
	RdbType() int
	ReadBuffer(*Loader)
	ExecCmd(RdbObjExecutor)
	Key() []byte
	Value() []byte
	CreateValueDump() []byte
	ValueDumpSize() int
	FirstBin() bool
	IsSplited() bool
	DB() uint32
	CanRestore() bool
}

func NewParser(t byte, targetRedisVersion string, rdbVersion int64) (Parser, error) {
	switch t {
	case RdbTypeString:
		// string
		p := &StringParser{}
		p.rtype = t
		p.targetRedisVersion = targetRedisVersion
		p.rdbVersion = rdbVersion
		p.otype = RdbObjectString
		return p, nil
	case RdbTypeListZiplist, RdbTypeList, RdbTypeQuicklist, RdbTypeQuicklist2:
		// list
		p := &ListParser{}
		p.rtype = t
		p.targetRedisVersion = targetRedisVersion
		p.rdbVersion = rdbVersion
		p.otype = RdbObjectList
		return p, nil
	case RdbTypeSet, RdbTypeSetIntset, RdbTypeSetListpack:
		// set
		p := &SetParser{}
		p.rtype = t
		p.targetRedisVersion = targetRedisVersion
		p.rdbVersion = rdbVersion
		p.otype = RdbObjectSet
		return p, nil
	case RdbTypeZSet, RdbTypeZSet2, RdbTypeZSetZiplist, RdbTypeZSetListpack:
		// zset
		p := &ZSetParser{}
		p.rtype = t
		p.targetRedisVersion = targetRedisVersion
		p.rdbVersion = rdbVersion
		p.otype = RdbObjectZSet
		return p, nil
	case RdbTypeHash, RdbTypeHashZipmap, RdbTypeHashZiplist, RdbTypeHashListpack:
		// hash
		p := &HashPaser{}
		p.rtype = t
		p.targetRedisVersion = targetRedisVersion
		p.rdbVersion = rdbVersion
		p.otype = RdbObjectHash
		return p, nil
	case RDBTypeStreamListPacks, RDBTypeStreamListPacks2, RdbTypeStreamListPacks3:
		// @TODO implement other logical, (>=redis 5)
		p := &StreamParser{}
		p.rtype = t
		p.targetRedisVersion = targetRedisVersion
		p.rdbVersion = rdbVersion
		p.otype = RdbObjectStream
		return p, nil
	case RdbTypeModule, RdbTypeModule2:
		// module
		p := &ModuleParser{}
		p.rtype = t
		p.targetRedisVersion = targetRedisVersion
		p.rdbVersion = rdbVersion
		p.otype = RdbObjectModule
		return p, nil
	case RdbTypeFunction2:
		p := &FunctionParser{}
		p.rtype = t
		p.targetRedisVersion = targetRedisVersion
		p.rdbVersion = rdbVersion
		p.otype = RdbObjectFunction
		return p, nil
	case RdbFlagAUX:
		p := &AuxParser{}
		p.rtype = t
		p.targetRedisVersion = targetRedisVersion
		p.rdbVersion = rdbVersion
		p.otype = RdbObjectAux
		return p, nil
	default:

	}
	return nil, fmt.Errorf("unknown type : %v", t)
}

type BaseParser struct {
	targetRedisVersion string
	rdbVersion         int64
	rtype              byte
	otype              int
	buf                bytes.Buffer
	key                []byte
	val                []byte
	db                 uint32
	cmd                string
	totalEntries       uint32
	readEntries        uint32
	historyEntries     uint32
	forceExecCmd       bool
}

func (bp *BaseParser) Type() int {
	return bp.otype
}

func (bp *BaseParser) RdbType() int {
	return int(bp.rtype)
}

func (bp *BaseParser) Key() []byte {
	return bp.key
}

func (bp *BaseParser) Value() []byte {
	return bp.val
}

func (bp *BaseParser) FirstBin() bool {
	return bp.historyEntries == 0
}

func (bp *BaseParser) IsSplited() bool {
	if bp.historyEntries != 0 {
		return true
	}
	return bp.totalEntries-bp.readEntries > 0
}

func (bp *BaseParser) CanRestore() bool {
	return !bp.forceExecCmd
}

func (bp *BaseParser) DB() uint32 {
	return bp.db
}

// redis, cluster.c:verifyDumpPayload
func (bp *BaseParser) CreateValueDump() []byte {
	return CreateValueDump(bp.rtype, bp.buf.Bytes())
}

func CreateValueDump(rtype byte, data []byte) []byte {
	var b bytes.Buffer
	c := digest.New()
	w := io.MultiWriter(&b, c)
	w.Write([]byte{rtype}) // 1B
	w.Write(data)          // xB
	// > redis:3.0.0
	binary.Write(w, binary.LittleEndian, uint16(6)) // 2B
	binary.Write(w, binary.LittleEndian, c.Sum64()) // 8B
	return b.Bytes()
}

func (bp *BaseParser) ValueDumpSize() int {
	return 1 + bp.buf.Len() + 2 + 8
}

func (bp *BaseParser) readBufferBegin(lr *Loader) {
	r := NewRdbReader(lr)
	bp.totalEntries = lr.totalEntries
	bp.readEntries = lr.readEntries
	bp.historyEntries = lr.readEntries

	if bp.totalEntries-bp.readEntries == 0 {
		bp.key = r.ReadStringP()
	} else {
		bp.key = lr.lastEntry.Key
	}
}

func (bp *BaseParser) readBufferEnd(lr *Loader) {
	if bp.totalEntries-bp.readEntries == 0 {
		lr.totalEntries = 0
		lr.readEntries = 0
	} else {
		lr.totalEntries = bp.totalEntries
		lr.readEntries = bp.readEntries
	}
}

type HashPaser struct {
	BaseParser
}

// RdbTypeHash
// RdbTypeHashZipmap
// RdbTypeHashZiplist
// RdbTypeHashListpack
// redis7.2(rdb 11) : if (sdslen(field)>64 || sdslen(value) > 64), RdbTypeHash; else RdbTypeHashListpack
// redis4.0(rdb 8) : if (ziplistLen(ziplist) < 512) then RdbTypeHashZiplist; else RdbTypeHash,
func (hp *HashPaser) ReadBuffer(lr *Loader) {
	hp.readBufferBegin(lr)
	hp.cmd = "HSET"
	r := NewRdbReader(io.TeeReader(lr, &hp.buf))
	switch hp.rtype {
	case RdbTypeHash:
		var n uint32
		if hp.totalEntries-hp.readEntries == 0 {
			rlen := r.ReadLengthP()
			n = rlen
			hp.totalEntries = n
		} else {
			n = hp.totalEntries - hp.readEntries
		}
		for i := 0; i < int(n); i++ {
			r.ReadStringP()
			r.ReadStringP()
			hp.readEntries++
			if hp.buf.Len() > maxBinEntryBuffer && i != int(n-1) { // gc
				break
			}
		}
	case RdbTypeHashZipmap, RdbTypeHashZiplist, RdbTypeHashListpack:
		r.ReadStringP()
	default:
		panic(fmt.Errorf("unknown hash type : %v", hp.rtype))
	}
	hp.readBufferEnd(lr)
}

func (hp *HashPaser) ExecCmd(cb RdbObjExecutor) {
	switch hp.rtype {
	case RdbTypeHashZiplist:
		hp.ziplist(cb)
	case RdbTypeHashZipmap:
		hp.zipmap(cb)
	case RdbTypeHash:
		hp.hash(cb)
	case RdbTypeHashListpack:
		hp.listpack(cb)
	}
}

func (hp *HashPaser) hash(cb RdbObjExecutor) {
	r := NewRdbReader(bytes.NewReader(hp.buf.Bytes()))
	curNo := hp.readEntries - hp.historyEntries
	if hp.FirstBin() {
		r.ReadLengthP()
	}
	for i := 0; i < int(curNo); i++ {
		field := r.ReadStringP()
		value := r.ReadStringP()
		panicIfErr(cb(hp.cmd, hp.key, field, value))
	}
}

func (hp *HashPaser) ziplist(cb RdbObjExecutor) {
	r := NewRdbReader(bytes.NewReader(hp.buf.Bytes()))
	ziplist := r.ReadStringP()

	zpl := types.NewZiplist(ziplist)
	field := zpl.Next()
	val := zpl.Next()

	for field != nil && val != nil {
		panicIfErr(cb(hp.cmd, hp.key, field, val))
		field = zpl.Next()
		val = zpl.Next()
	}
}

func (hp *HashPaser) listpack(cb RdbObjExecutor) {
	r := NewRdbReader(bytes.NewReader(hp.buf.Bytes()))
	vv := r.ReadStringP()
	listp := types.NewListpack(vv)
	length := int(listp.NumElements())
	if length%2 != 0 {
		panicIfErr(fmt.Errorf("hash list pack is not even : %d", length))
	}
	for i := 0; i < length/2; i++ {
		member := listp.Next()
		score := listp.Next()
		panicIfErr(cb(hp.cmd, hp.key, score, member))
	}
}

func (hp *HashPaser) zipmap(cb RdbObjExecutor) {
	r := NewRdbReader(bytes.NewReader(hp.buf.Bytes()))
	var length int
	zipmap := r.ReadStringP()
	buf := util.NewSliceBuffer(zipmap)
	lenByte := buf.ReadByte()
	if lenByte >= 254 { // we need to count the items manually
		length = r.CountZipmapItemsP(buf)
	} else {
		length = int(lenByte)
	}

	for i := 0; i < length; i++ {
		field := r.ReadZipmapItemP(buf, false)
		value := r.ReadZipmapItemP(buf, true)
		panicIfErr(cb(hp.cmd, hp.key, field, value))
	}
}

// list
type ListParser struct {
	BaseParser
}

// 4.0 : quicklist
func (lp *ListParser) ReadBuffer(lr *Loader) {
	lp.cmd = "RPUSH"
	lp.readBufferBegin(lr)
	r := NewRdbReader(io.TeeReader(lr, &lp.buf))
	switch lp.rtype {
	case RdbTypeListZiplist:
		r.ReadStringP()
	case RdbTypeList, RdbTypeQuicklist, RdbTypeQuicklist2:
		n := r.ReadLengthP()
		for i := 0; i < int(n); i++ {
			if lp.rtype == RdbTypeQuicklist2 {
				r.ReadLengthP()
			}
			r.ReadStringP()
		}
	}
	lp.readBufferEnd(lr)
}

func (lp *ListParser) ExecCmd(cb RdbObjExecutor) {
	// redis4.0+ : quicklist
	switch lp.rtype {
	case RdbTypeListZiplist: // redis 3.0
		lp.ziplist(cb)
	case RdbTypeList: // redis 3.0
		lp.list(cb)
	case RdbTypeQuicklist: // redis 4.0, RDB_VERSION=8
		lp.quicklist(cb)
	case RdbTypeQuicklist2:
		lp.quicklist2(cb)
	}
}

func (lp *ListParser) ziplist(cb RdbObjExecutor) {
	r := NewRdbReader(bytes.NewReader(lp.buf.Bytes()))
	ziplist := r.ReadStringP()
	zpl := types.NewZiplist(ziplist)
	for entry := zpl.Next(); entry != nil; entry = zpl.Next() {
		panicIfErr(cb(lp.cmd, lp.key, entry))
	}
}

func (lp *ListParser) list(cb RdbObjExecutor) {
	r := NewRdbReader(bytes.NewReader(lp.buf.Bytes()))
	n := r.ReadLengthP()
	for i := uint32(0); i < n; i++ {
		f := r.ReadStringP()
		panicIfErr(cb(lp.cmd, lp.key, f))
	}
}

/* quicklist node container formats */
const (
	quicklistNodeContainerPlain  = 1 // QUICKLIST_NODE_CONTAINER_PLAIN
	quicklistNodeContainerPacked = 2 // QUICKLIST_NODE_CONTAINER_PACKED
)

func (lp *ListParser) quicklist(cb RdbObjExecutor) {
	r := NewRdbReader(bytes.NewReader(lp.buf.Bytes()))
	n := r.ReadLengthP()
	for i := uint32(0); i < n; i++ {
		ziplist := r.ReadStringP()
		zpl := types.NewZiplist(ziplist)
		for entry := zpl.Next(); entry != nil; entry = zpl.Next() {
			panicIfErr(cb(lp.cmd, lp.key, entry))
		}
	}
}

func (lp *ListParser) quicklist2(cb RdbObjExecutor) {
	r := NewRdbReader(bytes.NewReader(lp.buf.Bytes()))
	csize := r.ReadLengthP()
	for i := uint32(0); i < csize; i++ {
		container := r.ReadLengthP()
		if container == quicklistNodeContainerPlain {
			entry := r.ReadStringP()
			panicIfErr(cb(lp.cmd, lp.key, entry))
		} else if container == quicklistNodeContainerPacked {
			vv := r.ReadStringP()
			listp := types.NewListpack(vv)
			length := int(listp.NumElements())
			for i := 0; i < length; i++ {
				mb := listp.Next()
				panicIfErr(cb(lp.cmd, lp.key, mb))
			}
		} else {
			panicIfErr(fmt.Errorf("unknown quicklist container : %v", container))
		}
	}
}

// string
type StringParser struct {
	BaseParser
}

func (sp *StringParser) ReadBuffer(lr *Loader) {
	sp.readBufferBegin(lr)
	r := NewRdbReader(io.TeeReader(lr, &sp.buf))

	sp.val = r.ReadStringP()
	sp.readBufferEnd(lr)
}

func (sp *StringParser) ExecCmd(cb RdbObjExecutor) {
	panicIfErr(cb("set", sp.key, sp.val))
}

// set

type SetParser struct {
	BaseParser
}

func (sp *SetParser) ReadBuffer(lr *Loader) {
	sp.cmd = "SADD"
	sp.readBufferBegin(lr)
	r := NewRdbReader(io.TeeReader(lr, &sp.buf))

	switch sp.rtype {
	case RdbTypeSet:

		n := r.ReadLengthP()
		for i := 0; i < int(n); i++ {
			r.ReadStringP()
		}
	case RdbTypeSetIntset, RdbTypeSetListpack: // @TODO test RdbTypeSetListpack

		r.ReadStringP()
	}
	sp.readBufferEnd(lr)
}

func (sp *SetParser) ExecCmd(cb RdbObjExecutor) {
	switch sp.rtype {
	case RdbTypeSet: // redis 4.0, redis 7.2
		r := NewRdbReader(bytes.NewReader(sp.buf.Bytes()))
		n := r.ReadLengthP()
		for i := uint32(0); i < n; i++ {
			entry := r.ReadStringP()
			panicIfErr(cb(sp.cmd, sp.key, entry))
		}
	case RdbTypeSetIntset: // redis 4.0, redis 7.2
		sp.intset(cb)
	case RdbTypeSetListpack: // redis 7.2
		r := NewRdbReader(bytes.NewReader(sp.buf.Bytes()))
		vv := r.ReadStringP()
		lp := types.NewListpack(vv)
		for i := 0; i < int(lp.NumElements()); i++ {
			entry := lp.Next()
			panicIfErr(cb(sp.cmd, sp.key, entry))
		}
	}
}

func (sp *SetParser) intset(cb RdbObjExecutor) {
	r := NewRdbReader(bytes.NewReader(sp.buf.Bytes()))
	intSet := r.ReadStringP()
	buf := util.NewSliceBuffer(intSet)
	intSizeBytes := buf.Slice(4)
	intSize := binary.LittleEndian.Uint32(intSizeBytes)
	if intSize != 2 && intSize != 4 && intSize != 8 {
		panicIfErr(fmt.Errorf("unknown intset encoding : %d", intSize))
	}
	lenBytes := buf.Slice(4)
	cardinality := binary.LittleEndian.Uint32(lenBytes)
	for j := uint32(0); j < cardinality; j++ {
		intBytes := buf.Slice(int(intSize))
		var intString string
		switch intSize {
		case 2:
			intString = strconv.FormatInt(int64(int16(binary.LittleEndian.Uint16(intBytes))), 10)
		case 4:
			intString = strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(intBytes))), 10)
		case 8:
			intString = strconv.FormatInt(int64(int64(binary.LittleEndian.Uint64(intBytes))), 10)
		}
		panicIfErr(cb(sp.cmd, sp.key, intString))
	}
}

// zset
type ZSetParser struct {
	BaseParser
}

// redis4.0(rdb8) : ziplist_entries <= 128 then ziplist; else zset2(skiplist)
// redis7.2(rdb11) : entries <= 128 then listpack; else zset2
func (zp *ZSetParser) ReadBuffer(lr *Loader) {
	zp.cmd = "ZADD"
	zp.readBufferBegin(lr)
	r := NewRdbReader(io.TeeReader(lr, &zp.buf))
	switch zp.rtype {
	case RdbTypeZSet, RdbTypeZSet2:
		n := r.ReadLengthP()
		for i := 0; i < int(n); i++ {
			r.ReadStringP()
			if zp.rtype == RdbTypeZSet2 {
				r.ReadDoubleP()
			} else {
				r.ReadFloatP()
			}
		}
	case RdbTypeZSetZiplist, RdbTypeZSetListpack:
		r.ReadStringP()
	}
	zp.readBufferEnd(lr)
}

func (zp *ZSetParser) ExecCmd(cb RdbObjExecutor) {
	switch zp.rtype {
	case RdbTypeZSet, RdbTypeZSet2:
		r := NewRdbReader(bytes.NewReader(zp.buf.Bytes()))
		n := r.ReadLengthP()
		for i := uint32(0); i < n; i++ {
			mb := r.ReadStringP()
			var score float64
			if zp.rtype == RdbTypeZSet {
				score = r.ReadFloatP()
			} else {
				score = r.ReadDoubleP()
			}
			panicIfErr(cb(zp.cmd, zp.key, score, mb))
		}
	case RdbTypeZSetZiplist:
		r := NewRdbReader(bytes.NewReader(zp.buf.Bytes()))
		ziplist := r.ReadStringP()
		zpl := types.NewZiplist(ziplist)
		member := zpl.Next()
		score := zpl.Next()
		for member != nil && score != nil {
			// zadd key score member
			panicIfErr(cb(zp.cmd, zp.key, score, member))
			member = zpl.Next()
			score = zpl.Next()
		}
	case RdbTypeZSetListpack:
		r := NewRdbReader(bytes.NewReader(zp.buf.Bytes()))
		vv := r.ReadStringP()
		listp := types.NewListpack(vv)
		length := int(listp.NumElements())
		if length%2 != 0 {
			panicIfErr(fmt.Errorf("zset list pack is not even : %d", length))
		}
		for i := 0; i < length/2; i++ {
			member := listp.Next()
			score := listp.Next()
			panicIfErr(cb(zp.cmd, zp.key, score, member))
		}
	}
}

func Float64ToByte(float float64) string {
	return strconv.FormatFloat(float, 'f', -1, 64)
}

// stream
// redis5.0+
// redis5.0 : RDBTypeStreamListPacks
// redis7.0 : RDBTypeStreamListPacks2
// redis7.2 : RdbTypeStreamListPacks3
type StreamParser struct {
	BaseParser
}

func (sp *StreamParser) ReadBuffer(lr *Loader) {
	sp.readBufferBegin(lr)
	r := NewRdbReader(io.TeeReader(lr, &sp.buf))

	switch sp.rtype {
	case RDBTypeStreamListPacks, RDBTypeStreamListPacks2, RdbTypeStreamListPacks3:
		// list pack length
		listpackLength := r.ReadLength64P()
		for i := 0; i < int(listpackLength); i++ {
			// rdbSaveRawString(rdb,ri.key,ri.key_len)
			key := r.ReadStringP()
			if len(key) != 16 {
				panicIfErr(fmt.Errorf("stream parser error, key length is not 16B : type(%d), key(%s)", sp.rtype, key))
			}
			// list pack : rdbSaveRawString(rdb,lp,lp_bytes)
			r.ReadStringP()
		}

		r.ReadLength64P() // rdbSaveLen(rdb,s->length)
		r.ReadLength64P() // rdbSaveLen(rdb,s->last_id.ms)
		r.ReadLength64P() // rdbSaveLen(rdb,s->last_id.seq)

		if sp.rtype >= RDBTypeStreamListPacks2 {
			/* Load the first entry ID. */
			r.ReadLength64P()
			r.ReadLength64P()
			/* Load the maximal deleted entry ID. */
			r.ReadLength64P()
			r.ReadLength64P()
			/* Load the offset. */
			r.ReadLength64P()
		}

		/* Load the number of groups. */
		nConsumerGroup := r.ReadLength64P()
		for i := 0; i < int(nConsumerGroup); i++ {
			/* Load the group name. */
			r.ReadStringP()

			/* Load the last ID */
			// rdbSaveLen(rdb,cg->last_id.ms)
			r.ReadLength64P()
			// rdbSaveLen(rdb,cg->last_id.seq)
			r.ReadLength64P()

			if sp.rtype >= RDBTypeStreamListPacks2 {
				r.ReadLength64P()
			}

			// the global PEL
			nPending := r.ReadLength64P()
			for i := 0; i < int(nPending); i++ {
				// stream ID 128bits : rdbWriteRaw(rdb,ri.key,sizeof(streamID))
				r.Read16ByteP()

				// delivery time : rdbSaveMillisecondTime(rdb,nack->delivery_time)
				r.Read8ByteP()

				// delivery count : rdbSaveLen(rdb,nack->delivery_count)
				r.ReadLengthP()
			}

			// the consumers of this group

			// Number of consumers in this consumer group
			numConsumer := r.ReadLengthP()
			for i := 0; i < int(numConsumer); i++ {
				// Consumer name
				r.ReadStringP()

				// Seen time : rdbSaveMillisecondTime(rdb,consumer->seen_time)
				r.Read8ByteP()

				if sp.rtype >= RdbTypeStreamListPacks3 {
					// Active time : rdbSaveMillisecondTime(rdb,consumer->active_time)
					r.Read8ByteP()
				}

				// Consumer PEL
				// Number of entries in the PEL
				nPending2 := r.ReadLength64P()
				for i := 0; i < int(nPending2); i++ {
					// stream ID
					r.Read16ByteP()
				}
			}
		}
	default:
		panicIfErr(fmt.Errorf("unknown stream type : %x", sp.rtype))
	}
	sp.readBufferEnd(lr)
}

func (sp *StreamParser) streamCompareID(aMs, aSeq, bMs, bSeq uint64) int {
	if aMs > bMs {
		return 1
	} else if aMs < bMs {
		return -1
	}
	if aSeq > bSeq {
		return 1
	} else if aSeq < bSeq {
		return -1
	}
	return 0
}

func (sp *StreamParser) ExecCmd(cb RdbObjExecutor) {
	r := NewRdbReader(bytes.NewReader(sp.buf.Bytes()))

	// read deseriliazation code in rdb.c(rdbLoadObject) first,
	// then read code in aof.c(rewriteStreamObject)

	// <listpack-length><stream-id><listpack>...<stream-id><listpack><active-len><last-id><group-length><group>...<group>
	// list pack length
	listpackLength := r.ReadLength64P()
	for i := uint64(0); i < listpackLength; i++ {
		// Get the master ID, key : rdbSaveRawString(rdb,ri.key,ri.key_len)
		key := r.ReadStringP()
		masterMs := int64(binary.BigEndian.Uint64(key[:8]))
		masterSeq := int64(binary.BigEndian.Uint64(key[8:]))

		/* Load the listpack. */
		val := r.ReadStringP()
		lp := types.NewListpack(val)

		// master entry
		// a new listpack is created, we populate it with a "master entry". This
		// is just a set of fields that is taken as references in order to compress
		// the stream entries that we'll add inside the listpack.

		// * +-------+---------+------------+---------+--/--+---------+---------+-+
		// * | count | deleted | num-fields | field_1 | field_2 | ... | field_N |0|
		// * +-------+---------+------------+---------+--/--+---------+---------+-+
		// * count and deleted just represent respectively the total number of
		// * entries inside the listpack that are valid, and marked as deleted
		// * (deleted flag in the entry flags set). So the total number of items
		// * actually inside the listpack (both deleted and not) is count+deleted.

		count := lp.NextInteger()              // items count
		deleted := lp.NextInteger()            // deleted count
		numFields := lp.NextInteger()          // num fields
		fields := make([][]byte, 0, numFields) // fields
		for j := int64(0); j < numFields; j++ {
			fields = append(fields, lp.Next())
		}

		/* the zero master entry terminator. */
		lastEntry := lp.Next()
		if !bytes.Equal(lastEntry, []byte("0")) {
			panicIfErr(fmt.Errorf("last entry is not zero"))
		}

		/* Populate the listpack with the new entry. We use the following
		 * encoding:
		 *
		 * +-----+--------+----------+-------+-------+-/-+-------+-------+--------+
		 * |flags|entry-id|num-fields|field-1|value-1|...|field-N|value-N|lp-count|
		 * +-----+--------+----------+-------+-------+-/-+-------+-------+--------+
		 *
		 * However if the SAMEFIELD flag is set, we have just to populate
		 * the entry with the values, so it becomes:
		 *
		 * +-----+--------+-------+-/-+-------+--------+
		 * |flags|entry-id|value-1|...|value-N|lp-count|
		 * +-----+--------+-------+-/-+-------+--------+
		 *
		 * The entry-id field is actually two separated fields: the ms
		 * and seq difference compared to the master entry.
		 */

		for count > 0 || deleted > 0 {
			/* Get the flags entry. */
			flags := lp.NextInteger()
			// entry ID
			entryMs := lp.NextInteger()
			entrySeq := lp.NextInteger()

			// The entry-id field is actually two separated fields: the ms
			// and seq difference compared to the master entry.
			args := []interface{}{sp.key, fmt.Sprintf("%d-%d", entryMs+masterMs, entrySeq+masterSeq)}

			if flags&2 == 2 { // STREAM_ITEM_FLAG_SAMEFIELDS
				for j := int64(0); j < numFields; j++ {
					args = append(args, fields[j], lp.Next())
				}
			} else {
				numFields = lp.NextInteger()
				for j := int64(0); j < numFields; j++ {
					args = append(args, lp.Next(), lp.Next())
				}
			}

			_ = lp.Next() // lp_count

			if flags&1 == 1 { // is_deleted
				deleted -= 1
			} else {
				count -= 1
				panicIfErr(cb("XADD", args...))
			}
		}
	}

	/* Load total number of items inside the stream. */
	// s->length
	streamLength := r.ReadLength64P()

	/* Load the last entry ID. */
	lastMs := r.ReadLength64P()
	lastSeq := r.ReadLength64P()
	lastid := fmt.Sprintf("%d-%d", lastMs, lastSeq)

	// empty stream
	if streamLength == 0 {
		/* Use the XADD MAXLEN 0 trick to generate an empty stream if
		 * the key we are serializing is an empty string, which is possible
		 * for the Stream type. */
		panicIfErr(cb("XADD", sp.key, "MAXLEN", "0", "0-1", "x", "y"))
	}

	entriesAdded := uint64(0)
	maxDelEntryIdSeq := uint64(0)
	maxDelEntryIdMs := uint64(0)
	firstIdMs := uint64(0)
	firstIdSeq := uint64(0)
	xsetidArgs := []interface{}{sp.key, lastid}
	if sp.rtype >= RDBTypeStreamListPacks2 {
		/* Load the first entry ID. */
		firstIdMs = r.ReadLength64P()  // s->first_id.ms
		firstIdSeq = r.ReadLength64P() // s->first_id.seq

		/* Load the maximal deleted entry ID. */
		maxDelEntryIdMs = r.ReadLength64P()  // s->max_deleted_entry_id.ms
		maxDelEntryIdSeq = r.ReadLength64P() // s->max_deleted_entry_id.seq

		/* Load the offset. */
		entriesAdded = r.ReadLength64P()
	} else {
		entriesAdded = streamLength
		maxDelEntryIdMs = 0
		maxDelEntryIdSeq = 0
	}

	if util.VersionGE(sp.targetRedisVersion, "7", util.VersionMajor) {
		xsetidArgs = append(xsetidArgs, "ENTRIESADDED", entriesAdded)
		xsetidArgs = append(xsetidArgs, "MAXDELETEDID", fmt.Sprintf("%d-%d", maxDelEntryIdMs, maxDelEntryIdSeq))
	}

	/* Append XSETID after XADD, make sure lastid is correct,
	 * in case of XDEL lastid. */
	panicIfErr(cb("XSETID", xsetidArgs...))

	/* Create all the stream consumer groups. */
	/* Load the number of groups. */
	nConsumerGroup := r.ReadLength64P()
	for i := uint64(0); i < nConsumerGroup; i++ {

		xgcArgs := []interface{}{"CREATE", sp.key}
		/* Load the group name. */
		groupName := r.ReadStringP()
		xgcArgs = append(xgcArgs, groupName)

		/* Load the last ID */
		cgMs := r.ReadLength64P()  // ms
		cgSeq := r.ReadLength64P() // seq
		cgID := []byte(fmt.Sprintf("%d-%d", cgMs, cgSeq))
		xgcArgs = append(xgcArgs, cgID)

		// redis7.0+ : entries_read
		cgOffset := uint64(0)
		if sp.rtype >= RDBTypeStreamListPacks2 {
			cgOffset = r.ReadLength64P() // offset
			if util.VersionGE(sp.targetRedisVersion, "7", util.VersionMajor) {
				xgcArgs = append(xgcArgs, "ENTRIESREAD", cgOffset)
			}
		} else {
			if util.VersionGE(sp.targetRedisVersion, "7", util.VersionMajor) {
				cgOffset = func() uint64 {
					SCG_INVALID_ENTRIES_READ := -1
					if entriesAdded == 0 {
						return 0
					}
					if streamLength == 0 && sp.streamCompareID(cgMs, cgSeq, lastMs, lastSeq) < 1 {
						return entriesAdded
					}
					cmpLast := sp.streamCompareID(cgMs, cgSeq, lastMs, lastSeq)
					if cmpLast == 0 {
						return entriesAdded
					} else if cmpLast > 0 {
						return uint64(SCG_INVALID_ENTRIES_READ) //SCG_INVALID_ENTRIES_READ, The counter of a future ID is unknown
					}
					cmpIdFirst := sp.streamCompareID(cgMs, cgSeq, firstIdMs, firstIdSeq)
					cmpXdelFirst := sp.streamCompareID(maxDelEntryIdMs, maxDelEntryIdSeq, firstIdMs, firstIdSeq)
					if (maxDelEntryIdMs == 0 && maxDelEntryIdSeq == 0) || cmpXdelFirst < 0 {
						/* There's definitely no fragmentation ahead. */
						if cmpIdFirst < 0 {
							/* Return the estimated counter. */
							return entriesAdded - streamLength
						} else if cmpIdFirst == 0 {
							/* Return the exact counter of the first entry in the stream. */
							return entriesAdded - streamLength + 1
						}
					}
					return uint64(SCG_INVALID_ENTRIES_READ)
				}()
				xgcArgs = append(xgcArgs, "ENTRIESREAD", cgOffset)
			}
		}

		/* Emit the XGROUP CREATE in order to create the group. */
		panicIfErr(cb("XGROUP", xgcArgs...))

		/* Generate XCLAIMs for each consumer that happens to
		 * have pending entries. Empty consumers would be generated with
		 * XGROUP CREATECONSUMER. */
		/* Load the global PEL */
		pelSize := r.ReadLength64P()
		streamNackTime := make(map[string]uint64)
		streamNackCount := make(map[string]uint64)

		for i := uint64(0); i < pelSize; i++ {
			/* Load streamId */
			tmpBytes := r.ReadBytesP(16)
			ms := binary.BigEndian.Uint64(tmpBytes[:8])
			seq := binary.BigEndian.Uint64(tmpBytes[8:])
			streamID := fmt.Sprintf("%v-%v", ms, seq)

			/* Load deliveryTime */
			tmpBytes = r.ReadBytesP(8)
			// @TODO big endian
			deliveryTime := binary.LittleEndian.Uint64(tmpBytes)
			/* Load deliveryCount */
			deliveryCount := r.ReadLength64P()

			/* Save deliveryTime and deliveryCount  */
			streamNackTime[streamID] = deliveryTime
			streamNackCount[streamID] = deliveryCount
		}

		/* Generate XCLAIMs for each consumer that happens to
		 * have pending entries. Empty consumers are discarded. */
		numConsumer := r.ReadLength64P()
		for i := uint64(0); i < numConsumer; i++ {
			/* For the current consumer, iterate all the PEL entries
			 * to emit the XCLAIM protocol. */

			consumerName := r.ReadStringP()

			// consumer->seen_time
			_ = r.ReadUint64P()

			if sp.rtype >= RdbTypeStreamListPacks3 {
				_ = r.ReadUint64P() // consumer->active_time
			}

			/* Consumer PEL */
			pelSize := r.ReadLength64P()
			for i := uint64(0); i < pelSize; i++ {
				// stream ID
				tmpBytes := r.ReadBytesP(16)
				ms := binary.BigEndian.Uint64(tmpBytes[:8])
				seq := binary.BigEndian.Uint64(tmpBytes[8:])
				streamID := fmt.Sprintf("%d-%d", ms, seq)

				panicIfErr(cb("XCLAIM", sp.key, groupName, consumerName,
					"0", streamID, "TIME", streamNackTime[streamID],
					"RETRYCOUNT", streamNackCount[streamID], "JUSTID", "FORCE"))
			}
		}
	}
}

// module
type ModuleParser struct {
	BaseParser
	id   uint64
	name string
}

func (mp *ModuleParser) ReadBuffer(lr *Loader) {
	mp.readBufferBegin(lr)
	r := NewRdbReader(io.TeeReader(lr, &mp.buf))

	// RdbTypeModule2, RdbTypeModule
	if mp.rtype == RdbTypeModule {
		panic(errors.New("does not support module type 1"))
	}

	// skip 64 bit
	moduleId, err := r.ReadLength64()
	panicIfErr(err)

	moduleName := moduleTypeNameByID(moduleId)

	mp.id = moduleId
	mp.name = moduleName
	//
	// parse modules
	log.Errorf("module parser error : %s", moduleName)
	mp.readBufferEnd(lr)
}

func (mp *ModuleParser) ExecCmd(cb RdbObjExecutor) {
	log.Warnf("unsupported module : id(%d), name(%s)", mp.id, mp.name)
}

// function
type FunctionParser struct {
	BaseParser
}

func (fp *FunctionParser) ReadBuffer(lr *Loader) {
	fp.totalEntries = lr.totalEntries
	fp.readEntries = lr.readEntries
	fp.historyEntries = lr.readEntries
	fp.forceExecCmd = true

	r := NewRdbReader(io.TeeReader(lr, &fp.buf))

	r.ReadStringP()
	fp.readBufferEnd(lr)
}

func (fp *FunctionParser) ExecCmd(cb RdbObjExecutor) {
	if util.VersionGE(fp.targetRedisVersion, "7", util.VersionMajor) {
		val := fp.CreateValueDump()

		if config.Get().Output.FunctionExists == "flush" {
			panicIfErr(cb("FUNCTION", "RESTORE", val, "FLUSH"))
		} else if config.Get().Output.FunctionExists == "append" {
			panicIfErr(cb("FUNCTION", "RESTORE", val))
		} else {
			panicIfErr(cb("FUNCTION", "RESTORE", val, "REPLACE"))
		}
	} else {
		log.Warnf("redis(%s) does not support function command", fp.targetRedisVersion)
	}
}

// aux
// redis, rdb.c:rdbSaveInfoAuxFields,rdbSaveModulesAux
type AuxParser struct {
	BaseParser
}

func (fp *AuxParser) ReadBuffer(lr *Loader) {
	fp.readBufferBegin(lr)
	r := NewRdbReader(io.TeeReader(lr, &fp.buf))
	fp.val = r.ReadStringP()
	fp.readBufferEnd(lr)
}

func (fp *AuxParser) ExecCmd(cb RdbObjExecutor) {
	if bytes.Equal(fp.key, []byte("lua")) {
		panicIfErr(cb("script", "load", fp.val))
	} else {
		log.Warnf("unsupported aux type : key(%s), value(%s)", fp.key, fp.val)
	}
}
