package rdb

// var RdbVersion int64 = 10 // redis:7.0.0
var RdbVersion int64 = 11 // redis:7.2.0

const (
	RdbObjectString   = iota
	RdbObjectList     = iota
	RdbObjectSet      = iota
	RdbObjectZSet     = iota
	RdbObjectHash     = iota
	RdbObjectStream   = iota
	RdbObjectModule   = iota
	RdbObjectFunction = iota
	RdbObjectAux      = iota
)

var (
	rdbObjectMap = []string{"string", "list", "set", "zset", "hash", "stream", "module", "function", "aux"}
)

func RdbObjectTypeToString(tp int) string {
	return rdbObjectMap[tp]
}

const (
	RdbTypeString  = 0
	RdbTypeList    = 1
	RdbTypeSet     = 2
	RdbTypeZSet    = 3
	RdbTypeHash    = 4
	RdbTypeZSet2   = 5
	RdbTypeModule  = 6
	RdbTypeModule2 = 7

	RdbTypeHashZipmap      = 9
	RdbTypeListZiplist     = 10
	RdbTypeSetIntset       = 11
	RdbTypeZSetZiplist     = 12
	RdbTypeHashZiplist     = 13
	RdbTypeQuicklist       = 14
	RDBTypeStreamListPacks = 15 // stream
	RdbTypeHashListpack    = 16
	RdbTypeZSetListpack    = 17

	RdbTypeQuicklist2       = 18
	RDBTypeStreamListPacks2 = 19 // stream

	RdbTypeSetListpack      = 20
	RdbTypeStreamListPacks3 = 21 // RDB_TYPE_STREAM_LISTPACKS_3

	RdbTypeFunction2 = 0xf5
	RdbTypeFunction  = 0xf6
	RdbFlagModuleAux = 0xf7
	RdbFlagIdle      = 0xf8
	RdbFlagFreq      = 0xf9
	RdbFlagAUX       = 0xfa
	RdbFlagResizeDB  = 0xfb
	RdbFlagExpiryMS  = 0xfc
	RdbFlagExpiry    = 0xfd
	RdbFlagSelectDB  = 0xfe
	RdbFlagEOF       = 0xff

	// Module serialized values sub opcodes
	rdbModuleOpcodeEof    = 0 // End of module value.
	rdbModuleOpcodeSint   = 1 // Signed integer.
	rdbModuleOpcodeUint   = 2 // Unsigned integer.
	rdbModuleOpcodeFloat  = 3 // Float.
	rdbModuleOpcodeDouble = 4 // Double.
	rdbModuleOpcodeString = 5 // String.

	moduleTypeNameCharSet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
)

const (
	rdb6bitLen  = 0
	rdb14bitLen = 1
	rdb32bitLen = 0x80
	rdb64bitLen = 0x81
	rdbEncVal   = 3

	rdbEncInt8  = 0
	rdbEncInt16 = 1
	rdbEncInt32 = 2
	rdbEncLZF   = 3
)
