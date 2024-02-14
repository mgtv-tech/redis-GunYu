package common

import "errors"

var (
	ErrMove        = errors.New("move")
	ErrAsk         = errors.New("ask")
	ErrConnTimeout = errors.New("conn timeout")
	ErrUnknown     = errors.New("unknown")
	ErrCrossSlots  = errors.New("cross slots")
)

// redis resp protocol data type.
const (
	RespStatus    = '+' // +<string>\r\n
	RespError     = '-' // -<string>\r\n
	RespString    = '$' // $<length>\r\n<bytes>\r\n
	RespInt       = ':' // :<number>\r\n
	RespNil       = '_' // _\r\n
	RespFloat     = ',' // ,<floating-point-number>\r\n (golang float)
	RespBool      = '#' // true: #t\r\n false: #f\r\n
	RespBlobError = '!' // !<length>\r\n<bytes>\r\n
	RespVerbatim  = '=' // =<length>\r\nFORMAT:<bytes>\r\n
	RespBigInt    = '(' // (<big number>\r\n
	RespArray     = '*' // *<len>\r\n... (same as resp2)
	RespMap       = '%' // %<len>\r\n(key)\r\n(value)\r\n... (golang map)
	RespSet       = '~' // ~<len>\r\n... (same as Array)
	RespAttr      = '|' // |<len>\r\n(key)\r\n(value)\r\n... + command reply
	RespPush      = '>' // ><len>\r\n... (same as Array)
)

// IsNilReply detects redis.Nil of RESP2.
func IsNilReply(line []byte) bool {
	return len(line) == 3 &&
		(line[0] == RespString || line[0] == RespArray) &&
		line[1] == '-' && line[2] == '1'
}
