package common

import (
	"fmt"
	"strings"
)

const (
	KrespOK          = 0
	KrespMove        = 1
	KrespAsk         = 2
	KrespConnTimeout = 3
	KrespError       = 4
	KrespCrossSlot   = 5
)

func CheckReply(reply interface{}) int {
	if _, ok := reply.(RedisError); !ok {
		return KrespOK
	}

	errMsg := reply.(RedisError).Error()

	if len(errMsg) >= 3 && string(errMsg[:3]) == "ASK" {
		return KrespAsk
	}

	if len(errMsg) >= 5 && string(errMsg[:5]) == "MOVED" {
		return KrespMove
	}

	if len(errMsg) >= 12 && string(errMsg[:12]) == "ECONNTIMEOUT" {
		return KrespConnTimeout
	}
	if len(errMsg) > 9 && string(errMsg) == "CROSSSLOT" {
		return KrespCrossSlot
	}
	return KrespError
}

func HandleReply(reply interface{}) (interface{}, error) {
	resp := CheckReply(reply)
	switch resp {
	case KrespOK, KrespError:
		return reply, nil
	case KrespMove:
		return nil, ErrMove
	case KrespAsk:
		return nil, ErrAsk
	case KrespConnTimeout:
		return nil, ErrConnTimeout
	case KrespCrossSlot:
		return nil, ErrCrossSlots
	}
	return nil, ErrUnknown
}

func CheckReplyError(reply interface{}) error {
	switch v := reply.(type) {
	case RedisError:
		return v
	case error:
		return v
	case []interface{}:
		for i, nested := range v {
			if err := CheckReplyError(nested); err != nil {
				return fmt.Errorf("[%d]: %w", i, err)
			}
		}
	}
	return nil
}

func CheckRepliesError(replies []interface{}) error {
	for i, reply := range replies {
		if err := CheckReplyError(reply); err != nil {
			return fmt.Errorf("reply[%d]: %w", i, err)
		}
	}
	return nil
}

func CheckTxnRepliesError(replies []interface{}, queued int) error {
	if len(replies) != queued+2 {
		return fmt.Errorf("unexpected txn reply count: got=%d want=%d", len(replies), queued+2)
	}
	if ok, err := String(replies[0], nil); err != nil || !strings.EqualFold(ok, ReplyOk) {
		return fmt.Errorf("unexpected MULTI reply: %v %w", replies[0], err)
	}
	for i := 1; i <= queued; i++ {
		queuedReply, err := String(replies[i], nil)
		if err != nil || !strings.EqualFold(queuedReply, "QUEUED") {
			return fmt.Errorf("unexpected QUEUED reply at index=%d: %v %w", i, replies[i], err)
		}
	}

	execReply, ok := replies[len(replies)-1].([]interface{})
	if !ok {
		if redisErr, ok := replies[len(replies)-1].(error); ok {
			return fmt.Errorf("transaction exec reply error: %w", redisErr)
		}
		return fmt.Errorf("unexpected EXEC reply type: %T value=%v", replies[len(replies)-1], replies[len(replies)-1])
	}
	if len(execReply) != queued {
		return fmt.Errorf("unexpected EXEC inner reply count: got=%d want=%d", len(execReply), queued)
	}
	for i, reply := range execReply {
		if err := CheckReplyError(reply); err != nil {
			return fmt.Errorf("exec[%d]: %w", i, err)
		}
	}
	return nil
}
