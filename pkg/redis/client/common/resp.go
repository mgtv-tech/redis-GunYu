package common

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
