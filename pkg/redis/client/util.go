package client

import "errors"

var (
	ErrNil       = errors.New("nil returned")
	ErrWrongType = errors.New("wrong type")
)
