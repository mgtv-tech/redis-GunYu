package net

import (
	"errors"
	"io"
	"net"
)

func CheckHandleNetError(err error) bool {
	if errors.Is(err, io.EOF) {
		return true
	}
	for {
		if err == nil {
			return false
		}
		if _, ok := err.(net.Error); ok {
			return true
		}
		err = errors.Unwrap(err)
	}
}
