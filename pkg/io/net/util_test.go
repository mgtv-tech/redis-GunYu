package net

import (
	"errors"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNetError(t *testing.T) {
	var err error
	err = &net.OpError{}
	err = fmt.Errorf("%w", err)
	err = fmt.Errorf("%w", err)
	err = fmt.Errorf("%w", err)
	err = fmt.Errorf("%w", err)

	assert.True(t, CheckHandleNetError(err))
	err = io.EOF
	assert.True(t, CheckHandleNetError(err))
	err = errors.New("not net error")
	err = fmt.Errorf("%w", err)
	err = fmt.Errorf("%w", err)
	assert.False(t, CheckHandleNetError(err))
}
