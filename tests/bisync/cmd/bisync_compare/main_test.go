package main

import (
	"bufio"
	"strings"
	"testing"

	"github.com/mgtv-tech/redis-GunYu/config"
	redisclient "github.com/mgtv-tech/redis-GunYu/pkg/redis/client"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
)

type fakeRedis struct {
	do func(cmd string, args ...interface{}) (interface{}, error)
}

func (f fakeRedis) Close() error { return nil }

func (f fakeRedis) Do(cmd string, args ...interface{}) (interface{}, error) {
	if f.do == nil {
		return nil, nil
	}
	return f.do(cmd, args...)
}

func (f fakeRedis) Send(string, ...interface{}) error                                     { return nil }
func (f fakeRedis) SendAndFlush(string, ...interface{}) error                             { return nil }
func (f fakeRedis) Receive() (interface{}, error)                                         { return nil, nil }
func (f fakeRedis) ReceiveString() (string, error)                                        { return "", nil }
func (f fakeRedis) ReceiveBool() (bool, error)                                            { return false, nil }
func (f fakeRedis) BufioReader() *bufio.Reader                                            { return nil }
func (f fakeRedis) BufioWriter() *bufio.Writer                                            { return nil }
func (f fakeRedis) Flush() error                                                          { return nil }
func (f fakeRedis) RedisType() config.RedisType                                           { return config.RedisTypeStandalone }
func (f fakeRedis) Addresses() []string                                                   { return nil }
func (f fakeRedis) NewBatcher(bool) common.CmdBatcher                                     { return nil }
func (f fakeRedis) NewTxnBatcher() common.CmdBatcher                                      { return nil }
func (f fakeRedis) IterateNodes(func(string, interface{}, error), string, ...interface{}) {}

var _ redisclient.Redis = fakeRedis{}

func TestReadKeyStateSupportsStream(t *testing.T) {
	t.Parallel()

	cli := fakeRedis{
		do: func(cmd string, args ...interface{}) (interface{}, error) {
			switch cmd {
			case "type":
				return "stream", nil
			case "xrange":
				return []interface{}{
					[]interface{}{
						[]byte("1-0"),
						[]interface{}{
							[]byte("side"), []byte("left"),
							[]byte("seq"), []byte("7"),
						},
					},
					[]interface{}{
						[]byte("2-0"),
						[]interface{}{
							[]byte("note"), []byte("line\nbreak"),
							[]byte("bin"), []byte{'a', 0, 'b'},
						},
					},
				}, nil
			default:
				t.Fatalf("unexpected command %s %v", cmd, args)
				return nil, nil
			}
		},
	}

	state, err := readKeyState(cli, "stream-key")
	if err != nil {
		t.Fatalf("readKeyState failed: %v", err)
	}

	want := `stream:"1-0"["side"="left","seq"="7"]|"2-0"["note"="line\nbreak","bin"="a\x00b"]`
	if state != want {
		t.Fatalf("unexpected state\nwant: %s\ngot:  %s", want, state)
	}
}

func TestReadKeyStateRejectsMalformedStreamEntry(t *testing.T) {
	t.Parallel()

	cli := fakeRedis{
		do: func(cmd string, args ...interface{}) (interface{}, error) {
			switch cmd {
			case "type":
				return "stream", nil
			case "xrange":
				return []interface{}{
					[]interface{}{
						[]byte("1-0"),
						[]interface{}{
							[]byte("field"),
						},
					},
				}, nil
			default:
				t.Fatalf("unexpected command %s %v", cmd, args)
				return nil, nil
			}
		},
	}

	_, err := readKeyState(cli, "stream-key")
	if err == nil {
		t.Fatalf("expected malformed stream entry to fail")
	}
	if !strings.Contains(err.Error(), "odd field/value list length") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestReadKeyStateQuotesBinaryString(t *testing.T) {
	t.Parallel()

	cli := fakeRedis{
		do: func(cmd string, args ...interface{}) (interface{}, error) {
			switch cmd {
			case "type":
				return "string", nil
			case "get":
				return string([]byte{'a', 0, 'b', '\n'}), nil
			default:
				t.Fatalf("unexpected command %s %v", cmd, args)
				return nil, nil
			}
		},
	}

	state, err := readKeyState(cli, "bin-key")
	if err != nil {
		t.Fatalf("readKeyState failed: %v", err)
	}

	want := `string:"a\x00b\n"`
	if state != want {
		t.Fatalf("unexpected state\nwant: %s\ngot:  %s", want, state)
	}
}
