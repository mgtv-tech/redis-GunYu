package main

import "testing"

func TestRewriteErrorPayload(t *testing.T) {
	mapping := map[string]string{
		"127.0.0.1:7000": "127.0.0.1:17000",
	}
	got := rewriteErrorPayload("MOVED 1234 127.0.0.1:7000", mapping)
	if got != "MOVED 1234 127.0.0.1:17000" {
		t.Fatalf("unexpected MOVED rewrite: %q", got)
	}
}

func TestRewriteClusterSlots(t *testing.T) {
	mapping := map[string]string{
		"127.0.0.1:7000": "127.0.0.1:17000",
		"127.0.0.1:7001": "127.0.0.1:17001",
	}
	input := respArray{
		Values: []respValue{
			respArray{
				Values: []respValue{
					respInteger{Value: 0},
					respInteger{Value: 8191},
					respArray{Values: []respValue{
						respBulkString{Value: []byte("127.0.0.1")},
						respInteger{Value: 7000},
						respBulkString{Value: []byte("node-a")},
					}},
					respArray{Values: []respValue{
						respBulkString{Value: []byte("127.0.0.1")},
						respInteger{Value: 7001},
						respBulkString{Value: []byte("node-b")},
					}},
				},
			},
		},
	}

	got := rewriteRESP(input, mapping).(respArray)
	slot := got.Values[0].(respArray)
	first := slot.Values[2].(respArray)
	second := slot.Values[3].(respArray)

	host, _ := respString(first.Values[0])
	port, _ := respInt(first.Values[1])
	if host != "127.0.0.1" || port != 17000 {
		t.Fatalf("unexpected first node rewrite: host=%s port=%d", host, port)
	}

	host, _ = respString(second.Values[0])
	port, _ = respInt(second.Values[1])
	if host != "127.0.0.1" || port != 17001 {
		t.Fatalf("unexpected second node rewrite: host=%s port=%d", host, port)
	}
}
