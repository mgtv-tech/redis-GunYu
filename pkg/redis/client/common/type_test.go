package common

import "testing"

func TestStringMapAcceptsStringValues(t *testing.T) {
	reply := []interface{}{"field1", "value1", "field2", "value2"}

	got, err := StringMap(reply, nil)
	if err != nil {
		t.Fatalf("StringMap returned error: %v", err)
	}

	if got["field1"] != "value1" || got["field2"] != "value2" {
		t.Fatalf("unexpected map contents: %#v", got)
	}
}

func TestStringMapAcceptsByteValues(t *testing.T) {
	reply := []interface{}{[]byte("field1"), []byte("value1")}

	got, err := StringMap(reply, nil)
	if err != nil {
		t.Fatalf("StringMap returned error: %v", err)
	}

	if got["field1"] != "value1" {
		t.Fatalf("unexpected map contents: %#v", got)
	}
}
