package keyspec

import (
	"reflect"
	"testing"
)

func TestCommandKeysModuleMergeUsesDestinationOnly(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		cmd  string
		args [][]byte
		want []string
	}{
		{
			name: "cms.merge",
			cmd:  "cms.merge",
			args: [][]byte{[]byte("dst{t}"), []byte("2"), []byte("src1{t}"), []byte("src2{t}")},
			want: []string{"dst{t}"},
		},
		{
			name: "tdigest.merge",
			cmd:  "tdigest.merge",
			args: [][]byte{[]byte("dst{t}"), []byte("2"), []byte("src1{t}"), []byte("src2{t}")},
			want: []string{"dst{t}"},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := CommandKeys(tc.cmd, tc.args)
			if !ok {
				t.Fatalf("CommandKeys(%s) reported unsupported", tc.cmd)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("CommandKeys(%s)=%v, want %v", tc.cmd, got, tc.want)
			}
		})
	}
}
