package syncer

import (
	"testing"

	"github.com/mgtv-tech/redis-GunYu/config"
)

func TestShouldRecordSentAudit(t *testing.T) {
	cases := []struct {
		name string
		ce   cmdExecution
		want bool
	}{
		{
			name: "drop ping",
			ce:   cmdExecution{Cmd: "ping"},
			want: false,
		},
		{
			name: "drop script load",
			ce: cmdExecution{
				Cmd:  "script",
				Args: []interface{}{[]byte("load"), []byte("return 1")},
			},
			want: false,
		},
		{
			name: "drop select zero",
			ce: cmdExecution{
				Cmd:  "select",
				Args: []interface{}{[]byte("0")},
			},
			want: false,
		},
		{
			name: "keep select non-zero",
			ce: cmdExecution{
				Cmd:  "select",
				Args: []interface{}{[]byte("15")},
			},
			want: true,
		},
		{
			name: "keep normal set",
			ce: cmdExecution{
				Cmd:  "set",
				Args: []interface{}{[]byte("k"), []byte("v")},
			},
			want: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldRecordSentAudit(tc.ce); got != tc.want {
				t.Fatalf("shouldRecordSentAudit() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestShouldRecordFilteredAudit(t *testing.T) {
	et := true
	ef := false
	tcfg := &config.AuditConfig{
		EnableRecordFiltered: &et,
	}
	if shouldRecordFilteredAudit(tcfg, "cmd_keys", "parse_aof") {
		t.Fatalf("expected cmd_keys to be dropped by default noise policy")
	}
	if !shouldRecordFilteredAudit(tcfg, "cmd_filtered", "parse_aof") {
		t.Fatalf("expected cmd_filtered to be kept")
	}
	if !shouldRecordFilteredAudit(tcfg, "cmd_keys", "txn_guard") {
		t.Fatalf("expected txn_guard cmd_keys to be kept for troubleshooting")
	}

	tcfg.EnableRecordFiltered = &ef
	if shouldRecordFilteredAudit(tcfg, "cmd_filtered", "parse_aof") {
		t.Fatalf("expected all filtered audit to be disabled when enableRecordFiltered=false")
	}
}

func TestBuildAuditNode(t *testing.T) {
	cfg := config.GetSyncerConfig()
	oldPort := cfg.Server.ListenPort
	oldListen := cfg.Server.Listen
	t.Cleanup(func() {
		cfg.Server.ListenPort = oldPort
		cfg.Server.Listen = oldListen
	})

	cfg.Server.ListenPort = 18012
	cfg.Server.Listen = "127.0.0.1:18012"
	if got := buildAuditNode("host-a"); got != "host-a:18012" {
		t.Fatalf("buildAuditNode with listen port = %q, want %q", got, "host-a:18012")
	}

	cfg.Server.ListenPort = 0
	cfg.Server.Listen = "127.0.0.1:19001"
	if got := buildAuditNode("host-b"); got != "host-b:19001" {
		t.Fatalf("buildAuditNode with listen fallback = %q, want %q", got, "host-b:19001")
	}

	cfg.Server.ListenPort = 0
	cfg.Server.Listen = ""
	if got := buildAuditNode("host-c"); got != "host-c" {
		t.Fatalf("buildAuditNode without port = %q, want %q", got, "host-c")
	}
}
