package syncer

import (
	"errors"
	"fmt"
	"testing"
)

func TestClassifyErrorDetail(t *testing.T) {
	cases := []struct {
		name         string
		err          error
		wantAction   ErrorAction
		wantReason   ErrorReason
		wantActionS  string
	}{
		{
			name:        "nil error means retry",
			err:         nil,
			wantAction:  ErrorActionRetry,
			wantReason:  ErrorReasonUnknown,
			wantActionS: "retry",
		},
		{
			name:        "stop sync exits",
			err:         ErrStopSync,
			wantAction:  ErrorActionExit,
			wantReason:  ErrorReasonStopSync,
			wantActionS: "exit",
		},
		{
			name:        "quit exits",
			err:         ErrQuit,
			wantAction:  ErrorActionExit,
			wantReason:  ErrorReasonQuit,
			wantActionS: "exit",
		},
		{
			name:        "typology changed is global restart",
			err:         ErrRedisTypologyChanged,
			wantAction:  ErrorActionGlobalRestart,
			wantReason:  ErrorReasonRedisTypologyChange,
			wantActionS: "global_restart",
		},
		{
			name:        "restart is local rebuild",
			err:         ErrRestart,
			wantAction:  ErrorActionLocalRebuild,
			wantReason:  ErrorReasonRestart,
			wantActionS: "local_rebuild",
		},
		{
			name:        "runid stuck is local rebuild with dedicated reason",
			err:         ErrRunIDStuck,
			wantAction:  ErrorActionLocalRebuild,
			wantReason:  ErrorReasonRunIDStuck,
			wantActionS: "local_rebuild",
		},
		{
			name:        "checkpoint fenced is local rebuild with dedicated reason",
			err:         ErrCheckpointFenced,
			wantAction:  ErrorActionLocalRebuild,
			wantReason:  ErrorReasonCheckpointFenced,
			wantActionS: "local_rebuild",
		},
		{
			name:        "role family is local rebuild",
			err:         ErrLeaderTakeover,
			wantAction:  ErrorActionLocalRebuild,
			wantReason:  ErrorReasonRole,
			wantActionS: "local_rebuild",
		},
		{
			name:        "corrupted exits",
			err:         ErrCorrupted,
			wantAction:  ErrorActionExit,
			wantReason:  ErrorReasonCorrupted,
			wantActionS: "exit",
		},
		{
			name:        "break exits",
			err:         ErrBreak,
			wantAction:  ErrorActionExit,
			wantReason:  ErrorReasonBreak,
			wantActionS: "exit",
		},
		{
			name:        "joined error keeps strongest semantic",
			err:         errors.Join(fmt.Errorf("io timeout"), ErrRestart),
			wantAction:  ErrorActionLocalRebuild,
			wantReason:  ErrorReasonRestart,
			wantActionS: "local_rebuild",
		},
		{
			name:        "wrapped role error still classifies",
			err:         fmt.Errorf("wrapped: %w", ErrLeaderHandover),
			wantAction:  ErrorActionLocalRebuild,
			wantReason:  ErrorReasonRole,
			wantActionS: "local_rebuild",
		},
		{
			name:        "unknown error is retry",
			err:         errors.New("random"),
			wantAction:  ErrorActionRetry,
			wantReason:  ErrorReasonUnknown,
			wantActionS: "retry",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotAction, gotReason := ClassifyErrorDetail(tc.err)
			if gotAction != tc.wantAction {
				t.Fatalf("action mismatch: got(%v) want(%v), err(%v)", gotAction, tc.wantAction, tc.err)
			}
			if gotReason != tc.wantReason {
				t.Fatalf("reason mismatch: got(%v) want(%v), err(%v)", gotReason, tc.wantReason, tc.err)
			}
			if gotAction.String() != tc.wantActionS {
				t.Fatalf("action string mismatch: got(%s) want(%s)", gotAction.String(), tc.wantActionS)
			}
			// Keep backward-compat helper aligned with detailed classifier.
			if gotSimple := ClassifyError(tc.err); gotSimple != tc.wantAction {
				t.Fatalf("classify helper mismatch: got(%v) want(%v)", gotSimple, tc.wantAction)
			}
		})
	}
}
