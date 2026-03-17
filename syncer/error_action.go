package syncer

import "errors"

// ErrorAction describes how the upper boundary should react to a syncer error.
type ErrorAction int

const (
	// ErrorActionRetry means transient issue, keep current lifecycle and retry in-place.
	ErrorActionRetry ErrorAction = iota
	// ErrorActionLocalRebuild means rebuild current shard/syncer only.
	ErrorActionLocalRebuild
	// ErrorActionGlobalRestart means restart whole run loop and re-evaluate topology/config.
	ErrorActionGlobalRestart
	// ErrorActionExit means stop process/run loop.
	ErrorActionExit
)

// ErrorReason is the normalized reason key for logs/metrics.
type ErrorReason string

const (
	ErrorReasonUnknown             ErrorReason = "unknown"
	ErrorReasonStopSync            ErrorReason = "stop_sync"
	ErrorReasonQuit                ErrorReason = "quit"
	ErrorReasonRedisTypologyChange ErrorReason = "redis_typology_changed"
	ErrorReasonRestart             ErrorReason = "restart"
	ErrorReasonRole                ErrorReason = "role_changed"
	ErrorReasonCorrupted           ErrorReason = "corrupted"
	ErrorReasonBreak               ErrorReason = "break"
	ErrorReasonRunIDStuck          ErrorReason = "runid_stuck"
	ErrorReasonCheckpointFenced    ErrorReason = "checkpoint_fenced"
	// Runtime/local-rebuild specific reasons (non-fatal classification helpers).
	ErrorReasonTopologyRefreshError ErrorReason = "topology_refresh_error"
	ErrorReasonMasterRebind         ErrorReason = "master_rebind"
	ErrorReasonRoleCheckError       ErrorReason = "role_check_error"
	ErrorReasonCampaignError        ErrorReason = "campaign_error"
	ErrorReasonRunIDSwitchToNewID   ErrorReason = "runid_switch_to_newid"
	ErrorReasonTickerRoleNotMaster  ErrorReason = "ticker_role_not_master"
	// Checkpoint-lag observation reasons.
	ErrorReasonRunIDEmpty              ErrorReason = "runid_empty"
	ErrorReasonReadInputCheckpointFail ErrorReason = "read_input_checkpoint_failed"
	ErrorReasonReadOutputCheckpointFail ErrorReason = "read_output_checkpoint_failed"
	ErrorReasonCheckpointNotFound      ErrorReason = "checkpoint_not_found"
	ErrorReasonCheckpointRunIDInvalid  ErrorReason = "checkpoint_runid_invalid"
	ErrorReasonRunIDMismatch           ErrorReason = "runid_mismatch"
	ErrorReasonCheckpointFieldInvalid  ErrorReason = "checkpoint_field_invalid"
	ErrorReasonNegativeLag             ErrorReason = "negative_lag"
	ErrorReasonCheckpointOffsetRollback ErrorReason = "checkpoint_offset_rollback"
)

func (ea ErrorAction) String() string {
	switch ea {
	case ErrorActionRetry:
		return "retry"
	case ErrorActionLocalRebuild:
		return "local_rebuild"
	case ErrorActionGlobalRestart:
		return "global_restart"
	case ErrorActionExit:
		return "exit"
	default:
		return "unknown"
	}
}

// ClassifyError maps current syncer errors to stable lifecycle actions.
// This is intentionally lightweight and keeps existing error types unchanged.
func ClassifyError(err error) ErrorAction {
	action, _ := ClassifyErrorDetail(err)
	return action
}

// ClassifyErrorDetail maps syncer errors to action + normalized reason.
func ClassifyErrorDetail(err error) (ErrorAction, ErrorReason) {
	if err == nil {
		return ErrorActionRetry, ErrorReasonUnknown
	}
	// Explicit stop/quit requests should terminate run loop directly.
	if errors.Is(err, ErrStopSync) {
		return ErrorActionExit, ErrorReasonStopSync
	}
	if errors.Is(err, ErrQuit) {
		return ErrorActionExit, ErrorReasonQuit
	}
	// Topology-wide changes still require a full refresh/restart.
	if errors.Is(err, ErrRedisTypologyChanged) {
		return ErrorActionGlobalRestart, ErrorReasonRedisTypologyChange
	}
	// Restart-level and role-level events should be absorbed by shard-local rebuild.
	if errors.Is(err, ErrRunIDStuck) {
		return ErrorActionLocalRebuild, ErrorReasonRunIDStuck
	}
	if errors.Is(err, ErrCheckpointFenced) {
		return ErrorActionLocalRebuild, ErrorReasonCheckpointFenced
	}
	if errors.Is(err, ErrRestart) {
		return ErrorActionLocalRebuild, ErrorReasonRestart
	}
	if errors.Is(err, ErrRole) {
		return ErrorActionLocalRebuild, ErrorReasonRole
	}
	// Break/corruption are treated as terminal unless caller explicitly overrides.
	if errors.Is(err, ErrCorrupted) {
		return ErrorActionExit, ErrorReasonCorrupted
	}
	if errors.Is(err, ErrBreak) {
		return ErrorActionExit, ErrorReasonBreak
	}
	return ErrorActionRetry, ErrorReasonUnknown
}
