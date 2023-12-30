package checkpoint

import "strconv"

/*
checkpoint_hash :
	values : [runid:cp_name, runid:cp_name, ...]
	DB : 0 (stored in DB 0)
cp_name :
	key : $prefix_mmm(hash to specify shard) or $prefix
	values : [$runid_offset:xx, $runid_runid:yy, $runid_version:zz, $runid_uptime]
	DB : any
*/

type CheckpointInfo struct {
	Key     string
	RunId   string
	Version string
	Offset  int64
	Mtime   int64
}

const (
	CheckpointOffsetSuffix  = "_offset"
	CheckpointRunIdSuffix   = "_runid"
	CheckpointVersionSuffix = "_version"
	CheckpointMtimeSuffix   = "_mtime"
)

func (cp *CheckpointInfo) KeyValus() []string {
	return []string{cp.RunId + CheckpointRunIdSuffix, cp.RunId,
		cp.RunId + CheckpointVersionSuffix, cp.Version,
		cp.RunId + CheckpointOffsetSuffix, strconv.FormatInt(cp.Offset, 10),
		cp.RunId + CheckpointMtimeSuffix, strconv.FormatInt(cp.Mtime, 10)}
}

func (cp *CheckpointInfo) MTimeKey() string {
	return cp.RunId + CheckpointMtimeSuffix
}

func (cp *CheckpointInfo) RunIdKey() string {
	return cp.RunId + CheckpointRunIdSuffix
}

func (cp *CheckpointInfo) VersionKey() string {
	return cp.RunId + CheckpointVersionSuffix
}

func (cp *CheckpointInfo) OffsetKey() string {
	return cp.RunId + CheckpointOffsetSuffix
}
