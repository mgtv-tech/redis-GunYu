package util

func AndCondition(execs ...func() bool) bool {
	for _, exec := range execs {
		if !exec() {
			return false
		}
	}
	return true
}
