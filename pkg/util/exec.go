package util

type OpenCircuitExec struct {
	err error
}

func (sce *OpenCircuitExec) Do(f func() error) error {
	if sce.err != nil {
		return sce.err
	}
	sce.err = f()
	return sce.err
}
