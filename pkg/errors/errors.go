package errors

import (
	"fmt"

	"github.com/ikenchina/redis-GunYu/pkg/util"
)

type TracedError struct {
	Stack util.FuncStack
	Cause error
}

func (e *TracedError) Error() string {
	return fmt.Sprintf("err(%s), stack(%s)", e.Cause.Error(), e.Stack.StringOneLine())
}

func (e *TracedError) Unwrap() error {
	return e.Cause
}

func WithStack(err error) error {
	if err == nil {
		return nil
	}
	return &TracedError{
		Stack: util.GetCallerStack(1, 3),
		Cause: err,
	}
}

func Errorf(f string, args ...interface{}) error {
	return WithStack(fmt.Errorf(f, args...))
}
