package util

import (
	"errors"
	"fmt"
)

func PanicIfErr(err error, appendErrs ...error) {
	if err != nil {
		if len(appendErrs) > 0 {
			xerr := errors.Join(appendErrs...)
			panic(errors.Join(err, xerr))
		}
		panic(errors.Join(err))
	}
}

func Xrecover(err *error, appendErrs ...error) {
	if x := recover(); x != nil {
		switch xt := x.(type) {
		case error:
			*err = fmt.Errorf("panic : %w", xt)
		default:
			*err = fmt.Errorf("panic : %v", x)
		}
		if len(appendErrs) > 0 {
			errx := errors.Join(appendErrs...)
			*err = errors.Join(*err, errx)
		}
	}
}
