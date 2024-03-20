// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package pipe

import "github.com/mgtv-tech/redis-GunYu/pkg/log"

func Must(b bool) {
	if b {
		return
	}
	log.Panic("assertion failed")
}

func MustNoError(err error) {
	if err == nil {
		return
	}
	log.PanicError(err, "error happens, assertion failed")
}
