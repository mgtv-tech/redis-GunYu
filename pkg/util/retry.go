package util

import (
	"context"
	"math/rand"
	"time"
)

func Retry(f func() error, times int) (err error) {
	for i := 0; i < times; i++ {
		err = f()
		if err == nil {
			return
		}
	}
	return
}

// waitBetween=1s and jitter=0.10 can generate waits between 900ms and 1100ms.
func RetryLinearJitter(ctx context.Context, f func() error, times int, waitBetween time.Duration, jitterFraction float64) (err error) {
	for i := 0; i < times; i++ {
		err = f()
		if err == nil || err == context.Canceled || err == context.DeadlineExceeded {
			return
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		waitTime := jitterUp(waitBetween, jitterFraction)
		if waitTime > 0 {
			t := time.NewTimer(waitTime)
			select {
			case <-ctx.Done():
				if !t.Stop() {
					<-t.C
				}
				return ctx.Err()
			case <-t.C:
			}
		}
	}
	return
}

func jitterUp(duration time.Duration, jitter float64) time.Duration {
	multiplier := jitter * (rand.Float64()*2 - 1)
	return time.Duration(float64(duration) * (1 + multiplier))
}

// The scalar is multiplied times 2 raised to the current attempt.
func RetryExponential(ctx context.Context, f func() error, times uint, scalar time.Duration) (err error) {
	for i := uint(0); i < times; i++ {
		err = f()
		if err == nil || err == context.Canceled || err == context.DeadlineExceeded {
			return
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		waitTime := backoffExponential(scalar, i)
		if waitTime > 0 {
			t := time.NewTimer(waitTime)
			select {
			case <-ctx.Done():
				if !t.Stop() {
					<-t.C
				}
				return ctx.Err()
			case <-t.C:
			}
		}
	}
	return
}

func backoffExponential(scalar time.Duration, attempt uint) time.Duration {
	return scalar * time.Duration(exponentBase2(attempt))
}

// exponentBase2 computes 2^(a-1) where a >= 1. If a is 0, the result is 0.
func exponentBase2(a uint) uint {
	return (1 << a) >> 1
}
