package util

import (
	"math/rand"
	"sync"
)

type SafeRand struct {
	mu sync.Mutex
	r  *rand.Rand
}

func NewSafeRand(seed int64) *SafeRand {
	return &SafeRand{r: rand.New(rand.NewSource(seed))}
}

func (sr *SafeRand) Seed(seed int64) {
	sr.mu.Lock()
	sr.r.Seed(seed)
	sr.mu.Unlock()
}

func (sr *SafeRand) Int63() int64 {
	sr.mu.Lock()
	v := sr.r.Int63()
	sr.mu.Unlock()
	return v
}

func (sr *SafeRand) Int63n(n int64) int64 {
	sr.mu.Lock()
	v := sr.r.Int63n(n)
	sr.mu.Unlock()
	return v
}

func (sr *SafeRand) Uint32() uint32 {
	sr.mu.Lock()
	v := sr.r.Uint32()
	sr.mu.Unlock()
	return v
}

func (sr *SafeRand) Uint64() uint64 {
	sr.mu.Lock()
	v := sr.r.Uint64()
	sr.mu.Unlock()
	return v
}

func (sr *SafeRand) Intn(n int) int {
	sr.mu.Lock()
	v := sr.r.Intn(n)
	sr.mu.Unlock()
	return v
}

func (sr *SafeRand) Float64() float64 {
	sr.mu.Lock()
	v := sr.r.Float64()
	sr.mu.Unlock()
	return v
}

func (sr *SafeRand) Perm(n int) []int {
	sr.mu.Lock()
	v := sr.r.Perm(n)
	sr.mu.Unlock()
	return v

}
