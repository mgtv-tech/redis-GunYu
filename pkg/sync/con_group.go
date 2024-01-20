package sync

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"golang.org/x/sync/semaphore"
)

var (
	ErrExceedLimit error = errors.New("exceed limit")
)

type GoFunc func(context.Context) error

// Group execute group
type Group interface {
	// cancel all execution unit
	Cancel()

	// Wait wait all execution unit to quit
	Wait() error

	// Go start an execution unit, would be blocked when exceed limitation
	Go(GoFunc)

	// TryGo if exceeds limitation, return ErrExceedLimit
	TryGo(GoFunc) error

	//
	Errors() []error

	//
	WrapError() error
}

func NewGroup(ctx context.Context, opts ...GroupOption) Group {
	return newGroup(nil, ctx, opts...)
}

type GroupOption interface {
	apply(*groupOption)
}

// WithCancelIfError cancel all executions when one return error
func WithCancelIfError(enable bool) GroupOption {
	return newGroupOption(func(o *groupOption) {
		o.cancelIfError = enable
	})
}

type ConGroup interface {
	NewGroup(ctx context.Context, opts ...GroupOption) Group
}

func NewConGroup(maxConcurrency int) ConGroup {
	return &conGroup{
		sem: semaphore.NewWeighted(int64(maxConcurrency)),
	}
}

type conGroup struct {
	sem *semaphore.Weighted
}

func (cg *conGroup) NewGroup(ctx context.Context, opts ...GroupOption) Group {
	return newGroup(cg, ctx, opts...)
}

func newGroup(cg *conGroup, ctx context.Context, opts ...GroupOption) Group {
	ctx2, cancel := context.WithCancel(ctx)

	g := &group{
		p:      cg,
		ctx:    ctx2,
		cancel: cancel,
	}
	for _, opt := range opts {
		opt.apply(&g.opts)
	}
	return g
}

type groupOption struct {
	cancelIfError bool
}

type funcGroupOption struct {
	f func(*groupOption)
}

func (fdo *funcGroupOption) apply(do *groupOption) {
	fdo.f(do)
}

func newGroupOption(f func(*groupOption)) *funcGroupOption {
	return &funcGroupOption{
		f: f,
	}
}

type group struct {
	p       *conGroup
	ctx     context.Context
	cancel  context.CancelFunc
	wait    sync.WaitGroup
	errs    []error
	mu      sync.Mutex
	errOnce sync.Once
	opts    groupOption
}

func (g *group) Cancel() {
	g.cancel()
}

func (g *group) Wait() error {
	g.wait.Wait()
	g.errOnce.Do(func() {
		g.cancel()
	})
	return g.WrapError()
}

func (g *group) Go(f GoFunc) {
	if g.p != nil {
		if err := g.p.sem.Acquire(g.ctx, 1); err != nil {
			g.appendErr(err)
			return
		}
	}

	g.mgo(f)
}

func (g *group) TryGo(f GoFunc) error {
	if g.p != nil && !g.p.sem.TryAcquire(1) {
		g.appendErr(ErrExceedLimit)
		return ErrExceedLimit
	}

	g.mgo(f)
	return nil
}

func (g *group) mgo(f GoFunc) {
	g.wait.Add(1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				g.appendErr(fmt.Errorf("panic : %v", r))
			}

			// TODO(ken) disastrous if throw a panic
			if g.p != nil {
				g.p.sem.Release(1)
			}
			g.wait.Done()
		}()

		err := f(g.ctx)
		if err == nil {
			return
		}
		g.appendErr(err)
		if g.opts.cancelIfError {
			g.errOnce.Do(func() {
				g.cancel()
			})
		}
	}()
}

func (g *group) appendErr(err error) {
	g.mu.Lock()
	g.errs = append(g.errs, err)
	g.mu.Unlock()
}

func (g *group) Errors() []error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.errs
}

func (g *group) WrapError() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.errs) == 0 {
		return nil
	}
	return errors.Join(g.errs...)
}
