package sync

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// WaitCloser is convenient for concurrent programming
// used to control lifetime of concurrent executions.
type WaitCloser interface {
	Close(error) bool
	IsClosed() bool
	Error() error

	Context() context.Context
	Done() WaitChannel

	// sleep until expired or closed
	Sleep(time.Duration)

	// wait group interface
	WgAdd(int)
	WgDone()
	WgWait()
}

type waitCloser struct {
	sync.RWMutex
	ctx       context.Context
	cancel    context.CancelFunc
	closed    atomic.Bool
	stopFun   func(error)
	err       error
	p         WaitCloser
	waitGroup sync.WaitGroup
}

type WaitChannel <-chan struct{}

func NewWaitChannel() WaitChannel {
	return make(chan struct{})
}

func NewWaitCloser(stopFun func(error)) WaitCloser {
	ctx, cancel := context.WithCancel(context.Background())
	return &waitCloser{
		//wait:    make(chan struct{}),
		stopFun: stopFun,
		ctx:     ctx,
		cancel:  cancel,
	}
}

func NewWaitCloserFromParent(p WaitCloser, stopFun func(error)) WaitCloser {
	ctx, cancel := context.WithCancel(p.Context())
	wc := &waitCloser{
		ctx:     ctx,
		cancel:  cancel,
		stopFun: stopFun,
		p:       p,
	}

	SafeGo(func() {
		for {
			select {
			case <-p.Done():
				wc.Close(p.Error())
			case <-wc.Done():
			}
		}
	}, nil)

	return wc
}

func NewWaitCloserFromContext(pctx context.Context, stopFun func(error)) WaitCloser {
	ctx, cancel := context.WithCancel(pctx)
	wc := &waitCloser{
		ctx:     ctx,
		cancel:  cancel,
		stopFun: stopFun,
	}

	SafeGo(func() {
		select {
		case <-pctx.Done():
			wc.Close(pctx.Err())
		case <-wc.Done():
		}
	}, nil)

	return wc
}

func (w *waitCloser) IsClosed() bool {
	return w.closed.Load()
}

func (w *waitCloser) WgAdd(delta int) {
	w.waitGroup.Add(delta)
}

func (w *waitCloser) WgDone() {
	w.waitGroup.Done()
}

func (w *waitCloser) WgWait() {
	w.waitGroup.Wait()
}

func (w *waitCloser) Sleep(d time.Duration) {
	timer := time.NewTimer(d)
	select {
	case <-w.ctx.Done():
		if !timer.Stop() {
			<-timer.C
		}
	case <-timer.C:
	}
}

func (w *waitCloser) Done() WaitChannel {
	return w.ctx.Done()
}

func (w *waitCloser) Error() (err error) {
	w.RLock()
	err = w.err
	w.RUnlock()
	return
}

func (w *waitCloser) Close(err error) bool {
	if w.closed.CompareAndSwap(false, true) {
		w.err = err
		w.cancel()
		if w.stopFun != nil {
			w.stopFun(err)
		}
		return true
	}

	return false
}

func (w *waitCloser) Context() context.Context {
	return w.ctx
}
