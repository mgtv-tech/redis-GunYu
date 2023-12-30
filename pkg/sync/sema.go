package sync

type Sema struct {
	ch chan struct{}
}

func NewSema(count int) *Sema {
	return &Sema{
		ch: make(chan struct{}, count),
	}
}

func (s *Sema) Signal() {
	s.ch <- struct{}{}
}

func (s *Sema) Wait() {
	<-s.ch
}

func (s *Sema) WaitChan() <-chan struct{} {
	return s.ch
}
