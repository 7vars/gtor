package rx

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/google/uuid"
)

type SourceStage interface {
	Connect(SinkStage) error
	Via(FlowStage) SourceStage
	To(SinkStage) error
	RunWith(SinkStage) Runnable
}

type SourceHandler interface {
	HandlePull(Outlet)
	HandleCancel(Outlet)
}

type Source struct {
	OnPull   func(Outlet)
	OnCancel func(Outlet)
}

func (src Source) HandlePull(o Outlet) {
	if src.OnPull != nil {
		src.OnPull(o)
		return
	}
	o.Complete()
}

func (src Source) HandleCancel(o Outlet) {
	if src.OnCancel != nil {
		src.OnCancel(o)
		return
	}
	o.Complete()
}

type sourceStage struct {
	handler SourceHandler
	pipe    Pipe
}

func NewSource(handler SourceHandler) SourceStage {
	return &sourceStage{
		handler: handler,
	}
}

func sourceWorker(handler SourceHandler, outline Outline) {
	defer fmt.Println("DEBUG SOURCE-WORK CLOSED")
	for cmd := range outline.Commands() {
		switch cmd {
		case PULL:
			handler.HandlePull(outline)
		case CANCEL:
			handler.HandleCancel(outline)
			return
		}
	}
}

func (src *sourceStage) connect(sin SinkStage, createPipe func() Pipe) error {
	if src.pipe != nil {
		return errors.New("source is already connected")
	}
	src.pipe = createPipe()

	if err := sin.Connected(src.pipe); err != nil {
		src.pipe = nil
		return err
	}

	go sourceWorker(src.handler, src.pipe)
	return nil
}

func (src *sourceStage) Connect(sin SinkStage) error {
	return src.connect(sin, newPipe)
}

func (src *sourceStage) Via(flow FlowStage) SourceStage {
	if err := src.Connect(flow); err != nil {
		errSrc := errorSource(err)
		return errSrc.Via(flow)
	}
	return flow
}

func (src *sourceStage) To(sink SinkStage) error {
	if err := src.Connect(sink); err != nil {
		return err
	}
	src.pipe.Pull()
	return nil
}

func (src *sourceStage) RunWith(sink SinkStage) Runnable {
	pipe := newPipe()
	runnable := newRunnable(pipe)
	pipe.SetEmitter(runnable)

	if err := src.connect(sink, func() Pipe { return pipe }); err != nil {
		return runnableWithError{err}
	}

	return runnable
}

// ===== sources =====

func errorSource(err error) SourceStage {
	return NewSource(Source{
		OnPull: func(o Outlet) {
			o.Error(err)
		},
	})
}

func SliceSource[T any](slice []T) SourceStage {
	var index int64
	return NewSource(Source{
		OnPull: func(o Outlet) {
			defer atomic.AddInt64(&index, 1)
			if idx := int(atomic.LoadInt64(&index)); idx < len(slice) {
				o.Push(slice[idx])
				return
			}
			o.Complete()
		},
	})
}

func UUIDSource() SourceStage {
	return NewSource(Source{
		OnPull: func(o Outlet) {
			o.Push(uuid.New().String())
		},
	})
}

func CountSource(start int64, inc int64, end int64) SourceStage {
	cur := start
	return NewSource(Source{
		OnPull: func(o Outlet) {
			defer atomic.AddInt64(&cur, inc)
			if c := atomic.LoadInt64(&cur); c < end {
				o.Push(c)
				return
			}
			o.Complete()
		},
	})
}

func IncSource() SourceStage {
	return CountSource(0, 1, int64(^uint64(0)>>1))
}
