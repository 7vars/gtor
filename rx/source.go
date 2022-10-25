package rx

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/google/uuid"
)

type Stage interface {
	start()
}

type SourceStage interface {
	Stage
	connect(SinkStage)
}

type Runner interface {
	Run()
}

type Runnable interface {
	Run() <-chan interface{}
	RunWithContext(context.Context) <-chan interface{}
	Execute() (interface{}, error)
	ExecuteWithContext(context.Context) (interface{}, error)
}

type SourceGraph interface {
	SourceStage
	Via(FlowStage) SourceGraph
	To(EmitterSinkStage) Runner
	RunWith(EmitterSinkStage) Runnable
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

type composedSourceStage struct {
	parent Stage
	source SourceStage
}

func composeSourceStage(parent Stage, source SourceStage) SourceGraph {
	return &composedSourceStage{
		parent: parent,
		source: source,
	}
}

func (css *composedSourceStage) start() {
	css.parent.start()
	css.source.start()
}

func (css *composedSourceStage) connect(sink SinkStage) {}

func (css *composedSourceStage) Via(flow FlowStage) SourceGraph {
	css.source.connect(flow)
	return composeSourceStage(css, flow)
}

func (css *composedSourceStage) To(sink EmitterSinkStage) Runner {
	css.source.connect(sink)
	return newRunner(sink, func() {
		sink.start()
		css.start()
	})
}

func (css *composedSourceStage) RunWith(sink EmitterSinkStage) Runnable {
	css.source.connect(sink)
	return newRunnable(sink, func() {
		sink.start()
		css.start()
	})
}

type sourceStage struct {
	handler SourceHandler
	pipe    Pipe
}

func NewSource(handler SourceHandler) SourceGraph {
	return &sourceStage{
		handler: handler,
	}
}

func sourceWorker(handler SourceHandler, outline Outline) {
	fmt.Println("DEBUG SOURCE-WORK STARTED")
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

func (src *sourceStage) start() {
	go sourceWorker(src.handler, src.pipe)
}

func (src *sourceStage) connect(sin SinkStage) {
	if src.pipe != nil {
		// TODO autocreate FanOut / Broadcast
		panic("source is already connected")
	}
	src.pipe = newPipe()

	sin.Connected(src.pipe)
}

func (src *sourceStage) Via(flow FlowStage) SourceGraph {
	src.connect(flow)
	return composeSourceStage(src, flow)
}

func (src *sourceStage) To(sink EmitterSinkStage) Runner {
	src.connect(sink)
	return newRunner(sink, func() {
		sink.start()
		src.start()
	})
}

func (src *sourceStage) RunWith(sink EmitterSinkStage) Runnable {
	src.connect(sink)
	return newRunnable(sink, func() {
		sink.start()
		src.start()
	})
}

// ===== sources =====

func SourceFunc[T any](f func() (T, error)) SourceGraph {
	return NewSource(Source{
		OnPull: func(o Outlet) {
			v, err := f()
			if err != nil {
				if errors.Is(err, io.EOF) {
					o.Complete()
					return
				}
				o.Error(err)
				return
			}
			o.Push(v)
		},
	})
}

func SliceSource[T any](slice []T) SourceGraph {
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

func UUIDSource() SourceGraph {
	return NewSource(Source{
		OnPull: func(o Outlet) {
			o.Push(uuid.New().String())
		},
	})
}

func CountSource(start int64, inc int64, end int64) SourceGraph {
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

func IncSource() SourceGraph {
	return CountSource(0, 1, int64(^uint64(0)>>1))
}
