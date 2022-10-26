package rx

import (
	"context"
	"errors"
	"io"
	"sync/atomic"

	"github.com/google/uuid"
)

type Stage interface {
	start()
}

type SourceStage interface {
	Stage
	connect(SinkStage) Inline
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
	active bool
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
	if !css.active {
		css.active = true
		css.parent.start()
		css.source.start()
	}
}

func (css *composedSourceStage) connect(sink SinkStage) Inline { return nil }

func (css *composedSourceStage) Via(flow FlowStage) SourceGraph {
	flow.Connected(newStageInline(css.source.connect(flow), css.start))
	return composeSourceStage(css, flow)
}

func (css *composedSourceStage) To(sink EmitterSinkStage) Runner {
	sink.Connected(newStageInline(css.source.connect(sink), css.start))
	return newRunner(sink, func() {
		sink.start()
		css.start()
	})
}

func (css *composedSourceStage) RunWith(sink EmitterSinkStage) Runnable {
	sink.Connected(newStageInline(css.source.connect(sink), css.start))
	return newRunnable(sink, func() {
		sink.start()
		css.start()
	})
}

type sourceStage struct {
	active  bool
	handler SourceHandler
	pipe    Pipe
}

func NewSource(handler SourceHandler) SourceGraph {
	return &sourceStage{
		handler: handler,
	}
}

func sourceWorker(handler SourceHandler, outline Outline) {
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
	if !src.active {
		go sourceWorker(src.handler, src.pipe)
		src.active = true
	}
}

func (src *sourceStage) connect(sin SinkStage) Inline {
	if src.pipe != nil {
		// TODO autocreate FanOut / Broadcast
		panic("source is already connected")
	}
	src.pipe = newPipe()

	return src.pipe
}

func (src *sourceStage) Via(flow FlowStage) SourceGraph {
	flow.Connected(newStageInline(src.connect(flow), src.start))
	return composeSourceStage(src, flow)
}

func (src *sourceStage) To(sink EmitterSinkStage) Runner {
	sink.Connected(newStageInline(src.connect(sink), src.start))
	return newRunner(sink, func() {
		sink.start()
		src.start()
	})
}

func (src *sourceStage) RunWith(sink EmitterSinkStage) Runnable {
	sink.Connected(newStageInline(src.connect(sink), src.start))
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
