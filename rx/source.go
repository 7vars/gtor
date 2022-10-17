package rx

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/google/uuid"
)

type Source struct {
	OnPull   func(Outlet)
	OnCancel func(Outlet)
}

func (s Source) HandlePull(out Outlet) {
	if s.OnPull != nil {
		s.OnPull(out)
		return
	}
	out.Complete()
}

func (s Source) HandleCancel(out Outlet) {
	if s.OnCancel != nil {
		s.OnCancel(out)
		return
	}
	out.Complete()
}

// ==============================================

type sourceStage struct {
	handler  SourceHandler
	abort    chan struct{}
	pipeline *pipe
}

func NewSource(handler SourceHandler) SourceStage {
	return &sourceStage{
		handler: handler,
		abort:   make(chan struct{}, 1),
	}
}

func sourceWorker(abort <-chan struct{}, handler SourceHandler, outline Outline) {
	defer fmt.Println("DEBUG SOURCE CLOSED")
	for {
		select {
		case <-abort:
			return
		case evt, open := <-outline.Commands():
			if !open {
				return
			}
			switch evt {
			case PULL:
				handler.HandlePull(outline)
			case CANCEL:
				handler.HandleCancel(outline)
			}
		}
	}
}

func (src *sourceStage) RunWith(ctx context.Context, sink SinkStage) <-chan interface{} {
	if src.pipeline != nil {
		// TODO Broadcast (FanOut)
		panic("source already connected")
	}

	emits := make(chan interface{}, 100) // TODO configure size
	channel := make(chan interface{}, 1)
	src.pipeline = newPipe(sink.Commands(), EmitterChan(emits))

	go func(c context.Context, in <-chan interface{}, out chan<- interface{}) {
		defer fmt.Println("DEBUG SOURCE-RUNNER CLOSED")
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case v, open := <-in:
				if !open {
					return
				}
				out <- v
			}
		}
	}(ctx, emits, channel)

	go sourceWorker(src.abort, src.handler, src.pipeline)

	sink.Connected(src.pipeline)

	return channel
}

func (src *sourceStage) Via(flow FlowStage) SourceStage {
	if src.pipeline != nil {
		// TODO Broadcast (FanOut)
		panic("source already connected")
	}

	src.pipeline = newPipe(flow.Commands(), nil)

	go sourceWorker(src.abort, src.handler, src.pipeline)

	flow.Connected(src.pipeline)

	return flow
}

// ===== sources =====

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
