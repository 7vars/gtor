package rx

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
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

type SourceFunc[T any] func() (T, error)

func (f SourceFunc[T]) HandleStartup() {}

func (f SourceFunc[T]) HandlePull(out Outlet) {
	t, err := f()
	if err != nil {
		if errors.Is(err, io.EOF) {
			out.Complete()
			return
		}
		out.Error(err)
		return
	}
	out.Push(t)
}

func (f SourceFunc[T]) HandleCancel(out Outlet) {
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
		defer fmt.Println("DEBUG RUNNER CLOSED")
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
