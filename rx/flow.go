package rx

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"sync/atomic"
)

type Flow struct {
	OnPull   func(IOlet)
	OnCancel func(IOlet)

	OnPush     func(IOlet, interface{})
	OnError    func(IOlet, error)
	OnComplete func(IOlet)
}

func (f Flow) HandlePull(io IOlet) {
	if f.OnPull != nil {
		f.OnPull(io)
		return
	}
	io.Pull()
}

func (f Flow) HandleCancel(io IOlet) {
	if f.OnCancel != nil {
		f.OnCancel(io)
		return
	}
	io.Cancel()
}

func (f Flow) HandlePush(io IOlet, v interface{}) {
	if f.OnPush != nil {
		f.OnPush(io, v)
		return
	}
	io.Push(v)
}

func (f Flow) HandleError(io IOlet, err error) {
	if f.OnError != nil {
		f.OnError(io, err)
		return
	}
	io.Error(err)
}

func (f Flow) HandleComplete(io IOlet) {
	if f.OnComplete != nil {
		f.OnComplete(io)
		return
	}
	io.Complete()
}

// ==============================================

type flowStage struct {
	handler  FlowHandler
	commands chan Command
	abort    chan struct{}
	pipeline *pipe
	inline   Inline
}

func NewFlow(handler FlowHandler) FlowStage {
	return &flowStage{
		handler:  handler,
		commands: make(chan Command, 100), // TODO configure size
		abort:    make(chan struct{}, 1),
	}
}

func flowWorker(abort <-chan struct{}, handler FlowHandler, outline Outline, inline Inline, iolet IOlet, onComplete func()) {
	defer fmt.Println("DEBUG FLOW CLOSED")
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
				handler.HandlePull(iolet)
			case CANCEL:
				handler.HandleCancel(iolet)
			}
		case evt, open := <-inline.Events():
			if !open {
				return
			}
			if evt.IsCompleted() {
				defer onComplete()
				handler.HandleComplete(iolet)
				return
			}
			if evt.IsError() {
				handler.HandleError(iolet, evt.Err)
				continue
			}
			handler.HandlePush(iolet, evt.Data)
		}
	}
}

// ===== SinkStage-Part =====

func (f *flowStage) Commands() <-chan Command {
	return f.commands
}

func (f *flowStage) start() {
	if f.pipeline != nil && f.inline != nil {
		iolet := newIOlet(f.pipeline, InletChan(f.commands))
		defer iolet.Pull()
		go flowWorker(f.abort, f.handler, f.pipeline, f.inline, iolet, func() { close(f.commands) })
	}
}

func (f *flowStage) Connected(inline Inline) {
	// TODO handle FanIn
	f.inline = inline
	f.start()
}

// ===== SourceStage-Part =====

func (f *flowStage) RunWith(ctx context.Context, sink SinkStage) <-chan interface{} {
	if f.pipeline != nil {
		// TODO Broadcast (FanOut)
		panic("flow already connected")
	}

	emits := make(chan interface{}, 100) // TODO configure size
	channel := make(chan interface{}, 1)
	f.pipeline = newPipe(sink.Commands(), EmitterChan(emits))

	go func(c context.Context, in <-chan interface{}, out chan<- interface{}) {
		defer fmt.Println("DEBUG FLOW-RUNNER CLOSED")
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

	f.start()

	sink.Connected(f.pipeline)

	return channel
}

func (f *flowStage) Via(flow FlowStage) SourceStage {
	if f.pipeline != nil {
		// TODO Broadcast (FanOut)
		panic("flow already connected")
	}

	f.pipeline = newPipe(flow.Commands(), nil)

	f.start()

	flow.Connected(f.pipeline)

	return flow
}

// ===== sinks =====

func FlowFunc[T, K any](f func(T) (K, error)) FlowStage {
	return NewFlow(Flow{
		OnPush: func(i IOlet, v interface{}) {
			if t, ok := v.(T); ok {
				k, err := f(t)
				if err != nil {
					if errors.Is(err, io.EOF) {
						i.Cancel()
						return
					}
					if errors.Is(err, ErrNext) {
						i.Pull()
						return
					}
					i.Error(err)
					return
				}
				i.Push(k)
				return
			}
			var t0 T
			i.Error(fmt.Errorf("flowFunc(%T): unsupported type %T", t0, v))
		},
	})
}

func Map[T, K any](f func(T) K) FlowStage {
	return NewFlow(Flow{
		OnPush: func(io IOlet, v interface{}) {
			if t, ok := v.(T); ok {
				io.Push(f(t))
				return
			}
			var t0 T
			io.Error(fmt.Errorf("unsupported type %T needs %T", v, t0))
		},
	})
}

func Filter[T any](f func(T) bool) FlowStage {
	return NewFlow(Flow{
		OnPush: func(io IOlet, v interface{}) {
			if t, ok := v.(T); ok {
				if f(t) {
					io.Push(t)
					return
				}
				io.Pull()
				return
			}
			var t0 T
			io.Error(fmt.Errorf("filter(%T): unsupported type %T", t0, v))
		},
	})
}

func Take(n uint64) FlowStage {
	var count uint64
	return NewFlow(Flow{
		OnPush: func(io IOlet, v interface{}) {
			defer atomic.AddUint64(&count, 1)
			if atomic.LoadUint64(&count) < n {
				io.Push(v)
				return
			}
			io.Cancel()
		},
	})
}

func TakeWhile[T any](f func(T) bool) FlowStage {
	return NewFlow(Flow{
		OnPush: func(io IOlet, v interface{}) {
			if t, ok := v.(T); ok {
				if f(t) {
					io.Push(t)
					return
				}
				io.Cancel()
				return
			}
			var t0 T
			io.Error(fmt.Errorf("takeWhile(%T): unsupported type %T", t0, v))
		},
	})
}

func Drop(n uint64) FlowStage {
	var count uint64
	return NewFlow(Flow{
		OnPush: func(io IOlet, v interface{}) {
			defer atomic.AddUint64(&count, 1)
			if atomic.LoadUint64(&count) < n {
				io.Pull()
				return
			}
			io.Push(v)
		},
	})
}

func DropWhile[T any](f func(T) bool) FlowStage {
	return NewFlow(Flow{
		OnPush: func(io IOlet, v interface{}) {
			if t, ok := v.(T); ok {
				if f(t) {
					io.Pull()
					return
				}
				io.Push(t)
				return
			}
			var t0 T
			io.Error(fmt.Errorf("dropWhile(%T): unsupported type %T", t0, v))
		},
	})
}

func Fold[T, K any](k K, f func(K, T) K) FlowStage {
	var m sync.Mutex
	x := k
	var complete bool
	return NewFlow(Flow{
		OnPush: func(io IOlet, v interface{}) {
			if t, ok := v.(T); ok {
				m.Lock()
				defer m.Unlock()
				x = f(x, t)
				io.Pull()
				return
			}
			var t0 T
			io.Error(fmt.Errorf("fold(%T): unsupported type %T", t0, v))
		},
		OnComplete: func(io IOlet) {
			m.Lock()
			defer m.Unlock()
			io.Push(x)
			complete = true
		},
		OnPull: func(io IOlet) {
			m.Lock()
			defer m.Unlock()
			if complete {
				io.Complete()
				return
			}
			io.Pull()
		},
	})
}

func Reduce[T any](f func(T, T) T) FlowStage {
	var t0 T
	return Fold(t0, f)
}

func Catch(f func(error) bool) FlowStage {
	return NewFlow(Flow{
		OnError: func(io IOlet, err error) {
			if f(err) {
				io.Pull()
				return
			}
			io.Error(err)
		},
	})
}

func toSlice(v interface{}) ([]interface{}, error) {
	value := reflect.ValueOf(v)
	if kind := value.Kind(); kind != reflect.Slice {
		return nil, fmt.Errorf("%T is not a slice", v)
	}
	result := make([]interface{}, 0)
	for i := 0; i < value.Len(); i++ {
		result = append(result, value.Index(i).Interface())
	}
	return result, nil
}

func Explode() FlowStage {
	var m sync.Mutex
	var values []interface{}

	grap := func() (interface{}, bool) {
		m.Lock()
		defer m.Unlock()
		if len(values) > 0 {
			result := values[0]
			values = values[1:]
			return result, true
		}
		return nil, false
	}

	return NewFlow(Flow{
		OnPush: func(io IOlet, v interface{}) {
			slice, err := toSlice(v)
			if err != nil {
				io.Error(fmt.Errorf("explode: %v", err))
				return
			}
			m.Lock()
			values = slice
			m.Unlock()
			if v, ok := grap(); ok {
				io.Push(v)
				return
			}
			io.Pull()
		},
		OnPull: func(io IOlet) {
			if v, ok := grap(); ok {
				io.Push(v)
				return
			}
			io.Pull()
		},
	})
}
