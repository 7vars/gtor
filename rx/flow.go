package rx

import (
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"sync/atomic"
)

var ErrNext = errors.New("next")

type FlowStage interface {
	SourceStage
	SinkStage
}

type FlowHandler interface {
	HandlePull(IOlet)
	HandleCancel(IOlet)

	HandlePush(IOlet, interface{})
	HandleError(IOlet, error)
	HandleComplete(IOlet)
}

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

type flowStage struct {
	active  bool
	handler FlowHandler
	pipe    Pipe
	inline  Inline
}

func NewFlow(handler FlowHandler) FlowStage {
	return &flowStage{
		handler: handler,
	}
}

func flowWorker(handler FlowHandler, pipe Pipe) {
	var eventsClosed, commandsClosed bool
	for {
		select {
		case evt, open := <-pipe.Events():
			if !open {
				if commandsClosed {
					return
				}
				eventsClosed = true
				continue
			}
			switch evt.Type() {
			case PUSH:
				handler.HandlePush(pipe, evt.Data)
			case ERROR:
				handler.HandleError(pipe, evt.Err)
			case COMPLETE:
				handler.HandleComplete(pipe)
			}
		case cmd, open := <-pipe.Commands():
			if !open {
				if eventsClosed {
					return
				}
				commandsClosed = true
				continue
			}
			switch cmd {
			case PULL:
				handler.HandlePull(pipe)
			case CANCEL:
				handler.HandleCancel(pipe)
			}
		}
	}
}

func (f *flowStage) start() {
	if !f.active {
		go flowWorker(f.handler, combineIO(f.inline, f.pipe))
		f.active = true
	}
}

func (f *flowStage) connect(sink SinkStage) Inline {
	if f.pipe != nil {
		// TODO autocreate FanOut
		panic("flow is already connected")
	}
	f.pipe = newPipe()

	return f.pipe
}

func (f *flowStage) Connected(inline StageInline) {
	if f.inline != nil {
		// TODO autocreate FanIn
		panic("flow is already connected")
	}
	f.inline = inline
}

// ===== flows =====

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
