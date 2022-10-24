package rx

import (
	"errors"
	"fmt"
	"sync"

	"github.com/7vars/gtor"
)

type SinkStage interface {
	Connected(EmittableInline) error
}

type SinkHandler interface {
	HandlePush(Emittable, interface{})
	HandleError(Emittable, error)
	HandleComplete(Emittable)
}

type Sink struct {
	OnPush     func(Emittable, interface{})
	OnError    func(Emittable, error)
	OnComplete func(Emittable)
}

func (s Sink) HandlePush(i Emittable, v interface{}) {
	if s.OnPush != nil {
		s.OnPush(i, v)
		return
	}
	i.Pull()
}

func (s Sink) HandleError(i Emittable, e error) {
	if s.OnError != nil {
		s.OnError(i, e)
		return
	}
	i.EmitError(e)
	i.Cancel()
}

func (s Sink) HandleComplete(i Emittable) {
	defer i.Close()
	if s.OnComplete != nil {
		s.OnComplete(i)
		return
	}
	i.Emit(gtor.DONE())
}

type sinkStage struct {
	handler SinkHandler
	inline  EmittableInline
}

func NewSink(handler SinkHandler) SinkStage {
	return &sinkStage{
		handler: handler,
	}
}

func sinkWorker(handler SinkHandler, inline EmittableInline) {
	// defer fmt.Println("DEBUG SINK-WORK CLOSED")
	for evt := range inline.Events() {
		switch evt.Type() {
		case PUSH:
			handler.HandlePush(inline, evt.Data)
		case ERROR:
			handler.HandleError(inline, evt.Err)
		case COMPLETE:
			handler.HandleComplete(inline)
		}
	}
}

func (snk *sinkStage) Connected(inline EmittableInline) error {
	if snk.inline != nil {
		return errors.New("sink is already connected")
	}
	snk.inline = inline
	go sinkWorker(snk.handler, snk.inline)
	return nil
}

// ===== sinks =====

func Empty() SinkStage {
	return NewSink(Sink{})
}

func ForEach[T any](f func(T)) SinkStage {
	return NewSink(Sink{
		OnPush: func(in Emittable, v interface{}) {
			if t, ok := v.(T); ok {
				f(t)
				in.Pull()
				return
			}
			var t0 T
			in.EmitError(fmt.Errorf("unsupported type %T needs %T", v, t0))
			in.Cancel()
		},
	})
}

func Println() SinkStage {
	return ForEach(func(v interface{}) {
		fmt.Println(v)
	})
}

func Collect[T any]() SinkStage {
	var m sync.Mutex
	slice := make([]T, 0)
	var hasError bool
	return NewSink(Sink{
		OnPush: func(in Emittable, v interface{}) {
			m.Lock()
			defer m.Unlock()
			if t, ok := v.(T); ok {
				slice = append(slice, t)
				in.Pull()
				return
			}
			hasError = true
			var t0 T
			in.EmitError(fmt.Errorf("unsupported type %T needs %T", v, t0))
			in.Cancel()
		},
		OnComplete: func(in Emittable) {
			m.Lock()
			defer m.Unlock()
			if !hasError {
				in.Emit(slice)
			}
		},
	})
}
