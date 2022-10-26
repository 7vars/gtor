package rx

import (
	"fmt"
	"sync"

	"github.com/7vars/gtor"
)

type SinkStage interface {
	Stage
	Connected(StageInline)
}

type EmitterSinkStage interface {
	Emitter
	SinkStage
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
	sync.RWMutex
	active  bool
	handler SinkHandler
	inline  Inline
	closed  bool
	emits   chan interface{}
}

func NewSink(handler SinkHandler) EmitterSinkStage {
	return &sinkStage{
		handler: handler,
		emits:   make(chan interface{}, 1),
	}
}

func sinkWorker(handler SinkHandler, inline EmittableInline) {
	inline.Pull()
	fmt.Println("DEBUG SINK-WORK STARTED")
	defer fmt.Println("DEBUG SINK-WORK CLOSED")
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

func (snk *sinkStage) start() {
	snk.Lock()
	defer snk.Unlock()
	if !snk.active {
		go sinkWorker(snk.handler, newEmittableInline(snk.inline, snk))
		snk.active = true
	}
}

func (snk *sinkStage) Connected(inline StageInline) {
	if snk.inline != nil {
		// TODO autocreate FanIn
		panic("sink is already connected")
	}
	snk.inline = inline
}

func (snk *sinkStage) isClosed() bool {
	snk.RLock()
	defer snk.RUnlock()
	return snk.closed
}

func (snk *sinkStage) Emits() <-chan interface{} {
	return snk.emits
}

func (snk *sinkStage) Emit(v interface{}) {
	if !snk.isClosed() {
		snk.emits <- v
	}
}

func (snk *sinkStage) EmitError(e error) {
	if !snk.isClosed() {
		snk.emits <- e
		snk.Close()
	}
}

func (snk *sinkStage) Close() {
	snk.Lock()
	defer snk.Unlock()
	if !snk.closed {
		close(snk.emits)
		snk.closed = true
	}
}

// ===== sinks =====

func Empty() EmitterSinkStage {
	return NewSink(Sink{})
}

func ForEach[T any](f func(T)) EmitterSinkStage {
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

func Println() EmitterSinkStage {
	return ForEach(func(v interface{}) {
		fmt.Println(v)
	})
}

func Collect[T any]() EmitterSinkStage {
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
