package rx

import (
	"fmt"
	"sync"

	"github.com/7vars/gtor"
)

type Sink struct {
	OnPush     func(Inlet, interface{})
	OnError    func(Inlet, error)
	OnComplete func(Inlet)
}

func (s Sink) HandlePush(in Inlet, v interface{}) {
	if s.OnPush != nil {
		s.OnPush(in, v)
		return
	}
	in.Pull()
}

func (s Sink) HandleError(in Inlet, err error) {
	if s.OnError != nil {
		s.OnError(in, err)
		return
	}
	in.Emit(err)
	in.Cancel()
}

func (s Sink) HandleComplete(in Inlet) {
	defer in.Close()
	if s.OnComplete != nil {
		s.OnComplete(in)
		return
	}
	in.Emit(gtor.DONE())
}

type SinkFunc[T any] func(T)

func (f SinkFunc[T]) HandleStartup() {}

func (f SinkFunc[T]) HandlePush(in Inlet, v interface{}) {
	if t, ok := v.(T); ok {
		f(t)
		in.Pull()
		return
	}
	var t0 T
	in.Emit(fmt.Errorf("unsupported type %T need %T", v, t0))
	in.Cancel()
}

func (f SinkFunc[T]) HandleError(in Inlet, err error) {
	in.Emit(err)
	in.Cancel()
}

func (f SinkFunc[T]) HandleComplete(in Inlet) {
	defer in.Close()
	in.Emit(gtor.DONE())
}

// ==============================================

type sinkStage struct {
	handler  SinkHandler
	abort    chan struct{}
	commands chan Command
}

func NewSink(handler SinkHandler) SinkStage {
	return &sinkStage{
		handler:  handler,
		abort:    make(chan struct{}, 1),
		commands: make(chan Command, 100), // TODO configure size
	}
}

func sinkWorker(abort <-chan struct{}, handler SinkHandler, inline Inline, inlet Inlet, onComplete func()) {
	defer fmt.Println("DEBUG SINK CLOSED")
	for {
		select {
		case <-abort:
			return
		case evt, open := <-inline.Events():
			if !open {
				return
			}
			if evt.IsCompleted() {
				defer onComplete()
				handler.HandleComplete(inlet)
				return
			}
			if evt.IsError() {
				handler.HandleError(inlet, evt.Err)
				continue
			}
			handler.HandlePush(inlet, evt.Data)
		}
	}
}

func (sink *sinkStage) Commands() <-chan Command {
	return sink.commands
}

func (sink *sinkStage) Connected(inline Inline) {
	// TODO handle fanIn
	inlet := Emittable(inline, InletChan(sink.commands))
	defer inlet.Pull()
	go sinkWorker(sink.abort, sink.handler, inline, inlet, func() { close(sink.commands) })
}

// ===== sinks =====

func ForEach[T any](f func(T)) SinkStage {
	return NewSink(Sink{
		OnPush: func(in Inlet, v interface{}) {
			if t, ok := v.(T); ok {
				f(t)
				in.Pull()
				return
			}
			var t0 T
			in.Emit(fmt.Errorf("unsupported type %T needs %T", v, t0))
			in.Cancel()
		},
	})
}

func Collect[T any]() SinkStage {
	var m sync.Mutex
	slice := make([]T, 0)
	var hasError bool
	return NewSink(Sink{
		OnPush: func(in Inlet, v interface{}) {
			m.Lock()
			defer m.Unlock()
			if t, ok := v.(T); ok {
				slice = append(slice, t)
				in.Pull()
				return
			}
			hasError = true
			var t0 T
			in.Emit(fmt.Errorf("unsupported type %T needs %T", v, t0))
			in.Cancel()
		},
		OnComplete: func(in Inlet) {
			m.Lock()
			defer m.Unlock()
			if !hasError {
				in.Emit(slice)
			}
		},
	})
}
