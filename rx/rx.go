package rx

import (
	"context"
	"errors"
)

var ErrNext = errors.New("next")

type Command int

const (
	PULL Command = iota
	CANCEL
)

type Event struct {
	Data    interface{}
	Err     error
	Comlete bool
}

func (e Event) IsError() bool {
	return e.Err != nil
}

func (e Event) HasData() bool {
	return e.Data != nil
}

func (e Event) IsCompleted() bool {
	return e.Comlete
}

type Emitter interface {
	Emit(interface{})
	Close()
}

type Outline interface {
	Commands() <-chan Command
	Outlet
}

type Outlet interface {
	Push(interface{})
	Error(error)
	Complete()
}

type SourceHandler interface {
	HandlePull(Outlet)
	HandleCancel(Outlet)
}

type SourceStage interface {
	RunWith(context.Context, SinkStage) <-chan interface{}
}

type Inline interface {
	Events() <-chan Event
	Emitter
}

type Inlet interface {
	Emitter
	Pull()
	Cancel()
}

type SinkHandler interface {
	HandlePush(Inlet, interface{})
	HandleError(Inlet, error)
	HandleComplete(Inlet)
}

type SinkStage interface {
	Commands() <-chan Command
	Connected(Inline)
}

// ==============================================

type Publisher[T any] interface {
	Subscribe(Subscriber[T])
}

type Subscription interface {
	Request(int)
	Cancel()
}

type Subscriber[T any] interface {
	OnSubscribe(Subscription)
	OnNext(T)
	OnError(error)
	OnComplete()
}

type Processor[T, K any] interface {
	Subscriber[T]
	Publisher[K]
}

func Via[T, K any](pub Publisher[T], proc Processor[T, K]) Publisher[K] {
	pub.Subscribe(proc)
	return proc
}

func To[T any](pub Publisher[T], subs ...Subscriber[T]) {
	for _, sub := range subs {
		pub.Subscribe(sub)
	}
}
