package rx

import "errors"

var ErrNext = errors.New("next")

type StartupHandler interface {
	HandleStartup()
}

type Outlet interface {
	Push(interface{})
	Error(error)
	Comlete()
}

type SourceHandler interface {
	StartupHandler
	HandlePull(Outlet)
	HandleCancel(Outlet)
}

type Inlet interface {
	Pull()
	Cancel()
}

type SinkHandler interface {
	StartupHandler
	HandlePush(Inlet, interface{})
	HandleError(Inlet, error)
	HandleComplete(Inlet)
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
