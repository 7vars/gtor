package rx

type Sink struct {
	OnStart    func()
	OnPush     func(Inlet, interface{})
	OnError    func(Inlet, error)
	OnComplete func(Inlet)
}

func (s Sink) HandleStartup() {
	if s.OnStart != nil {
		s.OnStart()
	}
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
	// TODO emit error
	in.Cancel()
}

func (s Sink) HandleComplete(in Inlet) {
	// TODO defer close emitter
	if s.OnComplete != nil {
		s.OnComplete(in)
		return
	}
	// TODO emit done
}

type SinkFunc[T any] func(T)

func (f SinkFunc[T]) HandleStartup() {}

func (f SinkFunc[T]) HandlePush(in Inlet, v interface{}) {
	if t, ok := v.(T); ok {
		f(t)
		in.Pull()
		return
	}
	// TODO emit error
	in.Cancel()
}

func (f SinkFunc[T]) HandleError(in Inlet, err error) {
	// TODO emit error
	in.Cancel()
}

func (f SinkFunc[T]) HandleComplete(in Inlet) {
	// TODO emit done
	// TODO close emitter
}

// ==============================================

type SinkStage interface {
}

type sinkStage struct {
	handler SinkHandler
}

func NewSink(handler SinkHandler) SinkStage {
	return &sinkStage{
		handler: handler,
	}
}
