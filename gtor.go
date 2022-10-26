package gtor

import (
	"os"
	"os/signal"
	"syscall"
)

type GtorSystem interface {
	Logger
	GtorExecuter

	Name() string

	Config() Config

	Publish(interface{})
	Subscribe(Ref, func(interface{}) bool)
	Unsubscribe(Ref)

	Terminate()
	Terminated() <-chan struct{}
}

type gtorSystem struct {
	Logger
	name   string
	config Config
	core   *gtorHandler
	pubsub Ref
}

func New(name string, opts ...Option) GtorSystem {
	core, err := newHandler(name, coreCreator, nil, nil, opts...)
	if err != nil {
		panic(err)
	}
	sys := &gtorSystem{
		Logger: newLogger().WithField("gtor", name),
		name:   name,
		config: config(),
		core:   core,
	}

	core.Lock()
	core.system = sys
	core.Unlock()

	sys.pubsub, err = core.ExecuteCreator("pubsub", pubsubCreator)
	if err != nil {
		panic(err)
	}

	if _, err := core.ExecuteCreator("services", serviceCreator); err != nil {
		panic(err)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func(log Logger) {
		sig := <-sigs
		log.Infof("receive signal: %v", sig)
		sys.Terminate()
	}(sys)

	return sys
}

func Gtor(name string, opts ...Option) GtorSystem {
	return New(name, opts...)
}

func (sys *gtorSystem) Name() string {
	return sys.name
}

func (sys *gtorSystem) Config() Config {
	return config()
}

func (sys *gtorSystem) ExecuteCreator(name string, creator ReceiveCreator, opts ...Option) (Ref, error) {
	return sys.core.ExecuteCreator(name, creator, opts...)
}

func (sys *gtorSystem) Execute(name string, v interface{}, opts ...Option) (Ref, error) {
	return sys.core.Execute(name, v, opts...)
}

func (sys *gtorSystem) At(path string) (Ref, bool) {
	return sys.core.At(path)
}

func (sys *gtorSystem) Publish(v interface{}) {
	sys.pubsub.Send(v)
}

func (sys *gtorSystem) Subscribe(ref Ref, f func(interface{}) bool) {
	sys.pubsub.Send(Subscribe{
		Ref:    ref,
		Filter: f,
	})
}

func (sys *gtorSystem) Unsubscribe(ref Ref) {
	sys.pubsub.Send(Unsubscribe{
		Ref: ref,
	})
}

func (sys *gtorSystem) Terminate() {
	sys.core.Close()
}

func (sys *gtorSystem) Terminated() <-chan struct{} {
	return sys.core.Closed()
}
