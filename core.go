package gtor

import "sync"

var (
	serviceMu sync.RWMutex
	services  = make(map[string]ReceiveCreator)
)

func RegisterService(name string, creator ReceiveCreator) {
	serviceMu.Lock()
	defer serviceMu.Unlock()
	if creator == nil {
		panic("gtor: creator for service " + name + " is nil")
	}
	if _, exists := services[name]; exists {
		panic("gtor: service " + name + " is already registered")
	}
	services[name] = creator
}

func coreCreator(GtorSystem) (Receiver, error) {
	return ReceiverFunc(func(ctx GtorContext, i interface{}) (interface{}, error) {
		switch i.(type) {
		case Startup:
			ctx.Info("start gtor-core")
		case Stop:
			ctx.Info("stop gtor-core")
		}
		return Done{}, nil
	}), nil
}

func serviceCreator(sys GtorSystem) (Receiver, error) {
	return ReceiverFunc(func(gc GtorContext, i interface{}) (interface{}, error) {
		switch i.(type) {
		case Startup:
			serviceMu.RLock()
			defer serviceMu.RUnlock()
			for name, creator := range services {
				gc.Infof("start service %s", name)
				if _, err := gc.ExecuteCreator(name, creator); err != nil {
					return nil, err
				}
			}
		case Stop:

		}
		return Done{}, nil
	}), nil
}
