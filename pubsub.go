package gtor

type Subscribe struct {
	Ref    Ref
	Filter func(interface{}) bool
}

type Unsubscribe struct {
	Ref Ref
}

func pubsubCreator(GtorSystem) (Receiver, error) {
	subscriptions := make(map[string]Subscribe)

	return ReceiverFunc(func(gc GtorContext, i interface{}) (interface{}, error) {
		switch cmd := i.(type) {
		case Subscribe:
			if _, ok := subscriptions[cmd.Ref.Path()]; !ok {
				subscriptions[cmd.Ref.Path()] = cmd
			}
			return Done{}, nil
		case Unsubscribe:
			delete(subscriptions, cmd.Ref.Path())
			return Done{}, nil
		default:
			for _, sub := range subscriptions {
				if sub.Filter(cmd) {
					sub.Ref.Send(cmd)
				}
			}
			return Done{}, nil
		}
	}), nil
}
