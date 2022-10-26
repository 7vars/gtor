package gtor

import (
	"fmt"
	"reflect"
)

type Receive func(GtorContext, interface{}) (interface{}, error)

type Receiver interface {
	Receive(GtorContext, interface{}) (interface{}, error)
}

type ReceiverFunc Receive

func (f ReceiverFunc) Receive(gc GtorContext, v interface{}) (interface{}, error) {
	return f(gc, v)
}

func Empty() Receiver {
	return ReceiverFunc(func(gc GtorContext, i interface{}) (interface{}, error) { return Done{}, nil })
}

type ReceiveCreator func(GtorSystem) (Receiver, error)

func Creator(v interface{}) ReceiveCreator {
	switch rec := v.(type) {
	case ReceiveCreator:
		return rec
	case func(GtorSystem) (Receiver, error):
		return rec
	case Receive:
		return func(GtorSystem) (Receiver, error) {
			return ReceiverFunc(rec), nil
		}
	case Receiver:
		return func(GtorSystem) (Receiver, error) {
			return rec, nil
		}
	default:
		recV := reflect.ValueOf(rec)
		recT := recV.Type()
		// func support
		if recT.Kind() == reflect.Func {
			_, f, err := buildReceiveFromFunc(recV)
			if err != nil {
				return func(gs GtorSystem) (Receiver, error) {
					return nil, err
				}
			}
			return func(gs GtorSystem) (Receiver, error) {
				return ReceiverFunc(f), nil
			}
		}

		// struct support
		if recT.Kind() == reflect.Ptr && recT.Elem().Kind() == reflect.Struct {
			return func(gs GtorSystem) (Receiver, error) {
				return buildReceiverFromPtrStruct(recV)
			}
		}
	}
	return func(GtorSystem) (Receiver, error) {
		return nil, fmt.Errorf("unsupported type %T", v)
	}
}
