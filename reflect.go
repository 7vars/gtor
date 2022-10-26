package gtor

import (
	"fmt"
	"reflect"
)

type none struct{}

var (
	errorType   = reflect.TypeOf((*error)(nil)).Elem()
	contextType = reflect.TypeOf((*GtorContext)(nil)).Elem()
	messageType = reflect.TypeOf((*Msg)(nil)).Elem()
	noneType    = reflect.TypeOf((*none)(nil)).Elem()
)

func isErrorType(gtype reflect.Type) bool {
	return gtype.Implements(errorType)
}

func isContextType(gtype reflect.Type) bool {
	return gtype.Implements(contextType)
}

func isMessageType(gtype reflect.Type) bool {
	return gtype.Implements(messageType)
}

func isNoneType(gtype reflect.Type) bool {
	return gtype.Implements(noneType)
}

func checkImplements(atype reflect.Type, btype reflect.Type) bool {
	if btype.Kind() == reflect.Interface {
		return atype.Implements(btype)
	} else if atype.Kind() == reflect.Interface {
		return btype.Implements(atype)
	}
	return false
}

func compareType(atype reflect.Type, btype reflect.Type) bool {
	return atype == btype || checkImplements(atype, btype)
}

func createParamFunc(t reflect.Type) (reflect.Type, func(gc GtorContext, i interface{}) ([]reflect.Value, error), bool) {
	// with no param
	if t.NumIn() == 0 {
		return noneType, func(gc GtorContext, i interface{}) ([]reflect.Value, error) {
			return []reflect.Value{}, nil
		}, true
	}

	// with context param
	if t.NumIn() == 1 && isContextType(t.In(0)) {
		return noneType, func(gc GtorContext, i interface{}) ([]reflect.Value, error) {
			return []reflect.Value{reflect.ValueOf(gc)}, nil
		}, true
	}

	// with value param
	if t.NumIn() == 1 {
		pt := t.In(0)
		return pt, func(gc GtorContext, i interface{}) ([]reflect.Value, error) {
			if compareType(pt, reflect.TypeOf(i)) {
				return []reflect.Value{reflect.ValueOf(i)}, nil
			}
			return nil, fmt.Errorf("unsupported type %T", i)
		}, true
	}

	// with context and value params
	if t.NumIn() == 2 && isContextType(t.In(0)) {
		pt := t.In(1)
		return pt, func(gc GtorContext, i interface{}) ([]reflect.Value, error) {
			if compareType(pt, reflect.TypeOf(i)) {
				return []reflect.Value{reflect.ValueOf(gc), reflect.ValueOf(i)}, nil
			}
			return nil, fmt.Errorf("unsupported type %T", i)
		}, true
	}

	return nil, nil, false
}

func buildReceiveFromFunc(value reflect.Value) (reflect.Type, Receive, error) {
	t := value.Type()
	if t.Kind() != reflect.Func {
		return noneType, nil, fmt.Errorf("%v is not a func", t)
	}

	if pt, f, ok := createParamFunc(t); ok {
		// with no return
		if t.NumOut() == 0 {
			return pt, func(gc GtorContext, i interface{}) (interface{}, error) {
				params, err := f(gc, i)
				if err != nil {
					return nil, err
				}
				value.Call(params)
				return Done{}, nil
			}, nil
		}
		// with value|error return
		if t.NumOut() == 1 {
			return pt, func(gc GtorContext, i interface{}) (interface{}, error) {
				params, err := f(gc, i)
				if err != nil {
					return nil, err
				}
				results := value.Call(params)
				result := results[0].Interface()
				if err, ok := result.(error); ok {
					return nil, err
				}

				if isErrorType(t.Out(0)) {
					return Done{}, nil
				}
				return result, nil
			}, nil
		}
		// with value, error return
		if t.NumOut() == 2 && isErrorType(t.Out(1)) {
			return pt, func(gc GtorContext, i interface{}) (interface{}, error) {
				params, err := f(gc, i)
				if err != nil {
					return nil, err
				}
				results := value.Call(params)
				if errRes := results[1].Interface(); errRes != nil {
					err = errRes.(error)
				}
				return results[0].Interface(), err
			}, nil
		}
	}

	return nil, nil, fmt.Errorf("unsupported func %v", t)
}

type reflectReceiver struct {
	ptr   reflect.Value
	funcs map[reflect.Type]Receive
}

func (rec *reflectReceiver) Receive(gc GtorContext, v interface{}) (interface{}, error) {
	switch msg := v.(type) {
	case Startup:
		if sta, ok := rec.ptr.Interface().(Startable); ok {
			if err := sta.OnStartup(gc); err != nil {
				return nil, err
			}
			return Done{}, nil
		}
	case Close:
		if sto, ok := rec.ptr.Interface().(Stopable); ok {
			if err := sto.OnStop(gc); err != nil {
				return nil, err
			}
			return Done{}, nil
		}
	default:
		msgT := reflect.TypeOf(msg)
		if r, ok := rec.funcs[msgT]; ok {
			return r(gc, msg)
		}
	}
	return nil, fmt.Errorf("unsupported message-type %T", v)
}

func chainReceiveWhenDone(a Receive, b Receive) Receive {
	return func(gc GtorContext, i interface{}) (interface{}, error) {
		val, err := a(gc, i)
		if err != nil {
			return nil, err
		}
		if _, ok := val.(Done); ok {
			return b(gc, i)
		}
		return val, nil
	}
}

func buildReceiverFromPtrStruct(value reflect.Value) (Receiver, error) {
	if value.Kind() != reflect.Ptr || value.Elem().Kind() != reflect.Struct {
		return nil, fmt.Errorf("unsupported pointer %v", value)
	}
	// create receiver instance
	ptr := reflect.New(value.Elem().Type())
	funcs := make(map[reflect.Type]Receive)
	for i := 0; i < ptr.NumMethod(); i++ {
		if m := ptr.Method(i); m.IsValid() {
			if mt, r, err := buildReceiveFromFunc(m); err == nil {
				if br, ok := funcs[mt]; ok {
					funcs[mt] = chainReceiveWhenDone(br, r)
					continue
				}
				funcs[mt] = r
			}
		}
	}

	return &reflectReceiver{
		ptr:   ptr,
		funcs: funcs,
	}, nil
}
