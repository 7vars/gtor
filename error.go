package gtor

import "fmt"

type RuntimeErr struct {
	err error
}

func RuntimeError(v interface{}) error {
	if err, ok := v.(error); ok {
		return RuntimeErr{err}
	}
	return RuntimeErr{fmt.Errorf("runtime-error: %v", v)}
}

func (e RuntimeErr) Error() string {
	return e.err.Error()
}

func (e RuntimeErr) Previous() error {
	return e.err
}
