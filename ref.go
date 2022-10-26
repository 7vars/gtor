package gtor

import (
	"context"
	"time"
)

type Ref interface {
	Name() string
	Path() string

	Send(interface{})
	Request(interface{}) (interface{}, error)
	RequestWithTimeout(interface{}, time.Duration) (interface{}, error)
	RequestWithContext(context.Context, interface{}) (interface{}, error)
}

type ref struct {
	name    string
	path    string
	channel chan<- Msg
}

func newRef(name, path string, channel chan<- Msg) Ref {
	r := &ref{
		name:    name,
		path:    path,
		channel: channel,
	}

	return r
}

func (r *ref) Name() string {
	return r.name
}

func (r *ref) Path() string {
	return r.path
}

func (r *ref) Send(v interface{}) {
	go func() {
		r.channel <- SendOnly(v)
	}()
}

func (r *ref) Request(v interface{}) (interface{}, error) {
	return r.RequestWithContext(context.Background(), v)
}

func (r *ref) RequestWithTimeout(v interface{}, timeout time.Duration) (interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return r.RequestWithContext(ctx, v)
}

func (r *ref) RequestWithContext(ctx context.Context, v interface{}) (interface{}, error) {
	result := make(chan interface{}, 1)
	r.channel <- Message(v, result)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-result:
		if err, ok := res.(error); ok {
			return nil, err
		}
		return res, nil
	}
}
