package rx

import (
	"context"
	"fmt"

	"github.com/7vars/gtor"
)

type runner struct {
	emitter Emitter
	onStart func()
}

func newRunner(emitter Emitter, onStart func()) Runner {
	return &runner{
		emitter: emitter,
		onStart: onStart,
	}
}

func (r *runner) Run() {
	r.onStart()
	<-r.emitter.Emits()
}

type runnable struct {
	emitter Emitter
	onStart func()
}

func newRunnable(emitter Emitter, onStart func()) *runnable {
	return &runnable{
		emitter: emitter,
		onStart: onStart,
	}
}

func (r *runnable) accidentlyClose() {
	r.emitter.Close()
}

func (r *runnable) RunWithContext(ctx context.Context) <-chan interface{} {
	out := make(chan interface{}, 1)
	r.onStart()

	go func() {
		defer fmt.Println("DEBUG RUNNABLE-WORK CLOSED")
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				defer r.accidentlyClose()
				out <- ctx.Err()
				return
			case e, open := <-r.emitter.Emits():
				if !open {
					return
				}
				out <- e
			}
		}
	}()

	return out
}

func (r *runnable) Run() <-chan interface{} {
	return r.RunWithContext(context.Background())
}

func (r *runnable) ExecuteWithContext(ctx context.Context) (interface{}, error) {
	results := make(chan interface{}, 1)
	go func() {
		defer close(results)
		result := make([]interface{}, 0)
		for e := range r.RunWithContext(ctx) {
			if err, ok := e.(error); ok {
				results <- err
				return
			}
			result = append(result, e)
		}
		results <- result
	}()

	switch res := (<-results).(type) {
	case error:
		return nil, res
	case []interface{}:
		if len(res) == 0 {
			return gtor.DONE(), nil
		}
		if len(res) == 1 {
			return res[0], nil
		}
		return res, nil
	default:
		return res, nil
	}
}

func (r *runnable) Execute() (interface{}, error) {
	return r.ExecuteWithContext(context.Background())
}
