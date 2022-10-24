package rx

import (
	"context"
	"fmt"
	"sync"

	"github.com/7vars/gtor"
)

type Runnable interface {
	Run() <-chan interface{}
	RunWithContext(context.Context) <-chan interface{}
	Execute() (interface{}, error)
	ExecuteWithContext(context.Context) (interface{}, error)
}

type runnable struct {
	sync.RWMutex
	closed bool
	emits  chan interface{}
	inlet  Inlet
}

func newRunnable(inlet Inlet) *runnable {
	return &runnable{
		emits: make(chan interface{}, 1),
		inlet: inlet,
	}
}

func (r *runnable) isClosed() bool {
	r.RLock()
	defer r.RUnlock()
	return r.closed
}

func (r *runnable) Emit(v interface{}) {
	if !r.isClosed() {
		r.emits <- v
	}
}

func (r *runnable) EmitError(e error) {
	if !r.isClosed() {
		r.emits <- e
		r.Close()
	}
}

func (r *runnable) accidentlyClose() {
	r.inlet.Cancel()
	r.Close()
}

func (r *runnable) Close() {
	r.Lock()
	defer r.Unlock()
	if !r.closed {
		close(r.emits)
		r.closed = true
	}
}

func (r *runnable) RunWithContext(ctx context.Context) <-chan interface{} {
	out := make(chan interface{}, 1)
	r.inlet.Pull()

	go func() {
		defer fmt.Println("DEBUG RUNNABLE-WORK CLOSED")
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				defer r.accidentlyClose()
				out <- ctx.Err()
				return
			case e, open := <-r.emits:
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

type runnableWithError struct {
	Err error
}

func (rwe runnableWithError) Run() <-chan interface{} {
	out := make(chan interface{}, 1)
	defer close(out)
	out <- rwe.Err
	return out
}

func (rwe runnableWithError) RunWithContext(context.Context) <-chan interface{} {
	return rwe.Run()
}

func (rwe runnableWithError) Execute() (interface{}, error) {
	return nil, rwe.Err
}

func (rwe runnableWithError) ExecuteWithContext(context.Context) (interface{}, error) {
	return rwe.Execute()
}
