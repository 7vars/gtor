package rx

import (
	"fmt"
	"sync"
)

type FanInStage interface {
	FlowStage
}

type fanin struct {
	active  bool
	worker  func(outline Outline, inlines ...StageInline)
	inlines []StageInline
	pipe    Pipe
}

func NewFanIn(worker func(outline Outline, inlines ...StageInline)) FanInStage {
	return &fanin{
		worker:  worker,
		inlines: make([]StageInline, 0),
	}
}

func (fi *fanin) start() {
	if !fi.active {
		fi.active = true
		for _, inline := range fi.inlines {
			inline.start()
		}
		go fi.worker(fi.pipe, fi.inlines...)
	}
}

func (fi *fanin) connect(sink SinkStage) Inline {
	if fi.pipe != nil {
		// TODO autocreate fanout/bradcast
		panic("merge is already connected")
	}
	fi.pipe = newPipe()

	return fi.pipe
}

func (fi *fanin) Connected(inline StageInline) {
	fi.inlines = append(fi.inlines, inline)
}

// ===== join =====

func Join() FanInStage {
	return newJoinStage(func(v []interface{}) (interface{}, error) { return v, nil })
}

func Zip[T any](f func([]interface{}) (T, error)) FanInStage {
	return newJoinStage(func(tuple []interface{}) (interface{}, error) {
		return f(tuple)
	})
}

func newJoinStage(handler func([]interface{}) (interface{}, error)) FanInStage {
	return NewFanIn(joinWorker(handler))
}

func joinWorker(handler func([]interface{}) (interface{}, error)) func(Outline, ...StageInline) {
	return func(outline Outline, inlines ...StageInline) {
		for cmd := range outline.Commands() {
			switch cmd {
			case PULL:
				if len(inlines) == 0 {
					outline.Complete()
					return
				}

				results := make([]interface{}, 0)
				errs := make([]error, 0)
				for _, inline := range inlines {
					inline.Pull()
					evt := <-inline.Events()
					switch evt.Type() {
					case PUSH:
						results = append(results, evt.Data)
					case ERROR:
						errs = append(errs, evt.Err)
					case COMPLETE:
						outline.Complete()
						return
					}
				}

				result, err := handler(results)
				if err != nil {
					errs = append(errs, err)
				}

				if len(errs) > 0 {
					err := errs[0]
					for i := 1; i < len(errs); i++ {
						err = fmt.Errorf("caused by: %w", errs[i])
					}
					outline.Error(err)
					continue
				}

				outline.Push(result)
			case CANCEL:
				for _, inline := range inlines {
					inline.Cancel()
				}
				outline.Complete()
				return
			}
		}
	}
}

// ===== merge =====

func Merge() FanInStage {
	return NewFanIn(mergeWorker)
}

func mergeInlineWorker(wg *sync.WaitGroup, pulls <-chan chan<- Event, inline Inline) {
	// defer fmt.Println("DEBUG MERGE-INLINE-WORK CLOSED")
	defer wg.Done()
	for pull := range pulls {
		inline.Pull()
		evt := <-inline.Events()
		switch evt.Type() {
		case PUSH, ERROR:
			pull <- evt
		case COMPLETE:
			pull <- evt
			return
		}
	}
	inline.Cancel()
}

func mergeOutlineWorker(pulls chan<- chan<- Event, events chan Event, outline Outline) {
	// defer fmt.Println("DEBUG MERGE-INLINE-WORK CLOSED")
	defer close(pulls)
	for cmd := range outline.Commands() {
		switch cmd {
		case PULL:
		pull:
			pulls <- events
			evt, open := <-events
			if !open {
				outline.Complete()
				return
			}
			switch evt.Type() {
			case PUSH:
				outline.Push(evt.Data)
			case ERROR:
				outline.Error(evt.Err)
			case COMPLETE:
				goto pull
			}

		case CANCEL:
			outline.Complete()
			return
		}
	}
}

func mergeWorker(outline Outline, inlines ...StageInline) {
	// defer fmt.Println("DEBUG MERGE-WORK CLOSED")
	var wg sync.WaitGroup
	pulls := make(chan chan<- Event, 1)
	events := make(chan Event)

	wg.Add(len(inlines))
	for _, inline := range inlines {
		go mergeInlineWorker(&wg, pulls, inline)
	}

	go mergeOutlineWorker(pulls, events, outline)

	wg.Wait()
	close(events)
}
