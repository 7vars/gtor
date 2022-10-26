package rx

import (
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
		go mergeWorker(fi.pipe, fi.inlines...)
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
