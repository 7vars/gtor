package rx

import (
	"fmt"
	"sort"
	"sync"
)

type FanOutStage interface {
	FlowStage
}

type broadcast struct {
	sync.RWMutex // TODO remove, not needed anymore
	active       bool
	pipes        []StagePipe
	inline       Inline
	pulls        chan []Outlet
}

func Broadcast() FanOutStage {
	return &broadcast{
		pipes: make([]StagePipe, 0),
		pulls: make(chan []Outlet, 1),
	}
}

func broadcastInlineWorker(pulls <-chan []Outlet, inline Inline) {
	fmt.Println("DEBUG BROADCAST-INLINE STARTED")
	defer fmt.Println("DEBUG BROADCAST-INLINE CLOSED")
	defer inline.Cancel()
	send := func(evt Event, outs ...Outlet) {
		for _, out := range outs {
			switch evt.Type() {
			case PUSH:
				out.Push(evt.Data)
			case ERROR:
				out.Error(evt.Err)
			case COMPLETE:
				out.Complete()
			}
		}
	}

	for pull := range pulls {
		if len(pull) == 0 {
			continue
		}
		inline.Pull()
		evt := <-inline.Events()
		send(evt, pull...)
	}
}

func broadcastOutlineWorker(pulls chan<- []Outlet, getPipes func() []StagePipe, removePipe func(int)) {
	fmt.Println("DEBUG BROADCAST-OUTLINE STARTED")
	defer fmt.Println("DEBUG BROADCAST-OUTLINE CLOSED")
	defer close(pulls)
	for {
		pipes := getPipes()

		if len(pipes) == 0 {
			return
		}

		outlets := make([]Outlet, 0)
		removes := make([]int, 0)
		for i, pipe := range pipes {
			cmd, open := <-pipe.Commands()
			if !open {
				removes = append(removes, i)
				continue
			}
			switch cmd {
			case PULL:
				outlets = append(outlets, pipe)
			case CANCEL:
				// TODO if primary/last send cancel to inline
				pipe.Complete()
				removes = append(removes, i)
			}
		}

		sort.Slice(removes, func(i, j int) bool {
			return removes[i] > removes[j]
		})
		for _, index := range removes {
			removePipe(index)
		}

		if len(outlets) == 0 {
			return
		}

		pulls <- outlets
	}
}

func (b *broadcast) start() {
	if !b.active {
		b.active = true
		for _, pipe := range b.getPipes() {
			pipe.start()
		}

		go broadcastInlineWorker(b.pulls, b.inline)
		go broadcastOutlineWorker(b.pulls, b.getPipes, b.removePipe)
	}
}

func (b *broadcast) getPipes() []StagePipe {
	b.Lock()
	defer b.Unlock()
	result := make([]StagePipe, len(b.pipes))
	copy(result, b.pipes)
	return result
}

func (b *broadcast) addPipe(pipe StagePipe) {
	b.Lock()
	defer b.Unlock()
	b.pipes = append(b.pipes, pipe)
}

func (b *broadcast) removePipe(index int) {
	b.Lock()
	defer b.Unlock()
	if index >= 0 && index < len(b.pipes) {
		b.pipes = append(b.pipes[:index], b.pipes[:index+1]...)
	}
}

func (b *broadcast) connect(sink SinkStage) Inline {
	pipe := newStagePipe(sink.start)
	b.addPipe(pipe)
	return pipe
}

func (b *broadcast) Connected(inline StageInline) {
	b.Lock()
	defer b.Unlock()
	if b.inline != nil {
		// TODO autocreate FanIn
		panic("broadcast is already connected")
	}
	b.inline = inline
}
