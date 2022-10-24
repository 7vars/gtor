package rx

import (
	"errors"
	"sort"
	"sync"
)

type FanOutStage interface {
	FlowStage
}

type broadcast struct {
	sync.RWMutex
	pipes                 []Pipe
	inline                Inline
	active                bool
	flowOrRunnablePresent bool
	pulls                 chan []Outlet
}

func Broadcast() FanOutStage {
	return &broadcast{
		pipes: make([]Pipe, 0),
		pulls: make(chan []Outlet, 1),
	}
}

func broadcastInlineWorker(pulls <-chan []Outlet, inline Inline) {
	// fmt.Println("DEBUG BROADCAST-INLINE STARTED")
	// defer fmt.Println("DEBUG BROADCAST-INLINE CLOSED")
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

func broadcastOutlineWorker(pulls chan<- []Outlet, getPipes func() []Pipe, removePipe func(int)) {
	// fmt.Println("DEBUG BROADCAST-OUTLINE STARTED")
	// defer fmt.Println("DEBUG BROADCAST-OUTLINE CLOSED")
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
				// TODO if primary send cancel to inline
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

func (b *broadcast) run() {
	b.Lock()
	defer b.Unlock()
	// fmt.Println("DEBUG BROADCAST-RUN EXECUTED", b.active, b.flowOrRunnablePresent)
	if !b.active && b.flowOrRunnablePresent {
		b.active = true
		go broadcastOutlineWorker(b.pulls, b.getPipes, b.removePipe)
	}
}

func (b *broadcast) getPipes() []Pipe {
	b.Lock()
	defer b.Unlock()
	result := make([]Pipe, len(b.pipes))
	copy(result, b.pipes)
	return result
}

func (b *broadcast) addPipe(pipe Pipe) {
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

func (b *broadcast) connect(sink SinkStage, createPipe func() Pipe) error {
	pipe := createPipe()
	if err := sink.Connected(pipe); err != nil {
		return err
	}

	b.addPipe(pipe)

	return nil
}

func (b *broadcast) Connect(sink SinkStage) error {
	pipe := newPipe()
	if err := b.connect(sink, func() Pipe { return pipe }); err != nil {
		return err
	}

	return nil
}

func (b *broadcast) Via(flow FlowStage) SourceStage {
	pipe := newPipe()
	if err := b.connect(flow, func() Pipe { return pipe }); err != nil {
		errSrc := errorSource(err)
		return errSrc.Via(flow)
	}

	b.Lock()
	defer b.run()
	defer b.Unlock()
	b.flowOrRunnablePresent = true

	return flow
}

func (b *broadcast) To(sink SinkStage) error {
	pipe := newPipe()
	if err := b.connect(sink, func() Pipe { return pipe }); err != nil {
		return err
	}

	pipe.Pull()

	return nil
}

func (b *broadcast) RunWith(sink SinkStage) Runnable {
	pipe := newPipe()
	runnable := newRunnable(pipe)
	pipe.SetEmitter(runnable)

	if err := b.connect(sink, func() Pipe { return pipe }); err != nil {
		return runnableWithError{err}
	}

	b.Lock()
	defer b.run()
	defer b.Unlock()
	b.flowOrRunnablePresent = true

	return runnable
}

func (b *broadcast) Connected(inline EmittableInline) error {
	b.Lock()
	defer b.Unlock()
	if b.inline != nil {
		return errors.New("broadcast is already connected")
	}
	b.inline = inline
	go broadcastInlineWorker(b.pulls, b.inline)
	return nil
}
