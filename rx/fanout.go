package rx

import (
	"sort"
)

type FanOutStage interface {
	FlowStage
}

type fanout struct {
	active bool
	worker func(Inline, ...Outline)
	pipes  []StagePipe
	inline Inline
}

func NewFanOut(worker func(Inline, ...Outline)) FanOutStage {
	return &fanout{
		worker: worker,
		pipes:  make([]StagePipe, 0),
	}
}

func (fo *fanout) start() {
	if !fo.active {
		fo.active = true
		outlines := make([]Outline, len(fo.pipes))
		for i, pipe := range fo.pipes {
			pipe.start()
			outlines[i] = pipe
		}

		go fo.worker(fo.inline, outlines...)
	}
}

func (fo *fanout) connect(sink SinkStage) Inline {
	pipe := newStagePipe(sink.start)
	fo.pipes = append(fo.pipes, pipe)
	return pipe
}

func (fo *fanout) Connected(inline StageInline) {
	if fo.inline != nil {
		// TODO autocreate FanIn
		panic("broadcast is already connected")
	}
	fo.inline = inline
}

// ===== broadcast =====

func Broadcast() FanOutStage {
	return NewFanOut(broadcastWorker)
}

func broadcastInlineWorker(pulls <-chan []Outlet, inline Inline) {
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

func broadcastOutlineWorker(pulls chan<- []Outlet, outlines ...Outline) {
	defer close(pulls)
	for {
		if len(outlines) == 0 {
			return
		}

		outlets := make([]Outlet, 0)
		removes := make([]int, 0)
		for i, outline := range outlines {
			cmd, open := <-outline.Commands()
			if !open {
				removes = append(removes, i)
				continue
			}
			switch cmd {
			case PULL:
				outlets = append(outlets, outline)
			case CANCEL:
				// TODO if primary/last send cancel to inline
				outline.Complete()
				removes = append(removes, i)
			}
		}

		sort.Slice(removes, func(i, j int) bool {
			return removes[i] > removes[j]
		})
		for _, index := range removes {
			if index >= 0 && index < len(outlines) {
				outlines = append(outlines[:index], outlines[:index+1]...)
			}
		}

		if len(outlets) == 0 {
			return
		}

		pulls <- outlets
	}
}

func broadcastWorker(inline Inline, outlines ...Outline) {
	pulls := make(chan []Outlet, 1)
	go broadcastInlineWorker(pulls, inline)
	go broadcastOutlineWorker(pulls, outlines...)
}
