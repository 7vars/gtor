package rx

import "sync"

type pipe struct {
	sync.RWMutex
	commands <-chan Command
	events   chan Event
	emitter  Emitter
}

func newPipe(commands <-chan Command, emitter Emitter) *pipe {
	return &pipe{
		commands: commands,
		events:   make(chan Event, 100), // TODO configure size
		emitter:  emitter,
	}
}

func (p *pipe) setEmitter(emitter Emitter) {
	p.Lock()
	defer p.Unlock()
	p.emitter = emitter
}

// ===== Outline =====

func (p *pipe) Commands() <-chan Command {
	return p.commands
}

func (p *pipe) Push(v interface{}) {
	p.events <- Event{Data: v}
}

func (p *pipe) Error(err error) {
	p.events <- Event{Err: err}
}

func (p *pipe) Complete() {
	p.events <- Event{Comlete: true}
}

// ===== Inline =====

func (p *pipe) Events() <-chan Event {
	return p.events
}

func (p *pipe) Emit(v interface{}) {
	p.RLock()
	defer p.RUnlock()
	if p.emitter != nil {
		p.emitter.Emit(v)
	}
}

func (p *pipe) Close() {
	p.RLock()
	defer p.RUnlock()
	if p.emitter != nil {
		p.emitter.Close()
	}
}
