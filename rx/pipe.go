package rx

import "sync"

type Command int
type EventType int

const (
	PULL Command = iota
	CANCEL

	PUSH EventType = iota
	ERROR
	COMPLETE
)

type Event struct {
	Data     interface{}
	Err      error
	Complete bool
}

func (evt Event) IsError() bool {
	return evt.Err != nil
}

func (evt Event) IsCompleted() bool {
	return evt.Complete
}

func (evt Event) Type() EventType {
	if evt.IsError() {
		return ERROR
	}
	if evt.IsCompleted() {
		return COMPLETE
	}
	return PUSH
}

type Outlet interface {
	Push(interface{})
	Error(error)
	Complete()
}

type Outline interface {
	Outlet
	Commands() <-chan Command
}

type Inlet interface {
	Pull()
	Cancel()
}

type Inline interface {
	Inlet
	Events() <-chan Event
}

type IOlet interface {
	Inlet
	Outlet
}

type EmittableInline interface {
	Emittable
	Inline
	SetEmitter(Emitter)
}

type Emitter interface {
	Emit(interface{})
	EmitError(error)
	Close()
}

type Emittable interface {
	Inlet
	Emitter
}

type Pipe interface {
	EmittableInline
	Outline
}

type pipe struct {
	sync.RWMutex
	eventsClosed   bool
	commandsClosed bool
	events         chan Event
	commands       chan Command
	emitter        Emitter
}

func newPipe() Pipe {
	return &pipe{
		events:   make(chan Event, 1),
		commands: make(chan Command, 1),
	}
}

// ===== Inline =====

func (p *pipe) closeCommands() {
	p.Lock()
	defer p.Unlock()
	if !p.commandsClosed {
		close(p.commands)
		p.commandsClosed = true
	}
}

func (p *pipe) isCommandsClosed() bool {
	p.RLock()
	defer p.RUnlock()
	return p.commandsClosed
}

func (p *pipe) Events() <-chan Event {
	return p.events
}

func (p *pipe) Pull() {
	if !p.isCommandsClosed() {
		p.commands <- PULL
	}
}

func (p *pipe) Cancel() {
	defer p.closeCommands()
	if !p.isCommandsClosed() {
		p.commands <- CANCEL
	}
}

// ===== Outline =====

func (p *pipe) closeEvents() {
	p.Lock()
	defer p.Unlock()
	if !p.eventsClosed {
		close(p.events)
		p.eventsClosed = true
	}
}

func (p *pipe) isEventsClosed() bool {
	p.RLock()
	defer p.RUnlock()
	return p.eventsClosed
}

func (p *pipe) Commands() <-chan Command {
	return p.commands
}

func (p *pipe) Push(v interface{}) {
	if !p.isEventsClosed() {
		p.events <- Event{Data: v}
	}
}

func (p *pipe) Error(e error) {
	if !p.isEventsClosed() {
		p.events <- Event{Err: e}
	}
}

func (p *pipe) Complete() {
	defer p.closeEvents()
	defer p.closeCommands()
	if !p.isEventsClosed() {
		p.events <- Event{Complete: true}
	}
}

// ====== Emitter =====

func (p *pipe) SetEmitter(emitter Emitter) {
	p.emitter = emitter
}

func (p *pipe) Emit(v interface{}) {
	if p.emitter != nil {
		p.emitter.Emit(v)
	}
}

func (p *pipe) EmitError(e error) {
	if p.emitter != nil {
		p.emitter.EmitError(e)
	}
}

func (p *pipe) Close() {
	if p.emitter != nil {
		p.emitter.Close()
	}
}

type combinedPipe struct {
	inline  Inline
	outline Outline
	emitter Emitter
}

func combineIO(inline Inline, outline Outline, emitter Emitter) Pipe {
	return &combinedPipe{
		inline:  inline,
		outline: outline,
		emitter: emitter,
	}
}

func (p *combinedPipe) Events() <-chan Event {
	return p.inline.Events()
}

func (p *combinedPipe) Commands() <-chan Command {
	return p.outline.Commands()
}

func (p *combinedPipe) Push(v interface{}) {
	p.outline.Push(v)
}

func (p *combinedPipe) Error(err error) {
	p.outline.Error(err)
}

func (p *combinedPipe) Complete() {
	p.outline.Complete()
}

func (p *combinedPipe) Pull() {
	p.inline.Pull()
}

func (p *combinedPipe) Cancel() {
	p.inline.Cancel()
}

func (p *combinedPipe) SetEmitter(emitter Emitter) {
	p.emitter = emitter
}

func (p *combinedPipe) Emit(v interface{}) {
	if p.emitter != nil {
		p.emitter.Emit(v)
	}
}

func (p *combinedPipe) EmitError(e error) {
	if p.emitter != nil {
		p.emitter.EmitError(e)
	}
}

func (p *combinedPipe) Close() {
	if p.emitter != nil {
		p.emitter.Close()
	}
}
