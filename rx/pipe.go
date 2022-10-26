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

type StageInline interface {
	Stage
	Inline
}

type stageInline struct {
	Inline
	onStart func()
}

func newStageInline(inline Inline, onStart func()) StageInline {
	return &stageInline{
		Inline:  inline,
		onStart: onStart,
	}
}

func (si *stageInline) start() {
	si.onStart()
}

type IOlet interface {
	Inlet
	Outlet
}

type Emitter interface {
	Emits() <-chan interface{}
	Emit(interface{})
	EmitError(error)
	Close()
}

type Emittable interface {
	Inlet
	Emitter
}

type EmittableInline interface {
	Emittable
	Inline
}

type emittableInline struct {
	inline  Inline
	emitter Emitter
}

func newEmittableInline(inline Inline, emitter Emitter) EmittableInline {
	return &emittableInline{
		inline:  inline,
		emitter: emitter,
	}
}

func (ei *emittableInline) Events() <-chan Event {
	return ei.inline.Events()
}

func (ei *emittableInline) Pull() {
	ei.inline.Pull()
}

func (ei *emittableInline) Cancel() {
	ei.inline.Cancel()
}

func (ei *emittableInline) Emits() <-chan interface{} {
	return ei.emitter.Emits()
}

func (ei *emittableInline) Emit(v interface{}) {
	ei.emitter.Emit(v)
}

func (ei *emittableInline) EmitError(e error) {
	ei.emitter.EmitError(e)
}

func (ei *emittableInline) Close() {
	ei.emitter.Close()
}

type Pipe interface {
	Inline
	Outline
}

type StagePipe interface {
	Stage
	Pipe
}

type pipe struct {
	sync.RWMutex
	eventsClosed   bool
	commandsClosed bool
	events         chan Event
	commands       chan Command
	onStart        func()
}

func newPipe() Pipe {
	return newStagePipe(func() {})
}

func newStagePipe(onStart func()) StagePipe {
	return &pipe{
		events:   make(chan Event, 1),
		commands: make(chan Command, 1),
		onStart:  onStart,
	}
}

// ===== Stage =====

func (p *pipe) start() {
	p.onStart()
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

type combinedPipe struct {
	inline  Inline
	outline Outline
}

func combineIO(inline Inline, outline Outline) Pipe {
	return &combinedPipe{
		inline:  inline,
		outline: outline,
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
