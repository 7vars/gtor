package gtor

import (
	"fmt"
	"strings"
	"sync"
)

type GtorSupport interface {
	Logger
	Ref

	Gtor() GtorSystem

	Children() []Ref
	Child(string) (Ref, bool)
}

type GtorExecuter interface {
	ExecuteCreator(string, ReceiveCreator, ...Option) (Ref, error)
	Execute(string, interface{}, ...Option) (Ref, error)
	At(string) (Ref, bool)
}

type GtorContext interface {
	GtorSupport
	GtorExecuter

	Become(Receive)
	Unbecome()
}

type gtorContext struct {
	*gtorHandler

	become   func(Receive)
	unbecome func()
}

func (gtx *gtorContext) Become(receive Receive) {
	gtx.become(receive)
}

func (gtx *gtorContext) Unbecome() {
	gtx.unbecome()
}

type GtorHandler interface {
	GtorSupport
	GtorExecuter

	Close()
	Closed() <-chan struct{}
}

type gtorHandler struct {
	sync.RWMutex
	Logger
	Ref

	name string

	creator ReceiveCreator

	channel chan Msg
	close   chan struct{}
	closed  chan struct{}

	system   GtorSystem
	parent   *gtorHandler
	children map[string]*gtorHandler
}

func newHandler(name string, creator ReceiveCreator, system GtorSystem, parent *gtorHandler, opts ...Option) (*gtorHandler, error) {
	hdl := &gtorHandler{
		name:     name,
		creator:  creator,
		channel:  make(chan Msg, 100), // TODO configure buffer size
		close:    make(chan struct{}),
		closed:   make(chan struct{}),
		system:   system,
		parent:   parent,
		children: make(map[string]*gtorHandler),
	}
	if parent != nil {
		hdl.Logger = parent.WithField("path", hdl.Path())
	} else {
		hdl.Logger = newLogger().With(map[string]interface{}{
			"gtor": name,
			"path": hdl.Path(),
		})
	}
	hdl.Debugf("handler %s created, execute creator", name)

	receiver, err := hdl.creator(system)
	if err != nil {
		return nil, err
	}

	hdl.Ref = newRef(hdl.Name(), hdl.Path(), hdl.channel)

	go hdl.run(receiver)

	return hdl, nil
}

func (hdl *gtorHandler) run(receiver Receiver) {
	defer func() {
		hdl.closed <- struct{}{}
	}()

	currentReceive := receiver.Receive
	ctx := &gtorContext{
		gtorHandler: hdl,
		become: func(r Receive) {
			currentReceive = r
		},
		unbecome: func() {
			currentReceive = receiver.Receive
		},
	}

	kill := func() {
		for _, child := range hdl.children {
			child.Send(Kill{})
		}
	}

	if _, err := currentReceive(ctx, Startup{}); err != nil {
		hdl.setErr(err)
	}
	for {
		select {
		case msg, open := <-hdl.channel:
			if !open {
				goto close
			}

			switch cmd := msg.Data().(type) {
			case Close:
				defer msg.Reply(Done{})
				goto close
			case Kill:
				kill()
				return
			default:
				result, err := currentReceive(ctx, cmd)
				if err != nil {
					if _, ok := err.(RuntimeErr); ok {
						hdl.setErr(err)
						kill()
						return
					}
					msg.Reply(err)
				}
				msg.Reply(result)
			}
		case <-hdl.close:
			goto close
		}
	}
close:
	hdl.Lock()
	defer hdl.Unlock()
	for key, child := range hdl.children {
		child.Request(Close{})
		delete(hdl.children, key)
	}
	// sending stop
	if _, err := currentReceive(ctx, Stop{}); err != nil {
		hdl.setErr(err)
	}
}

func (hdl *gtorHandler) root() *gtorHandler {
	if hdl.parent == nil {
		return hdl
	}
	return hdl.parent.root()
}

func (hdl *gtorHandler) setErr(err error) {
	// TODO
}

func (hdl *gtorHandler) Name() string {
	return hdl.name
}

func (hdl *gtorHandler) Path() string {
	if hdl.parent == nil {
		return "/"
	}
	ppth := hdl.parent.Path()
	if ppth == "/" {
		ppth = ""
	}
	return fmt.Sprintf("%s/%s", ppth, hdl.Name())
}

func (hdl *gtorHandler) Close() {
	hdl.close <- struct{}{}
}

func (hdl *gtorHandler) Closed() <-chan struct{} {
	return hdl.closed
}

// GtorSupport implementation

func (hdl *gtorHandler) Gtor() GtorSystem {
	return hdl.system
}

func (hdl *gtorHandler) Children() []Ref {
	hdl.RLock()
	defer hdl.RUnlock()
	result := make([]Ref, 0, len(hdl.children))
	for _, child := range hdl.children {
		result = append(result, child)
	}
	return result
}

func (hdl *gtorHandler) child(name string) (*gtorHandler, bool) {
	hdl.RLock()
	defer hdl.RUnlock()
	if child, ok := hdl.children[name]; ok {
		return child, true
	}
	return nil, false
}

func (hdl *gtorHandler) Child(name string) (Ref, bool) {
	return hdl.child(name)
}

// GtorExecuter implementation

func (hdl *gtorHandler) ExecuteCreator(name string, creator ReceiveCreator, opts ...Option) (Ref, error) {
	if _, ok := hdl.child(name); ok {
		return nil, fmt.Errorf("child '%s' exists", name)
	}

	child, err := newHandler(name, creator, hdl.system, hdl, opts...)
	if err != nil {
		return nil, err
	}

	hdl.Lock()
	defer hdl.Unlock()
	hdl.children[name] = child

	return child, nil
}

func (hdl *gtorHandler) Execute(name string, v interface{}, opts ...Option) (Ref, error) {
	return hdl.ExecuteCreator(name, Creator(v), opts...)
}

func (hdl *gtorHandler) at(path string) (*gtorHandler, bool) {
	if len(path) > 0 {
		if path[0] == '/' {
			if len(path) == 1 {
				return hdl.root(), true
			}
			return hdl.root().at(path[1:])
		} else if len(path) >= 2 && path[0:2] == ".." && hdl.parent != nil {
			if len(path) == 2 || path == "../" {
				return hdl.parent, true
			}
			return hdl.parent.at(path[3:])
		} else if path[0] == '.' || strings.HasPrefix(path, hdl.Name()) {
			if i := strings.IndexRune(path, '/'); i > -1 {
				if i < len(path)-1 {
					return hdl.at(path[i+1:])
				}
			} else {
				return hdl, true
			}
		} else {
			if i := strings.IndexRune(path, '/'); i != -1 {
				child, ok := hdl.child(path[:i])
				if !ok {
					return nil, false
				}
				if i < len(path)-1 {
					return child.at(path[i+1:])
				}
				return child, true
			}
			return hdl.child(path)
		}
	}
	return nil, false
}

func (hdl *gtorHandler) At(path string) (Ref, bool) {
	return hdl.at(path)
}
