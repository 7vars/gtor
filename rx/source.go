package rx

import (
	"errors"
	"io"
)

type Source struct {
	OnStart  func()
	OnPull   func(Outlet)
	OnCancel func(Outlet)
}

func (s Source) HandleStartup() {
	if s.OnStart != nil {
		s.OnStart()
	}
}

func (s Source) HandlePull(out Outlet) {
	if s.OnPull != nil {
		s.OnPull(out)
		return
	}
	out.Comlete()
}

func (s Source) HandleCancel(out Outlet) {
	if s.OnCancel != nil {
		s.OnCancel(out)
		return
	}
	out.Comlete()
}

type SourceFunc[T any] func() (T, error)

func (f SourceFunc[T]) HandleStartup() {}

func (f SourceFunc[T]) HandlePull(out Outlet) {
	t, err := f()
	if err != nil {
		if errors.Is(err, io.EOF) {
			out.Comlete()
			return
		}
		out.Error(err)
		return
	}
	out.Push(t)
}

func (f SourceFunc[T]) HandleCancel(out Outlet) {
	out.Comlete()
}

// ==============================================

type SourceStage interface {
}

type sourceStage struct {
	handler SourceHandler
}

func NewSource(handler SourceHandler) SourceStage {
	return &sourceStage{
		handler: handler,
	}
}
