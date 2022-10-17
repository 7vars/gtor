package rx

type OutletChan chan<- Event

func (oc OutletChan) Push(v interface{}) {
	oc <- Event{Data: v}
}

func (oc OutletChan) Error(err error) {
	oc <- Event{Err: err}
}

func (oc OutletChan) Complete() {
	oc <- Event{Comlete: true}
}

type InletChan chan<- Command

func (ic InletChan) Emit(interface{}) {}

func (ic InletChan) Close() {}

func (ic InletChan) Pull() {
	ic <- PULL
}

func (ic InletChan) Cancel() {
	ic <- CANCEL
}

type EmitterChan chan<- interface{}

func (ec EmitterChan) Emit(v interface{}) {
	ec <- v
}

func (ec EmitterChan) Close() {
	close(ec)
}

type emittable struct {
	emitter Emitter
	inlet   Inlet
}

func Emittable(emitter Emitter, inlet Inlet) Inlet {
	return &emittable{
		emitter: emitter,
		inlet:   inlet,
	}
}

func (em *emittable) Emit(v interface{}) {
	em.emitter.Emit(v)
}

func (em *emittable) Close() {
	em.emitter.Close()
}

func (em *emittable) Pull() {
	em.inlet.Pull()
}

func (em *emittable) Cancel() {
	em.inlet.Cancel()
}

type iolet struct {
	outlet Outlet
	inlet  Inlet
}

func newIOlet(outlet Outlet, inlet Inlet) IOlet {
	return &iolet{
		outlet: outlet,
		inlet:  inlet,
	}
}

func (io *iolet) Push(v interface{}) {
	io.outlet.Push(v)
}

func (io *iolet) Error(err error) {
	io.outlet.Error(err)
}

func (io *iolet) Complete() {
	io.outlet.Complete()
}

func (io *iolet) Pull() {
	io.inlet.Pull()
}

func (io *iolet) Cancel() {
	io.inlet.Cancel()
}
