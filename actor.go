package gtor

type Startable interface {
	OnStartup(GtorContext) error
}

type Stopable interface {
	OnStop(GtorContext) error
}

