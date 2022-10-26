package gtor

// commands
type Startup struct{}

type Stop struct{}

type Kill struct{}

type Close struct{}

// events
type Done struct{}

func DONE() Done { return Done{} }
