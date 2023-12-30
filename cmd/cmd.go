package cmd

type Cmd interface {
	Name() string
	Run() error  // Run is a blocking function
	Stop() error // non-block, just notify cmd to stop
}
