package common

type CmdBatcher interface {
	Put(string, ...interface{}) error
	Exec() ([]interface{}, error)
	Len() int
	Release()
}
