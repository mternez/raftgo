package raft

type Command interface{}

type Result interface{}

type StateMachine interface {
	Apply(command []byte) ([]byte, error)
	Snapshot() ([]byte, error)
	Restore(snapshot []byte) error
}
