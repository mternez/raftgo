package raft

type ServerNodeState struct {
	CurrentTerm int
	VotedFor    int
	Entries     []*LogEntry
}

type ServerNodeStorage interface {
	// Save into storage
	Save(state *ServerNodeState) error
	// Fetch current state
	Fetch() (*ServerNodeState, error)
}
