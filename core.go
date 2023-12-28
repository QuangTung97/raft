package raft

// Raft implements the Raft Consensus Algorithm
type Raft struct {
	client Client
}

func NewRaft(client Client) *Raft {
	return &Raft{
		client: client,
	}
}
