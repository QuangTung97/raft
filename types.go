package raft

import (
	"time"
)

// TimerCancelFunc for canceling timers
// returns true when cancelled successfully
type TimerCancelFunc func() bool

// Timer ...
type Timer interface {
	AddTimer(d time.Duration, callback func()) TimerCancelFunc
}

// TermNumber ...
type TermNumber uint64

// LogIndex ...
type LogIndex uint64

// NodeID a node id
type NodeID [16]byte

// NullNodeID ...
type NullNodeID struct {
	Valid  bool
	NodeID NodeID
}

// LogEntry ...
type LogEntry struct {
	Index LogIndex
	Term  TermNumber
	Data  []byte
}

// ===============================
// Raft Storage
// ===============================

// StorageState ...
type StorageState struct {
	NodeID      NodeID
	CurrentTerm TermNumber
	VotedFor    NullNodeID
}

// NullStorageState ...
type NullStorageState struct {
	Valid bool
	State StorageState
}

// Storage ...
type Storage interface {
	GetState() (StorageState, error)

	AppendEntries(entries []LogEntry, handler func(err error))
	DeleteEntries(from LogIndex, handler func(err error))
}

// ===============================
// Raft RPC Client
// ===============================

// RequestVoteInput ...
type RequestVoteInput struct {
	Term         TermNumber
	CandidateID  NodeID
	LastLogIndex LogIndex
	LastLogTerm  TermNumber
}

// RequestVoteOutput is the response
type RequestVoteOutput struct {
	Term        TermNumber
	VoteGranted bool
}

// RequestVoteHandler is the response handler
type RequestVoteHandler func(output RequestVoteOutput, err error)

// AppendEntriesInput ...
type AppendEntriesInput struct {
	Term     TermNumber
	LeaderID NodeID

	PrevLogIndex LogIndex
	PrevLogTerm  TermNumber

	Entries []LogEntry

	LeaderCommit LogIndex
}

// AppendEntriesOutput ...
type AppendEntriesOutput struct {
	Term    TermNumber
	Success bool
}

// AppendEntriesHandler ...
type AppendEntriesHandler func(output AppendEntriesOutput, err error)

// Client ...
type Client interface {
	RequestVote(input RequestVoteInput, handler RequestVoteHandler)
	AppendEntries(input AppendEntriesInput, handler AppendEntriesHandler)
}
