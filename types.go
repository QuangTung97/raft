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

// NodeID a node id, should be an uuid
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

	ClusterNodes []NodeID
	ClusterIndex LogIndex
}

// NullStorageState ...
type NullStorageState struct {
	Valid bool
	State StorageState
}

type MembershipLogType int

const (
	// MembershipLogTypeNormal ...
	MembershipLogTypeNormal MembershipLogType = iota + 1

	// MembershipLogTypeJointConsensus ...
	MembershipLogTypeJointConsensus
)

type MembershipLogEntry struct {
	Type      MembershipLogType
	OldConfig []NodeID // non-empty only for joint-consensus
	Config    []NodeID
}

// Storage ...
type Storage interface {
	GetState() NullStorageState
	PutState(state StorageState)

	AppendEntries(entries []LogEntry, handler func())
	DeleteEntries(from LogIndex, handler func())

	GetEntries(from LogIndex, limit uint64, handler func(entries []LogEntry))

	IsMembershipLogEntry(entry LogEntry) (MembershipLogEntry, bool)
}

// ===============================
// Raft RPC Client
// ===============================

// RequestVoteInput ...
type RequestVoteInput struct {
	NodeID       NodeID // destination node id
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
	NodeID NodeID // destination node id

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

// StateMachine ...
type StateMachine interface {
	LeaderChanged(term TermNumber, leaderID NodeID)
	Apply(entries []LogEntry)
	GetLastAppliedIndex() LogIndex
}
