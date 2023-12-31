package raft

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type raftTest struct {
	storage *StorageMock
	timer   *TimerMock
	client  *ClientMock
	raft    *Raft
}

func newRaftTest() *raftTest {
	r := &raftTest{
		storage: NewStorageMock(),
		timer:   &TimerMock{},
		client:  &ClientMock{},
	}

	r.timer.CancelFunc = func() bool { return false }

	r.raft = NewRaft(r.storage, r.timer, r.client)
	return r
}

var node1 = NodeID{100, 1}
var node2 = NodeID{100, 2}
var node3 = NodeID{100, 3}
var node4 = NodeID{100, 4}
var node5 = NodeID{100, 5}

const initTerm = TermNumber(70)

func newRaftTestWith3Nodes() *raftTest {
	r := newRaftTest()
	r.storage.GetStateFunc = func() NullStorageState {
		return NullStorageState{
			Valid: true,
			State: StorageState{
				NodeID:      node1,
				CurrentTerm: initTerm,
				VotedFor:    NullNodeID{},

				ClusterNodes: []NodeID{node1, node2, node3},
				ClusterIndex: 0,
			},
		}
	}
	return r
}

func (r *raftTest) resetTimerCalls() {
	r.timer.AddCallbacks = nil
	r.timer.AddDurations = nil
	r.timer.CancelCalls = 0
	r.timer.CancelIDs = nil
}

func (r *raftTest) startAtCandidate(t *testing.T) {
	r.raft.Start()

	assert.Equal(t, 1, len(r.timer.AddCallbacks))
	r.timer.AddCallbacks[0]()
	assert.Equal(t, raftStateCandidate, r.raft.state)
	r.resetTimerCalls()
}

func TestRaft_New(t *testing.T) {
	t.Run("check new", func(t *testing.T) {
		r := newRaftTest()
		assert.Equal(t, raftStateFollower, r.raft.state)
	})
}

func TestRaft_Start(t *testing.T) {
	const leaderTerm = initTerm + 1

	t.Run("call timer", func(t *testing.T) {
		r := newRaftTestWith3Nodes()
		r.raft.Start()

		assert.Equal(t, []time.Duration{
			10 * time.Second,
		}, r.timer.AddDurations)
		assert.Equal(t, 1, len(r.timer.AddCallbacks))
	})

	t.Run("switch to candidate state and call request vote when timer expire", func(t *testing.T) {
		r := newRaftTestWith3Nodes()
		r.raft.Start()

		assert.Equal(t, 1, len(r.timer.AddCallbacks))
		r.timer.AddCallbacks[0]()

		assert.Equal(t, raftStateCandidate, r.raft.state)

		// store state
		states := r.storage.PutStateInputs
		assert.Equal(t, 1, len(states))
		assert.Equal(t, initTerm+1, states[0].CurrentTerm)
		assert.Equal(t, NullNodeID{
			Valid:  true,
			NodeID: node1,
		}, states[0].VotedFor)

		// and then request votes
		inputs := r.client.RequestVoteInputs
		assert.Equal(t, 2, len(inputs))

		assert.Equal(t, node2, inputs[0].NodeID)
		assert.Equal(t, initTerm+1, inputs[0].Term)
		assert.Equal(t, node1, inputs[0].CandidateID)
		assert.Equal(t, TermNumber(0), inputs[0].LastLogTerm)
		assert.Equal(t, LogIndex(0), inputs[0].LastLogIndex)

		assert.Equal(t, node3, inputs[1].NodeID)
		assert.Equal(t, initTerm+1, inputs[1].Term)
		assert.Equal(t, node1, inputs[1].CandidateID)
		assert.Equal(t, TermNumber(0), inputs[1].LastLogTerm)
		assert.Equal(t, LogIndex(0), inputs[1].LastLogIndex)
	})

	t.Run("candidate recv votes from majority", func(t *testing.T) {
		r := newRaftTestWith3Nodes()
		r.raft.Start()

		assert.Equal(t, 1, len(r.timer.AddCallbacks))
		r.timer.AddCallbacks[0]()

		// and then request votes
		handlers := r.client.RequestVoteHandlers
		assert.Equal(t, 2, len(handlers))

		handlers[0](RequestVoteOutput{
			Term:        initTerm + 1,
			VoteGranted: true,
		}, nil)

		assert.Equal(t, raftStateLeader, r.raft.state)

		// do get entries
		getEntries := r.storage.GetEntriesHandlers
		assert.Equal(t, 2, len(getEntries))
		assert.Equal(t, []LogIndex{0, 0}, r.storage.GetEntriesFromIndices)
		assert.Equal(t, []uint64{128, 128}, r.storage.GetEntriesLimits)

		getEntries[0]()
		getEntries[1]()

		// do call append entries
		inputs := r.client.AppendInputs
		assert.Equal(t, 2, len(inputs))

		assert.Equal(t, node2, inputs[0].NodeID)
		assert.Equal(t, initTerm+1, inputs[0].Term)
		assert.Equal(t, node1, inputs[0].LeaderID)

		assert.Equal(t, TermNumber(0), inputs[0].PrevLogTerm)
		assert.Equal(t, LogIndex(0), inputs[0].PrevLogIndex)

		assert.Equal(t, 0, len(inputs[0].Entries))

		assert.Equal(t, LogIndex(0), inputs[0].LeaderCommit)

		// request node 3
		assert.Equal(t, node3, inputs[1].NodeID)
		assert.Equal(t, initTerm+1, inputs[1].Term)
		assert.Equal(t, node1, inputs[1].LeaderID)
	})

	t.Run("request vote rejected by higher term", func(t *testing.T) {
		r := newRaftTestWith3Nodes()
		r.raft.Start()

		assert.Equal(t, 1, len(r.timer.AddCallbacks))
		r.timer.AddCallbacks[0]()

		handlers := r.client.RequestVoteHandlers
		assert.Equal(t, 2, len(handlers))

		r.storage.PutStateInputs = nil

		handlers[0](RequestVoteOutput{
			Term:        initTerm + 2,
			VoteGranted: false,
		}, nil)

		assert.Equal(t, raftStateFollower, r.raft.state)
		assert.Nil(t, r.raft.candidate)

		// state updated
		states := r.storage.PutStateInputs
		assert.Equal(t, 1, len(states))
		assert.Equal(t, initTerm+2, states[0].CurrentTerm)
		assert.Equal(t, NullNodeID{}, states[0].VotedFor)
	})

	t.Run("first vote is not granted, second one granted", func(t *testing.T) {
		r := newRaftTestWith3Nodes()
		r.raft.Start()

		assert.Equal(t, 1, len(r.timer.AddCallbacks))
		r.timer.AddCallbacks[0]()

		handlers := r.client.RequestVoteHandlers
		assert.Equal(t, 2, len(handlers))

		handlers[0](RequestVoteOutput{
			Term:        initTerm + 1,
			VoteGranted: false,
		}, nil)

		assert.Equal(t, raftStateCandidate, r.raft.state)

		handlers[1](RequestVoteOutput{
			Term:        initTerm + 1,
			VoteGranted: true,
		}, nil)

		assert.Equal(t, raftStateLeader, r.raft.state)
	})

	t.Run("all votes not granted, become follower", func(t *testing.T) {
		r := newRaftTestWith3Nodes()
		r.raft.Start()

		assert.Equal(t, 1, len(r.timer.AddCallbacks))
		r.timer.AddCallbacks[0]()
		assert.Equal(t, raftStateCandidate, r.raft.state)

		handlers := r.client.RequestVoteHandlers
		assert.Equal(t, 2, len(handlers))

		handlers[0](RequestVoteOutput{
			Term:        initTerm + 1,
			VoteGranted: false,
		}, nil)
		handlers[1](RequestVoteOutput{
			Term:        initTerm + 1,
			VoteGranted: false,
		}, nil)

		assert.Equal(t, raftStateFollower, r.raft.state)
	})

	t.Run("request vote error, do retry", func(t *testing.T) {
		r := newRaftTestWith3Nodes()
		r.raft.Start()

		assert.Equal(t, 1, len(r.timer.AddCallbacks))
		r.timer.AddCallbacks[0]()
		assert.Equal(t, raftStateCandidate, r.raft.state)

		handlers := r.client.RequestVoteHandlers
		assert.Equal(t, 2, len(handlers))

		r.client.RequestVoteInputs = nil

		handlers[0](RequestVoteOutput{}, errors.New("some error"))

		inputs := r.client.RequestVoteInputs
		assert.Equal(t, 1, len(inputs))
		assert.Equal(t, node2, inputs[0].NodeID)
		assert.Equal(t, initTerm+1, inputs[0].Term)
		assert.Equal(t, node1, inputs[0].CandidateID)
	})

	t.Run("one vote not granted, one error, switch to follower state", func(t *testing.T) {
		r := newRaftTestWith3Nodes()
		r.raft.Start()

		assert.Equal(t, 1, len(r.timer.AddCallbacks))
		r.timer.AddCallbacks[0]()
		assert.Equal(t, raftStateCandidate, r.raft.state)

		handlers := r.client.RequestVoteHandlers
		assert.Equal(t, 2, len(handlers))

		handlers[0](RequestVoteOutput{
			Term:        initTerm + 1,
			VoteGranted: false,
		}, nil)
		handlers[1](RequestVoteOutput{}, errors.New("some error 2"))

		assert.Equal(t, raftStateFollower, r.raft.state)
	})

	t.Run("all request votes error, still in the candidate state", func(t *testing.T) {
		r := newRaftTestWith3Nodes()
		r.raft.Start()

		assert.Equal(t, 1, len(r.timer.AddCallbacks))
		r.timer.AddCallbacks[0]()
		assert.Equal(t, raftStateCandidate, r.raft.state)

		handlers := r.client.RequestVoteHandlers
		assert.Equal(t, 2, len(handlers))

		r.client.RequestVoteInputs = nil

		handlers[0](RequestVoteOutput{}, errors.New("some error 1"))
		handlers[1](RequestVoteOutput{}, errors.New("some error 2"))

		inputs := r.client.RequestVoteInputs
		assert.Equal(t, 2, len(inputs))

		assert.Equal(t, raftStateCandidate, r.raft.state)
	})

	t.Run("one vote not granted, one error, not request vote again", func(t *testing.T) {
		r := newRaftTestWith3Nodes()
		r.raft.Start()

		assert.Equal(t, 1, len(r.timer.AddCallbacks))
		r.timer.AddCallbacks[0]()
		assert.Equal(t, raftStateCandidate, r.raft.state)

		handlers := r.client.RequestVoteHandlers
		assert.Equal(t, 2, len(handlers))

		r.client.RequestVoteInputs = nil
		r.timer.AddDurations = nil

		handlers[0](RequestVoteOutput{
			Term:        initTerm + 1,
			VoteGranted: false,
		}, nil)
		handlers[1](RequestVoteOutput{}, errors.New("some error 2"))

		inputs := r.client.RequestVoteInputs
		assert.Equal(t, 0, len(inputs))

		assert.Equal(t, raftStateFollower, r.raft.state)
		assert.Equal(t, []time.Duration{10 * time.Second}, r.timer.AddDurations)
	})

	t.Run("recv append rpc when in candidate state", func(t *testing.T) {
		r := newRaftTestWith3Nodes()
		r.startAtCandidate(t)

		r.raft.AppendEntriesRPC(AppendEntriesInput{
			Term:     leaderTerm,
			LeaderID: node2,

			PrevLogIndex: 0,
			PrevLogTerm:  0,

			Entries: nil,
		})

		assert.Equal(t, 0, len(r.timer.CancelIDs))
		assert.Equal(t, raftStateFollower, r.raft.state)
	})
}

func TestRaft_RequestVote(t *testing.T) {
	t.Run("first times", func(t *testing.T) {
		r := newRaftTestWith3Nodes()
		r.raft.Start()

		output := r.raft.RequestVote(RequestVoteInput{
			Term:         initTerm + 1,
			CandidateID:  node2,
			LastLogIndex: 0,
			LastLogTerm:  0,
		})
		assert.Equal(t, initTerm+1, output.Term)
		assert.Equal(t, true, output.VoteGranted)

		// store state
		states := r.storage.PutStateInputs
		assert.Equal(t, 1, len(states))
		assert.Equal(t, initTerm+1, states[0].CurrentTerm)
		assert.Equal(t, NullNodeID{
			Valid:  true,
			NodeID: node2,
		}, states[0].VotedFor)
	})

	t.Run("second times rejected", func(t *testing.T) {
		r := newRaftTestWith3Nodes()
		r.raft.Start()

		output := r.raft.RequestVote(RequestVoteInput{
			Term:         initTerm + 1,
			CandidateID:  node2,
			LastLogIndex: 0,
			LastLogTerm:  0,
		})
		assert.Equal(t, initTerm+1, output.Term)
		assert.Equal(t, true, output.VoteGranted)

		// second time
		output = r.raft.RequestVote(RequestVoteInput{
			Term:         initTerm + 1,
			CandidateID:  node3,
			LastLogIndex: 0,
			LastLogTerm:  0,
		})
		assert.Equal(t, initTerm+1, output.Term)
		assert.Equal(t, false, output.VoteGranted)

		// same as the first time
		output = r.raft.RequestVote(RequestVoteInput{
			Term:         initTerm + 1,
			CandidateID:  node2,
			LastLogIndex: 0,
			LastLogTerm:  0,
		})
		assert.Equal(t, initTerm+1, output.Term)
		assert.Equal(t, true, output.VoteGranted)

		states := r.storage.PutStateInputs
		assert.Equal(t, 1, len(states))

		// with lower term
		output = r.raft.RequestVote(RequestVoteInput{
			Term:         initTerm,
			CandidateID:  node2,
			LastLogIndex: 0,
			LastLogTerm:  0,
		})
		assert.Equal(t, initTerm+1, output.Term)
		assert.Equal(t, false, output.VoteGranted)
	})

	t.Run("rejected after already became candidate", func(t *testing.T) {
		r := newRaftTestWith3Nodes()
		r.raft.Start()

		assert.Equal(t, raftStateFollower, r.raft.state)
		r.timer.AddCallbacks[0]()
		assert.Equal(t, raftStateCandidate, r.raft.state)

		r.storage.PutStateInputs = nil

		output := r.raft.RequestVote(RequestVoteInput{
			Term:         initTerm + 1,
			CandidateID:  node2,
			LastLogIndex: 0,
			LastLogTerm:  0,
		})
		assert.Equal(t, initTerm+1, output.Term)
		assert.Equal(t, false, output.VoteGranted)

		assert.Equal(t, 0, len(r.storage.PutStateInputs))
	})
}

func newRaftTestWith5Nodes() *raftTest {
	r := newRaftTest()
	r.storage.GetStateFunc = func() NullStorageState {
		return NullStorageState{
			Valid: true,
			State: StorageState{
				NodeID:      node1,
				CurrentTerm: initTerm,
				VotedFor:    NullNodeID{},

				ClusterNodes: []NodeID{node1, node2, node3, node4, node5},
				ClusterIndex: 0,
			},
		}
	}
	return r
}

func TestRaft_Start_With_5_Nodes(t *testing.T) {
	t.Run("first vote not granted, still in candidate state", func(t *testing.T) {
		r := newRaftTestWith5Nodes()
		r.raft.Start()

		assert.Equal(t, 1, len(r.timer.AddCallbacks))
		r.timer.AddCallbacks[0]()

		handlers := r.client.RequestVoteHandlers
		assert.Equal(t, 4, len(handlers))

		handlers[0](RequestVoteOutput{
			Term:        initTerm + 1,
			VoteGranted: true,
		}, nil)

		assert.Equal(t, raftStateCandidate, r.raft.state)

		handlers[1](RequestVoteOutput{
			Term:        initTerm + 1,
			VoteGranted: true,
		}, nil)

		assert.Equal(t, raftStateLeader, r.raft.state)
	})
}
