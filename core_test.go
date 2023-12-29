package raft

import (
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
		storage: &StorageMock{},
		timer:   &TimerMock{},
		client:  &ClientMock{},
	}

	r.raft = NewRaft(r.storage, r.timer, r.client)

	r.storage.PutStateFunc = func() error {
		return nil
	}

	return r
}

var node1 = NodeID{100, 1}
var node2 = NodeID{100, 2}
var node3 = NodeID{100, 3}

const initTerm = TermNumber(70)

func newRaftTestWith3Nodes() *raftTest {
	r := newRaftTest()
	r.storage.GetStateFunc = func() (NullStorageState, error) {
		return NullStorageState{
			Valid: true,
			State: StorageState{
				NodeID:      node1,
				CurrentTerm: initTerm,
				VotedFor:    NullNodeID{},

				ClusterNodes: []NodeID{node1, node2, node3},
				ClusterIndex: 0,
			},
		}, nil
	}
	return r
}

func TestRaft_New(t *testing.T) {
	t.Run("check new", func(t *testing.T) {
		r := newRaftTest()
		assert.Equal(t, raftStateFollower, r.raft.state)
	})
}

func TestRaft_Start(t *testing.T) {
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
	})
}
