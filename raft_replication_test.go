package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRaft_Append_Internal(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		r := newRaftTestWith3Nodes()
		r.raft.Start()

		assert.Equal(t, 1, len(r.timer.AddCallbacks))
		r.timer.AddCallbacks[0]()
		assert.Equal(t, raftStateCandidate, r.raft.state)

		handlers := r.client.RequestVoteHandlers

		handlers[0](RequestVoteOutput{
			Term:        initTerm + 1,
			VoteGranted: true,
		}, nil)
		handlers[1](RequestVoteOutput{
			Term:        initTerm + 1,
			VoteGranted: true,
		}, nil)

		assert.Equal(t, raftStateLeader, r.raft.state)

		getEntries := r.storage.GetEntriesHandlers
		getEntries[0]()
		getEntries[1]()

		appendHandlers := r.client.AppendHandlers
		assert.Equal(t, 2, len(appendHandlers))

		appendHandlers[0](AppendEntriesOutput{
			Term:    initTerm + 1,
			Success: true,
		}, nil)

		// Do Append Entries Internal
		r.client.AppendInputs = nil
		r.storage.GetEntriesHandlers = nil

		r.raft.AppendEntriesInternal([]LogEntry{
			{
				Index: 1,
				Term:  initTerm + 1,
				Data:  []byte("data 01"),
			},
		})

		assert.Equal(t, 1, len(r.storage.GetEntriesHandlers))
		r.storage.GetEntriesHandlers[0]()

		inputs := r.client.AppendInputs
		assert.Equal(t, 1, len(inputs))

		assert.Equal(t, node2, inputs[0].NodeID)
		assert.Equal(t, initTerm+1, inputs[0].Term)
		assert.Equal(t, node1, inputs[0].LeaderID)
		assert.Equal(t, TermNumber(0), inputs[0].PrevLogTerm)
		assert.Equal(t, LogIndex(0), inputs[0].PrevLogIndex)
		assert.Equal(t, []LogEntry{
			{
				Index: 1,
				Term:  initTerm + 1,
				Data:  []byte("data 01"),
			},
		}, inputs[0].Entries)
	})
}

func TestRaft_Append_RPC(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		r := newRaftTestWith3Nodes()
		r.raft.Start()

		assert.Equal(t, 1, len(r.timer.AddDurations))

		entry1 := LogEntry{
			Index: 1,
			Term:  initTerm + 1,
			Data:  []byte("data 01"),
		}

		output := r.raft.AppendEntriesRPC(AppendEntriesInput{
			NodeID:   node1,
			Term:     initTerm + 1,
			LeaderID: node2,

			PrevLogTerm:  0,
			PrevLogIndex: 0,

			Entries: []LogEntry{
				entry1,
			},
		})
		assert.Equal(t, AppendEntriesOutput{
			Term:    initTerm + 1,
			Success: true,
		}, output)

		assert.Equal(t, [][]LogEntry{
			{entry1},
		}, r.storage.AppendEntriesInputs)
		assert.Equal(t, []bool{true}, r.storage.AppendEntriesIsSyncs)

		assert.Equal(t, 1, r.timer.CancelCalls)
		assert.Equal(t, []int{1}, r.timer.CancelIDs)

		assert.Equal(t, 2, len(r.timer.AddDurations))
	})

	t.Run("term is smaller than current", func(t *testing.T) {
		r := newRaftTestWith3Nodes()
		r.raft.Start()

		assert.Equal(t, 1, len(r.timer.AddDurations))

		const term = initTerm - 1

		entry1 := LogEntry{
			Index: 1,
			Term:  term,
			Data:  []byte("data 01"),
		}

		output := r.raft.AppendEntriesRPC(AppendEntriesInput{
			NodeID:   node1,
			Term:     term,
			LeaderID: node2,

			PrevLogTerm:  0,
			PrevLogIndex: 0,

			Entries: []LogEntry{
				entry1,
			},
		})
		assert.Equal(t, AppendEntriesOutput{
			Term:    initTerm,
			Success: false,
		}, output)

		assert.Equal(t, 0, len(r.storage.AppendEntriesInputs))
		assert.Equal(t, 0, r.timer.CancelCalls)
	})
}
