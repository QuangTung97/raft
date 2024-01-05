package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func (r *raftTest) startAtLeader(t *testing.T) {
	r.startAtCandidate(t)

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
}

func TestRaft_Append_Internal(t *testing.T) {
	const leaderTerm = initTerm + 1
	t.Run("do call append rpc when not in progress", func(t *testing.T) {
		r := newRaftTestWith3Nodes()
		r.startAtLeader(t)

		getEntries := r.storage.GetEntriesHandlers
		getEntries[0]()
		getEntries[1]()

		appendHandlers := r.client.AppendHandlers
		assert.Equal(t, 2, len(appendHandlers))

		appendHandlers[0](AppendEntriesOutput{
			Term:    leaderTerm,
			Success: true,
		}, nil)

		// Do Append Entries Internal
		r.client.AppendInputs = nil
		r.storage.GetEntriesHandlers = nil

		r.raft.AppendEntriesInternal([]LogEntry{
			{
				Index: 1,
				Term:  leaderTerm,
				Data:  []byte("data 01"),
			},
		})

		assert.Equal(t, 1, len(r.storage.GetEntriesHandlers))
		r.storage.GetEntriesHandlers[0]()

		inputs := r.client.AppendInputs
		assert.Equal(t, 1, len(inputs))

		assert.Equal(t, node2, inputs[0].NodeID)
		assert.Equal(t, leaderTerm, inputs[0].Term)
		assert.Equal(t, node1, inputs[0].LeaderID)
		assert.Equal(t, TermNumber(0), inputs[0].PrevLogTerm)
		assert.Equal(t, LogIndex(0), inputs[0].PrevLogIndex)
		assert.Equal(t, []LogEntry{
			{
				Index: 1,
				Term:  leaderTerm,
				Data:  []byte("data 01"),
			},
		}, inputs[0].Entries)
	})

	t.Run("itself and another response for append, increase commit index", func(t *testing.T) {
		r := newRaftTestWith3Nodes()
		r.startAtLeader(t)

		getEntries := r.storage.GetEntriesHandlers
		assert.Equal(t, 2, len(getEntries))
		getEntries[0]()
		getEntries[1]()

		// initial append response after become leader
		appendHandlers := r.client.AppendHandlers
		assert.Equal(t, 2, len(appendHandlers))

		appendHandlers[0](AppendEntriesOutput{
			Term:    initTerm + 1,
			Success: true,
		}, nil)

		r.storage.ResetAppendEntries()
		r.storage.ResetGetEntries()
		r.client.ResetAppend()

		r.raft.AppendEntriesInternal([]LogEntry{
			{
				Index: 1,
				Term:  leaderTerm,
				Data:  []byte("data 01"),
			},
		})

		getEntries = r.storage.GetEntriesHandlers
		assert.Equal(t, 1, len(getEntries))
		getEntries[0]()

		// response from other servers
		appendHandlers = r.client.AppendHandlers
		assert.Equal(t, 1, len(appendHandlers))
		appendHandlers[0](AppendEntriesOutput{
			Term:    leaderTerm,
			Success: true,
		}, nil)

		assert.Equal(t, LogIndex(0), r.storage.GetCommitIndex())

		// response for its own storage
		storageAppend := r.storage.AppendEntriesHandlers
		assert.Equal(t, 1, len(storageAppend))
		storageAppend[0]()

		assert.Equal(t, LogIndex(1), r.storage.GetCommitIndex())
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

	t.Run("term is smaller than current, do not append", func(t *testing.T) {
		r := newRaftTestWith3Nodes()
		r.raft.Start()

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

	t.Run("with existing log not matching last index", func(t *testing.T) {
		r := newRaftTestWith3Nodes()
		r.storage.InitLogEntries(EntryRange{Term: initTerm - 2, Num: 4})
		r.raft.Start()

		entry1 := LogEntry{
			Index: 31,
			Term:  initTerm,
			Data:  []byte("data 01"),
		}

		output := r.raft.AppendEntriesRPC(AppendEntriesInput{
			NodeID:   node1,
			Term:     initTerm,
			LeaderID: node2,

			PrevLogTerm:  initTerm - 1,
			PrevLogIndex: 30,

			Entries: []LogEntry{
				entry1,
			},
		})
		assert.Equal(t, AppendEntriesOutput{
			Term:    initTerm,
			Success: false,
		}, output)

		assert.Equal(t, 0, len(r.storage.AppendEntriesInputs))
		assert.Equal(t, 1, r.timer.CancelCalls)
		assert.Equal(t, 2, len(r.timer.AddDurations))
	})
}
