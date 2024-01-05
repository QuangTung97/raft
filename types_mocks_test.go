package raft

import (
	"time"
)

// ClientMock ...
type ClientMock struct {
	RequestVoteInputs   []RequestVoteInput
	RequestVoteHandlers []RequestVoteHandler

	AppendInputs   []AppendEntriesInput
	AppendHandlers []AppendEntriesHandler
}

var _ Client = &ClientMock{}

// ResetAppend ...
func (m *ClientMock) ResetAppend() {
	m.AppendInputs = nil
	m.AppendHandlers = nil
}

// RequestVote ...
func (m *ClientMock) RequestVote(input RequestVoteInput, handler RequestVoteHandler) {
	m.RequestVoteInputs = append(m.RequestVoteInputs, input)
	m.RequestVoteHandlers = append(m.RequestVoteHandlers, handler)
}

// AppendEntries ...
func (m *ClientMock) AppendEntries(input AppendEntriesInput, handler AppendEntriesHandler) {
	m.AppendInputs = append(m.AppendInputs, input)
	m.AppendHandlers = append(m.AppendHandlers, handler)
}

// StorageMock ...
type StorageMock struct {
	GetStateCalls int
	GetStateFunc  func() NullStorageState

	PutStateInputs []StorageState

	AppendEntriesInputs   [][]LogEntry
	AppendEntriesIsSyncs  []bool
	AppendEntriesHandlers []func()

	GetEntriesFromIndices []LogIndex
	GetEntriesLimits      []uint64
	LogEntries            []LogEntry
	GetEntriesHandlers    []func()

	CommitIndex          LogIndex
	GetCommitIndexCalls  int
	SetCommitIndexInputs []LogIndex
}

var _ Storage = &StorageMock{}

func NewStorageMock() *StorageMock {
	return &StorageMock{
		LogEntries: []LogEntry{
			{Index: 0, Term: 0},
		},
	}
}

// ResetAppendEntries ...
func (m *StorageMock) ResetAppendEntries() {
	m.AppendEntriesInputs = nil
	m.AppendEntriesIsSyncs = nil
	m.AppendEntriesHandlers = nil
}

// ResetGetEntries ...
func (m *StorageMock) ResetGetEntries() {
	m.GetEntriesFromIndices = nil
	m.GetEntriesLimits = nil
	m.GetEntriesHandlers = nil
}

// GetState ...
func (m *StorageMock) GetState() NullStorageState {
	m.GetStateCalls++
	return m.GetStateFunc()
}

// PutState ...
func (m *StorageMock) PutState(state StorageState) {
	m.PutStateInputs = append(m.PutStateInputs, state)
}

// AppendEntries ...
func (m *StorageMock) AppendEntries(entries []LogEntry, isSync bool, handler func()) {
	m.AppendEntriesInputs = append(m.AppendEntriesInputs, entries)
	m.AppendEntriesIsSyncs = append(m.AppendEntriesIsSyncs, isSync)
	m.AppendEntriesHandlers = append(m.AppendEntriesHandlers, handler)

	first := entries[0].Index
	m.LogEntries = m.LogEntries[:first]
	m.LogEntries = append(m.LogEntries, entries...)
}

// GetEntries ...
func (m *StorageMock) GetEntries(from LogIndex, limit uint64, handler func(entries []LogEntry)) {
	m.GetEntriesFromIndices = append(m.GetEntriesFromIndices, from)
	m.GetEntriesLimits = append(m.GetEntriesLimits, limit)
	m.GetEntriesHandlers = append(m.GetEntriesHandlers, func() {
		var input []LogEntry
		for i := from; i < from+LogIndex(limit); i++ {
			if int(i) >= len(m.LogEntries) {
				break
			}
			input = append(input, m.LogEntries[i])
		}
		handler(input)
	})
}

// GetLastEntry ...
func (m *StorageMock) GetLastEntry() LogEntry {
	return LogEntry{}
}

// IsMembershipLogEntry ...
func (m *StorageMock) IsMembershipLogEntry(_ LogEntry) (MembershipLogEntry, bool) {
	return MembershipLogEntry{}, false
}

// GetCommitIndex ...
func (m *StorageMock) GetCommitIndex() LogIndex {
	m.GetCommitIndexCalls++
	return m.CommitIndex
}

// SetCommitIndex ...
func (m *StorageMock) SetCommitIndex(index LogIndex) {
	m.CommitIndex = index
	m.SetCommitIndexInputs = append(m.SetCommitIndexInputs, index)
}

// TimerMock ...
type TimerMock struct {
	AddDurations []time.Duration
	AddCallbacks []func()

	CancelCalls int
	CancelIDs   []int
	CancelFunc  func() bool
}

var _ Timer = &TimerMock{}

func (m *TimerMock) AddTimer(d time.Duration, callback func()) TimerCancelFunc {
	m.AddDurations = append(m.AddDurations, d)
	m.AddCallbacks = append(m.AddCallbacks, callback)
	return func() bool {
		m.CancelCalls++
		m.CancelIDs = append(m.CancelIDs, m.CancelCalls)
		return m.CancelFunc()
	}
}
