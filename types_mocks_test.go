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
	AppendEntriesHandlers []func()

	DeleteEntriesFromIndices []LogIndex
	DeleteEntriesHandlers    []func()

	GetEntriesFromIndices []LogIndex
	GetEntriesLimits      []uint64
	GetEntriesHandlers    []func(entries []LogEntry)
}

var _ Storage = &StorageMock{}

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
func (m *StorageMock) AppendEntries(entries []LogEntry, handler func()) {
	m.AppendEntriesInputs = append(m.AppendEntriesInputs, entries)
	m.AppendEntriesHandlers = append(m.AppendEntriesHandlers, handler)
}

// DeleteEntries ...
func (m *StorageMock) DeleteEntries(from LogIndex, handler func()) {
	m.DeleteEntriesFromIndices = append(m.DeleteEntriesFromIndices, from)
	m.DeleteEntriesHandlers = append(m.DeleteEntriesHandlers, handler)
}

// GetEntries ...
func (m *StorageMock) GetEntries(from LogIndex, limit uint64, handler func(entries []LogEntry)) {
	m.GetEntriesFromIndices = append(m.GetEntriesFromIndices, from)
	m.GetEntriesLimits = append(m.GetEntriesLimits, limit)
	m.GetEntriesHandlers = append(m.GetEntriesHandlers, handler)
}

// IsMembershipLogEntry ...
func (m *StorageMock) IsMembershipLogEntry(_ LogEntry) (MembershipLogEntry, bool) {
	return MembershipLogEntry{}, false
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
