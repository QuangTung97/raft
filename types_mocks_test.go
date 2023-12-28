package raft

// ClientMock ...
type ClientMock struct {
	RequestVoteInputs   []RequestVoteInput
	RequestVoteHandlers []RequestVoteHandler

	AppendInputs   []AppendEntriesInput
	AppendHandlers []AppendEntriesHandler
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
