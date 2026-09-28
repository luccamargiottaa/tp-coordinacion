package clienteofcounter

type ClientEofCounter struct {
	counters         map[uint64]int
	expectedEofCount int
}

func NewClientEofCounter(expectedEofCount int) *ClientEofCounter {
	return &ClientEofCounter{make(map[uint64]int), expectedEofCount}
}

func (clientEofCounter *ClientEofCounter) Increment(clientId uint64) {
	clientEofCounter.counters[clientId]++
}

func (clientEofCounter *ClientEofCounter) HasReachedExpectedEof(clientId uint64) bool {
	return clientEofCounter.counters[clientId] == clientEofCounter.expectedEofCount
}

func (clientEofCounter *ClientEofCounter) DeleteEofCounter(clientId uint64) {
	delete(clientEofCounter.counters, clientId)
}
