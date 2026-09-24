package boundedset

// BoundedSet is a set with a max size.
// If it's filled, the next element to be added replaces the oldest element.
type BoundedSet struct {
	maxSize int
	head    int
	order   []uint64
	items   map[uint64]struct{}
}

func NewBoundedSet(maxSize int) *BoundedSet {
	return &BoundedSet{
		maxSize: maxSize,
		head:    0,
		order:   make([]uint64, 0, maxSize),
		items:   make(map[uint64]struct{}),
	}
}

func (boundedSet *BoundedSet) Add(clientID uint64) {
	if boundedSet.Contains(clientID) {
		return
	}
	if len(boundedSet.order) < boundedSet.maxSize {
		boundedSet.order = append(boundedSet.order, clientID)
		boundedSet.items[clientID] = struct{}{}
	} else {
		oldest := boundedSet.order[boundedSet.head]
		delete(boundedSet.items, oldest)

		boundedSet.order[boundedSet.head] = clientID
		boundedSet.items[clientID] = struct{}{}

		boundedSet.head = (boundedSet.head + 1) % boundedSet.maxSize
	}
}

func (boundedSet *BoundedSet) Contains(clientID uint64) bool {
	_, ok := boundedSet.items[clientID]
	return ok
}
