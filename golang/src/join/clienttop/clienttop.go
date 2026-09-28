package clienttop

import (
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

type ClientTop struct {
	clientMap map[uint64][]fruititem.FruitItem
	topSize   int
}

func NewClientTop(topSize int) *ClientTop {
	return &ClientTop{make(map[uint64][]fruititem.FruitItem), topSize}
}

func (clientTop *ClientTop) UpdateTop(clientId uint64, fruitRecordsTop []fruititem.FruitItem) {
	currentTop := clientTop.clientMap[clientId]
	currentTop = append(currentTop, fruitRecordsTop...)

	sort.SliceStable(currentTop, func(i, j int) bool {
		return currentTop[j].Less(currentTop[i])
	})
	if len(currentTop) > clientTop.topSize {
		currentTop = currentTop[:clientTop.topSize]
	}
	clientTop.clientMap[clientId] = currentTop
}

func (clientTop *ClientTop) GetTop(clientId uint64) []fruititem.FruitItem {
	return clientTop.clientMap[clientId]
}

func (clientTop *ClientTop) DeleteTop(clientId uint64) {
	delete(clientTop.clientMap, clientId)
}
