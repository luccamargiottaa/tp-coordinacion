package clientfruitrecords

import (
	"iter"
	"maps"
	"slices"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

type fruitRecordsMap map[string]fruititem.FruitItem

type ClientFruitRecords struct {
	clientMap map[uint64]fruitRecordsMap
}

func NewClientFruitRecords() *ClientFruitRecords {
	return &ClientFruitRecords{make(map[uint64]fruitRecordsMap)}
}

func (clientFruitRecords *ClientFruitRecords) AddRecords(clientId uint64, fruitRecords []fruititem.FruitItem) {
	fruitMap, ok := clientFruitRecords.clientMap[clientId]

	if !ok {
		fruitMap = make(fruitRecordsMap)
		clientFruitRecords.clientMap[clientId] = fruitMap
	}
	for _, fruitRecord := range fruitRecords {
		if _, ok := fruitMap[fruitRecord.Fruit]; ok {
			fruitMap[fruitRecord.Fruit] = fruitMap[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			fruitMap[fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (clientFruitRecords *ClientFruitRecords) GetRecords(clientId uint64) iter.Seq[fruititem.FruitItem] {
	fruitMap, ok := clientFruitRecords.clientMap[clientId]

	if !ok {
		return slices.Values([]fruititem.FruitItem{})
	}
	return maps.Values(fruitMap)
}

func (clientFruitRecords *ClientFruitRecords) DeleteRecords(clientId uint64) {
	delete(clientFruitRecords.clientMap, clientId)
}
