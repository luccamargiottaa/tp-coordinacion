package clienttop

import (
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/clientfruitrecords"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

type ClientTop struct {
	clientFruitRecords clientfruitrecords.ClientFruitRecords
	topSize            int
}

func NewClientTop(topSize int) *ClientTop {
	clientFruitRecords := clientfruitrecords.NewClientFruitRecords()

	return &ClientTop{*clientFruitRecords, topSize}
}

func (clientTop *ClientTop) AddRecords(clientId uint64, fruitRecords []fruititem.FruitItem) {
	clientTop.clientFruitRecords.AddRecords(clientId, fruitRecords)
}

func (clientTop *ClientTop) GetTop(clientId uint64) []fruititem.FruitItem {
	var fruitRecordsTop []fruititem.FruitItem

	for fruitRecord := range clientTop.clientFruitRecords.GetRecords(clientId) {
		fruitRecordsTop = append(fruitRecordsTop, fruitRecord)
	}
	sort.SliceStable(fruitRecordsTop, func(i, j int) bool {
		return fruitRecordsTop[j].Less(fruitRecordsTop[i])
	})
	finalTopSize := min(clientTop.topSize, len(fruitRecordsTop))

	return fruitRecordsTop[:finalTopSize]
}

func (clientTop *ClientTop) DeleteRecords(clientId uint64) {
	clientTop.clientFruitRecords.DeleteRecords(clientId)
}
