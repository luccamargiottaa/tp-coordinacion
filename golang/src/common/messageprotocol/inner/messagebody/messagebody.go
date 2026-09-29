package messagebody

import "github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"

type MessageBody struct {
	ClientId     uint64
	FruitRecords []fruititem.FruitItem
	IsEof        bool
	RecordAmount int
}
