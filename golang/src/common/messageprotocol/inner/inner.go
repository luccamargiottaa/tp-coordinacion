package inner

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

func SerializeFruitRecordMessage(clientId uint64, fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	fruitRecords := []fruititem.FruitItem{fruitRecord}

	return serializeMessage(clientId, fruitRecords, false, false)
}

func SerializeFruitRecordsMessage(clientId uint64, fruitRecords []fruititem.FruitItem) (*middleware.Message, error) {
	return serializeMessage(clientId, fruitRecords, false, false)
}

func SerializeEofMessage(clientId uint64) (*middleware.Message, error) {
	return serializeMessage(clientId, nil, true, false)
}

func SerializeNotifyEofMessage(clientId uint64) (*middleware.Message, error) {
	return serializeMessage(clientId, nil, true, true)
}

func DeserializeMessage(message *middleware.Message) (uint64, []fruititem.FruitItem, bool, bool, error) {
	messageBody := &MessageBody{}

	if err := messageBody.Deserialize(message); err != nil {
		return 0, nil, false, false, err
	}
	fruitRecords, err := messageBody.FruitRecords()

	if err != nil {
		return 0, nil, false, false, err
	}
	return messageBody.ClientId, fruitRecords, messageBody.IsEof, messageBody.NotifyEof, nil
}

func serializeMessage(clientId uint64, fruitRecords []fruititem.FruitItem, isEof bool, notifyEof bool) (*middleware.Message, error) {
	return NewMessageBody(clientId, fruitRecords, isEof, notifyEof).Serialize()
}
