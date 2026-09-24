package inner

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

func SerializeMessage(clientID uint64, fruitRecords []fruititem.FruitItem, isEof bool, notifyEof bool) (*middleware.Message, error) {
	return NewMessageBody(clientID, fruitRecords, isEof, notifyEof).Serialize()
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
	return messageBody.ClientID, fruitRecords, messageBody.IsEof, messageBody.NotifyEof, nil
}
