package inner

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

func SerializeMessage(clientID uint64, fruitRecords []fruititem.FruitItem) (*middleware.Message, error) {
	return NewMessageBody(clientID, fruitRecords).Serialize()
}

func DeserializeMessage(message *middleware.Message) (uint64, []fruititem.FruitItem, bool, error) {
	messageBody := &MessageBody{}

	if err := messageBody.Deserialize(message); err != nil {
		return 0, nil, false, err
	}
	fruitRecords, err := messageBody.FruitRecords()

	if err != nil {
		return 0, nil, false, err
	}
	return messageBody.GetClientID(), fruitRecords, len(messageBody.Fruit) == 0, nil
}
