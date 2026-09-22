package messagehandler

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

const initialClientID = 0

var nextClientID uint64 = initialClientID

type MessageHandler struct {
	clientID uint64
}

func NewMessageHandler() MessageHandler {
	clientID := nextClientID
	nextClientID++

	return MessageHandler{clientID}
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	data := []fruititem.FruitItem{fruitRecord}

	return inner.SerializeMessage(messageHandler.clientID, data)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	var data []fruititem.FruitItem

	return inner.SerializeMessage(messageHandler.clientID, data)
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	clientID, fruitRecords, _, err := inner.DeserializeMessage(message)

	if err != nil {
		return nil, err
	}
	if clientID != messageHandler.clientID {
		return nil, nil
	}
	return fruitRecords, nil
}
