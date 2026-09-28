package messagehandler

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

const initialClientId = 0

var nextClientId uint64 = initialClientId

type MessageHandler struct {
	clientId uint64
}

func NewMessageHandler() MessageHandler {
	clientID := nextClientId
	nextClientId++

	return MessageHandler{clientID}
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	return inner.SerializeFruitRecordMessage(messageHandler.clientId, fruitRecord)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	return inner.SerializeNotifyEofMessage(messageHandler.clientId)
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	clientID, fruitRecords, _, _, err := inner.DeserializeMessage(message)

	if err != nil {
		return nil, err
	}
	if clientID != messageHandler.clientId {
		return nil, nil
	}
	return fruitRecords, nil
}
