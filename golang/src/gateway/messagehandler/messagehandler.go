package messagehandler

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

const initialClientId = 0

var nextClientId uint64 = initialClientId

type MessageHandler struct {
	clientId     uint64
	recordAmount int
}

func NewMessageHandler() MessageHandler {
	clientId := nextClientId
	nextClientId++

	recordAmount := 0

	return MessageHandler{clientId, recordAmount}
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	messageHandler.recordAmount++

	return inner.SerializeFruitRecordMessage(messageHandler.clientId, fruitRecord)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	return inner.SerializeRecordAmountEofMessage(messageHandler.clientId, messageHandler.recordAmount)
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	messageBody, err := inner.DeserializeMessage(message)

	if err != nil {
		return nil, err
	}
	if messageBody.ClientId != messageHandler.clientId {
		return nil, nil
	}
	return messageBody.FruitRecords, nil
}
