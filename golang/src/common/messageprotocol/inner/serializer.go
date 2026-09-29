package inner

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner/messagebody"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

func SerializeFruitRecordMessage(clientId uint64, fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	fruitRecords := []fruititem.FruitItem{fruitRecord}

	messageBody := messagebody.MessageBody{
		ClientId:     clientId,
		FruitRecords: fruitRecords,
		IsEof:        false,
		RecordAmount: 0,
	}
	return serializeMessage(messageBody)
}

func SerializeFruitRecordsMessage(clientId uint64, fruitRecords []fruititem.FruitItem) (*middleware.Message, error) {
	messageBody := messagebody.MessageBody{
		ClientId:     clientId,
		FruitRecords: fruitRecords,
		IsEof:        false,
		RecordAmount: 0,
	}
	return serializeMessage(messageBody)
}

func SerializeEofMessage(clientId uint64) (*middleware.Message, error) {
	messageBody := messagebody.MessageBody{
		ClientId:     clientId,
		FruitRecords: nil,
		IsEof:        true,
		RecordAmount: 0,
	}
	return serializeMessage(messageBody)
}

func SerializeRecordAmountEofMessage(clientId uint64, recordAmount int) (*middleware.Message, error) {
	messageBody := messagebody.MessageBody{
		ClientId:     clientId,
		FruitRecords: nil,
		IsEof:        true,
		RecordAmount: recordAmount,
	}
	return serializeMessage(messageBody)
}

func SerializeRecordAmountMessage(clientId uint64, recordAmount int) (*middleware.Message, error) {
	messageBody := messagebody.MessageBody{
		ClientId:     clientId,
		FruitRecords: nil,
		IsEof:        false,
		RecordAmount: recordAmount,
	}
	return serializeMessage(messageBody)
}

func DeserializeMessage(message *middleware.Message) (*messagebody.MessageBody, error) {
	return messagebody.Deserialize(message)
}

func serializeMessage(messageBody messagebody.MessageBody) (*middleware.Message, error) {
	return messagebody.NewJsonMessageBody(messageBody).Serialize()
}
