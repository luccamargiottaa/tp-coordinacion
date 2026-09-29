package messagebody

import (
	"encoding/json"
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

var (
	errNotPair  = errors.New("datum is not a (fruit, amount) pair")
	errTooShort = errors.New("datum is too short")
)

type JsonMessageBody struct {
	ClientId     uint64  `json:"client_id"`
	Fruit        [][]any `json:"fruit"`
	IsEof        bool    `json:"is_eof"`
	RecordAmount int     `json:"record_amount"`
}

func NewJsonMessageBody(messageBody MessageBody) *JsonMessageBody {
	fruitRecords := messageBody.FruitRecords
	fruit := make([][]any, 0, len(fruitRecords))

	for _, fruitRecord := range fruitRecords {
		fruit = append(fruit, []any{fruitRecord.Fruit, fruitRecord.Amount})
	}
	return &JsonMessageBody{messageBody.ClientId,
		fruit,
		messageBody.IsEof,
		messageBody.RecordAmount,
	}
}

func (jsonMessageBody *JsonMessageBody) Serialize() (*middleware.Message, error) {
	body, err := serializeJson(jsonMessageBody)

	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

func Deserialize(message *middleware.Message) (*MessageBody, error) {
	jsonMessageBody := JsonMessageBody{}

	err := deserializeJson([]byte((*message).Body), &jsonMessageBody)

	if err != nil {
		return nil, err
	}
	fruitRecords, err := jsonMessageBody.FruitRecords()

	if err != nil {
		return nil, err
	}
	messageBody := MessageBody{
		ClientId:     jsonMessageBody.ClientId,
		FruitRecords: fruitRecords,
		IsEof:        jsonMessageBody.IsEof,
		RecordAmount: jsonMessageBody.RecordAmount,
	}
	return &messageBody, nil
}

func (jsonMessageBody *JsonMessageBody) FruitRecords() ([]fruititem.FruitItem, error) {
	var fruitRecords []fruititem.FruitItem

	for _, data := range jsonMessageBody.Fruit {
		if len(data) < 2 {
			return nil, errTooShort
		}
		fruit, ok := data[0].(string)

		if !ok {
			return nil, errNotPair
		}
		fruitAmount, ok := data[1].(float64)

		if !ok {
			return nil, errNotPair
		}
		fruitRecord := fruititem.FruitItem{Fruit: fruit, Amount: uint32(fruitAmount)}
		fruitRecords = append(fruitRecords, fruitRecord)
	}
	return fruitRecords, nil
}

func serializeJson(messageBody *JsonMessageBody) ([]byte, error) {
	return json.Marshal(messageBody)
}

func deserializeJson(body []byte, messageBody *JsonMessageBody) error {
	return json.Unmarshal(body, messageBody)
}
