package inner

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

type MessageBody struct {
	ClientID uint64  `json:"client_id"`
	Fruit    [][]any `json:"fruit"`
}

func NewMessageBody(clientID uint64, fruitRecords []fruititem.FruitItem) *MessageBody {
	fruit := make([][]any, 0, len(fruitRecords))

	for _, fruitRecord := range fruitRecords {
		fruit = append(fruit, []any{fruitRecord.Fruit, fruitRecord.Amount})
	}
	return &MessageBody{clientID, fruit}
}

func (messageBody *MessageBody) Serialize() (*middleware.Message, error) {
	body, err := serializeJson(messageBody)

	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

func (messageBody *MessageBody) Deserialize(message *middleware.Message) error {
	return deserializeJson([]byte((*message).Body), messageBody)
}

func (messageBody *MessageBody) GetClientID() uint64 {
	return messageBody.ClientID
}

func (messageBody *MessageBody) FruitRecords() ([]fruititem.FruitItem, error) {
	var fruitRecords []fruititem.FruitItem

	for _, data := range messageBody.Fruit {
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

func serializeJson(messageBody *MessageBody) ([]byte, error) {
	return json.Marshal(messageBody)
}

func deserializeJson(body []byte, messageBody *MessageBody) error {
	return json.Unmarshal(body, messageBody)
}
