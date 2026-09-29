package inner

import (
	"log/slog"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner/messagebody"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type MessageHandler interface {
	HandleDataMessage(messageBody *messagebody.MessageBody) error
	HandleEofMessage(messageBody *messagebody.MessageBody) error
}

func HandleMessage(messageHandler MessageHandler, msg middleware.Message, ack func(), nack func()) error {
	messageBody, err := DeserializeMessage(&msg)

	if err != nil {
		slog.Error("While deserializing message", "err", err)
		nack()

		return err
	}
	defer ack()

	if messageBody.IsEof {
		if err = messageHandler.HandleEofMessage(messageBody); err != nil {
			slog.Error("While handling eof message", "err", err)

			return err
		}
	} else {
		if err = messageHandler.HandleDataMessage(messageBody); err != nil {
			slog.Error("While handling data message", "err", err)

			return err
		}
	}
	return nil
}
