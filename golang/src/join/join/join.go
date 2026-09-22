package join

import (
	"errors"
	"log/slog"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Join struct {
	inputQueue  middleware.Middleware
	outputQueue middleware.Middleware
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}
	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)

	if err != nil {
		return nil, err
	}
	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)

	if err != nil {
		_ = inputQueue.Close()

		return nil, err
	}
	join := &Join{inputQueue: inputQueue, outputQueue: outputQueue}

	return join, nil
}

func (join *Join) Run() {
	defer join.close()

	err := join.inputQueue.StartConsuming(join.handleMessage)

	if err != nil {
		slog.Error("While consuming messages from input queue", "err", err)
	}
}

func (join *Join) close() {
	err1 := join.inputQueue.Close()
	err2 := join.outputQueue.Close()

	if err := errors.Join(err1, err2); err != nil {
		slog.Error("While closing middleware", "err", err)
	}
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), _ func()) {
	defer ack()

	if _, _, isEof, _ := inner.DeserializeMessage(&msg); isEof {
		return
	}
	if err := join.outputQueue.Send(msg); err != nil {
		slog.Error("While sending top", "err", err)
	}
}
