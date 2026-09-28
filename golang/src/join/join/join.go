package join

import (
	"log/slog"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/clienttop"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/clienteofcounter"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
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
	inputQueue       middleware.Middleware
	outputQueue      middleware.Middleware
	clientTop        clienttop.ClientTop
	clientEofCounter clienteofcounter.ClientEofCounter
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
	clientTop := clienttop.NewClientTop(config.TopSize)
	clientEofCounter := clienteofcounter.NewClientEofCounter(config.AggregationAmount)

	join := &Join{
		inputQueue:       inputQueue,
		outputQueue:      outputQueue,
		clientTop:        *clientTop,
		clientEofCounter: *clientEofCounter,
	}
	return join, nil
}

func (join *Join) Run() {
	defer join.close()

	err := join.inputQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		if err := join.handleMessage(msg, ack, nack); err != nil {
			_ = join.inputQueue.StopConsuming()
		}
	})
	if err != nil {
		slog.Error("While consuming messages from input queue", "err", err)
	}
}

func (join *Join) close() {
	finalErr := join.inputQueue.StopConsuming()

	if finalErr == nil {
		finalErr = join.inputQueue.Close()
	}
	if err := join.outputQueue.Close(); err != nil {
		finalErr = err
	}
	if finalErr != nil {
		slog.Error("While closing middleware", "err", finalErr)
	}
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) error {
	clientId, fruitRecordsTop, isEof, _, err := inner.DeserializeMessage(&msg)

	if err != nil {
		slog.Error("While deserializing message", "err", err)
		nack()

		return err
	}
	defer ack()

	if isEof {
		if err = join.handleEndOfRecordsMessage(clientId); err != nil {
			slog.Error("While handling end of record message", "err", err)

			return err
		}
	} else {
		join.handleDataMessage(clientId, fruitRecordsTop)
	}
	return nil
}

func (join *Join) handleDataMessage(clientId uint64, fruitRecordsTop []fruititem.FruitItem) {
	join.clientTop.UpdateTop(clientId, fruitRecordsTop)
}

func (join *Join) handleEndOfRecordsMessage(clientId uint64) error {
	slog.Info("Received End Of Records message")

	join.clientEofCounter.Increment(clientId)

	if !join.clientEofCounter.HasReachedExpectedEof(clientId) {
		return nil
	}
	if err := join.sendFruitRecordsTop(clientId); err != nil {
		return err
	}
	join.clientEofCounter.DeleteEofCounter(clientId)

	return nil
}

func (join *Join) sendFruitRecordsTop(clientId uint64) error {
	fruitRecordsTop := join.clientTop.GetTop(clientId)
	message, err := inner.SerializeFruitRecordsMessage(clientId, fruitRecordsTop)

	if err != nil {
		slog.Debug("While serializing top message", "err", err)

		return err
	}
	if err = join.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending top message", "err", err)

		return err
	}
	join.clientTop.DeleteTop(clientId)

	return nil
}
