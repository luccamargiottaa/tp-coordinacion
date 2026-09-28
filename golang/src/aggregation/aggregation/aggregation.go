package aggregation

import (
	"fmt"
	"log/slog"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/clienttop"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/clienteofcounter"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type AggregationConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Aggregation struct {
	inputExchange    middleware.Middleware
	outputQueue      middleware.Middleware
	clientTop        clienttop.ClientTop
	clientEofCounter clienteofcounter.ClientEofCounter
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}
	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)

	if err != nil {
		return nil, err
	}
	key := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, key, connSettings)

	if err != nil {
		_ = outputQueue.Close()

		return nil, err
	}
	clientTop := clienttop.NewClientTop(config.TopSize)
	clientEofCounter := clienteofcounter.NewClientEofCounter(config.SumAmount)

	aggregation := &Aggregation{
		outputQueue:      outputQueue,
		inputExchange:    inputExchange,
		clientTop:        *clientTop,
		clientEofCounter: *clientEofCounter,
	}
	return aggregation, nil
}

func (aggregation *Aggregation) Run() {
	defer aggregation.close()

	err := aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		if err := aggregation.handleMessage(msg, ack, nack); err != nil {
			_ = aggregation.inputExchange.StopConsuming()
		}
	})
	if err != nil {
		slog.Error("While consuming messages from input queue", "err", err)
	}
}

func (aggregation *Aggregation) close() {
	finalErr := aggregation.inputExchange.StopConsuming()

	if finalErr == nil {
		finalErr = aggregation.inputExchange.Close()
	}
	if err := aggregation.outputQueue.Close(); err != nil {
		finalErr = err
	}
	if finalErr != nil {
		slog.Error("While closing middleware", "err", finalErr)
	}
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) error {
	clientId, fruitRecords, isEof, _, err := inner.DeserializeMessage(&msg)

	if err != nil {
		slog.Error("While deserializing message", "err", err)
		nack()

		return err
	}
	defer ack()

	if isEof {
		if err = aggregation.handleEndOfRecordsMessage(clientId); err != nil {
			slog.Error("While handling end of record message", "err", err)

			return err
		}
	} else {
		aggregation.handleDataMessage(clientId, fruitRecords)
	}
	return nil
}

func (aggregation *Aggregation) handleDataMessage(clientId uint64, fruitRecords []fruititem.FruitItem) {
	aggregation.clientTop.AddRecords(clientId, fruitRecords)
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(clientId uint64) error {
	slog.Info("Received End Of Records message")

	aggregation.clientEofCounter.Increment(clientId)

	if !aggregation.clientEofCounter.HasReachedExpectedEof(clientId) {
		return nil
	}
	if err := aggregation.sendFruitRecordsTop(clientId); err != nil {
		return err
	}
	aggregation.clientEofCounter.DeleteEofCounter(clientId)

	return nil
}

func (aggregation *Aggregation) sendFruitRecordsTop(clientId uint64) error {
	fruitRecordsTop := aggregation.clientTop.GetTop(clientId)

	if len(fruitRecordsTop) != 0 {
		message, err := inner.SerializeFruitRecordsMessage(clientId, fruitRecordsTop)

		if err != nil {
			slog.Debug("While serializing top message", "err", err)

			return err
		}
		if err = aggregation.outputQueue.Send(*message); err != nil {
			slog.Debug("While sending top message", "err", err)

			return err
		}
	}
	message, err := inner.SerializeEofMessage(clientId)

	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)

		return err
	}
	if err = aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)

		return err
	}
	aggregation.clientTop.DeleteRecords(clientId)

	return nil
}
