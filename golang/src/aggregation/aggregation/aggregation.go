package aggregation

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/clienttop"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/clienteofcounter"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner/messagebody"
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
	running          atomic.Bool
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

	aggregation.running.Store(true)
	doneCh := make(chan struct{})
	go aggregation.handleSignals(doneCh)

	err := aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		if err := inner.HandleMessage(aggregation, msg, ack, nack); err != nil {
			_ = aggregation.inputExchange.StopConsuming()

			if aggregation.running.Load() {
				slog.Error(err.Error())
			}
		}
	})
	if err != nil {
		if aggregation.running.Load() {
			slog.Error("While consuming messages from input queue", "err", err)
		}
	}
	<-doneCh
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
		if aggregation.running.Load() {
			slog.Error("While closing middleware", "err", finalErr)
		}
	}
}

func (aggregation *Aggregation) handleSignals(doneCh chan struct{}) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	<-signals
	slog.Info("SIGTERM signal received")

	aggregation.running.Store(false)
	aggregation.close()

	doneCh <- struct{}{}
	close(doneCh)
	close(signals)
}

func (aggregation *Aggregation) HandleDataMessage(messageBody *messagebody.MessageBody) error {
	aggregation.clientTop.AddRecords(messageBody.ClientId, messageBody.FruitRecords)

	return nil
}

func (aggregation *Aggregation) HandleEofMessage(messageBody *messagebody.MessageBody) error {
	clientId := messageBody.ClientId
	slog.Info("Received Eof message", "client_id", clientId)

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
			return fmt.Errorf("while serializing top message: %w", err)
		}
		if err = aggregation.outputQueue.Send(*message); err != nil {
			return fmt.Errorf("while sending top message: %w", err)
		}
	}
	message, err := inner.SerializeEofMessage(clientId)

	if err != nil {
		return fmt.Errorf("while serializing EOF message: %w", err)
	}
	if err = aggregation.outputQueue.Send(*message); err != nil {
		return fmt.Errorf("while sending EOF message: %w", err)
	}
	aggregation.clientTop.DeleteRecords(clientId)

	return nil
}
