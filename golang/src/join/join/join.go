package join

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
	running          atomic.Bool
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

	join.running.Store(true)
	doneCh := make(chan struct{})
	go join.handleSignals(doneCh)

	err := join.inputQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		if err := inner.HandleMessage(join, msg, ack, nack); err != nil {
			_ = join.inputQueue.StopConsuming()

			if join.running.Load() {
				slog.Error(err.Error())
			}
		}
	})
	if err != nil {
		if join.running.Load() {
			slog.Error("While consuming messages from input queue", "err", err)
		}
	}
	<-doneCh
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
		if join.running.Load() {
			slog.Error("While closing middleware", "err", finalErr)
		}
	}
}

func (join *Join) handleSignals(doneCh chan struct{}) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	<-signals
	slog.Info("SIGTERM signal received")

	join.running.Store(false)
	join.close()

	doneCh <- struct{}{}
	close(doneCh)
	close(signals)
}

func (join *Join) HandleDataMessage(messageBody *messagebody.MessageBody) error {
	join.clientTop.UpdateTop(messageBody.ClientId, messageBody.FruitRecords)

	return nil
}

func (join *Join) HandleEofMessage(messageBody *messagebody.MessageBody) error {
	clientId := messageBody.ClientId
	slog.Info("Received Eof message", "client_id", clientId)

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
		return fmt.Errorf("while serializing top message: %w", err)
	}
	if err = join.outputQueue.Send(*message); err != nil {
		return fmt.Errorf("while sending top message: %w", err)
	}
	join.clientTop.DeleteTop(clientId)

	return nil
}
