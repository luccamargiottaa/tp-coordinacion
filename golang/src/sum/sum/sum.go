package sum

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/clientrecordinfo"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/clientfruitrecords"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner/messagebody"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type Sum struct {
	inputQueue         middleware.Middleware
	sumExchange        middleware.Middleware
	eofCoordinator     *EofCoordinator
	mutex              *sync.Mutex
	clientFruitRecords *clientfruitrecords.ClientFruitRecords
	clientRecordInfo   *clientrecordinfo.ClientRecordInfo
	running            atomic.Bool
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}
	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)

	if err != nil {
		return nil, err
	}
	keys := []string{config.SumPrefix}
	sumExchange, err := middleware.CreateExchangeMiddleware(config.SumPrefix, keys, connSettings)

	if err != nil {
		_ = inputQueue.Close()

		return nil, err
	}
	clientFruitRecords := clientfruitrecords.NewClientFruitRecords()
	clientRecordInfo := clientrecordinfo.NewClientRecordInfo()
	mutex := &sync.Mutex{}

	eofCoordinator, err := newEofCoordinator(config, clientFruitRecords, clientRecordInfo, mutex)

	if err != nil {
		_ = inputQueue.Close()
		_ = sumExchange.Close()

		return nil, err
	}
	sum := &Sum{
		inputQueue:         inputQueue,
		sumExchange:        sumExchange,
		eofCoordinator:     eofCoordinator,
		mutex:              mutex,
		clientFruitRecords: clientFruitRecords,
		clientRecordInfo:   clientRecordInfo,
	}
	return sum, nil
}

func (sum *Sum) Run() {
	defer sum.close()

	sum.running.Store(true)
	doneCh := make(chan struct{})
	go sum.handleSignals(doneCh)
	go sum.eofCoordinator.Run()

	err := sum.inputQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		if err := inner.HandleMessage(sum, msg, ack, nack); err != nil {
			_ = sum.inputQueue.StopConsuming()

			if sum.running.Load() {
				slog.Error("MAIN", "err", err)
			}
		}
	})
	if err != nil {
		if sum.running.Load() {
			slog.Error("MAIN While consuming messages from input queue", "err", err)
		}
	}
	<-doneCh
}

func (sum *Sum) close() {
	finalErr := sum.inputQueue.StopConsuming()

	if finalErr == nil {
		finalErr = sum.inputQueue.Close()
	}
	err := sum.sumExchange.Close()

	if err != nil {
		finalErr = err
	}
	if finalErr != nil {
		if sum.running.Load() {
			slog.Error("MAIN While closing middleware", "err", finalErr)
		}
	}
	sum.eofCoordinator.close()
}

func (sum *Sum) handleSignals(doneCh chan struct{}) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	<-signals
	slog.Info("SIGTERM signal received")

	sum.running.Store(false)
	sum.eofCoordinator.running.Store(false)
	sum.close()

	doneCh <- struct{}{}
	close(doneCh)
	close(signals)
}

func (sum *Sum) HandleDataMessage(messageBody *messagebody.MessageBody) error {
	sum.mutex.Lock()
	defer sum.mutex.Unlock()

	clientId := messageBody.ClientId
	fruitRecords := messageBody.FruitRecords

	sum.clientFruitRecords.AddRecords(clientId, fruitRecords)

	if sum.clientRecordInfo.HasReceivedEof(clientId) {
		message, err := inner.SerializeRecordAmountMessage(clientId, len(fruitRecords))

		if err != nil {
			return fmt.Errorf("while serializing record amount message: %w", err)
		}
		if err = sum.sumExchange.Send(*message); err != nil {
			return fmt.Errorf("while sending record amount message: %w", err)
		}
	} else {
		sum.clientRecordInfo.AddRecords(clientId, len(fruitRecords))
	}
	return nil
}

func (sum *Sum) HandleEofMessage(messageBody *messagebody.MessageBody) error {
	clientId := messageBody.ClientId
	slog.Info("MAIN Received Eof message", "client_id", clientId)

	message, err := inner.SerializeRecordAmountEofMessage(clientId, messageBody.RecordAmount)

	if err != nil {
		return fmt.Errorf("while serializing record amount eof message: %w", err)
	}
	return sum.sumExchange.Send(*message)
}
