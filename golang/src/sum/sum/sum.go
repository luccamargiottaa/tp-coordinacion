package sum

import (
	"log/slog"
	"sync"

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
	eofCoordinator     EofCoordinator
	mutex              *sync.Mutex
	clientFruitRecords *clientfruitrecords.ClientFruitRecords
	clientRecordInfo   *clientrecordinfo.ClientRecordInfo
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
		inputQueue,
		sumExchange,
		*eofCoordinator,
		mutex,
		clientFruitRecords,
		clientRecordInfo,
	}
	return sum, nil
}

func (sum *Sum) Run() {
	defer sum.close()

	go sum.eofCoordinator.Run()

	err := sum.inputQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		if err := inner.HandleMessage(sum, msg, ack, nack); err != nil {
			_ = sum.inputQueue.StopConsuming()
		}
	})
	if err != nil {
		slog.Error("MAIN While consuming messages from input queue", "err", err)
	}
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
		slog.Error("MAIN While closing middleware", "err", finalErr)
	}
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
			return err
		}
		if err = sum.sumExchange.Send(*message); err != nil {
			return err
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
		return err
	}
	return sum.sumExchange.Send(*message)
}
