package sum

import (
	"fmt"
	"hash/fnv"
	"log/slog"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/boundedset"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/clientfruitrecords"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

const maxFinishedClients = 5000

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
	outputExchanges    []middleware.Middleware
	clientFruitRecords clientfruitrecords.ClientFruitRecords
	sumAmount          int
	finishedClients    boundedset.BoundedSet
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
	outputExchanges := make([]middleware.Middleware, config.AggregationAmount)

	for i := range outputExchanges {
		key := fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
		outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, []string{key}, connSettings)

		if err != nil {
			_ = inputQueue.Close()

			for j := range i {
				_ = outputExchanges[j].Close()
			}
			return nil, err
		}
		outputExchanges[i] = outputExchange
	}
	clientFruitRecords := clientfruitrecords.NewClientFruitRecords()
	finishedClients := boundedset.NewBoundedSet(maxFinishedClients)

	sum := &Sum{
		inputQueue:         inputQueue,
		outputExchanges:    outputExchanges,
		clientFruitRecords: *clientFruitRecords,
		sumAmount:          config.SumAmount,
		finishedClients:    *finishedClients,
	}
	return sum, nil
}

func (sum *Sum) Run() {
	defer sum.close()

	err := sum.inputQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		if err := sum.handleMessage(msg, ack, nack); err != nil {
			_ = sum.inputQueue.StopConsuming()
		}
	})
	if err != nil {
		slog.Error("While consuming messages from input queue", "err", err)
	}
}

func (sum *Sum) close() {
	finalErr := sum.inputQueue.StopConsuming()

	if finalErr == nil {
		finalErr = sum.inputQueue.Close()
	}
	for _, outputExchange := range sum.outputExchanges {
		if err := outputExchange.Close(); err != nil {
			finalErr = err
		}
	}
	if finalErr != nil {
		slog.Error("While closing middleware", "err", finalErr)
	}
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) error {
	clientId, fruitRecords, isEof, notifyEof, err := inner.DeserializeMessage(&msg)

	if err != nil {
		slog.Error("While deserializing message", "err", err)
		nack()

		return err
	}
	defer ack()

	if isEof {
		if err = sum.handleEndOfRecordMessage(clientId, notifyEof); err != nil {
			slog.Error("While handling end of record message", "err", err)

			return err
		}
	} else {
		sum.handleDataMessage(clientId, fruitRecords)
	}
	return nil
}

func (sum *Sum) handleDataMessage(clientId uint64, fruitRecords []fruititem.FruitItem) {
	sum.clientFruitRecords.AddRecords(clientId, fruitRecords)
}

func (sum *Sum) handleEndOfRecordMessage(clientId uint64, notifyEof bool) error {
	slog.Info("Received End Of Records message")

	if sum.finishedClients.Contains(clientId) {
		message, err := inner.SerializeEofMessage(clientId)

		if err != nil {
			slog.Debug("While serializing message", "err", err)

			return err
		}
		if err = sum.inputQueue.Send(*message); err != nil {
			slog.Debug("While sending EOF message", "err", err)

			return err
		}
		return nil
	}
	if notifyEof {
		if err := sum.notifyEof(clientId); err != nil {
			return err
		}
	}
	if err := sum.sendFruitRecords(clientId); err != nil {
		return err
	}
	sum.finishedClients.Add(clientId)

	return nil
}

func (sum *Sum) notifyEof(clientId uint64) error {
	slog.Info("Notifying peers of End Of Records message")

	message, err := inner.SerializeEofMessage(clientId)

	if err != nil {
		slog.Debug("While serializing message", "err", err)

		return err
	}
	for range sum.sumAmount - 1 {
		if err = sum.inputQueue.Send(*message); err != nil {
			slog.Debug("While sending EOF message", "err", err)

			return err
		}
	}
	return nil
}

func (sum *Sum) sendFruitRecords(clientId uint64) error {
	for fruitRecord := range sum.clientFruitRecords.GetRecords(clientId) {
		if err := sum.sendFruitRecord(clientId, fruitRecord); err != nil {
			return err
		}
	}
	message, err := inner.SerializeEofMessage(clientId)

	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)

		return err
	}
	for _, outputExchange := range sum.outputExchanges {
		if err = outputExchange.Send(*message); err != nil {
			slog.Debug("While sending EOF message", "err", err)

			return err
		}
	}
	sum.clientFruitRecords.DeleteRecords(clientId)

	return nil
}

func (sum *Sum) sendFruitRecord(clientId uint64, fruitRecord fruititem.FruitItem) error {
	message, err := inner.SerializeFruitRecordMessage(clientId, fruitRecord)

	if err != nil {
		slog.Debug("While serializing message", "err", err)

		return err
	}
	outputExchange := sum.getOutputExchange(clientId, fruitRecord)

	if err = outputExchange.Send(*message); err != nil {
		slog.Debug("While sending message", "err", err)

		return err
	}
	return nil
}

func (sum *Sum) getOutputExchange(clientId uint64, fruitRecord fruititem.FruitItem) middleware.Middleware {
	hash := fnv.New32a()

	key := fmt.Sprintf("%s_%d", fruitRecord.Fruit, clientId)
	_, _ = hash.Write([]byte(key))

	hashing := hash.Sum32()
	index := hashing % uint32(len(sum.outputExchanges)) //nolint:gosec

	return sum.outputExchanges[index]
}
