package sum

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
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
	inputQueue     middleware.Middleware
	outputExchange middleware.Middleware
	fruitItemMap   map[string]fruititem.FruitItem
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
	outputExchangeRouteKeys := make([]string, config.AggregationAmount)

	for i := range config.AggregationAmount {
		outputExchangeRouteKeys[i] = fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
	}
	outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, outputExchangeRouteKeys, connSettings)

	if err != nil {
		_ = inputQueue.Close()

		return nil, err
	}
	sum := &Sum{
		inputQueue:     inputQueue,
		outputExchange: outputExchange,
		fruitItemMap:   map[string]fruititem.FruitItem{},
	}
	return sum, nil
}

func (sum *Sum) Run() {
	defer sum.close()

	err := sum.inputQueue.StartConsuming(sum.handleMessage)

	if err != nil {
		slog.Error("While consuming messages from input queue", "err", err)
	}
}

func (sum *Sum) close() {
	err1 := sum.inputQueue.Close()
	err2 := sum.outputExchange.Close()

	if err := errors.Join(err1, err2); err != nil {
		slog.Error("While closing middleware", "err", err)
	}
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), _ func()) {
	defer ack()

	fruitRecords, isEof, err := inner.DeserializeMessage(&msg)

	if err != nil {
		slog.Error("While deserializing message", "err", err)

		return
	}
	if isEof {
		if err = sum.handleEndOfRecordMessage(); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}
	sum.handleDataMessage(fruitRecords)
}

func (sum *Sum) handleEndOfRecordMessage() error {
	slog.Info("Received End Of Records message")

	for key := range sum.fruitItemMap {
		fruitRecord := []fruititem.FruitItem{sum.fruitItemMap[key]}
		message, err := inner.SerializeMessage(fruitRecord)

		if err != nil {
			slog.Debug("While serializing message", "err", err)

			return err
		}
		if err = sum.outputExchange.Send(*message); err != nil {
			slog.Debug("While sending message", "err", err)

			return err
		}
	}
	var eofMessage []fruititem.FruitItem
	message, err := inner.SerializeMessage(eofMessage)

	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)

		return err
	}
	if err = sum.outputExchange.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)

		return err
	}
	return nil
}

func (sum *Sum) handleDataMessage(fruitRecords []fruititem.FruitItem) {
	for _, fruitRecord := range fruitRecords {
		if _, ok := sum.fruitItemMap[fruitRecord.Fruit]; ok {
			sum.fruitItemMap[fruitRecord.Fruit] = sum.fruitItemMap[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			sum.fruitItemMap[fruitRecord.Fruit] = fruitRecord
		}
	}
}
