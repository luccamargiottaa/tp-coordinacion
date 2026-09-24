package aggregation

import (
	"errors"
	"fmt"
	"log/slog"
	"sort"

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

type fruitItemMap map[string]fruititem.FruitItem
type clientFruitItemMap map[uint64]fruitItemMap

type Aggregation struct {
	outputQueue        middleware.Middleware
	inputExchange      middleware.Middleware
	clientFruitItemMap clientFruitItemMap
	topSize            int
	sumAmount          int
	eofPerClient       map[uint64]int
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
	inputExchangeRoutingKey := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, inputExchangeRoutingKey, connSettings)

	if err != nil {
		_ = outputQueue.Close()

		return nil, err
	}
	aggregation := &Aggregation{
		outputQueue:        outputQueue,
		inputExchange:      inputExchange,
		clientFruitItemMap: make(clientFruitItemMap),
		topSize:            config.TopSize,
		sumAmount:          config.SumAmount,
		eofPerClient:       make(map[uint64]int),
	}
	return aggregation, nil
}

func (aggregation *Aggregation) Run() {
	defer aggregation.close()

	err := aggregation.inputExchange.StartConsuming(aggregation.handleMessage)

	if err != nil {
		slog.Error("While consuming messages from input queue", "err", err)
	}
}

func (aggregation *Aggregation) close() {
	err1 := aggregation.outputQueue.Close()
	err2 := aggregation.inputExchange.Close()

	if err := errors.Join(err1, err2); err != nil {
		slog.Error("While closing middleware", "err", err)
	}
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), _ func()) {
	defer ack()

	clientID, fruitRecords, isEof, _, err := inner.DeserializeMessage(&msg)

	if err != nil {
		slog.Error("While deserializing message", "err", err)

		return
	}
	if isEof {
		if err = aggregation.handleEndOfRecordsMessage(clientID); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}
	aggregation.handleDataMessage(clientID, fruitRecords)
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(clientID uint64) error {
	slog.Info("Received End Of Records message")

	count, ok := aggregation.eofPerClient[clientID]

	if !ok {
		aggregation.eofPerClient[clientID] = 1
		count = 1
	} else {
		count++
		aggregation.eofPerClient[clientID] = count
	}
	if count < aggregation.sumAmount {
		return nil
	}
	if err := aggregation.sendFruitTop(clientID); err != nil {
		return err
	}
	delete(aggregation.eofPerClient, clientID)

	return nil
}

func (aggregation *Aggregation) sendFruitTop(clientID uint64) error {
	fruitTopRecords := aggregation.buildFruitTop(clientID)

	if fruitTopRecords != nil {
		message, err := inner.SerializeMessage(clientID, fruitTopRecords, false, false)

		if err != nil {
			slog.Debug("While serializing top message", "err", err)

			return err
		}
		if err = aggregation.outputQueue.Send(*message); err != nil {
			slog.Debug("While sending top message", "err", err)

			return err
		}
	}
	message, err := inner.SerializeMessage(clientID, nil, true, false)

	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)

		return err
	}
	if err = aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)

		return err
	}
	delete(aggregation.clientFruitItemMap, clientID)

	return nil
}

func (aggregation *Aggregation) handleDataMessage(clientID uint64, fruitRecords []fruititem.FruitItem) {
	fruitMap, ok := aggregation.clientFruitItemMap[clientID]

	if !ok {
		fruitMap = make(map[string]fruititem.FruitItem)
		aggregation.clientFruitItemMap[clientID] = fruitMap
	}
	for _, fruitRecord := range fruitRecords {
		if _, ok := fruitMap[fruitRecord.Fruit]; ok {
			fruitMap[fruitRecord.Fruit] = fruitMap[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			fruitMap[fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (aggregation *Aggregation) buildFruitTop(clientID uint64) []fruititem.FruitItem {
	fruitMap, ok := aggregation.clientFruitItemMap[clientID]

	if !ok {
		return nil
	}
	fruitItems := make([]fruititem.FruitItem, 0, len(fruitMap))

	for _, item := range fruitMap {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(aggregation.topSize, len(fruitItems))

	return fruitItems[:finalTopSize]
}
