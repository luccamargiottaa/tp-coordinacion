package join

import (
	"log/slog"
	"sort"

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

type fruitItemMap map[string]fruititem.FruitItem
type clientFruitItemMap map[uint64]fruitItemMap

type Join struct {
	inputQueue         middleware.Middleware
	outputQueue        middleware.Middleware
	clientFruitItemMap clientFruitItemMap
	topSize            int
	aggregationAmount  int
	clientEofs         map[uint64]int
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
	join := &Join{
		inputQueue:         inputQueue,
		outputQueue:        outputQueue,
		clientFruitItemMap: make(clientFruitItemMap),
		topSize:            config.TopSize,
		aggregationAmount:  config.AggregationAmount,
		clientEofs:         make(map[uint64]int),
	}
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
	finalErr := join.inputQueue.Close()

	if err := join.outputQueue.Close(); err != nil {
		finalErr = err
	}
	if finalErr != nil {
		slog.Error("While closing middleware", "err", finalErr)
	}
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
	clientID, fruitTop, isEof, _, err := inner.DeserializeMessage(&msg)

	if err != nil {
		slog.Error("While deserializing message", "err", err)
		nack()

		return
	}
	defer ack()

	if isEof {
		if err = join.handleEndOfRecordsMessage(clientID); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}
	join.handleDataMessage(clientID, fruitTop)
}

func (join *Join) handleEndOfRecordsMessage(clientID uint64) error {
	slog.Info("Received End Of Records message")

	count, ok := join.clientEofs[clientID]

	if !ok {
		join.clientEofs[clientID] = 1
		count = 1
	} else {
		count++
		join.clientEofs[clientID] = count
	}
	if count < join.aggregationAmount {
		return nil
	}
	if err := join.sendFruitTop(clientID); err != nil {
		return err
	}
	delete(join.clientEofs, clientID)

	return nil
}

func (join *Join) sendFruitTop(clientID uint64) error {
	fruitTopRecords := join.buildFruitTop(clientID)

	if fruitTopRecords == nil {
		slog.Debug("Empty client top")

		return nil
	}
	message, err := inner.SerializeMessage(clientID, fruitTopRecords, false, false)

	if err != nil {
		slog.Debug("While serializing top message", "err", err)

		return err
	}
	if err = join.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending top message", "err", err)

		return err
	}
	delete(join.clientFruitItemMap, clientID)

	return nil
}

func (join *Join) handleDataMessage(clientID uint64, fruitRecords []fruititem.FruitItem) {
	fruitMap, ok := join.clientFruitItemMap[clientID]

	if !ok {
		fruitMap = make(map[string]fruititem.FruitItem)
		join.clientFruitItemMap[clientID] = fruitMap
	}
	for _, fruitRecord := range fruitRecords {
		if _, ok := fruitMap[fruitRecord.Fruit]; ok {
			fruitMap[fruitRecord.Fruit] = fruitMap[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			fruitMap[fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (join *Join) buildFruitTop(clientID uint64) []fruititem.FruitItem {
	fruitMap, ok := join.clientFruitItemMap[clientID]

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
	finalTopSize := min(join.topSize, len(fruitItems))

	return fruitItems[:finalTopSize]
}
