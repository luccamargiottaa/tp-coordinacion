package sum

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/clientrecordinfo"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/clientfruitrecords"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner/messagebody"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type EofCoordinator struct {
	sumExchange        middleware.Middleware
	outputExchanges    []middleware.Middleware
	mutex              *sync.Mutex
	clientFruitRecords *clientfruitrecords.ClientFruitRecords
	clientRecordInfo   *clientrecordinfo.ClientRecordInfo
	running            atomic.Bool
}

func newEofCoordinator(
	config SumConfig,
	clientFruitRecords *clientfruitrecords.ClientFruitRecords,
	clientRecordInfo *clientrecordinfo.ClientRecordInfo,
	mutex *sync.Mutex,
) (*EofCoordinator, error) {

	connSettings := middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}
	keys := []string{config.SumPrefix}
	sumExchange, err := middleware.CreateExchangeMiddleware(config.SumPrefix, keys, connSettings)

	if err != nil {
		return nil, err
	}
	outputExchanges := make([]middleware.Middleware, config.AggregationAmount)

	for i := range outputExchanges {
		key := fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
		outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, []string{key}, connSettings)

		if err != nil {
			_ = sumExchange.Close()

			for j := range i {
				_ = outputExchanges[j].Close()
			}
			return nil, err
		}
		outputExchanges[i] = outputExchange
	}
	eofCoordinator := &EofCoordinator{
		sumExchange:        sumExchange,
		outputExchanges:    outputExchanges,
		mutex:              mutex,
		clientFruitRecords: clientFruitRecords,
		clientRecordInfo:   clientRecordInfo,
	}
	return eofCoordinator, nil
}

func (eofCoordinator *EofCoordinator) Run() {
	defer eofCoordinator.close()

	err := eofCoordinator.sumExchange.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		if err := inner.HandleMessage(eofCoordinator, msg, ack, nack); err != nil {
			_ = eofCoordinator.sumExchange.StopConsuming()

			if eofCoordinator.running.Load() {
				slog.Error("COORDINATOR", "err", err)
			}
		}
	})
	if err != nil {
		if eofCoordinator.running.Load() {
			slog.Error("COORDINATOR While consuming messages from sum exchange", "err", err)
		}
	}
}

func (eofCoordinator *EofCoordinator) close() {
	finalErr := eofCoordinator.sumExchange.StopConsuming()

	if finalErr == nil {
		finalErr = eofCoordinator.sumExchange.Close()
	}
	for _, outputExchange := range eofCoordinator.outputExchanges {
		if err := outputExchange.Close(); err != nil {
			finalErr = err
		}
	}
	if finalErr != nil {
		if eofCoordinator.running.Load() {
			slog.Error("COORDINATOR While closing middleware", "err", finalErr)
		}
	}
}

func (eofCoordinator *EofCoordinator) HandleDataMessage(messageBody *messagebody.MessageBody) error {
	eofCoordinator.mutex.Lock()
	defer eofCoordinator.mutex.Unlock()

	clientId := messageBody.ClientId

	eofCoordinator.clientRecordInfo.AddRecords(clientId, messageBody.RecordAmount)

	if eofCoordinator.clientRecordInfo.HasReachedExpectedRecords(clientId) {
		slog.Info("COORDINATOR Sending records", "client_id", clientId)

		err := eofCoordinator.sendFruitRecords(clientId)

		if err != nil {
			return err
		}
		eofCoordinator.clientRecordInfo.DeleteRecord(clientId)
	}
	return nil
}

func (eofCoordinator *EofCoordinator) sendFruitRecords(clientId uint64) error {
	for fruitRecord := range eofCoordinator.clientFruitRecords.GetRecords(clientId) {
		if err := eofCoordinator.sendFruitRecord(clientId, fruitRecord); err != nil {
			return err
		}
	}
	message, err := inner.SerializeEofMessage(clientId)

	if err != nil {
		return fmt.Errorf("while serializing EOF message: %w", err)
	}
	for _, outputExchange := range eofCoordinator.outputExchanges {
		if err = outputExchange.Send(*message); err != nil {
			return fmt.Errorf("while sending EOF message: %w", err)
		}
	}
	eofCoordinator.clientFruitRecords.DeleteRecords(clientId)

	return nil
}

func (eofCoordinator *EofCoordinator) sendFruitRecord(clientId uint64, fruitRecord fruititem.FruitItem) error {
	message, err := inner.SerializeFruitRecordMessage(clientId, fruitRecord)

	if err != nil {
		return fmt.Errorf("while serializing message: %w", err)
	}
	outputExchange := eofCoordinator.getOutputExchange(clientId, fruitRecord)

	if err = outputExchange.Send(*message); err != nil {
		return fmt.Errorf("while sending message: %w", err)
	}
	return nil
}

func (eofCoordinator *EofCoordinator) getOutputExchange(clientId uint64, fruitRecord fruititem.FruitItem) middleware.Middleware {
	hash := fnv.New32a()

	key := fmt.Sprintf("%s_%d", fruitRecord.Fruit, clientId)
	_, _ = hash.Write([]byte(key))

	hashing := hash.Sum32()
	index := hashing % uint32(len(eofCoordinator.outputExchanges)) //nolint:gosec

	return eofCoordinator.outputExchanges[index]
}

func (eofCoordinator *EofCoordinator) HandleEofMessage(messageBody *messagebody.MessageBody) error {
	clientId := messageBody.ClientId
	slog.Info("COORDINATOR Received Eof message", "client_id", clientId)

	eofCoordinator.mutex.Lock()
	defer eofCoordinator.mutex.Unlock()

	eofCoordinator.clientRecordInfo.SetExpectedRecords(clientId, messageBody.RecordAmount)

	recordAmount := eofCoordinator.clientRecordInfo.EmptyRecordAmount(clientId)
	message, err := inner.SerializeRecordAmountMessage(clientId, recordAmount)

	if err != nil {
		return fmt.Errorf("while serializing Record Amount message: %w", err)
	}
	if err = eofCoordinator.sumExchange.Send(*message); err != nil {
		return fmt.Errorf("while sending Record Amount message: %w", err)
	}
	return nil
}
