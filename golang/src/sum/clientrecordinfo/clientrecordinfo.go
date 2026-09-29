package clientrecordinfo

type ClientRecordInfo struct {
	clientMap map[uint64]RecordInfo
}

func NewClientRecordInfo() *ClientRecordInfo {
	return &ClientRecordInfo{
		clientMap: make(map[uint64]RecordInfo),
	}
}

func (clientRecordInfo *ClientRecordInfo) AddRecords(clientId uint64, recordAmount int) {
	recordInfo := clientRecordInfo.getRecordInfo(clientId)
	recordInfo.addRecords(recordAmount)
	clientRecordInfo.clientMap[clientId] = recordInfo
}

func (clientRecordInfo *ClientRecordInfo) SetExpectedRecords(clientId uint64, expectedRecords int) {
	recordInfo := clientRecordInfo.getRecordInfo(clientId)
	recordInfo.setExpectedRecords(expectedRecords)
	clientRecordInfo.clientMap[clientId] = recordInfo
}

func (clientRecordInfo *ClientRecordInfo) EmptyRecordAmount(clientId uint64) int {
	recordInfo := clientRecordInfo.getRecordInfo(clientId)
	recordAmount := recordInfo.emptyRecordAmount()
	clientRecordInfo.clientMap[clientId] = recordInfo

	return recordAmount
}

func (clientRecordInfo *ClientRecordInfo) HasReachedExpectedRecords(clientId uint64) bool {
	recordInfo := clientRecordInfo.getRecordInfo(clientId)

	return recordInfo.hasReachedExpectedRecords()
}

func (clientRecordInfo *ClientRecordInfo) HasReceivedEof(clientId uint64) bool {
	recordInfo := clientRecordInfo.getRecordInfo(clientId)

	return recordInfo.hasReceivedEof()
}

func (clientRecordInfo *ClientRecordInfo) DeleteRecord(clientId uint64) {
	delete(clientRecordInfo.clientMap, clientId)
}

func (clientRecordInfo *ClientRecordInfo) getRecordInfo(clientId uint64) RecordInfo {
	recordInfo, ok := clientRecordInfo.clientMap[clientId]

	if !ok {
		recordInfo = *newRecordInfo()
	}
	return recordInfo
}
