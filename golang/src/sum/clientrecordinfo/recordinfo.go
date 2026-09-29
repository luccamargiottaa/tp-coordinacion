package clientrecordinfo

type RecordInfo struct {
	amount          int
	expectedRecords int
	eofReceived     bool
}

func newRecordInfo() *RecordInfo {
	return &RecordInfo{0, 0, false}
}

func (recordInfo *RecordInfo) addRecords(recordAmount int) {
	recordInfo.amount += recordAmount
}

func (recordInfo *RecordInfo) setExpectedRecords(expectedRecords int) {
	recordInfo.expectedRecords = expectedRecords
	recordInfo.eofReceived = true
}

func (recordInfo *RecordInfo) emptyRecordAmount() int {
	recordAmount := recordInfo.amount
	recordInfo.amount = 0

	return recordAmount
}

func (recordInfo *RecordInfo) hasReachedExpectedRecords() bool {
	return recordInfo.amount == recordInfo.expectedRecords
}

func (recordInfo *RecordInfo) hasReceivedEof() bool {
	return recordInfo.eofReceived
}
