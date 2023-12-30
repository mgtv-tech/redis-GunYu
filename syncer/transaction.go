package syncer

type txnStatus int

const (
	txnStatusNo      txnStatus = iota // no
	txnStatusBarrier txnStatus = iota // force flush
	txnStatusBegin   txnStatus = iota // multi start a transaction
	txnStatusIn      txnStatus = iota // in a transaction
	txnStatusCommit  txnStatus = iota // exec commit a transaction
)

// https://redis.io/docs/interact/transactions/
// discard and watch are ignored because master just sends a committed transaction to replicas
var (
	transactionCmdMap = map[string]txnStatus{
		"select": txnStatusBarrier,
		"multi":  txnStatusBegin,
		"exec":   txnStatusCommit,
	}
)

// transaction
func transactionStatus(cmd string, prevTxnStatus txnStatus) (txnStatus, bool) {
	switch prevTxnStatus {
	case txnStatusNo, txnStatusBarrier, txnStatusCommit:
		ret, ok := transactionCmdMap[cmd]
		if !ok {
			return txnStatusNo, false
		}
		return ret, true
	case txnStatusBegin, txnStatusIn:
		ret := transactionCmdMap[cmd]
		if ret == txnStatusCommit {
			return txnStatusCommit, true
		}
		return txnStatusIn, false
	}
	return txnStatusNo, true
}
