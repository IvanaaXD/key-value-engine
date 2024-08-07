package tokenbucketv2

import (
	"encoding/binary"
	"time"
)

type TokenBucket struct {
	lastRefreshed      int64
	refreshingInterval uint16
	numTokens          uint16
	maxTokens          uint16
}

func MakeTokenBucket(maxTokens uint16, refreshingInterval uint16) *TokenBucket {
	return &TokenBucket{lastRefreshed: time.Now().Unix(), refreshingInterval: refreshingInterval, numTokens: maxTokens, maxTokens: maxTokens}
}

func (tb *TokenBucket) refreshIfNeeded() {
	currTime := time.Now().Unix()
	if tb.lastRefreshed+int64(tb.refreshingInterval) <= currTime {
		tb.numTokens = tb.maxTokens
		tb.lastRefreshed = currTime
	}
}

func (tb *TokenBucket) TokensAvailable() bool {
	tb.refreshIfNeeded()
	if tb.numTokens > 0 {
		tb.numTokens -= 1
		return true
	}
	return false
}

func (tb *TokenBucket) Serialize() []byte {
	serialized := make([]byte, 0)
	lrBytes := make([]byte, 8)
	riBytes := make([]byte, 2)
	ntBytes := make([]byte, 2)
	mtBytes := make([]byte, 2)
	binary.LittleEndian.PutUint64(lrBytes, uint64(tb.lastRefreshed))
	binary.LittleEndian.PutUint16(riBytes, tb.refreshingInterval)
	binary.LittleEndian.PutUint16(ntBytes, tb.numTokens)
	binary.LittleEndian.PutUint16(mtBytes, tb.maxTokens)
	serialized = append(serialized, lrBytes...)
	serialized = append(serialized, riBytes...)
	serialized = append(serialized, ntBytes...)
	serialized = append(serialized, mtBytes...)
	return serialized
}

func Deserialize(bytes []byte) *TokenBucket {
	if len(bytes) != 14 {
		return nil
	}
	lrBytes := bytes[:8]
	riBytes := bytes[8:10]
	ntBytes := bytes[10:12]
	mtBytes := bytes[12:]
	lr := int64(binary.LittleEndian.Uint64(lrBytes))
	ri := binary.LittleEndian.Uint16(riBytes)
	nt := binary.LittleEndian.Uint16(ntBytes)
	mt := binary.LittleEndian.Uint16(mtBytes)
	return &TokenBucket{lastRefreshed: lr, refreshingInterval: ri, numTokens: nt, maxTokens: mt}
}
