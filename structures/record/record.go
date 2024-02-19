package record

import (
	"encoding/binary"
	"github.com/IvanaaXD/NASP---Projekat/app/config"
)

// Struktura sloga u memoriji
type Record struct {
	Key       string
	Value     []byte
	Timestamp int64
	Tombstone bool
}

func BytesToRecord(b []byte) Record {
	timestampBytes := b[config.GlobalConfig.TimestampStart : config.GlobalConfig.TimestampStart+config.GlobalConfig.TimestampSize]
	timestamp := binary.LittleEndian.Uint64(timestampBytes)

	tombstoneByte := b[config.GlobalConfig.TombstoneStart]
	var tombstone bool
	if tombstoneByte == 0 {
		tombstone = false
	} else {
		tombstone = true
	}

	keySizeBytes := b[config.GlobalConfig.KeySizeStart : config.GlobalConfig.KeySizeStart+config.GlobalConfig.KeySizeSize]
	keySize := binary.LittleEndian.Uint64(keySizeBytes)
	key := string(b[config.GlobalConfig.KeyStart : int64(config.GlobalConfig.KeyStart)+int64(keySize)])

	valueSizeBytes := b[config.GlobalConfig.ValueSizeStart : config.GlobalConfig.ValueSizeStart+config.GlobalConfig.ValueSizeSize]
	valueSize := binary.LittleEndian.Uint64(valueSizeBytes)
	value := b[int64(config.GlobalConfig.KeyStart)+int64(keySize) : int64(config.GlobalConfig.KeyStart)+int64(keySize)+int64(valueSize)]

	return Record{Key: key, Value: value, Timestamp: int64(timestamp), Tombstone: tombstone}
}
