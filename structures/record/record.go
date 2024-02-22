package record

import (
	"encoding/binary"
	"github.com/IvanaaXD/NASP/app/config"
)

type Record struct {
	Key       string
	Value     []byte
	Timestamp int64
	Tombstone bool
}

func BytesToRec(b []byte) Record {

	config.Init()

	timestampBytes := b[0:config.GlobalConfig.TimestampSize]
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

	valueSizeBytes := b[config.GlobalConfig.ValueSizeStart+int(keySize) : config.GlobalConfig.ValueSizeStart+int(keySize)+config.GlobalConfig.ValueSizeSize]
	valueSize := binary.LittleEndian.Uint64(valueSizeBytes)
	value := b[int64(config.GlobalConfig.ValueSizeStart)+int64(keySize)+int64(config.GlobalConfig.ValueSizeSize) : int64(config.GlobalConfig.ValueSizeStart)+int64(keySize)+int64(config.GlobalConfig.ValueSizeSize)+int64(valueSize)]

	return Record{Key: key, Value: value, Timestamp: int64(timestamp), Tombstone: tombstone}
}

func RecToBytes(record Record) []byte {

	config.Init()

	timestampBytes := make([]byte, config.GlobalConfig.TimestampSize)
	binary.LittleEndian.PutUint64(timestampBytes, uint64(record.Timestamp))

	var tombstoneByte byte
	if record.Tombstone {
		tombstoneByte = 1
	}

	keySizeBytes := make([]byte, config.GlobalConfig.KeySizeSize)
	binary.LittleEndian.PutUint64(keySizeBytes, uint64(len(record.Key)))

	valueSizeBytes := make([]byte, config.GlobalConfig.ValueSizeSize)
	binary.LittleEndian.PutUint64(valueSizeBytes, uint64(len(record.Value)))

	// Check if slices have enough space before accessing them
	if len(keySizeBytes) < config.GlobalConfig.KeySizeSize ||
		len(valueSizeBytes) < config.GlobalConfig.ValueSizeSize {
		panic("Key or Value byte slices are too short")
	}

	result := append(timestampBytes, tombstoneByte)
	result = append(result, keySizeBytes...)
	result = append(result, []byte(record.Key)...)
	result = append(result, valueSizeBytes...)
	result = append(result, record.Value...)

	return result
}
