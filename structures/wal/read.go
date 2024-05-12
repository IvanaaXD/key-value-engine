package wal

import (
	"encoding/binary"
	"github.com/IvanaaXD/NASP/app/config"
	"github.com/IvanaaXD/NASP/structures/record"
	"os"
)

func ReadWalRecord(f *os.File) (record.Record, error) {
	_, err := f.Seek(4, 1) // Preskakanje CRC-a
	if err != nil {
		return record.Record{}, err
	}

	timestampBuff := make([]byte, config.GlobalConfig.TimestampSize)
	_, err = f.Read(timestampBuff)
	if err != nil {
		return record.Record{}, err
	}
	timestamp := binary.LittleEndian.Uint64(timestampBuff)

	tombstoneBuff := make([]byte, config.GlobalConfig.TombstoneSize)
	_, err = f.Read(tombstoneBuff)
	if err != nil {
		return record.Record{}, err
	}
	tombstone := tombstoneBuff[0] != 0

	keySizeBuff := make([]byte, config.GlobalConfig.KeySizeSize)
	_, err = f.Read(keySizeBuff)
	if err != nil {
		return record.Record{}, err
	}
	keySize := binary.LittleEndian.Uint64(keySizeBuff)

	valueSizeBuff := make([]byte, config.GlobalConfig.ValueSizeSize)
	_, err = f.Read(valueSizeBuff)
	if err != nil {
		return record.Record{}, err
	}
	valueSize := binary.LittleEndian.Uint64(valueSizeBuff)

	keyBuff := make([]byte, keySize)
	_, err = f.Read(keyBuff)
	if err != nil {
		return record.Record{}, err
	}
	key := string(keyBuff)

	value := make([]byte, valueSize)
	_, err = f.Read(value)
	if err != nil {
		return record.Record{}, err
	}

	return record.Record{
		Key:       key,
		Value:     value,
		Timestamp: int64(timestamp),
		Tombstone: tombstone,
	}, nil
}
