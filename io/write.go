package io

import (
	"github.com/IvanaaXD/NASP/app/config"
	"github.com/IvanaaXD/NASP/init"
	"github.com/IvanaaXD/NASP/structures/record"
	"github.com/IvanaaXD/NASP/structures/wal"
)

// PUT (Novi slog / azuriranje sloga)

func Put(key string, value []byte, timestamp int64) bool {
	tombstone := false

	log, err := wal.NewWAL(config.GlobalConfig.WalPath)
	if err != nil {
		return false
	}

	var lenOfRec int
	lenOfRec, err = log.Write([]byte(key), value, timestamp, tombstone)
	if err != nil {
		return false
	}

	rec := record.Record{Key: key, Value: value, Timestamp: timestamp, Tombstone: tombstone}

	err = init.Memtables.Write(rec)
	id := init.Memtables.Current
	err = wal.WriteOffsets(id, lenOfRec)
	if err != nil {
		return false
	}

	if err != nil {
		return false
	}

	return true
}

// DELETE (Brisanje sloga)

func Delete(key string, timestamp int64) bool {
	//value := []byte("d")
	//tombstone := true
	//
	//log, err := wal.NewWAL(config.GlobalConfig.WalPath)
	//if err != nil {
	//	return false
	//}
	//
	//_, err2 := log.Write([]byte(key), value, timestamp, tombstone)
	//if err2 != nil {
	//	return false
	//}
	//
	//record := record.Record{Key: key, Value: value, Timestamp: timestamp, Tombstone: tombstone}
	//
	//success := NASP.Memtables.Delete(record.Key)
	//
	//if success == nil {
	//	NASP.Cache.Delete(record)
	//}
	return true
}
