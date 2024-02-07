package io

import (
	"github.com/IvanaaXD/NASP---Projekat/config"
	"github.com/IvanaaXD/NASP---Projekat/record"
	"github.com/IvanaaXD/NASP---Projekat/structures"
	"github.com/IvanaaXD/NASP---Projekat/wal"
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

	err = structures.Memtables.Write(rec)
	id := structures.Memtables.Current
	wal.WriteOffsets(id, lenOfRec)

	if err != nil {
		return false
	}

	return true
}

// DELETE (Brisanje sloga)

func Delete(key string, timestamp int64) bool {
	value := []byte("d")
	tombstone := true

	log, err := wal.NewWAL(config.GlobalConfig.WalPath)
	if err != nil {
		return false
	}

	_, err2 := log.Write([]byte(key), value, timestamp, tombstone)
	if err2 != nil {
		return false
	}

	record := record.Record{Key: key, Value: value, Timestamp: timestamp, Tombstone: tombstone}

	success := structures.Memtables.Delete(record.Key)

	if success == nil {
		structures.Cache.Delete(record)
	}
	return true
}
