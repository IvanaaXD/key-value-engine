package io

import (
	"github.com/IvanaaXD/NASP/inicialize"
	"github.com/IvanaaXD/NASP/structures/record"
)

// PUT (Novi slog / azuriranje sloga)

func Put(key string, value []byte, timestamp int64) bool {

	rec := record.Record{Key: key, Value: value, Timestamp: timestamp, Tombstone: false}

	err := inicialize.Memtables.Write(rec)
	if err != nil {
		return false
	}

	return true
}

// DELETE (Brisanje sloga)

func Delete(key string, timestamp int64) bool {

	err := inicialize.Memtables.Delete(key, timestamp)
	if err != nil {
		return false
	}

	return true
}
