package io

import (
	"github.com/IvanaaXD/NASP/inicialize"
	"github.com/IvanaaXD/NASP/structures/record"
	"github.com/IvanaaXD/NASP/structures/writeAheadLog"
)

// PUT (Novi slog / azuriranje sloga)

func Put(key string, value []byte, timestamp int64) bool {

	rec := record.Record{key, value, timestamp, false}

	wal := writeaheadlog.InitializeWAL()
	wal.WriteRecord(rec, inicialize.Memtables.Current)

	err := inicialize.Memtables.Write(rec)
	if err != nil {
		return false
	}

	return true
}

// DELETE (Brisanje sloga)

func Delete(key string, timestamp int64) bool {

	value := []byte("d")
	rec := record.Record{key, value, timestamp, true}

	wal := writeaheadlog.InitializeWAL()
	wal.WriteRecord(rec, inicialize.Memtables.Current)

	err := inicialize.Memtables.Delete(rec.Key)
	if err != nil {
		return false
	}

	return true
}
