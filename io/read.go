package io

import (
	"github.com/IvanaaXD/NASP/app/config"
	"github.com/IvanaaXD/NASP/inicialize"
	"github.com/IvanaaXD/NASP/structures/record"
	"os"
	"sort"
)

func Get(key string) (record.Record, bool) {

	rec, _, exists := inicialize.Memtables.Read(key)
	if exists {
		return rec, true
	}

	rec, exists = inicialize.Cache.Find(key)
	if exists {
		return rec, true
	}

	// records := sstable.ReadTables([]string{key}, true)
	files, err := os.ReadDir(config.GlobalConfig.Prefix)

	if len(files) == 0 || os.IsNotExist(err) {
		return record.Record{}, false
	}

	// found, elem := sstable.Find(key, config.GlobalConfig.Prefix, uint64(config.GlobalConfig.LSMMaxLevels)) // , config.SST_FILES

	// record - imas kljuc  i dobijas vrednosti
	//ts := false
	//if elem.Tombstone == 1 {
	//	ts = true
	//} else {
	//	ts = false
	//}
	//
	//r := record.Record{
	//	Key:       key,
	//	Value:     elem.Value,
	//	Timestamp: int64(elem.Timestamp),
	//	Tombstone: ts,
	//}
	//
	//if found {
	//	NASP.Cache.Add(r)
	//
	//	if elem.Tombstone == 1 {
	//		return record.Record{}, false
	//	} else {
	//		return r, true
	//	}
	//}

	// FIND
	/*
		if len(records) > 0 {
			Cache.Add(records[0])
			return records[0]
		}*/

	return record.Record{}, false
}

func PrefixScan(key string, pageNumber, pageSize int, oldRecords []*record.Record) []record.Record {
	memtableRecords := inicialize.Memtables.PrefixScan(key, pageNumber, pageSize, oldRecords)

	var sstableRecords []*record.Record
	if len(memtableRecords) < pageNumber*pageSize {
		// sstableRecords = sstable.PrefixScanAll(start, end, pageNumber, pageSize, memtableRecords, oldRecords)
	}

	allRecords := append(memtableRecords, sstableRecords...)

	sort.Slice(allRecords, func(i, j int) bool {
		return allRecords[i].Key < allRecords[j].Key
	})

	result := make([]record.Record, len(allRecords))
	for i, rec := range allRecords {
		result[i] = *rec
	}

	return result
}

func RangeScan(start, end string, pageNumber, pageSize int, oldRecords []*record.Record) []record.Record {
	memtableRecords := inicialize.Memtables.RangeScan(start, end, pageNumber, pageSize, oldRecords)

	var sstableRecords []*record.Record
	if len(memtableRecords) < pageNumber*pageSize {
		// sstableRecords = sstable.RangeScanAll(start, end, pageNumber, pageSize, memtableRecords, oldRecords)
	}

	allRecords := append(memtableRecords, sstableRecords...)

	sort.Slice(allRecords, func(i, j int) bool {
		return allRecords[i].Key < allRecords[j].Key
	})

	result := make([]record.Record, len(allRecords))
	for i, rec := range allRecords {
		result[i] = *rec
	}

	return result
}
