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

func PrefixScan(key string) []record.Record {
	memtableRecords := inicialize.Memtables.PrefixScan(key)
	// sstableRecords := sstable.PrefixScanAll(key)

	var result []record.Record

	for _, memRec := range memtableRecords {
		result = append(result, *memRec)
	}

	//for _, sstRec := range sstableRecords {
	//	found := false
	//	for i := 0; i < len(result); i++ {
	//		if result[i].Key == sstRec.Key {
	//			found = true
	//		}
	//	}
	//	if found {
	//		result = append(result, *sstRec)
	//	}
	//}

	for _, rec := range result {
		if !rec.Tombstone {
			result = append(result, rec)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Key < result[j].Key
	})

	return result
}

func RangeScan(start, end string) []record.Record {
	memtableRecords := inicialize.Memtables.RangeScan(start, end)
	// sstableRecords := sstable.RangeScanAll(start, end)

	var result []record.Record

	for _, memRec := range memtableRecords {
		result = append(result, *memRec)
	}

	//for _, sstRec := range sstableRecords {
	//	found := false
	//	for i := 0; i < len(result); i++ {
	//		if result[i].Key == sstRec.Key {
	//			found = true
	//		}
	//	}
	//	if found {
	//		result = append(result, *sstRec)
	//	}
	//}

	for _, rec := range result {
		if !rec.Tombstone {
			result = append(result, rec)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Key < result[j].Key
	})

	return result
}
