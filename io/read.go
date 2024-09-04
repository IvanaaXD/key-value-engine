package io

import (
	"github.com/IvanaaXD/NASP/inicialize"
	"github.com/IvanaaXD/NASP/structures/record"
	"github.com/IvanaaXD/NASP/structures/sstable"
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

	rec, exists = sstable.SSTableGet(key)
	if exists {
		inicialize.Cache.Add(rec)
		return rec, true
	}

	return record.Record{}, false
}

func PrefixScan(key string) []record.Record {

	memtableRecords := inicialize.Memtables.PrefixScan(key)
	sstableRecords := sstable.PrefixScan(key, memtableRecords)

	sort.Slice(sstableRecords, func(i, j int) bool {
		return sstableRecords[i].Key < sstableRecords[j].Key
	})

	result := make([]record.Record, len(sstableRecords))
	for i, rec := range sstableRecords {
		result[i] = *rec
	}

	return result
}

func RangeScan(start, end string) []record.Record {
	memtableRecords := inicialize.Memtables.RangeScan(start, end)

	sstableRecords := sstable.RangeScan(start, end, memtableRecords)

	sort.Slice(sstableRecords, func(i, j int) bool {
		return sstableRecords[i].Key < sstableRecords[j].Key
	})

	result := make([]record.Record, len(sstableRecords))
	for i, rec := range sstableRecords {
		result[i] = *rec
	}

	return result
}

func PrefixIterate(key string) []record.Record {
	memtableRecords := inicialize.Memtables.PrefixIterate(key)

	var sstableRecords []*record.Record
	// sstableRecords = sstable.PrefixIterateAll(start, end, pageNumber, pageSize, memtableRecords, oldRecords)

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

func RangeIterate(start, end string) []record.Record {
	memtableRecords := inicialize.Memtables.RangeIterate(start, end)

	var sstableRecords []*record.Record
	// sstableRecords = sstable.RangeIterateAll(start, end, pageNumber, pageSize, memtableRecords, oldRecords)

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
