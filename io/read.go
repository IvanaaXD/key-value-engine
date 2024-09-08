package io

import (
	"github.com/IvanaaXD/NASP/inicialize"
	"github.com/IvanaaXD/NASP/structures/iterators"
	"github.com/IvanaaXD/NASP/structures/record"
	"github.com/IvanaaXD/NASP/structures/sstable"
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

func PrefixScan(key string, pageNum, pageSize int) []record.Record {

	iter := iterators.MakePrefixIterator(inicialize.Memtables.Tables, key)

	var records = make([]record.Record, 0)
	for i := 0; i < pageSize*(pageNum-1); i++ {
		iter.GetNext()
	}

	for i := 0; i < pageSize; i++ {
		rec, ok := iter.GetNext()
		if !ok {
			break
		}
		records = append(records, rec)
	}

	return records
}

func RangeScan(start, end string, pageNum, pageSize int) []record.Record {

	iter := iterators.MakeRangeIterator(inicialize.Memtables.Tables, start, end)

	var records = make([]record.Record, 0)
	for i := 0; i < pageSize*(pageNum-1); i++ {
		iter.GetNext()
	}

	for i := 0; i < pageSize; i++ {
		rec, ok := iter.GetNext()
		if !ok {
			break
		}
		records = append(records, rec)
	}

	return records
}

func PrefixIterate(key string) {

	iter := iterators.MakePrefixIterator(inicialize.Memtables.Tables, key)

	var numOfRecords = 1
	var numOfPages = (1 + numOfRecords - 1) / numOfRecords
	currentPage := 1

	for {
		record, exists := iter.GetNext()
		if !exists {
			println("No more pages!")
			break
		}

		movePages := printPage(record, currentPage, numOfPages)
		if movePages == 0 {
			break
		} else {
			currentPage += movePages
			continue
		}
	}
}

func RangeIterate(start, end string) {

	iter := iterators.MakeRangeIterator(inicialize.Memtables.Tables, start, end)

	var numOfRecords = 1
	var numOfPages = (1 + numOfRecords - 1) / numOfRecords
	currentPage := 1

	for {
		record, exists := iter.GetNext()
		if !exists {
			println("No more pages!")
			break
		}

		movePages := printPage(record, currentPage, numOfPages)
		if movePages == 0 {
			break
		} else {
			currentPage += movePages
			continue
		}
	}
}
