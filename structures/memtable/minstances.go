package memtable

import (
	"fmt"
	"github.com/IvanaaXD/NASP/app/config"
	"github.com/IvanaaXD/NASP/structures/record"
	"sort"
	"time"
)

type Memtables struct {
	Current   int
	Last      int
	MaxTables int
	Tables    []*Memtable
}

func NewMemtables(config *config.Config) *Memtables {

	structName := config.StructureType
	n := config.MemtableNum

	mi := &Memtables{}
	mi.Tables = make([]*Memtable, 0)
	mi.MaxTables = int(n)

	for i := 0; i < int(n); i++ {
		m := NewMemtable(config, structName)
		mi.Tables = append(mi.Tables, m)

	}
	return mi
}

//func (mi *Memtables) Recover() error {
//
//	n := config.GlobalConfig.MemtableSize
//
//	for i := 0; i < int(n); i++ {
//		err := mi.Tables[i].recover()
//		if mi.Tables[mi.Current].maxSize == mi.Tables[mi.Current].Structure.GetSize() {
//			mi.Current = (mi.Current + 1) % mi.MaxTables
//			if mi.Current == mi.Last {
//				err = mi.Flush()
//				if err != nil {
//					fmt.Println("Error flushing: ", err)
//					return err
//				}
//			}
//		}
//		if err != nil {
//			return err
//		}
//	}
//	return nil
//}

func (mi *Memtables) Write(rec record.Record) error {

	m := mi.Tables[mi.Current]

	if m.maxSize == m.Structure.GetSize() {
		if mi.Current == mi.Last {
			err := mi.Flush()
			if err != nil {
				fmt.Println("Error flushing: ", err)
				return err
			}
		}
		m = mi.Tables[mi.Current]
	}

	ok, err := m.Write(rec)
	if err != nil {
		fmt.Println("Error writing record:", err)
		return err
	}

	if ok {
		mi.Current = (mi.Current + 1) % mi.MaxTables
	}

	return nil
}

func (mi *Memtables) Flush() error {

	flushId := mi.Last
	m := mi.Tables[flushId]

	err := m.Flush()
	m.Clear()
	if err != nil {
		fmt.Println("Error flushing: ", err)
		return err
	}

	mi.Last = (mi.Last + 1) % mi.MaxTables

	return nil
}

func (mi *Memtables) Read(key string) (record.Record, int, bool) {

	result := []record.Record{}
	list := []int{}

	for i := 0; i < mi.MaxTables; i++ {
		rec, exists := mi.Tables[i].Structure.Read(key)
		if exists {
			result = append(result, rec)
			list = append(list, i)
		}
	}

	if len(result) == 0 {
		return record.Record{}, -1, false
	}

	newestRecord := result[0]
	index := list[0]

	for i := 0; i < len(result); i++ {
		if result[i].Timestamp > newestRecord.Timestamp {
			newestRecord = result[i]
			index = list[i]
		}
	}

	return newestRecord, index, true
}

func (mi *Memtables) Delete(key string) error {

	rec, id, ok := mi.Read(key)

	if ok {

		if id == mi.Current {
			mi.Tables[id].Structure.Delete(rec)
		} else {
			rec.Tombstone = true

			err := mi.Write(rec)
			if err != nil {
				println("Error writing to file")
				return err
			}
		}
		return nil

	} else {

		rec.Tombstone = true
		rec.Key = key
		rec.Timestamp = time.Now().UnixNano()
		err := mi.Write(rec)
		if err != nil {
			println("Error writing to file")
			return err
		}
		return nil
	}
}

// searching for key with given prefix

func (mi *Memtables) PrefixScan(prefix string) []*record.Record {

	records := make([]*record.Record, 0)
	latestTimestamps := make(map[string]int64)

	for i := 0; i < mi.MaxTables; i++ {

		if mi.Tables[i].Structure.GetSize() == 0 {
			continue
		}

		list := mi.Tables[i].PrefixScan(prefix)

		for _, rec := range list {

			if storedTimestamp, exists := latestTimestamps[rec.Key]; exists {
				if rec.Timestamp > storedTimestamp {
					latestTimestamps[rec.Key] = rec.Timestamp
					replaceRecord(records, rec)
				}
			} else {
				latestTimestamps[rec.Key] = rec.Timestamp
				records = append(records, rec)
			}
		}
	}

	sort.Slice(records, func(i, j int) bool {
		return records[i].Key < records[j].Key
	})

	return records
}

// replaceRecord replaces the existing record with a newer one

func replaceRecord(records []*record.Record, newRecord *record.Record) {
	for i, existingRecord := range records {
		if existingRecord.Key == newRecord.Key {
			records[i] = newRecord
			return
		}
	}
}

// searching for key in given rate

func (mi *Memtables) RangeScan(start, finish string) []*record.Record {

	var records []*record.Record
	latestTimestamps := make(map[string]int64)

	for i := 0; i < mi.MaxTables; i++ {
		list := mi.Tables[i].RangeScan(start, finish)

		for _, rec := range list {

			if storedTimestamp, exists := latestTimestamps[rec.Key]; exists {
				if rec.Timestamp > storedTimestamp {
					latestTimestamps[rec.Key] = rec.Timestamp
					replaceRecord(records, rec)
				}
			} else {
				latestTimestamps[rec.Key] = rec.Timestamp
				records = append(records, rec)
			}
		}
	}

	sort.Slice(records, func(i, j int) bool {
		return records[i].Key < records[j].Key
	})

	return records
}
