package memtable

import (
	"errors"
	"fmt"
	"github.com/IvanaaXD/NASP/app/config"
	"github.com/IvanaaXD/NASP/structures/record"
	"github.com/IvanaaXD/NASP/structures/writeAheadLog"
	"os"
	"sort"
	"time"
)

const NullElementKey string = "NULLELEMENT"

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

	exists := CheckWal()

	if exists {
		err := mi.Recover()
		if err != nil {
			return nil
		}
	}

	return mi
}

func CheckWal() bool {

	var found = false

	wall, err := os.Stat(config.GlobalConfig.WalPath)
	if err != nil {
		panic(fmt.Sprintf("Log file error: %s", err))
	}

	if wall.Size() > 0 {
		found = true
	}

	return found
}

// recovering in case of error

func (mi *Memtables) Recover() error {

	var i = 0
	currentMemtable := mi.Tables[i]

	wal := writeaheadlog.InitializeWAL()

	for {
		rec := wal.ReadRecord(i)

		recc := record.Record{Key: NullElementKey, Tombstone: true}
		if rec.Key == recc.Key {
			break
		}

		if currentMemtable.maxSize == currentMemtable.Structure.GetSize() {
			i++
			currentMemtable = mi.Tables[i]
		}

		var success bool
		if rec.Tombstone {
			success = currentMemtable.Structure.Delete(rec)
		} else {
			success = currentMemtable.Structure.Write(rec)
		}

		if !success {
			return errors.New("recovery fail")
		}
	}

	return nil
}

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

func (mi *Memtables) PrefixScan(prefix string, pageNumber, pageSize int, oldRecords []*record.Record) []*record.Record {

	var records []*record.Record
	latestTimestamps := make(map[string]int64)

	var count int
outerLoop:
	for i := 0; i < mi.MaxTables; i++ {
		list := mi.Tables[i].PrefixScan(prefix)
		list = CheckRecords(list, oldRecords)

		for _, rec := range list {
			if count >= pageSize*pageNumber {
				break outerLoop
			}

			if rec.Tombstone {
				continue
			}

			if storedTimestamp, exists := latestTimestamps[rec.Key]; exists {
				if rec.Timestamp > storedTimestamp {
					latestTimestamps[rec.Key] = rec.Timestamp
					replaceRecord(records, rec)
				}
			} else {
				latestTimestamps[rec.Key] = rec.Timestamp
				records = append(records, rec)
				count++
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

// check if records are in the founded list already

func CheckRecords(newRecords, oldRecords []*record.Record) []*record.Record {

	oldRecordsMap := make(map[string]*record.Record)
	for _, rec := range oldRecords {
		oldRecordsMap[rec.Key] = rec
	}

	for _, newRec := range newRecords {
		oldRec, exists := oldRecordsMap[newRec.Key]
		if exists {
			if newRec.Timestamp > oldRec.Timestamp {
				if newRec.Tombstone {
					delete(oldRecordsMap, oldRec.Key)
				}
				oldRecordsMap[newRec.Key] = newRec
			}
		} else {
			oldRecordsMap[newRec.Key] = newRec
		}
	}

	updatedRecords := make([]*record.Record, 0, len(oldRecordsMap))
	for _, rec := range oldRecordsMap {
		updatedRecords = append(updatedRecords, rec)
	}

	return updatedRecords
}

// searching for key in given rate

func (mi *Memtables) RangeScan(start, finish string, pageNumber, pageSize int, oldRecords []*record.Record) []*record.Record {

	var records []*record.Record
	latestTimestamps := make(map[string]int64)

	var count int
outerLoop:
	for i := 0; i < mi.MaxTables; i++ {
		list := mi.Tables[i].RangeScan(start, finish)
		list = CheckRecords(list, oldRecords)

		for _, rec := range list {
			if count >= pageSize*pageNumber {
				break outerLoop
			}

			if rec.Tombstone {
				continue
			}

			if storedTimestamp, exists := latestTimestamps[rec.Key]; exists {
				if rec.Timestamp > storedTimestamp {
					latestTimestamps[rec.Key] = rec.Timestamp
					replaceRecord(records, rec)
				}
			} else {
				latestTimestamps[rec.Key] = rec.Timestamp
				records = append(records, rec)
				count++
			}
		}
	}

	sort.Slice(records, func(i, j int) bool {
		return records[i].Key < records[j].Key
	})

	return records
}
