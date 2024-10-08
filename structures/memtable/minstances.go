package memtable

import (
	"errors"
	"fmt"
	"github.com/IvanaaXD/NASP/app/config"
	"github.com/IvanaaXD/NASP/structures/record"
	writeaheadlog "github.com/IvanaaXD/NASP/structures/writeAheadLog"
	"os"
)

const NullElementKey string = "NULLELEMENT"

type Memtables struct {
	Current   int
	Last      int
	MaxTables int
	Tables    []*Memtable
	Wal       *writeaheadlog.WriteAheadLog
}

func NewMemtables() *Memtables {

	config.Init()
	structName := config.GlobalConfig.StructureType
	n := config.GlobalConfig.MemtableNum

	mi := &Memtables{}
	mi.Tables = make([]*Memtable, 0)
	mi.MaxTables = int(n)

	for i := 0; i < int(n); i++ {
		m := NewMemtable(structName)
		mi.Tables = append(mi.Tables, m)
	}

	mi.Wal = writeaheadlog.InitializeWAL()
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

	currentMemtable := mi.Tables[mi.Current]

	for {
		rec := mi.Wal.ReadRecord(mi.Current)

		recc := record.Record{Key: NullElementKey, Tombstone: true}
		if rec.Key == recc.Key {
			break
		}

		if currentMemtable.maxSize == currentMemtable.Structure.GetSize() {
			mi.Current = (mi.Current + 1) % mi.MaxTables
			if mi.Current == mi.Last {
				err := mi.Flush()
				if err != nil {
					fmt.Println("Error flushing: ", err)
					return err
				}
			}
			currentMemtable = mi.Tables[mi.Current]
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

	mi.Wal.WriteRecord(rec, mi.Current)

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
	if err != nil {
		fmt.Println("Error flushing: ", err)
		return err
	}

	mi.Wal.DeleteSerializedRecords(mi.Last)
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

func (mi *Memtables) Delete(key string, timestamp int64) error {

	mi.Wal.WriteRecord(record.Record{Key: key, Value: []byte("d"), Timestamp: timestamp, Tombstone: true}, mi.Current)
	rec, id, ok := mi.Read(key)

	if ok {

		if id == mi.Current {
			mi.Tables[id].Structure.Delete(rec)
		} else {
			rec.Tombstone = true
			rec.Timestamp = timestamp

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
		rec.Timestamp = timestamp

		err := mi.Write(rec)
		if err != nil {
			println("Error writing to file")
			return err
		}
		return nil
	}
}
