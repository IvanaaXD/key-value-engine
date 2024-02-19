package memtable

import (
	"errors"
	"fmt"
	"github.com/IvanaaXD/NASP---Projekat/app/config"
	b_tree "github.com/IvanaaXD/NASP---Projekat/structures/b-tree"
	hash_map "github.com/IvanaaXD/NASP---Projekat/structures/hash-map"
	"github.com/IvanaaXD/NASP---Projekat/structures/iterator"
	"github.com/IvanaaXD/NASP---Projekat/structures/record"
	skip_list "github.com/IvanaaXD/NASP---Projekat/structures/skip-list"
	"os"
	"sort"
	"strings"
)

type Memtable struct {
	maxSize   uint      // max size
	Structure Structure // hash-map, skip-list or b-tree
}

// new memtable

func NewMemtable(config *config.Config, strucName string) *Memtable {
	var structure Structure

	maxSize := config.MemtableSize

	switch strucName {
	case "btree":
		structure = b_tree.NewBTree(config.BTreeOrder, maxSize)
	case "skiplist":
		structure = skip_list.NewSkipList(int(config.MemtableSize))
	case "hashmap":
		structure = hash_map.NewHashMap(uint32(config.MemtableSize))
	default:
		structure = skip_list.NewSkipList(int(config.MemtableSize))
	}

	mTable := Memtable{maxSize, structure}

	//wall, err := os.Stat(config.WalPath)
	//if err != nil {
	//	panic(fmt.Sprintf("Log file error: %s", err))
	//}

	//if wall.Size() > 0 {
	//	err = mTable.recover()
	//	if err != nil {
	//		panic(err)
	//	}
	//}

	return &mTable
}

// recovering in case of error

//func (m *Memtable) recover() error {
//
//	walFile, err := os.Open(config.GlobalConfig.WalPath)
//
//	if err != nil {
//		panic(fmt.Sprintf("Log file error: %s", err))
//	}
//
//	for {
//		rec, err1 := wal.ReadWalRecord(walFile)
//		if err1 == io.EOF {
//			break
//		} else if err1 != nil {
//			return err1
//		}
//
//		var success bool
//		if rec.Tombstone {
//			success = m.Structure.Delete(rec)
//		} else {
//			success = m.Structure.Write(rec)
//		}
//
//		if !success {
//			return errors.New("recovery fail")
//		}
//	}
//
//	return nil
//}

// clear memtable

func (m *Memtable) Clear() *Memtable {

	var structure Structure

	strucName := config.GlobalConfig.StructureType

	switch strucName {
	case "btree":
		structure = b_tree.NewBTree(config.GlobalConfig.BTreeOrder, config.GlobalConfig.MemtableSize)
	case "skiplist":
		structure = skip_list.NewSkipList(int(config.GlobalConfig.MemtableSize))
	case "hashmap":
		structure = hash_map.NewHashMap(uint32(config.GlobalConfig.MemtableSize))
	}

	m.Structure = structure
	return m
}

func byteBool(b bool) byte {
	if b {
		return 1
	}
	return 0
}

func (m *Memtable) CountMemSize() int {

	var size int

	for i := 0; i < int(m.maxSize); i++ {
		recs := m.Structure.GetItems()
		for _, rec := range recs {

			size += config.GlobalConfig.CrcSize + config.GlobalConfig.TimestampSize + config.GlobalConfig.TombstoneSize + config.GlobalConfig.KeySizeSize + config.GlobalConfig.ValueSizeSize + len([]byte(rec.Key)) + len(rec.Value)
		}
	}
	return size
}

// flush to disk aka sstable

func (m *Memtable) Flush() error {

	records := m.Structure.GetItems()

	sort.Slice(records, func(i, j int) bool {
		return records[i].Key < records[j].Key
	})

	/*var keyValArray []GTypes.KeyVal[string, elements_db.DatabaseElem]

	for _, rec := range records {
		keyVal := GTypes.KeyVal[string, elements_db.DatabaseElem]{
			Key: rec.Key,
			Value: elements_db.DatabaseElem{
				Tombstone: byteBool(rec.Tombstone),
				Value:     rec.Value,
				Timestamp: uint64(rec.Timestamp),
			},
		}
		keyValArray = append(keyValArray, keyVal)
	}

	sstable.CreateSStable(keyValArray, 1)

	if config.GlobalConfig.CompactionAlgorithm == "sizeTiered" {
		lsm_tree.SizeTieredCompaction(1)
	} else {
		lsm_tree.LeveledCompaction(1)
	}*/

	err := m.WalFlush2()
	if err != nil {
		return err
	}

	fmt.Println("Memtable flushed")
	return nil
}

func (m *Memtable) WalFlush2() error {

	size := m.CountMemSize()

	file, err := os.OpenFile(config.GlobalConfig.WalPath, os.O_RDWR, 0644)
	if err != nil {
		fmt.Println("Error opening wal.log:", err)
		return err
	}
	defer file.Close()

	_, err = file.Seek(int64(size), 0)
	if err != nil {
		fmt.Println("Error seeking in wal.log:", err)
		return err
	}

	err = file.Truncate(0)
	if err != nil {
		fmt.Println("Error truncating wal.log:", err)
		return err
	}

	fmt.Println("WalFlush successful.")
	return nil
}

// writing records to memtable

func (m *Memtable) Write(rec record.Record) (bool, error) {

	ok := m.Structure.Write(rec)
	if !ok {
		println("Error writing to memtable")
		return false, errors.New("")
	}

	if m.Structure.GetSize() >= m.maxSize {

		return true, nil

		//switch config.GlobalConfig.StructureType {
		//
		//case "skiplist":
		//	m.structure = skip_list.NewSkipList(config.GlobalConfig.SkipListHeight)
		//case "btree":
		//	m.structure = b_tree.NewBTree(config.GlobalConfig.BTreeOrder)
		//case "hashmap":
		//	m.structure = hash_map.NewHashMap(uint32(config.GlobalConfig.HashMapSize))
		//}
	}

	return false, nil
}

// deleting records from memtable

func (m *Memtable) Delete(rec record.Record) bool {

	ok := m.Structure.Delete(rec)

	if m.Structure.GetSize() >= m.maxSize {
		err := m.Flush()
		if err != nil {
			return false
		}

		m.Clear()
	}

	return ok
}

// reading from memtable

func (m *Memtable) Read(key string) (record.Record, bool) {
	return m.Structure.Read(key)
}

// getting iterator

func (m *Memtable) GetIterator() iterator.Iterator {

	if iter, err := m.Structure.NewIterator(); err == nil {

		return iter
	}

	return nil
}

// searching records by key prefix

func (m *Memtable) PrefixScan(prefix string) []*record.Record {

	iter := m.GetIterator()
	iter.SetIndex()
	records := make([]*record.Record, 0)

	for iter.Next() {
		currentRecord := iter.Value()

		if strings.HasPrefix(currentRecord.Key, prefix) {
			records = append(records, currentRecord)
		}
	}
	return records
}

// searching for key in given rate

func (m *Memtable) RangeScan(start, finish string) []*record.Record {

	iter := m.GetIterator()
	records := make([]*record.Record, 0)

	for iter.Next() {
		currentRecord := iter.Value()

		if currentRecord.Key >= start && currentRecord.Key <= finish {
			records = append(records, currentRecord)
		}
	}
	return records
}
