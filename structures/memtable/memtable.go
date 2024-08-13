package memtable

import (
	"errors"
	"fmt"
	"github.com/IvanaaXD/NASP/app/config"
	b_tree "github.com/IvanaaXD/NASP/structures/b-tree"
	hash_map "github.com/IvanaaXD/NASP/structures/hash-map"
	"github.com/IvanaaXD/NASP/structures/iterator"
	"github.com/IvanaaXD/NASP/structures/record"
	skip_list "github.com/IvanaaXD/NASP/structures/skip-list"
	"github.com/IvanaaXD/NASP/structures/sstable"
	"sort"
	"strings"
)

type Memtable struct {
	maxSize   uint      // max size
	Structure Structure // hash-map, skip-list or b-tree
}

// new memtable

func NewMemtable(strucName string) *Memtable {
	var structure Structure

	config.Init()
	maxSize := config.GlobalConfig.MemtableSize

	switch strucName {
	case "btree":
		structure = b_tree.NewBTree(config.GlobalConfig.BTreeOrder, maxSize)
	case "skiplist":
		structure = skip_list.NewSkipList(int(maxSize))
	case "hashmap":
		structure = hash_map.NewHashMap(uint32(maxSize))
	default:
		structure = skip_list.NewSkipList(int(maxSize))
	}

	mTable := Memtable{maxSize, structure}

	return &mTable
}

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

	sstable.CreateNewSSTable(records)

	/*if config.GlobalConfig.CompactionAlgorithm == "sizeTiered" {
		lsm_tree.SizeTieredCompaction(1)
	} else {
		lsm_tree.LeveledCompaction(1)
	}*/

	fmt.Println("Memtable flushed")
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
	if iter == nil {
		return make([]*record.Record, 0)
	}

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
	if iter == nil {
		return make([]*record.Record, 0)
	}

	records := make([]*record.Record, 0)

	for iter.Next() {
		currentRecord := iter.Value()

		if currentRecord.Key >= start && currentRecord.Key <= finish {
			records = append(records, currentRecord)
		}
	}
	return records
}
