package memtable

import (
	"errors"
	"fmt"
	"github.com/IvanaaXD/NASP/app/config"
	b_tree "github.com/IvanaaXD/NASP/structures/b-tree"
	hash_map "github.com/IvanaaXD/NASP/structures/hash-map"
	lsm_tree "github.com/IvanaaXD/NASP/structures/lsm-tree"
	"github.com/IvanaaXD/NASP/structures/record"
	skip_list "github.com/IvanaaXD/NASP/structures/skip-list"
	"github.com/IvanaaXD/NASP/structures/sstable"
	"sort"
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

func (m *Memtable) Clear() {

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
}

// flush to disk aka sstable

func (m *Memtable) Flush() error {

	records := m.Structure.GetItems()

	sort.Slice(records, func(i, j int) bool {
		return records[i].Key < records[j].Key
	})

	m.Clear()

	sstable.CreateNewSSTable(records)
	lsm_tree.InitializeLSMCheck()

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
	}

	return false, nil
}
