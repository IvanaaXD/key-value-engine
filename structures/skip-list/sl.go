package skip_list

import (
	"fmt"
	"github.com/IvanaaXD/NASP/structures/record"
	"math/rand"
)

type SkipList struct {
	Header    *SkipListNode
	maxHeight int
}

// new skiplist with one level

func NewSkipList(maxLvl int) *SkipList {
	return &SkipList{Header: NewSkipListNode(record.Record{Key: "*"}, maxLvl), maxHeight: maxLvl}
}

func (s *SkipList) GetSize() uint {
	if s == nil || s.Header == nil || s.Header.Next == nil {
		return 0
	}

	currentNode := s.Header.Next[0]
	size := uint(0)

	for currentNode != nil {
		size++
		currentNode = currentNode.Next[0]
	}

	return size
}

// getting all records from list

func (s *SkipList) GetItems() []record.Record {
	head := s.Header
	lvl := 0
	node := head.Next[lvl]
	records := make([]record.Record, 0)

	for node != nil {
		records = append(records, node.Record)
		node = node.Next[lvl]
	}

	return records
}

// adding to skiplist

func (sl *SkipList) Write(record record.Record) bool {

	// element already in skiplist
	if _, success := sl.Read(record.Key); success {
		fmt.Println("Element sa kljucem ", record.Key, "već postoji u skip listi.")
		return false
	}

	// making array in which for every level there is node after which there should be added new node
	update := make([]*SkipListNode, sl.maxHeight)
	current := sl.Header

	// loop for completing update array
	for i := sl.maxHeight - 1; i >= 0; i-- {
		for current.Next[i] != nil && current.Next[i].Record.Key < record.Key {
			current = current.Next[i]
		}
		update[i] = current
	}

	// counting the level
	level := sl.roll()

	// lifting level of Header pointer at highest level
	if level > sl.Header.Level {
		sl.Header.Level = level
	}

	// creating new node
	newNode := NewSkipListNode(record, level)

	// adding new node to every level
	for i := 0; i <= level && i < len(update); i++ {
		newNode.Next[i] = update[i].Next[i]
		update[i].Next[i] = newNode
	}

	return true
}

// reading record by searching through skiplist

func (sl *SkipList) Read(key string) (record.Record, bool) {
	current := sl.Header

	// searching for position where element should be
	for i := sl.Header.Level - 1; i >= 0; i-- {
		for current.Next[i] != nil && current.Next[i].Record.Key < key {
			current = current.Next[i]
		}
	}

	// if element is there:
	if current.Next[0] != nil && current.Next[0].Record.Key == key {
		return current.Record, true
	}

	return record.Record{}, false
}

// deleting the record

func (s *SkipList) Delete(r record.Record) bool {
	current := s.Header

	for i := s.maxHeight - 1; i >= 0; i-- {
		for current.Next[i] != nil && current.Next[i].Record.Key < r.Key {
			current = current.Next[i]
		}
	}

	if current.Next[0] != nil && current.Next[0].Record.Key == r.Key {
		current.Next[0].Record.Tombstone = true
	} else {
		current.Next[0].Record.Tombstone = true
		s.Write(r)
	}

	return true
}

// random number of levels

func (sl *SkipList) roll() int {
	Level := 0

	// if the head (1) is gotten => level += 1, rolling again
	for ; rand.Intn(2) == 1; Level++ {
		if Level >= sl.maxHeight {
			return Level
		}
	}
	return Level
}

// printing skiplist

func (sl *SkipList) Print() {
	for i := sl.maxHeight - 1; i >= 0; i-- {
		current := sl.Header
		for current.Next[i] != nil {
			fmt.Printf("%s ", current.Next[i].Record.Key)
			current = current.Next[i]
		}
		fmt.Println()
	}
	fmt.Println()
}

//func (s *SkipList) PrefixScan(prefix string, pageNumber, pageSize int) []record.Record {
//	current := s.Header
//	records := make([]record.Record, 0)
//
//	node := current.Next[0]
//	for node != nil && strings.HasPrefix(node.Record.Key, prefix) {
//		records = append(records, node.Record)
//		node = node.Next[0]
//	}
//
//	// Sortirajte zapise po ključu
//	sort.Slice(records, func(i, j int) bool {
//		return records[i].Key < records[j].Key
//	})
//
//	// Primena paginacije
//	startIdx := (pageNumber - 1) * pageSize
//	endIdx := startIdx + pageSize
//
//	if startIdx < 0 {
//		startIdx = 0
//	}
//
//	if endIdx > len(records) {
//		endIdx = len(records)
//	}
//
//	return records[startIdx:endIdx]
//}
//
//// Dobavljanje liste zapisa čiji ključ se nalazi u zadatom opsegu sa paginacijom
//func (s *SkipList) RangeScan(minKey, maxKey string, pageNumber, pageSize int) []record.Record {
//	current := s.Header
//	lvl := s.maxHeight - 1
//	records := make([]record.Record, 0)
//
//	for lvl >= 0 && current != nil {
//		for current.Next[lvl] != nil && current.Next[lvl].Record.Key < minKey {
//			current = current.Next[lvl]
//		}
//
//		if current.Next[lvl] != nil {
//			// Ako smo pronašli čvor sa ključem većim ili jednakim od minKey
//			// Prebacujemo se na nulti nivo kako bismo uzeli sve elemente do maxKey
//			lvl = 0
//		}
//
//		// Dodajte sve zapise unutar opsega
//		node := current.Next[lvl]
//		for node != nil && node.Record.Key <= maxKey {
//			records = append(records, node.Record)
//			node = node.Next[lvl]
//		}
//
//		lvl--
//	}
//
//	// Sortirajte zapise po ključu
//	sort.Slice(records, func(i, j int) bool {
//		return records[i].Key < records[j].Key
//	})
//
//	// Primena paginacije
//	startIdx := (pageNumber - 1) * pageSize
//	endIdx := startIdx + pageSize
//
//	if startIdx < 0 {
//		startIdx = 0
//	}
//
//	if endIdx > len(records) {
//		endIdx = len(records)
//	}
//
//	return records[startIdx:endIdx]
//}
