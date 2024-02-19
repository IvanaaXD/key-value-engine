package skip_list

import (
	"errors"
	"github.com/IvanaaXD/NASP---Projekat/iterator"
	"github.com/IvanaaXD/NASP---Projekat/record"
)

type SkipListIterator struct {
	skipList *SkipList
	index    int
	maxIndex int
	list     []*record.Record
}

func (sl *SkipList) NewIterator() (iterator.Iterator, error) {
	nodes := sl.GetItems()
	if len(nodes) == 0 {
		return nil, errors.New("SkipList empty")
	}

	iter := &SkipListIterator{
		skipList: sl,
		index:    0,
		list:     []*record.Record{},
		maxIndex: len(nodes),
	}

	for _, rec := range nodes {
		r := rec
		iter.list = append(iter.list, &r)
	}

	for iterator.IsInvalidKey(iter) {
		iter.index += 1
	}

	return iter, nil
}

func (i *SkipListIterator) SetIndex() {
	i.index = -1
}

func (i *SkipListIterator) Value() *record.Record {

	return i.list[i.index]
}

func (i *SkipListIterator) Next() bool {
	i.index += 1

	if !i.Stop() {
		for iterator.IsInvalidKey(i) {
			i.index += 1
		}
	}

	if i.Stop() {
		return false
	} else {
		return true
	}
}

func (i *SkipListIterator) Stop() bool {
	return i.index >= i.maxIndex
}
