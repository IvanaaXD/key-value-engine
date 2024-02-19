package b_tree

import (
	"errors"
	"github.com/IvanaaXD/NASP/structures/iterator"
	"github.com/IvanaaXD/NASP/structures/record"
)

type BTreeIterator struct {
	btree    *BTree
	index    int
	maxIndex int
	list     []*record.Record
}

func (tree *BTree) NewIterator() (iterator.Iterator, error) {

	if tree.size == 0 {
		return nil, errors.New("BTree empty")
	}

	items := tree.GetItems()
	records := []*record.Record{}

	for _, rec := range items {
		r := rec
		records = append(records, &r)
	}

	iter := &BTreeIterator{
		btree:    tree,
		index:    0,
		maxIndex: len(records),
		list:     records,
	}

	for iterator.IsInvalidKey(iter) {
		iter.index += 1
	}

	return iter, nil
}

func (i *BTreeIterator) SetIndex() {
	i.index = -1
}

func (i *BTreeIterator) Value() *record.Record {

	return i.list[i.index]
}

func (i *BTreeIterator) Next() bool {
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

func (i *BTreeIterator) Stop() bool {
	return i.index >= i.maxIndex
}
