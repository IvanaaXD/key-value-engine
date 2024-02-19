package hash_map

import (
	"errors"
	"github.com/IvanaaXD/NASP/structures/iterator"
	"github.com/IvanaaXD/NASP/structures/record"
	"sort"
)

type HashMapIterator struct {
	hashMap  *HashMap
	index    int
	maxIndex int
	keys     []string
}

func (hm *HashMap) NewIterator() (iterator.Iterator, error) {
	if len(hm.data) == 0 {
		return nil, errors.New("HashMap empty")
	}

	iter := &HashMapIterator{
		hashMap:  hm,
		index:    0,
		keys:     make([]string, 0, len(hm.data)),
		maxIndex: len(hm.data),
	}

	for key := range hm.data {
		iter.keys = append(iter.keys, key)
	}

	sort.Strings(iter.keys)

	for iterator.IsInvalidKey(iter) {
		iter.index += 1
	}

	return iter, nil
}

func (i *HashMapIterator) SetIndex() {
	i.index = -1
}

func (i *HashMapIterator) Value() *record.Record {

	rec, err := i.hashMap.Get(i.keys[i.index])
	if err != nil {
		return nil
	}
	return rec
}

func (i *HashMapIterator) Next() bool {
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

func (i *HashMapIterator) Stop() bool {
	return i.index >= i.maxIndex
}
