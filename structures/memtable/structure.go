package memtable

import (
	"github.com/IvanaaXD/NASP---Projekat/structures/iterator"
	"github.com/IvanaaXD/NASP---Projekat/structures/record"
)

// structure of memtable, includes b-tree and skip-list

type Structure interface {
	GetSize() uint
	Write(record record.Record) bool
	Read(key string) (record.Record, bool)
	Delete(record record.Record) bool
	GetItems() []record.Record
	NewIterator() (iterator.Iterator, error)
}
