package iterator

import "github.com/IvanaaXD/NASP---Projekat/structures/record"

// b-tree, hash-map or skip-list

type Iterator interface {
	SetIndex()
	Next() bool
	Value() *record.Record
	Stop() bool
}
