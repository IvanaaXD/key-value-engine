package skip_list

import "github.com/IvanaaXD/NASP/structures/record"

// node of skiplist

type SkipListNode struct {
	Record record.Record
	Level  int
	Next   []*SkipListNode
}

// new node of skiplist

func NewSkipListNode(record record.Record, level int) *SkipListNode {
	return &SkipListNode{Record: record, Level: level, Next: make([]*SkipListNode, level+1)}
}
