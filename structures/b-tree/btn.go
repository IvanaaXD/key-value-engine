package b_tree

import (
	"github.com/IvanaaXD/NASP/structures/record"
)

// node of tree

type BTreeNode struct {
	leaf   bool
	child  []*BTreeNode
	record []*record.Record
}

// new node is leaf

func NewBTreeNode(leaf bool) *BTreeNode {
	node := BTreeNode{leaf: leaf}
	return &node
}
