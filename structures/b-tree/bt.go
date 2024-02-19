package b_tree

import (
	"fmt"
	"github.com/IvanaaXD/NASP---Projekat/structures/record"
)

type BTree struct {
	root     *BTreeNode
	t        int // order of the B-tree
	size     int
	capacity uint
}

// new tree with one node

func NewBTree(i int, maxSize uint) *BTree {

	root := NewBTreeNode(true)
	tree := BTree{root, i, 0, maxSize}
	return &tree
}

func (tree *BTree) GetSize() uint {
	return uint(tree.size)
}

// getting all records from tree

func (tree *BTree) GetItems() []record.Record {

	list := RecordList{}
	list.getRecords(tree.root)

	records := []record.Record{}

	for _, p := range list.recordList {
		rec := *p
		records = append(records, rec)
	}

	return records

}

// splitting child nodes into two nodes

func (tree *BTree) split(node *BTreeNode, i int) {

	t := tree.t

	// creating new child in leaf of this one
	parent := node.child[i]
	child := NewBTreeNode(parent.leaf)

	// inserting new child in parent's children and moving median key from full child to parent
	node.child = insertChild(node.child, i+1, child)
	node.record = insertRecord(node.record, i, parent.record[t-1])

	child.record = parent.record[t : (2*t)-1]
	parent.record = parent.record[0 : t-1]

	if !parent.leaf {
		child.child = parent.child[t : 2*t]
		parent.child = parent.child[0:t]
	}

}

// inserting value to record while maintaining the order of records in a list

func insertRecord(record []*record.Record, index int, value *record.Record) []*record.Record {

	if len(record) == index {
		return append(record, value)
	}

	record = append(record[:index+1], record[index:]...)
	record[index] = value

	return record
}

// inserting value to node while maintaining the order of nodes

func insertChild(node []*BTreeNode, index int, value *BTreeNode) []*BTreeNode {

	if len(node) == index {
		return append(node, value)
	}

	node = append(node[:index+1], node[index:]...)
	node[index] = value

	return node
}

// inserting record in non full node

func (tree *BTree) insertNonFull(node *BTreeNode, newRecord record.Record) {
	length := len(node.record) - 1

	// inserting directly
	if node.leaf {
		copyRec := make([]*record.Record, length+1)
		copy(copyRec, node.record)

		node.record = append(copyRec, &record.Record{Key: "", Value: nil})

		for ; length >= 0 && newRecord.Key < node.record[length].Key; length-- {
			node.record[length+1] = node.record[length]
		}

		node.record[length+1] = &newRecord
		tree.size++

	} else {

		// if the node is not leaf, finding the correct child to insert record
		for length > 0 && newRecord.Key <= node.record[length].Key {
			length--
		}

		if len(node.child[length].record) == ((2 * tree.t) - 1) {
			tree.split(node, length)
			if newRecord.Key > node.record[length].Key {
				length++
			}
		}

		// recursively inserting record into child node
		tree.insertNonFull(node.child[length], newRecord)
	}
}

// adding key

func (tree *BTree) Insert(record record.Record) bool {

	root := tree.root

	if len(root.record) == (2*tree.t - 1) {
		temp := NewBTreeNode(false)
		tree.root = temp
		temp.child = append(temp.child, root)
		tree.split(temp, 0)
		tree.insertNonFull(temp, record)
		return true
	} else {
		tree.insertNonFull(root, record)
		return true
	}
}

// updating tree with new record on the same key

func (tree *BTree) Update(node *BTreeNode, newRecord record.Record) bool {

	if node != nil {

		i := 0

		for i < len(node.record) && newRecord.Key > node.record[i].Key {
			i++
		}

		if i < len(node.record) && newRecord.Key == node.record[i].Key {
			node.record[i] = &newRecord
			return true
		} else if node.leaf {
			return false
		} else {
			return tree.Update(node.child[i], newRecord)
		}

	} else {
		return tree.Update(tree.root, newRecord)
	}
}

func (tree *BTree) Delete(record record.Record) bool {

	node, key := tree.Search(record.Key, nil)

	if node != nil && key != "" {
		record.Tombstone = true
		return tree.Update(node, record)
	} else {
		record.Tombstone = true
		return tree.Insert(record)
	}
}

func (tree *BTree) Write(record record.Record) bool {

	node, key := tree.Search(record.Key, nil)

	if node != nil && key != "" {
		return tree.Update(node, record)
	} else {
		return tree.Insert(record)
	}
}

// wrapper around readAll that searches by key and reads specific record
// if it exists

func (tree *BTree) Read(key string) (record.Record, bool) {
	return tree.readAll(key, tree.root)
}

// goes through whole tree from root and searches all nodes

func (tree *BTree) readAll(key string, node *BTreeNode) (record.Record, bool) {

	i := 0
	for i < len(node.record) && key > node.record[i].Key {
		i++
	}

	if i < len(node.record) && key == node.record[i].Key {
		return *node.record[i], true
	} else if node.leaf {
		return record.Record{}, false
	} else {
		return tree.readAll(key, node.child[i])
	}
}

// searches for a key recursively

func (tree *BTree) Search(key string, node *BTreeNode) (*BTreeNode, string) {

	if node != nil {

		i := 0

		for i < len(node.record) && key > node.record[i].Key {
			i++
		}

		if i < len(node.record) && key == node.record[i].Key {
			return node, key
		} else if node.leaf {
			return nil, ""
		} else {
			return tree.Search(key, node.child[i])
		}

	} else {
		return tree.Search(key, tree.root)
	}
}

// printing the keys of tree nodes at each level

func (tree *BTree) Print(node *BTreeNode, l int) {

	fmt.Print("Level: ", l, ": ")

	for _, key := range node.record {
		fmt.Print(key.Key)
	}

	fmt.Println()
	l++

	if len(node.child) > 0 {
		for _, child := range node.child {
			tree.Print(child, l)
		}
	}
}

//// searching for key in given rate
//
//func (tree *BTree) RangeScan(start, finish string) []record.Record {
//
//	items := tree.GetItems()
//	sort.Slice(items, func(i, j int) bool {
//		return items[i].Key < items[j].Key
//	})
//
//	list := []record.Record{}
//
//	for _, i := range items {
//		if i.Key <= finish && i.Key >= start {
//			list = append(list, i)
//		}
//	}
//
//	return list
//}

//// searching records by key prefix
//
//func (tree *BTree) PrefixScan(prefix string) []record.Record {
//
//	foundedItems := []record.Record{}
//	searchedItems := []record.Record{}
//
//	foundedItems = tree.GetItems()
//
//	// Bubble sort
//	for i := 0; i < len(foundedItems)-1; i++ {
//		for j := 0; j < len(foundedItems)-i-1; j++ {
//			if foundedItems[j].Key > foundedItems[j+1].Key {
//				// Swap items
//				foundedItems[j], foundedItems[j+1] = foundedItems[j+1], foundedItems[j]
//			}
//		}
//	}
//
//	stop := false
//
//	for _, value := range foundedItems {
//		if strings.HasPrefix(value.Key, prefix) {
//			searchedItems = append(searchedItems, value)
//			stop = true
//		} else if stop {
//			break
//		}
//	}
//
//	return searchedItems
//}
