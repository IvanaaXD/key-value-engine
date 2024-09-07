package merkletree

import (
	"testing"

	"github.com/IvanaaXD/NASP/structures/record"
)

var RecordKeys = [...]string{
	"Alpha", "Bravo", "Charlie", "Delta", "Echo", "Foxtrot", "Golf", "Hotel", "India", "Juliet",
	"Kilo", "Lima", "Mike", "November", "Oscar", "Papa", "Quebec", "Romeo", "Sierra", "Tango",
	"Uniform", "Victor", "Whiskey", "Xray", "Yankee", "Zulu",
}

var hashedValsExpected []uint64 = make([]uint64, 0)
var hashedValActual []uint64 = make([]uint64, 0)

func TestMerkle(t *testing.T) {
	usedRecords := make([]record.Record, 0)
	for i := 0; i < 26; i++ {
		usedRecords = append(usedRecords, record.Record{Key: RecordKeys[i], Value: []byte(RecordKeys[i]), Timestamp: 100, Tombstone: false})
	}
	mt := MakeMerkleTree(usedRecords)
	mtBytes := mt.Serialize()

	expectedMt := MakeMerkleTree(usedRecords)
	actualMt := Deserialize(mtBytes)

	expectedMt.printTree()
	actualMt.printTreeActual()

	if len(hashedValsExpected) != len(hashedValActual) {
		t.Errorf("Bad len: %d != %d", len(hashedValsExpected), len(hashedValActual))
	}

	for index := range hashedValActual {
		if hashedValsExpected[index] != hashedValActual[index] {
			t.Errorf("Bad bad")
		}
	}
}

func (mt *MerkleTree) printTree() {
	printNode(mt.Root)
}

func (mt *MerkleTree) printTreeActual() {
	printNodeActual(mt.Root)
}

func printNode(mn merkleTreeNode) {
	if mn.leftChild != nil {
		printNode(*mn.leftChild)
	}
	//fmt.Println(mn.hashValue)
	hashedValsExpected = append(hashedValsExpected, mn.hashValue)
	if mn.rightChild != nil {
		printNode(*mn.rightChild)
	}
}

func printNodeActual(mn merkleTreeNode) {
	if mn.leftChild != nil {
		printNodeActual(*mn.leftChild)
	}
	//fmt.Println(mn.hashValue)
	hashedValActual = append(hashedValActual, mn.hashValue)
	if mn.rightChild != nil {
		printNodeActual(*mn.rightChild)
	}
}
