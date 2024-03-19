package merkletree

import (
	"encoding/binary"
	"math"

	"crypto/md5"

	rec "github.com/IvanaaXD/NASP/structures/record"
)

const emptyNodeHash uint64 = 0

type MerkleTree struct {
	Root   merkleTreeNode
	degree int
}

type merkleTreeNode struct {
	hashValue  uint64
	leftChild  *merkleTreeNode
	rightChild *merkleTreeNode
}

func calculateNumEmptyNodes(numOfRecords int) (int, int) {
	degree := 0
	for {
		if int(math.Pow(2, float64(degree))) >= numOfRecords {
			break
		}
		degree += 1
	}
	return int(math.Pow(2, float64(degree))) - numOfRecords, degree
}

func MakeMerkleTree(records []rec.Record) MerkleTree {
	numEmptyElems, degree := calculateNumEmptyNodes(len(records))
	totalElements := numEmptyElems + len(records)
	fn := md5.New()

	// Kreiranje listova stabla
	leafNodes := make([]*merkleTreeNode, totalElements)
	index := 0
	for _, record := range records {
		fn.Write(rec.RecToBytes(record))
		leafNodes[index] = &merkleTreeNode{hashValue: binary.BigEndian.Uint64(fn.Sum(nil)), leftChild: nil, rightChild: nil}
		fn.Reset()
		index += 1
	}

	// Kreiranje praznih listova (ne mora se kreirati vise kada su svi isti)
	emptyNodeAddress := &merkleTreeNode{hashValue: emptyNodeHash, leftChild: nil, rightChild: nil}
	i := 0
	for i < numEmptyElems {
		leafNodes[index] = emptyNodeAddress
		i += 1
		index += 1
	}

	// Prelazak na sledeci nivo stabla
	totalElements /= 2
	oldNodes := leafNodes

	// Pravljenje cvorova na jednom nivou, zatim prelazak na sledeci, sve dok nisu popunjeni
	for totalElements >= 1 {
		newNodesIndex := 0
		newNodes := make([]*merkleTreeNode, totalElements)
		for newNodesIndex < totalElements {
			oldNodesIndex := newNodesIndex * 2
			newNodes[newNodesIndex] = &merkleTreeNode{hashValue: oldNodes[oldNodesIndex].hashValue + oldNodes[oldNodesIndex+1].hashValue,
				leftChild: oldNodes[oldNodesIndex], rightChild: oldNodes[oldNodesIndex+1]}
			newNodesIndex += 1
		}
		totalElements /= 2
		oldNodes = newNodes
	}

	return MerkleTree{Root: *oldNodes[0], degree: degree}
}

func dfsSerialization(currentNode merkleTreeNode, bytes []byte) {
	nodeBytes := make([]byte, 8)
	bytes = append(bytes, byte(binary.PutVarint(nodeBytes, int64(currentNode.hashValue))))
	if currentNode.leftChild != nil {
		dfsSerialization(*currentNode.leftChild, bytes)
		dfsSerialization(*currentNode.rightChild, bytes)
	}
}

func (mt *MerkleTree) Serialize() []byte {
	serialized := make([]byte, 0)
	dfsSerialization(mt.Root, serialized)
	return serialized
}

func Deserialize(treeBytes []byte) MerkleTree {
	// TO-DO
	return MerkleTree{}
}
