package merkletree

import (
	"encoding/binary"
	"math"

	"crypto/md5"

	rec "github.com/IvanaaXD/NASP/structures/record"
)

const emptyNodeHash uint64 = 0

type MerkleTree struct {
	Root merkleTreeNode
}

type merkleTreeNode struct {
	hashValue  uint64
	leftChild  *merkleTreeNode
	rightChild *merkleTreeNode
}

type serializationUtil struct {
	bytes []byte
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
	numEmptyElems, _ := calculateNumEmptyNodes(len(records))
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

	return MerkleTree{Root: *oldNodes[0]}
}

func leafNodeSerialization(currentNode merkleTreeNode, serializationUtil *serializationUtil) {
	if currentNode.leftChild == nil {
		nodeBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(nodeBytes, uint64(currentNode.hashValue))
		serializationUtil.bytes = append(serializationUtil.bytes, nodeBytes...)
	} else {
		leafNodeSerialization(*currentNode.leftChild, serializationUtil)
		leafNodeSerialization(*currentNode.rightChild, serializationUtil)
	}
}

func (mt *MerkleTree) Serialize() []byte {
	serializationUtil := serializationUtil{bytes: make([]byte, 0)}
	leafNodeSerialization(mt.Root, &serializationUtil)
	return serializationUtil.bytes
}

func Deserialize(treeBytes []byte) MerkleTree {
	totalElements := len(treeBytes) / 8
	leafNodes := make([]*merkleTreeNode, totalElements)
	for index := range leafNodes {
		leafNodes[index] = &merkleTreeNode{hashValue: binary.BigEndian.Uint64(treeBytes[index*8 : index*8+8]), leftChild: nil, rightChild: nil}
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

	return MerkleTree{Root: *oldNodes[0]}
}
