package count_min_sketch

import (
	"encoding/binary"
)

type CountMinSketch struct {
	m             uint           //cols
	k             uint           //rows
	table         [][]uint64     //matrix
	hashFunctions []HashWithSeed //hash functions
}

func CreateCMS(epsilon, delta float64) *CountMinSketch {

	mParam := CalculateM(epsilon)
	kParam := CalculateK(delta)

	table := make([][]uint64, kParam)
	for i := 0; i < len(table); i++ {
		table[i] = make([]uint64, mParam)
	}

	hashFunctions := CreateHashFunctions(kParam)

	cms := CountMinSketch{m: mParam, k: kParam, table: table, hashFunctions: hashFunctions}
	return &cms

}

func (cms CountMinSketch) AddItem(K []byte) {

	for row := 0; row < len(cms.hashFunctions); row++ {
		hashFunction := cms.hashFunctions[row]

		hash := hashFunction.Hash(K)
		col := hash % uint64(cms.m)

		cms.table[row][col] += 1
	}
}

func (cms CountMinSketch) Read(data []byte) uint64 {
	counters := make([]uint64, cms.k)

	for row, hashFunction := range cms.hashFunctions {
		hash := hashFunction.Hash(data)
		col := hash % uint64(cms.m)

		counters[row] = cms.table[row][col]
	}

	minimum := Sort(counters)
	return minimum
}

func Sort(arr []uint64) uint64 {

	minEl := arr[0]

	for _, el := range arr {
		if el < minEl {
			minEl = el
		}
	}

	return minEl
}

func (cms CountMinSketch) GetFrequency(K []byte) uint64 {

	minNum := ^uint64(0)

	for row := 0; row < len(cms.hashFunctions); row++ {
		hashFunction := cms.hashFunctions[row]

		hash := hashFunction.Hash(K)
		col := hash % uint64(cms.m)

		num := cms.table[row][col]

		if num < minNum {
			minNum = num
		}
	}

	return minNum
}

func (cms *CountMinSketch) Serialize() []byte {
	ret := make([]byte, 0)

	// put k
	ret = binary.LittleEndian.AppendUint64(ret, uint64(cms.k))
	// put m
	ret = binary.LittleEndian.AppendUint64(ret, uint64(cms.m))
	// put valueMatrix
	for i := uint64(0); i < uint64(cms.k); i++ {
		for j := uint64(0); j < uint64(cms.m); j++ {
			ret = binary.LittleEndian.AppendUint64(ret, cms.table[i][j])
		}
	}
	// put hash functions (their seeds) 32-byte seed
	for _, hashFn := range cms.hashFunctions {
		ret = append(ret, hashFn.Seed...)
	}

	return ret
}

func Deserialize(byteArr []byte) *CountMinSketch {
	// get k
	k := binary.LittleEndian.Uint64(byteArr[0:8])
	// get m
	m := binary.LittleEndian.Uint64(byteArr[8:16])

	// move byteArr
	byteArr = byteArr[16:]

	// get valueMatrix
	valueMatrix := make([][]uint64, k)
	for i := uint64(0); i < k; i++ {
		valueMatrix[i] = make([]uint64, m)
		for j := uint64(0); j < m; j++ {
			valueMatrix[i][j] = binary.LittleEndian.Uint64(byteArr[0:8])
			byteArr = byteArr[8:]
		}
	}

	// get hash function seeds
	hashFunctions := make([]HashWithSeed, k)
	for i := uint64(0); i < k; i++ {
		seed := byteArr[0:32]
		hashFunctions[i].Seed = seed
		byteArr = byteArr[32:]
	}

	return &CountMinSketch{
		k:             uint(k),
		m:             uint(m),
		table:         valueMatrix,
		hashFunctions: hashFunctions,
	}
}
