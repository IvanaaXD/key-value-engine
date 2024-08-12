package bloom_filter

import (
	"encoding/binary"
	"math"
)

type BloomFilter struct {
	M             uint
	K             uint
	Data          []byte
	HashFunctions []HashWithSeed
}

// new instance of bloom filter

func NewBloomFilter(expectedElements int, falsePositiveRate float64) *BloomFilter {

	m := CalculateM(expectedElements, falsePositiveRate)
	k := CalculateK(expectedElements, m)

	bytess := int(math.Ceil(float64(m) / 8))
	data := make([]byte, bytess)

	hashf := CreateHashFunctions(k)

	bf := BloomFilter{m, k, data, hashf}
	return &bf
}

// Brisanje bloom filtera podrazumeva vracanje niza na pocetnu vrednost
// Zauzimamo novi niz, jer ce garbage collector da dealocira memoriju za prethodni niz
func (bf *BloomFilter) DeleteBloomFilter() {
	bf.Data = make([]byte, int(math.Ceil(float64(bf.M)/8)))
}

// Dodavanje elementa u bloomfilter
func (bf BloomFilter) Add(data []byte) *BloomFilter {
	for _, hashFunction := range bf.HashFunctions {
		hashed := hashFunction.Hash(data)

		bit := hashed % uint64(bf.M) // bit u nizu
		targetByte := bit / 8        // bajt u kome se bit nalazi
		bitMask := 1 << (bit % 8)    // maska sa vrednoscu 1 na potrebnom bitu

		index := int(targetByte)

		bf.Data[index] = bf.Data[index] | byte(bitMask) // bitwise OR kako bi upisali jedinicu
	}

	return &bf
}

// Citanje elementa - da li je mozda u bloom filteru
func (bf BloomFilter) Read(data []byte) bool {
	for _, hashFunction := range bf.HashFunctions {
		hashed := hashFunction.Hash(data)

		bit := hashed % uint64(bf.M) // bit u nizu
		targetByte := bit / 8        // bajt u kome se bit nalazi
		bitMask := 1 << (bit % 8)    // maska sa vrednoscu 1 na potrebnom bitu

		index := int(targetByte)

		// bitwise AND kako bi proverili da li je bit na datoj poziciji
		if bf.Data[index]&byte(bitMask) == 0 {
			return false
		}
	}

	return true
}

func (bf *BloomFilter) Serialize() []byte {
	ret := make([]byte, 0)

	ret = binary.LittleEndian.AppendUint64(ret, uint64(bf.M))
	ret = binary.LittleEndian.AppendUint64(ret, uint64(bf.K))
	ret = append(ret, bf.Data...)

	for _, hashFn := range bf.HashFunctions {
		ret = append(ret, hashFn.Seed...)
	}

	return ret
}

func Deserialize(byteArr []byte) *BloomFilter {
	m := binary.LittleEndian.Uint64(byteArr[0:8])
	k := binary.LittleEndian.Uint64(byteArr[8:16])

	byteArr = byteArr[16:]

	bits := make([]byte, int(math.Ceil(float64(m)/8)))
	copy(bits, byteArr[0:int(math.Ceil(float64(m)/8))])

	byteArr = byteArr[int(math.Ceil(float64(m)/8)):]

	hashFunctions := make([]HashWithSeed, k)
	for i := uint64(0); i < k; i++ {
		seed := byteArr[0:32]
		hashFunctions[i].Seed = seed
		byteArr = byteArr[32:]
	}

	return &BloomFilter{
		M:             uint(m),
		K:             uint(k),
		Data:          bits,
		HashFunctions: hashFunctions,
	}
}
