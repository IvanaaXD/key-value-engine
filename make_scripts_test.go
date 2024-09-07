package main

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/IvanaaXD/NASP/app/config"
	"github.com/IvanaaXD/NASP/inicialize"
	"github.com/IvanaaXD/NASP/io"
	"github.com/IvanaaXD/NASP/structures/iterators"
	"github.com/IvanaaXD/NASP/structures/memtable"
	"github.com/IvanaaXD/NASP/structures/merkletree"
	"github.com/IvanaaXD/NASP/structures/record"
	"github.com/IvanaaXD/NASP/structures/sstable"
)

// Duzina kljuca i vrednosti
var KEY_LENGTH = 20
var VALUE_LENGTH = 50

// Broj slogova za unos
var NUM_RECORDS = 100000

// Generisanje random seed-a
var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))

// charset za generisanje random kljuceva i vrednosti
const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var RecordKeys = [...]string{
	"Alpha", "Bravo", "Charlie", "Delta", "Echo", "Foxtrot", "Golf", "Hotel", "India", "Juliet",
	"Kilo", "Lima", "Mike", "November", "Oscar", "Papa", "Quebec", "Romeo", "Sierra", "Tango",
	"Uniform", "Victor", "Whiskey", "Xray", "Yankee", "Zulu",
}

func TestInsert100(t *testing.T) {

	config.Init()
	inicialize.Init()

	config.GlobalConfig.Compression = "no"
	config.GlobalConfig.MemtableSize = 1000
	config.GlobalConfig.TokenNumber = 1000

	keys := generateKey(100)

	for i := 0; i < NUM_RECORDS; i++ {
		err := io.Put(keys[randomIdx(100)], generateValue(), time.Now().UnixNano())
		if !err {
			t.Error(err)
		}
	}
}

func TestInsertSomething(t *testing.T) {

	config.Init()
	inicialize.Init()

	config.GlobalConfig.Compression = "no"
	config.GlobalConfig.MemtableSize = 1000
	config.GlobalConfig.TokenNumber = 1000

	keys := generateKey(100)

	for i := 0; i < 100; i++ {
		err := io.Put(keys[randomIdx(100)], generateValue(), time.Now().UnixNano())
		if !err {
			t.Error(err)
		}
	}
}

func TestOpen(t *testing.T) {
	// inst := sstable.OpenSSTable("0001sstable0005.bin")
	// for {
	// 	rec, ok := inst.ReadRecord()
	// 	if !ok {
	// 		break
	// 	}
	// 	fmt.Println(rec.Key)
	// }
	minst := make([]*memtable.Memtable, 0)
	iter := iterators.MakeRangeIterator(minst, "a", "c")
	rec, ok := iter.GetNext()
	for ok {
		fmt.Println(rec.Key, string(rec.Value), rec.Timestamp, rec.Tombstone)
		rec, ok = iter.GetNext()
	}
}

func TestMerkleSSTable(t *testing.T) {
	usedRecords := make([]record.Record, 0)
	for i := 0; i < 26; i++ {
		usedRecords = append(usedRecords, record.Record{Key: RecordKeys[i], Value: []byte(RecordKeys[i]), Timestamp: 100, Tombstone: false})
	}

	expectedMt := merkletree.MakeMerkleTree(usedRecords)

	sstable.CreateNewSSTable(usedRecords)
	usedSST := sstable.OpenSSTable("0001sstable0001")
	actualMt := usedSST.CreateMerkleHelper()

	expectedMerkleHashes := expectedMt.GetNodes()
	actualMerkleHashes := actualMt.GetNodes()

	if len(expectedMerkleHashes) != len(actualMerkleHashes) {
		t.Error("Bad len")
	}

	for index := range expectedMerkleHashes {
		if expectedMerkleHashes[index] != actualMerkleHashes[index] {
			t.Error("Bad hash")
		}
	}
}

func TestLsm(t *testing.T) {
	inst := sstable.OpenSSTable("0002sstable0001.bin")
	for {
		rec, ok := inst.ReadRecord()
		if !ok {
			break
		}
		fmt.Println(rec.Key, rec.Timestamp, rec.Tombstone)
	}
}

func TestInsertWithCompression100(t *testing.T) {

	config.Init()
	inicialize.Init()

	config.GlobalConfig.Compression = "yes"
	config.GlobalConfig.MemtableSize = 30
	config.GlobalConfig.TokenNumber = 1000

	keys := generateKey(100)

	for i := 0; i < NUM_RECORDS; i++ {
		err := io.Put(keys[randomIdx(100)], generateValue(), time.Now().UnixNano())
		if !err {
			t.Error(err)
		}
	}
}

func TestInsertWithCompression50000(t *testing.T) {

	config.GlobalConfig.Compression = "yes"
	config.GlobalConfig.MemtableSize = 30
	config.GlobalConfig.TokenNumber = 1000

	config.Init()
	inicialize.Init()

	keys := generateKey(50000)

	for i := 0; i < NUM_RECORDS; i++ {
		err := io.Put(keys[randomIdx(50000)], generateValue(), time.Now().UnixNano())
		if !err {
			t.Error(err)
		}
	}
}

func TestInsert50000(t *testing.T) {

	config.Init()
	inicialize.Init()

	config.GlobalConfig.Compression = "no"
	config.GlobalConfig.MemtableSize = 30
	config.GlobalConfig.TokenNumber = 1000

	keys := generateKey(50000)

	for i := 0; i < NUM_RECORDS; i++ {
		err := io.Put(keys[randomIdx(1000)], generateValue(), time.Now().UnixNano())
		if !err {
			t.Error(err)
		}
	}
}

// Funkcije za generisanje random vrednosti i kljuceva
func generateValue() []byte {
	b := make([]byte, VALUE_LENGTH)
	for i := 0; i < VALUE_LENGTH; i++ {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return b
}

func randomIdx(len int) int {
	return seededRand.Intn(len)
}

func generateKey(count int) []string {
	var keys []string
	for i := 0; i < count; i++ {
		b := make([]byte, KEY_LENGTH)
		for i := range b {
			b[i] = charset[seededRand.Intn(len(charset))]
		}
		keys = append(keys, string(b))
	}
	return keys

}
