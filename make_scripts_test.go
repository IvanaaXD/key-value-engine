package main

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/IvanaaXD/NASP/app/config"
	"github.com/IvanaaXD/NASP/inicialize"
	"github.com/IvanaaXD/NASP/io"
	bloom_filter "github.com/IvanaaXD/NASP/structures/bloom-filter"
	count_min_sketch "github.com/IvanaaXD/NASP/structures/count-min-sketch"
	hyper_log_log "github.com/IvanaaXD/NASP/structures/hyper-log-log"
	"github.com/IvanaaXD/NASP/structures/iterators"
	"github.com/IvanaaXD/NASP/structures/memtable"
	"github.com/IvanaaXD/NASP/structures/merkletree"
	"github.com/IvanaaXD/NASP/structures/record"
	simhash "github.com/IvanaaXD/NASP/structures/sim-hash"
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

func TestBloomfilter(t *testing.T) {
	config.Init()
	bf := bloom_filter.NewBloomFilter(config.GlobalConfig.BFExpectedElements, config.GlobalConfig.BFFalsePositiveRate)

	for i := 0; i < 3; i++ {
		if bf.Read([]byte(RecordKeys[i])) {
			t.Error("Bad read after init")
		}
	}

	for i := 0; i < 3; i++ {
		bf.Add([]byte(RecordKeys[i]))
	}

	for i := 0; i < 3; i++ {
		if !bf.Read([]byte(RecordKeys[i])) {
			t.Error("Bad read")
		}
	}

	bf = bloom_filter.NewBloomFilter(config.GlobalConfig.BFExpectedElements, config.GlobalConfig.BFFalsePositiveRate)
	for i := 0; i < 26; i++ {
		bf.Add([]byte(RecordKeys[i]))
	}

	for i := 0; i < 26; i++ {
		if !bf.Read([]byte(RecordKeys[i])) {
			t.Error("Bad read")
		}
	}

	bfBytes := bf.Serialize()

	expectedBf := bf

	actualBf := bloom_filter.Deserialize(bfBytes)
	for index := range actualBf.Data {
		if expectedBf.Data[index] != actualBf.Data[index] {
			t.Error("Bad serialization")
		}
	}
}

func TestCMS(t *testing.T) {
	config.Init()
	cms := count_min_sketch.CreateCMS(config.GlobalConfig.CmsEpsilon, config.GlobalConfig.CmsDelta)
	for i := 0; i < 26; i++ {
		cms.AddItem([]byte(RecordKeys[i]))
	}
	cms.AddItem([]byte(RecordKeys[0]))
	for i := 0; i < 26; i++ {
		fmt.Println(cms.GetFrequency([]byte(RecordKeys[i])))
	}
	cmsBytes := cms.Serialize()

	expectedCms := cms
	actualCms := count_min_sketch.Deserialize(cmsBytes)

	for i := 0; i < 26; i++ {
		if expectedCms.GetFrequency([]byte(RecordKeys[i])) != actualCms.GetFrequency([]byte(RecordKeys[i])) {
			t.Error("Bad serialization")
		}
	}
}

func TestHLL(t *testing.T) {
	hll := hyper_log_log.NewHyperLogLog(4)
	for i := 0; i < 26; i++ {
		hll.Add([]byte(RecordKeys[i]))
	}
	fmt.Println(hll.Count())

	hllBytes := hll.Serialize()

	expectedHll := hll
	actualHll := hyper_log_log.Deserialize(hllBytes)

	if expectedHll.Count() != actualHll.Count() {
		t.Error("Bad serialization")
	}
}

func TestSH(t *testing.T) {
	sh1 := simhash.NewSimHash(RecordKeys[0])
	sh2 := simhash.NewSimHash(RecordKeys[1])

	num := sh1.GetDistance(sh2)

	sh1Bytes := sh1.Serialize()
	sh2Bytes := sh2.Serialize()

	expectedSH1 := sh1
	actualSH1 := simhash.Deserialize(sh1Bytes)
	actualSH2 := simhash.Deserialize(sh2Bytes)

	// if expectedSH1.Text != actualSH1.Text {
	// 	t.Error("Bad serialization")
	// }

	for index := range expectedSH1.FingerPrint {
		if expectedSH1.FingerPrint[index] != actualSH1.FingerPrint[index] {
			t.Error("Bad serialization")
		}
	}

	if actualSH1.GetDistance(actualSH2) != num {
		t.Error("Bad serialization maybe")
	}
}

func TestFolder(t *testing.T) {
	folder, _ := os.ReadDir("resources/sstables")
	for _, path := range folder {
		fmt.Println(path.Name())
	}
}

func TestCompression(t *testing.T) {
	usedRecords := make([]record.Record, 0)
	for i := 0; i < 26; i++ {
		usedRecords = append(usedRecords, record.Record{Key: RecordKeys[i], Value: []byte(fmt.Sprintf("value%d", i)), Timestamp: 1000, Tombstone: true})
	}
	sstable.CreateNewSSTable(usedRecords)
	instance := sstable.OpenSSTable("0001sstable0001.bin")
	for {
		rec, ok := instance.ReadRecord()
		if !ok {
			break
		}
		fmt.Println(rec.Key, rec.Value, rec.Timestamp, rec.Tombstone)
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
