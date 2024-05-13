package memtable

import (
	"fmt"
	"github.com/IvanaaXD/NASP/app/config"
	"github.com/IvanaaXD/NASP/structures/record"
	"testing"
)

func add(mi *Memtables) {
	err := mi.Write(record.Record{
		Key:       "1",
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	if err != nil {
		panic(err)
	}
	err = mi.Write(record.Record{
		Key:       ("5"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	if err != nil {
		panic(err)
	}
	err = mi.Write(record.Record{
		Key:       ("7"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	if err != nil {
		panic(err)
	}
	err = mi.Write(record.Record{
		Key:       ("8"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	if err != nil {
		panic(err)
	}
	err = mi.Write(record.Record{
		Key:       ("4"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	if err != nil {
		panic(err)
	}
	err = mi.Write(record.Record{
		Key:       ("2"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	if err != nil {
		panic(err)
	}
}

func testLogicalDelete(t *testing.T, structure string) {
	config.Init()
	config.GlobalConfig.StructureType = structure

	mts := NewMemtables(&config.GlobalConfig)
	add(mts)
	err := mts.Delete("5")
	if err != nil {
		t.Errorf("error: [%s] '5' should be in structure", structure)
	}

	record, _, ok := mts.Read(("5"))
	if !ok {
		t.Errorf("error: [%s] '5' should be logically deleted", structure)
	}
	if !record.Tombstone {
		t.Errorf("error: [%s] '5' should be logically deleted", structure)
	}
}

func TestLogicalDelete(t *testing.T) {
	testLogicalDelete(t, "skiplist")
	testLogicalDelete(t, "btree")
	testLogicalDelete(t, "hashmap")
}

func TestTableSwitch(t *testing.T) {
	config.Init()

	config.GlobalConfig.MemtableNum = 4
	config.GlobalConfig.MemtableSize = 3
	mts := NewMemtables(&config.GlobalConfig)
	add(mts)
	err := mts.Write(record.Record{
		Key:       ("a"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})

	if err != nil {
		fmt.Println("Error")
	}

	if mts.Current != 2 {
		t.Errorf("error: expected current table to be %d, but got %d", 2, mts.Current)
	}
}

func addPrefix(mts *Memtables) {
	_ = mts.Write(record.Record{
		Key:       ("aaa"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 15,
	})
	_ = mts.Write(record.Record{
		Key:       ("aab"),
		Value:     nil,
		Tombstone: true,
		Timestamp: 12,
	})
	_ = mts.Write(record.Record{
		Key:       ("aabbc"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 13,
	})

	_ = mts.Write(record.Record{
		Key:       ("aabbc"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 16,
	})
	_ = mts.Write(record.Record{
		Key:       ("aabaa"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	_ = mts.Write(record.Record{
		Key:       ("aabaca"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 9,
	})

	_ = mts.Write(record.Record{
		Key:       ("aabbccdd"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 10,
	})
	_ = mts.Write(record.Record{
		Key:       ("aacda"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 14,
	})
	_ = mts.Write(record.Record{
		Key:       ("csdasd"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 5,
	})

	_ = mts.Write(record.Record{
		Key:       ("aab"),
		Value:     nil,
		Tombstone: true,
		Timestamp: 5,
	})
	_ = mts.Write(record.Record{
		Key:       ("aabcay"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 4,
	})
	_ = mts.Write(record.Record{
		Key:       ("aabbs"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 3,
	})

	_ = mts.Write(record.Record{
		Key:       ("aabacav"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 2,
	})
	_ = mts.Write(record.Record{
		Key:       ("adsadf"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 1,
	})

}

func addRange(mts *Memtables) {
	_ = mts.Write(record.Record{
		Key:       ("1"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	_ = mts.Write(record.Record{
		Key:       ("2"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	_ = mts.Write(record.Record{
		Key:       ("4"),
		Value:     nil,
		Tombstone: true,
		Timestamp: 0,
	})

	_ = mts.Write(record.Record{
		Key:       ("3"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 3,
	})
	_ = mts.Write(record.Record{
		Key:       ("5"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	_ = mts.Write(record.Record{
		Key:       ("8"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})

	_ = mts.Write(record.Record{
		Key:       ("1"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	_ = mts.Write(record.Record{
		Key:       ("2"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	_ = mts.Write(record.Record{
		Key:       ("3"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 5,
	})

	_ = mts.Write(record.Record{
		Key:       ("1"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	_ = mts.Write(record.Record{
		Key:       ("5"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 4,
	})
	_ = mts.Write(record.Record{
		Key:       ("8"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 3,
	})

	_ = mts.Write(record.Record{
		Key:       ("3"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 2,
	})
	_ = mts.Write(record.Record{
		Key:       ("6"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 1,
	})
}

/*
func testValidRangeScan(structure string, t *testing.T) {
	config.Init()
	config.GlobalConfig.MemtableSize = 3
	config.GlobalConfig.MemtableNum = 5
	config.GlobalConfig.StructureType = structure
	mts := NewMemtables(&config.GlobalConfig)
	addRange(mts)

	records := mts.RangeScan(("2"), ("8"))
	sol := []record.Record{
		{
			Key:       ("2"),
			Timestamp: 0,
		},
		{
			Key:       ("3"),
			Timestamp: 5,
		},
		{
			Key:       ("4"),
			Timestamp: 0,
		},
		{
			Key:       ("5"),
			Timestamp: 4,
		},
		{
			Key:       ("6"),
			Timestamp: 1,
		},
		{
			Key:       ("8"),
			Timestamp: 3,
		},
	}

	if len(records) != 6 {
		t.Errorf("error: [%s] records size to be 6, got %d", structure, len(records))
		return
	}

	for i, rec := range records {
		if rec.Key != sol[i].Key {
			t.Errorf("error: [%s] keys are not sorted correcly at %d", structure, i)
		}
		if rec.Timestamp != sol[i].Timestamp {
			t.Errorf("error: [%s] timestamp is not correct at %d", structure, i)
		}
	}
}

func TestValidRangeScan(t *testing.T) {
	testValidRangeScan("hashmap", t)
	testValidRangeScan("btree", t)
	testValidRangeScan("skiplist", t)
}

func testInvalidRangeScan(structure string, t *testing.T) {
	config.Init()
	config.GlobalConfig.MemtableSize = 3
	config.GlobalConfig.MemtableNum = 5
	config.GlobalConfig.StructureType = structure
	mts := NewMemtables(&config.GlobalConfig)
	addRange(mts)

	records := mts.RangeScan(("A"), ("F"))

	if len(records) != 0 {
		t.Errorf("error: [%s] expected records size to be 0, got %d", structure, len(records))
	}
}

func TestInvalidRangeScan(t *testing.T) {
	testInvalidRangeScan("hashmap", t)
	testInvalidRangeScan("btree", t)
	testInvalidRangeScan("skiplist", t)
}
func testValidPrefixScan(structure string, t *testing.T) {
	config.Init()
	config.GlobalConfig.MemtableSize = 3
	config.GlobalConfig.MemtableNum = 5
	config.GlobalConfig.StructureType = structure
	mts := NewMemtables(&config.GlobalConfig)
	addPrefix(mts)

	records := mts.PrefixScan("aab")
	sol := []record.Record{
		{
			Key:       "aab",
			Timestamp: 12,
		},
		{
			Key:       "aabaa",
			Timestamp: 0,
		},
		{
			Key:       "aabaca",
			Timestamp: 9,
		},
		{
			Key:       "aabacav",
			Timestamp: 2,
		},
		{
			Key:       "aabbc",
			Timestamp: 16,
		},
		{
			Key:       "aabbccdd",
			Timestamp: 10,
		},
		{
			Key:       "aabbs",
			Timestamp: 3,
		},
		{
			Key:       "aabcay",
			Timestamp: 4,
		},
	}

	if len(records) != 8 {
		t.Errorf("error: [%s] expected records size to be 8, got %d", structure, len(records))
	}

	for i, rec := range records {
		if rec.Key != sol[i].Key {
			t.Errorf("error: [%s] keys are not sorted correctly at %d", structure, i)
		}
		if rec.Timestamp != sol[i].Timestamp {
			t.Errorf("error: [%s] timestamp is not correct at %d", structure, i)
		}
	}
}

func TestValidPrefixScan(t *testing.T) {
	testValidPrefixScan("hashmap", t)
	testValidPrefixScan("btree", t)
	testValidPrefixScan("skiplist", t)
}

func testInvalidPrefixScan(structure string, t *testing.T) {
	config.Init()
	config.GlobalConfig.MemtableSize = 3
	config.GlobalConfig.MemtableNum = 5
	config.GlobalConfig.StructureType = structure
	mts := NewMemtables(&config.GlobalConfig)
	addRange(mts)

	records := mts.PrefixScan(("xyz"))

	if len(records) != 0 {
		t.Errorf("error: [%s] expected records size to be 0, got %d", structure, len(records))
	}
}

func TestInvalidPrefixScan(t *testing.T) {
	testInvalidPrefixScan("hashmap", t)
	testInvalidPrefixScan("btree", t)
	testInvalidPrefixScan("skiplist", t)
}
*/
//func addReserved(mts *Memtables) {
//	_ = mts.Add(&model.Record{
//		Key:       []byte(iterator.BloomFilterPrefix + "mojfilter"),
//		Value:     nil,
//		Tombstone: false,
//		Timestamp: 0,
//	})
//	_ = mts.Add(&model.Record{
//		Key:       []byte(iterator.HyperLogLogPrefix + "mojhajperloglog"),
//		Value:     nil,
//		Tombstone: false,
//		Timestamp: 0,
//	})
//	_ = mts.Add(&model.Record{
//		Key:       []byte("__a"),
//		Value:     nil,
//		Tombstone: false,
//		Timestamp: 0,
//	})
//
//	_ = mts.Add(&model.Record{
//		Key:       []byte(iterator.SimHashPrefix + "mojsimhes"),
//		Value:     nil,
//		Tombstone: false,
//		Timestamp: 3,
//	})
//	_ = mts.Add(&model.Record{
//		Key:       []byte("__Hll"),
//		Value:     nil,
//		Tombstone: false,
//		Timestamp: 0,
//	})
//	_ = mts.Add(&model.Record{
//		Key:       []byte(iterator.CountMinSketchPrefix + "mojcms"),
//		Value:     nil,
//		Tombstone: false,
//		Timestamp: 0,
//	})
//
//	_ = mts.Add(&model.Record{
//		Key:       []byte("adasda"),
//		Value:     nil,
//		Tombstone: false,
//		Timestamp: 0,
//	})
//	_ = mts.Add(&model.Record{
//		Key:       []byte("babva"),
//		Value:     nil,
//		Tombstone: false,
//		Timestamp: 0,
//	})
//	_ = mts.Add(&model.Record{
//		Key:       []byte("352523"),
//		Value:     nil,
//		Tombstone: false,
//		Timestamp: 5,
//	})
//
//	_ = mts.Add(&model.Record{
//		Key:       []byte("dasda"),
//		Value:     nil,
//		Tombstone: false,
//		Timestamp: 0,
//	})
//	_ = mts.Add(&model.Record{
//		Key:       []byte("basd"),
//		Value:     nil,
//		Tombstone: false,
//		Timestamp: 4,
//	})
//	_ = mts.Add(&model.Record{
//		Key:       []byte(iterator.RateLimiterKey),
//		Value:     nil,
//		Tombstone: false,
//		Timestamp: 3,
//	})
//
//	_ = mts.Add(&model.Record{
//		Key:       []byte("_a_a"),
//		Value:     nil,
//		Tombstone: false,
//		Timestamp: 2,
//	})
//	_ = mts.Add(&model.Record{
//		Key:       []byte("_CMS"),
//		Value:     nil,
//		Tombstone: false,
//		Timestamp: 1,
//	})
//
//}
//func testReservedScan(structure string, t *testing.T) {
//	iterator.GetConfig().Memtable.MaxSize = 3
//	iterator.GetConfig().Memtable.Instances = 5
//	iterator.GetConfig().Memtable.Structure = structure
//	mts := CreateMemtables(&iterator.GetConfig().Memtable)
//	addReserved(mts)
//
//	recordsPrefix := mts.PrefixScan([]byte("_"))
//	recordsRange := mts.RangeScan([]byte("_"), []byte("b"))
//
//	if len(recordsPrefix) != 4 {
//		t.Errorf("error: [Prefix Scan] [%s] expected records size to be 4, got %d", structure, len(recordsPrefix))
//	}
//
//	if len(recordsRange) != 5 {
//		t.Errorf("error: [Range Scan] [%s] expected records size to be 4, got %d", structure, len(recordsRange))
//	}
//}
//
//func TestReservedScan(t *testing.T) {
//	testReservedScan("HashMap", t)
//	testReservedScan("BTree", t)
//	testReservedScan("SkipList", t)
//}
