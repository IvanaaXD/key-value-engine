package writeaheadlog

import (
	"os"
	"testing"

	"github.com/IvanaaXD/NASP/structures/record"
)

var RecordKeys = [...]string{
	"Alpha", "Bravo", "Charlie", "Delta", "Echo", "Foxtrot", "Golf", "Hotel", "India", "Juliet",
	"Kilo", "Lima", "Mike", "November", "Oscar", "Papa", "Quebec", "Romeo", "Sierra", "Tango",
	"Uniform", "Victor", "Whiskey", "Xray", "Yankee", "Zulu",
}

func TestWrite(t *testing.T) {
	wal := InitializeWAL()
	wal.Filename = "testResources/wal_0001.log"
	f, _ := os.Create("testResources/wal_0001.log")
	f.Close()
	expectedBytes := 0
	for i := 0; i < 24; i++ {
		wal.WriteRecord(record.Record{Key: RecordKeys[i], Value: []byte(RecordKeys[i]), Timestamp: 100, Tombstone: false}, 0)
		expectedBytes += len(RecordKeys[i]) + len(RecordKeys[i]) + 4 + 8 + 1 + 8 + 8
	}

	actualBytes := 0
	logs, _ := os.ReadDir("testResources")
	for _, log := range logs {
		asd, err := os.ReadFile("testResources/" + log.Name())
		if err != nil {
			panic(err)
		}
		actualBytes += len(asd)
	}

	for _, log := range logs {
		os.Remove("testResources/" + log.Name())
	}
	if actualBytes != expectedBytes {
		t.Errorf("Not good: %d != %d", actualBytes, expectedBytes)
	}
}

func TestRead(t *testing.T) {
	wal := InitializeWAL()
	wal.Filename = "testResources/wal_0001.log"
	f, _ := os.Create("testResources/wal_0001.log")
	f.Close()

	expectedRecords := make([]record.Record, 0)
	for i := 0; i < 24; i++ {
		expectedRecords = append(expectedRecords, record.Record{Key: RecordKeys[i], Value: []byte(RecordKeys[i]), Timestamp: 100, Tombstone: false})
		wal.WriteRecord(record.Record{Key: RecordKeys[i], Value: []byte(RecordKeys[i]), Timestamp: 100, Tombstone: false}, 0)
	}

	newWal := InitializeWAL()
	newWal.Filename = "testResources/wal_0001.log"

	actualRecords := make([]record.Record, 0)
	for {
		rec := newWal.ReadRecord(0)
		if rec.Key == NullElementKey {
			break
		}
		actualRecords = append(actualRecords, rec)
	}

	if len(expectedRecords) != len(actualRecords) {
		t.Error("Bad lenght")
	}

	logs, _ := os.ReadDir("testResources")
	for _, log := range logs {
		os.Remove("testResources/" + log.Name())
	}

	for i := 0; i < 24; i++ {
		rec1 := expectedRecords[i]
		rec2 := actualRecords[i]
		if rec1.Key != rec2.Key || string(rec1.Value) != string(rec2.Value) || rec1.Timestamp != rec2.Timestamp || rec1.Tombstone != rec2.Tombstone {
			t.Error("Bad record")
		}
	}

}

func TestDelete(t *testing.T) {
	wal := InitializeWAL()
	wal.Filename = "testResources/wal_0001.log"
	f, _ := os.Create("testResources/wal_0001.log")
	f.Close()
	expectedRecords := make([]record.Record, 0)
	for i := 0; i < 12; i++ {
		//expectedRecords = append(expectedRecords, record.Record{Key: RecordKeys[i], Value: []byte(RecordKeys[i]), Timestamp: 100, Tombstone: false})
		wal.WriteRecord(record.Record{Key: RecordKeys[i], Value: []byte(RecordKeys[i]), Timestamp: 100, Tombstone: false}, 0)
	}

	for i := 12; i < 24; i++ {
		expectedRecords = append(expectedRecords, record.Record{Key: RecordKeys[i], Value: []byte(RecordKeys[i]), Timestamp: 100, Tombstone: false})
		wal.WriteRecord(record.Record{Key: RecordKeys[i], Value: []byte(RecordKeys[i]), Timestamp: 100, Tombstone: false}, 1)
	}

	wal.DeleteSerializedRecords(0)

	newWal := InitializeWAL()
	newWal.Filename = "testResources/wal_0001.log"
	actualRecords := make([]record.Record, 0)
	for {
		rec := newWal.ReadRecord(0)
		if rec.Key == NullElementKey {
			break
		}
		actualRecords = append(actualRecords, rec)
	}

	if len(expectedRecords) != len(actualRecords) {
		t.Error("Bad lenght")
	}

	for i := 0; i < 12; i++ {
		rec1 := expectedRecords[i]
		rec2 := actualRecords[i]
		if rec1.Key != rec2.Key || string(rec1.Value) != string(rec2.Value) || rec1.Timestamp != rec2.Timestamp || rec1.Tombstone != rec2.Tombstone {
			t.Errorf("Bad record: %s, %s", rec1.Key, rec2.Key)
		}
	}

	logs, _ := os.ReadDir("testResources")
	for _, log := range logs {
		os.Remove("testResources/" + log.Name())
	}
}
