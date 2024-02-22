package record

import (
	"reflect"
	"testing"
)

func TestSerializationDeserialization(t *testing.T) {

	record1 := Record{
		Key:       "testKey",
		Value:     []byte("testValue"),
		Timestamp: 123456789,
		Tombstone: false,
	}

	record2 := Record{
		Key:       "anotherKey",
		Value:     []byte("anotherValue"),
		Timestamp: 987654321,
		Tombstone: true,
	}

	record3 := Record{
		Key:       "emptyValueKey",
		Value:     []byte{},
		Timestamp: 555555555,
		Tombstone: false,
	}

	record4 := Record{
		Key:       "largeTimestampKey",
		Value:     []byte("largeTimestampValue"),
		Timestamp: 9223372036854775807, // Max int64 value
		Tombstone: true,
	}

	records := []Record{record1, record2, record3, record4}

	for _, originalRecord := range records {
		serializedBytes := RecToBytes(originalRecord)
		deserializedRecord := BytesToRec(serializedBytes)

		if !reflect.DeepEqual(originalRecord, deserializedRecord) {
			t.Errorf("Serialization and Deserialization failed. Expected %v, got %v", originalRecord, deserializedRecord)
		}
	}
}
