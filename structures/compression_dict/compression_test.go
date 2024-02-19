package compression_dict

import (
	"testing"
)

func TestSerializationDeserialization(t *testing.T) {
	// Create a CompressionDict instance
	compressionDict := NewCompressionDict()

	// Write some data to it
	compressionDict.Write("apple")
	compressionDict.Write("banana")
	compressionDict.Write("orange")

	// Serialize the CompressionDict
	//serializedData, err := compressionDict.Serialize()

	err := compressionDict.WriteToFile()

	if err != nil {
		t.Errorf("Serialization error: %v", err)
	}

	// Create a new CompressionDict instance
	newCompressionDict := NewCompressionDict()

	// Deserialize the serialized data into the new CompressionDict
	err = newCompressionDict.ReadFromFile()
	if err != nil {
		t.Errorf("Deserialization error: %v", err)
	}

	// Create an expected CompressionDict with the same data
	expectedCompressionDict := NewCompressionDict()
	expectedCompressionDict.Write("apple")
	expectedCompressionDict.Write("banana")
	expectedCompressionDict.Write("orange")

	// Compare the original and deserialized CompressionDict
	if len(expectedCompressionDict.keysString) != len(newCompressionDict.keysString) {
		t.Errorf("Key count mismatch after deserialization: expected %d, got %d", len(expectedCompressionDict.keysString), len(newCompressionDict.keysString))
	}

	for i, key := range expectedCompressionDict.keysString {
		if newKey, ok := newCompressionDict.GetKey(uint64(i)); ok {
			if key != newKey {
				t.Errorf("Key mismatch at index %d: expected %s, got %s", i, key, newKey)
			}
		} else {
			t.Errorf("Unable to retrieve key at index %d after deserialization", i)
		}
	}
}
