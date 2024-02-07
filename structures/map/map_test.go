package sstable

import (
	"fmt"
	"os"
	"testing"
)

func TestMapSerializationDeserialization(t *testing.T) {
	myMap := NewMap()
	myMap.Write("1", "value1", "compression1")
	myMap.Write("2", "value2", "compression2")

	serializedData, err := myMap.Serialize()
	fmt.Println(serializedData)
	if err != nil {
		t.Fatalf("Error during serialization: %v", err)
	}
	filePath := "files/test_map_data.bin"
	err = os.WriteFile(filePath, serializedData, 0644)
	if err != nil {
		t.Errorf("Error writing to file: %v", err)
	}
	defer os.Remove(filePath)

	dataFromFile, err := os.ReadFile(filePath)
	if err != nil {
		t.Errorf("Error reading from file: %v", err)
	}

	newMap := NewMap()
	err = newMap.Deserialize(dataFromFile)
	if err != nil {
		t.Errorf("Deserialization error: %v", err)
	}

	if size := newMap.GetSize(); size != myMap.GetSize() {
		t.Errorf("Expected size %d, got %d", myMap.GetSize(), size)
	}

	for id, value := range myMap.mapa {
		if newValue, ok := newMap.GetValueMode(id); !ok || newValue != value[0] {
			t.Errorf("Expected value %s, got %s", value[0], newValue)
		}
		if newCompression, ok := newMap.GetValueCompression(id); !ok || newCompression != value[1] {
			t.Errorf("Expected compression %s, got %s", value[1], newCompression)
		}
	}
}

func TestMapFileIO(t *testing.T) {
	myMap := NewMap()
	myMap.Write("1", "value1", "compression1")
	myMap.Write("2", "value2", "compression2")

	err := myMap.WriteToFile()
	if err != nil {
		t.Fatalf("Error writing to file: %v", err)
	}

	newMap := NewMap()

	err = newMap.ReadFromFile()
	if err != nil {
		t.Fatalf("Error reading from file: %v", err)
	}

	if size := newMap.GetSize(); size != myMap.GetSize() {
		t.Errorf("Expected size %d, got %d", myMap.GetSize(), size)
	}

	for id, value := range myMap.mapa {
		if newValue, ok := newMap.GetValueMode(id); !ok || newValue != value[0] {
			t.Errorf("Expected value %s, got %s", value[0], newValue)
		}
		if newCompression, ok := newMap.GetValueCompression(id); !ok || newCompression != value[1] {
			t.Errorf("Expected compression %s, got %s", value[1], newCompression)
		}
	}

}

func TestMapFileIOEmptyFile(t *testing.T) {
	myMap := NewMap()

	err := myMap.WriteToFile()
	if err != nil {
		t.Fatalf("Error writing to file: %v", err)
	}

	newMap := NewMap()

	err = newMap.ReadFromFile()
	if err != nil {
		t.Fatalf("Error reading from file: %v", err)
	}

	if size := newMap.GetSize(); size != 0 {
		t.Errorf("Expected size 0, got %d", size)
	}
}
