package wal

import (
	"github.com/IvanaaXD/NASP/app/config"
	"os"
	"path/filepath"
	"testing"
)

func TestWALOperations(t *testing.T) {

	config.Init()

	path := filepath.Join("..", config.GlobalConfig.WalPath)
	log, err2 := NewWAL(path)
	if err2 != nil {
		t.Fatalf("Error creating wal.log %v", err2)
	}

	err := CreateFile()
	if err != nil {
		t.Fatalf("Error creating WAL file: %v", err)
	}

	err = WriteOffsets(0, 5)
	if err != nil {
		t.Fatalf("Error writing offsets: %v", err)
	}
	log.Write([]byte("1"), []byte("d"), 0, false)

	err = WriteOffsets(1, 5)
	if err != nil {
		t.Fatalf("Error writing offsets: %v", err)
	}
	log.Write([]byte("2"), []byte("d"), 0, false)

	err = WriteOffsets(2, 5)
	if err != nil {
		t.Fatalf("Error writing offsets: %v", err)
	}
	log.Write([]byte("3"), []byte("d"), 0, false)

	err = WriteOffsets(3, 5)
	if err != nil {
		t.Fatalf("Error writing offsets: %v", err)
	}
	log.Write([]byte("4"), []byte("d"), 0, false)

	err = WriteOffsets(4, 5)
	if err != nil {
		t.Fatalf("Error writing offsets: %v", err)
	}
	log.Write([]byte("5"), []byte("d"), 0, false)

	offsets, err := ReadOffsets()
	if err != nil {
		t.Fatalf("Error reading offsets: %v", err)
	}
	expectedOffsets := []int{5, 10, 15, 20, 25}
	if !intSlicesEqual(offsets, expectedOffsets) {
		t.Fatalf("Expected offsets %v, got %v", expectedOffsets, offsets)
	}

	err = WalFlush(1)
	if err != nil {
		t.Fatalf("Error flushing WAL: %v", err)
	}

	offsetsAfterFlush, err := ReadOffsets()
	if err != nil {
		t.Fatalf("Error reading offsets after flush: %v", err)
	}
	expectedOffsetsAfterFlush := []int{5, 10, 15, 20, 0}
	if !intSlicesEqual(offsetsAfterFlush, expectedOffsetsAfterFlush) {
		t.Fatalf("Expected offsets %v after flush, got %v", expectedOffsetsAfterFlush, offsetsAfterFlush)
	}

	walPathRelative := filepath.Join("resources", config.GlobalConfig.OffsetPath)
	path1 := filepath.Join("..", walPathRelative)
	content, err := os.ReadFile(path1)
	if err != nil {
		t.Fatalf("Error reading wal.txt after flush: %v", err)
	}
	expectedContent := "5, 10, 15, 20, 0"
	if string(content) != expectedContent {
		t.Fatalf("Expected content %q in wal.txt after flush, got %q", expectedContent, string(content))
	}
}

func intSlicesEqual(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}
