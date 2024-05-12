package wal

import (
	"github.com/IvanaaXD/NASP/app/config"
	"testing"
	"time"
)

func TestWALWrite(t *testing.T) {
	config.Init()
	filePath := "test_wal.log"
	wal, err := NewWAL(filePath)
	if err != nil {
		t.Errorf("Greška pri kreiranju WAL-a: %v", err)
		return
	}
	defer wal.Close()

	key := []byte("test_key")
	value := []byte("test_value")

	_, err = wal.Write(key, value, time.Now().UnixNano(), false)
	if err != nil {
		t.Errorf("Greška pri pisanju u WAL: %v", err)
		return
	}

	t.Logf("Test uspešno završen")
}
