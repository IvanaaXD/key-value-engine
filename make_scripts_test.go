package main

import (
	"math/rand"
	"testing"
	"time"

	"github.com/IvanaaXD/NASP/app/config"
	"github.com/IvanaaXD/NASP/inicialize"
	"github.com/IvanaaXD/NASP/io"
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
