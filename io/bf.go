package io

import (
	"errors"
	"fmt"
	"github.com/IvanaaXD/NASP/app/config"
	bloom_filter "github.com/IvanaaXD/NASP/structures/bloom-filter"
	"time"
)

func NewBF(key string, expectedElements int, falsePositiveRate float64) error {

	key = config.GlobalConfig.BFPrefix + key

	_, exists := Get(key)
	if exists {
		return errors.New("bf with given key already exists")
	}

	bf := bloom_filter.NewBloomFilter(expectedElements, falsePositiveRate)

	success := Put(key, bf.Serialize(), time.Now().UnixNano())
	if success {
		fmt.Println("Saved.")
	} else {
		fmt.Println("Failed.")
	}
	return nil
}

func DeleteBF(key string) error {

	key = config.GlobalConfig.BFPrefix + key

	_, exists := Get(key)
	if !exists {
		return errors.New("no bf with given key")
	}

	ok := Delete(key, time.Now().UnixNano())
	if !ok {
		return errors.New("error deleting bloom filter")
	}
	return nil
}

func WriteBF(key string, value []byte) error {

	key = config.GlobalConfig.BFPrefix + key

	rec, exists := Get(key)
	if !exists || rec.Tombstone {
		return errors.New("no bf with given key")
	}

	bf := bloom_filter.Deserialize(rec.Value)
	bf.Add(value)

	success := Put(key, bf.Serialize(), time.Now().UnixNano())
	if success {
		fmt.Println("Saved.")
	} else {
		fmt.Println("Failed.")
	}
	return nil
}

func BFHasKey(key string, value []byte) bool {

	key = config.GlobalConfig.BFPrefix + key

	rec, exists := Get(key)
	if !exists || rec.Tombstone {
		return false
	}

	bf := bloom_filter.Deserialize(rec.Value)

	found := bf.Read(value)

	return found
}
