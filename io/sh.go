package io

import (
	"errors"
	"fmt"
	"github.com/IvanaaXD/NASP/app/config"
	simhash "github.com/IvanaaXD/NASP/structures/sim-hash"
	"time"
)

func NewSH(key string, text string) error {

	key = config.GlobalConfig.SHPrefix + key

	_, exists := Get(key)
	if exists {
		return errors.New("sh with given key already exists")
	}

	sh := simhash.NewSimHash(text)

	success := Put(key, sh.Serialize(), time.Now().UnixNano())
	if success {
		fmt.Println("Saved.")
	} else {
		fmt.Println("Failed.")
	}
	return nil
}

func DeleteSH(key string) error {

	key = config.GlobalConfig.SHPrefix + key

	_, exists := Get(key)
	if !exists {
		return errors.New("no sh with given key")
	}

	ok := Delete(key, time.Now().UnixNano())
	if !ok {
		return errors.New("error deleting sh")
	}
	return nil
}

func SHDistance(key1 string, key2 string) (int, bool) {

	key1 = config.GlobalConfig.SHPrefix + key1
	key2 = config.GlobalConfig.SHPrefix + key2

	rec1, exists := Get(key1)
	if !exists || rec1.Tombstone {
		return 0, false
	}

	rec2, exists := Get(key2)
	if !exists || rec2.Tombstone {
		return 0, false
	}

	sh1 := simhash.Deserialize(rec1.Value)
	sh2 := simhash.Deserialize(rec2.Value)

	found := sh1.GetDistance(sh2)

	return found, true
}
