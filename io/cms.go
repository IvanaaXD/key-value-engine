package io

import (
	"errors"
	"fmt"
	"github.com/IvanaaXD/NASP---Projekat/CountMinSketch"
	"github.com/IvanaaXD/NASP---Projekat/config"
	"time"
)

func NewCMS(key string, epsilon, delta float64) error {

	key = config.GlobalConfig.CMSPrefix + key

	_, exists := Get(key)
	if exists {
		return errors.New("cms with given key already exists")
	}

	cms := CountMinSketch.CreateCMS(epsilon, delta)

	success := Put(key, cms.Serialize(), time.Now().UnixNano())
	if success {
		fmt.Println("Saved.")
	} else {
		fmt.Println("Failed.")
	}
	return nil
}

func DeleteCMS(key string) error {

	key = config.GlobalConfig.CMSPrefix + key

	_, exists := Get(key)
	if !exists {
		return errors.New("no cms with given key")
	}

	ok := Delete(key, time.Now().UnixNano())
	if !ok {
		return errors.New("error deleting cms")
	}
	return nil
}

func WriteCMS(key string, value []byte) error {

	key = config.GlobalConfig.CMSPrefix + key

	rec, exists := Get(key)
	if !exists {
		return errors.New("no cms with given key")
	}

	cms := CountMinSketch.Deserialize(rec.Value)
	cms.AddItem(value)

	success := Put(key, cms.Serialize(), time.Now().UnixNano())
	if success {
		fmt.Println("Saved.")
	} else {
		fmt.Println("Failed.")
	}
	return nil
}

func CMSFrequency(key string) (uint64, bool) {

	key = config.GlobalConfig.CMSPrefix + key

	rec, exists := Get(key)
	if !exists {
		return 0, false
	}

	cms := CountMinSketch.Deserialize(rec.Value)

	found := cms.GetFrequency(rec.Value)

	return found, true
}
