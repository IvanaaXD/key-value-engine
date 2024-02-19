package io

//import (
//	"errors"
//	"fmt"
//	"github.com/IvanaaXD/NASP/app/config"
//	hll2 "github.com/IvanaaXD/NASP/hll"
//	"time"
//)
//
//func NewHLL(key string, n uint) error {
//
//	key = config.GlobalConfig.HLLPrefix + key
//
//	_, exists := Get(key)
//	if exists {
//		return errors.New("hll with given key already exists")
//	}
//
//	hll := hll2.NewHyperLogLog(n)
//
//	success := Put(key, hll.Serialize(), time.Now().UnixNano())
//	if success {
//		fmt.Println("Saved.")
//	} else {
//		fmt.Println("Failed.")
//	}
//	return nil
//}
//
//func DeleteHLL(key string) error {
//
//	key = config.GlobalConfig.HLLPrefix + key
//
//	_, exists := Get(key)
//	if !exists {
//		return errors.New("no hll with given key")
//	}
//
//	ok := Delete(key, time.Now().UnixNano())
//	if !ok {
//		return errors.New("error deleting hll")
//	}
//	return nil
//}
//
//func WriteHLL(key string, value []byte) error {
//
//	key = config.GlobalConfig.HLLPrefix + key
//
//	rec, exists := Get(key)
//	if !exists {
//		return errors.New("no hll with given key")
//	}
//
//	hll := hll2.Deserialize(rec.Value)
//	hll.Add(value)
//
//	success := Put(key, hll.Serialize(), time.Now().UnixNano())
//	if success {
//		fmt.Println("Saved.")
//	} else {
//		fmt.Println("Failed.")
//	}
//	return nil
//}
//
//func HLLDiscount(key string) (uint64, bool) {
//
//	key = config.GlobalConfig.HLLPrefix + key
//
//	rec, exists := Get(key)
//	if !exists {
//		return 0, false
//	}
//
//	hll := hll2.Deserialize(rec.Value)
//
//	found := hll.Count()
//
//	return found, true
//}
