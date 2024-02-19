package NASP

import (
	"errors"
	"github.com/IvanaaXD/NASP---Projekat/app/config"
	"github.com/IvanaaXD/NASP---Projekat/structures/cache"
	"github.com/IvanaaXD/NASP---Projekat/structures/memtable"
	"os"
)

var Memtables *memtable.Memtables
var Cache *cache.Cache

//var TokenBucket *tokenBucket.TokenBucket

func Init() {

	config.Init()

	if _, err := os.Stat("resources"); os.IsNotExist(err) {
		err := os.Mkdir("resources", 0700)
		if err != nil {
			panic("resources error")
		}
	}

	if _, err := os.Stat(config.GlobalConfig.WalPath); errors.Is(err, os.ErrNotExist) {
		f, err := os.Create(config.GlobalConfig.WalPath)
		if err != nil {
			panic("wal file error")
		}
		defer f.Close()
	}

	Memtables = memtable.NewMemtables(&config.GlobalConfig)
	Cache = cache.NewCache(config.GlobalConfig.CacheCapacity)
	//TokenBucket = tokenBucket.NewTokenBucket(config.GlobalConfig.TokenNumber, config.GlobalConfig.TokenRefreshTime)

	//wal.CreateFile()

}
