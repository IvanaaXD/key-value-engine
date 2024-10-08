package inicialize

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/IvanaaXD/NASP/app/config"
	"github.com/IvanaaXD/NASP/structures/cache"
	"github.com/IvanaaXD/NASP/structures/memtable"
	"github.com/IvanaaXD/NASP/structures/record"
	tokenbucket "github.com/IvanaaXD/NASP/structures/tokenBucket"
)

var Memtables *memtable.Memtables
var Cache *cache.Cache

var TokenBucket *tokenbucket.TokenBucket

func Init() {

	config.Init()

	if _, err := os.Stat("resources"); os.IsNotExist(err) {
		err := os.Mkdir("resources", 0700)
		if err != nil {
			panic("resources error")
		}
	}

	if _, err := os.Stat("resources/sstables"); os.IsNotExist(err) {
		err := os.Mkdir("resources/sstables", 0700)
		if err != nil {
			panic("sstables error")
		}
	}

	if _, err := os.Stat(config.GlobalConfig.WalPath); errors.Is(err, os.ErrNotExist) {
		f, err := os.Create(config.GlobalConfig.WalPath)
		if err != nil {
			panic("wal file error")
		}
		defer f.Close()
	}

	Memtables = memtable.NewMemtables()
	Cache = cache.NewCache(config.GlobalConfig.CacheCapacity)
	TokenBucket = tokenbucket.MakeTokenBucket(config.GlobalConfig.TokenNumber, config.GlobalConfig.TokenRefreshTime)

	key := config.GlobalConfig.TBPrefix + "key"
	value := TokenBucket.Serialize()

	Record := record.Record{Key: key, Value: value, Timestamp: time.Now().UnixNano(), Tombstone: false}
	err2 := Memtables.Write(Record)
	if err2 != nil {
		fmt.Println("Failed.")
	}
}
