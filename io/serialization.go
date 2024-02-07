package io

import (
	"bytes"
	"encoding/gob"
	"github.com/IvanaaXD/NASP---Projekat/CountMinSketch"
	bloom_filter "github.com/IvanaaXD/NASP---Projekat/bloom-filter"
	"github.com/IvanaaXD/NASP---Projekat/config"
	"github.com/IvanaaXD/NASP---Projekat/hll"
	simhash "github.com/IvanaaXD/NASP---Projekat/sim-hash"
)

func serializeStructure(key, val string) ([]byte, error) {
	var err error
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)

	switch key {
	case "hll":
		hl := hll.NewHyperLogLog(16)
		err = encoder.Encode(hl)
	case "cms":
		countMinSketch := CountMinSketch.CreateCMS(config.GlobalConfig.CmsEpsilon, config.GlobalConfig.CmsDelta)
		countMinSketch.Serialize()
	case "sh":
		simHash := simhash.NewSimHash(val)
		simHash.Serialize()
	case "bf":
		bf := bloom_filter.NewBloomFilter(config.GlobalConfig.BFExpectedElements, config.GlobalConfig.BFFalsePositiveRate)
		bf.Serialize()
	}

	return buf.Bytes(), err
}
