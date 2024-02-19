package io

import (
	"bytes"
	"github.com/IvanaaXD/NASP/app/config"
	count_min_sketch "github.com/IvanaaXD/NASP/structures/count-min-sketch"
	simhash "github.com/IvanaaXD/NASP/structures/sim-hash"
)

func serializeStructure(key, val string) ([]byte, error) {
	var err error
	var buf bytes.Buffer

	switch key {
	case "hll":
		// hl := hll.NewHyperLogLog(16)
		// hl.Serialize()
	case "cms":
		countMinSketch := count_min_sketch.CreateCMS(config.GlobalConfig.CmsEpsilon, config.GlobalConfig.CmsDelta)
		countMinSketch.Serialize()
	case "sh":
		simHash := simhash.NewSimHash(val)
		simHash.Serialize()
	case "bf":
		//bf := bloom_filter.NewBloomFilter(config.GlobalConfig.BFExpectedElements, config.GlobalConfig.BFFalsePositiveRate)
		//bf.Serialize()
	}

	return buf.Bytes(), err
}
