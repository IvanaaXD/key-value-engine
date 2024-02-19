package iterator

import (
	"bytes"
	"github.com/IvanaaXD/NASP/app/config"
)

func IsInvalidKey(i Iterator) bool {

	if i != nil && i.Value() != nil && !IsSpecialKey([]byte(i.Value().Key)) {
		return false

	}
	return true
}

func IsSpecialKey(key []byte) bool {

	specialPrefixes := [][]byte{
		[]byte(config.BF_PREFIX),
		[]byte(config.CMS_PREFIX),
		[]byte(config.HLL_PREFIX),
		[]byte(config.SH_PREFIX),
	}

	for _, spec := range specialPrefixes {
		if bytes.HasPrefix(key, spec) {
			return true
		}
	}

	specialKeys := [][]byte{
		[]byte(config.COMPRESSION_DICT),
		[]byte(config.RATE_LIMIT),
	}

	for _, spec := range specialKeys {
		if bytes.Equal(key, spec) {
			return true
		}
	}
	return false
}
