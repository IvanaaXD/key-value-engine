package simhash

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"strings"
)

func MD5Hash(text string) string {

	hash := md5.Sum([]byte(text))
	hexa := hex.EncodeToString(hash[:])

	return hexa
}

func ToBinary(s string) string {

	var builder strings.Builder

	for _, c := range s {
		fmt.Fprintf(&builder, "%.8b", c)
	}

	return builder.String()
}

func xor(fingerPrint1, fingerPrint2 []byte) []byte {

	result := make([]byte, len(fingerPrint1))
	for i := range fingerPrint1 {
		result[i] = fingerPrint1[i] ^ fingerPrint2[i]
	}
	return result
}

func numOfOnes(slice []byte) int {
	count := 0
	for _, b := range slice {
		for i := 0; i < 8; i++ {
			if (b>>uint(i))&1 == 1 {
				count++
			}
		}
	}
	return count
}
