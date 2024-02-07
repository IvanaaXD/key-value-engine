package simhash

import (
	"fmt"
	"strconv"
	"strings"
)

type SimHash struct {
	Text        string
	FingerPrint []byte
}

func NewSimHash(text string) *SimHash {

	//removing interpunction from text
	interpunction := `.,:;!?()[]{}'"`
	replacer := strings.NewReplacer(interpunction, "")
	newText := replacer.Replace(text)

	words := strings.Fields(newText)

	//counting words in text
	checkWordMap := make(map[string]int)
	hashWordMap := make(map[string]int)

	for i := 0; i < len(words); i++ {

		_, there1 := checkWordMap[words[i]]

		if there1 {
			checkWordMap[words[i]] += 1
		} else {
			checkWordMap[words[i]] = 1
		}

		key := ToBinary(MD5Hash(words[i]))

		_, there2 := hashWordMap[key]

		if there2 {
			hashWordMap[key] += 1
		} else {
			hashWordMap[key] = 1
		}
	}

	weights := make([]int, 256)

	for key, value := range hashWordMap {
		for i := 0; i < len(key); i++ {

			num, err := strconv.Atoi(string(key[i]))

			if err != nil {
				fmt.Println(err)
			}

			if num == 1 {
				weights[i] += num * value
			} else {
				weights[i] -= value
			}
		}
	}

	var fingerPrint []byte

	for i := 0; i < len(weights); i++ {

		if weights[i] > 0 {
			fingerPrint = append(fingerPrint, 1)
		} else {
			fingerPrint = append(fingerPrint, 0)
		}
	}

	s := SimHash{text, fingerPrint}
	return &s
}

func (sh1 SimHash) GetDistance(sh2 *SimHash) int {

	result := xor(sh1.FingerPrint, sh2.FingerPrint)

	return numOfOnes(result)
}

func (sh1 SimHash) Serialize() []byte {
	return sh1.FingerPrint
}

func Deserialize(byteArr []byte) *SimHash {
	return &SimHash{
		FingerPrint: byteArr,
	}
}
