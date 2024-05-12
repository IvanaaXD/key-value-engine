package io

import (
	"bufio"
	"fmt"
	"github.com/IvanaaXD/NASP/app/config"
	"github.com/IvanaaXD/NASP/structures/iterator"
	tokenbucketv2 "github.com/IvanaaXD/NASP/structures/tokenBucket"
	"os"
	"strconv"
	"time"
)

func GetInput(isNewWrite bool) (string, []byte) {

	scanner := bufio.NewScanner(os.Stdin)
	var key, value string
	value = ""

	for {
		fmt.Print("Key: ")
		scanner.Scan()
		key = scanner.Text()
		if len(key) <= 0 {
			fmt.Println("empty key")
			continue
		}
		if iterator.IsSpecialKey([]byte(key)) {
			fmt.Println("reserved key")
			continue
		}
		break
	}

	if isNewWrite {
		for {
			fmt.Print("Value: ")
			scanner.Scan()
			value = scanner.Text()
			if len(value) <= 0 {
				fmt.Println("Empty value")
				continue
			}
			break
		}
	}

	var bytes []byte
	bytes = []byte(value)

	return key, bytes
}

func GetKey() string {

	scanner := bufio.NewScanner(os.Stdin)
	var key string

	for {
		fmt.Print("Key: ")
		scanner.Scan()
		key = scanner.Text()

		if len(key) <= 0 {
			fmt.Println("empty key")
			continue
		}
		if iterator.IsSpecialKey([]byte(key)) {
			fmt.Println("reserved key")
			continue
		}
		break
	}
	return key
}

func RangeScanInput() (string, string) {

	scanner := bufio.NewScanner(os.Stdin)
	var start, end string

	for {
		fmt.Print("Start: ")
		scanner.Scan()
		start = scanner.Text()

		if len(start) <= 0 {
			fmt.Println("empty key")
			continue
		}
		if iterator.IsSpecialKey([]byte(start)) {
			fmt.Println("reserved key")
			continue
		}
		break
	}

	for {
		fmt.Print("End: ")
		scanner.Scan()
		end = scanner.Text()

		if len(end) <= 0 {
			fmt.Println("empty key")
			continue
		}
		if iterator.IsSpecialKey([]byte(end)) {
			fmt.Println("reserved key")
			continue
		}
		break
	}

	return start, end
}

func PrefixScanInput() string {

	scanner := bufio.NewScanner(os.Stdin)
	var prefix string

	for {
		fmt.Print("Prefix: ")
		scanner.Scan()
		prefix = scanner.Text()

		if len(prefix) <= 0 {
			fmt.Println("empty key")
			continue
		}
		if iterator.IsSpecialKey([]byte(prefix)) {
			fmt.Println("reserved key")
			continue
		}
		break
	}

	return prefix
}

func GetBF() (string, int, float64) {

	scanner := bufio.NewScanner(os.Stdin)
	var key, eeStr, fprStr string

	for {
		fmt.Print("Key: ")
		scanner.Scan()
		key = scanner.Text()

		if len(key) <= 0 {
			fmt.Println("empty key")
			continue
		}
		if iterator.IsSpecialKey([]byte(key)) {
			fmt.Println("reserved key")
			continue
		}
		break
	}

	for {
		fmt.Print("Expected elements: ")
		scanner.Scan()
		eeStr = scanner.Text()

		if len(eeStr) <= 0 {
			fmt.Println("empty expected elements")
			continue
		}
		break
	}
	ee, _ := strconv.Atoi(eeStr)

	for {
		fmt.Print("False positive rate: ")
		scanner.Scan()
		fprStr = scanner.Text()

		if len(fprStr) <= 0 {
			fmt.Println("empty false positive rate")
			continue
		}
		break
	}
	fpr, _ := strconv.ParseFloat(fprStr, 64)

	return key, ee, fpr
}

func GetCMS() (string, float64, float64) {

	scanner := bufio.NewScanner(os.Stdin)
	var key, eStr, dStr string

	for {
		fmt.Print("Key: ")
		scanner.Scan()
		key = scanner.Text()

		if len(key) <= 0 {
			fmt.Println("empty key")
			continue
		}
		if iterator.IsSpecialKey([]byte(key)) {
			fmt.Println("reserved key")
			continue
		}
		break
	}

	for {
		fmt.Print("Epsilon: ")
		scanner.Scan()
		eStr = scanner.Text()

		if len(eStr) <= 0 {
			fmt.Println("empty epsilon")
			continue
		}
		break
	}
	e, _ := strconv.ParseFloat(eStr, 64)

	for {
		fmt.Print("Delta: ")
		scanner.Scan()
		dStr = scanner.Text()

		if len(dStr) <= 0 {
			fmt.Println("empty delta")
			continue
		}
		break
	}
	d, _ := strconv.ParseFloat(dStr, 64)

	return key, e, d
}

func GetSH() (string, string) {

	scanner := bufio.NewScanner(os.Stdin)
	var key, text string

	for {
		fmt.Print("Key: ")
		scanner.Scan()
		key = scanner.Text()

		if len(key) <= 0 {
			fmt.Println("empty key")
			continue
		}
		if iterator.IsSpecialKey([]byte(key)) {
			fmt.Println("reserved key")
			continue
		}
		break
	}

	for {
		fmt.Print("Text: ")
		scanner.Scan()
		text = scanner.Text()

		if len(text) <= 0 {
			fmt.Println("empty text")
			continue
		}
		break
	}
	return key, text
}

func GetKeysSH() (string, string) {

	scanner := bufio.NewScanner(os.Stdin)
	var key1, key2 string

	for {
		fmt.Print("Key 1: ")
		scanner.Scan()
		key1 = scanner.Text()

		if len(key1) <= 0 {
			fmt.Println("empty key")
			continue
		}
		if iterator.IsSpecialKey([]byte(key1)) {
			fmt.Println("reserved key")
			continue
		}
		break
	}

	for {
		fmt.Print("Key 2: ")
		scanner.Scan()
		key2 = scanner.Text()

		if len(key2) <= 0 {
			fmt.Println("empty key")
			continue
		}
		if iterator.IsSpecialKey([]byte(key2)) {
			fmt.Println("reserved key")
			continue
		}
		break
	}
	return key1, key2
}

func GetHLL() (string, uint) {

	scanner := bufio.NewScanner(os.Stdin)
	var key, mStr string

	for {
		fmt.Print("Key: ")
		scanner.Scan()
		key = scanner.Text()

		if len(key) <= 0 {
			fmt.Println("empty key")
			continue
		}
		if iterator.IsSpecialKey([]byte(key)) {
			fmt.Println("reserved key")
			continue
		}
		break
	}

	for {
		fmt.Print("Num of registers: ")
		scanner.Scan()
		mStr = scanner.Text()

		if len(mStr) <= 0 {
			fmt.Println("empty num of registers")
			continue
		}
		break
	}
	m, _ := strconv.ParseUint(mStr, 10, 64)

	return key, uint(m)
}

func IsTBAvailable() bool {

	key := config.GlobalConfig.TBPrefix + "key"
	rec, _ := Get(key)

	value := rec.Value
	token := tokenbucketv2.Deserialize(value)

	available := token.TokensAvailable()

	Put(key, token.Serialize(), time.Now().UnixNano())

	return available
}

func Menu() error {
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Println()
		fmt.Println("================MENI================")
		fmt.Println()
		fmt.Println("1. Put")
		fmt.Println("2. Read")
		fmt.Println("3. Delete")
		fmt.Println("4. Range scan")
		fmt.Println("5. Prefix scan")
		fmt.Println()
		fmt.Println("============BLOOM FILTER============")
		fmt.Println()
		fmt.Println("6. Make new BF")
		fmt.Println("7. Add to BF")
		fmt.Println("8. Find in BF")
		fmt.Println("9. Delete BF")
		fmt.Println()
		fmt.Println("==============SIM HASH==============")
		fmt.Println()
		fmt.Println("10. Make new SH \n (adding to sh at same time)")
		fmt.Println("11. Find distance")
		fmt.Println("12. Delete SH")
		fmt.Println()
		fmt.Println("==========COUNT MIN SKETCH==========")
		fmt.Println()
		fmt.Println("13. Make new CMS")
		fmt.Println("14. Add to CMS")
		fmt.Println("15. Find frequency")
		fmt.Println("16. Delete CMS")
		fmt.Println()
		fmt.Println("============HYPER LOGLOG============")
		fmt.Println()
		fmt.Println("17. Make new HLL")
		fmt.Println("18. Add to HLL")
		fmt.Println("19. Find cardinality")
		fmt.Println("20. Delete HLL")
		fmt.Println()
		fmt.Println("===================================")
		fmt.Println()
		fmt.Println("x. Exit")
		fmt.Println()
		fmt.Println("===================================")
		fmt.Println()

		fmt.Print(">")
		scanner.Scan()

		switch scanner.Text() {
		case "1": // PUT
			if !IsTBAvailable() {
				fmt.Println("Too many requests. Please wait.")
			} else {
				key, value := GetInput(true)

				timestamp := time.Now().UnixNano()

				success := Put(key, value, timestamp)
				if success {
					fmt.Println("Write saved.")
				} else {
					fmt.Println("Write failed.")
				}
			}

		case "2": // READ
			if !IsTBAvailable() {
				fmt.Println("Too many requests. Please wait.")
			} else {
				key := GetKey()
				rec, _ := Get(key)
				if rec.Tombstone || rec.Key == "" {
					fmt.Println("Record not found")
				} else {
					fmt.Print("Record found: ")
					fmt.Println(key)
					fmt.Println(string(rec.Value))
				}
			}

		case "3": // DELETE
			if !IsTBAvailable() {
				fmt.Println("Too many requests. Please wait.")
			} else {
				key := GetKey()
				timestamp := time.Now().UnixNano()

				success := Delete(key, timestamp)
				if success {
					fmt.Println("Delete saved.")
				} else {
					fmt.Println("Delete failed.")
				}
			}

		case "5": // PREFIX SCAN
			if !IsTBAvailable() {
				fmt.Println("Too many requests. Please wait.")
			} else {
				prefix := PrefixScanInput()
				records := PrefixScan(prefix)
				GetPage(records)
			}

		case "4": // RANGE SCAN
			if !IsTBAvailable() {
				fmt.Println("Too many requests. Please wait.")
			} else {
				start, end := RangeScanInput()
				records := RangeScan(start, end)
				GetPage(records)
			}

		case "6": // MAKE NEW BF
			if !IsTBAvailable() {
				fmt.Println("Too many requests. Please wait.")
			} else {
				key, expectedElements, falsePositiveRate := GetBF()

				err := NewBF(key, expectedElements, falsePositiveRate)
				if err != nil {
					fmt.Println("Error making bf")
				}
			}

		case "7": // ADD T0 BF
			if !IsTBAvailable() {
				fmt.Println("Too many requests. Please wait.")
			} else {
				key, val := GetInput(true)

				err := WriteBF(key, val)
				if err != nil {
					fmt.Println("Error writing to bf")
				}
			}

		case "8": // FIND IN BF
			if !IsTBAvailable() {
				fmt.Println("Too many requests. Please wait.")
			} else {
				key := GetKey()

				ok := BFHasKey(key)
				if !ok {
					fmt.Println("Record does not exist")
				}
				fmt.Println("Record may exist")
			}

		case "9": // DELETE BF
			if !IsTBAvailable() {
				fmt.Println("Too many requests. Please wait.")
			} else {
				key := GetKey()

				err := DeleteBF(key)
				if err != nil {
					fmt.Println("Error deleting bf")
				}
			}

		case "10": // MAKE NEW SH
			if !IsTBAvailable() {
				fmt.Println("Too many requests. Please wait.")
			} else {
				key, text := GetSH()

				err := NewSH(key, text)
				if err != nil {
					fmt.Println("Error making sh")
				}
			}

		case "11": // DISTANCE IN SH
			if !IsTBAvailable() {
				fmt.Println("Too many requests. Please wait.")
			} else {
				key1, key2 := GetKeysSH()

				ok, exists := SHDistance(key1, key2)
				if !exists {
					fmt.Println("Distance is none")
				}
				fmt.Printf("Distance is %d", ok)
			}

		case "12": // DELETE SH
			if !IsTBAvailable() {
				fmt.Println("Too many requests. Please wait.")
			} else {
				key := GetKey()

				err := DeleteSH(key)
				if err != nil {
					fmt.Println("Error deleting sh")
				}
			}

		case "13": // MAKE NEW CMS
			if !IsTBAvailable() {
				fmt.Println("Too many requests. Please wait.")
			} else {
				key, epsilon, delta := GetCMS()

				err := NewCMS(key, epsilon, delta)
				if err != nil {
					fmt.Println("Error making cms")
				}
			}

		case "14": // ADD T0 CMS
			if !IsTBAvailable() {
				fmt.Println("Too many requests. Please wait.")
			} else {
				key, val := GetInput(true)

				err := WriteCMS(key, val)
				if err != nil {
					fmt.Println("Error writing to cms")
				}
			}

		case "15": // FREQUENCY IN CMS
			if !IsTBAvailable() {
				fmt.Println("Too many requests. Please wait.")
			} else {
				key := GetKey()

				ok, exists := CMSFrequency(key)
				if !exists {
					fmt.Println("Frequency is none")
				}
				fmt.Printf("Frequency is %d", ok)
			}

		case "16": // DELETE CMS
			if !IsTBAvailable() {
				fmt.Println("Too many requests. Please wait.")
			} else {
				key := GetKey()

				err := DeleteCMS(key)
				if err != nil {
					fmt.Println("Error deleting cms")
				}
			}

		case "17": // MAKE NEW HLL
			if !IsTBAvailable() {
				fmt.Println("Too many requests. Please wait.")
			} else {
				key, m := GetHLL()
				err := NewHLL(key, m)
				if err != nil {
					fmt.Println("Error making hll")
				}
			}

		case "18": // ADD T0 HLL
			if !IsTBAvailable() {
				fmt.Println("Too many requests. Please wait.")
			} else {
				key, val := GetInput(true)

				err := WriteHLL(key, val)
				if err != nil {
					fmt.Println("Error writing to hll")
				}
			}

		case "19": // COUNT IN HLL
			if !IsTBAvailable() {
				fmt.Println("Too many requests. Please wait.")
			} else {
				key := GetKey()

				ok, exists := HLLDiscount(key)
				if !exists {
					fmt.Println("Discount is none")
				}
				fmt.Printf("Discount is %d", ok)
			}

		case "20": // DELETE HLL
			if !IsTBAvailable() {
				fmt.Println("Too many requests. Please wait.")
			} else {
				key := GetKey()

				err := DeleteHLL(key)
				if err != nil {
					fmt.Println("Error deleting hll")
				}
			}

		case "x": // EXIT
			return nil
		case "X":
			return nil

		default:
			fmt.Println("Invalid input.")
		}
	}
}
