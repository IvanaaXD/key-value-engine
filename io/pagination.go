package io

import (
	"fmt"
	"github.com/IvanaaXD/NASP/structures/record"
	"math"
)

func GetRangePage(start, end string, pageNum, pageSize int) {

	var records = RangeScan(start, end, pageNum, pageSize, []*record.Record{})

	var numOfRecords int
	var numOfPages int

	numOfRecords = pageSize

	numOfPages = int(math.Ceil(float64(len(records)) / float64(numOfRecords)))

	for {
		var pageRecords []record.Record
		if (pageNum-1)*numOfRecords+numOfRecords > len(records) {
			pageRecords = records[(pageNum-1)*numOfRecords:]
		} else {
			pageRecords = records[(pageNum-1)*numOfRecords : (pageNum-1)*numOfRecords+numOfRecords]
		}
		movePages := printPage(pageRecords, pageNum, numOfPages)
		if movePages == 0 {
			break
		} else {
			pageNum += movePages

			if movePages == 1 {
				var newRecords = RangeScan(start, end, movePages, pageSize, GetListOfPointers(records))
				records = append(records, newRecords...)
			}

			continue
		}
	}
}

func GetListOfPointers(listOfRecords []record.Record) []*record.Record {

	pointers := make([]*record.Record, len(listOfRecords))

	for i, rec := range listOfRecords {
		pointers[i] = &rec
	}

	return pointers
}

func GetPrefixPage(prefix string, pageNum, pageSize int) {

	var records = PrefixScan(prefix, pageNum, pageSize, []*record.Record{})

	var numOfRecords int
	var numOfPages int

	numOfRecords = pageSize

	numOfPages = int(math.Ceil(float64(len(records)) / float64(numOfRecords)))

	for {
		var pageRecords []record.Record
		if (pageNum-1)*numOfRecords+numOfRecords > len(records) {
			pageRecords = records[(pageNum-1)*numOfRecords:]
		} else {
			pageRecords = records[(pageNum-1)*numOfRecords : (pageNum-1)*numOfRecords+numOfRecords]
		}
		movePages := printPage(pageRecords, pageNum, numOfPages)
		if movePages == 0 {
			break
		} else {
			pageNum += movePages

			if movePages == 1 {
				var newRecords = PrefixScan(prefix, movePages, pageSize, GetListOfPointers(records))
				records = append(records, newRecords...)
			}

			continue
		}
	}
}

func printPage(records []record.Record, pageNum, numOfPages int) int {
	var next string

	fmt.Printf("\n=========================PAGE %d=========================\n", pageNum)
	for i := 0; i < len(records); i++ {
		fmt.Printf("%s : %s\n", records[i].Key, string(records[i].Value))
	}

	switch pageNum {
	case 1:
		if pageNum == numOfPages {
			fmt.Println("-------------------------------------------------------")
			fmt.Println("		                	X	                		")
		} else {
			fmt.Println("-------------------------------------------------------")
			fmt.Println("		                	X	                	   R")
		}
	case numOfPages:
		fmt.Println("-------------------------------------------------------")
		fmt.Println("L		                	X	                		")
	default:
		fmt.Println("-------------------------------------------------------")
		fmt.Println("L		                	X	                	   R")
	}

	for {
		fmt.Scanln(&next)
		switch next {
		case "r":
			if pageNum != numOfPages {
				return 1
			}
			fmt.Println("There are no next pages. Try again... ")

		case "R":
			if pageNum != numOfPages {
				return 1
			}
			fmt.Println("There are no next pages. Try again... ")

		case "L":
			if pageNum != 1 {
				return -1
			}
			fmt.Println("There are no previous pages. Try again... ")

		case "l":
			if pageNum != 1 {
				return -1
			}
			fmt.Println("There are no previous pages. Try again...")
		case "x":
			return 0
		case "X":
			return 0
		default:
			fmt.Println("Invalid option (l / x / r). Try again...")
		}
	}
}
