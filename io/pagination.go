package io

import (
	"fmt"
	"github.com/IvanaaXD/NASP/structures/record"
	"strings"
)

func GetRangePage(start, end string, pageNum, pageSize int) {

	var records = RangeScan(start, end, pageNum, pageSize)

	/*var numOfRecords int
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
				var newRecords = RangeScan(start, end, movePages, pageSize)
				records = append(records, newRecords...)
			}

			continue
		}
	}*/

	startIndex := (pageNum - 1) * pageSize
	endIndex := startIndex + pageSize

	if endIndex > len(records) {
		endIndex = len(records)
	}

	if startIndex >= len(records) {
		fmt.Println("No records found on this page.")
		return
	}
	pageRecords := records[startIndex:endIndex]

	printRecords(pageRecords)
	//printPage(pageRecords, pageNum, pageSize)
}

func GetPrefixPage(prefix string, pageNum, pageSize int) {

	var records = PrefixScan(prefix, pageNum, pageSize)

	/*var numOfRecords int
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
				var newRecords = PrefixScan(prefix, movePages, pageSize)
				records = append(records, newRecords...)
			}

			continue
		}
	}*/

	startIndex := (pageNum - 1) * pageSize
	endIndex := startIndex + pageSize

	if endIndex > len(records) {
		endIndex = len(records)
	}

	if startIndex >= len(records) {
		fmt.Println("No records found on this page.")
		return
	}
	pageRecords := records[startIndex:endIndex]

	printRecords(pageRecords)
}

func GetRangeIteratorPage(start, end string) {

	var records = RangeIterate(start, end)

	var numOfRecords int
	var numOfPages int

	numOfRecords = 1

	numOfPages = len(records)
	currentPage := 1

	for {
		var pageRecords []record.Record
		if (numOfPages-1)*numOfRecords+numOfRecords > len(records) {
			pageRecords = records[(numOfPages-1)*numOfRecords:]
		} else {
			pageRecords = records[(numOfPages-1)*numOfRecords : (numOfPages-1)*numOfRecords+numOfRecords]
		}
		movePages := printPage(pageRecords, currentPage, numOfPages)
		if movePages == 0 {
			break
		} else {
			currentPage += movePages
			continue
		}
	}
}

func GetPrefixIteratorPage(prefix string) {

	var records = PrefixIterate(prefix)

	var numOfRecords int
	var numOfPages int

	numOfRecords = 1

	numOfPages = len(records)
	currentPage := 1

	for {
		var pageRecords []record.Record
		if (numOfPages-1)*numOfRecords+numOfRecords > len(records) {
			pageRecords = records[(numOfPages-1)*numOfRecords:]
		} else {
			pageRecords = records[(numOfPages-1)*numOfRecords : (numOfPages-1)*numOfRecords+numOfRecords]
		}
		movePages := printPage(pageRecords, currentPage, numOfPages)
		if movePages == 0 {
			break
		} else {
			currentPage += movePages
			continue
		}
	}
}

func printPage(records []record.Record, pageNum, numOfPages int) int {
	var next string

	fmt.Printf("\n=========================PAGE %d=========================\n", pageNum)
	printRecords(records)

	switch pageNum {
	case 1:
		if pageNum == numOfPages {
			fmt.Println("-------------------------------------------------------")
			fmt.Println("		                	X	                		")
		} else {
			fmt.Println("-------------------------------------------------------")
			fmt.Println("		                	X	                	next")
		}
	case numOfPages:
		fmt.Println("-------------------------------------------------------")
		fmt.Println("		                	X	                		")
	default:
		fmt.Println("-------------------------------------------------------")
		fmt.Println("		                	X	                	next")
	}

	for {
		fmt.Scanln(&next)
		next = strings.ToLower(next)
		switch next {
		case "next":
			if pageNum != numOfPages {
				return 1
			}
			fmt.Println("There are no next pages. Try again... ")

		case "stop":
			return 0

		default:
			fmt.Println("Invalid option (next / stop). Try again...")
		}
	}
}

func printRecords(records []record.Record) {
	for i := 0; i < len(records); i++ {
		fmt.Printf("%s : %s\n", records[i].Key, string(records[i].Value))
	}
}
