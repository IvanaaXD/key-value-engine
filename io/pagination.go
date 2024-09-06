package io

import (
	"fmt"
	"github.com/IvanaaXD/NASP/structures/record"
	"strings"
)

func GetRangePage(start, end string, pageNum, pageSize int) {

	var records = RangeScan(start, end)

	startIndex := (pageNum - 1) * pageSize
	endIndex := startIndex + pageSize

	if endIndex > len(records) {
		endIndex = len(records) - 1
	}

	if startIndex >= len(records) {
		fmt.Println("No records found on this page.")
		return
	}

	pageRecords := records[startIndex:endIndex]
	printRecords(pageRecords)
}

func GetPrefixPage(prefix string, pageNum, pageSize int) {

	var records = PrefixScan(prefix)

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

func printPage(record record.Record, pageNum, numOfPages int) int {
	var next string

	fmt.Printf("\n=========================PAGE %d=========================\n", pageNum)
	fmt.Printf("%s : %s\n", record.Key, string(record.Value))

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
