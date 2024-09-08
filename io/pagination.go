package io

import (
	"fmt"
	"github.com/IvanaaXD/NASP/structures/record"
	"strings"
)

func printPage(record record.Record, pageNum int) int {
	var next string

	fmt.Printf("\n=========================PAGE %d=========================\n", pageNum)
	fmt.Printf("%s : %s\n", record.Key, string(record.Value))

	fmt.Println("-------------------------------------------------------")
	fmt.Println("		                	X	                	next")

	for {
		fmt.Scanln(&next)
		next = strings.ToLower(next)
		switch next {
		case "next":
			return 1

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

func PrintPage(records []record.Record, pageNum int) {
	fmt.Printf("\n=========================PAGE %d=========================\n", pageNum)
	printRecords(records)
}
