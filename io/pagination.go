package io

import (
	"fmt"
	"github.com/IvanaaXD/NASP---Projekat/record"
	"math"
	"strconv"
)

func GetPage(records []record.Record) {
	var pages string
	var page string
	var numOfRecords int
	var numOfPages int
	var pageNum int

	for {
		fmt.Print("Insert number of records on a page: ")
		fmt.Scanln(&pages)

		num, err := strconv.Atoi(pages)

		if err != nil {
			fmt.Println("Invalid input. Not a number.")
			continue
		} else {
			if num < 1 {
				fmt.Println("Invalid number of pages.. Try again.")
				continue
			}
			numOfRecords = num
			break
		}
	}

	numOfPages = int(math.Ceil(float64(len(records)) / float64(numOfRecords)))

	for {
		fmt.Print("Insert page number you want to look: ")
		fmt.Scanln(&page)
		num, err := strconv.Atoi(page)

		if err != nil {
			fmt.Println("Invalid input. Not a number.")
			continue
		} else {
			if num < 1 || num > numOfPages {
				fmt.Printf("Invalid page number... Try again from range [1-%d]\n", numOfPages)
				continue
			}
			pageNum = num
			break
		}
	}

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
			continue
		}
	}

}

func printPage(records []record.Record, pageNum, numOfPages int) int {
	var next string
	fmt.Printf("\n==================STRANICA %d==================\n", pageNum)
	for i := 0; i < len(records); i++ {
		fmt.Printf("%s : %s\n", records[i].Key, string(records[i].Value))
	}
	switch pageNum {
	case 1:
		if pageNum == numOfPages {
			fmt.Println("------------------------------------------------")
			fmt.Println("			X			")
		} else {
			fmt.Println("------------------------------------------------")
			fmt.Println("			X			R")
		}
	case numOfPages:
		fmt.Println("------------------------------------------------")
		fmt.Println("L			X			")
	default:
		fmt.Println("------------------------------------------------")
		fmt.Println("L			X			R")
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
