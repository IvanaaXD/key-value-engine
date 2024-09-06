package lsm_tree

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/IvanaaXD/NASP/app/config"
	rec "github.com/IvanaaXD/NASP/structures/record"
	sst "github.com/IvanaaXD/NASP/structures/sstable"
)

func isEverythingFullyRead(isReadArray []bool) bool {
	for _, elem := range isReadArray {
		if elem {
			return false
		}
	}
	return true
}

func isSSTableInLSMLevel(sstableName string, lsmLevel int) bool {
	sstableLSMLevel, _ := strconv.Atoi(sstableName[:4])
	return sstableLSMLevel == lsmLevel
}

func extractSSTablePathsOfLSMLevel(lsmLevel int) []string {
	sstablePaths, err := os.ReadDir(sst.SSTableFolderPath)
	if err != nil {
		log.Fatal(err)
	}

	wantedPaths := make([]string, 0)
	// helper variable to help with not iterating through all the paths
	lsmLevelReached := false

	for _, path := range sstablePaths {
		if isSSTableInLSMLevel(path.Name(), lsmLevel) {
			lsmLevelReached = true
			wantedPaths = append(wantedPaths, path.Name())
		} else if lsmLevelReached {
			break
		}
	}

	return wantedPaths
}

func isLeveledCompactionConditionFulfilled(lsmLevel int) bool {
	config.Init()

	requiredSSTableCount := config.GlobalConfig.LSMMaxTables
	for i := 1; i < lsmLevel; i++ {
		requiredSSTableCount = requiredSSTableCount * config.GlobalConfig.LsmLeveledComp
	}

	return len(extractSSTablePathsOfLSMLevel(lsmLevel)) == int(requiredSSTableCount)
}

func findLexicallySmallestRecord(records []rec.Record, isRead []bool) int {
	var smallestKey string
	var smallestIndex int
	for index, record := range records {
		if isRead[index] {
			smallestKey = record.Key
			smallestIndex = index
			break
		}
	}

	for index, record := range records {
		if isRead[index] && record.Key < smallestKey {
			smallestKey = record.Key
			smallestIndex = index
		}
	}

	return smallestIndex
}

func findRepeatingRecords(records []rec.Record, isRead []bool) (bool, []int) {
	replacableRecords := make([]int, 0)
	for index, record := range records {

		if slices.Contains(replacableRecords, index) || !isRead[index] {
			continue
		}

		for other_index, other_record := range records {
			// if it's the same element, an invalid element, or an already added element
			if index == other_index || !isRead[index] || slices.Contains(replacableRecords, other_index) {
				continue
			}

			if record.Key == other_record.Key {
				if record.Timestamp > other_record.Timestamp {
					replacableRecords = append(replacableRecords, other_index)
				} else {
					replacableRecords = append(replacableRecords, index)
					break
				}
			}
		}

	}
	return len(replacableRecords) != 0, replacableRecords
}

// Funkcija koja vrsi proveru uslova za LSM kompakcije, i ako su zadovoljeni, izvrsava ih
func InitializeLSMCheck() {
	initializeLSMCheckRecursive(1)
}

func getWantedPathsForLeveledAlgorithm(lsmLevel int) []string {
	finalPaths := make([]string, 0)

	randomChoiceLevel := lsmLevel - 1
	randomChoicePaths := extractSSTablePathsOfLSMLevel(randomChoiceLevel)
	randomChoicePath := randomChoicePaths[rand.Intn(len(randomChoicePaths))]
	randomChoiceTable := sst.OpenSSTable(randomChoicePath)
	finalPaths = append(finalPaths, randomChoicePath)

	nextLevelTables := extractSSTablePathsOfLSMLevel(lsmLevel)
	possibleMatches := nextLevelTables[1:]

	firstKey, lastKey := randomChoiceTable.GetFirstAndLastKeyInSSTable()

	for _, path := range possibleMatches {
		sstable := sst.OpenSSTable(path)
		otherFirst, otherLast := sstable.GetFirstAndLastKeyInSSTable()
		if otherLast >= firstKey && otherFirst <= lastKey {
			finalPaths = append(finalPaths, path)
		}
	}

	return finalPaths
}

func initializeLSMCheckRecursive(lsmLevel int) {
	config.Init()

	if lsmLevel > config.GlobalConfig.LSMMaxLevels {
		return
	}

	wantedPaths := extractSSTablePathsOfLSMLevel(lsmLevel)

	if config.GlobalConfig.CompactionAlgorithm == "sizeTiered" && uint64(len(wantedPaths)) >= config.GlobalConfig.LSMMaxTables {
		compactBySizeTier(lsmLevel, wantedPaths)
		initializeLSMCheckRecursive(lsmLevel + 1)
	} else if config.GlobalConfig.CompactionAlgorithm == "leveled" && isLeveledCompactionConditionFulfilled(lsmLevel) {
		compactByLevel(lsmLevel)
		initializeLSMCheckRecursive(lsmLevel + 1)
	}
}

func compactBySizeTier(lsmLevel int, paths []string) {
	sstableInstances := make([]sst.SSTableInstance, 0)
	for _, path := range paths {
		sstableInstances = append(sstableInstances, sst.OpenSSTable(path))
	}

	sst.UpdateSSTableNames(lsmLevel + 1)
	newInstance := sst.MakeNewSSTableInstance(lsmLevel + 1)
	newCreator := sst.MakeNewSSTableCreator(*newInstance)

	currentRecords := make([]rec.Record, len(sstableInstances))
	isReadArray := make([]bool, len(sstableInstances))

	for index, instance := range sstableInstances {
		currentRecords[index], isReadArray[index] = instance.ReadRecord()
	}

	for !isEverythingFullyRead(isReadArray) {
		// check if there are any records with the same key and replace the one with the smaller Timestamp value
		isOverlapFound, overlappingRecords := findRepeatingRecords(currentRecords, isReadArray)
		for isOverlapFound {
			for _, toChangeIndex := range overlappingRecords {
				replacementRecord, replacementIsRead := sstableInstances[toChangeIndex].ReadRecord()
				currentRecords[toChangeIndex] = replacementRecord
				isReadArray[toChangeIndex] = replacementIsRead

				isOverlapFound, overlappingRecords = findRepeatingRecords(currentRecords, isReadArray)
			}
		}
		// find the record with the lexically smallest key in currentRecords
		smallestIndex := findLexicallySmallestRecord(currentRecords, isReadArray)
		// write the record into the new sstable
		newCreator.WriteRecord(currentRecords[smallestIndex])
		// get the record from that sstable using sstableInstances and ReadRecord
		newestRecord, newestIsRead := sstableInstances[smallestIndex].ReadRecord()
		// update currentRecords and isReadArray
		currentRecords[smallestIndex] = newestRecord
		isReadArray[smallestIndex] = newestIsRead
	}

	newCreator.CreateIndex()
	newCreator.CreateSummary()
	newCreator.CreateMerkle()
	newCreator.CreateMetadata()

	for _, path := range paths {
		os.Remove(sst.SSTableFolderPath + "/" + path)
	}

}

func compactByLevel(lsmLevel int) {
	sst.UpdateSSTableNames(lsmLevel + 1)
	newInstance := sst.MakeNewSSTableInstance(lsmLevel + 1)
	newCreator := sst.MakeNewSSTableCreator(*newInstance)

	paths := getWantedPathsForLeveledAlgorithm(lsmLevel + 1)

	sstableInstances := make([]sst.SSTableInstance, 0)
	for _, path := range paths {
		sstableInstances = append(sstableInstances, sst.OpenSSTable(path))
	}

	currentRecords := make([]rec.Record, len(sstableInstances))
	isReadArray := make([]bool, len(sstableInstances))

	for index, instance := range sstableInstances {
		currentRecords[index], isReadArray[index] = instance.ReadRecord()
	}

	for !isEverythingFullyRead(isReadArray) {
		// check if there are any records with the same key and replace the one with the smaller Timestamp value
		isOverlapFound, overlappingRecords := findRepeatingRecords(currentRecords, isReadArray)
		for isOverlapFound {
			for _, toChangeIndex := range overlappingRecords {
				replacementRecord, replacementIsRead := sstableInstances[toChangeIndex].ReadRecord()
				currentRecords[toChangeIndex] = replacementRecord
				isReadArray[toChangeIndex] = replacementIsRead

				isOverlapFound, overlappingRecords = findRepeatingRecords(currentRecords, isReadArray)
			}
		}
		// find the record with the lexically smallest key in currentRecords
		smallestIndex := findLexicallySmallestRecord(currentRecords, isReadArray)
		// write the record into the new sstable
		newCreator.WriteRecord(currentRecords[smallestIndex])
		// get the record from that sstable using sstableInstances and ReadRecord
		newestRecord, newestIsRead := sstableInstances[smallestIndex].ReadRecord()
		// update currentRecords and isReadArray
		currentRecords[smallestIndex] = newestRecord
		isReadArray[smallestIndex] = newestIsRead
	}

	newCreator.CreateIndex()
	newCreator.CreateSummary()
	newCreator.CreateMerkle()
	newCreator.CreateMetadata()

	fixIndexAndDeleteUsedSSTables(lsmLevel+1, paths)
}

func fixIndexAndDeleteUsedSSTables(lsmLevel int, usedPaths []string) {
	config.Init()
	// determine which index the newly created sstable needs
	paths := extractSSTablePathsOfLSMLevel(lsmLevel)
	newestTable := sst.OpenSSTable(paths[0])

	paths = paths[1:]
	sinstances := make([]sst.SSTableInstance, 0)
	for _, path := range paths {
		sinstances = append(sinstances, sst.OpenSSTable(path))
	}

	newestFirstKey, newestLastKey := newestTable.GetFirstAndLastKeyInSSTable()

	inRangeValues := make([]bool, len(sinstances))
	mostRecentKeys := make(map[string]string)

	for index, instance := range sinstances {
		ignoreFirstKey, ignoreLastKey := false, false
		key1, key2 := instance.GetFirstAndLastKeyInSSTable()
		inRangeValues[index] = instance.CheckIfContainsRange(newestFirstKey, newestLastKey)

		for otherIndex, otherInstance := range sinstances {
			if otherIndex >= index {
				break
			}

			if isPossiblyContained(otherInstance, key1) {
				ignoreFirstKey = true
			}

			if isPossiblyContained(otherInstance, key2) {
				ignoreLastKey = true
			}

		}

		if !ignoreFirstKey {
			_, ok := mostRecentKeys[key1]
			if !ok {
				mostRecentKeys[key1] = paths[index]
			}
		}

		if !ignoreLastKey {
			_, ok := mostRecentKeys[key2]
			if !ok {
				mostRecentKeys[key2] = paths[index]
			}
		}
	}

	bestFittingIndex := 0

	unmergedPaths := make([]string, 0)
	for _, path := range paths {
		if !slices.Contains(usedPaths, path) {
			unmergedPaths = append(unmergedPaths, path)
		}
	}

	for key, relevantPath := range mostRecentKeys {
		if isPossiblyContained(newestTable, key) && !slices.Contains(usedPaths, relevantPath) && bestFittingIndex <= slices.Index(unmergedPaths, relevantPath)+1 {
			bestFittingIndex = slices.Index(unmergedPaths, relevantPath) + 1
		}
	}

	// delete usedPaths BUT ONLY AFTER THE NAMES THEMSELVES HAVE BEEN UPDATED ACCORDINGLY
	for _, path := range usedPaths {
		os.Remove(sst.SSTableFolderPath + "/" + path)
	}

	newPaths := make([]string, 0)

	for index, path := range unmergedPaths {
		newPath := fmt.Sprintf("temptable%04d", index)
		if strings.Contains(path, ".bin") {
			newPath += ".bin"
		}
		newPaths = append(newPaths, newPath)
		os.Rename(sst.SSTableFolderPath+"/"+path, sst.SSTableFolderPath+"/"+newPath)
	}

	var newestTablePath string
	if config.GlobalConfig.SSTFiles == "one" {
		newestTablePath = sst.SSTableFolderPath + "/" + "tempnewtable.bin"
	} else {
		newestTablePath = sst.SSTableFolderPath + "/" + "tempnewtable"
	}
	os.Rename(sst.SSTableFolderPath+"/"+fmt.Sprintf("%04dsstable0001", lsmLevel), newestTablePath)

	isNewTableInserted := false
	for index, newPath := range newPaths {
		var path string
		if index == bestFittingIndex {
			path = fmt.Sprintf("%04dsstable%04d", lsmLevel, index+1)
			if strings.Contains(newestTablePath, ".bin") {
				path += ".bin"
			}
			os.Rename(newestTablePath, sst.SSTableFolderPath+"/"+newPath)
		}
		if isNewTableInserted {
			path = fmt.Sprintf("%04dsstable%04d", lsmLevel, index+1)
		} else {
			path = fmt.Sprintf("%04dsstable%04d", lsmLevel, index)
		}

		if strings.Contains(newPath, ".bin") {
			path += ".bin"
		}

		os.Rename(sst.SSTableFolderPath+"/"+newPath, sst.SSTableFolderPath+"/"+path)
	}
}

func isPossiblyContained(sstable sst.SSTableInstance, key string) bool {
	first, last := sstable.GetFirstAndLastKeyInSSTable()
	return first <= key && key <= last
}
