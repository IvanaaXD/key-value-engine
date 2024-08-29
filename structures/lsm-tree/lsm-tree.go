package lsm_tree

import (
	"log"
	"os"
	"slices"
	"strconv"

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
	// TO-DO: proveriti sa Ivanom sta je sta u configu
	return false
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

// TO-DO: leveled compaction requires different wantedPaths
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
		compactByLevel(wantedPaths)
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

func compactByLevel(paths []string) {

}
