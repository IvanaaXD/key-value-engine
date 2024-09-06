package iterators

import (
	"log"
	"os"
	"slices"
	"sort"
	"strings"

	"github.com/IvanaaXD/NASP/structures/memtable"
	"github.com/IvanaaXD/NASP/structures/record"
	"github.com/IvanaaXD/NASP/structures/sstable"
)

type PrefixIterator struct {
	memtableRecords  []record.Record
	sstableInstances []sstable.SSTableInstance
	currentRecords   []record.Record
	isValid          []bool
	prefix           string
}

func (iter *PrefixIterator) loadNewRecord(indexToLoad int) {
	if indexToLoad == 0 {
		// load from memtable record array
		if len(iter.memtableRecords) != 0 {
			iter.currentRecords[0] = iter.memtableRecords[0]
			iter.isValid[0] = true
			iter.memtableRecords = iter.memtableRecords[1:]
		} else {
			iter.currentRecords[0] = record.Record{}
			iter.isValid[0] = false
		}

	} else {
		// load from sstable instances
		indexToLoad -= 1
		replacementRecord, replacementIsRead := iter.sstableInstances[indexToLoad].ReadRecord()

		if strings.HasPrefix(replacementRecord.Key, iter.prefix) {
			iter.currentRecords[indexToLoad+1] = replacementRecord
			iter.isValid[indexToLoad+1] = replacementIsRead
		} else {
			iter.currentRecords[indexToLoad+1] = record.Record{}
			iter.isValid[indexToLoad+1] = false
		}

	}

}

func (iter *PrefixIterator) resolveRepeatingRecords() {
	replacableRecords := make([]int, 0)
	for index, record := range iter.currentRecords {

		if slices.Contains(replacableRecords, index) || !iter.isValid[index] {
			continue
		}

		for other_index, other_record := range iter.currentRecords {
			// if it's the same element, an invalid element, or an already added element
			if index == other_index || !iter.isValid[index] || slices.Contains(replacableRecords, other_index) {
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

	if len(replacableRecords) == 0 {
		return
	}

	for _, toChangeIndex := range replacableRecords {
		iter.loadNewRecord(toChangeIndex)
	}

	iter.resolveRepeatingRecords()
}

func (iter *PrefixIterator) findLexicallySmallestRecord() (record.Record, bool) {
	var smallestKey string
	var smallestIndex int
	noValidRecordsFound := true
	for index, record := range iter.currentRecords {
		if iter.isValid[index] {
			smallestKey = record.Key
			smallestIndex = index
			noValidRecordsFound = false
			break
		}
	}

	if noValidRecordsFound {
		return record.Record{}, false
	}

	for index, record := range iter.currentRecords {
		if iter.isValid[index] && record.Key < smallestKey {
			smallestKey = record.Key
			smallestIndex = index
		}
	}

	// update currentRecords and isValid arrays with new values
	returningRecord := iter.currentRecords[smallestIndex]
	iter.loadNewRecord(smallestIndex)

	return returningRecord, true
}

// Konstruktor za Range iterator. Prosledjuju se instance memtabeli koje se trenutno koriste u sistemu i
// pocetak i kraj opsega nad kojim se traze kljucevi
func MakePrefixIterator(minstances []*memtable.Memtable, prefix string) *PrefixIterator {
	// memtables
	tempMemRecords := make([]record.Record, 0)

	for _, instance := range minstances {
		tempMemRecords = append(tempMemRecords, instance.Structure.GetItems()...)
	}

	recordMap := make(map[string]record.Record)
	for _, rec := range tempMemRecords {
		savedRec, exists := recordMap[rec.Key]
		if exists {
			if rec.Timestamp > savedRec.Timestamp {
				recordMap[rec.Key] = rec
			}
		}
	}
	tempMemRecords = make([]record.Record, 0)
	for _, rec := range recordMap {
		tempMemRecords = append(tempMemRecords, rec)
	}

	sort.Slice(tempMemRecords, func(i, j int) bool {
		return tempMemRecords[i].Key < tempMemRecords[j].Key
	})

	actualMemRecords := make([]record.Record, 0)
	for _, rec := range tempMemRecords {
		if strings.HasPrefix(rec.Key, prefix) {
			actualMemRecords = append(actualMemRecords, rec)
		}
	}

	// sstables
	actualSinstances := make([]sstable.SSTableInstance, 0)

	sstablePaths, err := os.ReadDir(sstable.SSTableFolderPath)
	if err != nil {
		log.Fatal(err)
	}

	tempSinstances := make([]sstable.SSTableInstance, 0)
	for _, path := range sstablePaths {
		tempSinstances = append(actualSinstances, sstable.OpenSSTable(path.Name()))
	}

	for _, instance := range tempSinstances {
		if instance.CheckIfContainsPrefix(prefix) {
			actualSinstances = append(actualSinstances, instance)
		}
	}

	// initialization
	beginningRecords := make([]record.Record, len(actualSinstances)+1)
	beginningValidity := make([]bool, len(actualSinstances)+1)

	if len(actualMemRecords) > 0 {
		beginningRecords[0] = actualMemRecords[0]
		beginningValidity[0] = true
	} else {
		beginningRecords[0] = record.Record{}
		beginningValidity[0] = false
	}

	for index, instance := range actualSinstances {
		rec, valid := instance.ReadRecord()
		beginningRecords[index+1] = rec
		beginningValidity[index+1] = valid
	}

	return &PrefixIterator{memtableRecords: actualMemRecords, sstableInstances: actualSinstances, currentRecords: beginningRecords, isValid: beginningValidity, prefix: prefix}
}

// Funkcija sa kojom se dobija sledeci record koji se nalazi u opsegu kreiranog iteratora.
// Vraca record i true ako postoji; vraca prazan record i false ako ne postoji
func (iter *PrefixIterator) GetNext() (record.Record, bool) {
	iter.resolveRepeatingRecords()
	return iter.findLexicallySmallestRecord()
}
