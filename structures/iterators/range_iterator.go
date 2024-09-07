package iterators

import (
	"log"
	"os"
	"slices"
	"sort"

	"github.com/IvanaaXD/NASP/app/config"
	"github.com/IvanaaXD/NASP/structures/memtable"
	"github.com/IvanaaXD/NASP/structures/record"
	"github.com/IvanaaXD/NASP/structures/sstable"
)

type RangeIterator struct {
	memtableRecords  []record.Record
	sstableInstances []sstable.SSTableInstance
	currentRecords   []record.Record
	isValid          []bool
	begin            string
	end              string
}

func (iter *RangeIterator) loadNewRecord(indexToLoad int) {
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

		if iter.begin <= replacementRecord.Key && replacementRecord.Key <= iter.end {
			iter.currentRecords[indexToLoad+1] = replacementRecord
			iter.isValid[indexToLoad+1] = replacementIsRead
		} else {
			iter.currentRecords[indexToLoad+1] = record.Record{}
			iter.isValid[indexToLoad+1] = false
		}

	}

}

func (iter *RangeIterator) resolveRepeatingRecords() {
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

func (iter *RangeIterator) findLexicallySmallestRecord() (record.Record, bool) {
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
func MakeRangeIterator(minstances []*memtable.Memtable, begin, end string) *RangeIterator {
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
		if begin <= rec.Key && rec.Key <= end {
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
		tempSinstances = append(tempSinstances, sstable.OpenSSTable(path.Name()))
	}

	for _, instance := range tempSinstances {
		if instance.CheckIfContainsRange(begin, end) {
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

	for index := range actualSinstances {
		rec, valid := actualSinstances[index].ReadRecord()
		beginningRecords[index+1] = rec
		beginningValidity[index+1] = valid
	}

	return &RangeIterator{memtableRecords: actualMemRecords, sstableInstances: actualSinstances, currentRecords: beginningRecords, isValid: beginningValidity, begin: begin, end: end}
}

// Funkcija sa kojom se dobija sledeci record koji se nalazi u opsegu kreiranog iteratora.
// Vraca record i true ako postoji; vraca prazan record i false ako ne postoji
func (iter *RangeIterator) GetNext() (record.Record, bool) {
	iter.resolveRepeatingRecords()
	rec, ok := iter.findLexicallySmallestRecord()
	if isReservedKey(rec.Key) || rec.Tombstone {
		return iter.GetNext()
	}
	return rec, ok
}

func isReservedKey(key string) bool {
	config.Init()
	// specialPrefixes := []string{
	// 	config.BF_PREFIX,
	// 	config.CMS_PREFIX,
	// 	config.HLL_PREFIX,
	// 	config.SH_PREFIX,
	// }

	// for _, spec := range specialPrefixes {
	// 	if strings.HasPrefix(key, spec) {
	// 		return true
	// 	}
	// }

	specialKeys := []string{
		config.COMPRESSION_DICT,
		config.RATE_LIMIT,
	}

	for _, spec := range specialKeys {
		if key == spec {
			return true
		}
	}
	return false
}
