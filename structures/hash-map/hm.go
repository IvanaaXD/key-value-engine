package hash_map

import (
	"errors"
	"github.com/IvanaaXD/NASP/structures/record"
	"sort"
	"time"
)

type HashMap struct {
	data     map[string]*record.Record
	capacity uint32
}

// creating new HashMap

func NewHashMap(capacity uint32) *HashMap {
	return &HashMap{
		data:     make(map[string]*record.Record),
		capacity: capacity,
	}
}

// returns the size of the hash map

func (hm *HashMap) GetSize() uint {
	return uint(len(hm.data))
}

// GetItems retrieves all records from the HashMap

func (hm *HashMap) GetItems() []record.Record {
	results := []record.Record{}
	for _, rec := range hm.data {
		results = append(results, *rec)
	}
	return results
}

// reads record based on key

func (hm *HashMap) Read(key string) (record.Record, bool) {
	if rec, ok := hm.data[key]; ok {
		return *rec, true
	} else {
		return record.Record{}, false
	}
}

// adds new record

func (hm *HashMap) Write(record record.Record) bool {
	if int(hm.capacity) == len(hm.data) {
		return false
	}
	hm.data[record.Key] = &record
	return true
}

// deletes record and returns true on successful deletion, false otherwise

func (hm *HashMap) Delete(rec record.Record) bool {
	recc, err := hm.Get((rec.Key))

	if err != nil {
		// Return false if the record is not found
		return false
	}

	recc.Tombstone = true
	recc.Value = nil
	recc.Timestamp = int64(time.Now().Unix())

	// Return true to indicate successful deletion
	return true
}

// returns record based on key

func (hm *HashMap) Get(key string) (*record.Record, error) {
	if _, ok := hm.data[(key)]; ok {
		return hm.data[(key)], nil
	} else {
		return nil, errors.New("error: key '" + (key) + "' not found in Hash Map")
	}
}

// searching records by key prefix

func (hm *HashMap) PrefixScan(prefix string, pageNumber, pageSize int) []record.Record {
	results := hm.prefixScanHelper(prefix)

	// Sort the results by key
	sort.Slice(results, func(i, j int) bool {
		return results[i].Key < results[j].Key
	})

	// Paginate the results
	startIdx := (pageNumber - 1) * pageSize
	endIdx := startIdx + pageSize

	if startIdx < 0 {
		startIdx = 0
	}

	if endIdx > len(results) {
		endIdx = len(results)
	}

	return results[startIdx:endIdx]
}

func (hm *HashMap) prefixScanHelper(prefix string) []record.Record {
	results := []record.Record{}
	for key, rec := range hm.data {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			results = append(results, *rec)
		}
	}
	return results
}

// searching for key in given rate

func (hm *HashMap) RangeScan(start, finish string, pageNumber, pageSize int) []record.Record {
	results := hm.rangeScanHelper(start, finish)

	// Sort the results by key
	sort.Slice(results, func(i, j int) bool {
		return results[i].Key < results[j].Key
	})

	// Paginate the results
	startIdx := (pageNumber - 1) * pageSize
	endIdx := startIdx + pageSize

	if startIdx < 0 {
		startIdx = 0
	}

	if endIdx > len(results) {
		endIdx = len(results)
	}

	return results[startIdx:endIdx]
}

func (hm *HashMap) rangeScanHelper(start, finish string) []record.Record {
	results := []record.Record{}
	for key, rec := range hm.data {
		if key >= start && key <= finish {
			results = append(results, *rec)
		}
	}
	return results
}
