package writeaheadlog

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/IvanaaXD/NASP/app/config"
	record "github.com/IvanaaXD/NASP/structures/record"
)

type WriteAheadLog struct {
	Filename         string
	SegmentLength    int
	CurrentOffset    int64
	NumSavedElements []uint64
}

const crcLen int = 4
const timestampLen int = 8
const tombstoneLen int = 1
const valuelenLen int = 8
const keylenLen int = 8
const walFolderName string = "resources/"
const NullElementKey string = "NULLELEMENT"

// Konstruktor za WAL
func InitializeWAL() *WriteAheadLog {
	config.Init()
	return &WriteAheadLog{Filename: config.GlobalConfig.WalPath, SegmentLength: config.GlobalConfig.SegmentSize, CurrentOffset: 0, NumSavedElements: make([]uint64, config.GlobalConfig.MemtableNum)}
}

// wal_0001.log
// Pomocna funkcija koja menja filename wal-a na sledeci indeks
func (wal *WriteAheadLog) increaseFileIndex() {
	delovi := strings.Split(wal.Filename, "_")

	indeks := strings.Split(delovi[1], ".")

	intIndeks, err := strconv.Atoi(indeks[0])
	if err != nil {
		log.Panic(err)
	}
	intIndeks += 1
	strIndeks := fmt.Sprintf("%04d", intIndeks)
	wal.Filename = delovi[0] + "_" + strIndeks + "." + indeks[1]
}

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func (wal *WriteAheadLog) checkIfLastWalFile() bool {
	allFiles, err := os.ReadDir(walFolderName)
	if err != nil {
		log.Panic(err)
	}
	return strings.Contains(wal.Filename, allFiles[len(allFiles)-1].Name())
}

// Pomocna funkcija cita citavu vrednost nekog polja (timestamp, keyLen...)
func (wal *WriteAheadLog) readNextValue(bytesToRead int, isCRCBeingRead bool) []byte {
	buffer := make([]byte, bytesToRead)

	file, err := os.OpenFile(wal.Filename, os.O_RDONLY, 0777)
	if err != nil {
		log.Panic(err)
	}
	defer file.Close()

	file.Seek(wal.CurrentOffset, 0)

	bytesRead, err := file.Read(buffer)
	if err == io.EOF && isCRCBeingRead && wal.checkIfLastWalFile() {
		return make([]byte, 0)
	} else if err != nil && err != io.EOF {
		log.Panic(err)
	}

	if bytesRead != bytesToRead {
		wal.increaseFileIndex()
		secondFile, err := os.OpenFile(wal.Filename, os.O_RDONLY, 0777)
		if err != nil {
			log.Panic(err)
		}
		defer secondFile.Close()

		secondBytesRead, err := secondFile.Read(buffer[bytesRead:])
		if secondBytesRead+bytesRead != bytesToRead {
			log.Panic(err)
		}
		wal.CurrentOffset = int64(secondBytesRead)
	} else {
		wal.CurrentOffset += int64(bytesRead)
	}

	return buffer
}

// Funkcija koja ucitava sledeci zapis iz WAL-a. Potrebno proslediti indeks memtabele u koji ce se record upisati
func (wal *WriteAheadLog) ReadRecord(memtableIndex int) record.Record {
	allBytes := make([]byte, 0)

	crcBytes := wal.readNextValue(crcLen, true)
	if len(crcBytes) == 0 {
		return record.Record{Key: NullElementKey, Tombstone: true}
	}
	crcActual := binary.LittleEndian.Uint32(crcBytes)

	timestampBytes := wal.readNextValue(timestampLen, false)
	allBytes = append(allBytes, timestampBytes...)

	tombstoneByte := wal.readNextValue(tombstoneLen, false)
	allBytes = append(allBytes, tombstoneByte...)

	keyLenBytes := wal.readNextValue(keylenLen, false)
	allBytes = append(allBytes, keyLenBytes...)
	keyLenActual := binary.LittleEndian.Uint64(keyLenBytes)

	valueLenBytes := wal.readNextValue(valuelenLen, false)
	allBytes = append(allBytes, valueLenBytes...)
	valueLenActual := binary.LittleEndian.Uint64(valueLenBytes)

	keyBytes := wal.readNextValue(int(keyLenActual), false)
	allBytes = append(allBytes, keyBytes...)

	valueBytes := wal.readNextValue(int(valueLenActual), false)
	allBytes = append(allBytes, valueBytes...)

	if crcActual != CRC32(allBytes) {
		fmt.Println("OPREZ! Moguce je da su podaci iz WAL-a nevalidni!")
	}

	wal.NumSavedElements[memtableIndex] += 1

	return record.BytesToRec(allBytes)
}

// Funkcija koja upisuje prosledjeni zapis u WAL. Potrebno takodje proslediti indeks memtabele u koji je record bio upisan
func (wal *WriteAheadLog) WriteRecord(inputRecord record.Record, memtableIndex int) {
	file, err := os.OpenFile(wal.Filename, os.O_APPEND|os.O_CREATE, 0777)
	if err != nil {
		log.Panic(err)
	}
	defer file.Close()

	recordBytes := record.RecToBytes(inputRecord)
	recordCRC := CRC32(recordBytes)
	allBytes := make([]byte, crcLen)
	binary.LittleEndian.PutUint32(allBytes, recordCRC)
	allBytes = append(allBytes, recordBytes...)

	freeSpace := wal.SegmentLength - int(wal.CurrentOffset)
	if freeSpace >= len(allBytes) {
		_, err = file.Write(allBytes)
		if err != nil {
			log.Panic(err)
		}
		wal.CurrentOffset += int64(len(allBytes))
	} else {
		part1 := allBytes[:freeSpace]
		part2 := allBytes[freeSpace:]

		_, err = file.Write(part1)
		if err != nil {
			log.Panic(err)
		}

		wal.increaseFileIndex()

		file2, err := os.OpenFile(wal.Filename, os.O_APPEND|os.O_CREATE, 0777)
		if err != nil {
			log.Panic(err)
		}
		defer file2.Close()

		_, err = file2.Write(part2)
		if err != nil {
			log.Panic(err)
		}
		wal.CurrentOffset = int64(len(part2))
	}
	wal.NumSavedElements[memtableIndex] += 1
}

// Funkcija koja brise serializovane zapise iz WAL-a. Potrebno je proslediti indeks memtabele koja se flushovala
func (wal *WriteAheadLog) DeleteSerializedRecords(memtableIndex int) {
	allFilesBeforeDeletion, err := os.ReadDir(walFolderName)
	if err != nil {
		log.Panic(err)
	}

	deleterWal := InitializeWAL()
	elementsToDelete := wal.NumSavedElements[memtableIndex]

	for i := 0; i < int(elementsToDelete); i++ {
		deleterWal.ReadRecord(0)
	}

	remainingLoggedRecords := make([]record.Record, 0)
	for {
		loggedRecord := deleterWal.ReadRecord(0)
		if loggedRecord.Tombstone && loggedRecord.Key == "NULLELEMENT" {
			break
		}
		remainingLoggedRecords = append(remainingLoggedRecords, loggedRecord)
	}

	allLogs := make([]string, 0)
	for _, logName := range allFilesBeforeDeletion {
		if strings.Contains(logName.Name(), ".log") {
			allLogs = append(allLogs, logName.Name())
		}
	}

	for _, logForDeletion := range allLogs {
		os.Remove(walFolderName + logForDeletion)
	}

	deleterWal = InitializeWAL()
	for _, record := range remainingLoggedRecords {
		deleterWal.WriteRecord(record, 0)
	}

	allFilesAfterDeletion, err := os.ReadDir(walFolderName)
	if err != nil {
		log.Panic(err)
	}

	wal.Filename = walFolderName + allFilesAfterDeletion[len(allFilesAfterDeletion)-1].Name()
	if !strings.Contains(wal.Filename, "wal_") {
		wal.Filename = walFolderName + "wal_0001.log"
		os.Create(wal.Filename)
	}

	lastFileInfo, err := os.Stat(wal.Filename)
	if err != nil {
		log.Panic(err)
	}
	wal.CurrentOffset = lastFileInfo.Size()
	wal.NumSavedElements[memtableIndex] = 0
}
