package sstable

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/IvanaaXD/NASP/app/config"
	bf "github.com/IvanaaXD/NASP/structures/bloom-filter"
	compress "github.com/IvanaaXD/NASP/structures/compression_dict"
	merk "github.com/IvanaaXD/NASP/structures/merkletree"
	rec "github.com/IvanaaXD/NASP/structures/record"
)

type SSTableInstance struct {
	currentOffset            int64
	dataBeginOffset          int64
	indexBeginOffset         int64
	indexLastElementOffset   int64
	summaryBeginOffset       int64
	summaryLastElementOffset int64
	bloomfilterBeginOffset   int64
	merkleBeginOffset        int64
	filename                 string
	isSingleFile             bool
	isCompressed             bool
}

type SSTableCreator struct {
	Instance             SSTableInstance
	currentIndexNumber   uint32
	currentSummaryNumber uint32
	currentDataOffset    uint64
	currentIndexOffset   uint64
	currentSummaryOffset uint64
}

// Notes
// mozda jos neke druge stvari kao dataBegin, merkleBegin itd.

const tombstoneTrue byte = 255
const tombstoneFalse byte = 0
const sstableFolderPath string = "./resources/sstables"
const newSSTablePath string = "./resources/sstables/0001sstable0001"
const FirstOrLastElement uint32 = 9999999
const singleFileMetaLength uint16 = 57

// const metaIsCompressedLength uint16 = 1
// const metaAnyOffsetLength uint16 = 8

// const metaBloomfilterBeginOffset int64 = 1
// const metaDataBeginOffset int64 = 9
// const metaIndexBeginOffset int64 = 17
// const metaIndexLastElementOffset int64 = 25
// const metaSummaryBeginOffset int64 = 33
// const metaSummaryLastElementOffset int64 = 41
// const metaMerkleBeginOffset int64 = 49
const metaBloomfilterBegin int64 = 57

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func makeMerkleTreeFromData(sstable SSTableInstance) merk.MerkleTree {
	allRecords := make([]rec.Record, 0)
	for {
		newRecord, recordExists := sstable.ReadRecord()
		if !recordExists {
			break
		}
		allRecords = append(allRecords, newRecord)
	}
	return merk.MakeMerkleTree(allRecords)
}

// Funkcija pretvara Record u zapis za SSTabelu BEZ KOMPRESIJE (public funkcija zbog LSM)
func RecordToSSTableRecord(inputRecord rec.Record) []byte {
	config.Init()
	compressionDictionary := compress.NewCompressionDict()
	compressionDictionary.ReadFromFile()

	ssRecordBytes := make([]byte, 0)
	normalRecordBytes := rec.RecToBytes(inputRecord)
	crcValue := CRC32(normalRecordBytes)
	crcValueBytes := make([]byte, 5)
	writtenBytes := binary.PutUvarint(crcValueBytes, uint64(crcValue))
	ssRecordBytes = append(ssRecordBytes, crcValueBytes[:writtenBytes]...)

	timestampBytes := make([]byte, 9)
	writtenBytes = binary.PutUvarint(timestampBytes, uint64(inputRecord.Timestamp))
	ssRecordBytes = append(ssRecordBytes, timestampBytes[:writtenBytes]...)

	var tombstoneByte byte
	if inputRecord.Tombstone {
		tombstoneByte = tombstoneTrue
	} else {
		tombstoneByte = tombstoneFalse
	}
	ssRecordBytes = append(ssRecordBytes, tombstoneByte)

	keySizeBytes := make([]byte, 8)

	if config.GlobalConfig.Compression == "no" {
		writtenBytes = binary.PutUvarint(keySizeBytes, uint64(len(inputRecord.Key)))
		ssRecordBytes = append(ssRecordBytes, keySizeBytes[:writtenBytes]...)
	} else {
		compressionDictionary.Write(inputRecord.Key)
		compressedKey, _ := compressionDictionary.GetId(inputRecord.Key)
		keyTemp := make([]byte, 100)
		writtenBytes = binary.PutUvarint(keyTemp, compressedKey)
		actualWrittenBytes := binary.PutUvarint(keySizeBytes, uint64(writtenBytes))
		ssRecordBytes = append(ssRecordBytes, keySizeBytes[:actualWrittenBytes]...)
	}

	if !inputRecord.Tombstone {
		// Ako nije obrisan, serijalizovati i value size
		valueSizeBytes := make([]byte, 8)
		writtenBytes = binary.PutUvarint(valueSizeBytes, uint64(len(inputRecord.Value)))
		ssRecordBytes = append(ssRecordBytes, valueSizeBytes[:writtenBytes]...)
	}

	if config.GlobalConfig.Compression == "no" {
		keyBytes := []byte(inputRecord.Key)
		ssRecordBytes = append(ssRecordBytes, keyBytes...)
	} else {
		compressedKey, _ := compressionDictionary.GetId(inputRecord.Key)
		compKeyBytes := make([]byte, 8)
		writtenBytes = binary.PutUvarint(compKeyBytes, compressedKey)
		ssRecordBytes = append(ssRecordBytes, compKeyBytes[:writtenBytes]...)
	}

	if !inputRecord.Tombstone {
		// Ako nije obrisan, serijalizovati i value
		ssRecordBytes = append(ssRecordBytes, inputRecord.Value...)
	}
	return ssRecordBytes
}

// Pomocna funkcija proverava da li se dati kljuc nalazi u bloomfilteru od sstabele.
// Vraca true ako se mozda nalazi. Vraca false ako se sigurno ne nalazi
func (sstable *SSTableInstance) checkBloomfilter(key string) bool {
	var bfBytes []byte
	if !sstable.isSingleFile {
		bfFile, err := os.Open(sstable.filename + "/bloomfilter.bin")
		if err != nil {
			log.Fatal(err)
		}

		endOffset, _ := bfFile.Seek(0, 2)
		bfBytes = make([]byte, endOffset)

		bfFile.Seek(0, 0)
		bfFile.Read(bfBytes)

		bfFile.Close()
	} else {
		bfFile, err := os.Open(sstable.filename)
		if err != nil {
			log.Fatal(err)
		}

		bfFile.Seek(metaBloomfilterBegin, 0)

		placeholder := bf.NewBloomFilter(config.GlobalConfig.BFExpectedElements, config.GlobalConfig.BFFalsePositiveRate)
		placeholderBytes := placeholder.Serialize()
		bfBytes = make([]byte, len(placeholderBytes))
		bfFile.Read(bfBytes)
	}
	bloom := bf.Deserialize(bfBytes)
	return bloom.Read([]byte(key))
}

// Funkcija pretrazuje sve SSTabele za dati kljuc, trazi most recent pojavu kljuca.
// Funkcija vraca Record i true ako je record sa unetim kljucem pronadjen. U suprotnom, vraca prazan Record i false
func SSTableGet(key string) (rec.Record, bool) {
	// 1) Procitaj folder sa svim SSTabelama da bi dobili pathove
	sstablePaths, err := os.ReadDir(sstableFolderPath)
	if err != nil {
		log.Fatal(err)
	}

	// 2) For each petlja
	for _, path := range sstablePaths {
		currentInstance := OpenSSTable(path.Name())

		if currentInstance.checkBloomfilter(key) { // Kljuc se mozda nalazi u sstabeli
			//  6) Proveriti da li je kljuc u opsegu
			if !currentInstance.checkIfInRange(key) { // Ako nije, nastaviti dalje
				continue
			}
			//	7) Pozicionirati se na najblizi element u summary
			var previousIndexOffset uint64
			var lastStatus byte
			currentInstance.currentOffset = 0
			for {
				offset, keyStatus := currentInstance.readSummaryRecordForKey(key)
				lastStatus = keyStatus
				if keyStatus == 0 {
					previousIndexOffset = offset
				} else if keyStatus == 1 {
					previousIndexOffset = offset
					break
				} else {
					lastStatus = keyStatus
					break
				}
			}

			if lastStatus == 3 { // Ako je ipak dosao do kraja zbog nekog razloga
				continue
			}
			//	8) Otvoriti/Preskociti na index deo na datom offsetu
			var previousDataOffset uint64
			currentInstance.currentOffset = int64(previousIndexOffset)
			//	9) Pozicionirati se na najblizi element u index
			for {
				offset, keyStatus := currentInstance.readIndexRecordForKey(key)
				lastStatus = keyStatus
				if keyStatus == 0 {
					previousDataOffset = offset
				} else if keyStatus == 1 {
					previousDataOffset = offset
					break
				} else {
					lastStatus = keyStatus
					break
				}
			}

			if lastStatus == 3 {
				continue
			}
			//	10) Otvoriti/Preskociti na data deo na datom index-u
			var previousRecord rec.Record
			currentInstance.currentOffset = int64(previousDataOffset)
			//	11) Citati record po record iz data dela
			for {
				record, status := currentInstance.ReadRecord()

				if !status {
					break
				}

				if key < record.Key {
					break
				}
				previousRecord = record
			}
			//	12a) Pronadjen record -> return Record.Deserialize, true
			if previousRecord.Key == key {
				return previousRecord, true
			}
			//	12b) Nije pronadjen record -> continue
			continue
		} else { // Kljuc se definitivno ne nalazi u sstabeli
			continue
		}
	}
	return rec.Record{Key: "", Value: make([]byte, 0), Timestamp: 0, Tombstone: false}, false
}

func (sstable *SSTableInstance) checkIfInRange(key string) bool {
	var file *os.File
	var err error

	if sstable.isSingleFile {
		file, err = os.OpenFile(sstable.filename, os.O_RDONLY, 0777)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		file, err = os.OpenFile(sstable.filename+"/index.bin", os.O_RDONLY, 0777)
		if err != nil {
			log.Fatal(err)
		}
	}

	file.Seek(sstable.indexBeginOffset, 0)
	firstKeyLengthBytes := make([]byte, 8)
	file.Read(firstKeyLengthBytes)
	firstKeyLength := binary.LittleEndian.Uint64(firstKeyLengthBytes)

	firstKeyBytes := make([]byte, firstKeyLength)
	file.Read(firstKeyBytes)
	firstKeyFromIndex := string(firstKeyBytes)

	file.Seek(sstable.indexLastElementOffset, 0)
	secondKeyLengthBytes := make([]byte, 8)
	file.Read(secondKeyLengthBytes)
	secondKeyLength := binary.LittleEndian.Uint64(secondKeyLengthBytes)

	secondKeyBytes := make([]byte, secondKeyLength)
	file.Read(secondKeyBytes)
	secondKeyFromIndex := string(secondKeyBytes)

	return key >= firstKeyFromIndex && key <= secondKeyFromIndex
}

// Pomocna funkcija povecava sve indekse SSTabeli na nekom LSM nivou za 1
func updateSSTableNames(lsmLevel int) {
	pathsToChange := make([]string, 0)
	pathIsDir := make([]bool, 0)
	// Ucitaj imena svih sstabela
	sstablePaths, err := os.ReadDir(sstableFolderPath)
	if err != nil {
		log.Fatal(err)
	}

	for _, path := range sstablePaths {
		// podeli "XXXXsstableYYYY[.bin]"
		splitPath := strings.Split(path.Name(), "sstables")
		if splitPath[0] != "0001" {
			// ako nije LSM nivo 1, ne preimenuj vise nista
			break
		}
		// dodaj ga u listu za promenu naziva ako jeste
		pathsToChange = append(pathsToChange, path.Name())
		pathIsDir = append(pathIsDir, path.IsDir())
	}

	// iteriraj u obrnutom redosledu (da ne bi doslo do errora)
	for i := len(pathsToChange) - 1; i >= 0; i-- {
		// podeli "XXXXsstableYYYY[.bin]"
		splitPath := strings.Split(pathsToChange[i], "sstables")
		// uzmi YYYY
		oldIndex := splitPath[1][:4]
		// pretvori indeks u broj i povecaj ga za jedan
		oldIndexNumber, _ := strconv.Atoi(oldIndex)
		newIndexNumber := oldIndexNumber + 1
		newIndex := fmt.Sprintf("%04d", newIndexNumber)
		// napravi novi ceo path sa novim indeksom
		newPath := sstableFolderPath + "/0001sstable" + newIndex
		if !pathIsDir[i] {
			// dodaj jos ".bin" ako je bio u pitanju singleFile
			newPath += ".bin"
		}

		// preimenuj ga
		err := os.Rename(sstableFolderPath+"/"+pathsToChange[i], newPath)
		if err != nil {
			log.Println(err)
		}
	}
}

// Pomocna funkcija pravi novu SSTabelu koja je takodje i najnovija, 0001sstable0001
func makeNewSSTableInstance() *SSTableInstance {
	var compression bool
	config.Init()
	if config.GlobalConfig.Compression == "yes" {
		compression = true
	} else {
		compression = false
	}

	if config.GlobalConfig.SSTFiles == "one" {
		sstablePath := newSSTablePath + ".bin"
		return &SSTableInstance{filename: sstablePath, currentOffset: 0, isSingleFile: true, isCompressed: compression,
			bloomfilterBeginOffset: metaBloomfilterBegin}
	} else { // SSTFiles == "many"
		return &SSTableInstance{filename: newSSTablePath, currentOffset: 0, dataBeginOffset: 0, isSingleFile: false, isCompressed: compression,
			bloomfilterBeginOffset: 0, indexBeginOffset: 0, summaryBeginOffset: 0, merkleBeginOffset: 0}
	}
}

// Funkcija pravi novu SSTabelu koja sadrzi prosledjen niz Recorda
func CreateNewSSTable(records []rec.Record) {
	newSSTableInstance := makeNewSSTableInstance()
	updateSSTableNames(1)
	newCreator := SSTableCreator{Instance: *newSSTableInstance, currentIndexNumber: 0, currentSummaryNumber: 0}
	if config.GlobalConfig.SSTFiles == "one" {

		metaBytes := make([]byte, 57)
		file, err := os.OpenFile(newCreator.Instance.filename, os.O_CREATE, 0777)
		if err != nil {
			log.Fatal(err)
		}
		_, err = file.Write(metaBytes)
		if err != nil {
			log.Fatal(err)
		}
		placeholderBloom := bf.NewBloomFilter(config.GlobalConfig.BFExpectedElements, config.GlobalConfig.BFFalsePositiveRate)
		file.Write(placeholderBloom.Serialize())
		dataOffset, _ := file.Seek(0, 2)
		newCreator.Instance.dataBeginOffset = dataOffset
		file.Close()

		for _, record := range records {
			newCreator.WriteRecord(record)
		}
		newCreator.createIndex()
		newCreator.createSummary()
		newCreator.createMerkle(records)
	} else {
		os.Mkdir(newCreator.Instance.filename, 0777)
		for index, record := range records {
			if index == 0 || index == len(records)-1 {
				newCreator.currentIndexNumber = FirstOrLastElement
				newCreator.currentSummaryNumber = FirstOrLastElement
			}
			newCreator.WriteRecord(record)
		}

		merkleTree := merk.MakeMerkleTree(records)
		merkleBytes := merkleTree.Serialize()

		merkleFile, err := os.OpenFile(newCreator.Instance.filename+"/merkle.bin", os.O_WRONLY|os.O_CREATE, 0777)
		if err != nil {
			log.Fatal(err)
		}
		merkleFile.Write(merkleBytes)
		merkleFile.Close()

	}
	newCreator.createMetadata()
}

// Funkcija otvara SSTabelu sa prosledjenim file pathom. Vraca otvorenu instancu SSTabele
func OpenSSTable(filename string) SSTableInstance {
	var compression bool = false
	var singlefile bool = false
	var dataOffset int64 = 0
	var indexOffset int64 = 0
	var indexLastElemOffset int64 = 0
	var summaryOffset int64 = 0
	var summaryLastElemOffset int64 = 0
	var merkleOffset int64 = 0
	var bloomOffset int64 = 0
	actualPath := sstableFolderPath + "/" + filename
	_, err := os.Stat(actualPath)
	if err == os.ErrNotExist {
		actualPath += ".bin"
		singlefile = true
	}
	_, err = os.Stat(actualPath)
	if err == os.ErrNotExist {
		log.Fatal("SSTable does not exist! Path: " + actualPath)
	}

	if singlefile {
		file, err := os.OpenFile(actualPath, os.O_RDONLY, 0777)
		if err != nil {
			log.Fatal(err)
		}
		metaBytes := make([]byte, singleFileMetaLength)
		file.Read(metaBytes)
		if metaBytes[0] == 255 {
			compression = true
		}
		bloomOffset = int64(binary.LittleEndian.Uint64(metaBytes[1:9]))
		dataOffset = int64(binary.LittleEndian.Uint64(metaBytes[9:17]))
		indexOffset = int64(binary.LittleEndian.Uint64(metaBytes[17:25]))
		indexLastElemOffset = int64(binary.LittleEndian.Uint64(metaBytes[25:33]))
		summaryOffset = int64(binary.LittleEndian.Uint64(metaBytes[33:41]))
		summaryLastElemOffset = int64(binary.LittleEndian.Uint64(metaBytes[41:49]))
		merkleOffset = int64(binary.LittleEndian.Uint64(metaBytes[49:]))
	} else {
		file, err := os.OpenFile(actualPath+"/meta.bin", os.O_RDONLY, 0777)
		if err != nil {
			log.Fatal(err)
		}
		metaBytes := make([]byte, 17)
		file.Read(metaBytes)
		if metaBytes[0] == 255 {
			compression = true
		}
		indexLastElemOffset = int64(binary.LittleEndian.Uint64(metaBytes[1:9]))
		summaryLastElemOffset = int64(binary.LittleEndian.Uint64(metaBytes[9:]))
	}
	return SSTableInstance{filename: actualPath, currentOffset: 0, isSingleFile: singlefile, isCompressed: compression,
		indexBeginOffset: indexOffset, indexLastElementOffset: indexLastElemOffset, summaryBeginOffset: summaryOffset, summaryLastElementOffset: summaryLastElemOffset,
		dataBeginOffset: dataOffset, merkleBeginOffset: merkleOffset, bloomfilterBeginOffset: bloomOffset}
}

// Pomocna funkcija koja cita jednu variable-encoded vrednost
func (sstable *SSTableInstance) readValue(file *os.File) []byte {
	claimer := make([]byte, 1)
	buffer := make([]byte, 0)
	_, err := file.Read(claimer)
	if err != nil {
		log.Fatal(err)
	}
	buffer = append(buffer, claimer...)
	for (128 & claimer[0]) == 128 {
		_, err := file.Read(claimer)
		if err != nil {
			log.Fatal(err)
		}
		buffer = append(buffer, claimer...)
	}
	sstable.currentOffset += int64(len(buffer))
	return buffer
}

// Funkcija cita sledeci record upisan u SSTabelu, deserijalizuje ga i vraca ga kao povratnu vrednost.
// Pamti se dokle se stiglo sa citanjem u SSTabeli.
// Ako nema sta da se procita, vraca se prazan record i false
func (sstable *SSTableInstance) ReadRecord() (rec.Record, bool) {
	var file *os.File
	var err error
	var crcActual uint64
	var timestampActual uint64
	var tombstoneActual bool
	var keyLengthActual uint64
	var valueLengthActual uint64 = 0
	var key string
	var value []byte

	if sstable.isSingleFile {
		dataPath := sstable.filename
		file, err = os.OpenFile(dataPath, os.O_RDONLY, 0777)
		if err != nil {
			log.Fatal(err)
		}
		file.Seek(sstable.dataBeginOffset+sstable.currentOffset, 0)
	} else {
		dataPath := sstable.filename + "/data.bin"
		file, err = os.OpenFile(dataPath, os.O_RDONLY, 0777)
		if err != nil {
			log.Fatal(err)
		}
		file.Seek(sstable.currentOffset, 0)
	}

	fileInfo, _ := file.Stat()
	if (!sstable.isSingleFile && sstable.currentOffset == fileInfo.Size()) || (sstable.isSingleFile && sstable.dataBeginOffset+sstable.currentOffset >= sstable.indexBeginOffset) {
		return rec.Record{}, false
	}

	crcBytes := sstable.readValue(file)
	crcActual, _ = binary.Uvarint(crcBytes)

	timestampBytes := sstable.readValue(file)
	timestampActual, _ = binary.Uvarint(timestampBytes)

	tombstoneByte := make([]byte, 1)
	file.Read(tombstoneByte)
	if tombstoneByte[0] == 255 {
		tombstoneActual = true
	} else {
		tombstoneActual = false
	}
	sstable.currentOffset += 1

	keyLengthBytes := sstable.readValue(file)
	keyLengthActual, _ = binary.Uvarint(keyLengthBytes)

	if !tombstoneActual {
		valueLengthBytes := sstable.readValue(file)
		valueLengthActual, _ = binary.Uvarint(valueLengthBytes)
	}

	var keyBytes []byte
	if sstable.isCompressed {
		dict := compress.NewCompressionDict()
		dict.ReadFromFile()
		keyBytes = make([]byte, keyLengthActual)
		file.Read(keyBytes)
		key, _ = dict.GetKey(binary.LittleEndian.Uint64(keyBytes))

	} else {
		keyBytes = make([]byte, keyLengthActual)
		file.Read(keyBytes)
		key = string(keyBytes)
	}
	sstable.currentOffset += int64(len(keyBytes))

	if !tombstoneActual {
		value = make([]byte, valueLengthActual)
		file.Read(value)
		sstable.currentOffset += int64(len(value))
	}
	recordActual := rec.Record{Key: key, Value: value, Timestamp: int64(timestampActual), Tombstone: tombstoneActual}
	crcCurrent := CRC32(rec.RecToBytes(recordActual))
	if crcCurrent != uint32(crcActual) {
		fmt.Println("Oprez! Moguce je da dobijena vrednost nije validna!")
	}
	return recordActual, true
}

// Pomocna funkcija cita index red po red i trazi kljuc
// Vraca offset procitanog rekorda i kod vezan za kljuc
// 0 - procitani kljuc je manji od trazenog
// 1 - procitani kljuc jeste trazeni
// 2 - procitani kljuc je veci od trazenog
// 3 - error kod
func (sstable *SSTableInstance) readIndexRecordForKey(key string) (uint64, byte) {
	var file *os.File
	var err error

	if sstable.isSingleFile {
		file, err = os.OpenFile(sstable.filename, os.O_RDONLY, 0777)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		file, err = os.OpenFile(sstable.filename+"/index.bin", os.O_RDONLY, 0777)
		if err != nil {
			log.Fatal(err)
		}
	}
	defer file.Close()

	fileStats, _ := file.Stat()
	if (sstable.isSingleFile && sstable.indexBeginOffset+sstable.currentOffset >= sstable.summaryBeginOffset) || (!sstable.isSingleFile && sstable.indexBeginOffset+sstable.currentOffset >= fileStats.Size()) {
		return 0, 3
	}

	file.Seek(sstable.indexBeginOffset+sstable.currentOffset, 0)

	keyLengthBytes := make([]byte, 8)
	file.Read(keyLengthBytes)
	keyLength := binary.LittleEndian.Uint64(keyLengthBytes)

	keyBytes := make([]byte, keyLength)
	file.Read(keyBytes)
	recordKey := string(keyBytes)

	offsetBytes := make([]byte, 8)
	file.Read(offsetBytes)
	offset := binary.LittleEndian.Uint64(offsetBytes)

	if recordKey < key {
		return offset, 0
	}

	if recordKey == key {
		return offset, 1
	}

	return offset, 2
}

func (sstable *SSTableInstance) readSummaryRecordForKey(key string) (uint64, byte) {
	var file *os.File
	var err error

	if sstable.isSingleFile {
		file, err = os.OpenFile(sstable.filename, os.O_RDONLY, 0777)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		file, err = os.OpenFile(sstable.filename+"/summary.bin", os.O_RDONLY, 0777)
		if err != nil {
			log.Fatal(err)
		}
	}
	defer file.Close()

	fileStats, _ := file.Stat()
	if (sstable.isSingleFile && sstable.summaryBeginOffset+sstable.currentOffset >= sstable.merkleBeginOffset) || (!sstable.isSingleFile && sstable.summaryBeginOffset+sstable.currentOffset >= fileStats.Size()) {
		return 0, 3
	}

	file.Seek(sstable.summaryBeginOffset+sstable.currentOffset, 0)

	keyLengthBytes := make([]byte, 8)
	file.Read(keyLengthBytes)
	keyLength := binary.LittleEndian.Uint64(keyLengthBytes)

	keyBytes := make([]byte, keyLength)
	file.Read(keyBytes)
	recordKey := string(keyBytes)

	offsetBytes := make([]byte, 8)
	file.Read(offsetBytes)
	offset := binary.LittleEndian.Uint64(offsetBytes)

	if recordKey < key {
		return offset, 0
	}

	if recordKey == key {
		return offset, 1
	}

	return offset, 2
}

func (sstable *SSTableInstance) readIndexRecord() (string, bool) {
	var file *os.File
	var err error

	if sstable.isSingleFile {
		file, err = os.OpenFile(sstable.filename, os.O_RDONLY, 0777)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		file, err = os.OpenFile(sstable.filename+"/index.bin", os.O_RDONLY, 0777)
		if err != nil {
			log.Fatal(err)
		}
	}
	defer file.Close()

	fileStats, _ := file.Stat()
	if (sstable.isSingleFile && sstable.indexBeginOffset+sstable.currentOffset >= sstable.summaryBeginOffset) || (!sstable.isSingleFile && sstable.indexBeginOffset+sstable.currentOffset >= fileStats.Size()) {
		return "", false
	}

	file.Seek(sstable.indexBeginOffset+sstable.currentOffset, 0)

	keyLengthBytes := make([]byte, 8)
	file.Read(keyLengthBytes)
	keyLength := binary.LittleEndian.Uint64(keyLengthBytes)

	keyBytes := make([]byte, keyLength)
	file.Read(keyBytes)
	key := string(keyBytes)

	offsetBytes := make([]byte, 8)
	file.Read(offsetBytes)

	sstable.currentOffset += 8 + int64(len(keyBytes)) + 8

	return key, true
}

func (sstable *SSTableCreator) createSummary() {
	config.Init()

	file, err := os.OpenFile(sstable.Instance.filename, os.O_RDWR, 0777)
	if err != nil {
		log.Fatal(err)
	}
	sstable.Instance.summaryBeginOffset, _ = file.Seek(0, 2)
	file.Close()

	var previousKey string
	sstable.Instance.currentOffset = 0
	sstable.currentIndexOffset = 0
	sstable.currentSummaryNumber = 0
	isFirstOrLast := true
	for {
		offsetAtBeginning := sstable.Instance.currentOffset
		indexKey, isRead := sstable.Instance.readIndexRecord()
		offsetAtEnd := sstable.Instance.currentOffset

		if !isRead {
			finalKey := previousKey

			keyLength := uint64(len(finalKey))
			keyLengthBytes := make([]byte, 8)
			binary.LittleEndian.PutUint64(keyLengthBytes, keyLength)

			offsetBytes := make([]byte, 8)
			binary.LittleEndian.PutUint64(offsetBytes, sstable.currentIndexOffset)

			summaryBytes := make([]byte, 0)
			summaryBytes = append(summaryBytes, keyLengthBytes...)
			summaryBytes = append(summaryBytes, []byte(finalKey)...)
			summaryBytes = append(summaryBytes, offsetBytes...)

			file, err := os.OpenFile(sstable.Instance.filename, os.O_RDWR, 0777)
			if err != nil {
				log.Fatal(err)
			}
			file.Seek(0, 2)
			file.Write(summaryBytes)
			file.Close()
			break
		}

		if isFirstOrLast || sstable.currentSummaryNumber == config.DEGREE_OF_DILUTION-1 {
			finalKeyBytes := []byte(indexKey)
			keyLength := uint64(len(indexKey))

			keyLengthBytes := make([]byte, 8)
			binary.LittleEndian.PutUint64(keyLengthBytes, keyLength)

			offsetBytes := make([]byte, 8)
			binary.LittleEndian.PutUint64(offsetBytes, sstable.currentIndexOffset)

			summaryBytes := make([]byte, 0)
			summaryBytes = append(summaryBytes, keyLengthBytes...)
			summaryBytes = append(summaryBytes, finalKeyBytes...)
			summaryBytes = append(summaryBytes, offsetBytes...)

			file, err := os.OpenFile(sstable.Instance.filename, os.O_RDWR, 0777)
			if err != nil {
				log.Fatal(err)
			}
			file.Seek(0, 2)
			file.Write(summaryBytes)
			file.Close()
			sstable.currentSummaryNumber = 0
		} else {
			sstable.currentSummaryNumber += 1
		}

		sstable.currentIndexOffset += uint64(offsetAtEnd) - uint64(offsetAtBeginning)
		previousKey = indexKey
	}
}

func (sstable *SSTableCreator) createIndex() {
	config.Init()

	file, err := os.OpenFile(sstable.Instance.filename, os.O_RDWR, 0777)
	if err != nil {
		log.Fatal(err)
	}
	sstable.Instance.indexBeginOffset, _ = file.Seek(0, 2)
	file.Close()

	sstable.Instance.currentOffset = 0
	sstable.currentDataOffset = 0
	sstable.currentIndexNumber = 0
	recordsToRead := true
	isFirstOrLast := true

	var previousRecord rec.Record
	for recordsToRead {

		offsetAtBeginning := sstable.Instance.currentOffset
		record, isRead := sstable.Instance.ReadRecord()
		offsetAtEnd := sstable.Instance.currentOffset

		if !isRead {
			finalKey := previousRecord.Key

			keyLength := uint64(len(finalKey))
			keyLengthBytes := make([]byte, 8)
			binary.LittleEndian.PutUint64(keyLengthBytes, keyLength)

			offsetBytes := make([]byte, 8)
			binary.LittleEndian.PutUint64(offsetBytes, sstable.currentDataOffset)

			indexBytes := make([]byte, 0)
			indexBytes = append(indexBytes, keyLengthBytes...)
			indexBytes = append(indexBytes, []byte(finalKey)...)
			indexBytes = append(indexBytes, offsetBytes...)

			file, err := os.OpenFile(sstable.Instance.filename, os.O_RDWR, 0777)
			if err != nil {
				log.Fatal(err)
			}
			file.Seek(0, 2)
			file.Write(indexBytes)
			file.Close()
			break
		}

		if isFirstOrLast || sstable.currentIndexNumber == config.DEGREE_OF_DILUTION-1 {
			isFirstOrLast = false
			finalKey := record.Key

			keyLength := uint64(len(finalKey))
			keyLengthBytes := make([]byte, 8)
			binary.LittleEndian.PutUint64(keyLengthBytes, keyLength)

			offsetBytes := make([]byte, 8)
			binary.LittleEndian.PutUint64(offsetBytes, sstable.currentDataOffset)

			indexBytes := make([]byte, 0)
			indexBytes = append(indexBytes, keyLengthBytes...)
			indexBytes = append(indexBytes, []byte(finalKey)...)
			indexBytes = append(indexBytes, offsetBytes...)

			file, err := os.OpenFile(sstable.Instance.filename, os.O_RDWR, 0777)
			if err != nil {
				log.Fatal(err)
			}
			file.Seek(0, 2)
			file.Write(indexBytes)
			file.Close()

			sstable.currentIndexNumber = 0
		} else {
			sstable.currentIndexNumber += 1
		}

		sstable.currentDataOffset += uint64(offsetAtEnd) - uint64(offsetAtBeginning)
		previousRecord = record
	}
}

func (sstable *SSTableCreator) createMerkle(records []rec.Record) {
	file, err := os.OpenFile(sstable.Instance.filename, os.O_RDWR, 0777)
	if err != nil {
		log.Fatal(err)
	}
	file.Seek(0, 2)
	merkleTree := merk.MakeMerkleTree(records)
	file.Write(merkleTree.Serialize())
	file.Close()
}

func (sstable *SSTableCreator) createMetadata() {

	if sstable.Instance.isSingleFile {
		file, err := os.OpenFile(sstable.Instance.filename, os.O_RDWR, 0777)
		if err != nil {
			log.Fatal(err)
		}
		file.Seek(0, 0)
		defer file.Close()

		var compressionBytes byte
		if sstable.Instance.isCompressed {
			compressionBytes = 255
		} else {
			compressionBytes = 0
		}

		bloomfilterOffsetBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(bloomfilterOffsetBytes, uint64(sstable.Instance.bloomfilterBeginOffset))
		dataOffsetBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(dataOffsetBytes, uint64(sstable.Instance.dataBeginOffset))
		indexOffsetBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(indexOffsetBytes, uint64(sstable.Instance.indexBeginOffset))
		lastIndexOffsetBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(lastIndexOffsetBytes, uint64(sstable.Instance.indexLastElementOffset))
		summaryOffsetBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(summaryOffsetBytes, uint64(sstable.Instance.summaryBeginOffset))
		lastSummaryOffsetBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(lastSummaryOffsetBytes, uint64(sstable.Instance.summaryLastElementOffset))
		merkleOffsetBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(merkleOffsetBytes, uint64(sstable.Instance.merkleBeginOffset))

		metaBytes := make([]byte, 0)
		metaBytes = append(metaBytes, compressionBytes)
		metaBytes = append(metaBytes, bloomfilterOffsetBytes...)
		metaBytes = append(metaBytes, dataOffsetBytes...)
		metaBytes = append(metaBytes, indexOffsetBytes...)
		metaBytes = append(metaBytes, lastIndexOffsetBytes...)
		metaBytes = append(metaBytes, summaryOffsetBytes...)
		metaBytes = append(metaBytes, lastSummaryOffsetBytes...)
		metaBytes = append(metaBytes, merkleOffsetBytes...)

		file.Write(metaBytes)
	} else {
		file, err := os.OpenFile(sstable.Instance.filename+"/meta.bin", os.O_RDWR|os.O_CREATE, 0777)
		if err != nil {
			log.Fatal(err)
		}

		var compressionBytes byte
		if sstable.Instance.isCompressed {
			compressionBytes = 255
		} else {
			compressionBytes = 0
		}

		lastIndexOffsetBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(lastIndexOffsetBytes, uint64(sstable.Instance.indexLastElementOffset))
		lastSummaryOffsetBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(lastSummaryOffsetBytes, uint64(sstable.Instance.summaryLastElementOffset))

		metaBytes := make([]byte, 0)
		metaBytes = append(metaBytes, compressionBytes)
		metaBytes = append(metaBytes, lastIndexOffsetBytes...)
		metaBytes = append(metaBytes, lastSummaryOffsetBytes...)

		file.Write(metaBytes)
		file.Close()
	}

}

// Funkcija upisuje prosledjen record na sledece mesto u SSTabeli. Koristice se za LSM
// AKO JE SINGLE FILE SSTABELA, DODAJE SE SAMO U BLOOMFILTER I DATA DEO!!!
// AKO JE MULTI FILE SSTABELA, DODAJE SE U SVE!!!
func (sstable *SSTableCreator) WriteRecord(record rec.Record) {
	config.Init()
	dict := compress.NewCompressionDict()
	dict.ReadFromFile()

	if sstable.Instance.isCompressed {
		dict.Write(record.Key)
		dict.WriteToFile()
	}

	if sstable.Instance.isSingleFile {
		file, err := os.OpenFile(sstable.Instance.filename, os.O_RDWR|os.O_CREATE, 0777)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()
		{
			file.Seek(sstable.Instance.bloomfilterBeginOffset, 0)
			// read bloomfilter bytes
			placeholder := bf.NewBloomFilter(config.GlobalConfig.BFExpectedElements, config.GlobalConfig.BFFalsePositiveRate)
			placeholderBytes := placeholder.Serialize()
			bfBytes := make([]byte, len(placeholderBytes))
			file.Read(bfBytes)
			// deserialize bloomfilter
			bloom := bf.Deserialize(bfBytes)
			// add element to bloomfilter
			bloom.Add([]byte(record.Key))
			// serialize bloomfilter
			newBfBytes := bloom.Serialize()
			file.Seek(sstable.Instance.bloomfilterBeginOffset, 0)
			// write the bloomfilter back in
			file.Write(newBfBytes)
		}
		// Data
		{
			file.Seek(0, 2)
			file.Write(RecordToSSTableRecord(record))
		}
	} else {
		// Bloomfilter
		{
			var bloomfilter *bf.BloomFilter
			bfPath := sstable.Instance.filename + "/bloomfilter.bin"

			bfBytes, err := os.ReadFile(bfPath)
			if os.IsNotExist(err) {
				bloomfilter = bf.NewBloomFilter(config.GlobalConfig.BFExpectedElements, config.GlobalConfig.BFFalsePositiveRate)
			} else {
				bloomfilter = bf.Deserialize(bfBytes)
			}
			bloomfilter.Add([]byte(record.Key))
			bfBytes = bloomfilter.Serialize()
			os.Remove(bfPath)
			bfFile, err := os.Create(bfPath)
			if err != nil {
				log.Fatal(err)
			}
			_, err = bfFile.Write(bfBytes)
			if err != nil {
				log.Fatal(err)
			}
			bfFile.Close()
		}

		// Summary
		{

			if sstable.currentSummaryNumber == (config.DEGREE_OF_DILUTION*config.DEGREE_OF_DILUTION-1) || sstable.currentSummaryNumber == FirstOrLastElement {
				summaryPath := sstable.Instance.filename + "/summary.bin"
				summaryFile, err := os.OpenFile(summaryPath, os.O_APPEND|os.O_CREATE, 0777)
				if err != nil {
					log.Fatal(err)
				}

				finalKeyBytes := []byte(record.Key)
				keyLength := uint64(len(record.Key))

				keyLengthBytes := make([]byte, 8)
				binary.LittleEndian.PutUint64(keyLengthBytes, keyLength)

				offsetBytes := make([]byte, 8)
				binary.LittleEndian.PutUint64(offsetBytes, sstable.currentIndexOffset)

				summaryBytes := make([]byte, 0)
				summaryBytes = append(summaryBytes, keyLengthBytes...)
				summaryBytes = append(summaryBytes, finalKeyBytes...)
				summaryBytes = append(summaryBytes, offsetBytes...)

				summaryFile.Write(summaryBytes)
				summaryFile.Close()

				sstable.currentSummaryNumber = 0
			}
		}

		// Index
		{
			if sstable.currentIndexNumber == config.DEGREE_OF_DILUTION-1 || sstable.currentIndexNumber == FirstOrLastElement {
				indexPath := sstable.Instance.filename + "/index.bin"
				indexFile, err := os.OpenFile(indexPath, os.O_APPEND|os.O_CREATE, 0777)
				if err != nil {
					log.Fatal(err)
				}

				finalKey := record.Key

				keyLength := uint64(len(finalKey))
				keyLengthBytes := make([]byte, 8)
				binary.LittleEndian.PutUint64(keyLengthBytes, keyLength)

				offsetBytes := make([]byte, 8)
				binary.LittleEndian.PutUint64(offsetBytes, sstable.currentDataOffset)

				indexBytes := make([]byte, 0)
				indexBytes = append(indexBytes, keyLengthBytes...)
				indexBytes = append(indexBytes, []byte(finalKey)...)
				indexBytes = append(indexBytes, offsetBytes...)

				indexFile.Write(indexBytes)
				indexFile.Close()

				sstable.currentIndexOffset += uint64(len(indexBytes))
				sstable.currentIndexNumber = 0
			}
		}

		// Data
		{
			dataPath := sstable.Instance.filename + "/data.bin"
			dataFile, err := os.OpenFile(dataPath, os.O_APPEND|os.O_CREATE, 0777)
			if err != nil {
				log.Fatal(err)
			}
			recordBytes := RecordToSSTableRecord(record)
			dataFile.Write(recordBytes)
			dataFile.Close()
			sstable.currentDataOffset += uint64(len(recordBytes))
			sstable.currentIndexNumber += 1
			sstable.currentSummaryNumber += 1
		}
	}
}
