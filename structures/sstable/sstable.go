package sstable

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"log"
	"os"

	"github.com/IvanaaXD/NASP/app/config"
	bf "github.com/IvanaaXD/NASP/structures/bloom-filter"
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
}

// Notes
// Potencijalno moguce napraviti da RecordToSSTableRecord bude samo jedna funkcija
// Potencijalno dobro staviti firstSummaryElement, lastSummaryElement, firstIndexElement, lastIndexElement u metadata
// mozda jos neke druge stvari kao dataBegin, merkleBegin itd.
// Potencijalno dobro da SSTableInstance cuva vise offsetova

const tombstoneTrue byte = 255
const tombstoneFalse byte = 0
const sstableFolderPath string = "./resources/sstables"
const newSSTablePath string = "./resources/sstables/0001sstable0001"
const FirstOrLastElement uint32 = 9999999
const singleFilemetaLength uint16 = 57

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// Funkcija pretvara Record u zapis za SSTabelu BEZ KOMPRESIJE (public funkcija zbog LSM)
func RecordToSSTableRecord(inputRecord rec.Record) []byte {
	ssRecordBytes := make([]byte, 0)
	normalRecordBytes := rec.RecToBytes(inputRecord)
	crcValue := CRC32(normalRecordBytes)
	crcValueBytes := make([]byte, 4)
	writtenBytes := binary.PutUvarint(crcValueBytes, uint64(crcValue))
	ssRecordBytes = append(ssRecordBytes, crcValueBytes[:writtenBytes]...)

	timestampBytes := make([]byte, 8)
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
	writtenBytes = binary.PutUvarint(keySizeBytes, uint64(len(inputRecord.Key)))
	ssRecordBytes = append(ssRecordBytes, keySizeBytes[:writtenBytes]...)

	if !inputRecord.Tombstone {
		// Ako nije obrisan, serijalizovati i value size
		valueSizeBytes := make([]byte, 8)
		writtenBytes = binary.PutUvarint(valueSizeBytes, uint64(len(inputRecord.Value)))
		ssRecordBytes = append(ssRecordBytes, valueSizeBytes[:writtenBytes]...)
	}

	keyBytes := []byte(inputRecord.Key)
	ssRecordBytes = append(ssRecordBytes, keyBytes...)

	if !inputRecord.Tombstone {
		// Ako nije obrisan, serijalizovati i value
		ssRecordBytes = append(ssRecordBytes, inputRecord.Value...)
	}
	return ssRecordBytes
}

// Funkcija pretvara Record u zapis za SSTabelu SA KOMPRESIJOM (public funkcija zbog LSM)
func RecordToCompressedSSTableRecord(inputRecord rec.Record) []byte {
	// TO-DO: kompresovan kljuc i to, ali mozda moguce i u prethodnoj funkciji
	return make([]byte, 0)
}

// Pomocna funkcija deserijalizuje bloomfilter od datih bajtova i proverava da li se dati kljuc nalazi u njemu.
// Vraca true ako se mozda nalazi. Vraca false ako se sigurno ne nalazi
func loadBfAndCheck(serializedBF []byte, key string) bool {
	bloom := bf.Deserialize(serializedBF)
	return bloom.Read([]byte(key))
}

// Funkcija pretrazuje sve SSTabele za dati kljuc, trazi most recent pojavu kljuca.
// Funkcija vraca Record i true ako je record sa unetim kljucem pronadjen. U suprotnom, vraca prazan Record i false
func SSTableGet(key string) (rec.Record, bool) {
	// TO-DO: ceo get za ss tabelu, ali to moze zadnje
	// 1) Procitaj folder sa svim SSTabelama da bi dobili pathove
	sstablePaths, err := os.ReadDir(sstableFolderPath)
	if err != nil {
		log.Fatal(err)
	}

	// 2) For each petlja
	for _, path := range sstablePaths {
		// 	3) Proveriti da li je SSTabela u jednom fajlu ili u vise fajlova (fajl vs folder)
		if path.IsDir() {
			//	4) Otvoriti bloomfilter SSTabele
			bfFile, err := os.Open(sstableFolderPath + "/" + path.Name() + "/bloomfilter.bin")
			if err != nil {
				log.Fatal(err)
			}
			//	5) Proveriti bloomfilter SSTabele da li je kljuc mozda u SSTabeli
			endOffset, _ := bfFile.Seek(0, 2)
			bfBytes := make([]byte, endOffset)
			bfFile.Seek(0, 0)
			bfFile.Read(bfBytes)

			if loadBfAndCheck(bfBytes, key) { // 6a) Kljuc mozda jeste u SSTabeli
				//	7) Pozicionirati se na najblizi element u summary
				//	8) Otvoriti/Preskociti na index deo na datom offsetu
				//	9) Pozicionirati se na najblizi element u index
				//	10) Otvoriti/Preskociti na data deo na datom index-u
				//	11) Citati record po record iz data dela
				//	12a) Pronadjen record -> return Record.Deserialize, true
				//	12b) Nije pronadjen record -> continue
			} else { //	6b) Kljuc definitivno nije u SSTabeli -> continue;
				continue
			}

		} else {
			//	4) Otvoriti SSTabelu
			file, err := os.Open(sstableFolderPath)
			if err != nil {
				log.Fatal(err)
			}
			//	4a) Pozicioniraj se na bloomfilter
			file.Seek(0, 0)
			//	5) Proveriti bloomfilter SSTabele da li je kljuc mozda u SSTabeli
			//	6a) Kljuc definitivno nije u SSTabeli -> continue;
			//	6b) Kljuc mozda jeste u SSTabeli
			//			7) Pozicionirati se na najblizi element u summary
			//			8) Otvoriti/Preskociti na index deo na datom offsetu
			//			9) Pozicionirati se na najblizi element u index
			//			10) Otvoriti/Preskociti na data deo na datom index-u
			//			11) Citati record po record iz data dela
			//			12a) Pronadjen record -> return Record.Deserialize, true
			//			12b) Nije pronadjen record -> continue
		}

	}
	return rec.Record{Key: "", Value: make([]byte, 0), Timestamp: 0, Tombstone: false}, false
}

// Pomocna funkcija povecava sve indekse SSTabeli na nekom LSM nivou za 1
func updateSSTableNames(lsmLevel int) {
	// TO-DO: azuriranje imena
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
		return &SSTableInstance{filename: sstablePath, currentOffset: 0, dataBeginOffset: 0, isSingleFile: true, isCompressed: compression}
	} else { // SSTFiles == "many"
		return &SSTableInstance{filename: newSSTablePath, currentOffset: 0, dataBeginOffset: 0, isSingleFile: false, isCompressed: compression}
	}
}

// Funkcija pravi novu SSTabelu koja sadrzi prosledjen niz Recorda
func CreateNewSSTable(records []rec.Record) {
	newSSTableInstance := makeNewSSTableInstance()
	updateSSTableNames(1)
	newCreator := SSTableCreator{Instance: *newSSTableInstance, currentIndexNumber: 0, currentSummaryNumber: 0}
	if config.GlobalConfig.SSTFiles == "one" {
		// TO-DO: Napraviti SSTabelu u jednom fajlu
	} else {
		for index, record := range records {
			if index == 0 || index == len(records)-1 {
				newCreator.currentIndexNumber = FirstOrLastElement
			}
			newCreator.WriteRecord(record)
		}
		// TO-DO: Napravi i sacuvaj Merkle stablo
		file, err := os.OpenFile(newCreator.Instance.filename+"/meta.bin", os.O_CREATE|os.O_WRONLY, 0777)
		if err != nil {
			log.Fatal(err)
		}
		metaBytes := make([]byte, 1)
		metaBytes[0] = 255
		file.Write(metaBytes)
		file.Close()
	}

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
		// TO-DO: Citanje meta podataka jednog fajla
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
		dataBeginOffset: dataOffset, merkleBeginOffset: merkleOffset}
}

// Pomocna funkcija koja cita jednu variable-encoded vrednost
func (sstable *SSTableInstance) readValue(file *os.File) []byte {
	claimer := make([]byte, 1)
	buffer := make([]byte, 0)
	for (128 & claimer[0]) == 0 {
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
func (sstable *SSTableInstance) ReadRecord() rec.Record {
	var file *os.File
	var err error
	var crcActual uint64
	var timestampActual uint64
	var tombstoneActual bool
	var keyLengthActual uint64
	var valueLengthActual uint64 = 0
	var key string
	var value []byte = make([]byte, 0)
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

	keyLengthBytes := sstable.readValue(file)
	keyLengthActual, _ = binary.Uvarint(keyLengthBytes)

	if !tombstoneActual {
		valueLengthBytes := sstable.readValue(file)
		valueLengthActual, _ = binary.Uvarint(valueLengthBytes)
	}

	if sstable.isCompressed {
		// TO-DO: read compressed
	} else {
		keyBytes := make([]byte, keyLengthActual)
		file.Read(keyBytes)
		key = string(keyBytes)
	}

	if !tombstoneActual {
		value = make([]byte, valueLengthActual)
		file.Read(value)
	}
	recordActual := rec.Record{Key: key, Value: value, Timestamp: int64(timestampActual), Tombstone: tombstoneActual}
	crcCurrent := CRC32(rec.RecToBytes(recordActual))
	if crcCurrent != uint32(crcActual) {
		fmt.Println("Oprez! Moguce je da dobijena vrednost nije validna!")
	}
	return recordActual
}

// Funkcija upisuje prosledjen record na sledece mesto u SSTabeli. Koristice se za LSM
// AKO JE SINGLE FILE SSTABELA, DODAJE SE SAMO U BLOOMFILTER I DATA DEO!!!
// AKO JE MULTI FILE SSTABELA, DODAJE SE U SVE!!!
func (sstable *SSTableCreator) WriteRecord(record rec.Record) {
	config.Init()

	if sstable.Instance.isCompressed {
		// TO-DO: dodaj kljuc u mapu za kompresiju ako je to potrebno
	}

	if sstable.Instance.isSingleFile {
		// TO-DO: dodaj na data i upisi u bloomfilter
		file, err := os.OpenFile(sstable.Instance.filename, os.O_RDWR, 0777)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()
		// TO-DO: Bloomfilter
		{
			file.Seek(sstable.Instance.bloomfilterBeginOffset, 0)
			// deserialize bloomfilter
			// add element to bloomfilter
			// serialize bloomfilter
			file.Seek(sstable.Instance.bloomfilterBeginOffset, 0)
			// write the bloomfilter back in
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

				var finalKey string
				if sstable.Instance.isCompressed {
					// TO-DO: dobavi kljuc u finalKey iz mape
				} else {
					finalKey = record.Key
				}
				keyLength := uint64(len(finalKey))
				keyLengthBytes := make([]byte, 8)
				binary.LittleEndian.PutUint64(keyLengthBytes, keyLength)

				offsetBytes := make([]byte, 8)
				binary.LittleEndian.PutUint64(offsetBytes, sstable.currentIndexOffset)

				summaryBytes := make([]byte, 0)
				summaryBytes = append(summaryBytes, keyLengthBytes...)
				summaryBytes = append(summaryBytes, []byte(finalKey)...)
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

				var finalKey string
				if sstable.Instance.isCompressed {
					// TO-DO: dobavi kljuc u finalKey iz mape
				} else {
					finalKey = record.Key
				}
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
