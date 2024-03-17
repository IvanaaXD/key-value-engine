package sstable

import (
	"fmt"
	"log"
	"os"

	"github.com/IvanaaXD/NASP/app/config"
	bf "github.com/IvanaaXD/NASP/structures/bloom-filter"
	rec "github.com/IvanaaXD/NASP/structures/record"
)

type SSTableInstance struct {
	currentOffset int64
	filename      string
	isSingleFile  bool
	isCompressed  bool
}

type SSTableCreator struct {
	Instance             SSTableInstance
	currentIndexNumber   uint32
	currentSummaryNumber uint32
}

const sstableFolderPath string = "./resources/sstables"
const newSSTablePath string = "./resources/sstables/0001sstable0001"

// Funkcija pretvara Record u zapis za SSTabelu BEZ KOMPRESIJE (public funkcija zbog LSM)
func RecordToSSTableRecord(inputRecord rec.Record) []byte {
	// TO-DO
	return make([]byte, 0)
}

// Funkcija pretvara Record u zapis za SSTabelu SA KOMPRESIJOM (public funkcija zbog LSM)
func RecordToCompressedSSTableRecord(inputRecord rec.Record) []byte {
	// TO-DO
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

}

// Funkcija pravi novu SSTabelu koja sadrzi prosledjen niz Recorda
func CreateNewSSTable(records []rec.Record) {
	var newInstance SSTableInstance
	var compression bool
	config.Init()
	if config.GlobalConfig.Compression == "yes" {
		compression = true
	} else {
		compression = false
	}

	if config.GlobalConfig.SSTFiles == "one" {
		sstablePath := newSSTablePath + ".bin"
		fmt.Println(sstablePath)
		newInstance = SSTableInstance{filename: sstablePath, currentOffset: 0, isSingleFile: true, isCompressed: compression}
	} else { // SSTFiles == "many"
		newInstance = SSTableInstance{filename: newSSTablePath, currentOffset: 0, isSingleFile: false, isCompressed: compression}
	}

	newCreator := SSTableCreator{Instance: newInstance, currentIndexNumber: 0, currentSummaryNumber: 0}
	bloom := bf.NewBloomFilter(config.GlobalConfig.BFExpectedElements, config.GlobalConfig.BFFalsePositiveRate)
	for _, record := range records {
		newCreator.WriteRecord(record)
		bloom.Add([]byte(record.Key))
	}
	// SERIALIZE THE RECORDS
	// CREATE INDEX AND SUMMARY
	// SERIALIZE THE BLOOMFILTER
	// SERIALIZE THE MERKLE TREE
	// ADD ANY NECESSARY METADATA
}

// Funkcija otvara SSTabelu sa prosledjenim file pathom. Vraca otvorenu instancu SSTabele
func OpenSSTable(filename string) SSTableInstance {
	var compression bool
	// CHECK METADATA IF SSTABLE IS COMPRESSED

	// TO DOOOOOOOOOOOOOOOOOOOOOOO

	var singlefile bool = false
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
	return SSTableInstance{filename: actualPath, currentOffset: 0, isSingleFile: singlefile, isCompressed: compression}
}

// Pomocna funkcija menja offset u otvorenoj instanci SSTabele
func (sstable *SSTableInstance) changeOffset(newOffset int64) {
	sstable.currentOffset = newOffset
}

// Funkcija cita sledeci record upisan u SSTabelu, deserijalizuje ga i vraca ga kao povratnu vrednost.
// Pamti se dokle se stiglo sa citanjem u SSTabeli.
func (sstable *SSTableInstance) ReadRecord() rec.Record {
	return rec.Record{}
}

// Funkcija upisuje prosledjen record na sledece mesto u SSTabeli. Koristice se za LSM
func (sstable *SSTableCreator) WriteRecord(record rec.Record) {
	if sstable.Instance.isSingleFile {
		// pisi data na jedno, index na drugo, summary na trece mesto itd
	} else {
		// pisi data u jedan, index u drugi, summary u treci fajl
	}
}
