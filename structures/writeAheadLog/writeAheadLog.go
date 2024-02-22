package writeaheadlog

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/IvanaaXD/NASP/app/config"
	record "github.com/IvanaaXD/NASP/structures/record"
)

type WriteAheadLog struct {
	Filename         string
	SegmentLength    uint64
	CurrentOffset    uint64
	NumSavedElements []uint64
}

// Pomocna funkcija koja menja filename wal-a na sledeci indeks
func (wal *WriteAheadLog) povecajIndeksFajla() {
	delovi := strings.Split(wal.Filename, "_")

	indeks := strings.Split(delovi[1], ".")

	intIndeks, err := strconv.Atoi(indeks[0])
	if err != nil {
		log.Fatal(err)
	}
	intIndeks += 1
	strIndeks := fmt.Sprintf("%04d", intIndeks)
	wal.Filename = delovi[0] + "_" + strIndeks + "." + indeks[1]
}

// Pomocna funkcija koja otvara sledeci fajl i cita podatke iz njega
func (wal *WriteAheadLog) procitajOverflow(file *os.File, podaci []byte, indeksOffset int) []byte {
	if file.Name() == wal.Filename {
		wal.povecajIndeksFajla()
	}
	file, err := os.OpenFile(wal.Filename, os.O_RDONLY, 0664)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	file.Seek(int64(wal.CurrentOffset), 0)
	bytesRead, err := file.Read(podaci[indeksOffset:])
	if err != nil {
		log.Fatal(err)
	}
	wal.CurrentOffset += uint64(bytesRead)
	return podaci
}

// Pomocna funkcija koja vraca niz bajtova koji odgovara jednom zapisu, i bool vrednost da li je promenjen indeks fajla ili ne
func (wal *WriteAheadLog) procitajSledeci(file *os.File) ([]byte, bool) {
	var fileChanged bool = false
	allBytes := make([]byte, 0)

	CRCBytes := make([]byte, config.CRC_SIZE)
	bytesRead, err := file.Read(CRCBytes)
	if bytesRead == 0 { // Ako nije nista ucitano - nema vise podataka
		return nil, false
	}
	if err != nil && bytesRead != 0 {
		log.Fatal(err)
	}
	wal.CurrentOffset = wal.CurrentOffset + uint64(bytesRead)
	if uint64(bytesRead) != config.CRC_SIZE {
		if !fileChanged {
			wal.CurrentOffset = 0
		}
		fileChanged = true

		wal.procitajOverflow(file, CRCBytes, bytesRead)
		// Open a new file, read the bytes you need, set the offset right
	}
	allBytes = append(allBytes, CRCBytes...)

	timestampBytes := make([]byte, config.TIMESTAMP_SIZE)
	bytesRead, err = file.Read(timestampBytes)
	if err != nil && bytesRead != 0 {
		log.Fatal(err)
	}
	wal.CurrentOffset = wal.CurrentOffset + uint64(bytesRead)
	if uint64(bytesRead) != config.TIMESTAMP_SIZE {
		if !fileChanged {
			wal.CurrentOffset = 0
		}
		fileChanged = true

		wal.procitajOverflow(file, timestampBytes, bytesRead)
		// Open a new file, read the bytes you need, set the offset right
	}
	allBytes = append(allBytes, timestampBytes...)

	tombstoneBytes := make([]byte, config.TOMBSTONE_SIZE)
	bytesRead, err = file.Read(tombstoneBytes)
	if err != nil && bytesRead != 0 {
		log.Fatal(err)
	}
	wal.CurrentOffset = wal.CurrentOffset + uint64(bytesRead)
	if uint64(bytesRead) != config.TOMBSTONE_SIZE {
		if !fileChanged {
			wal.CurrentOffset = 0
		}
		fileChanged = true

		wal.procitajOverflow(file, tombstoneBytes, bytesRead)
		// Open a new file, read the bytes you need, set the offset right
	}
	allBytes = append(allBytes, tombstoneBytes...)

	keySizeBytes := make([]byte, config.KEY_SIZE_SIZE)
	bytesRead, err = file.Read(keySizeBytes)
	if err != nil && bytesRead != 0 {
		log.Fatal(err)
	}
	wal.CurrentOffset = wal.CurrentOffset + uint64(bytesRead)
	if uint64(bytesRead) != config.KEY_SIZE_SIZE {
		if !fileChanged {
			wal.CurrentOffset = 0
		}
		fileChanged = true

		wal.procitajOverflow(file, keySizeBytes, bytesRead)
		// Open a new file, read the bytes you need, set the offset right
	}
	allBytes = append(allBytes, keySizeBytes...)

	valueSizeBytes := make([]byte, config.VALUE_SIZE_SIZE)
	bytesRead, err = file.Read(valueSizeBytes)
	if err != nil && bytesRead != 0 {
		log.Fatal(err)
	}
	wal.CurrentOffset = wal.CurrentOffset + uint64(bytesRead)
	if uint64(bytesRead) != config.VALUE_SIZE_SIZE {
		if !fileChanged {
			wal.CurrentOffset = 0
		}
		fileChanged = true

		wal.procitajOverflow(file, valueSizeBytes, bytesRead)
		// Open a new file, read the bytes you need, set the offset right
	}
	allBytes = append(allBytes, valueSizeBytes...)

	keyLength := binary.BigEndian.Uint64(keySizeBytes)
	keyBytes := make([]byte, keyLength)
	bytesRead, err = file.Read(keyBytes)
	if err != nil && bytesRead != 0 {
		log.Fatal(err)
	}
	wal.CurrentOffset = wal.CurrentOffset + uint64(bytesRead)
	if uint64(bytesRead) != keyLength {
		if !fileChanged {
			wal.CurrentOffset = 0
		}
		fileChanged = true

		wal.procitajOverflow(file, keyBytes, bytesRead)
		// Open a new file, read the bytes you need, set the offset right
	}
	allBytes = append(allBytes, keyBytes...)

	valueLength := binary.BigEndian.Uint64(valueSizeBytes)
	valueBytes := make([]byte, valueLength)
	bytesRead, err = file.Read(valueBytes)
	if err != nil && bytesRead != 0 {
		log.Fatal(err)
	}
	wal.CurrentOffset = wal.CurrentOffset + uint64(bytesRead)
	if uint64(bytesRead) != valueLength {
		if !fileChanged {
			wal.CurrentOffset = 0
		}
		fileChanged = true

		wal.procitajOverflow(file, valueBytes, bytesRead)
		// Open a new file, read the bytes you need, set the offset right
	}
	allBytes = append(allBytes, valueBytes...)

	return allBytes, fileChanged
}

// Pomocna funkcija koja radi isto sto i UcitajSve(), samo sto ne stavlja offset na 0
func (wal *WriteAheadLog) ucitajSvePomocna() []record.Record {
	allElements := make([]record.Record, 0)

	_, err := os.Stat(wal.Filename) // Provera da li postoji ikakav log
	if os.IsNotExist(err) {
		return allElements
	}

	file, err := os.OpenFile(wal.Filename, os.O_RDONLY, 0664)
	if err != nil {
		log.Fatal(err)
	}
	file.Seek(int64(wal.CurrentOffset), 0)

	for { // go sintaksa za while true
		podaci, fileChanged := wal.procitajSledeci(file)
		if podaci == nil { // Ako nista nije ucitano, to je to
			file.Close()
			break
		}
		konvertovano := record.BytesToRecord(podaci)
		if fileChanged {
			file.Close()
			file, err = os.OpenFile(wal.Filename, os.O_RDONLY|os.O_CREATE, 0664)
			if err != nil {
				log.Fatal(err)
			}
			file.Seek(int64(wal.CurrentOffset), 0)
		}
		allElements = append(allElements, konvertovano)
	}

	return allElements
}

// Funkcija ucitava sve elemente trenutno upisane u write ahead log i vraca niz svih njih
func (wal *WriteAheadLog) UcitajSve() []record.Record {
	allElements := make([]record.Record, 0)

	_, err := os.Stat("./wal/")
	if os.IsNotExist(err) {
		if err := os.Mkdir("wal", os.ModePerm); err != nil {
			log.Fatal(err)
		}
	}

	fajlovi, err := os.ReadDir("./wal/")
	if err != nil {
		log.Fatal(err)
	}

	if len(fajlovi) < 1 {
		wal.Filename = "./wal/wal_0001.log"
	} else {
		wal.Filename = "./wal/" + fajlovi[0].Name() // stavi da je filename prvi fajl
	}

	wal.CurrentOffset = 0

	_, err = os.Stat(wal.Filename) // Provera da li postoji ikakav log
	if os.IsNotExist(err) {
		return allElements
	}

	file, err := os.OpenFile(wal.Filename, os.O_RDONLY, 0664)
	if err != nil {
		log.Fatal(err)
	}

	wal.CurrentOffset = 0

	for { // go sintaksa za while true
		podaci, fileChanged := wal.procitajSledeci(file)
		if podaci == nil { // Ako nista nije ucitano, to je to
			file.Close()
			break
		}
		konvertovano := record.BytesToRecord(podaci)
		if fileChanged {
			file.Close()
			file, err = os.OpenFile(wal.Filename, os.O_RDONLY|os.O_CREATE, 0664)
			if err != nil {
				log.Fatal(err)
			}
			file.Seek(int64(wal.CurrentOffset), 0)
		}
		allElements = append(allElements, konvertovano)
	}

	return allElements
}

// Pomocna funkcija pri brisanju delova wal-a. Radi isto sto i dodaj zapis, osim azuriranja broja sacuvanih elemenata po memtabeli
func (wal *WriteAheadLog) dodajZapisPomocna(element record.Record) {
	file, err := os.OpenFile(wal.Filename, os.O_APPEND|os.O_CREATE, 0664)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	fileStats, err := file.Stat()
	if err != nil {
		log.Fatal(err)
	}

	wal.CurrentOffset = uint64(fileStats.Size())
	serijalizovanElement := element.Serijalizuj()
	slobodnoMesta := int(wal.SegmentLength) - int(wal.CurrentOffset)
	if len(serijalizovanElement) <= slobodnoMesta {
		_, err = file.Write(serijalizovanElement)
		if err != nil {
			log.Fatal(err)
		}
		wal.CurrentOffset += uint64(len(serijalizovanElement))
	} else {
		deo1 := serijalizovanElement[:slobodnoMesta]
		deo2 := serijalizovanElement[slobodnoMesta:]

		_, err = file.Write(deo1)
		if err != nil {
			log.Fatal(err)
		}

		wal.povecajIndeksFajla()

		file2, err := os.OpenFile(wal.Filename, os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			log.Fatal(err)
		}
		defer file2.Close()

		_, err2 := file2.Write(deo2)
		if err2 != nil {
			log.Fatal(err)
		}
		wal.CurrentOffset = uint64(len(deo2))
	}
}

// Funkcija dodaje zapis u write ahead log
func (wal *WriteAheadLog) DodajZapis(element record.Record, indeksMemtabele int, sstableCreated bool) {
	file, err := os.OpenFile(wal.Filename, os.O_APPEND|os.O_CREATE, 0664)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	fileStats, err := file.Stat()
	if err != nil {
		log.Fatal(err)
	}

	wal.CurrentOffset = uint64(fileStats.Size())
	serijalizovanElement := element.Serijalizuj()
	slobodnoMesta := int(wal.SegmentLength) - int(wal.CurrentOffset)
	if len(serijalizovanElement) <= slobodnoMesta {
		_, err = file.Write(serijalizovanElement)
		if err != nil {
			log.Fatal(err)
		}
		wal.CurrentOffset += uint64(len(serijalizovanElement))
	} else {
		deo1 := serijalizovanElement[:slobodnoMesta]
		deo2 := serijalizovanElement[slobodnoMesta:]

		_, err = file.Write(deo1)
		if err != nil {
			log.Fatal(err)
		}

		wal.povecajIndeksFajla()

		file2, err := os.OpenFile(wal.Filename, os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			log.Fatal(err)
		}
		defer file2.Close()

		_, err2 := file2.Write(deo2)
		if err2 != nil {
			log.Fatal(err)
		}
		wal.CurrentOffset = uint64(len(deo2))
	}

	if sstableCreated {
		indeksMemtabele = len(wal.NumSavedElements) - 1
	}
	wal.NumSavedElements[indeksMemtabele] += 1
}

// Funkcija koja brise serijalizovani deo WAL-a (pokrece se pri pravljenju SSTabele)
func (wal *WriteAheadLog) IzbrisiSerijalizovano() {
	fajlovi, err := os.ReadDir("./wal/")
	if err != nil {
		log.Fatal(err)
	}
	wal.Filename = "./wal/" + fajlovi[0].Name() // stavi da je filename prvi fajl
	wal.CurrentOffset = 0

	file, err := os.OpenFile(wal.Filename, os.O_RDONLY, 0664)
	if err != nil {
		log.Fatal(err)
	}

	// preskoci sve serijalizovane elemente
	elementsToDelete := wal.NumSavedElements[0]
	for i := 0; i < int(elementsToDelete); i++ {
		_, fileChanged := wal.procitajSledeci(file)
		if fileChanged {
			file.Close()
			file, err = os.OpenFile(wal.Filename, os.O_RDONLY|os.O_CREATE, 0664)
			if err != nil {
				log.Fatal(err)
			}
			file.Seek(int64(wal.CurrentOffset), 0)
		}
	}

	file.Close()

	// ucitaj sve neserijalizovane elemente
	elementiZaCuvanje := wal.ucitajSvePomocna()
	fmt.Println(len(elementiZaCuvanje))
	// izbrisi svaki wal zapis
	for _, fajlZaBris := range fajlovi {
		os.Remove("./wal/" + fajlZaBris.Name())
	}

	// dodaj sve elemente ponovo
	wal.Filename = "./wal/" + fajlovi[0].Name()
	for _, elementZaUpis := range elementiZaCuvanje {
		wal.dodajZapisPomocna(elementZaUpis)
	}
	wal.Filename = "./wal/" + fajlovi[0].Name() // stavi da je filename prvi fajl
	wal.CurrentOffset = 0
	if len(wal.NumSavedElements) > 1 {
		wal.NumSavedElements = append(wal.NumSavedElements[1:], 0)
	} else {
		wal.NumSavedElements[0] = 0
	}

	wal.UcitajSve() // postavlja se na zadnji fajl i na pravilan offset
}
