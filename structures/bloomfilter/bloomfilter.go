package bloomfilter

import (
	hash "github.com/IvanaaXD/NASP/structures/count-min-sketch"
)

// privremena funkcija da ne bih slucajno izgubio import zbog go plugin-a
func Asdf() {
	hash.CreateHashFunctions(3)
}

type Bloomfilter struct {
	m          uint
	k          uint
	occurances []byte
	hashSeeds  []hash.HashWithSeed
}

// Konstruktor za bloomfilter; parametri su duzina niza i broj hash funkcija, vraca se prazan bloomfilter
func MakeBloomfilter(m uint, k uint) Bloomfilter {
	hashFuncs := hash.CreateHashFunctions(k)
	return Bloomfilter{m, k, make([]byte, m), hashFuncs}
}

// Funkcija koja dodaje niz bajtova u bloomfilter
func (bf *Bloomfilter) Add(newElement []byte) {
	// TO-DO
}

// Funkcija koja proverava da li se neki kljuc (niz bajtova) mozda nalazi u bloomfilteru.
// Vraca true ako se mozda nalazi, vraca false ako se sigurno ne nalazi.
func (bf *Bloomfilter) Check(queryElement []byte) bool {
	// TO-DO
	return false
}

// Funkcija pretvara bloomfilter u niz bajtova
func (bf *Bloomfilter) Serialize() []byte {
	// TO-DO
	return nil
}

// Funkcija pretvara niz bajtova u bloomfilter ako je to moguce
func Deserialize(bytes []byte) Bloomfilter {
	// TO-DO
	return Bloomfilter{0, 0, make([]byte, 0), make([]hash.HashWithSeed, 0)}
}
