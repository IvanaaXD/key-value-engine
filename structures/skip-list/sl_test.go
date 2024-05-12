package skip_list

import (
	"fmt"
	"github.com/IvanaaXD/NASP/structures/record"
	"testing"
)

func Test(t *testing.T) {
	fmt.Println("\nTest Skip list:\n")
	s := NewSkipList(4)

	// Dodavanje
	s.Write(record.Record{Key: "3"})
	s.Write(record.Record{Key: "6"})
	s.Write(record.Record{Key: "7"})
	s.Write(record.Record{Key: "9"})
	s.Write(record.Record{Key: "12"})
	s.Write(record.Record{Key: "19"})
	s.Write(record.Record{Key: "17"})
	s.Write(record.Record{Key: "26"})
	s.Write(record.Record{Key: "21"})
	s.Write(record.Record{Key: "25"})

	fmt.Println("Skip lista:")
	s.Print()

	// Pretraga
	searchRecord := record.Record{Key: "6"}
	_, b := s.Read(searchRecord.Key)
	if b {
		fmt.Printf("Element sa kljucem %s je pronađen u Skip listi.\n", searchRecord.Key)
	} else {
		fmt.Printf("Element sa kljucem %s nije pronađen u Skip listi.\n", searchRecord.Key)
	}

	// Brisanje
	deleteRecord := record.Record{Key: "17"}
	obrisan := s.Delete(deleteRecord)
	if obrisan {
		fmt.Printf("Element sa kljucem %s je obrisan iz Skip liste.\n", deleteRecord.Key)

		fmt.Println("Skip lista nakon brisanja:")
		s.Print()
	}

}
