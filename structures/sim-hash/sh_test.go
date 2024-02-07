package simhash

import (
	"fmt"
	"io/ioutil"
	"testing"
)

func loadTextFromFile(filePath string) (string, error) {
	content, err := ioutil.ReadFile(filePath)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

func TestSimHash(t *testing.T) {

	file1Path := "tekst1.txt"
	file2Path := "tekst2.txt"

	text1, err := loadTextFromFile(file1Path)
	if err != nil {
		fmt.Printf("Error loading text from %s: %v\n", file1Path, err)
		return
	}

	text2, err := loadTextFromFile(file2Path)
	if err != nil {
		fmt.Printf("Error loading text from %s: %v\n", file2Path, err)
		return
	}

	sh1 := NewSimHash(text1)
	sh2 := NewSimHash(text2)

	expectedDistance := 22
	actualDistance := sh1.GetDistance(sh2)

	if actualDistance != expectedDistance {
		t.Errorf("Distance mismatch. Expected: %d, Got: %d", expectedDistance, actualDistance)
	}

}
