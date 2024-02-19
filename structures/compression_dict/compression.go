package compression_dict

import (
	"encoding/binary"
	"fmt"
	"github.com/IvanaaXD/NASP---Projekat/app/config"
	"io"
	"os"
	"path/filepath"
)

type CompressionDict struct {
	keysString []string
	keysInt    map[string]uint64
	index      uint64
}

func NewCompressionDict() *CompressionDict {
	return &CompressionDict{
		keysString: []string{},
		keysInt:    make(map[string]uint64),
		index:      1,
	}
}

func (cd *CompressionDict) Write(key string) {

	b := false
	for _, k := range cd.keysString {
		if k == key {
			b = true
			return
		}
	}
	if !b {
		cd.keysString = append(cd.keysString, key)
		cd.keysInt[key] = cd.index
		cd.index += 1
	}
}

func (cd *CompressionDict) GetId(key string) (uint64, bool) {

	for _, k := range cd.keysString {
		if k == key {
			return cd.keysInt[key], true
		}
	}
	return 0, false
}

func (cd *CompressionDict) GetKey(id uint64) (string, bool) {

	for _, k := range cd.keysInt {
		if k == id {
			return cd.keysString[id-1], true
		}
	}
	return "", false
}

func (cd *CompressionDict) Serialize() ([]byte, error) {
	var serializedData []byte

	indexBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(indexBytes, uint64(cd.index))
	serializedData = append(serializedData, indexBytes...)

	for _, key := range cd.keysString {

		keyLenBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(keyLenBytes, uint64(len(key)))
		serializedData = append(serializedData, keyLenBytes...)

		serializedData = append(serializedData, []byte(key)...)

		keyIDBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(keyIDBytes, uint64(cd.keysInt[key]))
		serializedData = append(serializedData, keyIDBytes...)
	}

	return serializedData, nil
}

func (cd *CompressionDict) Deserialize(data []byte) error {

	cd.index = uint64(int(binary.LittleEndian.Uint64(data[:8])))
	data = data[8:]

	for i := 1; i < int(cd.index); i++ {

		keyLen := int(binary.LittleEndian.Uint64(data[:8]))
		data = data[8:]

		keyBytes := data[:keyLen]
		data = data[keyLen:]

		keyID := binary.LittleEndian.Uint64(data[:8])
		data = data[8:]

		key := string(keyBytes)

		cd.keysString = append(cd.keysString, key)
		cd.keysInt[key] = keyID
	}

	return nil
}

// WORKING WITH FILE

func (cd *CompressionDict) WriteToFile() error {

	config.Init()
	filePath := config.GlobalConfig.CompressionDict
	filePath2 := filepath.Join("..", filePath)

	serializedData, err := cd.Serialize()
	if err != nil {
		return fmt.Errorf("serialization error: %v", err)
	}

	file, err := os.Create(filePath)
	if err != nil {
		file, err = os.Create(filePath2)
		if err != nil {
			return fmt.Errorf("error opening file: %v", err)
		}
	}

	_, err = file.Write(serializedData)
	if err != nil {
		return fmt.Errorf("error writing to file: %v", err)
	}

	err = file.Close()
	if err != nil {
		return err
	}

	return nil
}

func (cd *CompressionDict) ReadFromFile() error {

	config.Init()
	filePath := config.GlobalConfig.CompressionDict
	filePath2 := filepath.Join("..", filePath)

	file, err := os.Open(filePath)
	if err != nil {
		file, err = os.Open(filePath2)
		if err != nil {
			return fmt.Errorf("error opening file: %v", err)
		}
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("error getting file info: %v", err)
	}

	fileSize := fileInfo.Size()
	if fileSize == 0 {
		return nil
	}

	fileContent := make([]byte, fileSize)
	_, err = io.ReadFull(file, fileContent)
	if err != nil {
		return fmt.Errorf("error reading from file: %v", err)
	}

	err = cd.Deserialize(fileContent)
	if err != nil {
		return fmt.Errorf("deserialization error: %v", err)
	}

	err = file.Close()
	if err != nil {
		return err
	}

	return nil
}
