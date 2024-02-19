package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/IvanaaXD/NASP/app/config"
	"io"
	"os"
	"path/filepath"
)

type Map struct {
	mapa map[string][]string
	size uint64
}

func NewMap() *Map {
	return &Map{
		mapa: make(map[string][]string),
		size: 0,
	}
}

func (m *Map) Write(filenameId string, oneOrMore string, compression string) {

	if m.mapa[filenameId] != nil {
		m.mapa[filenameId] = []string{}
	} else {
		m.size += 1
	}

	m.mapa[filenameId] = append(m.mapa[filenameId], oneOrMore)
	m.mapa[filenameId] = append(m.mapa[filenameId], compression)
}

func (m *Map) GetValueMode(filenameId string) (string, bool) {

	for k, v := range m.mapa {
		if k == filenameId {

			for i := range v {
				return v[i], true
			}
		}
	}
	return "", false
}

func (m *Map) GetValueCompression(filenameId string) (string, bool) {

	for k, v := range m.mapa {
		if k == filenameId {

			return v[1], true
		}
	}
	return "", false
}
func (m *Map) GetSize() uint64 {
	return m.size
}

func (m *Map) Serialize() ([]byte, error) {
	var serializedData []byte

	sizeBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(sizeBytes, m.size)
	serializedData = append(serializedData, sizeBytes...)

	for filenameId, value := range m.mapa {

		// Convert filenameId string to bytes
		filenameIdBytes := []byte(filenameId)
		filenameIdLenBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(filenameIdLenBytes, uint64(len(filenameIdBytes)))
		serializedData = append(serializedData, filenameIdLenBytes...)
		serializedData = append(serializedData, filenameIdBytes...)

		for _, v := range value {
			valueLenBytes := make([]byte, 8)
			binary.LittleEndian.PutUint64(valueLenBytes, uint64(len(v)))
			serializedData = append(serializedData, valueLenBytes...)

			serializedData = append(serializedData, []byte(v)...)
		}
	}

	return serializedData, nil
}

func (m *Map) Deserialize(data []byte) error {
	if len(data) < 8 {
		return errors.New("insufficient data for size field")
	}

	m.size = binary.LittleEndian.Uint64(data[:8])
	data = data[8:]

	for i := 0; i < int(m.size); i++ {
		if len(data) < 8 {
			return errors.New("insufficient data for filenameIdLen field")
		}

		filenameIdLen := binary.LittleEndian.Uint64(data[:8])
		data = data[8:]

		if len(data) < int(filenameIdLen) {
			return errors.New("insufficient data for filenameIdBytes")
		}

		// Convert bytes to filenameId string
		filenameId := string(data[:filenameIdLen])
		data = data[filenameIdLen:]

		var value []string
		for j := 0; j < 2; j++ {
			if len(data) < 8 {
				return errors.New("insufficient data for valueLen field")
			}

			valueLen := binary.LittleEndian.Uint64(data[:8])
			data = data[8:]

			if valueLen == 0 {
				break // End of list
			}

			if len(data) < int(valueLen) {
				return errors.New("insufficient data for valueBytes")
			}

			v := string(data[:valueLen])
			data = data[valueLen:]

			value = append(value, v)
		}

		m.mapa[filenameId] = value
	}

	return nil
}

// WORKING WITH FILE

func (m *Map) WriteToFile() error {
	config.Init()
	filePath := config.GlobalConfig.MapFileName
	filePath2 := filepath.Join("..", filePath)

	serializedData, err := m.Serialize()
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
		file.Close()
		return fmt.Errorf("error writing to file: %v", err)
	}

	file.Close()

	return nil
}

func (m *Map) ReadFromFile() error {

	config.Init()

	filePath := config.GlobalConfig.MapFileName
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
	_, err = io.ReadFull(file, fileContent) // fileContent = 0
	if err != nil {
		return fmt.Errorf("error reading from file: %v", err)
	}

	err = m.Deserialize(fileContent)
	if err != nil {
		return fmt.Errorf("deserialization error: %v", err)
	}

	file.Close()

	return nil
}
