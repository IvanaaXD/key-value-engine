package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/IvanaaXD/NASP/app/config"
	"hash/crc32"
	"os"
	"strconv"
	"strings"
	"unicode"
)

type WAL struct {
	file   *os.File
	writer *bufio.Writer
}

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func NewWAL(filePath string) (*WAL, error) {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	writer := bufio.NewWriter(file)

	file.Close()
	return &WAL{file, writer}, nil
}

func (w *WAL) Write(key, value []byte, timestamp int64, thumbstone bool) (int, error) {
	// Calculate the payload length
	payloadLength := config.GlobalConfig.CrcSize + config.GlobalConfig.TimestampSize + config.GlobalConfig.TombstoneSize + config.GlobalConfig.KeySizeSize + config.GlobalConfig.ValueSizeSize + len(key) + len(value)

	// Allocate the payload buffer
	payload := make([]byte, payloadLength)

	// Write the timestamp to the payload
	binary.LittleEndian.PutUint64(payload[config.GlobalConfig.TimestampStart:config.GlobalConfig.TimestampStart+config.GlobalConfig.TimestampSize], uint64(timestamp))

	// Write the thumbstone flag to the payload
	var thumbstoneByte byte
	if thumbstone {
		thumbstoneByte = 1
	} else {
		thumbstoneByte = 0
	}
	payload[config.GlobalConfig.TombstoneStart] = thumbstoneByte

	// Write the key size to the payload
	binary.LittleEndian.PutUint64(payload[config.GlobalConfig.KeySizeStart:config.GlobalConfig.KeySizeStart+config.GlobalConfig.KeySizeSize], uint64(len(key)))

	// Write the value size to the payload
	binary.LittleEndian.PutUint64(payload[config.GlobalConfig.ValueSizeStart:config.GlobalConfig.ValueSizeStart+config.GlobalConfig.ValueSizeSize], uint64(len(value)))

	// Write the key and value to the payload
	copy(payload[config.GlobalConfig.KeyStart:config.GlobalConfig.KeyStart+len(key)], key)
	copy(payload[config.GlobalConfig.KeyStart+len(key):], value)

	// Compute the CRC
	crc := CRC32(value)

	// Write the CRC to the payload
	binary.LittleEndian.PutUint32(payload[config.GlobalConfig.CrcStart:config.GlobalConfig.CrcStart+config.GlobalConfig.CrcSize], crc)

	// Write the payload to the WAL
	w.writer.Write(payload)

	// Flush to disk
	w.writer.Flush()

	return payloadLength, nil
}

func (w *WAL) Close() error {
	w.writer.Flush()
	return w.file.Close()

}
func CreateFile() error {

	numberOfZeros := config.MEMTABLE_NUM

	if _, err := os.Stat(config.GlobalConfig.OffsetPath); err == nil {
		return nil
	} else if os.IsNotExist(err) {
		file, errr := os.Create(config.GlobalConfig.OffsetPath)
		if errr != nil {
			return errr
		}
		defer file.Close()

		errr = file.Truncate(0)
		if errr != nil {
			return errr
		}

		_, errr = file.Seek(0, 0)
		if errr != nil {
			return errr
		}

		for i := 0; i < numberOfZeros; i++ {
			if i > 0 {
				_, errr = file.WriteString(", ")
				if errr != nil {
					return err
				}
			}

			_, errr = file.WriteString("0")
			if errr != nil {
				return errr
			}
		}

		return nil
	} else {
		return err
	}
}

func WriteOffsets(currentMemtable int, lenOfRec int) error {

	file, err := os.OpenFile(config.GlobalConfig.OffsetPath, os.O_RDWR, 0644)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanWords)

	var values []int

	for scanner.Scan() {
		for _, part := range strings.FieldsFunc(scanner.Text(), func(r rune) bool {
			return r == ',' || unicode.IsSpace(r)
		}) {
			val, err := strconv.Atoi(part)
			if err != nil {
				fmt.Println("Error converting string to int:", err)
				return err
			}
			values = append(values, val)
		}
	}

	for i := 0; i < len(values); i++ {
		if i != currentMemtable {
			values[i] += lenOfRec
		}
	}

	_, err2 := file.Seek(0, 0)
	if err2 != nil {
		fmt.Println("Error seeking file:", err)
		return err2
	}

	var updatedValues []string
	for _, v := range values {
		updatedValues = append(updatedValues, strconv.Itoa(v))
	}

	newContent := strings.Join(updatedValues, ", ")
	_, err3 := file.WriteString(newContent)
	if err3 != nil {
		fmt.Println("Error writing to file:", err)
		return err3
	}

	//fmt.Println("Offsets updated successfully.")
	return nil
}

func ReadOffsets() ([]int, error) {

	file, err := os.Open(config.GlobalConfig.OffsetPath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanWords)

	var values []int

	for scanner.Scan() {
		for _, part := range strings.FieldsFunc(scanner.Text(), func(r rune) bool {
			return r == ',' || unicode.IsSpace(r)
		}) {
			val, err := strconv.Atoi(part)
			if err != nil {
				fmt.Println("Error converting string to int:", err)
				return nil, err
			}
			values = append(values, val)
		}
	}

	return values, nil
}

func WalFlush(i int) error {

	offsets, err := ReadOffsets()
	if err != nil {
		fmt.Println("Error reading offsets:", err)
		return err
	}

	if len(offsets) == 0 {
		fmt.Println("No offsets to flush.")
		return nil
	}

	offsetToDelete := offsets[i]

	content, err := os.ReadFile(config.GlobalConfig.OffsetPath)
	if err != nil {
		fmt.Println("Error reading wal.txt:", err)
		return err
	}

	values := strings.Split(string(content), ", ")
	if len(values) == 0 {
		fmt.Println("No values in wal.txt to flush.")
		return nil
	}

	values = values[1:]

	for l := range values {
		val, err1 := strconv.Atoi(values[l])
		if err1 != nil {
			fmt.Println("Error converting string to int:", err)
			return err1
		}
		val -= offsetToDelete
		values[l] = strconv.Itoa(val)
	}

	values = append(values, "0")

	updatedContent := strings.Join(values, ", ")
	err = os.WriteFile(config.GlobalConfig.OffsetPath, []byte(updatedContent), 0644)
	if err != nil {
		fmt.Println("Error writing to wal.txt:", err)
		return err
	}

	file, err := os.OpenFile(config.GlobalConfig.WalPath, os.O_RDWR, 0644)
	if err != nil {
		fmt.Println("Error opening wal.log:", err)
		return err
	}
	defer file.Close()

	_, err = file.Seek(int64(offsetToDelete), 0)
	if err != nil {
		fmt.Println("Error seeking in wal.log:", err)
		return err
	}

	err = file.Truncate(0)
	if err != nil {
		fmt.Println("Error truncating wal.log:", err)
		return err
	}

	fmt.Println("WalFlush successful.")
	return nil
}
