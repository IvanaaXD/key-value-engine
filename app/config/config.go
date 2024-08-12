package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"gopkg.in/yaml.v3"
)

var GlobalConfig Config
var separator = string(filepath.Separator)

const (
	COMPRESSION_DICT = "___cd___"
	RATE_LIMIT       = "___rl___"

	BF_PREFIX  = "___bf___"
	CMS_PREFIX = "___cms___"
	HLL_PREFIX = "___hll___"
	SH_PREFIX  = "___sh___"
	TB_PREFIX  = "___tb___"

	BF_EXPECTED_EL             = 1000  // broj ocekivanih elemenata u bloom filteru
	BF_FALSE_POSITIVE_RATE     = 0.001 // bloom filter false positive
	CMS_EPSILON                = 0.001
	CMS_DELTA                  = 0.001
	CACHE_CAP                  = 100
	MEMTABLE_SIZE              = 10
	MEMTABLE_NUM               = 5
	STRUCTURE_TYPE             = "btree"
	B_TREE_ORDER               = 3
	MAP_FILE_PATH              = "resources/map_file.dat"
	TOKEN_NUMBER               = 20
	TOKEN_REFRESH_TIME         = 2
	MAX_ENTRY_SIZE             = 1024
	COMPRESSION_DICT_FILE_PATH = "resources/compression_dict.dat"
	WAL_PATH                   = "resources/wal_0001.log"
	SCALING_FACTOR             = 2
	COMPACTION_ALGORITHM       = "sizeTiered"
	SEGMENT_SIZE               = 256
	DEGREE_OF_DILUTION         = 5      // stepen proredjenosti
	SST_FILES                  = "many" // one or many
	SSTABLE_SIZE               = 20
	COMPRESSION                = "no" // yes or no
	PREFIX                     = "data/usertables"
	LSM_MAX_LEVELS             = 4
	LSM_MAX_TABLES             = 4

	TIMESTAMP_SIZE  = 8
	TOMBSTONE_SIZE  = 1
	KEY_SIZE_SIZE   = 8
	VALUE_SIZE_SIZE = 8

	TIMESTAMP_START  = 0
	TOMBSTONE_START  = TIMESTAMP_START + TIMESTAMP_SIZE
	KEY_SIZE_START   = TOMBSTONE_START + TOMBSTONE_SIZE
	VALUE_SIZE_START = KEY_SIZE_START + KEY_SIZE_SIZE
	KEY_START        = VALUE_SIZE_START + VALUE_SIZE_SIZE
)

type Config struct {
	BFExpectedElements  int      `yaml:"BFExpectedElements"`
	BFFalsePositiveRate float64  `yaml:"bloomFalsePositive"`
	CmsEpsilon          float64  `yaml:"cmsEpsilon"`
	CmsDelta            float64  `yaml:"cmsDelta"`
	CacheCapacity       int      `yaml:"cacheCapacity"`
	MemtableSize        uint     `yaml:"memtableSize"`
	MemtableNum         uint     `yaml:"memtableNum"`
	StructureType       string   `yaml:"structureType"`
	TokenNumber         uint16   `yaml:"tokenNumber"`
	TokenRefreshTime    uint16   `yaml:"tokenRefreshTime"`
	WalPath             string   `yaml:"walPath"`
	MaxEntrySize        int      `yaml:"maxEntrySize"`
	CrcSize             int      `yaml:"crcSize"`
	TimestampSize       int      `yaml:"timestampSize"`
	TombstoneSize       int      `yaml:"tombstoneSize"`
	KeySizeSize         int      `yaml:"keySizeSize"`
	ValueSizeSize       int      `yaml:"valueSizeSize"`
	CrcStart            int      `yaml:"crcStart"`
	BTreeOrder          int      `yaml:"bTreeOrder"`
	ScalingFactor       int      `yaml:"scalingFactor"`
	CompactionAlgorithm string   `yaml:"compactionAlgorithm"`
	Condition           string   `yaml:"condition"`
	SegmentSize         int      `yaml:"segmentSize"`
	DegreeOfDilution    int      `yaml:"degreeOfDilution"`
	SSTFiles            string   `yaml:"sstFiles"`
	SSTableSize         uint64   `yaml:"sstableSize"`
	Prefix              string   `yaml:"prefix"`
	TimestampStart      int      `yaml:"timestampStart"`
	TombstoneStart      int      `yaml:"tombstoneStart"`
	KeySizeStart        int      `yaml:"keySizeStart"`
	ValueSizeStart      int      `yaml:"ValueSizeStart"`
	KeyStart            int      `yaml:"keyStart"`
	CompressionDict     string   `yaml:"dictionary"`
	BFPrefix            string   `yaml:"bfPrefix"`
	CMSPrefix           string   `yaml:"cmsPrefix"`
	HLLPrefix           string   `yaml:"hllPrefix"`
	SHPrefix            string   `yaml:"shPrefix"`
	TBPrefix            string   `yaml:"tbPrefix"`
	Compression         string   `yaml:"compression"`
	MapFileName         string   `yaml:"mapFileName"`
	LSMMaxLevels        int      `yaml:"lsmMaxLevels"`
	LSMMaxTables        uint64   `yaml:"lsmMaxTables"`
	LsmLeveledComp      []uint64 `yaml:"lsmLeveledComp"`
}

func NewConfig(filename string) *Config {
	var config Config
	yamlFile, err := os.ReadFile(filename)
	if err != nil {
		config.BFExpectedElements = BF_EXPECTED_EL
		config.BFFalsePositiveRate = BF_FALSE_POSITIVE_RATE
		config.CmsDelta = CMS_DELTA
		config.CmsEpsilon = CMS_EPSILON
		config.CacheCapacity = CACHE_CAP
		config.MemtableSize = MEMTABLE_SIZE
		config.MemtableNum = MEMTABLE_NUM
		config.StructureType = STRUCTURE_TYPE
		config.TokenNumber = TOKEN_NUMBER
		config.TokenRefreshTime = TOKEN_REFRESH_TIME
		config.WalPath = WAL_PATH
		config.MaxEntrySize = MAX_ENTRY_SIZE
		config.TimestampSize = TIMESTAMP_SIZE
		config.TombstoneSize = TOMBSTONE_SIZE
		config.KeySizeSize = KEY_SIZE_SIZE
		config.ValueSizeSize = VALUE_SIZE_SIZE
		config.BTreeOrder = B_TREE_ORDER
		config.SegmentSize = SEGMENT_SIZE
		config.ScalingFactor = SCALING_FACTOR
		config.CompactionAlgorithm = COMPACTION_ALGORITHM
		config.DegreeOfDilution = DEGREE_OF_DILUTION
		config.SSTFiles = SST_FILES
		config.SSTableSize = SSTABLE_SIZE
		config.Prefix = PREFIX
		config.TimestampStart = TIMESTAMP_START
		config.TombstoneStart = TOMBSTONE_START
		config.KeySizeStart = KEY_SIZE_START
		config.ValueSizeStart = VALUE_SIZE_START
		config.KeyStart = KEY_START
		config.CompressionDict = COMPRESSION_DICT_FILE_PATH
		config.BFPrefix = BF_PREFIX
		config.CMSPrefix = CMS_PREFIX
		config.HLLPrefix = HLL_PREFIX
		config.SHPrefix = SH_PREFIX
		config.TBPrefix = TB_PREFIX
		config.Compression = COMPRESSION
		config.MapFileName = MAP_FILE_PATH
		config.LSMMaxTables = LSM_MAX_TABLES
		config.LSMMaxLevels = LSM_MAX_LEVELS
		config.LsmLeveledComp = []uint64{4, 10, 100, 500}

	} else {
		err = yaml.Unmarshal(yamlFile, &config)

		if err != nil {
			fmt.Printf("Unmarshal: %v", err)
		}

		config.BFExpectedElements = BF_EXPECTED_EL
		config.BFFalsePositiveRate = BF_FALSE_POSITIVE_RATE
		config.CmsDelta = CMS_DELTA
		config.CmsEpsilon = CMS_EPSILON
		config.WalPath = WAL_PATH
		config.MaxEntrySize = MAX_ENTRY_SIZE
		config.TimestampSize = TIMESTAMP_SIZE
		config.TombstoneSize = TOMBSTONE_SIZE
		config.KeySizeSize = KEY_SIZE_SIZE
		config.ValueSizeSize = VALUE_SIZE_SIZE
		config.BTreeOrder = B_TREE_ORDER
		config.ScalingFactor = SCALING_FACTOR
		config.SSTableSize = SSTABLE_SIZE
		config.Prefix = PREFIX
		config.Compression = COMPRESSION
		config.TimestampStart = TIMESTAMP_START
		config.TombstoneStart = TOMBSTONE_START
		config.KeySizeStart = KEY_SIZE_START
		config.ValueSizeStart = VALUE_SIZE_START
		config.KeyStart = KEY_START
		config.CompressionDict = COMPRESSION_DICT_FILE_PATH
		config.BFPrefix = BF_PREFIX
		config.CMSPrefix = CMS_PREFIX
		config.HLLPrefix = HLL_PREFIX
		config.SHPrefix = SH_PREFIX
		config.TBPrefix = TB_PREFIX
		config.MapFileName = MAP_FILE_PATH
	}

	return &config
}

func getExecutablePath() string {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		panic("Failed to get the executable path.")
	}
	return filepath.Dir(filename)
}

func Init() {
	executablePath := getExecutablePath()
	//yamlPath := filepath.Join(executablePath, "app", "config", "config.yaml")
	yamlPath := executablePath

	if _, err := os.Stat(yamlPath); errors.Is(err, os.ErrNotExist) {
		f, err := os.Create(yamlPath)
		if err != nil {
			panic(err)
		}
		defer f.Close()

		out, err := yaml.Marshal(&GlobalConfig)
		if err != nil {
			panic(err)
		}

		f.Write(out)
	}

	GlobalConfig = *NewConfig(yamlPath)
}
