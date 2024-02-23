package hyper_log_log

import (
	"encoding/binary"
	"hash/fnv"
	"math"
	"math/bits"
	"math/rand"
	"time"
)

type HyperLogLog struct {
	registers []int
	m         uint // number of registers
	b         uint // bits to calculate [4..16]
}

// new hyperloglog instance with the specified precision

func NewHyperLogLog(m uint) *HyperLogLog {
	return &HyperLogLog{
		registers: make([]int, m),
		m:         m,
		b:         uint(math.Ceil(math.Log2(float64(m)))),
	}
}

func leftmostActiveBit(x uint32) int {
	return 1 + bits.LeadingZeros32(x)
}

func createHash(stream []byte) uint32 {
	h := fnv.New32()
	h.Write(stream)
	sum := h.Sum32()
	h.Reset()
	return sum
}

func (hll HyperLogLog) Add(data []byte) *HyperLogLog {
	x := createHash(data)
	k := 32 - hll.b
	r := leftmostActiveBit(x << hll.b)
	j := x >> k

	if r > hll.registers[j] {
		hll.registers[j] = r
	}
	return &hll
}

func (hll HyperLogLog) Count() uint64 {
	sum := 0.
	m := float64(hll.m)
	for _, v := range hll.registers {
		sum += math.Pow(math.Pow(2, float64(v)), -1)
	}
	estimate := .79402 * m * m / sum
	return uint64(estimate)
}

func (hll *HyperLogLog) Delete() {
	hll.registers = nil
}

func (hll HyperLogLog) Serialize() []byte {

	ret := make([]byte, 9)
	ret[0] = byte(hll.b)

	binary.LittleEndian.PutUint64(ret[1:], uint64(hll.m))

	for _, reg := range hll.registers {
		ret = append(ret, byte(reg))
	}

	return ret
}

func Deserialize(byteArr []byte) *HyperLogLog {

	b := uint(byteArr[0])
	m := binary.LittleEndian.Uint64(byteArr[1:9])

	reg := make([]int, len(byteArr)-9)

	for i := 9; i < len(byteArr); i++ {
		reg[i-9] = int(byteArr[i])
	}

	return &HyperLogLog{
		registers: reg,
		m:         uint(m),
		b:         b,
	}
}

// ----------- NOT USED IN PROJECT -----------

func getRandomData() (out [][]byte, intout []uint32) {
	for i := 0; i < math.MaxInt16; i++ {
		rand.Seed(time.Now().UnixNano())
		i := rand.Uint32()
		b := make([]byte, 4)
		binary.LittleEndian.PutUint32(b, i)
		out = append(out, b)
		intout = append(intout, i)
	}
	return
}

func classicCountDistinct(input []uint32) int {
	m := map[uint32]struct{}{}
	for _, i := range input {
		if _, ok := m[i]; !ok {
			m[i] = struct{}{}
		}
	}
	return len(m)
}

func firstKbits(value, k uint64) uint64 {
	return value >> (64 - k)
}

func trailingZeroBits(value uint64) int {
	return bits.TrailingZeros64(value)
}

func countDistinct(input []int) int {
	m := map[int]struct{}{}
	for _, i := range input {
		if _, ok := m[i]; !ok {
			m[i] = struct{}{}
		}
	}
	return len(m)
}
