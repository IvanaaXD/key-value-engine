package count_min_sketch

import (
	"testing"
)

type encodeTest struct {
	arg      []byte
	expected uint64
}

var encodetests = []encodeTest{
	{[]byte("The quick brown fox jumps over the lazy dog."), 4}, // Assuming this sentence hasn't been added
	{[]byte("To be or not to be, that is the question."), 3},    // Assuming this sentence hasn't been added
	{[]byte("All that glitters is not gold."), 2},               // Assuming this sentence hasn't been added
	{[]byte("A journey of a thousand miles begins with a single step."), 1},
}

func TestEncode(t *testing.T) {
	cms := CreateCMS(0.001, 0.001)
	cms.AddItem([]byte("The quick brown fox jumps over the lazy dog."))
	cms.AddItem([]byte("To be or not to be, that is the question."))
	cms.AddItem([]byte("The quick brown fox jumps over the lazy dog."))
	cms.AddItem([]byte("All that glitters is not gold."))
	cms.AddItem([]byte("The quick brown fox jumps over the lazy dog."))
	cms.AddItem([]byte("To be or not to be, that is the question."))
	cms.AddItem([]byte("All that glitters is not gold."))
	cms.AddItem([]byte("A journey of a thousand miles begins with a single step."))
	cms.AddItem([]byte("The quick brown fox jumps over the lazy dog."))
	cms.AddItem([]byte("To be or not to be, that is the question."))

	for _, test := range encodetests {
		if output := cms.GetFrequency(test.arg); output != test.expected {
			t.Errorf("Got %d, expected %d", output, test.expected)
		}
	}
}
