package hyper_log_log

import (
	"testing"
)

func TestHyperLogLog(t *testing.T) {
	hll := NewHyperLogLog(4)

	data1 := []string{"ana", "ivana", "milan", "milica", "danica", "stefan", "vuk"}
	data2 := []string{"10", "stanoje", "proba", "test", "test222", "abcdefg", "123"}

	for _, d := range data1 {
		hll = hll.Add([]byte(d))
	}

	if hll.Count() <= 1 {
		t.Fatalf("HyperLogLog failed for data1. Estimate: %v", hll.Count())
	}

	for _, d := range data2 {
		hll = hll.Add([]byte(d))
	}

	if hll.Count() <= 1 {
		t.Fatalf("HyperLogLog failed for data2. Estimate: %v", hll.Count())
	}

	serialization := hll.Serialize()
	hll = Deserialize(serialization)

	if hll.Count() <= 1 {
		t.Fatalf("HyperLogLog serialization failed. Estimate: %v", hll.Count())
	}
}
