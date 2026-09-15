package forwarder

import (
	"testing"
)

func TestForwarderNewValidation(t *testing.T) {
	_, err := New([]string{}, "trades.normalized")
	if err == nil {
		t.Error("expected error when brokers list is empty")
	}

	f, err := New([]string{"localhost:9092"}, "")
	if err != nil {
		t.Fatalf("unexpected error creating forwarder: %v", err)
	}
	defer f.Close()
}
