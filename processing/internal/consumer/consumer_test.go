package consumer

import (
	"testing"
)

func TestConsumerNewValidation(t *testing.T) {
	_, err := New([]string{}, "group1", "trades.raw")
	if err == nil {
		t.Error("expected error when brokers list is empty")
	}

	c, err := New([]string{"localhost:9092"}, "group1", "")
	if err != nil {
		t.Fatalf("unexpected error creating consumer: %v", err)
	}
	defer c.Close()
}
