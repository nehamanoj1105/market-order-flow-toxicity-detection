package model

import (
	"strconv"
	"testing"
)

func TestFmtInt(t *testing.T) {
	cases := []int64{0, 1, -1, 42, 1704153600000, -9223372036854775808, 9223372036854775807}
	for _, c := range cases {
		if got, want := fmtInt(c), strconv.FormatInt(c, 10); got != want {
			t.Errorf("fmtInt(%d) = %q, want %q", c, got, want)
		}
	}
}
