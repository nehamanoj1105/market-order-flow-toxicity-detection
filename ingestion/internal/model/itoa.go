package model

// fmtInt is a dependency-free int64 formatter used on the hot path
// (building message headers). It matches strconv.FormatInt for all inputs.
func fmtInt(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		// math.MinInt64 would overflow on negation, so divide first.
		if v == -9223372036854775808 {
			return "-9223372036854775808"
		}
		v = -v
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
