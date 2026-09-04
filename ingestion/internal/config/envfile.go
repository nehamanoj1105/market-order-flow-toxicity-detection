package config

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// applyEnvFile reads a docker/.env style file and populates the process
// environment for any key that is not already set. Real environment variables
// always win, which keeps container deployments predictable.
//
// Supported syntax: KEY=VALUE, optional `export ` prefix, `#` comments (whole
// line, or trailing after " #"), blank lines ignored, surrounding single or
// double quotes stripped.
func applyEnvFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("env file: %w", err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	line := 0
	for sc.Scan() {
		line++
		raw := strings.TrimSpace(sc.Text())
		if raw == "" || strings.HasPrefix(raw, "#") {
			continue
		}
		raw = strings.TrimPrefix(raw, "export ")
		// Inline comments ("KEY=value # note") are stripped for convenience, but
		// only when a space precedes the hash, so values that legitimately
		// contain one (passwords, hashes) survive untouched.
		if !strings.HasPrefix(raw, "#") {
			if idx := strings.Index(raw, " #"); idx >= 0 {
				raw = strings.TrimSpace(raw[:idx])
			}
		}
		key, value, ok := strings.Cut(raw, "=")
		if !ok {
			return fmt.Errorf("env file %s:%d: expected KEY=VALUE", path, line)
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" {
			continue
		}
		if len(value) >= 2 {
			if (strings.HasPrefix(value, `"`) && strings.HasSuffix(value, `"`)) ||
				(strings.HasPrefix(value, `'`) && strings.HasSuffix(value, `'`)) {
				value = value[1 : len(value)-1]
			}
		}
		if _, already := os.LookupEnv(key); already {
			continue
		}
		if err := os.Setenv(key, value); err != nil {
			return fmt.Errorf("env file %s:%d: %w", path, line, err)
		}
	}
	return sc.Err()
}
