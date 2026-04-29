package config

import (
	"fmt"
	"regexp"
)

var ttlPattern = regexp.MustCompile(`^(\d+[smhd])+$`)

func validateTTL(raw string) error {
	if !ttlPattern.MatchString(raw) {
		return fmt.Errorf("invalid duration %q", raw)
	}
	return nil
}
