package common

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"strconv"
	"strings"
)

func Getenv(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func GetenvInt(name string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func NewRequestID() string {
	buf := make([]byte, 6)
	if _, err := rand.Read(buf); err != nil {
		return "request-fallback"
	}
	return "request-" + hex.EncodeToString(buf)
}

func BoolString(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func ReservationID(requestID string) string {
	suffix := strings.TrimSpace(requestID)
	if suffix == "" {
		suffix = NewRequestID()
	}
	if len(suffix) > 6 {
		suffix = suffix[len(suffix)-6:]
	}
	return "reservation-" + suffix
}
