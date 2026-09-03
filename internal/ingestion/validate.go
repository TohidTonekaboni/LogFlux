package ingestion

import (
	"errors"

	logfluxv1 "github.com/TohidTonekaboni/LogFlux/proto/logflux/v1"
)

var (
	ErrMissingServiceName = errors.New("service_name is required")
	ErrMissingTimestamp   = errors.New("timestamp_unix_ms must be positive")
	ErrMissingLevel       = errors.New("level must be set")
	ErrMissingMessage     = errors.New("message is required")
)

// Validate applies the schema-validation rules
func Validate(entry *logfluxv1.LogEntry) error {
	if entry.GetServiceName() == "" {
		return ErrMissingServiceName
	}
	if entry.GetTimestampUnixMs() <= 0 {
		return ErrMissingTimestamp
	}
	if entry.GetLevel() == logfluxv1.Level_LEVEL_UNSPECIFIED {
		return ErrMissingLevel
	}
	if entry.GetMessage() == "" {
		return ErrMissingMessage
	}
	return nil
}
