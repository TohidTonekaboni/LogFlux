package ingestion

import (
	"testing"

	logfluxv1 "github.com/TohidTonekaboni/LogFlux/proto/logflux/v1"
)

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		entry   *logfluxv1.LogEntry
		wantErr error
	}{
		{
			"valid entry",
			&logfluxv1.LogEntry{ServiceName: "svc", TimestampUnixMs: 1, Level: logfluxv1.Level_INFO, Message: "hello"},
			nil,
		},
		{
			"missing service_name",
			&logfluxv1.LogEntry{TimestampUnixMs: 1, Level: logfluxv1.Level_INFO, Message: "hello"},
			ErrMissingServiceName,
		},
		{
			"zero timestamp",
			&logfluxv1.LogEntry{ServiceName: "svc", Level: logfluxv1.Level_INFO, Message: "hello"},
			ErrMissingTimestamp,
		},
		{
			"unspecified level",
			&logfluxv1.LogEntry{ServiceName: "svc", TimestampUnixMs: 1, Message: "hello"},
			ErrMissingLevel,
		},
		{
			"missing message",
			&logfluxv1.LogEntry{ServiceName: "svc", TimestampUnixMs: 1, Level: logfluxv1.Level_INFO},
			ErrMissingMessage,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Validate(tt.entry); err != tt.wantErr {
				t.Errorf("Validate() = %v, want %v", err, tt.wantErr)
			}
		})
	}
}
