package esclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"

	logfluxv1 "github.com/TohidTonekaboni/LogFlux/proto/logflux/v1"
)

// Client wraps Elasticsearch with LogFlux's bulk-write contract
// (ARCHITECTURE.md §3.5): one daily index per day, with a deterministic doc
// ID wherever possible so retried writes don't create duplicates (§5).
type Client struct {
	es *elasticsearch.Client
}

func New(addresses []string, username, password string) (*Client, error) {
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: addresses,
		Username:  username,
		Password:  password,
	})
	if err != nil {
		return nil, err
	}
	return &Client{es: es}, nil
}

type bulkDoc struct {
	ServiceName string            `json:"service"`
	Environment string            `json:"environment,omitempty"`
	Timestamp   string            `json:"timestamp"`
	Level       string            `json:"level"`
	Message     string            `json:"message"`
	TraceID     string            `json:"trace_id,omitempty"`
	Fields      map[string]string `json:"fields,omitempty"`
}

// BulkIndex writes entries into their daily index (logs-YYYY.MM.DD) via one
// newline-delimited _bulk request.
func (c *Client) BulkIndex(ctx context.Context, entries []*logfluxv1.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	var buf bytes.Buffer
	for _, e := range entries {
		ts := time.UnixMilli(e.GetTimestampUnixMs()).UTC()
		index := fmt.Sprintf("logs-%s", ts.Format("2006.01.02"))

		action := map[string]any{"_index": index}
		if e.GetTraceId() != "" {
			action["_id"] = e.GetTraceId() + "-" + strconv.FormatInt(e.GetTimestampUnixMs(), 10)
		}
		meta, err := json.Marshal(map[string]any{"index": action})
		if err != nil {
			return err
		}
		doc, err := json.Marshal(bulkDoc{
			ServiceName: e.GetServiceName(),
			Environment: e.GetEnvironment(),
			Timestamp:   ts.Format(time.RFC3339Nano),
			Level:       e.GetLevel().String(),
			Message:     e.GetMessage(),
			TraceID:     e.GetTraceId(),
			Fields:      e.GetFields(),
		})
		if err != nil {
			return err
		}

		buf.Write(meta)
		buf.WriteByte('\n')
		buf.Write(doc)
		buf.WriteByte('\n')
	}

	res, err := esapi.BulkRequest{Body: bytes.NewReader(buf.Bytes())}.Do(ctx, c.es)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("bulk request failed: %s", res.String())
	}

	var parsed struct {
		Errors bool `json:"errors"`
		Items  []map[string]struct {
			Status int `json:"status"`
			Error  *struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
			} `json:"error"`
		} `json:"items"`
	}
	if err := json.NewDecoder(res.Body).Decode(&parsed); err != nil {
		return err
	}
	if parsed.Errors {
		var reasons []string
		for _, item := range parsed.Items {
			for _, r := range item {
				if r.Error != nil {
					reasons = append(reasons, fmt.Sprintf("%s: %s", r.Error.Type, r.Error.Reason))
				}
			}
		}
		return fmt.Errorf("bulk request had %d item error(s): %s", len(reasons), strings.Join(reasons, "; "))
	}

	return nil
}
