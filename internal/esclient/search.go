package esclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/elastic/go-elasticsearch/v8/esapi"
)

const logsIndexPattern = "logs-*"

type LogQuery struct {
	Service string
	Level   string
	From    time.Time // zero value means unbounded
	To      time.Time
	Q       string // free-text match against message
	Size    int    // defaults to 100 if <= 0
}

// LogHit is one matched log entry, with its Elasticsearch document ID so
// callers can round-trip it through GET /logs/:id.
type LogHit struct {
	ID          string            `json:"id"`
	ServiceName string            `json:"service"`
	Environment string            `json:"environment,omitempty"`
	Timestamp   string            `json:"timestamp"`
	Level       string            `json:"level"`
	Message     string            `json:"message"`
	TraceID     string            `json:"trace_id,omitempty"`
	Fields      map[string]string `json:"fields,omitempty"`
}

func (c *Client) SearchLogs(ctx context.Context, q LogQuery) ([]LogHit, error) {
	size := q.Size
	if size <= 0 {
		size = 100
	}

	var filters []map[string]any
	if q.Service != "" {
		filters = append(filters, map[string]any{"term": map[string]any{"service": q.Service}})
	}
	if q.Level != "" {
		filters = append(filters, map[string]any{"term": map[string]any{"level": q.Level}})
	}
	if !q.From.IsZero() || !q.To.IsZero() {
		rng := map[string]any{}
		if !q.From.IsZero() {
			rng["gte"] = q.From.Format(time.RFC3339Nano)
		}
		if !q.To.IsZero() {
			rng["lte"] = q.To.Format(time.RFC3339Nano)
		}
		filters = append(filters, map[string]any{"range": map[string]any{"timestamp": rng}})
	}
	if q.Q != "" {
		filters = append(filters, map[string]any{"match": map[string]any{"message": q.Q}})
	}

	query := map[string]any{"match_all": map[string]any{}}
	if len(filters) > 0 {
		query = map[string]any{"bool": map[string]any{"filter": filters}}
	}

	body, err := json.Marshal(map[string]any{
		"query": query,
		"sort":  []map[string]any{{"timestamp": map[string]any{"order": "desc"}}},
		"size":  size,
	})
	if err != nil {
		return nil, err
	}

	res, err := esapi.SearchRequest{
		Index: []string{logsIndexPattern},
		Body:  bytes.NewReader(body),
	}.Do(ctx, c.es)
	if err != nil {
		return nil, err
	}
	defer func() { _ = res.Body.Close() }()

	if res.IsError() {
		return nil, fmt.Errorf("search failed: %s", res.String())
	}
	return decodeHits(res)
}

// GetLogByID fetches a single entry. Docs are spread across daily indices, so
// this is an _id query over the logs-* pattern rather than a direct GET.
func (c *Client) GetLogByID(ctx context.Context, id string) (*LogHit, error) {
	body, err := json.Marshal(map[string]any{
		"query": map[string]any{"ids": map[string]any{"values": []string{id}}},
		"size":  1,
	})
	if err != nil {
		return nil, err
	}

	res, err := esapi.SearchRequest{
		Index: []string{logsIndexPattern},
		Body:  bytes.NewReader(body),
	}.Do(ctx, c.es)
	if err != nil {
		return nil, err
	}
	defer func() { _ = res.Body.Close() }()

	if res.IsError() {
		return nil, fmt.Errorf("search failed: %s", res.String())
	}

	hits, err := decodeHits(res)
	if err != nil {
		return nil, err
	}
	if len(hits) == 0 {
		return nil, nil // not found; handler maps this to 404
	}
	return &hits[0], nil
}

// ListServices returns the distinct service names seen across logs-*.
func (c *Client) ListServices(ctx context.Context) ([]string, error) {
	body, err := json.Marshal(map[string]any{
		"size": 0,
		"aggs": map[string]any{
			"services": map[string]any{
				"terms": map[string]any{"field": "service", "size": 1000},
			},
		},
	})
	if err != nil {
		return nil, err
	}

	res, err := esapi.SearchRequest{
		Index: []string{logsIndexPattern},
		Body:  bytes.NewReader(body),
	}.Do(ctx, c.es)
	if err != nil {
		return nil, err
	}
	defer func() { _ = res.Body.Close() }()

	if res.IsError() {
		return nil, fmt.Errorf("aggregation failed: %s", res.String())
	}

	var parsed struct {
		Aggregations struct {
			Services struct {
				Buckets []struct {
					Key string `json:"key"`
				} `json:"buckets"`
			} `json:"services"`
		} `json:"aggregations"`
	}
	if err := json.NewDecoder(res.Body).Decode(&parsed); err != nil {
		return nil, err
	}

	services := make([]string, 0, len(parsed.Aggregations.Services.Buckets))
	for _, b := range parsed.Aggregations.Services.Buckets {
		services = append(services, b.Key)
	}
	return services, nil
}

func decodeHits(res *esapi.Response) ([]LogHit, error) {
	var parsed struct {
		Hits struct {
			Hits []struct {
				ID     string          `json:"_id"`
				Source json.RawMessage `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := json.NewDecoder(res.Body).Decode(&parsed); err != nil {
		return nil, err
	}

	hits := make([]LogHit, 0, len(parsed.Hits.Hits))
	for _, h := range parsed.Hits.Hits {
		var hit LogHit
		if err := json.Unmarshal(h.Source, &hit); err != nil {
			return nil, err
		}
		hit.ID = h.ID
		hits = append(hits, hit)
	}
	return hits, nil
}
