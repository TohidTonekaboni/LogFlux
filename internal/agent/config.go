package agent

import "time"

// Config controls the client SDK per ARCHITECTURE.md §3.1.
type Config struct {
	ServerAddr    string
	BatchSize     int
	FlushInterval time.Duration
	QueueCapacity int
	StaticLabels  map[string]string // merged into each entry's Fields, e.g. service name/environment
}

func (c Config) withDefaults() Config {
	if c.BatchSize <= 0 {
		c.BatchSize = 100
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = time.Second
	}
	if c.QueueCapacity <= 0 {
		c.QueueCapacity = 1000
	}
	return c
}
