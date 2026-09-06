package api

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/TohidTonekaboni/LogFlux/internal/esclient"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// LogSearcher is the subset of *esclient.Client the API needs.
type LogSearcher interface {
	SearchLogs(ctx context.Context, q esclient.LogQuery) ([]esclient.LogHit, error)
	GetLogByID(ctx context.Context, id string) (*esclient.LogHit, error)
	ListServices(ctx context.Context) ([]string, error)
}

type API struct {
	es LogSearcher
}

func New(es LogSearcher) *API {
	return &API{es: es}
}

func (a *API) Routes() *gin.Engine {
	r := gin.Default()
	r.GET("/healthz", a.healthz)
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))
	r.GET("/logs", a.listLogs)
	r.GET("/logs/:id", a.getLog)
	r.GET("/services", a.listServices)
	return r
}

func (a *API) healthz(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (a *API) listLogs(c *gin.Context) {
	q := esclient.LogQuery{
		Service: c.Query("service"),
		Level:   c.Query("level"),
		Q:       c.Query("q"),
	}

	if from := c.Query("from"); from != "" {
		t, err := time.Parse(time.RFC3339, from)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid from: " + err.Error()})
			return
		}
		q.From = t
	}
	if to := c.Query("to"); to != "" {
		t, err := time.Parse(time.RFC3339, to)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid to: " + err.Error()})
			return
		}
		q.To = t
	}
	if size := c.Query("size"); size != "" {
		n, err := strconv.Atoi(size)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid size: " + err.Error()})
			return
		}
		q.Size = n
	}

	hits, err := a.es.SearchLogs(c.Request.Context(), q)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"logs": hits})
}

func (a *API) getLog(c *gin.Context) {
	hit, err := a.es.GetLogByID(c.Request.Context(), c.Param("id"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if hit == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "log not found"})
		return
	}
	c.JSON(http.StatusOK, hit)
}

func (a *API) listServices(c *gin.Context) {
	services, err := a.es.ListServices(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"services": services})
}
