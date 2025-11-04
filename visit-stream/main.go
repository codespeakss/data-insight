package main

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	kafka "github.com/segmentio/kafka-go"
)

//go:embed templates/index.html
var embeddedFiles embed.FS

type VisitActivity struct {
	Timestamp   int64             `json:"timestamp"`    // unix milliseconds
	ISOTime     string            `json:"iso_time"`     // readable time
	HTTPMethod  string            `json:"http_method"`
	Host        string            `json:"host"`         // Host header (target domain)
	URL         string            `json:"url"`          // full URL or requestURI
	QueryParams map[string]string `json:"query_params"`
	Headers     map[string]string `json:"headers"`
	Body        string            `json:"body,omitempty"`
	ClientIP    string            `json:"client_ip"`
}

func main() {
	// Kafka writer (shared)
	kWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{"kaixina.site:9092"},
		Topic:    "visit-activity",
		Balancer: &kafka.LeastBytes{},
		Async:    false, // sync write to handle errors simply; change to true if needed
	})
	defer func() {
		if err := kWriter.Close(); err != nil {
			log.Printf("kafka writer close error: %v", err)
		}
	}()

	r := gin.Default()

	// Middleware that records request info and sends to Kafka
	r.Use(func(c *gin.Context) {
		start := time.Now()
		// Read small body safely (non-blocking); limit to e.g. 4KB
		var bodyStr string
		if c.Request.Body != nil {
			// copy the body for downstream handlers
			buf := new(bytes.Buffer)
			limit := int64(4096) // 4 KB
			n, _ := io.CopyN(buf, c.Request.Body, limit)
			// restore the remaining body (if any) plus what we read
			// we must reconstruct the Body for the next handlers
			rest, _ := io.ReadAll(c.Request.Body)
			full := append(buf.Bytes()[:n], rest...)
			bodyStr = string(full)
			// put back a new ReadCloser for downstream
			c.Request.Body = io.NopCloser(bytes.NewReader(full))
		}

		// gather headers (as map[string]string, last value only)
		headers := make(map[string]string)
		for k, v := range c.Request.Header {
			headers[k] = strings.Join(v, ",")
		}

		// gather query params (take first value)
		q := make(map[string]string)
		for k, vals := range c.Request.URL.Query() {
			if len(vals) > 0 {
				q[k] = vals[0]
			}
		}

		// construct full URL (try to include scheme)
		scheme := "http"
		if c.Request.TLS != nil {
			scheme = "https"
		} else if proto := c.GetHeader("X-Forwarded-Proto"); proto != "" {
			scheme = proto
		}
		// If Request.Host is empty (rare), fall back to URL.Host
		host := c.Request.Host
		if host == "" {
			host = c.Request.URL.Host
		}
		fullURL := scheme + "://" + host + c.Request.RequestURI

		clientIP := clientIPFromRequest(c.Request)

		activity := VisitActivity{
			Timestamp:   start.UnixNano() / int64(time.Millisecond),
			ISOTime:     start.UTC().Format(time.RFC3339Nano),
			HTTPMethod:  c.Request.Method,
			Host:        host,
			URL:         fullURL,
			QueryParams: q,
			Headers:     headers,
			Body:        bodyStr,
			ClientIP:    clientIP,
		}

		// serialize to JSON
		data, err := json.Marshal(activity)
		if err != nil {
			log.Printf("failed to marshal activity: %v", err)
		} else {
			// send to kafka (synchronous)
			msg := kafka.Message{
				Value: data,
				Time:  time.Now(),
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := kWriter.WriteMessages(ctx, msg); err != nil {
				// log error but don't block the request; you may want to add retry/metrics
				log.Printf("kafka write error: %v", err)
			}
		}

		// continue serving
		c.Next()
	})

	// root serves embedded index.html
	r.GET("/", func(c *gin.Context) {
		data, err := embeddedFiles.ReadFile("templates/index.html")
		if err != nil {
			c.String(http.StatusInternalServerError, "internal error")
			return
		}
		c.Data(http.StatusOK, "text/html; charset=utf-8", data)
	})

	// example echo route to test query/body capture
	r.Any("/echo", func(c *gin.Context) {
		// echo back info for convenience
		c.JSON(http.StatusOK, gin.H{
			"method": c.Request.Method,
			"query":  c.Request.URL.Query(),
		})
	})

	// start server
	log.Println("starting server on :80")
	if err := r.Run(":80"); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

// helper to get client IP considering proxies
func clientIPFromRequest(r *http.Request) string {
	// check X-Forwarded-For
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// take the first IP in the list
		parts := strings.Split(xff, ",")
		if len(parts) > 0 {
			return strings.TrimSpace(parts[0])
		}
	}
	if xr := r.Header.Get("X-Real-IP"); xr != "" {
		return xr
	}
	// fall back to remote address
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

