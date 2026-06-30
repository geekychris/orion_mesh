// Package orionmesh is the official Go client for OrionMesh.
//
// Mirrors the Python and Java clients' surface: a Client struct that
// handles every REST verb, plus a Queue helper for JetStream pub/sub.
//
//	c, err := orionmesh.New()
//	if err != nil { panic(err) }
//	defer c.Close()
//
//	c.Apply(`apiVersion: orionmesh.dev/v1
//	kind: Queue
//	metadata: { name: events }
//	spec: { type: work }`)
//
//	q := c.Queue("events")
//	q.Pub(map[string]any{"hello": "world"})
//	for row := range q.Sub("readers", 5) { fmt.Println(row) }
package orionmesh

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"gopkg.in/yaml.v3"
)

// Client is the high-level OrionMesh client. All methods are
// synchronous. Safe for concurrent use.
type Client struct {
	Controller string
	NATSURL    string
	Token      string
	Timeout    time.Duration

	http    *http.Client
	natsOnce sync.Once
	nc      *nats.Conn
	js      jetstream.JetStream
	natsErr error
}

// Option mutates a Client during New().
type Option func(*Client)

func WithController(url string) Option { return func(c *Client) { c.Controller = strings.TrimRight(url, "/") } }
func WithNATSURL(url string) Option    { return func(c *Client) { c.NATSURL = url } }
func WithToken(t string) Option        { return func(c *Client) { c.Token = t } }
func WithTimeout(d time.Duration) Option { return func(c *Client) { c.Timeout = d } }

// New returns a Client configured from environment defaults, with any
// Option overrides applied in order.
func New(opts ...Option) (*Client, error) {
	c := &Client{
		Controller: envOr("ORION_CONTROLLER_URL", "http://127.0.0.1:7878"),
		NATSURL:    envOr("NATS_URL", "nats://127.0.0.1:4222"),
		Token:      os.Getenv("ORION_CLUSTER_TOKEN"),
		Timeout:    10 * time.Second,
	}
	c.Controller = strings.TrimRight(c.Controller, "/")
	for _, o := range opts {
		o(c)
	}
	c.http = &http.Client{Timeout: c.Timeout}
	return c, nil
}

// Close releases the NATS connection (if any). Safe to call repeatedly.
func (c *Client) Close() {
	if c.nc != nil {
		c.nc.Close()
		c.nc = nil
		c.js = nil
	}
}

// ----------------------------------------------------------------- REST

// Health returns true if /health responds 2xx.
func (c *Client) Health() bool {
	resp, err := c.send("GET", "/health", "", "")
	if err != nil { return false }
	defer resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

// Resource is a lightweight view over a Resource document.
type Resource struct {
	Kind       string                 `json:"kind"`
	APIVersion string                 `json:"apiVersion"`
	Metadata   map[string]any         `json:"metadata"`
	Spec       map[string]any         `json:"spec"`
	Status     map[string]any         `json:"status,omitempty"`
}

func (r Resource) Name() string {
	if r.Metadata == nil { return "" }
	if n, ok := r.Metadata["name"].(string); ok { return n }
	return ""
}

// Get fetches a single resource by kind + name.
func (c *Client) Get(kind, name string) (*Resource, error) {
	kind = CanonicalKind(kind)
	resp, err := c.send("GET", "/v1/resources/"+kind+"/"+name, "", "")
	if err != nil { return nil, err }
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		return nil, &ResourceNotFoundError{Kind: kind, Name: name}
	}
	if resp.StatusCode/100 != 2 {
		return nil, c.errorFrom(resp, "GET "+kind+"/"+name)
	}
	var r Resource
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return nil, err
	}
	return &r, nil
}

// List returns every resource of a given kind.
func (c *Client) List(kind string) ([]Resource, error) {
	kind = CanonicalKind(kind)
	resp, err := c.send("GET", "/v1/resources/"+kind, "", "")
	if err != nil { return nil, err }
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, c.errorFrom(resp, "GET "+kind)
	}
	var rs []Resource
	if err := json.NewDecoder(resp.Body).Decode(&rs); err != nil {
		return nil, err
	}
	return rs, nil
}

// Apply POSTs a YAML body to /v1/resources/apply. Returns the parsed JSON response.
func (c *Client) Apply(body string) (map[string]any, error) {
	return c.applyBody(body, "application/yaml")
}

// ApplyMap encodes a map as YAML and applies it.
func (c *Client) ApplyMap(m map[string]any) (map[string]any, error) {
	yamlBytes, err := yaml.Marshal(m)
	if err != nil { return nil, err }
	return c.applyBody(string(yamlBytes), "application/yaml")
}

func (c *Client) applyBody(body, contentType string) (map[string]any, error) {
	resp, err := c.send("POST", "/v1/resources/apply", body, contentType)
	if err != nil { return nil, err }
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		text, _ := io.ReadAll(resp.Body)
		return nil, &ApplyFailedError{Status: resp.StatusCode, Detail: string(text)}
	}
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Client) Delete(kind, name string) (bool, error) {
	kind = CanonicalKind(kind)
	resp, err := c.send("DELETE", "/v1/resources/"+kind+"/"+name, "", "")
	if err != nil { return false, err }
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		return false, &ResourceNotFoundError{Kind: kind, Name: name}
	}
	if resp.StatusCode/100 != 2 {
		return false, c.errorFrom(resp, "DELETE "+kind+"/"+name)
	}
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return false, err
	}
	d, _ := out["deleted"].(bool)
	return d, nil
}

func (c *Client) Dispatch(kind, name string) (map[string]any, error) {
	kind = CanonicalKind(kind)
	resp, err := c.send("POST", "/v1/dispatch/"+kind+"/"+name, "", "application/json")
	if err != nil { return nil, err }
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		text, _ := io.ReadAll(resp.Body)
		return nil, &DispatchFailedError{Detail: fmt.Sprintf("dispatch %s/%s: %d %s", kind, name, resp.StatusCode, text)}
	}
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	return out, nil
}

func (c *Client) Logs(kind, name string, since int) (map[string]any, error) {
	kind = CanonicalKind(kind)
	resp, err := c.send("GET", fmt.Sprintf("/v1/logs/%s/%s?since=%d", kind, name, since), "", "")
	if err != nil { return nil, err }
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, c.errorFrom(resp, "GET logs "+kind+"/"+name)
	}
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	return out, nil
}

// Find issues a POST /v1/find with a capability selector.
func (c *Client) Find(selector map[string]any) ([]Resource, error) {
	body, err := json.Marshal(selector)
	if err != nil { return nil, err }
	resp, err := c.send("POST", "/v1/find", string(body), "application/json")
	if err != nil { return nil, err }
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, c.errorFrom(resp, "POST find")
	}
	var rs []Resource
	json.NewDecoder(resp.Body).Decode(&rs)
	return rs, nil
}

func (c *Client) Doctor() (map[string]any, error) {
	resp, err := c.send("GET", "/v1/diag/system", "", "")
	if err != nil { return nil, err }
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, c.errorFrom(resp, "GET diag")
	}
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	return out, nil
}

// ----------------------------------------------------------------- queues

// Queue returns a helper for one named queue.
func (c *Client) Queue(name string) *Queue {
	return &Queue{client: c, name: name}
}

// ----------------------------------------------------------------- internal

func (c *Client) send(method, path, body, contentType string) (*http.Response, error) {
	u, err := url.Parse(c.Controller + path)
	if err != nil { return nil, err }
	var rdr io.Reader
	if body != "" { rdr = strings.NewReader(body) }
	req, err := http.NewRequestWithContext(context.Background(), method, u.String(), rdr)
	if err != nil { return nil, err }
	if contentType != "" {
		req.Header.Set("content-type", contentType)
	}
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	return c.http.Do(req)
}

func (c *Client) errorFrom(resp *http.Response, op string) error {
	text, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("%s: status %d: %s", op, resp.StatusCode, text)
}

func (c *Client) nats() (jetstream.JetStream, error) {
	c.natsOnce.Do(func() {
		opts := []nats.Option{}
		if c.Token != "" {
			opts = append(opts, nats.Token(c.Token))
		}
		nc, err := nats.Connect(c.NATSURL, opts...)
		if err != nil {
			c.natsErr = err
			return
		}
		js, err := jetstream.New(nc)
		if err != nil {
			nc.Close()
			c.natsErr = err
			return
		}
		c.nc = nc
		c.js = js
	})
	return c.js, c.natsErr
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// CanonicalKind normalises plural / lowercase to PascalCase singular, matching
// the Rust CLI and Python client.
func CanonicalKind(s string) string {
	if strings.HasSuffix(s, "s") && !strings.HasSuffix(s, "ss") {
		s = s[:len(s)-1]
	}
	if s == "" { return s }
	return strings.ToUpper(s[:1]) + s[1:]
}

// readAll discards a response body so the connection can be reused.
func readAll(r io.Reader) []byte {
	b, _ := io.ReadAll(r)
	return b
}

// ensure unused imports are kept under refactor:
var _ = bytes.NewReader
