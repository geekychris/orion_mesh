package orionmesh

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ----------------------------------------------------------- canonical kind

func TestCanonicalKindNormalises(t *testing.T) {
	cases := map[string]string{
		"service":  "Service",
		"Service":  "Service",
		"services": "Service",
		"queue":    "Queue",
		"address":  "Address",
	}
	for in, want := range cases {
		got := CanonicalKind(in)
		if got != want {
			t.Errorf("CanonicalKind(%q) = %q, want %q", in, got, want)
		}
	}
}

// ----------------------------------------------------------- helper

type stub struct {
	method, path string
	status       int
	body         string
	lastBody     string
	lastAuth     string
}

func server(t *testing.T, stubs []*stub) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, s := range stubs {
			if r.Method == s.method && (r.URL.Path == s.path || r.URL.String() == s.path) {
				body, _ := io.ReadAll(r.Body)
				s.lastBody = string(body)
				s.lastAuth = r.Header.Get("Authorization")
				w.WriteHeader(s.status)
				w.Write([]byte(s.body))
				return
			}
		}
		w.WriteHeader(404)
		w.Write([]byte("no stub for " + r.Method + " " + r.URL.String()))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newClientFor(base string, token string) *Client {
	c, _ := New(WithController(base), WithNATSURL("nats://0"), WithToken(token))
	return c
}

// ----------------------------------------------------------- tests

func TestHealthOk(t *testing.T) {
	srv := server(t, []*stub{{method: "GET", path: "/health", status: 200, body: "ok"}})
	c := newClientFor(srv.URL, "")
	defer c.Close()
	if !c.Health() {
		t.Fatal("expected health true")
	}
}

func TestHealthFalseOnError(t *testing.T) {
	srv := server(t, []*stub{{method: "GET", path: "/health", status: 500, body: "boom"}})
	c := newClientFor(srv.URL, "")
	defer c.Close()
	if c.Health() {
		t.Fatal("expected health false")
	}
}

func TestGetReturnsResource(t *testing.T) {
	srv := server(t, []*stub{{
		method: "GET", path: "/v1/resources/Service/web", status: 200,
		body: `{"kind":"Service","apiVersion":"orionmesh.dev/v1","metadata":{"name":"web"},"spec":{"replicas":2}}`,
	}})
	c := newClientFor(srv.URL, "")
	defer c.Close()
	r, err := c.Get("Service", "web")
	if err != nil { t.Fatal(err) }
	if r.Kind != "Service" || r.Name() != "web" {
		t.Errorf("got %+v", r)
	}
	if v, _ := r.Spec["replicas"].(float64); v != 2 {
		t.Errorf("spec.replicas = %v", r.Spec["replicas"])
	}
}

func TestGetPluralAccepted(t *testing.T) {
	srv := server(t, []*stub{{
		method: "GET", path: "/v1/resources/Service/web", status: 200,
		body: `{"kind":"Service","metadata":{"name":"web"},"spec":{}}`,
	}})
	c := newClientFor(srv.URL, "")
	defer c.Close()
	if _, err := c.Get("services", "web"); err != nil {
		t.Fatal(err)
	}
}

func TestGet404RaisesResourceNotFound(t *testing.T) {
	srv := server(t, []*stub{{
		method: "GET", path: "/v1/resources/Service/missing", status: 404, body: "",
	}})
	c := newClientFor(srv.URL, "")
	defer c.Close()
	_, err := c.Get("Service", "missing")
	if err == nil { t.Fatal("expected error") }
	nf, ok := err.(*ResourceNotFoundError)
	if !ok {
		t.Fatalf("expected ResourceNotFoundError, got %T", err)
	}
	if nf.Kind != "Service" || nf.Name != "missing" {
		t.Errorf("wrong details: %+v", nf)
	}
}

func TestApplyYAML(t *testing.T) {
	apply := &stub{
		method: "POST", path: "/v1/resources/apply", status: 200,
		body: `{"applied":true,"kind":"Service","name":"x"}`,
	}
	srv := server(t, []*stub{apply})
	c := newClientFor(srv.URL, "")
	defer c.Close()
	out, err := c.Apply("kind: Service\nmetadata:\n  name: x\n")
	if err != nil { t.Fatal(err) }
	if applied, _ := out["applied"].(bool); !applied {
		t.Errorf("expected applied=true, got %+v", out)
	}
	if !strings.Contains(apply.lastBody, "kind: Service") {
		t.Errorf("server didn't see yaml: %s", apply.lastBody)
	}
}

func TestApply4xxRaisesApplyFailed(t *testing.T) {
	srv := server(t, []*stub{{
		method: "POST", path: "/v1/resources/apply", status: 400, body: "invalid yaml at line 3",
	}})
	c := newClientFor(srv.URL, "")
	defer c.Close()
	_, err := c.Apply("kind: bogus\n")
	if err == nil { t.Fatal("expected error") }
	af, ok := err.(*ApplyFailedError)
	if !ok { t.Fatalf("expected ApplyFailedError, got %T", err) }
	if af.Status != 400 { t.Errorf("status = %d", af.Status) }
}

func TestDeleteReturnsBool(t *testing.T) {
	srv := server(t, []*stub{{
		method: "DELETE", path: "/v1/resources/Service/web", status: 200,
		body: `{"deleted":true}`,
	}})
	c := newClientFor(srv.URL, "")
	defer c.Close()
	ok, err := c.Delete("Service", "web")
	if err != nil { t.Fatal(err) }
	if !ok { t.Error("expected true") }
}

func TestDispatchOk(t *testing.T) {
	srv := server(t, []*stub{{
		method: "POST", path: "/v1/dispatch/Service/web", status: 200,
		body: `{"instance_id":"abc","node":"n1"}`,
	}})
	c := newClientFor(srv.URL, "")
	defer c.Close()
	out, err := c.Dispatch("Service", "web")
	if err != nil { t.Fatal(err) }
	if n, _ := out["node"].(string); n != "n1" {
		t.Errorf("node = %v", out["node"])
	}
}

func TestDispatchFailed(t *testing.T) {
	srv := server(t, []*stub{{
		method: "POST", path: "/v1/dispatch/Service/web", status: 400, body: "no live nodes",
	}})
	c := newClientFor(srv.URL, "")
	defer c.Close()
	_, err := c.Dispatch("Service", "web")
	if err == nil { t.Fatal("expected error") }
	if _, ok := err.(*DispatchFailedError); !ok {
		t.Errorf("got %T", err)
	}
}

func TestFindPostsSelector(t *testing.T) {
	find := &stub{
		method: "POST", path: "/v1/find", status: 200,
		body: `[{"kind":"Service","metadata":{"name":"llm"},"spec":{}}]`,
	}
	srv := server(t, []*stub{find})
	c := newClientFor(srv.URL, "")
	defer c.Close()
	out, err := c.Find(map[string]any{"llm": map[string]any{"min_vram_gb": map[string]any{"gte": 24}}})
	if err != nil { t.Fatal(err) }
	if len(out) != 1 || out[0].Name() != "llm" {
		t.Errorf("got %+v", out)
	}
	var sel map[string]any
	json.Unmarshal([]byte(find.lastBody), &sel)
	if _, ok := sel["llm"]; !ok {
		t.Errorf("selector not forwarded: %s", find.lastBody)
	}
}

func TestTokenInjectedAsBearer(t *testing.T) {
	hi := &stub{method: "GET", path: "/health", status: 200, body: "ok"}
	srv := server(t, []*stub{hi})
	c := newClientFor(srv.URL, "secret")
	defer c.Close()
	c.Health()
	if hi.lastAuth != "Bearer secret" {
		t.Errorf("expected Bearer secret, got %q", hi.lastAuth)
	}
}

func TestNoTokenMeansNoAuthHeader(t *testing.T) {
	hi := &stub{method: "GET", path: "/health", status: 200, body: "ok"}
	srv := server(t, []*stub{hi})
	c := newClientFor(srv.URL, "")
	defer c.Close()
	c.Health()
	if hi.lastAuth != "" {
		t.Errorf("unexpected auth: %q", hi.lastAuth)
	}
}

func TestListReturnsArray(t *testing.T) {
	srv := server(t, []*stub{{
		method: "GET", path: "/v1/resources/Service", status: 200,
		body: `[{"kind":"Service","metadata":{"name":"a"},"spec":{}},{"kind":"Service","metadata":{"name":"b"},"spec":{}}]`,
	}})
	c := newClientFor(srv.URL, "")
	defer c.Close()
	rs, err := c.List("Service")
	if err != nil { t.Fatal(err) }
	if len(rs) != 2 || rs[0].Name() != "a" {
		t.Errorf("got %+v", rs)
	}
}

func TestLogsPassesSinceParam(t *testing.T) {
	logs := &stub{
		method: "GET", path: "/v1/logs/Service/web?since=42", status: 200,
		body: `{"total":5,"entries":[]}`,
	}
	srv := server(t, []*stub{logs})
	c := newClientFor(srv.URL, "")
	defer c.Close()
	out, err := c.Logs("Service", "web", 42)
	if err != nil { t.Fatal(err) }
	if v, _ := out["total"].(float64); v != 5 {
		t.Errorf("total = %v", out["total"])
	}
}

func TestDoctorPassesThrough(t *testing.T) {
	srv := server(t, []*stub{{
		method: "GET", path: "/v1/diag/system", status: 200, body: `{"agents":1}`,
	}})
	c := newClientFor(srv.URL, "")
	defer c.Close()
	out, err := c.Doctor()
	if err != nil { t.Fatal(err) }
	if v, _ := out["agents"].(float64); v != 1 {
		t.Errorf("agents = %v", out["agents"])
	}
}
