package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type RuntimeStatus struct {
	StartedMs   int64
	SpotRows    atomic.Uint64
	FuturesRows atomic.Uint64
	EventRows   atomic.Uint64
	LastError   atomic.Value
	Feeds       *Feeds
	Recent      *RecentBuffer
}

type APILimiter struct {
	mu    sync.Mutex
	hits  []int64
	limit int
}

func NewAPILimiter(n int) *APILimiter { return &APILimiter{limit: n} }
func (l *APILimiter) Allow() bool {
	now := time.Now().UnixMilli()
	cut := now - 60_000
	l.mu.Lock()
	defer l.mu.Unlock()
	j := 0
	for _, x := range l.hits {
		if x >= cut {
			l.hits[j] = x
			j++
		}
	}
	l.hits = l.hits[:j]
	if len(l.hits) >= l.limit {
		return false
	}
	l.hits = append(l.hits, now)
	return true
}
func ctEq(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
func bearer(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if !strings.HasPrefix(h, "Bearer ") {
		return ""
	}
	return strings.TrimSpace(strings.TrimPrefix(h, "Bearer "))
}

func signSession(secret string, exp int64) string {
	msg := fmt.Sprintf("admin|%d", exp)
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(msg))
	return base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf("%d", exp))) + "." + base64.RawURLEncoding.EncodeToString(h.Sum(nil))
}
func validSession(secret, val string) bool {
	p := strings.Split(val, ".")
	if len(p) != 2 {
		return false
	}
	eb, err := base64.RawURLEncoding.DecodeString(p[0])
	if err != nil {
		return false
	}
	exp, err := strconv.ParseInt(string(eb), 10, 64)
	if err != nil || time.Now().Unix() > exp {
		return false
	}
	want := signSession(secret, exp)
	return hmac.Equal([]byte(val), []byte(want))
}

var loginTpl = template.Must(template.New("login").Parse(`<!doctype html><meta charset="utf-8"><title>MarketDepth</title><style>body{font-family:system-ui;max-width:520px;margin:60px auto;padding:0 20px}input,button{font-size:16px;padding:10px;margin:5px 0;width:100%;box-sizing:border-box}.e{color:#b00}</style><h1>MarketDepth</h1>{{if .}}<p class=e>{{.}}</p>{{end}}<form method=post action=/login><input type=password name=password placeholder="Admin password" autofocus><button>Login</button></form>`))
var dashTpl = template.Must(template.New("dash").Parse(`<!doctype html><meta charset="utf-8"><title>MarketDepth</title><style>body{font-family:system-ui;max-width:1050px;margin:30px auto;padding:0 18px}table{border-collapse:collapse;width:100%}td,th{border-bottom:1px solid #ddd;padding:8px;text-align:left}button{padding:6px 10px}.ok{color:#087}.warn{color:#b60}code{font-size:14px}</style><h1>MarketDepth</h1><p>Schema: <code>{{.Schema}}</code></p><p>Uptime: {{.Uptime}} | Spot rows: {{.SpotRows}} | Futures rows: {{.FuturesRows}} | Disk free: {{.Free}}</p><p>Dataset: {{.Dataset}}</p><h2>12-hour Packages</h2><table><tr><th>ID</th><th>Range (+08:00)</th><th>Size</th><th>Downloaded</th><th>Action</th></tr>{{range .Packages}}<tr><td><code>{{.ID}}</code></td><td>{{.Start}} → {{.End}}</td><td>{{.Size}}</td><td>{{.Downloaded}}</td><td><a href="/download/{{.ID}}">Download TAR</a>{{if .Downloaded}}<form style="display:inline" method=post action="/purge/{{.ID}}"><input type=password name=password placeholder="password" required><button>Purge</button></form>{{end}}</td></tr>{{end}}</table><p><a href=/logout>Logout</a></p>`))

type dashPkg struct {
	ID, Start, End, Size string
	Downloaded           bool
}
type dashData struct {
	Schema, Uptime, Free, Dataset string
	SpotRows, FuturesRows         uint64
	Packages                      []dashPkg
}

func formatTaipei(ms int64) string {
	z := time.FixedZone("Asia/Taipei", 8*3600)
	return time.UnixMilli(ms).In(z).Format("2006-01-02T15:04:05-07:00")
}
func humanBytes(n int64) string {
	if n < 1024 {
		return fmt.Sprintf("%d B", n)
	}
	if n < 1024*1024 {
		return fmt.Sprintf("%.1f KB", float64(n)/1024)
	}
	return fmt.Sprintf("%.2f MB", float64(n)/(1024*1024))
}

func StartServer(cfg Config, store *Store, rt *RuntimeStatus) *http.Server {
	mux := http.NewServeMux()
	lim := NewAPILimiter(cfg.HistoryRatePerMinute)
	loginLim := NewAPILimiter(10)
	recentLim := NewCooldownLimiter(recentPollMinInterval)
	isAdmin := func(r *http.Request) bool {
		c, err := r.Cookie("marketdepth_admin")
		return err == nil && validSession(cfg.SessionSecret, c.Value)
	}
	requireAdmin := func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if !isAdmin(r) {
				http.Redirect(w, r, "/login", http.StatusSeeOther)
				return
			}
			next(w, r)
		}
	}
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"ok": true,
			"schema": SchemaVersion,
			"spotSchema": SpotSchema,
			"spotCadenceMs": cfg.SpotCadence.Milliseconds(),
			"depthUnit": DepthUnit,
			"quoteToUSDPolicy": QuoteToUSDPolicy,
			"timezone": "Asia/Taipei",
			"utcOffset": "+08:00",
		})
	})
	mux.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			_ = loginTpl.Execute(w, nil)
			return
		}
		if r.Method != "POST" {
			http.Error(w, "method", 405)
			return
		}
		if !loginLim.Allow() {
			http.Error(w, "too many attempts", http.StatusTooManyRequests)
			return
		}
		_ = r.ParseForm()
		if !ctEq(r.Form.Get("password"), cfg.AdminPassword) {
			w.WriteHeader(401)
			_ = loginTpl.Execute(w, "Invalid password")
			return
		}
		exp := time.Now().Add(12 * time.Hour).Unix()
		http.SetCookie(w, &http.Cookie{Name: "marketdepth_admin", Value: signSession(cfg.SessionSecret, exp), Path: "/", HttpOnly: true, Secure: true, SameSite: http.SameSiteLaxMode, Expires: time.Unix(exp, 0)})
		http.Redirect(w, r, "/", 303)
	})
	mux.HandleFunc("/logout", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "marketdepth_admin", Value: "", Path: "/", MaxAge: -1, HttpOnly: true, Secure: true})
		http.Redirect(w, r, "/login", 303)
	})
	mux.HandleFunc("/", requireAdmin(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		total, free, _, _ := store.Disk()
		_ = total
		ps := store.Packages()
		d := dashData{Schema: SchemaVersion, Uptime: time.Since(time.UnixMilli(rt.StartedMs)).Round(time.Second).String(), Free: humanBytes(free), Dataset: "market-depth", SpotRows: rt.SpotRows.Load(), FuturesRows: rt.FuturesRows.Load()}
		for _, p := range ps {
			d.Packages = append(d.Packages, dashPkg{ID: p.ID, Start: formatTaipei(p.StartMs), End: formatTaipei(p.EndMs), Size: humanBytes(p.Bytes), Downloaded: p.Downloaded})
		}
		_ = dashTpl.Execute(w, d)
	}))
	mux.HandleFunc("/download/", requireAdmin(func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/download/")
		p, ok := store.Package(id)
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/x-tar")
		w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s.tar"`, p.ID))
		if err := store.StreamPackage(id, w); err != nil {
			return
		}
		_ = store.MarkDownloaded(id)
	}))
	mux.HandleFunc("/purge/", requireAdmin(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "method", 405)
			return
		}
		_ = r.ParseForm()
		if !ctEq(r.Form.Get("password"), cfg.AdminPassword) {
			http.Error(w, "unauthorized", 401)
			return
		}
		id := strings.TrimPrefix(r.URL.Path, "/purge/")
		if err := store.Purge(id); err != nil {
			http.Error(w, err.Error(), 409)
			return
		}
		http.Redirect(w, r, "/", 303)
	}))

	authAPI := func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "GET" {
				http.Error(w, "method", 405)
				return
			}
			tok := bearer(r)
			if !ctEq(tok, cfg.HistoryToken) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(401)
				io.WriteString(w, `{"ok":false,"error":{"code":"UNAUTHORIZED"}}`)
				return
			}
			if !lim.Allow() {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", "60")
				w.WriteHeader(http.StatusTooManyRequests)
				io.WriteString(w, `{"ok":false,"error":{"code":"RATE_LIMIT"}}`)
				return
			}
			next(w, r)
		}
	}
	mux.HandleFunc("/api/v1/recent/depth", authAPI(func(w http.ResponseWriter, r *http.Request) {
		if rt.Recent == nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			io.WriteString(w, `{"ok":false,"error":{"code":"RECENT_UNAVAILABLE"}}`)
			return
		}
		since := int64(0)
		if raw := r.URL.Query().Get("since"); raw != "" {
			n, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || n < 0 {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				io.WriteString(w, `{"ok":false,"error":{"code":"BAD_SINCE"}}`)
				return
			}
			since = n
		}
		if ok, retryMs := recentLim.Allow(); !ok {
			retrySec := (retryMs + 999) / 1000
			if retrySec < 1 {
				retrySec = 1
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Retry-After", strconv.FormatInt(retrySec, 10))
			w.WriteHeader(http.StatusTooManyRequests)
			json.NewEncoder(w).Encode(map[string]any{
				"ok": false,
				"error": map[string]any{"code": "POLL_TOO_FAST"},
				"retryAfterMs": retryMs,
			})
			return
		}

		rows, oldest, latest := rt.Recent.RowsSince(since)
		now := nowMs()
		var dataAgeMs any
		stale := true
		if latest > 0 {
			age := now - latest
			if age < 0 {
				age = 0
			}
			dataAgeMs = age
			stale = age > recentStaleAfter.Milliseconds()
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "private, max-age=30")
		json.NewEncoder(w).Encode(map[string]any{
			"ok": true,
			"data": rows,
			"meta": map[string]any{
				"schema": SpotSchema,
				"depthUnit": DepthUnit,
				"quoteToUSDPolicy": QuoteToUSDPolicy,
				"timezone": "Asia/Taipei",
				"utcOffset": "+08:00",
				"cadenceMs": cfg.SpotCadence.Milliseconds(),
				"bufferMs": recentBufferDuration.Milliseconds(),
				"pollMinMs": recentPollMinInterval.Milliseconds(),
				"staleAfterMs": recentStaleAfter.Milliseconds(),
				"oldestAvailableTsMs": oldest,
				"latestTargetTsMs": latest,
				"dataAgeMs": dataAgeMs,
				"stale": stale,
			},
		})
	}))
	mux.HandleFunc("/api/v1/history/depth/index", authAPI(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		from, _ := strconv.ParseInt(q.Get("from"), 10, 64)
		to, _ := strconv.ParseInt(q.Get("to"), 10, 64)
		limit, _ := strconv.Atoi(q.Get("limit"))
		cursor, _ := strconv.Atoi(q.Get("cursor"))
		indexVersion := int64(0)
		if raw := q.Get("indexVersion"); raw != "" {
			n, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || n < 0 {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				io.WriteString(w, `{"ok":false,"error":{"code":"BAD_INDEX_VERSION"}}`)
				return
			}
			indexVersion = n
		}
		if limit <= 0 {
			limit = 100
		}
		if limit > 500 {
			limit = 500
		}
		chunks, next, version, consistent := store.History(from, to, limit, cursor, indexVersion)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "private, max-age=15")
		if !consistent {
			w.WriteHeader(http.StatusConflict)
			json.NewEncoder(w).Encode(map[string]any{
				"ok": false,
				"error": map[string]any{
					"code": "INDEX_CHANGED",
					"message": "Historical index changed during pagination; restart from the first page",
				},
				"meta": map[string]any{
					"requestedIndexVersion": indexVersion,
					"currentIndexVersion": version,
					"indexUpdatedTsMs": version,
				},
			})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{
			"ok": true,
			"data": chunks,
			"nextCursor": next,
			"meta": map[string]any{
				"schema": HistoryIndexSchema,
				"depthUnit": DepthUnit,
				"quoteToUSDPolicy": QuoteToUSDPolicy,
				"timezone": "Asia/Taipei",
				"utcOffset": "+08:00",
				"indexVersion": version,
				"indexUpdatedTsMs": version,
			},
		})
	}))
	mux.HandleFunc("/api/v1/history/depth/chunks/", authAPI(func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/api/v1/history/depth/chunks/")
		m, ok := store.Chunk(id)
		if !ok {
			http.NotFound(w, r)
			return
		}
		etag := `"` + m.SHA256 + `"`
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(304)
			return
		}
		f, err := os.Open(filepath.Join(cfg.DatasetDir, filepath.FromSlash(m.Rel)))
		if err != nil {
			http.NotFound(w, r)
			return
		}
		defer f.Close()
		w.Header().Set("Content-Type", "application/gzip")
		w.Header().Set("Content-Length", strconv.FormatInt(m.Bytes, 10))
		w.Header().Set("ETag", etag)
		w.Header().Set("Cache-Control", "private, max-age=31536000, immutable")
		_, _ = io.Copy(w, f)
	}))

	srv := &http.Server{Addr: ":" + cfg.Port, Handler: mux, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32 << 10}
	go srv.ListenAndServe()
	return srv
}
