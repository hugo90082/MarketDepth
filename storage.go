package main

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type chunkBuffer struct {
	kind    string
	startMs int64
	rows    [][]byte
}

var ErrStorageStopped = errors.New("storage stopped")

type Store struct {
	cfg           Config
	mu            sync.Mutex
	writesStopped bool
	buffers       map[string]*chunkBuffer
	index       HistoryIndex
	packages    []PackageManifest
	active      *PackageManifest
	lastFlushMs int64
}

func NewStore(cfg Config) (*Store, error) {
	for _, d := range []string{
		filepath.Join(cfg.DatasetDir, "chunks", "spot"), filepath.Join(cfg.DatasetDir, "chunks", "futures"), filepath.Join(cfg.DatasetDir, "chunks", "events"), filepath.Join(cfg.DatasetDir, "chunks", "audit"),
		filepath.Join(cfg.DatasetDir, "history"), filepath.Join(cfg.DatasetDir, "packages", "ready"), filepath.Join(cfg.DatasetDir, "packages", "tombstones"), filepath.Join(cfg.DatasetDir, "meta"), filepath.Join(cfg.DatasetDir, "recovery"),
	} {
		if err := os.MkdirAll(d, 0755); err != nil {
			return nil, err
		}
	}
	s := &Store{cfg: cfg, buffers: map[string]*chunkBuffer{}, index: HistoryIndex{Schema: HistoryIndexSchema, Chunks: []ChunkMeta{}}}
	s.loadIndex()
	s.loadPackages()
	s.loadActive()
	if err := s.loadRecovery(); err != nil {
		return nil, err
	}
	return s, nil
}

func floorMs(t int64, d time.Duration) int64 { m := d.Milliseconds(); return (t / m) * m }
func (s *Store) Add(kind string, ts int64, row any) error {
	b, err := json.Marshal(row)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.writesStopped {
		return ErrStorageStopped
	}
	start := floorMs(ts, s.cfg.ChunkDuration)
	key := kind + ":" + fmt.Sprint(start)
	cb := s.buffers[key]
	if cb == nil {
		cb = &chunkBuffer{kind: kind, startMs: start}
		s.buffers[key] = cb
	}
	cb.rows = append(cb.rows, b)
	return nil
}

func (s *Store) FlushClosed(now int64) ([]ChunkMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := floorMs(now, s.cfg.ChunkDuration)
	keys := []string{}
	for k, b := range s.buffers {
		if b.startMs < cur {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	out := []ChunkMeta{}
	for _, k := range keys {
		m, err := s.flushLocked(s.buffers[k])
		if err != nil {
			return out, err
		}
		delete(s.buffers, k)
		out = append(out, m)
	}
	if len(out) > 0 {
		s.lastFlushMs = time.Now().UnixMilli()
		if err := s.addChunksLocked(out); err != nil {
			return out, err
		}
	}
	return out, nil
}
func (s *Store) FlushAll() ([]ChunkMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.buffers))
	for k := range s.buffers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := []ChunkMeta{}
	for _, k := range keys {
		if len(s.buffers[k].rows) == 0 {
			continue
		}
		m, err := s.flushLocked(s.buffers[k])
		if err != nil {
			return out, err
		}
		out = append(out, m)
		delete(s.buffers, k)
	}
	if len(out) > 0 {
		if err := s.addChunksLocked(out); err != nil {
			return out, err
		}
	}
	return out, nil
}

func atomicWrite(path string, b []byte) error {
	tmp := path + ".part"
	if err := os.WriteFile(tmp, b, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func writeGzipRowsAtomic(path string, rows [][]byte) error {
	tmp := path + ".part"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	gz := gzip.NewWriter(f)
	bw := bufio.NewWriterSize(gz, 64*1024)
	for _, row := range rows {
		if _, err = bw.Write(row); err != nil {
			f.Close()
			return err
		}
		if err = bw.WriteByte('\n'); err != nil {
			f.Close()
			return err
		}
	}
	if err = bw.Flush(); err != nil {
		f.Close()
		return err
	}
	if err = gz.Close(); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func readGzipRows(path string) ([][]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	sc := bufio.NewScanner(gz)
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)
	var rows [][]byte
	for sc.Scan() {
		rows = append(rows, append([]byte(nil), sc.Bytes()...))
	}
	return rows, sc.Err()
}

func recoveryName(kind string, startMs int64) string {
	return fmt.Sprintf("%s-%d.jsonl.gz", kind, startMs)
}
func (s *Store) recoveryPath(kind string, startMs int64) string {
	return filepath.Join(s.cfg.DatasetDir, "recovery", recoveryName(kind, startMs))
}
func (s *Store) CheckpointOpen() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cb := range s.buffers {
		if len(cb.rows) == 0 {
			continue
		}
		if err := writeGzipRowsAtomic(s.recoveryPath(cb.kind, cb.startMs), cb.rows); err != nil {
			return err
		}
	}
	return nil
}
func (s *Store) loadRecovery() error {
	dir := filepath.Join(s.cfg.DatasetDir, "recovery")
	ents, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range ents {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".jsonl.gz") {
			continue
		}
		base := strings.TrimSuffix(e.Name(), ".jsonl.gz")
		i := strings.LastIndex(base, "-")
		if i <= 0 {
			continue
		}
		kind := base[:i]
		if kind != "spot" && kind != "futures" && kind != "events" {
			continue
		}
		start, err := strconv.ParseInt(base[i+1:], 10, 64)
		if err != nil {
			continue
		}
		full := filepath.Join(dir, e.Name())
		rows, err := readGzipRows(full)
		if err != nil {
			return err
		}
		if len(rows) > 0 {
			key := kind + ":" + fmt.Sprint(start)
			s.buffers[key] = &chunkBuffer{kind: kind, startMs: start, rows: rows}
		}
		// Keep the recovery checkpoint until the final immutable chunk is
		// successfully sealed. If the process crashes again immediately after
		// startup, the last durable checkpoint is still available.
	}
	return nil
}

type spotChunkAudit struct {
	Schema   string      `json:"schema"`
	StartMs  int64       `json:"startTsMs"`
	EndMs    int64       `json:"endTsMs"`
	Rows     int         `json:"rows"`
	ValidBid [][][]int   `json:"validBid"`
	ValidAsk [][][]int   `json:"validAsk"`
}

func make3DInts(a, b, d int) [][][]int {
	x := make([][][]int, a)
	for i := range x {
		x[i] = make([][]int, b)
		for j := range x[i] {
			x[i][j] = make([]int, d)
		}
	}
	return x
}
func (s *Store) writeSpotAudit(cb *chunkBuffer) (string, error) {
	a := spotChunkAudit{
		Schema: "MD-SPOT-AUDIT-1", StartMs: cb.startMs,
		EndMs: cb.startMs + s.cfg.ChunkDuration.Milliseconds(), Rows: len(cb.rows),
		ValidBid: make3DInts(len(Venues), len(Assets), len(Zones)),
		ValidAsk: make3DInts(len(Venues), len(Assets), len(Zones)),
	}
	for _, raw := range cb.rows {
		var row SpotSnapshot
		if json.Unmarshal(raw, &row) != nil {
			continue
		}
		for vi := 0; vi < len(Venues) && vi < len(row.D); vi++ {
			for ai := 0; ai < len(Assets) && ai < len(row.D[vi]); ai++ {
				for zi := 0; zi < len(Zones) && zi < len(row.D[vi][ai]); zi++ {
					if row.D[vi][ai][zi][0] != nil {
						a.ValidBid[vi][ai][zi]++
					}
					if row.D[vi][ai][zi][1] != nil {
						a.ValidAsk[vi][ai][zi]++
					}
				}
			}
		}
	}
	b, _ := json.Marshal(a)
	rel := filepath.ToSlash(filepath.Join("chunks", "audit", fmt.Sprintf("%d.json", cb.startMs)))
	if err := atomicWrite(filepath.Join(s.cfg.DatasetDir, filepath.FromSlash(rel)), b); err != nil {
		return "", err
	}
	log.Printf("spot-chunk-audit %s", string(b))
	return rel, nil
}

func schemaFor(kind string) string {
	switch kind {
	case "spot":
		return SpotSchema
	case "futures":
		return FuturesSchema
	default:
		return EventSchema
	}
}
func (s *Store) flushLocked(cb *chunkBuffer) (ChunkMeta, error) {
	// Refresh the durable recovery image immediately before sealing so a crash
	// anywhere in the seal path can replay the complete buffer.
	if err := writeGzipRowsAtomic(s.recoveryPath(cb.kind, cb.startMs), cb.rows); err != nil {
		return ChunkMeta{}, err
	}
	relDir := filepath.Join("chunks", cb.kind)
	dir := filepath.Join(s.cfg.DatasetDir, relDir)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return ChunkMeta{}, err
	}
	name := fmt.Sprintf("%d.jsonl.gz", cb.startMs)
	rel := filepath.Join(relDir, name)
	full := filepath.Join(s.cfg.DatasetDir, rel)
	tmp := full + ".part"
	f, err := os.Create(tmp)
	if err != nil {
		return ChunkMeta{}, err
	}
	h := sha256.New()
	mw := io.MultiWriter(f, h)
	gz := gzip.NewWriter(mw)
	bw := bufio.NewWriterSize(gz, 64*1024)
	for _, r := range cb.rows {
		if _, err = bw.Write(r); err != nil {
			f.Close()
			return ChunkMeta{}, err
		}
		if err = bw.WriteByte('\n'); err != nil {
			f.Close()
			return ChunkMeta{}, err
		}
	}
	if err = bw.Flush(); err != nil {
		f.Close()
		return ChunkMeta{}, err
	}
	if err = gz.Close(); err != nil {
		f.Close()
		return ChunkMeta{}, err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return ChunkMeta{}, err
	}
	if err = f.Close(); err != nil {
		return ChunkMeta{}, err
	}
	if err = os.Rename(tmp, full); err != nil {
		return ChunkMeta{}, err
	}
	sha := hex.EncodeToString(h.Sum(nil))
	shaRel := rel + ".sha256"
	if err = os.WriteFile(filepath.Join(s.cfg.DatasetDir, shaRel), []byte(fmt.Sprintf("%s  %s\n", sha, name)), 0644); err != nil {
		return ChunkMeta{}, err
	}
	st, _ := os.Stat(full)
	auditRel := ""
	if cb.kind == "spot" {
		auditRel, err = s.writeSpotAudit(cb)
		if err != nil {
			return ChunkMeta{}, err
		}
	}
	return ChunkMeta{Kind: cb.kind, ID: fmt.Sprintf("%s-%d", cb.kind, cb.startMs), Rel: filepath.ToSlash(rel), ShaRel: filepath.ToSlash(shaRel), AuditRel: auditRel, StartMs: cb.startMs, EndMs: cb.startMs + s.cfg.ChunkDuration.Milliseconds(), Rows: len(cb.rows), Bytes: st.Size(), SHA256: sha, Schema: schemaFor(cb.kind)}, nil
}

func (s *Store) indexPath() string {
	return filepath.Join(s.cfg.DatasetDir, "history", "depth-index.json")
}
func (s *Store) loadIndex() {
	b, err := os.ReadFile(s.indexPath())
	if err == nil {
		var x HistoryIndex
		if json.Unmarshal(b, &x) == nil && x.Schema == HistoryIndexSchema {
			s.index = x
		}
	}
}
func (s *Store) saveIndexLocked() error {
	s.index.UpdatedMs = time.Now().UnixMilli()
	b, _ := json.MarshalIndent(s.index, "", "  ")
	return atomicWrite(s.indexPath(), b)
}
func hasString(xs []string, want string) bool {
	for _, x := range xs {
		if filepath.ToSlash(x) == filepath.ToSlash(want) {
			return true
		}
	}
	return false
}
func (s *Store) packageHasRelLocked(rel string) bool {
	if s.active != nil && hasString(s.active.Files, rel) {
		return true
	}
	for _, p := range s.packages {
		if hasString(p.Files, rel) {
			return true
		}
	}
	return false
}
func (s *Store) indexHasRelLocked(rel string) bool {
	for _, x := range s.index.Chunks {
		if filepath.ToSlash(x.Rel) == filepath.ToSlash(rel) {
			return true
		}
	}
	return false
}
func (s *Store) addChunksLocked(ms []ChunkMeta) error {
	newForPackage := make([]ChunkMeta, 0, len(ms))
	indexChanged := false
	for _, m := range ms {
		if m.Kind == "spot" && !s.indexHasRelLocked(m.Rel) {
			s.index.Chunks = append(s.index.Chunks, m)
			indexChanged = true
		}
		if !s.packageHasRelLocked(m.Rel) {
			newForPackage = append(newForPackage, m)
		}
	}
	if indexChanged {
		sort.Slice(s.index.Chunks, func(i, j int) bool { return s.index.Chunks[i].StartMs < s.index.Chunks[j].StartMs })
		if err := s.saveIndexLocked(); err != nil {
			return err
		}
	}
	if len(newForPackage) > 0 {
		if err := s.addPackageFilesLocked(newForPackage); err != nil {
			return err
		}
	}
	for _, m := range ms {
		_ = os.Remove(s.recoveryPath(m.Kind, m.StartMs))
	}
	return nil
}

func (s *Store) activePath() string {
	return filepath.Join(s.cfg.DatasetDir, "packages", "active.json")
}
func (s *Store) loadActive() {
	b, err := os.ReadFile(s.activePath())
	if err == nil {
		var p PackageManifest
		if json.Unmarshal(b, &p) == nil {
			s.active = &p
		}
	}
}
func (s *Store) saveActiveLocked() error {
	if s.active == nil {
		if err := os.Remove(s.activePath()); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	b, _ := json.MarshalIndent(s.active, "", "  ")
	return atomicWrite(s.activePath(), b)
}
func (s *Store) loadPackages() {
	dir := filepath.Join(s.cfg.DatasetDir, "packages", "ready")
	ents, _ := os.ReadDir(dir)
	for _, e := range ents {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			continue
		}
		var p PackageManifest
		if json.Unmarshal(b, &p) == nil {
			s.packages = append(s.packages, p)
		}
	}
	sort.Slice(s.packages, func(i, j int) bool { return s.packages[i].StartMs < s.packages[j].StartMs })
}
func appendUnique(xs []string, vals ...string) []string {
	seen := make(map[string]bool, len(xs)+len(vals))
	for _, x := range xs {
		seen[x] = true
	}
	for _, v := range vals {
		if v != "" && !seen[v] {
			xs = append(xs, v)
			seen[v] = true
		}
	}
	return xs
}
func (s *Store) addPackageFilesLocked(ms []ChunkMeta) error {
	if len(ms) == 0 {
		return nil
	}
	sort.Slice(ms, func(i, j int) bool { return ms[i].StartMs < ms[j].StartMs })
	if s.active == nil {
		st := ms[0].StartMs
		s.active = &PackageManifest{
			Schema: PackageSchema, ID: "PKG-" + time.UnixMilli(st).UTC().Format("20060102T150405Z"),
			StartMs: st, EndMs: ms[0].EndMs, Files: []string{},
			ChunkCounts: map[string]int{}, RowCounts: map[string]int{},
		}
	}
	if s.active.ChunkCounts == nil {
		s.active.ChunkCounts = map[string]int{}
	}
	if s.active.RowCounts == nil {
		s.active.RowCounts = map[string]int{}
	}
	for _, m := range ms {
		s.active.Files = appendUnique(s.active.Files, m.Rel, m.ShaRel, m.AuditRel)
		s.active.Bytes += m.Bytes
		s.active.ChunkCounts[m.Kind]++
		s.active.RowCounts[m.Kind] += m.Rows
		if m.EndMs > s.active.EndMs {
			s.active.EndMs = m.EndMs
		}
	}
	if err := s.saveActiveLocked(); err != nil {
		return err
	}
	timeReady := s.active.EndMs-s.active.StartMs >= s.cfg.PackageDuration.Milliseconds()
	sizeReady := s.cfg.PackageMaxBytes > 0 && s.active.Bytes >= s.cfg.PackageMaxBytes
	if timeReady || sizeReady {
		return s.sealLocked()
	}
	return nil
}
func (s *Store) sealLocked() error {
	if s.active == nil {
		return nil
	}
	p := *s.active
	p.SealedAtMs = time.Now().UnixMilli()
	path := filepath.Join(s.cfg.DatasetDir, "packages", "ready", p.ID+".json")
	b, _ := json.MarshalIndent(p, "", "  ")
	if err := atomicWrite(path, b); err != nil {
		return err
	}
	s.packages = append(s.packages, p)
	s.active = nil
	_ = os.Remove(s.activePath())
	return nil
}

func (s *Store) Packages() []PackageManifest {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]PackageManifest(nil), s.packages...)
	return out
}
func (s *Store) Package(id string) (PackageManifest, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range s.packages {
		if p.ID == id {
			return p, true
		}
	}
	return PackageManifest{}, false
}
func (s *Store) MarkDownloaded(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.packages {
		if s.packages[i].ID == id {
			s.packages[i].Downloaded = true
			s.packages[i].DownloadedAtMs = time.Now().UnixMilli()
			b, _ := json.MarshalIndent(s.packages[i], "", "  ")
			return atomicWrite(filepath.Join(s.cfg.DatasetDir, "packages", "ready", id+".json"), b)
		}
	}
	return fmt.Errorf("package not found")
}
func (s *Store) StreamPackage(id string, w io.Writer) error {
	s.mu.Lock()
	var p *PackageManifest
	for i := range s.packages {
		if s.packages[i].ID == id {
			q := s.packages[i]
			p = &q
			break
		}
	}
	s.mu.Unlock()
	if p == nil {
		return fmt.Errorf("package not found")
	}
	tw := tar.NewWriter(w)
	for _, rel := range p.Files {
		full := filepath.Join(s.cfg.DatasetDir, filepath.FromSlash(rel))
		st, err := os.Stat(full)
		if err != nil {
			return err
		}
		f, err := os.Open(full)
		if err != nil {
			return err
		}
		hdr := &tar.Header{Name: filepath.ToSlash(rel), Mode: 0644, Size: st.Size(), ModTime: st.ModTime()}
		if err = tw.WriteHeader(hdr); err != nil {
			f.Close()
			return err
		}
		if _, err = io.Copy(tw, f); err != nil {
			f.Close()
			return err
		}
		f.Close()
	}
	schemaRel := filepath.Join(s.cfg.DatasetDir, "meta", "schema.json")
	if st, e := os.Stat(schemaRel); e == nil {
		f, e := os.Open(schemaRel)
		if e != nil {
			return e
		}
		if e = tw.WriteHeader(&tar.Header{Name: "meta/schema.json", Mode: 0644, Size: st.Size(), ModTime: st.ModTime()}); e != nil {
			f.Close()
			return e
		}
		if _, e = io.Copy(tw, f); e != nil {
			f.Close()
			return e
		}
		f.Close()
	}
	durMs := p.EndMs - p.StartMs
	audit := map[string]any{
		"schema": "MD-PACKAGE-AUDIT-1",
		"packageId": p.ID,
		"startTsMs": p.StartMs,
		"endTsMs": p.EndMs,
		"chunkCounts": p.ChunkCounts,
		"rowCounts": p.RowCounts,
		"expectedSpotRows": durMs / s.cfg.SpotCadence.Milliseconds(),
		"expectedFuturesRows": durMs / int64(time.Second/time.Millisecond),
	}
	auditBytes, _ := json.MarshalIndent(audit, "", "  ")
	if err := tw.WriteHeader(&tar.Header{Name: "audit-summary.json", Mode: 0644, Size: int64(len(auditBytes)), ModTime: time.Now()}); err != nil {
		return err
	}
	if _, err := tw.Write(auditBytes); err != nil {
		return err
	}
	manifest, _ := json.MarshalIndent(p, "", "  ")
	hdr := &tar.Header{Name: "manifest.json", Mode: 0644, Size: int64(len(manifest)), ModTime: time.Now()}
	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}
	if _, err := tw.Write(manifest); err != nil {
		return err
	}
	return tw.Close()
}
func (s *Store) Purge(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	idx := -1
	var p PackageManifest
	for i := range s.packages {
		if s.packages[i].ID == id {
			idx = i
			p = s.packages[i]
			break
		}
	}
	if idx < 0 {
		return fmt.Errorf("package not found")
	}
	if !p.Downloaded {
		return fmt.Errorf("download first")
	}
	set := map[string]bool{}
	for _, rel := range p.Files {
		set[filepath.ToSlash(rel)] = true
		_ = os.Remove(filepath.Join(s.cfg.DatasetDir, filepath.FromSlash(rel)))
	}
	filtered := s.index.Chunks[:0]
	for _, c := range s.index.Chunks {
		if !set[c.Rel] {
			filtered = append(filtered, c)
		}
	}
	s.index.Chunks = filtered
	if err := s.saveIndexLocked(); err != nil {
		return err
	}
	tomb := map[string]any{"packageId": p.ID, "startTsMs": p.StartMs, "endTsMs": p.EndMs, "bytes": p.Bytes, "deletedAtMs": time.Now().UnixMilli(), "fileCount": len(p.Files)}
	b, _ := json.MarshalIndent(tomb, "", "  ")
	_ = atomicWrite(filepath.Join(s.cfg.DatasetDir, "packages", "tombstones", p.ID+".json"), b)
	_ = os.Remove(filepath.Join(s.cfg.DatasetDir, "packages", "ready", p.ID+".json"))
	s.packages = append(s.packages[:idx], s.packages[idx+1:]...)
	return nil
}

func (s *Store) History(from, to int64, limit int, cursor int) ([]ChunkMeta, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	if cursor < 0 {
		cursor = 0
	}
	out := []ChunkMeta{}
	i := cursor
	for ; i < len(s.index.Chunks) && len(out) < limit; i++ {
		c := s.index.Chunks[i]
		if from > 0 && c.EndMs <= from {
			continue
		}
		if to > 0 && c.StartMs >= to {
			continue
		}
		out = append(out, c)
	}
	// Cursor is the absolute index inspected in the immutable history index, not
	// cursor+returnedRows. This remains correct when from/to filters skip chunks.
	if i >= len(s.index.Chunks) {
		return out, -1
	}
	return out, i
}
func (s *Store) Chunk(id string) (ChunkMeta, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.index.Chunks {
		if c.ID == id {
			return c, true
		}
	}
	return ChunkMeta{}, false
}
func (s *Store) Disk() (total, free, used int64, err error) {
	var st syscall.Statfs_t
	if err = syscall.Statfs(s.cfg.DatasetDir, &st); err != nil {
		return
	}
	total = int64(st.Blocks) * int64(st.Bsize)
	free = int64(st.Bavail) * int64(st.Bsize)
	used = total - free
	return
}
func (s *Store) LastFlush() int64 { s.mu.Lock(); defer s.mu.Unlock(); return s.lastFlushMs }

func (s *Store) SetWritesStopped(v bool) {
	s.mu.Lock()
	s.writesStopped = v
	s.mu.Unlock()
}
func (s *Store) WritesStopped() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writesStopped
}
