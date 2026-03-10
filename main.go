// Package main aggregates RSS/Atom feeds from a sources file (default data/rss.txt)
// and writes feeds.json (default data/feeds.json). Supports concurrent fetch
// (per-request parser for safety), exponential backoff retry, optional category
// via "category,url" lines, and configurable log directory with retention cleanup.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mmcdole/gofeed"
)

const (
	defaultWorkers       = 8
	maxWorkers           = 64
	minWorkers           = 1
	maxRetries           = 3
	initialBackoff       = time.Second
	backoffMultiplier    = 2
	defaultRequestTimeout = 30 * time.Second
	defaultLogRetention  = 7
	datetimeLayout       = "2006-01-02 15:04:05"
	userAgent            = "FeedsAggregator/1.0 (+https://github.com/chensoul/chensoul.github.io)"
)

var requestTimeout = defaultRequestTimeout

// FeedItem matches the JSON structure expected by the blog feeds page.
type FeedItem struct {
	Category  string `json:"category,omitempty"`  // 来自 rss.txt 的「分类,url」中的分类
	BlogName  string `json:"blog_name"`
	Title     string `json:"title"`
	Published string `json:"published"`
	Link      string `json:"link"`
	Avatar    string `json:"avatar"`
}

// sourceEntry is one line from rss.txt: optional category and URL.
type sourceEntry struct {
	Category string
	URL     string
}

// Output is the root object written to feeds.json.
type Output struct {
	Items   []FeedItem `json:"items"`
	Updated string     `json:"updated"`
}

func main() {
	sourcesPath := flag.String("sources", "data/rss.txt", "Path to txt file with one RSS URL per line")
	outputPath := flag.String("output", "data/feeds.json", "Path to output feeds.json")
	workers := flag.Int("workers", defaultWorkers, "Concurrent fetch workers")
	logDir := flag.String("logdir", "logs", "Directory for log files (daily log)")
	logRetention := flag.Int("logretention", defaultLogRetention, "Keep log files only for the last N days (0 to disable cleanup)")
	timezone := flag.String("timezone", "", "IANA timezone for updated field (e.g. Asia/Shanghai), empty for local")
	maxItemsPerFeed := flag.Int("maxItemsPerFeed", 0, "Max items to take per RSS source (0 = unlimited); when > 0, keeps latest entries only")
	maxTotalItems := flag.Int("maxTotalItems", 0, "Max total items in output (0 = unlimited); applied after sort and dedup")
	dedup := flag.Bool("dedup", true, "Deduplicate by link (keep newest)")
	requestTimeoutStr := flag.String("requestTimeout", "30s", "HTTP request timeout per feed (e.g. 30s, 1m)")
	flag.Parse()

	if d, err := time.ParseDuration(*requestTimeoutStr); err == nil && d > 0 {
		requestTimeout = d
	}

	// Cleanup old logs before opening new one
	if *logRetention > 0 && *logDir != "" {
		cleanupOldLogs(*logDir, *logRetention)
	}

	// Setup logging: both file (daily) and stderr
	logFile, err := setupLogging(*logDir)
	if err != nil {
		log.Printf("WARN: could not create log file: %v; logging to stderr only", err)
	} else if logFile != nil {
		defer logFile.Close()
		log.SetOutput(io.MultiWriter(os.Stderr, logFile))
	}

	sources, err := readSources(*sourcesPath)
	if err != nil {
		log.Fatalf("read sources: %v", err)
	}
	if len(sources) == 0 {
		log.Fatalf("no RSS URLs in sources file %q: add one URL per line (or \"category,url\"), and ensure lines are not commented with # or empty", *sourcesPath)
	}

	start := time.Now()
	workersNum := *workers
	if workersNum < minWorkers {
		workersNum = minWorkers
	}
	if workersNum > maxWorkers {
		workersNum = maxWorkers
	}

	type result struct {
		items []FeedItem
		url   string
		err  error
	}

	work := make(chan sourceEntry, len(sources))
	results := make(chan result, len(sources))

	for _, s := range sources {
		work <- s
	}
	close(work)

	var wg sync.WaitGroup
	for i := 0; i < workersNum; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for s := range work {
				items, err := fetchAndParse(s.URL, s.Category, *maxItemsPerFeed)
				results <- result{items: items, url: s.URL, err: err}
				if err != nil {
					log.Printf("ERROR [%s]: %v", s.URL, err)
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var all []FeedItem
	var failedCount int
	for r := range results {
		if r.err != nil {
			failedCount++
			continue
		}
		all = append(all, r.items...)
	}

	if *dedup {
		seen := make(map[string]bool, len(all))
		filtered := all[:0]
		for _, it := range all {
			if it.Link == "" || seen[it.Link] {
				continue
			}
			seen[it.Link] = true
			filtered = append(filtered, it)
		}
		all = filtered
	}

	sort.Slice(all, func(i, j int) bool {
		ti, _ := time.Parse(datetimeLayout, all[i].Published)
		tj, _ := time.Parse(datetimeLayout, all[j].Published)
		if !ti.IsZero() && !tj.IsZero() {
			if !ti.Equal(tj) {
				return ti.After(tj)
			}
			return all[i].Link < all[j].Link
		}
		if !ti.IsZero() {
			return true
		}
		if !tj.IsZero() {
			return false
		}
		return all[i].Link < all[j].Link
	})

	if *maxTotalItems > 0 && len(all) > *maxTotalItems {
		all = all[:*maxTotalItems]
	}

	now := time.Now()
	if *timezone != "" {
		if loc, err := time.LoadLocation(*timezone); err == nil {
			now = now.In(loc)
		}
	}
	out := Output{
		Items:   all,
		Updated: now.Format(datetimeLayout),
	}

	enc, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		log.Fatalf("json encode: %v", err)
	}

	if *outputPath == "" {
		os.Stdout.Write(enc)
		return
	}

	if err := os.MkdirAll(filepath.Dir(*outputPath), 0755); err != nil {
		log.Fatalf("mkdir output: %v", err)
	}
	if err := os.WriteFile(*outputPath, enc, 0644); err != nil {
		log.Fatalf("write output: %v", err)
	}
	elapsed := time.Since(start)
	log.Printf("done: %d sources ok, %d failed, %d items -> %s (in %v)", len(sources)-failedCount, failedCount, len(all), *outputPath, elapsed.Round(time.Millisecond))
}

// cleanupOldLogs removes log files in logDir older than retentionDays.
func cleanupOldLogs(logDir string, retentionDays int) {
	if retentionDays <= 0 {
		return
	}
	cutoff := time.Now().AddDate(0, 0, -retentionDays)
	entries, err := os.ReadDir(logDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), "feeds-") || !strings.HasSuffix(e.Name(), ".log") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			p := filepath.Join(logDir, e.Name())
			if err := os.Remove(p); err != nil {
				log.Printf("WARN: remove old log %s: %v", p, err)
			}
		}
	}
}

func setupLogging(logDir string) (*os.File, error) {
	if logDir == "" {
		return nil, nil
	}
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, err
	}
	name := filepath.Join(logDir, "feeds-"+time.Now().Format("2006-01-02")+".log")
	f, err := os.OpenFile(name, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// originFromURL 从 URL 解析出 origin（scheme + host），用于拼站点自身的 favicon 地址。
// 例如 https://blog.example.com/path -> https://blog.example.com，解析失败返回空。
func originFromURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if !strings.HasPrefix(raw, "http://") && !strings.HasPrefix(raw, "https://") {
		raw = "https://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return ""
	}
	u.Path = ""
	u.RawPath = ""
	u.RawQuery = ""
	u.Fragment = ""
	return u.String()
}

// faviconClient 用于 HEAD 检查 favicon 是否存在，短超时避免拖慢整体抓取。
var faviconClient = &http.Client{
	Timeout: 5 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:    10,
		IdleConnTimeout: 10 * time.Second,
	},
}

// urlExists 对 url 发 HEAD，返回是否为 2xx。若服务器不支持 HEAD 则尝试 GET 并立即关闭 body。
func urlExists(u string) bool {
	req, err := http.NewRequest(http.MethodHead, u, nil)
	if err != nil {
		return false
	}
	req.Header.Set("User-Agent", userAgent)
	resp, err := faviconClient.Do(req)
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return true
	}
	if resp.StatusCode == http.StatusMethodNotAllowed {
		req.Method = http.MethodGet
		resp2, err := faviconClient.Do(req)
		if err != nil {
			return false
		}
		_ = resp2.Body.Close()
		return resp2.StatusCode >= 200 && resp2.StatusCode < 300
	}
	return false
}

// resolveFavicon 返回 origin 下存在的 favicon URL：先试 favicon.ico，再试 favicon.svg；都不存在则返回空。
func resolveFavicon(origin string) string {
	origin = strings.TrimSuffix(origin, "/")
	if origin == "" {
		return ""
	}
	if urlExists(origin + "/favicon.ico") {
		return origin + "/favicon.ico"
	}
	if urlExists(origin + "/favicon.svg") {
		return origin + "/favicon.svg"
	}
	return ""
}

// readSources 读取 rss.txt：每行一个 URL，或 "分类,URL"（逗号前为分类，会写入 feeds.json 的 category）
func readSources(path string) ([]sourceEntry, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out []sourceEntry
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		entry := sourceEntry{}
		if idx := strings.Index(line, ","); idx >= 0 {
			entry.Category = strings.TrimSpace(line[:idx])
			entry.URL = strings.TrimSpace(line[idx+1:])
		} else {
			entry.URL = line
		}
		if entry.URL == "" {
			continue
		}
		out = append(out, entry)
	}
	return out, nil
}

func newParser() *gofeed.Parser {
	p := gofeed.NewParser()
	p.Client = &http.Client{
		Timeout: requestTimeout,
		Transport: &http.Transport{
			MaxIdleConns:        10,
			IdleConnTimeout:     30 * time.Second,
			DisableCompression:  false,
		},
	}
	p.UserAgent = userAgent
	return p
}

func fetchWithBackoff(feedURL string) (*gofeed.Feed, error) {
	var lastErr error
	backoff := initialBackoff
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			log.Printf("retry %d/%d for %s after %v", attempt, maxRetries-1, feedURL, backoff)
			time.Sleep(backoff)
			backoff *= backoffMultiplier
		}
		parser := newParser()
		feed, err := parser.ParseURL(feedURL)
		if err == nil {
			return feed, nil
		}
		lastErr = err
	}
	return nil, fmt.Errorf("after %d retries: %w", maxRetries, lastErr)
}

func fetchAndParse(feedURL string, category string, maxItemsPerFeed int) ([]FeedItem, error) {
	feed, err := fetchWithBackoff(feedURL)
	if err != nil {
		return nil, err
	}

	blogName := strings.TrimSpace(feed.Title)
	if blogName != "" && !strings.HasPrefix(blogName, "@") {
		blogName = "@" + blogName
	}
	avatar := ""
	if feed.Image != nil && feed.Image.URL != "" {
		avatar = strings.TrimSpace(feed.Image.URL)
	}
	if avatar == "" {
		origin := ""
		if o := originFromURL(feed.Link); o != "" {
			origin = strings.TrimSuffix(o, "/")
		} else if len(feed.Items) > 0 {
			origin = strings.TrimSuffix(originFromURL(feed.Items[0].Link), "/")
		}
		if origin != "" {
			avatar = resolveFavicon(origin)
		}
	}

	entries := feed.Items
		if maxItemsPerFeed > 0 && len(entries) > 0 {
			sort.Slice(entries, func(i, j int) bool {
				ti, tj := entries[i].PublishedParsed, entries[j].PublishedParsed
				if ti == nil {
					ti = entries[i].UpdatedParsed
				}
				if tj == nil {
					tj = entries[j].UpdatedParsed
				}
				if ti != nil && tj != nil {
					return (*ti).After(*tj)
				}
				if ti != nil {
					return true
				}
				return false
			})
		if len(entries) > maxItemsPerFeed {
			entries = entries[:maxItemsPerFeed]
		}
	}

	var items []FeedItem
	for _, it := range entries {
		link := strings.TrimSpace(it.Link)
		if link == "" {
			continue
		}
		pub := ""
		if it.PublishedParsed != nil {
			pub = it.PublishedParsed.Format(datetimeLayout)
		} else if it.UpdatedParsed != nil {
			pub = it.UpdatedParsed.Format(datetimeLayout)
		}
		items = append(items, FeedItem{
			Category:  category,
			BlogName:  blogName,
			Title:     strings.TrimSpace(it.Title),
			Published: pub,
			Link:      link,
			Avatar:    avatar,
		})
	}
	return items, nil
}
