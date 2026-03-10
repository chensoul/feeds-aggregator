// Package main aggregates RSS/Atom feeds from a sources file (default data/rss.txt)
// and writes feeds.json (default data/feeds.json). Supports concurrent fetch
// (per-request parser for safety), exponential backoff retry, optional category
// via "category,url" lines, and configurable log directory with retention cleanup.
package main

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/mmcdole/gofeed"
)

const (
	defaultWorkers       = 8
	maxWorkers           = 64
	minWorkers           = 1
	maxRetries           = 3
	initialBackoff       = time.Second
	backoffMultiplier    = 2
	defaultRequestTimeout = 60 * time.Second
	defaultLogRetention  = 7
	datetimeLayout       = "2006-01-02 15:04:05"
	userAgent            = "FeedsAggregator/1.0 (+https://github.com/chensoul/feeds-aggregator)"
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
	sourcesPath := flag.String("sources", "data/rss.txt", "Path to rss.txt (or .opml) with RSS URLs")
	outputPath := flag.String("output", "data/feeds.json", "Path to output feeds.json")
	workers := flag.Int("workers", defaultWorkers, "Concurrent fetch workers")
	logDir := flag.String("logdir", "logs", "Directory for log files (daily log)")
	logRetention := flag.Int("logretention", defaultLogRetention, "Keep log files only for the last N days (0 to disable cleanup)")
	timezone := flag.String("timezone", "", "IANA timezone for updated field (e.g. Asia/Shanghai), empty for local")
	maxItemsPerFeed := flag.Int("maxItemsPerFeed", 0, "Max items to take per RSS source (0 = unlimited); when > 0, keeps latest entries only")
	maxTotalItems := flag.Int("maxTotalItems", 0, "Max total items in output (0 = unlimited); applied after sort and dedup")
	dedup := flag.Bool("dedup", true, "Deduplicate by link (keep newest)")
	requestTimeoutStr := flag.String("requestTimeout", "10s", "HTTP request timeout per feed (e.g. 30s, 1m)")
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
		log.Fatalf("no RSS URLs in sources file %q: use .txt (one URL per line or \"category,url\") or .opml", *sourcesPath)
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

// isIcoContent 根据 Content-Type 或内容前几个字节判断是否为 ICO 格式。
func isIcoContent(contentType string, head []byte) bool {
	ct := strings.ToLower(contentType)
	if strings.Contains(ct, "icon") || strings.Contains(ct, "x-ico") || ct == "image/ico" {
		return true
	}
	// ICO 文件魔数：00 00 01 00
	if len(head) >= 4 && head[0] == 0 && head[1] == 0 && head[2] == 1 && head[3] == 0 {
		return true
	}
	return false
}

// isSvgContent 根据 Content-Type 或内容前几个字节判断是否为 SVG 格式。
func isSvgContent(contentType string, head []byte) bool {
	ct := strings.ToLower(contentType)
	if strings.Contains(ct, "svg") {
		return true
	}
	if len(head) >= 5 {
		s := strings.ToLower(string(head[:min(32, len(head))]))
		return strings.HasPrefix(s, "<?xml") || strings.HasPrefix(s, "<svg")
	}
	return false
}

// urlExistsWithType 检查 URL 是否可访问且返回内容为指定类型（ico 或 svg）。使用 GET 并校验 Content-Type 或内容。
func urlExistsWithType(u string, isCorrectType func(contentType string, head []byte) bool) bool {
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return false
	}
	req.Header.Set("User-Agent", userAgent)
	resp, err := faviconClient.Do(req)
	if err != nil {
		return false
	}
	head, _ := io.ReadAll(io.LimitReader(resp.Body, 64))
	_ = resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return false
	}
	contentType := resp.Header.Get("Content-Type")
	return isCorrectType(contentType, head)
}

// linkRelIconRegex 匹配 <link rel="icon" href="..."> 或 <link rel="shortcut icon" href="...">，href 可在 rel 前或后。
var linkRelIconRegex = regexp.MustCompile(`(?i)<link[^>]*\srel=["'](?:shortcut\s+)?icon["'][^>]*\shref=["']([^"']+)["']|` +
	`<link[^>]*\shref=["']([^"']+)["'][^>]*\srel=["'](?:shortcut\s+)?icon["']`)

// resolveFaviconFromHTML 抓取 origin 首页 HTML，解析 link rel="icon" 的 href，
// 并验证解析出的 URL 是否存在（检查内容是否为 ico 或 svg），返回有效的绝对 URL；失败返回空。
func resolveFaviconFromHTML(origin string) string {
	req, err := http.NewRequest(http.MethodGet, origin+"/", nil)
	if err != nil {
		return ""
	}
	req.Header.Set("User-Agent", userAgent)
	resp, err := faviconClient.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ""
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if err != nil {
		return ""
	}
	base, err := url.Parse(origin + "/")
	if err != nil {
		return ""
	}
	for _, sub := range linkRelIconRegex.FindAllSubmatch(body, -1) {
		href := ""
		if len(sub[1]) > 0 {
			href = string(sub[1])
		} else if len(sub[2]) > 0 {
			href = string(sub[2])
		}
		href = strings.TrimSpace(href)
		if href == "" {
			continue
		}
		u, err := base.Parse(href)
		if err != nil {
			continue
		}
		if u.Scheme != "" && u.Host != "" {
			// 验证解析出的 URL 是否存在且返回 ico 或 svg 内容
			if urlExistsWithType(u.String(), func(ct string, head []byte) bool {
				return isIcoContent(ct, head) || isSvgContent(ct, head)
			}) {
				return u.String()
			}
		}
	}
	return ""
}

// googleFaviconURL 从 origin 生成 Google favicon 服务 URL，作为兜底。
func googleFaviconURL(origin string) string {
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" {
		return ""
	}
	domain := u.Hostname()
	if domain == "" {
		return ""
	}
	return "https://www.google.com/s2/favicons?domain=" + url.QueryEscape(domain) + "&sz=64"
}

// resolveFavicon 返回 origin 下存在的 favicon URL：先试 favicon.ico，再试 favicon.svg；
// 都不存在则抓取首页解析 link rel="icon" 的 href（并验证 URL 是否存在）；
// 仍无则兜底使用 Google favicon 服务。
func resolveFavicon(origin string) string {
	origin = strings.TrimSuffix(origin, "/")
	if origin == "" {
		return ""
	}
	if urlExistsWithType(origin+"/favicon.ico", isIcoContent) {
		return origin + "/favicon.ico"
	}
	if urlExistsWithType(origin+"/favicon.svg", isSvgContent) {
		return origin + "/favicon.svg"
	}
	if u := resolveFaviconFromHTML(origin); u != "" {
		return u
	}
	return googleFaviconURL(origin)
}

// looksLikeOPML 根据内容前若干字符判断是否为 OPML（XML 格式）。
func looksLikeOPML(data []byte) bool {
	s := strings.TrimSpace(strings.ToLower(string(data)))
	if len(s) > 256 {
		s = s[:256]
	}
	return strings.HasPrefix(s, "<?xml") || strings.HasPrefix(s, "<opml")
}

// readSources 根据文件扩展名读取源：.opml 解析 OPML，否则按 rss.txt 格式（每行 URL 或 "分类,URL"）。
// 会校验文件内容与扩展名是否匹配。
func readSources(path string) ([]sourceEntry, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	isOPML := strings.HasSuffix(strings.ToLower(path), ".opml")
	if isOPML {
		if !looksLikeOPML(b) {
			return nil, fmt.Errorf("file %q has .opml extension but content does not appear to be OPML (expected <?xml or <opml)", path)
		}
		return parseSourcesOPML(b, path)
	}
	if looksLikeOPML(b) {
		return nil, fmt.Errorf("file %q has non-.opml extension but content appears to be OPML - use .opml extension", path)
	}
	return parseSourcesTxt(b)
}

// parseSourcesTxt 解析 txt 格式内容：每行一个 URL，或 "分类,URL"（逗号前为分类，会写入 feeds.json 的 category）。
func parseSourcesTxt(b []byte) ([]sourceEntry, error) {
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

// parseSourcesOPML 解析 OPML 内容，提取所有含 xmlUrl 的 outline；父级 outline 的 text 作为 category。
func parseSourcesOPML(b []byte, path string) ([]sourceEntry, error) {
	var doc opmlDoc
	if err := xml.Unmarshal(b, &doc); err != nil {
		return nil, fmt.Errorf("parse OPML %q: %w", path, err)
	}
	var out []sourceEntry
	var collect func(outlines []opmlOutline, category string)
	collect = func(outlines []opmlOutline, category string) {
		for _, o := range outlines {
			xmlUrl := strings.TrimSpace(o.XMLURL)
			if xmlUrl != "" {
				out = append(out, sourceEntry{Category: strings.TrimSpace(category), URL: xmlUrl})
			}
			if len(o.Outlines) > 0 {
				parentCat := strings.TrimSpace(o.Text)
				if parentCat == "" {
					parentCat = category
				}
				collect(o.Outlines, parentCat)
			}
		}
	}
	collect(doc.Body.Outlines, "")
	return out, nil
}

// opmlOutline 用于解析 OPML 的 outline 元素。
type opmlOutline struct {
	XMLName xml.Name     `xml:"outline"`
	Text    string       `xml:"text,attr"`
	XMLURL  string       `xml:"xmlUrl,attr"`
	Type    string       `xml:"type,attr"`
	Outlines []opmlOutline `xml:"outline"`
}

// opmlBody 用于解析 OPML 的 body。
type opmlBody struct {
	Outlines []opmlOutline `xml:"outline"`
}

// opmlDoc 用于解析 OPML 根元素。
type opmlDoc struct {
	XMLName xml.Name `xml:"opml"`
	Body    opmlBody `xml:"body"`
}


func newFeedClient() *http.Client {
	return &http.Client{
		Timeout: requestTimeout,
		Transport: &http.Transport{
			MaxIdleConns:        10,
			IdleConnTimeout:     30 * time.Second,
			DisableCompression:  false,
		},
	}
}

func newParser() *gofeed.Parser {
	p := gofeed.NewParser()
	p.UserAgent = userAgent
	return p
}

// sanitizeXMLBytes 移除 XML 1.0 不允许的控制字符（U+0000–U+001F 除 0x09/0x0A/0x0D），修复如 tech.youzan.com 的 illegal character U+0008 错误。
func sanitizeXMLBytes(b []byte) []byte {
	return bytes.Map(func(r rune) rune {
		if r == 0x09 || r == 0x0A || r == 0x0D {
			return r
		}
		if r >= 0x00 && r <= 0x1F || r == 0x7F {
			return -1 // 丢弃
		}
		if unicode.Is(unicode.Cc, r) {
			return -1
		}
		return r
	}, b)
}

func fetchWithBackoff(feedURL string) (*gofeed.Feed, error) {
	var lastErr error
	backoff := initialBackoff
	parser := newParser()
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			log.Printf("retry %d/%d for %s after %v", attempt, maxRetries-1, feedURL, backoff)
			time.Sleep(backoff)
			backoff *= backoffMultiplier
		}
		req, err := http.NewRequest(http.MethodGet, feedURL, nil)
		if err != nil {
			lastErr = err
			continue
		}
		req.Header.Set("User-Agent", userAgent)
		req.Header.Set("Accept", "application/rss+xml, application/atom+xml, application/xml, text/xml, */*")
		resp, err := newFeedClient().Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			backoff = 5 * time.Second
			lastErr = fmt.Errorf("http error: 429 Too Many Requests")
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			lastErr = fmt.Errorf("http error: %d %s", resp.StatusCode, resp.Status)
			continue
		}
		body = sanitizeXMLBytes(body)
		feed, err := parser.Parse(bytes.NewReader(body))
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
