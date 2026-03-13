# feeds-aggregator

从 RSS 源列表文件（默认 `data/rss.txt` 或 `data/rss.opml`）并发抓取并解析，生成 `feeds.json`（默认 `data/feeds.json`）。支持 **.txt** 与 **.opml** 两种输入格式。会根据扩展名校验文件内容是否匹配（.opml 需为 OPML 格式，.txt 不能为 OPML）。支持作为 **GitHub Action** 在任意仓库中使用，并可通过市场发布。

## 功能

- **并发抓取与解析**：通过 `-workers` 控制并发数（默认 8，范围 1–64）；每次请求使用独立 Parser/Client，避免并发竞争
- **异常与日志**：错误写入 stderr 与 `logs/feeds-YYYY-MM-DD.log`，按日分文件，可配置保留天数
- **超时**：单次 HTTP 请求超时 30 秒，整体运行无总时长限制
- **指数退避重试**：请求失败时按 1s → 2s → 4s 退避，最多 3 次重试
- **时区**：`updated` 字段可按 IANA 时区（如 `Asia/Shanghai`）输出
- **HTTP**：自定义 User-Agent、合理连接池与超时，减少被站点拒绝
- **排序**：按发布时间倒序，相同时按 link 稳定排序
- **单源条数**：`-maxItemsPerFeed`（默认 0=不限制），先按发布时间排序再取最新 N 条
- **总条数上限**：`-maxTotalItems`（默认 0=不限制），在排序、去重后截断，控制最终 JSON 大小
- **抓取最近 N 天**：`-maxDays`（默认 365），只保留发布时间在最近 N 天内的条目；0 表示不按天数限制
- **按 link 去重**：`-dedup`（默认 true），同一链接只保留一条（保留最新），避免多源重复
- **Favicon 本地化**：`-faviconDir` 为写入目录（如 `public/favicon`），`-faviconPathPrefix` 为 JSON 中 avatar 的前缀；文件名为 `{domain}_{hash(avatarURL)}.{原始后缀}`，同一头像 URL 对应同一文件，不同频道/用户（如 YouTube、B站）因 URL 不同而各有一份；`avatar = faviconPathPrefix + "/" + filename`；已存在则跳过下载
- **请求超时**：`-requestTimeout`（默认 30s），可设为 1m 等
- **运行统计**：结束时输出成功/失败源数、总条数、输出路径与耗时
- **输出方式**：每次运行**覆盖**写入 `output` 指定文件，不追加

---

## 作为 GitHub Action 使用

在任意仓库的 workflow 中引用本 action，通过 inputs 配置 sources、output、timezone 等：

```yaml
steps:
  - uses: actions/checkout@v4

  - name: Run feeds-aggregator
    uses: chensoul/feeds-aggregator@main
    with:
      sources: data/rss.txt          # 相对工作区的 RSS 列表路径（默认）
      output: data/feeds.json       # 输出路径（默认）
      timezone: Asia/Shanghai
      workers: "8"
      logdir: logs                  # 日志目录（默认），空则不打文件日志
```

### Action 输入（inputs）

| 输入                | 必填 | 默认值               | 说明                            |
|-------------------|----|-------------------|-------------------------------|
| `sources`         | 是  | `data/rss.txt`    | RSS 源列表路径：.txt 或 .opml（相对仓库根） |
| `output`          | 是  | `data/feeds.json` | 输出的 feeds.json 路径（相对仓库根）      |
| `timezone`        | 否  | `Asia/Shanghai`   | IANA 时区，用于 `updated` 显示时间     |
| `workers`         | 否  | `8`               | 并发抓取数                         |
| `logdir`          | 否  | `logs`            | 日志目录（相对仓库根），空则不打文件日志          |
| `logretention`    | 否  | `7`               | 日志保留天数，超过的自动删除；0 表示不清理        |
| `maxItemsPerFeed` | 否  | `0`               | 每个 RSS 源最多取几条（0=不限制）          |
| `maxTotalItems`   | 否  | `0`               | 输出总条数上限（0=不限制）                |
| `maxDays`         | 否  | `365`             | 只保留最近 N 天的条目（0=不限制）           |
| `dedup`           | 否  | `true`            | 是否按 link 去重                   |
| `faviconDir`        | 否  | 空                | 将 favicon 下载到此目录（相对工作区）；空则使用远程 URL     |
| `faviconPathPrefix` | 否  | 空（同 faviconDir） | JSON 中 avatar 路径前缀，avatar = faviconPathPrefix + "/" + filename（filename = domain_hash.ext） |
| `requestTimeout`    | 否  | `10s`             | 单次 HTTP 请求超时（如 30s、1m）          |

### 发布到 Action 市场

1. 在本仓库打 tag（如 `feeds-aggregator/v1.0.0`）或使用默认分支。
2. 在 GitHub 仓库 **Settings → Actions → General** 中允许该仓库的 Action 被其他仓库使用。
3. 若要上架 [GitHub Marketplace](https://github.com/marketplace?type=actions)，在 **Releases** 中创建 Release 并勾选 “Publish to GitHub Marketplace”，按提示填写描述与分类。

他人引用示例：`uses: chensoul/feeds-aggregator@v1`

---

## 本地 / CLI 用法

首次使用建议在 `feeds-aggregator` 目录执行 `go mod tidy` 生成 `go.sum`（CI 中会自动拉取依赖）。

```bash
go mod tidy   # 可选，用于生成 go.sum
go run . -sources=data/rss.txt -output=data/feeds.json
# 或使用 OPML：
go run . -sources=data/rss.opml -output=data/feeds.json
```

或先编译再运行：

```bash
go build -o feeds-aggregator .
./feeds-aggregator -sources=data/rss.txt -output=data/feeds.json -workers=8 -logdir=logs
```

### 参数

| 参数                 | 默认值               | 说明                                        |
|--------------------|-------------------|-------------------------------------------|
| `-sources`         | `data/rss.txt`    | RSS 源列表：.txt（每行 URL 或「分类,URL」）或 .opml     |
| `-output`          | `data/feeds.json` | 输出的 `feeds.json` 路径                       |
| `-workers`         | `8`               | 并发抓取数                                     |
| `-timezone`        | 空（本机时区）           | IANA 时区，用于 `updated` 字段                   |
| `-logdir`          | `logs`            | 日志目录，文件名为 `feeds-2006-01-02.log`；空则不打文件日志 |
| `-logretention`    | `7`               | 日志保留最近 N 天，超期自动删除；0 表示不清理                 |
| `-maxItemsPerFeed` | `0`               | 每个 RSS 源最多取几条（0=不限制）；会先按时间排序再取最新 N 条      |
| `-maxTotalItems`   | `0`               | 输出中最多保留总条数（0=不限制），在排序、去重之后截断              |
| `-maxDays`         | `365`             | 只保留最近 N 天内的条目（0=不限制）                        |
| `-dedup`           | `true`            | 是否按 link 去重（保留首次出现即最新一条）                  |
| `-faviconDir`        | 空                | 将 favicon 下载到该目录（如 public/favicon），文件名为 `{domain}_{hash}.{后缀}`，同一 URL 同文件、多用户/频道（如 YouTube、B站）不冲突；已存在则跳过 |
| `-faviconPathPrefix` | 空（同 faviconDir） | JSON 中 avatar 前缀，avatar = faviconPathPrefix + "/" + filename   |
| `-requestTimeout`    | `10s`             | 单次 HTTP 请求超时（如 30s、1m）                        |

## 输入格式

### .txt 格式（如 data/rss.txt）

- **仅 URL**：一行只有一个地址时，该源下所有文章在 feeds.json 中**不带** `category` 字段（前端不显示分类）
- **分类,URL**：行内第一个逗号前为分类，会写入每条文章的 `category`，便于前端分类展示
- 空行、以 `#` 开头的行会被忽略  
- 同一文件中可以混用「仅 URL」和「分类,URL」两种格式

示例：

```
https://blog.example.com/feed.xml
blog,https://another.com/atom.xml
https://third.com/rss
```

### .opml 格式（如 data/rss.opml）

支持标准 OPML 2.0 订阅导出。从所有 `<outline xmlUrl="...">` 提取 RSS 地址；父级 `<outline text="...">` 的 `text` 作为 `category`。例如：

```xml
<opml><body>
  <outline text="博客">
    <outline text="某博客" xmlUrl="https://blog.example.com/feed.xml" type="rss"/>
  </outline>
</body></opml>
```

上述条目会以 `category="博客"` 写入 feeds.json。

**校验规则**：程序会校验文件内容与扩展名是否匹配。`.opml` 文件内容需以 `<?xml` 或 `<opml` 开头；`.txt` 文件若内容为 OPML 格式，会提示改用 `.opml` 扩展名。

## 输出格式

生成的 JSON 供博客邻居页使用。`published` 与 `updated` 均为 `YYYY-MM-DD HH:MM:SS`。每条 item 可含 `category`（来自 rss.txt 的「分类,URL」）。**avatar**：若 feed 未提供图片，则检查站点 `origin/favicon.ico` 是否存在；不存在则检查 `origin/favicon.svg`；再不存在则抓取首页 HTML，解析 `link rel="icon"` 的 `href`；仍无则兜底使用 Google favicon 服务。当指定 `-faviconDir` 时，会将 favicon 下载到该目录，文件名为 `{domain}_{hash(avatarURL)}.{后缀}`（同一头像 URL 对应同一文件，多用户/频道如 YouTube、B站互不覆盖），已存在则跳过；`avatar` = `faviconPathPrefix` + "/" + filename；未指定 `-faviconDir` 时 `avatar` 为远程 URL。由前端回退到默认图仅在以上均不可用时。

```json
{
  "items": [
    {
      "category": "blog",
      "blog_name": "@博客名",
      "title": "文章标题",
      "published": "2026-03-10 12:00:00",
      "link": "https://...",
      "avatar": "https://..."
    }
  ],
  "updated": "2026-03-10 12:00:00"
}
```

前端会解析并按「今年内相对时间、往年绝对日期」展示。