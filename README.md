# feeds-aggregator

一个面向中小规模场景的订阅内容聚合工具。

它读取一组订阅源输入，抓取并汇总来源内容，整理为统一结果，并输出到 `feeds.json` 一类结果文件中，供博客、导航站或聚合页前端消费。

## 安装与运行

要求：
- Python `3.11+`

本地直接运行：

```bash
PYTHONPATH=src python3 -m feeds_aggregator.cli --sources data/rss.txt --output data/feeds.json
```

## 常用参数

- `--sources`：输入文件路径
- `--output`：输出结果文件路径，默认 `data/feeds.json`
- `--workers`：并发抓取数，默认 `8`
- `--timeout`：单个来源请求超时秒数，默认 `15`
- `--avatar-delay-ms`：avatar 发现和下载请求之间的延迟毫秒数，默认 `200`
- `--max-items-per-source`：每个来源最多保留几条，默认 `10`
- `--max-total-items`：最终结果最多保留几条，默认 `0` 表示不限制
- `--max-days`：仅保留最近多少天内容，默认 `0` 表示不限制
- `--timezone`：输出时间使用的 IANA 时区，默认 `UTC`
- `--avatar-dir`：avatar 图片本地保存目录，默认 `<output-dir>/avatars`
- `--failure-log`：可选，把失败源详情写入一个 JSON 文件
- `--validate-only`：只校验输入和配置，不抓取 feed，也不写输出文件

注意：输出的 JSON 文件中 `avatar` 字段仅包含文件名（如 `example_com_abc123.png`），不带路径前缀。

## 输入格式

当前支持两种输入形式：

### 1. 文本输入

每行一个来源，支持：
- `URL`
- `分类,URL`

示例：

```txt
https://example.com/feed.xml
tech,https://another.com/rss.xml
```

### 2. OPML 输入

支持常见 OPML 订阅导出文件。

示例文件：`data/follow.opml`

## 输出格式

输出结果是单个 JSON 文件，顶层结构如下：

```json
{
  "items": [
    {
      "title": "文章标题",
      "link": "https://example.com/post",
      "published": "2026-03-13 10:00:00",
      "name": "@Example Blog",
      "category": "tech",
      "avatar": null
    }
  ],
  "updated": "2026-03-13 12:00:00"
}
```

## 示例

生成聚合结果：

```bash
PYTHONPATH=src python3 -m feeds_aggregator.cli \
  --sources data/rss.txt \
  --output data/feeds.json \
  --workers 8 \
  --avatar-delay-ms 300 \
  --max-total-items 200 \
  --max-items-per-source 3 \
  --timezone Asia/Shanghai
```

输出运行摘要：

```bash
PYTHONPATH=src python3 -m feeds_aggregator.cli \
  --sources data/follow.opml \
  --output data/feeds.json
```

摘要 JSON 示例：

```json
{
  "outcome": "partial_success",
  "total_sources": 12,
  "successful_sources": 10,
  "failed_sources": 2,
  "output_items": 48,
  "downloaded_avatars": 9,
  "duration_seconds": 3.214,
  "output_path": "data/feeds.json",
  "failure_log_path": "data/failures.json",
  "validated_only": false,
  "failed_feed_urls": [
    "https://bad.example.com/feed.xml",
    "https://timeout.example.com/rss.xml"
  ]
}
```

写出失败源日志：

```bash
PYTHONPATH=src python3 -m feeds_aggregator.cli \
  --sources data/rss.txt \
  --output data/feeds.json \
  --failure-log data/failures.json
```

仅校验输入和参数：

```bash
PYTHONPATH=src python3 -m feeds_aggregator.cli \
  --sources data/follow.opml \
  --validate-only
```

更多约束和设计背景见：`docs/REQUIREMENTS.md`

## 测试

运行测试：

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## GitHub Actions

项目现在同时提供两种 GitHub Actions 集成方式。

标准 GitHub Action 插件入口在 [action.yml](/Users/chensoul/development/personal/feeds-aggregator/action.yml)，其他仓库可以直接 `uses:`：

```yaml
jobs:
  aggregate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: chensoul/feeds-aggregator@main
        with:
          sources: data/rss.txt
          output: data/feeds.json
          avatar-delay-ms: 300
          max-items-per-source: 20
          max-days: 30
```

标准 action 只暴露高频业务参数，不负责 Python 版本选择和 CI 编排能力。

如果你更希望把环境准备和 artifact 上传都封装在这个仓库里，也可以调用可复用工作流 [feeds-aggregator.yml](/Users/chensoul/development/personal/feeds-aggregator/.github/workflows/feeds-aggregator.yml)：

```yaml
jobs:
  aggregate:
    uses: chensoul/feeds-aggregator/.github/workflows/feeds-aggregator.yml@main
    with:
      sources: data/rss.txt
      output: data/feeds.json
      avatar-delay-ms: 300
      avatar-dir: data/avatars
      max-items-per-source: 20
      max-days: 30
```

两种方式都要求调用方仓库先 checkout 自己的代码，并把输入文件放在工作区里。可复用工作流负责 Python 环境准备；标准 action 只负责执行聚合。失败的 feed 会在任务日志和 CLI 汇总输出里显示。
