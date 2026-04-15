# PDF 基金报告解析工具

将基金年报/季报/审计报告 PDF 自动解析为结构化 Markdown，支持文本型和扫描型 PDF 混合处理。

## 核心特性

- **逐页智能分类**：文本页直接提取（零 API 调用），扫描/乱码页走视觉模型 OCR
- **两阶段解耦**：提取（阶段1）和汇总（阶段2）独立运行，换 prompt 或模型只需重跑阶段2
- **并发处理**：页级 OCR 并发（默认8路）+ 文件级并发（默认3路）+ 分块汇总并发
- **乱码自动检测**：识别 PDF 字体编码映射错误，自动降级为图片 OCR
- **质量校验**：财务指标交叉验证（实缴≤认缴、净资产≤总资产、DPI 交叉验算）
- **断点续跑**：已完成的文件自动跳过
- **成本预估**：`--dry-run` 模式预估 token 用量和费用

## 快速开始

### 1. 安装依赖

```bash
pip install pymupdf openai python-dotenv
```

### 2. 配置 API Key

复制 `.env.example` 为 `.env`，填入 API Key：

```bash
cp .env.example .env
# 编辑 .env 填入你的 key
```

### 3. 运行

```bash
# 处理单个文件
python parse_pdf.py 某个报告.pdf

# 处理整个文件夹
python parse_pdf.py ~/reports/

# 指定输出目录
python parse_pdf.py ~/reports/ -o ./output
```

## 用法详解

### 基本参数

```bash
python parse_pdf.py <输入路径> [选项]

选项：
  -o, --output          输出目录（默认：输入路径同级的 output/）
  -p, --provider        文本模型（默认 openai）
  -v, --vision          视觉模型（默认同 -p）
  --force               强制重新处理，忽略已有输出
```

### 阶段控制

```bash
# 只跑提取（OCR），不做汇总
python parse_pdf.py ~/reports/ --stage extract

# 只跑汇总（复用已有的 .pages.md），适合调 prompt 或换模型
python parse_pdf.py ~/reports/ --stage summarize

# 全部运行（默认）
python parse_pdf.py ~/reports/ --stage all
```

### 成本预估

```bash
python parse_pdf.py ~/reports/ --dry-run
```

输出示例：
```
  文件: 13 个
  总页数: 433 (文本 312, 扫描 121)
  预估token: ~443K input + ~64K output
  预估成本: $0.229
```

### 并发调优

```bash
# 加大 OCR 并发（适合扫描件多的场景）
python parse_pdf.py ~/reports/ --ocr-concurrency 12

# 多文件同时处理
python parse_pdf.py ~/reports/ --file-concurrency 5
```

## 输出文件

| 文件 | 说明 |
|------|------|
| `报告名.md` | 结构化汇总报告 |
| `报告名.pages.md` | 逐页提取原文（阶段1产物，可复用） |
| `报告名.errors.txt` | 失败页面记录（仅在有错误时生成） |

## 处理流程

```
PDF 输入
  │
  ├─ 逐页分类
  │   ├─ 文字量 ≥ 30 且无乱码 → 文本页（PyMuPDF 直取，0 成本）
  │   └─ 否则 → 扫描页（导出 JPEG → 视觉模型 OCR）
  │
  ├─ 阶段1：全部页面文字持久化 → .pages.md
  │
  └─ 阶段2：分块并发提取 → 归并 → 最终汇总 → .md
```

## 支持的模型

通过 `-p` 和 `-v` 参数指定，可混合使用不同模型：

| 名称 | 模型 | 适用场景 |
|------|------|---------|
| `openai` | gpt-5.4-mini（默认） | 性价比最优 |
| `5.4` | gpt-5.4 | 最高准确率 |
| `5.4-nano` | gpt-5.4-nano | 最快最便宜 |
| `4o-mini` | gpt-4o-mini | 备用 |
| `xiaomi` | mimo-v2-pro | 仅文本汇总，不支持视觉 |

```bash
# OCR 用 5.4（最准），汇总用 5.4-mini（省钱）
python parse_pdf.py report.pdf -v 5.4 -p openai
```

## 配置文件

`.env` 示例：

```env
OPENAI_API_KEY=sk-xxx
OPENAI_BASE_URL=https://api.openai.com/v1
OPENAI_MODEL=gpt-5.4-mini
```

完整配置见 `.env.example`。
