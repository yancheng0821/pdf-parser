"""
PDF基金报告解析工具
- 逐页判断：文本页直取文字，扫描/乱码页走视觉OCR
- 页级并发OCR，文件级并发处理
- 长文档按页分块抽取后再汇总
- 缺页显式失败，避免静默输出错误结果
"""

import asyncio
import base64
import os
import sys
import time
from collections import Counter
from pathlib import Path

import fitz  # pymupdf
from dotenv import load_dotenv
from openai import AsyncOpenAI

load_dotenv(Path(__file__).parent / ".env")

for key in ["all_proxy", "ALL_PROXY"]:
    os.environ.pop(key, None)

# ── 配置 ──────────────────────────────────────────────

PROVIDERS = {
    "xiaomi": {
        "api_key": os.getenv("XIAOMI_API_KEY"),
        "base_url": os.getenv("XIAOMI_BASE_URL"),
        "model": os.getenv("XIAOMI_MODEL"),
    },
    "openai": {
        "api_key": os.getenv("OPENAI_API_KEY"),
        "base_url": os.getenv("OPENAI_BASE_URL"),
        "model": os.getenv("OPENAI_MODEL"),
    },
    "4o-mini": {
        "api_key": os.getenv("OPENAI_4O_MINI_API_KEY"),
        "base_url": os.getenv("OPENAI_4O_MINI_BASE_URL"),
        "model": os.getenv("OPENAI_4O_MINI_MODEL"),
    },
    "4.1-mini": {
        "api_key": os.getenv("OPENAI_41_MINI_API_KEY"),
        "base_url": os.getenv("OPENAI_41_MINI_BASE_URL"),
        "model": os.getenv("OPENAI_41_MINI_MODEL"),
    },
    "4.1-nano": {
        "api_key": os.getenv("OPENAI_41_NANO_API_KEY"),
        "base_url": os.getenv("OPENAI_41_NANO_BASE_URL"),
        "model": os.getenv("OPENAI_41_NANO_MODEL"),
    },
    "5.4": {
        "api_key": os.getenv("OPENAI_54_API_KEY"),
        "base_url": os.getenv("OPENAI_54_BASE_URL"),
        "model": os.getenv("OPENAI_54_MODEL"),
    },
    "5.4-mini": {
        "api_key": os.getenv("OPENAI_54_MINI_API_KEY"),
        "base_url": os.getenv("OPENAI_54_MINI_BASE_URL"),
        "model": os.getenv("OPENAI_54_MINI_MODEL"),
    },
    "5.4-nano": {
        "api_key": os.getenv("OPENAI_54_NANO_API_KEY"),
        "base_url": os.getenv("OPENAI_54_NANO_BASE_URL"),
        "model": os.getenv("OPENAI_54_NANO_MODEL"),
    },
}

TEXT_THRESHOLD = 30
IMAGE_DPI = 150
IMAGE_FORMAT = "jpeg"   # jpeg比png小40%+，上传更快，省token
IMAGE_QUALITY = 85      # jpeg质量，85足够OCR
MAX_RETRIES = 2
OCR_CONCURRENCY = 8     # 提高并发
FILE_CONCURRENCY = 3
CHUNK_CHAR_LIMIT = 24_000


def get_async_client(provider: str) -> tuple[AsyncOpenAI, str]:
    cfg = PROVIDERS[provider]
    client = AsyncOpenAI(api_key=cfg["api_key"], base_url=cfg["base_url"])
    return client, cfg["model"]


# ── 文本质量检测 ──────────────────────────────────────

def is_garbled(text: str) -> bool:
    """检测文字是否乱码（字体编码映射错误）"""
    if len(text) < 30:
        return False

    cjk_chars = [c for c in text if "\u4e00" <= c <= "\u9fff"]
    if len(cjk_chars) < 10:
        return False

    freq = Counter(cjk_chars)
    top_count = sum(c for _, c in freq.most_common(8))
    top_ratio = top_count / len(cjk_chars)
    digit_ratio = sum(1 for c in text if c.isdigit()) / max(len(text), 1)

    return top_ratio > 0.4 and digit_ratio < 0.02


# ── PDF 页面分类与提取 ────────────────────────────────

def classify_pages(pdf_path: str) -> tuple[dict[int, str], dict[int, str]]:
    """
    分类所有页面（不跳页，全量处理），返回:
    - text_pages: {page_idx: extracted_text}
    - image_pages: {page_idx: base64_jpeg}
    """
    doc = fitz.open(pdf_path)
    text_pages = {}
    image_pages = {}

    for i in range(len(doc)):
        page = doc[i]
        text = page.get_text().strip()

        if len(text) >= TEXT_THRESHOLD and not is_garbled(text):
            text_pages[i] = text
        else:
            pix = page.get_pixmap(dpi=IMAGE_DPI)
            img_bytes = pix.tobytes(IMAGE_FORMAT, jpg_quality=IMAGE_QUALITY)
            image_pages[i] = base64.b64encode(img_bytes).decode()

    doc.close()
    return text_pages, image_pages


def assemble_pages(
    total_pages: int,
    text_pages: dict[int, str],
    ocr_results: dict[int, str],
    failed_pages: dict[int, str] | None = None,
) -> tuple[list[tuple[int, str]], list[int]]:
    """按页码组装全文，并显式标记缺失页。"""
    failed_pages = failed_pages or {}
    page_entries: list[tuple[int, str]] = []
    missing_pages: list[int] = []

    for page_idx in range(total_pages):
        if page_idx in text_pages:
            body = text_pages[page_idx]
        elif page_idx in ocr_results:
            body = ocr_results[page_idx]
        else:
            reason = failed_pages.get(page_idx, "未提取到任何内容")
            body = f"[OCR失败] 第{page_idx + 1}页未成功提取：{reason}"
            missing_pages.append(page_idx)

        page_entries.append((page_idx, f"--- 第{page_idx + 1}页 ---\n{body}"))

    return page_entries, missing_pages


def chunk_page_entries(page_entries: list[tuple[int, str]], max_chars: int = CHUNK_CHAR_LIMIT) -> list[str]:
    """按页分块，避免单次汇总吞下整份长文档。"""
    if not page_entries:
        return []

    chunks: list[str] = []
    current_chunk: list[str] = []
    current_len = 0

    for _, page_text in page_entries:
        page_len = len(page_text)
        next_len = current_len + page_len + (2 if current_chunk else 0)

        if current_chunk and next_len > max_chars:
            chunks.append("\n\n".join(current_chunk))
            current_chunk = [page_text]
            current_len = page_len
        else:
            current_chunk.append(page_text)
            current_len = next_len

    if current_chunk:
        chunks.append("\n\n".join(current_chunk))

    return chunks


# ── LLM 调用 ─────────────────────────────────────────

OCR_PROMPT = """请将这个扫描页面中的所有文字和表格内容提取出来，用纯文本输出。
要求：
1. 所有数字必须精确，不要猜测或四舍五入
2. 表格用 markdown 表格格式还原
3. 保持原文的段落和层次结构
4. 如果是封面、目录、免责声明等无数据页，输出"[无关键数据]"即可
直接输出内容，不要加解释。"""

CHUNK_SUMMARY_PROMPT = """你在做基金报告的分块抽取。下面只是一部分页面，不是整份报告。

请只基于当前分块中实际出现的内容，提取关键事实，使用 Markdown 输出。
要求：
1. 不要补全未出现的信息，不要猜测
2. 所有数字、日期、单位保持原文精确值
3. 表格信息尽量保留成 markdown 表格
4. 如果同一字段有多个版本，保留原值并注明对应页码
5. 没有信息的栏目写“未提及”

输出结构：
## 基本信息
## 财务指标
## 投资组合
## 现金分配
## 费用与合规
## 其他重要信息
"""

REDUCE_SUMMARY_PROMPT = """你将收到若干个分块提取结果，请合并为更紧凑的中间摘要。

要求：
1. 合并重复项，但不要丢数字、日期、单位
2. 保留投资组合和现金分配表格
3. 有冲突的数据并列保留，不要自行裁决
4. 不要生成最终结论，只做信息归并
"""

FINAL_SUMMARY_PROMPT = """你是一个专业的基金报告分析助手。以下是从一份基金报告PDF中分块提取并归并后的内容。
请将这些数据整理成一份结构化的汇总报告，使用Markdown格式。

要求的输出结构：

# [基金名称]

## 一、基金基本信息
- 基金全称、成立日期、注册地址、组织形式、存续期限
- 管理人/GP、托管人等

## 二、主要财务指标
- 认缴出资额、实缴出资额
- 基金资产总额、净资产
- DPI、IRR、TVPI/MOIC 等回报指标

## 三、投资组合概况
用表格展示所有被投企业：
| 企业名称 | 行业 | 投资日期 | 投资金额 | 最新估值/退出金额 | 持股比例 | 回报倍数 | 状态 |

分为：已退出项目、未退出项目

## 四、现金分配记录
| 序号 | 分配时间 | 分配事项 | 本次分配金额 | 累计分配金额 |

## 五、其他重要信息
- 基金费用、管理费、业绩报酬等
- 风险提示、合规信息等
- 任何其他值得注意的数据

注意：
- 所有数字保持原文精确值
- 如果某些信息在报告中未提及，标注"未提及"
- 金额单位统一标注清楚（万元/元/美元等）
- 如果输入中明确提示有缺失页或OCR失败，要在文末增加“解析风险提示”说明结果可能不完整
"""


async def call_llm(client: AsyncOpenAI, model: str, system: str, content_parts: list, retry: int = 0) -> str:
    # gpt-5+ 系列要求 max_completion_tokens，老模型用 max_tokens
    use_new_param = any(model.startswith(p) for p in ("gpt-5", "o1", "o3", "o4"))
    token_param = {"max_completion_tokens": 8192} if use_new_param else {"max_tokens": 8192}

    try:
        resp = await client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": content_parts},
            ],
            temperature=0.1,
            **token_param,
        )
        return resp.choices[0].message.content or ""
    except Exception:
        if retry < MAX_RETRIES:
            await asyncio.sleep(2 ** retry)
            return await call_llm(client, model, system, content_parts, retry + 1)
        raise


async def ocr_single_page(
    client: AsyncOpenAI,
    model: str,
    page_idx: int,
    b64_img: str,
    semaphore: asyncio.Semaphore,
    progress: dict,
) -> tuple[int, str]:
    """OCR单页，受信号量控制并发。"""
    async with semaphore:
        content = [
            {"type": "text", "text": f"第{page_idx + 1}页："},
            {
                "type": "image_url",
                "image_url": {
                    "url": f"data:image/{IMAGE_FORMAT};base64,{b64_img}",
                    "detail": "high",
                },
            },
        ]
        result = await call_llm(client, model, OCR_PROMPT, content)
        progress["done"] += 1
        print(f"    OCR {progress['done']}/{progress['total']} 完成 (第{page_idx + 1}页)")
        return page_idx, result


async def summarize_document(
    client: AsyncOpenAI,
    model: str,
    page_entries: list[tuple[int, str]],
    chunk_chars: int = CHUNK_CHAR_LIMIT,
) -> str:
    """长文档采用分块抽取 + 必要时中间归并 + 最终汇总。"""
    chunks = chunk_page_entries(page_entries, max_chars=chunk_chars)
    if not chunks:
        return ""

    if len(chunks) == 1:
        return await call_llm(client, model, FINAL_SUMMARY_PROMPT, [{"type": "text", "text": chunks[0]}])

    # 所有分块并发提取
    print(f"  并发抽取 {len(chunks)} 个分块...")

    async def extract_chunk(idx: int, chunk: str) -> str:
        note = await call_llm(
            client, model, CHUNK_SUMMARY_PROMPT,
            [{"type": "text", "text": chunk}],
        )
        print(f"    分块 {idx}/{len(chunks)} 完成")
        return f"## 分块 {idx}\n{note}"

    chunk_tasks = [extract_chunk(i, c) for i, c in enumerate(chunks, 1)]
    chunk_notes = list(await asyncio.gather(*chunk_tasks))

    merged_notes = "\n\n".join(chunk_notes)
    while len(merged_notes) > chunk_chars and len(chunk_notes) > 1:
        pairs = [chunk_notes[i:i + 2] for i in range(0, len(chunk_notes), 2)]
        print(f"  并发归并 {len(pairs)} 组...")

        async def reduce_pair(group: list[str]) -> str:
            if len(group) == 1:
                return group[0]
            return await call_llm(
                client, model, REDUCE_SUMMARY_PROMPT,
                [{"type": "text", "text": "\n\n".join(group)}],
            )

        chunk_notes = list(await asyncio.gather(*[reduce_pair(p) for p in pairs]))
        merged_notes = "\n\n".join(chunk_notes)

    print("  生成最终汇总...")
    return await call_llm(
        client,
        model,
        FINAL_SUMMARY_PROMPT,
        [{"type": "text", "text": merged_notes}],
    )


# ── 单文件处理 ────────────────────────────────────────

async def process_pdf(
    pdf_path: str,
    output_dir: Path,
    provider: str,
    vision_provider: str | None = None,
    file_sem: asyncio.Semaphore | None = None,
    ocr_concurrency: int = OCR_CONCURRENCY,
    chunk_chars: int = CHUNK_CHAR_LIMIT,
) -> bool:
    """处理单个PDF，返回是否成功。"""
    pdf_path = Path(pdf_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / (pdf_path.stem + ".md")
    raw_path = output_dir / (pdf_path.stem + ".pages.md")
    error_path = output_dir / (pdf_path.stem + ".errors.txt")

    if out_path.exists():
        print(f"  [跳过] {pdf_path.name} (已有输出)")
        return True

    sem = file_sem or asyncio.Semaphore(1)
    async with sem:
        t0 = time.time()
        print(f"\n{'─' * 60}")
        print(f"  处理: {pdf_path.name}")

        text_pages, image_pages = classify_pages(str(pdf_path))
        total_pages = len(text_pages) + len(image_pages)
        print(f"  共 {total_pages} 页 | 文本 {len(text_pages)} (直取) | 扫描 {len(image_pages)} (需OCR)")

        ocr_results: dict[int, str] = {}
        failed_pages: dict[int, str] = {}

        if image_pages:
            v_client, v_model = get_async_client(vision_provider or provider)
            ocr_sem = asyncio.Semaphore(ocr_concurrency)
            progress = {"done": 0, "total": len(image_pages)}

            jobs = list(image_pages.items())
            tasks = [
                ocr_single_page(v_client, v_model, idx, b64, ocr_sem, progress)
                for idx, b64 in jobs
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for (page_idx, _), result in zip(jobs, results):
                if isinstance(result, Exception):
                    failed_pages[page_idx] = str(result)
                    print(f"    OCR失败: 第{page_idx + 1}页 | {result}")
                else:
                    ocr_results[result[0]] = result[1]

        page_entries, missing_pages = assemble_pages(
            total_pages=total_pages,
            text_pages=text_pages,
            ocr_results=ocr_results,
            failed_pages=failed_pages,
        )

        combined = "\n\n".join(page_text for _, page_text in page_entries)
        raw_path.write_text(combined, encoding="utf-8")
        print(f"  提取后全文 {len(combined)} 字符")

        if missing_pages:
            missing_page_numbers = ", ".join(str(idx + 1) for idx in missing_pages)
            error_message = (
                f"以下页面提取失败，已中止最终汇总：第 {missing_page_numbers} 页。\n"
                "请降低并发、重试，或单独处理失败页面。"
            )
            error_path.write_text(error_message, encoding="utf-8")
            print(f"  [失败] {error_message}")
            return False

        if error_path.exists():
            error_path.unlink()

        t_client, t_model = get_async_client(provider)
        summary = await summarize_document(
            client=t_client,
            model=t_model,
            page_entries=page_entries,
            chunk_chars=chunk_chars,
        )

        out_path.write_text(summary, encoding="utf-8")
        elapsed = time.time() - t0
        print(f"  done {pdf_path.name} ({elapsed:.1f}s)")
        return True


# ── 批量处理 ──────────────────────────────────────────

async def process_batch(
    pdf_files: list[Path],
    output_dir: Path,
    provider: str,
    vision_provider: str | None = None,
    file_concurrency: int = FILE_CONCURRENCY,
    ocr_concurrency: int = OCR_CONCURRENCY,
    chunk_chars: int = CHUNK_CHAR_LIMIT,
):
    """并发处理多个PDF文件。"""
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"共 {len(pdf_files)} 个PDF")
    print(f"文本模型: {provider} ({PROVIDERS[provider]['model']})")
    vp = vision_provider or provider
    print(f"视觉模型: {vp} ({PROVIDERS[vp]['model']})")
    print(f"并发: 文件×{file_concurrency}, 页OCR×{ocr_concurrency}")
    print(f"分块阈值: {chunk_chars} 字符")
    print(f"输出: {output_dir}")

    file_sem = asyncio.Semaphore(file_concurrency)
    tasks = [
        process_pdf(
            f,
            output_dir,
            provider,
            vision_provider=vision_provider,
            file_sem=file_sem,
            ocr_concurrency=ocr_concurrency,
            chunk_chars=chunk_chars,
        )
        for f in pdf_files
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    success = sum(1 for r in results if r is True)
    failed = len(results) - success
    print(f"\n{'=' * 60}")
    print(f"完成! 成功 {success}, 失败 {failed}")
    print(f"输出目录: {output_dir}")


# ── CLI ───────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="PDF基金报告解析工具")
    parser.add_argument("input", help="PDF文件或文件夹路径")
    parser.add_argument("-o", "--output", default=None, help="输出目录")
    parser.add_argument(
        "-p",
        "--provider",
        default="openai",
        choices=list(PROVIDERS.keys()),
        help="文本模型",
    )
    parser.add_argument(
        "-v",
        "--vision",
        default=None,
        choices=list(PROVIDERS.keys()),
        help="视觉模型（默认同-p）",
    )
    parser.add_argument(
        "--ocr-concurrency",
        type=int,
        default=OCR_CONCURRENCY,
        help=f"单文件OCR并发数 (默认{OCR_CONCURRENCY})",
    )
    parser.add_argument(
        "--file-concurrency",
        type=int,
        default=FILE_CONCURRENCY,
        help=f"文件并发数 (默认{FILE_CONCURRENCY})",
    )
    parser.add_argument(
        "--chunk-chars",
        type=int,
        default=CHUNK_CHAR_LIMIT,
        help=f"分块汇总字符上限 (默认{CHUNK_CHAR_LIMIT})",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="强制重新处理（忽略已有输出）",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    if input_path.is_file():
        pdf_files = [input_path]
    elif input_path.is_dir():
        pdf_files = sorted(list(input_path.glob("*.pdf")) + list(input_path.glob("*.PDF")))
    else:
        print(f"错误: {input_path} 不存在")
        sys.exit(1)

    if not pdf_files:
        print("未找到PDF文件")
        sys.exit(1)

    output_dir = Path(args.output) if args.output else input_path.parent / "output"

    if args.force:
        for f in pdf_files:
            for suffix in (".md", ".pages.md", ".errors.txt"):
                target = output_dir / f"{f.stem}{suffix}"
                if target.exists():
                    target.unlink()

    asyncio.run(
        process_batch(
            pdf_files=pdf_files,
            output_dir=output_dir,
            provider=args.provider,
            vision_provider=args.vision,
            file_concurrency=args.file_concurrency,
            ocr_concurrency=args.ocr_concurrency,
            chunk_chars=args.chunk_chars,
        )
    )


if __name__ == "__main__":
    main()
