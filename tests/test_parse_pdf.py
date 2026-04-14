import importlib.util
from pathlib import Path

import pytest


MODULE_PATH = Path(__file__).resolve().parents[1] / "parse_pdf.py"
SPEC = importlib.util.spec_from_file_location("parse_pdf", MODULE_PATH)
parse_pdf = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(parse_pdf)


def test_assemble_pages_marks_missing_ocr_pages():
    pages, missing_pages = parse_pdf.assemble_pages(
        total_pages=3,
        text_pages={0: "第一页正文", 2: "第三页正文"},
        ocr_results={},
        failed_pages={1: "vision timeout"},
    )

    assert missing_pages == [1]
    assert pages[1][0] == 1
    assert "OCR失败" in pages[1][1]
    assert "vision timeout" in pages[1][1]


def test_chunk_page_entries_preserves_page_order_and_limits():
    page_entries = [
        (0, "--- 第1页 ---\n" + "A" * 40),
        (1, "--- 第2页 ---\n" + "B" * 40),
        (2, "--- 第3页 ---\n" + "C" * 40),
    ]

    chunks = parse_pdf.chunk_page_entries(page_entries, max_chars=90)

    assert len(chunks) == 3
    assert chunks[0].startswith("--- 第1页 ---")
    assert chunks[1].startswith("--- 第2页 ---")
    assert chunks[2].startswith("--- 第3页 ---")


@pytest.mark.asyncio
async def test_summarize_document_uses_chunk_then_reduce(monkeypatch):
    calls = []

    async def fake_call_llm(client, model, system, content_parts, retry=0):
        calls.append({"system": system, "content": content_parts[0]["text"]})
        return f"result-{len(calls)}"

    monkeypatch.setattr(parse_pdf, "call_llm", fake_call_llm)

    summary = await parse_pdf.summarize_document(
        client=None,
        model="fake-model",
        page_entries=[
            (0, "--- 第1页 ---\n" + "A" * 50),
            (1, "--- 第2页 ---\n" + "B" * 50),
        ],
        chunk_chars=80,
    )

    assert summary == "result-3"
    assert len(calls) == 3


@pytest.mark.asyncio
async def test_process_pdf_fails_when_ocr_pages_remain_missing(monkeypatch, tmp_path):
    monkeypatch.setattr(parse_pdf, "classify_pages", lambda _path: ({0: "第一页正文"}, {1: "fake-b64"}))
    monkeypatch.setattr(parse_pdf, "get_async_client", lambda _provider: (object(), "fake-model"))

    async def fake_ocr_single_page(*args, **kwargs):
        raise RuntimeError("ocr unavailable")

    async def fail_if_called(*args, **kwargs):
        raise AssertionError("summary should not run when pages are missing")

    monkeypatch.setattr(parse_pdf, "ocr_single_page", fake_ocr_single_page)
    monkeypatch.setattr(parse_pdf, "summarize_document", fail_if_called)

    ok = await parse_pdf.process_pdf(
        pdf_path=tmp_path / "sample.pdf",
        output_dir=tmp_path / "out",
        provider="openai",
        vision_provider=None,
    )

    assert ok is False
    assert not (tmp_path / "out" / "sample.md").exists()


@pytest.mark.asyncio
async def test_process_batch_passes_runtime_concurrency(monkeypatch, tmp_path):
    seen = []

    async def fake_process_pdf(pdf_path, output_dir, provider, vision_provider=None, file_sem=None, ocr_concurrency=None, chunk_chars=None):
        seen.append(
            {
                "pdf": Path(pdf_path).name,
                "ocr_concurrency": ocr_concurrency,
                "chunk_chars": chunk_chars,
            }
        )
        return True

    monkeypatch.setattr(parse_pdf, "process_pdf", fake_process_pdf)

    pdfs = [tmp_path / "a.pdf", tmp_path / "b.pdf"]
    for pdf in pdfs:
        pdf.write_text("stub", encoding="utf-8")

    await parse_pdf.process_batch(
        pdf_files=pdfs,
        output_dir=tmp_path / "out",
        provider="openai",
        vision_provider=None,
        file_concurrency=7,
        ocr_concurrency=2,
        chunk_chars=1234,
    )

    assert [item["ocr_concurrency"] for item in seen] == [2, 2]
    assert [item["chunk_chars"] for item in seen] == [1234, 1234]
