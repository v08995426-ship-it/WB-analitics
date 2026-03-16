#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import difflib
import hashlib
import html
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections import deque
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

USER_AGENT = "Mozilla/5.0 (compatible; OzonDocsMonitor/2.0; +https://github.com/)"
REQUEST_TIMEOUT = 40
RETRY_COUNT = 3
STATE_DIRNAME = ".ozon-docs-monitor"
STATE_FILENAME = "state.json"
RUN_REPORT_FILENAME = "last_report.json"
URL_INDEX_FILENAME = "url_index.json"
DEBUG_FILENAME = "debug.json"
TELEGRAM_MESSAGE_LIMIT = 3800
MAX_CRAWL_PAGES = 120
CRAWL_DEPTH = 2
URL_INDEX_TTL_HOURS = 24

DOC_SEEDS = [
    "https://docs.ozon.ru/global/en/api/intro/",
    "https://docs.ozon.ru/global/en/api/",
    "https://docs.ozon.ru/api/intro/",
    "https://docs.ozon.ru/api/",
    "https://docs.ozon.ru/global/en/api/seller/",
    "https://docs.ozon.ru/api/seller/",
]

SITEMAP_CANDIDATES = [
    "https://docs.ozon.ru/sitemap.xml",
    "https://docs.ozon.ru/sitemap_index.xml",
    "https://docs.ozon.ru/global/en/sitemap.xml",
    "https://docs.ozon.ru/global/en/sitemap_index.xml",
]

ALERT_PATTERNS = [
    re.compile(r"метод\s+устарева", re.IGNORECASE),
    re.compile(r"будет\s+отключ", re.IGNORECASE),
    re.compile(r"deprecated", re.IGNORECASE),
    re.compile(r"deprecat", re.IGNORECASE),
    re.compile(r"sunset", re.IGNORECASE),
    re.compile(r"removed?\s+after", re.IGNORECASE),
    re.compile(r"no\s+longer\s+supported", re.IGNORECASE),
    re.compile(r"используйте\s+/v\d+/[\w\-/{}/]+", re.IGNORECASE),
    re.compile(r"use\s+/v\d+/[\w\-/{}/]+", re.IGNORECASE),
    re.compile(r"замен[её]н", re.IGNORECASE),
]

DATE_PATTERNS = [
    re.compile(r"\b\d{4}-\d{2}-\d{2}\b"),
    re.compile(r"\b\d{1,2}\.\d{1,2}\.\d{4}\b"),
    re.compile(r"\b\d{1,2}\s+[а-яА-ЯёЁ]+\s+\d{4}\s+года\b"),
    re.compile(r"\b\d{1,2}\s+[A-Za-z]+\s+\d{4}\b"),
]

URL_RE = re.compile(r'https?://[^\s"\'<>]+')
HREF_RE = re.compile(r'href=["\']([^"\']+)["\']', re.IGNORECASE)
ABS_ENDPOINT_RE = re.compile(r'/v\d+/[\w\-/{}/]+')
NEXT_DATA_RE = re.compile(r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>', re.IGNORECASE | re.DOTALL)


@dataclass
class MethodResult:
    endpoint: str
    doc_url: Optional[str]
    found: bool
    changed: bool
    severity: str
    warnings: List[str]
    replacements: List[str]
    dates: List[str]
    summary: str
    diff_excerpt: Optional[str]
    error: Optional[str] = None
    status: str = "ok"
    diagnostics: Optional[List[str]] = None


class MonitorError(Exception):
    pass


class NotFoundInDocs(MonitorError):
    pass


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_iso() -> str:
    return now_utc().replace(microsecond=0).isoformat()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def read_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise MonitorError(f"Файл JSON поврежден: {path}: {exc}") from exc


def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def fetch_url(url: str) -> Tuple[str, str, str]:
    last_error = None
    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Language": "ru,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    for attempt in range(1, RETRY_COUNT + 1):
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                content_type = resp.headers.get_content_type() or "text/html"
                charset = resp.headers.get_content_charset() or "utf-8"
                body = resp.read().decode(charset, errors="replace")
                return body, resp.geturl(), content_type
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as exc:
            last_error = exc
            if attempt < RETRY_COUNT:
                time.sleep(2 * attempt)
            continue
    raise MonitorError(f"Не удалось загрузить {url}: {last_error}")


def normalize_whitespace(text: str) -> str:
    text = text.replace("\xa0", " ")
    text = re.sub(r"\r", "\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def clean_html_to_text(raw_html: str) -> str:
    text = raw_html
    text = re.sub(r"(?is)<script.*?>.*?</script>", " ", text)
    text = re.sub(r"(?is)<style.*?>.*?</style>", " ", text)
    text = re.sub(r"(?is)<!--.*?-->", " ", text)
    text = re.sub(r"(?is)<br\s*/?>", "\n", text)
    text = re.sub(r"(?is)</(p|div|li|tr|section|article|h\d)>", "\n", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    text = html.unescape(text)
    lines = [re.sub(r"\s+", " ", line).strip() for line in text.splitlines()]
    lines = [line for line in lines if line]
    return "\n".join(lines)


def extract_next_data_text(raw_html: str) -> str:
    chunks: List[str] = []
    for m in NEXT_DATA_RE.finditer(raw_html):
        raw_json = html.unescape(m.group(1))
        try:
            payload = json.loads(raw_json)
        except json.JSONDecodeError:
            continue

        def walk(obj):
            if isinstance(obj, dict):
                for value in obj.values():
                    yield from walk(value)
            elif isinstance(obj, list):
                for value in obj:
                    yield from walk(value)
            elif isinstance(obj, str):
                value = obj.strip()
                if value:
                    yield value

        chunks.extend(walk(payload))
    return "\n".join(chunks)


def normalize_doc_text(raw_html: str) -> str:
    parts = [clean_html_to_text(raw_html)]
    next_data_text = extract_next_data_text(raw_html)
    if next_data_text:
        parts.append(next_data_text)
    combined = "\n".join(part for part in parts if part)
    return normalize_whitespace(combined)


def normalize_endpoint(endpoint: str) -> str:
    endpoint = str(endpoint).strip()
    endpoint = re.sub(r"\s+", "", endpoint)
    if not endpoint.startswith("/"):
        endpoint = "/" + endpoint
    return endpoint


def endpoint_variants(endpoint: str) -> List[str]:
    endpoint = normalize_endpoint(endpoint)
    variants = {endpoint}
    variants.add(endpoint.rstrip("/"))
    variants.add(endpoint.strip("/"))
    variants.add(endpoint.replace("{", "").replace("}", ""))
    variants.add(endpoint.replace("{", "[").replace("}", "]"))
    variants.add(endpoint.replace("{", "<").replace("}", ">"))
    variants.add(endpoint.replace("{", ":").replace("}", ""))
    variants.add(endpoint.replace("{", "").replace("}", "").replace("//", "/"))
    return [v for v in variants if v]


def endpoint_regex(endpoint: str) -> re.Pattern:
    endpoint = normalize_endpoint(endpoint)
    out = []
    i = 0
    while i < len(endpoint):
        ch = endpoint[i]
        if ch == "{":
            j = endpoint.find("}", i + 1)
            if j == -1:
                out.append(re.escape(ch))
                i += 1
                continue
            out.append(r"[^/\\\s\"'<>]+")
            i = j + 1
            continue
        out.append(re.escape(ch))
        i += 1
    pattern = "".join(out)
    return re.compile(pattern)


def parse_sitemap_xml(xml_text: str) -> List[str]:
    urls: List[str] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return urls

    for elem in root.iter():
        tag = elem.tag.lower()
        if tag.endswith("loc") and elem.text:
            urls.append(elem.text.strip())
    return urls


def normalize_url(url: str, base: Optional[str] = None) -> Optional[str]:
    if base:
        url = urllib.parse.urljoin(base, url)
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return None
    if not parsed.netloc.endswith("docs.ozon.ru"):
        return None
    cleaned = parsed._replace(fragment="", query="")
    normalized = urllib.parse.urlunparse(cleaned)
    if normalized.endswith("/") and len(normalized) > len("https://docs.ozon.ru/"):
        normalized = normalized.rstrip("/") + "/"
    return normalized


def is_promising_doc_url(url: str) -> bool:
    parsed = urllib.parse.urlparse(url)
    path = parsed.path.lower()
    if not parsed.netloc.endswith("docs.ozon.ru"):
        return False
    if "/api/" not in path and not path.endswith("/api/"):
        return False
    if path.endswith((".jpg", ".jpeg", ".png", ".gif", ".svg", ".css", ".js", ".ico", ".pdf", ".xml")):
        return False
    return True


def extract_links(raw_html: str, base_url: str) -> List[str]:
    links: Set[str] = set()
    for href in HREF_RE.findall(raw_html):
        normalized = normalize_url(href, base=base_url)
        if normalized and is_promising_doc_url(normalized):
            links.add(normalized)
    for found in URL_RE.findall(raw_html):
        normalized = normalize_url(found)
        if normalized and is_promising_doc_url(normalized):
            links.add(normalized)
    return sorted(links)


def build_url_index(state_dir: Path) -> Dict[str, List[str]]:
    index_path = state_dir / URL_INDEX_FILENAME
    existing = read_json(index_path, {}) if index_path.exists() else {}
    fetched_at = existing.get("fetched_at") if isinstance(existing, dict) else None
    urls = existing.get("urls") if isinstance(existing, dict) else None

    if fetched_at and isinstance(urls, list):
        try:
            dt = datetime.fromisoformat(fetched_at)
            if now_utc() - dt < timedelta(hours=URL_INDEX_TTL_HOURS):
                return {"fetched_at": fetched_at, "urls": urls}
        except ValueError:
            pass

    discovered: Set[str] = set()
    diagnostics: List[str] = []

    for sm_url in SITEMAP_CANDIDATES:
        try:
            body, final_url, content_type = fetch_url(sm_url)
            if "xml" in content_type or body.lstrip().startswith("<?xml") or "<urlset" in body or "<sitemapindex" in body:
                for url in parse_sitemap_xml(body):
                    normalized = normalize_url(url)
                    if normalized and is_promising_doc_url(normalized):
                        discovered.add(normalized)
            diagnostics.append(f"sitemap ok: {final_url}")
        except Exception as exc:  # noqa: BLE001
            diagnostics.append(f"sitemap fail: {sm_url}: {exc}")

    queue = deque((normalize_url(seed), 0) for seed in DOC_SEEDS if normalize_url(seed))
    seen: Set[str] = set()
    crawled = 0

    while queue and crawled < MAX_CRAWL_PAGES:
        url, depth = queue.popleft()
        if not url or url in seen:
            continue
        seen.add(url)
        try:
            raw_html, final_url, content_type = fetch_url(url)
            crawled += 1
            final_url = normalize_url(final_url) or url
            if is_promising_doc_url(final_url):
                discovered.add(final_url)
            if "html" in content_type:
                for link in extract_links(raw_html, final_url):
                    if link not in seen:
                        discovered.add(link)
                        if depth < CRAWL_DEPTH:
                            queue.append((link, depth + 1))
        except Exception as exc:  # noqa: BLE001
            diagnostics.append(f"crawl fail: {url}: {exc}")

    payload = {
        "fetched_at": now_iso(),
        "urls": sorted(discovered),
        "diagnostics": diagnostics,
    }
    write_json(index_path, payload)
    return payload


def score_url_for_endpoint(url: str, endpoint: str) -> int:
    endpoint = normalize_endpoint(endpoint)
    path = urllib.parse.urlparse(url).path.lower()
    score = 0
    chunks = [chunk for chunk in endpoint.strip("/").split("/") if chunk and not chunk.startswith("{")]
    for chunk in chunks:
        if chunk.lower() in path:
            score += 2
    last = chunks[-1].lower() if chunks else ""
    if last and last in path:
        score += 3
    if "/seller/" in path:
        score += 1
    if "/api/" in path:
        score += 1
    return score


def extract_context(text: str, endpoint: str, radius: int = 4000) -> Optional[str]:
    regex = endpoint_regex(endpoint)
    variants = endpoint_variants(endpoint)

    idx = -1
    matched = None
    for variant in variants:
        idx = text.find(variant)
        if idx != -1:
            matched = variant
            break
    if idx == -1:
        regex_match = regex.search(text)
        if regex_match:
            idx = regex_match.start()
            matched = regex_match.group(0)
    if idx == -1:
        return None

    start = max(0, idx - radius)
    end = min(len(text), idx + radius)
    snippet = text[start:end]
    parts = snippet.split("\n")
    context_lines: List[str] = []

    for i, line in enumerate(parts):
        if matched and matched in line:
            lo = max(0, i - 6)
            hi = min(len(parts), i + 7)
            context_lines.extend(parts[lo:hi])
            continue
        if any(p.search(line) for p in ALERT_PATTERNS):
            lo = max(0, i - 3)
            hi = min(len(parts), i + 4)
            context_lines.extend(parts[lo:hi])

    if not context_lines:
        return snippet[: min(len(snippet), 5000)]

    deduped: List[str] = []
    seen: Set[str] = set()
    for line in context_lines:
        line = line.strip()
        if line and line not in seen:
            seen.add(line)
            deduped.append(line)
    return "\n".join(deduped[:120])


def detect_warnings(context: str) -> Tuple[List[str], List[str], List[str]]:
    warnings: List[str] = []
    replacements: List[str] = []
    dates: List[str] = []

    for line in context.splitlines():
        line_clean = line.strip()
        if not line_clean:
            continue
        if any(p.search(line_clean) for p in ALERT_PATTERNS):
            warnings.append(line_clean)
        for m in ABS_ENDPOINT_RE.finditer(line_clean):
            replacements.append(m.group(0))
        for dp in DATE_PATTERNS:
            for m in dp.finditer(line_clean):
                dates.append(m.group(0))

    return sorted(set(warnings)), sorted(set(replacements)), sorted(set(dates))


def make_diff(old: str, new: str, max_lines: int = 120) -> Optional[str]:
    if old == new:
        return None
    diff = list(
        difflib.unified_diff(
            old.splitlines(),
            new.splitlines(),
            fromfile="previous",
            tofile="current",
            lineterm="",
        )
    )
    if not diff:
        return None
    return "\n".join(diff[:max_lines])


def classify(status: str, changed: bool, warnings: List[str], error: Optional[str]) -> str:
    if status == "fetch_error" or error:
        return "error"
    if warnings:
        return "critical"
    if status == "not_found":
        return "warning"
    if changed:
        return "important"
    return "info"


def summarize(endpoint: str, status: str, changed: bool, warnings: List[str], replacements: List[str], dates: List[str], error: Optional[str]) -> str:
    if status == "fetch_error":
        return f"{endpoint}: ошибка загрузки документации"
    if status == "not_found":
        return f"{endpoint}: метод не найден в доступных страницах документации"
    if warnings:
        parts = [f"{endpoint}: найдено предупреждение в документации"]
        if dates:
            parts.append(f"дата: {', '.join(dates)}")
        if replacements:
            parts.append(f"замена: {', '.join(replacements)}")
        return "; ".join(parts)
    if changed:
        return f"{endpoint}: страница метода изменилась, но явного deprecation-предупреждения не найдено"
    return f"{endpoint}: изменений не обнаружено"


def load_tracked_methods(path: Path) -> List[str]:
    payload = read_json(path, None)
    if payload is None:
        raise MonitorError(f"Не найден файл со списком методов: {path}")
    methods = payload.get("methods", []) if isinstance(payload, dict) else payload
    methods = [normalize_endpoint(x) for x in methods if normalize_endpoint(x)]
    methods = sorted(set(methods))
    if not methods:
        raise MonitorError("Список отслеживаемых методов пуст")
    return methods


def try_pages_for_endpoint(endpoint: str, urls: Iterable[str], debug: Dict[str, dict]) -> Tuple[Optional[str], Optional[str], List[str]]:
    diagnostics: List[str] = []
    regex = endpoint_regex(endpoint)
    variants = endpoint_variants(endpoint)

    ordered_urls = sorted(set(urls), key=lambda u: (-score_url_for_endpoint(u, endpoint), len(u)))
    for url in ordered_urls:
        try:
            raw_html, final_url, content_type = fetch_url(url)
            text = normalize_doc_text(raw_html)
            matched = any(variant and variant in text for variant in variants) or bool(regex.search(text))
            debug[url] = {
                "final_url": final_url,
                "content_type": content_type,
                "length": len(text),
                "contains_any_variant": matched,
            }
            if matched:
                context = extract_context(text, endpoint)
                if context:
                    return final_url, context, diagnostics
            diagnostics.append(f"{url}: endpoint not found")
        except Exception as exc:  # noqa: BLE001
            diagnostics.append(f"{url}: {exc}")
    return None, None, diagnostics


def find_endpoint_context(endpoint: str, url_index: Dict[str, List[str]], debug_bucket: Dict[str, dict]) -> Tuple[Optional[str], Optional[str], str, List[str]]:
    endpoint = normalize_endpoint(endpoint)
    urls = url_index.get("urls", []) if isinstance(url_index, dict) else []
    if not urls:
        return None, None, "fetch_error", ["url index is empty"]

    candidates = [u for u in urls if score_url_for_endpoint(u, endpoint) > 0]
    broad_candidates = list(urls) if not candidates else candidates[:40] + [u for u in urls if u not in candidates][:20]
    broad_candidates = broad_candidates[:60]

    doc_url, context, diagnostics = try_pages_for_endpoint(endpoint, broad_candidates, debug_bucket)
    if context:
        return doc_url, context, "ok", diagnostics

    fetch_failures = [d for d in diagnostics if "Не удалось загрузить" in d or "HTTP Error" in d or "timed out" in d]
    if len(fetch_failures) >= max(3, len(broad_candidates) // 3):
        return None, None, "fetch_error", diagnostics
    return None, None, "not_found", diagnostics


def build_results(methods: List[str], previous_state: Dict[str, dict], url_index: Dict[str, List[str]], debug_store: Dict[str, dict]) -> Tuple[List[MethodResult], Dict[str, dict]]:
    results: List[MethodResult] = []
    new_state: Dict[str, dict] = {}

    for endpoint in methods:
        endpoint_debug: Dict[str, dict] = {}
        doc_url, context, status, diagnostics = find_endpoint_context(endpoint, url_index, endpoint_debug)
        debug_store[endpoint] = endpoint_debug

        if status != "ok" or context is None:
            severity = classify(status, False, [], None)
            res = MethodResult(
                endpoint=endpoint,
                doc_url=doc_url,
                found=False,
                changed=False,
                severity=severity,
                warnings=[],
                replacements=[],
                dates=[],
                summary=summarize(endpoint, status, False, [], [], [], None),
                diff_excerpt=None,
                error="; ".join(diagnostics[:12]) if diagnostics else None,
                status=status,
                diagnostics=diagnostics[:12],
            )
            results.append(res)
            new_state[endpoint] = {
                "endpoint": endpoint,
                "doc_url": doc_url,
                "found": False,
                "status": status,
                "error": res.error,
                "checked_at": now_iso(),
            }
            continue

        warnings, replacements, dates = detect_warnings(context)
        prev_context = previous_state.get(endpoint, {}).get("context", "")
        changed = prev_context != "" and prev_context != context
        diff_excerpt = make_diff(prev_context, context)
        severity = classify(status, changed, warnings, None)

        res = MethodResult(
            endpoint=endpoint,
            doc_url=doc_url,
            found=True,
            changed=changed,
            severity=severity,
            warnings=warnings,
            replacements=replacements,
            dates=dates,
            summary=summarize(endpoint, status, changed, warnings, replacements, dates, None),
            diff_excerpt=diff_excerpt,
            error=None,
            status=status,
            diagnostics=diagnostics[:12],
        )
        results.append(res)
        new_state[endpoint] = {
            "endpoint": endpoint,
            "doc_url": doc_url,
            "found": True,
            "status": status,
            "context": context,
            "hash": sha256_text(context),
            "warnings": warnings,
            "replacements": replacements,
            "dates": dates,
            "checked_at": now_iso(),
        }

    return results, new_state


def build_report(results: List[MethodResult], url_index: Dict[str, List[str]]) -> dict:
    counts = {
        "critical": sum(1 for r in results if r.severity == "critical"),
        "important": sum(1 for r in results if r.severity == "important"),
        "warning": sum(1 for r in results if r.severity == "warning"),
        "info": sum(1 for r in results if r.severity == "info"),
        "error": sum(1 for r in results if r.severity == "error"),
        "not_found": sum(1 for r in results if r.status == "not_found"),
        "fetch_error": sum(1 for r in results if r.status == "fetch_error"),
        "ok": sum(1 for r in results if r.status == "ok"),
    }
    has_actionable = any(r.severity in {"critical", "important", "error", "warning"} for r in results)
    return {
        "generated_at": now_iso(),
        "counts": counts,
        "has_actionable": has_actionable,
        "indexed_urls": len(url_index.get("urls", [])) if isinstance(url_index, dict) else 0,
        "results": [asdict(r) for r in results],
    }


def trim_line(text: str, limit: int = 300) -> str:
    text = (text or "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


def build_telegram_message(report: dict, only_alerts: bool = True) -> str:
    results = report["results"]
    if only_alerts:
        selected = [r for r in results if r["severity"] in {"critical", "important", "warning", "error"}]
    else:
        selected = results

    lines = [
        "Ozon API monitor",
        f"Проверка: {report['generated_at']}",
        (
            f"Итог: critical={report['counts']['critical']}, "
            f"important={report['counts']['important']}, "
            f"warning={report['counts']['warning']}, "
            f"error={report['counts']['error']}, "
            f"not_found={report['counts']['not_found']}, "
            f"indexed_urls={report['indexed_urls']}"
        ),
        "",
    ]

    if not selected:
        lines.append("Изменений по отслеживаемым методам нет.")
        return "\n".join(lines)[:TELEGRAM_MESSAGE_LIMIT]

    shown = 0
    total = min(len(selected), 20)
    for item in selected[:20]:
        block = [f"• {item['endpoint']} — {item['severity']} ({item['status']})", trim_line(item['summary'], 500)]
        if item.get("warnings"):
            block.append("warning: " + trim_line(item["warnings"][0], 250))
        if item.get("doc_url"):
            block.append(trim_line(item["doc_url"], 300))
        elif item.get("error"):
            block.append("diag: " + trim_line(item["error"], 250))
        block.append("")
        candidate = "\n".join(lines + ["\n".join(block)])
        if len(candidate) > TELEGRAM_MESSAGE_LIMIT:
            break
        lines.append("\n".join(block))
        shown += 1

    if shown < total:
        lines.append(f"... сообщение сокращено. Показано: {shown} из {total}")

    return "\n".join(lines)[:TELEGRAM_MESSAGE_LIMIT]


def send_telegram(message: str) -> None:
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not bot_token or not chat_id:
        raise MonitorError("Не заданы TELEGRAM_BOT_TOKEN и/или TELEGRAM_CHAT_ID")

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = urllib.parse.urlencode(
        {
            "chat_id": chat_id,
            "text": message,
            "disable_web_page_preview": "true",
        }
    ).encode("utf-8")
    req = urllib.request.Request(url, data=payload, headers={"User-Agent": USER_AGENT})

    try:
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            if resp.status != 200:
                raise MonitorError(f"Telegram вернул HTTP {resp.status}: {body}")
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise MonitorError(f"Telegram HTTP {exc.code}: {error_body}") from exc
    except urllib.error.URLError as exc:
        raise MonitorError(f"Ошибка соединения с Telegram: {exc}") from exc


def main() -> int:
    parser = argparse.ArgumentParser(description="Monitor Ozon Seller API docs for tracked methods")
    parser.add_argument("--methods-file", default="tracked_methods.json", help="Path to tracked methods JSON")
    parser.add_argument("--state-dir", default=STATE_DIRNAME, help="Directory for persisted state")
    parser.add_argument("--send-ok", action="store_true", help="Send Telegram even when there are no alerts")
    parser.add_argument("--fail-on-fetch-error", action="store_true", help="Return non-zero when docs could not be fetched")
    parser.add_argument("--fail-on-not-found", action="store_true", help="Return non-zero when a method is not found in docs")
    args = parser.parse_args()

    methods_file = Path(args.methods_file)
    state_dir = Path(args.state_dir)
    state_dir.mkdir(parents=True, exist_ok=True)

    state_path = state_dir / STATE_FILENAME
    report_path = state_dir / RUN_REPORT_FILENAME
    debug_path = state_dir / DEBUG_FILENAME

    methods = load_tracked_methods(methods_file)
    previous_state = read_json(state_path, {})
    url_index = build_url_index(state_dir)
    debug_store: Dict[str, dict] = {"url_index": url_index.get("diagnostics", [])}
    results, new_state = build_results(methods, previous_state, url_index, debug_store)
    report = build_report(results, url_index)

    write_json(report_path, report)
    write_json(state_path, new_state)
    write_json(debug_path, debug_store)

    should_send = args.send_ok or report["has_actionable"]
    if should_send:
        message = build_telegram_message(report, only_alerts=not args.send_ok)
        send_telegram(message)

    print(json.dumps(report, ensure_ascii=False, indent=2))

    if args.fail_on_fetch_error and report["counts"]["fetch_error"] > 0:
        return 2
    if args.fail_on_not_found and report["counts"]["not_found"] > 0:
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
