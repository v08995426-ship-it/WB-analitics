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
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

DOC_PAGE_CANDIDATES = [
    "https://docs.ozon.ru/api/seller/",
    "https://docs.ozon.ru/global/en/api/seller/",
]

USER_AGENT = "Mozilla/5.0 (compatible; OzonDocsMonitor/1.0; +https://github.com/)"
REQUEST_TIMEOUT = 40
RETRY_COUNT = 3
STATE_DIRNAME = ".ozon-docs-monitor"
STATE_FILENAME = "state.json"
RUN_REPORT_FILENAME = "last_report.json"

ALERT_PATTERNS = [
    re.compile(r"метод\s+устарева", re.IGNORECASE),
    re.compile(r"будет\s+отключ", re.IGNORECASE),
    re.compile(r"deprecated", re.IGNORECASE),
    re.compile(r"deprecat", re.IGNORECASE),
    re.compile(r"sunset", re.IGNORECASE),
    re.compile(r"use\s+/v\d+/[\w\-/]+", re.IGNORECASE),
    re.compile(r"используйте\s+/v\d+/[\w\-/]+", re.IGNORECASE),
]

DATE_PATTERNS = [
    re.compile(r"\b\d{4}-\d{2}-\d{2}\b"),
    re.compile(r"\b\d{1,2}\s+[а-яА-ЯёЁ]+\s+\d{4}\s+года\b"),
    re.compile(r"\b\d{1,2}\s+[A-Za-z]+\s+\d{4}\b"),
]


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


class MonitorError(Exception):
    pass


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def read_json(path: Path, default):
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def fetch_url(url: str) -> str:
    last_error = None
    for attempt in range(1, RETRY_COUNT + 1):
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                charset = resp.headers.get_content_charset() or "utf-8"
                return resp.read().decode(charset, errors="replace")
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as exc:
            last_error = exc
            if attempt < RETRY_COUNT:
                time.sleep(2 * attempt)
            continue
    raise MonitorError(f"Не удалось загрузить {url}: {last_error}")


def clean_html_to_text(raw_html: str) -> str:
    text = raw_html
    text = re.sub(r"(?is)<script.*?>.*?</script>", " ", text)
    text = re.sub(r"(?is)<style.*?>.*?</style>", " ", text)
    text = re.sub(r"(?is)<!--.*?-->", " ", text)
    text = re.sub(r"(?is)<br\s*/?>", "\n", text)
    text = re.sub(r"(?is)</p>", "\n", text)
    text = re.sub(r"(?is)</div>", "\n", text)
    text = re.sub(r"(?is)</li>", "\n", text)
    text = re.sub(r"(?is)</tr>", "\n", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    text = html.unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"\r", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    lines = [re.sub(r"\s+", " ", line).strip() for line in text.splitlines()]
    lines = [line for line in lines if line]
    return "\n".join(lines)


def normalize_endpoint(endpoint: str) -> str:
    return endpoint.strip()


def extract_context(text: str, endpoint: str, radius: int = 6000) -> Optional[str]:
    idx = text.find(endpoint)
    if idx == -1:
        return None
    start = max(0, idx - radius)
    end = min(len(text), idx + radius)
    snippet = text[start:end]
    # Cut to nearby paragraph-ish chunk around endpoint to keep state smaller.
    parts = snippet.split("\n")
    important = [line for line in parts if endpoint in line or any(p.search(line) for p in ALERT_PATTERNS) or "deprecated" in line.lower() or "устар" in line.lower() or "отключ" in line.lower() or re.search(r"/v\d+/", line)]
    if important:
        # add some surrounding lines around endpoint match
        context_lines = []
        for i, line in enumerate(parts):
            if endpoint in line or any(p.search(line) for p in ALERT_PATTERNS):
                lo = max(0, i - 5)
                hi = min(len(parts), i + 6)
                context_lines.extend(parts[lo:hi])
        deduped = []
        seen = set()
        for line in context_lines:
            if line not in seen:
                seen.add(line)
                deduped.append(line)
        return "\n".join(deduped)
    return snippet[: min(len(snippet), 5000)]


def find_endpoint_context(endpoint: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    endpoint = normalize_endpoint(endpoint)
    errors = []
    for url in DOC_PAGE_CANDIDATES:
        try:
            raw_html = fetch_url(url)
            text = clean_html_to_text(raw_html)
            context = extract_context(text, endpoint)
            if context:
                return url, context, None
            errors.append(f"{url}: endpoint not found")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{url}: {exc}")
    return None, None, "; ".join(errors)


def detect_warnings(context: str) -> Tuple[List[str], List[str], List[str]]:
    warnings = []
    replacements = []
    dates = []

    for line in context.splitlines():
        line_clean = line.strip()
        if not line_clean:
            continue
        if any(p.search(line_clean) for p in ALERT_PATTERNS):
            warnings.append(line_clean)
        for m in re.finditer(r"/v\d+/[\w\-/]+", line_clean):
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


def classify(changed: bool, warnings: List[str], replacements: List[str], error: Optional[str]) -> str:
    if error:
        return "error"
    if warnings:
        return "critical"
    if changed:
        return "important"
    return "info"


def summarize(endpoint: str, changed: bool, warnings: List[str], replacements: List[str], dates: List[str], error: Optional[str]) -> str:
    if error:
        return f"{endpoint}: ошибка проверки документации"
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
    if isinstance(payload, dict):
        methods = payload.get("methods", [])
    else:
        methods = payload
    methods = [normalize_endpoint(x) for x in methods if str(x).strip()]
    methods = sorted(set(methods))
    if not methods:
        raise MonitorError("Список отслеживаемых методов пуст")
    return methods


def build_results(methods: List[str], previous_state: Dict[str, dict]) -> Tuple[List[MethodResult], Dict[str, dict]]:
    results: List[MethodResult] = []
    new_state: Dict[str, dict] = {}

    for endpoint in methods:
        doc_url, context, error = find_endpoint_context(endpoint)
        if error and context is None:
            res = MethodResult(
                endpoint=endpoint,
                doc_url=doc_url,
                found=False,
                changed=False,
                severity="error",
                warnings=[],
                replacements=[],
                dates=[],
                summary=summarize(endpoint, False, [], [], [], error),
                diff_excerpt=None,
                error=error,
            )
            results.append(res)
            new_state[endpoint] = {
                "endpoint": endpoint,
                "doc_url": doc_url,
                "found": False,
                "error": error,
                "checked_at": now_iso(),
            }
            continue

        context = context or ""
        warnings, replacements, dates = detect_warnings(context)
        prev_context = previous_state.get(endpoint, {}).get("context", "")
        changed = prev_context != "" and prev_context != context
        diff_excerpt = make_diff(prev_context, context)
        severity = classify(changed, warnings, replacements, None)

        res = MethodResult(
            endpoint=endpoint,
            doc_url=doc_url,
            found=True,
            changed=changed,
            severity=severity,
            warnings=warnings,
            replacements=replacements,
            dates=dates,
            summary=summarize(endpoint, changed, warnings, replacements, dates, None),
            diff_excerpt=diff_excerpt,
            error=None,
        )
        results.append(res)
        new_state[endpoint] = {
            "endpoint": endpoint,
            "doc_url": doc_url,
            "found": True,
            "context": context,
            "hash": sha256_text(context),
            "warnings": warnings,
            "replacements": replacements,
            "dates": dates,
            "checked_at": now_iso(),
        }

    return results, new_state


def build_report(results: List[MethodResult]) -> dict:
    counts = {
        "critical": sum(1 for r in results if r.severity == "critical"),
        "important": sum(1 for r in results if r.severity == "important"),
        "info": sum(1 for r in results if r.severity == "info"),
        "error": sum(1 for r in results if r.severity == "error"),
    }
    has_actionable = any(r.severity in {"critical", "important", "error"} for r in results)
    return {
        "generated_at": now_iso(),
        "counts": counts,
        "has_actionable": has_actionable,
        "results": [r.__dict__ for r in results],
    }


def escape_markdown(text: str) -> str:
    for ch in r"_[]()~`>#+-=|{}.!":
        text = text.replace(ch, f"\\{ch}")
    return text


def build_telegram_message(report: dict, only_alerts: bool = True) -> str:
    results = report["results"]
    if only_alerts:
        selected = [r for r in results if r["severity"] in {"critical", "important", "error"}]
    else:
        selected = results

    lines = [
        "*Ozon API monitor*",
        f"Проверка: {escape_markdown(report['generated_at'])}",
        (
            "Итог: "
            f"critical={report['counts']['critical']}, "
            f"important={report['counts']['important']}, "
            f"error={report['counts']['error']}"
        ),
        "",
    ]

    if not selected:
        lines.append("Изменений по отслеживаемым методам нет.")
        return "\n".join(lines)

    for item in selected[:20]:
        lines.append(f"• `{item['endpoint']}` — {escape_markdown(item['severity'])}")
        lines.append(escape_markdown(item["summary"]))
        if item.get("warnings"):
            first_warning = item["warnings"][0]
            lines.append(f"warning: {escape_markdown(first_warning[:300])}")
        if item.get("doc_url"):
            lines.append(escape_markdown(item["doc_url"]))
        lines.append("")

    return "\n".join(lines).strip()


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
            "parse_mode": "MarkdownV2",
            "disable_web_page_preview": "true",
        }
    ).encode("utf-8")
    req = urllib.request.Request(url, data=payload, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
        body = resp.read().decode("utf-8", errors="replace")
        if resp.status != 200:
            raise MonitorError(f"Telegram вернул HTTP {resp.status}: {body}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Monitor Ozon Seller API docs for tracked methods")
    parser.add_argument("--methods-file", default="tracked_methods.json", help="Path to tracked methods JSON")
    parser.add_argument("--state-dir", default=STATE_DIRNAME, help="Directory for persisted state")
    parser.add_argument("--send-ok", action="store_true", help="Send Telegram even when there are no alerts")
    parser.add_argument("--fail-on-error", action="store_true", help="Return non-zero when some methods could not be checked")
    args = parser.parse_args()

    methods_file = Path(args.methods_file)
    state_dir = Path(args.state_dir)
    state_dir.mkdir(parents=True, exist_ok=True)
    state_path = state_dir / STATE_FILENAME
    report_path = state_dir / RUN_REPORT_FILENAME

    methods = load_tracked_methods(methods_file)
    previous_state = read_json(state_path, {})
    results, new_state = build_results(methods, previous_state)
    report = build_report(results)

    write_json(report_path, report)
    write_json(state_path, new_state)

    should_send = args.send_ok or report["has_actionable"]
    if should_send:
        message = build_telegram_message(report, only_alerts=not args.send_ok)
        send_telegram(message)

    print(json.dumps(report, ensure_ascii=False, indent=2))

    if args.fail_on_error and report["counts"]["error"] > 0:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
