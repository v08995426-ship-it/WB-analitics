
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import io
import json
import math
import os
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import boto3
import numpy as np
import pandas as pd
import requests
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError
from openpyxl import load_workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

STORE_NAME = "TOPFACE"
TARGET_SUBJECTS = {"кисти косметические", "блески", "помады", "косметические карандаши"}
GROWTH_SUBJECTS = {"блески", "помады", "косметические карандаши"}
WB_BIDS_URL = "https://advert-api.wildberries.ru/api/advert/v1/bids"
WB_BIDS_MIN_URL = "https://advert-api.wildberries.ru/api/advert/v1/bids/min"
WB_NMS_URL = "https://advert-api.wildberries.ru/adv/v0/auction/nms"

ADS_ANALYSIS_KEY = f"Отчёты/Реклама/{STORE_NAME}/Анализ рекламы.xlsx"
ECONOMICS_KEY = f"Отчёты/Финансовые показатели/{STORE_NAME}/Экономика.xlsx"
FUNNEL_KEY = f"Отчёты/Воронка продаж/{STORE_NAME}/Воронка продаж.xlsx"
ORDERS_WEEKLY_PREFIX = f"Отчёты/Заказы/{STORE_NAME}/Недельные/"
KEYWORDS_WEEKLY_PREFIX = f"Отчёты/Поисковые запросы/{STORE_NAME}/Недельные/"

SERVICE_ROOT = f"Служебные файлы/Ассистент WB/{STORE_NAME}/"
OUT_PREVIEW = SERVICE_ROOT + "Предпросмотр_последнего_запуска.xlsx"
OUT_SUMMARY = SERVICE_ROOT + "Сводка_последнего_запуска.json"
OUT_ARCHIVE = SERVICE_ROOT + "Архив_решений.xlsx"
OUT_BID_HISTORY = SERVICE_ROOT + "История_ставок.xlsx"
OUT_LIMITS = SERVICE_ROOT + "Лимиты_ставок_ежедневно.xlsx"
OUT_PRODUCT = SERVICE_ROOT + "Метрики_по_товарам.xlsx"
OUT_EFF = SERVICE_ROOT + "Эффективность_ставки_ежедневно.xlsx"
OUT_WEAK = SERVICE_ROOT + "Слабые_позиции_приоритет.xlsx"
OUT_EFFECTS = SERVICE_ROOT + "Эффект_изменений.xlsx"
OUT_SHADE_ACTIONS = SERVICE_ROOT + "Рекомендации_по_оттенкам.xlsx"
OUT_SHADE_PORTFOLIO = SERVICE_ROOT + "Состав_кампаний_по_оттенкам.xlsx"
OUT_SHADE_TESTS = SERVICE_ROOT + "Тесты_оттенков.xlsx"
OUT_BENCHMARK = SERVICE_ROOT + "Сравнение_с_сильными_РК.xlsx"

# Единый итоговый файл. Все отчёты пишем только сюда.
OUT_SINGLE_REPORT = OUT_PREVIEW

MIN_RATING_SHADE = 4.6
MATURE_START_OFFSET = 7
MATURE_END_OFFSET = 3
WINDOW_LEN = 5

API_CALL_LOGS: List[Dict[str, Any]] = []
MIN_BID_ROWS: List[Dict[str, Any]] = []
_LAST_API_CALL_AT: Dict[str, float] = {}
_API_MIN_INTERVAL_SEC = {
    WB_BIDS_MIN_URL: 3.1,   # 20 req/min, interval 3 sec
    WB_NMS_URL: 1.05,       # 1 req/sec
    WB_BIDS_URL: 0.25,      # 5 req/sec
}

def now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def json_dumps_safe(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, default=str)
    except Exception:
        return str(value)

def truncate_text(value: Any, limit: int = 4000) -> str:
    text_value = value if isinstance(value, str) else json_dumps_safe(value)
    return text_value[:limit]

def canonical_payment_type(value: Any) -> str:
    v = str(value or "").strip().lower()
    return "cpc" if v == "cpc" else "cpm"

def normalize_internal_placement(value: Any) -> str:
    v = str(value or "").strip().lower()
    mapping = {
        "combined": "combined",
        "search": "search",
        "recommendation": "recommendation",
        "recommendations": "recommendation",
    }
    return mapping.get(v, "search")

def placement_for_min_endpoint(value: Any) -> str:
    v = normalize_internal_placement(value)
    return "recommendation" if v == "recommendation" else v

def placement_for_bids_endpoint(value: Any) -> str:
    v = normalize_internal_placement(value)
    return "recommendations" if v == "recommendation" else v

def wait_for_rate_limit(url: str) -> None:
    delay = _API_MIN_INTERVAL_SEC.get(url, 0.0)
    if delay <= 0:
        return
    last = _LAST_API_CALL_AT.get(url, 0.0)
    now = time.time()
    sleep_for = delay - (now - last)
    if sleep_for > 0:
        time.sleep(sleep_for)

def extract_request_id(response_text: str) -> str:
    if not response_text:
        return ""
    try:
        data = json.loads(response_text)
        return str(data.get("requestId") or data.get("request_id") or "")
    except Exception:
        return ""

def append_api_log(
    *,
    method_name: str,
    http_method: str,
    url: str,
    request_body: Any,
    response_status: Any = "",
    response_text: Any = "",
    status: str = "",
    context: Optional[Dict[str, Any]] = None,
) -> None:
    row: Dict[str, Any] = {
        "timestamp": now_ts(),
        "Метод": method_name,
        "HTTP метод": http_method.upper(),
        "URL": url,
        "status": status,
        "http_status": response_status,
        "request_id": extract_request_id(str(response_text)),
        "request_body": truncate_text(request_body, 8000),
        "response": truncate_text(response_text, 8000),
    }
    if context:
        for k, v in context.items():
            row[k] = v
    API_CALL_LOGS.append(row)

def wb_api_request(
    http_method: str,
    url: str,
    api_key: str,
    body: Any,
    *,
    method_name: str,
    timeout: int = 120,
    dry_run: bool = False,
    context: Optional[Dict[str, Any]] = None,
) -> Optional[requests.Response]:
    if not api_key:
        append_api_log(
            method_name=method_name,
            http_method=http_method,
            url=url,
            request_body=body,
            response_status="",
            response_text="Нет WB_PROMO_KEY_TOPFACE, вызов не выполнен",
            status="skipped",
            context=context,
        )
        return None
    if dry_run:
        append_api_log(
            method_name=method_name,
            http_method=http_method,
            url=url,
            request_body=body,
            response_status="",
            response_text="dry-run",
            status="dry-run",
            context=context,
        )
        return None

    wait_for_rate_limit(url)
    headers = {"Authorization": api_key.strip(), "Content-Type": "application/json"}
    try:
        resp = requests.request(http_method.upper(), url, headers=headers, json=body, timeout=timeout)
        _LAST_API_CALL_AT[url] = time.time()
        append_api_log(
            method_name=method_name,
            http_method=http_method,
            url=url,
            request_body=body,
            response_status=resp.status_code,
            response_text=resp.text,
            status="ok" if resp.status_code == 200 else "failed",
            context=context,
        )
        return resp
    except Exception as e:
        _LAST_API_CALL_AT[url] = time.time()
        append_api_log(
            method_name=method_name,
            http_method=http_method,
            url=url,
            request_body=body,
            response_status="",
            response_text=str(e),
            status="failed",
            context=context,
        )
        return None


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)

def safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if pd.isna(v):
            return default
        if isinstance(v, str):
            v = v.replace("\xa0", " ").replace("%", "").replace(",", ".").strip()
            if not v:
                return default
        return float(v)
    except Exception:
        return default

def safe_int(v: Any, default: int = 0) -> int:
    try:
        if pd.isna(v):
            return default
        if isinstance(v, str):
            v = v.replace("\xa0", " ").replace(",", ".").strip()
        return int(float(v))
    except Exception:
        return default

def canonical_subject(v: Any) -> str:
    return str(v or "").strip().lower()

def product_root_from_supplier_article(v: Any) -> str:
    s = str(v or "").strip()
    if not s or s.lower() in {"nan", "none"}:
        return ""
    root = s.split("/")[0].strip()
    root = re.sub(r"[^0-9A-Za-zА-Яа-я_-]+", "", root)
    root = re.sub(r"[_-]+$", "", root)
    return root.upper()

def pct(a: float, b: float) -> float:
    return (safe_float(a) / safe_float(b) * 100.0) if safe_float(b) else 0.0

def growth_pct(cur: float, base: float) -> float:
    cur = safe_float(cur)
    base = safe_float(base)
    if base <= 0:
        return 100.0 if cur > 0 else 0.0
    return (cur / base - 1.0) * 100.0

def clamp(x: float, low: float, high: float) -> float:
    return max(low, min(high, x))

def daterange(start: date, end: date) -> Iterable[date]:
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)

def sanitize_sheet_name(name: str, used: Optional[set] = None) -> str:
    name = re.sub(r'[:\\/?*\[\]]', '_', str(name))
    name = re.sub(r'\s+', '_', name)
    name = re.sub(r'_+', '_', name).strip('_')
    name = name[:31] if len(name) > 31 else name
    if used is None:
        return name or "Лист"
    base = name or "Лист"
    candidate = base
    i = 2
    while candidate in used:
        suffix = f"_{i}"
        candidate = (base[:31-len(suffix)] + suffix) if len(base)+len(suffix) > 31 else base + suffix
        i += 1
    used.add(candidate)
    return candidate

def style_workbook(path: Path) -> None:
    try:
        wb = load_workbook(path)
        header_fill = PatternFill("solid", fgColor="1F4E78")
        header_font = Font(color="FFFFFF", bold=True)
        for ws in wb.worksheets:
            ws.freeze_panes = "A2"
            max_widths: Dict[int, int] = {}
            for row_idx, row in enumerate(ws.iter_rows(), start=1):
                for col_idx, cell in enumerate(row, start=1):
                    cell.alignment = Alignment(vertical="top", wrap_text=True)
                    if row_idx == 1:
                        cell.fill = header_fill
                        cell.font = header_font
                    val = "" if cell.value is None else str(cell.value)
                    width = min(max(len(val) + 2, 10), 40)
                    max_widths[col_idx] = max(max_widths.get(col_idx, 0), width)
            for col_idx, width in max_widths.items():
                ws.column_dimensions[get_column_letter(col_idx)].width = width
            ws.row_dimensions[1].height = 34
        wb.save(path)
    except Exception:
        pass

class BaseProvider:
    def read_excel(self, key: str, sheet_name: Any = 0) -> pd.DataFrame:
        raise NotImplementedError
    def read_excel_all_sheets(self, key: str) -> Dict[str, pd.DataFrame]:
        raise NotImplementedError
    def write_excel(self, key: str, sheets: Dict[str, pd.DataFrame]) -> None:
        raise NotImplementedError
    def read_text(self, key: str) -> str:
        raise NotImplementedError
    def write_text(self, key: str, text: str) -> None:
        raise NotImplementedError
    def file_exists(self, key: str) -> bool:
        raise NotImplementedError
    def list_keys(self, prefix: str) -> List[str]:
        raise NotImplementedError

class S3Provider(BaseProvider):
    def __init__(self, access_key: str, secret_key: str, bucket_name: str):
        self.bucket = bucket_name
        self.s3 = boto3.client(
            "s3",
            endpoint_url="https://storage.yandexcloud.net",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="ru-central1",
            config=BotoConfig(signature_version="s3v4", read_timeout=300, connect_timeout=60, retries={"max_attempts": 5}),
        )
    def read_bytes(self, key: str) -> bytes:
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        return obj["Body"].read()
    def read_excel(self, key: str, sheet_name: Any = 0) -> pd.DataFrame:
        return pd.read_excel(io.BytesIO(self.read_bytes(key)), sheet_name=sheet_name)
    def read_excel_all_sheets(self, key: str) -> Dict[str, pd.DataFrame]:
        data = self.read_bytes(key)
        xls = pd.ExcelFile(io.BytesIO(data))
        return {sh: pd.read_excel(io.BytesIO(data), sheet_name=sh) for sh in xls.sheet_names}
    def write_excel(self, key: str, sheets: Dict[str, pd.DataFrame]) -> None:
        tmp = Path("/tmp") / f"{int(time.time()*1000)}.xlsx"
        with pd.ExcelWriter(tmp, engine="openpyxl") as writer:
            for sh, df in sheets.items():
                (df if df is not None else pd.DataFrame()).to_excel(writer, sheet_name=sanitize_sheet_name(sh), index=False)
        style_workbook(tmp)
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=tmp.read_bytes())
        tmp.unlink(missing_ok=True)
    def read_text(self, key: str) -> str:
        return self.read_bytes(key).decode("utf-8")
    def write_text(self, key: str, text: str) -> None:
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=text.encode("utf-8"))
    def file_exists(self, key: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False
    def list_keys(self, prefix: str) -> List[str]:
        out: List[str] = []
        token = None
        while True:
            kwargs = {"Bucket": self.bucket, "Prefix": prefix, "MaxKeys": 1000}
            if token:
                kwargs["ContinuationToken"] = token
            resp = self.s3.list_objects_v2(**kwargs)
            out.extend([x["Key"] for x in resp.get("Contents", [])])
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        return out

class LocalProvider(BaseProvider):
    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)
    def _search(self, patterns: List[str]) -> List[Path]:
        out = []
        for child in self.base_dir.iterdir():
            if child.is_file():
                for p in patterns:
                    if re.search(p, child.name, flags=re.I):
                        out.append(child)
                        break
        return sorted(out)
    def _resolve(self, key: str) -> Path:
        p = Path(key)
        if p.exists():
            return p
        mappings = [
            (ADS_ANALYSIS_KEY, [r"^Анализ рекламы.*\.xlsx$"]),
            (ECONOMICS_KEY, [r"^Экономика.*\.xlsx$"]),
            (FUNNEL_KEY, [r"^Воронка продаж.*\.xlsx$"]),
            (OUT_BID_HISTORY, [r"^История_ставок.*\.xlsx$", r"^bid_history.*\.xlsx$"]),
            (OUT_PREVIEW, [r"^Предпросмотр_последнего_запуска.*\.xlsx$", r"^preview_last_run.*\.xlsx$"]),
            (OUT_SUMMARY, [r"^Сводка_последнего_запуска.*\.json$", r"^last_run_summary.*\.json$"]),
            (OUT_ARCHIVE, [r"^Архив_решений.*\.xlsx$", r"^decision_archive.*\.xlsx$"]),
        ]
        for logical, pats in mappings:
            if key == logical:
                found = self._search(pats)
                if found:
                    return found[0]
        return self.base_dir / Path(key).name
    def read_excel(self, key: str, sheet_name: Any = 0) -> pd.DataFrame:
        return pd.read_excel(self._resolve(key), sheet_name=sheet_name)
    def read_excel_all_sheets(self, key: str) -> Dict[str, pd.DataFrame]:
        path = self._resolve(key)
        xls = pd.ExcelFile(path)
        return {sh: pd.read_excel(path, sheet_name=sh) for sh in xls.sheet_names}
    def write_excel(self, key: str, sheets: Dict[str, pd.DataFrame]) -> None:
        path = self._resolve(key)
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            for sh, df in sheets.items():
                (df if df is not None else pd.DataFrame()).to_excel(writer, sheet_name=sanitize_sheet_name(sh), index=False)
        style_workbook(path)
    def read_text(self, key: str) -> str:
        return self._resolve(key).read_text(encoding="utf-8")
    def write_text(self, key: str, text: str) -> None:
        self._resolve(key).write_text(text, encoding="utf-8")
    def file_exists(self, key: str) -> bool:
        return self._resolve(key).exists()
    def list_keys(self, prefix: str) -> List[str]:
        if prefix == ORDERS_WEEKLY_PREFIX:
            return [str(p) for p in self._search([r"^Заказы_\d{4}-W\d{2}.*\.xlsx$"])]
        if prefix == KEYWORDS_WEEKLY_PREFIX:
            return [str(p) for p in self._search([r"^Неделя .*\.xlsx$", r"^W\d+.*\.xlsx$"])]
        return []

@dataclass
class Config:
    comfort_drr_min: float = 0.10
    comfort_drr_max: float = 0.12
    max_drr: float = 0.15
    max_up_step: float = 0.08
    test_up_step: float = 0.05
    down_step: float = 0.08
    recovery_down_step: float = 0.12
    critical_drr: float = 0.20
    order_maturity_lag_days: int = MATURE_END_OFFSET
    daily_lookback_days: int = 35
    gp_growth_test_days: int = 7
    gp_recovery_days: int = 14

def compute_analysis_window(as_of_date: date) -> Dict[str, date]:
    cur_end = as_of_date - timedelta(days=MATURE_END_OFFSET)
    cur_start = cur_end - timedelta(days=WINDOW_LEN-1)
    base_end = cur_start - timedelta(days=1)
    base_start = base_end - timedelta(days=WINDOW_LEN-1)
    return {"cur_start": cur_start, "cur_end": cur_end, "base_start": base_start, "base_end": base_end}

def parse_date_col(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.date

def choose_provider(local_data_dir: str = "") -> BaseProvider:
    if local_data_dir:
        return LocalProvider(local_data_dir)
    access = os.getenv("YC_ACCESS_KEY_ID", "")
    secret = os.getenv("YC_SECRET_ACCESS_KEY", "")
    bucket = os.getenv("YC_BUCKET_NAME", "")
    if not (access and secret and bucket):
        raise RuntimeError("Не заданы YC_ACCESS_KEY_ID / YC_SECRET_ACCESS_KEY / YC_BUCKET_NAME")
    return S3Provider(access, secret, bucket)

def load_ads(provider: BaseProvider) -> Tuple[pd.DataFrame, pd.DataFrame]:
    sheets = provider.read_excel_all_sheets(ADS_ANALYSIS_KEY)
    daily = sheets.get("Статистика_Ежедневно", pd.DataFrame()).copy()
    campaigns = sheets.get("Список_кампаний", pd.DataFrame()).copy()
    if daily.empty:
        return daily, campaigns
    daily = daily.rename(columns={
        "ID кампании": "id_campaign",
        "Артикул WB": "nmId",
        "Название предмета": "subject",
        "Дата": "date",
    })
    daily["date"] = parse_date_col(daily["date"])
    for c in ["Показы","Клики","Заказы","Расход","Сумма заказов","CTR","CR","ДРР"]:
        if c not in daily.columns:
            daily[c] = 0
    daily["Показы"] = daily["Показы"].map(safe_float)
    daily["Клики"] = daily["Клики"].map(safe_float)
    daily["Заказы"] = daily["Заказы"].map(safe_float)
    daily["Расход"] = daily["Расход"].map(safe_float)
    daily["Сумма заказов"] = daily["Сумма заказов"].map(safe_float)
    daily["subject_norm"] = daily["subject"].map(canonical_subject)
    daily = daily[daily["subject_norm"].isin(TARGET_SUBJECTS)].copy()

    if not campaigns.empty:
        campaigns = campaigns.rename(columns={"ID кампании":"id_campaign","Артикул WB":"nmId","Название предмета":"subject"})
        campaigns["subject_norm"] = campaigns["subject"].map(canonical_subject)
        campaigns = campaigns[campaigns["subject_norm"].isin(TARGET_SUBJECTS)].copy()
        campaigns["payment_type"] = campaigns["Тип оплаты"].astype(str).str.lower().str.strip()
        campaigns["bid_search_rub"] = campaigns.get("Ставка в поиске (руб)", 0).map(safe_float)
        campaigns["bid_reco_rub"] = campaigns.get("Ставка в рекомендациях (руб)", 0).map(safe_float)
        def _placement(r):
            s = safe_float(r["bid_search_rub"])
            rr = safe_float(r["bid_reco_rub"])
            if s > 0 and rr > 0:
                return "combined"
            if s > 0:
                return "search"
            if rr > 0:
                return "recommendation"
            return "search"
        campaigns["placement"] = campaigns.apply(_placement, axis=1)
        campaigns["current_bid_rub"] = campaigns.apply(lambda r: r["bid_search_rub"] if r["placement"] in {"search","combined"} else r["bid_reco_rub"], axis=1)
        campaigns["campaign_status"] = campaigns.get("Статус", "").astype(str)
    return daily, campaigns

def load_economics(provider: BaseProvider) -> pd.DataFrame:
    df = provider.read_excel(ECONOMICS_KEY, sheet_name="Юнит экономика").copy()
    df = df.rename(columns={"Артикул WB":"nmId","Артикул продавца":"supplier_article","Предмет":"subject"})
    df["subject_norm"] = df["subject"].map(canonical_subject)
    df = df[df["subject_norm"].isin(TARGET_SUBJECTS)].copy()
    df["product_root"] = df["supplier_article"].map(product_root_from_supplier_article)
    df["buyout_rate"] = df.get("Процент выкупа", 0).map(lambda x: safe_float(x) / 100.0 if safe_float(x) > 1 else safe_float(x))
    df["gp_unit"] = df.get("Валовая прибыль, руб/ед", 0).map(safe_float)
    df["np_unit"] = df.get("Чистая прибыль, руб/ед", 0).map(safe_float)
    df["gp_realized"] = df["gp_unit"] * df["buyout_rate"].clip(lower=0, upper=1)
    return df

def load_orders(provider: BaseProvider) -> pd.DataFrame:
    keys = provider.list_keys(ORDERS_WEEKLY_PREFIX)
    frames = []
    for key in keys:
        try:
            df = provider.read_excel(key).copy()
            if df.empty:
                continue
            df = df.rename(columns={"nmID":"nmId"})
            df["date"] = parse_date_col(df["date"])
            df["supplier_article"] = df.get("supplierArticle", "")
            df["subject"] = df.get("subject", "")
            df["subject_norm"] = df["subject"].map(canonical_subject)
            df = df[df["subject_norm"].isin(TARGET_SUBJECTS)].copy()
            df["product_root"] = df["supplier_article"].map(product_root_from_supplier_article)
            df["finishedPrice"] = df.get("finishedPrice", 0).map(safe_float)
            df["isCancel"] = df.get("isCancel", False).fillna(False).astype(bool)
            frames.append(df)
        except Exception:
            continue
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def load_funnel(provider: BaseProvider) -> pd.DataFrame:
    try:
        df = provider.read_excel(FUNNEL_KEY).copy()
    except Exception:
        return pd.DataFrame()
    df = df.rename(columns={"nmID":"nmId","dt":"date"})
    df["date"] = parse_date_col(df["date"])
    for c in ["openCardCount","addToCartCount","ordersCount","buyoutsCount","addToCartConversion","cartToOrderConversion","buyoutPercent"]:
        if c in df.columns:
            df[c] = df[c].map(safe_float)
    return df

def load_keywords(provider: BaseProvider) -> pd.DataFrame:
    keys = provider.list_keys(KEYWORDS_WEEKLY_PREFIX)
    frames = []
    for key in keys:
        try:
            xls = provider.read_excel_all_sheets(key)
            sheet = xls.get("Позиции по Ключам", next(iter(xls.values())))
            df = sheet.copy()
            if df.empty:
                continue
            df = df.rename(columns={
                "Дата":"date",
                "Артикул WB":"nmId",
                "Артикул продавца":"supplier_article",
                "Предмет":"subject",
                "Рейтинг отзывов":"rating_reviews",
                "Рейтинг карточки":"rating_card",
                "Частота запросов":"query_freq",
                "Частота за неделю":"demand_week",
                "Медианная позиция":"median_position",
                "Переходы в карточку":"clicks_to_card",
                "Заказы":"keyword_orders",
                "Конверсия в заказ %":"keyword_conversion",
                "Видимость %":"visibility_pct",
            })
            df["date"] = parse_date_col(df["date"])
            df["subject_norm"] = df["subject"].map(canonical_subject)
            df = df[df["subject_norm"].isin(TARGET_SUBJECTS)].copy()
            df["product_root"] = df["supplier_article"].map(product_root_from_supplier_article)
            for c in ["query_freq","demand_week","median_position","clicks_to_card","keyword_orders","keyword_conversion","visibility_pct","rating_reviews","rating_card"]:
                if c in df.columns:
                    df[c] = df[c].map(safe_float)
            frames.append(df)
        except Exception:
            continue
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def load_bid_history(provider: BaseProvider) -> pd.DataFrame:
    if not provider.file_exists(OUT_BID_HISTORY):
        return pd.DataFrame()
    try:
        df = provider.read_excel(OUT_BID_HISTORY).copy()
    except Exception:
        return pd.DataFrame()
    df = df.rename(columns={"Дата запуска":"run_ts","ID кампании":"id_campaign","Артикул WB":"nmId","Тип кампании":"campaign_type"})
    if df.empty:
        return df

    df["run_ts"] = pd.to_datetime(df["run_ts"], errors="coerce")
    df["date"] = df["run_ts"].dt.normalize().astype("datetime64[ns]")

    search_col = pd.to_numeric(df.get("Ставка поиск, коп", 0), errors="coerce") if "Ставка поиск, коп" in df.columns else pd.Series(0, index=df.index, dtype=float)
    reco_col = pd.to_numeric(df.get("Ставка рекомендации, коп", 0), errors="coerce") if "Ставка рекомендации, коп" in df.columns else pd.Series(0, index=df.index, dtype=float)
    bid_kop = search_col.where(search_col.fillna(0) > 0, reco_col)
    df["bid_rub"] = (bid_kop.fillna(0) / 100.0).astype(float)

    df["id_campaign"] = pd.to_numeric(df.get("id_campaign"), errors="coerce")
    df["nmId"] = pd.to_numeric(df.get("nmId"), errors="coerce")
    df = df.dropna(subset=["run_ts", "date", "id_campaign", "nmId"]).copy()
    df["id_campaign"] = df["id_campaign"].astype("int64")
    df["nmId"] = df["nmId"].astype("int64")
    return df

def build_master(econ: pd.DataFrame, orders: pd.DataFrame, keywords: pd.DataFrame, campaigns: pd.DataFrame) -> pd.DataFrame:
    frames = []
    if not econ.empty:
        frames.append(econ[["nmId","supplier_article","product_root","subject","subject_norm","buyout_rate","gp_realized"]].copy())
    if not orders.empty:
        t = orders[["nmId","supplier_article","product_root","subject","subject_norm"]].copy()
        frames.append(t)
    if not keywords.empty:
        t = keywords[["nmId","supplier_article","product_root","subject","subject_norm","rating_reviews","rating_card"]].copy()
        frames.append(t)
    if not campaigns.empty:
        nm_map = campaigns[["id_campaign","nmId","subject","subject_norm"]].copy()
        frames.append(nm_map.rename(columns={"id_campaign":"_drop"}).drop(columns=["_drop"]))
    if not frames:
        return pd.DataFrame(columns=["nmId","supplier_article","product_root","subject","subject_norm","buyout_rate","gp_realized","rating_reviews","rating_card"])
    master = pd.concat(frames, ignore_index=True, sort=False)
    def first_non_empty(s):
        for v in s:
            if pd.notna(v) and str(v) != "":
                return v
        return None
    agg = master.groupby("nmId", as_index=False).agg({
        "supplier_article": first_non_empty,
        "product_root": first_non_empty,
        "subject": first_non_empty,
        "subject_norm": first_non_empty,
        "buyout_rate": "max",
        "gp_realized": "max",
        "rating_reviews": "max",
        "rating_card": "max",
    })
    agg["product_root"] = agg["product_root"].fillna(agg["supplier_article"].map(product_root_from_supplier_article))
    return agg

def aggregate_orders(orders: pd.DataFrame, start: date, end: date, control_field: str) -> pd.DataFrame:
    if orders.empty:
        return pd.DataFrame(columns=[control_field, "total_orders", "total_revenue", "total_orders_raw"])
    df = orders[(orders["date"] >= start) & (orders["date"] <= end) & (~orders["isCancel"])].copy()
    if df.empty:
        return pd.DataFrame(columns=[control_field, "total_orders", "total_revenue", "total_orders_raw"])
    out = df.groupby(control_field, as_index=False).agg(
        total_orders=("nmId", "count"),
        total_revenue=("finishedPrice", "sum"),
    )
    return out

def aggregate_ads_control(ads_daily: pd.DataFrame, start: date, end: date, mapping: pd.DataFrame, control_field: str) -> pd.DataFrame:
    if ads_daily.empty:
        return pd.DataFrame(columns=[control_field, "ad_spend", "ad_clicks", "ad_orders", "ad_impressions", "ad_revenue"])
    df = ads_daily[(ads_daily["date"] >= start) & (ads_daily["date"] <= end)].copy()
    if df.empty:
        return pd.DataFrame(columns=[control_field, "ad_spend", "ad_clicks", "ad_orders", "ad_impressions", "ad_revenue"])
    df = df.merge(mapping[["nmId", control_field]].drop_duplicates(), on="nmId", how="left")
    df = df[df[control_field].notna()].copy()
    if df.empty:
        return pd.DataFrame(columns=[control_field, "ad_spend", "ad_clicks", "ad_orders", "ad_impressions", "ad_revenue"])
    return df.groupby(control_field, as_index=False).agg(
        ad_spend=("Расход", "sum"),
        ad_clicks=("Клики", "sum"),
        ad_orders=("Заказы", "sum"),
        ad_impressions=("Показы", "sum"),
        ad_revenue=("Сумма заказов", "sum"),
    )

def aggregate_keyword_item(keywords: pd.DataFrame, start: date, end: date) -> pd.DataFrame:
    if keywords.empty:
        return pd.DataFrame(columns=["nmId","supplier_article","demand_week","median_position","visibility_pct","rating_reviews","rating_card"])
    df = keywords[(keywords["date"] >= start) & (keywords["date"] <= end)].copy()
    if df.empty:
        return pd.DataFrame(columns=["nmId","supplier_article","demand_week","median_position","visibility_pct","rating_reviews","rating_card"])
    return df.groupby(["nmId","supplier_article"], as_index=False).agg(
        demand_week=("demand_week", "sum"),
        median_position=("median_position", "median"),
        visibility_pct=("visibility_pct", "mean"),
        rating_reviews=("rating_reviews", "max"),
        rating_card=("rating_card", "max"),
        keyword_orders=("keyword_orders", "sum"),
        keyword_clicks=("clicks_to_card", "sum"),
    )

def aggregate_keyword_daily(keywords: pd.DataFrame) -> pd.DataFrame:
    if keywords.empty:
        return pd.DataFrame(columns=["date","nmId","supplier_article","demand","median_position","visibility_pct"])
    return keywords.groupby(["date","nmId","supplier_article"], as_index=False).agg(
        demand=("demand_week", "sum"),
        median_position=("median_position", "median"),
        visibility_pct=("visibility_pct", "mean"),
    )


def build_daily_profit_metrics(
    orders: pd.DataFrame,
    ads_daily: pd.DataFrame,
    master: pd.DataFrame,
    econ_latest: pd.DataFrame,
    as_of_date: date,
    cfg: Config,
) -> pd.DataFrame:
    start_date = as_of_date - timedelta(days=max(cfg.daily_lookback_days - 1, 0))
    end_date = as_of_date
    mature_cutoff = as_of_date - timedelta(days=cfg.order_maturity_lag_days)

    article_map = master[["nmId", "supplier_article", "product_root", "subject", "subject_norm"]].dropna(subset=["nmId"]).drop_duplicates("nmId").copy() if not master.empty else pd.DataFrame(columns=["nmId", "supplier_article", "product_root", "subject", "subject_norm"])
    econ_article = econ_latest[["nmId", "supplier_article", "product_root", "subject", "subject_norm", "buyout_rate", "gp_realized"]].copy() if not econ_latest.empty else pd.DataFrame(columns=["nmId", "supplier_article", "product_root", "subject", "subject_norm", "buyout_rate", "gp_realized"])
    if not econ_article.empty:
        econ_article = econ_article.sort_values(["supplier_article", "nmId"]).drop_duplicates("supplier_article", keep="last")

    empty_cols = [
        "Дата", "Артикул продавца", "Артикул WB", "Предмет", "Показы РК", "Клики РК", "Заказы товара, шт", "Выручка товара, ₽",
        "Расходы РК, ₽", "Выручка РК, ₽", "ДРР, %", "ВП до рекламы, ₽", "Валовая прибыль, ₽", "gp_realized, ₽/ед",
        "Зрелые данные", "Комментарий по зрелости", "Рост ВП 7д, %", "Рост заказов 7д, %", "Рост показов 7д, %",
        "Дней падения ВП подряд", "Дней просадки ВП при росте заказов"
    ]

    orders_daily = pd.DataFrame(columns=["date", "supplier_article", "orders_cnt", "revenue_total", "nmId", "subject", "subject_norm", "product_root"])
    if not orders.empty:
        od = orders[(orders["date"] >= start_date) & (orders["date"] <= end_date) & (~orders["isCancel"])].copy()
        if not od.empty:
            od["supplier_article"] = od["supplier_article"].astype(str)
            orders_daily = od.groupby(["date", "supplier_article"], as_index=False).agg(
                orders_cnt=("nmId", "count"),
                revenue_total=("finishedPrice", "sum"),
                nmId=("nmId", "max"),
                subject=("subject", "first"),
                subject_norm=("subject_norm", "first"),
                product_root=("product_root", "first"),
            )

    ads_by_article = pd.DataFrame(columns=["date", "supplier_article", "ad_spend", "ad_revenue", "ad_impressions", "ad_clicks", "nmId", "subject", "subject_norm", "product_root"])
    if not ads_daily.empty:
        ad = ads_daily[(ads_daily["date"] >= start_date) & (ads_daily["date"] <= end_date)].copy()
        if not ad.empty:
            ad = ad.merge(article_map, on="nmId", how="left", suffixes=("", "_m"))
            for col in ["supplier_article", "product_root", "subject", "subject_norm"]:
                map_col = f"{col}_m"
                if map_col in ad.columns:
                    if col in ad.columns:
                        ad[col] = ad[col].where(ad[col].astype(str).str.strip() != "", ad[map_col])
                    else:
                        ad[col] = ad[map_col]
            ad["supplier_article"] = ad.get("supplier_article", "").astype(str)
            ad = ad[ad["supplier_article"].str.strip() != ""].copy()
            ads_by_article = ad.groupby(["date", "supplier_article"], as_index=False).agg(
                ad_spend=("Расход", "sum"),
                ad_revenue=("Сумма заказов", "sum"),
                ad_impressions=("Показы", "sum"),
                ad_clicks=("Клики", "sum"),
                nmId=("nmId", "max"),
                subject=("subject", "first"),
                subject_norm=("subject_norm", "first"),
                product_root=("product_root", "first"),
            )

    frames = []
    if not orders_daily.empty:
        frames.append(orders_daily[["date", "supplier_article", "nmId", "subject", "subject_norm", "product_root"]].copy())
    if not ads_by_article.empty:
        frames.append(ads_by_article[["date", "supplier_article", "nmId", "subject", "subject_norm", "product_root"]].copy())
    if not econ_article.empty:
        date_range = list(daterange(start_date, end_date))
        base = econ_article[["nmId", "supplier_article", "subject", "subject_norm", "product_root"]].copy()
        base["_key"] = 1
        dates_df = pd.DataFrame({"date": date_range, "_key": 1})
        frames.append(base.merge(dates_df, on="_key", how="inner").drop(columns=["_key"]))

    if not frames:
        return pd.DataFrame(columns=empty_cols)

    universe = pd.concat(frames, ignore_index=True, sort=False).drop_duplicates(["date", "supplier_article"]).copy()
    df = universe.merge(orders_daily[["date", "supplier_article", "orders_cnt", "revenue_total"]], on=["date", "supplier_article"], how="left")
    df = df.merge(ads_by_article[["date", "supplier_article", "ad_spend", "ad_revenue", "ad_impressions", "ad_clicks"]], on=["date", "supplier_article"], how="left")
    df = df.merge(econ_article[["supplier_article", "nmId", "buyout_rate", "gp_realized"]], on="supplier_article", how="left", suffixes=("", "_econ"))
    if "nmId_econ" in df.columns:
        df["nmId"] = df["nmId"].fillna(df["nmId_econ"])
        df = df.drop(columns=["nmId_econ"])
    for c in ["orders_cnt", "revenue_total", "ad_spend", "ad_revenue", "ad_impressions", "ad_clicks", "buyout_rate", "gp_realized"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    df["nmId"] = pd.to_numeric(df.get("nmId"), errors="coerce")
    df["Зрелые данные"] = df["date"].apply(lambda x: "Да" if pd.notna(x) and x <= mature_cutoff else "Нет")
    df["Комментарий по зрелости"] = df["date"].apply(lambda x: "Зрелые заказы" if pd.notna(x) and x <= mature_cutoff else f"Ожидаем дозагрузку заказов ({cfg.order_maturity_lag_days} дн.)")
    df["ВП до рекламы, ₽"] = (df["orders_cnt"] * df["gp_realized"]).round(2)
    df["Валовая прибыль, ₽"] = (df["ВП до рекламы, ₽"] - df["ad_spend"]).round(2)
    df["ДРР, %"] = np.where(df["revenue_total"] > 0, (df["ad_spend"] / df["revenue_total"]) * 100.0, 0.0)

    out_frames = []
    for _, g in df.groupby("supplier_article", dropna=False):
        g = g.sort_values("date").copy()
        mature_mask = g["Зрелые данные"].eq("Да")
        mature = g.loc[mature_mask].copy()
        if not mature.empty:
            mature["gp7"] = mature["Валовая прибыль, ₽"].rolling(7, min_periods=3).sum()
            mature["gp7_prev"] = mature["gp7"].shift(7)
            mature["ord7"] = mature["orders_cnt"].rolling(7, min_periods=3).sum()
            mature["ord7_prev"] = mature["ord7"].shift(7)
            mature["imp7"] = mature["ad_impressions"].rolling(7, min_periods=3).sum()
            mature["imp7_prev"] = mature["imp7"].shift(7)
            mature["Рост ВП 7д, %"] = np.where(mature["gp7_prev"].abs() > 0, (mature["gp7"] / mature["gp7_prev"] - 1.0) * 100.0, np.nan)
            mature["Рост заказов 7д, %"] = np.where(mature["ord7_prev"] > 0, (mature["ord7"] / mature["ord7_prev"] - 1.0) * 100.0, np.nan)
            mature["Рост показов 7д, %"] = np.where(mature["imp7_prev"] > 0, (mature["imp7"] / mature["imp7_prev"] - 1.0) * 100.0, np.nan)
            gp_down_streak = []
            tradeoff_streak = []
            down = 0
            tradeoff = 0
            prev_gp = None
            for _, row in mature.iterrows():
                gp_val = safe_float(row.get("Валовая прибыль, ₽"))
                if prev_gp is not None and gp_val < prev_gp:
                    down += 1
                else:
                    down = 0
                prev_gp = gp_val
                gp_g = row.get("Рост ВП 7д, %")
                ord_g = row.get("Рост заказов 7д, %")
                imp_g = row.get("Рост показов 7д, %")
                cond = pd.notna(gp_g) and pd.notna(ord_g) and pd.notna(imp_g) and safe_float(ord_g) > 0 and safe_float(imp_g) >= 0 and safe_float(gp_g) < 0
                if cond:
                    tradeoff += 1
                else:
                    tradeoff = 0
                gp_down_streak.append(down)
                tradeoff_streak.append(tradeoff)
            mature["Дней падения ВП подряд"] = gp_down_streak
            mature["Дней просадки ВП при росте заказов"] = tradeoff_streak
            cols_to_copy = ["Рост ВП 7д, %", "Рост заказов 7д, %", "Рост показов 7д, %", "Дней падения ВП подряд", "Дней просадки ВП при росте заказов"]
            g.loc[mature.index, cols_to_copy] = mature[cols_to_copy]
        out_frames.append(g)

    out = pd.concat(out_frames, ignore_index=True) if out_frames else pd.DataFrame(columns=empty_cols)
    out = out.rename(columns={
        "date": "Дата",
        "supplier_article": "Артикул продавца",
        "nmId": "Артикул WB",
        "subject": "Предмет",
        "orders_cnt": "Заказы товара, шт",
        "revenue_total": "Выручка товара, ₽",
        "ad_spend": "Расходы РК, ₽",
        "ad_revenue": "Выручка РК, ₽",
        "ad_impressions": "Показы РК",
        "ad_clicks": "Клики РК",
        "gp_realized": "gp_realized, ₽/ед",
    })
    for c in ["Артикул WB", "Показы РК", "Клики РК", "Заказы товара, шт", "Выручка товара, ₽", "Расходы РК, ₽", "Выручка РК, ₽", "ДРР, %", "ВП до рекламы, ₽", "Валовая прибыль, ₽", "gp_realized, ₽/ед", "Рост ВП 7д, %", "Рост заказов 7д, %", "Рост показов 7д, %", "Дней падения ВП подряд", "Дней просадки ВП при росте заказов"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    keep_cols = [c for c in empty_cols if c in out.columns]
    return out[keep_cols].sort_values(["Артикул продавца", "Дата"])


def build_profit_state(daily_profit: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    cols = ["Артикул продавца", "Фаза ВП", "Дней в фазе", "Рост ВП 7д, %", "Рост заказов 7д, %", "Рост показов 7д, %", "Дней падения ВП подряд", "Комментарий ВП"]
    if daily_profit.empty:
        return pd.DataFrame(columns=cols)

    mature = daily_profit[daily_profit["Зрелые данные"].eq("Да")].copy()
    if mature.empty:
        return pd.DataFrame(columns=cols)

    rows = []
    for article, g in mature.groupby("Артикул продавца", dropna=False):
        g = g.sort_values("Дата").copy()
        last = g.iloc[-1]
        tradeoff_streak = safe_int(last.get("Дней просадки ВП при росте заказов"))
        gp_down_streak = safe_int(last.get("Дней падения ВП подряд"))
        gp_growth_7 = last.get("Рост ВП 7д, %")
        order_growth_7 = last.get("Рост заказов 7д, %")
        imp_growth_7 = last.get("Рост показов 7д, %")
        recent_tradeoff_days = 0
        for _, r in g.tail(14).iterrows():
            gp_g = r.get("Рост ВП 7д, %")
            ord_g = r.get("Рост заказов 7д, %")
            imp_g = r.get("Рост показов 7д, %")
            cond = pd.notna(gp_g) and pd.notna(ord_g) and pd.notna(imp_g) and safe_float(ord_g) > 0 and safe_float(imp_g) >= 0 and safe_float(gp_g) < 0
            recent_tradeoff_days += int(cond)

        if tradeoff_streak > cfg.gp_growth_test_days or recent_tradeoff_days >= cfg.gp_growth_test_days:
            phase = "Рост ВП"
            phase_days = max(tradeoff_streak - cfg.gp_growth_test_days, 1)
            comment = "Больше недели росли заказы/показы при падающей ВП: дальше приоритет — рост валовой прибыли"
        elif tradeoff_streak > 0:
            phase = "Тест роста заказов"
            phase_days = tradeoff_streak
            comment = "Временно допускаем просадку ВП ради теста роста заказов, но не дольше недели"
        else:
            phase = "Нейтрально"
            phase_days = 0
            comment = "ВП под контролем"
        rows.append({
            "Артикул продавца": article,
            "Фаза ВП": phase,
            "Дней в фазе": phase_days,
            "Рост ВП 7д, %": round(safe_float(gp_growth_7), 2) if pd.notna(gp_growth_7) else None,
            "Рост заказов 7д, %": round(safe_float(order_growth_7), 2) if pd.notna(order_growth_7) else None,
            "Рост показов 7д, %": round(safe_float(imp_growth_7), 2) if pd.notna(imp_growth_7) else None,
            "Дней падения ВП подряд": gp_down_streak,
            "Комментарий ВП": comment,
        })
    return pd.DataFrame(rows)

def build_funnel_item(funnel: pd.DataFrame, master: pd.DataFrame, start: date, end: date) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if funnel.empty:
        cols1 = ["nmId","addToCartConversion","cartToOrderConversion","buyoutPercent"]
        cols2 = ["subject_norm","subj_addToCart","subj_cartToOrder"]
        return pd.DataFrame(columns=cols1), pd.DataFrame(columns=cols2)
    df = funnel[(funnel["date"] >= start) & (funnel["date"] <= end)].copy()
    if df.empty:
        cols1 = ["nmId","addToCartConversion","cartToOrderConversion","buyoutPercent"]
        cols2 = ["subject_norm","subj_addToCart","subj_cartToOrder"]
        return pd.DataFrame(columns=cols1), pd.DataFrame(columns=cols2)
    item = df.groupby("nmId", as_index=False).agg(
        addToCartConversion=("addToCartConversion", "mean"),
        cartToOrderConversion=("cartToOrderConversion", "mean"),
        buyoutPercent=("buyoutPercent", "mean"),
    )
    subj = item.merge(master[["nmId","subject_norm"]].drop_duplicates(), on="nmId", how="left")
    subj = subj.groupby("subject_norm", as_index=False).agg(
        subj_addToCart=("addToCartConversion", "median"),
        subj_cartToOrder=("cartToOrderConversion", "median"),
    )
    return item, subj

def compute_required_growth(blended_drr: float, spend_growth: float, subject_norm: str) -> float:
    sg = max(0.0, safe_float(spend_growth))
    if subject_norm in GROWTH_SUBJECTS:
        if blended_drr <= 0.12:
            return min(max(3.0, sg * 0.40), 15.0)
        if blended_drr <= 0.15:
            return min(max(6.0, sg * 0.60), 20.0)
        return min(max(10.0, sg * 0.80), 25.0)
    else:
        if blended_drr <= 0.12:
            return min(max(3.0, sg * 0.50), 12.0)
        if blended_drr <= 0.15:
            return min(max(6.0, sg * 0.75), 18.0)
        return min(max(10.0, sg * 1.00), 25.0)

def choose_control_key(subject_norm: str, supplier_article: str, product_root: str) -> str:
    return product_root if subject_norm in GROWTH_SUBJECTS else supplier_article

def build_subject_benchmarks(rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return pd.DataFrame(columns=["subject_norm","placement","bench_ctr","bench_capture_imp","bench_capture_click"])
    df = rows.copy()
    df["capture_imp"] = df["capture_imp"].map(safe_float)
    df["capture_click"] = df["capture_click"].map(safe_float)
    df["ctr_pct"] = df["ctr_pct"].map(safe_float)
    eligible = df[df["total_orders"] > 0].copy()
    if eligible.empty:
        eligible = df.copy()
    out = eligible.groupby(["subject_norm","placement"], as_index=False).agg(
        bench_ctr=("ctr_pct","median"),
        bench_capture_imp=("capture_imp","median"),
        bench_capture_click=("capture_click","median"),
    )
    return out

def compute_bid_limits(row: pd.Series, subject_benchmarks: pd.DataFrame) -> Tuple[Optional[float], Optional[float], Optional[float], str]:
    subject_norm = row["subject_norm"]
    gp_realized = safe_float(row.get("gp_realized"))
    current_bid = safe_float(row.get("current_bid_rub"))
    payment_type = str(row.get("payment_type","cpm"))
    placement = str(row.get("placement","search"))
    # choose reliable clicks per order
    limit_type = "Нет данных"
    cpo = None
    if safe_float(row.get("Клики")) >= 50 and safe_float(row.get("Заказы")) >= 3:
        cpo = safe_float(row.get("Клики")) / max(safe_float(row.get("Заказы")), 1.0)
        limit_type = "Фактический"
    elif safe_float(row.get("control_ad_clicks")) >= 50 and safe_float(row.get("total_orders")) >= 5:
        cpo = safe_float(row.get("control_ad_clicks")) / max(safe_float(row.get("total_orders")), 1.0)
        limit_type = "Наследуемый"
    if gp_realized <= 0 or not cpo or cpo <= 0:
        return None, None, None, limit_type
    comfort_share, max_share = (0.50, 0.80) if subject_norm in GROWTH_SUBJECTS else (0.40, 0.65)
    comfort_cpo = gp_realized * comfort_share
    max_cpo = gp_realized * max_share
    comfort_cpc = comfort_cpo / cpo
    max_cpc = max_cpo / cpo

    if payment_type == "cpc":
        comfort_bid = round(comfort_cpc, 2)
        max_bid = round(max_cpc, 2)
    else:
        ctr = safe_float(row.get("ctr_pct")) / 100.0
        if ctr <= 0:
            bench = subject_benchmarks[(subject_benchmarks["subject_norm"] == subject_norm) & (subject_benchmarks["placement"] == placement)]
            ctr = safe_float(bench["bench_ctr"].iloc[0]) / 100.0 if not bench.empty else 0.02
        ctr = max(ctr, 0.005)
        comfort_bid = round(comfort_cpc * 1000 * ctr, 2)
        max_bid = round(max_cpc * 1000 * ctr, 2)
    if payment_type == "cpc":
        comfort_bid = clamp(comfort_bid, 4.0, 150.0)
        max_bid = clamp(max_bid, 4.0, 300.0)
    else:
        low = 80.0 if placement == "recommendation" else 80.0
        high = 700.0 if placement in {"search","combined"} else 1200.0
        comfort_bid = clamp(comfort_bid, low, high)
        max_bid = clamp(max_bid, low, high)
    experiment_bid = round(min(max_bid * 1.15, max_bid), 2)
    return round(comfort_bid, 2), round(max_bid, 2), round(experiment_bid, 2), limit_type

def determine_action(row: pd.Series, cfg: Config) -> Tuple[str, float, str, bool]:
    subject_norm = row["subject_norm"]
    current_bid = safe_float(row["current_bid_rub"])
    comfort_bid = row.get("comfort_bid_rub")
    max_bid = row.get("max_bid_rub")
    total_orders = safe_float(row.get("total_orders"))
    ad_orders = safe_float(row.get("Заказы"))
    blended_drr = safe_float(row.get("blended_drr"))
    order_growth = safe_float(row.get("order_growth_pct"))
    required_growth = safe_float(row.get("required_growth_pct"))
    position = safe_float(row.get("median_position"))
    demand = safe_float(row.get("demand_week"))
    rating = safe_float(row.get("rating_reviews"))
    buyout = safe_float(row.get("buyout_rate"))
    gp_realized = safe_float(row.get("gp_realized"))
    weak_card = bool(row.get("card_issue"))
    weak_eff = safe_float(row.get("eff_index_click")) < 0.7 if pd.notna(row.get("eff_index_click")) else False
    growth = subject_norm in GROWTH_SUBJECTS
    payment_type = str(row.get("payment_type") or "").lower()
    min_floor = 4.0 if payment_type == "cpc" else 80.0
    rate_limit = False

    gp_phase = str(row.get("Фаза ВП") or "")
    gp_phase_days = safe_int(row.get("Дней в фазе"))
    gp_growth_7d = row.get("Рост ВП 7д, %")
    orders_growth_7d = row.get("Рост заказов 7д, %")
    imps_growth_7d = row.get("Рост показов 7д, %")
    gp_down_streak = safe_int(row.get("Дней падения ВП подряд"))
    bad_gp_dynamics = pd.notna(gp_growth_7d) and pd.notna(orders_growth_7d) and pd.notna(imps_growth_7d) and safe_float(orders_growth_7d) > 0 and safe_float(imps_growth_7d) >= 0 and safe_float(gp_growth_7d) < 0

    if pd.notna(max_bid) and safe_float(max_bid) > 0:
        rate_limit = current_bid >= safe_float(max_bid) * 0.95

    if (pd.isna(max_bid) or safe_float(max_bid) <= 0) and total_orders <= 0 and ad_orders <= 0:
        return "Без изменений", current_bid, "Недостаточно данных для расчёта лимитов, собираем статистику", rate_limit

    if gp_phase == "Рост ВП":
        target = current_bid
        if pd.notna(comfort_bid) and safe_float(comfort_bid) > 0:
            target = min(target, safe_float(comfort_bid))
        if pd.notna(max_bid) and safe_float(max_bid) > 0:
            target = min(target, safe_float(max_bid))
        target = min(target, round(current_bid * (1 - cfg.recovery_down_step), 2)) if current_bid > min_floor else target
        target = max(round(target, 2), min_floor)
        if target < current_bid - 0.01:
            return "Снизить", target, "Фаза роста ВП: после недели теста заказов приоритет — восстановление валовой прибыли", rate_limit
        return "Без изменений", current_bid, "Фаза роста ВП: не повышаем ставки, ждём роста валовой прибыли", rate_limit

    if blended_drr > cfg.max_drr:
        if pd.notna(max_bid) and safe_float(max_bid) > 0 and current_bid > safe_float(max_bid) + 0.01:
            target = max(min_floor, round(safe_float(max_bid), 2))
            return "Снизить", target, f"Общий ДРР {blended_drr*100:.1f}% выше 15% и ставка выше расчётного max", True
        if blended_drr >= cfg.critical_drr and current_bid > min_floor:
            new_bid = max(min_floor, round(current_bid * (1 - max(cfg.down_step, cfg.recovery_down_step)), 2))
            return "Снизить", new_bid, f"Общий ДРР {blended_drr*100:.1f}% критически высокий", True
        if current_bid > 0 and order_growth < required_growth:
            new_bid = round(current_bid * (1 - cfg.down_step), 2)
            return "Снизить", max(new_bid, min_floor), f"Общий ДРР {blended_drr*100:.1f}% выше 15% и рост заказов слабый", rate_limit
        return "Предел эффективности ставки", current_bid, f"Общий ДРР {blended_drr*100:.1f}% выше 15%: дальше ставкой расти нельзя", True

    if gp_realized <= 0 or (rating and rating < 4.5) or (buyout and buyout < 0.70):
        if growth:
            return "Без изменений", current_bid, "Локальная экономика слабая: для growth-товара не режем автоматически, наблюдаем", rate_limit
        if current_bid > 0:
            new_bid = round(current_bid * (1 - cfg.down_step), 2)
            return "Снизить", max(new_bid, min_floor), "Негативная экономика / рейтинг / выкуп", rate_limit
        return "Без изменений", current_bid, "Негативная экономика / рейтинг / выкуп", rate_limit

    weak_position = position <= 0 or position > 15
    demand_high = demand >= 3000
    can_raise = pd.notna(max_bid) and safe_float(max_bid) > current_bid + 0.01

    if weak_eff and rate_limit and weak_position:
        return "Предел эффективности ставки", current_bid, "Ставка близка к максимуму, а трафик/позиция не улучшаются", True

    if bad_gp_dynamics:
        if gp_phase == "Тест роста заказов" and gp_phase_days <= cfg.gp_growth_test_days:
            return "Без изменений", current_bid, "Временно жертвуем ВП ради теста роста заказов, но не дольше недели", rate_limit
        if current_bid > min_floor:
            target = current_bid
            if pd.notna(comfort_bid) and safe_float(comfort_bid) > 0:
                target = min(target, safe_float(comfort_bid))
            target = min(target, round(current_bid * (1 - cfg.down_step), 2))
            return "Снизить", max(round(target, 2), min_floor), "Показы/заказы растут, но ВП 7д падает — ставка неэффективна", rate_limit

    if growth:
        if weak_position and demand_high and can_raise and not weak_card:
            step = cfg.test_up_step if blended_drr >= cfg.comfort_drr_max else cfg.max_up_step
            proposed = round(current_bid * (1 + step), 2)
            new_bid = min(round(safe_float(max_bid), 2), proposed)
            if blended_drr <= cfg.comfort_drr_max:
                return "Повысить", new_bid, "Есть запас по max-ставке и потенциал роста позиции", rate_limit
            return "Тест роста", new_bid, "Запускаем осторожный тест роста в зоне 12–15%", rate_limit
        if weak_card and order_growth < required_growth:
            return "Предел эффективности ставки", current_bid, "Проблема в карточке / воронке: ставкой дальше не лечится", True
        if current_bid > safe_float(max_bid) > 0 and order_growth < required_growth:
            target = max(min_floor, round(safe_float(max_bid), 2))
            return "Снизить", target, "Ростовый товар, но ставка выше расчётного max и не даёт достаточного роста", rate_limit
        return "Без изменений", current_bid, "Growth-товар: удерживаем ставку, пока нет сильного сигнала на снижение", rate_limit

    severe = 0
    severe += 1 if weak_card else 0
    severe += 1 if weak_eff else 0
    severe += 1 if order_growth < required_growth else 0
    severe += 1 if weak_position and demand_high else 0

    if weak_position and demand_high and can_raise and order_growth >= 0 and not bad_gp_dynamics and gp_phase != "Рост ВП":
        proposed = round(current_bid * (1 + cfg.max_up_step), 2)
        return "Повысить", min(round(safe_float(max_bid), 2), proposed), "Слабая позиция: подтягиваем ставку к комфортной", rate_limit
    if severe >= 3 and current_bid > 0:
        new_bid = round(current_bid * (1 - cfg.down_step), 2)
        return "Снизить", max(new_bid, min_floor), "Проблема в карточке / воронке или рост заказов слабый", rate_limit
    return "Без изменений", current_bid, "Без изменений", rate_limit


def build_shade_portfolio(campaigns: pd.DataFrame, master: pd.DataFrame, orders_60: pd.DataFrame) -> pd.DataFrame:
    if campaigns.empty:
        return pd.DataFrame()
    df = campaigns[campaigns["subject_norm"].isin(GROWTH_SUBJECTS)].copy()
    if df.empty:
        return pd.DataFrame()
    df = df.merge(master[["nmId","supplier_article","product_root","rating_reviews"]].drop_duplicates(), on="nmId", how="left")
    ord_map = orders_60.groupby("supplier_article", as_index=False).agg(total_orders_60=("nmId", "count"))
    df = df.merge(ord_map, on="supplier_article", how="left")
    df["total_orders_60"] = df["total_orders_60"].fillna(0)
    core_rows = []
    for advert_id, g in df.groupby("id_campaign"):
        g = g.sort_values(["total_orders_60","rating_reviews"], ascending=[False, False]).copy()
        core_article = g["supplier_article"].iloc[0] if not g.empty else ""
        g["роль"] = g["supplier_article"].eq(core_article).map({True:"CORE", False:"WORKING"})
        core_rows.append(g)
    return pd.concat(core_rows, ignore_index=True) if core_rows else pd.DataFrame()


def build_shade_actions(campaigns: pd.DataFrame, portfolio: pd.DataFrame, master: pd.DataFrame, orders_60: pd.DataFrame, product_metrics: pd.DataFrame, api_key: str = "") -> Tuple[pd.DataFrame, pd.DataFrame]:
    if campaigns.empty or portfolio.empty:
        return pd.DataFrame([{"Комментарий":"Нет подходящих кампаний для анализа оттенков"}]), pd.DataFrame([{"Комментарий":"Нет активных тестов оттенков"}])

    actions: List[Dict[str, Any]] = []

    order_stats = pd.DataFrame()
    if not orders_60.empty:
        order_stats = orders_60.groupby("supplier_article", as_index=False).agg(
            total_orders_60=("nmId", "count"),
            revenue_60=("finishedPrice", "sum"),
        )

    universe = master[["supplier_article", "product_root", "nmId", "rating_reviews", "subject"]].dropna(subset=["supplier_article", "nmId"]).drop_duplicates().copy()
    if not order_stats.empty:
        universe = universe.merge(order_stats, on="supplier_article", how="left")
    universe["total_orders_60"] = pd.to_numeric(universe.get("total_orders_60"), errors="coerce").fillna(0)
    universe["revenue_60"] = pd.to_numeric(universe.get("revenue_60"), errors="coerce").fillna(0)
    universe["rating_reviews"] = pd.to_numeric(universe.get("rating_reviews"), errors="coerce").fillna(0)

    control_drr = product_metrics[["control_key", "blended_drr", "subject_norm"]].drop_duplicates().copy()
    control_drr["blended_drr"] = pd.to_numeric(control_drr.get("blended_drr"), errors="coerce").fillna(0)

    for advert_id, g in portfolio.groupby("id_campaign"):
        current = g.iloc[0]
        product_root = current["product_root"]
        control = control_drr[control_drr["control_key"] == product_root]
        blended = safe_float(control["blended_drr"].iloc[0]) if not control.empty else 0.0

        if blended > 0.15:
            actions.append({
                "ID кампании": safe_int(advert_id),
                "Товар": product_root,
                "Предмет": current.get("subject", ""),
                "Текущий CORE": current.get("supplier_article", ""),
                "Новый оттенок": "",
                "Артикул WB": "",
                "Действие": "Нет действий",
                "Минимальная ставка WB, ₽": None,
                "Причина": "Общий ДРР товара выше 15%, новые оттенки не добавляем",
                "Действие API": "",
                "Статус применения": "не требуется",
                "Тип кампании": f'{current.get("payment_type", "cpm")}_{current.get("placement", "combined")}',
            })
            continue

        used_articles = set(g["supplier_article"].dropna().astype(str))
        candidates = universe[(universe["product_root"] == product_root) & (~universe["supplier_article"].astype(str).isin(used_articles))].copy()
        candidates = candidates[candidates["rating_reviews"] >= MIN_RATING_SHADE].copy()

        if candidates.empty:
            actions.append({
                "ID кампании": safe_int(advert_id),
                "Товар": product_root,
                "Предмет": current.get("subject", ""),
                "Текущий CORE": current.get("supplier_article", ""),
                "Новый оттенок": "",
                "Артикул WB": "",
                "Действие": "Нет действий",
                "Минимальная ставка WB, ₽": None,
                "Причина": "Нет подходящих оттенков с рейтингом >= 4.6",
                "Действие API": "",
                "Статус применения": "не требуется",
                "Тип кампании": f'{current.get("payment_type", "cpm")}_{current.get("placement", "combined")}',
            })
            continue

        candidates = candidates.sort_values(["total_orders_60", "revenue_60", "rating_reviews"], ascending=[False, False, False])
        best = candidates.iloc[0]
        actions.append({
            "ID кампании": safe_int(advert_id),
            "Товар": product_root,
            "Предмет": current.get("subject", ""),
            "Текущий CORE": current.get("supplier_article", ""),
            "Новый оттенок": best["supplier_article"],
            "Артикул WB": safe_int(best["nmId"]),
            "Действие": "Добавить тестовый оттенок",
            "Минимальная ставка WB, ₽": None,
            "Причина": "Расширяем охват товара новым оттенком, старт с минимальной ставкой WB",
            "Действие API": "add",
            "Статус применения": "готово к применению",
            "Тип кампании": f'{current.get("payment_type", "cpm")}_{current.get("placement", "combined")}',
            "Заказы оттенка за 60 дней": round(safe_float(best.get("total_orders_60")), 2),
            "Выручка оттенка за 60 дней, ₽": round(safe_float(best.get("revenue_60")), 2),
            "Рейтинг оттенка": round(safe_float(best.get("rating_reviews")), 2),
        })

    actions_df = pd.DataFrame(actions)
    if actions_df.empty:
        actions_df = pd.DataFrame([{"Комментарий":"Нет действий по оттенкам"}])
    return actions_df, pd.DataFrame([{"Комментарий":"История тестов оттенков начнёт копиться после первого успешного добавления"}])


def apply_shade_actions(actions_df: pd.DataFrame, api_key: str, dry_run: bool) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if actions_df.empty or "Действие API" not in actions_df.columns:
        empty_log = pd.DataFrame([{"Комментарий":"Нет действий по оттенкам"}])
        empty_tests = pd.DataFrame([{"Комментарий":"Нет активных тестов оттенков"}])
        return empty_log, actions_df.copy(), empty_tests

    work = actions_df.copy()
    add_rows = work[work["Действие API"] == "add"].copy()
    add_rows["ID кампании"] = pd.to_numeric(add_rows.get("ID кампании"), errors="coerce")
    add_rows["Артикул WB"] = pd.to_numeric(add_rows.get("Артикул WB"), errors="coerce")
    add_rows = add_rows.dropna(subset=["ID кампании", "Артикул WB"]).copy()

    if add_rows.empty:
        empty_log = pd.DataFrame([{"Комментарий":"Нет валидных оттенков для добавления"}])
        empty_tests = pd.DataFrame([{"Комментарий":"Нет активных тестов оттенков"}])
        return empty_log, work, empty_tests

    logs: List[Dict[str, Any]] = []
    tests_rows: List[Dict[str, Any]] = []

    for advert_id, g in add_rows.groupby("ID кампании"):
        nm_ids = sorted({safe_int(x) for x in g["Артикул WB"].tolist() if safe_int(x) > 0})
        if not nm_ids:
            continue

        payload = {
            "nms": [
                {
                    "advert_id": safe_int(advert_id),
                    "nms": {"add": nm_ids, "delete": []},
                }
            ]
        }
        context = {
            "advert_id": safe_int(advert_id),
            "nm_ids": ",".join(map(str, nm_ids)),
        }

        resp = wb_api_request(
            "PATCH",
            WB_NMS_URL,
            api_key,
            payload,
            method_name="Изменение оттенков",
            timeout=120,
            dry_run=dry_run,
            context=context,
        )

        if dry_run or not api_key:
            logs.append({
                "timestamp": now_ts(),
                "advert_id": safe_int(advert_id),
                "status": "dry-run" if api_key else "skipped",
                "http_status": "",
                "nm_count": len(nm_ids),
                "request_body": json_dumps_safe(payload),
                "response": "dry-run" if api_key else "Нет WB_PROMO_KEY_TOPFACE",
            })
            for idx in g.index:
                work.at[idx, "Статус применения"] = "dry-run" if api_key else "пропущено: нет ключа"
            continue

        ok = bool(resp is not None and resp.status_code == 200)
        response_text = resp.text if resp is not None else ""
        logs.append({
            "timestamp": now_ts(),
            "advert_id": safe_int(advert_id),
            "status": "ok" if ok else "failed",
            "http_status": resp.status_code if resp is not None else "",
            "nm_count": len(nm_ids),
            "request_body": json_dumps_safe(payload),
            "response": truncate_text(response_text, 4000),
        })

        added_set: set[int] = set()
        if ok:
            try:
                data = resp.json()
                for row in data.get("nms", []) or []:
                    if safe_int(row.get("advert_id")) == safe_int(advert_id):
                        added_set = {safe_int(x) for x in ((row.get("nms") or {}).get("added") or [])}
                        break
            except Exception:
                added_set = set()

        for idx in g.index:
            nm_id = safe_int(work.at[idx, "Артикул WB"])
            if ok and (not added_set or nm_id in added_set):
                work.at[idx, "Статус применения"] = "успешно"
                tests_rows.append({
                    "Дата запуска": now_ts(),
                    "ID кампании": safe_int(advert_id),
                    "Артикул WB": nm_id,
                    "Новый оттенок": work.at[idx, "Новый оттенок"],
                    "Минимальная ставка WB, ₽": work.at[idx, "Минимальная ставка WB, ₽"],
                    "Статус": "добавлен",
                })
            else:
                work.at[idx, "Статус применения"] = "ошибка"

    log_df = pd.DataFrame(logs) if logs else pd.DataFrame([{"Комментарий":"Нет оттенков для применения"}])
    tests_df = pd.DataFrame(tests_rows) if tests_rows else pd.DataFrame([{"Комментарий":"Нет успешных добавлений оттенков в этом запуске"}])
    return log_df, work, tests_df


def fetch_wb_min_bids(api_key: str, advert_id: int, nm_ids: List[int], payment_type: str, placement_types: List[str]) -> Dict[int, Dict[str, float]]:
    if not nm_ids:
        return {}
    placement_types = [placement_for_min_endpoint(x) for x in placement_types if str(x).strip()]
    placement_types = list(dict.fromkeys(placement_types))
    body = {
        "advert_id": safe_int(advert_id),
        "nm_ids": [safe_int(x) for x in nm_ids[:100] if safe_int(x) > 0],
        "payment_type": canonical_payment_type(payment_type),
        "placement_types": placement_types or ["combined"],
    }
    resp = wb_api_request(
        "POST",
        WB_BIDS_MIN_URL,
        api_key,
        body,
        method_name="Минимальные ставки",
        timeout=60,
        dry_run=False,
        context={
            "advert_id": safe_int(advert_id),
            "payment_type": canonical_payment_type(payment_type),
            "placement_types": ",".join(body["placement_types"]),
            "nm_count": len(body["nm_ids"]),
        },
    )
    if resp is None or resp.status_code != 200:
        return {}

    out: Dict[int, Dict[str, float]] = {}
    try:
        data = resp.json()
    except Exception:
        return {}

    for item in data.get("bids", []) or []:
        nm_id = safe_int(item.get("nm_id"))
        if nm_id <= 0:
            continue
        by_type: Dict[str, float] = {}
        for bid in item.get("bids", []) or []:
            ptype = placement_for_min_endpoint(bid.get("type"))
            val = safe_float(bid.get("value"))
            if val > 0:
                by_type[ptype] = round(val / 100.0, 2)
                MIN_BID_ROWS.append({
                    "ID кампании": safe_int(advert_id),
                    "Артикул WB": nm_id,
                    "Тип оплаты": canonical_payment_type(payment_type),
                    "Плейсмент": ptype,
                    "Минимальная ставка WB, ₽": round(val / 100.0, 2),
                })
        if by_type:
            out[nm_id] = by_type
    return out

def enrich_with_min_bids(results: Dict[str, Any], api_key: str) -> Dict[str, Any]:
    decisions = results.get("decisions", pd.DataFrame()).copy()
    shade_actions = results.get("shade_actions", pd.DataFrame()).copy()
    MIN_BID_ROWS.clear()

    requests_rows: List[Dict[str, Any]] = []

    if not decisions.empty:
        d = decisions.copy()
        d["Тип оплаты"] = d["Тип кампании"].map(lambda x: "cpc" if "cpc" in str(x).lower() else "cpm")
        d["Плейсмент API min"] = d["Плейсмент"].map(placement_for_min_endpoint)
        for _, r in d.iterrows():
            advert_id = safe_int(r.get("ID кампании"))
            nm_id = safe_int(r.get("Артикул WB"))
            if advert_id > 0 and nm_id > 0:
                requests_rows.append({
                    "source": "решения",
                    "advert_id": advert_id,
                    "nm_id": nm_id,
                    "payment_type": canonical_payment_type(r.get("Тип оплаты")),
                    "placement_type": placement_for_min_endpoint(r.get("Плейсмент")),
                })

    if not shade_actions.empty and "Артикул WB" in shade_actions.columns:
        s = shade_actions.copy()
        s["Тип оплаты"] = s["Тип кампании"].map(lambda x: "cpc" if "cpc" in str(x).lower() else "cpm")
        s["Плейсмент API min"] = s["Тип кампании"].map(
            lambda x: "combined" if "combined" in str(x).lower() else ("search" if "search" in str(x).lower() else "recommendation")
        )
        action_series = s["Действие API"].astype(str) if "Действие API" in s.columns else pd.Series("", index=s.index)
        for _, r in s[action_series.eq("add")].iterrows():
            advert_id = safe_int(r.get("ID кампании"))
            nm_id = safe_int(r.get("Артикул WB"))
            if advert_id > 0 and nm_id > 0:
                requests_rows.append({
                    "source": "оттенки",
                    "advert_id": advert_id,
                    "nm_id": nm_id,
                    "payment_type": canonical_payment_type(r.get("Тип оплаты")),
                    "placement_type": placement_for_min_endpoint(r.get("Плейсмент API min")),
                })

    if not api_key or not requests_rows:
        results["decisions"] = decisions
        if not shade_actions.empty and "Статус применения" in shade_actions.columns:
            action_series = shade_actions["Действие API"].astype(str) if "Действие API" in shade_actions.columns else pd.Series("", index=shade_actions.index)
            mask = action_series.eq("add")
            shade_actions.loc[mask & shade_actions["Статус применения"].astype(str).isin(["ожидает", ""]), "Статус применения"] = "готово к применению"
        results["shade_actions"] = shade_actions
        results["min_bids_df"] = pd.DataFrame(MIN_BID_ROWS)
        return results

    req_df = pd.DataFrame(requests_rows).drop_duplicates()
    for (advert_id, payment_type), grp in req_df.groupby(["advert_id", "payment_type"]):
        nm_ids = sorted({safe_int(x) for x in grp["nm_id"].tolist() if safe_int(x) > 0})
        placement_types = sorted({placement_for_min_endpoint(x) for x in grp["placement_type"].tolist() if str(x).strip()})
        for i in range(0, len(nm_ids), 100):
            fetch_wb_min_bids(api_key, safe_int(advert_id), nm_ids[i:i+100], payment_type, placement_types)

    min_df = pd.DataFrame(MIN_BID_ROWS).drop_duplicates() if MIN_BID_ROWS else pd.DataFrame(columns=["ID кампании", "Артикул WB", "Тип оплаты", "Плейсмент", "Минимальная ставка WB, ₽"])
    if not min_df.empty:
        min_df["ID кампании"] = pd.to_numeric(min_df["ID кампании"], errors="coerce")
        min_df["Артикул WB"] = pd.to_numeric(min_df["Артикул WB"], errors="coerce")
        lookup = {
            (safe_int(r["ID кампании"]), safe_int(r["Артикул WB"]), canonical_payment_type(r["Тип оплаты"]), placement_for_min_endpoint(r["Плейсмент"])): safe_float(r["Минимальная ставка WB, ₽"])
            for _, r in min_df.iterrows()
        }

        if not decisions.empty:
            decisions["Тип оплаты"] = decisions["Тип кампании"].map(lambda x: "cpc" if "cpc" in str(x).lower() else "cpm")
            decisions["Плейсмент API min"] = decisions["Плейсмент"].map(placement_for_min_endpoint)
            decisions["Минимальная ставка WB, ₽"] = decisions.apply(
                lambda r: lookup.get((safe_int(r["ID кампании"]), safe_int(r["Артикул WB"]), canonical_payment_type(r["Тип оплаты"]), placement_for_min_endpoint(r["Плейсмент"]))),
                axis=1,
            )
            for idx, row in decisions.iterrows():
                min_bid = safe_float(row.get("Минимальная ставка WB, ₽"), default=-1)
                new_bid = safe_float(row.get("Новая ставка, ₽"))
                if min_bid > 0 and new_bid > 0 and new_bid < min_bid:
                    decisions.at[idx, "Новая ставка, ₽"] = round(min_bid, 2)
                    reason = str(decisions.at[idx, "Причина"])
                    suffix = f" | Подняли до минимума WB {min_bid:.2f} ₽"
                    if suffix not in reason:
                        decisions.at[idx, "Причина"] = reason + suffix

        if not shade_actions.empty and "Артикул WB" in shade_actions.columns:
            shade_actions["Тип оплаты"] = shade_actions["Тип кампании"].map(lambda x: "cpc" if "cpc" in str(x).lower() else "cpm")
            shade_actions["Плейсмент API min"] = shade_actions["Тип кампании"].map(
                lambda x: "combined" if "combined" in str(x).lower() else ("search" if "search" in str(x).lower() else "recommendation")
            )
            shade_actions["Минимальная ставка WB, ₽"] = shade_actions.apply(
                lambda r: lookup.get((safe_int(r["ID кампании"]), safe_int(r["Артикул WB"]), canonical_payment_type(r["Тип оплаты"]), placement_for_min_endpoint(r["Плейсмент API min"]))),
                axis=1,
            )
    if not shade_actions.empty and "Статус применения" in shade_actions.columns:
        action_series = shade_actions["Действие API"].astype(str) if "Действие API" in shade_actions.columns else pd.Series("", index=shade_actions.index)
        mask = action_series.eq("add")
        shade_actions.loc[mask & shade_actions["Статус применения"].astype(str).isin(["ожидает", "", "готово к применению"]), "Статус применения"] = "готово к применению"

    results["decisions"] = decisions
    results["shade_actions"] = shade_actions
    results["min_bids_df"] = min_df
    return results

def build_efficiency_history(ads_daily: pd.DataFrame, campaigns: pd.DataFrame, keywords_daily: pd.DataFrame, master: pd.DataFrame, bid_history: pd.DataFrame, as_of_date: date) -> Dict[str, pd.DataFrame]:
    if ads_daily.empty:
        return {"Нет данных": pd.DataFrame([{"Комментарий":"Нет рекламной дневной статистики"}])}
    hist = ads_daily.merge(campaigns[["id_campaign","nmId","placement","payment_type","current_bid_rub"]].drop_duplicates(), on=["id_campaign","nmId"], how="left")
    hist = hist.merge(master[["nmId","supplier_article","subject"]].drop_duplicates(), on="nmId", how="left")
    hist = hist.merge(keywords_daily, on=["date","nmId","supplier_article"], how="left")
    hist["demand"] = hist.get("demand", 0).map(safe_float)
    hist["current_bid_rub"] = hist["current_bid_rub"].map(safe_float)
    hist["id_campaign"] = pd.to_numeric(hist.get("id_campaign"), errors="coerce")
    hist["nmId"] = pd.to_numeric(hist.get("nmId"), errors="coerce")
    hist["date"] = pd.to_datetime(hist["date"], errors="coerce").dt.normalize().astype("datetime64[ns]")
    hist = hist.dropna(subset=["date", "id_campaign", "nmId"]).copy()
    hist["id_campaign"] = hist["id_campaign"].astype("int64")
    hist["nmId"] = hist["nmId"].astype("int64")

    # bid history merge_asof: only datetime64 is valid here
    if not bid_history.empty:
        events = bid_history[["id_campaign","nmId","date","bid_rub"]].copy()
        events["id_campaign"] = pd.to_numeric(events.get("id_campaign"), errors="coerce")
        events["nmId"] = pd.to_numeric(events.get("nmId"), errors="coerce")
        events["date"] = pd.to_datetime(events["date"], errors="coerce").dt.normalize().astype("datetime64[ns]")
        events["bid_rub"] = pd.to_numeric(events.get("bid_rub"), errors="coerce")
        events = events.dropna(subset=["id_campaign", "nmId", "date", "bid_rub"]).copy()
        if not events.empty:
            events["id_campaign"] = events["id_campaign"].astype("int64")
            events["nmId"] = events["nmId"].astype("int64")
        out_parts = []
        for (cid, nm), g in hist.groupby(["id_campaign","nmId"], dropna=False):
            gg = g.sort_values("date").copy()
            ev = events[(events["id_campaign"] == cid) & (events["nmId"] == nm)].copy() if not events.empty else pd.DataFrame()
            if not ev.empty:
                gg = pd.merge_asof(
                    gg.sort_values("date"),
                    ev[["date","bid_rub"]].sort_values("date"),
                    on="date",
                    direction="backward",
                    allow_exact_matches=True,
                )
                gg["bid_rub"] = gg["bid_rub"].fillna(gg["current_bid_rub"])
            else:
                gg["bid_rub"] = gg["current_bid_rub"]
            out_parts.append(gg)
        hist = pd.concat(out_parts, ignore_index=True) if out_parts else hist.assign(bid_rub=hist["current_bid_rub"])
    else:
        hist["bid_rub"] = hist["current_bid_rub"]
    hist["ctr_pct"] = hist["CTR"].map(safe_float)
    hist["capture_imp"] = hist.apply(lambda r: safe_float(r["Показы"]) / safe_float(r["demand"]) if safe_float(r["demand"]) else math.nan, axis=1)
    hist["capture_click"] = hist.apply(lambda r: safe_float(r["Клики"]) / safe_float(r["demand"]) if safe_float(r["demand"]) else math.nan, axis=1)
    hist["eff_imp"] = hist.apply(lambda r: (safe_float(r["Показы"]) / safe_float(r["demand"]) / safe_float(r["bid_rub"])) if safe_float(r["demand"]) and safe_float(r["bid_rub"]) else math.nan, axis=1)
    hist["eff_click"] = hist.apply(lambda r: (safe_float(r["Клики"]) / safe_float(r["demand"]) / safe_float(r["bid_rub"])) if safe_float(r["demand"]) and safe_float(r["bid_rub"]) else math.nan, axis=1)
    hist["Тип кампании"] = hist["payment_type"].astype(str) + "_" + hist["placement"].astype(str)
    hist = hist.sort_values(["supplier_article","date","id_campaign"])

    # conclusions
    out_sheets: Dict[str, pd.DataFrame] = {}
    used_names = set()
    for article, g in hist.groupby("supplier_article"):
        if not str(article):
            continue
        g = g.copy().sort_values(["date","id_campaign"])
        conclusions = []
        prev_eff = {}
        for _, r in g.iterrows():
            key = (r["id_campaign"], r["Тип кампании"])
            cur = safe_float(r["eff_click"], math.nan)
            if math.isnan(cur):
                conclusions.append("Нет спроса или данных")
                continue
            prior = prev_eff.get(key, [])
            prior_valid = [x for x in prior if not math.isnan(x)]
            if len(prior_valid) >= 3:
                base = float(pd.Series(prior_valid[-7:]).median())
                if base > 0:
                    ratio = cur / base
                    if ratio >= 1.10:
                        conclusions.append("За ту же ставку начали получать больше кликов")
                    elif ratio <= 0.90:
                        conclusions.append("Эффективность ставки снижается")
                    else:
                        conclusions.append("Без существенных изменений")
                else:
                    conclusions.append("Недостаточно истории")
            else:
                conclusions.append("Недостаточно истории")
            prev_eff.setdefault(key, []).append(cur)
        sheet = pd.DataFrame({
            "Дата": g["date"],
            "ID кампании": g["id_campaign"],
            "Тип кампании": g["Тип кампании"],
            "Плейсмент": g["placement"],
            "Ставка, ₽": g["bid_rub"].round(2),
            "Показы": g["Показы"].round(0),
            "Клики": g["Клики"].round(0),
            "CTR, %": g["ctr_pct"].round(2),
            "Спрос": g["demand"].round(0),
            "Доля показов": (g["capture_imp"] * 100).round(4),
            "Доля кликов": (g["capture_click"] * 100).round(4),
            "Эффективность ставки по показам": g["eff_imp"].round(6),
            "Эффективность ставки по кликам": g["eff_click"].round(6),
            "Вывод": conclusions,
        })
        out_sheets[sanitize_sheet_name(str(article), used_names)] = sheet
    if not out_sheets:
        out_sheets = {"Нет данных": pd.DataFrame([{"Комментарий":"Нет истории эффективности ставки"}])}
    return out_sheets

def prepare_metrics(provider: BaseProvider, cfg: Config, as_of_date: date) -> Dict[str, Any]:
    window = compute_analysis_window(as_of_date)
    log(f"📅 Анализируем зрелое окно {window['cur_start']} .. {window['cur_end']}; база сравнения {window['base_start']} .. {window['base_end']}")
    ads_daily, campaigns = load_ads(provider)
    econ = load_economics(provider)
    orders = load_orders(provider)
    funnel = load_funnel(provider)
    keywords = load_keywords(provider)
    bid_history = load_bid_history(provider)
    log(f"📣 Реклама: {len(ads_daily):,} строк; кампании: {campaigns['id_campaign'].nunique() if not campaigns.empty else 0}; placement-строк: {len(campaigns):,}")
    log(f"💰 Экономика: {len(econ):,} SKU; Заказы: {len(orders):,} строк; Воронка: {len(funnel):,}; Keywords: {len(keywords):,}")

    master = build_master(econ, orders, keywords, campaigns)
    keywords_current = aggregate_keyword_item(keywords, window["cur_start"], window["cur_end"])
    keywords_daily = aggregate_keyword_daily(keywords)
    funnel_item, funnel_subject = build_funnel_item(funnel, master, window["cur_start"], window["cur_end"])

    econ_latest = econ.sort_values("Неделя").drop_duplicates("nmId", keep="last")[["nmId","supplier_article","product_root","subject","subject_norm","buyout_rate","gp_realized"]]
    daily_profit = build_daily_profit_metrics(orders, ads_daily, master, econ_latest, as_of_date, cfg)
    profit_state = build_profit_state(daily_profit, cfg)
    campaign_base = campaigns.merge(master[["nmId","supplier_article","product_root","subject","subject_norm","rating_reviews","rating_card"]].drop_duplicates(), on="nmId", how="left")
    campaign_base = campaign_base.merge(econ_latest[["nmId","buyout_rate","gp_realized"]], on="nmId", how="left")
    if campaign_base.empty:
        raise RuntimeError("Нет кампаний целевых предметов в файле рекламы")

    campaign_cur = ads_daily[(ads_daily["date"] >= window["cur_start"]) & (ads_daily["date"] <= window["cur_end"])].groupby(["id_campaign","nmId"], as_index=False).agg(
        Показы=("Показы","sum"), Клики=("Клики","sum"), Заказы=("Заказы","sum"), Расход=("Расход","sum"), Сумма_заказов=("Сумма заказов","sum")
    )
    campaign_base_stats = ads_daily[(ads_daily["date"] >= window["base_start"]) & (ads_daily["date"] <= window["base_end"])].groupby(["id_campaign","nmId"], as_index=False).agg(
        base_Показы=("Показы","sum"), base_Клики=("Клики","sum"), base_Заказы=("Заказы","sum"), base_Расход=("Расход","sum"), base_Сумма_заказов=("Сумма заказов","sum")
    )
    rows = campaign_base.merge(campaign_cur, on=["id_campaign","nmId"], how="left").merge(campaign_base_stats, on=["id_campaign","nmId"], how="left").fillna(0)

    # robustly restore key descriptive columns after merges
    # restore subject and subject_norm after merges
    if "subject" not in rows.columns:
        subject_cols = [c for c in ["subject_x", "subject_y"] if c in rows.columns]
        if subject_cols:
            rows["subject"] = rows[subject_cols[0]]
            for c in subject_cols[1:]:
                rows["subject"] = rows["subject"].where(rows["subject"].astype(str).str.strip() != "", rows[c])
        else:
            rows["subject"] = ""
    else:
        rows["subject"] = rows["subject"].fillna("")

    if "subject_norm" not in rows.columns:
        subject_candidates = [c for c in ["subject_norm_x", "subject_norm_y"] if c in rows.columns]
        if subject_candidates:
            rows["subject_norm"] = rows[subject_candidates[0]]
            for c in subject_candidates[1:]:
                rows["subject_norm"] = rows["subject_norm"].where(rows["subject_norm"].astype(str).str.strip() != "", rows[c])
        else:
            rows["subject_norm"] = rows["subject"].map(canonical_subject)
    else:
        rows["subject_norm"] = rows["subject_norm"].fillna("")
        mask_empty = rows["subject_norm"].astype(str).str.strip() == ""
        rows.loc[mask_empty, "subject_norm"] = rows.loc[mask_empty, "subject"].map(canonical_subject)

    if "supplier_article" not in rows.columns:
        for c in ["supplier_article_x", "supplier_article_y", "supplierArticle", "supplierArticle_x", "supplierArticle_y"]:
            if c in rows.columns:
                rows["supplier_article"] = rows[c]
                break
        else:
            rows["supplier_article"] = ""
    rows["supplier_article"] = rows["supplier_article"].fillna("").astype(str)

    if "product_root" not in rows.columns:
        for c in ["product_root_x", "product_root_y"]:
            if c in rows.columns:
                rows["product_root"] = rows[c]
                break
        else:
            rows["product_root"] = rows["supplier_article"].map(product_root_from_supplier_article)
    missing_root = rows["product_root"].isna() | (rows["product_root"].astype(str).str.strip() == "")
    rows.loc[missing_root, "product_root"] = rows.loc[missing_root, "supplier_article"].map(product_root_from_supplier_article)

    # control metrics
    rows["control_key"] = rows.apply(lambda r: choose_control_key(r.get("subject_norm", ""), r.get("supplier_article", ""), r.get("product_root", "")), axis=1)
    orders_cur_root = aggregate_orders(orders, window["cur_start"], window["cur_end"], "product_root")
    orders_base_root = aggregate_orders(orders, window["base_start"], window["base_end"], "product_root").rename(columns={"total_orders":"base_total_orders","total_revenue":"base_total_revenue"})
    orders_cur_article = aggregate_orders(orders, window["cur_start"], window["cur_end"], "supplier_article")
    orders_base_article = aggregate_orders(orders, window["base_start"], window["base_end"], "supplier_article").rename(columns={"total_orders":"base_total_orders","total_revenue":"base_total_revenue"})

    ads_cur_root = aggregate_ads_control(ads_daily, window["cur_start"], window["cur_end"], master, "product_root")
    ads_base_root = aggregate_ads_control(ads_daily, window["base_start"], window["base_end"], master, "product_root").rename(columns={
        "ad_spend":"base_ad_spend","ad_clicks":"base_ad_clicks","ad_orders":"base_ad_orders","ad_impressions":"base_ad_impressions","ad_revenue":"base_ad_revenue"})
    ads_cur_article = aggregate_ads_control(ads_daily, window["cur_start"], window["cur_end"], master, "supplier_article")
    ads_base_article = aggregate_ads_control(ads_daily, window["base_start"], window["base_end"], master, "supplier_article").rename(columns={
        "ad_spend":"base_ad_spend","ad_clicks":"base_ad_clicks","ad_orders":"base_ad_orders","ad_impressions":"base_ad_impressions","base_ad_revenue":"ad_revenue","ad_revenue":"base_ad_revenue"})

    # attach based on control type (safe merges without duplicate key columns)
    root_rows = rows["subject_norm"].isin(GROWTH_SUBJECTS)

    growth_part = rows[root_rows].copy()
    growth_part = growth_part.merge(orders_cur_root.rename(columns={"product_root": "control_key"}), on="control_key", how="left")
    growth_part = growth_part.merge(orders_base_root.rename(columns={"product_root": "control_key"}), on="control_key", how="left")
    growth_part = growth_part.merge(ads_cur_root.rename(columns={"product_root": "control_key"}), on="control_key", how="left")
    growth_part = growth_part.merge(ads_base_root.rename(columns={"product_root": "control_key"}), on="control_key", how="left")

    brush_part = rows[~root_rows].copy()
    brush_part = brush_part.merge(orders_cur_article.rename(columns={"supplier_article": "control_key"}), on="control_key", how="left")
    brush_part = brush_part.merge(orders_base_article.rename(columns={"supplier_article": "control_key"}), on="control_key", how="left")
    brush_part = brush_part.merge(ads_cur_article.rename(columns={"supplier_article": "control_key"}), on="control_key", how="left")
    brush_part = brush_part.merge(ads_base_article.rename(columns={"supplier_article": "control_key"}), on="control_key", how="left")

    rows = pd.concat([growth_part, brush_part], ignore_index=True, sort=False).fillna(0)

    rows = rows.merge(keywords_current, on=["nmId","supplier_article"], how="left")
    rows = rows.merge(funnel_item, on="nmId", how="left").merge(funnel_subject, on="subject_norm", how="left")
    if not profit_state.empty:
        rows = rows.merge(profit_state.rename(columns={"Артикул продавца": "supplier_article"}), on="supplier_article", how="left")
    rows["ctr_pct"] = rows.apply(lambda r: pct(r["Клики"], r["Показы"]), axis=1)
    rows["capture_imp"] = rows.apply(lambda r: safe_float(r["Показы"]) / safe_float(r["demand_week"]) if safe_float(r["demand_week"]) else 0.0, axis=1)
    rows["capture_click"] = rows.apply(lambda r: safe_float(r["Клики"]) / safe_float(r["demand_week"]) if safe_float(r["demand_week"]) else 0.0, axis=1)
    rows["blended_drr"] = rows.apply(lambda r: safe_float(r["ad_spend"]) / safe_float(r["total_revenue"]) if safe_float(r["total_revenue"]) else 0.0, axis=1)
    rows["ad_drr"] = rows.apply(lambda r: safe_float(r["Расход"]) / safe_float(r["Сумма_заказов"]) if safe_float(r["Сумма_заказов"]) else 0.0, axis=1)
    rows["order_growth_pct"] = rows.apply(lambda r: growth_pct(r["total_orders"], r["base_total_orders"]), axis=1)
    rows["spend_growth_pct"] = rows.apply(lambda r: growth_pct(r["ad_spend"], r["base_ad_spend"]), axis=1)
    rows["drr_growth_pp"] = rows.apply(lambda r: (safe_float(r["blended_drr"]) - (safe_float(r["base_ad_spend"]) / safe_float(r["base_total_revenue"]) if safe_float(r["base_total_revenue"]) else 0.0))*100.0, axis=1)
    rows["required_growth_pct"] = rows.apply(lambda r: compute_required_growth(safe_float(r["blended_drr"]), safe_float(r["spend_growth_pct"]), r["subject_norm"]), axis=1)
    rows["card_issue"] = rows.apply(lambda r: (safe_float(r.get("addToCartConversion")) > 0 and safe_float(r.get("subj_addToCart")) > 0 and safe_float(r["addToCartConversion"]) < safe_float(r["subj_addToCart"]) * 0.7) or (safe_float(r.get("cartToOrderConversion")) > 0 and safe_float(r.get("subj_cartToOrder")) > 0 and safe_float(r["cartToOrderConversion"]) < safe_float(r["subj_cartToOrder"]) * 0.7), axis=1)
    if "Фаза ВП" not in rows.columns:
        rows["Фаза ВП"] = "Нейтрально"
    if "Дней в фазе" not in rows.columns:
        rows["Дней в фазе"] = 0
    if "Комментарий ВП" not in rows.columns:
        rows["Комментарий ВП"] = ""
    for _c in ["Рост ВП 7д, %", "Рост заказов 7д, %", "Рост показов 7д, %", "Дней падения ВП подряд"]:
        if _c not in rows.columns:
            rows[_c] = np.nan

    # preliminary rows for benchmarks
    rows["bid_eff_imp"] = rows.apply(lambda r: (safe_float(r["capture_imp"]) / safe_float(r["current_bid_rub"])) if safe_float(r["current_bid_rub"]) else 0.0, axis=1)
    rows["bid_eff_click"] = rows.apply(lambda r: (safe_float(r["capture_click"]) / safe_float(r["current_bid_rub"])) if safe_float(r["current_bid_rub"]) else 0.0, axis=1)
    subject_benchmarks = build_subject_benchmarks(rows)
    rows = rows.merge(subject_benchmarks, on=["subject_norm","placement"], how="left")
    rows["eff_index_imp"] = rows.apply(lambda r: safe_float(r["capture_imp"]) / safe_float(r["bench_capture_imp"]) if safe_float(r["bench_capture_imp"]) else 1.0, axis=1)
    rows["eff_index_click"] = rows.apply(lambda r: safe_float(r["capture_click"]) / safe_float(r["bench_capture_click"]) if safe_float(r["bench_capture_click"]) else 1.0, axis=1)

    # limits and decisions
    limits = rows.apply(lambda r: pd.Series(compute_bid_limits(r, subject_benchmarks), index=["comfort_bid_rub","max_bid_rub","experiment_bid_rub","limit_type"]), axis=1)
    rows = pd.concat([rows, limits], axis=1)
    decisions = []
    for _, r in rows.iterrows():
        action, new_bid, reason, rate_limit = determine_action(r, cfg)
        decisions.append({
            "Дата запуска": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "ID кампании": safe_int(r["id_campaign"]),
            "Артикул WB": safe_int(r["nmId"]),
            "Артикул продавца": r["supplier_article"],
            "Товар": r["control_key"],
            "Предмет": r.get("subject", ""),
            "Плейсмент": r["placement"],
            "Тип кампании": f'{r["payment_type"]}_{r["placement"]}',
            "Текущая ставка, ₽": round(safe_float(r["current_bid_rub"]), 2),
            "Комфортная ставка, ₽": round(safe_float(r["comfort_bid_rub"]), 2) if pd.notna(r["comfort_bid_rub"]) else None,
            "Максимальная ставка, ₽": round(safe_float(r["max_bid_rub"]), 2) if pd.notna(r["max_bid_rub"]) else None,
            "Экспериментальная ставка, ₽": round(safe_float(r["experiment_bid_rub"]), 2) if pd.notna(r["experiment_bid_rub"]) else None,
            "Тип лимита": r["limit_type"],
            "Действие": action,
            "Новая ставка, ₽": round(safe_float(new_bid), 2),
            "Причина": reason,
            "Показы": round(safe_float(r["Показы"]), 0),
            "Клики": round(safe_float(r["Клики"]), 0),
            "CTR, %": round(safe_float(r["ctr_pct"]), 2),
            "Заказы РК": round(safe_float(r["Заказы"]), 2),
            "Все заказы товара": round(safe_float(r["total_orders"]), 2),
            "Расход РК, ₽": round(safe_float(r["Расход"]), 2),
            "Выручка РК, ₽": round(safe_float(r["Сумма_заказов"]), 2),
            "Выручка товара, ₽": round(safe_float(r["total_revenue"]), 2),
            "Общий ДРР товара, %": round(safe_float(r["blended_drr"]) * 100, 2),
            "Рекламный ДРР, %": round(safe_float(r["ad_drr"]) * 100, 2),
            "Рост заказов, %": round(safe_float(r["order_growth_pct"]), 2),
            "Рост расходов, %": round(safe_float(r["spend_growth_pct"]), 2),
            "Требуемый рост заказов, %": round(safe_float(r["required_growth_pct"]), 2),
            "Спрос за окно": round(safe_float(r["demand_week"]), 0),
            "Медианная позиция": round(safe_float(r["median_position"]), 2),
            "Видимость, %": round(safe_float(r["visibility_pct"]), 2),
            "Индекс эффективности ставки по показам": round(safe_float(r["eff_index_imp"]), 4),
            "Индекс эффективности ставки по кликам": round(safe_float(r["eff_index_click"]), 4),
            "Предел эффективности": "Да" if rate_limit or action == "Предел эффективности ставки" else "Нет",
            "Проблема карточки": "Да" if bool(r["card_issue"]) else "Нет",
            "Фаза ВП": r.get("Фаза ВП", "Нейтрально"),
            "Дней в фазе ВП": safe_int(r.get("Дней в фазе")),
            "Рост ВП 7д, %": round(safe_float(r.get("Рост ВП 7д, %")), 2) if pd.notna(r.get("Рост ВП 7д, %")) else None,
            "Рост заказов 7д, %": round(safe_float(r.get("Рост заказов 7д, %")), 2) if pd.notna(r.get("Рост заказов 7д, %")) else None,
            "Рост показов 7д, %": round(safe_float(r.get("Рост показов 7д, %")), 2) if pd.notna(r.get("Рост показов 7д, %")) else None,
            "Дней падения ВП подряд": safe_int(r.get("Дней падения ВП подряд")),
            "Комментарий ВП": r.get("Комментарий ВП", ""),
        })
    decisions_df = pd.DataFrame(decisions)
    # weak positions simple
    weak = decisions_df[(decisions_df["Действие"].isin(["Снизить","Предел эффективности ставки"])) | (decisions_df["Медианная позиция"] > 20)].copy()
    weak["Комментарий"] = weak["Причина"]
    weak = weak[["Артикул продавца","Артикул WB","ID кампании","Тип кампании","Плейсмент","Действие","Комментарий"]].drop_duplicates()

    # product metrics
    product_metrics = rows.groupby(["control_key","subject_norm"], as_index=False).agg(
        total_orders=("total_orders","max"),
        total_revenue=("total_revenue","max"),
        ad_spend=("ad_spend","max"),
        ad_orders=("ad_orders","max"),
        ad_clicks=("ad_clicks","max"),
        blended_drr=("blended_drr","max"),
        order_growth_pct=("order_growth_pct","max"),
        spend_growth_pct=("spend_growth_pct","max"),
        required_growth_pct=("required_growth_pct","max"),
        gp_growth_7d_pct=("Рост ВП 7д, %", "max"),
        orders_growth_7d_pct=("Рост заказов 7д, %", "max"),
        imps_growth_7d_pct=("Рост показов 7д, %", "max"),
    ).rename(columns={"control_key":"Товар","subject_norm":"Предмет код"})
    product_metrics["Общий ДРР товара, %"] = (product_metrics["blended_drr"]*100).round(2)
    product_metrics["Рост ВП 7д, %"] = pd.to_numeric(product_metrics["gp_growth_7d_pct"], errors="coerce").round(2)
    product_metrics["Рост заказов 7д, %"] = pd.to_numeric(product_metrics["orders_growth_7d_pct"], errors="coerce").round(2)
    product_metrics["Рост показов 7д, %"] = pd.to_numeric(product_metrics["imps_growth_7d_pct"], errors="coerce").round(2)

    # benchmark comparison clean
    bench_cmp = decisions_df.merge(subject_benchmarks, left_on=["Предмет","Плейсмент"], right_on=["subject_norm","placement"], how="left")
    bench_cmp = bench_cmp[["Артикул продавца","ID кампании","Тип кампании","Плейсмент","CTR, %","Индекс эффективности ставки по показам","Индекс эффективности ставки по кликам","Причина","bench_ctr","bench_capture_imp","bench_capture_click"]].copy()
    bench_cmp = bench_cmp.rename(columns={"bench_ctr":"Эталон CTR, %","bench_capture_imp":"Эталон доля показов","bench_capture_click":"Эталон доля кликов"})

    # effects: simple from changed decisions
    changed = decisions_df[decisions_df["Действие"].isin(["Повысить","Снизить","Тест роста"]) & (decisions_df["Текущая ставка, ₽"] != decisions_df["Новая ставка, ₽"])].copy()
    if changed.empty:
        effects = pd.DataFrame([{"Комментарий":"В этом запуске не было изменений ставок"}])
    else:
        effects = changed[["Дата запуска","Артикул продавца","ID кампании","Тип кампании","Текущая ставка, ₽","Новая ставка, ₽","Действие","Причина"]].copy()
        effects["Комментарий"] = "Ожидаем накопление зрелых данных после изменения"

    orders_60 = orders[(orders["date"] >= as_of_date - timedelta(days=60)) & (orders["date"] <= as_of_date) & (~orders["isCancel"])].copy() if not orders.empty else pd.DataFrame()
    shade_portfolio = build_shade_portfolio(campaigns, master, orders_60)
    shade_actions, shade_tests = build_shade_actions(campaigns, shade_portfolio, master, orders_60, product_metrics.rename(columns={"Товар":"control_key","Предмет код":"subject_norm","Общий ДРР товара, %":"blended_drr"}), api_key=os.getenv("WB_PROMO_KEY_TOPFACE",""))
    if shade_actions.empty:
        shade_actions = pd.DataFrame([{"Комментарий":"Нет действий по оттенкам"}])

    return {
        "rows": rows,
        "decisions": decisions_df,
        "weak": weak,
        "product_metrics": product_metrics,
        "bench_cmp": bench_cmp,
        "effects": effects,
        "shade_portfolio": shade_portfolio if not shade_portfolio.empty else pd.DataFrame([{"Комментарий":"Нет кампаний по оттенкам"}]),
        "shade_actions": shade_actions,
        "shade_tests": shade_tests,
        "eff_history_sheets": build_efficiency_history(ads_daily, campaigns, keywords_daily, master, bid_history, as_of_date),
        "daily_profit": daily_profit,
        "profit_state": profit_state,
        "window": window,
    }

def normalize_bid_for_wb(value_rub: float, payment_type: str, placement: str) -> int:
    value_rub = safe_float(value_rub)
    if payment_type == "cpc":
        return int(round(value_rub * 100))
    # cpm in WB examples also in kopecks
    return int(round(value_rub * 100))

def decisions_to_payload(decisions_df: pd.DataFrame) -> Dict[str, Any]:
    changed = decisions_df[decisions_df["Действие"].isin(["Повысить","Снизить","Тест роста"]) & (decisions_df["Новая ставка, ₽"] != decisions_df["Текущая ставка, ₽"])].copy()
    grouped = {}
    for _, r in changed.iterrows():
        advert = safe_int(r["ID кампании"])
        nm_id = safe_int(r["Артикул WB"])
        payment_type = "cpc" if "cpc" in str(r["Тип кампании"]).lower() else "cpm"
        placement = str(r["Плейсмент"])
        grouped.setdefault((advert, payment_type), []).append({
            "nm_id": nm_id,
            "placement": placement_for_bids_endpoint(placement),
            "bid_kopecks": normalize_bid_for_wb(r["Новая ставка, ₽"], payment_type, placement),
        })
    out = []
    for (advert, payment_type), items in grouped.items():
        out.append({"advert_id": advert, "payment_type": payment_type, "nm_bids": items})
    return {"bids": out}


def send_payload(payload: Dict[str, Any], api_key: str, dry_run: bool) -> pd.DataFrame:
    logs: List[Dict[str, Any]] = []
    for block in payload.get("bids", []):
        advert_id = safe_int(block["advert_id"])
        nm_bids = []
        for item in block.get("nm_bids", []):
            nm_bids.append({
                "nm_id": safe_int(item.get("nm_id")),
                "bid_kopecks": safe_int(item.get("bid_kopecks")),
                "placement": placement_for_bids_endpoint(item.get("placement")),
            })
        body = {"bids": [{"advert_id": advert_id, "nm_bids": nm_bids}]}
        resp = wb_api_request(
            "PATCH",
            WB_BIDS_URL,
            api_key,
            body,
            method_name="Изменение ставок",
            timeout=120,
            dry_run=dry_run,
            context={"advert_id": advert_id, "nm_count": len(nm_bids)},
        )
        logs.append({
            "timestamp": now_ts(),
            "advert_id": advert_id,
            "status": "dry-run" if dry_run and api_key else ("skipped" if not api_key else ("ok" if resp is not None and resp.status_code == 200 else "failed")),
            "http_status": resp.status_code if resp is not None else "",
            "request_body": json_dumps_safe(body),
            "response": truncate_text(resp.text if resp is not None else ("dry-run" if api_key else "Нет WB_PROMO_KEY_TOPFACE"), 4000),
        })
    return pd.DataFrame(logs)

def save_outputs(provider: BaseProvider, results: Dict[str, Any], run_mode: str, bid_send_log: Optional[pd.DataFrame], shade_apply_log: Optional[pd.DataFrame], history_append: pd.DataFrame) -> None:
    decisions = results["decisions"].copy()
    limits_df = decisions[["Артикул продавца","ID кампании","Тип кампании","Текущая ставка, ₽","Комфортная ставка, ₽","Максимальная ставка, ₽","Экспериментальная ставка, ₽","Тип лимита"]].copy()

    min_bids_df = results.get("min_bids_df", pd.DataFrame()).copy()
    if not min_bids_df.empty:
        sort_cols = [c for c in ["ID кампании", "Артикул WB", "Плейсмент"] if c in min_bids_df.columns]
        min_bids_df = min_bids_df.sort_values(sort_cols).drop_duplicates()

    summary = {
        "Режим": run_mode,
        "Дата формирования": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Всего рекомендаций": int(len(decisions)),
        "Изменённых ставок": int(len(decisions[(decisions["Действие"].isin(["Повысить","Снизить","Тест роста"])) & (decisions["Новая ставка, ₽"] != decisions["Текущая ставка, ₽"])])),
        "Достигнут предел эффективности": int((decisions["Действие"] == "Предел эффективности ставки").sum()) if "Действие" in decisions.columns else 0,
        "Слабых позиций": int(len(results["weak"])),
        "Рекомендаций по оттенкам": 0 if results["shade_actions"].empty else int(len(results["shade_actions"])),
        "Блоков отправки ставок": 0 if bid_send_log is None or bid_send_log.empty else int(len(bid_send_log)),
        "Блоков применения оттенков": 0 if shade_apply_log is None or shade_apply_log.empty else int(len(shade_apply_log)),
        "Артикулов с фазой роста ВП": 0 if results.get("profit_state", pd.DataFrame()).empty else int((results.get("profit_state", pd.DataFrame())["Фаза ВП"] == "Рост ВП").sum()),
        "Артикулов в тесте роста заказов": 0 if results.get("profit_state", pd.DataFrame()).empty else int((results.get("profit_state", pd.DataFrame())["Фаза ВП"] == "Тест роста заказов").sum()),
        "Текущее окно с": results["window"]["cur_start"],
        "Текущее окно по": results["window"]["cur_end"],
        "База с": results["window"]["base_start"],
        "База по": results["window"]["base_end"],
    }
    summary_df = pd.DataFrame([summary])

    # История решений и ставок храним внутри того же единого файла.
    old_sheets = {}
    try:
        if provider.file_exists(OUT_SINGLE_REPORT):
            old_sheets = provider.read_excel_all_sheets(OUT_SINGLE_REPORT)
    except Exception:
        old_sheets = {}

    old_archive = old_sheets.get("Архив решений", old_sheets.get("Архив_решений", pd.DataFrame()))
    new_archive = pd.concat([old_archive, decisions], ignore_index=True) if not old_archive.empty else decisions.copy()

    old_bid_hist = old_sheets.get("История_ставок", pd.DataFrame())
    if history_append is not None and not history_append.empty:
        history_append = history_append.copy()
        new_bid_hist = pd.concat([old_bid_hist, history_append], ignore_index=True) if not old_bid_hist.empty else history_append
    else:
        new_bid_hist = old_bid_hist

    api_log_df = pd.DataFrame(API_CALL_LOGS).copy() if API_CALL_LOGS else pd.DataFrame()

    single_report_sheets = {
        "Решения": decisions,
        "Сводка": summary_df,
        "Минимальные ставки WB": min_bids_df if not min_bids_df.empty else pd.DataFrame([{"Комментарий": "Минимальные ставки не получены"}]),
        "Лимиты ставок": limits_df if not limits_df.empty else pd.DataFrame([{"Комментарий": "Нет данных"}]),
        "Расчёт логики": results["rows"],
        "Метрики по товарам": results["product_metrics"],
        "Валовая прибыль по дням": results.get("daily_profit", pd.DataFrame()) if not results.get("daily_profit", pd.DataFrame()).empty else pd.DataFrame([{"Комментарий":"Нет дневных данных по валовой прибыли"}]),
        "Статус ВП товаров": results.get("profit_state", pd.DataFrame()) if not results.get("profit_state", pd.DataFrame()).empty else pd.DataFrame([{"Комментарий":"Нет зрелых данных для статуса ВП"}]),
        "Слабые позиции": results["weak"] if not results["weak"].empty else pd.DataFrame([{"Комментарий":"Нет слабых позиций"}]),
        "Рекомендации по оттенкам": results["shade_actions"] if not results["shade_actions"].empty else pd.DataFrame([{"Комментарий":"Нет рекомендаций"}]),
        "Состав кампаний по оттенкам": results["shade_portfolio"] if not results["shade_portfolio"].empty else pd.DataFrame([{"Комментарий":"Нет данных"}]),
        "Тесты оттенков": results["shade_tests"] if not results["shade_tests"].empty else pd.DataFrame([{"Комментарий":"Нет данных"}]),
        "Сравнение с сильными РК": results["bench_cmp"] if not results["bench_cmp"].empty else pd.DataFrame([{"Комментарий":"Нет данных"}]),
        "Эффект изменений": results["effects"] if not results["effects"].empty else pd.DataFrame([{"Комментарий":"Нет данных"}]),
        "Эффективность ставки": pd.DataFrame([{"Комментарий":"См. листы ниже по истории эффективности"}]),
        "Лог API": api_log_df if not api_log_df.empty else pd.DataFrame([{"Комментарий":"API-вызовы в этом запуске не выполнялись"}]),
        "Архив решений": new_archive,
        "История ставок": new_bid_hist if new_bid_hist is not None and not new_bid_hist.empty else pd.DataFrame([{"Комментарий":"История ставок пока пуста"}]),
        "Окно анализа": pd.DataFrame([{
            "Текущее окно с": results["window"]["cur_start"],
            "Текущее окно по": results["window"]["cur_end"],
            "База с": results["window"]["base_start"],
            "База по": results["window"]["base_end"],
            "Режим": run_mode,
        }]),
    }

    # Добавляем листы ежедневной эффективности в конец единого файла.
    eff_sheets = results.get("eff_history_sheets", {}) or {}
    for sh_name, sh_df in eff_sheets.items():
        single_report_sheets[f"Эффективность {sh_name}"] = sh_df

    provider.write_excel(OUT_SINGLE_REPORT, single_report_sheets)


def build_history_append(changed: pd.DataFrame, as_of_date: date) -> pd.DataFrame:
    if changed.empty:
        return pd.DataFrame()
    rows = []
    week = f"{as_of_date.isocalendar().year}-W{as_of_date.isocalendar().week:02d}"
    for _, r in changed.iterrows():
        placement = normalize_internal_placement(r.get("Плейсмент"))
        bid_kop = normalize_bid_for_wb(r.get("Новая ставка, ₽"), "cpc" if "cpc" in str(r.get("Тип кампании", "")).lower() else "cpm", placement)
        rows.append({
            "Дата запуска": now_ts(),
            "Неделя": week,
            "ID кампании": safe_int(r.get("ID кампании")),
            "Артикул WB": safe_int(r.get("Артикул WB")),
            "Тип кампании": r.get("Тип кампании"),
            "Ставка поиск, коп": bid_kop if placement in {"search", "combined"} else 0,
            "Ставка рекомендации, коп": bid_kop if placement in {"recommendation", "combined"} else 0,
            "Стратегия": "STABLE_V2",
        })
    return pd.DataFrame(rows)

def run_manager(args: argparse.Namespace) -> None:
    API_CALL_LOGS.clear()
    MIN_BID_ROWS.clear()
    provider = choose_provider(args.local_data_dir)
    as_of_date = datetime.strptime(args.as_of_date, "%Y-%m-%d").date() if args.as_of_date else datetime.now().date()
    cfg = Config()
    results = prepare_metrics(provider, cfg, as_of_date)

    api_key = os.getenv("WB_PROMO_KEY_TOPFACE","").strip()
    results = enrich_with_min_bids(results, api_key)

    decisions = results["decisions"].copy()
    log(f"✅ Всего строк решений: {len(decisions)}")
    if not results.get("profit_state", pd.DataFrame()).empty:
        ps = results["profit_state"]
        log(f"💹 Фаза ВП: рост ВП={int((ps['Фаза ВП'] == 'Рост ВП').sum())}, тест роста заказов={int((ps['Фаза ВП'] == 'Тест роста заказов').sum())}")
    changed = decisions[(decisions["Действие"].isin(["Повысить","Снизить","Тест роста"])) & (decisions["Текущая ставка, ₽"] != decisions["Новая ставка, ₽"])].copy()
    log(f"🔁 Изменённых ставок: {len(changed)}")
    log(f"📊 Разбивка по действиям: {dict(decisions['Действие'].value_counts())}")
    if not changed.empty:
        print(changed[["Товар","Артикул продавца","Предмет","ID кампании","Плейсмент","Текущая ставка, ₽","Новая ставка, ₽","Действие","Причина"]].head(20).to_string(index=False), flush=True)

    bid_send_log = pd.DataFrame()
    shade_apply_log = pd.DataFrame()
    history_append = pd.DataFrame()

    apply_shades_flag = args.apply_shades if args.apply_shades is not None else (args.mode == "run")

    if args.mode == "run":
        payload = decisions_to_payload(decisions)
        bid_send_log = send_payload(payload, api_key, dry_run=not bool(api_key))
        log(f"📤 Отправлено блоков в WB: {len(payload.get('bids', []))}")
        history_append = build_history_append(changed, as_of_date)

        if apply_shades_flag:
            shade_apply_log, updated_shade_actions, tests_df = apply_shade_actions(results["shade_actions"], api_key, dry_run=not bool(api_key))
            results["shade_actions"] = updated_shade_actions
            results["shade_tests"] = tests_df
            log(f"🎨 Блоков оттенков к применению: {0 if shade_apply_log.empty else len(shade_apply_log)}")
        else:
            log("🎨 Применение оттенков отключено")
    else:
        log("🧪 Preview-режим: ставки не отправлялись")
        if apply_shades_flag:
            log("🧪 Preview: оттенки не применялись, только подготовлены")

    save_outputs(provider, results, args.mode, bid_send_log, shade_apply_log, history_append)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Стабильный менеджер ставок WB для TOPFACE")
    p.add_argument("mode", choices=["preview","run"], help="preview = только рекомендации, run = применить ставки")
    p.add_argument("--apply-shades", dest="apply_shades", action="store_true", default=None, help="Применить рекомендации по оттенкам через API")
    p.add_argument("--skip-shades", dest="apply_shades", action="store_false", help="Не применять рекомендации по оттенкам")
    p.add_argument("--local-data-dir", default="", help="Локальная папка с файлами")
    p.add_argument("--as-of-date", default="", help="Дата расчёта YYYY-MM-DD")
    return p

def main() -> None:
    args = build_parser().parse_args()
    run_manager(args)

if __name__ == "__main__":
    main()
