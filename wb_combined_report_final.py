#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import calendar
import io
import math
import os
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import boto3
import numpy as np
import pandas as pd
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter


# =========================================================
# Helpers
# =========================================================

def log(message: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {message}", flush=True)


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("\xa0", " ").strip()
    text = re.sub(r"\s+", " ", text)
    return text


def norm_key(value: Any) -> str:
    text = normalize_text(value).lower().replace("ё", "е")
    text = text.replace("%", " pct ")
    text = re.sub(r"[^\w]+", " ", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def clean_article(value: Any) -> str:
    text = normalize_text(value)
    if not text or text.lower() in {"nan", "none"}:
        return ""
    return text


EXCLUDED_ARTICLES = {
    "cz420",
    "cz420брови",
    "cz420глаза",
    "de49",
    "de49глаза",
    "pt901",
}


def is_excluded_article(article: Any) -> bool:
    return clean_article(article).lower() in EXCLUDED_ARTICLES


def extract_code(article: Any) -> str:
    text = clean_article(article)
    if not text:
        return ""
    low = text.lower()
    if low.startswith("pt901.f"):
        return "901"
    m = re.search(r"(\d{2,4})", low)
    if m:
        return str(int(m.group(1)))
    return text


def article_to_rrp_key(article: Any) -> str:
    text = clean_article(article)
    if not text:
        return ""
    low = text.lower().replace("_", "/")
    # PT-form already
    m = re.match(r"^pt(\d+)\.f(\d+)$", low)
    if m:
        return f"PT{int(m.group(1)):03d}.F{int(m.group(2)):02d}"
    m = re.match(r"^pt(\d+)\.(\d+)$", low)
    if m:
        return f"PT{int(m.group(1)):03d}.{int(m.group(2)):03d}"
    # slash form
    m = re.match(r"^(\d+)\/(\d+)$", low)
    if m:
        code = int(m.group(1))
        shade = int(m.group(2))
        if code == 901:
            return f"PT901.F{shade:02d}"
        return f"PT{code:03d}.{shade:03d}"
    # brush text pt901.f25-like with uppercase/lowercase noise
    m = re.match(r"^pt?(\d+)\.f(\d+)$", low)
    if m:
        code = int(m.group(1))
        shade = int(m.group(2))
        return f"PT{code:03d}.F{shade:02d}"
    m = re.match(r"^pt?(\d+)\.(\d+)$", low)
    if m:
        code = int(m.group(1))
        shade = int(m.group(2))
        return f"PT{code:03d}.{shade:03d}"
    # exact pt901 excluded but keep key blank
    return text.upper()


def to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def to_dt(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def safe_div(a: Any, b: Any) -> float:
    try:
        a = float(a)
        b = float(b)
    except Exception:
        return np.nan
    if b == 0 or math.isnan(b):
        return np.nan
    return a / b


def pct_delta(cur: Any, prev: Any) -> float:
    if pd.isna(prev) or prev == 0 or pd.isna(cur):
        return np.nan
    return (float(cur) - float(prev)) / float(prev)


def pp_delta(cur: Any, prev: Any) -> float:
    if pd.isna(prev) or pd.isna(cur):
        return np.nan
    return float(cur) - float(prev)


def week_code_from_date(dt_value: Any) -> Optional[str]:
    if pd.isna(dt_value):
        return None
    ts = pd.Timestamp(dt_value)
    iso = ts.isocalendar()
    return f"{int(iso.year)}-W{int(iso.week):02d}"


def week_bounds_from_code(week_code: str) -> Tuple[Optional[date], Optional[date]]:
    m = re.match(r"^(\d{4})-W(\d{2})$", str(week_code))
    if not m:
        return None, None
    year = int(m.group(1))
    week = int(m.group(2))
    start = date.fromisocalendar(year, week, 1)
    end = date.fromisocalendar(year, week, 7)
    return start, end


def parse_week_code_from_name(name: str) -> Optional[str]:
    m = re.search(r"(\d{4})-W(\d{2})", name)
    if not m:
        return None
    return f"{m.group(1)}-W{m.group(2)}"


def parse_abc_period_from_name(name: str) -> Tuple[Optional[date], Optional[date]]:
    m = re.search(r"__(\d{2})\.(\d{2})\.(\d{4})-(\d{2})\.(\d{2})\.(\d{4})__", name)
    if not m:
        return None, None
    start = date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    end = date(int(m.group(6)), int(m.group(5)), int(m.group(4)))
    return start, end


def parse_entry_period_from_name(name: str) -> Tuple[Optional[date], Optional[date]]:
    m = re.search(r"с (\d{2})-(\d{2})-(\d{4}) по (\d{2})-(\d{2})-(\d{4})", name)
    if not m:
        return None, None
    start = date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    end = date(int(m.group(6)), int(m.group(5)), int(m.group(4)))
    return start, end


def unique_preserve(items: Iterable[Any]) -> List[Any]:
    out: List[Any] = []
    seen = set()
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def dedupe_columns(cols: Iterable[Any]) -> List[str]:
    result: List[str] = []
    counts: Dict[str, int] = {}
    for c in cols:
        base = normalize_text(c) or "unnamed"
        counts[base] = counts.get(base, 0) + 1
        result.append(base if counts[base] == 1 else f"{base}__{counts[base]}")
    return result


def pick_best_sheet(sheet_names: List[str], preferred: Iterable[str]) -> Any:
    if not sheet_names:
        return 0
    norm_map = {norm_key(s): s for s in sheet_names}
    for name in preferred:
        k = norm_key(name)
        if k in norm_map:
            return norm_map[k]
    return sheet_names[0]


def read_excel_flexible(
    data: bytes,
    filename: str,
    preferred_sheets: Optional[Iterable[str]] = None,
    header_candidates: Iterable[int] = (0, 1, 2),
) -> Tuple[pd.DataFrame, str, int]:
    bio = io.BytesIO(data)
    xl = pd.ExcelFile(bio)
    sheet = pick_best_sheet(xl.sheet_names, preferred_sheets or [])
    best_df: Optional[pd.DataFrame] = None
    best_header = 0
    best_score = -10**9
    for header in header_candidates:
        try:
            df = xl.parse(sheet_name=sheet, header=header, dtype=object)
        except Exception:
            continue
        df = df.copy()
        df.columns = dedupe_columns(df.columns)
        df = df.dropna(axis=0, how="all").dropna(axis=1, how="all")
        score = -1000 if df.empty else len(df.columns)
        if score > best_score:
            best_score = score
            best_df = df
            best_header = header
    if best_df is None:
        raise ValueError(f"Не удалось прочитать Excel: {filename}")
    best_df.columns = dedupe_columns(best_df.columns)
    return best_df, str(sheet), best_header


def rename_using_aliases(df: pd.DataFrame, alias_map: Dict[str, List[str]]) -> pd.DataFrame:
    norm_existing = {norm_key(col): col for col in df.columns}
    out = df.copy()
    for target, aliases in alias_map.items():
        chosen = None
        for alias in aliases:
            k = norm_key(alias)
            if k in norm_existing:
                chosen = norm_existing[k]
                break
        if chosen is not None and chosen != target:
            out[target] = out[chosen]
        elif chosen is None and target not in out.columns:
            out[target] = np.nan
    return out


def weighted_mean(values: pd.Series, weights: pd.Series) -> float:
    v = to_numeric(values)
    w = to_numeric(weights)
    m = v.notna() & w.notna() & (w >= 0)
    if not m.any():
        return np.nan
    if w[m].sum() == 0:
        return v[m].mean()
    return np.average(v[m], weights=w[m])


def weighted_avg_position(values: pd.Series, weights: pd.Series) -> float:
    return weighted_mean(values, weights)


COMMON_ALIASES: Dict[str, List[str]] = {
    "day": ["Дата", "dt", "date", "Дата заказа", "Дата отчета", "Дата сбора"],
    "week": ["Неделя", "week"],
    "nm_id": ["Артикул WB", "Артикул ВБ", "nmID", "nmId"],
    "supplier_article": ["Артикул продавца", "supplierArticle", "Артикул WB продавца"],
    "brand": ["Бренд", "brand"],
    "subject": ["Предмет", "subject", "Название предмета"],
    "title": ["Название", "Название товара", "Товар"],
    "query": ["Поисковый запрос", "Запрос", "query", "Ключевое слово"],
    "frequency": ["Частота запросов", "Частота WB", "Частота", "frequency"],
    "median_position": ["Медианная позиция", "Средняя позиция", "Позиция"],
    "visibility_pct": ["Видимость", "Видимость, %"],
    "warehouse": ["Склад", "warehouseName"],
    "region": ["Регион", "regionName"],
    "stock_available": ["Доступно для продажи", "Остаток", "stock"],
    "stock_total": ["Полное количество"],
    "impressions": ["Показы", "impressions"],
    "clicks": ["Клики", "Клики в карточку", "Переходы в карточку", "Перешли в карточку", "clicks"],
    "ctr": ["CTR"],
    "cart": ["Добавили в корзину", "Добавления в корзину", "Добавлени в корзину", "addToCartCount"],
    "conv_cart": ["Конверсия в корзину", "addToCartConversion"],
    "orders": ["Заказы", "Заказали", "orders", "ordersCount", "Кол-во продаж"],
    "conv_order": ["Конверсия в заказ", "cartToOrderConversion", "CR", "Конверсия в заказ (из корзины), %"],
    "open_card_count": ["Открытие карточки", "openCardCount", "Открытия карточки"],
    "buyouts_count": ["buyoutsCount", "Выкупы заказов"],
    "cancel_count": ["cancelCount", "Отмена заказа"],
    "spend": ["Расход", "spend", "Продвижение"],
    "cpc": ["CPC"],
    "campaign_id": ["ID кампании"],
    "campaign_name": ["Название"],
    "status": ["Статус"],
    "payment_type": ["Тип оплаты"],
    "gross_profit": ["Валовая прибыль", "Чистая прибыль"],
    "gross_revenue": ["Валовая выручка"],
    "drr_pct": ["ДРР, %"],
    "margin_pct": ["Маржинальность, %", "Валовая рентабельность, %"],
    "profitability_pct": ["Рентабельность, %", "Чистая рентабельность, %"],
    "abc_class": ["ABC-анализ"],
    "section": ["Раздел"],
    "entry_point": ["Точка входа"],
    "finished_price": ["finishedPrice", "Ср. цена продажи", "Цена с учетом всех скидок, кроме суммы по WB Кошельку", "Средняя цена покупателя"],
    "price_with_disc": ["priceWithDisc", "Цена со скидкой продавца, в том числе со скидкой WB Клуба", "Средняя цена продажи"],
    "spp": ["SPP", "СПП", "Скидка WB, %", "СПП, %"],
}


# =========================================================
# Storage
# =========================================================

class BaseStorage:
    def list_files(self, prefix: str) -> List[str]:
        raise NotImplementedError

    def read_bytes(self, path: str) -> bytes:
        raise NotImplementedError

    def write_bytes(self, path: str, data: bytes) -> None:
        raise NotImplementedError

    def exists(self, path: str) -> bool:
        raise NotImplementedError


class LocalStorage(BaseStorage):
    def __init__(self, root: str):
        self.root = Path(root)

    def _abs(self, rel_path: str) -> Path:
        return self.root / rel_path

    def list_files(self, prefix: str) -> List[str]:
        prefix = prefix.replace("\\", "/").rstrip("/")
        prefix_path = self._abs(prefix)
        base = prefix_path if prefix_path.exists() else prefix_path.parent
        if not base.exists():
            return []
        files = []
        for p in base.rglob("*"):
            if p.is_file():
                rel = str(p.relative_to(self.root)).replace("\\", "/")
                if rel.startswith(prefix):
                    files.append(rel)
        return sorted(files)

    def glob_root(self, pattern: str) -> List[str]:
        return sorted(str(p.relative_to(self.root)).replace("\\", "/") for p in self.root.glob(pattern) if p.is_file())

    def read_bytes(self, path: str) -> bytes:
        return self._abs(path).read_bytes()

    def write_bytes(self, path: str, data: bytes) -> None:
        out = self._abs(path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(data)

    def exists(self, path: str) -> bool:
        return self._abs(path).exists()


class S3Storage(BaseStorage):
    def __init__(self, bucket: str, access_key: str, secret_key: str):
        self.bucket = bucket
        self.s3 = boto3.client(
            "s3",
            endpoint_url="https://storage.yandexcloud.net",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

    def list_files(self, prefix: str) -> List[str]:
        files = []
        token = None
        while True:
            kwargs = {"Bucket": self.bucket, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            resp = self.s3.list_objects_v2(**kwargs)
            for item in resp.get("Contents", []):
                key = item["Key"]
                if not key.endswith("/"):
                    files.append(key)
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        return sorted(files)

    def read_bytes(self, path: str) -> bytes:
        obj = self.s3.get_object(Bucket=self.bucket, Key=path)
        return obj["Body"].read()

    def write_bytes(self, path: str, data: bytes) -> None:
        self.s3.put_object(Bucket=self.bucket, Key=path, Body=data)

    def exists(self, path: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=path)
            return True
        except Exception:
            return False


def make_storage(root: str) -> BaseStorage:
    bucket = os.getenv("YC_BUCKET_NAME", "").strip()
    access_key = os.getenv("YC_ACCESS_KEY_ID", "").strip()
    secret_key = os.getenv("YC_SECRET_ACCESS_KEY", "").strip()
    if bucket and access_key and secret_key:
        log("Using Yandex Object Storage (S3)")
        return S3Storage(bucket=bucket, access_key=access_key, secret_key=secret_key)
    log("Using local filesystem")
    return LocalStorage(root=root)


# =========================================================
# Loader
# =========================================================

@dataclass
class LoadedData:
    orders: pd.DataFrame = field(default_factory=pd.DataFrame)
    stocks: pd.DataFrame = field(default_factory=pd.DataFrame)
    search: pd.DataFrame = field(default_factory=pd.DataFrame)
    funnel: pd.DataFrame = field(default_factory=pd.DataFrame)
    ads_daily: pd.DataFrame = field(default_factory=pd.DataFrame)
    ads_total: pd.DataFrame = field(default_factory=pd.DataFrame)
    campaigns: pd.DataFrame = field(default_factory=pd.DataFrame)
    economics: pd.DataFrame = field(default_factory=pd.DataFrame)
    abc: pd.DataFrame = field(default_factory=pd.DataFrame)
    entry_points_category: pd.DataFrame = field(default_factory=pd.DataFrame)
    entry_points_sku: pd.DataFrame = field(default_factory=pd.DataFrame)
    rrp: pd.DataFrame = field(default_factory=pd.DataFrame)
    warnings: List[str] = field(default_factory=list)


class DataLoader:
    def __init__(self, storage: BaseStorage, store: str, reports_root: str = "Отчёты"):
        self.storage = storage
        self.store = store
        self.reports_root = reports_root.rstrip("/")
        self.warnings: List[str] = []

    def _prefix(self, *parts: str) -> str:
        return "/".join([self.reports_root, *parts]).replace("//", "/")

    def _list_under(self, prefixes: Iterable[str]) -> List[str]:
        all_files: List[str] = []
        for prefix in prefixes:
            try:
                all_files.extend(self.storage.list_files(prefix))
            except Exception:
                pass
        return unique_preserve(sorted([f for f in all_files if f.lower().endswith(".xlsx")]))

    def _glob_root(self, patterns: Iterable[str]) -> List[str]:
        if not isinstance(self.storage, LocalStorage):
            return []
        files: List[str] = []
        for pattern in patterns:
            files.extend(self.storage.glob_root(pattern))
        return unique_preserve(sorted(files))

    def _read_df(self, path: str, preferred_sheets: Optional[Iterable[str]] = None, header_candidates: Iterable[int] = (0, 1, 2)) -> pd.DataFrame:
        raw, _sheet, _header = read_excel_flexible(self.storage.read_bytes(path), path, preferred_sheets, header_candidates)
        raw.columns = dedupe_columns(raw.columns)
        return rename_using_aliases(raw, COMMON_ALIASES)

    def _read_selected(self, path: str, sheet_name: Optional[str] = None, header: int = 0, usecols=None) -> pd.DataFrame:
        data = self.storage.read_bytes(path)
        bio = io.BytesIO(data)
        df = pd.read_excel(bio, sheet_name=sheet_name, header=header, dtype=object, usecols=usecols)
        df.columns = dedupe_columns(df.columns)
        return rename_using_aliases(df, COMMON_ALIASES)

    def _read_selected_fast(self, path: str, sheet_name: Optional[str] = None, header_row: int = 1, needed_headers: Optional[Iterable[str]] = None) -> pd.DataFrame:
        from openpyxl import load_workbook as _load_wb
        data = self.storage.read_bytes(path)
        wb = _load_wb(io.BytesIO(data), read_only=True, data_only=True)
        ws = wb[sheet_name] if isinstance(sheet_name, str) and sheet_name in wb.sheetnames else wb[wb.sheetnames[0] if sheet_name in [None, 0] else wb.sheetnames[int(sheet_name)]]
        needed = {norm_key(x) for x in (needed_headers or [])}
        rows = ws.iter_rows(values_only=True)
        header = None
        for _ in range(max(header_row - 1, 0)):
            next(rows, None)
        header = [normalize_text(x) for x in (next(rows, []) or [])]
        if not header:
            return pd.DataFrame()
        idxs = [i for i, h in enumerate(header) if norm_key(h) in needed] if needed else list(range(len(header)))
        cols = [header[i] for i in idxs]
        recs = []
        for r in rows:
            if r is None:
                continue
            rec = [r[i] if i < len(r) else None for i in idxs]
            if all(v is None or v == '' for v in rec):
                continue
            recs.append(rec)
        df = pd.DataFrame(recs, columns=cols)
        df.columns = dedupe_columns(df.columns)
        return rename_using_aliases(df, COMMON_ALIASES)

    def _finalize_common(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        if "nm_id" in df.columns:
            df["nm_id"] = to_numeric(df["nm_id"])
        if "supplier_article" in df.columns:
            df["supplier_article"] = df["supplier_article"].map(clean_article)
        if "subject" in df.columns:
            df["subject"] = df["subject"].map(normalize_text)
        if "brand" in df.columns:
            df["brand"] = df["brand"].map(normalize_text)
        if "title" in df.columns:
            df["title"] = df["title"].map(normalize_text)
        return df

    def load_orders(self) -> pd.DataFrame:
        files = self._list_under([
            self._prefix("Заказы", self.store, "Недельные"),
            self._prefix("Заказы", self.store),
        ])
        if not files:
            files = self._glob_root(["Заказы_*.xlsx"])
        dfs = []
        usecols = lambda c: norm_key(c) in {norm_key(x) for x in ["date", "Дата", "warehouseName", "Склад", "supplierArticle", "Артикул продавца", "nmId", "Артикул WB", "subject", "Предмет", "brand", "Бренд", "finishedPrice", "priceWithDisc", "spp", "isCancel"]}
        for path in files:
            try:
                try:
                    df = self._read_selected_fast(path, sheet_name=0, header_row=1, needed_headers=["date", "warehouseName", "supplierArticle", "nmId", "subject", "brand", "finishedPrice", "priceWithDisc", "spp"])
                except Exception:
                    try:
                        df = self._read_selected(path, sheet_name=0, header=0, usecols=usecols)
                    except Exception:
                        df = self._read_df(path, None, (0, 1, 2))
                week_code = parse_week_code_from_name(Path(path).name)
                start, end = week_bounds_from_code(week_code) if week_code else (None, None)
                df["week_code"] = week_code
                df["week_start"] = pd.Timestamp(start) if start else pd.NaT
                df["week_end"] = pd.Timestamp(end) if end else pd.NaT
                if "day" not in df.columns and "date" in df.columns:
                    df["day"] = df["date"]
                df["day"] = to_dt(df["day"]).dt.normalize()
                df = self._finalize_common(df)
                for c in ["finished_price", "price_with_disc", "spp"]:
                    df[c] = to_numeric(df.get(c, np.nan))
                df["orders"] = 1
                if "warehouse" not in df.columns and "warehouseName" in df.columns:
                    df["warehouse"] = df["warehouseName"]
                df["warehouse"] = df.get("warehouse", "").map(normalize_text)
                dfs.append(df)
            except Exception as e:
                self.warnings.append(f"Orders read error {path}: {e}")
        out = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        if not out.empty:
            out = out[out["day"].notna()].copy()
        return out

    def load_stocks(self) -> pd.DataFrame:
        files = self._list_under([self._prefix("Остатки", self.store, "Недельные")])
        if not files:
            files = self._glob_root(["Остатки_*.xlsx"])
        dfs = []
        usecols = lambda c: norm_key(c) in {norm_key(x) for x in ["Дата запроса", "Дата сбора", "Склад", "Артикул продавца", "Артикул WB", "Доступно для продажи", "Полное количество", "Предмет", "Бренд"]}
        for path in files:
            try:
                try:
                    df = self._read_selected_fast(path, sheet_name=0, header_row=1, needed_headers=["Дата запроса", "Склад", "Артикул продавца", "Артикул WB", "Доступно для продажи", "Полное количество", "Предмет", "Бренд"])
                except Exception:
                    try:
                        df = self._read_selected(path, sheet_name=0, header=0, usecols=usecols)
                    except Exception:
                        df = self._read_df(path, None, (0,))
                week_code = parse_week_code_from_name(Path(path).name)
                start, end = week_bounds_from_code(week_code) if week_code else (None, None)
                df["week_code"] = week_code
                df["week_start"] = pd.Timestamp(start) if start else pd.NaT
                df["week_end"] = pd.Timestamp(end) if end else pd.NaT
                df = self._finalize_common(df)
                df["warehouse"] = df.get("warehouse", "").map(normalize_text)
                df["stock_available"] = to_numeric(df.get("stock_available", 0)).fillna(0)
                df["stock_total"] = to_numeric(df.get("stock_total", np.nan)).fillna(df["stock_available"]).fillna(0)
                if "Дата запроса" in df.columns and "day" not in df.columns:
                    df["day"] = to_dt(df["Дата запроса"]).dt.normalize()
                dfs.append(df)
            except Exception as e:
                self.warnings.append(f"Stocks read error {path}: {e}")
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    def load_search(self) -> pd.DataFrame:
        files = self._list_under([self._prefix("Поисковые запросы", self.store, "Недельные")])
        if not files:
            files = self._glob_root(["Неделя *.xlsx"])
        dfs = []
        usecols = lambda c: norm_key(c) in {norm_key(x) for x in ["Дата", "Артикул WB", "Артикул продавца", "Предмет", "Бренд", "Поисковый запрос", "Частота запросов", "Медианная позиция", "Видимость"]}
        for path in files:
            try:
                try:
                    df = self._read_selected_fast(path, sheet_name=0, header_row=1, needed_headers=["Дата", "Артикул WB", "Артикул продавца", "Предмет", "Бренд", "Поисковый запрос", "Частота запросов", "Медианная позиция", "Видимость"])
                except Exception:
                    try:
                        df = self._read_selected(path, sheet_name=0, header=0, usecols=usecols)
                    except Exception:
                        df = self._read_df(path, None, (0,))
                week_code = parse_week_code_from_name(Path(path).name)
                start, end = week_bounds_from_code(week_code) if week_code else (None, None)
                df["week_code"] = week_code
                df["week_start"] = pd.Timestamp(start) if start else pd.NaT
                df["week_end"] = pd.Timestamp(end) if end else pd.NaT
                df["day"] = to_dt(df["day"]).dt.normalize()
                df = self._finalize_common(df)
                for c in ["frequency", "median_position", "visibility_pct"]:
                    df[c] = to_numeric(df.get(c, np.nan))
                dfs.append(df)
            except Exception as e:
                self.warnings.append(f"Search read error {path}: {e}")
        out = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        if not out.empty:
            out = out[out["day"].notna()].copy()
        return out

    def load_funnel(self) -> pd.DataFrame:
        candidates = [
            self._prefix("Воронка продаж", self.store, "Воронка продаж.xlsx"),
            self._prefix("Воронка продаж", "Воронка продаж.xlsx"),
            "Воронка продаж (1).xlsx",
            "Воронка продаж.xlsx",
        ]
        path = next((c for c in candidates if self.storage.exists(c)), None)
        if not path:
            files = self._glob_root(["Воронка продаж*.xlsx"])
            path = files[0] if files else None
        if not path:
            return pd.DataFrame()
        try:
            df = self._read_df(path, None, (0,))
            df["day"] = to_dt(df["day"]).dt.normalize()
            df = self._finalize_common(df)
            for c in ["open_card_count", "cart", "orders", "buyouts_count", "cancel_count", "conv_cart", "conv_order"]:
                df[c] = to_numeric(df.get(c, np.nan))
            return df[df["day"].notna()].copy()
        except Exception as e:
            self.warnings.append(f"Funnel read error {path}: {e}")
            return pd.DataFrame()

    def load_ads(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        files = self._list_under([
            self._prefix("Реклама", self.store, "Недельные"),
            self._prefix("Реклама", self.store),
        ])
        if not files:
            files = self._glob_root(["Анализ рекламы.xlsx", "Реклама_*.xlsx"])
        daily_dfs, total_dfs, camp_dfs = [], [], []
        for path in files:
            name = Path(path).name
            week_code = parse_week_code_from_name(name)
            start, end = week_bounds_from_code(week_code) if week_code else (None, None)
            for target, sheets, holder in [
                ("daily", ["Статистика_Ежедневно"], daily_dfs),
                ("total", ["Статистика_Итого"], total_dfs),
                ("camp", ["Список_кампаний"], camp_dfs),
            ]:
                try:
                    df = self._read_df(path, sheets, (0,))
                    df["week_code"] = week_code
                    df["week_start"] = pd.Timestamp(start) if start else pd.NaT
                    df["week_end"] = pd.Timestamp(end) if end else pd.NaT
                    if "day" in df.columns:
                        df["day"] = to_dt(df["day"]).dt.normalize()
                    df = self._finalize_common(df)
                    for c in ["impressions", "clicks", "ctr", "cpc", "orders", "conv_order", "spend"]:
                        df[c] = to_numeric(df.get(c, np.nan))
                    holder.append(df)
                except Exception:
                    continue
        return (
            pd.concat(daily_dfs, ignore_index=True) if daily_dfs else pd.DataFrame(),
            pd.concat(total_dfs, ignore_index=True) if total_dfs else pd.DataFrame(),
            pd.concat(camp_dfs, ignore_index=True) if camp_dfs else pd.DataFrame(),
        )

    def load_economics(self) -> pd.DataFrame:
        candidates = [
            self._prefix("Финансовые показатели", self.store, "Экономика.xlsx"),
            self._prefix("Финансовые показатели", self.store, "Недельные", "Экономика.xlsx"),
            "Экономика (2).xlsx",
            "Экономика.xlsx",
        ]
        path = next((c for c in candidates if self.storage.exists(c)), None)
        if not path:
            files = self._glob_root(["Экономика*.xlsx"])
            path = files[0] if files else None
        if not path:
            return pd.DataFrame()
        try:
            df = self._read_df(path, ["Юнит экономика"], (0, 1, 2))
            df = self._finalize_common(df)
            rename_map = {
                "Продажи, шт": "sales_qty",
                "Возвраты, шт": "returns_qty",
                "Чистые продажи, шт": "net_sales_qty",
                "Процент выкупа": "buyout_pct",
                "Средняя цена продажи": "econ_price_with_disc",
                "Средняя цена покупателя": "econ_finished_price",
                "Комиссия WB, руб/ед": "commission_unit",
                "Эквайринг, руб/ед": "acquiring_unit",
                "Логистика прямая, руб/ед": "logistics_direct_unit",
                "Логистика обратная, руб/ед": "logistics_return_unit",
                "Хранение, руб/ед": "storage_unit",
                "Приёмка, руб/ед": "acceptance_unit",
                "Штрафы и удержания, руб/ед": "penalties_unit",
                "Реклама, руб/ед": "ads_unit",
                "Прочие расходы, руб/ед": "other_unit",
                "Себестоимость, руб": "cost_unit",
                "НДС, руб/ед": "vat_unit",
                "Валовая прибыль, руб/ед": "gp_unit",
                "Чистая прибыль, руб/ед": "np_unit",
                "Валовая рентабельность, %": "margin_pct",
                "Чистая рентабельность, %": "profitability_pct",
            }
            for old, new in rename_map.items():
                if old in df.columns and new not in df.columns:
                    df[new] = df[old]
            num_cols = list(rename_map.values())
            for c in num_cols:
                df[c] = to_numeric(df.get(c, np.nan))
            return df
        except Exception as e:
            self.warnings.append(f"Economics read error {path}: {e}")
            return pd.DataFrame()

    def load_abc(self) -> pd.DataFrame:
        files = self._list_under([self._prefix("ABC")])
        files = [f for f in files if "wb_abc_report_goods__" in Path(f).name]
        if not files:
            files = self._glob_root(["wb_abc_report_goods__*.xlsx"])
        dfs = []
        for path in files:
            try:
                df = self._read_df(path, None, (0,))
                start, end = parse_abc_period_from_name(Path(path).name)
                df["week_start"] = pd.Timestamp(start) if start else pd.NaT
                df["week_end"] = pd.Timestamp(end) if end else pd.NaT
                df["week_code"] = week_code_from_date(start) if start else None
                df = self._finalize_common(df)
                if "Кол-во продаж" in df.columns and "abc_sales_qty" not in df.columns:
                    df["abc_sales_qty"] = df["Кол-во продаж"]
                elif "orders" in df.columns:
                    df["abc_sales_qty"] = df["orders"]
                else:
                    df["abc_sales_qty"] = np.nan
                extra_renames = {
                    "Эквайринг": "abc_acquiring",
                    "Комиссия": "abc_commission",
                    "Логистика": "abc_logistics",
                    "Платное хранение": "abc_storage",
                    "Платная приемка": "abc_acceptance",
                    "Продвижение": "abc_promotion",
                    "Доплаты": "abc_extra",
                    "Штрафы": "abc_fines",
                    "Себестоимость": "abc_cost",
                    "Налог": "abc_tax",
                    "Внешние расходы": "abc_external",
                    "Чистая прибыль": "abc_net_profit",
                    "Чистая прибыль на 1 товар": "abc_net_profit_unit",
                    "Сумма продаж": "abc_sales_sum",
                    "Заказы": "abc_orders_funnel",
                    "Открытие карточки": "abc_open_card",
                    "Добавлени в корзину": "abc_cart",
                    "Конверсия в корзину, %": "abc_conv_cart",
                    "Конверсия в заказ (из корзины), %": "abc_conv_order",
                    "Ср. цена продажи": "abc_finished_price",
                    "Процент выкупов, %": "abc_buyout_pct",
                }
                for old, new in extra_renames.items():
                    if old in df.columns:
                        df[new] = df[old]
                for c in [
                    "gross_profit", "gross_revenue", "abc_sales_qty", "drr_pct", "margin_pct", "profitability_pct",
                    "abc_acquiring", "abc_commission", "abc_logistics", "abc_storage", "abc_acceptance", "abc_promotion",
                    "abc_extra", "abc_fines", "abc_cost", "abc_tax", "abc_external", "abc_net_profit", "abc_net_profit_unit",
                    "abc_orders_funnel", "abc_open_card", "abc_cart", "abc_conv_cart", "abc_conv_order", "abc_finished_price", "abc_buyout_pct",
                ]:
                    df[c] = to_numeric(df.get(c, np.nan))
                dfs.append(df)
            except Exception as e:
                self.warnings.append(f"ABC read error {path}: {e}")
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    def load_entry_points(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        files = self._list_under([self._prefix("Точки входа", self.store)])
        if not files:
            files = self._glob_root(["*Портрет покупателя*.xlsx"])
        cat_dfs, sku_dfs = [], []
        for path in files:
            start, end = parse_entry_period_from_name(Path(path).name)
            week_code = week_code_from_date(start) if start else None
            for sheets, holder in [(["Детализация по точкам входа"], cat_dfs), (["Детализация по артикулам"], sku_dfs)]:
                try:
                    sheet_name = sheets[0] if sheets else 0
                    try:
                        df = self._read_selected_fast(path, sheet_name=sheet_name, header_row=2, needed_headers=["Раздел", "Точка входа", "Показы", "Переходы в карточку", "CTR", "Добавления в корзину", "Конверсия в корзину", "Заказы", "Конверсия в заказ", "Артикул ВБ", "Артикул продавца", "Бренд", "Название", "Предмет"])
                    except Exception:
                        df = self._read_df(path, sheets, (1,))
                    df["week_start"] = pd.Timestamp(start) if start else pd.NaT
                    df["week_end"] = pd.Timestamp(end) if end else pd.NaT
                    df["week_code"] = week_code
                    df = self._finalize_common(df)
                    for c in ["impressions", "clicks", "ctr", "cart", "conv_cart", "orders", "conv_order"]:
                        df[c] = to_numeric(df.get(c, np.nan))
                    df["section"] = df.get("section", "").map(normalize_text)
                    df["entry_point"] = df.get("entry_point", "").map(normalize_text)
                    holder.append(df)
                except Exception as e:
                    self.warnings.append(f"Entry points read error {path}: {e}")
        return (
            pd.concat(cat_dfs, ignore_index=True) if cat_dfs else pd.DataFrame(),
            pd.concat(sku_dfs, ignore_index=True) if sku_dfs else pd.DataFrame(),
        )

    def load_rrp(self) -> pd.DataFrame:
        candidates = [
            self._prefix("Финансовые показатели", self.store, "РРЦ.xlsx"),
            "РРЦ.xlsx",
        ]
        path = next((c for c in candidates if self.storage.exists(c)), None)
        if not path:
            files = self._glob_root(["РРЦ.xlsx"])
            path = files[0] if files else None
        if not path:
            return pd.DataFrame()
        try:
            raw, _, _ = read_excel_flexible(self.storage.read_bytes(path), path, preferred_sheets=["TF"], header_candidates=(0,))
            raw.columns = dedupe_columns(raw.columns)
            df = raw.copy()
            col_article = next((c for c in df.columns if norm_key(c) == norm_key("ПРАВИЛЬНЫЙ АРТИКУЛ")), None)
            col_rrp = next((c for c in df.columns if norm_key(c) == norm_key("РРЦ")), None)
            col_name = next((c for c in df.columns if norm_key(c) == norm_key("Наименование")), None)
            if col_article is None or col_rrp is None:
                return pd.DataFrame()
            out = pd.DataFrame({
                "rrp_key": df[col_article].map(lambda x: normalize_text(x).upper()),
                "rrp": to_numeric(df[col_rrp]),
                "rrp_name": df[col_name].map(normalize_text) if col_name else "",
            })
            out = out[out["rrp_key"] != ""].copy()
            out = out.drop_duplicates(subset=["rrp_key"], keep="first")
            return out
        except Exception as e:
            self.warnings.append(f"RRP read error {path}: {e}")
            return pd.DataFrame()

    def load_all(self) -> LoadedData:
        log("Loading orders")
        orders = self.load_orders()
        log("Loading stocks")
        stocks = self.load_stocks()
        log("Loading search")
        search = self.load_search()
        log("Loading funnel")
        funnel = self.load_funnel()
        log("Loading ads")
        ads_daily, ads_total, campaigns = self.load_ads()
        log("Loading economics")
        economics = self.load_economics()
        log("Loading ABC")
        abc = self.load_abc()
        log("Loading entry points")
        entry_cat, entry_sku = self.load_entry_points()
        log("Loading RRP")
        rrp = self.load_rrp()
        return LoadedData(
            orders=orders,
            stocks=stocks,
            search=search,
            funnel=funnel,
            ads_daily=ads_daily,
            ads_total=ads_total,
            campaigns=campaigns,
            economics=economics,
            abc=abc,
            entry_points_category=entry_cat,
            entry_points_sku=entry_sku,
            rrp=rrp,
            warnings=self.warnings,
        )


# =========================================================
# Analytics
# =========================================================

class Part2Analyzer:
    def __init__(self, data: LoadedData):
        self.data = data
        self.master = self.build_master()
        # windows must exist before any daily builders call _period_name()
        self.windows = self.determine_windows()
        self.daily_article = self.build_daily_article()
        self.demand_daily_subject = self.build_subject_demand_daily()
        self.localization_daily = self.build_localization_daily()
        self.article_period = self.build_article_period_metrics()
        self.article_compare = self.build_compare(self.article_period, ["supplier_article", "nm_id", "code", "subject", "brand", "title"])
        self.product_compare = self.aggregate_compare(self.article_period, level="product")
        self.category_compare = self.aggregate_compare(self.article_period, level="category")
        self.channel_compare = self.build_channel_compare()
        self.sku_contribution = self.build_sku_contribution()
        self.price_monitor = self.build_price_monitor()
        self.example_901_5 = self.build_example_901_5()

    # ---------- Master ----------
    def build_master(self) -> pd.DataFrame:
        frames = []
        base_cols = ["nm_id", "supplier_article", "subject", "brand", "title"]
        for df in [self.data.orders, self.data.search, self.data.stocks, self.data.abc, self.data.economics, self.data.entry_points_sku]:
            if df.empty:
                continue
            x = df[[c for c in base_cols if c in df.columns]].copy()
            for c in base_cols:
                if c not in x.columns:
                    x[c] = np.nan
            frames.append(x[base_cols])
        if not frames:
            return pd.DataFrame(columns=base_cols + ["code", "rrp_key"])
        master = pd.concat(frames, ignore_index=True)
        master["nm_id"] = to_numeric(master["nm_id"])
        master["supplier_article"] = master["supplier_article"].map(clean_article)
        master = master[master["supplier_article"] != ""].copy()
        master = master[~master["supplier_article"].map(is_excluded_article)].copy()
        master["code"] = master["supplier_article"].map(extract_code)
        master["rrp_key"] = master["supplier_article"].map(article_to_rrp_key)
        master["quality"] = (
            master["subject"].map(lambda x: 1 if clean_article(x) else 0) * 4
            + master["title"].map(lambda x: 1 if clean_article(x) else 0) * 2
            + master["brand"].map(lambda x: 1 if clean_article(x) else 0)
        )
        master = master.sort_values("quality", ascending=False)
        master = master.drop_duplicates(subset=["supplier_article"], keep="first")
        if not self.data.rrp.empty:
            master = master.merge(self.data.rrp, on="rrp_key", how="left")
        return master.drop(columns=["quality"])

    def attach_master(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or self.master.empty:
            return df.copy()
        out = df.copy()
        if "supplier_article" in out.columns:
            out = out.merge(self.master[["supplier_article", "nm_id", "subject", "brand", "title", "code", "rrp_key", "rrp"]], on="supplier_article", how="left", suffixes=("", "_m"))
            for c in ["nm_id", "subject", "brand", "title", "code", "rrp_key", "rrp"]:
                if f"{c}_m" in out.columns:
                    if c not in out.columns:
                        out[c] = out[f"{c}_m"]
                    else:
                        cond = out[c].isna() | (out[c] == "")
                        out.loc[cond, c] = out.loc[cond, f"{c}_m"]
                    out = out.drop(columns=[f"{c}_m"])
        if "nm_id" in out.columns:
            nm_map = self.master.dropna(subset=["nm_id"]).drop_duplicates(subset=["nm_id"])[["nm_id", "supplier_article", "subject", "brand", "title", "code", "rrp_key", "rrp"]]
            out = out.merge(nm_map, on="nm_id", how="left", suffixes=("", "_n"))
            for c in ["supplier_article", "subject", "brand", "title", "code", "rrp_key", "rrp"]:
                if f"{c}_n" in out.columns:
                    if c not in out.columns:
                        out[c] = out[f"{c}_n"]
                    else:
                        cond = out[c].isna() | (out[c] == "")
                        out.loc[cond, c] = out.loc[cond, f"{c}_n"]
                    out = out.drop(columns=[f"{c}_n"])
        if "supplier_article" in out.columns:
            out = out[~out["supplier_article"].map(is_excluded_article)].copy()
            out["code"] = out["supplier_article"].map(extract_code)
            out["rrp_key"] = out["supplier_article"].map(article_to_rrp_key)
        return out

    # ---------- Windows ----------
    def determine_windows(self) -> Dict[str, pd.Timestamp]:
        candidates = []
        for df, col in [
            (self.data.funnel, "day"),
            (self.data.orders, "day"),
            (self.data.search, "day"),
        ]:
            if not df.empty and col in df.columns:
                candidates.append(pd.to_datetime(df[col], errors="coerce").max())
        last_day = max([c for c in candidates if pd.notna(c)]) if candidates else pd.Timestamp.today().normalize()
        cur_end = pd.Timestamp(last_day).normalize()
        cur_start = cur_end - pd.Timedelta(days=13)
        prev_end = cur_start - pd.Timedelta(days=1)
        prev_start = prev_end - pd.Timedelta(days=13)
        return {
            "cur_start": cur_start,
            "cur_end": cur_end,
            "prev_start": prev_start,
            "prev_end": prev_end,
        }

    def _period_name(self, day: pd.Timestamp) -> Optional[str]:
        if pd.isna(day):
            return None
        windows = getattr(self, "windows", None)
        if not windows:
            windows = self.determine_windows()
            self.windows = windows
        if windows["cur_start"] <= day <= windows["cur_end"]:
            return "cur_14d"
        if windows["prev_start"] <= day <= windows["prev_end"]:
            return "prev_14d"
        return None

    # ---------- Daily article operations ----------
    def build_daily_article(self) -> pd.DataFrame:
        parts = []
        if not self.data.funnel.empty:
            f = self.data.funnel.copy()
            f = f[f["day"].notna()].copy()
            grp = f.groupby(["day", "nm_id"], dropna=False).agg(
                open_card_count=("open_card_count", "sum"),
                cart_count=("cart", "sum"),
                orders_funnel=("orders", "sum"),
                buyouts_funnel=("buyouts_count", "sum"),
                cancel_funnel=("cancel_count", "sum"),
                conv_to_cart=("conv_cart", "mean"),
                conv_cart_to_order=("conv_order", "mean"),
            ).reset_index()
            parts.append(grp)
        if parts:
            cur = parts[0]
        else:
            cur = pd.DataFrame(columns=["day", "nm_id"])

        if not self.data.orders.empty:
            o = self.data.orders.copy()
            o = o[o["day"].notna()].copy()
            grp = o.groupby(["day", "supplier_article", "nm_id"], dropna=False).apply(
                lambda g: pd.Series({
                    "orders_from_orders": to_numeric(g["orders"]).fillna(1).sum(),
                    "finishedPrice_avg": weighted_mean(g["finished_price"], g["orders"].fillna(1)),
                    "priceWithDisc_avg": weighted_mean(g["price_with_disc"], g["orders"].fillna(1)),
                    "spp_avg": weighted_mean(g["spp"], g["orders"].fillna(1)),
                })
            ).reset_index()
            cur = cur.merge(grp, on=[c for c in ["day", "supplier_article", "nm_id"] if c in cur.columns and c in grp.columns], how="outer") if not cur.empty else grp

        if not self.data.search.empty:
            s = self.data.search.copy()
            s = s[s["day"].notna()].copy()
            grp = s.groupby(["day", "supplier_article", "nm_id"], dropna=False).apply(
                lambda g: pd.Series({
                    "search_frequency": to_numeric(g["frequency"]).sum(),
                    "median_position": weighted_avg_position(g["median_position"], g["frequency"].fillna(1)),
                    "visibility_pct": weighted_mean(g["visibility_pct"], g["frequency"].fillna(1)),
                    "search_queries_count": g["query"].nunique(),
                })
            ).reset_index()
            cur = cur.merge(grp, on=[c for c in ["day", "supplier_article", "nm_id"] if c in cur.columns and c in grp.columns], how="outer") if not cur.empty else grp

        if not self.data.ads_daily.empty:
            a = self.data.ads_daily.copy()
            a = a[a["day"].notna()].copy()
            grp = a.groupby(["day", "nm_id", "supplier_article"], dropna=False).apply(
                lambda g: pd.Series({
                    "ad_impressions": to_numeric(g["impressions"]).sum(),
                    "ad_clicks": to_numeric(g["clicks"]).sum(),
                    "ad_orders": to_numeric(g["orders"]).sum(),
                    "ad_spend": to_numeric(g["spend"]).sum(),
                    "ad_ctr": weighted_mean(g["ctr"], g["impressions"].fillna(1)),
                    "ad_cpc": weighted_mean(g["cpc"], g["clicks"].fillna(1)),
                })
            ).reset_index()
            cur = cur.merge(grp, on=[c for c in ["day", "supplier_article", "nm_id"] if c in cur.columns and c in grp.columns], how="outer") if not cur.empty else grp

        cur = self.attach_master(cur)
        cur = cur[~cur["supplier_article"].map(is_excluded_article)].copy()
        for c in [
            "open_card_count", "cart_count", "orders_funnel", "buyouts_funnel", "cancel_funnel",
            "orders_from_orders", "search_frequency", "search_queries_count", "ad_impressions", "ad_clicks", "ad_orders", "ad_spend",
        ]:
            cur[c] = to_numeric(cur.get(c, 0)).fillna(0)
        for c in ["conv_to_cart", "conv_cart_to_order", "finishedPrice_avg", "priceWithDisc_avg", "spp_avg", "median_position", "visibility_pct", "ad_ctr", "ad_cpc"]:
            cur[c] = to_numeric(cur.get(c, np.nan))
        cur["period_name"] = cur["day"].map(self._period_name)
        cur = cur[cur["period_name"].notna()].copy()
        return cur.sort_values(["day", "subject", "supplier_article"])

    # ---------- Demand ----------
    def build_subject_demand_daily(self) -> pd.DataFrame:
        if self.data.search.empty:
            return pd.DataFrame(columns=["day", "subject", "demand_day"])
        s = self.data.search.copy()
        s = s[s["day"].notna()].copy()
        # unique query per day per subject: max frequency to avoid duplicates across sku
        uq = s.groupby(["day", "subject", "query"], dropna=False)["frequency"].max().reset_index()
        out = uq.groupby(["day", "subject"], dropna=False)["frequency"].sum().reset_index(name="demand_day")
        out["period_name"] = out["day"].map(self._period_name)
        return out[out["period_name"].notna()].copy()

    # ---------- Localization ----------
    def build_localization_daily(self) -> pd.DataFrame:
        if self.data.orders.empty or self.data.stocks.empty:
            return pd.DataFrame()
        latest_day = self.windows["cur_end"]
        lookback_start = latest_day - pd.Timedelta(days=27)

        orders = self.data.orders.copy()
        orders = orders[(orders["day"] >= lookback_start) & (orders["day"] <= latest_day)].copy()
        orders = orders[~orders["supplier_article"].map(is_excluded_article)].copy()
        orders["orders"] = to_numeric(orders["orders"]).fillna(1)
        wh_avg = orders.groupby(["supplier_article", "warehouse"], dropna=False)["orders"].sum().reset_index(name="orders_28d")
        wh_avg["avg_orders_per_day_warehouse"] = wh_avg["orders_28d"] / 28.0

        latest_stock = self.data.stocks.copy()
        latest_stock = latest_stock[~latest_stock["supplier_article"].map(is_excluded_article)].copy()
        # Main warehouses from latest snapshot inside current window, fallback latest overall
        if "day" in latest_stock.columns and latest_stock["day"].notna().any():
            last_snapshot_day = pd.to_datetime(latest_stock["day"], errors="coerce").max()
            snap = latest_stock[pd.to_datetime(latest_stock["day"], errors="coerce") == last_snapshot_day].copy()
        else:
            max_week = latest_stock["week_end"].max()
            snap = latest_stock[latest_stock["week_end"] == max_week].copy()
        main_wh_rows = []
        for art, g in snap.groupby("supplier_article"):
            g = g.groupby("warehouse", dropna=False)["stock_available"].sum().reset_index()
            total = g["stock_available"].sum()
            if total <= 0:
                continue
            g = g.sort_values("stock_available", ascending=False)
            g["share"] = g["stock_available"] / total
            g["cum"] = g["share"].cumsum()
            cutoff = g.index[g["cum"] >= 0.97]
            end_idx = cutoff[0] if len(cutoff) else g.index[-1]
            kept = g.loc[:end_idx].copy()
            # weights from orders, fallback stock share
            ords = wh_avg[wh_avg["supplier_article"] == art][["warehouse", "orders_28d", "avg_orders_per_day_warehouse"]]
            kept = kept.merge(ords, on="warehouse", how="left")
            if kept["orders_28d"].fillna(0).sum() > 0:
                kept["warehouse_weight"] = kept["orders_28d"].fillna(0) / kept["orders_28d"].fillna(0).sum()
            else:
                kept["warehouse_weight"] = kept["share"]
            kept["supplier_article"] = art
            main_wh_rows.append(kept[["supplier_article", "warehouse", "share", "warehouse_weight", "avg_orders_per_day_warehouse"]])
        main_wh = pd.concat(main_wh_rows, ignore_index=True) if main_wh_rows else pd.DataFrame(columns=["supplier_article", "warehouse", "share", "warehouse_weight", "avg_orders_per_day_warehouse"])

        # expand weekly stock snapshots to days
        expanded = []
        for _, row in latest_stock.iterrows():
            art = row.get("supplier_article", "")
            if is_excluded_article(art):
                continue
            week_code = row.get("week_code")
            ws, we = week_bounds_from_code(week_code) if week_code else (None, None)
            if ws is None:
                if pd.notna(row.get("week_start")) and pd.notna(row.get("week_end")):
                    ws = pd.Timestamp(row["week_start"]).date()
                    we = pd.Timestamp(row["week_end"]).date()
                elif pd.notna(row.get("day")):
                    d = pd.Timestamp(row["day"]).normalize()
                    ws = d.date()
                    we = d.date()
            if ws is None:
                continue
            d = pd.Timestamp(ws)
            end_d = pd.Timestamp(we)
            while d <= end_d:
                if self.windows["prev_start"] <= d <= self.windows["cur_end"]:
                    expanded.append({
                        "day": d,
                        "supplier_article": art,
                        "warehouse": normalize_text(row.get("warehouse")),
                        "stock_qty": float(row.get("stock_available", 0) or 0),
                    })
                d += pd.Timedelta(days=1)
        if not expanded:
            return pd.DataFrame()
        exp = pd.DataFrame(expanded)
        exp = exp.merge(main_wh, on=["supplier_article", "warehouse"], how="inner")
        if exp.empty:
            return pd.DataFrame()
        exp["avg_orders_per_day_warehouse"] = to_numeric(exp["avg_orders_per_day_warehouse"]).fillna(0)
        exp["coverage_days"] = exp.apply(lambda r: safe_div(r["stock_qty"], r["avg_orders_per_day_warehouse"]) if r["avg_orders_per_day_warehouse"] > 0 else (999999 if r["stock_qty"] > 0 else 0), axis=1)
        exp["is_available_flag"] = np.where(
            exp["avg_orders_per_day_warehouse"] <= 0,
            np.where(exp["stock_qty"] > 0, 1, 0),
            np.where(exp["stock_qty"] >= exp["avg_orders_per_day_warehouse"], 1, 0),
        )
        exp["period_name"] = exp["day"].map(self._period_name)
        exp = exp[exp["period_name"].notna()].copy()
        return exp.sort_values(["supplier_article", "day", "warehouse"])

    def build_localization_period(self) -> pd.DataFrame:
        if self.localization_daily.empty:
            return pd.DataFrame()
        g = self.localization_daily.groupby(["supplier_article", "period_name"], dropna=False).apply(
            lambda x: pd.Series({
                "localization_coverage_count": x.groupby(["day"])["is_available_flag"].mean().mean(),
                "localization_coverage_weighted": x.groupby(["day"]).apply(lambda d: np.average(d["is_available_flag"], weights=d["warehouse_weight"])) .mean(),
                "main_warehouses_count": x["warehouse"].nunique(),
            })
        ).reset_index()
        return self.attach_master(g)

    # ---------- Period metrics ----------
    def build_article_period_metrics(self) -> pd.DataFrame:
        rows = []
        periods = ["prev_14d", "cur_14d"]

        # Daily ops aggregation
        ops = self.daily_article.copy()
        ops_period = ops.groupby(["supplier_article", "period_name"], dropna=False).apply(
            lambda g: pd.Series({
                "open_card_count": g["open_card_count"].sum(),
                "cart_count": g["cart_count"].sum(),
                "orders_funnel": g["orders_funnel"].sum(),
                "buyouts_funnel": g["buyouts_funnel"].sum(),
                "cancel_funnel": g["cancel_funnel"].sum(),
                "conv_to_cart": safe_div(g["cart_count"].sum(), g["open_card_count"].sum()) * 100 if g["open_card_count"].sum() else np.nan,
                "conv_cart_to_order": safe_div(g["orders_funnel"].sum(), g["cart_count"].sum()) * 100 if g["cart_count"].sum() else np.nan,
                "finishedPrice_avg": weighted_mean(g["finishedPrice_avg"], g["orders_from_orders"].replace(0, 1)),
                "priceWithDisc_avg": weighted_mean(g["priceWithDisc_avg"], g["orders_from_orders"].replace(0, 1)),
                "spp_avg": weighted_mean(g["spp_avg"], g["orders_from_orders"].replace(0, 1)),
                "search_frequency_article": g["search_frequency"].sum(),
                "median_position_article": weighted_avg_position(g["median_position"], g["search_frequency"].replace(0, 1)),
                "visibility_pct_article": weighted_mean(g["visibility_pct"], g["search_frequency"].replace(0, 1)),
                "search_queries_count": g["search_queries_count"].sum(),
                "ad_impressions": g["ad_impressions"].sum(),
                "ad_clicks": g["ad_clicks"].sum(),
                "ad_orders": g["ad_orders"].sum(),
                "ad_spend": g["ad_spend"].sum(),
                "ad_ctr": safe_div(g["ad_clicks"].sum(), g["ad_impressions"].sum()) * 100 if g["ad_impressions"].sum() else np.nan,
                "ad_cpc": safe_div(g["ad_spend"].sum(), g["ad_clicks"].sum()) if g["ad_clicks"].sum() else np.nan,
            })
        ).reset_index()

        # ABC fact
        abc = self.data.abc.copy()
        if not abc.empty:
            abc["period_name"] = abc["week_end"].map(lambda d: self._period_name(pd.Timestamp(d)) if pd.notna(d) else None)
            abc = abc[abc["period_name"].isin(periods)].copy()
            abc_period = abc.groupby(["supplier_article", "period_name"], dropna=False).apply(
                lambda g: pd.Series({
                    "abc_revenue": to_numeric(g["gross_revenue"]).sum(),
                    "abc_gp": to_numeric(g["gross_profit"]).sum(),
                    "abc_sales_qty": to_numeric(g["abc_sales_qty"]).sum(),
                    "drr_pct": weighted_mean(g["drr_pct"], g["gross_revenue"].replace(0, np.nan)),
                    "margin_pct": weighted_mean(g["margin_pct"], g["gross_revenue"].replace(0, np.nan)),
                    "profitability_pct": weighted_mean(g["profitability_pct"], g["gross_revenue"].replace(0, np.nan)),
                    "abc_finished_price": weighted_mean(g["abc_finished_price"], g["abc_sales_qty"].replace(0, 1)),
                    "abc_open_card": to_numeric(g.get("abc_open_card", pd.Series())).sum() if "abc_open_card" in g.columns else np.nan,
                    "abc_cart": to_numeric(g.get("abc_cart", pd.Series())).sum() if "abc_cart" in g.columns else np.nan,
                    "abc_orders_funnel": to_numeric(g.get("abc_orders_funnel", pd.Series())).sum() if "abc_orders_funnel" in g.columns else np.nan,
                })
            ).reset_index()
        else:
            abc_period = pd.DataFrame(columns=["supplier_article", "period_name"])

        # Economics weighted by net_sales_qty
        econ = self.data.economics.copy()
        if not econ.empty:
            week_to_period = {}
            for week_code in econ["week"].dropna().astype(str).unique():
                ws, we = week_bounds_from_code(week_code)
                if we is None:
                    continue
                week_to_period[week_code] = self._period_name(pd.Timestamp(we))
            econ["period_name"] = econ["week"].astype(str).map(week_to_period)
            econ = econ[econ["period_name"].isin(periods)].copy()
            econ["weight"] = to_numeric(econ.get("net_sales_qty", np.nan)).fillna(to_numeric(econ.get("sales_qty", np.nan))).fillna(0)
            econ_period = econ.groupby(["supplier_article", "period_name"], dropna=False).apply(
                lambda g: pd.Series({
                    "econ_sales_qty": to_numeric(g["sales_qty"]).sum(),
                    "econ_net_sales_qty": to_numeric(g["net_sales_qty"]).sum(),
                    "buyout_pct": weighted_mean(g["buyout_pct"], g["weight"].replace(0, 1)),
                    "econ_priceWithDisc": weighted_mean(g["econ_price_with_disc"], g["weight"].replace(0, 1)),
                    "econ_finishedPrice": weighted_mean(g["econ_finished_price"], g["weight"].replace(0, 1)),
                    "econ_spp": weighted_mean(g["spp"], g["weight"].replace(0, 1)),
                    "commission_unit": weighted_mean(g["commission_unit"], g["weight"].replace(0, 1)),
                    "acquiring_unit": weighted_mean(g["acquiring_unit"], g["weight"].replace(0, 1)),
                    "logistics_direct_unit": weighted_mean(g["logistics_direct_unit"], g["weight"].replace(0, 1)),
                    "logistics_return_unit": weighted_mean(g["logistics_return_unit"], g["weight"].replace(0, 1)),
                    "storage_unit": weighted_mean(g["storage_unit"], g["weight"].replace(0, 1)),
                    "acceptance_unit": weighted_mean(g["acceptance_unit"], g["weight"].replace(0, 1)),
                    "penalties_unit": weighted_mean(g["penalties_unit"], g["weight"].replace(0, 1)),
                    "ads_unit": weighted_mean(g["ads_unit"], g["weight"].replace(0, 1)),
                    "other_unit": weighted_mean(g["other_unit"], g["weight"].replace(0, 1)),
                    "cost_unit": weighted_mean(g["cost_unit"], g["weight"].replace(0, 1)),
                    "gp_unit": weighted_mean(g["gp_unit"], g["weight"].replace(0, 1)),
                    "np_unit": weighted_mean(g["np_unit"], g["weight"].replace(0, 1)),
                    "econ_margin_pct": weighted_mean(g["margin_pct"], g["weight"].replace(0, 1)),
                    "econ_profitability_pct": weighted_mean(g["profitability_pct"], g["weight"].replace(0, 1)),
                })
            ).reset_index()
        else:
            econ_period = pd.DataFrame(columns=["supplier_article", "period_name"])

        # Article clicks from entry points sku weekly
        entry = self.data.entry_points_sku.copy()
        if not entry.empty:
            entry["period_name"] = entry["week_end"].map(lambda d: self._period_name(pd.Timestamp(d)) if pd.notna(d) else None)
            entry = entry[entry["period_name"].isin(periods)].copy()
            entry_period = entry.groupby(["supplier_article", "period_name"], dropna=False).apply(
                lambda g: pd.Series({
                    "impressions_total": to_numeric(g["impressions"]).sum(),
                    "clicks_total": to_numeric(g["clicks"]).sum(),
                    "entry_cart": to_numeric(g["cart"]).sum(),
                    "entry_orders": to_numeric(g["orders"]).sum(),
                    "ctr_total": safe_div(to_numeric(g["clicks"]).sum(), to_numeric(g["impressions"]).sum()) * 100 if to_numeric(g["impressions"]).sum() else np.nan,
                    "cr_click_to_order": safe_div(to_numeric(g["orders"]).sum(), to_numeric(g["clicks"]).sum()) * 100 if to_numeric(g["clicks"]).sum() else np.nan,
                    "entry_conv_cart": safe_div(to_numeric(g["cart"]).sum(), to_numeric(g["clicks"]).sum()) * 100 if to_numeric(g["clicks"]).sum() else np.nan,
                    "entry_conv_order": safe_div(to_numeric(g["orders"]).sum(), to_numeric(g["cart"]).sum()) * 100 if to_numeric(g["cart"]).sum() else np.nan,
                })
            ).reset_index()
        else:
            entry_period = pd.DataFrame(columns=["supplier_article", "period_name"])

        # Localization
        localization_period = self.build_localization_period()

        # Category demand map
        demand = self.demand_daily_subject.copy()
        if not demand.empty:
            demand_period = demand.groupby(["subject", "period_name"], dropna=False)["demand_day"].sum().reset_index(name="category_demand")
        else:
            demand_period = pd.DataFrame(columns=["subject", "period_name", "category_demand"])

        # merge all by article-period
        keys = unique_preserve(
            list(ops_period[["supplier_article", "period_name"]].drop_duplicates().itertuples(index=False, name=None)) +
            list(abc_period[["supplier_article", "period_name"]].drop_duplicates().itertuples(index=False, name=None)) +
            list(econ_period[["supplier_article", "period_name"]].drop_duplicates().itertuples(index=False, name=None)) +
            list(entry_period[["supplier_article", "period_name"]].drop_duplicates().itertuples(index=False, name=None)) +
            list(localization_period[["supplier_article", "period_name"]].drop_duplicates().itertuples(index=False, name=None))
        )
        base = pd.DataFrame(keys, columns=["supplier_article", "period_name"])
        for x in [ops_period, abc_period, econ_period, entry_period, localization_period]:
            if not x.empty:
                base = base.merge(x, on=["supplier_article", "period_name"], how="left")
        base = self.attach_master(base)
        base = base.merge(demand_period, on=["subject", "period_name"], how="left")
        # prices vs RRP
        base["finishedPrice_rrp_coeff"] = base.apply(lambda r: safe_div(r.get("finishedPrice_avg"), r.get("rrp")), axis=1)
        base["priceWithDisc_rrp_coeff"] = base.apply(lambda r: safe_div(r.get("priceWithDisc_avg"), r.get("rrp")), axis=1)
        base = base[~base["supplier_article"].map(is_excluded_article)].copy()
        return base.sort_values(["subject", "code", "supplier_article", "period_name"])

    # ---------- Compare ----------
    def build_compare(self, df: pd.DataFrame, id_cols: List[str]) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()
        metric_cols = [c for c in df.columns if c not in set(id_cols + ["period_name"]) ]
        cur = df[df["period_name"] == "cur_14d"].copy().drop(columns=["period_name"])
        prev = df[df["period_name"] == "prev_14d"].copy().drop(columns=["period_name"])
        cur = cur.rename(columns={c: f"{c}_cur" for c in metric_cols})
        prev = prev.rename(columns={c: f"{c}_prev" for c in metric_cols})
        out = prev.merge(cur, on=id_cols, how="outer")

        # top level deltas
        delta_pairs_pct = [
            "abc_revenue", "abc_gp", "abc_sales_qty", "gp_unit", "open_card_count", "clicks_total", "ctr_total", "cr_click_to_order",
            "finishedPrice_avg", "priceWithDisc_avg", "spp_avg", "category_demand", "median_position_article", "visibility_pct_article",
            "ad_spend", "ad_clicks", "ad_orders", "localization_coverage_weighted", "drr_pct", "margin_pct", "profitability_pct",
        ]
        for m in delta_pairs_pct:
            if f"{m}_cur" in out.columns or f"{m}_prev" in out.columns:
                out[f"{m}_delta_pct"] = out.apply(lambda r: pct_delta(r.get(f"{m}_cur"), r.get(f"{m}_prev")), axis=1)
                out[f"{m}_delta_abs"] = out.apply(lambda r: (r.get(f"{m}_cur") - r.get(f"{m}_prev")) if pd.notna(r.get(f"{m}_cur")) and pd.notna(r.get(f"{m}_prev")) else np.nan, axis=1)
        out["gp_per_order_prev"] = out.apply(lambda r: safe_div(r.get("abc_gp_prev"), r.get("abc_sales_qty_prev")), axis=1)
        out["gp_per_order_cur"] = out.apply(lambda r: safe_div(r.get("abc_gp_cur"), r.get("abc_sales_qty_cur")), axis=1)
        out["gp_per_order_delta_pct"] = out.apply(lambda r: pct_delta(r.get("gp_per_order_cur"), r.get("gp_per_order_prev")), axis=1)
        out["revenue_per_order_prev"] = out.apply(lambda r: safe_div(r.get("abc_revenue_prev"), r.get("abc_sales_qty_prev")), axis=1)
        out["revenue_per_order_cur"] = out.apply(lambda r: safe_div(r.get("abc_revenue_cur"), r.get("abc_sales_qty_cur")), axis=1)
        out["revenue_per_order_delta_pct"] = out.apply(lambda r: pct_delta(r.get("revenue_per_order_cur"), r.get("revenue_per_order_prev")), axis=1)

        # contributions
        out["gp_volume_effect"] = (out["abc_sales_qty_cur"].fillna(0) - out["abc_sales_qty_prev"].fillna(0)) * out["gp_per_order_prev"].fillna(0)
        out["gp_economy_effect"] = (out["gp_per_order_cur"].fillna(0) - out["gp_per_order_prev"].fillna(0)) * out["abc_sales_qty_cur"].fillna(0)
        out["orders_traffic_effect"] = (out["clicks_total_cur"].fillna(0) - out["clicks_total_prev"].fillna(0)) * out["cr_click_to_order_prev"].fillna(0) / 100.0
        out["orders_conversion_effect"] = (out["cr_click_to_order_cur"].fillna(0) - out["cr_click_to_order_prev"].fillna(0)) * out["clicks_total_cur"].fillna(0) / 100.0
        out["revenue_order_effect"] = (out["abc_sales_qty_cur"].fillna(0) - out["abc_sales_qty_prev"].fillna(0)) * out["revenue_per_order_prev"].fillna(0)
        out["revenue_price_effect"] = (out["revenue_per_order_cur"].fillna(0) - out["revenue_per_order_prev"].fillna(0)) * out["abc_sales_qty_cur"].fillna(0)

        # flags & reasons
        out = self.add_flags_and_reasons(out)
        return out

    def add_flags_and_reasons(self, out: pd.DataFrame) -> pd.DataFrame:
        def flag(col, cond):
            out[col] = cond.astype(int)
        flag("flag_orders_down", out["abc_sales_qty_delta_pct"] <= -0.08)
        flag("flag_orders_up", out["abc_sales_qty_delta_pct"] >= 0.08)
        flag("flag_clicks_down", out["clicks_total_delta_pct"] <= -0.08)
        flag("flag_clicks_up", out["clicks_total_delta_pct"] >= 0.08)
        flag("flag_ctr_down", out["ctr_total_delta_pct"] <= -0.05)
        flag("flag_ctr_up", out["ctr_total_delta_pct"] >= 0.05)
        flag("flag_conversion_down", out["cr_click_to_order_delta_pct"] <= -0.08)
        flag("flag_conversion_up", out["cr_click_to_order_delta_pct"] >= 0.08)
        flag("flag_market_demand_down", out["category_demand_delta_pct"] <= -0.08)
        flag("flag_market_demand_up", out["category_demand_delta_pct"] >= 0.08)
        flag("flag_visibility_down", (out["visibility_pct_article_delta_pct"] <= -0.08) | (out["median_position_article_delta_abs"] >= 1.0))
        flag("flag_visibility_up", (out["visibility_pct_article_delta_pct"] >= 0.08) | (out["median_position_article_delta_abs"] <= -1.0))
        flag("flag_finished_price_up", out["finishedPrice_avg_delta_pct"] >= 0.03)
        flag("flag_finished_price_down", out["finishedPrice_avg_delta_pct"] <= -0.03)
        flag("flag_pwd_up", out["priceWithDisc_avg_delta_pct"] >= 0.02)
        flag("flag_pwd_down", out["priceWithDisc_avg_delta_pct"] <= -0.02)
        flag("flag_spp_up", out["spp_avg_delta_abs"] >= 2.0)
        flag("flag_spp_down", out["spp_avg_delta_abs"] <= -2.0)
        flag("flag_stock_constraint", out["localization_coverage_weighted_delta_abs"] <= -0.15)
        flag("flag_gp_per_order_down", out["gp_per_order_delta_pct"] <= -0.05)
        flag("flag_gp_per_order_up", out["gp_per_order_delta_pct"] >= 0.05)
        flag("flag_margin_down", out["margin_pct_delta_abs"] <= -2.0)
        flag("flag_margin_up", out["margin_pct_delta_abs"] >= 2.0)
        flag("flag_logistics_up", ((out["logistics_direct_unit_cur"].fillna(0) + out["logistics_return_unit_cur"].fillna(0)) - (out["logistics_direct_unit_prev"].fillna(0) + out["logistics_return_unit_prev"].fillna(0))) >= 0.10 * (out["logistics_direct_unit_prev"].fillna(0) + out["logistics_return_unit_prev"].fillna(0)).replace(0, np.nan))
        flag("flag_commission_up", (out["commission_unit_delta_abs"] >= 0.05 * out["commission_unit_prev"].replace(0, np.nan)))
        flag("flag_ads_unit_up", out["ads_unit_delta_abs"] >= 0.10 * out["ads_unit_prev"].replace(0, np.nan))
        flag("flag_ad_spend_up", out["ad_spend_delta_pct"] >= 0.15)
        flag("flag_ad_traffic_no_result", (out["ad_spend_delta_pct"] >= 0.15) & (out["ad_clicks_delta_pct"] >= 0.08) & (out["abc_sales_qty_delta_pct"] < 0.05))
        flag("flag_ad_growth_effective", (out["ad_spend_delta_pct"] >= 0.15) & (out["ad_clicks_delta_pct"] >= 0.08) & (out["abc_sales_qty_delta_pct"] >= 0.08) & (out["abc_gp_delta_pct"] >= 0.05))
        flag("flag_ad_destructive", (out["ad_spend_delta_pct"] >= 0.15) & (out["abc_gp_delta_pct"] < 0))
        flag("flag_price_cut_unprofitable", (out["priceWithDisc_avg_delta_pct"] <= -0.02) & (out["abc_sales_qty_delta_pct"] < 0.08) & (out["abc_gp_delta_pct"] < -0.05))
        flag("flag_price_cut_profitable", (out["priceWithDisc_avg_delta_pct"] <= -0.02) & (out["abc_sales_qty_delta_pct"] >= 0.12) & (out["abc_gp_delta_pct"] > 0))
        flag("flag_price_hike_profitable", (out["priceWithDisc_avg_delta_pct"] >= 0.02) & (out["abc_sales_qty_delta_pct"] > -0.05) & (out["abc_gp_delta_pct"] > 0))
        flag("flag_price_hike_harmful", (out["priceWithDisc_avg_delta_pct"] >= 0.02) & (out["abc_sales_qty_delta_pct"] < -0.08) & (out["abc_gp_delta_pct"] < 0))

        def ad_assessment(r: pd.Series) -> str:
            if r.get("flag_ad_growth_effective", 0):
                return "Эффективно"
            if r.get("flag_ad_destructive", 0):
                return "Неэффективно"
            if r.get("flag_ad_traffic_no_result", 0):
                return "Частично эффективно"
            if pd.notna(r.get("ad_spend_delta_pct")) and r.get("ad_spend_delta_pct") > 0 and (r.get("flag_visibility_up", 0) or (pd.notna(r.get("visibility_pct_article_delta_abs")) and r.get("visibility_pct_article_delta_abs") >= 0)):
                return "Защитно"
            return "Нейтрально"

        def price_assessment(r: pd.Series) -> str:
            if r.get("flag_price_cut_unprofitable", 0):
                return "Снижение priceWithDisc не оправдано"
            if r.get("flag_price_cut_profitable", 0):
                return "Снижение priceWithDisc оправдано"
            if r.get("flag_price_hike_profitable", 0):
                return "Повышение priceWithDisc оправдано"
            if r.get("flag_price_hike_harmful", 0):
                return "Повышение priceWithDisc вредно"
            if r.get("flag_finished_price_up", 0) and (r.get("flag_ctr_down", 0) or r.get("flag_conversion_down", 0)):
                return "finishedPrice давит на конверсию"
            if r.get("flag_finished_price_down", 0) and not (r.get("flag_orders_up", 0) or r.get("flag_conversion_up", 0)):
                return "Снижение finishedPrice не дало заметного эффекта"
            return "Нейтрально"

        def choose_reasons(r: pd.Series) -> Tuple[str, str]:
            volume_driven = abs(r.get("gp_volume_effect", 0) or 0) >= abs(r.get("gp_economy_effect", 0) or 0)
            primary = "Нейтрально"
            secondary = ""
            if volume_driven:
                if r.get("flag_stock_constraint", 0):
                    primary = "Ограничение локализации"
                elif r.get("flag_market_demand_down", 0):
                    primary = "Снижение рыночного спроса"
                elif r.get("flag_visibility_down", 0) and r.get("flag_clicks_down", 0):
                    primary = "Потеря поисковой доли"
                elif r.get("flag_ctr_down", 0) and r.get("flag_clicks_down", 0):
                    primary = "Снижение CTR"
                elif r.get("flag_conversion_down", 0):
                    primary = "Снижение конверсии"
                elif r.get("flag_finished_price_up", 0):
                    primary = "Ценовой фактор для покупателя"
                elif r.get("flag_spp_down", 0) or r.get("flag_spp_up", 0):
                    primary = "Изменение SPP"
                else:
                    primary = "Изменение объема заказов"
                if r.get("flag_ad_destructive", 0) or r.get("flag_ad_traffic_no_result", 0):
                    secondary = "Реклама"
                elif r.get("flag_finished_price_up", 0) or r.get("flag_finished_price_down", 0):
                    secondary = "Цена для покупателя"
                elif r.get("flag_stock_constraint", 0):
                    secondary = "Локализация"
            else:
                if r.get("flag_price_cut_unprofitable", 0) or r.get("flag_price_hike_harmful", 0):
                    primary = "Неудачное изменение priceWithDisc"
                elif r.get("flag_ads_unit_up", 0):
                    primary = "Реклама съела прибыль"
                elif r.get("flag_logistics_up", 0):
                    primary = "Рост логистики"
                elif r.get("flag_commission_up", 0):
                    primary = "Рост комиссии"
                elif r.get("flag_margin_down", 0):
                    primary = "Снижение рентабельности"
                elif r.get("flag_gp_per_order_down", 0):
                    primary = "Снижение прибыли на единицу"
                else:
                    primary = "Изменение unit economics"
                if r.get("flag_finished_price_up", 0) or r.get("flag_pwd_up", 0) or r.get("flag_pwd_down", 0):
                    secondary = "Цена"
                elif r.get("flag_ad_destructive", 0):
                    secondary = "Реклама"
            return primary, secondary

        out["ad_assessment"] = out.apply(ad_assessment, axis=1)
        out["price_assessment"] = out.apply(price_assessment, axis=1)
        reasons = out.apply(choose_reasons, axis=1)
        out["primary_reason"] = [x[0] for x in reasons]
        out["secondary_reason"] = [x[1] for x in reasons]
        return out

    def aggregate_compare(self, period_df: pd.DataFrame, level: str) -> pd.DataFrame:
        if period_df.empty:
            return pd.DataFrame()
        if level == "product":
            group_cols = ["code", "subject"]
        elif level == "category":
            group_cols = ["subject"]
        else:
            raise ValueError(level)

        sum_cols = [
            "abc_revenue", "abc_gp", "abc_sales_qty", "open_card_count", "cart_count", "orders_funnel", "buyouts_funnel", "cancel_funnel",
            "search_frequency_article", "search_queries_count", "ad_impressions", "ad_clicks", "ad_orders", "ad_spend",
            "impressions_total", "clicks_total", "entry_cart", "entry_orders",
        ]
        weight_cols = {
            "finishedPrice_avg": "abc_sales_qty",
            "priceWithDisc_avg": "abc_sales_qty",
            "spp_avg": "abc_sales_qty",
            "finishedPrice_rrp_coeff": "abc_sales_qty",
            "priceWithDisc_rrp_coeff": "abc_sales_qty",
            "median_position_article": "search_frequency_article",
            "visibility_pct_article": "search_frequency_article",
            "category_demand": "abc_sales_qty",
            "localization_coverage_count": "abc_sales_qty",
            "localization_coverage_weighted": "abc_sales_qty",
            "drr_pct": "abc_revenue",
            "margin_pct": "abc_revenue",
            "profitability_pct": "abc_revenue",
            "commission_unit": "abc_sales_qty",
            "acquiring_unit": "abc_sales_qty",
            "logistics_direct_unit": "abc_sales_qty",
            "logistics_return_unit": "abc_sales_qty",
            "storage_unit": "abc_sales_qty",
            "acceptance_unit": "abc_sales_qty",
            "penalties_unit": "abc_sales_qty",
            "ads_unit": "abc_sales_qty",
            "other_unit": "abc_sales_qty",
            "cost_unit": "abc_sales_qty",
            "gp_unit": "abc_sales_qty",
            "np_unit": "abc_sales_qty",
            "econ_margin_pct": "abc_sales_qty",
            "econ_profitability_pct": "abc_sales_qty",
            "ctr_total": "impressions_total",
            "cr_click_to_order": "clicks_total",
            "entry_conv_cart": "clicks_total",
            "entry_conv_order": "entry_cart",
            "ad_ctr": "ad_impressions",
            "ad_cpc": "ad_clicks",
        }
        rows = []
        for keys, g in period_df.groupby(group_cols + ["period_name"], dropna=False):
            if not isinstance(keys, tuple):
                keys = (keys,)
            row = dict(zip(group_cols + ["period_name"], keys))
            for c in sum_cols:
                row[c] = to_numeric(g.get(c, pd.Series(dtype=float))).sum()
            for c, w in weight_cols.items():
                row[c] = weighted_mean(g.get(c, pd.Series(dtype=float)), g.get(w, pd.Series(dtype=float)).replace(0, 1))
            rows.append(row)
        agg = pd.DataFrame(rows)
        return self.build_compare(agg, group_cols)

    # ---------- Channels ----------
    def build_channel_compare(self) -> pd.DataFrame:
        rows = []
        periods = ["prev_14d", "cur_14d"]
        if not self.data.entry_points_sku.empty:
            x = self.data.entry_points_sku.copy()
            x["period_name"] = x["week_end"].map(lambda d: self._period_name(pd.Timestamp(d)) if pd.notna(d) else None)
            x = x[x["period_name"].isin(periods)].copy()
            x = self.attach_master(x)
            # article channels
            for (article, entry_point, period_name), g in x.groupby(["supplier_article", "entry_point", "period_name"], dropna=False):
                rows.append({
                    "entity_level": "article",
                    "entity_id": article,
                    "entry_point": entry_point,
                    "period_name": period_name,
                    "impressions": to_numeric(g["impressions"]).sum(),
                    "clicks": to_numeric(g["clicks"]).sum(),
                    "orders": to_numeric(g["orders"]).sum(),
                    "ctr": safe_div(to_numeric(g["clicks"]).sum(), to_numeric(g["impressions"]).sum()) * 100 if to_numeric(g["impressions"]).sum() else np.nan,
                    "conv_order": safe_div(to_numeric(g["orders"]).sum(), to_numeric(g["clicks"]).sum()) * 100 if to_numeric(g["clicks"]).sum() else np.nan,
                })
            # product channels
            for (code, entry_point, period_name), g in x.groupby(["code", "entry_point", "period_name"], dropna=False):
                rows.append({
                    "entity_level": "product",
                    "entity_id": code,
                    "entry_point": entry_point,
                    "period_name": period_name,
                    "impressions": to_numeric(g["impressions"]).sum(),
                    "clicks": to_numeric(g["clicks"]).sum(),
                    "orders": to_numeric(g["orders"]).sum(),
                    "ctr": safe_div(to_numeric(g["clicks"]).sum(), to_numeric(g["impressions"]).sum()) * 100 if to_numeric(g["impressions"]).sum() else np.nan,
                    "conv_order": safe_div(to_numeric(g["orders"]).sum(), to_numeric(g["clicks"]).sum()) * 100 if to_numeric(g["clicks"]).sum() else np.nan,
                })
        if not self.data.entry_points_category.empty:
            x = self.data.entry_points_category.copy()
            x["period_name"] = x["week_end"].map(lambda d: self._period_name(pd.Timestamp(d)) if pd.notna(d) else None)
            x = x[x["period_name"].isin(periods)].copy()
            for (entry_point, period_name), g in x.groupby(["entry_point", "period_name"], dropna=False):
                rows.append({
                    "entity_level": "category_total",
                    "entity_id": "ALL",
                    "entry_point": entry_point,
                    "period_name": period_name,
                    "impressions": to_numeric(g["impressions"]).sum(),
                    "clicks": to_numeric(g["clicks"]).sum(),
                    "orders": to_numeric(g["orders"]).sum(),
                    "ctr": safe_div(to_numeric(g["clicks"]).sum(), to_numeric(g["impressions"]).sum()) * 100 if to_numeric(g["impressions"]).sum() else np.nan,
                    "conv_order": safe_div(to_numeric(g["orders"]).sum(), to_numeric(g["clicks"]).sum()) * 100 if to_numeric(g["clicks"]).sum() else np.nan,
                })
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        cur = df[df["period_name"] == "cur_14d"].drop(columns=["period_name"]).rename(columns={c: f"{c}_cur" for c in ["impressions", "clicks", "orders", "ctr", "conv_order"]})
        prev = df[df["period_name"] == "prev_14d"].drop(columns=["period_name"]).rename(columns={c: f"{c}_prev" for c in ["impressions", "clicks", "orders", "ctr", "conv_order"]})
        out = prev.merge(cur, on=["entity_level", "entity_id", "entry_point"], how="outer")
        for m in ["impressions", "clicks", "orders", "ctr", "conv_order"]:
            out[f"{m}_delta_pct"] = out.apply(lambda r: pct_delta(r.get(f"{m}_cur"), r.get(f"{m}_prev")), axis=1)
            out[f"{m}_delta_abs"] = out.apply(lambda r: (r.get(f"{m}_cur") - r.get(f"{m}_prev")) if pd.notna(r.get(f"{m}_cur")) and pd.notna(r.get(f"{m}_prev")) else np.nan, axis=1)
        # contribution to order delta within entity
        contrib = []
        for (lvl, eid), g in out.groupby(["entity_level", "entity_id"], dropna=False):
            total_delta = to_numeric(g["orders_delta_abs"]).sum()
            gg = g.copy()
            gg["orders_delta_contribution"] = gg["orders_delta_abs"] / total_delta if total_delta not in [0, np.nan] else np.nan
            contrib.append(gg)
        return pd.concat(contrib, ignore_index=True) if contrib else out

    # ---------- SKU contribution ----------
    def build_sku_contribution(self) -> pd.DataFrame:
        if self.article_compare.empty:
            return pd.DataFrame()
        x = self.article_compare.copy()
        x["sku_gp_delta"] = x["abc_gp_cur"].fillna(0) - x["abc_gp_prev"].fillna(0)
        x["sku_revenue_delta"] = x["abc_revenue_cur"].fillna(0) - x["abc_revenue_prev"].fillna(0)
        x["sku_orders_delta"] = x["abc_sales_qty_cur"].fillna(0) - x["abc_sales_qty_prev"].fillna(0)
        product_delta = x.groupby("code")["sku_gp_delta"].sum().reset_index(name="product_gp_delta")
        x = x.merge(product_delta, on="code", how="left")
        x["sku_contribution_to_product_gp"] = x.apply(lambda r: safe_div(r["sku_gp_delta"], r["product_gp_delta"]), axis=1)
        return x.sort_values(["code", "sku_gp_delta"], ascending=[True, False])

    # ---------- Price monitor ----------
    def build_price_monitor(self) -> pd.DataFrame:
        if self.article_compare.empty:
            return pd.DataFrame()
        cols = [
            "supplier_article", "code", "subject", "rrp",
            "finishedPrice_avg_prev", "finishedPrice_avg_cur", "finishedPrice_avg_delta_pct",
            "priceWithDisc_avg_prev", "priceWithDisc_avg_cur", "priceWithDisc_avg_delta_pct",
            "finishedPrice_rrp_coeff_prev", "finishedPrice_rrp_coeff_cur",
            "priceWithDisc_rrp_coeff_prev", "priceWithDisc_rrp_coeff_cur",
            "spp_avg_prev", "spp_avg_cur", "spp_avg_delta_abs",
            "abc_sales_qty_prev", "abc_sales_qty_cur", "abc_sales_qty_delta_pct",
            "abc_gp_prev", "abc_gp_cur", "abc_gp_delta_pct",
            "margin_pct_prev", "margin_pct_cur", "margin_pct_delta_abs",
            "price_assessment",
        ]
        keep = [c for c in cols if c in self.article_compare.columns]
        x = self.article_compare[keep].copy()
        return x.sort_values("abc_gp_cur", ascending=False)

    # ---------- Example ----------
    def build_example_901_5(self) -> Dict[str, pd.DataFrame]:
        art = "901/5"
        res: Dict[str, pd.DataFrame] = {}
        if not self.article_compare.empty:
            x = self.article_compare[self.article_compare["supplier_article"].str.lower() == art.lower()].copy()
            if not x.empty:
                cols = [
                    "supplier_article", "primary_reason", "secondary_reason", "ad_assessment", "price_assessment",
                    "abc_revenue_prev", "abc_revenue_cur", "abc_revenue_delta_pct",
                    "abc_gp_prev", "abc_gp_cur", "abc_gp_delta_pct",
                    "abc_sales_qty_prev", "abc_sales_qty_cur", "abc_sales_qty_delta_pct",
                    "gp_per_order_prev", "gp_per_order_cur", "gp_per_order_delta_pct",
                    "gp_volume_effect", "gp_economy_effect",
                    "open_card_count_prev", "open_card_count_cur", "open_card_count_delta_pct",
                    "clicks_total_prev", "clicks_total_cur", "clicks_total_delta_pct",
                    "ctr_total_prev", "ctr_total_cur", "ctr_total_delta_pct",
                    "cr_click_to_order_prev", "cr_click_to_order_cur", "cr_click_to_order_delta_pct",
                    "category_demand_prev", "category_demand_cur", "category_demand_delta_pct",
                    "median_position_article_prev", "median_position_article_cur", "visibility_pct_article_prev", "visibility_pct_article_cur",
                    "finishedPrice_avg_prev", "finishedPrice_avg_cur", "priceWithDisc_avg_prev", "priceWithDisc_avg_cur",
                    "rrp", "finishedPrice_rrp_coeff_prev", "finishedPrice_rrp_coeff_cur", "priceWithDisc_rrp_coeff_prev", "priceWithDisc_rrp_coeff_cur",
                    "spp_avg_prev", "spp_avg_cur", "spp_avg_delta_abs",
                    "ad_spend_prev", "ad_spend_cur", "ad_clicks_prev", "ad_clicks_cur", "ad_orders_prev", "ad_orders_cur",
                    "localization_coverage_weighted_prev", "localization_coverage_weighted_cur",
                    "commission_unit_prev", "commission_unit_cur", "logistics_direct_unit_prev", "logistics_direct_unit_cur", "logistics_return_unit_prev", "logistics_return_unit_cur",
                    "ads_unit_prev", "ads_unit_cur", "cost_unit_prev", "cost_unit_cur", "margin_pct_prev", "margin_pct_cur", "profitability_pct_prev", "profitability_pct_cur",
                ]
                res["summary"] = x[[c for c in cols if c in x.columns]].copy()
        if not self.localization_daily.empty:
            x = self.localization_daily[self.localization_daily["supplier_article"].str.lower() == art.lower()].copy()
            res["localization"] = x.sort_values(["day", "warehouse"], ascending=[False, True])
        if not self.channel_compare.empty:
            x = self.channel_compare[(self.channel_compare["entity_level"] == "article") & (self.channel_compare["entity_id"].astype(str).str.lower() == art.lower())].copy()
            res["channels"] = x.sort_values("orders_delta_abs", ascending=False)
        if not self.daily_article.empty:
            x = self.daily_article[self.daily_article["supplier_article"].str.lower() == art.lower()].copy()
            res["daily_ops"] = x.sort_values("day", ascending=False)
        return res


# =========================================================
# Writer
# =========================================================

MONEY_FILL = PatternFill("solid", fgColor="F2F7FF")
HEADER_FILL = PatternFill("solid", fgColor="D9EAF7")
TITLE_FILL = PatternFill("solid", fgColor="BFD7EA")
THIN = Side(style="thin", color="C0C0C0")
BORDER = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)


class ExcelReport:
    def __init__(self):
        self.wb = Workbook()
        self.wb.remove(self.wb.active)

    def add_df(self, name: str, df: pd.DataFrame) -> None:
        ws = self.wb.create_sheet(self.safe_title(name))
        if df is None or df.empty:
            ws["A1"] = "Нет данных"
            return
        x = df.copy().replace([np.inf, -np.inf], np.nan)
        x = x.where(pd.notna(x), "")
        headers = list(x.columns)
        for j, h in enumerate(headers, 1):
            c = ws.cell(1, j, h)
            c.fill = HEADER_FILL
            c.font = Font(bold=True)
            c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            c.border = BORDER
        for i, row in enumerate(x.itertuples(index=False), 2):
            for j, v in enumerate(row, 1):
                c = ws.cell(i, j, v)
                c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
                c.border = BORDER
        self._format_sheet(ws, headers)

    def add_example(self, name: str, blocks: Dict[str, pd.DataFrame]) -> None:
        ws = self.wb.create_sheet(self.safe_title(name))
        if not blocks:
            ws["A1"] = "Нет данных"
            return
        row = 1
        for title, df in blocks.items():
            ws.cell(row, 1, title).fill = TITLE_FILL
            ws.cell(row, 1).font = Font(bold=True)
            row += 1
            if df is None or df.empty:
                ws.cell(row, 1, "Нет данных")
                row += 2
                continue
            headers = list(df.columns)
            for j, h in enumerate(headers, 1):
                c = ws.cell(row, j, h)
                c.fill = HEADER_FILL
                c.font = Font(bold=True)
                c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
                c.border = BORDER
            row += 1
            for _, rec in df.iterrows():
                for j, h in enumerate(headers, 1):
                    c = ws.cell(row, j, rec[h] if pd.notna(rec[h]) else "")
                    c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
                    c.border = BORDER
                row += 1
            row += 2
        self._format_sheet(ws, None)

    def _format_sheet(self, ws, headers: Optional[List[str]]) -> None:
        ws.freeze_panes = "A2"
        for col_cells in ws.columns:
            col_letter = get_column_letter(col_cells[0].column)
            max_len = 10
            for c in col_cells[:300]:
                max_len = max(max_len, len(normalize_text(c.value)))
            ws.column_dimensions[col_letter].width = min(max_len + 3, 28)
        # formats
        if headers:
            header_map = {h: idx + 1 for idx, h in enumerate(headers)}
            for h, idx in header_map.items():
                h_norm = norm_key(h)
                for row in range(2, ws.max_row + 1):
                    cell = ws.cell(row, idx)
                    if any(k in h_norm for k in ["pct", "конверсия", "visibility", "ctr", "drr", "margin", "profitability", "coverage", "coeff", "spp", "delta pct"]):
                        cell.number_format = '0.00'
                    elif any(k in h_norm for k in ["price", "revenue", "profit", "spend", "commission", "logistics", "cost", "orders delta contribution", "effect", "rrp"]):
                        cell.number_format = '#,##0 ₽'
                    elif "дата" in h_norm or h_norm == "day":
                        cell.number_format = 'DD.MM.YYYY'
                    elif any(k in h_norm for k in ["qty", "count", "orders", "clicks", "impressions"]):
                        cell.number_format = '0'
        for row in ws.iter_rows():
            for c in row:
                c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
                if c.row > 1:
                    c.border = BORDER

    @staticmethod
    def safe_title(name: str) -> str:
        bad = r'[]:*?/\\'
        cleaned = ''.join('_' if ch in bad else ch for ch in name)
        return cleaned[:31] or 'Sheet'

    def save_bytes(self) -> bytes:
        bio = io.BytesIO()
        self.wb.save(bio)
        return bio.getvalue()


# =========================================================
# Runner
# =========================================================

def build_output_workbook(an: Part2Analyzer) -> Workbook:
    rep = ExcelReport()
    summary_rows = []
    for title, comp in [("Результаты_категории", an.category_compare), ("Результаты_товары", an.product_compare), ("Результаты_артикулы", an.article_compare)]:
        if comp.empty:
            rep.add_df(title, pd.DataFrame())
            continue
        x = comp.copy()
        if title == "Результаты_категории":
            keep = [
                "subject", "abc_gp_prev", "abc_gp_cur", "abc_gp_delta_pct", "abc_revenue_prev", "abc_revenue_cur", "abc_revenue_delta_pct",
                "abc_sales_qty_prev", "abc_sales_qty_cur", "abc_sales_qty_delta_pct", "gp_volume_effect", "gp_economy_effect",
                "category_demand_prev", "category_demand_cur", "category_demand_delta_pct",
                "clicks_total_prev", "clicks_total_cur", "clicks_total_delta_pct",
                "ctr_total_prev", "ctr_total_cur", "ctr_total_delta_pct",
                "cr_click_to_order_prev", "cr_click_to_order_cur", "cr_click_to_order_delta_pct",
                "finishedPrice_avg_prev", "finishedPrice_avg_cur", "priceWithDisc_avg_prev", "priceWithDisc_avg_cur",
                "spp_avg_prev", "spp_avg_cur", "localization_coverage_weighted_prev", "localization_coverage_weighted_cur",
                "ad_spend_prev", "ad_spend_cur", "primary_reason", "secondary_reason", "ad_assessment", "price_assessment",
            ]
            x = x[[c for c in keep if c in x.columns]].sort_values("abc_gp_cur", ascending=False)
        elif title == "Результаты_товары":
            keep = [
                "code", "subject", "abc_gp_prev", "abc_gp_cur", "abc_gp_delta_pct", "abc_revenue_prev", "abc_revenue_cur", "abc_revenue_delta_pct",
                "abc_sales_qty_prev", "abc_sales_qty_cur", "abc_sales_qty_delta_pct", "gp_volume_effect", "gp_economy_effect",
                "category_demand_delta_pct", "clicks_total_delta_pct", "ctr_total_delta_pct", "cr_click_to_order_delta_pct",
                "finishedPrice_avg_prev", "finishedPrice_avg_cur", "priceWithDisc_avg_prev", "priceWithDisc_avg_cur", "spp_avg_prev", "spp_avg_cur",
                "localization_coverage_weighted_prev", "localization_coverage_weighted_cur", "ad_spend_prev", "ad_spend_cur",
                "primary_reason", "secondary_reason", "ad_assessment", "price_assessment",
            ]
            x = x[[c for c in keep if c in x.columns]].sort_values("abc_gp_cur", ascending=False)
        else:
            keep = [
                "supplier_article", "code", "subject", "abc_gp_prev", "abc_gp_cur", "abc_gp_delta_pct", "abc_revenue_prev", "abc_revenue_cur", "abc_revenue_delta_pct",
                "abc_sales_qty_prev", "abc_sales_qty_cur", "abc_sales_qty_delta_pct", "gp_per_order_prev", "gp_per_order_cur", "gp_per_order_delta_pct",
                "gp_volume_effect", "gp_economy_effect", "clicks_total_delta_pct", "ctr_total_delta_pct", "cr_click_to_order_delta_pct",
                "category_demand_delta_pct", "median_position_article_prev", "median_position_article_cur", "visibility_pct_article_prev", "visibility_pct_article_cur",
                "finishedPrice_avg_prev", "finishedPrice_avg_cur", "priceWithDisc_avg_prev", "priceWithDisc_avg_cur", "rrp",
                "finishedPrice_rrp_coeff_prev", "finishedPrice_rrp_coeff_cur", "priceWithDisc_rrp_coeff_prev", "priceWithDisc_rrp_coeff_cur",
                "spp_avg_prev", "spp_avg_cur", "localization_coverage_weighted_prev", "localization_coverage_weighted_cur",
                "ad_spend_prev", "ad_spend_cur", "primary_reason", "secondary_reason", "ad_assessment", "price_assessment",
            ]
            x = x[[c for c in keep if c in x.columns]].sort_values("abc_gp_cur", ascending=False)
        rep.add_df(title, x)
    rep.add_df("Вклад_SKU_в_товар", an.sku_contribution[[c for c in [
        "code", "supplier_article", "subject", "abc_gp_prev", "abc_gp_cur", "sku_gp_delta", "abc_revenue_prev", "abc_revenue_cur", "sku_revenue_delta",
        "abc_sales_qty_prev", "abc_sales_qty_cur", "sku_orders_delta", "sku_contribution_to_product_gp", "primary_reason", "secondary_reason"
    ] if c in an.sku_contribution.columns]])
    rep.add_df("Цена_RRP_SPP", an.price_monitor)
    rep.add_df("Каналы_входа", an.channel_compare.sort_values(["entity_level", "entity_id", "orders_delta_abs"], ascending=[True, True, False]))
    rep.add_df("Локализация_daily", an.localization_daily.sort_values(["supplier_article", "day", "warehouse"], ascending=[True, False, True]))
    rep.add_example("Пример_901_5", an.example_901_5)
    return rep.wb


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="WB combined report with part2 cause analytics")
    p.add_argument("--root", default=".", help="Project root")
    p.add_argument("--reports-root", default="Отчёты", help="Reports root in S3/local structure")
    p.add_argument("--store", default="TOPFACE", help="Store")
    p.add_argument("--out-subdir", default="Отчёты/Объединенный отчет/TOPFACE", help="Output folder")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    storage = make_storage(args.root)
    loader = DataLoader(storage=storage, store=args.store, reports_root=args.reports_root)
    log("Loading data")
    data = loader.load_all()
    if data.warnings:
        for w in data.warnings:
            log(f"WARN: {w}")
    log("Building analytics")
    analyzer = Part2Analyzer(data)
    log("Building workbook")
    wb = build_output_workbook(analyzer)
    out_name = f"{args.out_subdir}/Объединенный_отчет_{args.store}_{datetime.now().strftime('%Y-%m-%d')}.xlsx"
    bio = io.BytesIO()
    wb.save(bio)
    storage.write_bytes(out_name, bio.getvalue())
    log(f"Saved: {out_name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
