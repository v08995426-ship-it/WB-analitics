
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
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

# -------------------------
# Logging / helpers
# -------------------------

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

def extract_code(supplier_article: Any) -> str:
    text = clean_article(supplier_article)
    if not text:
        return ""
    low = text.lower()
    # treat pt901.* as 901
    m = re.match(r"^pt(\d+)", low)
    if m:
        return m.group(1)
    m = re.match(r"^([A-Za-zА-Яа-я0-9]+)", text)
    return m.group(1) if m else text

def to_numeric(series: Any) -> pd.Series:
    if isinstance(series, pd.Series):
        return pd.to_numeric(series, errors="coerce")
    return pd.to_numeric(pd.Series(series), errors="coerce")

def to_dt(series: Any) -> pd.Series:
    if isinstance(series, pd.Series):
        return pd.to_datetime(series, errors="coerce")
    return pd.to_datetime(pd.Series(series), errors="coerce")

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

def parse_week_code_from_name(name: str) -> Optional[str]:
    m = re.search(r"(\d{4})-W(\d{2})", str(name))
    if not m:
        return None
    return f"{m.group(1)}-W{m.group(2)}"

def week_bounds_from_code(week_code: str) -> Tuple[Optional[date], Optional[date]]:
    m = re.match(r"^(\d{4})-W(\d{2})$", str(week_code))
    if not m:
        return None, None
    year = int(m.group(1))
    week = int(m.group(2))
    start = date.fromisocalendar(year, week, 1)
    end = date.fromisocalendar(year, week, 7)
    return start, end

def week_code_from_date(dt_value: Any) -> Optional[str]:
    if pd.isna(dt_value):
        return None
    ts = pd.Timestamp(dt_value)
    iso = ts.isocalendar()
    return f"{int(iso.year)}-W{int(iso.week):02d}"

def parse_entry_period_from_name(name: str) -> Tuple[Optional[date], Optional[date]]:
    m = re.search(r"с (\d{2})-(\d{2})-(\d{4}) по (\d{2})-(\d{2})-(\d{4})", name)
    if not m:
        return None, None
    start = date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    end = date(int(m.group(6)), int(m.group(5)), int(m.group(4)))
    return start, end

def parse_abc_period_from_name(name: str) -> Tuple[Optional[date], Optional[date]]:
    m = re.search(r"__(\d{2})\.(\d{2})\.(\d{4})-(\d{2})\.(\d{2})\.(\d{4})__", name)
    if not m:
        return None, None
    start = date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    end = date(int(m.group(6)), int(m.group(5)), int(m.group(4)))
    return start, end

def dedupe_columns(cols: Iterable[Any]) -> List[str]:
    out = []
    counts: Dict[str, int] = {}
    for c in cols:
        base = normalize_text(c) or "unnamed"
        counts[base] = counts.get(base, 0) + 1
        out.append(base if counts[base] == 1 else f"{base}__{counts[base]}")
    return out

def pick_best_sheet(sheet_names: List[str], preferred: Iterable[str]) -> Any:
    if not sheet_names:
        return 0
    norm_map = {norm_key(s): s for s in sheet_names}
    for name in preferred:
        if norm_key(name) in norm_map:
            return norm_map[norm_key(name)]
    return sheet_names[0]

def required_score(columns: Iterable[Any], expected_aliases: Dict[str, List[str]]) -> int:
    norm_cols = {norm_key(c) for c in columns if normalize_text(c)}
    score = 0
    for aliases in expected_aliases.values():
        for alias in aliases:
            if norm_key(alias) in norm_cols:
                score += 1
                break
    return score

def read_excel_flexible(
    data: bytes,
    filename: str,
    preferred_sheets: Optional[Iterable[str]] = None,
    header_candidates: Iterable[int] = (0, 1, 2),
    expected_aliases: Optional[Dict[str, List[str]]] = None,
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
        score = -1000 if df.empty else len([c for c in df.columns if normalize_text(c)])
        if expected_aliases and not df.empty:
            score += required_score(df.columns, expected_aliases) * 100
        if score > best_score:
            best_df = df
            best_header = header
            best_score = score
    if best_df is None:
        raise ValueError(f"Не удалось прочитать Excel: {filename}")
    best_df.columns = dedupe_columns(best_df.columns)
    return best_df, str(sheet), best_header

def rename_using_aliases(df: pd.DataFrame, alias_map: Dict[str, List[str]]) -> Tuple[pd.DataFrame, Dict[str, Optional[str]]]:
    norm_existing: Dict[str, str] = {}
    for col in df.columns:
        norm_existing.setdefault(norm_key(col), col)
    mapping: Dict[str, Optional[str]] = {}
    out = df.copy()
    for target, aliases in alias_map.items():
        chosen = None
        for alias in aliases:
            k = norm_key(alias)
            if k in norm_existing:
                chosen = norm_existing[k]
                break
        mapping[target] = chosen
        if chosen is not None and chosen != target:
            out[target] = out[chosen]
        elif chosen is None and target not in out.columns:
            out[target] = np.nan
    return out, mapping

def safe_map_col(df: pd.DataFrame, col: str, func) -> None:
    if col not in df.columns:
        df[col] = np.nan
        return
    obj = df[col]
    if isinstance(obj, pd.DataFrame):
        ser = obj.iloc[:, 0]
    elif isinstance(obj, pd.Series):
        ser = obj
    else:
        ser = pd.Series(obj, index=df.index)
    df[col] = ser.map(func)

def unique_preserve(items: Iterable[Any]) -> List[Any]:
    out = []
    seen = set()
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out

def normalize_excluded_code(code: Any) -> str:
    return normalize_text(code).lower().replace(" ", "")

EXCLUDED_CODES = {
    "cz420", "cz420брови", "cz420глаза", "de49", "de49глаза", "pt901",
}
SUBJECT_ORDER = ["Кисти косметические", "Косметические карандаши", "Помады", "Блески"]

def price_fmt(v):
    return v

# -------------------------
# Storage
# -------------------------

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

# -------------------------
# Aliases / data model
# -------------------------

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
    "frequency_week": ["Частота за неделю"],
    "median_position": ["Медианная позиция", "Средняя позиция", "Позиция"],
    "visibility_pct": ["Видимость", "Видимость, %"],
    "warehouse": ["Склад", "warehouseName"],
    "region": ["Регион", "regionName"],
    "stock_available": ["Доступно для продажи", "Остаток", "stock"],
    "stock_total": ["Полное количество"],
    "impressions": ["Показы", "impressions"],
    "clicks": ["Клики", "Клики в карточку", "Переходы в карточку", "Перешли в карточку", "clicks"],
    "ctr": ["CTR"],
    "cart": ["Добавили в корзину", "Добавлени в корзину", "addToCartCount"],
    "conv_cart": ["Конверсия в корзину", "addToCartConversion"],
    "orders": ["Заказы", "Заказали", "orders", "ordersCount", "Кол-во продаж", "Чистые продажи, шт"],
    "conv_order": ["Конверсия в заказ", "cartToOrderConversion", "CR"],
    "open_card_count": ["Открытие карточки", "openCardCount"],
    "buyouts_count": ["buyoutsCount"],
    "cancel_count": ["cancelCount", "Отмена заказа"],
    "spend": ["Расход", "spend", "Продвижение"],
    "cpc": ["CPC"],
    "campaign_id": ["ID кампании"],
    "campaign_name": ["Название"],
    "status": ["Статус"],
    "payment_type": ["Тип оплаты"],
    "bid_search": ["Ставка в поиске (руб)"],
    "bid_reco": ["Ставка в рекомендациях (руб)"],
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

@dataclass
class FileLoadInfo:
    dataset: str
    file_path: str
    sheet_name: str
    header_row_excel: int
    rows: int
    columns: List[str]
    mapping: Dict[str, Optional[str]] = field(default_factory=dict)
    error: str = ""

@dataclass
class LoadedData:
    orders: pd.DataFrame = field(default_factory=pd.DataFrame)
    stocks: pd.DataFrame = field(default_factory=pd.DataFrame)
    search: pd.DataFrame = field(default_factory=pd.DataFrame)
    funnel: pd.DataFrame = field(default_factory=pd.DataFrame)
    ads_daily: pd.DataFrame = field(default_factory=pd.DataFrame)
    economics: pd.DataFrame = field(default_factory=pd.DataFrame)
    abc: pd.DataFrame = field(default_factory=pd.DataFrame)
    entry_points_category: pd.DataFrame = field(default_factory=pd.DataFrame)
    entry_points_sku: pd.DataFrame = field(default_factory=pd.DataFrame)
    rrp: pd.DataFrame = field(default_factory=pd.DataFrame)
    diagnostics: List[FileLoadInfo] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

# -------------------------
# Data loader
# -------------------------

class DataLoader:
    def __init__(self, storage: BaseStorage, store: str, reports_root: str = "Отчёты"):
        self.storage = storage
        self.store = store
        self.reports_root = reports_root.rstrip("/")
        self.diagnostics: List[FileLoadInfo] = []
        self.warnings: List[str] = []

    def _prefix(self, *parts: str) -> str:
        return "/".join([self.reports_root, *parts]).replace("//", "/")

    def _list_under(self, prefixes: Iterable[str], must_contain: Optional[Iterable[str]] = None) -> List[str]:
        all_files: List[str] = []
        for prefix in prefixes:
            try:
                all_files.extend(self.storage.list_files(prefix))
            except Exception as e:
                self.warnings.append(f"Не удалось получить список файлов {prefix}: {e}")
        all_files = [f for f in unique_preserve(sorted(all_files)) if f.lower().endswith(".xlsx") and "/~$" not in f]
        if must_contain:
            all_files = [f for f in all_files if all(part.lower() in Path(f).name.lower() for part in must_contain)]
        return sorted(all_files)

    def _record(self, info: FileLoadInfo) -> None:
        self.diagnostics.append(info)

    def _read_and_standardize(
        self,
        dataset: str,
        path: str,
        preferred_sheets: Optional[Iterable[str]],
        header_candidates: Iterable[int],
        alias_map: Dict[str, List[str]],
    ) -> pd.DataFrame:
        data = self.storage.read_bytes(path)
        raw, sheet, header = read_excel_flexible(
            data=data,
            filename=path,
            preferred_sheets=preferred_sheets,
            header_candidates=header_candidates,
            expected_aliases=alias_map,
        )
        raw.columns = dedupe_columns(raw.columns)
        df, mapping = rename_using_aliases(raw, alias_map)
        self._record(FileLoadInfo(dataset, path, sheet, header + 1, len(df), [normalize_text(c) for c in raw.columns], mapping))
        return df

    def load_orders(self) -> pd.DataFrame:
        files = self._list_under([self._prefix("Заказы", self.store, "Недельные"), self._prefix("Заказы", self.store)])
        dfs = []
        for path in files:
            try:
                df = self._read_and_standardize("orders", path, None, (0, 1, 2), {**COMMON_ALIASES, "is_cancel": ["isCancel", "Отмена заказа"]})
                week_code = parse_week_code_from_name(Path(path).name)
                start, end = week_bounds_from_code(week_code) if week_code else (None, None)
                df["week_code"] = week_code
                df["week_start"] = pd.Timestamp(start) if start else pd.NaT
                df["week_end"] = pd.Timestamp(end) if end else pd.NaT
                df["day"] = to_dt(df["day"]).dt.normalize()
                df["nm_id"] = to_numeric(df["nm_id"])
                safe_map_col(df, "supplier_article", clean_article)
                safe_map_col(df, "subject", normalize_text)
                safe_map_col(df, "brand", normalize_text)
                safe_map_col(df, "warehouse", normalize_text)
                safe_map_col(df, "region", normalize_text)
                df["finished_price"] = to_numeric(df["finished_price"])
                df["price_with_disc"] = to_numeric(df["price_with_disc"])
                df["spp"] = to_numeric(df["spp"])
                df["orders"] = to_numeric(df["orders"])
                if df["orders"].isna().all():
                    df["orders"] = 1
                dfs.append(df)
            except Exception as e:
                self.warnings.append(f"Orders read error {path}: {e}")
                self._record(FileLoadInfo("orders", path, "", 0, 0, [], error=str(e)))
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    def load_stocks(self) -> pd.DataFrame:
        files = self._list_under([self._prefix("Остатки", self.store, "Недельные")])
        dfs = []
        for path in files[-12:]:
            try:
                df = self._read_and_standardize("stocks", path, None, (0,), COMMON_ALIASES)
                week_code = parse_week_code_from_name(Path(path).name)
                start, end = week_bounds_from_code(week_code) if week_code else (None, None)
                df["week_code"] = week_code
                df["week_start"] = pd.Timestamp(start) if start else pd.NaT
                df["week_end"] = pd.Timestamp(end) if end else pd.NaT
                df["nm_id"] = to_numeric(df["nm_id"])
                safe_map_col(df, "supplier_article", clean_article)
                safe_map_col(df, "subject", normalize_text)
                safe_map_col(df, "brand", normalize_text)
                safe_map_col(df, "warehouse", normalize_text)
                df["stock_available"] = to_numeric(df["stock_available"]).fillna(0)
                df["stock_total"] = to_numeric(df["stock_total"]).fillna(df["stock_available"]).fillna(0)
                dfs.append(df)
            except Exception as e:
                self.warnings.append(f"Stocks read error {path}: {e}")
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    def load_search(self) -> pd.DataFrame:
        files = self._list_under([self._prefix("Поисковые запросы", self.store, "Недельные")])
        dfs = []
        for path in files[-16:]:
            try:
                df = self._read_and_standardize("search", path, None, (0,), COMMON_ALIASES)
                week_code = parse_week_code_from_name(Path(path).name)
                start, end = week_bounds_from_code(week_code) if week_code else (None, None)
                df["week_code"] = week_code
                df["week_start"] = pd.Timestamp(start) if start else pd.NaT
                df["week_end"] = pd.Timestamp(end) if end else pd.NaT
                df["day"] = to_dt(df["day"]).dt.normalize()
                df["nm_id"] = to_numeric(df["nm_id"])
                safe_map_col(df, "supplier_article", clean_article)
                safe_map_col(df, "subject", normalize_text)
                safe_map_col(df, "brand", normalize_text)
                safe_map_col(df, "query", normalize_text)
                for c in ["frequency", "frequency_week", "median_position", "visibility_pct"]:
                    df[c] = to_numeric(df[c])
                dfs.append(df)
            except Exception as e:
                self.warnings.append(f"Search read error {path}: {e}")
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    def load_funnel(self) -> pd.DataFrame:
        candidates = [
            self._prefix("Воронка продаж", self.store, "Воронка продаж.xlsx"),
            self._prefix("Воронка продаж", "Воронка продаж.xlsx"),
        ]
        path = None
        for c in candidates:
            if self.storage.exists(c):
                path = c
                break
        if not path:
            return pd.DataFrame()
        try:
            df = self._read_and_standardize("funnel", path, None, (0,), COMMON_ALIASES)
            df["day"] = to_dt(df["day"]).dt.normalize()
            df["nm_id"] = to_numeric(df["nm_id"])
            for c in ["open_card_count", "cart", "orders", "buyouts_count", "cancel_count", "conv_cart", "conv_order"]:
                df[c] = to_numeric(df[c])
            return df
        except Exception as e:
            self.warnings.append(f"Funnel read error {path}: {e}")
            return pd.DataFrame()

    def load_ads(self) -> pd.DataFrame:
        files = self._list_under([self._prefix("Реклама", self.store, "Недельные"), self._prefix("Реклама", self.store)])
        dfs = []
        for path in files[-16:]:
            try:
                df = self._read_and_standardize("ads_daily", path, ["Статистика_Ежедневно"], (0,), COMMON_ALIASES)
                week_code = parse_week_code_from_name(Path(path).name)
                start, end = week_bounds_from_code(week_code) if week_code else (None, None)
                df["week_code"] = week_code
                df["week_start"] = pd.Timestamp(start) if start else pd.NaT
                df["week_end"] = pd.Timestamp(end) if end else pd.NaT
                df["day"] = to_dt(df["day"]).dt.normalize()
                df["nm_id"] = to_numeric(df["nm_id"])
                safe_map_col(df, "supplier_article", clean_article)
                safe_map_col(df, "subject", normalize_text)
                for c in ["impressions", "clicks", "ctr", "cpc", "orders", "conv_order", "spend"]:
                    df[c] = to_numeric(df[c])
                dfs.append(df)
            except Exception as e:
                self.warnings.append(f"Ads read error {path}: {e}")
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    def load_economics(self) -> pd.DataFrame:
        candidates = [
            self._prefix("Финансовые показатели", self.store, "Экономика.xlsx"),
            self._prefix("Финансовые показатели", self.store, "Недельные", "Экономика.xlsx"),
        ]
        path = None
        for c in candidates:
            if self.storage.exists(c):
                path = c
                break
        if not path:
            return pd.DataFrame()
        try:
            alias_map = {
                **COMMON_ALIASES,
                "sales_qty": ["Продажи, шт", "Чистые продажи, шт"],
                "returns_qty": ["Возвраты, шт"],
                "buyout_pct": ["Процент выкупа"],
                "selling_price": ["Средняя цена продажи"],
                "buyer_price": ["Средняя цена покупателя"],
                "commission_unit": ["Комиссия WB, руб/ед"],
                "acquiring_unit": ["Эквайринг, руб/ед"],
                "logistics_direct_unit": ["Логистика прямая, руб/ед"],
                "logistics_return_unit": ["Логистика обратная, руб/ед"],
                "storage_unit": ["Хранение, руб/ед"],
                "acceptance_unit": ["Приёмка, руб/ед"],
                "ads_unit": ["Реклама, руб/ед"],
                "other_unit": ["Прочие расходы, руб/ед", "Штрафы и удержания, руб/ед"],
                "cost_unit": ["Себестоимость, руб"],
                "gp_unit": ["Валовая прибыль, руб/ед"],
                "np_unit": ["Чистая прибыль, руб/ед"],
            }
            df = self._read_and_standardize("economics", path, ["Юнит экономика"], (0,1,2), alias_map)
            df["week_code"] = df["week"].astype(str).str.strip()
            df["nm_id"] = to_numeric(df["nm_id"])
            safe_map_col(df, "supplier_article", clean_article)
            safe_map_col(df, "subject", normalize_text)
            safe_map_col(df, "brand", normalize_text)
            for c in ["sales_qty","returns_qty","buyout_pct","selling_price","buyer_price","spp","commission_unit","acquiring_unit","logistics_direct_unit","logistics_return_unit","storage_unit","acceptance_unit","ads_unit","other_unit","cost_unit","gp_unit","np_unit","margin_pct","profitability_pct"]:
                if c in df.columns:
                    df[c] = to_numeric(df[c])
            return df
        except Exception as e:
            self.warnings.append(f"Economics read error {path}: {e}")
            return pd.DataFrame()

    def load_abc(self) -> pd.DataFrame:
        files = self._list_under([self._prefix("ABC")], must_contain=["wb_abc_report_goods__"])
        dfs = []
        for path in files[-16:]:
            try:
                df = self._read_and_standardize("abc", path, None, (0,), COMMON_ALIASES)
                start, end = parse_abc_period_from_name(Path(path).name)
                df["week_start"] = pd.Timestamp(start) if start else pd.NaT
                df["week_end"] = pd.Timestamp(end) if end else pd.NaT
                df["week_code"] = week_code_from_date(start) if start else None
                df["nm_id"] = to_numeric(df["nm_id"])
                safe_map_col(df, "supplier_article", clean_article)
                safe_map_col(df, "subject", normalize_text)
                safe_map_col(df, "brand", normalize_text)
                for c in ["gross_profit","gross_revenue","orders","drr_pct","margin_pct","profitability_pct"]:
                    df[c] = to_numeric(df[c])
                dfs.append(df)
            except Exception as e:
                self.warnings.append(f"ABC read error {path}: {e}")
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    def load_entry_points(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        files = self._list_under([self._prefix("Точки входа", self.store)])
        cat_dfs: List[pd.DataFrame] = []
        sku_dfs: List[pd.DataFrame] = []
        for path in files[-24:]:
            log(f"  entry workbook: {Path(path).name}")
            start, end = parse_entry_period_from_name(Path(path).name)
            week_code = week_code_from_date(start) if start else None
            for dataset, sheet_names, target_list in [
                ("entry_points_category", ["Детализация по точкам входа"], cat_dfs),
                ("entry_points_sku", ["Детализация по артикулам"], sku_dfs),
            ]:
                try:
                    df = self._read_and_standardize(dataset, path, sheet_names, (1, 0, 2), COMMON_ALIASES)
                    df["week_start"] = pd.Timestamp(start) if start else pd.NaT
                    df["week_end"] = pd.Timestamp(end) if end else pd.NaT
                    df["week_code"] = week_code
                    df["nm_id"] = to_numeric(df["nm_id"])
                    safe_map_col(df, "supplier_article", clean_article)
                    safe_map_col(df, "subject", normalize_text)
                    safe_map_col(df, "brand", normalize_text)
                    safe_map_col(df, "section", normalize_text)
                    safe_map_col(df, "entry_point", normalize_text)
                    for c in ["impressions","clicks","ctr","cart","conv_cart","orders","conv_order"]:
                        df[c] = to_numeric(df[c])
                    target_list.append(df)
                except Exception as e:
                    self.warnings.append(f"Entry points read error {path}: {e}")
                    log(f"WARN: Entry points read error {path}: {e}")
        return (
            pd.concat(cat_dfs, ignore_index=True) if cat_dfs else pd.DataFrame(),
            pd.concat(sku_dfs, ignore_index=True) if sku_dfs else pd.DataFrame(),
        )

    def normalize_article_for_rrp(self, article: str) -> str:
        a = clean_article(article)
        if not a:
            return ""
        low = a.lower()
        m = re.match(r"^pt(\d+)\.f(\d+)$", low)
        if m:
            return f"PT{int(m.group(1))}.F{int(m.group(2)):02d}"
        m = re.match(r"^(\d+)\/(\d+)$", low)
        if m:
            return f"PT{int(m.group(1))}.{int(m.group(2)):03d}"
        m = re.match(r"^(\d+)\/f(\d+)$", low)
        if m:
            return f"PT{int(m.group(1))}.F{int(m.group(2)):02d}"
        if low.startswith("pt"):
            return a.upper()
        return a.upper()

    def load_rrp(self) -> pd.DataFrame:
        candidates = [
            self._prefix("Финансовые показатели", self.store, "РРЦ.xlsx"),
            self._prefix("Финансовые показатели", "РРЦ.xlsx"),
        ]
        path = None
        for c in candidates:
            if self.storage.exists(c):
                path = c
                break
        if not path:
            return pd.DataFrame()
        try:
            data = self.storage.read_bytes(path)
            raw, sheet, header = read_excel_flexible(data, path, ["TF"], (0,1,2))
            raw.columns = dedupe_columns(raw.columns)
            col_art = None
            col_rrp = None
            for c in raw.columns:
                k = norm_key(c)
                if k in {norm_key("ПРАВИЛЬНЫЙ АРТИКУЛ"), norm_key("Артикул"), norm_key("Артикул продавца")}:
                    col_art = c
                if k in {norm_key("РРЦ"), norm_key("RRP")}:
                    col_rrp = c
            if col_art is None or col_rrp is None:
                raise ValueError("Не найдены колонки артикула/РРЦ")
            df = pd.DataFrame({
                "rrp_article": raw[col_art].map(normalize_text),
                "rrp": pd.to_numeric(raw[col_rrp], errors="coerce"),
            })
            df["rrp_key"] = df["rrp_article"].map(lambda x: self.normalize_article_for_rrp(x))
            return df.dropna(subset=["rrp"]).drop_duplicates(subset=["rrp_key"], keep="first")
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
        ads_daily = self.load_ads()
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
            economics=economics,
            abc=abc,
            entry_points_category=entry_cat,
            entry_points_sku=entry_sku,
            rrp=rrp,
            diagnostics=self.diagnostics,
            warnings=self.warnings,
        )

# -------------------------
# Part 2 analyzer
# -------------------------

class Part2Analyzer:
    def __init__(self, data: LoadedData):
        self.data = data
        self.windows = self.determine_windows()
        self.daily_article = self.build_daily_article()
        self.localization_daily = self.build_localization_daily()
        self.article_period = self.build_article_period()
        self.product_period = self.aggregate_period(self.article_period, "code")
        self.category_period = self.aggregate_period(self.article_period, "subject")
        self.sku_contrib = self.build_sku_contrib()
        self.channels = self.build_channels()
        self.example_901_5 = self.build_example("901/5")

    def determine_windows(self) -> Dict[str, pd.Timestamp]:
        days = []
        for df in [self.data.orders, self.data.search, self.data.funnel, self.data.ads_daily]:
            if not df.empty and "day" in df.columns:
                d = pd.to_datetime(df["day"], errors="coerce")
                if not d.dropna().empty:
                    days.append(d.max())
        end = max(days) if days else pd.Timestamp.today().normalize()
        cur_end = pd.Timestamp(end).normalize()
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
        if not hasattr(self, "windows") or not isinstance(getattr(self, "windows", None), dict):
            self.windows = self.determine_windows()
        day = pd.Timestamp(day).normalize()
        if self.windows["cur_start"] <= day <= self.windows["cur_end"]:
            return "cur_14d"
        if self.windows["prev_start"] <= day <= self.windows["prev_end"]:
            return "prev_14d"
        return None

    def daily_base_orders(self) -> pd.DataFrame:
        if self.data.orders.empty:
            return pd.DataFrame(columns=["day","supplier_article","nm_id","subject","brand","code"])
        o = self.data.orders.copy()
        o["day"] = pd.to_datetime(o["day"], errors="coerce").dt.normalize()
        o = o[(o["day"] >= self.windows["prev_start"]) & (o["day"] <= self.windows["cur_end"])].copy()
        o["supplier_article"] = o["supplier_article"].map(clean_article)
        o["code"] = o["supplier_article"].map(extract_code)
        o = o[~o["code"].map(normalize_excluded_code).isin(EXCLUDED_CODES)].copy()
        return o

    def build_daily_article(self) -> pd.DataFrame:
        o = self.daily_base_orders()
        if o.empty:
            return pd.DataFrame()
        grp_orders = o.groupby(["day","supplier_article","nm_id","subject","brand","code"], dropna=False).agg(
            orders_from_orders=("orders","sum"),
            finishedPrice_avg=("finished_price","mean"),
            priceWithDisc_avg=("price_with_disc","mean"),
            spp_avg=("spp","mean"),
            warehouse_count=("warehouse","nunique"),
        ).reset_index()

        cur = grp_orders.copy()

        if not self.data.funnel.empty:
            f = self.data.funnel.copy()
            f["day"] = pd.to_datetime(f["day"], errors="coerce").dt.normalize()
            f = f[(f["day"] >= self.windows["prev_start"]) & (f["day"] <= self.windows["cur_end"])].copy()
            f["nm_id"] = to_numeric(f["nm_id"])
            map_sku = cur[["nm_id","supplier_article","subject","brand","code"]].dropna(subset=["nm_id"]).drop_duplicates("nm_id")
            f = f.merge(map_sku, on="nm_id", how="left")
            fg = f.groupby(["day","supplier_article","nm_id","subject","brand","code"], dropna=False).agg(
                open_card_count=("open_card_count","sum"),
                cart_count=("cart","sum"),
                orders_funnel=("orders","sum"),
                buyouts_funnel=("buyouts_count","sum"),
                cancel_funnel=("cancel_count","sum"),
            ).reset_index()
            cur = cur.merge(fg, on=["day","supplier_article","nm_id","subject","brand","code"], how="left")

        if not self.data.search.empty:
            s = self.data.search.copy()
            s["day"] = pd.to_datetime(s["day"], errors="coerce").dt.normalize()
            s = s[(s["day"] >= self.windows["prev_start"]) & (s["day"] <= self.windows["cur_end"])].copy()
            s["supplier_article"] = s["supplier_article"].map(clean_article)
            s["code"] = s["supplier_article"].map(extract_code)
            sg = s.groupby(["day","supplier_article","nm_id","subject","brand","code"], dropna=False).agg(
                search_frequency=("frequency","sum"),
                median_position=("median_position","median"),
                visibility_pct=("visibility_pct","mean"),
                search_queries_count=("query","nunique"),
            ).reset_index()
            cur = cur.merge(sg, on=["day","supplier_article","nm_id","subject","brand","code"], how="left")

        if not self.data.ads_daily.empty:
            a = self.data.ads_daily.copy()
            a["day"] = pd.to_datetime(a["day"], errors="coerce").dt.normalize()
            a = a[(a["day"] >= self.windows["prev_start"]) & (a["day"] <= self.windows["cur_end"])].copy()
            a["nm_id"] = to_numeric(a["nm_id"])
            map_sku = cur[["nm_id","supplier_article","subject","brand","code"]].dropna(subset=["nm_id"]).drop_duplicates("nm_id")
            a = a.merge(map_sku, on="nm_id", how="left")
            ag = a.groupby(["day","supplier_article","nm_id","subject","brand","code"], dropna=False).agg(
                ad_impressions=("impressions","sum"),
                ad_clicks=("clicks","sum"),
                ad_orders=("orders","sum"),
                ad_spend=("spend","sum"),
                ad_ctr=("ctr","mean"),
                ad_cpc=("cpc","mean"),
            ).reset_index()
            cur = cur.merge(ag, on=["day","supplier_article","nm_id","subject","brand","code"], how="left")

        # fill and derive
        for c in ["open_card_count","cart_count","orders_funnel","buyouts_funnel","cancel_funnel","search_frequency","search_queries_count","ad_impressions","ad_clicks","ad_orders","ad_spend","warehouse_count"]:
            if c not in cur.columns:
                cur[c] = 0
            cur[c] = pd.to_numeric(cur[c], errors="coerce").fillna(0)
        for c in ["median_position","visibility_pct","finishedPrice_avg","priceWithDisc_avg","spp_avg","ad_ctr","ad_cpc"]:
            if c not in cur.columns:
                cur[c] = np.nan
            cur[c] = pd.to_numeric(cur[c], errors="coerce")
        cur["clicks_total"] = cur["open_card_count"]
        cur["impressions_total"] = cur["ad_impressions"]  # proxy when no organic impressions
        cur["ctr_total"] = cur["ad_ctr"]
        cur["cr_click_to_order"] = cur.apply(lambda r: safe_div(r["orders_funnel"], r["clicks_total"]), axis=1)
        cur["conv_to_cart"] = cur.apply(lambda r: safe_div(r["cart_count"], r["open_card_count"]), axis=1)
        cur["conv_cart_to_order"] = cur.apply(lambda r: safe_div(r["orders_funnel"], r["cart_count"]), axis=1)
        cur["period_name"] = cur["day"].map(self._period_name)
        return cur[cur["period_name"].notna()].copy()

    def _weeks_for_period(self, period_name: str) -> List[str]:
        if period_name == "cur_14d":
            start, end = self.windows["cur_start"], self.windows["cur_end"]
        else:
            start, end = self.windows["prev_start"], self.windows["prev_end"]
        dates = pd.date_range(start, end, freq="D")
        return sorted(unique_preserve([week_code_from_date(d) for d in dates]))

    def build_article_period(self) -> pd.DataFrame:
        daily = self.daily_article.copy()
        if daily.empty:
            return pd.DataFrame()
        keys = ["supplier_article","nm_id","subject","brand","code","period_name"]

        ops = daily.groupby(keys, dropna=False).agg(
            clicks_total=("clicks_total","sum"),
            impressions_total=("impressions_total","sum"),
            open_card_count=("open_card_count","sum"),
            cart_count=("cart_count","sum"),
            orders_funnel=("orders_funnel","sum"),
            buyouts_funnel=("buyouts_funnel","sum"),
            cancel_funnel=("cancel_funnel","sum"),
            finishedPrice_avg=("finishedPrice_avg","mean"),
            priceWithDisc_avg=("priceWithDisc_avg","mean"),
            spp_avg=("spp_avg","mean"),
            search_frequency=("search_frequency","sum"),
            median_position=("median_position","median"),
            visibility_pct=("visibility_pct","mean"),
            search_queries_count=("search_queries_count","sum"),
            ad_spend=("ad_spend","sum"),
            ad_clicks=("ad_clicks","sum"),
            ad_orders=("ad_orders","sum"),
            ad_impressions=("ad_impressions","sum"),
            ad_ctr=("ad_ctr","mean"),
            ad_cpc=("ad_cpc","mean"),
        ).reset_index()
        ops["ctr_total"] = ops.apply(lambda r: safe_div(r["clicks_total"], r["impressions_total"]), axis=1)
        ops["cr_click_to_order"] = ops.apply(lambda r: safe_div(r["orders_funnel"], r["clicks_total"]), axis=1)
        ops["conv_to_cart"] = ops.apply(lambda r: safe_div(r["cart_count"], r["open_card_count"]), axis=1)
        ops["conv_cart_to_order"] = ops.apply(lambda r: safe_div(r["orders_funnel"], r["cart_count"]), axis=1)

        # ABC facts
        if not self.data.abc.empty:
            abc = self.data.abc.copy()
            abc["supplier_article"] = abc["supplier_article"].map(clean_article)
            abc["code"] = abc["supplier_article"].map(extract_code)
            frames = []
            for pn in ["prev_14d","cur_14d"]:
                weeks = self._weeks_for_period(pn)
                x = abc[abc["week_code"].isin(weeks)].copy()
                x["period_name"] = pn
                g = x.groupby(["supplier_article","nm_id","subject","brand","code","period_name"], dropna=False).agg(
                    abc_revenue=("gross_revenue","sum"),
                    abc_gp=("gross_profit","sum"),
                    abc_orders=("orders","sum"),
                    drr_pct=("drr_pct","mean"),
                    margin_pct=("margin_pct","mean"),
                    profitability_pct=("profitability_pct","mean"),
                ).reset_index()
                frames.append(g)
            abc_period = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
            ops = ops.merge(abc_period, on=["supplier_article","nm_id","subject","brand","code","period_name"], how="left")

        # Economics
        if not self.data.economics.empty:
            econ = self.data.economics.copy()
            econ["supplier_article"] = econ["supplier_article"].map(clean_article)
            econ["code"] = econ["supplier_article"].map(extract_code)
            frames = []
            for pn in ["prev_14d","cur_14d"]:
                weeks = self._weeks_for_period(pn)
                x = econ[econ["week_code"].isin(weeks)].copy()
                x["period_name"] = pn
                # weighted by sales qty
                if "sales_qty" not in x.columns:
                    x["sales_qty"] = 1
                x["sales_qty"] = pd.to_numeric(x["sales_qty"], errors="coerce").fillna(0)
                def wavg(g, col):
                    vals = pd.to_numeric(g[col], errors="coerce")
                    w = pd.to_numeric(g["sales_qty"], errors="coerce").fillna(0)
                    if (w > 0).sum() == 0:
                        return vals.mean()
                    return np.average(vals.fillna(0), weights=w)
                rows=[]
                grp_cols=["supplier_article","nm_id","subject","brand","code","period_name"]
                for keys2, g in x.groupby(grp_cols, dropna=False):
                    row = dict(zip(grp_cols, keys2))
                    for col in ["selling_price","buyer_price","spp","commission_unit","acquiring_unit","logistics_direct_unit","logistics_return_unit","storage_unit","acceptance_unit","ads_unit","other_unit","cost_unit","gp_unit","np_unit","margin_pct","profitability_pct"]:
                        if col in g.columns:
                            row[col] = wavg(g, col)
                    rows.append(row)
                frames.append(pd.DataFrame(rows))
            econ_period = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
            ops = ops.merge(econ_period, on=["supplier_article","nm_id","subject","brand","code","period_name"], how="left", suffixes=("","_econ"))

        # RRP
        if not self.data.rrp.empty:
            ops["rrp_key"] = ops["supplier_article"].map(lambda x: DataLoader.normalize_article_for_rrp(DataLoader.__new__(DataLoader), x))
            ops = ops.merge(self.data.rrp[["rrp_key","rrp"]], on="rrp_key", how="left")
        else:
            ops["rrp"] = np.nan
        ops["finishedPrice_rrp_coeff"] = ops.apply(lambda r: safe_div(r["finishedPrice_avg"], r["rrp"]), axis=1)
        ops["priceWithDisc_rrp_coeff"] = ops.apply(lambda r: safe_div(r["priceWithDisc_avg"], r["rrp"]), axis=1)

        # Demand by category
        if not self.data.search.empty:
            s = self.data.search.copy()
            s["day"] = pd.to_datetime(s["day"], errors="coerce").dt.normalize()
            s = s[(s["day"] >= self.windows["prev_start"]) & (s["day"] <= self.windows["cur_end"])].copy()
            s["period_name"] = s["day"].map(self._period_name)
            day_unique = s.drop_duplicates(subset=["day","subject","query"]).copy()
            demand = day_unique.groupby(["subject","period_name"], dropna=False).agg(category_demand=("frequency","sum")).reset_index()
            ops = ops.merge(demand, on=["subject","period_name"], how="left")

        # localization coverage
        if not self.localization_daily.empty:
            locp = self.localization_daily.groupby(["supplier_article","period_name"], dropna=False).agg(
                localization_coverage_weighted=("weighted_available","mean"),
                localization_coverage_count=("available_count_pct","mean"),
            ).reset_index()
            ops = ops.merge(locp, on=["supplier_article","period_name"], how="left")

        # clean exclusions and gp/order
        ops = ops[~ops["code"].map(normalize_excluded_code).isin(EXCLUDED_CODES)].copy()
        ops["gp_per_order"] = ops.apply(lambda r: safe_div(r.get("abc_gp"), r.get("abc_orders")), axis=1)
        ops["revenue_per_order"] = ops.apply(lambda r: safe_div(r.get("abc_revenue"), r.get("abc_orders")), axis=1)

        # pivot prev/cur into one row per entity
        prev = ops[ops["period_name"] == "prev_14d"].copy()
        cur = ops[ops["period_name"] == "cur_14d"].copy()
        id_cols = ["supplier_article","nm_id","subject","brand","code"]
        result = prev.merge(cur, on=id_cols, how="outer", suffixes=("_prev","_cur"))
        result = self.add_deltas(result)
        result = self.classify_reasons(result, level="article")
        result = result.sort_values(["abc_gp_cur","abc_gp_prev"], ascending=[False,False])
        return result

    def add_deltas(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        pairs = [
            "abc_revenue","abc_gp","abc_orders","gp_per_order","revenue_per_order","clicks_total","impressions_total","ctr_total",
            "cr_click_to_order","conv_to_cart","conv_cart_to_order","finishedPrice_avg","priceWithDisc_avg","spp_avg",
            "finishedPrice_rrp_coeff","priceWithDisc_rrp_coeff","search_frequency","median_position","visibility_pct",
            "ad_spend","ad_clicks","ad_orders","ad_impressions","ad_ctr","ad_cpc","category_demand",
            "localization_coverage_weighted","localization_coverage_count",
            "commission_unit","acquiring_unit","logistics_direct_unit","logistics_return_unit","storage_unit","acceptance_unit","ads_unit","other_unit","cost_unit","gp_unit","np_unit","margin_pct","profitability_pct",
        ]
        for base in pairs:
            prev, cur = f"{base}_prev", f"{base}_cur"
            if prev in df.columns or cur in df.columns:
                if prev not in df.columns:
                    df[prev] = np.nan
                if cur not in df.columns:
                    df[cur] = np.nan
                df[f"{base}_delta_abs"] = df[cur] - df[prev]
                if base == "median_position":
                    df[f"{base}_delta_pct"] = np.nan
                else:
                    df[f"{base}_delta_pct"] = df.apply(lambda r: pct_delta(r[cur], r[prev]), axis=1)
        # contribution
        df["gp_volume_effect"] = (df["abc_orders_cur"].fillna(0) - df["abc_orders_prev"].fillna(0)) * df["gp_per_order_prev"].fillna(0)
        df["gp_economy_effect"] = (df["gp_per_order_cur"].fillna(0) - df["gp_per_order_prev"].fillna(0)) * df["abc_orders_cur"].fillna(0)
        df["orders_traffic_effect"] = (df["clicks_total_cur"].fillna(0) - df["clicks_total_prev"].fillna(0)) * df["cr_click_to_order_prev"].fillna(0)
        df["orders_conversion_effect"] = (df["cr_click_to_order_cur"].fillna(0) - df["cr_click_to_order_prev"].fillna(0)) * df["clicks_total_cur"].fillna(0)
        df["revenue_order_effect"] = (df["abc_orders_cur"].fillna(0) - df["abc_orders_prev"].fillna(0)) * df["revenue_per_order_prev"].fillna(0)
        df["revenue_price_effect"] = (df["revenue_per_order_cur"].fillna(0) - df["revenue_per_order_prev"].fillna(0)) * df["abc_orders_cur"].fillna(0)
        return df

    def classify_reasons(self, df: pd.DataFrame, level: str) -> pd.DataFrame:
        df = df.copy()
        reasons_main = []
        reasons_secondary = []
        ad_assessment = []
        price_assessment = []
        for _, r in df.iterrows():
            # ad assessment
            ad_spend_delta = r.get("ad_spend_delta_pct", np.nan)
            ad_clicks_delta = r.get("ad_clicks_delta_pct", np.nan)
            orders_delta = r.get("abc_orders_delta_pct", np.nan)
            gp_delta = r.get("abc_gp_delta_pct", np.nan)
            visibility_delta = r.get("visibility_pct_delta_pct", np.nan)
            ad_assess = ""
            if pd.notna(ad_spend_delta) and ad_spend_delta >= 0.15:
                if pd.notna(ad_clicks_delta) and ad_clicks_delta >= 0.08 and pd.notna(orders_delta) and orders_delta >= 0.08 and pd.notna(gp_delta) and gp_delta >= 0.05:
                    ad_assess = "Эффективно"
                elif pd.notna(ad_clicks_delta) and ad_clicks_delta >= 0.08 and (pd.isna(orders_delta) or orders_delta < 0.05):
                    ad_assess = "Частично эффективно"
                elif pd.notna(gp_delta) and gp_delta < 0:
                    ad_assess = "Неэффективно"
                elif pd.notna(visibility_delta) and visibility_delta >= 0:
                    ad_assess = "Защитно"
            ad_assessment.append(ad_assess)

            # price assessment from priceWithDisc
            pwd_delta = r.get("priceWithDisc_avg_delta_pct", np.nan)
            fp_delta = r.get("finishedPrice_avg_delta_pct", np.nan)
            margin_delta_pp = r.get("margin_pct_delta_abs", np.nan)
            price_assess = ""
            if pd.notna(pwd_delta) and pwd_delta <= -0.02:
                if (pd.notna(orders_delta) and orders_delta < 0.08) and (pd.notna(gp_delta) and gp_delta < -0.05):
                    price_assess = "Снижение цены не оправдано"
                elif (pd.notna(orders_delta) and orders_delta >= 0.12) and (pd.notna(gp_delta) and gp_delta > 0):
                    price_assess = "Снижение цены оправдано"
            elif pd.notna(pwd_delta) and pwd_delta >= 0.02:
                if (pd.notna(orders_delta) and orders_delta > -0.05) and (pd.notna(gp_delta) and gp_delta > 0):
                    price_assess = "Повышение цены оправдано"
                elif (pd.notna(orders_delta) and orders_delta < -0.08) and (pd.notna(gp_delta) and gp_delta < 0):
                    price_assess = "Повышение цены вредно"
            elif pd.notna(fp_delta) and abs(fp_delta) >= 0.03:
                if fp_delta > 0 and (pd.notna(r.get("cr_click_to_order_delta_pct", np.nan)) and r.get("cr_click_to_order_delta_pct", np.nan) < 0):
                    price_assess = "Рост цены для покупателя давит конверсию"
                elif fp_delta < 0 and (pd.notna(orders_delta) and orders_delta < 0.05):
                    price_assess = "Снижение цены для покупателя не дало эффекта"
            price_assessment.append(price_assess)

            # main / secondary reason
            vol_effect = abs(r.get("gp_volume_effect", np.nan)) if pd.notna(r.get("gp_volume_effect", np.nan)) else 0
            econ_effect = abs(r.get("gp_economy_effect", np.nan)) if pd.notna(r.get("gp_economy_effect", np.nan)) else 0
            main = ""
            sec = ""
            if vol_effect >= econ_effect:
                if pd.notna(r.get("localization_coverage_weighted_delta_abs", np.nan)) and r.get("localization_coverage_weighted_delta_abs", 0) <= -0.15:
                    main = "Ограничение локализации"
                elif pd.notna(r.get("category_demand_delta_pct", np.nan)) and r.get("category_demand_delta_pct", 0) <= -0.08:
                    main = "Снижение рыночного спроса"
                elif (pd.notna(r.get("visibility_pct_delta_pct", np.nan)) and r.get("visibility_pct_delta_pct", 0) <= -0.08) or (pd.notna(r.get("median_position_delta_abs", np.nan)) and r.get("median_position_delta_abs", 0) > 1):
                    main = "Потеря поисковой доли"
                elif pd.notna(r.get("ctr_total_delta_pct", np.nan)) and r.get("ctr_total_delta_pct", 0) <= -0.05:
                    main = "Снижение CTR"
                elif pd.notna(r.get("cr_click_to_order_delta_pct", np.nan)) and r.get("cr_click_to_order_delta_pct", 0) <= -0.08:
                    main = "Снижение конверсии"
                elif price_assess:
                    main = "Ценовой фактор"
                else:
                    main = "Изменение заказов"
                if ad_assess in {"Эффективно", "Частично эффективно", "Неэффективно", "Защитно"}:
                    sec = f"Реклама: {ad_assess}"
            else:
                if pd.notna(r.get("ads_unit_delta_pct", np.nan)) and r.get("ads_unit_delta_pct", 0) >= 0.10 and pd.notna(r.get("gp_per_order_delta_pct", np.nan)) and r.get("gp_per_order_delta_pct", 0) <= -0.05:
                    main = "Реклама съела прибыль"
                elif pd.notna(r.get("logistics_direct_unit_delta_pct", np.nan)) and r.get("logistics_direct_unit_delta_pct", 0) >= 0.10:
                    main = "Рост логистики"
                elif pd.notna(r.get("commission_unit_delta_pct", np.nan)) and r.get("commission_unit_delta_pct", 0) >= 0.05:
                    main = "Рост комиссии"
                elif price_assess:
                    main = "Ценовой фактор"
                else:
                    main = "Изменение экономики"
                if pd.notna(r.get("abc_orders_delta_pct", np.nan)) and abs(r.get("abc_orders_delta_pct", 0)) >= 0.08:
                    sec = "Изменение объема заказов"
            reasons_main.append(main)
            reasons_secondary.append(sec)
        df["main_reason"] = reasons_main
        df["secondary_reason"] = reasons_secondary
        df["ad_assessment"] = ad_assessment
        df["price_assessment"] = price_assessment
        return df

    def aggregate_period(self, article_period: pd.DataFrame, level_col: str) -> pd.DataFrame:
        if article_period.empty:
            return pd.DataFrame()
        rows = []
        for lvl, g in article_period.groupby(level_col, dropna=False):
            if normalize_excluded_code(lvl) in EXCLUDED_CODES:
                continue
            row = {level_col: lvl}
            # carry subject for product, etc.
            if level_col == "code":
                subject_mode = g["subject"].mode()
                row["subject"] = subject_mode.iloc[0] if not subject_mode.empty else ""
            for suffix in ["prev", "cur"]:
                weight_gp = pd.to_numeric(g[f"abc_gp_{suffix}"], errors="coerce").fillna(0)
                weight_orders = pd.to_numeric(g[f"abc_orders_{suffix}"], errors="coerce").fillna(0)
                for c in ["abc_revenue","abc_gp","abc_orders","clicks_total","impressions_total","open_card_count","cart_count","orders_funnel","buyouts_funnel","cancel_funnel","search_frequency","search_queries_count","ad_spend","ad_clicks","ad_orders","ad_impressions"]:
                    row[f"{c}_{suffix}"] = pd.to_numeric(g[f"{c}_{suffix}"], errors="coerce").fillna(0).sum() if f"{c}_{suffix}" in g.columns else np.nan
                for c in ["finishedPrice_avg","priceWithDisc_avg","spp_avg","median_position","visibility_pct","ad_ctr","ad_cpc","ctr_total","cr_click_to_order","conv_to_cart","conv_cart_to_order","drr_pct","margin_pct","profitability_pct","finishedPrice_rrp_coeff","priceWithDisc_rrp_coeff","localization_coverage_weighted","localization_coverage_count","commission_unit","acquiring_unit","logistics_direct_unit","logistics_return_unit","storage_unit","acceptance_unit","ads_unit","other_unit","cost_unit","gp_unit","np_unit","category_demand"]:
                    col = f"{c}_{suffix}"
                    if col in g.columns:
                        vals = pd.to_numeric(g[col], errors="coerce")
                        w = weight_orders if c not in {"median_position","visibility_pct","category_demand","finishedPrice_rrp_coeff","priceWithDisc_rrp_coeff","localization_coverage_weighted","localization_coverage_count"} else weight_gp.replace(0, np.nan)
                        if w.fillna(0).sum() > 0:
                            row[col] = np.average(vals.fillna(0), weights=w.fillna(0))
                        else:
                            row[col] = vals.mean()
                row[f"gp_per_order_{suffix}"] = safe_div(row.get(f"abc_gp_{suffix}", np.nan), row.get(f"abc_orders_{suffix}", np.nan))
                row[f"revenue_per_order_{suffix}"] = safe_div(row.get(f"abc_revenue_{suffix}", np.nan), row.get(f"abc_orders_{suffix}", np.nan))
            rows.append(row)
        out = pd.DataFrame(rows)
        out = self.add_deltas(out)
        out = self.classify_reasons(out, level=level_col)
        sort_col = "abc_gp_cur" if "abc_gp_cur" in out.columns else out.columns[0]
        return out.sort_values(sort_col, ascending=False)

    def build_sku_contrib(self) -> pd.DataFrame:
        if self.article_period.empty:
            return pd.DataFrame()
        ap = self.article_period.copy()
        ap["gp_delta"] = ap["abc_gp_delta_abs"]
        prod_delta = ap.groupby("code", dropna=False)["gp_delta"].sum().reset_index().rename(columns={"gp_delta":"product_gp_delta"})
        x = ap.merge(prod_delta, on="code", how="left")
        x["contribution_to_product_gp"] = x.apply(lambda r: safe_div(r["gp_delta"], r["product_gp_delta"]), axis=1)
        cols = ["code","supplier_article","abc_gp_prev","abc_gp_cur","gp_delta","abc_orders_prev","abc_orders_cur","abc_orders_delta_abs","abc_revenue_prev","abc_revenue_cur","abc_revenue_delta_abs","contribution_to_product_gp","main_reason"]
        return x[[c for c in cols if c in x.columns]].sort_values(["code","gp_delta"], ascending=[True,False])

    def build_channels(self) -> pd.DataFrame:
        if self.data.entry_points_sku.empty:
            return pd.DataFrame()
        e = self.data.entry_points_sku.copy()
        if "week_start" not in e.columns:
            return pd.DataFrame()
        rows = []
        for pn in ["prev_14d","cur_14d"]:
            if pn == "cur_14d":
                start, end = self.windows["cur_start"], self.windows["cur_end"]
            else:
                start, end = self.windows["prev_start"], self.windows["prev_end"]
            x = e[(pd.to_datetime(e["week_start"], errors="coerce") <= end) & (pd.to_datetime(e["week_end"], errors="coerce") >= start)].copy()
            x["period_name"] = pn
            x["supplier_article"] = x["supplier_article"].map(clean_article)
            x["code"] = x["supplier_article"].map(extract_code)
            x = x[~x["code"].map(normalize_excluded_code).isin(EXCLUDED_CODES)].copy()
            g = x.groupby(["period_name","supplier_article","code","subject","entry_point"], dropna=False).agg(
                impressions=("impressions","sum"),
                clicks=("clicks","sum"),
                orders=("orders","sum"),
                ctr=("ctr","mean"),
                conv_order=("conv_order","mean"),
            ).reset_index()
            rows.append(g)
        if not rows:
            return pd.DataFrame()
        both = pd.concat(rows, ignore_index=True)
        prev = both[both["period_name"]=="prev_14d"]
        cur = both[both["period_name"]=="cur_14d"]
        out = prev.merge(cur, on=["supplier_article","code","subject","entry_point"], how="outer", suffixes=("_prev","_cur"))
        out["orders_delta_abs"] = out["orders_cur"].fillna(0) - out["orders_prev"].fillna(0)
        return out.sort_values(["supplier_article","orders_delta_abs"], ascending=[True,False])

    def build_localization_daily(self) -> pd.DataFrame:
        if self.data.stocks.empty or self.data.orders.empty:
            return pd.DataFrame()
        stocks = self.data.stocks.copy()
        stocks["supplier_article"] = stocks["supplier_article"].map(clean_article)
        stocks["code"] = stocks["supplier_article"].map(extract_code)
        stocks = stocks[~stocks["code"].map(normalize_excluded_code).isin(EXCLUDED_CODES)].copy()
        # choose main warehouses from latest current period snapshot by cumulative 97%
        latest_week_end = pd.to_datetime(stocks["week_end"], errors="coerce").max()
        ref = stocks[stocks["week_end"] == latest_week_end].copy()
        main_rows = []
        for art, g in ref.groupby("supplier_article", dropna=False):
            s = g.groupby("warehouse", dropna=False)["stock_available"].sum().sort_values(ascending=False)
            total = s.sum()
            cum = 0
            for wh, qty in s.items():
                cum += qty
                main_rows.append({"supplier_article": art, "warehouse": wh, "warehouse_weight_stock": safe_div(qty, total), "is_main": 1})
                if total > 0 and cum / total >= 0.97:
                    break
        main = pd.DataFrame(main_rows)
        if main.empty:
            return pd.DataFrame()
        # avg orders per warehouse/day over last 28d
        o = self.data.orders.copy()
        o["day"] = pd.to_datetime(o["day"], errors="coerce").dt.normalize()
        lookback_start = self.windows["cur_end"] - pd.Timedelta(days=27)
        o = o[(o["day"] >= lookback_start) & (o["day"] <= self.windows["cur_end"])].copy()
        o["supplier_article"] = o["supplier_article"].map(clean_article)
        o["warehouse"] = o["warehouse"].map(normalize_text)
        ow = o.groupby(["supplier_article","warehouse"], dropna=False)["orders"].sum().reset_index()
        ow["avg_orders_per_day_warehouse"] = ow["orders"] / 28.0
        # expand weekly stocks snapshots to daily rows
        rows = []
        for _, r in stocks.iterrows():
            ws = pd.to_datetime(r["week_start"], errors="coerce")
            we = pd.to_datetime(r["week_end"], errors="coerce")
            if pd.isna(ws) or pd.isna(we):
                continue
            start = max(ws, self.windows["prev_start"])
            end = min(we, self.windows["cur_end"])
            if start > end:
                continue
            for d in pd.date_range(start, end, freq="D"):
                rows.append({
                    "day": d.normalize(),
                    "supplier_article": r["supplier_article"],
                    "warehouse": normalize_text(r["warehouse"]),
                    "stock_qty": r["stock_available"],
                })
        loc = pd.DataFrame(rows)
        if loc.empty:
            return pd.DataFrame()
        loc = loc.merge(main[["supplier_article","warehouse","is_main","warehouse_weight_stock"]], on=["supplier_article","warehouse"], how="inner")
        loc = loc.merge(ow[["supplier_article","warehouse","avg_orders_per_day_warehouse"]], on=["supplier_article","warehouse"], how="left")
        loc["avg_orders_per_day_warehouse"] = loc["avg_orders_per_day_warehouse"].fillna(0)
        loc["coverage_days"] = loc.apply(lambda r: safe_div(r["stock_qty"], r["avg_orders_per_day_warehouse"]), axis=1)
        loc["is_available_flag"] = np.where(
            (loc["avg_orders_per_day_warehouse"] > 0) & (loc["stock_qty"] < loc["avg_orders_per_day_warehouse"]), 0, 1
        )
        loc["period_name"] = loc["day"].map(self._period_name)
        # weights by warehouse order share
        order_share = ow.groupby("supplier_article", dropna=False)["orders"].sum().reset_index().rename(columns={"orders":"orders_total"})
        loc = loc.merge(order_share, on="supplier_article", how="left")
        loc["warehouse_weight"] = np.where(
            loc["orders_total"].fillna(0) > 0,
            (loc["avg_orders_per_day_warehouse"] * 28.0) / loc["orders_total"].replace(0, np.nan),
            loc["warehouse_weight_stock"],
        )
        loc["warehouse_weight"] = loc["warehouse_weight"].fillna(loc["warehouse_weight_stock"]).fillna(0)
        dayagg = loc.groupby(["day","supplier_article","period_name"], dropna=False).agg(
            weighted_available=("is_available_flag", lambda s: np.nan),
            available_count_pct=("is_available_flag","mean"),
        ).reset_index()
        # weighted available manually
        wa = loc.groupby(["day","supplier_article","period_name"], dropna=False).apply(
            lambda g: np.average(g["is_available_flag"], weights=g["warehouse_weight"].fillna(0)) if g["warehouse_weight"].fillna(0).sum() > 0 else g["is_available_flag"].mean()
        ).reset_index(name="weighted_available")
        dayagg = dayagg.drop(columns=["weighted_available"]).merge(wa, on=["day","supplier_article","period_name"], how="left")
        detailed = loc.merge(dayagg, on=["day","supplier_article","period_name"], how="left")
        return detailed.sort_values(["supplier_article","day","warehouse"])

    def build_example(self, article: str) -> pd.DataFrame:
        if self.article_period.empty:
            return pd.DataFrame()
        x = self.article_period[self.article_period["supplier_article"].astype(str).str.lower() == article.lower()].copy()
        if x.empty and article.lower() == "901/5":
            x = self.article_period[self.article_period["supplier_article"].astype(str).str.lower().isin(["901/5","pt901.f05"])].copy()
        return x

# -------------------------
# Writer
# -------------------------

TITLE_FILL = PatternFill("solid", fgColor="D9EAF7")
HEADER_FILL = PatternFill("solid", fgColor="EAF2F8")
THIN_BORDER = Border(
    left=Side(style="thin", color="C0C0C0"),
    right=Side(style="thin", color="C0C0C0"),
    top=Side(style="thin", color="C0C0C0"),
    bottom=Side(style="thin", color="C0C0C0"),
)

class ReportWriter:
    def __init__(self) -> None:
        self.wb = Workbook()
        self.wb.remove(self.wb.active)

    def add_df_sheet(self, name: str, df: pd.DataFrame) -> None:
        ws = self.wb.create_sheet(self._safe_title(name))
        if df is None or df.empty:
            ws["A1"] = "Нет данных"
            return
        x = df.copy()
        x = x.replace([np.inf, -np.inf], np.nan).where(pd.notna(x), "")
        headers = list(x.columns)
        for j, h in enumerate(headers, 1):
            c = ws.cell(1, j, h)
            c.fill = HEADER_FILL
            c.border = THIN_BORDER
            c.font = Font(bold=True)
            c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        for i, row in enumerate(x.itertuples(index=False), 2):
            for j, v in enumerate(row, 1):
                c = ws.cell(i, j, v)
                c.border = THIN_BORDER
                c.alignment = Alignment(horizontal="center", vertical="center")
        self._format_sheet(ws)

    def _format_sheet(self, ws) -> None:
        headers = {ws.cell(1, j).value: j for j in range(1, ws.max_column + 1)}
        money_like = ["ВП", "Выручка", "прибыль", "расход", "цена", "price", "rrp", "комиссия", "логистика", "себестоимость"]
        pct_like = ["pct", "%", "coeff", "coverage", "ctr", "conv", "spp", "visibility"]
        for col_cells in ws.columns:
            max_len = max(len(normalize_text(c.value)) for c in col_cells[: min(len(col_cells), 200)]) if col_cells else 10
            ws.column_dimensions[get_column_letter(col_cells[0].column)].width = min(max(max_len + 2, 12), 28)
        for row in range(2, ws.max_row + 1):
            for header, idx in headers.items():
                cell = ws.cell(row, idx)
                h = normalize_text(header).lower()
                if isinstance(cell.value, (int, float)) and not isinstance(cell.value, bool):
                    if any(k in h for k in money_like):
                        cell.number_format = '# ##0 "₽"'
                    elif any(k in h for k in pct_like):
                        cell.number_format = '0.00%'
                    else:
                        cell.number_format = '0'
                if "дата" in h or "day" in h:
                    cell.number_format = 'DD.MM.YYYY'
        ws.freeze_panes = "A2"

    def _safe_title(self, name: str) -> str:
        bad = r'[]:*?/\\'
        cleaned = "".join("_" if ch in bad else ch for ch in name)[:31]
        return cleaned or "Sheet"

def workbook_to_bytes(wb: Workbook) -> bytes:
    bio = io.BytesIO()
    wb.save(bio)
    return bio.getvalue()

# -------------------------
# Main
# -------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="WB combined report with part 2")
    p.add_argument("--root", default=".", help="Project root")
    p.add_argument("--reports-root", default="Отчёты", help="Reports root")
    p.add_argument("--store", default="TOPFACE", help="Store")
    p.add_argument("--out-subdir", default="Отчёты/Объединенный отчет/TOPFACE", help="Output dir")
    return p.parse_args()

def main() -> int:
    args = parse_args()
    storage = make_storage(args.root)
    loader = DataLoader(storage=storage, store=args.store, reports_root=args.reports_root)
    log("Loading data")
    data = loader.load_all()
    log("Building analytics")
    analyzer = Part2Analyzer(data)

    writer = ReportWriter()
    writer.add_df_sheet("Результаты_категории", analyzer.category_period)
    writer.add_df_sheet("Результаты_товары", analyzer.product_period)
    writer.add_df_sheet("Результаты_артикулы", analyzer.article_period)
    writer.add_df_sheet("Вклад_SKU_в_товар", analyzer.sku_contrib)
    writer.add_df_sheet("Каналы_входа", analyzer.channels)
    writer.add_df_sheet("Локализация_daily", analyzer.localization_daily)
    writer.add_df_sheet("Пример_901_5", analyzer.example_901_5)
    if data.warnings:
        writer.add_df_sheet("Warnings", pd.DataFrame({"warning": data.warnings}))
    if data.diagnostics:
        writer.add_df_sheet("Diagnostics", pd.DataFrame([{
            "dataset": d.dataset,
            "file_path": d.file_path,
            "sheet_name": d.sheet_name,
            "header_row_excel": d.header_row_excel,
            "rows": d.rows,
            "error": d.error,
        } for d in data.diagnostics]))

    stamp = datetime.now().strftime("%Y-%m-%d")
    out_main = f"{args.out_subdir}/Объединенный_отчет_{args.store}_{stamp}.xlsx"
    storage.write_bytes(out_main, workbook_to_bytes(writer.wb))
    log(f"Saved report: {out_main}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
