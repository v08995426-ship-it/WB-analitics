
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import calendar
import io
import math
import os
import re
import sys
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
# Logging
# -------------------------

def log(message: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {message}", flush=True)


# -------------------------
# Helpers
# -------------------------

def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("\xa0", " ").strip()
    text = re.sub(r"\s+", " ", text)
    return text


def norm_key(value: Any) -> str:
    text = normalize_text(value).lower()
    text = text.replace("ё", "е")
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
    m = re.match(r"^([A-Za-zА-Яа-я0-9]+)", text)
    return m.group(1) if m else text


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
    if pd.isna(prev) or prev == 0:
        return np.nan
    if pd.isna(cur):
        return np.nan
    return (float(cur) - float(prev)) / float(prev)


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


def parse_search_history_period_from_name(name: str) -> Tuple[Optional[date], Optional[date]]:
    m = re.search(r"Запросы (\d{2})\.(\d{2})\.(\d{4})-(\d{2})\.(\d{2})\.(\d{4})", name)
    if not m:
        return None, None
    start = date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    end = date(int(m.group(6)), int(m.group(5)), int(m.group(4)))
    return start, end


def parse_subject_from_history_name(name: str) -> str:
    m = re.search(r"Предметы - (.+?) - Запросы", name)
    return m.group(1).strip() if m else ""


def unique_preserve(items: Iterable[Any]) -> List[Any]:
    out = []
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
        if counts[base] == 1:
            result.append(base)
        else:
            result.append(f"{base}__{counts[base]}")
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
        if df.empty:
            score = -1000
        else:
            score = len([c for c in df.columns if normalize_text(c)])
            if expected_aliases:
                score += required_score(df.columns, expected_aliases) * 100
        if score > best_score:
            best_score = score
            best_df = df
            best_header = header

    if best_df is None:
        raise ValueError(f"Не удалось прочитать Excel: {filename}")
    best_df.columns = dedupe_columns(best_df.columns)
    return best_df, str(sheet), best_header


def rename_using_aliases(df: pd.DataFrame, alias_map: Dict[str, List[str]]) -> Tuple[pd.DataFrame, Dict[str, Optional[str]]]:
    norm_existing = {}
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
        elif chosen == target:
            pass
        else:
            out[target] = np.nan
    return out, mapping


def any_date_columns(df: pd.DataFrame) -> List[str]:
    out = []
    for c in df.columns:
        if re.match(r"^\d{2}\.\d{2}\.\d{4}$", normalize_text(c)):
            out.append(c)
    return out


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
# Column dictionaries
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
    "orders": ["Заказы", "Заказали", "orders", "ordersCount", "Кол-во продаж"],
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
    "margin_pct": ["Маржинальность, %"],
    "profitability_pct": ["Рентабельность, %"],
    "abc_class": ["ABC-анализ"],
    "section": ["Раздел"],
    "entry_point": ["Точка входа"],
    "finished_price": ["finishedPrice", "Ср. цена продажи", "Цена с учетом всех скидок, кроме суммы по WB Кошельку"],
    "price_with_disc": ["priceWithDisc", "Цена со скидкой продавца, в том числе со скидкой WB Клуба"],
    "spp": ["SPP", "СПП", "Скидка WB, %"],
}

INPUT_DICTIONARY_ROWS = [
    ("stocks_weekly", "Отчёты/Остатки/<STORE>/Недельные/*.xlsx", "sheet0", "header row 1", "Дата последнего изменения, Склад, Артикул продавца, Артикул WB, Доступно для продажи, Полное количество, Предмет, Бренд"),
    ("search_weekly", "Отчёты/Поисковые запросы/<STORE>/Недельные/*.xlsx", "sheet0", "header row 1", "Дата, [Магазин], Поисковый запрос, Артикул WB, Артикул продавца, Предмет, Бренд, Частота запросов, Медианная позиция, Видимость"),
    ("search_history", "Отчёты/Поисковые запросы/<STORE>/История/*.xlsx", "ag-grid", "header row 1", "Ключевое слово, Кластер WB, Частота WB, Товаров в запросе"),
    ("entry_points_category", "Отчёты/Точки входа/<STORE>/*.xlsx", "Детализация по точкам входа", "header row 2", "Раздел, Точка входа, Показы, Перешли в карточку, CTR, Добавили в корзину, Конверсия в корзину, Заказали, Конверсия в заказ"),
    ("entry_points_sku", "Отчёты/Точки входа/<STORE>/*.xlsx", "Детализация по артикулам", "header row 2", "Раздел, Точка входа, Артикул ВБ, Артикул продавца, Бренд, Название, Предмет, Показы, Перешли в карточку, Заказали"),
    ("ads_daily", "Отчёты/Реклама/<STORE>/Недельные/Реклама_*.xlsx", "Статистика_Ежедневно", "header row 1", "ID кампании, Артикул WB, Название, Название предмета, Дата, Показы, Клики, CTR, CPC, Заказы, CR, Расход, Сумма заказов"),
    ("ads_total", "Отчёты/Реклама/<STORE>/Недельные/Реклама_*.xlsx", "Статистика_Итого", "header row 1", "ID кампании, Артикул WB, Название, Название предмета, Показы, Клики, Заказы, Расход, Сумма заказов, CTR, CPC, CR"),
    ("campaigns", "Отчёты/Реклама/<STORE>/Недельные/Реклама_*.xlsx", "Список_кампаний", "header row 1", "ID кампании, Название, Статус, Тип оплаты, Тип ставки, Ставка в поиске (руб), Ставка в рекомендациях (руб), Название предмета, Артикул WB"),
    ("abc_weekly", "Отчёты/ABC/wb_abc_report_goods__*.xlsx", "sheet0", "header row 1", "Артикул WB, [Артикул продавца], [Бренд], Предмет, ABC-анализ, ДРР, Маржинальность, Рентабельность, Валовая выручка, Кол-во продаж, Валовая прибыль"),
    ("funnel_daily", "Отчёты/Воронка продаж/<STORE>/Воронка продаж.xlsx", "sheet0", "header row 1", "nmID, dt, openCardCount, addToCartCount, ordersCount, ordersSumRub, buyoutsCount, cancelCount, addToCartConversion, cartToOrderConversion"),
    ("stock_history_days", "Отчёты/История остатков/<STORE>/*.xlsx", "Остатки по дням", "header row 2", "Артикул продавца, Название, Артикул WB, Предмет, Бренд, Размер, Склад, <date columns>"),
    ("stock_history_detail", "Отчёты/История остатков/<STORE>/*.xlsx", "Детальная информация", "header row 2", "Артикул продавца, Название, Артикул WB, Предмет, Бренд, Регион, Склад, Доступность, Заказали, Выкупили, Процент выкупа"),
    ("orders_weekly", "Отчёты/Заказы/<STORE>/Недельные/*.xlsx", "sheet0", "auto header", "Дата / Дата заказа, Артикул WB / nmId, Артикул продавца / supplierArticle, finishedPrice / priceWithDisc / spp, Заказы"),
    ("economics", "Отчёты/Финансовые показатели/<STORE>[/Недельные]/Экономика.xlsx", "Юнит экономика or first sheet", "auto header", "Артикул WB / nmId, Артикул продавца / supplierArticle, Предмет, Валовая прибыль / Чистая прибыль / Себестоимость / Комиссия / Логистика / Эквайринг"),
]


# -------------------------
# Diagnostics
# -------------------------

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
    search_history: pd.DataFrame = field(default_factory=pd.DataFrame)
    funnel: pd.DataFrame = field(default_factory=pd.DataFrame)
    ads_daily: pd.DataFrame = field(default_factory=pd.DataFrame)
    ads_total: pd.DataFrame = field(default_factory=pd.DataFrame)
    campaigns: pd.DataFrame = field(default_factory=pd.DataFrame)
    economics: pd.DataFrame = field(default_factory=pd.DataFrame)
    abc: pd.DataFrame = field(default_factory=pd.DataFrame)
    entry_points_category: pd.DataFrame = field(default_factory=pd.DataFrame)
    entry_points_sku: pd.DataFrame = field(default_factory=pd.DataFrame)
    stock_history_days: pd.DataFrame = field(default_factory=pd.DataFrame)
    stock_history_detail: pd.DataFrame = field(default_factory=pd.DataFrame)
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
        self._record(
            FileLoadInfo(
                dataset=dataset,
                file_path=path,
                sheet_name=sheet,
                header_row_excel=header + 1,
                rows=len(df),
                columns=[normalize_text(c) for c in raw.columns],
                mapping=mapping,
            )
        )
        return df

    def load_orders(self) -> pd.DataFrame:
        files = self._list_under([
            self._prefix("Заказы", self.store, "Недельные"),
            self._prefix("Заказы", self.store),
        ])
        dfs = []
        alias_map = {
            **COMMON_ALIASES,
            "is_cancel": ["isCancel", "Отмена заказа"],
        }
        for path in files:
            try:
                df = self._read_and_standardize("orders", path, None, (0, 1, 2), alias_map)
                week_code = parse_week_code_from_name(Path(path).name)
                start, end = week_bounds_from_code(week_code) if week_code else (None, None)
                df["week_code"] = week_code
                df["week_start"] = pd.Timestamp(start) if start else pd.NaT
                df["week_end"] = pd.Timestamp(end) if end else pd.NaT
                df["day"] = to_dt(df["day"]).dt.normalize()
                df["nm_id"] = to_numeric(df["nm_id"])
                df["supplier_article"] = df["supplier_article"].map(clean_article)
                df["subject"] = df["subject"].map(normalize_text)
                df["finished_price"] = to_numeric(df["finished_price"])
                df["price_with_disc"] = to_numeric(df["price_with_disc"])
                df["spp"] = to_numeric(df["spp"])
                # derive order count if explicit orders missing
                df["orders"] = to_numeric(df["orders"])
                if df["orders"].isna().all():
                    df["orders"] = 1
                dfs.append(df)
            except Exception as e:
                self._record(FileLoadInfo(dataset="orders", file_path=path, sheet_name="", header_row_excel=0, rows=0, columns=[], error=str(e)))
                log(f"Failed to load orders {path}: {e}")
        if not dfs:
            self.warnings.append("Файлы заказов не найдены или не прочитались.")
            return pd.DataFrame()
        out = pd.concat(dfs, ignore_index=True)
        out = out[out["day"].notna()].copy()
        return out

    def load_stocks(self) -> pd.DataFrame:
        files = self._list_under([self._prefix("Остатки", self.store, "Недельные")])
        files = files[-8:]
        dfs = []
        for path in files:
            try:
                log(f"  stocks file: {Path(path).name}")
                df = self._read_and_standardize("stocks", path, None, (0,), COMMON_ALIASES)
                week_code = parse_week_code_from_name(Path(path).name)
                start, end = week_bounds_from_code(week_code) if week_code else (None, None)
                df["week_code"] = week_code
                df["week_start"] = pd.Timestamp(start) if start else pd.NaT
                df["week_end"] = pd.Timestamp(end) if end else pd.NaT
                df["nm_id"] = to_numeric(df["nm_id"])
                df["supplier_article"] = df["supplier_article"].map(clean_article)
                df["warehouse"] = df["warehouse"].map(normalize_text)
                df["subject"] = df["subject"].map(normalize_text)
                df["brand"] = df["brand"].map(normalize_text)
                df["stock_available"] = to_numeric(df["stock_available"]).fillna(0)
                df["stock_total"] = to_numeric(df["stock_total"]).fillna(df["stock_available"]).fillna(0)
                dfs.append(df)
            except Exception as e:
                self._record(FileLoadInfo(dataset="stocks", file_path=path, sheet_name="", header_row_excel=0, rows=0, columns=[], error=str(e)))
                log(f"Failed to load stocks {path}: {e}")
        if not dfs:
            self.warnings.append("Файлы остатков не найдены или не прочитались.")
            return pd.DataFrame()
        return pd.concat(dfs, ignore_index=True)

    def load_search(self) -> pd.DataFrame:
        files = self._list_under([self._prefix("Поисковые запросы", self.store, "Недельные")])
        files = files[-13:]
        dfs = []
        for path in files:
            try:
                log(f"  search file: {Path(path).name}")
                df = self._read_and_standardize("search", path, None, (0,), COMMON_ALIASES)
                week_code = parse_week_code_from_name(Path(path).name)
                start, end = week_bounds_from_code(week_code) if week_code else (None, None)
                df["week_code"] = week_code
                df["week_start"] = pd.Timestamp(start) if start else pd.NaT
                df["week_end"] = pd.Timestamp(end) if end else pd.NaT
                df["day"] = to_dt(df["day"]).dt.normalize()
                df["nm_id"] = to_numeric(df["nm_id"])
                df["supplier_article"] = df["supplier_article"].map(clean_article)
                df["subject"] = df["subject"].map(normalize_text)
                df["brand"] = df["brand"].map(normalize_text)
                for c in ["frequency", "frequency_week", "median_position", "visibility_pct"]:
                    df[c] = to_numeric(df[c])
                dfs.append(df)
            except Exception as e:
                self._record(FileLoadInfo(dataset="search", file_path=path, sheet_name="", header_row_excel=0, rows=0, columns=[], error=str(e)))
                log(f"Failed to load search {path}: {e}")
        if not dfs:
            self.warnings.append("Файлы поисковых запросов (недельные) не найдены или не прочитались.")
            return pd.DataFrame()
        return pd.concat(dfs, ignore_index=True)

    def load_search_history(self) -> pd.DataFrame:
        files = self._list_under([
            self._prefix("Поисковые запросы", self.store, "История"),
            self._prefix("Поисковые запросы история"),
        ])
        files = files[-24:]
        dfs = []
        alias_map = {
            "query": ["Ключевое слово"],
            "cluster_wb": ["Кластер WB"],
            "frequency": ["Частота WB"],
            "items_in_query": ["Товаров в запросе"],
        }
        for path in files:
            try:
                df = self._read_and_standardize("search_history", path, ["ag-grid"], (0,), alias_map)
                start, end = parse_search_history_period_from_name(Path(path).name)
                df["period_start"] = pd.Timestamp(start) if start else pd.NaT
                df["period_end"] = pd.Timestamp(end) if end else pd.NaT
                df["subject"] = parse_subject_from_history_name(Path(path).name)
                df["query"] = df["query"].map(normalize_text)
                df["cluster_wb"] = df["cluster_wb"].map(normalize_text)
                df["frequency"] = to_numeric(df["frequency"])
                df["items_in_query"] = to_numeric(df["items_in_query"])
                dfs.append(df)
            except Exception as e:
                self._record(FileLoadInfo(dataset="search_history", file_path=path, sheet_name="", header_row_excel=0, rows=0, columns=[], error=str(e)))
                log(f"Failed to load search history {path}: {e}")
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
            self.warnings.append("Файл воронки продаж не найден.")
            return pd.DataFrame()
        try:
            df = self._read_and_standardize("funnel", path, None, (0,), COMMON_ALIASES)
            df["day"] = to_dt(df["day"]).dt.normalize()
            df["nm_id"] = to_numeric(df["nm_id"])
            for c in ["open_card_count", "cart", "orders", "buyouts_count", "cancel_count", "conv_cart", "conv_order"]:
                df[c] = to_numeric(df[c])
            return df
        except Exception as e:
            self._record(FileLoadInfo(dataset="funnel", file_path=path, sheet_name="", header_row_excel=0, rows=0, columns=[], error=str(e)))
            self.warnings.append(f"Файл воронки продаж не прочитался: {e}")
            return pd.DataFrame()

    def load_ads(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        files = self._list_under([
            self._prefix("Реклама", self.store, "Недельные"),
            self._prefix("Реклама", self.store),
        ])
        files = files[-16:]
        daily_dfs: List[pd.DataFrame] = []
        total_dfs: List[pd.DataFrame] = []
        campaign_dfs: List[pd.DataFrame] = []

        for path in files:
            log(f"  ads workbook: {Path(path).name}")
            name = Path(path).name
            if not name.lower().endswith(".xlsx"):
                continue
            week_code = parse_week_code_from_name(name)
            start, end = week_bounds_from_code(week_code) if week_code else (None, None)

            for dataset, preferred_sheet, target_list in [
                ("ads_daily", ["Статистика_Ежедневно"], daily_dfs),
                ("ads_total", ["Статистика_Итого"], total_dfs),
                ("campaigns", ["Список_кампаний"], campaign_dfs),
            ]:
                try:
                    df = self._read_and_standardize(dataset, path, preferred_sheet, (0,), COMMON_ALIASES)
                    df["week_code"] = week_code
                    df["week_start"] = pd.Timestamp(start) if start else pd.NaT
                    df["week_end"] = pd.Timestamp(end) if end else pd.NaT
                    if "day" in df.columns:
                        df["day"] = to_dt(df["day"]).dt.normalize()
                    df["nm_id"] = to_numeric(df["nm_id"])
                    df["campaign_id"] = to_numeric(df["campaign_id"])
                    for c in ["impressions", "clicks", "ctr", "cpc", "orders", "conv_order", "spend", "bid_search", "bid_reco"]:
                        df[c] = to_numeric(df[c])
                    df["campaign_name"] = df["campaign_name"].map(normalize_text)
                    df["status"] = df["status"].map(normalize_text)
                    df["payment_type"] = df["payment_type"].map(normalize_text)
                    df["subject"] = df["subject"].map(normalize_text)
                    target_list.append(df)
                except Exception as e:
                    self._record(FileLoadInfo(dataset=dataset, file_path=path, sheet_name="", header_row_excel=0, rows=0, columns=[], error=str(e)))
                    log(f"Failed to load {dataset} from {path}: {e}")

        if not daily_dfs:
            self.warnings.append("Файлы рекламы не найдены или не прочитались.")
        return (
            pd.concat(daily_dfs, ignore_index=True) if daily_dfs else pd.DataFrame(),
            pd.concat(total_dfs, ignore_index=True) if total_dfs else pd.DataFrame(),
            pd.concat(campaign_dfs, ignore_index=True) if campaign_dfs else pd.DataFrame(),
        )

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
            self.warnings.append("Файл экономики не найден.")
            return pd.DataFrame()
        try:
            df = self._read_and_standardize("economics", path, ["Юнит экономика"], (0, 1, 2), COMMON_ALIASES)
            df["nm_id"] = to_numeric(df["nm_id"])
            df["supplier_article"] = df["supplier_article"].map(clean_article)
            df["subject"] = df["subject"].map(normalize_text)
            for c in ["gross_profit"]:
                df[c] = to_numeric(df[c])
            return df
        except Exception as e:
            self._record(FileLoadInfo(dataset="economics", file_path=path, sheet_name="", header_row_excel=0, rows=0, columns=[], error=str(e)))
            self.warnings.append(f"Файл экономики не прочитался: {e}")
            return pd.DataFrame()

    def load_abc(self) -> pd.DataFrame:
        files = self._list_under([self._prefix("ABC")], must_contain=["wb_abc_report_goods__"])
        files = files[-16:]
        dfs = []
        for path in files:
            try:
                log(f"  abc file: {Path(path).name}")
                df = self._read_and_standardize("abc", path, None, (0,), COMMON_ALIASES)
                start, end = parse_abc_period_from_name(Path(path).name)
                df["week_start"] = pd.Timestamp(start) if start else pd.NaT
                df["week_end"] = pd.Timestamp(end) if end else pd.NaT
                df["week_code"] = week_code_from_date(start) if start else None
                df["nm_id"] = to_numeric(df["nm_id"])
                df["supplier_article"] = df["supplier_article"].map(clean_article)
                df["subject"] = df["subject"].map(normalize_text)
                df["brand"] = df["brand"].map(normalize_text)
                for c in ["gross_profit", "gross_revenue", "orders", "drr_pct", "margin_pct", "profitability_pct"]:
                    df[c] = to_numeric(df[c])
                dfs.append(df)
            except Exception as e:
                self._record(FileLoadInfo(dataset="abc", file_path=path, sheet_name="", header_row_excel=0, rows=0, columns=[], error=str(e)))
                log(f"Failed to load abc {path}: {e}")
        if not dfs:
            self.warnings.append("Файлы ABC не найдены или не прочитались.")
            return pd.DataFrame()
        return pd.concat(dfs, ignore_index=True)

    def load_entry_points(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        files = self._list_under([self._prefix("Точки входа", self.store)])
        files = files[-16:]
        cat_dfs: List[pd.DataFrame] = []
        sku_dfs: List[pd.DataFrame] = []
        for path in files:
            log(f"  entry workbook: {Path(path).name}")
            start, end = parse_entry_period_from_name(Path(path).name)
            week_code = week_code_from_date(start) if start else None
            for dataset, sheet_names, target in [
                ("entry_points_category", ["Детализация по точкам входа"], cat_dfs),
                ("entry_points_sku", ["Детализация по артикулам"], sku_dfs),
            ]:
                try:
                    df = self._read_and_standardize(dataset, path, sheet_names, (1,), COMMON_ALIASES)
                    df["week_start"] = pd.Timestamp(start) if start else pd.NaT
                    df["week_end"] = pd.Timestamp(end) if end else pd.NaT
                    df["week_code"] = week_code
                    df["nm_id"] = to_numeric(df["nm_id"])
                    df["supplier_article"] = df["supplier_article"].map(clean_article)
                    df["subject"] = df["subject"].map(normalize_text)
                    df["brand"] = df["brand"].map(normalize_text)
                    df["section"] = df["section"].map(normalize_text)
                    df["entry_point"] = df["entry_point"].map(normalize_text)
                    for c in ["impressions", "clicks", "ctr", "cart", "conv_cart", "orders", "conv_order"]:
                        df[c] = to_numeric(df[c])
                    target.append(df)
                except Exception as e:
                    self._record(FileLoadInfo(dataset=dataset, file_path=path, sheet_name="", header_row_excel=0, rows=0, columns=[], error=str(e)))
                    log(f"Failed to load {dataset} {path}: {e}")
        if not sku_dfs:
            self.warnings.append("Файлы точек входа не найдены или не прочитались.")
        return (
            pd.concat(cat_dfs, ignore_index=True) if cat_dfs else pd.DataFrame(),
            pd.concat(sku_dfs, ignore_index=True) if sku_dfs else pd.DataFrame(),
        )

    def load_stock_history(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        files = self._list_under([
            self._prefix("История остатков", self.store),
            self._prefix("История остатков"),
        ])
        if not files:
            return pd.DataFrame(), pd.DataFrame()
        path = files[-1]

        # Остатки по дням
        days_df = pd.DataFrame()
        detail_df = pd.DataFrame()

        try:
            raw = pd.read_excel(io.BytesIO(self.storage.read_bytes(path)), sheet_name="Остатки по дням", header=1, dtype=object)
            raw.columns = dedupe_columns(raw.columns)
            self._record(FileLoadInfo(dataset="stock_history_days", file_path=path, sheet_name="Остатки по дням", header_row_excel=2, rows=len(raw), columns=list(raw.columns)))
            date_cols = any_date_columns(raw)
            id_cols = [c for c in raw.columns if c not in date_cols]
            raw = raw.dropna(axis=0, how="all")
            if date_cols:
                melted = raw.melt(id_vars=id_cols, value_vars=date_cols, var_name="day", value_name="stock_qty")
                df, _ = rename_using_aliases(melted, COMMON_ALIASES)
                df["day"] = pd.to_datetime(df["day"], format="%d.%m.%Y", errors="coerce")
                df["nm_id"] = to_numeric(df["nm_id"])
                df["supplier_article"] = df["supplier_article"].map(clean_article)
                df["subject"] = df["subject"].map(normalize_text)
                df["brand"] = df["brand"].map(normalize_text)
                df["warehouse"] = df["warehouse"].map(normalize_text)
                df["stock_qty"] = to_numeric(df["stock_qty"]).fillna(0)
                days_df = df
        except Exception as e:
            self._record(FileLoadInfo(dataset="stock_history_days", file_path=path, sheet_name="Остатки по дням", header_row_excel=2, rows=0, columns=[], error=str(e)))
            log(f"Failed to load stock history days {path}: {e}")

        try:
            raw = pd.read_excel(io.BytesIO(self.storage.read_bytes(path)), sheet_name="Детальная информация", header=1, dtype=object)
            raw.columns = dedupe_columns(raw.columns)
            df, mapping = rename_using_aliases(raw, COMMON_ALIASES)
            self._record(FileLoadInfo(dataset="stock_history_detail", file_path=path, sheet_name="Детальная информация", header_row_excel=2, rows=len(df), columns=list(raw.columns), mapping=mapping))
            df["nm_id"] = to_numeric(df["nm_id"])
            df["supplier_article"] = df["supplier_article"].map(clean_article)
            df["subject"] = df["subject"].map(normalize_text)
            df["brand"] = df["brand"].map(normalize_text)
            df["warehouse"] = df["warehouse"].map(normalize_text)
            df["region"] = df["region"].map(normalize_text)
            detail_df = df
        except Exception as e:
            self._record(FileLoadInfo(dataset="stock_history_detail", file_path=path, sheet_name="Детальная информация", header_row_excel=2, rows=0, columns=[], error=str(e)))
            log(f"Failed to load stock history detail {path}: {e}")

        return days_df, detail_df

    def load_all(self) -> LoadedData:
        log("Loading orders")
        orders = self.load_orders()
        log("Loading stocks")
        stocks = self.load_stocks()
        log("Loading weekly search")
        search = self.load_search()
        log("Loading search history")
        search_history = self.load_search_history()
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
        log("Loading stock history")
        stock_days, stock_detail = self.load_stock_history()

        return LoadedData(
            orders=orders,
            stocks=stocks,
            search=search,
            search_history=search_history,
            funnel=funnel,
            ads_daily=ads_daily,
            ads_total=ads_total,
            campaigns=campaigns,
            economics=economics,
            abc=abc,
            entry_points_category=entry_cat,
            entry_points_sku=entry_sku,
            stock_history_days=stock_days,
            stock_history_detail=stock_detail,
            diagnostics=self.diagnostics,
            warnings=self.warnings,
        )


# -------------------------
# Metric builder
# -------------------------

class MetricsBuilder:
    def __init__(self, data: LoadedData):
        self.data = data
        self.sku_master = self.build_sku_master()

    def build_sku_master(self) -> pd.DataFrame:
        frames = []

        def take(df: pd.DataFrame, cols: List[str]) -> None:
            if df.empty:
                return
            existing = [c for c in cols if c in df.columns]
            if not existing:
                return
            x = df[existing].copy()
            for c in cols:
                if c not in x.columns:
                    x[c] = np.nan
            frames.append(x[cols])

        base_cols = ["nm_id", "supplier_article", "subject", "brand", "title"]
        for df in [
            self.data.search,
            self.data.stocks,
            self.data.abc,
            self.data.entry_points_sku,
            self.data.ads_daily,
            self.data.orders,
            self.data.stock_history_days,
            self.data.stock_history_detail,
        ]:
            take(df, base_cols)

        if not frames:
            return pd.DataFrame(columns=base_cols + ["code"])

        master = pd.concat(frames, ignore_index=True)
        master["nm_id"] = to_numeric(master["nm_id"])
        master["supplier_article"] = master["supplier_article"].map(clean_article)
        master["subject"] = master["subject"].map(normalize_text)
        master["brand"] = master["brand"].map(normalize_text)
        master["title"] = master["title"].map(normalize_text)

        master = master[
            (master["nm_id"].notna()) |
            (master["supplier_article"] != "")
        ].copy()

        master["code"] = master["supplier_article"].map(extract_code)

        # prefer rows with article + subject filled
        master["quality_rank"] = (
            master["supplier_article"].ne("").astype(int) * 4
            + master["subject"].ne("").astype(int) * 2
            + master["title"].ne("").astype(int)
        )

        master = master.sort_values(["quality_rank"], ascending=False)
        master = master.drop_duplicates(subset=["nm_id", "supplier_article"], keep="first")
        master = master.drop(columns=["quality_rank"])
        return master

    def _attach_master(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or self.sku_master.empty:
            return df.copy()
        out = df.copy()
        join_cols = [c for c in ["nm_id", "supplier_article"] if c in out.columns]
        if "nm_id" in join_cols:
            out = out.merge(
                self.sku_master[["nm_id", "supplier_article", "subject", "brand", "title", "code"]]
                .dropna(subset=["nm_id"])
                .drop_duplicates(subset=["nm_id"]),
                on="nm_id",
                how="left",
                suffixes=("", "_m1"),
            )
            for c in ["supplier_article", "subject", "brand", "title", "code"]:
                if f"{c}_m1" in out.columns:
                    out[c] = out[c].where(out[c].notna() & (out[c] != ""), out[f"{c}_m1"])
                    out = out.drop(columns=[f"{c}_m1"])

        if "supplier_article" in join_cols:
            out = out.merge(
                self.sku_master[["supplier_article", "nm_id", "subject", "brand", "title", "code"]]
                .query("supplier_article != ''")
                .drop_duplicates(subset=["supplier_article"]),
                on="supplier_article",
                how="left",
                suffixes=("", "_m2"),
            )
            for c in ["nm_id", "subject", "brand", "title", "code"]:
                if f"{c}_m2" in out.columns:
                    cond = out[c].isna() | (out[c] == "")
                    out[c] = out[c].where(~cond, out[f"{c}_m2"])
                    out = out.drop(columns=[f"{c}_m2"])
        return out

    def build_daily_current(self) -> pd.DataFrame:
        parts = []

        # funnel by day
        if not self.data.funnel.empty:
            f = self.data.funnel.copy()
            f = f[f["day"].notna()].copy()
            grp = f.groupby(["day", "nm_id"], dropna=False).agg(
                open_card_count=("open_card_count", "sum"),
                cart_count=("cart", "sum"),
                orders_day=("orders", "sum"),
                buyouts_day=("buyouts_count", "sum"),
                cancel_day=("cancel_count", "sum"),
                conv_to_cart=("conv_cart", "mean"),
                conv_cart_to_order=("conv_order", "mean"),
            ).reset_index()
            parts.append(grp)

        # orders by day
        orders_agg = pd.DataFrame()
        if not self.data.orders.empty:
            o = self.data.orders.copy()
            o = o[o["day"].notna()].copy()
            grp = o.groupby(["day", "nm_id", "supplier_article"], dropna=False).agg(
                orders_from_orders=("orders", "sum"),
                avg_finished_price_day=("finished_price", "mean"),
                avg_price_with_disc_day=("price_with_disc", "mean"),
                avg_spp_day=("spp", "mean"),
            ).reset_index()
            orders_agg = grp

        # ads by day
        ads_agg = pd.DataFrame()
        if not self.data.ads_daily.empty:
            a = self.data.ads_daily.copy()
            a = a[a["day"].notna()].copy()
            grp = a.groupby(["day", "nm_id"], dropna=False).agg(
                ad_impressions=("impressions", "sum"),
                ad_clicks=("clicks", "sum"),
                ad_orders=("orders", "sum"),
                ad_spend=("spend", "sum"),
                ad_ctr=("ctr", "mean"),
                ad_cpc=("cpc", "mean"),
            ).reset_index()
            ads_agg = grp

        # search by day
        search_agg = pd.DataFrame()
        if not self.data.search.empty:
            s = self.data.search.copy()
            s = s[s["day"].notna()].copy()
            grp = s.groupby(["day", "nm_id", "supplier_article"], dropna=False).agg(
                search_frequency=("frequency", "sum"),
                search_frequency_week=("frequency_week", "sum"),
                median_position=("median_position", "median"),
                visibility_pct=("visibility_pct", "mean"),
                search_queries_count=("query", "nunique"),
            ).reset_index()
            search_agg = grp

        # merge all keys
        all_keys = []
        for df in [orders_agg, ads_agg, search_agg]:
            if not df.empty:
                all_keys.append(df[[c for c in ["day", "nm_id", "supplier_article"] if c in df.columns]].copy())
        if parts:
            base = parts[0]
        else:
            base = pd.concat(all_keys, ignore_index=True) if all_keys else pd.DataFrame(columns=["day", "nm_id", "supplier_article"])
            base = base.drop_duplicates()

        if parts:
            cur = base
        else:
            cur = base

        # if funnel exists and base is funnel aggregate
        if not parts and cur.empty:
            return cur

        if not orders_agg.empty:
            cur = cur.merge(orders_agg, on=[c for c in ["day", "nm_id", "supplier_article"] if c in cur.columns and c in orders_agg.columns], how="outer")
        if not ads_agg.empty:
            cur = cur.merge(ads_agg, on=[c for c in ["day", "nm_id"] if c in cur.columns and c in ads_agg.columns], how="outer")
        if not search_agg.empty:
            cur = cur.merge(search_agg, on=[c for c in ["day", "nm_id", "supplier_article"] if c in cur.columns and c in search_agg.columns], how="outer")

        if parts:
            # merge funnel aggregate at end
            cur = cur.merge(parts[0], on=[c for c in ["day", "nm_id"] if c in cur.columns and c in parts[0].columns], how="outer")

        cur = self._attach_master(cur)

        # latest stock snapshot
        if not self.data.stocks.empty:
            stocks = self.data.stocks.copy()
            stocks = stocks.sort_values(["week_end"]).copy()
            latest_week = stocks["week_end"].max()
            x = stocks[stocks["week_end"] == latest_week].copy()
            x = x.groupby(["nm_id", "supplier_article"], dropna=False).agg(
                stock_available_now=("stock_available", "sum"),
                stock_total_now=("stock_total", "sum"),
                stock_warehouses=("warehouse", "nunique"),
            ).reset_index()
            cur = cur.merge(x, on=[c for c in ["nm_id", "supplier_article"] if c in cur.columns and c in x.columns], how="left")

        # latest abc economics
        if not self.data.abc.empty:
            abc = self.data.abc.copy()
            abc = abc.sort_values(["week_end"]).copy()
            latest_week = abc["week_end"].max()
            x = abc[abc["week_end"] == latest_week].copy()
            x["gross_profit_per_order"] = x.apply(lambda r: safe_div(r.get("gross_profit"), r.get("orders")), axis=1)
            x = x.groupby(["nm_id", "supplier_article"], dropna=False).agg(
                abc_class=("abc_class", "first"),
                drr_pct_latest=("drr_pct", "mean"),
                margin_pct_latest=("margin_pct", "mean"),
                gross_profit_per_order=("gross_profit_per_order", "mean"),
                gross_profit_week_latest=("gross_profit", "sum"),
            ).reset_index()
            cur = cur.merge(x, on=[c for c in ["nm_id", "supplier_article"] if c in cur.columns and c in x.columns], how="left")

        # estimate gp
        if "orders_day" not in cur.columns:
            cur["orders_day"] = cur["orders_from_orders"].fillna(0)
        cur["orders_day"] = cur["orders_day"].fillna(cur.get("orders_from_orders", np.nan)).fillna(0)
        cur["gross_profit_day_est"] = cur["orders_day"] * cur["gross_profit_per_order"].fillna(0)

        for c in [
            "open_card_count", "cart_count", "orders_day", "buyouts_day", "cancel_day",
            "ad_impressions", "ad_clicks", "ad_orders", "ad_spend",
            "search_frequency", "search_frequency_week", "search_queries_count",
            "stock_available_now", "stock_total_now", "stock_warehouses",
        ]:
            if c not in cur.columns:
                cur[c] = 0
            cur[c] = to_numeric(cur[c]).fillna(0)

        for c in ["conv_to_cart", "conv_cart_to_order", "ad_ctr", "ad_cpc", "median_position", "visibility_pct", "avg_finished_price_day", "avg_price_with_disc_day", "avg_spp_day"]:
            if c not in cur.columns:
                cur[c] = np.nan
            cur[c] = to_numeric(cur[c])

        if "day" in cur.columns:
            cur["weekday"] = pd.to_datetime(cur["day"], errors="coerce").dt.weekday

        cur["supplier_article"] = cur["supplier_article"].map(clean_article)
        cur["subject"] = cur["subject"].map(normalize_text)
        cur["brand"] = cur["brand"].map(normalize_text)
        cur["code"] = cur["supplier_article"].map(extract_code)
        cur = cur.sort_values(["day", "subject", "supplier_article", "nm_id"])
        return cur

    def build_daily_targets(self, daily_current: pd.DataFrame) -> pd.DataFrame:
        if daily_current.empty:
            return pd.DataFrame()
        df = daily_current.copy()
        if "day" not in df.columns:
            return pd.DataFrame()
        df["weekday"] = pd.to_datetime(df["day"], errors="coerce").dt.weekday
        # last 90 days best-sustained proxy: avg of rows above median for each weekday and sku
        metric_cols = [
            "orders_day", "gross_profit_day_est", "open_card_count", "ad_impressions",
            "search_frequency", "stock_available_now", "conv_to_cart", "conv_cart_to_order",
            "avg_finished_price_day",
        ]
        rows = []
        group_cols = ["supplier_article", "nm_id", "weekday"]
        for keys, g in df.groupby(group_cols, dropna=False):
            row: Dict[str, Any] = {
                "supplier_article": keys[0],
                "nm_id": keys[1],
                "weekday": keys[2],
            }
            g = g.sort_values("day").tail(90)
            if g.empty:
                continue
            for m in metric_cols:
                s = to_numeric(g[m])
                med = s.median()
                strong = s[s >= med]
                row[f"target_{m}"] = strong.mean() if not strong.empty else s.mean()
            rows.append(row)
        out = pd.DataFrame(rows)
        out = self._attach_master(out)
        return out

    def build_weekly_summary(self) -> pd.DataFrame:
        frames = []

        if not self.data.funnel.empty:
            f = self.data.funnel.copy()
            f["week_code"] = f["day"].map(week_code_from_date)
            g = f.groupby(["week_code", "nm_id"], dropna=False).agg(
                open_card_count=("open_card_count", "sum"),
                cart_count=("cart", "sum"),
                orders_count=("orders", "sum"),
                buyouts_count=("buyouts_count", "sum"),
                cancel_count=("cancel_count", "sum"),
            ).reset_index()
            frames.append(g)

        weekly = frames[0] if frames else pd.DataFrame(columns=["week_code", "nm_id"])

        if not self.data.search.empty:
            s = self.data.search.copy()
            g = s.groupby(["week_code", "nm_id", "supplier_article"], dropna=False).agg(
                search_frequency=("frequency", "sum"),
                median_position=("median_position", "median"),
                visibility_pct=("visibility_pct", "mean"),
                search_queries_count=("query", "nunique"),
            ).reset_index()
            weekly = weekly.merge(g, on=[c for c in ["week_code", "nm_id", "supplier_article"] if c in weekly.columns and c in g.columns], how="outer") if not weekly.empty else g

        if not self.data.ads_daily.empty:
            a = self.data.ads_daily.copy()
            g = a.groupby(["week_code", "nm_id"], dropna=False).agg(
                ad_impressions=("impressions", "sum"),
                ad_clicks=("clicks", "sum"),
                ad_orders=("orders", "sum"),
                ad_spend=("spend", "sum"),
            ).reset_index()
            weekly = weekly.merge(g, on=[c for c in ["week_code", "nm_id"] if c in weekly.columns and c in g.columns], how="outer") if not weekly.empty else g

        if not self.data.entry_points_sku.empty:
            e = self.data.entry_points_sku.copy()
            g = e.groupby(["week_code", "nm_id", "supplier_article"], dropna=False).agg(
                entry_impressions=("impressions", "sum"),
                entry_clicks=("clicks", "sum"),
                entry_orders=("orders", "sum"),
            ).reset_index()
            weekly = weekly.merge(g, on=[c for c in ["week_code", "nm_id", "supplier_article"] if c in weekly.columns and c in g.columns], how="outer") if not weekly.empty else g

        if not self.data.abc.empty:
            a = self.data.abc.copy()
            g = a.groupby(["week_code", "nm_id", "supplier_article"], dropna=False).agg(
                abc_class=("abc_class", "first"),
                gross_profit=("gross_profit", "sum"),
                gross_revenue=("gross_revenue", "sum"),
                abc_orders=("orders", "sum"),
                drr_pct=("drr_pct", "mean"),
                margin_pct=("margin_pct", "mean"),
                profitability_pct=("profitability_pct", "mean"),
            ).reset_index()
            weekly = weekly.merge(g, on=[c for c in ["week_code", "nm_id", "supplier_article"] if c in weekly.columns and c in g.columns], how="outer") if not weekly.empty else g

        if not self.data.orders.empty:
            o = self.data.orders.copy()
            g = o.groupby(["week_code", "nm_id", "supplier_article"], dropna=False).agg(
                orders_from_orders=("orders", "sum"),
                avg_finished_price=("finished_price", "mean"),
                avg_price_with_disc=("price_with_disc", "mean"),
            ).reset_index()
            weekly = weekly.merge(g, on=[c for c in ["week_code", "nm_id", "supplier_article"] if c in weekly.columns and c in g.columns], how="outer") if not weekly.empty else g

        if weekly.empty:
            return weekly

        weekly = self._attach_master(weekly)
        weekly["week_start"] = weekly["week_code"].map(lambda x: week_bounds_from_code(x)[0] if x else None)
        weekly["week_end"] = weekly["week_code"].map(lambda x: week_bounds_from_code(x)[1] if x else None)
        weekly["gross_profit_per_order"] = weekly.apply(lambda r: safe_div(r.get("gross_profit"), r.get("abc_orders")), axis=1)
        return weekly.sort_values(["week_code", "subject", "supplier_article", "nm_id"])

    def build_monthly_summary(self, daily_current: pd.DataFrame) -> pd.DataFrame:
        if daily_current.empty:
            return pd.DataFrame()
        df = daily_current.copy()
        df["month_key"] = pd.to_datetime(df["day"], errors="coerce").dt.to_period("M").astype(str)
        out = df.groupby(["month_key", "nm_id", "supplier_article", "subject", "brand", "code"], dropna=False).agg(
            orders_day=("orders_day", "sum"),
            gross_profit_day_est=("gross_profit_day_est", "sum"),
            ad_spend=("ad_spend", "sum"),
            search_frequency=("search_frequency", "sum"),
            open_card_count=("open_card_count", "sum"),
        ).reset_index()
        return out

    def build_monthly_forecast(self, daily_current: pd.DataFrame) -> pd.DataFrame:
        if daily_current.empty:
            return pd.DataFrame()
        df = daily_current.copy()
        df["day"] = pd.to_datetime(df["day"], errors="coerce")
        df = df[df["day"].notna()].copy()
        if df.empty:
            return pd.DataFrame()
        df["month_key"] = df["day"].dt.to_period("M").astype(str)

        latest_month = max(df["month_key"])
        cur = df[df["month_key"] == latest_month].copy()
        if cur.empty:
            return pd.DataFrame()

        year = int(latest_month.split("-")[0])
        month = int(latest_month.split("-")[1])
        last_day_of_month = calendar.monthrange(year, month)[1]

        result_rows = []
        prev_month_key = (pd.Period(latest_month) - 1).strftime("%Y-%m")

        monthly = self.build_monthly_summary(daily_current)
        prev_map = monthly[monthly["month_key"] == prev_month_key].set_index(["nm_id", "supplier_article"])

        for keys, g in cur.groupby(["nm_id", "supplier_article", "subject", "brand", "code"], dropna=False):
            days_elapsed = g["day"].dt.day.nunique()
            orders_fact = g["orders_day"].sum()
            gp_fact = g["gross_profit_day_est"].sum()
            run_rate_orders = safe_div(orders_fact, days_elapsed)
            run_rate_gp = safe_div(gp_fact, days_elapsed)
            forecast_orders = run_rate_orders * last_day_of_month if pd.notna(run_rate_orders) else np.nan
            forecast_gp = run_rate_gp * last_day_of_month if pd.notna(run_rate_gp) else np.nan

            prev_orders = np.nan
            prev_gp = np.nan
            prev_key = (keys[0], keys[1])
            if prev_key in prev_map.index:
                prev_row = prev_map.loc[prev_key]
                if isinstance(prev_row, pd.DataFrame):
                    prev_row = prev_row.iloc[0]
                prev_orders = prev_row.get("orders_day", np.nan)
                prev_gp = prev_row.get("gross_profit_day_est", np.nan)

            result_rows.append({
                "month_key": latest_month,
                "nm_id": keys[0],
                "supplier_article": keys[1],
                "subject": keys[2],
                "brand": keys[3],
                "code": keys[4],
                "days_elapsed": days_elapsed,
                "orders_fact_mtd": orders_fact,
                "gross_profit_fact_mtd": gp_fact,
                "forecast_orders_month": forecast_orders,
                "forecast_gross_profit_month": forecast_gp,
                "prev_month_orders": prev_orders,
                "prev_month_gross_profit": prev_gp,
                "forecast_vs_prev_month_abs": forecast_gp - prev_gp if pd.notna(forecast_gp) and pd.notna(prev_gp) else np.nan,
                "forecast_vs_prev_month_pct": pct_delta(forecast_gp, prev_gp),
            })
        return pd.DataFrame(result_rows)

    def build_source_summary(self) -> pd.DataFrame:
        rows = []
        for name in [
            "orders", "stocks", "search", "search_history", "funnel",
            "ads_daily", "ads_total", "campaigns", "economics", "abc",
            "entry_points_category", "entry_points_sku",
            "stock_history_days", "stock_history_detail",
        ]:
            df = getattr(self.data, name)
            rows.append({
                "dataset": name,
                "rows": len(df),
                "columns": len(df.columns),
                "is_empty": df.empty,
            })
        return pd.DataFrame(rows)


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
SUBJECT_ORDER = [
    "Кисти косметические",
    "Косметические карандаши",
    "Помады",
    "Блески",
]


class ReportWriter:
    def __init__(self) -> None:
        self.wb = Workbook()
        self.wb.remove(self.wb.active)

    def add_df_sheet(self, name: str, df: pd.DataFrame, limit: Optional[int] = None) -> None:
        ws = self.wb.create_sheet(self._safe_title(name))
        if df is None or df.empty:
            ws["A1"] = "Нет данных"
            return
        x = df.copy()
        if limit is not None:
            x = x.head(limit)
        x = x.replace([np.inf, -np.inf], np.nan)
        x = x.where(pd.notna(x), "")
        headers = list(x.columns)
        for col_idx, col_name in enumerate(headers, 1):
            cell = ws.cell(row=1, column=col_idx, value=col_name)
            cell.fill = HEADER_FILL
            cell.border = THIN_BORDER
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            cell.font = Font(bold=True)
        for row_idx, row in enumerate(x.itertuples(index=False), 2):
            for col_idx, value in enumerate(row, 1):
                cell = ws.cell(row=row_idx, column=col_idx, value=value)
                cell.border = THIN_BORDER
                if isinstance(value, (float, int)) and not isinstance(value, bool):
                    cell.alignment = Alignment(horizontal="right")
        self._autofit(ws)

    def add_title_block(self, sheet_name: str, title: str, rows: List[Tuple[str, Any]]) -> None:
        ws = self.wb.create_sheet(self._safe_title(sheet_name))
        ws["A1"] = title
        ws["A1"].fill = TITLE_FILL
        ws["A1"].font = Font(bold=True, size=14)
        for i, (k, v) in enumerate(rows, start=3):
            ws.cell(i, 1, k).font = Font(bold=True)
            ws.cell(i, 2, v)
        self._autofit(ws)

    def _autofit(self, ws) -> None:
        for col_cells in ws.columns:
            try:
                max_len = max(len(normalize_text(c.value)) for c in col_cells[:200])
            except Exception:
                max_len = 12
            ws.column_dimensions[get_column_letter(col_cells[0].column)].width = min(max(max_len + 2, 10), 45)
        ws.freeze_panes = "A2"

    def _safe_title(self, name: str) -> str:
        bad = r'[]:*?/\\'
        cleaned = "".join("_" if ch in bad else ch for ch in name)
        cleaned = cleaned[:31]
        return cleaned or "Sheet"


def workbook_to_bytes(wb: Workbook) -> bytes:
    bio = io.BytesIO()
    wb.save(bio)
    return bio.getvalue()


# -------------------------
# Combined report
# -------------------------

class CombinedReport:
    def __init__(self, data: LoadedData, store: str):
        self.data = data
        self.store = store
        self.mb = MetricsBuilder(data)

        log("Building derived tables")
        self.sku_master = self.mb.sku_master
        self.daily_current = self.mb.build_daily_current()
        self.daily_targets = self.mb.build_daily_targets(self.daily_current)
        self.weekly_summary = self.mb.build_weekly_summary()
        self.monthly_summary = self.mb.build_monthly_summary(self.daily_current)
        self.monthly_forecast = self.mb.build_monthly_forecast(self.daily_current)
        self.source_summary = self.mb.build_source_summary()

    def build_main_report(self) -> Workbook:
        writer = ReportWriter()

        # Summary
        rows = [
            ("Магазин", self.store),
            ("Дата формирования", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            ("Источников с данными", int((~self.source_summary["is_empty"]).sum()) if not self.source_summary.empty else 0),
            ("SKU в мастер-таблице", len(self.sku_master) if self.sku_master is not None else 0),
        ]
        if not self.daily_current.empty and "day" in self.daily_current.columns:
            last_day = pd.to_datetime(self.daily_current["day"], errors="coerce").max()
            rows.extend([
                ("Последний день в daily", str(last_day.date()) if pd.notna(last_day) else ""),
                ("Заказы за последний день", float(self.daily_current[self.daily_current["day"] == last_day]["orders_day"].sum()) if pd.notna(last_day) else 0),
                ("Оценка валовой прибыли за последний день", float(self.daily_current[self.daily_current["day"] == last_day]["gross_profit_day_est"].sum()) if pd.notna(last_day) else 0),
            ])
        writer.add_title_block("Сводка", f"WB Combined Report — {self.store}", rows)

        writer.add_df_sheet("Диагностика_источников", self.source_summary)
        writer.add_df_sheet("Daily_current", self.daily_current)
        writer.add_df_sheet("Daily_targets", self.daily_targets)
        writer.add_df_sheet("Weekly_summary", self.weekly_summary)
        writer.add_df_sheet("Monthly_summary", self.monthly_summary)
        writer.add_df_sheet("Monthly_forecast", self.monthly_forecast)

        # subject sheets
        if not self.daily_current.empty:
            latest_day = pd.to_datetime(self.daily_current["day"], errors="coerce").max()
            current = self.daily_current[self.daily_current["day"] == latest_day].copy()
            current = current.sort_values(["subject", "orders_day", "gross_profit_day_est"], ascending=[True, False, False])

            subjects = [s for s in SUBJECT_ORDER if s in set(current["subject"].dropna())]
            other_subjects = sorted(set(current["subject"].dropna()) - set(subjects))
            for subject in subjects + other_subjects:
                x = current[current["subject"] == subject].copy()
                keep = [
                    "supplier_article", "nm_id", "title", "orders_day", "gross_profit_day_est",
                    "open_card_count", "cart_count", "conv_to_cart", "conv_cart_to_order",
                    "ad_impressions", "ad_clicks", "ad_spend", "search_frequency",
                    "median_position", "visibility_pct", "stock_available_now", "stock_total_now",
                    "abc_class", "drr_pct_latest", "margin_pct_latest",
                ]
                keep = [c for c in keep if c in x.columns]
                writer.add_df_sheet(subject, x[keep])

        # dictionary
        dict_df = pd.DataFrame(INPUT_DICTIONARY_ROWS, columns=["dataset", "expected_path", "sheet", "header_rule", "key_columns"])
        writer.add_df_sheet("Словарь_соответствий", dict_df)

        # warnings
        warnings_df = pd.DataFrame({"warning": self.data.warnings}) if self.data.warnings else pd.DataFrame({"warning": []})
        writer.add_df_sheet("Warnings", warnings_df)
        return writer.wb

    def build_log_report(self) -> Workbook:
        writer = ReportWriter()
        writer.add_df_sheet("sku_master", self.sku_master)
        writer.add_df_sheet("orders", self.data.orders)
        writer.add_df_sheet("stocks", self.data.stocks)
        writer.add_df_sheet("search", self.data.search)
        writer.add_df_sheet("search_history", self.data.search_history)
        writer.add_df_sheet("funnel", self.data.funnel)
        writer.add_df_sheet("ads_daily", self.data.ads_daily)
        writer.add_df_sheet("ads_total", self.data.ads_total)
        writer.add_df_sheet("campaigns", self.data.campaigns)
        writer.add_df_sheet("economics", self.data.economics)
        writer.add_df_sheet("abc", self.data.abc)
        writer.add_df_sheet("entry_points_category", self.data.entry_points_category)
        writer.add_df_sheet("entry_points_sku", self.data.entry_points_sku)
        writer.add_df_sheet("stock_history_days", self.data.stock_history_days)
        writer.add_df_sheet("stock_history_detail", self.data.stock_history_detail)
        writer.add_df_sheet("daily_current", self.daily_current)
        writer.add_df_sheet("daily_targets", self.daily_targets)
        writer.add_df_sheet("weekly_summary", self.weekly_summary)
        writer.add_df_sheet("monthly_summary", self.monthly_summary)
        writer.add_df_sheet("monthly_forecast", self.monthly_forecast)

        if self.data.diagnostics:
            diag_rows = []
            for d in self.data.diagnostics:
                diag_rows.append({
                    "dataset": d.dataset,
                    "file_path": d.file_path,
                    "sheet_name": d.sheet_name,
                    "header_row_excel": d.header_row_excel,
                    "rows": d.rows,
                    "columns": " | ".join(d.columns[:40]),
                    "mapping": " | ".join(f"{k}={v}" for k, v in d.mapping.items() if v),
                    "error": d.error,
                })
            writer.add_df_sheet("diagnostics", pd.DataFrame(diag_rows))
        else:
            writer.add_df_sheet("diagnostics", pd.DataFrame({"message": ["Нет диагностики"]}))

        dict_df = pd.DataFrame(INPUT_DICTIONARY_ROWS, columns=["dataset", "expected_path", "sheet", "header_rule", "key_columns"])
        writer.add_df_sheet("column_dictionary", dict_df)
        return writer.wb


# -------------------------
# CLI
# -------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="WB combined report rebuilt from scratch")
    p.add_argument("--root", default=".", help="Project root for local mode")
    p.add_argument("--reports-root", default="Отчёты", help="Root folder with reports")
    p.add_argument("--store", default="TOPFACE", help="Store name")
    p.add_argument("--out-subdir", default="Отчёты/Объединенный отчет/TOPFACE", help="Output folder")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    storage = make_storage(args.root)
    loader = DataLoader(storage=storage, store=args.store, reports_root=args.reports_root)

    log("Loading data")
    data = loader.load_all()

    log("Building report")
    report = CombinedReport(data=data, store=args.store)
    main_wb = report.build_main_report()
    log_wb = report.build_log_report()

    stamp = datetime.now().strftime("%Y-%m-%d")
    out_main = f"{args.out_subdir}/Объединенный_отчет_{args.store}_{stamp}.xlsx"
    out_log = f"{args.out_subdir}/Лог_расчетов_{args.store}_{stamp}.xlsx"

    storage.write_bytes(out_main, workbook_to_bytes(main_wb))
    storage.write_bytes(out_log, workbook_to_bytes(log_wb))

    log(f"Saved report: {out_main}")
    log(f"Saved log: {out_log}")

    if data.warnings:
        log("Warnings:")
        for w in data.warnings:
            log(f" - {w}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
