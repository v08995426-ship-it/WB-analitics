#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WB TOPFACE report from scratch: «Валовая прибыль».

Creates exactly 3 output workbooks and overwrites them on every run:
1) Отчёты/Объединенный отчет/TOPFACE/Объединенный_отчет_TOPFACE.xlsx
2) Отчёты/Объединенный отчет/TOPFACE/Технические_расчеты_TOPFACE.xlsx
3) Отчёты/Объединенный отчет/TOPFACE/Пример_расчета_901_TOPFACE.xlsx

Stage 1 + Stage 2: gross profit potential, localization, and plan deviation conclusions.
"""

from __future__ import annotations

import argparse
import calendar
import io
import os
import re
import shutil
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import boto3
import numpy as np
import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter


# =============================================================================
# CONFIG
# =============================================================================

TARGET_SUBJECTS: List[str] = [
    "Кисти косметические",
    "Помады",
    "Блески",
    "Косметические карандаши",
]

EXCLUDE_ARTICLES_UPPER = {
    "CZ420",
    "CZ420БРОВИ",
    "CZ420ГЛАЗА",
    "DE49",
    "DE49ГЛАЗА",
    "PT901",
}

EXAMPLE_ARTICLES = ["901/5", "901/8", "901/14", "901/18"]

MAIN_REPORT_NAME = "Объединенный_отчет_TOPFACE.xlsx"
TECH_REPORT_NAME = "Технические_расчеты_TOPFACE.xlsx"
EXAMPLE_REPORT_NAME = "Пример_расчета_901_TOPFACE.xlsx"

HEADER_FILL = PatternFill("solid", fgColor="17365D")
HEADER_FONT = Font(color="FFFFFF", bold=True)
TITLE_FILL = PatternFill("solid", fgColor="1F4E79")
TOTAL_FILL = PatternFill("solid", fgColor="D9EAF7")
ARTICLE_FILL = PatternFill("solid", fgColor="FFFFFF")
PLAN_FILL = PatternFill("solid", fgColor="EAF2F8")
CATEGORY_FILLS = {
    "Кисти косметические": PatternFill("solid", fgColor="9DC3E6"),
    "Помады": PatternFill("solid", fgColor="B4C7E7"),
    "Блески": PatternFill("solid", fgColor="C6E0F5"),
    "Косметические карандаши": PatternFill("solid", fgColor="DDEBF7"),
}
PRODUCT_FILL = PatternFill("solid", fgColor="EAF4FF")
THIN_SIDE = Side(style="thin", color="D9D9D9")
BORDER = Border(left=THIN_SIDE, right=THIN_SIDE, top=THIN_SIDE, bottom=THIN_SIDE)

WEEKDAY_RU = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
MONTH_RU = {
    1: "Январь",
    2: "Февраль",
    3: "Март",
    4: "Апрель",
    5: "Май",
    6: "Июнь",
    7: "Июль",
    8: "Август",
    9: "Сентябрь",
    10: "Октябрь",
    11: "Ноябрь",
    12: "Декабрь",
}

ALIASES: Dict[str, Sequence[str]] = {
    "day": ["Дата", "Дата заказа", "date", "dt", "День"],
    "nm_id": ["Артикул WB", "Артикул ВБ", "nmID", "nmId", "nm_id", "Номенклатура WB", "Номенклатура"],
    "supplier_article": ["Артикул продавца", "supplierArticle", "supplier_article", "Артикул", "Артикул WB продавца"],
    "subject": ["Предмет", "subject", "Название предмета", "Категория", "category"],
    "brand": ["Бренд", "brand"],
    "title": ["Название", "Название товара", "Товар", "Наименование"],
    "orders": ["Заказы", "orders", "ordersCount", "Кол-во заказов", "Количество заказов"],
    "buyouts_count": ["buyoutsCount", "Выкупы", "Кол-во выкупов", "Количество выкупов"],
    "finished_price": ["finishedPrice", "Цена с учетом всех скидок, кроме суммы по WB Кошельку", "Ср. цена продажи"],
    "price_with_disc": ["priceWithDisc", "Цена со скидкой продавца, в том числе со скидкой WB Клуба"],
    "spp": ["СПП, %", "SPP", "Скидка WB, %", "spp"],
    "warehouse": ["Склад", "warehouseName", "warehouse"],
    "spend": ["Расход", "spend", "Продвижение", "Затраты", "Расходы"],
    "gross_profit": ["Валовая прибыль", "Валовая прибыль, руб", "Валовая прибыль, руб/ед"],
    "gross_revenue": ["Валовая выручка", "Выручка", "Валовая выручка, руб"],
    "commission_pct": ["Комиссия WB, %", "Комиссия ВБ, %", "Комиссия, %"],
    "acquiring_pct": ["Эквайринг, %", "Эквайринг WB, %"],
    "logistics_direct": ["Логистика прямая, руб/ед", "Логистика прямая"],
    "logistics_return": ["Логистика обратная, руб/ед", "Логистика обратная"],
    "storage": ["Хранение, руб/ед", "Хранение"],
    "other_costs": ["Прочие расходы, руб/ед", "Прочие расходы"],
    "cost": ["Себестоимость, руб", "Себестоимость", "Себестоимость, руб/ед"],
    "week": ["Неделя", "week", "week_code"],
    "plan": ["План", "Валовая прибыль"],
    "stock": ["Остаток", "Остатки", "stock", "quantity", "qty", "Доступно", "Доступный остаток", "Количество", "Всего", "остаток, шт", "Остаток, шт"],
}


# =============================================================================
# BASIC HELPERS
# =============================================================================

def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


def normalize_text(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).replace("\xa0", " ").strip()
    return re.sub(r"\s+", " ", text)


def norm_key(value: Any) -> str:
    text = normalize_text(value).lower().replace("ё", "е")
    text = re.sub(r"[^a-zа-я0-9%]+", " ", text, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", text).strip()


def clean_article(value: Any) -> str:
    text = normalize_text(value)
    if text.lower() in {"", "nan", "none", "null"}:
        return ""
    return text


def article_upper(value: Any) -> str:
    return clean_article(value).upper().replace(" ", "")


def product_code(article: Any) -> str:
    text = article_upper(article)
    if not text or text in EXCLUDE_ARTICLES_UPPER:
        return ""
    text = text.replace("_", "/")
    m = re.match(r"^PT(\d+)", text)
    if m:
        return m.group(1)
    m = re.match(r"^(\d+)", text)
    if m:
        return m.group(1)
    m = re.match(r"^([A-ZА-Я]+\d+)", text)
    if m:
        return m.group(1)
    return text.split("/")[0].split(".")[0]


def to_number(value: Any) -> float:
    if value is None:
        return np.nan
    if isinstance(value, str):
        value = value.replace("\xa0", " ").replace(" ", "").replace("₽", "").replace("%", "").replace(",", ".")
    return pd.to_numeric(value, errors="coerce")


def num_series(series: pd.Series) -> pd.Series:
    return series.map(to_number)


def date_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.normalize()


def safe_ratio(a: Any, b: Any, default: float = np.nan) -> float:
    a_num = to_number(a)
    b_num = to_number(b)
    if pd.isna(a_num) or pd.isna(b_num) or b_num == 0:
        return default
    return float(a_num / b_num)


def week_code(ts: Any) -> str:
    if pd.isna(ts):
        return ""
    d = pd.Timestamp(ts)
    iso = d.isocalendar()
    return f"{int(iso.year)}-W{int(iso.week):02d}"


def week_start_from_code(code: str) -> Optional[pd.Timestamp]:
    m = re.search(r"(\d{4})-W(\d{2})", str(code))
    if not m:
        return None
    return pd.Timestamp(date.fromisocalendar(int(m.group(1)), int(m.group(2)), 1))


def parse_abc_period(filename: str) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    # Supports both wb_abc_report_goods__01.05.2026-07.05.2026__x.xlsx and single underscore variants.
    m = re.search(r"(\d{2})\.(\d{2})\.(\d{4})-(\d{2})\.(\d{2})\.(\d{4})", filename)
    if not m:
        return None, None
    start = pd.Timestamp(date(int(m.group(3)), int(m.group(2)), int(m.group(1))))
    end = pd.Timestamp(date(int(m.group(6)), int(m.group(5)), int(m.group(4))))
    return start, end


def is_month_file(start: pd.Timestamp, end: pd.Timestamp) -> bool:
    if pd.isna(start) or pd.isna(end):
        return False
    last_day = calendar.monthrange(start.year, start.month)[1]
    return start.day == 1 and end.day == last_day and start.month == end.month and start.year == end.year


def month_key(ts: Any) -> str:
    d = pd.Timestamp(ts)
    return f"{d.year:04d}-{d.month:02d}"


def money_format() -> str:
    return '# ##0 ₽;[Red]-# ##0 ₽;0 ₽'


# =============================================================================
# STORAGE
# =============================================================================

class Storage:
    def list_files(self, prefix: str) -> List[str]:
        raise NotImplementedError

    def read_bytes(self, key: str) -> bytes:
        raise NotImplementedError

    def write_bytes(self, key: str, data: bytes) -> None:
        raise NotImplementedError

    def exists(self, key: str) -> bool:
        raise NotImplementedError


class LocalStorage(Storage):
    def __init__(self, root: str):
        self.root = Path(root)

    def _full(self, key: str) -> Path:
        return self.root / key

    def list_files(self, prefix: str) -> List[str]:
        prefix = prefix.replace("\\", "/").strip("/")
        start = self._full(prefix)
        base = start if start.is_dir() else start.parent
        if not base.exists():
            return []
        out: List[str] = []
        for path in base.rglob("*"):
            if path.is_file():
                rel = str(path.relative_to(self.root)).replace("\\", "/")
                if rel.startswith(prefix):
                    out.append(rel)
        return sorted(out)

    def read_bytes(self, key: str) -> bytes:
        return self._full(key).read_bytes()

    def write_bytes(self, key: str, data: bytes) -> None:
        path = self._full(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def exists(self, key: str) -> bool:
        return self._full(key).exists()


class S3Storage(Storage):
    def __init__(self, bucket: str, access_key: str, secret_key: str, endpoint_url: str):
        self.bucket = bucket
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

    def list_files(self, prefix: str) -> List[str]:
        out: List[str] = []
        token: Optional[str] = None
        while True:
            kwargs: Dict[str, Any] = {"Bucket": self.bucket, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            resp = self.client.list_objects_v2(**kwargs)
            for item in resp.get("Contents", []):
                key = item.get("Key", "")
                if key and not key.endswith("/"):
                    out.append(key)
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        return sorted(out)

    def read_bytes(self, key: str) -> bytes:
        return self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def write_bytes(self, key: str, data: bytes) -> None:
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data)

    def exists(self, key: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except Exception:
            return False


def make_storage(root: str) -> Storage:
    bucket = os.getenv("YC_BUCKET_NAME", "").strip()
    access_key = os.getenv("YC_ACCESS_KEY_ID", "").strip()
    secret_key = os.getenv("YC_SECRET_ACCESS_KEY", "").strip()
    endpoint = os.getenv("YC_ENDPOINT_URL", "https://storage.yandexcloud.net").strip()
    if bucket and access_key and secret_key:
        log(f"Storage: Yandex Object Storage bucket={bucket}")
        return S3Storage(bucket, access_key, secret_key, endpoint)
    log(f"Storage: local root={Path(root).resolve()}")
    return LocalStorage(root)


# =============================================================================
# EXCEL READ NORMALIZATION
# =============================================================================

def add_alias_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    col_by_key = {norm_key(c): c for c in out.columns}
    for target, variants in ALIASES.items():
        if target in out.columns:
            continue
        found = None
        for v in variants:
            if norm_key(v) in col_by_key:
                found = col_by_key[norm_key(v)]
                break
        out[target] = out[found] if found is not None else np.nan
    return out


def read_excel_table(data: bytes, preferred_sheet: Optional[str] = None, header_rows: Iterable[int] = (0, 1, 2, 3)) -> pd.DataFrame:
    book = pd.ExcelFile(io.BytesIO(data))
    sheet = preferred_sheet if preferred_sheet in book.sheet_names else book.sheet_names[0]
    best: Optional[pd.DataFrame] = None
    best_score = -1
    for header in header_rows:
        try:
            df = book.parse(sheet_name=sheet, header=header, dtype=object)
        except Exception:
            continue
        df = df.dropna(how="all").dropna(axis=1, how="all")
        df.columns = [normalize_text(c) or f"col_{i}" for i, c in enumerate(df.columns)]
        alias_df = add_alias_columns(df)
        score = 0
        for required in ("day", "nm_id", "supplier_article", "subject", "orders", "spend", "gross_profit"):
            if required in alias_df.columns and not alias_df[required].isna().all():
                score += 1
        score += min(len(df.columns), 30) / 100
        if score > best_score:
            best = alias_df
            best_score = score
    if best is None:
        raise ValueError(f"Не удалось прочитать лист {sheet}")
    return best


def only_xlsx(files: Iterable[str]) -> List[str]:
    return [f for f in files if f.lower().endswith((".xlsx", ".xlsm")) and "/~$" not in f and not Path(f).name.startswith("~$")]


# =============================================================================
# DATA MODEL
# =============================================================================

@dataclass
class Diagnostics:
    rows: List[Dict[str, Any]] = field(default_factory=list)

    def add(self, level: str, source: str, message: str, details: Any = "") -> None:
        self.rows.append({
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "level": level,
            "source": source,
            "message": message,
            "details": normalize_text(details),
        })

    def frame(self) -> pd.DataFrame:
        return pd.DataFrame(self.rows, columns=["timestamp", "level", "source", "message", "details"])


@dataclass
class DataPack:
    orders: pd.DataFrame
    funnel: pd.DataFrame
    ads_raw: pd.DataFrame
    ads_used: pd.DataFrame
    economics: pd.DataFrame
    abc_weekly: pd.DataFrame
    abc_monthly: pd.DataFrame
    plan: pd.DataFrame
    stock: pd.DataFrame
    diagnostics: Diagnostics
    latest_day: pd.Timestamp


# =============================================================================
# LOADER LAYER
# =============================================================================

class LoaderLayer:
    def __init__(self, storage: Storage, reports_root: str, store: str, diagnostics: Diagnostics):
        self.storage = storage
        self.reports_root = reports_root.strip("/")
        self.store = store
        self.diagnostics = diagnostics

    def path(self, *parts: str) -> str:
        return "/".join([self.reports_root, *parts]).replace("//", "/")

    def list_xlsx(self, *parts: str) -> List[str]:
        return only_xlsx(self.storage.list_files(self.path(*parts)))

    def _log_frame(self, name: str, df: pd.DataFrame, date_col: Optional[str] = None) -> None:
        if date_col and date_col in df.columns and not df.empty:
            mn = pd.to_datetime(df[date_col], errors="coerce").min()
            mx = pd.to_datetime(df[date_col], errors="coerce").max()
            log(f"{name}: rows={len(df):,}, dates={mn.date() if pd.notna(mn) else '-'}..{mx.date() if pd.notna(mx) else '-'}")
        else:
            log(f"{name}: rows={len(df):,}")

    def load_orders(self) -> pd.DataFrame:
        files = self.list_xlsx("Заказы", self.store, "Недельные")
        frames: List[pd.DataFrame] = []
        for key in files:
            try:
                df = read_excel_table(self.storage.read_bytes(key), preferred_sheet="Заказы", header_rows=(0, 1, 2))
                out = pd.DataFrame({
                    "day": date_series(df["day"]),
                    "nm_id": num_series(df["nm_id"]),
                    "supplier_article": df["supplier_article"].map(clean_article),
                    "subject": df["subject"].map(normalize_text),
                    "finished_price": num_series(df["finished_price"]),
                    "price_with_disc": num_series(df["price_with_disc"]),
                    "spp": num_series(df["spp"]),
                    "orders": num_series(df["orders"]),
                    "warehouse": df["warehouse"].map(normalize_text),
                    "source_file": key,
                })
                if out["orders"].isna().all():
                    out["orders"] = 1.0
                out["orders"] = out["orders"].fillna(1.0)
                frames.append(out)
            except Exception as exc:
                self.diagnostics.add("ERROR", "orders", f"Не прочитан файл заказов: {key}", exc)
        result = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        if not result.empty:
            result = result[result["day"].notna()].copy()
        self._log_frame("orders", result, "day")
        return result

    def load_funnel(self) -> pd.DataFrame:
        candidates = [
            self.path("Воронка продаж", self.store, "Воронка продаж.xlsx"),
            self.path("Воронка продаж", "Воронка продаж.xlsx"),
        ]
        key = next((x for x in candidates if self.storage.exists(x)), None)
        if not key:
            self.diagnostics.add("ERROR", "funnel", "Файл воронки продаж не найден")
            return pd.DataFrame()
        try:
            df = read_excel_table(self.storage.read_bytes(key), preferred_sheet=None, header_rows=(0, 1, 2))
            out = pd.DataFrame({
                "day": date_series(df["day"]),
                "nm_id": num_series(df["nm_id"]),
                "orders": num_series(df["orders"]),
                "buyouts_count": num_series(df["buyouts_count"]),
                "source_file": key,
            })
            out = out[out["day"].notna()].copy()
            self._log_frame("funnel", out, "day")
            return out
        except Exception as exc:
            self.diagnostics.add("ERROR", "funnel", f"Не прочитан файл воронки: {key}", exc)
            return pd.DataFrame()

    def load_ads(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        files = self.list_xlsx("Реклама", self.store, "Недельные")
        frames: List[pd.DataFrame] = []
        for key in files:
            try:
                df = read_excel_table(self.storage.read_bytes(key), preferred_sheet="Статистика_Ежедневно", header_rows=(0, 1, 2))
                out = pd.DataFrame({
                    "day": date_series(df["day"]),
                    "nm_id": num_series(df["nm_id"]),
                    "spend": num_series(df["spend"]).fillna(0),
                    "source_file": key,
                })
                out = out[out["day"].notna() & out["nm_id"].notna()].copy()
                frames.append(out)
            except Exception as exc:
                self.diagnostics.add("ERROR", "ads", f"Не прочитан файл рекламы: {key}", exc)
        raw = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["day", "nm_id", "spend", "source_file"])
        if raw.empty:
            used = pd.DataFrame(columns=["day", "nm_id", "spend"])
        else:
            # Critical rule: aggregate only by day + nmId before dictionary enrichment.
            used = raw.groupby(["day", "nm_id"], dropna=False, as_index=False).agg(spend=("spend", "sum"))
        self._log_frame("ads_raw", raw, "day")
        self._log_frame("ads_used_day_nm", used, "day")
        log(f"ads_used_day_nm: total_spend={used['spend'].sum():,.2f}" if not used.empty else "ads_used_day_nm: total_spend=0")
        return raw, used

    def load_economics(self) -> pd.DataFrame:
        candidates = [
            self.path("Финансовые показатели", self.store, "Экономика.xlsx"),
            self.path("Финансовые показатели", self.store, "Недельные", "Экономика.xlsx"),
        ]
        key = next((x for x in candidates if self.storage.exists(x)), None)
        if not key:
            self.diagnostics.add("ERROR", "economics", "Файл Экономика.xlsx не найден")
            return pd.DataFrame()
        try:
            df = read_excel_table(self.storage.read_bytes(key), preferred_sheet="Юнит экономика", header_rows=(0, 1, 2, 3))
            out = pd.DataFrame({
                "week_code": df["week"].map(normalize_text),
                "nm_id": num_series(df["nm_id"]),
                "supplier_article": df["supplier_article"].map(clean_article),
                "subject": df["subject"].map(normalize_text),
                "brand": df["brand"].map(normalize_text),
                "title": df["title"].map(normalize_text),
                "commission_pct": num_series(df["commission_pct"]),
                "acquiring_pct": num_series(df["acquiring_pct"]),
                "logistics_direct": num_series(df["logistics_direct"]),
                "logistics_return": num_series(df["logistics_return"]),
                "storage": num_series(df["storage"]),
                "other_costs": num_series(df["other_costs"]),
                "cost": num_series(df["cost"]),
                "source_file": key,
            })
            out["product"] = out["supplier_article"].map(product_code)
            self._log_frame("economics", out)
            return out
        except Exception as exc:
            self.diagnostics.add("ERROR", "economics", f"Не прочитан файл экономики: {key}", exc)
            return pd.DataFrame()

    def load_abc(self, current_year: int, latest_day: pd.Timestamp) -> Tuple[pd.DataFrame, pd.DataFrame]:
        files = self.list_xlsx("ABC")
        weekly_frames: List[pd.DataFrame] = []
        monthly_frames: List[pd.DataFrame] = []
        for key in files:
            name = Path(key).name
            if "abc" not in name.lower():
                continue
            start, end = parse_abc_period(name)
            if start is None or end is None:
                continue
            try:
                df = read_excel_table(self.storage.read_bytes(key), preferred_sheet=None, header_rows=(0, 1, 2))
                out = pd.DataFrame({
                    "period_start": start,
                    "period_end": end,
                    "week_code": week_code(start),
                    "week_label": f"{start.strftime('%d.%m')}-{end.strftime('%d.%m')}",
                    "month_key": month_key(start),
                    "nm_id": num_series(df["nm_id"]),
                    "supplier_article": df["supplier_article"].map(clean_article),
                    "subject": df["subject"].map(normalize_text),
                    "gross_profit": num_series(df["gross_profit"]).fillna(0),
                    "gross_revenue": num_series(df["gross_revenue"]).fillna(0),
                    "orders": num_series(df["orders"]).fillna(0),
                    "source_file": key,
                })
                out["product"] = out["supplier_article"].map(product_code)
                if is_month_file(start, end) and start.year == current_year and start <= latest_day:
                    monthly_frames.append(out)
                elif start.year == current_year or end.year == current_year:
                    weekly_frames.append(out)
            except Exception as exc:
                self.diagnostics.add("ERROR", "abc", f"Не прочитан ABC: {key}", exc)
        weekly = pd.concat(weekly_frames, ignore_index=True) if weekly_frames else pd.DataFrame()
        monthly = pd.concat(monthly_frames, ignore_index=True) if monthly_frames else pd.DataFrame()
        self._log_frame("abc_weekly", weekly, "period_start")
        self._log_frame("abc_monthly_current_year", monthly, "period_start")
        if not weekly.empty:
            log("abc_weekly periods: " + ", ".join(sorted(weekly["week_label"].dropna().astype(str).unique())))
        if not monthly.empty:
            log("abc_monthly periods: " + ", ".join(sorted(monthly["month_key"].dropna().astype(str).unique())))
        return weekly, monthly

    def load_plan(self, latest_day: pd.Timestamp) -> pd.DataFrame:
        key = self.path("Объединенный отчет", self.store, "План.xlsx")
        if not self.storage.exists(key):
            self.diagnostics.add("WARN", "plan", f"План не найден: {key}")
            return pd.DataFrame()
        try:
            # Plan files often have a title above table, try several headers.
            raw = read_excel_table(self.storage.read_bytes(key), preferred_sheet="Итог_все_категории", header_rows=(0, 1, 2, 3, 4))
            raw_cols = list(raw.columns)
            chosen_col = None
            target_month = MONTH_RU[latest_day.month]
            patterns = [
                f"план {target_month} {latest_day.year}",
                f"валовая прибыль {target_month} {latest_day.year}",
            ]
            for col in raw_cols:
                k = norm_key(col).replace("-", " ")
                if any(p in k for p in patterns):
                    chosen_col = col
                    break
            if chosen_col is None:
                for col in raw_cols:
                    k = norm_key(col)
                    if str(latest_day.year) in k and norm_key(target_month) in k and ("план" in k or "валовая прибыль" in k):
                        chosen_col = col
                        break
            if chosen_col is None and "plan" in raw.columns and not raw["plan"].isna().all():
                chosen_col = "plan"
            if chosen_col is None:
                self.diagnostics.add("WARN", "plan", "Не найдена колонка плана на текущий месяц", f"columns={raw_cols}")
                return pd.DataFrame()
            out = pd.DataFrame({
                "supplier_article": raw["supplier_article"].map(clean_article),
                "subject": raw["subject"].map(normalize_text),
                "plan_month": num_series(raw[chosen_col]),
                "source_file": key,
                "source_column": chosen_col,
            })
            out["product"] = out["supplier_article"].map(product_code)
            self._log_frame("plan", out)
            return out
        except Exception as exc:
            self.diagnostics.add("ERROR", "plan", f"Не прочитан план: {key}", exc)
            return pd.DataFrame()


    def load_stock(self) -> pd.DataFrame:
        """Load stock snapshots from weekly/current stock files.

        The loader supports two common layouts:
        1) flat rows: date / supplier_article or nm_id / warehouse / stock;
        2) wide daily columns: supplier_article or nm_id / warehouse / 01.04.2026 / 02.04.2026 / ...

        If a file has no explicit date, the end date parsed from the filename is used as snapshot day.
        """
        stock_prefixes = [
            ("Остатки", self.store, "Недельные"),
            ("Остатки", self.store),
            ("Остатки",),
            ("Остатки и товары в пути", self.store, "Недельные"),
            ("Остатки и товары в пути", self.store),
            ("Остатки и товары в пути",),
        ]
        files: List[str] = []
        for parts in stock_prefixes:
            files.extend(self.list_xlsx(*parts))
        # Deduplicate while preserving order.
        seen = set()
        files = [x for x in files if not (x in seen or seen.add(x))]
        frames: List[pd.DataFrame] = []
        date_col_pattern = re.compile(r"^\d{1,2}[.\-/]\d{1,2}([.\-/]\d{2,4})?$")
        for key in files:
            try:
                raw = read_excel_table(self.storage.read_bytes(key), preferred_sheet=None, header_rows=(0, 1, 2, 3))
                raw_cols = list(raw.columns)
                parsed_start, parsed_end = parse_abc_period(Path(key).name)
                # Also support names like Остатки_2026-W15.xlsx.
                if parsed_end is None:
                    wk = re.search(r"(\d{4})-W(\d{2})", Path(key).name)
                    if wk:
                        parsed_start = pd.Timestamp(date.fromisocalendar(int(wk.group(1)), int(wk.group(2)), 1))
                        parsed_end = pd.Timestamp(date.fromisocalendar(int(wk.group(1)), int(wk.group(2)), 7))
                fallback_day = parsed_end if parsed_end is not None else pd.NaT

                base_cols = [c for c in ["day", "nm_id", "supplier_article", "subject", "warehouse", "stock"] if c in raw.columns]
                has_flat_stock = "stock" in raw.columns and not raw["stock"].isna().all()
                if has_flat_stock:
                    out = pd.DataFrame({
                        "day": date_series(raw["day"]) if "day" in raw.columns else pd.Series([fallback_day] * len(raw)),
                        "nm_id": num_series(raw["nm_id"]) if "nm_id" in raw.columns else pd.Series([np.nan] * len(raw)),
                        "supplier_article": raw["supplier_article"].map(clean_article) if "supplier_article" in raw.columns else pd.Series([""] * len(raw)),
                        "subject": raw["subject"].map(normalize_text) if "subject" in raw.columns else pd.Series([""] * len(raw)),
                        "warehouse": raw["warehouse"].map(normalize_text) if "warehouse" in raw.columns else pd.Series([""] * len(raw)),
                        "stock_qty": num_series(raw["stock"]).fillna(0),
                        "source_file": key,
                    })
                    out["day"] = out["day"].fillna(fallback_day)
                    frames.append(out)
                    continue

                # Wide layout: date-like columns contain stock values.
                date_cols = []
                for c in raw_cols:
                    c_text = normalize_text(c)
                    if date_col_pattern.match(c_text):
                        date_cols.append(c)
                        continue
                    parsed = pd.to_datetime(c_text, dayfirst=True, errors="coerce")
                    if pd.notna(parsed) and 2000 <= parsed.year <= 2100:
                        date_cols.append(c)
                if date_cols:
                    id_cols = [c for c in raw_cols if c not in date_cols]
                    melted = raw.melt(id_vars=id_cols, value_vars=date_cols, var_name="stock_day_raw", value_name="stock")
                    melted = add_alias_columns(melted)
                    out = pd.DataFrame({
                        "day": pd.to_datetime(melted["stock_day_raw"], dayfirst=True, errors="coerce").dt.normalize(),
                        "nm_id": num_series(melted["nm_id"]) if "nm_id" in melted.columns else pd.Series([np.nan] * len(melted)),
                        "supplier_article": melted["supplier_article"].map(clean_article) if "supplier_article" in melted.columns else pd.Series([""] * len(melted)),
                        "subject": melted["subject"].map(normalize_text) if "subject" in melted.columns else pd.Series([""] * len(melted)),
                        "warehouse": melted["warehouse"].map(normalize_text) if "warehouse" in melted.columns else pd.Series([""] * len(melted)),
                        "stock_qty": num_series(melted["stock"]).fillna(0),
                        "source_file": key,
                    })
                    out["day"] = out["day"].fillna(fallback_day)
                    frames.append(out)
                else:
                    self.diagnostics.add("WARN", "stock", f"Файл остатков не распознан: {key}", f"columns={raw_cols}")
            except Exception as exc:
                self.diagnostics.add("ERROR", "stock", f"Не прочитан файл остатков: {key}", exc)
        result = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["day", "nm_id", "supplier_article", "subject", "warehouse", "stock_qty", "source_file"])
        if not result.empty:
            result["day"] = pd.to_datetime(result["day"], errors="coerce").dt.normalize()
            result["nm_id"] = num_series(result["nm_id"])
            result["supplier_article"] = result["supplier_article"].map(clean_article)
            result["subject"] = result["subject"].map(normalize_text)
            result["warehouse"] = result["warehouse"].map(normalize_text)
            result["stock_qty"] = num_series(result["stock_qty"]).fillna(0)
            result = result[result["day"].notna()].copy()
            result["product"] = result["supplier_article"].map(product_code)
        self._log_frame("stock", result, "day")
        return result

    def load_all(self) -> DataPack:
        orders = self.load_orders()
        funnel = self.load_funnel()
        ads_raw, ads_used = self.load_ads()
        economics = self.load_economics()

        candidates: List[pd.Timestamp] = []
        for df, col in ((orders, "day"), (funnel, "day"), (ads_used, "day")):
            if not df.empty and col in df.columns:
                mx = pd.to_datetime(df[col], errors="coerce").max()
                if pd.notna(mx):
                    candidates.append(pd.Timestamp(mx).normalize())
        latest_day = max(candidates) if candidates else pd.Timestamp(datetime.today().date())

        abc_weekly, abc_monthly = self.load_abc(latest_day.year, latest_day)
        if not abc_weekly.empty:
            mx = pd.to_datetime(abc_weekly["period_end"], errors="coerce").max()
            if pd.notna(mx):
                latest_day = max(latest_day, pd.Timestamp(mx).normalize())
        plan = self.load_plan(latest_day)
        stock = self.load_stock()

        return DataPack(
            orders=orders,
            funnel=funnel,
            ads_raw=ads_raw,
            ads_used=ads_used,
            economics=economics,
            abc_weekly=abc_weekly,
            abc_monthly=abc_monthly,
            plan=plan,
            stock=stock,
            diagnostics=self.diagnostics,
            latest_day=latest_day,
        )


# =============================================================================
# DICTIONARY LAYER
# =============================================================================

class DictionaryLayer:
    def __init__(self, pack: DataPack):
        self.pack = pack

    @staticmethod
    def _base_fields(df: pd.DataFrame, source: str) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=["subject", "product", "supplier_article", "nm_id", "brand", "title", "source"])
        x = df.copy()
        for col in ["subject", "product", "supplier_article", "nm_id", "brand", "title"]:
            if col not in x.columns:
                x[col] = "" if col != "nm_id" else np.nan
        x["subject"] = x["subject"].map(normalize_text)
        x["supplier_article"] = x["supplier_article"].map(clean_article)
        x["nm_id"] = num_series(x["nm_id"])
        x["product"] = x["supplier_article"].map(product_code).where(x["product"].map(normalize_text).eq(""), x["product"].map(normalize_text))
        x["brand"] = x["brand"].map(normalize_text)
        x["title"] = x["title"].map(normalize_text)
        x["source"] = source
        return x[["subject", "product", "supplier_article", "nm_id", "brand", "title", "source"]]

    def build(self) -> pd.DataFrame:
        frames = [
            self._base_fields(self.pack.orders, "orders"),
            self._base_fields(self.pack.economics, "economics"),
            self._base_fields(self.pack.abc_weekly, "abc_weekly"),
            self._base_fields(self.pack.abc_monthly, "abc_monthly"),
            self._base_fields(self.pack.stock, "stock"),
        ]
        master = pd.concat(frames, ignore_index=True)
        master = master[master["subject"].isin(TARGET_SUBJECTS)].copy()
        master = master[master["supplier_article"].ne("") & master["product"].ne("")].copy()
        master = master[~master["supplier_article"].map(article_upper).isin(EXCLUDE_ARTICLES_UPPER)].copy()
        master["quality"] = (
            master["subject"].ne("").astype(int) * 10
            + master["supplier_article"].ne("").astype(int) * 10
            + master["nm_id"].notna().astype(int) * 5
            + master["title"].ne("").astype(int)
        )
        master = master.sort_values(["quality", "source"], ascending=[False, True])
        by_article = master.drop_duplicates(["supplier_article", "nm_id"], keep="first")
        by_article = by_article[["subject", "product", "supplier_article", "nm_id", "brand", "title", "source"]].copy()
        log(f"dictionary: rows={len(by_article):,}, nm_ids={by_article['nm_id'].nunique(dropna=True):,}, articles={by_article['supplier_article'].nunique():,}")
        return by_article

    @staticmethod
    def enrich_by_nm(df: pd.DataFrame, dictionary: pd.DataFrame, diagnostics: Diagnostics, source: str) -> pd.DataFrame:
        if df.empty:
            return df.copy()
        out = df.copy()
        dict_nm = dictionary.dropna(subset=["nm_id"]).sort_values("source").drop_duplicates("nm_id")
        add_cols = ["subject", "product", "supplier_article", "brand", "title"]
        for col in add_cols:
            if col not in out.columns:
                out[col] = ""
        before_unmapped = int(out["nm_id"].notna().sum()) if "nm_id" in out.columns else 0
        out = out.merge(dict_nm[["nm_id", *add_cols]], on="nm_id", how="left", suffixes=("", "_dict"))
        for col in add_cols:
            out[col] = out[col].where(out[col].map(normalize_text).ne(""), out[f"{col}_dict"])
        out = out.drop(columns=[c for c in out.columns if c.endswith("_dict")])
        out["subject"] = out["subject"].map(normalize_text)
        out["supplier_article"] = out["supplier_article"].map(clean_article)
        out["product"] = out["product"].map(normalize_text).where(out["product"].map(normalize_text).ne(""), out["supplier_article"].map(product_code))
        unmapped = int(out.loc[out["nm_id"].notna() & out["supplier_article"].map(clean_article).eq(""), "nm_id"].nunique())
        if unmapped:
            diagnostics.add("WARN", source, "Есть nmId без сопоставления в master-словаре", f"unmapped_nm_ids={unmapped}; total_nm_rows={before_unmapped}")
        return out

    @staticmethod
    def filter_target(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df.copy()
        out = df.copy()
        for col in ["subject", "supplier_article", "product"]:
            if col not in out.columns:
                out[col] = ""
        out["subject"] = out["subject"].map(normalize_text)
        out["supplier_article"] = out["supplier_article"].map(clean_article)
        out["product"] = out["product"].map(normalize_text).where(out["product"].map(normalize_text).ne(""), out["supplier_article"].map(product_code))
        out = out[out["subject"].isin(TARGET_SUBJECTS)].copy()
        out = out[out["supplier_article"].ne("") & out["product"].ne("")].copy()
        out = out[~out["supplier_article"].map(article_upper).isin(EXCLUDE_ARTICLES_UPPER)].copy()
        return out


# =============================================================================
# STAGE 1 CALCULATION LAYER
# =============================================================================

class Stage1Layer:
    def __init__(self, pack: DataPack, dictionary: pd.DataFrame):
        self.pack = pack
        self.dictionary = dictionary
        self.diag = pack.diagnostics
        self.latest_day = pack.latest_day
        self.current_year = int(self.latest_day.year)
        self.current_month = int(self.latest_day.month)
        self.month_start = pd.Timestamp(date(self.current_year, self.current_month, 1))
        self.days_in_month = calendar.monthrange(self.current_year, self.current_month)[1]
        self.week_start = self.latest_day - pd.Timedelta(days=int(self.latest_day.weekday()))
        self.week_days = [self.week_start + pd.Timedelta(days=i) for i in range(7)]

    def buyout_90(self) -> pd.DataFrame:
        if self.pack.funnel.empty:
            self.diag.add("WARN", "funnel", "Воронка пустая, buyout_pct_90 будет заменён на 1")
            return pd.DataFrame(columns=["nm_id", "orders_90", "buyouts_90", "buyout_pct_90"])
        f = self.pack.funnel.copy()
        f = f[(f["day"] >= self.latest_day - pd.Timedelta(days=89)) & (f["day"] <= self.latest_day)].copy()
        g = f.groupby("nm_id", dropna=False, as_index=False).agg(
            orders_90=("orders", "sum"),
            buyouts_90=("buyouts_count", "sum"),
        )
        g["buyout_pct_90"] = g.apply(lambda r: safe_ratio(r["buyouts_90"], r["orders_90"], np.nan), axis=1)
        g["buyout_pct_90"] = g["buyout_pct_90"].clip(lower=0, upper=1)
        return g

    def economics_for_rows(self, rows: pd.DataFrame) -> pd.DataFrame:
        if rows.empty:
            return rows.copy()
        econ = DictionaryLayer.filter_target(self.pack.economics)
        if econ.empty:
            self.diag.add("ERROR", "economics", "Нет экономики по целевым категориям")
            out = rows.copy()
            for c in ["commission_pct", "acquiring_pct", "logistics_direct", "logistics_return", "storage", "other_costs", "cost"]:
                out[c] = 0.0
            out["economics_match_type"] = "missing_all"
            return out

        econ = econ.copy()
        econ["week_start"] = econ["week_code"].map(week_start_from_code)
        econ["week_start"] = pd.to_datetime(econ["week_start"], errors="coerce")
        econ = econ.sort_values(["supplier_article", "week_start"], ascending=[True, False])

        exact_keys = ["supplier_article", "week_code"]
        exact = econ.drop_duplicates(exact_keys, keep="first")
        out = rows.merge(
            exact[[*exact_keys, "commission_pct", "acquiring_pct", "logistics_direct", "logistics_return", "storage", "other_costs", "cost"]],
            on=exact_keys,
            how="left",
        )
        out["economics_match_type"] = np.where(out["commission_pct"].notna() & (out["commission_pct"] != 0), "article_week", "")

        latest_article = econ.drop_duplicates(["supplier_article"], keep="first")
        out = out.merge(
            latest_article[["supplier_article", "commission_pct", "acquiring_pct", "logistics_direct", "logistics_return", "storage", "other_costs", "cost"]],
            on="supplier_article",
            how="left",
            suffixes=("", "_article_latest"),
        )
        cost_cols = ["commission_pct", "acquiring_pct", "logistics_direct", "logistics_return", "storage", "other_costs", "cost"]
        need_article = out["commission_pct"].isna() | (out["commission_pct"] == 0)
        for c in cost_cols:
            out[c] = out[c].where(~need_article | out[c].notna() & (out[c] != 0), out[f"{c}_article_latest"])
        out["economics_match_type"] = out["economics_match_type"].where(~need_article, "article_latest")
        out = out.drop(columns=[c for c in out.columns if c.endswith("_article_latest")])

        # Commission fallback: same subject in same week, then latest non-zero by subject.
        subject_week = econ[econ["commission_pct"].notna() & (econ["commission_pct"] != 0)]
        subject_week = subject_week.groupby(["subject", "week_code"], as_index=False)["commission_pct"].median().rename(columns={"commission_pct": "commission_subject_week"})
        out = out.merge(subject_week, on=["subject", "week_code"], how="left")
        need_comm = out["commission_pct"].isna() | (out["commission_pct"] == 0)
        out["commission_pct"] = out["commission_pct"].where(~need_comm, out["commission_subject_week"])
        out["economics_match_type"] = out["economics_match_type"].where(~need_comm, "commission_subject_week")

        subject_latest = econ[econ["commission_pct"].notna() & (econ["commission_pct"] != 0)]
        subject_latest = subject_latest.sort_values("week_start", ascending=False).drop_duplicates("subject")
        subject_latest = subject_latest[["subject", "commission_pct"]].rename(columns={"commission_pct": "commission_subject_latest"})
        out = out.merge(subject_latest, on="subject", how="left")
        need_comm2 = out["commission_pct"].isna() | (out["commission_pct"] == 0)
        out["commission_pct"] = out["commission_pct"].where(~need_comm2, out["commission_subject_latest"])
        out["economics_match_type"] = out["economics_match_type"].where(~need_comm2, "commission_subject_latest")
        out = out.drop(columns=["commission_subject_week", "commission_subject_latest"], errors="ignore")

        for c in cost_cols:
            out[c] = num_series(out[c]).fillna(0.0)
        direct_share = float((out["economics_match_type"] == "article_week").mean()) if len(out) else 0.0
        fallback_share = 1.0 - direct_share
        self.diag.add("INFO", "economics", "Доля прямого совпадения и fallback", f"direct={direct_share:.1%}; fallback={fallback_share:.1%}")
        return out

    def ads_by_article_day(self) -> pd.DataFrame:
        ads = self.pack.ads_used.copy()
        if ads.empty:
            return pd.DataFrame(columns=["day", "nm_id", "spend", "subject", "product", "supplier_article"])
        ads = DictionaryLayer.enrich_by_nm(ads, self.dictionary, self.diag, "ads")
        ads = DictionaryLayer.filter_target(ads)
        total_before = self.pack.ads_used["spend"].sum() if not self.pack.ads_used.empty else 0.0
        total_after = ads["spend"].sum() if not ads.empty else 0.0
        self.diag.add("INFO", "ads", "Расход рекламы после сопоставления с целевыми категориями", f"before={total_before:.2f}; after={total_after:.2f}")
        return ads

    def daily_formula(self) -> pd.DataFrame:
        orders = DictionaryLayer.enrich_by_nm(self.pack.orders, self.dictionary, self.diag, "orders")
        orders = DictionaryLayer.filter_target(orders)
        if orders.empty:
            self.diag.add("ERROR", "orders", "После фильтра целевых категорий нет заказов")
            return pd.DataFrame()
        orders = orders[(orders["day"] >= self.week_start) & (orders["day"] <= self.latest_day)].copy()
        if orders.empty:
            self.diag.add("WARN", "orders", "Нет заказов за текущую неделю")
            return pd.DataFrame()
        orders["week_code"] = orders["day"].map(week_code)
        # Prices are weighted by order quantity.
        orders["finished_price_order_sum"] = orders["orders"].fillna(0) * orders["finished_price"].fillna(0)
        orders["price_with_disc_order_sum"] = orders["orders"].fillna(0) * orders["price_with_disc"].fillna(0)
        orders["spp_order_sum"] = orders["orders"].fillna(0) * orders["spp"].fillna(0)
        grouped = orders.groupby(["day", "week_code", "subject", "product", "supplier_article", "nm_id"], dropna=False, as_index=False).agg(
            orders_qty=("orders", "sum"),
            finished_price_order_sum=("finished_price_order_sum", "sum"),
            price_with_disc_order_sum=("price_with_disc_order_sum", "sum"),
            spp_order_sum=("spp_order_sum", "sum"),
        )
        grouped["finished_price"] = grouped.apply(lambda r: safe_ratio(r["finished_price_order_sum"], r["orders_qty"], 0.0), axis=1)
        grouped["price_with_disc"] = grouped.apply(lambda r: safe_ratio(r["price_with_disc_order_sum"], r["orders_qty"], 0.0), axis=1)
        grouped["spp"] = grouped.apply(lambda r: safe_ratio(r["spp_order_sum"], r["orders_qty"], np.nan), axis=1)
        grouped = grouped.merge(self.buyout_90(), on="nm_id", how="left")
        grouped["buyout_pct_90"] = grouped["buyout_pct_90"].fillna(1.0).clip(lower=0, upper=1)

        ads = self.ads_by_article_day()
        if not ads.empty:
            ads = ads[(ads["day"] >= self.week_start) & (ads["day"] <= self.latest_day)].copy()
            ads_article = ads.groupby(["day", "nm_id", "supplier_article"], dropna=False, as_index=False).agg(ad_spend=("spend", "sum"))
            grouped = grouped.merge(ads_article, on=["day", "nm_id", "supplier_article"], how="left")
        else:
            grouped["ad_spend"] = 0.0
        grouped["ad_spend"] = grouped["ad_spend"].fillna(0.0)

        enriched = self.economics_for_rows(grouped)
        enriched["buyout_qty"] = enriched["orders_qty"] * enriched["buyout_pct_90"]
        enriched["revenue"] = enriched["price_with_disc_order_sum"] * enriched["buyout_pct_90"]
        enriched["commission_wb"] = enriched["revenue"] * enriched["commission_pct"] / 100.0
        enriched["acquiring"] = enriched["revenue"] * enriched["acquiring_pct"] / 100.0
        enriched["logistics_direct_total"] = enriched["buyout_qty"] * enriched["logistics_direct"]
        enriched["logistics_return_total"] = enriched["buyout_qty"] * enriched["logistics_return"]
        enriched["storage_total"] = enriched["buyout_qty"] * enriched["storage"]
        enriched["other_costs_total"] = enriched["buyout_qty"] * enriched["other_costs"]
        enriched["cost_total"] = enriched["buyout_qty"] * enriched["cost"]
        enriched["gross_profit"] = (
            enriched["revenue"]
            - enriched["commission_wb"]
            - enriched["acquiring"]
            - enriched["logistics_direct_total"]
            - enriched["logistics_return_total"]
            - enriched["storage_total"]
            - enriched["other_costs_total"]
            - enriched["cost_total"]
            - enriched["ad_spend"]
        )
        enriched["day_label"] = enriched["day"].dt.strftime("%d.%m")
        enriched["weekday_label"] = enriched["day"].apply(lambda x: f"{WEEKDAY_RU[int(pd.Timestamp(x).weekday())]} {pd.Timestamp(x).strftime('%d.%m')}")
        return enriched

    def weekly_abc_current_month(self) -> pd.DataFrame:
        df = DictionaryLayer.enrich_by_nm(self.pack.abc_weekly, self.dictionary, self.diag, "abc_weekly")
        df = DictionaryLayer.filter_target(df)
        if df.empty:
            return df
        df = df[(df["period_end"] >= self.month_start) & (df["period_start"] <= self.latest_day)].copy()
        return df

    def monthly_abc_current_year(self) -> pd.DataFrame:
        monthly = DictionaryLayer.enrich_by_nm(self.pack.abc_monthly, self.dictionary, self.diag, "abc_monthly")
        monthly = DictionaryLayer.filter_target(monthly)
        frames = []
        if not monthly.empty:
            monthly = monthly[pd.to_datetime(monthly["period_start"]).dt.year == self.current_year].copy()
            monthly = monthly[pd.to_datetime(monthly["period_start"]) <= self.latest_day].copy()
            frames.append(monthly)
        # For current month only: if no full monthly ABC, build from weekly ABC of current month.
        current_key = f"{self.current_year:04d}-{self.current_month:02d}"
        has_current_month = False if monthly.empty else current_key in set(monthly["month_key"].astype(str))
        if not has_current_month:
            weekly = self.weekly_abc_current_month()
            if not weekly.empty:
                synth = weekly.groupby(["month_key", "subject", "product", "supplier_article", "nm_id"], dropna=False, as_index=False).agg(
                    period_start=("period_start", "min"),
                    period_end=("period_end", "max"),
                    gross_profit=("gross_profit", "sum"),
                    gross_revenue=("gross_revenue", "sum"),
                    orders=("orders", "sum"),
                )
                synth["source_file"] = "SYNTH_FROM_WEEKLY_ABC_CURRENT_MONTH"
                frames.append(synth)
                self.diag.add("INFO", "abc_monthly", "Текущий месяц собран из недельных ABC", current_key)
        if not frames:
            return pd.DataFrame()
        out = pd.concat(frames, ignore_index=True)
        out = out[out["month_key"].astype(str).str.startswith(str(self.current_year))].copy()
        return out

    def plan_used(self) -> pd.DataFrame:
        """Plan for the current month = previous month gross profit * 1.1.

        This replaces План.xlsx for the new gross-profit report.
        Source priority: monthly ABC for the previous month; if absent, weekly ABC rows whose period starts in the previous month.
        """
        prev_month_end = self.month_start - pd.Timedelta(days=1)
        prev_key = prev_month_end.to_period("M").strftime("%Y-%m")
        source = pd.DataFrame()
        monthly = DictionaryLayer.enrich_by_nm(self.pack.abc_monthly, self.dictionary, self.diag, "plan_prev_month_abc_monthly")
        monthly = DictionaryLayer.filter_target(monthly)
        if not monthly.empty:
            source = monthly[monthly["month_key"].astype(str) == prev_key].copy()
            source["plan_source"] = "abc_monthly_previous_month"
        if source.empty:
            weekly = DictionaryLayer.enrich_by_nm(self.pack.abc_weekly, self.dictionary, self.diag, "plan_prev_month_abc_weekly")
            weekly = DictionaryLayer.filter_target(weekly)
            if not weekly.empty:
                weekly["period_mid"] = pd.to_datetime(weekly["period_start"]) + (pd.to_datetime(weekly["period_end"]) - pd.to_datetime(weekly["period_start"])) / 2
                source = weekly[weekly["period_mid"].dt.to_period("M").astype(str) == prev_key].copy()
                source["plan_source"] = "abc_weekly_previous_month"
        if source.empty:
            self.diag.add("WARN", "plan", "Нет ABC предыдущего месяца для плана", f"prev_month={prev_key}")
            return pd.DataFrame(columns=["subject", "product", "supplier_article", "nm_id", "prev_month_gross_profit", "plan_month", "plan_source"])
        plan = source.groupby(["subject", "product", "supplier_article", "nm_id"], dropna=False, as_index=False).agg(
            prev_month_gross_profit=("gross_profit", "sum"),
            plan_source=("plan_source", "first"),
        )
        plan["plan_month"] = plan["prev_month_gross_profit"] * 1.10
        self.diag.add("INFO", "plan", "План рассчитан от предыдущего месяца ×1.1", f"prev_month={prev_key}; rows={len(plan)}; plan_sum={plan['plan_month'].sum():.2f}")
        return plan

    def make_fact_table(self, source: pd.DataFrame, label_col: str, value_col: str, labels: List[str]) -> pd.DataFrame:
        rows: List[Dict[str, Any]] = []
        if source.empty:
            return pd.DataFrame(columns=["name", "level", "subject", "product", *labels, "План"])

        plan = self.plan_used()
        article_plan = plan.groupby("supplier_article", dropna=False)["plan_month"].sum().to_dict() if not plan.empty else {}
        product_plan = plan.groupby(["subject", "product"], dropna=False)["plan_month"].sum().to_dict() if not plan.empty else {}
        subject_plan = plan.groupby("subject", dropna=False)["plan_month"].sum().to_dict() if not plan.empty else {}

        def plan_value(level: str, subject: str, product: str = "", article: str = "", facts: Optional[List[float]] = None, daily: bool = False) -> float:
            facts = facts or []
            if level == "article":
                val = article_plan.get(article, np.nan)
            elif level == "product":
                val = product_plan.get((subject, product), np.nan)
            else:
                val = subject_plan.get(subject, np.nan)
            if pd.isna(val):
                return float(sum(facts)) if facts else 0.0
            if daily:
                return float(val) / self.days_in_month
            return float(val)

        is_daily = label_col == "weekday_label"
        for subject in TARGET_SUBJECTS:
            s = source[source["subject"] == subject].copy()
            if s.empty:
                continue
            values = [float(s.loc[s[label_col] == lab, value_col].sum()) for lab in labels]
            rows.append({"name": subject, "level": "category", "subject": subject, "product": "", **dict(zip(labels, values)), "План": plan_value("category", subject, facts=values, daily=is_daily)})

            products = s.groupby("product", dropna=False)[value_col].sum().sort_values(ascending=False).index.tolist()
            for prod in products:
                p = s[s["product"] == prod].copy()
                values = [float(p.loc[p[label_col] == lab, value_col].sum()) for lab in labels]
                rows.append({"name": prod, "level": "product", "subject": subject, "product": prod, **dict(zip(labels, values)), "План": plan_value("product", subject, prod, facts=values, daily=is_daily)})

                articles = p.groupby("supplier_article", dropna=False)[value_col].sum().sort_values(ascending=False).index.tolist()
                for art in articles:
                    a = p[p["supplier_article"] == art].copy()
                    values = [float(a.loc[a[label_col] == lab, value_col].sum()) for lab in labels]
                    rows.append({"name": art, "level": "article", "subject": subject, "product": prod, **dict(zip(labels, values)), "План": plan_value("article", subject, prod, art, values, daily=is_daily)})

        total_values = [float(source.loc[source[label_col] == lab, value_col].sum()) for lab in labels]
        total_plan = sum(v for v in subject_plan.values() if pd.notna(v))
        if is_daily:
            total_plan = total_plan / self.days_in_month if total_plan else sum(total_values)
        elif not total_plan:
            total_plan = sum(total_values)
        rows.append({"name": "Итого по всем 4 категориям", "level": "grand_total", "subject": "", "product": "", **dict(zip(labels, total_values)), "План": float(total_plan)})
        return pd.DataFrame(rows)

    def build_outputs(self) -> Dict[str, pd.DataFrame]:
        daily = self.daily_formula()
        day_labels = [f"{WEEKDAY_RU[i]} {d.strftime('%d.%m')}" for i, d in enumerate(self.week_days)]
        daily_block = self.make_fact_table(daily, "weekday_label", "gross_profit", day_labels) if not daily.empty else pd.DataFrame()
        # Future weekdays must be visually empty in main block.
        if not daily_block.empty:
            for d, lab in zip(self.week_days, day_labels):
                if d > self.latest_day:
                    daily_block[lab] = np.nan

        weekly = self.weekly_abc_current_month()
        week_labels = sorted(weekly["week_label"].dropna().astype(str).unique(), key=lambda x: x) if not weekly.empty else []
        weekly_block = self.make_fact_table(weekly, "week_label", "gross_profit", week_labels) if not weekly.empty else pd.DataFrame()

        monthly = self.monthly_abc_current_year()
        month_labels = [f"{self.current_year:04d}-{m:02d}" for m in range(1, self.current_month + 1)]
        monthly_block = self.make_fact_table(monthly, "month_key", "gross_profit", month_labels) if not monthly.empty else pd.DataFrame()

        return {
            "main_daily": daily_block,
            "main_weekly": weekly_block,
            "main_monthly": monthly_block,
            "dictionary": self.dictionary,
            "orders_used": DictionaryLayer.filter_target(DictionaryLayer.enrich_by_nm(self.pack.orders, self.dictionary, self.diag, "orders_used")),
            "funnel_used": self.buyout_90(),
            "ads_used": self.ads_by_article_day(),
            "economics_used": DictionaryLayer.filter_target(self.pack.economics),
            "abc_weekly_used": weekly,
            "abc_monthly_used": monthly,
            "plan_used": self.plan_used(),
            "stock_used": DictionaryLayer.filter_target(DictionaryLayer.enrich_by_nm(self.pack.stock, self.dictionary, self.diag, "stock_used")) if not self.pack.stock.empty else pd.DataFrame(),
            "daily_formula": daily,
            "diagnostics": self.diag.frame(),
            "example_daily": daily[daily["supplier_article"].isin(EXAMPLE_ARTICLES)].copy() if not daily.empty else pd.DataFrame(),
            "example_weekly": self.example_weekly(EXAMPLE_ARTICLES),
        }

    def example_weekly(self, articles: List[str]) -> pd.DataFrame:
        weekly = self.weekly_abc_current_month()
        daily = self.daily_formula()
        frames: List[pd.DataFrame] = []
        if not daily.empty:
            d = daily[daily["supplier_article"].isin(articles)].copy()
            if not d.empty:
                d["week_label"] = d["week_code"]
                calc = d.groupby(["supplier_article", "subject", "product", "week_code"], as_index=False).agg(
                    orders_qty=("orders_qty", "sum"),
                    buyout_qty=("buyout_qty", "sum"),
                    revenue=("revenue", "sum"),
                    commission_wb=("commission_wb", "sum"),
                    acquiring=("acquiring", "sum"),
                    logistics_direct_total=("logistics_direct_total", "sum"),
                    logistics_return_total=("logistics_return_total", "sum"),
                    storage_total=("storage_total", "sum"),
                    other_costs_total=("other_costs_total", "sum"),
                    cost_total=("cost_total", "sum"),
                    ad_spend=("ad_spend", "sum"),
                    gross_profit=("gross_profit", "sum"),
                )
                calc["source"] = "stage1_formula_current_week"
                frames.append(calc)
        if not weekly.empty:
            w = weekly[weekly["supplier_article"].isin(articles)].copy()
            if not w.empty:
                wcalc = w.groupby(["supplier_article", "subject", "product", "week_code"], as_index=False).agg(
                    orders_qty=("orders", "sum"),
                    gross_profit=("gross_profit", "sum"),
                )
                wcalc["source"] = "abc_weekly"
                frames.append(wcalc)
        return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()


# =============================================================================
# STAGE 2: GP POTENTIAL, LOCALIZATION, CONCLUSIONS
# =============================================================================

MIN_STOCK_DAYS = 2.0
PLAN_GROWTH_FACTOR = 1.10

CENTRAL_POOL = [
    "коледино", "электросталь", "белая дача", "вешки", "вёшки",
    "рязань", "тюшев", "тула", "алексин", "владимир", "котовск", "воронеж",
]
SOUTH_POOL = ["краснодар", "невинномысск", "волгоград", "ростов", "аксай"]
VOLGA_POOL = ["казань", "пенза", "сарапул", "новосемейкино", "самара"]
NW_POOL = ["шушары", "санкт", "спб", "уткина", "заводь"]
URAL_POOL = ["екатеринбург", "челябинск", "перм"]
SIBERIA_POOL = ["новосибирск", "красноярск", "кемеров"]


def warehouse_pool_name(warehouse: Any) -> str:
    w = norm_key(warehouse)
    for name, pool in [
        ("ЦФО", CENTRAL_POOL),
        ("Юг", SOUTH_POOL),
        ("Поволжье", VOLGA_POOL),
        ("Северо-Запад", NW_POOL),
        ("Урал", URAL_POOL),
        ("Сибирь", SIBERIA_POOL),
    ]:
        if any(token in w for token in pool):
            return name
    return f"СКЛАД:{normalize_text(warehouse)}"


class Stage2Layer:
    def __init__(self, pack: DataPack, dictionary: pd.DataFrame, stage1: Stage1Layer):
        self.pack = pack
        self.dictionary = dictionary
        self.stage1 = stage1
        self.diag = pack.diagnostics
        self.latest_day = pack.latest_day
        self.current_year = int(self.latest_day.year)
        self.current_month = int(self.latest_day.month)
        self.month_start = pd.Timestamp(date(self.current_year, self.current_month, 1))
        self.days_in_month = calendar.monthrange(self.current_year, self.current_month)[1]
        self.days_elapsed = max(1, min(int(self.latest_day.day), self.days_in_month))
        self.current_month_key = self.latest_day.to_period("M").strftime("%Y-%m")
        self.lookback_start = self.latest_day - pd.Timedelta(days=89)

    def weekly_abc_90d(self) -> pd.DataFrame:
        weekly = DictionaryLayer.enrich_by_nm(self.pack.abc_weekly, self.dictionary, self.diag, "stage2_abc_weekly_90d")
        weekly = DictionaryLayer.filter_target(weekly)
        if weekly.empty:
            return weekly
        weekly = weekly[(weekly["period_end"] >= self.lookback_start) & (weekly["period_start"] <= self.latest_day)].copy()
        weekly["days_in_period"] = (pd.to_datetime(weekly["period_end"]) - pd.to_datetime(weekly["period_start"])).dt.days + 1
        weekly["days_in_period"] = weekly["days_in_period"].clip(lower=1).fillna(7)
        # Aggregate duplicate article rows inside one ABC period before calculating article potential.
        grouped = weekly.groupby(["subject", "product", "supplier_article", "nm_id", "week_code", "week_label", "period_start", "period_end"], dropna=False, as_index=False).agg(
            gross_profit=("gross_profit", "sum"),
            orders=("orders", "sum"),
            days_in_period=("days_in_period", "max"),
        )
        grouped["weekly_gp_per_day"] = grouped["gross_profit"] / grouped["days_in_period"].replace(0, np.nan)
        grouped["weekly_gp_per_day"] = grouped["weekly_gp_per_day"].fillna(0)
        return grouped

    def current_month_fact_from_weekly(self) -> pd.DataFrame:
        weekly = DictionaryLayer.enrich_by_nm(self.pack.abc_weekly, self.dictionary, self.diag, "stage2_current_month_weekly")
        weekly = DictionaryLayer.filter_target(weekly)
        if weekly.empty:
            return weekly
        weekly["period_mid"] = pd.to_datetime(weekly["period_start"]) + (pd.to_datetime(weekly["period_end"]) - pd.to_datetime(weekly["period_start"])) / 2
        weekly = weekly[weekly["period_mid"].dt.to_period("M").astype(str) == self.current_month_key].copy()
        return weekly.groupby(["subject", "product", "supplier_article", "nm_id"], dropna=False, as_index=False).agg(
            current_month_gross_profit=("gross_profit", "sum"),
            current_month_orders=("orders", "sum"),
        )

    def gp_potential_90d(self) -> pd.DataFrame:
        weekly = self.weekly_abc_90d()
        plan = self.stage1.plan_used()
        current = self.current_month_fact_from_weekly()
        if weekly.empty:
            base_cols = ["subject", "product", "supplier_article", "nm_id"]
            if not plan.empty:
                base = plan[base_cols].drop_duplicates().copy()
            else:
                base = self.dictionary[base_cols].drop_duplicates().copy() if not self.dictionary.empty else pd.DataFrame(columns=base_cols)
            base["weeks_in_analysis"] = 0
            base["gp_90d"] = 0.0
            base["avg_gp_per_day"] = 0.0
            base["target_gp_per_day"] = 0.0
            base["best_week_gp_per_day"] = 0.0
        else:
            rows = []
            for keys, g in weekly.groupby(["subject", "product", "supplier_article", "nm_id"], dropna=False):
                vals = pd.to_numeric(g["weekly_gp_per_day"], errors="coerce").fillna(0)
                avg = float(vals.mean()) if len(vals) else 0.0
                above = vals[vals > avg]
                target = float(above.mean()) if len(above) else avg
                rows.append({
                    "subject": keys[0],
                    "product": keys[1],
                    "supplier_article": keys[2],
                    "nm_id": keys[3],
                    "weeks_in_analysis": int(g["week_code"].nunique()),
                    "gp_90d": float(g["gross_profit"].sum()),
                    "avg_gp_per_day": avg,
                    "target_gp_per_day": target,
                    "best_week_gp_per_day": float(vals.max()) if len(vals) else 0.0,
                })
            base = pd.DataFrame(rows)
        if not plan.empty:
            base = base.merge(plan[["subject", "product", "supplier_article", "nm_id", "prev_month_gross_profit", "plan_month"]], on=["subject", "product", "supplier_article", "nm_id"], how="outer")
        else:
            base["prev_month_gross_profit"] = 0.0
            base["plan_month"] = 0.0
        if not current.empty:
            base = base.merge(current, on=["subject", "product", "supplier_article", "nm_id"], how="left")
        else:
            base["current_month_gross_profit"] = 0.0
            base["current_month_orders"] = 0.0
        for c in ["weeks_in_analysis", "gp_90d", "avg_gp_per_day", "target_gp_per_day", "best_week_gp_per_day", "prev_month_gross_profit", "plan_month", "current_month_gross_profit", "current_month_orders"]:
            if c not in base.columns:
                base[c] = 0.0
            base[c] = pd.to_numeric(base[c], errors="coerce").fillna(0)
        # If there is no previous-month base but there is current fact, neutralize division by zero and treat current fact as plan.
        no_plan_has_fact = (base["plan_month"] <= 0) & (base["current_month_gross_profit"] > 0)
        base.loc[no_plan_has_fact, "plan_month"] = base.loc[no_plan_has_fact, "current_month_gross_profit"]
        base["plan_to_date"] = base["plan_month"] / self.days_in_month * self.days_elapsed
        base["plan_completion_pct"] = np.where(base["plan_month"] > 0, base["current_month_gross_profit"] / base["plan_month"] * 100, np.nan)
        base["plan_to_date_completion_pct"] = np.where(base["plan_to_date"] > 0, base["current_month_gross_profit"] / base["plan_to_date"] * 100, np.nan)
        base["current_gp_per_day"] = base["current_month_gross_profit"] / self.days_elapsed
        base["potential_status"] = np.select(
            [
                base["weeks_in_analysis"] <= 0,
                base["current_gp_per_day"] < base["avg_gp_per_day"],
                base["current_gp_per_day"] >= base["target_gp_per_day"],
                base["current_gp_per_day"] >= base["target_gp_per_day"] * 0.90,
            ],
            [
                "Нет истории ABC",
                "Ниже минимальной планки",
                "Выше целевого уровня",
                "Идёт к целевому уровню",
            ],
            default="В пределах минимальной планки",
        )
        base["metric_note"] = "Средняя ВП/день = минимум; целевая ВП/день = среднее недель выше среднего"
        return base.sort_values(["subject", "product", "supplier_article"]).reset_index(drop=True)

    def stock_enriched(self) -> pd.DataFrame:
        stock = self.pack.stock.copy()
        if stock.empty:
            return stock
        stock = DictionaryLayer.enrich_by_nm(stock, self.dictionary, self.diag, "stage2_stock")
        stock = DictionaryLayer.filter_target(stock)
        if stock.empty:
            return stock
        stock["warehouse_pool"] = stock["warehouse"].map(warehouse_pool_name)
        stock = stock.groupby(["day", "subject", "product", "supplier_article", "nm_id", "warehouse", "warehouse_pool"], dropna=False, as_index=False).agg(stock_qty=("stock_qty", "sum"))
        return stock

    def warehouse_weights(self) -> pd.DataFrame:
        orders = DictionaryLayer.enrich_by_nm(self.pack.orders, self.dictionary, self.diag, "stage2_orders_warehouse_weights")
        orders = DictionaryLayer.filter_target(orders)
        if orders.empty:
            return pd.DataFrame()
        orders = orders[(orders["day"] >= self.lookback_start) & (orders["day"] <= self.latest_day)].copy()
        orders["warehouse"] = orders["warehouse"].map(normalize_text)
        orders = orders[orders["warehouse"].ne("")].copy()
        if orders.empty:
            return pd.DataFrame()
        wh = orders.groupby(["subject", "product", "supplier_article", "nm_id", "warehouse"], dropna=False, as_index=False).agg(orders_90=("orders", "sum"))
        wh["article_orders_90"] = wh.groupby(["supplier_article", "nm_id"], dropna=False)["orders_90"].transform("sum")
        wh["warehouse_weight"] = np.where(wh["article_orders_90"] > 0, wh["orders_90"] / wh["article_orders_90"], 0)
        wh["avg_daily_orders_warehouse"] = wh["orders_90"] / 90.0
        wh = wh.sort_values(["supplier_article", "nm_id", "warehouse_weight"], ascending=[True, True, False])
        wh["cum_weight"] = wh.groupby(["supplier_article", "nm_id"], dropna=False)["warehouse_weight"].cumsum()
        wh["prev_cum_weight"] = wh.groupby(["supplier_article", "nm_id"], dropna=False)["cum_weight"].shift(1).fillna(0)
        # Keep warehouses required to cover approximately 97%; include the first warehouse that crosses the threshold.
        wh["is_key_warehouse"] = (wh["prev_cum_weight"] < 0.97) | (wh["warehouse_weight"] >= 0.03)
        wh = wh[wh["is_key_warehouse"]].copy()
        wh["warehouse_pool"] = wh["warehouse"].map(warehouse_pool_name)
        wh["needed_stock_qty"] = wh["avg_daily_orders_warehouse"] * MIN_STOCK_DAYS
        return wh

    def localization_detail(self) -> pd.DataFrame:
        stock = self.stock_enriched()
        weights = self.warehouse_weights()
        if stock.empty or weights.empty:
            self.diag.add("WARN", "localization", "Недостаточно данных для локализации", f"stock_rows={len(stock)}; weights_rows={len(weights)}")
            return pd.DataFrame()
        stock_days = stock["day"].dropna().sort_values().unique()
        # Grid: every stock day x every key warehouse, so missing stock rows become zero.
        grid = weights.assign(_key=1).merge(pd.DataFrame({"day": stock_days, "_key": 1}), on="_key", how="outer").drop(columns="_key")
        detail = grid.merge(stock[["day", "supplier_article", "nm_id", "warehouse", "stock_qty"]], on=["day", "supplier_article", "nm_id", "warehouse"], how="left")
        detail["stock_qty"] = detail["stock_qty"].fillna(0)
        pool_stock = stock.groupby(["day", "supplier_article", "nm_id", "warehouse_pool"], dropna=False, as_index=False).agg(pool_stock_qty=("stock_qty", "sum"))
        detail = detail.merge(pool_stock, on=["day", "supplier_article", "nm_id", "warehouse_pool"], how="left")
        detail["pool_stock_qty"] = detail["pool_stock_qty"].fillna(0)
        detail["replacement_stock_qty"] = (detail["pool_stock_qty"] - detail["stock_qty"]).clip(lower=0)
        detail["direct_available"] = detail["stock_qty"] >= detail["needed_stock_qty"]
        detail["replacement_available"] = (~detail["direct_available"]) & (detail["replacement_stock_qty"] >= detail["needed_stock_qty"])
        detail["available_with_replacement"] = detail["direct_available"] | detail["replacement_available"]
        detail["direct_coverage_contribution_pct"] = np.where(detail["direct_available"], detail["warehouse_weight"] * 100, 0)
        detail["replacement_coverage_contribution_pct"] = np.where(detail["available_with_replacement"], detail["warehouse_weight"] * 100, 0)
        detail["localization_status"] = np.select(
            [detail["direct_available"], detail["replacement_available"]],
            ["Покрыт напрямую", "Покрыт заменой"],
            default="Не покрыт",
        )
        return detail.sort_values(["day", "supplier_article", "warehouse_weight"], ascending=[True, True, False]).reset_index(drop=True)

    def localization_summary(self, detail: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        if detail is None:
            detail = self.localization_detail()
        if detail.empty:
            return pd.DataFrame()
        summary = detail.groupby(["day", "subject", "product", "supplier_article", "nm_id"], dropna=False, as_index=False).agg(
            direct_localization_pct=("direct_coverage_contribution_pct", "sum"),
            localization_with_replacements_pct=("replacement_coverage_contribution_pct", "sum"),
            stock_qty_total=("stock_qty", "sum"),
            key_warehouses=("warehouse", "nunique"),
            uncovered_warehouses=("localization_status", lambda s: int((s == "Не покрыт").sum())),
        )
        latest_by_article = summary.sort_values("day").groupby(["supplier_article", "nm_id"], dropna=False).tail(1).copy()
        avg_period = summary.groupby(["supplier_article", "nm_id"], dropna=False).agg(
            avg_direct_localization_pct=("direct_localization_pct", "mean"),
            avg_localization_with_replacements_pct=("localization_with_replacements_pct", "mean"),
        ).reset_index()
        latest_by_article = latest_by_article.merge(avg_period, on=["supplier_article", "nm_id"], how="left")
        latest_by_article["localization_status"] = pd.cut(
            latest_by_article["localization_with_replacements_pct"],
            bins=[-1, 30, 60, 85, 1000],
            labels=["Критично", "Плохая локализация", "Риск", "Норма"],
        ).astype(str)
        return latest_by_article.sort_values(["subject", "product", "supplier_article"]).reset_index(drop=True)

    def conclusions(self, potential: Optional[pd.DataFrame] = None, loc_summary: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        if potential is None:
            potential = self.gp_potential_90d()
        if loc_summary is None:
            loc_summary = self.localization_summary()
        out = potential.copy()
        if not loc_summary.empty:
            out = out.merge(
                loc_summary[["supplier_article", "nm_id", "direct_localization_pct", "localization_with_replacements_pct", "stock_qty_total", "uncovered_warehouses", "localization_status"]],
                on=["supplier_article", "nm_id"],
                how="left",
            )
        for c in ["direct_localization_pct", "localization_with_replacements_pct", "stock_qty_total", "uncovered_warehouses"]:
            if c not in out.columns:
                out[c] = np.nan
        if "localization_status" not in out.columns:
            out["localization_status"] = "Нет данных"
        out["plan_tempo_status"] = np.select(
            [out["plan_to_date_completion_pct"] < 90, out["plan_to_date_completion_pct"] > 110],
            ["Отстаём от плана", "Опережаем план"],
            default="Идём по плану",
        )
        reasons = []
        recommendations = []
        for _, r in out.iterrows():
            tempo = r.get("plan_tempo_status", "")
            loc = r.get("localization_with_replacements_pct", np.nan)
            direct_loc = r.get("direct_localization_pct", np.nan)
            pot_status = r.get("potential_status", "")
            if tempo == "Отстаём от плана":
                if pd.notna(loc) and loc < 85:
                    reason = f"Отставание связано с локализацией/остатками: покрытие с заменами {loc:.1f}%, прямое {direct_loc if pd.notna(direct_loc) else 0:.1f}%."
                    rec = "Восстановить остатки на ключевых складах и складах-заменителях; сначала закрыть склады с максимальным весом заказов."
                elif "Ниже минимальной" in str(pot_status):
                    reason = "Локализация не выглядит главным ограничителем; текущая ВП/день ниже минимальной 90-дневной планки."
                    rec = "Проверить цену, рекламу, спрос и карточку; вернуть товар хотя бы к средней ВП/день за 90 дней."
                else:
                    reason = "Темп ниже плана, но по потенциалу товар не провален; план мог вырасти быстрее фактического темпа."
                    rec = "Сравнить условия лучших недель: ассортимент, цена, рекламная поддержка и наличие."
            elif tempo == "Опережаем план":
                reason = "Факт на дату выше плана; товар опережает плановый темп."
                rec = "Удерживать условия роста: не допускать просадки остатков, контролировать рекламу и маржинальность."
            else:
                reason = "Факт близок к плановому темпу."
                rec = "Поддерживать текущие условия и следить, чтобы ВП/день не упала ниже минимальной планки."
            reasons.append(reason)
            recommendations.append(rec)
        out["reason"] = reasons
        out["recommendation"] = recommendations
        cols_first = [
            "subject", "product", "supplier_article", "nm_id", "plan_tempo_status",
            "plan_to_date_completion_pct", "plan_completion_pct", "current_month_gross_profit", "plan_month",
            "avg_gp_per_day", "target_gp_per_day", "current_gp_per_day", "potential_status",
            "direct_localization_pct", "localization_with_replacements_pct", "localization_status", "reason", "recommendation",
        ]
        rest = [c for c in out.columns if c not in cols_first]
        return out[cols_first + rest].sort_values(["subject", "product", "supplier_article"]).reset_index(drop=True)

    def build_outputs(self) -> Dict[str, pd.DataFrame]:
        potential_weekly = self.weekly_abc_90d()
        potential = self.gp_potential_90d()
        loc_detail = self.localization_detail()
        loc_summary = self.localization_summary(loc_detail)
        conclusions = self.conclusions(potential, loc_summary)
        return {
            "stage2_gp_potential_weekly_90d": potential_weekly,
            "stage2_gp_potential_90d": potential,
            "stage2_localization_detail": loc_detail,
            "stage2_localization_summary": loc_summary,
            "stage2_conclusions": conclusions,
        }


# =============================================================================
# EXPORT LAYER
# =============================================================================

def write_dataframe_sheet(wb: Workbook, title: str, df: pd.DataFrame) -> None:
    ws = wb.create_sheet(title[:31])
    if df is None or df.empty:
        ws.cell(1, 1, "Нет данных")
        return
    safe = df.copy()
    for col in safe.columns:
        if pd.api.types.is_datetime64_any_dtype(safe[col]):
            safe[col] = safe[col].dt.strftime("%Y-%m-%d")
    ws.append(list(safe.columns))
    for row in safe.itertuples(index=False, name=None):
        ws.append(list(row))
    for cell in ws[1]:
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = Alignment(horizontal="center")
    for row in ws.iter_rows():
        for cell in row:
            cell.border = BORDER
            if isinstance(cell.value, (int, float)) and not isinstance(cell.value, bool):
                cell.number_format = '# ##0.00'
    ws.freeze_panes = "A2"
    autofit(ws)


def autofit(ws) -> None:
    for col_idx in range(1, ws.max_column + 1):
        letter = get_column_letter(col_idx)
        max_len = 8
        for cell in ws[letter]:
            value = "" if cell.value is None else str(cell.value)
            max_len = max(max_len, min(len(value), 60))
        ws.column_dimensions[letter].width = max_len + 2


def write_main_block(ws, start_row: int, title: str, df: pd.DataFrame) -> int:
    if df is None or df.empty:
        ws.cell(start_row, 1, title)
        ws.cell(start_row + 1, 1, "Нет данных")
        return start_row + 3
    display_cols = ["name"] + [c for c in df.columns if c not in {"name", "level", "subject", "product"}]
    max_col = len(display_cols)

    ws.merge_cells(start_row=start_row, start_column=1, end_row=start_row, end_column=max_col)
    title_cell = ws.cell(start_row, 1, title)
    title_cell.fill = TITLE_FILL
    title_cell.font = Font(color="FFFFFF", bold=True, size=14)
    title_cell.alignment = Alignment(horizontal="center")

    header_row = start_row + 1
    headers = ["Категория" if c == "name" else c for c in display_cols]
    for col_idx, header in enumerate(headers, start=1):
        cell = ws.cell(header_row, col_idx, header)
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = Alignment(horizontal="center", vertical="center")
        cell.border = BORDER

    excel_row = header_row + 1
    for record in df.to_dict("records"):
        level = record.get("level", "")
        subject = record.get("subject", "")
        for col_idx, col in enumerate(display_cols, start=1):
            cell = ws.cell(excel_row, col_idx, record.get(col, None))
            cell.border = BORDER
            cell.alignment = Alignment(vertical="center")
            if col_idx > 1 and isinstance(cell.value, (int, float, np.number)) and not pd.isna(cell.value):
                cell.number_format = money_format()
            if col == "План":
                cell.font = Font(bold=True)
                cell.fill = PLAN_FILL
        if level == "category":
            for cell in ws[excel_row]:
                cell.fill = CATEGORY_FILLS.get(subject, CATEGORY_FILLS[TARGET_SUBJECTS[0]])
                cell.font = Font(bold=True)
            ws.row_dimensions[excel_row].outlineLevel = 0
            ws.row_dimensions[excel_row].hidden = False
        elif level == "product":
            for cell in ws[excel_row]:
                cell.fill = PRODUCT_FILL
                cell.font = Font(bold=True)
            ws.row_dimensions[excel_row].outlineLevel = 1
            ws.row_dimensions[excel_row].hidden = True
        elif level == "article":
            for cell in ws[excel_row]:
                cell.fill = ARTICLE_FILL
            ws.row_dimensions[excel_row].outlineLevel = 2
            ws.row_dimensions[excel_row].hidden = True
        elif level == "grand_total":
            for cell in ws[excel_row]:
                cell.fill = TOTAL_FILL
                cell.font = Font(bold=True)
            ws.row_dimensions[excel_row].outlineLevel = 0
            ws.row_dimensions[excel_row].hidden = False
        excel_row += 1

    ws.sheet_properties.outlinePr.summaryBelow = False
    return excel_row + 2


def export_all(outputs: Dict[str, pd.DataFrame], local_dir: Path) -> Tuple[Path, Path, Path]:
    local_dir.mkdir(parents=True, exist_ok=True)
    report_path = local_dir / MAIN_REPORT_NAME
    tech_path = local_dir / TECH_REPORT_NAME
    example_path = local_dir / EXAMPLE_REPORT_NAME

    wb = Workbook()
    ws = wb.active
    ws.title = "Сводка"
    row = 1
    row = write_main_block(ws, row, "Валовая прибыль", outputs.get("main_daily", pd.DataFrame()))
    row = write_main_block(ws, row, "Текущий месяц по неделям", outputs.get("main_weekly", pd.DataFrame()))
    row = write_main_block(ws, row, "Месяцы текущего года", outputs.get("main_monthly", pd.DataFrame()))
    ws.freeze_panes = "B3"
    autofit(ws)
    wb.save(report_path)

    tech_wb = Workbook()
    tech_wb.remove(tech_wb.active)
    for sheet in [
        "dictionary",
        "orders_used",
        "funnel_used",
        "ads_used",
        "economics_used",
        "abc_weekly_used",
        "abc_monthly_used",
        "plan_used",
        "stock_used",
        "daily_formula",
        "stage2_gp_potential_weekly_90d",
        "stage2_gp_potential_90d",
        "stage2_localization_summary",
        "stage2_localization_detail",
        "stage2_conclusions",
        "diagnostics",
    ]:
        write_dataframe_sheet(tech_wb, sheet, outputs.get(sheet, pd.DataFrame()))
    tech_wb.save(tech_path)

    ex_wb = Workbook()
    ex_wb.remove(ex_wb.active)
    write_dataframe_sheet(ex_wb, "daily_901", outputs.get("example_daily", pd.DataFrame()))
    write_dataframe_sheet(ex_wb, "weekly_901", outputs.get("example_weekly", pd.DataFrame()))
    ex_wb.save(example_path)

    return report_path, tech_path, example_path


# =============================================================================
# RUNNER
# =============================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TOPFACE WB report: Валовая прибыль")
    parser.add_argument("--root", default=".", help="Local reports root when S3 env vars are not set")
    parser.add_argument("--reports-root", default="Отчёты", help="Base reports folder/key")
    parser.add_argument("--store", default="TOPFACE", help="Store/brand folder")
    parser.add_argument("--out-subdir", default="Отчёты/Объединенный отчет/TOPFACE", help="Output folder/key")
    parser.add_argument("--local-tmp", default="/tmp/wb_topface_gp", help="Local temporary folder for generated workbooks")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    diagnostics = Diagnostics()
    storage = make_storage(args.root)
    loader = LoaderLayer(storage, args.reports_root, args.store, diagnostics)
    pack = loader.load_all()

    dictionary = DictionaryLayer(pack).build()
    stage1 = Stage1Layer(pack, dictionary)
    outputs = stage1.build_outputs()
    stage2 = Stage2Layer(pack, dictionary, stage1)
    outputs.update(stage2.build_outputs())
    outputs["diagnostics"] = pack.diagnostics.frame()

    local_report, local_tech, local_example = export_all(outputs, Path(args.local_tmp))

    out_report = f"{args.out_subdir.strip('/')}/{MAIN_REPORT_NAME}"
    out_tech = f"{args.out_subdir.strip('/')}/{TECH_REPORT_NAME}"
    out_example = f"{args.out_subdir.strip('/')}/{EXAMPLE_REPORT_NAME}"

    storage.write_bytes(out_report, local_report.read_bytes())
    storage.write_bytes(out_tech, local_tech.read_bytes())
    storage.write_bytes(out_example, local_example.read_bytes())

    # GitHub Actions upload-artifact searches the runner filesystem, not S3.
    # Therefore always keep local copies under --root/--out-subdir as well.
    local_out_dir = Path(args.root) / args.out_subdir
    local_out_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(local_report, local_out_dir / MAIN_REPORT_NAME)
    shutil.copy2(local_tech, local_out_dir / TECH_REPORT_NAME)
    shutil.copy2(local_example, local_out_dir / EXAMPLE_REPORT_NAME)

    log(f"Saved local copies: {local_out_dir}")
    log(f"Saved: {out_report}")
    log(f"Saved: {out_tech}")
    log(f"Saved: {out_example}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
