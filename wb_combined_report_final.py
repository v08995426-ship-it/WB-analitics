#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WB TOPFACE report from scratch: «Валовая Прибыль - НДС».

Creates exactly 3 output workbooks and overwrites them on every run:
1) Отчёты/Объединенный отчет/TOPFACE/Объединенный_отчет_TOPFACE.xlsx
2) Отчёты/Объединенный отчет/TOPFACE/Технические_расчеты_TOPFACE.xlsx
3) Отчёты/Объединенный отчет/TOPFACE/Пример_расчета_901_TOPFACE.xlsx

Stage 1 only. Stage 2 is reserved by module stubs and diagnostics schema.
"""

from __future__ import annotations

import argparse
import calendar
import io
import os
import re
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
    "plan": ["План", "ВП-НДС", "Валовая Прибыль - НДС", "Валовая прибыль - НДС"],
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
                out["vat"] = out["gross_revenue"] * 7.0 / 107.0
                out["gp_minus_nds"] = out["gross_profit"] - out["vat"]
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
                f"вп ндс {target_month} {latest_day.year}",
                f"валовая прибыль ндс {target_month} {latest_day.year}",
                f"план {target_month} {latest_day.year}",
            ]
            for col in raw_cols:
                k = norm_key(col).replace("-", " ")
                if any(p in k for p in patterns):
                    chosen_col = col
                    break
            if chosen_col is None:
                for col in raw_cols:
                    k = norm_key(col)
                    if str(latest_day.year) in k and norm_key(target_month) in k and ("ндс" in k or "план" in k):
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

        return DataPack(
            orders=orders,
            funnel=funnel,
            ads_raw=ads_raw,
            ads_used=ads_used,
            economics=economics,
            abc_weekly=abc_weekly,
            abc_monthly=abc_monthly,
            plan=plan,
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
        grouped = orders.groupby(["day", "week_code", "subject", "product", "supplier_article", "nm_id"], dropna=False, as_index=False).agg(
            orders_qty=("orders", "sum"),
            finished_price=("finished_price", "mean"),
            price_with_disc=("price_with_disc", "mean"),
            spp=("spp", "mean"),
        )
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
        enriched["revenue"] = enriched["buyout_qty"] * enriched["price_with_disc"].fillna(0)
        enriched["commission_wb"] = enriched["revenue"] * enriched["commission_pct"] / 100.0
        enriched["acquiring"] = enriched["revenue"] * enriched["acquiring_pct"] / 100.0
        enriched["logistics_direct_total"] = enriched["buyout_qty"] * enriched["logistics_direct"]
        enriched["logistics_return_total"] = enriched["buyout_qty"] * enriched["logistics_return"]
        enriched["storage_total"] = enriched["buyout_qty"] * enriched["storage"]
        enriched["other_costs_total"] = enriched["buyout_qty"] * enriched["other_costs"]
        enriched["cost_total"] = enriched["buyout_qty"] * enriched["cost"]
        enriched["vat"] = enriched["buyout_qty"] * enriched["finished_price"].fillna(0) * 7.0 / 107.0
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
        enriched["gp_minus_nds"] = enriched["gross_profit"] - enriched["vat"]
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
                    vat=("vat", "sum"),
                    gp_minus_nds=("gp_minus_nds", "sum"),
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
        plan = DictionaryLayer.enrich_by_nm(self.pack.plan, self.dictionary, self.diag, "plan") if not self.pack.plan.empty else self.pack.plan.copy()
        plan = DictionaryLayer.filter_target(plan)
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
        daily_block = self.make_fact_table(daily, "weekday_label", "gp_minus_nds", day_labels) if not daily.empty else pd.DataFrame()
        # Future weekdays must be visually empty in main block.
        if not daily_block.empty:
            for d, lab in zip(self.week_days, day_labels):
                if d > self.latest_day:
                    daily_block[lab] = np.nan

        weekly = self.weekly_abc_current_month()
        week_labels = sorted(weekly["week_label"].dropna().astype(str).unique(), key=lambda x: x) if not weekly.empty else []
        weekly_block = self.make_fact_table(weekly, "week_label", "gp_minus_nds", week_labels) if not weekly.empty else pd.DataFrame()

        monthly = self.monthly_abc_current_year()
        month_labels = [f"{self.current_year:04d}-{m:02d}" for m in range(1, self.current_month + 1)]
        monthly_block = self.make_fact_table(monthly, "month_key", "gp_minus_nds", month_labels) if not monthly.empty else pd.DataFrame()

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
                    vat=("vat", "sum"),
                    gross_profit=("gross_profit", "sum"),
                    gp_minus_nds=("gp_minus_nds", "sum"),
                )
                calc["source"] = "stage1_formula_current_week"
                frames.append(calc)
        if not weekly.empty:
            w = weekly[weekly["supplier_article"].isin(articles)].copy()
            if not w.empty:
                wcalc = w.groupby(["supplier_article", "subject", "product", "week_code"], as_index=False).agg(
                    orders_qty=("orders", "sum"),
                    gross_profit=("gross_profit", "sum"),
                    vat=("vat", "sum"),
                    gp_minus_nds=("gp_minus_nds", "sum"),
                )
                wcalc["source"] = "abc_weekly"
                frames.append(wcalc)
        return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()


# =============================================================================
# STAGE 2 PLACEHOLDER
# =============================================================================

class Stage2Layer:
    """Reserved architecture for future causal analysis: traffic, conversion, price/SPP, RRP, stock coverage."""

    def __init__(self, *_: Any, **__: Any):
        pass


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
    row = write_main_block(ws, row, "Валовая Прибыль - НДС", outputs.get("main_daily", pd.DataFrame()))
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
        "daily_formula",
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
    parser = argparse.ArgumentParser(description="TOPFACE WB report: Валовая Прибыль - НДС")
    parser.add_argument("--root", default=".", help="Local reports root when S3 env vars are not set")
    parser.add_argument("--reports-root", default="Отчёты", help="Base reports folder/key")
    parser.add_argument("--store", default="TOPFACE", help="Store/brand folder")
    parser.add_argument("--out-subdir", default="Отчёты/Объединенный отчет/TOPFACE", help="Output folder/key")
    parser.add_argument("--local-tmp", default="/tmp/wb_topface_gp_nds", help="Local temporary folder for generated workbooks")
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

    local_report, local_tech, local_example = export_all(outputs, Path(args.local_tmp))

    out_report = f"{args.out_subdir.strip('/')}/{MAIN_REPORT_NAME}"
    out_tech = f"{args.out_subdir.strip('/')}/{TECH_REPORT_NAME}"
    out_example = f"{args.out_subdir.strip('/')}/{EXAMPLE_REPORT_NAME}"

    storage.write_bytes(out_report, local_report.read_bytes())
    storage.write_bytes(out_tech, local_tech.read_bytes())
    storage.write_bytes(out_example, local_example.read_bytes())

    log(f"Saved: {out_report}")
    log(f"Saved: {out_tech}")
    log(f"Saved: {out_example}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
