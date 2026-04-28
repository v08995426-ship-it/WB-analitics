#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import calendar
import io
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import boto3
import numpy as np
import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

TARGET_SUBJECTS = [
    "Кисти косметические",
    "Помады",
    "Блески",
    "Косметические карандаши",
]

EXCLUDE_ARTICLES = {
    "CZ420", "CZ420БРОВИ", "CZ420ГЛАЗА", "DE49", "DE49ГЛАЗА", "PT901", "CZ420", "CZ420ГЛАЗА"
}

EXAMPLE_ARTICLES = ["901/5", "901/8", "901/14", "901/18"]

THIN = Side(style="thin", color="D9D9D9")
BORDER = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)
FILL_HEADER = PatternFill("solid", fgColor="DDEBF7")
FILL_SECTION = PatternFill("solid", fgColor="E2F0D9")
FILL_CATEGORY = PatternFill("solid", fgColor="EAF4FF")
FILL_PRODUCT = PatternFill("solid", fgColor="F7FBFF")
FILL_TOTAL = PatternFill("solid", fgColor="FFF2CC")


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("\xa0", " ").strip()
    return re.sub(r"\s+", " ", text)


def norm_key(value: Any) -> str:
    text = normalize_text(value).lower().replace("ё", "е")
    text = re.sub(r"[^\w]+", " ", text, flags=re.UNICODE)
    return re.sub(r"\s+", " ", text).strip()


def clean_article(value: Any) -> str:
    text = normalize_text(value)
    if text.lower() in {"nan", "none"}:
        return ""
    return text


def upper_article(value: Any) -> str:
    return clean_article(value).upper()


def is_excluded_article(value: Any) -> bool:
    return upper_article(value) in EXCLUDE_ARTICLES


def extract_code(value: Any) -> str:
    text = upper_article(value)
    if not text or is_excluded_article(text):
        return ""
    m = re.match(r"^PT(\d+)", text)
    if m:
        return m.group(1)
    m = re.match(r"^(\d+)", text)
    if m:
        return m.group(1)
    return ""


def article_to_rrp_key(article: Any) -> str:
    text = upper_article(article)
    if not text:
        return ""
    m = re.match(r"^(\d+)/(\d+)$", text)
    if m:
        return f"PT{int(m.group(1))}.{int(m.group(2)):03d}"
    m = re.match(r"^PT(\d+)\.F(\d+)$", text)
    if m:
        return f"PT{int(m.group(1))}.F{int(m.group(2)):02d}"
    m = re.match(r"^PT(\d+)\.(\d+)$", text)
    if m:
        return f"PT{int(m.group(1))}.{int(m.group(2)):03d}"
    m = re.match(r"^(\d+)$", text)
    if m:
        return f"PT{int(m.group(1))}"
    return text


def to_numeric(series: Any) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def to_dt(series: Any) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.normalize()


def safe_div(a: Any, b: Any) -> float:
    try:
        a = float(a)
        b = float(b)
    except Exception:
        return np.nan
    if pd.isna(a) or pd.isna(b) or b == 0:
        return np.nan
    return a / b


def weighted_mean(values: pd.Series, weights: pd.Series) -> float:
    v = pd.to_numeric(values, errors="coerce")
    w = pd.to_numeric(weights, errors="coerce")
    mask = v.notna() & w.notna()
    if not mask.any():
        return np.nan
    v = v[mask]
    w = w[mask]
    if w.sum() == 0:
        return np.nan
    return float(np.average(v, weights=w))


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


def russian_month_name(month_num: int) -> str:
    names = {
        1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
        5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
        9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
    }
    return names[month_num]


COMMON_ALIASES: Dict[str, List[str]] = {
    "day": ["Дата", "dt", "date", "Дата заказа", "Дата отчета", "Дата сбора"],
    "week": ["Неделя", "week"],
    "nm_id": ["Артикул WB", "Артикул ВБ", "nmID", "nmId"],
    "supplier_article": ["Артикул продавца", "supplierArticle", "Артикул WB продавца"],
    "brand": ["Бренд", "brand"],
    "subject": ["Предмет", "subject", "Название предмета", "Категория"],
    "title": ["Название", "Название товара", "Товар"],
    "warehouse": ["Склад", "warehouseName"],
    "orders": ["Заказы", "Заказали", "orders", "ordersCount", "Кол-во продаж"],
    "buyouts_count": ["buyoutsCount", "Выкупы заказов"],
    "finished_price": ["finishedPrice", "Ср. цена продажи", "Цена с учетом всех скидок, кроме суммы по WB Кошельку", "Средняя цена покупателя"],
    "price_with_disc": ["priceWithDisc", "Цена со скидкой продавца, в том числе со скидкой WB Клуба", "Средняя цена продажи"],
    "spp": ["SPP", "СПП", "Скидка WB, %", "СПП, %"],
    "gross_profit": ["Валовая прибыль"],
    "gross_revenue": ["Валовая выручка"],
    "spend": ["Расход", "spend", "Продвижение"],
}


def rename_using_aliases(df: pd.DataFrame, aliases: Dict[str, List[str]] = COMMON_ALIASES) -> pd.DataFrame:
    out = df.copy()
    norm_existing = {norm_key(c): c for c in out.columns}
    for target, variants in aliases.items():
        if target in out.columns:
            continue
        found = None
        for variant in variants:
            key = norm_key(variant)
            if key in norm_existing:
                found = norm_existing[key]
                break
        if found is not None:
            out[target] = out[found]
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


def read_excel_flexible(data: bytes, preferred_sheets: Optional[Iterable[str]] = None, header_candidates: Iterable[int] = (0, 1, 2)) -> pd.DataFrame:
    bio = io.BytesIO(data)
    xl = pd.ExcelFile(bio)
    sheet = pick_best_sheet(xl.sheet_names, preferred_sheets or [])
    best_df = None
    best_score = -10**9
    for header in header_candidates:
        try:
            df = xl.parse(sheet_name=sheet, header=header, dtype=object)
        except Exception:
            continue
        df = df.copy()
        df.columns = dedupe_columns(df.columns)
        df = df.dropna(axis=0, how="all").dropna(axis=1, how="all")
        score = len(df.columns) - (1000 if df.empty else 0)
        if score > best_score:
            best_score = score
            best_df = df
    if best_df is None:
        raise ValueError(f"Не удалось прочитать Excel: {sheet}")
    best_df.columns = dedupe_columns(best_df.columns)
    return rename_using_aliases(best_df)


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
        return self.s3.get_object(Bucket=self.bucket, Key=path)["Body"].read()

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
        return S3Storage(bucket, access_key, secret_key)
    log("Using local filesystem")
    return LocalStorage(root)


@dataclass
class LoadedData:
    orders: pd.DataFrame
    funnel: pd.DataFrame
    ads_daily: pd.DataFrame
    economics: pd.DataFrame
    abc_weekly: pd.DataFrame
    abc_monthly: pd.DataFrame
    plan: pd.DataFrame
    latest_day: pd.Timestamp
    warnings: List[str]


class Stage1Loader:
    def __init__(self, storage: BaseStorage, reports_root: str = "Отчёты", store: str = "TOPFACE"):
        self.storage = storage
        self.reports_root = reports_root.rstrip("/")
        self.store = store
        self.warnings: List[str] = []

    def _prefix(self, *parts: str) -> str:
        return "/".join([self.reports_root, *parts]).replace("//", "/")

    def _list_under(self, prefixes: List[str]) -> List[str]:
        files: List[str] = []
        for prefix in prefixes:
            files.extend(self.storage.list_files(prefix))
        return sorted(set(files))

    def _glob_root(self, patterns: List[str]) -> List[str]:
        if hasattr(self.storage, "glob_root"):
            out: List[str] = []
            for pattern in patterns:
                out.extend(self.storage.glob_root(pattern))
            return sorted(set(out))
        return []

    def _finalize(self, df: pd.DataFrame) -> pd.DataFrame:
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
        for path in files:
            try:
                df = read_excel_flexible(self.storage.read_bytes(path), preferred_sheets=[])
                df["day"] = to_dt(df.get("day", pd.Series(dtype=object))).dt.normalize()
                df = self._finalize(df)
                df["orders"] = to_numeric(df.get("orders", np.nan))
                if df["orders"].isna().all():
                    df["orders"] = 1.0
                for c in ["finished_price", "price_with_disc", "spp"]:
                    df[c] = to_numeric(df.get(c, np.nan))
                if "warehouse" not in df.columns:
                    df["warehouse"] = ""
                dfs.append(df[[c for c in ["day", "nm_id", "supplier_article", "subject", "brand", "title", "orders", "finished_price", "price_with_disc", "spp", "warehouse"] if c in df.columns]])
            except Exception as e:
                self.warnings.append(f"Orders read error {path}: {e}")
        out = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        if not out.empty:
            out = out[out["day"].notna()].copy()
            log(f"Orders rows loaded: {len(out):,}; date range {out['day'].min().date()} .. {out['day'].max().date()}")
        else:
            log("Orders rows loaded: 0")
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
            df = read_excel_flexible(self.storage.read_bytes(path), preferred_sheets=[])
            df["day"] = to_dt(df.get("day", pd.Series(dtype=object))).dt.normalize()
            df = self._finalize(df)
            df["orders"] = to_numeric(df.get("orders", np.nan))
            df["buyouts_count"] = to_numeric(df.get("buyouts_count", np.nan))
            out = df[df["day"].notna()].copy()
            if not out.empty:
                log(f"Funnel rows loaded: {len(out):,}; date range {out['day'].min().date()} .. {out['day'].max().date()}")
            else:
                log("Funnel rows loaded: 0")
            return out
        except Exception as e:
            self.warnings.append(f"Funnel read error {path}: {e}")
            return pd.DataFrame()

    def load_ads(self) -> pd.DataFrame:
        files = self._list_under([
            self._prefix("Реклама", self.store, "Недельные"),
            self._prefix("Реклама", self.store),
        ])
        if not files:
            files = self._glob_root(["Анализ рекламы.xlsx", "Реклама_*.xlsx"])
        daily_dfs = []
        for path in files:
            try:
                df = read_excel_flexible(self.storage.read_bytes(path), preferred_sheets=["Статистика_Ежедневно"])
                if "day" not in df.columns and "Дата" in df.columns:
                    df["day"] = df["Дата"]
                df["day"] = to_dt(df.get("day", pd.Series(dtype=object))).dt.normalize()
                df = self._finalize(df)
                df["spend"] = to_numeric(df.get("spend", np.nan)).fillna(0)
                for c in ["impressions", "clicks", "orders", "ctr", "cpc"]:
                    df[c] = to_numeric(df.get(c, np.nan))
                if "supplier_article" not in df.columns:
                    df["supplier_article"] = ""
                if "subject" not in df.columns:
                    df["subject"] = ""
                out = df[[c for c in ["day", "nm_id", "supplier_article", "subject", "brand", "title", "impressions", "clicks", "orders", "ctr", "cpc", "spend"] if c in df.columns]].copy()
                daily_dfs.append(out)
            except Exception as e:
                self.warnings.append(f"Ads read error {path}: {e}")
        out = pd.concat(daily_dfs, ignore_index=True) if daily_dfs else pd.DataFrame()
        if not out.empty:
            out = out[out["day"].notna()].copy()
            log(f"Ads rows loaded: {len(out):,}; date range {out['day'].min().date()} .. {out['day'].max().date()}; spend sum {out['spend'].sum():,.0f}")
        else:
            log("Ads rows loaded: 0")
        return out

    def load_economics(self) -> pd.DataFrame:
        candidates = [
            self._prefix("Финансовые показатели", self.store, "Экономика.xlsx"),
            self._prefix("Финансовые показатели", self.store, "Недельные", "Экономика.xlsx"),
            "Экономика (4).xlsx",
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
            df = read_excel_flexible(self.storage.read_bytes(path), preferred_sheets=["Юнит экономика"], header_candidates=(0, 1, 2))
            df = self._finalize(df)
            if "week" not in df.columns:
                df["week"] = df.get("Неделя", np.nan)
            df["week"] = df["week"].astype(str).str.strip()
            mapping = {
                "Процент выкупа": "buyout_pct",
                "Комиссия WB, %": "commission_pct",
                "Эквайринг, %": "acquiring_pct",
                "Логистика прямая, руб/ед": "logistics_direct_unit",
                "Логистика обратная, руб/ед": "logistics_return_unit",
                "Хранение, руб/ед": "storage_unit",
                "Прочие расходы, руб/ед": "other_unit",
                "Себестоимость, руб": "cost_unit",
                "Валовая прибыль, руб/ед": "gp_unit",
                "НДС, руб/ед": "vat_unit",
                "Средняя цена продажи": "econ_price_with_disc",
                "Средняя цена покупателя": "econ_finished_price",
            }
            for src, dst in mapping.items():
                if src in df.columns and dst not in df.columns:
                    df[dst] = df[src]
            for c in mapping.values():
                if c not in df.columns:
                    df[c] = np.nan
                df[c] = to_numeric(df[c])
            if "title" not in df.columns:
                df["title"] = ""
            out = df[[c for c in ["week", "supplier_article", "nm_id", "subject", "brand", "title", *mapping.values()] if c in df.columns]].copy()
            log(f"Economics rows loaded: {len(out):,}; weeks {', '.join(sorted(out['week'].dropna().astype(str).unique())[:8])}{' ...' if out['week'].nunique() > 8 else ''}")
            return out
        except Exception as e:
            self.warnings.append(f"Economics read error {path}: {e}")
            return pd.DataFrame()

    def load_abc(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        files = self._list_under([self._prefix("ABC")])
        files = [f for f in files if "wb_abc_report_goods__" in Path(f).name]
        if not files:
            files = self._glob_root(["wb_abc_report_goods__*.xlsx"])
        weekly_dfs = []
        monthly_dfs = []
        for path in files:
            try:
                name = Path(path).name
                df = read_excel_flexible(self.storage.read_bytes(path), preferred_sheets=[], header_candidates=(0,))
                start, end = parse_abc_period_from_name(name)
                if not start or not end:
                    continue
                df = self._finalize(df)
                df["week_start"] = pd.Timestamp(start)
                df["week_end"] = pd.Timestamp(end)
                df["week_code"] = week_code_from_date(start)
                df["code"] = df.get("supplier_article", pd.Series(dtype=object)).map(extract_code)
                df["gross_profit"] = to_numeric(df.get("gross_profit", np.nan))
                df["gross_revenue"] = to_numeric(df.get("gross_revenue", np.nan))
                if "orders" not in df.columns and "Кол-во продаж" in df.columns:
                    df["orders"] = df["Кол-во продаж"]
                df["orders"] = to_numeric(df.get("orders", np.nan))
                df["vat"] = df["gross_revenue"] * 7.0 / 107.0
                df["gp_minus_nds"] = df["gross_profit"] - df["vat"]
                month_end = (pd.Timestamp(start).to_period("M").end_time.normalize()).date()
                if start.day == 1 and end == month_end:
                    df["month_key"] = pd.Timestamp(start).strftime("%Y-%m")
                    monthly_dfs.append(df[[c for c in ["month_key", "supplier_article", "nm_id", "subject", "brand", "title", "code", "gross_profit", "gross_revenue", "vat", "gp_minus_nds", "orders"] if c in df.columns]])
                else:
                    df["week_label"] = pd.Timestamp(start).strftime("%d.%m")
                    weekly_dfs.append(df[[c for c in ["week_code", "week_label", "week_start", "week_end", "supplier_article", "nm_id", "subject", "brand", "title", "code", "gross_profit", "gross_revenue", "vat", "gp_minus_nds", "orders"] if c in df.columns]])
            except Exception as e:
                self.warnings.append(f"ABC read error {path}: {e}")
        weekly = pd.concat(weekly_dfs, ignore_index=True) if weekly_dfs else pd.DataFrame()
        monthly = pd.concat(monthly_dfs, ignore_index=True) if monthly_dfs else pd.DataFrame()
        if not weekly.empty:
            log(f"ABC weekly rows loaded: {len(weekly):,}; weeks {', '.join(sorted(weekly['week_code'].dropna().astype(str).unique()))}")
        else:
            log("ABC weekly rows loaded: 0")
        if not monthly.empty:
            log(f"ABC monthly rows loaded: {len(monthly):,}; months {', '.join(sorted(monthly['month_key'].dropna().astype(str).unique()))}")
        else:
            log("ABC monthly rows loaded: 0")
        return weekly, monthly

    def load_plan(self, current_month_key: str) -> pd.DataFrame:
        candidates = [self._prefix("Объединенный отчет", self.store, "План.xlsx"), "План.xlsx"]
        path = next((c for c in candidates if self.storage.exists(c)), None)
        if not path:
            return pd.DataFrame()
        try:
            df = pd.read_excel(io.BytesIO(self.storage.read_bytes(path)), sheet_name="Итог_все_категории", header=2)
            df.columns = [normalize_text(c) for c in df.columns]
            df = rename_using_aliases(df)
            df = self._finalize(df)
            current_period = pd.Period(current_month_key, freq="M")
            target_col = f"ВП-НДС {russian_month_name(current_period.month)} {current_period.year}"
            chosen = None
            for c in df.columns:
                if norm_key(c) == norm_key(target_col) or norm_key(target_col) in norm_key(c):
                    chosen = c
                    break
            if chosen is None:
                return pd.DataFrame()
            out = df[["supplier_article", "subject", chosen]].copy()
            out["plan_gp_minus_nds_month"] = to_numeric(out[chosen])
            out["code"] = out["supplier_article"].map(extract_code)
            out = out.drop(columns=[chosen])
            log(f"Plan rows loaded: {len(out):,}; non-null plan {out['plan_gp_minus_nds_month'].notna().sum():,}")
            return out
        except Exception as e:
            self.warnings.append(f"Plan read error {path}: {e}")
            return pd.DataFrame()

    def load_all(self) -> LoadedData:
        log("Loading data")
        log("Loading orders")
        orders = self.load_orders()
        log("Loading funnel")
        funnel = self.load_funnel()
        log("Loading ads")
        ads_daily = self.load_ads()
        log("Loading economics")
        economics = self.load_economics()
        log("Loading ABC")
        abc_weekly, abc_monthly = self.load_abc()
        latest_candidates = []
        for df, col in [(orders, "day"), (funnel, "day"), (ads_daily, "day")]:
            if not df.empty and col in df.columns:
                latest_candidates.append(pd.to_datetime(df[col], errors="coerce").max())
        latest_day = max([x for x in latest_candidates if pd.notna(x)], default=pd.Timestamp.today().normalize())
        current_month_key = latest_day.to_period("M").strftime("%Y-%m")
        log("Loading plan")
        plan = self.load_plan(current_month_key)
        return LoadedData(
            orders=orders,
            funnel=funnel,
            ads_daily=ads_daily,
            economics=economics,
            abc_weekly=abc_weekly,
            abc_monthly=abc_monthly,
            plan=plan,
            latest_day=pd.Timestamp(latest_day).normalize(),
            warnings=self.warnings,
        )


class Stage1Builder:
    def __init__(self, data: LoadedData):
        self.data = data
        self.latest_day = pd.Timestamp(data.latest_day).normalize()
        self.current_week_start = self.latest_day - pd.Timedelta(days=self.latest_day.weekday())
        self.current_week_days = [self.current_week_start + pd.Timedelta(days=i) for i in range((self.latest_day - self.current_week_start).days + 1)]
        self.current_month_key = self.latest_day.to_period("M").strftime("%Y-%m")
        self.current_month_start = self.latest_day.replace(day=1)
        self.days_in_month = calendar.monthrange(self.latest_day.year, self.latest_day.month)[1]
        self.master = self.build_master()
        self.buyout90 = self.build_buyout90()
        self.econ_map, self.econ_diag = self.build_econ_week_map()

    def _filter_subjects(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df.copy()
        out = df.copy()
        if "subject" in out.columns:
            out["subject"] = out["subject"].map(normalize_text)
            out = out[out["subject"].isin(TARGET_SUBJECTS)].copy()
        if "supplier_article" in out.columns:
            out["supplier_article"] = out["supplier_article"].map(clean_article)
            out = out[~out["supplier_article"].map(is_excluded_article)].copy()
        if "code" not in out.columns:
            out["code"] = out.get("supplier_article", pd.Series(dtype=object)).map(extract_code)
        out = out[out["code"] != ""].copy()
        return out

    def build_master(self) -> pd.DataFrame:
        frames = []
        for df in [self.data.orders, self.data.economics, self.data.abc_weekly, self.data.abc_monthly, self.data.ads_daily]:
            if df.empty:
                continue
            x = df.copy()
            for c in ["supplier_article", "nm_id", "subject", "brand", "title"]:
                if c not in x.columns:
                    x[c] = np.nan
            x = x[["supplier_article", "nm_id", "subject", "brand", "title"]].copy()
            frames.append(x)
        if not frames:
            return pd.DataFrame(columns=["supplier_article", "nm_id", "subject", "brand", "title", "code", "rrp_key"])
        m = pd.concat(frames, ignore_index=True)
        m["supplier_article"] = m["supplier_article"].map(clean_article)
        m["nm_id"] = to_numeric(m["nm_id"])
        m["subject"] = m["subject"].map(normalize_text)
        m["brand"] = m["brand"].map(normalize_text)
        m["title"] = m["title"].map(normalize_text)
        m["code"] = m["supplier_article"].map(extract_code)
        m = self._filter_subjects(m)
        m["quality"] = m["subject"].ne("").astype(int) * 4 + m["title"].ne("").astype(int) * 2 + m["brand"].ne("").astype(int)
        m = m.sort_values("quality", ascending=False).drop_duplicates(subset=["supplier_article", "nm_id"], keep="first")
        return m[["supplier_article", "nm_id", "subject", "brand", "title", "code"]]

    def attach_master(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or self.master.empty:
            return df.copy()
        out = df.copy()
        if "supplier_article" in out.columns:
            out = out.merge(self.master[["supplier_article", "nm_id", "subject", "brand", "title", "code"]], on="supplier_article", how="left", suffixes=("", "_m"))
            for c in ["nm_id", "subject", "brand", "title", "code"]:
                if f"{c}_m" in out.columns:
                    if c not in out.columns:
                        out[c] = out[f"{c}_m"]
                    else:
                        cond = out[c].isna() | (out[c] == "")
                        out.loc[cond, c] = out.loc[cond, f"{c}_m"]
                    out.drop(columns=[f"{c}_m"], inplace=True)
        if "nm_id" in out.columns:
            nm_map = self.master.dropna(subset=["nm_id"]).drop_duplicates(subset=["nm_id"])[["nm_id", "supplier_article", "subject", "brand", "title", "code"]]
            out = out.merge(nm_map, on="nm_id", how="left", suffixes=("", "_n"))
            for c in ["supplier_article", "subject", "brand", "title", "code"]:
                if f"{c}_n" in out.columns:
                    if c not in out.columns:
                        out[c] = out[f"{c}_n"]
                    else:
                        cond = out[c].isna() | (out[c] == "")
                        out.loc[cond, c] = out.loc[cond, f"{c}_n"]
                    out.drop(columns=[f"{c}_n"], inplace=True)
        return self._filter_subjects(out)

    def build_buyout90(self) -> pd.DataFrame:
        if self.data.funnel.empty:
            return pd.DataFrame(columns=["nm_id", "buyout_pct_90"])
        f = self.data.funnel.copy()
        f = f[(f["day"] >= self.latest_day - pd.Timedelta(days=89)) & (f["day"] <= self.latest_day)].copy()
        out = f.groupby("nm_id", dropna=False).agg(orders_90=("orders", "sum"), buyouts_90=("buyouts_count", "sum")).reset_index()
        out["buyout_pct_90"] = out.apply(lambda r: safe_div(r["buyouts_90"], r["orders_90"]), axis=1)
        log(f"Buyout90 rows: {len(out):,}; non-null ratios {out['buyout_pct_90'].notna().sum():,}")
        return out[["nm_id", "buyout_pct_90"]]

    def build_econ_week_map(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        econ = self._filter_subjects(self.data.economics)
        if econ.empty:
            return pd.DataFrame(), pd.DataFrame()
        econ = self.attach_master(econ)
        econ["week_start"] = econ["week"].map(lambda x: pd.Timestamp(week_bounds_from_code(str(x))[0]) if week_bounds_from_code(str(x))[0] else pd.NaT)
        econ = econ.sort_values(["supplier_article", "week_start"], ascending=[True, False])
        diag = econ[[c for c in ["supplier_article", "nm_id", "week", "commission_pct", "acquiring_pct", "logistics_direct_unit", "logistics_return_unit", "storage_unit", "other_unit", "cost_unit"] if c in econ.columns]].copy()
        log(f"Economics usable rows: {len(econ):,}; articles {econ['supplier_article'].nunique():,}")
        return econ, diag

    def match_ads_daily(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        ads = self._filter_subjects(self.data.ads_daily)
        if ads.empty:
            return pd.DataFrame(columns=["day", "nm_id", "supplier_article", "ad_spend"]), pd.DataFrame()
        ads = self.attach_master(ads)
        by_both = ads.groupby(["day", "nm_id", "supplier_article"], dropna=False).agg(ad_spend=("spend", "sum")).reset_index()
        by_nm = ads.groupby(["day", "nm_id"], dropna=False).agg(ad_spend=("spend", "sum")).reset_index()
        diag = pd.DataFrame({
            "ads_rows_total": [len(ads)],
            "ads_rows_with_article": [ads['supplier_article'].fillna('').ne('').sum()],
            "ads_rows_with_nm": [ads['nm_id'].notna().sum()],
            "ads_spend_total": [ads['spend'].sum()],
        })
        return by_both, by_nm, diag

    def pick_econ_for_day(self, daily: pd.DataFrame) -> pd.DataFrame:
        if daily.empty or self.econ_map.empty:
            return pd.DataFrame(columns=["day", "supplier_article", "nm_id"])
        rows = []
        exact = 0
        fallback = 0
        missing = 0
        daily_keys = daily[["day", "supplier_article", "nm_id"]].drop_duplicates()
        econ = self.econ_map
        for rec in daily_keys.itertuples(index=False):
            day, art, nm_id = rec.day, rec.supplier_article, rec.nm_id
            target_week = week_code_from_date(day)
            g = econ[econ["supplier_article"] == art].copy()
            if g.empty and pd.notna(nm_id):
                g = econ[econ["nm_id"] == nm_id].copy()
            if g.empty:
                missing += 1
                continue
            exact_g = g[g["week"].astype(str) == str(target_week)]
            if not exact_g.empty:
                chosen = exact_g.iloc[0]
                exact += 1
            else:
                chosen = g.iloc[0]
                fallback += 1
            row = {"day": day, "supplier_article": art, "nm_id": nm_id}
            for c in ["week", "buyout_pct", "commission_pct", "acquiring_pct", "logistics_direct_unit", "logistics_return_unit", "storage_unit", "other_unit", "cost_unit", "vat_unit", "gp_unit", "econ_price_with_disc", "econ_finished_price"]:
                row[c] = chosen.get(c, np.nan)
            rows.append(row)
        log(f"Economics matching: exact week = {exact:,}, fallback latest = {fallback:,}, missing = {missing:,}")
        return pd.DataFrame(rows)

    def build_daily_calc(self) -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame]]:
        orders = self._filter_subjects(self.data.orders)
        orders = self.attach_master(orders)
        orders = orders[(orders["day"] >= self.current_week_start) & (orders["day"] <= self.latest_day)].copy()
        if orders.empty:
            log("Current week orders: 0 rows")
            return pd.DataFrame(), {"orders_daily": orders}
        log(f"Current week order rows: {len(orders):,}; day range {orders['day'].min().date()} .. {orders['day'].max().date()}")
        daily = orders.groupby(["day", "supplier_article", "nm_id", "subject", "brand", "title", "code"], dropna=False).agg(
            orders_day=("orders", "sum"),
            finished_price_avg=("finished_price", lambda s: weighted_mean(s, orders.loc[s.index, 'orders'].fillna(1))),
            price_with_disc_avg=("price_with_disc", lambda s: weighted_mean(s, orders.loc[s.index, 'orders'].fillna(1))),
            spp_avg=("spp", lambda s: weighted_mean(s, orders.loc[s.index, 'orders'].fillna(1))),
        ).reset_index()

        daily = daily.merge(self.buyout90, on="nm_id", how="left")
        econ_pick = self.pick_econ_for_day(daily)
        daily = daily.merge(econ_pick, on=["day", "supplier_article", "nm_id"], how="left")

        ads_by_both, ads_by_nm, ads_diag = self.match_ads_daily()
        tech = {"orders_daily": daily.copy(), "ads_diag": ads_diag, "econ_diag": self.econ_diag.copy()}

        if not ads_by_both.empty:
            daily = daily.merge(ads_by_both, on=["day", "supplier_article", "nm_id"], how="left")
        else:
            daily["ad_spend"] = np.nan
        if not ads_by_nm.empty:
            nm_merge = daily[["day", "nm_id"]].merge(ads_by_nm, on=["day", "nm_id"], how="left")
            daily["ad_spend_nm"] = nm_merge["ad_spend"]
        else:
            daily["ad_spend_nm"] = np.nan
        daily["ad_spend"] = daily["ad_spend"].fillna(daily["ad_spend_nm"]).fillna(0.0)
        matched_ads = int((daily["ad_spend"] > 0).sum())
        log(f"Ads matching to daily rows: matched rows = {matched_ads:,} из {len(daily):,}; spend matched = {daily['ad_spend'].sum():,.0f}")

        daily["buyout_factor"] = daily["buyout_pct_90"].fillna(to_numeric(daily["buyout_pct"]) / 100.0).fillna(1.0)
        daily["buyout_qty"] = daily["orders_day"] * daily["buyout_factor"]
        daily["revenue_pwd"] = daily["buyout_qty"] * daily["price_with_disc_avg"].fillna(daily["econ_price_with_disc"]).fillna(0)
        daily["commission_rub"] = daily["revenue_pwd"] * daily["commission_pct"].fillna(0) / 100.0
        daily["acquiring_rub"] = daily["revenue_pwd"] * daily["acquiring_pct"].fillna(0) / 100.0
        daily["logistics_direct_rub"] = daily["buyout_qty"] * daily["logistics_direct_unit"].fillna(0)
        daily["logistics_return_rub"] = daily["buyout_qty"] * daily["logistics_return_unit"].fillna(0)
        daily["storage_rub"] = daily["buyout_qty"] * daily["storage_unit"].fillna(0)
        daily["other_rub"] = daily["buyout_qty"] * daily["other_unit"].fillna(0)
        daily["cost_rub"] = daily["buyout_qty"] * daily["cost_unit"].fillna(0)
        daily["vat_rub"] = daily["buyout_qty"] * daily["finished_price_avg"].fillna(daily["econ_finished_price"]).fillna(0) * 7.0 / 107.0
        daily["gross_profit_rub"] = (
            daily["revenue_pwd"] - daily["commission_rub"] - daily["acquiring_rub"] - daily["logistics_direct_rub"]
            - daily["logistics_return_rub"] - daily["storage_rub"] - daily["other_rub"] - daily["cost_rub"] - daily["ad_spend"]
        )
        daily["gp_minus_nds_rub"] = daily["gross_profit_rub"] - daily["vat_rub"]
        daily["day_label"] = pd.to_datetime(daily["day"]).dt.strftime("%d.%m")
        # diagnostics around commission zeros
        zero_comm = int((daily["commission_pct"].fillna(0) == 0).sum())
        log(f"Commission diagnostics: zero/empty commission_pct rows = {zero_comm:,} из {len(daily):,}")
        tech["daily_calc"] = daily.copy()
        return daily, tech

    def build_weekly_fact(self) -> pd.DataFrame:
        abc = self._filter_subjects(self.data.abc_weekly)
        if abc.empty:
            return pd.DataFrame()
        abc = self.attach_master(abc)
        abc = abc[(abc["week_end"] >= self.current_month_start) & (abc["week_start"] <= self.latest_day)].copy()
        available_weeks = sorted(abc["week_code"].dropna().astype(str).unique())
        log(f"ABC weeks used in current month block: {', '.join(available_weeks) if available_weeks else 'none'}")
        return abc

    def build_monthly_fact(self) -> pd.DataFrame:
        abc_month = self._filter_subjects(self.data.abc_monthly)
        abc_week = self._filter_subjects(self.data.abc_weekly)
        abc_month = self.attach_master(abc_month) if not abc_month.empty else abc_month
        abc_week = self.attach_master(abc_week) if not abc_week.empty else abc_week
        periods = [self.latest_day.to_period("M") - 2, self.latest_day.to_period("M") - 1, self.latest_day.to_period("M")]
        month_keys = [p.strftime("%Y-%m") for p in periods]
        frames = []
        if not abc_month.empty:
            frames.append(abc_month[abc_month["month_key"].isin(month_keys)].copy())
        if self.current_month_key not in set(abc_month.get("month_key", pd.Series(dtype=str)).astype(str)):
            wk = abc_week.copy()
            if not wk.empty:
                wk["month_key"] = pd.to_datetime(wk["week_start"]).dt.to_period("M").astype(str)
                wk = wk[wk["month_key"] == self.current_month_key].copy()
                if not wk.empty:
                    curm = wk.groupby(["month_key", "supplier_article", "nm_id", "subject", "brand", "title", "code"], dropna=False).agg(
                        gross_profit=("gross_profit", "sum"), gross_revenue=("gross_revenue", "sum"), vat=("vat", "sum"), gp_minus_nds=("gp_minus_nds", "sum"), orders=("orders", "sum")
                    ).reset_index()
                    frames = [f[f["month_key"] != self.current_month_key] for f in frames]
                    frames.append(curm)
        out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        if not out.empty:
            log(f"ABC months used in 3-month block: {', '.join(sorted(out['month_key'].dropna().astype(str).unique()))}")
        return out

    def build_plan(self) -> pd.DataFrame:
        plan = self._filter_subjects(self.data.plan)
        plan = self.attach_master(plan) if not plan.empty else plan
        return plan

    def month_fact_maps(self, monthly: pd.DataFrame) -> Tuple[Dict, Dict, Dict]:
        if monthly.empty:
            return {}, {}, {}
        cur = monthly[monthly["month_key"] == self.current_month_key].copy()
        art = cur.groupby("supplier_article", dropna=False)["gp_minus_nds"].sum().to_dict()
        prod = cur.groupby(["subject", "code"], dropna=False)["gp_minus_nds"].sum().to_dict()
        cat = cur.groupby(["subject"], dropna=False)["gp_minus_nds"].sum().to_dict()
        return art, prod, cat

    def aggregate_hierarchy(self, base: pd.DataFrame, value_col: str, label_col: str, labels: List[str], plan_mode: str, monthly_fact: pd.DataFrame, plan_df: pd.DataFrame) -> pd.DataFrame:
        if base.empty:
            return pd.DataFrame()
        art_fact_map, prod_fact_map, cat_fact_map = self.month_fact_maps(monthly_fact)
        art_plan_map = plan_df.set_index("supplier_article")["plan_gp_minus_nds_month"].to_dict() if not plan_df.empty else {}
        prod_plan_map = plan_df.groupby(["subject", "code"], dropna=False)["plan_gp_minus_nds_month"].sum().to_dict() if not plan_df.empty else {}
        cat_plan_map = plan_df.groupby(["subject"], dropna=False)["plan_gp_minus_nds_month"].sum().to_dict() if not plan_df.empty else {}
        rows = []

        def calc_plan(level: str, subject: str, code: str, article: str, fact_values: List[float]) -> float:
            if plan_mode == "daily":
                if level == "article":
                    p = art_plan_map.get(article, np.nan)
                    if pd.isna(p):
                        return float(np.nanmean(fact_values)) if len(fact_values) else 0.0
                    return float(p) / self.days_in_month
                if level == "product":
                    p = prod_plan_map.get((subject, code), np.nan)
                    if pd.isna(p):
                        return float(np.nanmean(fact_values)) if len(fact_values) else 0.0
                    return float(p) / self.days_in_month
                p = cat_plan_map.get(subject, np.nan)
                if pd.isna(p):
                    return float(np.nanmean(fact_values)) if len(fact_values) else 0.0
                return float(p) / self.days_in_month
            else:
                if level == "article":
                    p = art_plan_map.get(article, np.nan)
                    if pd.isna(p):
                        return float(art_fact_map.get(article, 0.0))
                    return float(p)
                if level == "product":
                    p = prod_plan_map.get((subject, code), np.nan)
                    if pd.isna(p):
                        return float(prod_fact_map.get((subject, code), 0.0))
                    return float(p)
                p = cat_plan_map.get(subject, np.nan)
                if pd.isna(p):
                    return float(cat_fact_map.get(subject, 0.0))
                return float(p)

        for subject in TARGET_SUBJECTS:
            sg = base[base["subject"] == subject].copy()
            if sg.empty:
                continue
            fact_values = [float(sg.loc[sg[label_col] == lbl, value_col].sum()) for lbl in labels]
            row = {"Наименование": subject, "_kind": "category", "_subject": subject}
            for lbl, val in zip(labels, fact_values):
                row[lbl] = val
            row["План"] = calc_plan("category", subject, "", "", fact_values)
            rows.append(row)

            product_order = sg.groupby("code", dropna=False)[value_col].sum().sort_values(ascending=False).index.tolist()
            for code in product_order:
                pg = sg[sg["code"] == code].copy()
                fact_values = [float(pg.loc[pg[label_col] == lbl, value_col].sum()) for lbl in labels]
                row = {"Наименование": str(code), "_kind": "product", "_subject": subject, "_code": code}
                for lbl, val in zip(labels, fact_values):
                    row[lbl] = val
                row["План"] = calc_plan("product", subject, code, "", fact_values)
                rows.append(row)

                article_order = pg.groupby("supplier_article", dropna=False)[value_col].sum().sort_values(ascending=False).index.tolist()
                for art in article_order:
                    ag = pg[pg["supplier_article"] == art].copy()
                    fact_values = [float(ag.loc[ag[label_col] == lbl, value_col].sum()) for lbl in labels]
                    row = {"Наименование": art, "_kind": "article", "_subject": subject, "_code": code, "_article": art}
                    for lbl, val in zip(labels, fact_values):
                        row[lbl] = val
                    row["План"] = calc_plan("article", subject, code, art, fact_values)
                    rows.append(row)
            total = {"Наименование": f"Итого {subject}", "_kind": "subject_total", "_subject": subject}
            for lbl in labels:
                total[lbl] = float(sg.loc[sg[label_col] == lbl, value_col].sum())
            total["План"] = calc_plan("category", subject, "", "", [total[lbl] for lbl in labels])
            rows.append(total)

        grand = {"Наименование": "Итого по всем 4 категориям", "_kind": "grand_total"}
        for lbl in labels:
            grand[lbl] = float(base.loc[base[label_col] == lbl, value_col].sum())
        if plan_mode == "daily":
            grand["План"] = float(sum(v for v in cat_plan_map.values() if pd.notna(v))) / self.days_in_month if cat_plan_map else float(np.nanmean([grand[lbl] for lbl in labels]))
        else:
            grand["План"] = float(sum(v for v in cat_plan_map.values() if pd.notna(v))) if cat_plan_map else float(sum(cat_fact_map.values()))
        rows.append(grand)
        return pd.DataFrame(rows)

    def build_example_weekly(self, articles: List[str], daily: pd.DataFrame, weekly: pd.DataFrame) -> pd.DataFrame:
        if daily.empty and weekly.empty:
            return pd.DataFrame()
        orders = self.attach_master(self._filter_subjects(self.data.orders))
        orders["week_code"] = orders["day"].map(week_code_from_date)
        funnel = self.data.funnel.copy()
        ads = self.attach_master(self._filter_subjects(self.data.ads_daily)) if not self.data.ads_daily.empty else pd.DataFrame()
        econ = self.econ_map.copy()
        abc = self.attach_master(self._filter_subjects(self.data.abc_weekly))
        rows = []
        for art in articles:
            oa = orders[orders["supplier_article"] == art].copy()
            if oa.empty:
                continue
            nm = oa["nm_id"].dropna().iloc[0] if oa["nm_id"].notna().any() else np.nan
            subject = oa["subject"].dropna().iloc[0] if oa["subject"].notna().any() else ""
            weeks = sorted(oa["week_code"].dropna().unique())[-4:]
            for wk in weeks:
                ws, we = week_bounds_from_code(wk)
                ws = pd.Timestamp(ws) if ws else pd.NaT
                we = pd.Timestamp(we) if we else pd.NaT
                ow = oa[oa["week_code"] == wk].copy()
                orders_week = ow["orders"].sum()
                if pd.notna(nm):
                    ff = funnel[(funnel["nm_id"] == nm) & (funnel["day"] >= we - pd.Timedelta(days=89)) & (funnel["day"] <= we)].copy()
                    buyout90 = safe_div(ff["buyouts_count"].sum(), ff["orders"].sum())
                else:
                    buyout90 = np.nan
                ew = econ[(econ["supplier_article"] == art) & (econ["week"].astype(str) == str(wk))].copy()
                if ew.empty:
                    ew = econ[econ["supplier_article"] == art].copy().sort_values("week_start", ascending=False).head(1)
                if ew.empty:
                    continue
                e = ew.iloc[0]
                pwd = weighted_mean(ow["price_with_disc"], ow["orders"].fillna(1))
                fp = weighted_mean(ow["finished_price"], ow["orders"].fillna(1))
                buyout_factor = buyout90 if pd.notna(buyout90) else safe_div(e.get("buyout_pct"), 100)
                if pd.isna(buyout_factor):
                    buyout_factor = 1.0
                buyout_qty = orders_week * buyout_factor
                revenue = buyout_qty * pwd
                commission = revenue * float(e.get("commission_pct", 0) or 0) / 100.0
                acquiring = revenue * float(e.get("acquiring_pct", 0) or 0) / 100.0
                logistics_direct = buyout_qty * float(e.get("logistics_direct_unit", 0) or 0)
                logistics_return = buyout_qty * float(e.get("logistics_return_unit", 0) or 0)
                storage = buyout_qty * float(e.get("storage_unit", 0) or 0)
                other = buyout_qty * float(e.get("other_unit", 0) or 0)
                cost = buyout_qty * float(e.get("cost_unit", 0) or 0)
                ad_spend = 0.0
                if not ads.empty and pd.notna(ws):
                    ad_spend = ads[(ads["supplier_article"] == art) & (ads["day"] >= ws) & (ads["day"] <= we)]["spend"].sum()
                vat = buyout_qty * fp * 7.0 / 107.0
                gp = revenue - commission - acquiring - logistics_direct - logistics_return - storage - other - cost - ad_spend
                gp_minus_nds = gp - vat
                ab = abc[(abc["supplier_article"] == art) & (abc["week_code"].astype(str) == str(wk))]
                abc_gp = ab["gross_profit"].sum() if not ab.empty else np.nan
                abc_vat = ab["vat"].sum() if not ab.empty else np.nan
                abc_gp_minus_nds = ab["gp_minus_nds"].sum() if not ab.empty else np.nan
                rows.append({
                    "Артикул": art,
                    "Категория": subject,
                    "Неделя": wk,
                    "Заказы": orders_week,
                    "% выкупа 90д": buyout_factor,
                    "Выкупленные продажи": buyout_qty,
                    "Средний priceWithDisc": pwd,
                    "Средний finishedPrice": fp,
                    "Выручка по priceWithDisc": revenue,
                    "Комиссия WB": commission,
                    "Эквайринг": acquiring,
                    "Логистика прямая": logistics_direct,
                    "Логистика обратная": logistics_return,
                    "Хранение": storage,
                    "Прочие расходы": other,
                    "Себестоимость": cost,
                    "Реклама": ad_spend,
                    "НДС": vat,
                    "Валовая прибыль прогноз": gp,
                    "Валовая прибыль - НДС прогноз": gp_minus_nds,
                    "ABC Валовая прибыль": abc_gp,
                    "ABC НДС": abc_vat,
                    "ABC Валовая прибыль - НДС": abc_gp_minus_nds,
                    "Отклонение прогноза к ABC": gp_minus_nds - abc_gp_minus_nds if pd.notna(abc_gp_minus_nds) else np.nan,
                })
        return pd.DataFrame(rows)

    def build_main_blocks(self) -> Dict[str, pd.DataFrame]:
        log("Building stage 1")
        daily, tech_daily = self.build_daily_calc()
        weekly = self.build_weekly_fact()
        monthly = self.build_monthly_fact()
        plan = self.build_plan()
        day_labels = [d.strftime("%d.%m") for d in self.current_week_days]
        week_labels = sorted(weekly["week_label"].dropna().unique().tolist()) if not weekly.empty else []
        month_keys = [(self.latest_day.to_period("M") - 2).strftime("%Y-%m"), (self.latest_day.to_period("M") - 1).strftime("%Y-%m"), self.current_month_key]

        daily_main = self.aggregate_hierarchy(daily, "gp_minus_nds_rub", "day_label", day_labels, "daily", monthly, plan) if not daily.empty else pd.DataFrame()
        daily_gp = self.aggregate_hierarchy(daily, "gross_profit_rub", "day_label", day_labels, "daily", monthly, plan) if not daily.empty else pd.DataFrame()
        daily_vat = self.aggregate_hierarchy(daily, "vat_rub", "day_label", day_labels, "daily", monthly, plan) if not daily.empty else pd.DataFrame()
        weekly_main = self.aggregate_hierarchy(weekly, "gp_minus_nds", "week_label", week_labels, "month", monthly, plan) if not weekly.empty else pd.DataFrame()
        weekly_gp = self.aggregate_hierarchy(weekly, "gross_profit", "week_label", week_labels, "month", monthly, plan) if not weekly.empty else pd.DataFrame()
        weekly_vat = self.aggregate_hierarchy(weekly, "vat", "week_label", week_labels, "month", monthly, plan) if not weekly.empty else pd.DataFrame()
        monthly_main = self.aggregate_hierarchy(monthly, "gp_minus_nds", "month_key", month_keys, "month", monthly, plan) if not monthly.empty else pd.DataFrame()
        monthly_gp = self.aggregate_hierarchy(monthly, "gross_profit", "month_key", month_keys, "month", monthly, plan) if not monthly.empty else pd.DataFrame()
        monthly_vat = self.aggregate_hierarchy(monthly, "vat", "month_key", month_keys, "month", monthly, plan) if not monthly.empty else pd.DataFrame()

        example = self.build_example_weekly(EXAMPLE_ARTICLES, daily, weekly)
        return {
            "daily_main": daily_main,
            "daily_gp": daily_gp,
            "daily_vat": daily_vat,
            "weekly_main": weekly_main,
            "weekly_gp": weekly_gp,
            "weekly_vat": weekly_vat,
            "monthly_main": monthly_main,
            "monthly_gp": monthly_gp,
            "monthly_vat": monthly_vat,
            "tech_daily": tech_daily.get("daily_calc", pd.DataFrame()),
            "tech_orders_daily": tech_daily.get("orders_daily", pd.DataFrame()),
            "tech_ads_diag": tech_daily.get("ads_diag", pd.DataFrame()),
            "tech_econ_diag": tech_daily.get("econ_diag", pd.DataFrame()),
            "tech_weekly": weekly,
            "tech_monthly": monthly,
            "tech_buyout90": self.buyout90,
            "tech_plan": plan,
            "tech_master": self.master,
            "example_weekly": example,
        }


def fmt_money(cell) -> None:
    cell.number_format = '# ##0 "₽"'


def fmt_pct(cell) -> None:
    cell.number_format = '0.00%'


def fmt_num(cell) -> None:
    cell.number_format = '# ##0.00'


def set_header(cell, fill=FILL_HEADER):
    cell.fill = fill
    cell.font = Font(bold=True)
    cell.border = BORDER
    cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)


def style_title(ws, row: int, start_col: int, end_col: int, title: str):
    ws.merge_cells(start_row=row, start_column=start_col, end_row=row, end_column=end_col)
    c = ws.cell(row, start_col, title)
    c.fill = FILL_SECTION
    c.font = Font(bold=True, size=12)
    c.alignment = Alignment(horizontal="center", vertical="center")


def autofit(ws):
    widths: Dict[int, int] = {}
    for row in ws.iter_rows():
        for c in row:
            if c.value is None:
                continue
            widths[c.column] = max(widths.get(c.column, 0), len(str(c.value)) + 2)
    for col_idx, width in widths.items():
        if col_idx == 1:
            ws.column_dimensions[get_column_letter(col_idx)].width = 28
        else:
            ws.column_dimensions[get_column_letter(col_idx)].width = min(max(width, 12), 18)


def write_hierarchy_block(ws, start_row: int, title: str, df: pd.DataFrame) -> int:
    if df.empty:
        ws.cell(start_row, 1, title).font = Font(bold=True)
        ws.cell(start_row + 1, 1, "Нет данных")
        return start_row + 3

    display_cols = [c for c in df.columns if not c.startswith("_")]
    style_title(ws, start_row, 1, len(display_cols), title)
    hdr = start_row + 1
    for j, col in enumerate(display_cols, start=1):
        set_header(ws.cell(hdr, j, col if col != "Наименование" else ""))

    row = hdr + 1
    current_category_start = None
    current_product_start = None
    for _, rec in df.iterrows():
        kind = rec.get("_kind", "")
        if kind == "category":
            if current_product_start and row - 1 >= current_product_start[0]:
                ws.row_dimensions.group(current_product_start[0], current_product_start[1], outline_level=2, hidden=True)
                current_product_start = None
            if current_category_start and row - 1 >= current_category_start[0]:
                ws.row_dimensions.group(current_category_start[0], current_category_start[1], outline_level=1, hidden=True)
                current_category_start = None
            category_row = row
        elif kind == "product":
            if current_product_start and row - 1 >= current_product_start[0]:
                ws.row_dimensions.group(current_product_start[0], current_product_start[1], outline_level=2, hidden=True)
            product_row = row
            current_product_start = [row + 1, row]
            if current_category_start is None:
                current_category_start = [row, row]
            else:
                current_category_start[1] = row
        elif kind == "article":
            if current_product_start is None:
                current_product_start = [row, row]
            else:
                current_product_start[1] = row
            if current_category_start is None:
                current_category_start = [row, row]
            else:
                current_category_start[1] = row
        else:
            if current_product_start and row - 1 >= current_product_start[0]:
                ws.row_dimensions.group(current_product_start[0], current_product_start[1], outline_level=2, hidden=True)
                current_product_start = None
            if current_category_start and row - 1 >= current_category_start[0]:
                ws.row_dimensions.group(current_category_start[0], current_category_start[1], outline_level=1, hidden=True)
                current_category_start = None

        for j, col in enumerate(display_cols, start=1):
            c = ws.cell(row, j, rec[col])
            c.border = BORDER
            c.alignment = Alignment(horizontal="center", vertical="center")
            if j >= 2 and isinstance(rec[col], (int, float, np.integer, np.floating)) and not pd.isna(rec[col]):
                fmt_money(c)

        if kind == "category":
            for j in range(1, len(display_cols) + 1):
                ws.cell(row, j).font = Font(bold=True)
                ws.cell(row, j).fill = FILL_CATEGORY
        elif kind == "product":
            for j in range(1, len(display_cols) + 1):
                ws.cell(row, j).font = Font(bold=True, italic=True)
                ws.cell(row, j).fill = FILL_PRODUCT
        elif kind in {"subject_total", "grand_total"}:
            for j in range(1, len(display_cols) + 1):
                ws.cell(row, j).font = Font(bold=True)
                ws.cell(row, j).fill = FILL_TOTAL
        row += 1

    if current_product_start and current_product_start[1] >= current_product_start[0]:
        ws.row_dimensions.group(current_product_start[0], current_product_start[1], outline_level=2, hidden=True)
    if current_category_start and current_category_start[1] >= current_category_start[0]:
        ws.row_dimensions.group(current_category_start[0], current_category_start[1], outline_level=1, hidden=True)

    ws.sheet_properties.outlinePr.summaryBelow = False
    return row + 2


def write_dataframe_sheet(wb: Workbook, sheet_name: str, df: pd.DataFrame):
    ws = wb.create_sheet(sheet_name[:31])
    if df is None or df.empty:
        ws.cell(1, 1, "Нет данных")
        return
    for j, col in enumerate(df.columns, start=1):
        set_header(ws.cell(1, j, col))
    for i, values in enumerate(df.itertuples(index=False), start=2):
        for j, val in enumerate(values, start=1):
            c = ws.cell(i, j, val)
            c.border = BORDER
            c.alignment = Alignment(horizontal="center", vertical="center")
            if isinstance(val, (int, float, np.integer, np.floating)) and not pd.isna(val):
                name = df.columns[j - 1].lower()
                if "%" in df.columns[j - 1] or "процент" in name:
                    fmt_pct(c)
                elif any(k in name for k in ["руб", "прибыль", "ндс", "расход", "выруч", "цена", "себестоим", "план", "отклон"]):
                    fmt_money(c)
                else:
                    fmt_num(c)
    autofit(ws)
    ws.freeze_panes = "A2"


def export_files(blocks: Dict[str, pd.DataFrame], report_path: str, tech_path: str, example_path: str):
    wb = Workbook()
    ws = wb.active
    ws.title = "Сводка"
    row = 1
    row = write_hierarchy_block(ws, row, "Текущая неделя — Валовая прибыль - НДС", blocks.get("daily_main", pd.DataFrame()))
    row = write_hierarchy_block(ws, row, "Текущая неделя — Валовая прибыль", blocks.get("daily_gp", pd.DataFrame()))
    row = write_hierarchy_block(ws, row, "Текущая неделя — НДС", blocks.get("daily_vat", pd.DataFrame()))
    row = write_hierarchy_block(ws, row, "Текущий месяц — Валовая прибыль - НДС по неделям", blocks.get("weekly_main", pd.DataFrame()))
    row = write_hierarchy_block(ws, row, "Последние 3 месяца — Валовая прибыль - НДС", blocks.get("monthly_main", pd.DataFrame()))
    ws.freeze_panes = "B3"
    autofit(ws)
    wb.save(report_path)

    twb = Workbook()
    twb.remove(twb.active)
    for key in ["tech_daily", "tech_orders_daily", "tech_ads_diag", "tech_econ_diag", "tech_weekly", "tech_monthly", "tech_buyout90", "tech_plan", "tech_master"]:
        write_dataframe_sheet(twb, key.replace("tech_", ""), blocks.get(key, pd.DataFrame()))
    twb.save(tech_path)

    ewb = Workbook()
    ewb.remove(ewb.active)
    ex = blocks.get("example_weekly", pd.DataFrame())
    if ex is None or ex.empty:
        ws = ewb.create_sheet("Пример")
        ws.cell(1, 1, "Нет данных")
    else:
        for art in EXAMPLE_ARTICLES:
            write_dataframe_sheet(ewb, art.replace("/", "_"), ex[ex["Артикул"] == art].copy())
    ewb.save(example_path)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--root", default=".")
    p.add_argument("--reports-root", default="Отчёты")
    p.add_argument("--store", default="TOPFACE")
    p.add_argument("--out-subdir", default="Отчёты/Объединенный отчет/TOPFACE")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    storage = make_storage(args.root)
    loader = Stage1Loader(storage, args.reports_root, args.store)
    data = loader.load_all()
    for w in data.warnings:
        log(f"WARN: {w}")
    builder = Stage1Builder(data)
    blocks = builder.build_main_blocks()

    stamp = datetime.now().strftime("%Y-%m-%d")
    out_report = f"{args.out_subdir}/Объединенный_отчет_{args.store}_{stamp}.xlsx"
    out_tech = f"{args.out_subdir}/Технические_расчеты_{args.store}_{stamp}.xlsx"
    out_example = f"{args.out_subdir}/Пример_расчета_901_{args.store}_{stamp}.xlsx"

    local_report = Path("/tmp") / f"wb_report_{stamp}.xlsx"
    local_tech = Path("/tmp") / f"wb_tech_{stamp}.xlsx"
    local_example = Path("/tmp") / f"wb_example_{stamp}.xlsx"
    export_files(blocks, str(local_report), str(local_tech), str(local_example))

    storage.write_bytes(out_report, local_report.read_bytes())
    storage.write_bytes(out_tech, local_tech.read_bytes())
    storage.write_bytes(out_example, local_example.read_bytes())
    log(f"Saved report: {out_report}")
    log(f"Saved technical workbook: {out_tech}")
    log(f"Saved example workbook: {out_example}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
