#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
WB Combined Report for TOPFACE.

Builds a user-friendly Excel report and a calculation log using prepared reports
stored either locally or in Yandex Object Storage (S3-compatible).

Main outputs go to:
    Отчёты/Объединенный отчет/TOPFACE/

Design goals:
- Daily diagnostic of orders and approximate gross profit using latest unit economics
- Weekly retrospective with root-cause analysis
- Monthly retrospective and forecast for incomplete month
- Simple business-oriented layout: subject sheets, code sheets, narrative on the right,
  raw calculations in a separate log workbook

This script intentionally does not call WB APIs. It relies on already prepared reports.
To fetch/update reports, run your updater separately.
"""

from __future__ import annotations

import argparse
import calendar
import io
import math
import os
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import boto3
import numpy as np
import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.worksheet import Worksheet

# -------------------------
# Constants / presentation
# -------------------------

TITLE_FILL = PatternFill("solid", fgColor="D9EAF7")
HEADER_FILL = PatternFill("solid", fgColor="EAF2F8")
SUBHEADER_FILL = PatternFill("solid", fgColor="F4F6F7")
GOOD_FILL = PatternFill("solid", fgColor="E8F8F5")
BAD_FILL = PatternFill("solid", fgColor="FDEDEC")
NOTE_FILL = PatternFill("solid", fgColor="FEF9E7")
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

# Regions service map can be extended over time.
MOSCOW_CLUSTER = {"Коледино", "Электросталь", "Подольск", "Белые Столбы"}
REGION_CLUSTER_MAP = {
    "Москва": "MOSCOW_CLUSTER",
    "Московская область": "MOSCOW_CLUSTER",
    "Санкт-Петербург": "СПБ Шушары",
    "Ленинградская область": "СПБ Шушары",
    "Свердловская область": "Екатеринбург - Перспективная 14",
    "Челябинская область": "Екатеринбург - Перспективная 14",
    "Тюменская область": "Екатеринбург - Перспективная 14",
    "Ханты-Мансийский автономный округ": "Екатеринбург - Перспективная 14",
    "Ямало-Ненецкий автономный округ": "Екатеринбург - Перспективная 14",
    "Пермский край": "Екатеринбург - Перспективная 14",
    "Краснодарский край": "Краснодар",
    "Ростовская область": "Краснодар",
    "Республика Крым": "Краснодар",
    "Севастополь": "Краснодар",
    "Ставропольский край": "Невинномысск",
    "Республика Дагестан": "Невинномысск",
    "Чеченская Республика": "Невинномысск",
    "Кабардино-Балкарская Республика": "Невинномысск",
    "Карачаево-Черкесская Республика": "Невинномысск",
    "Республика Ингушетия": "Невинномысск",
    "Республика Северная Осетия — Алания": "Невинномысск",
    "Самарская область": "Самара (Новосемейкино)",
    "Оренбургская область": "Самара (Новосемейкино)",
    "Республика Татарстан": "Казань",
    "Ульяновская область": "Казань",
    "Чувашская Республика": "Казань",
    "Пензенская область": "Пенза",
    "Саратовская область": "Пенза",
    "Республика Мордовия": "Пенза",
    "Рязанская область": "Рязань (Тюшевское)",
    "Калужская область": "Рязань (Тюшевское)",
    "Владимирская область": "Владимир",
    "Нижегородская область": "Владимир",
    "Ярославская область": "Владимир",
    "Ивановская область": "Владимир",
    "Костромская область": "Владимир",
    "Волгоградская область": "Волгоград",
    "Астраханская область": "Волгоград",
    "Республика Калмыкия": "Волгоград",
    "Тульская область": "Тула",
    "Белгородская область": "Тула",
    "Курская область": "Тула",
    "Брянская область": "Тула",
    "Смоленская область": "Тула",
    "Орловская область": "Тула",
    "Республика Башкортостан": "Сарапул",
    "Удмуртская Республика": "Сарапул",
    "Воронежская область": "Воронеж",
    "Тамбовская область": "Котовск",
    "Липецкая область": "Котовск",
}

# -------------------------
# Helpers
# -------------------------


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


def safe_sheet_name(name: str) -> str:
    name = re.sub(r"[\\/*?:\[\]]", "_", str(name))
    return name[:31] if len(name) > 31 else name


def week_code_from_date(d: date) -> str:
    iso_year, iso_week, _ = d.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


def parse_date_maybe(x) -> Optional[pd.Timestamp]:
    if pd.isna(x):
        return None
    if isinstance(x, pd.Timestamp):
        return x
    try:
        return pd.to_datetime(x, errors="coerce")
    except Exception:
        return None


def clean_article(x) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip()
    return s


def extract_code(article: str) -> str:
    article = clean_article(article)
    if not article:
        return ""
    m = re.match(r"(\d+)", article)
    return m.group(1) if m else article.split("/")[0].replace("_", "")


def is_brush_subject(subject: str) -> bool:
    return clean_article(subject).lower() == "кисти косметические"


def month_key(ts: pd.Timestamp) -> str:
    return ts.strftime("%Y-%m")


def pct_delta(cur: float, prev: float) -> Optional[float]:
    if prev is None or pd.isna(prev) or prev == 0:
        return None
    return (cur / prev - 1.0) * 100.0


def safe_div(a, b) -> float:
    try:
        if b in (0, None) or pd.isna(b):
            return 0.0
        return float(a) / float(b)
    except Exception:
        return 0.0


def find_col(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    norm = {str(c).strip().lower(): c for c in df.columns}
    for cand in candidates:
        key = cand.strip().lower()
        if key in norm:
            return norm[key]
    return None


def read_excel_normalized(data: bytes, filename: str, sheet_name=0) -> pd.DataFrame:
    bio = io.BytesIO(data)
    return pd.read_excel(bio, sheet_name=sheet_name)


# -------------------------
# Storage abstraction
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
        prefix_path = self._abs(prefix)
        base = prefix_path if prefix_path.exists() else prefix_path.parent
        if not base.exists():
            return []
        files = []
        for p in base.rglob("*"):
            if p.is_file():
                rel = str(p.relative_to(self.root)).replace("\\", "/")
                if rel.startswith(prefix.replace("\\", "/")):
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
    use_s3 = os.getenv("USE_S3", "0") == "1"
    if use_s3:
        needed = ["YC_BUCKET_NAME", "YC_ACCESS_KEY_ID", "YC_SECRET_ACCESS_KEY"]
        missing = [k for k in needed if not os.getenv(k)]
        if missing:
            raise RuntimeError(f"Missing S3 env vars: {missing}")
        return S3Storage(
            bucket=os.environ["YC_BUCKET_NAME"],
            access_key=os.environ["YC_ACCESS_KEY_ID"],
            secret_key=os.environ["YC_SECRET_ACCESS_KEY"],
        )
    return LocalStorage(root)


# -------------------------
# Loaders
# -------------------------

@dataclass
class LoadedData:
    orders: pd.DataFrame
    stocks: pd.DataFrame
    search: pd.DataFrame
    funnel: pd.DataFrame
    ads: pd.DataFrame
    economics: pd.DataFrame
    abc: pd.DataFrame
    entry_points_category: pd.DataFrame
    entry_points_sku: pd.DataFrame


class DataLoader:
    def __init__(self, storage: BaseStorage, store: str, reports_root: str = "Отчёты"):
        self.storage = storage
        self.store = store
        self.reports_root = reports_root.rstrip("/")

    def _prefix(self, *parts: str) -> str:
        return "/".join([self.reports_root, *parts]).replace("//", "/")

    def _load_first_sheet(self, path: str) -> pd.DataFrame:
        return read_excel_normalized(self.storage.read_bytes(path), path, 0)

    def _pick_latest(self, files: List[str]) -> Optional[str]:
        return sorted(files)[-1] if files else None

    def load_orders(self) -> pd.DataFrame:
        files = self.storage.list_files(self._prefix("Заказы", self.store, "Недельные"))
        xlsx = [f for f in files if f.lower().endswith(".xlsx") and "/~$" not in f]
        dfs = []
        for f in xlsx:
            try:
                df = self._load_first_sheet(f)
                df["__source_file"] = f
                dfs.append(df)
            except Exception as e:
                log(f"Failed to load orders {f}: {e}")
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    def load_stocks(self) -> pd.DataFrame:
        files = self.storage.list_files(self._prefix("Остатки", self.store, "Недельные"))
        xlsx = [f for f in files if f.lower().endswith(".xlsx")]
        dfs = []
        for f in xlsx:
            try:
                df = self._load_first_sheet(f)
                df["__source_file"] = f
                dfs.append(df)
            except Exception as e:
                log(f"Failed to load stocks {f}: {e}")
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    def load_search(self) -> pd.DataFrame:
        files = self.storage.list_files(self._prefix("Поисковые запросы", self.store, "Недельные"))
        xlsx = [f for f in files if f.lower().endswith(".xlsx")]
        dfs = []
        for f in xlsx:
            try:
                df = self._load_first_sheet(f)
                df["__source_file"] = f
                dfs.append(df)
            except Exception as e:
                log(f"Failed to load search {f}: {e}")
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    def load_funnel(self) -> pd.DataFrame:
        path = self._prefix("Воронка продаж", self.store, "Воронка продаж.xlsx")
        return self._load_first_sheet(path) if self.storage.exists(path) else pd.DataFrame()

    def load_ads(self) -> pd.DataFrame:
        path = self._prefix("Реклама", self.store, "Анализ рекламы.xlsx")
        return self._load_first_sheet(path) if self.storage.exists(path) else pd.DataFrame()

    def load_economics(self) -> pd.DataFrame:
        path = self._prefix("Финансовые показатели", self.store, "Экономика.xlsx")
        if not self.storage.exists(path):
            path = self._prefix("Финансовые показатели", self.store, "Недельные", "Экономика.xlsx")
        if not self.storage.exists(path):
            return pd.DataFrame()
        return read_excel_normalized(self.storage.read_bytes(path), path, "Юнит экономика")

    def load_abc(self) -> pd.DataFrame:
        files = self.storage.list_files(self._prefix("ABC"))
        xlsx = [f for f in files if f.lower().endswith(".xlsx") and "wb_abc_report_goods__" in Path(f).name]
        dfs = []
        for f in sorted(xlsx):
            try:
                df = self._load_first_sheet(f)
                start, end = parse_abc_period_from_name(Path(f).name)
                df["week_start"] = pd.Timestamp(start) if start else pd.NaT
                df["week_end"] = pd.Timestamp(end) if end else pd.NaT
                df["week_code"] = week_code_from_date(start) if start else None
                df["__source_file"] = f
                dfs.append(df)
            except Exception as e:
                log(f"Failed to load ABC {f}: {e}")
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    def load_entry_points(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        files = self.storage.list_files(self._prefix("Точки входа", self.store))
        xlsx = [f for f in files if f.lower().endswith(".xlsx") and "Точки входа" in Path(f).name]
        cat_dfs, sku_dfs = [], []
        for f in sorted(xlsx):
            try:
                data = self.storage.read_bytes(f)
                xl = pd.ExcelFile(io.BytesIO(data))
                if "Детализация по точкам входа" in xl.sheet_names:
                    cdf = pd.read_excel(io.BytesIO(data), sheet_name="Детализация по точкам входа")
                    start, end = parse_entry_period_from_name(Path(f).name)
                    cdf["week_start"] = pd.Timestamp(start) if start else pd.NaT
                    cdf["week_end"] = pd.Timestamp(end) if end else pd.NaT
                    cdf["week_code"] = week_code_from_date(start) if start else None
                    cdf["__source_file"] = f
                    cat_dfs.append(cdf)
                if "Детализация по артикулам" in xl.sheet_names:
                    sdf = pd.read_excel(io.BytesIO(data), sheet_name="Детализация по артикулам")
                    start, end = parse_entry_period_from_name(Path(f).name)
                    sdf["week_start"] = pd.Timestamp(start) if start else pd.NaT
                    sdf["week_end"] = pd.Timestamp(end) if end else pd.NaT
                    sdf["week_code"] = week_code_from_date(start) if start else None
                    sdf["__source_file"] = f
                    sku_dfs.append(sdf)
            except Exception as e:
                log(f"Failed to load entry points {f}: {e}")
        cat = pd.concat(cat_dfs, ignore_index=True) if cat_dfs else pd.DataFrame()
        sku = pd.concat(sku_dfs, ignore_index=True) if sku_dfs else pd.DataFrame()
        return cat, sku

    def load_all(self) -> LoadedData:
        entry_cat, entry_sku = self.load_entry_points()
        return LoadedData(
            orders=self.load_orders(),
            stocks=self.load_stocks(),
            search=self.load_search(),
            funnel=self.load_funnel(),
            ads=self.load_ads(),
            economics=self.load_economics(),
            abc=self.load_abc(),
            entry_points_category=entry_cat,
            entry_points_sku=entry_sku,
        )


# -------------------------
# Parsing periods from filenames
# -------------------------


def parse_abc_period_from_name(filename: str) -> Tuple[Optional[date], Optional[date]]:
    m = re.search(r"__(\d{2}\.\d{2}\.\d{4})-(\d{2}\.\d{2}\.\d{4})__", filename)
    if not m:
        return None, None
    return datetime.strptime(m.group(1), "%d.%m.%Y").date(), datetime.strptime(m.group(2), "%d.%m.%Y").date()


def parse_entry_period_from_name(filename: str) -> Tuple[Optional[date], Optional[date]]:
    m = re.search(r"с (\d{2})-(\d{2})-(\d{4}) по (\d{2})-(\d{2})-(\d{4})", filename)
    if not m:
        return None, None
    start = date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    end = date(int(m.group(6)), int(m.group(5)), int(m.group(4)))
    return start, end


def explode_week_rows_to_months(df: pd.DataFrame, start_col: str = "week_start", end_col: str = "week_end") -> pd.DataFrame:
    """Split weekly rows across months by overlap days."""
    if df.empty or start_col not in df.columns or end_col not in df.columns:
        return df.copy()
    rows = []
    for _, row in df.iterrows():
        st = parse_date_maybe(row.get(start_col))
        en = parse_date_maybe(row.get(end_col))
        if st is None or en is None or pd.isna(st) or pd.isna(en):
            nr = row.copy()
            nr["month_key"] = pd.NaT
            nr["month_weight"] = 1.0
            nr["overlap_days"] = np.nan
            rows.append(nr)
            continue
        st = pd.Timestamp(st).normalize()
        en = pd.Timestamp(en).normalize()
        total_days = max((en - st).days + 1, 1)
        cur = st.replace(day=1)
        while cur <= en:
            month_start = cur
            month_end = (cur + pd.offsets.MonthEnd(0)).normalize()
            ov_start = max(st, month_start)
            ov_end = min(en, month_end)
            if ov_start <= ov_end:
                overlap_days = (ov_end - ov_start).days + 1
                nr = row.copy()
                nr["month_key"] = ov_start.to_period("M").strftime("%Y-%m")
                nr["month_weight"] = overlap_days / total_days
                nr["overlap_days"] = overlap_days
                rows.append(nr)
            cur = (cur + pd.offsets.MonthBegin(1)).normalize()
    return pd.DataFrame(rows)


# -------------------------
# Metrics builders
# -------------------------

class MetricsBuilder:
    def __init__(self, data: LoadedData):
        self.data = data

    def build_daily_orders(self) -> pd.DataFrame:
        df = self.data.orders.copy()
        if df.empty:
            return df
        date_col = find_col(df, ["date", "Дата", "Дата заказа"])
        article_col = find_col(df, ["supplierArticle", "Артикул продавца"])
        nm_col = find_col(df, ["nmId", "Артикул WB"])
        subject_col = find_col(df, ["subject", "Предмет"])
        finished_col = find_col(df, ["finishedPrice", "Цена с учетом всех скидок, кроме суммы по WB Кошельку"])
        pwd_col = find_col(df, ["priceWithDisc", "Цена со скидкой продавца, в том числе со скидкой WB Клуба"])
        spp_col = find_col(df, ["spp", "СПП", "Скидка WB, %"])
        cancel_col = find_col(df, ["isCancel", "Отмена заказа"])
        region_col = find_col(df, ["regionName", "Регион"])

        df["day"] = pd.to_datetime(df[date_col], errors="coerce").dt.date
        df["supplier_article"] = df[article_col].map(clean_article)
        df["nm_id"] = df[nm_col].astype(str)
        df["subject"] = df[subject_col].astype(str)
        df["code"] = df["supplier_article"].map(extract_code)
        df["finished_price"] = pd.to_numeric(df[finished_col], errors="coerce")
        df["price_with_disc"] = pd.to_numeric(df[pwd_col], errors="coerce")
        df["spp"] = pd.to_numeric(df[spp_col], errors="coerce")
        if cancel_col:
            df["is_cancel"] = df[cancel_col].astype(str).str.lower().isin(["true", "1", "да"])
        else:
            df["is_cancel"] = False
        if region_col:
            df["region"] = df[region_col].astype(str)
        else:
            df["region"] = ""

        good = df[~df["is_cancel"]].copy()
        agg = (
            good.groupby(["day", "supplier_article", "nm_id", "subject", "code"], dropna=False)
            .agg(
                orders_day=("supplier_article", "size"),
                avg_finished_price_day=("finished_price", "mean"),
                avg_price_with_disc_day=("price_with_disc", "mean"),
                avg_spp_day=("spp", "mean"),
            )
            .reset_index()
        )
        return agg

    def build_daily_orders_region(self) -> pd.DataFrame:
        df = self.data.orders.copy()
        if df.empty:
            return df
        date_col = find_col(df, ["date", "Дата", "Дата заказа"])
        article_col = find_col(df, ["supplierArticle", "Артикул продавца"])
        subject_col = find_col(df, ["subject", "Предмет"])
        region_col = find_col(df, ["regionName", "Регион"])
        cancel_col = find_col(df, ["isCancel", "Отмена заказа"])
        if not (date_col and article_col and region_col):
            return pd.DataFrame()
        df["day"] = pd.to_datetime(df[date_col], errors="coerce").dt.date
        df["supplier_article"] = df[article_col].map(clean_article)
        df["subject"] = df[subject_col].astype(str) if subject_col else ""
        df["code"] = df["supplier_article"].map(extract_code)
        df["region"] = df[region_col].astype(str)
        if cancel_col:
            df["is_cancel"] = df[cancel_col].astype(str).str.lower().isin(["true", "1", "да"])
        else:
            df["is_cancel"] = False
        good = df[~df["is_cancel"]].copy()
        reg = (
            good.groupby(["day", "supplier_article", "subject", "code", "region"], dropna=False)
            .size()
            .reset_index(name="regional_orders")
        )
        reg["local_cluster"] = reg["region"].map(lambda x: REGION_CLUSTER_MAP.get(x, ""))
        return reg

    def build_daily_stocks(self) -> pd.DataFrame:
        df = self.data.stocks.copy()
        if df.empty:
            return df
        date_col = find_col(df, ["Дата сбора", "date", "Дата"])
        article_col = find_col(df, ["Артикул продавца", "supplierArticle", "Артикул WB продавца"])
        wh_col = find_col(df, ["Склад", "warehouseName"])
        qty_col = find_col(df, ["Доступно для продажи", "Остаток", "stock"])
        nm_col = find_col(df, ["nmId", "Артикул WB"])
        if not (date_col and wh_col and qty_col):
            return pd.DataFrame()
        df["day"] = pd.to_datetime(df[date_col], errors="coerce").dt.date
        if article_col:
            df["supplier_article"] = df[article_col].map(clean_article)
        elif nm_col:
            df["supplier_article"] = df[nm_col].astype(str)
        else:
            df["supplier_article"] = ""
        df["warehouse"] = df[wh_col].astype(str)
        df["stock_qty"] = pd.to_numeric(df[qty_col], errors="coerce").fillna(0)
        df["code"] = df["supplier_article"].map(extract_code)
        return df[["day", "supplier_article", "code", "warehouse", "stock_qty"]].copy()

    def build_daily_search(self) -> pd.DataFrame:
        df = self.data.search.copy()
        if df.empty:
            return df
        date_col = find_col(df, ["Дата", "date"])
        article_col = find_col(df, ["Артикул продавца", "supplierArticle"])
        subject_col = find_col(df, ["Предмет", "subject"])
        nm_col = find_col(df, ["Артикул WB", "nmId"])
        query_col = find_col(df, ["Поисковый запрос", "query", "Запрос"])
        freq_col = find_col(df, ["Частота запросов", "Частота", "frequency"])
        clicks_col = find_col(df, ["Переходы в карточку", "Клики", "Клики в карточку"])
        orders_col = find_col(df, ["Заказы", "Заказали"])
        pos_col = find_col(df, ["Средняя позиция", "Позиция"])
        vis_col = find_col(df, ["Видимость, %", "Видимость"])
        if not (date_col and article_col):
            return pd.DataFrame()
        df["day"] = pd.to_datetime(df[date_col], errors="coerce").dt.date
        df["supplier_article"] = df[article_col].map(clean_article)
        df["subject"] = df[subject_col].astype(str) if subject_col else ""
        df["nm_id"] = df[nm_col].astype(str) if nm_col else ""
        df["query"] = df[query_col].astype(str) if query_col else ""
        df["search_frequency"] = pd.to_numeric(df[freq_col], errors="coerce").fillna(0) if freq_col else 0
        df["search_clicks"] = pd.to_numeric(df[clicks_col], errors="coerce").fillna(0) if clicks_col else 0
        df["search_orders"] = pd.to_numeric(df[orders_col], errors="coerce").fillna(0) if orders_col else 0
        df["avg_position"] = pd.to_numeric(df[pos_col], errors="coerce") if pos_col else np.nan
        df["visibility_pct"] = pd.to_numeric(df[vis_col], errors="coerce") if vis_col else np.nan
        df["code"] = df["supplier_article"].map(extract_code)

        dedup_cols = [c for c in ["day", "supplier_article", "nm_id", "query"] if c in df.columns]
        df = df.drop_duplicates(subset=dedup_cols, keep="first")
        return df[[
            "day", "supplier_article", "subject", "nm_id", "code", "query",
            "search_frequency", "search_clicks", "search_orders", "avg_position", "visibility_pct"
        ]].copy()

    def build_daily_search_agg(self) -> pd.DataFrame:
        df = self.build_daily_search()
        if df.empty:
            return df
        agg = (
            df.groupby(["day", "supplier_article", "subject", "nm_id", "code"], dropna=False)
            .agg(
                search_frequency=("search_frequency", "sum"),
                search_clicks=("search_clicks", "sum"),
                search_orders=("search_orders", "sum"),
                avg_position=("avg_position", "mean"),
                visibility_pct=("visibility_pct", "mean"),
            )
            .reset_index()
        )
        agg["search_capture_share"] = agg.apply(lambda r: safe_div(r["search_clicks"], r["search_frequency"]), axis=1)
        return agg

    def build_daily_funnel(self) -> pd.DataFrame:
        df = self.data.funnel.copy()
        if df.empty:
            return df
        date_col = find_col(df, ["date", "Дата", "Дата отчета"])
        article_col = find_col(df, ["supplierArticle", "Артикул продавца"])
        nm_col = find_col(df, ["nmId", "Артикул WB"])
        subject_col = find_col(df, ["subject", "Предмет"])
        open_col = find_col(df, ["openCardCount", "Открытие карточки"])
        cart_col = find_col(df, ["addToCartCount", "Добавлени в корзину", "Добавили в корзину"])
        orders_col = find_col(df, ["ordersCount", "Заказы"])
        if not (date_col and article_col and open_col):
            return pd.DataFrame()
        df["day"] = pd.to_datetime(df[date_col], errors="coerce").dt.date
        df["supplier_article"] = df[article_col].map(clean_article)
        df["nm_id"] = df[nm_col].astype(str) if nm_col else ""
        df["subject"] = df[subject_col].astype(str) if subject_col else ""
        df["code"] = df["supplier_article"].map(extract_code)
        df["open_card_count"] = pd.to_numeric(df[open_col], errors="coerce").fillna(0)
        df["add_to_cart_count"] = pd.to_numeric(df[cart_col], errors="coerce").fillna(0) if cart_col else 0
        df["funnel_orders"] = pd.to_numeric(df[orders_col], errors="coerce").fillna(0) if orders_col else 0
        agg = (
            df.groupby(["day", "supplier_article", "nm_id", "subject", "code"], dropna=False)
            .agg(
                open_card_count=("open_card_count", "sum"),
                add_to_cart_count=("add_to_cart_count", "sum"),
                funnel_orders=("funnel_orders", "sum"),
            )
            .reset_index()
        )
        agg["conv_to_cart"] = agg.apply(lambda r: safe_div(r["add_to_cart_count"], r["open_card_count"]), axis=1)
        agg["conv_to_order"] = agg.apply(lambda r: safe_div(r["funnel_orders"], r["open_card_count"]), axis=1)
        agg["conv_cart_to_order"] = agg.apply(lambda r: safe_div(r["funnel_orders"], r["add_to_cart_count"]), axis=1)
        return agg

    def build_daily_ads(self) -> pd.DataFrame:
        df = self.data.ads.copy()
        if df.empty:
            return df
        date_col = find_col(df, ["date", "Дата"])
        article_col = find_col(df, ["supplierArticle", "Артикул продавца"])
        nm_col = find_col(df, ["nmId", "Артикул WB"])
        subject_col = find_col(df, ["subject", "Предмет"])
        imp_col = find_col(df, ["impressions", "Показы"])
        clicks_col = find_col(df, ["clicks", "Клики"])
        orders_col = find_col(df, ["orders", "Заказы"])
        spend_col = find_col(df, ["spend", "Расход"])
        type_col = find_col(df, ["campaignType", "Тип кампании", "type"])
        if not (date_col and article_col):
            return pd.DataFrame()
        df["day"] = pd.to_datetime(df[date_col], errors="coerce").dt.date
        df["supplier_article"] = df[article_col].map(clean_article)
        df["nm_id"] = df[nm_col].astype(str) if nm_col else ""
        df["subject"] = df[subject_col].astype(str) if subject_col else ""
        df["code"] = df["supplier_article"].map(extract_code)
        df["ad_impressions"] = pd.to_numeric(df[imp_col], errors="coerce").fillna(0) if imp_col else 0
        df["ad_clicks"] = pd.to_numeric(df[clicks_col], errors="coerce").fillna(0) if clicks_col else 0
        df["ad_orders"] = pd.to_numeric(df[orders_col], errors="coerce").fillna(0) if orders_col else 0
        df["ad_spend"] = pd.to_numeric(df[spend_col], errors="coerce").fillna(0) if spend_col else 0
        df["campaign_type"] = df[type_col].astype(str).str.lower() if type_col else ""
        agg = (
            df.groupby(["day", "supplier_article", "nm_id", "subject", "code"], dropna=False)
            .agg(
                ad_impressions=("ad_impressions", "sum"),
                ad_clicks=("ad_clicks", "sum"),
                ad_orders=("ad_orders", "sum"),
                ad_spend=("ad_spend", "sum"),
            )
            .reset_index()
        )
        unified = df[df["campaign_type"].str.contains("unified", na=False)].groupby(
            ["day", "supplier_article", "nm_id", "subject", "code"], dropna=False
        ).agg(unified_impressions=("ad_impressions", "sum"), unified_clicks=("ad_clicks", "sum"), unified_orders=("ad_orders", "sum")).reset_index()
        agg = agg.merge(unified, on=["day", "supplier_article", "nm_id", "subject", "code"], how="left")
        agg[["unified_impressions", "unified_clicks", "unified_orders"]] = agg[["unified_impressions", "unified_clicks", "unified_orders"]].fillna(0)
        agg["ad_ctr"] = agg.apply(lambda r: safe_div(r["ad_clicks"], r["ad_impressions"]), axis=1)
        agg["ad_cr"] = agg.apply(lambda r: safe_div(r["ad_orders"], r["ad_clicks"]), axis=1)
        return agg

    def build_economics_latest(self) -> pd.DataFrame:
        df = self.data.economics.copy()
        if df.empty:
            return df
        article_col = find_col(df, ["Артикул продавца", "supplierArticle"])
        nm_col = find_col(df, ["Артикул WB", "nmId"])
        subject_col = find_col(df, ["Предмет", "subject"])
        week_col = find_col(df, ["Неделя", "week"])
        df["supplier_article"] = df[article_col].map(clean_article)
        df["nm_id"] = df[nm_col].astype(str) if nm_col else ""
        df["subject"] = df[subject_col].astype(str) if subject_col else ""
        df["code"] = df["supplier_article"].map(extract_code)
        latest_week = sorted(df[week_col].dropna().astype(str).unique())[-1]
        latest = df[df[week_col].astype(str) == latest_week].copy()
        buyout_col = find_col(latest, ["Процент выкупа"])
        latest["buyout_rate"] = pd.to_numeric(latest[buyout_col], errors="coerce").fillna(0) / 100.0 if buyout_col else 0.8
        # Normalize columns
        mapping = {
            "avg_sale_price_week": ["Средняя цена продажи"],
            "avg_buyer_price_week": ["Средняя цена покупателя"],
            "spp_week": ["СПП, %"],
            "commission_rub_unit": ["Комиссия WB, руб/ед"],
            "acquiring_rub_unit": ["Эквайринг, руб/ед"],
            "vat_rub_unit": ["НДС, руб/ед"],
            "logistics_direct_rub_unit": ["Логистика прямая, руб/ед"],
            "logistics_reverse_rub_unit": ["Логистика обратная, руб/ед"],
            "storage_rub_unit": ["Хранение, руб/ед"],
            "acceptance_rub_unit": ["Приёмка, руб/ед"],
            "penalties_rub_unit": ["Штрафы и удержания, руб/ед"],
            "ads_rub_unit": ["Реклама, руб/ед"],
            "other_rub_unit": ["Прочие расходы, руб/ед"],
            "cost_rub_unit": ["Себестоимость, руб"],
            "gross_profit_rub_unit": ["Валовая прибыль, руб/ед"],
            "net_profit_rub_unit": ["Чистая прибыль, руб/ед"],
            "gross_margin_pct": ["Валовая рентабельность, %"],
            "net_margin_pct": ["Чистая рентабельность, %"],
            "sales_units_week": ["Продажи, шт"],
            "returns_units_week": ["Возвраты, шт"],
            "net_sales_units_week": ["Чистые продажи, шт"],
        }
        out = latest[[c for c in ["supplier_article", "nm_id", "subject", "code"] if c in latest.columns]].copy()
        out["week_code"] = latest_week
        buyout_col = find_col(latest, ["Процент выкупа"])
        out["buyout_rate"] = pd.to_numeric(latest[buyout_col], errors="coerce").fillna(0) / 100.0 if buyout_col else 0.8
        for dst, cands in mapping.items():
            col = find_col(latest, cands)
            out[dst] = pd.to_numeric(latest[col], errors="coerce").fillna(0) if col else 0.0
        out["commission_rate"] = out.apply(lambda r: safe_div(r["commission_rub_unit"], r["avg_sale_price_week"]), axis=1)
        out["acquiring_rate"] = out.apply(lambda r: safe_div(r["acquiring_rub_unit"], r["avg_sale_price_week"]), axis=1)
        out["vat_rate"] = out.apply(lambda r: safe_div(r["vat_rub_unit"], r["avg_sale_price_week"]), axis=1)
        return out

    def build_daily_localization(self, orders_region: pd.DataFrame, stocks_daily: pd.DataFrame) -> pd.DataFrame:
        if orders_region.empty or stocks_daily.empty:
            return pd.DataFrame()
        cluster_rows = []
        # aggregate stocks per cluster/day/sku
        stocks = stocks_daily.copy()
        stocks["cluster"] = stocks["warehouse"].map(lambda w: "MOSCOW_CLUSTER" if w in MOSCOW_CLUSTER else w)
        cluster_stock = stocks.groupby(["day", "supplier_article", "cluster"], dropna=False)["stock_qty"].sum().reset_index()
        merged = orders_region.merge(
            cluster_stock,
            left_on=["day", "supplier_article", "local_cluster"],
            right_on=["day", "supplier_article", "cluster"],
            how="left",
        )
        merged["stock_qty"] = merged["stock_qty"].fillna(0)
        merged["local_orders_covered"] = merged[["regional_orders", "stock_qty"]].min(axis=1)
        merged["nonlocal_orders"] = (merged["regional_orders"] - merged["local_orders_covered"]).clip(lower=0)
        agg = merged.groupby(["day", "supplier_article", "subject", "code"], dropna=False).agg(
            total_regional_orders=("regional_orders", "sum"),
            local_orders_covered=("local_orders_covered", "sum"),
            nonlocal_orders=("nonlocal_orders", "sum"),
        ).reset_index()
        agg["localization_index"] = agg.apply(lambda r: safe_div(r["local_orders_covered"], r["total_regional_orders"]), axis=1)
        return agg

    def build_daily_profit_estimate(self, daily_orders: pd.DataFrame, econ_latest: pd.DataFrame) -> pd.DataFrame:
        if daily_orders.empty or econ_latest.empty:
            return pd.DataFrame()
        df = daily_orders.merge(econ_latest, on=["supplier_article", "nm_id", "subject", "code"], how="left")
        fixed_cols = [
            "logistics_direct_rub_unit", "logistics_reverse_rub_unit", "storage_rub_unit",
            "acceptance_rub_unit", "penalties_rub_unit", "ads_rub_unit", "other_rub_unit", "cost_rub_unit"
        ]
        df["fixed_costs_per_unit"] = df[fixed_cols].fillna(0).sum(axis=1)
        df["expected_sales_day"] = df["orders_day"] * df["buyout_rate"].fillna(0)
        df["gross_profit_unit_est"] = (
            df["avg_price_with_disc_day"].fillna(0)
            * (1 - df["commission_rate"].fillna(0) - df["acquiring_rate"].fillna(0) - df["vat_rate"].fillna(0))
            - df["fixed_costs_per_unit"].fillna(0)
        )
        df["gross_profit_day_est"] = df["expected_sales_day"] * df["gross_profit_unit_est"]
        return df[[
            "day", "supplier_article", "nm_id", "subject", "code",
            "orders_day", "expected_sales_day", "avg_finished_price_day", "avg_price_with_disc_day", "avg_spp_day",
            "gross_profit_unit_est", "gross_profit_day_est"
        ]].copy()

    def build_daily_targets(self, daily_current: pd.DataFrame) -> pd.DataFrame:
        if daily_current.empty:
            return pd.DataFrame()
        df = daily_current.copy()
        df["weekday"] = pd.to_datetime(df["day"]).dt.weekday
        # Список метрик, которые реально существуют в daily_current
        possible_metrics = [
            "orders_day", "gross_profit_day_est", "search_frequency", "search_clicks", "search_capture_share",
            "avg_position", "visibility_pct", "open_card_count", "add_to_cart_count", "funnel_orders",
            "conv_to_cart", "conv_to_order", "conv_cart_to_order", "ad_impressions", "ad_clicks",
            "ad_ctr", "ad_cr", "ad_spend", "localization_index", "avg_finished_price_day", "avg_price_with_disc_day", "avg_spp_day"
        ]
        metrics = [m for m in possible_metrics if m in df.columns]
        rows = []
        for (sku, wd), sub in df.groupby(["supplier_article", "weekday"], dropna=False):
            row = {
                "supplier_article": sku,
                "weekday": wd,
                "nm_id": sub["nm_id"].dropna().astype(str).iloc[0] if "nm_id" in sub.columns and len(sub) else "",
                "subject": sub["subject"].dropna().astype(str).iloc[0] if len(sub) else "",
                "code": sub["code"].dropna().astype(str).iloc[0] if len(sub) else "",
                "days_count": len(sub),
            }
            for m in metrics:
                vals = pd.to_numeric(sub[m], errors="coerce").dropna()
                if vals.empty:
                    row[f"base_{m}"] = np.nan
                    row[f"target_{m}"] = np.nan
                    continue
                mean_v = vals.mean()
                upper = vals[vals > mean_v]
                target_v = upper.mean() if not upper.empty else mean_v
                row[f"base_{m}"] = mean_v
                row[f"target_{m}"] = target_v
            rows.append(row)
        return pd.DataFrame(rows)

    def build_daily_current(self) -> pd.DataFrame:
        daily_orders = self.build_daily_orders()
        reg_orders = self.build_daily_orders_region()
        stocks = self.build_daily_stocks()
        daily_search = self.build_daily_search_agg()
        daily_funnel = self.build_daily_funnel()
        daily_ads = self.build_daily_ads()
        econ_latest = self.build_economics_latest()
        localization = self.build_daily_localization(reg_orders, stocks)
        gp_est = self.build_daily_profit_estimate(daily_orders, econ_latest)

        df = daily_orders.copy()
        for addon in [daily_search, daily_funnel, daily_ads, localization, gp_est.drop(columns=["orders_day", "avg_finished_price_day", "avg_price_with_disc_day", "avg_spp_day"], errors="ignore")]:
            if addon is not None and not addon.empty:
                keys = [c for c in ["day", "supplier_article", "nm_id", "subject", "code"] if c in addon.columns and c in df.columns]
                if not keys:
                    keys = [c for c in ["day", "supplier_article", "subject", "code"] if c in addon.columns and c in df.columns]
                df = df.merge(addon, on=keys, how="left")
        fill_cols = [c for c in df.columns if c not in ["day", "supplier_article", "nm_id", "subject", "code"]]
        for c in fill_cols:
            if df[c].dtype.kind in "biufc":
                df[c] = df[c].fillna(0)
        return df

    def build_weekly_orders(self) -> pd.DataFrame:
        daily = self.build_daily_current()
        if daily.empty:
            return daily
        daily["week_code"] = pd.to_datetime(daily["day"]).dt.date.map(week_code_from_date)
        # Определяем доступные колонки для группировки
        group_cols = [c for c in ["week_code", "supplier_article", "nm_id", "subject", "code"] if c in daily.columns]
        # Определяем доступные метрики для агрегации
        agg_dict = {}
        metric_map = {
            "orders_day": ("orders_day", "sum"),
            "gp_est_week": ("gross_profit_day_est", "sum"),
            "finished_price_week": ("avg_finished_price_day", "mean"),
            "pwd_week": ("avg_price_with_disc_day", "mean"),
            "spp_week": ("avg_spp_day", "mean"),
            "search_freq_week": ("search_frequency", "sum"),
            "search_clicks_week": ("search_clicks", "sum"),
            "search_capture_share_week": ("search_capture_share", "mean"),
            "position_week": ("avg_position", "mean"),
            "visibility_week": ("visibility_pct", "mean"),
            "localization_week": ("localization_index", "mean"),
            "ad_clicks_week": ("ad_clicks", "sum"),
            "ad_spend_week": ("ad_spend", "sum"),
        }
        for new_col, (src_col, how) in metric_map.items():
            if src_col in daily.columns:
                agg_dict[new_col] = (src_col, how)
        if not agg_dict:
            return pd.DataFrame()
        agg = daily.groupby(group_cols, dropna=False).agg(**agg_dict).reset_index()
        return agg

    def build_monthly_daily(self) -> pd.DataFrame:
        daily = self.build_daily_current()
        if daily.empty:
            return daily
        daily["month_key"] = pd.to_datetime(daily["day"]).dt.to_period("M").astype(str)
        # Доступные колонки для агрегации
        group_cols = [c for c in ["month_key", "supplier_article", "nm_id", "subject", "code"] if c in daily.columns]
        agg_dict = {}
        metric_map = {
            "orders_mtd": ("orders_day", "sum"),
            "gp_est_mtd": ("gross_profit_day_est", "sum"),
            "avg_finished_price": ("avg_finished_price_day", "mean"),
            "avg_pwd": ("avg_price_with_disc_day", "mean"),
            "avg_spp": ("avg_spp_day", "mean"),
            "avg_localization": ("localization_index", "mean"),
            "avg_search_capture_share": ("search_capture_share", "mean"),
        }
        for new_col, (src_col, how) in metric_map.items():
            if src_col in daily.columns:
                agg_dict[new_col] = (src_col, how)
        if not agg_dict:
            return pd.DataFrame()
        agg = daily.groupby(group_cols, dropna=False).agg(**agg_dict).reset_index()
        return agg

    def build_monthly_abc(self) -> pd.DataFrame:
        df = self.data.abc.copy()
        if df.empty:
            return df
        art = find_col(df, ["Артикул продавца"])
        sub = find_col(df, ["Предмет"])
        sales = find_col(df, ["Кол-во продаж"])
        gp = find_col(df, ["Валовая прибыль"])
        net = find_col(df, ["Чистая прибыль"])
        price = find_col(df, ["Ср. цена продажи"])
        orders = find_col(df, ["Заказы"])
        promo = find_col(df, ["Продвижение"])
        commission = find_col(df, ["Комиссия"])
        logistics = find_col(df, ["Логистика"])
        acquiring = find_col(df, ["Эквайринг"])
        cost = find_col(df, ["Себестоимость"])
        tax = find_col(df, ["Налог"])
        if not art:
            return pd.DataFrame()

        df["supplier_article"] = df[art].map(clean_article)
        df["subject"] = df[sub].astype(str) if sub else ""
        df["code"] = df["supplier_article"].map(extract_code)
        df = explode_week_rows_to_months(df, "week_start", "week_end")
        if df.empty:
            return pd.DataFrame()

        numeric_sources = [sales, gp, net, orders, promo, commission, logistics, acquiring, cost, tax, price]
        for c in [x for x in numeric_sources if x]:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
        w = df["month_weight"].fillna(1.0)
        df["abc_sales_alloc"] = df[sales] * w if sales else 0.0
        df["abc_gp_alloc"] = df[gp] * w if gp else 0.0
        df["abc_net_alloc"] = df[net] * w if net else 0.0
        df["abc_orders_alloc"] = df[orders] * w if orders else 0.0
        df["abc_promo_alloc"] = df[promo] * w if promo else 0.0
        df["abc_commission_alloc"] = df[commission] * w if commission else 0.0
        df["abc_logistics_alloc"] = df[logistics] * w if logistics else 0.0
        df["abc_acquiring_alloc"] = df[acquiring] * w if acquiring else 0.0
        df["abc_cost_alloc"] = df[cost] * w if cost else 0.0
        df["abc_tax_alloc"] = df[tax] * w if tax else 0.0
        df["abc_price_weighted"] = (df[price] * df[sales] * w) if (price and sales) else 0.0

        agg = df.groupby(["month_key", "supplier_article", "subject", "code"], dropna=False).agg(
            abc_sales=("abc_sales_alloc", "sum"),
            abc_gp=("abc_gp_alloc", "sum"),
            abc_net=("abc_net_alloc", "sum"),
            abc_orders=("abc_orders_alloc", "sum"),
            abc_promo=("abc_promo_alloc", "sum"),
            abc_commission=("abc_commission_alloc", "sum"),
            abc_logistics=("abc_logistics_alloc", "sum"),
            abc_acquiring=("abc_acquiring_alloc", "sum"),
            abc_cost=("abc_cost_alloc", "sum"),
            abc_tax=("abc_tax_alloc", "sum"),
            abc_price_weighted=("abc_price_weighted", "sum"),
        ).reset_index()
        agg["abc_gp_unit"] = agg.apply(lambda r: safe_div(r["abc_gp"], r["abc_sales"]), axis=1)
        agg["abc_price"] = agg.apply(lambda r: safe_div(r["abc_price_weighted"], r["abc_sales"]), axis=1)
        agg["abc_buyout"] = agg.apply(lambda r: safe_div(r["abc_sales"], r["abc_orders"]) * 100.0, axis=1)
        return agg.drop(columns=["abc_price_weighted"], errors="ignore")

    def build_monthly_forecast(self) -> pd.DataFrame:
        monthly_abc = self.build_monthly_abc()
        monthly_daily = self.build_monthly_daily()
        if monthly_daily.empty:
            return pd.DataFrame()
        cur_month = sorted(monthly_daily["month_key"].unique())[-1]
        cur_ts = pd.Period(cur_month).to_timestamp()
        month_end = (cur_ts + pd.offsets.MonthEnd(0)).normalize()

        abc = self.data.abc.copy()
        if abc.empty:
            return pd.DataFrame()
        art = find_col(abc, ["Артикул продавца"])
        sub = find_col(abc, ["Предмет"])
        gp_col = find_col(abc, ["Валовая прибыль"])
        if not (art and gp_col):
            return pd.DataFrame()
        abc["supplier_article"] = abc[art].map(clean_article)
        abc["subject"] = abc[sub].astype(str) if sub else ""
        abc["code"] = abc["supplier_article"].map(extract_code)
        abc[gp_col] = pd.to_numeric(abc[gp_col], errors="coerce").fillna(0)
        abc = explode_week_rows_to_months(abc, "week_start", "week_end")
        abc_cur = abc[abc["month_key"] == cur_month].copy()
        if abc_cur.empty:
            return pd.DataFrame()
        abc_cur["gp_alloc"] = abc_cur[gp_col] * abc_cur["month_weight"].fillna(1.0)

        latest_week_start = abc_cur["week_start"].dropna().max()
        latest_week_end = pd.Timestamp(latest_week_start).normalize() + pd.Timedelta(days=6)
        remaining_days = max((month_end - latest_week_end).days, 0)
        fact_closed = abc_cur.groupby(["supplier_article", "subject", "code"], dropna=False)["gp_alloc"].sum().reset_index(name="fact_gp_mtd_from_abc")
        last_week = abc_cur[abc_cur["week_start"] == latest_week_start].copy()
        baseline = last_week.groupby(["supplier_article", "subject", "code"], dropna=False)["gp_alloc"].sum().reset_index(name="gp_last_week_month_part")
        baseline["gp_last_week_daily_avg"] = baseline["gp_last_week_month_part"] / 7.0
        baseline = baseline.merge(fact_closed, on=["supplier_article", "subject", "code"], how="left")
        baseline["remaining_days_after_last_closed_week"] = remaining_days
        baseline["forecast_month_gp_base"] = baseline["fact_gp_mtd_from_abc"].fillna(0) + baseline["gp_last_week_daily_avg"] * remaining_days

        daily = self.build_daily_current()
        if daily.empty:
            out = baseline.copy()
            out["month_key"] = cur_month
            out["trend_coeff"] = 1.0
            out["forecast_month_gp_adjusted"] = out["forecast_month_gp_base"]
        else:
            daily["day"] = pd.to_datetime(daily["day"])
            current_month_df = daily[daily["day"].dt.to_period("M").astype(str) == cur_month]
            last_28 = daily[daily["day"] >= (daily["day"].max() - pd.Timedelta(days=27))]
            mtd = current_month_df.groupby("supplier_article")["orders_day"].mean().reset_index(name="avg_orders_cur")
            prev = last_28.groupby("supplier_article")["orders_day"].mean().reset_index(name="avg_orders_28d")
            trend = mtd.merge(prev, on="supplier_article", how="outer")
            trend["trend_coeff"] = trend.apply(lambda r: safe_div(r["avg_orders_cur"], r["avg_orders_28d"]) if r["avg_orders_28d"] else 1.0, axis=1)
            out = baseline.merge(trend[["supplier_article", "trend_coeff"]], on="supplier_article", how="left")
            out["trend_coeff"] = out["trend_coeff"].replace([np.inf, -np.inf], np.nan).fillna(1.0)
            out["forecast_month_gp_adjusted"] = out["forecast_month_gp_base"] * out["trend_coeff"]
            out["month_key"] = cur_month

        prev_month = (pd.Period(cur_month) - 1).strftime("%Y-%m")
        prev_gp = monthly_abc[monthly_abc["month_key"] == prev_month][["supplier_article", "abc_gp"]].rename(columns={"abc_gp": "prev_month_gp"})
        out = out.merge(prev_gp, on="supplier_article", how="left")
        out["forecast_vs_prev_month_abs"] = out["forecast_month_gp_adjusted"] - out["prev_month_gp"].fillna(0)
        out["forecast_vs_prev_month_pct"] = out.apply(lambda r: pct_delta(r["forecast_month_gp_adjusted"], r["prev_month_gp"]), axis=1)
        return out

    def build_weekly_entry_sku(self) -> pd.DataFrame:
        df = self.data.entry_points_sku.copy()
        if df.empty:
            return df
        art = find_col(df, ["Артикул продавца"])
        sub = find_col(df, ["Предмет"])
        point = find_col(df, ["Точка входа"])
        section = find_col(df, ["Раздел"])
        clicks = find_col(df, ["Перешли в карточку"])
        shows = find_col(df, ["Показы"])
        ctr = find_col(df, ["CTR"])
        orders = find_col(df, ["Заказали"])
        cart = find_col(df, ["Добавили в корзину", "Добавили в корзину"])
        conv_cart = find_col(df, ["Конверсия в корзину"])
        conv_order = find_col(df, ["Конверсия в заказ"])
        df["supplier_article"] = df[art].map(clean_article)
        df["subject"] = df[sub].astype(str) if sub else ""
        df["code"] = df["supplier_article"].map(extract_code)
        df["entry_point"] = df[point].astype(str) if point else ""
        df["entry_section"] = df[section].astype(str) if section else ""
        for c in [clicks, shows, ctr, orders, cart, conv_cart, conv_order]:
            if c:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
        out = df[[
            "week_code", "week_start", "week_end", "supplier_article", "subject", "code",
            "entry_section", "entry_point",
            shows, clicks, ctr, cart, conv_cart, orders, conv_order
        ]].copy()
        out = out.rename(columns={
            shows: "entry_shows", clicks: "entry_clicks", ctr: "entry_ctr",
            cart: "entry_cart", conv_cart: "entry_conv_cart", orders: "entry_orders", conv_order: "entry_conv_order"
        })
        return out


# -------------------------
# Narrative / business logic
# -------------------------


def compose_daily_reason(row: pd.Series, target: Optional[pd.Series]) -> str:
    reasons = []
    if target is None or target.empty:
        return "Недостаточно истории для автоматического объяснения."

    def cmp_less(cur, tgt, label, threshold=0.10):
        if pd.notna(cur) and pd.notna(tgt) and tgt > 0 and cur < tgt * (1 - threshold):
            reasons.append(f"{label} ниже цели")

    def cmp_more(cur, tgt, label, threshold=0.10):
        if pd.notna(cur) and pd.notna(tgt) and tgt > 0 and cur > tgt * (1 + threshold):
            reasons.append(f"{label} выше цели")

    cmp_less(row.get("search_frequency", 0), target.get("target_search_frequency", np.nan), "спрос на площадке")
    cmp_less(row.get("search_capture_share", 0), target.get("target_search_capture_share", np.nan), "доля поискового трафика")
    cmp_more(row.get("avg_finished_price_day", 0), target.get("target_avg_finished_price_day", np.nan), "цена для покупателя")
    cmp_less(row.get("localization_index", 0), target.get("target_localization_index", np.nan), "локализация")
    cmp_less(row.get("open_card_count", 0), target.get("target_open_card_count", np.nan), "входящий трафик")
    cmp_less(row.get("ad_impressions", 0), target.get("target_ad_impressions", np.nan), "рекламный охват")
    cmp_less(row.get("conv_to_cart", 0), target.get("target_conv_to_cart", np.nan), "конверсия в корзину", 0.05)
    cmp_less(row.get("conv_cart_to_order", 0), target.get("target_conv_cart_to_order", np.nan), "конверсия в заказ", 0.05)

    if not reasons:
        if row.get("orders_day", 0) >= target.get("target_orders_day", 0):
            return "Показатель достигнут или перевыполнен. Ключевые метрики близки к сильному историческому диапазону."
        return "Отклонение есть, но ярко выраженный драйвер не выделился: вероятно, действует сочетание нескольких умеренных факторов."
    return "; ".join(reasons).capitalize() + "."


def compose_weekly_reason(weekly_row: pd.Series, abc_prev: Optional[pd.Series], abc_cur: Optional[pd.Series]) -> str:
    if abc_prev is None or abc_cur is None or abc_prev.empty or abc_cur.empty:
        return "Недостаточно ABC-истории для weekly-вывода."
    sales_prev = abc_prev.get("Кол-во продаж", np.nan)
    sales_cur = abc_cur.get("Кол-во продаж", np.nan)
    gp_prev = abc_prev.get("Валовая прибыль", np.nan)
    gp_cur = abc_cur.get("Валовая прибыль", np.nan)
    gp_unit_prev = safe_div(gp_prev, sales_prev)
    gp_unit_cur = safe_div(gp_cur, sales_cur)
    messages = []
    if pd.notna(sales_prev) and pd.notna(sales_cur):
        if sales_cur < sales_prev:
            messages.append("объём продаж ниже прошлой недели")
        elif sales_cur > sales_prev:
            messages.append("объём продаж выше прошлой недели")
    if gp_unit_cur < gp_unit_prev * 0.95:
        messages.append("прибыль на 1 продажу ухудшилась")
    elif gp_unit_cur > gp_unit_prev * 1.05:
        messages.append("прибыль на 1 продажу улучшилась")
    if pd.notna(gp_prev) and pd.notna(gp_cur):
        if gp_cur < gp_prev:
            messages.append("валовая прибыль ниже прошлой недели")
    return "; ".join(messages).capitalize() + "." if messages else "Weekly-картина нейтральна."


# -------------------------
# Report writer
# -------------------------

class ReportWriter:
    def __init__(self, out_title: str):
        self.wb = Workbook()
        self.wb.remove(self.wb.active)
        self.out_title = out_title

    def add_df_sheet(self, name: str, df: pd.DataFrame) -> Worksheet:
        ws = self.wb.create_sheet(safe_sheet_name(name))
        if df is None or df.empty:
            ws["A1"] = "Нет данных"
            return ws
        for j, col in enumerate(df.columns, start=1):
            cell = ws.cell(row=1, column=j, value=str(col))
            cell.font = Font(bold=True)
            cell.fill = HEADER_FILL
            cell.border = THIN_BORDER
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        for i, row in enumerate(df.itertuples(index=False), start=2):
            for j, val in enumerate(row, start=1):
                c = ws.cell(row=i, column=j, value=None if pd.isna(val) else val)
                c.border = THIN_BORDER
        self.auto_width(ws)
        ws.freeze_panes = "A2"
        return ws

    def auto_width(self, ws: Worksheet, max_width: int = 35) -> None:
        widths: Dict[int, int] = {}
        for row in ws.iter_rows():
            for cell in row:
                val = "" if cell.value is None else str(cell.value)
                widths[cell.column] = min(max(widths.get(cell.column, 0), len(val) + 2), max_width)
        for col_idx, width in widths.items():
            ws.column_dimensions[get_column_letter(col_idx)].width = width

    def write_title(self, ws: Worksheet, title: str, row: int = 1, end_col: int = 8) -> None:
        ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=end_col)
        c = ws.cell(row=row, column=1, value=title)
        c.font = Font(bold=True, size=14)
        c.fill = TITLE_FILL
        c.alignment = Alignment(horizontal="left", vertical="center")
        for col in range(1, end_col + 1):
            ws.cell(row=row, column=col).border = THIN_BORDER

    def write_kv_table(self, ws: Worksheet, start_row: int, data: List[Tuple[str, object, object, object]], title: str) -> int:
        self.write_title(ws, title, row=start_row, end_col=4)
        hdr = start_row + 1
        headers = ["Метрика", "Цель", "Текущее", "Отклонение"]
        for j, h in enumerate(headers, start=1):
            c = ws.cell(row=hdr, column=j, value=h)
            c.font = Font(bold=True)
            c.fill = HEADER_FILL
            c.border = THIN_BORDER
            c.alignment = Alignment(horizontal="center", vertical="center")
        r = hdr + 1
        for metric, target, current, delta in data:
            vals = [metric, target, current, delta]
            for j, val in enumerate(vals, start=1):
                c = ws.cell(row=r, column=j, value=val)
                c.border = THIN_BORDER
            r += 1
        return r

    def write_narrative_box(self, ws: Worksheet, start_row: int, start_col: int, text: str, width_cols: int = 5, height_rows: int = 18, title: str = "Вывод и рекомендации") -> None:
        ws.merge_cells(start_row=start_row, start_column=start_col, end_row=start_row, end_column=start_col + width_cols - 1)
        title_cell = ws.cell(row=start_row, column=start_col, value=title)
        title_cell.font = Font(bold=True)
        title_cell.fill = NOTE_FILL
        title_cell.border = THIN_BORDER
        title_cell.alignment = Alignment(horizontal="left", vertical="center")
        ws.merge_cells(start_row=start_row + 1, start_column=start_col, end_row=start_row + height_rows, end_column=start_col + width_cols - 1)
        body = ws.cell(row=start_row + 1, column=start_col, value=text)
        body.alignment = Alignment(wrap_text=True, vertical="top")
        body.border = THIN_BORDER
        body.fill = SUBHEADER_FILL


# -------------------------
# Main report assembly
# -------------------------

class CombinedReport:
    def __init__(self, data: LoadedData, store: str):
        self.data = data
        self.store = store
        self.mb = MetricsBuilder(data)

        log("Building derived tables")
        self.daily_current = self.mb.build_daily_current()
        self.daily_targets = self.mb.build_daily_targets(self.daily_current)
        self.weekly_orders = self.mb.build_weekly_orders()
        self.econ_latest = self.mb.build_economics_latest()
        self.monthly_daily = self.mb.build_monthly_daily()
        self.monthly_abc = self.mb.build_monthly_abc()
        self.monthly_forecast = self.mb.build_monthly_forecast()
        self.weekly_entry_sku = self.mb.build_weekly_entry_sku()

    def _current_day_rows(self) -> pd.DataFrame:
        if self.daily_current.empty:
            return self.daily_current
        last_day = pd.to_datetime(self.daily_current["day"]).max().date()
        return self.daily_current[self.daily_current["day"] == last_day].copy()

    def _target_for_row(self, row: pd.Series) -> Optional[pd.Series]:
        if self.daily_targets.empty:
            return None
        wd = pd.Timestamp(row["day"]).weekday()
        x = self.daily_targets[(self.daily_targets["supplier_article"] == row["supplier_article"]) & (self.daily_targets["weekday"] == wd)]
        return x.iloc[0] if not x.empty else None

    def build_main_report(self) -> Workbook:
        writer = ReportWriter(out_title=f"Объединенный отчет {self.store}")
        self._write_summary(writer)
        self._write_subject_and_code_sheets(writer)
        return writer.wb

    def build_log_report(self) -> Workbook:
        writer = ReportWriter(out_title=f"Лог расчетов {self.store}")
        writer.add_df_sheet("daily_current", self.daily_current)
        writer.add_df_sheet("daily_targets", self.daily_targets)
        writer.add_df_sheet("weekly_orders", self.weekly_orders)
        writer.add_df_sheet("economics_latest", self.econ_latest)
        writer.add_df_sheet("abc", self.data.abc)
        writer.add_df_sheet("entry_points_sku", self.weekly_entry_sku)
        writer.add_df_sheet("monthly_daily", self.monthly_daily)
        writer.add_df_sheet("monthly_abc", self.monthly_abc)
        writer.add_df_sheet("monthly_forecast", self.monthly_forecast)
        return writer.wb

    def _write_summary(self, writer: ReportWriter) -> None:
        ws = writer.wb.create_sheet("Сводка")
        writer.write_title(ws, f"Объединенный отчет {self.store}", row=1, end_col=8)
        ws[2][0].value = f"Дата формирования: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        current = self._current_day_rows()
        if current.empty:
            ws[4][0].value = "Нет daily-данных"
            return
        total_orders = current["orders_day"].sum()
        total_gp = current["gross_profit_day_est"].sum() if "gross_profit_day_est" in current.columns else 0
        last_day = pd.to_datetime(current["day"]).max().date()
        ws[4][1] = "Последний день"
        ws[4][2] = str(last_day)
        ws[5][1] = "Заказы, факт"
        ws[5][2] = float(total_orders)
        ws[6][1] = "Валовая прибыль, оценка"
        ws[6][2] = float(total_gp)

        # top negatives / positives by month forecast and current GP
        if not self.monthly_forecast.empty:
            cur_month = sorted(self.monthly_forecast["month_key"].unique())[-1]
            mf = self.monthly_forecast[self.monthly_forecast["month_key"] == cur_month].copy()
            top_down = mf.sort_values("forecast_vs_prev_month_abs").head(10)[["supplier_article", "subject", "forecast_vs_prev_month_abs", "forecast_vs_prev_month_pct"]]
            top_up = mf.sort_values("forecast_vs_prev_month_abs", ascending=False).head(10)[["supplier_article", "subject", "forecast_vs_prev_month_abs", "forecast_vs_prev_month_pct"]]
            writer.add_df_sheet("Топ_месяц_вниз", top_down)
            writer.add_df_sheet("Топ_месяц_вверх", top_up)

        # Show current biggest losers / winners by estimated GP vs target orders
        summary_rows = []
        for _, row in current.iterrows():
            tgt = self._target_for_row(row)
            target_orders = tgt.get("target_orders_day", np.nan) if tgt is not None else np.nan
            delta_pct = pct_delta(row.get("orders_day", 0), target_orders)
            summary_rows.append({
                "supplier_article": row["supplier_article"],
                "subject": row["subject"],
                "orders_day": row.get("orders_day", 0),
                "target_orders_day": target_orders,
                "orders_vs_target_pct": delta_pct,
                "gross_profit_day_est": row.get("gross_profit_day_est", 0),
            })
        s_df = pd.DataFrame(summary_rows)
        if not s_df.empty:
            top_today_down = s_df.sort_values("orders_vs_target_pct").head(15)
            top_today_up = s_df.sort_values("orders_vs_target_pct", ascending=False).head(15)
            writer.add_df_sheet("Топ_день_ниже_цели", top_today_down)
            writer.add_df_sheet("Топ_день_выше_цели", top_today_up)
        writer.auto_width(ws)

    def _write_subject_and_code_sheets(self, writer: ReportWriter) -> None:
        current = self._current_day_rows()
        if current.empty:
            return
        subjects = sorted([s for s in current["subject"].dropna().astype(str).unique() if s], key=lambda x: SUBJECT_ORDER.index(x) if x in SUBJECT_ORDER else 999)
        current = current.copy()
        current["code"] = current["supplier_article"].map(extract_code)

        for subject in subjects:
            sub_df = current[current["subject"] == subject].copy()
            if sub_df.empty:
                continue
            ws = writer.wb.create_sheet(safe_sheet_name(subject))
            writer.write_title(ws, f"{subject}: сводка", row=1, end_col=8)
            by_code = sub_df.groupby("code", dropna=False).agg(
                sku_count=("supplier_article", "nunique"),
                orders_day=("orders_day", "sum"),
                gross_profit_day_est=("gross_profit_day_est", "sum"),
                avg_finished_price_day=("avg_finished_price_day", "mean"),
                localization_index=("localization_index", "mean"),
                search_capture_share=("search_capture_share", "mean"),
            ).reset_index().sort_values("orders_day", ascending=False)
            writer.add_df_sheet(safe_sheet_name(subject + "_codes_tmp"), by_code)  # temp helper sheet to keep data accessible in log-like manner
            # rewrite pretty on subject sheet
            rows = []
            for _, r in by_code.iterrows():
                rows.append((r["code"], r["sku_count"], r["orders_day"], r["gross_profit_day_est"], r["avg_finished_price_day"], r["localization_index"], r["search_capture_share"]))
            headers = ["Код", "SKU", "Заказы", "Валовая прибыль, оценка", "Цена покупателя", "Локализация", "Доля трафика"]
            for j, h in enumerate(headers, start=1):
                c = ws.cell(row=3, column=j, value=h)
                c.font = Font(bold=True)
                c.fill = HEADER_FILL
                c.border = THIN_BORDER
            for i, vals in enumerate(rows, start=4):
                for j, val in enumerate(vals, start=1):
                    ws.cell(row=i, column=j, value=val).border = THIN_BORDER
            narrative = (
                f"Предмет: {subject}.\n\n"
                f"Лист показывает коды внутри предмета и их текущий срез по заказам, приблизительной валовой прибыли, цене, локализации и доле поискового трафика.\n"
                f"Ниже используйте листы по кодам для причин роста/падения и анализа по оттенкам/позициям."
            )
            writer.write_narrative_box(ws, start_row=3, start_col=10, text=narrative, width_cols=6, height_rows=16)
            writer.auto_width(ws)

            for code in by_code["code"].astype(str).tolist():
                code_df = sub_df[sub_df["code"] == code].copy()
                if code_df.empty:
                    continue
                self._write_code_sheet(writer, subject, code, code_df)

    def _write_code_sheet(self, writer: ReportWriter, subject: str, code: str, code_df: pd.DataFrame) -> None:
        ws = writer.wb.create_sheet(safe_sheet_name(code))
        title_suffix = "позиции" if is_brush_subject(subject) else "оттенки"
        writer.write_title(ws, f"{subject} / код {code} / {title_suffix}", row=1, end_col=8)

        # overall current vs target at code level
        current_row = code_df.groupby(["subject", "code"], dropna=False).agg(
            orders_day=("orders_day", "sum"),
            gross_profit_day_est=("gross_profit_day_est", "sum"),
            avg_finished_price_day=("avg_finished_price_day", "mean"),
            avg_price_with_disc_day=("avg_price_with_disc_day", "mean"),
            avg_spp_day=("avg_spp_day", "mean"),
            localization_index=("localization_index", "mean"),
            search_capture_share=("search_capture_share", "mean"),
            open_card_count=("open_card_count", "sum"),
            add_to_cart_count=("add_to_cart_count", "sum"),
            funnel_orders=("funnel_orders", "sum"),
            conv_to_cart=("conv_to_cart", "mean"),
            conv_cart_to_order=("conv_cart_to_order", "mean"),
            ad_impressions=("ad_impressions", "sum"),
            ad_clicks=("ad_clicks", "sum"),
            ad_ctr=("ad_ctr", "mean"),
        ).reset_index().iloc[0]

        # aggregate target from member SKUs
        targets = []
        for _, sku_row in code_df.iterrows():
            tgt = self._target_for_row(sku_row)
            if tgt is not None:
                targets.append(tgt)
        tgt_map = {}
        if targets:
            tgt_df = pd.DataFrame(targets)
            for c in tgt_df.columns:
                if c.startswith("target_"):
                    tgt_map[c] = pd.to_numeric(tgt_df[c], errors="coerce").mean()

        kv_data = [
            ("Заказы", tgt_map.get("target_orders_day"), current_row.get("orders_day"), pct_delta(current_row.get("orders_day"), tgt_map.get("target_orders_day"))),
            ("Валовая прибыль, оценка", tgt_map.get("target_gross_profit_day_est"), current_row.get("gross_profit_day_est"), pct_delta(current_row.get("gross_profit_day_est"), tgt_map.get("target_gross_profit_day_est"))),
            ("Цена покупателя", tgt_map.get("target_avg_finished_price_day"), current_row.get("avg_finished_price_day"), pct_delta(current_row.get("avg_finished_price_day"), tgt_map.get("target_avg_finished_price_day"))),
            ("Наша цена", tgt_map.get("target_avg_price_with_disc_day"), current_row.get("avg_price_with_disc_day"), pct_delta(current_row.get("avg_price_with_disc_day"), tgt_map.get("target_avg_price_with_disc_day"))),
            ("SPP", tgt_map.get("target_avg_spp_day"), current_row.get("avg_spp_day"), pct_delta(current_row.get("avg_spp_day"), tgt_map.get("target_avg_spp_day"))),
            ("Локализация", tgt_map.get("target_localization_index"), current_row.get("localization_index"), pct_delta(current_row.get("localization_index"), tgt_map.get("target_localization_index"))),
            ("Доля поискового трафика", tgt_map.get("target_search_capture_share"), current_row.get("search_capture_share"), pct_delta(current_row.get("search_capture_share"), tgt_map.get("target_search_capture_share"))),
            ("Открытия карточки", tgt_map.get("target_open_card_count"), current_row.get("open_card_count"), pct_delta(current_row.get("open_card_count"), tgt_map.get("target_open_card_count"))),
            ("Конверсия в корзину", tgt_map.get("target_conv_to_cart"), current_row.get("conv_to_cart"), pct_delta(current_row.get("conv_to_cart"), tgt_map.get("target_conv_to_cart"))),
            ("Конверсия в заказ", tgt_map.get("target_conv_cart_to_order"), current_row.get("conv_cart_to_order"), pct_delta(current_row.get("conv_cart_to_order"), tgt_map.get("target_conv_cart_to_order"))),
            ("Рекламный CTR", tgt_map.get("target_ad_ctr"), current_row.get("ad_ctr"), pct_delta(current_row.get("ad_ctr"), tgt_map.get("target_ad_ctr"))),
        ]
        end_row = writer.write_kv_table(ws, start_row=3, data=kv_data, title="Целевые и текущие показатели")

        # positions/shades table sorted by clicks from latest ABC or funnel opens
        latest_week = None
        if not self.data.abc.empty:
            latest_week = self.data.abc["week_start"].dropna().max()
        sku_table = code_df.copy()
        if latest_week is not None and not self.data.abc.empty:
            abc = self.data.abc.copy()
            art = find_col(abc, ["Артикул продавца"])
            clicks = find_col(abc, ["Открытие карточки"])
            orders = find_col(abc, ["Заказы"])
            netp = find_col(abc, ["Чистая прибыль"])
            gp = find_col(abc, ["Валовая прибыль"])
            if art and clicks:
                abc_latest = abc[abc["week_start"] == latest_week].copy()
                abc_latest["supplier_article"] = abc_latest[art].map(clean_article)
                for c in [clicks, orders, netp, gp]:
                    if c:
                        abc_latest[c] = pd.to_numeric(abc_latest[c], errors="coerce").fillna(0)
                keep_cols = ["supplier_article"] + [c for c in [clicks, orders, netp, gp] if c]
                abc_latest = abc_latest[keep_cols]
                rename = {}
                if clicks: rename[clicks] = "abc_clicks_week"
                if orders: rename[orders] = "abc_orders_week"
                if netp: rename[netp] = "abc_net_profit_week"
                if gp: rename[gp] = "abc_gross_profit_week"
                sku_table = sku_table.merge(abc_latest.rename(columns=rename), on="supplier_article", how="left")
        sort_col = "abc_clicks_week" if "abc_clicks_week" in sku_table.columns else "open_card_count"
        sku_table = sku_table.sort_values(sort_col, ascending=False)
        show_cols = [c for c in [
            "supplier_article", "orders_day", "gross_profit_day_est", "avg_finished_price_day",
            "avg_price_with_disc_day", "avg_spp_day", "localization_index", "search_capture_share",
            "open_card_count", "conv_to_cart", "conv_cart_to_order", "ad_ctr",
            "abc_clicks_week", "abc_orders_week", "abc_gross_profit_week", "abc_net_profit_week"
        ] if c in sku_table.columns]
        start_table = end_row + 2
        for j, col in enumerate(show_cols, start=1):
            c = ws.cell(row=start_table, column=j, value=col)
            c.font = Font(bold=True)
            c.fill = HEADER_FILL
            c.border = THIN_BORDER
        for i, row in enumerate(sku_table[show_cols].itertuples(index=False), start=start_table + 1):
            for j, val in enumerate(row, start=1):
                ws.cell(row=i, column=j, value=None if pd.isna(val) else val).border = THIN_BORDER

        # Narrative
        lead_row = code_df.sort_values("orders_day", ascending=False).iloc[0]
        tgt = self._target_for_row(lead_row)
        narrative = compose_daily_reason(lead_row, tgt)
        if latest_week is not None and not self.data.abc.empty:
            abc = self.data.abc.copy()
            art = find_col(abc, ["Артикул продавца"])
            abc["supplier_article"] = abc[art].map(clean_article) if art else ""
            abc_code = abc[abc["supplier_article"].map(extract_code) == str(code)].copy()
            weeks = sorted(abc_code["week_start"].dropna().unique())
            if len(weeks) >= 2:
                cur = abc_code[abc_code["week_start"] == weeks[-1]]
                prev = abc_code[abc_code["week_start"] == weeks[-2]]
                gp_col = find_col(abc, ["Валовая прибыль"])
                sales_col = find_col(abc, ["Кол-во продаж"])
                cur_row = pd.Series({
                    "Кол-во продаж": pd.to_numeric(cur[find_col(cur, ["Кол-во продаж"])], errors="coerce").sum() if sales_col else np.nan,
                    "Валовая прибыль": pd.to_numeric(cur[find_col(cur, ["Валовая прибыль"])], errors="coerce").sum() if gp_col else np.nan,
                })
                prev_row = pd.Series({
                    "Кол-во продаж": pd.to_numeric(prev[find_col(prev, ["Кол-во продаж"])], errors="coerce").sum() if sales_col else np.nan,
                    "Валовая прибыль": pd.to_numeric(prev[find_col(prev, ["Валовая прибыль"])], errors="coerce").sum() if gp_col else np.nan,
                })
                weekly_reason = compose_weekly_reason(pd.Series(dtype=float), prev_row, cur_row)
                narrative += "\n\nWeekly: " + weekly_reason
        writer.write_narrative_box(ws, start_row=3, start_col=8, text=narrative, width_cols=7, height_rows=20)

        # ABC weekly block
        if not self.data.abc.empty:
            abc = self.data.abc.copy()
            art = find_col(abc, ["Артикул продавца"])
            if art:
                abc["supplier_article"] = abc[art].map(clean_article)
                abc_code = abc[abc["supplier_article"].map(extract_code) == str(code)].copy()
                if not abc_code.empty:
                    group_cols = ["week_code"]
                    metrics = {}
                    for src, dst in [
                        ("Кол-во продаж", "abc_sales"),
                        ("Заказы", "abc_orders"),
                        ("Сумма продаж", "abc_sales_sum"),
                        ("Валовая прибыль", "abc_gp"),
                        ("Чистая прибыль", "abc_net"),
                        ("Продвижение", "abc_promo"),
                        ("Комиссия", "abc_commission"),
                        ("Логистика", "abc_logistics"),
                        ("Эквайринг", "abc_acquiring"),
                        ("Себестоимость", "abc_cost"),
                        ("Открытие карточки", "abc_clicks"),
                    ]:
                        col = find_col(abc_code, [src])
                        if col:
                            abc_code[col] = pd.to_numeric(abc_code[col], errors="coerce").fillna(0)
                            metrics[dst] = (col, "sum")
                    if metrics:
                        abc_sum = abc_code.groupby(group_cols, dropna=False).agg(**metrics).reset_index().sort_values("week_code")
                        st = start_table + len(sku_table) + 3
                        writer.write_title(ws, f"ABC по неделям / код {code}", row=st, end_col=min(len(abc_sum.columns), 8))
                        for j, col in enumerate(abc_sum.columns, start=1):
                            c = ws.cell(row=st + 1, column=j, value=col)
                            c.font = Font(bold=True)
                            c.fill = HEADER_FILL
                            c.border = THIN_BORDER
                        for i, row in enumerate(abc_sum.itertuples(index=False), start=st + 2):
                            for j, val in enumerate(row, start=1):
                                ws.cell(row=i, column=j, value=None if pd.isna(val) else val).border = THIN_BORDER
        writer.auto_width(ws)


# -------------------------
# Save workbook to storage
# -------------------------


def workbook_to_bytes(wb: Workbook) -> bytes:
    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio.read()


# -------------------------
# CLI
# -------------------------


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="WB combined report for TOPFACE")
    p.add_argument("--root", default=".", help="Local project root. Ignored when USE_S3=1")
    p.add_argument("--reports-root", default="Отчёты")
    p.add_argument("--store", default="TOPFACE")
    p.add_argument("--out-subdir", default="Отчёты/Объединенный отчет/TOPFACE")
    p.add_argument("--daily-only", action="store_true")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    storage = make_storage(args.root)
    loader = DataLoader(storage=storage, store=args.store, reports_root=args.reports_root)
    log("Loading data")
    entry_cat, entry_sku = loader.load_entry_points()
    data = LoadedData(
        orders=loader.load_orders(),
        stocks=loader.load_stocks(),
        search=loader.load_search(),
        funnel=loader.load_funnel(),
        ads=loader.load_ads(),
        economics=loader.load_economics(),
        abc=loader.load_abc(),
        entry_points_category=entry_cat,
        entry_points_sku=entry_sku,
    )
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
