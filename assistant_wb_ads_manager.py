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
    "CZ420", "CZ420БРОВИ", "CZ420ГЛАЗА", "DE49", "DE49ГЛАЗА", "PT901"
}

TITLE = "Валовая Прибыль-НДС"
EXAMPLE_ARTICLES = ["901/5", "901/8", "901/14", "901/18"]

THIN = Side(style="thin", color="D9E2F3")
BORDER = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)

FILL_DARK = PatternFill("solid", fgColor="1F4E78")
FILL_BLUE_1 = PatternFill("solid", fgColor="D9EAF7")
FILL_BLUE_2 = PatternFill("solid", fgColor="CFE2F3")
FILL_BLUE_3 = PatternFill("solid", fgColor="B6D7F0")
FILL_BLUE_4 = PatternFill("solid", fgColor="A9CCE3")
FILL_PRODUCT = PatternFill("solid", fgColor="EEF5FC")
FILL_ARTICLE = PatternFill("solid", fgColor="F8FBFF")
FILL_TOTAL = PatternFill("solid", fgColor="9FC5E8")

CATEGORY_FILLS = {
    "Кисти косметические": FILL_BLUE_1,
    "Помады": FILL_BLUE_2,
    "Блески": FILL_BLUE_3,
    "Косметические карандаши": FILL_BLUE_4,
}


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def normalize_text(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).replace("\xa0", " ").strip()
    return re.sub(r"\s+", " ", s)


def canonical_subject(v: Any) -> str:
    s = normalize_text(v).lower().replace("ё", "е")
    mapping = {
        "кисти косметические": "Кисти косметические",
        "помады": "Помады",
        "блески": "Блески",
        "косметические карандаши": "Косметические карандаши",
    }
    return mapping.get(s, normalize_text(v))


def clean_article(v: Any) -> str:
    s = normalize_text(v)
    if s.lower() in {"nan", "none"}:
        return ""
    return s


def upper_article(v: Any) -> str:
    return clean_article(v).upper()


def is_excluded_article(v: Any) -> bool:
    return upper_article(v) in EXCLUDE_ARTICLES


def product_root(v: Any) -> str:
    s = upper_article(v)
    if not s or is_excluded_article(s):
        return ""
    m = re.match(r"^PT(\d+)", s)
    if m:
        return m.group(1)
    m = re.match(r"^(\d+)", s)
    if m:
        return m.group(1)
    return ""


def safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if pd.isna(v):
            return default
        if isinstance(v, str):
            t = v.replace("\xa0", " ").replace("%", "").replace(",", ".").strip()
            if not t:
                return default
            return float(t)
        return float(v)
    except Exception:
        return default


def to_dt(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce").dt.normalize()


def find_col(df: pd.DataFrame, candidates: Iterable[str]) -> str:
    if df is None or df.empty:
        return ""
    by_norm = {}
    for c in df.columns:
        key = re.sub(r"[^\w]+", "", normalize_text(c).lower().replace("ё", "е"))
        by_norm[key] = c
    for cand in candidates:
        key = re.sub(r"[^\w]+", "", normalize_text(cand).lower().replace("ё", "е"))
        if key in by_norm:
            return by_norm[key]
    return ""


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


def week_code_from_date(v: Any) -> Optional[str]:
    if pd.isna(v):
        return None
    ts = pd.Timestamp(v)
    iso = ts.isocalendar()
    return f"{int(iso.year)}-W{int(iso.week):02d}"


def parse_abc_period_from_name(name: str) -> Tuple[Optional[date], Optional[date]]:
    m = re.search(r"__(\d{2})\.(\d{2})\.(\d{4})-(\d{2})\.(\d{2})\.(\d{4})__", name)
    if not m:
        return None, None
    return (
        date(int(m.group(3)), int(m.group(2)), int(m.group(1))),
        date(int(m.group(6)), int(m.group(5)), int(m.group(4))),
    )


def last_day_of_month(d: date) -> date:
    if d.month == 12:
        return date(d.year + 1, 1, 1) - timedelta(days=1)
    return date(d.year, d.month + 1, 1) - timedelta(days=1)


def month_key(v: Any) -> str:
    ts = pd.Timestamp(v)
    return ts.strftime("%Y-%m")


def russian_month(month_num: int) -> str:
    return {
        1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
        5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
        9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
    }[month_num]


def money_fmt() -> str:
    return '# ##0 "₽"'


class BaseStorage:
    def list_files(self, prefix: str) -> List[str]:
        raise NotImplementedError
    def read_bytes(self, path: str) -> bytes:
        raise NotImplementedError
    def write_bytes(self, path: str, data: bytes) -> None:
        raise NotImplementedError
    def exists(self, path: str) -> bool:
        raise NotImplementedError
    def glob_root(self, pattern: str) -> List[str]:
        return []


class LocalStorage(BaseStorage):
    def __init__(self, root: str):
        self.root = Path(root)
    def _abs(self, rel: str) -> Path:
        return self.root / rel
    def list_files(self, prefix: str) -> List[str]:
        prefix = prefix.replace("\\", "/").rstrip("/")
        base = self._abs(prefix)
        if base.is_file():
            return [prefix]
        start = base if base.exists() else base.parent
        if not start.exists():
            return []
        out = []
        for p in start.rglob("*"):
            if p.is_file():
                rel = str(p.relative_to(self.root)).replace("\\", "/")
                if rel.startswith(prefix):
                    out.append(rel)
        return sorted(set(out))
    def read_bytes(self, path: str) -> bytes:
        return self._abs(path).read_bytes()
    def write_bytes(self, path: str, data: bytes) -> None:
        p = self._abs(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
    def exists(self, path: str) -> bool:
        return self._abs(path).exists()
    def glob_root(self, pattern: str) -> List[str]:
        return sorted(str(p.relative_to(self.root)).replace("\\", "/") for p in self.root.glob(pattern) if p.is_file())


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
        out, token = [], None
        while True:
            kwargs = {"Bucket": self.bucket, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            resp = self.s3.list_objects_v2(**kwargs)
            out.extend([x["Key"] for x in resp.get("Contents", []) if not x["Key"].endswith("/")])
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        return sorted(set(out))
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
    access = os.getenv("YC_ACCESS_KEY_ID", "").strip()
    secret = os.getenv("YC_SECRET_ACCESS_KEY", "").strip()
    if bucket and access and secret:
        log("Using Yandex Object Storage (S3)")
        return S3Storage(bucket, access, secret)
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
    source_paths: pd.DataFrame


class Loader:
    def __init__(self, storage: BaseStorage, reports_root: str = "Отчёты", store: str = "TOPFACE"):
        self.storage = storage
        self.reports_root = reports_root.rstrip("/")
        self.store = store
        self.warnings: List[str] = []
        self.paths_used: List[Dict[str, Any]] = []

    def p(self, *parts: str) -> str:
        return "/".join([self.reports_root, *parts]).replace("//", "/")

    def _record_path(self, source_type: str, path: str, sheet: str = "") -> None:
        self.paths_used.append({"Источник": source_type, "Путь": path, "Лист": sheet})

    def read_excel_flexible(self, path: str, preferred_sheets: Optional[List[str]] = None, header_rows=(0,1,2)) -> pd.DataFrame:
        data = self.storage.read_bytes(path)
        xls = pd.ExcelFile(io.BytesIO(data))
        sheet = preferred_sheets[0] if preferred_sheets and preferred_sheets[0] in xls.sheet_names else (preferred_sheets and next((s for s in preferred_sheets if s in xls.sheet_names), None)) or xls.sheet_names[0]
        best, best_score = None, -10**9
        used_sheet = sheet
        for hdr in header_rows:
            try:
                df = pd.read_excel(io.BytesIO(data), sheet_name=used_sheet, header=hdr)
            except Exception:
                continue
            df = df.dropna(how="all").dropna(axis=1, how="all")
            score = len(df.columns) - (1000 if df.empty else 0)
            if score > best_score:
                best, best_score = df, score
        if best is None:
            raise ValueError(f"cannot read {path}")
        self._record_path("Excel", path, used_sheet)
        return best

    def load_orders(self) -> pd.DataFrame:
        files = self.storage.list_files(self.p("Заказы", self.store, "Недельные"))
        if not files:
            files = self.storage.list_files(self.p("Заказы", self.store))
        if not files and hasattr(self.storage, "glob_root"):
            files = self.storage.glob_root("Заказы_*.xlsx")
        out = []
        for path in files:
            try:
                df = self.read_excel_flexible(path, preferred_sheets=["Заказы"])
                df = df.rename(columns={"nmID":"nmId", "supplierArticle":"supplier_article", "date":"day", "subject":"subject"})
                if "supplier_article" not in df.columns:
                    c = find_col(df, ["Артикул продавца", "supplierArticle"])
                    if c:
                        df["supplier_article"] = df[c]
                if "nmId" not in df.columns:
                    c = find_col(df, ["Артикул WB", "nmId", "nmID"])
                    if c:
                        df["nmId"] = df[c]
                if "subject" not in df.columns:
                    c = find_col(df, ["Предмет", "subject"])
                    if c:
                        df["subject"] = df[c]
                if "day" not in df.columns:
                    c = find_col(df, ["Дата", "date"])
                    if c:
                        df["day"] = df[c]
                if "finishedPrice" not in df.columns:
                    c = find_col(df, ["finishedPrice", "Цена покупателя", "Средняя цена покупателя"])
                    if c:
                        df["finishedPrice"] = df[c]
                if "priceWithDisc" not in df.columns:
                    c = find_col(df, ["priceWithDisc", "Цена со скидкой продавца", "Средняя цена продажи"])
                    if c:
                        df["priceWithDisc"] = df[c]
                if "isCancel" not in df.columns:
                    df["isCancel"] = False
                df["day"] = to_dt(df["day"])
                df["supplier_article"] = df["supplier_article"].map(clean_article)
                df["subject"] = df["subject"].map(canonical_subject)
                df["nmId"] = pd.to_numeric(df["nmId"], errors="coerce")
                df["finishedPrice"] = pd.to_numeric(df.get("finishedPrice", np.nan), errors="coerce")
                df["priceWithDisc"] = pd.to_numeric(df.get("priceWithDisc", np.nan), errors="coerce")
                df["orders"] = 1.0
                df["product"] = df["supplier_article"].map(product_root)
                df = df[
                    df["subject"].isin(TARGET_SUBJECTS)
                    & df["supplier_article"].ne("")
                    & ~df["supplier_article"].map(is_excluded_article)
                    & df["nmId"].notna()
                    & df["day"].notna()
                ].copy()
                out.append(df[["day", "nmId", "supplier_article", "product", "subject", "orders", "finishedPrice", "priceWithDisc", "isCancel"]])
            except Exception as e:
                self.warnings.append(f"Orders read error {path}: {e}")
        df = pd.concat(out, ignore_index=True) if out else pd.DataFrame(columns=["day","nmId","supplier_article","product","subject","orders","finishedPrice","priceWithDisc","isCancel"])
        if not df.empty:
            log(f"Orders rows loaded: {len(df):,}; date range {df['day'].min().date()} .. {df['day'].max().date()}")
        else:
            log("Orders rows loaded: 0")
        return df

    def load_funnel(self) -> pd.DataFrame:
        candidates = [
            self.p("Воронка продаж", self.store, "Воронка продаж.xlsx"),
            self.p("Воронка продаж", "Воронка продаж.xlsx"),
            "Воронка продаж.xlsx",
            "Воронка продаж (1).xlsx",
        ]
        path = next((p for p in candidates if self.storage.exists(p)), None)
        if path is None and hasattr(self.storage, "glob_root"):
            gl = self.storage.glob_root("Воронка продаж*.xlsx")
            path = gl[0] if gl else None
        if not path:
            return pd.DataFrame(columns=["day","nmId","ordersCount","buyoutsCount","buyout_rate"])
        try:
            df = self.read_excel_flexible(path)
            ren = {}
            for target, candidates in {
                "day": ["Дата", "date", "dt"],
                "nmId": ["Артикул WB", "nmId", "nmID"],
                "ordersCount": ["ordersCount", "Заказы", "orders"],
                "buyoutsCount": ["buyoutsCount", "Выкупы", "Выкупы заказов"],
                "buyoutPercent": ["buyoutPercent", "Процент выкупа", "% выкупа"],
            }.items():
                c = find_col(df, candidates)
                if c:
                    ren[c] = target
            df = df.rename(columns=ren)
            df["day"] = to_dt(df["day"])
            df["nmId"] = pd.to_numeric(df["nmId"], errors="coerce")
            df["ordersCount"] = pd.to_numeric(df.get("ordersCount", np.nan), errors="coerce").fillna(0)
            df["buyoutsCount"] = pd.to_numeric(df.get("buyoutsCount", np.nan), errors="coerce").fillna(0)
            if "buyoutPercent" in df.columns:
                bp = pd.to_numeric(df["buyoutPercent"], errors="coerce")
                df["buyout_rate"] = np.where(bp > 1, bp / 100.0, bp)
            else:
                df["buyout_rate"] = np.where(df["ordersCount"] > 0, df["buyoutsCount"] / df["ordersCount"], np.nan)
            df = df[df["day"].notna() & df["nmId"].notna()].copy()
            log(f"Funnel rows loaded: {len(df):,}; date range {df['day'].min().date()} .. {df['day'].max().date()}")
            return df[["day","nmId","ordersCount","buyoutsCount","buyout_rate"]]
        except Exception as e:
            self.warnings.append(f"Funnel read error {path}: {e}")
            return pd.DataFrame(columns=["day","nmId","ordersCount","buyoutsCount","buyout_rate"])

    def load_ads(self) -> pd.DataFrame:
        files = self.storage.list_files(self.p("Реклама", self.store, "Недельные"))
        if not files:
            files = self.storage.list_files(self.p("Реклама", self.store))
        if not files and hasattr(self.storage, "glob_root"):
            files = self.storage.glob_root("Реклама_*.xlsx") + self.storage.glob_root("Анализ рекламы*.xlsx")
        out = []
        for path in files:
            try:
                data = self.storage.read_bytes(path)
                xls = pd.ExcelFile(io.BytesIO(data))
                sheet = "Статистика_Ежедневно" if "Статистика_Ежедневно" in xls.sheet_names else xls.sheet_names[0]
                df = pd.read_excel(io.BytesIO(data), sheet_name=sheet)
                self._record_path("Реклама", path, sheet)
                df = df.rename(columns={"Артикул WB":"nmId", "Дата":"day", "Расход":"spend", "Название предмета":"subject"})
                if "nmId" not in df.columns:
                    c = find_col(df, ["Артикул WB", "nmId"])
                    if c:
                        df["nmId"] = df[c]
                if "day" not in df.columns:
                    c = find_col(df, ["Дата", "date"])
                    if c:
                        df["day"] = df[c]
                if "spend" not in df.columns:
                    c = find_col(df, ["Расход", "spend"])
                    if c:
                        df["spend"] = df[c]
                if "subject" not in df.columns:
                    c = find_col(df, ["Название предмета", "Предмет", "subject"])
                    if c:
                        df["subject"] = df[c]
                df["day"] = to_dt(df["day"])
                df["nmId"] = pd.to_numeric(df["nmId"], errors="coerce")
                df["spend"] = pd.to_numeric(df["spend"], errors="coerce").fillna(0.0)
                df["subject"] = df["subject"].map(canonical_subject)
                df = df[df["day"].notna() & df["nmId"].notna()].copy()
                out.append(df[["day","nmId","subject","spend"]])
            except Exception as e:
                self.warnings.append(f"Ads read error {path}: {e}")
        df = pd.concat(out, ignore_index=True) if out else pd.DataFrame(columns=["day","nmId","subject","spend"])
        if not df.empty:
            log(f"Ads rows loaded: {len(df):,}; date range {df['day'].min().date()} .. {df['day'].max().date()}; spend sum {df['spend'].sum():,.2f}")
        else:
            log("Ads rows loaded: 0")
        return df

    def load_economics(self) -> pd.DataFrame:
        candidates = [
            self.p("Финансовые показатели", self.store, "Экономика.xlsx"),
            self.p("Финансовые показатели", self.store, "Недельные", "Экономика.xlsx"),
            "Экономика (4).xlsx",
            "Экономика.xlsx",
        ]
        path = next((p for p in candidates if self.storage.exists(p)), None)
        if path is None and hasattr(self.storage, "glob_root"):
            gl = sorted(self.storage.glob_root("Экономика*.xlsx"))
            path = gl[0] if gl else None
        if not path:
            return pd.DataFrame(columns=["week","nmId","supplier_article","product","subject"])
        try:
            df = self.read_excel_flexible(path, preferred_sheets=["Юнит экономика"])
            df = df.rename(columns={
                "Неделя": "week",
                "Артикул WB": "nmId",
                "Артикул продавца": "supplier_article",
                "Предмет": "subject",
                "Комиссия WB, %": "commission_pct",
                "Эквайринг, %": "acquiring_pct",
                "Логистика прямая, руб/ед": "logistics_direct_unit",
                "Логистика обратная, руб/ед": "logistics_return_unit",
                "Хранение, руб/ед": "storage_unit",
                "Прочие расходы, руб/ед": "other_unit",
                "Себестоимость, руб": "cost_unit",
                "Средняя цена продажи": "econ_priceWithDisc",
                "Средняя цена покупателя": "econ_finishedPrice",
            })
            need = {
                "week": ["Неделя", "week"],
                "nmId": ["Артикул WB", "nmId"],
                "supplier_article": ["Артикул продавца", "supplierArticle"],
                "subject": ["Предмет", "subject"],
                "commission_pct": ["Комиссия WB, %"],
                "acquiring_pct": ["Эквайринг, %"],
                "logistics_direct_unit": ["Логистика прямая, руб/ед"],
                "logistics_return_unit": ["Логистика обратная, руб/ед"],
                "storage_unit": ["Хранение, руб/ед"],
                "other_unit": ["Прочие расходы, руб/ед"],
                "cost_unit": ["Себестоимость, руб"],
                "econ_priceWithDisc": ["Средняя цена продажи"],
                "econ_finishedPrice": ["Средняя цена покупателя"],
            }
            for k, cand in need.items():
                if k not in df.columns:
                    c = find_col(df, cand)
                    if c:
                        df[k] = df[c]
            for c in ["commission_pct","acquiring_pct","logistics_direct_unit","logistics_return_unit","storage_unit","other_unit","cost_unit","econ_priceWithDisc","econ_finishedPrice"]:
                if c not in df.columns:
                    df[c] = np.nan
                df[c] = pd.to_numeric(df[c], errors="coerce")
            df["week"] = df["week"].astype(str).str.strip()
            df["nmId"] = pd.to_numeric(df["nmId"], errors="coerce")
            df["supplier_article"] = df["supplier_article"].map(clean_article)
            df["subject"] = df["subject"].map(canonical_subject)
            df["product"] = df["supplier_article"].map(product_root)
            df = df[
                df["subject"].isin(TARGET_SUBJECTS)
                & df["supplier_article"].ne("")
                & ~df["supplier_article"].map(is_excluded_article)
                & df["nmId"].notna()
            ].copy()
            uniq_weeks = sorted(df["week"].dropna().unique().tolist())
            log(f"Economics rows loaded: {len(df):,}; weeks {', '.join(uniq_weeks[:10])}")
            return df[["week","nmId","supplier_article","product","subject","commission_pct","acquiring_pct","logistics_direct_unit","logistics_return_unit","storage_unit","other_unit","cost_unit","econ_priceWithDisc","econ_finishedPrice"]]
        except Exception as e:
            self.warnings.append(f"Economics read error {path}: {e}")
            return pd.DataFrame(columns=["week","nmId","supplier_article","product","subject"])

    def load_abc(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        files = self.storage.list_files(self.p("ABC"))
        if not files and hasattr(self.storage, "glob_root"):
            files = self.storage.glob_root("wb_abc_report_goods__*.xlsx")
        files = [f for f in files if "wb_abc_report_goods__" in Path(f).name]
        weekly, monthly = [], []
        for path in files:
            try:
                df = self.read_excel_flexible(path)
                start, end = parse_abc_period_from_name(Path(path).name)
                if not start or not end:
                    continue
                ren = {
                    "Артикул WB":"nmId",
                    "Артикул продавца":"supplier_article",
                    "Предмет":"subject",
                    "Валовая прибыль":"gross_profit",
                    "Валовая выручка":"gross_revenue",
                    "Заказы":"orders",
                    "Кол-во продаж":"sales_count",
                }
                df = df.rename(columns=ren)
                for k, cand in {
                    "nmId": ["Артикул WB", "nmId"],
                    "supplier_article": ["Артикул продавца", "supplierArticle"],
                    "subject": ["Предмет", "subject"],
                    "gross_profit": ["Валовая прибыль", "gross_profit"],
                    "gross_revenue": ["Валовая выручка", "gross_revenue"],
                    "orders": ["Заказы", "orders"],
                }.items():
                    if k not in df.columns:
                        c = find_col(df, cand)
                        if c:
                            df[k] = df[c]
                df["nmId"] = pd.to_numeric(df["nmId"], errors="coerce")
                df["supplier_article"] = df["supplier_article"].map(clean_article)
                df["subject"] = df["subject"].map(canonical_subject)
                df["product"] = df["supplier_article"].map(product_root)
                df["gross_profit"] = pd.to_numeric(df.get("gross_profit", np.nan), errors="coerce").fillna(0.0)
                df["gross_revenue"] = pd.to_numeric(df.get("gross_revenue", np.nan), errors="coerce").fillna(0.0)
                df["orders"] = pd.to_numeric(df.get("orders", np.nan), errors="coerce").fillna(0.0)
                df["vat"] = df["gross_revenue"] * 7.0 / 107.0
                df["gp_minus_nds"] = df["gross_profit"] - df["vat"]
                df["week_code"] = week_code_from_date(start)
                df["week_start"] = pd.Timestamp(start)
                df["week_end"] = pd.Timestamp(end)
                df["month_key"] = pd.Timestamp(start).strftime("%Y-%m")
                df = df[
                    df["subject"].isin(TARGET_SUBJECTS)
                    & df["supplier_article"].ne("")
                    & ~df["supplier_article"].map(is_excluded_article)
                    & df["nmId"].notna()
                ].copy()
                if start.day == 1 and end == last_day_of_month(start):
                    monthly.append(df[["month_key","nmId","supplier_article","product","subject","gross_profit","gross_revenue","vat","gp_minus_nds","orders"]])
                else:
                    weekly.append(df[["week_code","week_start","week_end","month_key","nmId","supplier_article","product","subject","gross_profit","gross_revenue","vat","gp_minus_nds","orders"]])
            except Exception as e:
                self.warnings.append(f"ABC read error {path}: {e}")
        w = pd.concat(weekly, ignore_index=True) if weekly else pd.DataFrame(columns=["week_code","week_start","week_end","month_key","nmId","supplier_article","product","subject","gross_profit","gross_revenue","vat","gp_minus_nds","orders"])
        m = pd.concat(monthly, ignore_index=True) if monthly else pd.DataFrame(columns=["month_key","nmId","supplier_article","product","subject","gross_profit","gross_revenue","vat","gp_minus_nds","orders"])
        if not w.empty:
            log(f"ABC weekly rows loaded: {len(w):,}; weeks {', '.join(sorted(w['week_code'].dropna().unique().tolist()))}")
        else:
            log("ABC weekly rows loaded: 0")
        if not m.empty:
            log(f"ABC monthly rows loaded: {len(m):,}; months {', '.join(sorted(m['month_key'].dropna().unique().tolist()))}")
        else:
            log("ABC monthly rows loaded: 0")
        return w, m

    def load_plan(self, current_month_key: str) -> pd.DataFrame:
        candidates = [self.p("Объединенный отчет", self.store, "План.xlsx"), "План.xlsx"]
        path = next((p for p in candidates if self.storage.exists(p)), None)
        if not path:
            return pd.DataFrame(columns=["supplier_article","product","subject","plan_month"])
        try:
            data = self.storage.read_bytes(path)
            xls = pd.ExcelFile(io.BytesIO(data))
            sheet = next((s for s in xls.sheet_names if "итог" in s.lower()), xls.sheet_names[0])
            best, best_score = None, -10**9
            for hdr in [0,1,2,3]:
                try:
                    df = pd.read_excel(io.BytesIO(data), sheet_name=sheet, header=hdr)
                except Exception:
                    continue
                df = df.dropna(how="all").dropna(axis=1, how="all")
                score = len(df.columns) - (1000 if df.empty else 0)
                if score > best_score:
                    best, best_score = df, score
            df = best
            self._record_path("План", path, sheet)
            ren = {}
            sa = find_col(df, ["Артикул продавца", "supplierArticle"])
            subj = find_col(df, ["Предмет", "subject"])
            if sa: ren[sa] = "supplier_article"
            if subj: ren[subj] = "subject"
            df = df.rename(columns=ren)
            target = f"ВП-НДС {russian_month(int(current_month_key[-2:]))} {current_month_key[:4]}"
            col = next((c for c in df.columns if normalize_text(c) == target), None)
            if col is None:
                col = next((c for c in df.columns if current_month_key[:4] in normalize_text(c) and russian_month(int(current_month_key[-2:])) in normalize_text(c)), None)
            if col is None:
                self.warnings.append(f"Plan target column not found for {current_month_key}")
                return pd.DataFrame(columns=["supplier_article","product","subject","plan_month"])
            df["supplier_article"] = df["supplier_article"].map(clean_article)
            df["subject"] = df["subject"].map(canonical_subject)
            df["product"] = df["supplier_article"].map(product_root)
            df["plan_month"] = pd.to_numeric(df[col], errors="coerce")
            df = df[
                df["subject"].isin(TARGET_SUBJECTS)
                & df["supplier_article"].ne("")
                & ~df["supplier_article"].map(is_excluded_article)
            ].copy()
            log(f"Plan rows loaded: {len(df):,}; non-null plan {df['plan_month'].notna().sum():,}")
            return df[["supplier_article","product","subject","plan_month"]]
        except Exception as e:
            self.warnings.append(f"Plan read error {path}: {e}")
            return pd.DataFrame(columns=["supplier_article","product","subject","plan_month"])

    def load_all(self) -> LoadedData:
        log("Loading data")
        log("Loading orders")
        orders = self.load_orders()
        log("Loading funnel")
        funnel = self.load_funnel()
        log("Loading ads")
        ads = self.load_ads()
        log("Loading economics")
        econ = self.load_economics()
        log("Loading ABC")
        abc_w, abc_m = self.load_abc()
        latest_candidates = []
        for df, col in [(orders, "day"), (ads, "day"), (funnel, "day")]:
            if not df.empty:
                latest_candidates.append(pd.to_datetime(df[col], errors="coerce").max())
        latest_day = max([x for x in latest_candidates if pd.notna(x)], default=pd.Timestamp.today().normalize())
        current_month = latest_day.strftime("%Y-%m")
        log("Loading plan")
        plan = self.load_plan(current_month)
        return LoadedData(
            orders=orders, funnel=funnel, ads_daily=ads, economics=econ,
            abc_weekly=abc_w, abc_monthly=abc_m, plan=plan,
            latest_day=pd.Timestamp(latest_day).normalize(),
            warnings=self.warnings,
            source_paths=pd.DataFrame(self.paths_used),
        )


class Builder:
    def __init__(self, data: LoadedData):
        self.data = data
        self.latest_day = pd.Timestamp(data.latest_day).normalize()
        self.week_start = self.latest_day - pd.Timedelta(days=self.latest_day.weekday())
        self.week_days = [self.week_start + pd.Timedelta(days=i) for i in range(7)]
        self.current_month = self.latest_day.strftime("%Y-%m")
        self.days_in_month = calendar.monthrange(self.latest_day.year, self.latest_day.month)[1]
        self.dictionary = self.build_dictionary()
        self.buyout90 = self.build_buyout90()
        self.ads_by_day_nm = self.build_ads_by_day_nm()
        self.subject_week_commission_pct, self.subject_latest_commission_pct = self.build_commission_maps()

    def build_dictionary(self) -> pd.DataFrame:
        parts = []
        for df in [self.data.orders, self.data.economics, self.data.abc_weekly, self.data.abc_monthly]:
            if df is not None and not df.empty:
                cols = [c for c in ["subject","product","supplier_article","nmId"] if c in df.columns]
                parts.append(df[cols].copy())
        if self.data.ads_daily is not None and not self.data.ads_daily.empty:
            parts.append(self.data.ads_daily[["subject","nmId"]].copy())
        if not parts:
            return pd.DataFrame(columns=["subject","product","supplier_article","nmId"])
        m = pd.concat(parts, ignore_index=True, sort=False)
        m["subject"] = m.get("subject", "").map(canonical_subject)
        if "supplier_article" in m.columns:
            m["supplier_article"] = m["supplier_article"].map(clean_article)
        else:
            m["supplier_article"] = ""
        if "product" not in m.columns:
            m["product"] = m["supplier_article"].map(product_root)
        m["product"] = m["product"].fillna("").astype(str)
        m["nmId"] = pd.to_numeric(m.get("nmId", np.nan), errors="coerce")
        best = m.sort_values(["subject","product","supplier_article"]).drop_duplicates(subset=["nmId","supplier_article"], keep="first")
        by_nm = best.dropna(subset=["nmId"]).sort_values(by=["supplier_article","product","subject"], ascending=[False,False,False]).drop_duplicates(subset=["nmId"], keep="first")[["nmId","supplier_article","product","subject"]]
        art = best[best["supplier_article"].ne("") & best["subject"].isin(TARGET_SUBJECTS)].copy()
        art = art.merge(by_nm, on="nmId", how="left", suffixes=("","_nm"))
        for c in ["supplier_article","product","subject"]:
            art[c] = art[c].where(art[c].astype(str).str.strip().ne(""), art[f"{c}_nm"])
        art["product"] = art["product"].where(art["product"].astype(str).str.strip().ne(""), art["supplier_article"].map(product_root))
        art = art[["subject","product","supplier_article","nmId"]].dropna(subset=["nmId"]).drop_duplicates()
        art = art[art["subject"].isin(TARGET_SUBJECTS)].copy()
        art = art[~art["supplier_article"].map(is_excluded_article)].copy()
        art["nmId"] = art["nmId"].astype("int64")
        return art.sort_values(["subject","product","supplier_article","nmId"]).reset_index(drop=True)

    def build_buyout90(self) -> pd.DataFrame:
        f = self.data.funnel.copy()
        if f.empty:
            return pd.DataFrame(columns=["nmId","buyout_rate_90"])
        start = self.latest_day - pd.Timedelta(days=89)
        f = f[(f["day"] >= start) & (f["day"] <= self.latest_day)].copy()
        out = f.groupby("nmId", as_index=False).agg(orders_90=("ordersCount","sum"), buyouts_90=("buyoutsCount","sum"), buyout_pct_src=("buyout_rate","median"))
        out["buyout_rate_90"] = np.where(out["orders_90"] > 0, out["buyouts_90"] / out["orders_90"], out["buyout_pct_src"])
        out["buyout_rate_90"] = out["buyout_rate_90"].fillna(0.85).clip(lower=0, upper=1)
        log(f"Buyout90 rows: {len(out):,}; non-null ratios {out['buyout_rate_90'].notna().sum():,}")
        return out[["nmId","buyout_rate_90"]]

    def build_ads_by_day_nm(self) -> pd.DataFrame:
        ads = self.data.ads_daily.copy()
        if ads.empty:
            return pd.DataFrame(columns=["day","nmId","ad_spend"])
        out = ads.groupby(["day","nmId"], as_index=False).agg(ad_spend=("spend","sum"))
        return out

    def build_commission_maps(self) -> Tuple[Dict[Tuple[str, str], float], Dict[str, float]]:
        econ = self.data.economics.copy()
        if econ.empty:
            return {}, {}
        econ = econ[econ["commission_pct"].fillna(0) > 0].copy()
        if econ.empty:
            return {}, {}
        by_week = econ.groupby(["subject","week"], as_index=False)["commission_pct"].median()
        sw = {(r["subject"], str(r["week"])): float(r["commission_pct"]) for _, r in by_week.iterrows()}
        econ["_week_sort"] = econ["week"].map(lambda x: int(str(x).replace("-W","")) if re.match(r"^\d{4}-W\d{2}$", str(x)) else 0)
        latest = econ.sort_values("_week_sort").groupby("subject", as_index=False)["commission_pct"].last()
        sl = {r["subject"]: float(r["commission_pct"]) for _, r in latest.iterrows()}
        return sw, sl

    def pick_econ_for_day(self, day: pd.Timestamp, supplier_article: str, nm_id: int, subject: str) -> Dict[str, Any]:
        econ = self.data.economics
        if econ.empty:
            return {}
        week = week_code_from_date(day)
        subset = econ[(econ["supplier_article"] == supplier_article) & (econ["week"].astype(str) == str(week))]
        exact = not subset.empty
        if subset.empty and pd.notna(nm_id):
            subset = econ[(econ["nmId"] == nm_id) & (econ["week"].astype(str) == str(week))]
            exact = not subset.empty
        if subset.empty:
            subset = econ[econ["supplier_article"] == supplier_article]
        if subset.empty and pd.notna(nm_id):
            subset = econ[econ["nmId"] == nm_id]
        if subset.empty:
            subset = econ[econ["subject"] == subject]
        if subset.empty:
            return {
                "econ_week_used": "",
                "commission_pct_used": self.subject_latest_commission_pct.get(subject, np.nan),
                "acquiring_pct": np.nan,
                "logistics_direct_unit": np.nan,
                "logistics_return_unit": np.nan,
                "storage_unit": np.nan,
                "other_unit": np.nan,
                "cost_unit": np.nan,
                "econ_priceWithDisc": np.nan,
                "econ_finishedPrice": np.nan,
                "econ_exact_week": False,
                "econ_source": "none",
            }
        subset = subset.copy()
        subset["_sort"] = subset["week"].map(lambda x: int(str(x).replace("-W","")) if re.match(r"^\d{4}-W\d{2}$", str(x)) else 0)
        row = subset.sort_values("_sort").iloc[-1]
        commission_pct = safe_float(row.get("commission_pct"), np.nan)
        if not commission_pct:
            commission_pct = self.subject_week_commission_pct.get((subject, str(week)), np.nan)
        if not commission_pct:
            commission_pct = self.subject_latest_commission_pct.get(subject, np.nan)
        return {
            "econ_week_used": row.get("week", ""),
            "commission_pct_used": commission_pct,
            "acquiring_pct": row.get("acquiring_pct", np.nan),
            "logistics_direct_unit": row.get("logistics_direct_unit", np.nan),
            "logistics_return_unit": row.get("logistics_return_unit", np.nan),
            "storage_unit": row.get("storage_unit", np.nan),
            "other_unit": row.get("other_unit", np.nan),
            "cost_unit": row.get("cost_unit", np.nan),
            "econ_priceWithDisc": row.get("econ_priceWithDisc", np.nan),
            "econ_finishedPrice": row.get("econ_finishedPrice", np.nan),
            "econ_exact_week": exact,
            "econ_source": "article" if row.get("supplier_article","") == supplier_article else ("nm" if pd.notna(nm_id) and safe_float(row.get("nmId"), -1) == safe_float(nm_id,-2) else "subject"),
        }

    def build_daily_article_calc(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        orders = self.data.orders.copy()
        if orders.empty:
            return pd.DataFrame(), pd.DataFrame()
        orders = orders[(orders["day"] >= self.week_start) & (orders["day"] <= self.latest_day) & (~orders["isCancel"].fillna(False))].copy()
        if orders.empty:
            return pd.DataFrame(), pd.DataFrame()
        log(f"Current week order rows: {len(orders):,}; day range {orders['day'].min().date()} .. {orders['day'].max().date()}")
        g = orders.groupby(["day","nmId","supplier_article","product","subject"], as_index=False).agg(
            orders_day=("orders","sum"),
            finishedPrice=("finishedPrice", lambda s: weighted_mean(s, orders.loc[s.index, "orders"])),
            priceWithDisc=("priceWithDisc", lambda s: weighted_mean(s, orders.loc[s.index, "orders"])),
        )
        g = g.merge(self.buyout90, on="nmId", how="left")
        g = g.merge(self.ads_by_day_nm, on=["day","nmId"], how="left")
        g["ad_spend"] = g["ad_spend"].fillna(0.0)

        econ_rows = []
        for _, r in g[["day","supplier_article","nmId","subject"]].drop_duplicates().iterrows():
            er = self.pick_econ_for_day(r["day"], r["supplier_article"], int(r["nmId"]), r["subject"])
            er.update({"day": r["day"], "supplier_article": r["supplier_article"], "nmId": r["nmId"]})
            econ_rows.append(er)
        econ_pick = pd.DataFrame(econ_rows)
        if econ_pick.empty:
            econ_pick = pd.DataFrame(columns=["day","supplier_article","nmId"])
        g = g.merge(econ_pick, on=["day","supplier_article","nmId"], how="left")

        g["buyout_rate_90"] = g["buyout_rate_90"].fillna(0.85).clip(lower=0, upper=1)
        g["buyout_qty"] = g["orders_day"] * g["buyout_rate_90"]
        g["price_pwd_use"] = g["priceWithDisc"].fillna(g["econ_priceWithDisc"]).fillna(0.0)
        g["price_finished_use"] = g["finishedPrice"].fillna(g["econ_finishedPrice"]).fillna(0.0)
        g["revenue_pwd"] = g["buyout_qty"] * g["price_pwd_use"]
        g["commission_pct_used"] = pd.to_numeric(g["commission_pct_used"], errors="coerce").fillna(0.0)
        g["acquiring_pct"] = pd.to_numeric(g["acquiring_pct"], errors="coerce").fillna(0.0)
        for c in ["logistics_direct_unit","logistics_return_unit","storage_unit","other_unit","cost_unit"]:
            g[c] = pd.to_numeric(g[c], errors="coerce").fillna(0.0)

        g["commission_rub"] = g["revenue_pwd"] * g["commission_pct_used"] / 100.0
        g["acquiring_rub"] = g["revenue_pwd"] * g["acquiring_pct"] / 100.0
        g["logistics_direct_rub"] = g["buyout_qty"] * g["logistics_direct_unit"]
        g["logistics_return_rub"] = g["buyout_qty"] * g["logistics_return_unit"]
        g["storage_rub"] = g["buyout_qty"] * g["storage_unit"]
        g["other_rub"] = g["buyout_qty"] * g["other_unit"]
        g["cost_rub"] = g["buyout_qty"] * g["cost_unit"]
        g["vat_rub"] = g["buyout_qty"] * g["price_finished_use"] * 7.0 / 107.0
        g["gp_minus_nds"] = (
            g["revenue_pwd"]
            - g["commission_rub"]
            - g["acquiring_rub"]
            - g["logistics_direct_rub"]
            - g["logistics_return_rub"]
            - g["storage_rub"]
            - g["other_rub"]
            - g["cost_rub"]
            - g["ad_spend"]
            - g["vat_rub"]
        )

        exact = int(pd.to_numeric(g.get("econ_exact_week"), errors="coerce").fillna(False).astype(bool).sum())
        log(f"Economics matching: exact week = {exact}, fallback latest = {len(g)-exact}, missing = {int(g['econ_week_used'].isna().sum())}")
        log(f"Ads matching to daily rows: matched rows = {int((g['ad_spend'] > 0).sum())} из {len(g)}; spend matched = {g['ad_spend'].sum():,.2f}")
        log(f"Commission diagnostics: zero/empty commission_pct rows = {int((g['commission_pct_used'].fillna(0)==0).sum())} из {len(g)}")
        return g, g.copy()

    def current_month_plan_maps(self) -> Tuple[Dict[str,float], Dict[Tuple[str,str],float], Dict[str,float]]:
        p = self.data.plan.copy()
        if p.empty:
            return {}, {}, {}
        art = p.groupby("supplier_article", as_index=False)["plan_month"].sum()
        prod = p.groupby(["subject","product"], as_index=False)["plan_month"].sum()
        cat = p.groupby("subject", as_index=False)["plan_month"].sum()
        return (
            dict(zip(art["supplier_article"], art["plan_month"])),
            {(r["subject"], r["product"]): float(r["plan_month"]) for _, r in prod.iterrows()},
            dict(zip(cat["subject"], cat["plan_month"])),
        )

    def current_month_fact_maps(self, daily_article: pd.DataFrame, weekly_abc: pd.DataFrame, monthly_abc: pd.DataFrame) -> Tuple[Dict[str,float], Dict[Tuple[str,str],float], Dict[str,float]]:
        frames = []
        if not weekly_abc.empty:
            frames.append(weekly_abc[weekly_abc["month_key"] == self.current_month].copy())
        if not daily_article.empty:
            d = daily_article.copy()
            d["month_key"] = d["day"].map(month_key)
            frames.append(d[d["month_key"] == self.current_month][["supplier_article","product","subject","gp_minus_nds"]].copy())
        if not frames:
            return {}, {}, {}
        m = pd.concat(frames, ignore_index=True, sort=False)
        art = m.groupby("supplier_article", as_index=False)["gp_minus_nds"].sum()
        prod = m.groupby(["subject","product"], as_index=False)["gp_minus_nds"].sum()
        cat = m.groupby("subject", as_index=False)["gp_minus_nds"].sum()
        return (
            dict(zip(art["supplier_article"], art["gp_minus_nds"])),
            {(r["subject"], r["product"]): float(r["gp_minus_nds"]) for _, r in prod.iterrows()},
            dict(zip(cat["subject"], cat["gp_minus_nds"])),
        )

    def prepare_weekly_and_monthly(self, daily_article: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        weekly = self.data.abc_weekly.copy()
        monthly = self.data.abc_monthly.copy()
        current_month_in_monthly = set(monthly["month_key"].astype(str).unique()) if not monthly.empty else set()
        if self.current_month not in current_month_in_monthly:
            parts = []
            if not weekly.empty:
                parts.append(weekly[weekly["month_key"] == self.current_month][["supplier_article","product","subject","gp_minus_nds"]].copy())
            if not daily_article.empty:
                d = daily_article.copy()
                d["month_key"] = d["day"].map(month_key)
                parts.append(d[d["month_key"] == self.current_month][["supplier_article","product","subject","gp_minus_nds"]].copy())
            if parts:
                curm = pd.concat(parts, ignore_index=True)
                curm = curm.groupby(["supplier_article","product","subject"], as_index=False)["gp_minus_nds"].sum()
                curm["month_key"] = self.current_month
                curm["gross_profit"] = np.nan
                curm["gross_revenue"] = np.nan
                curm["vat"] = np.nan
                if monthly.empty:
                    monthly = curm[["month_key","supplier_article","product","subject","gp_minus_nds","gross_profit","gross_revenue","vat"]].copy()
                else:
                    monthly = pd.concat([monthly, curm[monthly.columns.intersection(curm.columns)]], ignore_index=True, sort=False)
        return weekly, monthly

    def block_hierarchy(self, base: pd.DataFrame, label_col: str, labels: List[str], plan_mode: str, current_month_fact_maps: Tuple[Dict[str,float], Dict[Tuple[str,str],float], Dict[str,float]]) -> pd.DataFrame:
        if base.empty:
            return pd.DataFrame()
        art_plan, prod_plan, cat_plan = self.current_month_plan_maps()
        art_fact, prod_fact, cat_fact = current_month_fact_maps
        rows = []
        for subj in TARGET_SUBJECTS:
            sg = base[base["subject"] == subj].copy()
            if sg.empty:
                continue
            row = {"Категория": subj, "_kind": "category", "_subject": subj}
            for lb in labels:
                row[lb] = float(sg.loc[sg[label_col] == lb, "gp_minus_nds"].sum())
            plan_val = cat_plan.get(subj, np.nan)
            if pd.isna(plan_val):
                plan_val = cat_fact.get(subj, 0.0)
            if plan_mode == "daily":
                plan_val = float(plan_val) / self.days_in_month
            row["План"] = float(plan_val)
            rows.append(row)

            product_order = sg.groupby("product", as_index=False)["gp_minus_nds"].sum().sort_values("gp_minus_nds", ascending=False)["product"].tolist()
            for prod in product_order:
                pg = sg[sg["product"] == prod].copy()
                prow = {"Категория": str(prod), "_kind": "product", "_subject": subj, "_product": prod}
                for lb in labels:
                    prow[lb] = float(pg.loc[pg[label_col] == lb, "gp_minus_nds"].sum())
                pplan = prod_plan.get((subj, prod), np.nan)
                if pd.isna(pplan):
                    pplan = prod_fact.get((subj, prod), 0.0)
                if plan_mode == "daily":
                    pplan = float(pplan) / self.days_in_month
                prow["План"] = float(pplan)
                rows.append(prow)

                article_order = pg.groupby("supplier_article", as_index=False)["gp_minus_nds"].sum().sort_values("gp_minus_nds", ascending=False)["supplier_article"].tolist()
                for art in article_order:
                    ag = pg[pg["supplier_article"] == art].copy()
                    arow = {"Категория": art, "_kind": "article", "_subject": subj, "_product": prod, "_article": art}
                    for lb in labels:
                        arow[lb] = float(ag.loc[ag[label_col] == lb, "gp_minus_nds"].sum())
                    aplan = art_plan.get(art, np.nan)
                    if pd.isna(aplan):
                        aplan = art_fact.get(art, 0.0)
                    if plan_mode == "daily":
                        aplan = float(aplan) / self.days_in_month
                    arow["План"] = float(aplan)
                    rows.append(arow)

        total = {"Категория": "Итого по всем 4 категориям", "_kind": "grand_total"}
        for lb in labels:
            total[lb] = float(base.loc[base[label_col] == lb, "gp_minus_nds"].sum())
        grand_plan = sum(v for v in self.current_month_plan_maps()[2].values() if pd.notna(v))
        if not grand_plan:
            grand_plan = sum(current_month_fact_maps[2].values()) if current_month_fact_maps[2] else 0.0
        if plan_mode == "daily":
            grand_plan = float(grand_plan) / self.days_in_month
        total["План"] = float(grand_plan)
        rows.append(total)
        return pd.DataFrame(rows)

    def build(self) -> Dict[str, pd.DataFrame]:
        daily_article, tech_daily = self.build_daily_article_calc()
        weekly_abc, monthly_abc = self.prepare_weekly_and_monthly(daily_article)
        day_labels = [f"{['Пн','Вт','Ср','Чт','Пт','Сб','Вс'][i]} {d.strftime('%d.%m')}" for i, d in enumerate(self.week_days)]
        current_fact_maps = self.current_month_fact_maps(daily_article, weekly_abc, monthly_abc)
        current_week = pd.DataFrame()
        if not daily_article.empty:
            d = daily_article.copy()
            d["label"] = d["day"].map(lambda x: f"{['Пн','Вт','Ср','Чт','Пт','Сб','Вс'][pd.Timestamp(x).weekday()]} {pd.Timestamp(x).strftime('%d.%m')}")
            current_week = self.block_hierarchy(d, "label", day_labels, "daily", current_fact_maps)

        completed_weeks = []
        if not weekly_abc.empty:
            current_week_code = week_code_from_date(self.latest_day)
            completed_weeks = [w for w in sorted(weekly_abc["week_code"].astype(str).unique().tolist()) if w < str(current_week_code)]
            completed_weeks = completed_weeks[-4:]
        previous_weeks = pd.DataFrame()
        if completed_weeks:
            w = weekly_abc[weekly_abc["week_code"].astype(str).isin(completed_weeks)].copy()
            w["label"] = w["week_code"].astype(str)
            previous_weeks = self.block_hierarchy(w, "label", completed_weeks, "weekly", current_fact_maps)

        period = self.latest_day.to_period("M")
        months = [(period - 2).strftime("%Y-%m"), (period - 1).strftime("%Y-%m"), period.strftime("%Y-%m")]
        months_block = pd.DataFrame()
        if not monthly_abc.empty:
            m = monthly_abc[monthly_abc["month_key"].astype(str).isin(months)].copy()
            m["label"] = m["month_key"].astype(str)
            months_block = self.block_hierarchy(m, "label", months, "month", current_fact_maps)

        example = self.build_example_file(daily_article, weekly_abc)
        return {
            "current_week": current_week,
            "previous_weeks": previous_weeks,
            "months": months_block,
            "dictionary": self.dictionary,
            "ads_day_nm": self.ads_by_day_nm,
            "daily_article_calc": tech_daily,
            "abc_weekly": weekly_abc,
            "abc_monthly": monthly_abc,
            "plan": self.data.plan,
            "buyout90": self.buyout90,
            "paths": self.data.source_paths,
            "warnings": pd.DataFrame({"warning": self.data.warnings}) if self.data.warnings else pd.DataFrame({"warning": []}),
            "example": example,
        }

    def build_example_file(self, daily_article: pd.DataFrame, weekly_abc: pd.DataFrame) -> pd.DataFrame:
        rows = []
        arts = set(EXAMPLE_ARTICLES)
        if not daily_article.empty:
            for _, r in daily_article[daily_article["supplier_article"].isin(arts)].iterrows():
                rows.append({
                    "Период": pd.Timestamp(r["day"]).strftime("%Y-%m-%d"),
                    "Артикул": r["supplier_article"],
                    "Тип": "День",
                    "Заказы": r["orders_day"],
                    "Выкуп 90д": r["buyout_rate_90"],
                    "Выкупленные продажи": r["buyout_qty"],
                    "priceWithDisc": r["price_pwd_use"],
                    "finishedPrice": r["price_finished_use"],
                    "Выручка": r["revenue_pwd"],
                    "Реклама": r["ad_spend"],
                    "Комиссия WB, %": r["commission_pct_used"],
                    "Комиссия WB, ₽": r["commission_rub"],
                    "Эквайринг, ₽": r["acquiring_rub"],
                    "Логистика прямая, ₽": r["logistics_direct_rub"],
                    "Логистика обратная, ₽": r["logistics_return_rub"],
                    "Хранение, ₽": r["storage_rub"],
                    "Прочие, ₽": r["other_rub"],
                    "Себестоимость, ₽": r["cost_rub"],
                    "НДС, ₽": r["vat_rub"],
                    "Валовая прибыль-НДС, ₽": r["gp_minus_nds"],
                    "Неделя экономики": r["econ_week_used"],
                })
        if not weekly_abc.empty:
            for _, r in weekly_abc[weekly_abc["supplier_article"].isin(arts)].iterrows():
                rows.append({
                    "Период": r["week_code"],
                    "Артикул": r["supplier_article"],
                    "Тип": "ABC неделя",
                    "Заказы": r["orders"],
                    "Валовая прибыль-НДС, ₽": r["gp_minus_nds"],
                    "НДС, ₽": r["vat"],
                    "Валовая прибыль, ₽": r["gross_profit"],
                })
        return pd.DataFrame(rows)


def set_header(cell):
    cell.fill = FILL_DARK
    cell.font = Font(color="FFFFFF", bold=True)
    cell.border = BORDER
    cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)


def set_money(cell):
    cell.number_format = money_fmt()


def set_body(cell, bold=False, fill=None):
    cell.border = BORDER
    cell.alignment = Alignment(horizontal="center", vertical="center")
    if bold:
        cell.font = Font(bold=True)
    if fill:
        cell.fill = fill


def autofit(ws):
    widths: Dict[int, int] = {}
    for row in ws.iter_rows():
        for c in row:
            if c.value is None:
                continue
            widths[c.column] = max(widths.get(c.column, 0), min(len(str(c.value)) + 2, 28))
    for col_idx, width in widths.items():
        if col_idx == 1:
            ws.column_dimensions[get_column_letter(col_idx)].width = 28
        else:
            ws.column_dimensions[get_column_letter(col_idx)].width = max(12, min(width, 16))


def write_block(ws, start_row: int, title: str, df: pd.DataFrame) -> int:
    cols = ["Категория"] + [c for c in df.columns if c not in {"Категория", "_kind", "_subject", "_product", "_article"}]
    ws.merge_cells(start_row=start_row, start_column=1, end_row=start_row, end_column=len(cols))
    cell = ws.cell(start_row, 1, title)
    cell.fill = FILL_DARK
    cell.font = Font(color="FFFFFF", bold=True, size=12)
    cell.alignment = Alignment(horizontal="center", vertical="center")
    header_row = start_row + 1
    for j, c in enumerate(cols, start=1):
        set_header(ws.cell(header_row, j, c))
    if df.empty:
        ws.cell(header_row + 1, 1, "Нет данных")
        return header_row + 3
    row = header_row + 1
    cat_group_start = None
    prod_group_start = None
    for _, rec in df.iterrows():
        kind = rec.get("_kind", "")
        if kind == "category":
            if prod_group_start and prod_group_start[1] >= prod_group_start[0]:
                ws.row_dimensions.group(prod_group_start[0], prod_group_start[1], outline_level=2, hidden=True)
                prod_group_start = None
            if cat_group_start and cat_group_start[1] >= cat_group_start[0]:
                ws.row_dimensions.group(cat_group_start[0], cat_group_start[1], outline_level=1, hidden=True)
            cat_group_start = [row + 1, row]
        elif kind == "product":
            if prod_group_start and prod_group_start[1] >= prod_group_start[0]:
                ws.row_dimensions.group(prod_group_start[0], prod_group_start[1], outline_level=2, hidden=True)
            prod_group_start = [row + 1, row]
            if cat_group_start:
                cat_group_start[1] = row
        elif kind == "article":
            if prod_group_start:
                prod_group_start[1] = row
            if cat_group_start:
                cat_group_start[1] = row
        for j, c in enumerate(cols, start=1):
            value = rec.get(c, None)
            cell = ws.cell(row, j, value)
            fill = None
            bold = False
            if kind == "category":
                fill = CATEGORY_FILLS.get(rec.get("_subject", ""), FILL_BLUE_1)
                bold = True
            elif kind == "product":
                fill = FILL_PRODUCT
                bold = True
            elif kind == "article":
                fill = FILL_ARTICLE
            elif kind == "grand_total":
                fill = FILL_TOTAL
                bold = True
            set_body(cell, bold=bold or c == "План", fill=fill)
            if j >= 2 and isinstance(value, (int, float, np.integer, np.floating)) and not pd.isna(value):
                set_money(cell)
        if kind == "grand_total":
            if prod_group_start and prod_group_start[1] >= prod_group_start[0]:
                ws.row_dimensions.group(prod_group_start[0], prod_group_start[1], outline_level=2, hidden=True)
                prod_group_start = None
            if cat_group_start and cat_group_start[1] >= cat_group_start[0]:
                ws.row_dimensions.group(cat_group_start[0], cat_group_start[1], outline_level=1, hidden=True)
                cat_group_start = None
        row += 1
    if prod_group_start and prod_group_start[1] >= prod_group_start[0]:
        ws.row_dimensions.group(prod_group_start[0], prod_group_start[1], outline_level=2, hidden=True)
    if cat_group_start and cat_group_start[1] >= cat_group_start[0]:
        ws.row_dimensions.group(cat_group_start[0], cat_group_start[1], outline_level=1, hidden=True)
    ws.sheet_properties.outlinePr.summaryBelow = False
    return row + 2


def write_df_sheet(wb: Workbook, name: str, df: pd.DataFrame):
    ws = wb.create_sheet(name[:31])
    if df is None or df.empty:
        ws.cell(1, 1, "Нет данных")
        return
    for j, c in enumerate(df.columns, start=1):
        set_header(ws.cell(1, j, str(c)))
    for i, row in enumerate(df.itertuples(index=False), start=2):
        for j, v in enumerate(row, start=1):
            cell = ws.cell(i, j, v)
            cell.border = BORDER
            cell.alignment = Alignment(horizontal="center", vertical="center")
            if isinstance(v, (int, float, np.integer, np.floating)) and not pd.isna(v):
                name_l = str(df.columns[j-1]).lower()
                if "процент" in name_l or "%" in name_l or "rate" in name_l:
                    cell.number_format = "0.00%"
                elif any(k in name_l for k in ["прибыль", "ндс", "выруч", "расход", "план", "цена", "себестоим", "комиссия", "логистика", "эквайринг"]):
                    set_money(cell)
                else:
                    cell.number_format = '# ##0.00'
    autofit(ws)


def export_workbooks(blocks: Dict[str, pd.DataFrame], out_report: str, out_tech: str, out_example: str):
    wb = Workbook()
    ws = wb.active
    ws.title = "Сводка"
    ws.merge_cells("A1:I1")
    t = ws["A1"]
    t.value = TITLE
    t.fill = FILL_DARK
    t.font = Font(color="FFFFFF", bold=True, size=14)
    t.alignment = Alignment(horizontal="center", vertical="center")
    row = 3
    row = write_block(ws, row, TITLE, blocks["current_week"])
    row = write_block(ws, row, "Прошлые недели", blocks["previous_weeks"])
    row = write_block(ws, row, "Последние 3 месяца", blocks["months"])
    ws.freeze_panes = "B4"
    autofit(ws)
    wb.save(out_report)

    twb = Workbook()
    twb.remove(twb.active)
    for key in ["dictionary","paths","buyout90","ads_day_nm","daily_article_calc","abc_weekly","abc_monthly","plan","warnings"]:
        write_df_sheet(twb, key, blocks.get(key, pd.DataFrame()))
    twb.save(out_tech)

    ewb = Workbook()
    ewb.remove(ewb.active)
    ex = blocks.get("example", pd.DataFrame())
    if ex is None or ex.empty:
        ws = ewb.create_sheet("Пример")
        ws.cell(1,1,"Нет данных")
    else:
        for art in EXAMPLE_ARTICLES:
            part = ex[ex["Артикул"] == art].copy() if "Артикул" in ex.columns else pd.DataFrame()
            write_df_sheet(ewb, art.replace("/","_"), part)
    ewb.save(out_example)


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
    loader = Loader(storage, args.reports_root, args.store)
    data = loader.load_all()
    for w in data.warnings:
        log(f"WARN: {w}")
    builder = Builder(data)
    blocks = builder.build()

    stamp = datetime.now().strftime("%Y-%m-%d")
    out_report = f"{args.out_subdir}/Объединенный_отчет_{args.store}_{stamp}.xlsx"
    out_tech = f"{args.out_subdir}/Технические_расчеты_{args.store}_{stamp}.xlsx"
    out_example = f"{args.out_subdir}/Пример_расчета_901_{args.store}_{stamp}.xlsx"

    local_report = Path("/tmp") / f"wb_report_{stamp}.xlsx"
    local_tech = Path("/tmp") / f"wb_tech_{stamp}.xlsx"
    local_example = Path("/tmp") / f"wb_example_{stamp}.xlsx"

    export_workbooks(blocks, str(local_report), str(local_tech), str(local_example))
    storage.write_bytes(out_report, local_report.read_bytes())
    storage.write_bytes(out_tech, local_tech.read_bytes())
    storage.write_bytes(out_example, local_example.read_bytes())

    log(f"Saved report: {out_report}")
    log(f"Saved technical workbook: {out_tech}")
    log(f"Saved example workbook: {out_example}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
