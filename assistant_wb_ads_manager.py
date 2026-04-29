
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
from botocore.exceptions import ClientError
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

STORE = "TOPFACE"
TARGET_SUBJECTS = [
    "Кисти косметические",
    "Помады",
    "Блески",
    "Косметические карандаши",
]
TARGET_SUBJECTS_NORM = {s.lower(): s for s in TARGET_SUBJECTS}
EXCLUDE_ARTICLES = {
    "CZ420", "CZ420БРОВИ", "CZ420ГЛАЗА", "DE49", "DE49ГЛАЗА", "PT901"
}
EXAMPLE_ARTICLES = ["901/5", "901/8", "901/14", "901/18"]

THIN = Side(style="thin", color="D9D9D9")
BORDER = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)
FILL_HEADER = PatternFill("solid", fgColor="DDEBF7")
FILL_SECTION = PatternFill("solid", fgColor="E2F0D9")
FILL_CATEGORY = PatternFill("solid", fgColor="EAF4FF")
FILL_PRODUCT = PatternFill("solid", fgColor="F7FBFF")
FILL_TOTAL = PatternFill("solid", fgColor="FFF2CC")


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def normalize_text(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).replace("\xa0", " ").strip()
    return re.sub(r"\s+", " ", s)


def norm_key(v: Any) -> str:
    s = normalize_text(v).lower().replace("ё", "е")
    s = re.sub(r"[^\w]+", " ", s, flags=re.UNICODE)
    return re.sub(r"\s+", " ", s).strip()


def clean_article(v: Any) -> str:
    s = normalize_text(v)
    if s.lower() in {"nan", "none"}:
        return ""
    return s


def upper_article(v: Any) -> str:
    return clean_article(v).upper()


def is_excluded_article(v: Any) -> bool:
    return upper_article(v) in EXCLUDE_ARTICLES


def canonical_subject(v: Any) -> str:
    s = normalize_text(v).lower()
    return TARGET_SUBJECTS_NORM.get(s, "")


def extract_code(v: Any) -> str:
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


def safe_div(a: Any, b: Any) -> float:
    try:
        a = float(a)
        b = float(b)
    except Exception:
        return np.nan
    if pd.isna(a) or pd.isna(b) or b == 0:
        return np.nan
    return a / b


def to_dt(series: Any) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.normalize()


def to_num(series: Any) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def week_code_from_date(dt_value: Any) -> Optional[str]:
    if pd.isna(dt_value):
        return None
    iso = pd.Timestamp(dt_value).isocalendar()
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


def parse_abc_period_from_name(name: str) -> Tuple[Optional[date], Optional[date]]:
    m = re.search(r"__(\d{2})\.(\d{2})\.(\d{4})-(\d{2})\.(\d{2})\.(\d{4})__", name)
    if not m:
        return None, None
    return date(int(m.group(3)), int(m.group(2)), int(m.group(1))), date(int(m.group(6)), int(m.group(5)), int(m.group(4)))


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


COMMON_ALIASES: Dict[str, List[str]] = {
    "day": ["Дата", "date", "dt"],
    "nm_id": ["Артикул WB", "nmID", "nmId", "nm_id"],
    "supplier_article": ["Артикул продавца", "supplierArticle", "supplier_article"],
    "subject": ["Предмет", "Название предмета", "subject"],
    "title": ["Название", "Название товара"],
    "brand": ["Бренд", "brand"],
    "orders": ["Заказы", "orders", "ordersCount", "Кол-во продаж"],
    "finished_price": ["finishedPrice", "Средняя цена покупателя", "Цена с учетом всех скидок, кроме суммы по WB Кошельку"],
    "price_with_disc": ["priceWithDisc", "Средняя цена продажи", "Цена со скидкой продавца, в том числе со скидкой WB Клуба"],
    "spend": ["Расход", "spend", "Продвижение"],
}


def rename_using_aliases(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    existing = {norm_key(c): c for c in out.columns}
    for target, variants in COMMON_ALIASES.items():
        if target in out.columns:
            continue
        found = None
        for variant in variants:
            k = norm_key(variant)
            if k in existing:
                found = existing[k]
                break
        if found is not None:
            out[target] = out[found]
    return out


def dedupe_columns(cols: Iterable[Any]) -> List[str]:
    result: List[str] = []
    seen: Dict[str, int] = {}
    for c in cols:
        base = normalize_text(c) or "unnamed"
        seen[base] = seen.get(base, 0) + 1
        result.append(base if seen[base] == 1 else f"{base}__{seen[base]}")
    return result


def read_excel_flexible(data: bytes, preferred_sheets: Optional[List[str]] = None, header_candidates: Iterable[int] = (0, 1, 2)) -> pd.DataFrame:
    bio = io.BytesIO(data)
    xls = pd.ExcelFile(bio)
    sheet_name = xls.sheet_names[0]
    if preferred_sheets:
        names = {norm_key(x): x for x in xls.sheet_names}
        for wanted in preferred_sheets:
            if norm_key(wanted) in names:
                sheet_name = names[norm_key(wanted)]
                break
    best_df = None
    best_score = -10**9
    for header in header_candidates:
        try:
            df = xls.parse(sheet_name=sheet_name, header=header, dtype=object)
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
        raise ValueError(f"Не удалось прочитать {sheet_name}")
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
        base = self._abs(prefix)
        search_root = base if base.exists() else base.parent
        if not search_root.exists():
            return []
        out = []
        for p in search_root.rglob("*"):
            if p.is_file():
                rel = str(p.relative_to(self.root)).replace("\\", "/")
                if rel.startswith(prefix):
                    out.append(rel)
        return sorted(out)

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
        out: List[str] = []
        token = None
        while True:
            kwargs = {"Bucket": self.bucket, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            resp = self.s3.list_objects_v2(**kwargs)
            for item in resp.get("Contents", []):
                key = item["Key"]
                if not key.endswith("/"):
                    out.append(key)
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        return sorted(out)

    def read_bytes(self, path: str) -> bytes:
        return self.s3.get_object(Bucket=self.bucket, Key=path)["Body"].read()

    def write_bytes(self, path: str, data: bytes) -> None:
        self.s3.put_object(Bucket=self.bucket, Key=path, Body=data)

    def exists(self, path: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=path)
            return True
        except ClientError:
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
    dictionary: pd.DataFrame
    latest_day: pd.Timestamp
    source_paths: pd.DataFrame
    warnings: List[str]


class Loader:
    def __init__(self, storage: BaseStorage, reports_root: str = "Отчёты", store: str = STORE):
        self.storage = storage
        self.reports_root = reports_root.rstrip("/")
        self.store = store
        self.source_rows: List[Dict[str, Any]] = []
        self.warnings: List[str] = []

    def _prefix(self, *parts: str) -> str:
        return "/".join([self.reports_root, *parts]).replace("//", "/")

    def _record_source(self, source_type: str, path: str, sheet: str = "") -> None:
        self.source_rows.append({"Источник": source_type, "Путь": path, "Лист": sheet})

    def _list_under(self, prefixes: List[str]) -> List[str]:
        out: List[str] = []
        for prefix in prefixes:
            out.extend(self.storage.list_files(prefix))
        return sorted(set(out))

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
            df["nm_id"] = to_num(df["nm_id"])
        if "supplier_article" in df.columns:
            df["supplier_article"] = df["supplier_article"].map(clean_article)
        if "subject" in df.columns:
            df["subject"] = df["subject"].map(normalize_text)
        if "title" in df.columns:
            df["title"] = df["title"].map(normalize_text)
        if "brand" in df.columns:
            df["brand"] = df["brand"].map(normalize_text)
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
                df = pd.read_excel(io.BytesIO(self.storage.read_bytes(path)), dtype=object)
                self._record_source("Заказы", path, "")
                df = df.rename(columns={"supplierArticle": "supplier_article", "nmId": "nm_id", "finishedPrice": "finished_price", "priceWithDisc": "price_with_disc"})
                df["day"] = to_dt(df.get("date", pd.Series(dtype=object)))
                df["orders"] = 1.0
                df["is_cancel"] = df.get("isCancel", False).fillna(False).astype(bool)
                df = self._finalize(df)
                df["subject_norm"] = df.get("subject", "").map(canonical_subject)
                df["code"] = df.get("supplier_article", "").map(extract_code)
                for c in ["finished_price", "price_with_disc", "spp"]:
                    if c not in df.columns:
                        df[c] = np.nan
                    df[c] = to_num(df[c])
                dfs.append(df[["day", "nm_id", "supplier_article", "subject", "subject_norm", "brand", "orders", "finished_price", "price_with_disc", "spp", "is_cancel", "code"]])
            except Exception as e:
                self.warnings.append(f"Orders read error {path}: {e}")
        out = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        if not out.empty:
            out = out[out["subject_norm"].isin(TARGET_SUBJECTS) & (~out["is_cancel"]) & (out["code"] != "") & (~out["supplier_article"].map(is_excluded_article))].copy()
            log(f"Orders rows loaded: {len(out):,}; date range {out['day'].min().date()} .. {out['day'].max().date()}")
        return out

    def load_funnel(self) -> pd.DataFrame:
        candidates = [
            self._prefix("Воронка продаж", self.store, "Воронка продаж.xlsx"),
            self._prefix("Воронка продаж", "Воронка продаж.xlsx"),
            "Воронка продаж.xlsx",
            "Воронка продаж (1).xlsx",
        ]
        path = next((p for p in candidates if self.storage.exists(p)), None)
        if not path:
            files = self._glob_root(["Воронка продаж*.xlsx"])
            path = files[0] if files else None
        if not path:
            return pd.DataFrame()
        try:
            df = read_excel_flexible(self.storage.read_bytes(path))
            self._record_source("Воронка", path, "")
            df["day"] = to_dt(df.get("day", df.get("date", df.get("dt", pd.Series(dtype=object)))))
            df = self._finalize(df)
            if "ordersCount" in df.columns:
                df["orders_cnt"] = to_num(df["ordersCount"])
            else:
                df["orders_cnt"] = to_num(df.get("orders", np.nan))
            if "buyoutsCount" in df.columns:
                df["buyouts_cnt"] = to_num(df["buyoutsCount"])
            else:
                df["buyouts_cnt"] = to_num(df.get("buyouts_count", np.nan))
            out = df[["day", "nm_id", "supplier_article", "subject", "orders_cnt", "buyouts_cnt"]].copy()
            out["subject_norm"] = out["subject"].map(canonical_subject)
            out["code"] = out["supplier_article"].map(extract_code)
            out = out[out["subject_norm"].isin(TARGET_SUBJECTS) & (out["code"] != "")].copy()
            log(f"Funnel rows loaded: {len(out):,}; date range {out['day'].min().date()} .. {out['day'].max().date()}")
            return out
        except Exception as e:
            self.warnings.append(f"Funnel read error {path}: {e}")
            return pd.DataFrame()

    def load_ads(self) -> pd.DataFrame:
        weekly_files = self._list_under([
            self._prefix("Реклама", self.store, "Недельные"),
        ])
        weekly_files = [p for p in weekly_files if Path(p).suffix.lower() == ".xlsx"]
        use_files = weekly_files
        if not use_files:
            fallback = self._list_under([self._prefix("Реклама", self.store)])
            use_files = [p for p in fallback if Path(p).name.lower().startswith("анализ рекламы")]
            if not use_files:
                use_files = self._glob_root(["Реклама_*.xlsx", "Анализ рекламы*.xlsx"])
        daily_dfs = []
        for path in use_files:
            try:
                xls = pd.ExcelFile(io.BytesIO(self.storage.read_bytes(path)))
                if "Статистика_Ежедневно" not in xls.sheet_names:
                    continue
                df = pd.read_excel(io.BytesIO(self.storage.read_bytes(path)), sheet_name="Статистика_Ежедневно", dtype=object)
                self._record_source("Реклама", path, "Статистика_Ежедневно")
                df = df.rename(columns={"Артикул WB": "nm_id", "Дата": "day", "Название предмета": "subject", "Расход": "spend"})
                df["day"] = to_dt(df["day"])
                df["nm_id"] = to_num(df["nm_id"])
                df["spend"] = to_num(df["spend"]).fillna(0.0)
                df["subject"] = df.get("subject", "").map(normalize_text)
                df["subject_norm"] = df["subject"].map(canonical_subject)
                df = df[df["nm_id"].notna() & df["day"].notna()].copy()
                daily_dfs.append(df[["day", "nm_id", "subject", "subject_norm", "spend"]])
            except Exception as e:
                self.warnings.append(f"Ads read error {path}: {e}")
        out = pd.concat(daily_dfs, ignore_index=True) if daily_dfs else pd.DataFrame(columns=["day", "nm_id", "subject", "subject_norm", "spend"])
        if not out.empty:
            out = out.groupby(["day", "nm_id"], as_index=False).agg(
                spend=("spend", "sum"),
                subject=("subject", "last"),
                subject_norm=("subject_norm", "last"),
            )
            log(f"Ads rows loaded: {len(out):,}; date range {out['day'].min().date()} .. {out['day'].max().date()}; spend sum {out['spend'].sum():,.0f}")
        return out

    def load_economics(self) -> pd.DataFrame:
        candidates = [
            self._prefix("Финансовые показатели", self.store, "Экономика.xlsx"),
            self._prefix("Финансовые показатели", self.store, "Недельные", "Экономика.xlsx"),
            "Экономика (4).xlsx",
            "Экономика.xlsx",
        ]
        path = next((p for p in candidates if self.storage.exists(p)), None)
        if not path:
            files = self._glob_root(["Экономика*.xlsx"])
            path = files[0] if files else None
        if not path:
            return pd.DataFrame()
        try:
            df = pd.read_excel(io.BytesIO(self.storage.read_bytes(path)), sheet_name="Юнит экономика", dtype=object)
            self._record_source("Экономика", path, "Юнит экономика")
            df = df.rename(columns={
                "Артикул WB": "nm_id",
                "Артикул продавца": "supplier_article",
                "Предмет": "subject",
                "Средняя цена продажи": "econ_price_with_disc",
                "Средняя цена покупателя": "econ_finished_price",
                "Комиссия WB, %": "commission_pct",
                "Эквайринг, %": "acquiring_pct",
                "Логистика прямая, руб/ед": "logistics_direct_unit",
                "Логистика обратная, руб/ед": "logistics_return_unit",
                "Хранение, руб/ед": "storage_unit",
                "Прочие расходы, руб/ед": "other_unit",
                "Себестоимость, руб": "cost_unit",
                "НДС, руб/ед": "vat_unit",
                "Валовая прибыль, руб/ед": "gp_unit",
                "Неделя": "week",
            })
            df["nm_id"] = to_num(df["nm_id"])
            df["supplier_article"] = df["supplier_article"].map(clean_article)
            df["subject"] = df["subject"].map(normalize_text)
            df["subject_norm"] = df["subject"].map(canonical_subject)
            df["code"] = df["supplier_article"].map(extract_code)
            for c in ["econ_price_with_disc", "econ_finished_price", "commission_pct", "acquiring_pct", "logistics_direct_unit", "logistics_return_unit", "storage_unit", "other_unit", "cost_unit", "vat_unit", "gp_unit"]:
                if c not in df.columns:
                    df[c] = np.nan
                df[c] = to_num(df[c])
            out = df[["week", "nm_id", "supplier_article", "subject", "subject_norm", "code", "econ_price_with_disc", "econ_finished_price", "commission_pct", "acquiring_pct", "logistics_direct_unit", "logistics_return_unit", "storage_unit", "other_unit", "cost_unit", "vat_unit", "gp_unit"]].copy()
            out = out[out["subject_norm"].isin(TARGET_SUBJECTS) & (out["code"] != "") & (~out["supplier_article"].map(is_excluded_article))].copy()
            log(f"Economics rows loaded: {len(out):,}; weeks {', '.join(sorted(out['week'].dropna().astype(str).unique())[:10])}")
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
                start, end = parse_abc_period_from_name(Path(path).name)
                if not start or not end:
                    continue
                df = pd.read_excel(io.BytesIO(self.storage.read_bytes(path)), dtype=object)
                self._record_source("ABC", path, "")
                df = df.rename(columns={
                    "Артикул WB": "nm_id",
                    "Артикул продавца": "supplier_article",
                    "Предмет": "subject",
                    "Валовая прибыль": "gross_profit",
                    "Валовая выручка": "gross_revenue",
                    "Кол-во продаж": "sales_count",
                    "Заказы": "orders",
                })
                df["nm_id"] = to_num(df["nm_id"])
                df["supplier_article"] = df["supplier_article"].map(clean_article)
                df["subject"] = df["subject"].map(normalize_text)
                df["subject_norm"] = df["subject"].map(canonical_subject)
                df["code"] = df["supplier_article"].map(extract_code)
                for c in ["gross_profit", "gross_revenue", "sales_count", "orders"]:
                    if c not in df.columns:
                        df[c] = 0.0
                    df[c] = to_num(df[c]).fillna(0.0)
                df["vat"] = df["gross_revenue"] * 7.0 / 107.0
                df["gp_minus_nds"] = df["gross_profit"] - df["vat"]
                df["period_start"] = pd.Timestamp(start)
                df["period_end"] = pd.Timestamp(end)
                df["week_code"] = week_code_from_date(start)
                df["week_label"] = f"{pd.Timestamp(start).strftime('%d.%m')}-{pd.Timestamp(end).strftime('%d.%m')}"
                df["month_key"] = pd.Timestamp(start).strftime("%Y-%m")
                df = df[df["subject_norm"].isin(TARGET_SUBJECTS) & (df["code"] != "") & (~df["supplier_article"].map(is_excluded_article))].copy()
                month_end = (pd.Timestamp(start).to_period("M").end_time.normalize()).date()
                if start.day == 1 and end == month_end:
                    monthly_dfs.append(df[["month_key", "nm_id", "supplier_article", "subject", "subject_norm", "code", "gross_profit", "gross_revenue", "vat", "gp_minus_nds", "orders"]])
                else:
                    weekly_dfs.append(df[["week_code", "week_label", "period_start", "period_end", "nm_id", "supplier_article", "subject", "subject_norm", "code", "gross_profit", "gross_revenue", "vat", "gp_minus_nds", "orders"]])
            except Exception as e:
                self.warnings.append(f"ABC read error {path}: {e}")
        weekly = pd.concat(weekly_dfs, ignore_index=True) if weekly_dfs else pd.DataFrame()
        monthly = pd.concat(monthly_dfs, ignore_index=True) if monthly_dfs else pd.DataFrame()
        if not weekly.empty:
            log(f"ABC weekly rows loaded: {len(weekly):,}; weeks {', '.join(sorted(weekly['week_code'].dropna().astype(str).unique()))}")
        if not monthly.empty:
            log(f"ABC monthly rows loaded: {len(monthly):,}; months {', '.join(sorted(monthly['month_key'].dropna().astype(str).unique()))}")
        return weekly, monthly

    def load_plan(self, month_key: str) -> pd.DataFrame:
        candidates = [
            self._prefix("Объединенный отчет", self.store, "План.xlsx"),
            "План.xlsx",
        ]
        path = next((p for p in candidates if self.storage.exists(p)), None)
        if not path:
            return pd.DataFrame()
        try:
            raw = pd.read_excel(io.BytesIO(self.storage.read_bytes(path)), sheet_name="Итог_все_категории", header=None, dtype=object)
            self._record_source("План", path, "Итог_все_категории")
            header_row = None
            for i in range(min(10, len(raw))):
                vals = [normalize_text(x) for x in raw.iloc[i].tolist()]
                if "Артикул продавца" in vals:
                    header_row = i
                    break
            if header_row is None:
                return pd.DataFrame()
            headers = [normalize_text(x) for x in raw.iloc[header_row].tolist()]
            df = raw.iloc[header_row + 1 :].copy()
            df.columns = headers
            df = df.dropna(axis=0, how="all")
            target_month = pd.Period(month_key, freq="M")
            month_name = {
                1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель", 5: "Май", 6: "Июнь",
                7: "Июль", 8: "Август", 9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
            }[target_month.month]
            target_col = f"ВП-НДС {month_name} {target_month.year}"
            plan_col = None
            for c in df.columns:
                if norm_key(c) == norm_key(target_col):
                    plan_col = c
                    break
            if plan_col is None:
                for c in df.columns:
                    if norm_key(target_col) in norm_key(c):
                        plan_col = c
                        break
            if plan_col is None:
                return pd.DataFrame()
            out = df[["Категория", "Артикул продавца", plan_col]].copy()
            out = out.rename(columns={"Категория": "subject", "Артикул продавца": "supplier_article", plan_col: "plan_gp_minus_nds_month"})
            out["subject"] = out["subject"].map(normalize_text)
            out["subject_norm"] = out["subject"].map(canonical_subject)
            out["supplier_article"] = out["supplier_article"].map(clean_article)
            out["code"] = out["supplier_article"].map(extract_code)
            out["plan_gp_minus_nds_month"] = to_num(out["plan_gp_minus_nds_month"]).fillna(np.nan)
            out = out[out["subject_norm"].isin(TARGET_SUBJECTS) & (out["code"] != "") & (~out["supplier_article"].map(is_excluded_article))].copy()
            log(f"Plan rows loaded: {len(out):,}; non-null plan {out['plan_gp_minus_nds_month'].notna().sum():,}")
            return out
        except Exception as e:
            self.warnings.append(f"Plan read error {path}: {e}")
            return pd.DataFrame()

    def build_dictionary(self, orders: pd.DataFrame, funnel: pd.DataFrame, ads: pd.DataFrame, econ: pd.DataFrame, abc_w: pd.DataFrame, abc_m: pd.DataFrame, plan: pd.DataFrame) -> pd.DataFrame:
        rows = []
        def append(df: pd.DataFrame):
            if df is None or df.empty:
                return
            work = df.copy()
            if "nm_id" not in work.columns:
                work["nm_id"] = np.nan
            if "supplier_article" not in work.columns:
                work["supplier_article"] = ""
            if "subject" not in work.columns:
                work["subject"] = ""
            work["code"] = work.get("supplier_article", "").map(extract_code)
            rows.append(work[["subject", "supplier_article", "nm_id", "code"]].copy())

        append(orders); append(funnel); append(econ); append(abc_w); append(abc_m); append(plan)
        if rows:
            d = pd.concat(rows, ignore_index=True)
        else:
            d = pd.DataFrame(columns=["subject", "supplier_article", "nm_id", "code"])
        d["subject"] = d["subject"].map(normalize_text)
        d["subject_norm"] = d["subject"].map(canonical_subject)
        d["supplier_article"] = d["supplier_article"].map(clean_article)
        d["nm_id"] = to_num(d["nm_id"])
        d["code"] = d["supplier_article"].map(extract_code).where(lambda s: s != "", d["code"])
        d = d[d["subject_norm"].isin(TARGET_SUBJECTS) & (d["code"] != "") & (~d["supplier_article"].map(is_excluded_article))].copy()

        # nm_id from orders/econ/abc
        by_article = (
            d.sort_values(["subject", "supplier_article"])
             .groupby("supplier_article", as_index=False)
             .agg(subject=("subject", "first"), subject_norm=("subject_norm", "first"), code=("code", "first"), nm_id=("nm_id", "max"))
        )
        # supplement missing nm from economics/article
        by_nm = d.dropna(subset=["nm_id"]).drop_duplicates(subset=["nm_id", "supplier_article"])
        if not ads.empty:
            ads_nm = ads[["nm_id"]].drop_duplicates().copy()
            missing_nm = ads_nm.merge(by_nm[["nm_id", "supplier_article", "subject", "subject_norm", "code"]], on="nm_id", how="left")
        else:
            missing_nm = pd.DataFrame(columns=["nm_id", "supplier_article", "subject", "subject_norm", "code"])
        out = pd.concat([by_article, missing_nm[["supplier_article", "subject", "subject_norm", "code", "nm_id"]]], ignore_index=True)
        out = out.drop_duplicates(subset=["supplier_article"], keep="first")
        out = out[out["supplier_article"].astype(str).str.strip() != ""].copy()
        out["Товар"] = out["code"]
        out["Категория"] = out["subject"]
        out["Артикул продавца"] = out["supplier_article"]
        out["Артикул WB"] = out["nm_id"]
        return out[["Категория", "Товар", "Артикул продавца", "Артикул WB"]].sort_values(["Категория", "Товар", "Артикул продавца"])

    def load_all(self) -> LoadedData:
        log("Loading data")
        log("Loading orders"); orders = self.load_orders()
        log("Loading funnel"); funnel = self.load_funnel()
        log("Loading ads"); ads = self.load_ads()
        log("Loading economics"); econ = self.load_economics()
        log("Loading ABC"); abc_w, abc_m = self.load_abc()
        latest_candidates = [pd.to_datetime(df["day"], errors="coerce").max() for df in [orders, funnel, ads] if not df.empty and "day" in df.columns]
        latest_day = max([x for x in latest_candidates if pd.notna(x)], default=pd.Timestamp.today().normalize())
        month_key = latest_day.to_period("M").strftime("%Y-%m")
        log("Loading plan"); plan = self.load_plan(month_key)
        dictionary = self.build_dictionary(orders, funnel, ads, econ, abc_w, abc_m, plan)
        return LoadedData(
            orders=orders,
            funnel=funnel,
            ads_daily=ads,
            economics=econ,
            abc_weekly=abc_w,
            abc_monthly=abc_m,
            plan=plan,
            dictionary=dictionary,
            latest_day=pd.Timestamp(latest_day).normalize(),
            source_paths=pd.DataFrame(self.source_rows),
            warnings=self.warnings,
        )


class Builder:
    def __init__(self, data: LoadedData):
        self.data = data
        self.latest_day = pd.Timestamp(data.latest_day).normalize()
        self.current_week_start = self.latest_day - pd.Timedelta(days=self.latest_day.weekday())
        self.current_week_days = [self.current_week_start + pd.Timedelta(days=i) for i in range(7)]
        self.current_month_key = self.latest_day.to_period("M").strftime("%Y-%m")
        self.current_month_start = self.latest_day.replace(day=1)
        self.days_in_month = calendar.monthrange(self.latest_day.year, self.latest_day.month)[1]
        self.dict_article = self.prepare_dictionary()
        self.buyout90 = self.build_buyout90()
        self.econ_week, self.econ_subject_week_comm, self.econ_subject_latest_comm = self.prepare_economics()
        self.plan_maps = self.prepare_plan_maps()

    def prepare_dictionary(self) -> pd.DataFrame:
        d = self.data.dictionary.copy()
        if d.empty:
            return pd.DataFrame(columns=["Категория", "Товар", "Артикул продавца", "Артикул WB"])
        d["Артикул продавца"] = d["Артикул продавца"].map(clean_article)
        d["Артикул WB"] = to_num(d["Артикул WB"])
        d["Товар"] = d["Товар"].astype(str)
        d["Категория"] = d["Категория"].map(normalize_text)
        d = d.drop_duplicates(subset=["Артикул продавца"], keep="first")
        return d

    def build_buyout90(self) -> pd.DataFrame:
        f = self.data.funnel.copy()
        if f.empty:
            return pd.DataFrame(columns=["nm_id", "buyout_rate_90"])
        start = self.latest_day - pd.Timedelta(days=89)
        f = f[(f["day"] >= start) & (f["day"] <= self.latest_day)].copy()
        out = f.groupby("nm_id", as_index=False).agg(orders_90=("orders_cnt", "sum"), buyouts_90=("buyouts_cnt", "sum"))
        out["buyout_rate_90"] = np.where(out["orders_90"] > 0, out["buyouts_90"] / out["orders_90"], np.nan)
        return out[["nm_id", "buyout_rate_90"]]

    def prepare_economics(self) -> Tuple[pd.DataFrame, Dict[Tuple[str, str], float], Dict[str, float]]:
        econ = self.data.economics.copy()
        if econ.empty:
            return econ, {}, {}
        econ["week_sort"] = econ["week"].astype(str)
        subject_week: Dict[Tuple[str, str], float] = {}
        subject_latest: Dict[str, float] = {}
        valid = econ[(econ["commission_pct"].fillna(0) > 0) & (econ["subject_norm"].astype(str) != "")]
        if not valid.empty:
            sw = valid.groupby(["subject_norm", "week"], as_index=False).agg(comm=("commission_pct", "median"))
            subject_week = {(r.subject_norm, str(r.week)): float(r.comm) for r in sw.itertuples(index=False)}
            latest = valid.sort_values(["subject_norm", "week_sort"]).groupby("subject_norm", as_index=False).tail(1)
            subject_latest = {str(r.subject_norm): float(r.commission_pct) for r in latest.itertuples(index=False)}
        return econ, subject_week, subject_latest

    def prepare_plan_maps(self) -> Dict[str, Dict[Any, float]]:
        plan = self.data.plan.copy()
        if plan.empty:
            return {"article": {}, "product": {}, "category": {}}
        article = plan.dropna(subset=["plan_gp_minus_nds_month"]).groupby("supplier_article", as_index=False)["plan_gp_minus_nds_month"].sum()
        product = plan.dropna(subset=["plan_gp_minus_nds_month"]).groupby(["subject", "code"], as_index=False)["plan_gp_minus_nds_month"].sum()
        category = plan.dropna(subset=["plan_gp_minus_nds_month"]).groupby("subject", as_index=False)["plan_gp_minus_nds_month"].sum()
        return {
            "article": dict(zip(article["supplier_article"], article["plan_gp_minus_nds_month"])),
            "product": {(r.subject, r.code): float(r.plan_gp_minus_nds_month) for r in product.itertuples(index=False)},
            "category": dict(zip(category["subject"], category["plan_gp_minus_nds_month"])),
        }

    def map_nm_to_article(self, df: pd.DataFrame, nm_col: str = "nm_id") -> pd.DataFrame:
        if df.empty:
            return df
        out = df.copy()
        nm_map = self.dict_article[["Артикул продавца", "Артикул WB", "Категория", "Товар"]].dropna(subset=["Артикул WB"]).drop_duplicates(subset=["Артикул WB"])
        nm_map = nm_map.rename(columns={"Артикул WB": nm_col})
        out = out.merge(nm_map, on=nm_col, how="left")
        if "supplier_article" not in out.columns:
            out["supplier_article"] = out["Артикул продавца"]
        else:
            mask = out["supplier_article"].astype(str).str.strip().eq("")
            out.loc[mask, "supplier_article"] = out.loc[mask, "Артикул продавца"]
        if "subject" not in out.columns:
            out["subject"] = out["Категория"]
        else:
            mask = out["subject"].astype(str).str.strip().eq("")
            out.loc[mask, "subject"] = out.loc[mask, "Категория"]
        if "code" not in out.columns:
            out["code"] = out["Товар"]
        else:
            mask = out["code"].astype(str).str.strip().eq("")
            out.loc[mask, "code"] = out.loc[mask, "Товар"]
        out.drop(columns=["Артикул продавца", "Категория", "Товар"], inplace=True, errors="ignore")
        out["supplier_article"] = out["supplier_article"].map(clean_article)
        out["subject"] = out["subject"].map(normalize_text)
        out["subject_norm"] = out["subject"].map(canonical_subject)
        out["code"] = out["supplier_article"].map(extract_code).where(lambda s: s != "", out["code"])
        out = out[out["subject_norm"].isin(TARGET_SUBJECTS) & (out["code"] != "") & (~out["supplier_article"].map(is_excluded_article))].copy()
        return out

    def pick_econ_row(self, supplier_article: str, nm_id: Any, day: pd.Timestamp) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        target_week = week_code_from_date(day)
        econ = self.econ_week
        g = econ[econ["supplier_article"] == supplier_article].copy()
        if g.empty and pd.notna(nm_id):
            g = econ[econ["nm_id"] == nm_id].copy()
        diag = {"supplier_article": supplier_article, "nm_id": nm_id, "target_week": target_week, "week_used": "", "commission_source": "", "exact_week": 0}
        if g.empty:
            return {}, diag
        exact = g[g["week"].astype(str) == str(target_week)].copy()
        if not exact.empty:
            chosen = exact.iloc[0]
            diag["week_used"] = str(chosen["week"])
            diag["exact_week"] = 1
        else:
            g = g.sort_values("week_sort")
            chosen = g.iloc[-1]
            diag["week_used"] = str(chosen["week"])
        result = {c: chosen.get(c, np.nan) for c in ["week", "econ_price_with_disc", "econ_finished_price", "commission_pct", "acquiring_pct", "logistics_direct_unit", "logistics_return_unit", "storage_unit", "other_unit", "cost_unit", "vat_unit", "gp_unit", "subject_norm"]}
        comm = float(chosen.get("commission_pct", np.nan)) if pd.notna(chosen.get("commission_pct", np.nan)) else np.nan
        subj = chosen.get("subject_norm", "")
        if pd.isna(comm) or comm <= 0:
            comm = self.econ_subject_week_comm.get((subj, str(target_week)), np.nan)
            if pd.notna(comm) and comm > 0:
                diag["commission_source"] = "subject_week"
            else:
                comm = self.econ_subject_latest_comm.get(subj, np.nan)
                if pd.notna(comm) and comm > 0:
                    diag["commission_source"] = "subject_latest"
        else:
            diag["commission_source"] = "article_week_or_latest"
        result["commission_pct"] = comm
        return result, diag

    def build_current_week_daily(self) -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame]]:
        orders = self.data.orders.copy()
        if orders.empty:
            return pd.DataFrame(), {}
        orders = self.map_nm_to_article(orders)
        orders = orders[(orders["day"] >= self.current_week_start) & (orders["day"] <= self.latest_day)].copy()
        if orders.empty:
            return pd.DataFrame(), {}
        log(f"Current week order rows: {len(orders):,}; day range {orders['day'].min().date()} .. {orders['day'].max().date()}")
        grouped = orders.groupby(["day", "nm_id", "supplier_article", "subject", "code"], as_index=False).agg(
            orders_day=("orders", "sum"),
            finished_price_avg=("finished_price", lambda s: weighted_mean(s, orders.loc[s.index, "orders"])),
            price_with_disc_avg=("price_with_disc", lambda s: weighted_mean(s, orders.loc[s.index, "orders"])),
        )
        grouped = grouped.merge(self.buyout90, on="nm_id", how="left")

        econ_rows = []
        econ_diag_rows = []
        for rec in grouped[["day", "supplier_article", "nm_id"]].drop_duplicates().itertuples(index=False):
            econ_row, diag = self.pick_econ_row(rec.supplier_article, rec.nm_id, rec.day)
            diag["day"] = rec.day
            if econ_row:
                econ_row.update({"day": rec.day, "supplier_article": rec.supplier_article, "nm_id": rec.nm_id})
                econ_rows.append(econ_row)
            econ_diag_rows.append(diag)
        econ_df = pd.DataFrame(econ_rows)
        econ_diag = pd.DataFrame(econ_diag_rows)
        grouped = grouped.merge(econ_df, on=["day", "supplier_article", "nm_id"], how="left")

        ads = self.data.ads_daily.copy()
        ads = self.map_nm_to_article(ads)
        ads = ads[(ads["day"] >= self.current_week_start) & (ads["day"] <= self.latest_day)].copy()
        if not ads.empty:
            ads_day = ads.groupby(["day", "nm_id"], as_index=False).agg(ad_spend_day=("spend", "sum"))
        else:
            ads_day = pd.DataFrame(columns=["day", "nm_id", "ad_spend_day"])
        grouped = grouped.merge(ads_day, on=["day", "nm_id"], how="left")
        grouped["ad_spend_day"] = grouped["ad_spend_day"].fillna(0.0)

        grouped["buyout_rate_used"] = grouped["buyout_rate_90"].fillna(1.0)
        grouped["sold_qty"] = grouped["orders_day"] * grouped["buyout_rate_used"]
        grouped["price_with_disc_used"] = grouped["price_with_disc_avg"].fillna(grouped["econ_price_with_disc"]).fillna(0.0)
        grouped["finished_price_used"] = grouped["finished_price_avg"].fillna(grouped["econ_finished_price"]).fillna(0.0)
        grouped["revenue_rub"] = grouped["sold_qty"] * grouped["price_with_disc_used"]
        grouped["commission_pct_used"] = grouped["commission_pct"].fillna(0.0)
        grouped["acquiring_pct_used"] = grouped["acquiring_pct"].fillna(0.0)
        grouped["commission_rub"] = grouped["revenue_rub"] * grouped["commission_pct_used"] / 100.0
        grouped["acquiring_rub"] = grouped["revenue_rub"] * grouped["acquiring_pct_used"] / 100.0
        grouped["logistics_direct_rub"] = grouped["sold_qty"] * grouped["logistics_direct_unit"].fillna(0.0)
        grouped["logistics_return_rub"] = grouped["sold_qty"] * grouped["logistics_return_unit"].fillna(0.0)
        grouped["storage_rub"] = grouped["sold_qty"] * grouped["storage_unit"].fillna(0.0)
        grouped["other_rub"] = grouped["sold_qty"] * grouped["other_unit"].fillna(0.0)
        grouped["cost_rub"] = grouped["sold_qty"] * grouped["cost_unit"].fillna(0.0)
        grouped["vat_rub"] = grouped["sold_qty"] * grouped["finished_price_used"] * 7.0 / 107.0
        grouped["gross_profit_rub"] = (
            grouped["revenue_rub"]
            - grouped["commission_rub"]
            - grouped["acquiring_rub"]
            - grouped["logistics_direct_rub"]
            - grouped["logistics_return_rub"]
            - grouped["storage_rub"]
            - grouped["other_rub"]
            - grouped["cost_rub"]
            - grouped["ad_spend_day"]
            - grouped["vat_rub"]
        )
        log(f"Ads spend matched (day+nm_id): {grouped['ad_spend_day'].sum():,.0f}")
        log(f"Commission pct zero rows: {(grouped['commission_pct_used'] <= 0).sum():,} из {len(grouped):,}")
        grouped["day_label"] = pd.to_datetime(grouped["day"]).dt.strftime("%a %d.%m")
        return grouped, {
            "daily_calc": grouped.copy(),
            "economics_match": econ_diag,
            "ads_used": ads_day,
            "orders_used": grouped[["day", "nm_id", "supplier_article", "subject", "code", "orders_day", "finished_price_avg", "price_with_disc_avg", "buyout_rate_used"]],
        }

    def build_weekly_fact(self) -> pd.DataFrame:
        abc = self.data.abc_weekly.copy()
        if abc.empty:
            return pd.DataFrame()
        abc = self.map_nm_to_article(abc)
        abc = abc[(abc["period_start"] >= self.current_month_start) & (abc["period_start"] <= self.latest_day)].copy()
        return abc

    def build_monthly_fact(self) -> pd.DataFrame:
        abc_m = self.data.abc_monthly.copy()
        abc_w = self.data.abc_weekly.copy()
        if not abc_m.empty:
            abc_m = self.map_nm_to_article(abc_m)
        if not abc_w.empty:
            abc_w = self.map_nm_to_article(abc_w)
        periods = [self.latest_day.to_period("M") - 2, self.latest_day.to_period("M") - 1, self.latest_day.to_period("M")]
        month_keys = [p.strftime("%Y-%m") for p in periods]
        frames = []
        if not abc_m.empty:
            frames.append(abc_m[abc_m["month_key"].isin(month_keys)].copy())
        if self.current_month_key not in set(abc_m.get("month_key", pd.Series(dtype=str)).astype(str)):
            cur_w = abc_w[pd.to_datetime(abc_w["period_start"]).dt.to_period("M").astype(str) == self.current_month_key].copy() if not abc_w.empty else pd.DataFrame()
            if not cur_w.empty:
                cur_m = cur_w.groupby(["month_key", "nm_id", "supplier_article", "subject", "code"], as_index=False).agg(
                    gross_profit=("gross_profit", "sum"),
                    gross_revenue=("gross_revenue", "sum"),
                    vat=("vat", "sum"),
                    gp_minus_nds=("gp_minus_nds", "sum"),
                    orders=("orders", "sum"),
                )
                cur_m["month_key"] = self.current_month_key
                frames = [x[x["month_key"] != self.current_month_key] for x in frames]
                frames.append(cur_m)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def get_article_month_plan(self, subject: str, code: str, article: str, fact_fallback: float) -> float:
        val = self.plan_maps["article"].get(article, np.nan)
        if pd.notna(val):
            return float(val)
        val = self.plan_maps["product"].get((subject, code), np.nan)
        if pd.notna(val):
            # if article plan absent, count 100% execution => plan=fact for article
            return float(fact_fallback)
        return float(fact_fallback)

    def get_product_month_plan(self, subject: str, code: str, fact_fallback: float) -> float:
        val = self.plan_maps["product"].get((subject, code), np.nan)
        if pd.notna(val):
            return float(val)
        return float(fact_fallback)

    def get_category_month_plan(self, subject: str, fact_fallback: float) -> float:
        val = self.plan_maps["category"].get(subject, np.nan)
        if pd.notna(val):
            return float(val)
        return float(fact_fallback)

    def hierarchy_block(self, base: pd.DataFrame, value_col: str, col_key: str, columns: List[str], plan_mode: str) -> pd.DataFrame:
        rows = []
        if base.empty:
            return pd.DataFrame()
        if col_key not in base.columns:
            return pd.DataFrame()
        data = base.copy()
        data["subject"] = data["subject"].map(normalize_text)
        data["supplier_article"] = data["supplier_article"].map(clean_article)
        data["code"] = data["code"].astype(str)

        for subject in TARGET_SUBJECTS:
            sg = data[data["subject"] == subject].copy()
            if sg.empty:
                row = {"Категория": subject, "_kind": "category", "_subject": subject}
                for c in columns:
                    row[c] = np.nan
                row["План"] = 0.0
                rows.append(row)
                continue
            cat_values = [float(sg.loc[sg[col_key] == c, value_col].sum()) for c in columns]
            cat_fact_month = float(sg[value_col].sum()) if plan_mode == "month" else float(sg[value_col].sum())
            row = {"Категория": subject, "_kind": "category", "_subject": subject}
            for c, v in zip(columns, cat_values):
                row[c] = v
            if plan_mode == "day":
                row["План"] = self.get_category_month_plan(subject, float(np.nansum(cat_values))) / self.days_in_month
            elif plan_mode == "week":
                row["План"] = self.get_category_month_plan(subject, float(np.nansum(cat_values))) * 7.0 / self.days_in_month
            else:
                row["План"] = self.get_category_month_plan(subject, cat_fact_month)
            rows.append(row)

            prod_order = sg.groupby("code", as_index=False)[value_col].sum().sort_values(value_col, ascending=False)["code"].tolist()
            for code in prod_order:
                pg = sg[sg["code"] == code].copy()
                prod_values = [float(pg.loc[pg[col_key] == c, value_col].sum()) for c in columns]
                prod_fact_month = float(pg[value_col].sum())
                row = {"Категория": str(code), "_kind": "product", "_subject": subject, "_code": code}
                for c, v in zip(columns, prod_values):
                    row[c] = v
                if plan_mode == "day":
                    row["План"] = self.get_product_month_plan(subject, code, float(np.nansum(prod_values))) / self.days_in_month
                elif plan_mode == "week":
                    row["План"] = self.get_product_month_plan(subject, code, float(np.nansum(prod_values))) * 7.0 / self.days_in_month
                else:
                    row["План"] = self.get_product_month_plan(subject, code, prod_fact_month)
                rows.append(row)

                art_order = pg.groupby("supplier_article", as_index=False)[value_col].sum().sort_values(value_col, ascending=False)["supplier_article"].tolist()
                for art in art_order:
                    ag = pg[pg["supplier_article"] == art].copy()
                    art_values = [float(ag.loc[ag[col_key] == c, value_col].sum()) for c in columns]
                    art_fact_month = float(ag[value_col].sum())
                    row = {"Категория": art, "_kind": "article", "_subject": subject, "_code": code, "_article": art}
                    for c, v in zip(columns, art_values):
                        row[c] = v
                    if plan_mode == "day":
                        row["План"] = self.get_article_month_plan(subject, code, art, float(np.nansum(art_values))) / self.days_in_month
                    elif plan_mode == "week":
                        row["План"] = self.get_article_month_plan(subject, code, art, float(np.nansum(art_values))) * 7.0 / self.days_in_month
                    else:
                        row["План"] = self.get_article_month_plan(subject, code, art, art_fact_month)
                    rows.append(row)

        total = {"Категория": "Итого по всем 4 категориям", "_kind": "grand_total"}
        for c in columns:
            total[c] = float(base.loc[base[col_key] == c, value_col].sum()) if not base.empty else 0.0
        total_month_plan = sum(self.plan_maps["category"].values()) if self.plan_maps["category"] else float(np.nansum([total[c] for c in columns]))
        if plan_mode == "day":
            total["План"] = total_month_plan / self.days_in_month
        elif plan_mode == "week":
            total["План"] = total_month_plan * 7.0 / self.days_in_month
        else:
            total["План"] = total_month_plan
        rows.append(total)
        return pd.DataFrame(rows)

    def build(self) -> Dict[str, pd.DataFrame]:
        daily, tech_daily = self.build_current_week_daily()
        week_cols = []
        day_col_names = []
        for d in self.current_week_days:
            day_col_names.append(f"{['Пн','Вт','Ср','Чт','Пт','Сб','Вс'][d.weekday()]} {d.strftime('%d.%m')}")
        if not daily.empty:
            daily["day_col"] = daily["day"].apply(lambda d: f"{['Пн','Вт','Ср','Чт','Пт','Сб','Вс'][pd.Timestamp(d).weekday()]} {pd.Timestamp(d).strftime('%d.%m')}")
        daily_block = self.hierarchy_block(daily, "gross_profit_rub", "day_col", day_col_names, "day") if not daily.empty else pd.DataFrame()

        weekly = self.build_weekly_fact()
        week_order = []
        if not weekly.empty:
            weekly = weekly.sort_values("period_start")
            week_order = weekly[["week_code", "week_label"]].drop_duplicates().sort_values("period_start" if "period_start" in weekly.columns else "week_code")["week_label"].tolist() if False else weekly[["week_code", "week_label"]].drop_duplicates().sort_values("week_code")["week_label"].tolist()
        weekly_block = self.hierarchy_block(weekly, "gp_minus_nds", "week_label", week_order, "week") if not weekly.empty else pd.DataFrame()

        monthly = self.build_monthly_fact()
        month_keys = [(self.latest_day.to_period("M") - 2).strftime("%Y-%m"), (self.latest_day.to_period("M") - 1).strftime("%Y-%m"), self.current_month_key]
        month_labels = {k: pd.Period(k).strftime("%m.%Y") for k in month_keys}
        if not monthly.empty:
            monthly["month_label"] = monthly["month_key"].map(month_labels)
        monthly_block = self.hierarchy_block(monthly, "gp_minus_nds", "month_label", [month_labels[k] for k in month_keys], "month") if not monthly.empty else pd.DataFrame()

        example = self.build_examples()
        return {
            "current_week": daily_block,
            "past_weeks": weekly_block,
            "months": monthly_block,
            "dictionary": self.dict_article,
            "paths": self.data.source_paths,
            "daily_calc": tech_daily.get("daily_calc", pd.DataFrame()),
            "economics_match": tech_daily.get("economics_match", pd.DataFrame()),
            "ads_used": tech_daily.get("ads_used", pd.DataFrame()),
            "orders_used": tech_daily.get("orders_used", pd.DataFrame()),
            "abc_weekly_used": weekly,
            "abc_monthly_used": monthly,
            "plan_used": self.data.plan,
            "warnings": pd.DataFrame({"warning": self.data.warnings}) if self.data.warnings else pd.DataFrame([{"warning": ""}]),
            "example": example,
        }

    def build_examples(self) -> pd.DataFrame:
        daily, _ = self.build_current_week_daily()
        weekly = self.build_weekly_fact()
        ads = self.data.ads_daily.copy()
        ads = self.map_nm_to_article(ads)
        out_rows = []
        articles = set(EXAMPLE_ARTICLES)
        if not weekly.empty:
            for art in articles:
                w = weekly[weekly["supplier_article"] == art].sort_values("period_start").tail(6)
                for r in w.itertuples(index=False):
                    out_rows.append({
                        "Артикул": art,
                        "Период": getattr(r, "week_label"),
                        "Тип": "ABC",
                        "Валовая прибыль": float(getattr(r, "gross_profit")),
                        "Валовая прибыль - НДС": float(getattr(r, "gp_minus_nds")),
                    })
        if not daily.empty:
            d = daily[daily["supplier_article"].isin(articles)].copy()
            for r in d.itertuples(index=False):
                out_rows.append({
                    "Артикул": r.supplier_article,
                    "Период": pd.Timestamp(r.day).strftime("%d.%m"),
                    "Тип": "День",
                    "Выкупленные продажи": float(r.sold_qty),
                    "Выручка": float(r.revenue_rub),
                    "Комиссия WB": float(r.commission_rub),
                    "Эквайринг": float(r.acquiring_rub),
                    "Логистика прямая": float(r.logistics_direct_rub),
                    "Логистика обратная": float(r.logistics_return_rub),
                    "Хранение": float(r.storage_rub),
                    "Прочие расходы": float(r.other_rub),
                    "Себестоимость": float(r.cost_rub),
                    "Реклама": float(r.ad_spend_day),
                    "НДС": float(r.vat_rub),
                    "Валовая прибыль": float(r.gross_profit_rub),
                })
        return pd.DataFrame(out_rows)


def money_fmt(cell) -> None:
    cell.number_format = '# ##0 "₽"'


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
            ws.column_dimensions[get_column_letter(col_idx)].width = 32
        else:
            ws.column_dimensions[get_column_letter(col_idx)].width = min(max(width, 12), 18)


def write_block(ws, start_row: int, title: str, df: pd.DataFrame) -> int:
    if df.empty:
        style_title(ws, start_row, 1, 10, title)
        ws.cell(start_row + 1, 1, "Нет данных")
        return start_row + 3

    visible_cols = [c for c in df.columns if not c.startswith("_")]
    style_title(ws, start_row, 1, len(visible_cols), title)
    hdr = start_row + 1
    for j, col in enumerate(visible_cols, start=1):
        set_header(ws.cell(hdr, j, col))

    row = hdr + 1
    category_children_start = None
    category_children_end = None
    product_children_start = None
    product_children_end = None
    for rec in df.to_dict("records"):
        kind = rec.get("_kind", "")
        if kind == "category":
            if product_children_start is not None and product_children_end is not None and product_children_end >= product_children_start:
                ws.row_dimensions.group(product_children_start, product_children_end, outline_level=2, hidden=True)
            if category_children_start is not None and category_children_end is not None and category_children_end >= category_children_start:
                ws.row_dimensions.group(category_children_start, category_children_end, outline_level=1, hidden=True)
            category_children_start = None
            category_children_end = None
            product_children_start = None
            product_children_end = None
        elif kind == "product":
            if product_children_start is not None and product_children_end is not None and product_children_end >= product_children_start:
                ws.row_dimensions.group(product_children_start, product_children_end, outline_level=2, hidden=True)
            product_children_start = None
            product_children_end = None
            if category_children_start is None:
                category_children_start = row
            category_children_end = row
        elif kind == "article":
            if category_children_start is None:
                category_children_start = row
            category_children_end = row
            if product_children_start is None:
                product_children_start = row
            product_children_end = row

        for j, col in enumerate(visible_cols, start=1):
            c = ws.cell(row, j, rec[col])
            c.border = BORDER
            c.alignment = Alignment(horizontal="center", vertical="center")
            if j >= 2 and isinstance(rec[col], (int, float, np.integer, np.floating)) and not pd.isna(rec[col]):
                money_fmt(c)
        if kind == "category":
            for j in range(1, len(visible_cols) + 1):
                ws.cell(row, j).fill = FILL_CATEGORY
                ws.cell(row, j).font = Font(bold=True)
        elif kind == "product":
            for j in range(1, len(visible_cols) + 1):
                ws.cell(row, j).fill = FILL_PRODUCT
                ws.cell(row, j).font = Font(bold=True, italic=True)
        elif kind == "grand_total":
            for j in range(1, len(visible_cols) + 1):
                ws.cell(row, j).fill = FILL_TOTAL
                ws.cell(row, j).font = Font(bold=True)
        row += 1

    if product_children_start is not None and product_children_end is not None and product_children_end >= product_children_start:
        ws.row_dimensions.group(product_children_start, product_children_end, outline_level=2, hidden=True)
    if category_children_start is not None and category_children_end is not None and category_children_end >= category_children_start:
        ws.row_dimensions.group(category_children_start, category_children_end, outline_level=1, hidden=True)
    ws.sheet_properties.outlinePr.summaryBelow = False
    return row + 2


def write_df_sheet(wb: Workbook, title: str, df: pd.DataFrame) -> None:
    ws = wb.create_sheet(title[:31])
    if df is None or df.empty:
        ws.cell(1, 1, "Нет данных")
        return
    for j, col in enumerate(df.columns, start=1):
        set_header(ws.cell(1, j, col))
    for i, vals in enumerate(df.itertuples(index=False), start=2):
        for j, val in enumerate(vals, start=1):
            c = ws.cell(i, j, val)
            c.border = BORDER
            c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            if isinstance(val, (int, float, np.integer, np.floating)) and not pd.isna(val):
                name = df.columns[j - 1].lower()
                if "став" in name or "цена" in name or "расход" in name or "прибыл" in name or "выруч" in name or "ндс" in name or "план" in name or "комис" in name:
                    money_fmt(c)
    autofit(ws)
    ws.freeze_panes = "A2"


def export_report(blocks: Dict[str, pd.DataFrame], report_path: str, tech_path: str, example_path: str) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = "Сводка"
    row = 1
    row = write_block(ws, row, "Текущая неделя", blocks["current_week"])
    row = write_block(ws, row, "Прошлые недели", blocks["past_weeks"])
    row = write_block(ws, row, "Месяцы", blocks["months"])
    ws.freeze_panes = "A3"
    autofit(ws)
    wb.save(report_path)

    twb = Workbook()
    twb.remove(twb.active)
    for name in ["dictionary", "paths", "orders_used", "ads_used", "economics_match", "daily_calc", "abc_weekly_used", "abc_monthly_used", "plan_used", "warnings"]:
        write_df_sheet(twb, name, blocks.get(name, pd.DataFrame()))
    twb.save(tech_path)

    ewb = Workbook()
    ewb.remove(ewb.active)
    example = blocks.get("example", pd.DataFrame())
    if example is None or example.empty:
        ws = ewb.create_sheet("Пример")
        ws.cell(1, 1, "Нет данных")
    else:
        for art in EXAMPLE_ARTICLES:
            write_df_sheet(ewb, art.replace("/", "_"), example[example["Артикул"] == art].copy())
    ewb.save(example_path)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--root", default=".")
    p.add_argument("--reports-root", default="Отчёты")
    p.add_argument("--store", default=STORE)
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

    stamp = pd.Timestamp(data.latest_day).strftime("%Y-%m-%d")
    report_key = f"{args.out_subdir}/Объединенный_отчет_{args.store}_{stamp}.xlsx"
    tech_key = f"{args.out_subdir}/Технические_расчеты_{args.store}_{stamp}.xlsx"
    example_key = f"{args.out_subdir}/Пример_расчета_901_{args.store}_{stamp}.xlsx"

    local_report = Path("/tmp") / f"wb_report_{stamp}.xlsx"
    local_tech = Path("/tmp") / f"wb_tech_{stamp}.xlsx"
    local_example = Path("/tmp") / f"wb_example_{stamp}.xlsx"
    export_report(blocks, str(local_report), str(local_tech), str(local_example))

    storage.write_bytes(report_key, local_report.read_bytes())
    storage.write_bytes(tech_key, local_tech.read_bytes())
    storage.write_bytes(example_key, local_example.read_bytes())
    log(f"Saved report: {report_key}")
    log(f"Saved technical workbook: {tech_key}")
    log(f"Saved example workbook: {example_key}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
