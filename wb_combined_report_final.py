
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import calendar
import io
import math
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

# =========================
# Constants
# =========================

TARGET_SUBJECTS = [
    "Кисти косметические",
    "Помады",
    "Блески",
    "Косметические карандаши",
]

EXCLUDED_ARTICLES = {
    "CZ420", "CZ420БРОВИ", "CZ420ГЛАЗА",
    "DE49", "DE49ГЛАЗА",
    "PT901",  # исключаем именно PT901 как standalone article
}

HEADER_FILL = PatternFill("solid", fgColor="D9EAF7")
SUBHEADER_FILL = PatternFill("solid", fgColor="EAF2F8")
THIN = Side(style="thin", color="C7C7C7")
THIN_BORDER = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)

# =========================
# Logging / helpers
# =========================

def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)

def normalize_text(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).replace("\xa0", " ").strip()
    s = re.sub(r"\s+", " ", s)
    return s

def clean_article(v: Any) -> str:
    s = normalize_text(v)
    if not s or s.lower() in {"nan", "none"}:
        return ""
    return s

def upper_clean(v: Any) -> str:
    return clean_article(v).upper().replace(" ", "")

def norm_key(v: Any) -> str:
    s = normalize_text(v).lower().replace("ё", "е")
    s = re.sub(r"[^\w]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")

def to_dt(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce")

def safe_div(a: Any, b: Any) -> float:
    try:
        a = float(a)
        b = float(b)
    except Exception:
        return np.nan
    if b == 0 or pd.isna(b):
        return np.nan
    return a / b

def pct_delta(cur: Any, prev: Any) -> float:
    if pd.isna(cur) or pd.isna(prev) or prev == 0:
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
    y = int(m.group(1))
    w = int(m.group(2))
    return date.fromisocalendar(y, w, 1), date.fromisocalendar(y, w, 7)

def parse_week_from_name(name: str) -> Optional[str]:
    m = re.search(r"(\d{4})-W(\d{2})", name)
    return f"{m.group(1)}-W{m.group(2)}" if m else None

def parse_period_from_entry_name(name: str) -> Tuple[Optional[date], Optional[date]]:
    m = re.search(r"с (\d{2})-(\d{2})-(\d{4}) по (\d{2})-(\d{2})-(\d{4})", name)
    if not m:
        return None, None
    return (
        date(int(m.group(3)), int(m.group(2)), int(m.group(1))),
        date(int(m.group(6)), int(m.group(5)), int(m.group(4))),
    )

def parse_period_from_abc_name(name: str) -> Tuple[Optional[date], Optional[date]]:
    m = re.search(r"__(\d{2})\.(\d{2})\.(\d{4})-(\d{2})\.(\d{2})\.(\d{4})__", name)
    if not m:
        return None, None
    return (
        date(int(m.group(3)), int(m.group(2)), int(m.group(1))),
        date(int(m.group(6)), int(m.group(5)), int(m.group(4))),
    )

def pick_sheet(sheet_names: List[str], preferred: Iterable[str]) -> Any:
    if not sheet_names:
        return 0
    by_norm = {norm_key(x): x for x in sheet_names}
    for p in preferred:
        if norm_key(p) in by_norm:
            return by_norm[norm_key(p)]
    return sheet_names[0]

def dedupe_columns(cols: Iterable[Any]) -> List[str]:
    out = []
    cnt: Dict[str, int] = {}
    for c in cols:
        base = normalize_text(c) or "unnamed"
        cnt[base] = cnt.get(base, 0) + 1
        out.append(base if cnt[base] == 1 else f"{base}__{cnt[base]}")
    return out

def read_excel_flexible(data: bytes, preferred_sheets: Iterable[str], headers=(0,1,2)) -> Tuple[pd.DataFrame, str, int]:
    xl = pd.ExcelFile(io.BytesIO(data))
    sheet = pick_sheet(xl.sheet_names, preferred_sheets)
    best_df = None
    best_header = 0
    best_score = -1
    for h in headers:
        try:
            df = xl.parse(sheet_name=sheet, header=h, dtype=object)
            df.columns = dedupe_columns(df.columns)
            df = df.dropna(how="all").dropna(axis=1, how="all")
            score = len(df.columns) + (100 if not df.empty else 0)
            if score > best_score:
                best_score = score
                best_df = df
                best_header = h
        except Exception:
            continue
    if best_df is None:
        raise ValueError(f"Не удалось прочитать Excel sheet={sheet}")
    return best_df, str(sheet), best_header

def coalesce_columns(df: pd.DataFrame, target: str, candidates: List[str]) -> pd.DataFrame:
    if target not in df.columns:
        df[target] = np.nan
    for c in candidates:
        if c in df.columns:
            if df[target].dtype == object:
                mask = df[target].isna() | (df[target].astype(str).str.strip() == "") | (df[target].astype(str).str.lower() == "nan")
            else:
                mask = df[target].isna()
            df.loc[mask, target] = df.loc[mask, c]
    return df

def series_or_blank(df: pd.DataFrame, col: str) -> pd.Series:
    return df[col] if col in df.columns else pd.Series([""] * len(df), index=df.index)

# ---------- article / code / RRP normalization ----------

def code_from_article(article: Any) -> str:
    s = upper_clean(article)
    if not s:
        return ""
    # normalize pt901.f25 / PT901.F25 -> 901
    m = re.match(r"^PT?(\d+)", s)
    if m:
        return str(int(m.group(1)))
    m = re.match(r"^(\d+)", s)
    if m:
        return str(int(m.group(1)))
    return ""

def canonical_article(article: Any) -> str:
    s = upper_clean(article)
    if not s:
        return ""
    # direct PT pattern
    m = re.match(r"^PT(\d+)\.F(\d{1,2})$", s)
    if m:
        return f"{int(m.group(1))}/{int(m.group(2))}"
    m = re.match(r"^PT(\d+)\.(\d{1,3})$", s)
    if m:
        return f"{int(m.group(1))}/{int(m.group(2))}"
    # already slash article
    m = re.match(r"^(\d+)\/([A-Z]?\d{1,3})$", s)
    if m:
        second = m.group(2)
        mm = re.match(r"^F?(\d{1,3})$", second)
        if mm:
            return f"{int(m.group(1))}/{int(mm.group(1))}"
        return f"{int(m.group(1))}/{second}"
    # 901_5 or 901-5
    m = re.match(r"^(\d+)[_\-]F?(\d{1,3})$", s)
    if m:
        return f"{int(m.group(1))}/{int(m.group(2))}"
    return clean_article(article)

def canonical_rrp_key(article: Any) -> str:
    s = upper_clean(article)
    if not s:
        return ""
    m = re.match(r"^PT(\d+)\.F(\d{1,2})$", s)
    if m:
        return f"PT{int(m.group(1)):03d}.F{int(m.group(2)):02d}"
    m = re.match(r"^PT(\d+)\.(\d{1,3})$", s)
    if m:
        return f"PT{int(m.group(1)):03d}.{int(m.group(2)):03d}"
    s2 = canonical_article(article).upper()
    m = re.match(r"^(\d+)\/(\d+)$", s2)
    if m:
        code = int(m.group(1))
        shade = int(m.group(2))
        if code == 901:
            return f"PT{code:03d}.F{shade:02d}"
        return f"PT{code:03d}.{shade:03d}"
    return s

def is_excluded_article(article: Any) -> bool:
    s = upper_clean(article)
    return s in EXCLUDED_ARTICLES

# =========================
# Storage
# =========================

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
    def _abs(self, rel: str) -> Path:
        return self.root / rel
    def list_files(self, prefix: str) -> List[str]:
        prefix = prefix.replace("\\", "/").rstrip("/")
        p = self._abs(prefix)
        base = p if p.exists() else p.parent
        if not base.exists():
            return []
        out = []
        for f in base.rglob("*"):
            if f.is_file():
                rel = str(f.relative_to(self.root)).replace("\\", "/")
                if rel.startswith(prefix):
                    out.append(rel)
        return sorted(out)
    def read_bytes(self, path: str) -> bytes:
        return self._abs(path).read_bytes()
    def write_bytes(self, path: str, data: bytes) -> None:
        p = self._abs(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
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
        out = []
        token = None
        while True:
            kwargs = {"Bucket": self.bucket, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            resp = self.s3.list_objects_v2(**kwargs)
            for x in resp.get("Contents", []):
                k = x["Key"]
                if not k.endswith("/"):
                    out.append(k)
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

# =========================
# Loaded data
# =========================

@dataclass
class LoadedData:
    orders: pd.DataFrame
    stocks: pd.DataFrame
    search: pd.DataFrame
    funnel: pd.DataFrame
    ads_daily: pd.DataFrame
    abc: pd.DataFrame
    economics_unit: pd.DataFrame
    entry_cat: pd.DataFrame
    entry_sku: pd.DataFrame
    rrp: pd.DataFrame
    warnings: List[str]

# =========================
# Loader
# =========================

class Loader:
    def __init__(self, storage: BaseStorage, store: str, reports_root: str):
        self.storage = storage
        self.store = store
        self.reports_root = reports_root.rstrip("/")
        self.warnings: List[str] = []

    def _prefix(self, *parts: str) -> str:
        return "/".join([self.reports_root, *parts]).replace("//", "/")

    def _safe_read(self, path: str, preferred_sheets: Iterable[str], headers=(0,1,2)) -> pd.DataFrame:
        df, _, _ = read_excel_flexible(self.storage.read_bytes(path), preferred_sheets, headers=headers)
        df.columns = dedupe_columns(df.columns)
        return df

    def _filter_subjects(self, df: pd.DataFrame, subject_col: str = "subject") -> pd.DataFrame:
        if df.empty or subject_col not in df.columns:
            return df
        df[subject_col] = series_or_blank(df, subject_col).map(normalize_text)
        return df[df[subject_col].isin(TARGET_SUBJECTS)].copy()

    def _limit_week_files(self, files: List[str], n: int = 8) -> List[str]:
        items = []
        for f in files:
            wk = parse_week_from_name(Path(f).name)
            if wk:
                _, end = week_bounds_from_code(wk)
                items.append((end or date.min, f))
        if items:
            return [x[1] for x in sorted(items)[-n:]]
        return sorted(files)[-n:]

    def load_orders(self) -> pd.DataFrame:
        log("Loading orders")
        files = self.storage.list_files(self._prefix("Заказы", self.store, "Недельные"))
        files = [f for f in files if f.lower().endswith(".xlsx")]
        files = self._limit_week_files(files, 8)
        out = []
        for f in files:
            try:
                df = self._safe_read(f, ["Заказы", "sheet1", "Sheet1"], headers=(0,1,2))
                lower = {norm_key(c): c for c in df.columns}
                cols = {
                    "day": lower.get("date") or lower.get("дата") or lower.get("дата заказа"),
                    "warehouse": lower.get("warehousename") or lower.get("склад"),
                    "region": lower.get("regionname") or lower.get("регион"),
                    "supplier_article": lower.get("supplierarticle") or lower.get("артикул продавца"),
                    "nm_id": lower.get("nmid") or lower.get("артикул wb"),
                    "subject": lower.get("subject") or lower.get("предмет"),
                    "brand": lower.get("brand") or lower.get("бренд"),
                    "finished_price": lower.get("finishedprice"),
                    "price_with_disc": lower.get("pricewithdisc"),
                    "spp": lower.get("spp") or lower.get("спп"),
                    "is_cancel": lower.get("iscancel") or lower.get("отмена заказа"),
                }
                x = pd.DataFrame({
                    "day": to_dt(df[cols["day"]]).dt.normalize() if cols["day"] else pd.NaT,
                    "warehouse": df[cols["warehouse"]].map(normalize_text) if cols["warehouse"] else "",
                    "region": df[cols["region"]].map(normalize_text) if cols["region"] else "",
                    "supplier_article": df[cols["supplier_article"]].map(clean_article) if cols["supplier_article"] else "",
                    "nm_id": to_num(df[cols["nm_id"]]) if cols["nm_id"] else np.nan,
                    "subject": df[cols["subject"]].map(normalize_text) if cols["subject"] else "",
                    "brand": df[cols["brand"]].map(normalize_text) if cols["brand"] else "",
                    "finished_price": to_num(df[cols["finished_price"]]) if cols["finished_price"] else np.nan,
                    "price_with_disc": to_num(df[cols["price_with_disc"]]) if cols["price_with_disc"] else np.nan,
                    "spp": to_num(df[cols["spp"]]) if cols["spp"] else np.nan,
                    "is_cancel": to_num(df[cols["is_cancel"]]) if cols["is_cancel"] else 0,
                })
                x["week_code"] = parse_week_from_name(Path(f).name)
                x = x[(x["day"].notna()) & (x["supplier_article"] != "")].copy()
                x = x[~x["supplier_article"].map(is_excluded_article)]
                x["code"] = x["supplier_article"].map(code_from_article)
                out.append(x)
            except Exception as e:
                self.warnings.append(f"Orders read error {f}: {e}")
        if not out:
            return pd.DataFrame(columns=["day","warehouse","supplier_article","nm_id","subject","brand","finished_price","price_with_disc","spp","code"])
        df = pd.concat(out, ignore_index=True)
        return self._filter_subjects(df)

    def load_stocks(self) -> pd.DataFrame:
        log("Loading stocks")
        files = self.storage.list_files(self._prefix("Остатки", self.store, "Недельные"))
        files = [f for f in files if f.lower().endswith(".xlsx")]
        files = self._limit_week_files(files, 6)
        out = []
        for f in files:
            try:
                df = self._safe_read(f, ["Остатки"], headers=(0,))
                lower = {norm_key(c): c for c in df.columns}
                x = pd.DataFrame({
                    "warehouse": df[lower.get("склад")].map(normalize_text) if lower.get("склад") else "",
                    "supplier_article": df[lower.get("артикул продавца")].map(clean_article) if lower.get("артикул продавца") else "",
                    "nm_id": to_num(df[lower.get("артикул wb")]) if lower.get("артикул wb") else np.nan,
                    "stock_available": to_num(df[lower.get("доступно для продажи")]) if lower.get("доступно для продажи") else 0,
                    "stock_total": to_num(df[lower.get("полное количество")]) if lower.get("полное количество") else 0,
                    "subject": df[lower.get("предмет")].map(normalize_text) if lower.get("предмет") else "",
                    "brand": df[lower.get("бренд")].map(normalize_text) if lower.get("бренд") else "",
                })
                wk = parse_week_from_name(Path(f).name)
                ws, we = week_bounds_from_code(wk) if wk else (None, None)
                x["week_code"] = wk
                x["week_start"] = pd.Timestamp(ws) if ws else pd.NaT
                x["week_end"] = pd.Timestamp(we) if we else pd.NaT
                x = x[(x["supplier_article"] != "")].copy()
                x = x[~x["supplier_article"].map(is_excluded_article)]
                x["code"] = x["supplier_article"].map(code_from_article)
                out.append(x)
            except Exception as e:
                self.warnings.append(f"Stocks read error {f}: {e}")
        if not out:
            return pd.DataFrame()
        df = pd.concat(out, ignore_index=True)
        return self._filter_subjects(df)

    def load_search(self) -> pd.DataFrame:
        log("Loading search")
        files = self.storage.list_files(self._prefix("Поисковые запросы", self.store, "Недельные"))
        files = [f for f in files if f.lower().endswith(".xlsx")]
        files = self._limit_week_files(files, 8)
        out = []
        for f in files:
            try:
                df = self._safe_read(f, ["Поисковые запросы", "sheet1", "Sheet1"], headers=(0,1))
                lower = {norm_key(c): c for c in df.columns}
                x = pd.DataFrame({
                    "day": to_dt(df[lower.get("дата")]).dt.normalize() if lower.get("дата") else pd.NaT,
                    "query": df[lower.get("поисковый запрос")].map(normalize_text) if lower.get("поисковый запрос") else "",
                    "supplier_article": df[lower.get("артикул продавца")].map(clean_article) if lower.get("артикул продавца") else "",
                    "nm_id": to_num(df[lower.get("артикул wb")]) if lower.get("артикул wb") else np.nan,
                    "subject": df[lower.get("предмет")].map(normalize_text) if lower.get("предмет") else "",
                    "brand": df[lower.get("бренд")].map(normalize_text) if lower.get("бренд") else "",
                    "frequency": to_num(df[lower.get("частота запросов")]) if lower.get("частота запросов") else np.nan,
                    "median_position": to_num(df[lower.get("медианная позиция")]) if lower.get("медианная позиция") else np.nan,
                    "visibility_pct": to_num(df[lower.get("видимость")]) if lower.get("видимость") else np.nan,
                })
                x["week_code"] = parse_week_from_name(Path(f).name)
                x = x[(x["day"].notna()) & (x["supplier_article"] != "")].copy()
                x = x[~x["supplier_article"].map(is_excluded_article)]
                x["code"] = x["supplier_article"].map(code_from_article)
                out.append(x)
            except Exception as e:
                self.warnings.append(f"Search read error {f}: {e}")
        if not out:
            return pd.DataFrame()
        df = pd.concat(out, ignore_index=True)
        return self._filter_subjects(df)

    def load_funnel(self) -> pd.DataFrame:
        log("Loading funnel")
        paths = [
            self._prefix("Воронка продаж", self.store, "Воронка продаж.xlsx"),
            self._prefix("Воронка продаж", "Воронка продаж.xlsx"),
        ]
        path = next((p for p in paths if self.storage.exists(p)), None)
        if not path:
            self.warnings.append("Funnel file not found")
            return pd.DataFrame()
        try:
            df = self._safe_read(path, ["Воронка продаж", "Sheet1"], headers=(0,))
            lower = {norm_key(c): c for c in df.columns}
            x = pd.DataFrame({
                "day": to_dt(df[lower.get("dt") or lower.get("дата")]).dt.normalize(),
                "nm_id": to_num(df[lower.get("nmid") or lower.get("артикул wb")]),
                "open_card_count": to_num(df[lower.get("opencardcount") or lower.get("открытие карточки")]),
                "cart_count": to_num(df[lower.get("addtocartcount") or lower.get("добавили в корзину")]),
                "orders_funnel": to_num(df[lower.get("orderscount") or lower.get("заказы")]),
                "buyouts_funnel": to_num(df[lower.get("buyoutscount")]),
                "cancel_funnel": to_num(df[lower.get("cancelcount")]),
                "conv_to_cart": to_num(df[lower.get("addtocartconversion") or lower.get("конверсия в корзину")]),
                "conv_cart_to_order": to_num(df[lower.get("carttoorderconversion") or lower.get("конверсия в заказ")]),
            })
            x = x[x["day"].notna()].copy()
            return x
        except Exception as e:
            self.warnings.append(f"Funnel read error {path}: {e}")
            return pd.DataFrame()

    def load_ads_daily(self) -> pd.DataFrame:
        log("Loading ads")
        # prefer weekly files; stable structure
        files = self.storage.list_files(self._prefix("Реклама", self.store, "Недельные"))
        files = [f for f in files if f.lower().endswith(".xlsx")]
        files = self._limit_week_files(files, 8)
        out = []
        for f in files:
            try:
                df = self._safe_read(f, ["Статистика_Ежедневно"], headers=(0,))
                lower = {norm_key(c): c for c in df.columns}
                x = pd.DataFrame({
                    "day": to_dt(df[lower.get("дата")]).dt.normalize() if lower.get("дата") else pd.NaT,
                    "nm_id": to_num(df[lower.get("артикул wb")]) if lower.get("артикул wb") else np.nan,
                    "subject": df[lower.get("название предмета")].map(normalize_text) if lower.get("название предмета") else "",
                    "campaign_name": df[lower.get("название")].map(normalize_text) if lower.get("название") else "",
                    "ad_impressions": to_num(df[lower.get("показы")]) if lower.get("показы") else 0,
                    "ad_clicks": to_num(df[lower.get("клики")]) if lower.get("клики") else 0,
                    "ad_orders": to_num(df[lower.get("заказы")]) if lower.get("заказы") else 0,
                    "ad_spend": to_num(df[lower.get("расход")]) if lower.get("расход") else 0,
                    "ad_ctr": to_num(df[lower.get("ctr")]) if lower.get("ctr") else np.nan,
                    "ad_cpc": to_num(df[lower.get("cpc")]) if lower.get("cpc") else np.nan,
                })
                x = x[x["day"].notna()].copy()
                out.append(x)
            except Exception as e:
                self.warnings.append(f"Ads read error {f}: {e}")
        if not out:
            return pd.DataFrame()
        df = pd.concat(out, ignore_index=True)
        return self._filter_subjects(df)

    def load_economics_unit(self) -> pd.DataFrame:
        log("Loading economics")
        paths = [
            self._prefix("Финансовые показатели", self.store, "Экономика.xlsx"),
            self._prefix("Финансовые показатели", self.store, "Недельные", "Экономика.xlsx"),
        ]
        path = next((p for p in paths if self.storage.exists(p)), None)
        if not path:
            self.warnings.append("Economics file not found")
            return pd.DataFrame()
        try:
            df = self._safe_read(path, ["Юнит экономика"], headers=(0,))
            lower = {norm_key(c): c for c in df.columns}
            def col(name: str) -> Optional[str]:
                return lower.get(norm_key(name))
            x = pd.DataFrame({
                "week_code": df[col("Неделя")].map(normalize_text),
                "nm_id": to_num(df[col("Артикул WB")]) if col("Артикул WB") else np.nan,
                "supplier_article": df[col("Артикул продавца")].map(clean_article) if col("Артикул продавца") else "",
                "subject": df[col("Предмет")].map(normalize_text) if col("Предмет") else "",
                "brand": df[col("Бренд")].map(normalize_text) if col("Бренд") else "",
                "clean_sales_qty": to_num(df[col("Чистые продажи, шт")]) if col("Чистые продажи, шт") else np.nan,
                "buyout_pct": to_num(df[col("Процент выкупа")]) if col("Процент выкупа") else np.nan,
                "sale_price_unit": to_num(df[col("Средняя цена продажи")]) if col("Средняя цена продажи") else np.nan,
                "buyer_price_unit": to_num(df[col("Средняя цена покупателя")]) if col("Средняя цена покупателя") else np.nan,
                "spp_unit_pct": to_num(df[col("СПП, %")]) if col("СПП, %") else np.nan,
                "commission_unit": to_num(df[col("Комиссия WB, руб/ед")]) if col("Комиссия WB, руб/ед") else np.nan,
                "acquiring_unit": to_num(df[col("Эквайринг, руб/ед")]) if col("Эквайринг, руб/ед") else np.nan,
                "logistics_direct_unit": to_num(df[col("Логистика прямая, руб/ед")]) if col("Логистика прямая, руб/ед") else np.nan,
                "logistics_return_unit": to_num(df[col("Логистика обратная, руб/ед")]) if col("Логистика обратная, руб/ед") else np.nan,
                "storage_unit": to_num(df[col("Хранение, руб/ед")]) if col("Хранение, руб/ед") else np.nan,
                "acceptance_unit": to_num(df[col("Приёмка, руб/ед")]) if col("Приёмка, руб/ед") else np.nan,
                "fines_unit": to_num(df[col("Штрафы и удержания, руб/ед")]) if col("Штрафы и удержания, руб/ед") else np.nan,
                "ads_unit": to_num(df[col("Реклама, руб/ед")]) if col("Реклама, руб/ед") else np.nan,
                "other_unit": to_num(df[col("Прочие расходы, руб/ед")]) if col("Прочие расходы, руб/ед") else np.nan,
                "cost_unit": to_num(df[col("Себестоимость, руб")]) if col("Себестоимость, руб") else np.nan,
                "gp_unit": to_num(df[col("Валовая прибыль, руб/ед")]) if col("Валовая прибыль, руб/ед") else np.nan,
                "np_unit": to_num(df[col("Чистая прибыль, руб/ед")]) if col("Чистая прибыль, руб/ед") else np.nan,
                "margin_pct": to_num(df[col("Валовая рентабельность, %")]) if col("Валовая рентабельность, %") else np.nan,
                "profitability_pct": to_num(df[col("Чистая рентабельность, %")]) if col("Чистая рентабельность, %") else np.nan,
            })
            x = x[(x["supplier_article"] != "")].copy()
            x = x[~x["supplier_article"].map(is_excluded_article)]
            x["code"] = x["supplier_article"].map(code_from_article)
            return self._filter_subjects(x)
        except Exception as e:
            self.warnings.append(f"Economics read error {path}: {e}")
            return pd.DataFrame()

    def load_abc(self) -> pd.DataFrame:
        log("Loading ABC")
        files = self.storage.list_files(self._prefix("ABC"))
        files = [f for f in files if "wb_abc_report_goods__" in Path(f).name and f.lower().endswith(".xlsx")]
        files = sorted(files, key=lambda x: parse_period_from_abc_name(Path(x).name)[1] or date.min)[-8:]
        out = []
        for f in files:
            try:
                df = self._safe_read(f, ["sheet1", "Sheet1"], headers=(0,))
                lower = {norm_key(c): c for c in df.columns}
                ws, we = parse_period_from_abc_name(Path(f).name)
                x = pd.DataFrame({
                    "week_code": week_code_from_date(pd.Timestamp(ws)) if ws else None,
                    "week_start": pd.Timestamp(ws) if ws else pd.NaT,
                    "week_end": pd.Timestamp(we) if we else pd.NaT,
                    "nm_id": to_num(df[lower.get("артикул wb")]) if lower.get("артикул wb") else np.nan,
                    "supplier_article": df[lower.get("артикул продавца")].map(clean_article) if lower.get("артикул продавца") else "",
                    "subject": df[lower.get("предмет")].map(normalize_text) if lower.get("предмет") else "",
                    "brand": df[lower.get("бренд")].map(normalize_text) if lower.get("бренд") else "",
                    "abc_class": df[lower.get("abc анализ")].map(normalize_text) if lower.get("abc анализ") else "",
                    "gross_profit": to_num(df[lower.get("валовая прибыль")]) if lower.get("валовая прибыль") else np.nan,
                    "gross_revenue": to_num(df[lower.get("валовая выручка")]) if lower.get("валовая выручка") else np.nan,
                    "abc_orders": to_num(df[lower.get("кол во продаж") or lower.get("заказы")]) if (lower.get("кол во продаж") or lower.get("заказы")) else np.nan,
                    "drr_pct": to_num(df[lower.get("дрр") or lower.get("дрр %")]) if (lower.get("дрр") or lower.get("дрр %")) else np.nan,
                    "margin_pct_abc": to_num(df[lower.get("маржинальность") or lower.get("маржинальность %")]) if (lower.get("маржинальность") or lower.get("маржинальность %")) else np.nan,
                    "profitability_pct_abc": to_num(df[lower.get("рентабельность") or lower.get("рентабельность %")]) if (lower.get("рентабельность") or lower.get("рентабельность %")) else np.nan,
                })
                x = x[(x["supplier_article"] != "")].copy()
                x = x[~x["supplier_article"].map(is_excluded_article)]
                x["code"] = x["supplier_article"].map(code_from_article)
                out.append(x)
            except Exception as e:
                self.warnings.append(f"ABC read error {f}: {e}")
        if not out:
            return pd.DataFrame()
        df = pd.concat(out, ignore_index=True)
        return self._filter_subjects(df)

    def load_entry_points(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        log("Loading entry points")
        files = self.storage.list_files(self._prefix("Точки входа", self.store))
        files = [f for f in files if f.lower().endswith(".xlsx")]
        files = sorted(files, key=lambda x: parse_period_from_entry_name(Path(x).name)[1] or date.min)[-8:]
        cat_out, sku_out = [], []
        for f in files:
            log(f"  entry workbook: {Path(f).name}")
            start, end = parse_period_from_entry_name(Path(f).name)
            week_code = week_code_from_date(pd.Timestamp(start)) if start else None
            for preferred, target in [
                (["Детализация по точкам входа"], "cat"),
                (["Детализация по артикулам"], "sku"),
            ]:
                try:
                    df = self._safe_read(f, preferred, headers=(1,0,2))
                    lower = {norm_key(c): c for c in df.columns}
                    common = {
                        "week_code": week_code,
                        "week_start": pd.Timestamp(start) if start else pd.NaT,
                        "week_end": pd.Timestamp(end) if end else pd.NaT,
                        "section": df[lower.get("раздел")].map(normalize_text) if lower.get("раздел") else "",
                        "entry_point": df[lower.get("точка входа")].map(normalize_text) if lower.get("точка входа") else "",
                        "impressions": to_num(df[lower.get("показы")]) if lower.get("показы") else np.nan,
                        "clicks": to_num(df[lower.get("перешли в карточку")]) if lower.get("перешли в карточку") else np.nan,
                        "ctr": to_num(df[lower.get("ctr")]) if lower.get("ctr") else np.nan,
                        "cart_count": to_num(df[lower.get("добавили в корзину")]) if lower.get("добавили в корзину") else np.nan,
                        "conv_to_cart": to_num(df[lower.get("конверсия в корзину")]) if lower.get("конверсия в корзину") else np.nan,
                        "orders": to_num(df[lower.get("заказали")]) if lower.get("заказали") else np.nan,
                        "conv_order": to_num(df[lower.get("конверсия в заказ")]) if lower.get("конверсия в заказ") else np.nan,
                    }
                    x = pd.DataFrame(common)
                    if target == "sku":
                        x["nm_id"] = to_num(df[lower.get("артикул вб")]) if lower.get("артикул вб") else np.nan
                        x["supplier_article"] = df[lower.get("артикул продавца")].map(clean_article) if lower.get("артикул продавца") else ""
                        x["brand"] = df[lower.get("бренд")].map(normalize_text) if lower.get("бренд") else ""
                        x["subject"] = df[lower.get("предмет")].map(normalize_text) if lower.get("предмет") else ""
                        x = x[(x["supplier_article"] != "")].copy()
                        x = x[~x["supplier_article"].map(is_excluded_article)]
                        x["code"] = x["supplier_article"].map(code_from_article)
                        x = self._filter_subjects(x)
                        sku_out.append(x)
                    else:
                        x["subject"] = x["section"]  # section is subject-like for cat sheet
                        x = self._filter_subjects(x, "subject")
                        cat_out.append(x)
                except Exception as e:
                    self.warnings.append(f"Entry points read error {f}: {e}")
        return (
            pd.concat(cat_out, ignore_index=True) if cat_out else pd.DataFrame(),
            pd.concat(sku_out, ignore_index=True) if sku_out else pd.DataFrame(),
        )

    def load_rrp(self) -> pd.DataFrame:
        log("Loading RRP")
        paths = [
            self._prefix("Финансовые показатели", self.store, "РРЦ.xlsx"),
            self._prefix("Финансовые показатели", self.store, "RRP.xlsx"),
        ]
        path = next((p for p in paths if self.storage.exists(p)), None)
        if not path:
            self.warnings.append("RRP file not found")
            return pd.DataFrame(columns=["rrp_key", "rrp"])
        try:
            df = self._safe_read(path, ["TF"], headers=(0,))
            lower = {norm_key(c): c for c in df.columns}
            art_col = lower.get("правильный артикул") or list(df.columns)[0]
            rrp_col = lower.get("ррц") or list(df.columns)[3]
            x = pd.DataFrame({
                "rrp_key": df[art_col].map(canonical_rrp_key),
                "rrp": to_num(df[rrp_col]),
            })
            x = x.dropna(subset=["rrp"]).copy()
            x = x.groupby("rrp_key", as_index=False)["rrp"].mean()
            return x
        except Exception as e:
            self.warnings.append(f"RRP read error {path}: {e}")
            return pd.DataFrame(columns=["rrp_key", "rrp"])

    def load_all(self) -> LoadedData:
        orders = self.load_orders()
        stocks = self.load_stocks()
        search = self.load_search()
        funnel = self.load_funnel()
        ads_daily = self.load_ads_daily()
        economics_unit = self.load_economics_unit()
        abc = self.load_abc()
        entry_cat, entry_sku = self.load_entry_points()
        rrp = self.load_rrp()
        return LoadedData(
            orders=orders,
            stocks=stocks,
            search=search,
            funnel=funnel,
            ads_daily=ads_daily,
            abc=abc,
            economics_unit=economics_unit,
            entry_cat=entry_cat,
            entry_sku=entry_sku,
            rrp=rrp,
            warnings=self.warnings,
        )

# =========================
# Analyzer
# =========================

class Analyzer:
    def __init__(self, data: LoadedData):
        self.data = data
        self.master = self.build_master()
        self.analysis_end = self.determine_end_day()
        self.windows = self.determine_windows(self.analysis_end)
        log(f"Analysis windows: prev={self.windows['prev_start'].date()}..{self.windows['prev_end'].date()} cur={self.windows['cur_start'].date()}..{self.windows['cur_end'].date()}")
        log("Building daily article")
        self.daily_article = self.build_daily_article()
        log("Allocating GP by day")
        self.article_gp_daily = self.allocate_weekly_gp_daily()
        log("Building localization daily")
        self.localization_daily, self.localization_period = self.build_localization()
        log("Building period tables")
        self.article_period = self.build_article_period()
        self.product_period = self.aggregate_period(self.article_period, "code", "Товар")
        self.category_period = self.aggregate_period(self.article_period, "subject", "Категория")
        log("Building sku contribution")
        self.sku_contrib = self.build_sku_contrib()
        log("Building channels")
        self.channels = self.build_channels()
        log("Building results matrices")
        self.results_category = self.make_matrix("subject", "Категория", self.article_gp_daily, self.windows["cur_dates"])
        self.product_day = self.make_matrix("code", "Товар", self.article_gp_daily, self.windows["cur_dates"])
        self.article_day = self.make_matrix("supplier_article", "Артикул", self.article_gp_daily, self.windows["cur_dates"])
        self.product_week = self.make_week_matrix("code", "Товар")
        self.article_week = self.make_week_matrix("supplier_article", "Артикул")
        self.product_month = self.make_month_matrix("code", "Товар")
        self.article_month = self.make_month_matrix("supplier_article", "Артикул")
        log("Building example 901/5")
        self.example_901_5 = self.build_example_tables("901/5")

    def determine_end_day(self) -> pd.Timestamp:
        candidates = []
        for df, c in [
            (self.data.orders, "day"),
            (self.data.search, "day"),
            (self.data.funnel, "day"),
            (self.data.ads_daily, "day"),
        ]:
            if not df.empty and c in df.columns:
                x = pd.to_datetime(df[c], errors="coerce").max()
                if pd.notna(x):
                    candidates.append(pd.Timestamp(x).normalize())
        if candidates:
            return max(candidates)
        return pd.Timestamp.today().normalize()

    def determine_windows(self, end_day: pd.Timestamp) -> Dict[str, Any]:
        cur_end = end_day.normalize()
        cur_start = cur_end - pd.Timedelta(days=13)
        prev_end = cur_start - pd.Timedelta(days=1)
        prev_start = prev_end - pd.Timedelta(days=13)
        return {
            "cur_start": cur_start,
            "cur_end": cur_end,
            "prev_start": prev_start,
            "prev_end": prev_end,
            "cur_dates": list(pd.date_range(cur_start, cur_end, freq="D")[::-1]),
            "prev_dates": list(pd.date_range(prev_start, prev_end, freq="D")[::-1]),
        }

    def build_master(self) -> pd.DataFrame:
        frames = []
        for df in [self.data.orders, self.data.stocks, self.data.search, self.data.abc, self.data.economics_unit, self.data.entry_sku]:
            if df.empty:
                continue
            cols = [c for c in ["nm_id","supplier_article","subject","brand","code"] if c in df.columns]
            if cols:
                frames.append(df[cols].copy())
        if not frames:
            return pd.DataFrame(columns=["nm_id","supplier_article","subject","brand","code"])
        m = pd.concat(frames, ignore_index=True)
        for c in ["supplier_article","subject","brand","code"]:
            if c in m.columns:
                m[c] = series_or_blank(m, c).map(normalize_text if c in {"subject","brand"} else clean_article)
        if "code" not in m.columns:
            m["code"] = m["supplier_article"].map(code_from_article)
        m = m[(series_or_blank(m, "supplier_article") != "") | m["nm_id"].notna()].copy()
        m = m.sort_values(by=["subject","supplier_article"], na_position="last")
        by_nm = m.dropna(subset=["nm_id"]).drop_duplicates("nm_id")
        by_art = m[m["supplier_article"] != ""].drop_duplicates("supplier_article")
        return {"by_nm": by_nm, "by_art": by_art}

    def attach_master(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        if not out.empty and "nm_id" in out.columns and not self.master["by_nm"].empty:
            out = out.merge(self.master["by_nm"][["nm_id","supplier_article","subject","brand","code"]], on="nm_id", how="left", suffixes=("","_m"))
            for c in ["supplier_article","subject","brand","code"]:
                out = coalesce_columns(out, c, [f"{c}_m"])
                if f"{c}_m" in out.columns:
                    out.drop(columns=[f"{c}_m"], inplace=True)
        if not out.empty and "supplier_article" in out.columns and not self.master["by_art"].empty:
            out = out.merge(self.master["by_art"][["supplier_article","nm_id","subject","brand","code"]], on="supplier_article", how="left", suffixes=("","_a"))
            for c in ["nm_id","subject","brand","code"]:
                out = coalesce_columns(out, c, [f"{c}_a"])
                if f"{c}_a" in out.columns:
                    out.drop(columns=[f"{c}_a"], inplace=True)
        return out

    def build_daily_article(self) -> pd.DataFrame:
        # daily orders / prices
        if self.data.orders.empty:
            oagg = pd.DataFrame(columns=["day","supplier_article","nm_id","subject","brand","code"])
        else:
            o = self.data.orders.copy()
            o = o[(o["day"] >= self.windows["prev_start"]) & (o["day"] <= self.windows["cur_end"])].copy()
            o["orders_lines"] = np.where((o["is_cancel"].fillna(0) == 1), 0, 1)
            oagg = o.groupby(["day","supplier_article","nm_id","subject","brand","code"], dropna=False).agg(
                orders_lines=("orders_lines","sum"),
                finished_price_avg=("finished_price","mean"),
                price_with_disc_avg=("price_with_disc","mean"),
                spp_avg=("spp","mean"),
            ).reset_index()

        # funnel by nm/day
        if self.data.funnel.empty:
            fagg = pd.DataFrame(columns=["day","nm_id"])
        else:
            f = self.data.funnel.copy()
            f = f[(f["day"] >= self.windows["prev_start"]) & (f["day"] <= self.windows["cur_end"])].copy()
            fagg = f.groupby(["day","nm_id"], dropna=False).agg(
                open_card_count=("open_card_count","sum"),
                cart_count=("cart_count","sum"),
                orders_funnel=("orders_funnel","sum"),
                buyouts_funnel=("buyouts_funnel","sum"),
                cancel_funnel=("cancel_funnel","sum"),
                conv_to_cart=("conv_to_cart","mean"),
                conv_cart_to_order=("conv_cart_to_order","mean"),
            ).reset_index()

        # search
        if self.data.search.empty:
            sagg = pd.DataFrame(columns=["day","supplier_article","nm_id"])
        else:
            s = self.data.search.copy()
            s = s[(s["day"] >= self.windows["prev_start"]) & (s["day"] <= self.windows["cur_end"])].copy()
            sagg = s.groupby(["day","supplier_article","nm_id"], dropna=False).agg(
                search_frequency=("frequency","sum"),
                median_position=("median_position","median"),
                visibility_pct=("visibility_pct","mean"),
                search_queries_count=("query","nunique"),
            ).reset_index()

        # ads
        if self.data.ads_daily.empty:
            aagg = pd.DataFrame(columns=["day","nm_id"])
        else:
            a = self.data.ads_daily.copy()
            a = a[(a["day"] >= self.windows["prev_start"]) & (a["day"] <= self.windows["cur_end"])].copy()
            aagg = a.groupby(["day","nm_id"], dropna=False).agg(
                ad_impressions=("ad_impressions","sum"),
                ad_clicks=("ad_clicks","sum"),
                ad_orders=("ad_orders","sum"),
                ad_spend=("ad_spend","sum"),
                ad_ctr=("ad_ctr","mean"),
                ad_cpc=("ad_cpc","mean"),
            ).reset_index()

        # entry sku clicks/orders by day unavailable; keep period-level later
        base = oagg.copy()
        if base.empty and not fagg.empty:
            base = fagg[["day","nm_id"]].drop_duplicates()
        if base.empty and not sagg.empty:
            base = sagg[["day","supplier_article","nm_id"]].drop_duplicates()

        if not fagg.empty:
            base = base.merge(fagg, on=[c for c in ["day","nm_id"] if c in base.columns and c in fagg.columns], how="outer")
        if not sagg.empty:
            base = base.merge(sagg, on=[c for c in ["day","supplier_article","nm_id"] if c in base.columns and c in sagg.columns], how="outer")
        if not aagg.empty:
            base = base.merge(aagg, on=[c for c in ["day","nm_id"] if c in base.columns and c in aagg.columns], how="outer")

        base = self.attach_master(base)
        base["supplier_article"] = series_or_blank(base, "supplier_article").map(clean_article)
        base["subject"] = series_or_blank(base, "subject").map(normalize_text)
        base["brand"] = series_or_blank(base, "brand").map(normalize_text)
        base["code"] = series_or_blank(base, "code")
        base["code"] = np.where(base["code"].astype(str).str.strip()=="", base["supplier_article"].map(code_from_article), base["code"])
        base = base[base["subject"].isin(TARGET_SUBJECTS)].copy()
        base = base[~base["supplier_article"].map(is_excluded_article)].copy()

        for c in ["orders_lines","open_card_count","cart_count","orders_funnel","buyouts_funnel","cancel_funnel","search_frequency","search_queries_count","ad_impressions","ad_clicks","ad_orders","ad_spend"]:
            if c not in base.columns:
                base[c] = 0
            base[c] = to_num(base[c]).fillna(0)
        for c in ["conv_to_cart","conv_cart_to_order","median_position","visibility_pct","ad_ctr","ad_cpc","finished_price_avg","price_with_disc_avg","spp_avg"]:
            if c not in base.columns:
                base[c] = np.nan
            base[c] = to_num(base[c])
        base["week_code"] = base["day"].map(week_code_from_date)
        base["period_name"] = np.where((base["day"] >= self.windows["cur_start"]) & (base["day"] <= self.windows["cur_end"]), "cur_14d", "prev_14d")
        return base

    def allocate_weekly_gp_daily(self) -> pd.DataFrame:
        if self.data.abc.empty:
            return pd.DataFrame(columns=["day","supplier_article","nm_id","subject","code","gp_day_fact","revenue_day_fact","abc_orders_day_fact"])
        # daily shares from funnel/orders
        d = self.daily_article.copy()
        d["alloc_orders"] = np.where(d["orders_funnel"] > 0, d["orders_funnel"], d["orders_lines"])
        daily_week = d.groupby(["week_code","supplier_article","nm_id","subject","brand","code","day"], dropna=False).agg(
            alloc_orders=("alloc_orders","sum"),
        ).reset_index()

        out_rows = []
        abc = self.data.abc.copy()
        abc = abc[(abc["week_end"] >= self.windows["prev_start"]) & (abc["week_start"] <= self.windows["cur_end"])].copy()
        for _, row in abc.iterrows():
            wk = row["week_code"]
            art = row["supplier_article"]
            g = daily_week[(daily_week["week_code"] == wk) & (daily_week["supplier_article"] == art)].copy()
            ws = pd.Timestamp(row["week_start"])
            we = pd.Timestamp(row["week_end"])
            days = pd.date_range(max(ws, self.windows["prev_start"]), min(we, self.windows["cur_end"]), freq="D")
            if g.empty:
                if len(days) == 0:
                    continue
                share = 1.0 / len(days)
                for day in days:
                    out_rows.append({
                        "day": day.normalize(),
                        "supplier_article": art,
                        "nm_id": row["nm_id"],
                        "subject": row["subject"],
                        "brand": row["brand"],
                        "code": row["code"],
                        "gp_day_fact": float(row["gross_profit"]) * share if pd.notna(row["gross_profit"]) else np.nan,
                        "revenue_day_fact": float(row["gross_revenue"]) * share if pd.notna(row["gross_revenue"]) else np.nan,
                        "abc_orders_day_fact": float(row["abc_orders"]) * share if pd.notna(row["abc_orders"]) else np.nan,
                        "week_code": wk,
                    })
                continue
            total = g["alloc_orders"].sum()
            if total <= 0:
                total = len(g)
                g["share"] = 1 / total
            else:
                g["share"] = g["alloc_orders"] / total
            for _, r in g.iterrows():
                out_rows.append({
                    "day": pd.Timestamp(r["day"]).normalize(),
                    "supplier_article": art,
                    "nm_id": row["nm_id"],
                    "subject": row["subject"],
                    "brand": row["brand"],
                    "code": row["code"],
                    "gp_day_fact": float(row["gross_profit"]) * float(r["share"]) if pd.notna(row["gross_profit"]) else np.nan,
                    "revenue_day_fact": float(row["gross_revenue"]) * float(r["share"]) if pd.notna(row["gross_revenue"]) else np.nan,
                    "abc_orders_day_fact": float(row["abc_orders"]) * float(r["share"]) if pd.notna(row["abc_orders"]) else np.nan,
                    "week_code": wk,
                })
        out = pd.DataFrame(out_rows)
        if out.empty:
            return out
        out["period_name"] = np.where((out["day"] >= self.windows["cur_start"]) & (out["day"] <= self.windows["cur_end"]), "cur_14d", "prev_14d")
        return out

    def build_localization(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        # average daily orders per warehouse over 28d lookback
        if self.data.orders.empty or self.data.stocks.empty:
            return pd.DataFrame(), pd.DataFrame()
        orders = self.data.orders.copy()
        lookback_start = self.windows["prev_start"]
        lookback_end = self.windows["cur_end"]
        orders = orders[(orders["day"] >= lookback_start) & (orders["day"] <= lookback_end) & (orders["is_cancel"].fillna(0) != 1)].copy()
        warehouse_avg = orders.groupby(["supplier_article","warehouse"], dropna=False).agg(
            orders_lookback=("supplier_article","size")
        ).reset_index()
        num_days = (lookback_end - lookback_start).days + 1
        warehouse_avg["avg_orders_per_day_warehouse"] = warehouse_avg["orders_lookback"] / max(num_days, 1)

        # main warehouses by latest stock snapshot
        stocks = self.data.stocks.copy()
        latest_week = stocks["week_end"].max()
        latest = stocks[stocks["week_end"] == latest_week].copy()
        main_rows = []
        for art, g in latest.groupby("supplier_article", dropna=False):
            g = g.groupby("warehouse", as_index=False)["stock_available"].sum().sort_values("stock_available", ascending=False)
            total = g["stock_available"].sum()
            if total <= 0:
                continue
            g["share"] = g["stock_available"] / total
            g["cum_share"] = g["share"].cumsum()
            keep = g[g["cum_share"] <= 0.97].copy()
            if keep.empty or keep["cum_share"].max() < 0.97:
                keep = g[g.index <= g[g["cum_share"] >= 0.97].index.min()].copy()
            for _, r in keep.iterrows():
                main_rows.append({"supplier_article": art, "warehouse": r["warehouse"], "main_share_stock": r["share"]})
        main_wh = pd.DataFrame(main_rows)

        # warehouse weight by actual orders
        wh_weight = orders.groupby(["supplier_article","warehouse"], dropna=False).size().reset_index(name="orders_cnt")
        if not wh_weight.empty:
            wh_weight["warehouse_weight"] = wh_weight.groupby("supplier_article")["orders_cnt"].transform(lambda s: s / s.sum())

        # expand weekly stocks to days only over 28 days
        rows = []
        for _, r in stocks.iterrows():
            ws = pd.Timestamp(r["week_start"])
            we = pd.Timestamp(r["week_end"])
            if pd.isna(ws) or pd.isna(we):
                continue
            start = max(ws, lookback_start)
            end = min(we, lookback_end)
            if start > end:
                continue
            if main_wh.empty:
                continue
            for day in pd.date_range(start, end, freq="D"):
                rows.append({
                    "day": day.normalize(),
                    "supplier_article": r["supplier_article"],
                    "warehouse": r["warehouse"],
                    "stock_qty": float(r["stock_available"]) if pd.notna(r["stock_available"]) else 0.0,
                    "subject": r["subject"],
                    "code": r["code"],
                })
        daily = pd.DataFrame(rows)
        if daily.empty:
            return pd.DataFrame(), pd.DataFrame()
        daily = daily.merge(main_wh, on=["supplier_article","warehouse"], how="inner")
        daily = daily.merge(warehouse_avg[["supplier_article","warehouse","avg_orders_per_day_warehouse"]], on=["supplier_article","warehouse"], how="left")
        daily = daily.merge(wh_weight[["supplier_article","warehouse","warehouse_weight"]], on=["supplier_article","warehouse"], how="left")
        daily["avg_orders_per_day_warehouse"] = daily["avg_orders_per_day_warehouse"].fillna(0)
        daily["warehouse_weight"] = daily["warehouse_weight"].fillna(daily["main_share_stock"]).fillna(0)
        daily["coverage_days"] = np.where(daily["avg_orders_per_day_warehouse"] > 0, daily["stock_qty"] / daily["avg_orders_per_day_warehouse"], np.where(daily["stock_qty"] > 0, 999.0, 0.0))
        daily["is_available_flag"] = np.where(
            (daily["avg_orders_per_day_warehouse"] <= 0) & (daily["stock_qty"] > 0), 1,
            np.where(daily["stock_qty"] >= daily["avg_orders_per_day_warehouse"], 1, 0)
        )
        daily["period_name"] = np.where((daily["day"] >= self.windows["cur_start"]) & (daily["day"] <= self.windows["cur_end"]), "cur_14d", "prev_14d")

        period = daily.groupby(["period_name","supplier_article","subject","code"], dropna=False).apply(
            lambda g: pd.Series({
                "localization_coverage_weighted": safe_div((g["warehouse_weight"] * g["is_available_flag"]).sum(), g["warehouse_weight"].sum()),
                "localization_coverage_count": safe_div(g["is_available_flag"].sum(), len(g)),
            })
        ).reset_index()
        return daily, period

    def weighted_period_economics(self) -> pd.DataFrame:
        econ = self.data.economics_unit.copy()
        if econ.empty:
            return pd.DataFrame()
        # map week -> period
        econ["period_name"] = np.where(
            econ["week_code"].isin({week_code_from_date(d) for d in self.windows["cur_dates"]}), "cur_14d",
            np.where(econ["week_code"].isin({week_code_from_date(d) for d in self.windows["prev_dates"]}), "prev_14d", "")
        )
        econ = econ[econ["period_name"] != ""].copy()
        value_cols = ["sale_price_unit","buyer_price_unit","spp_unit_pct","commission_unit","acquiring_unit","logistics_direct_unit","logistics_return_unit","storage_unit","acceptance_unit","fines_unit","ads_unit","other_unit","cost_unit","gp_unit","np_unit","margin_pct","profitability_pct","buyout_pct"]
        rows = []
        for keys, g in econ.groupby(["period_name","supplier_article","nm_id","subject","brand","code"], dropna=False):
            w = to_num(g["clean_sales_qty"]).fillna(0)
            ww = w.sum()
            row = {
                "period_name": keys[0], "supplier_article": keys[1], "nm_id": keys[2],
                "subject": keys[3], "brand": keys[4], "code": keys[5],
            }
            for c in value_cols:
                s = to_num(g[c])
                row[c] = np.average(s.fillna(0), weights=w if ww > 0 else None) if len(s) else np.nan
            rows.append(row)
        return pd.DataFrame(rows)

    def build_article_period(self) -> pd.DataFrame:
        # base from gp daily fact + daily operational metrics
        gp = self.article_gp_daily.copy()
        daily = self.daily_article.copy()

        gp_period = gp.groupby(["period_name","supplier_article","nm_id","subject","brand","code"], dropna=False).agg(
            gp=("gp_day_fact","sum"),
            revenue=("revenue_day_fact","sum"),
            abc_orders=("abc_orders_day_fact","sum"),
        ).reset_index()

        daily_period = daily.groupby(["period_name","supplier_article","nm_id","subject","brand","code"], dropna=False).agg(
            open_card_count=("open_card_count","sum"),
            cart_count=("cart_count","sum"),
            orders_funnel=("orders_funnel","sum"),
            buyouts_funnel=("buyouts_funnel","sum"),
            cancel_funnel=("cancel_funnel","sum"),
            conv_to_cart=("conv_to_cart","mean"),
            conv_cart_to_order=("conv_cart_to_order","mean"),
            finished_price_avg=("finished_price_avg","mean"),
            price_with_disc_avg=("price_with_disc_avg","mean"),
            spp_avg=("spp_avg","mean"),
            search_frequency=("search_frequency","sum"),
            search_queries_count=("search_queries_count","sum"),
            median_position=("median_position","median"),
            visibility_pct=("visibility_pct","mean"),
            ad_impressions=("ad_impressions","sum"),
            ad_clicks=("ad_clicks","sum"),
            ad_orders=("ad_orders","sum"),
            ad_spend=("ad_spend","sum"),
            ad_ctr=("ad_ctr","mean"),
            ad_cpc=("ad_cpc","mean"),
        ).reset_index()

        out = gp_period.merge(daily_period, on=["period_name","supplier_article","nm_id","subject","brand","code"], how="outer")
        out["gp_per_order"] = out.apply(lambda r: safe_div(r.get("gp"), r.get("abc_orders")), axis=1)
        out["revenue_per_order"] = out.apply(lambda r: safe_div(r.get("revenue"), r.get("abc_orders")), axis=1)

        econ = self.weighted_period_economics()
        if not econ.empty:
            out = out.merge(econ, on=["period_name","supplier_article","nm_id","subject","brand","code"], how="left")

        if not self.localization_period.empty:
            out = out.merge(self.localization_period, on=["period_name","supplier_article","subject","code"], how="left")

        # RRP
        if not self.data.rrp.empty:
            out["rrp_key"] = out["supplier_article"].map(canonical_rrp_key)
            out = out.merge(self.data.rrp, on="rrp_key", how="left")
            out["finished_price_rrp_coeff"] = out.apply(lambda r: safe_div(r.get("finished_price_avg"), r.get("rrp")), axis=1)
            out["price_with_disc_rrp_coeff"] = out.apply(lambda r: safe_div(r.get("price_with_disc_avg"), r.get("rrp")), axis=1)
        else:
            out["rrp"] = np.nan
            out["finished_price_rrp_coeff"] = np.nan
            out["price_with_disc_rrp_coeff"] = np.nan

        # period pivot / deltas
        prev = out[out["period_name"] == "prev_14d"].copy()
        cur = out[out["period_name"] == "cur_14d"].copy()
        keys = ["supplier_article","nm_id","subject","brand","code"]
        merged = prev.merge(cur, on=keys, how="outer", suffixes=("_prev","_cur"))
        num_candidates = [
            "gp","revenue","abc_orders","gp_per_order","revenue_per_order",
            "open_card_count","cart_count","orders_funnel","buyouts_funnel","cancel_funnel",
            "conv_to_cart","conv_cart_to_order","finished_price_avg","price_with_disc_avg","spp_avg",
            "search_frequency","search_queries_count","median_position","visibility_pct",
            "ad_impressions","ad_clicks","ad_orders","ad_spend","ad_ctr","ad_cpc",
            "sale_price_unit","buyer_price_unit","spp_unit_pct","commission_unit","acquiring_unit",
            "logistics_direct_unit","logistics_return_unit","storage_unit","acceptance_unit","fines_unit","ads_unit",
            "other_unit","cost_unit","gp_unit","np_unit","margin_pct","profitability_pct","buyout_pct",
            "localization_coverage_weighted","localization_coverage_count","rrp","finished_price_rrp_coeff","price_with_disc_rrp_coeff",
        ]
        for c in num_candidates:
            prev_c, cur_c = f"{c}_prev", f"{c}_cur"
            if prev_c not in merged.columns:
                merged[prev_c] = np.nan
            if cur_c not in merged.columns:
                merged[cur_c] = np.nan
            merged[f"{c}_delta"] = merged[cur_c] - merged[prev_c]
            merged[f"{c}_delta_pct"] = [pct_delta(a,b) for a,b in zip(merged[cur_c], merged[prev_c])]

        # effects
        merged["gp_volume_effect"] = (merged["abc_orders_cur"].fillna(0) - merged["abc_orders_prev"].fillna(0)) * merged["gp_per_order_prev"].fillna(0)
        merged["gp_economy_effect"] = (merged["gp_per_order_cur"].fillna(0) - merged["gp_per_order_prev"].fillna(0)) * merged["abc_orders_cur"].fillna(0)
        merged["revenue_order_effect"] = (merged["abc_orders_cur"].fillna(0) - merged["abc_orders_prev"].fillna(0)) * merged["revenue_per_order_prev"].fillna(0)
        merged["revenue_price_effect"] = (merged["revenue_per_order_cur"].fillna(0) - merged["revenue_per_order_prev"].fillna(0)) * merged["abc_orders_cur"].fillna(0)

        # channel metrics by entry points sku
        entry_sku = self.data.entry_sku.copy()
        if not entry_sku.empty:
            entry_sku["period_name"] = np.where(
                entry_sku["week_code"].isin({week_code_from_date(d) for d in self.windows["cur_dates"]}), "cur_14d",
                np.where(entry_sku["week_code"].isin({week_code_from_date(d) for d in self.windows["prev_dates"]}), "prev_14d", "")
            )
            entry_sku = entry_sku[entry_sku["period_name"] != ""].copy()
            ep = entry_sku.groupby(["period_name","supplier_article"], dropna=False).agg(
                ep_impressions=("impressions","sum"),
                ep_clicks=("clicks","sum"),
                ep_orders=("orders","sum"),
                ep_ctr=("ctr","mean"),
                ep_conv_order=("conv_order","mean"),
            ).reset_index()
            ep_prev = ep[ep["period_name"]=="prev_14d"].drop(columns=["period_name"]).add_suffix("_prev")
            ep_cur = ep[ep["period_name"]=="cur_14d"].drop(columns=["period_name"]).add_suffix("_cur")
            if "supplier_article_prev" in ep_prev.columns:
                ep_prev = ep_prev.rename(columns={"supplier_article_prev":"supplier_article"})
            if "supplier_article_cur" in ep_cur.columns:
                ep_cur = ep_cur.rename(columns={"supplier_article_cur":"supplier_article"})
            merged = merged.merge(ep_prev, on="supplier_article", how="left")
            merged = merged.merge(ep_cur, on="supplier_article", how="left")
            for c in ["ep_impressions","ep_clicks","ep_orders","ep_ctr","ep_conv_order"]:
                merged[f"{c}_delta"] = merged[f"{c}_cur"] - merged[f"{c}_prev"]
                merged[f"{c}_delta_pct"] = [pct_delta(a,b) for a,b in zip(merged[f"{c}_cur"], merged[f"{c}_prev"])]
        else:
            merged["ep_clicks_prev"] = np.nan
            merged["ep_clicks_cur"] = np.nan

        # derived total clicks
        merged["clicks_total_prev"] = merged["ep_clicks_prev"].fillna(merged["open_card_count_prev"])
        merged["clicks_total_cur"] = merged["ep_clicks_cur"].fillna(merged["open_card_count_cur"])
        merged["clicks_total_delta"] = merged["clicks_total_cur"] - merged["clicks_total_prev"]
        merged["clicks_total_delta_pct"] = [pct_delta(a,b) for a,b in zip(merged["clicks_total_cur"], merged["clicks_total_prev"])]
        merged["cr_click_to_order_prev"] = merged.apply(lambda r: safe_div(r["abc_orders_prev"], r["clicks_total_prev"]), axis=1)
        merged["cr_click_to_order_cur"] = merged.apply(lambda r: safe_div(r["abc_orders_cur"], r["clicks_total_cur"]), axis=1)
        merged["cr_click_to_order_delta"] = merged["cr_click_to_order_cur"] - merged["cr_click_to_order_prev"]
        merged["cr_click_to_order_delta_pct"] = [pct_delta(a,b) for a,b in zip(merged["cr_click_to_order_cur"], merged["cr_click_to_order_prev"])]
        merged["orders_traffic_effect"] = (merged["clicks_total_cur"].fillna(0) - merged["clicks_total_prev"].fillna(0)) * merged["cr_click_to_order_prev"].fillna(0)
        merged["orders_conversion_effect"] = (merged["cr_click_to_order_cur"].fillna(0) - merged["cr_click_to_order_prev"].fillna(0)) * merged["clicks_total_cur"].fillna(0)

        # main / secondary reasons
        reasons = []
        for _, r in merged.iterrows():
            main, second = self.detect_reasons(r)
            ad_assess = self.assess_ads(r)
            price_assess = self.assess_price(r)
            reasons.append((main, second, ad_assess, price_assess))
        merged["main_reason"] = [x[0] for x in reasons]
        merged["secondary_reason"] = [x[1] for x in reasons]
        merged["ad_assessment"] = [x[2] for x in reasons]
        merged["price_assessment"] = [x[3] for x in reasons]

        # order of columns kept manageable
        return merged

    def aggregate_period(self, df: pd.DataFrame, group_col: str, entity_label: str) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()
        rows = []
        for key, g in df.groupby(group_col, dropna=False):
            row = {
                entity_label: key,
                "Категория": g["subject"].mode().iat[0] if "subject" in g and not g["subject"].dropna().empty else "",
                "sku_count": g["supplier_article"].nunique(),
            }
            sum_cols = ["gp_prev","gp_cur","revenue_prev","revenue_cur","abc_orders_prev","abc_orders_cur",
                        "clicks_total_prev","clicks_total_cur","ad_spend_prev","ad_spend_cur","ad_orders_prev","ad_orders_cur","ad_clicks_prev","ad_clicks_cur"]
            for c in sum_cols:
                row[c] = g[c].sum(skipna=True)
            # weighted / mean cols
            for base in ["finished_price_avg","price_with_disc_avg","spp_avg","search_frequency","median_position","visibility_pct",
                         "gp_per_order","margin_pct","profitability_pct","finished_price_rrp_coeff","price_with_disc_rrp_coeff",
                         "localization_coverage_weighted","localization_coverage_count"]:
                for suf in ["prev","cur"]:
                    c = f"{base}_{suf}"
                    if c in g.columns:
                        weights = g[f"abc_orders_{suf}"].fillna(0) if f"abc_orders_{suf}" in g.columns else None
                        vals = pd.to_numeric(g[c], errors="coerce")
                        if weights is not None and weights.sum() > 0:
                            row[c] = np.average(vals.fillna(0), weights=weights)
                        else:
                            row[c] = vals.mean()
            row["gp_delta"] = row["gp_cur"] - row["gp_prev"]
            row["gp_delta_pct"] = pct_delta(row["gp_cur"], row["gp_prev"])
            row["revenue_delta"] = row["revenue_cur"] - row["revenue_prev"]
            row["revenue_delta_pct"] = pct_delta(row["revenue_cur"], row["revenue_prev"])
            row["abc_orders_delta"] = row["abc_orders_cur"] - row["abc_orders_prev"]
            row["abc_orders_delta_pct"] = pct_delta(row["abc_orders_cur"], row["abc_orders_prev"])
            row["gp_per_order_prev"] = safe_div(row["gp_prev"], row["abc_orders_prev"])
            row["gp_per_order_cur"] = safe_div(row["gp_cur"], row["abc_orders_cur"])
            row["gp_per_order_delta_pct"] = pct_delta(row["gp_per_order_cur"], row["gp_per_order_prev"])
            row["gp_volume_effect"] = (row["abc_orders_cur"] - row["abc_orders_prev"]) * (row["gp_per_order_prev"] if pd.notna(row["gp_per_order_prev"]) else 0)
            row["gp_economy_effect"] = ((row["gp_per_order_cur"] if pd.notna(row["gp_per_order_cur"]) else 0) - (row["gp_per_order_prev"] if pd.notna(row["gp_per_order_prev"]) else 0)) * row["abc_orders_cur"]
            row["main_reason"], row["secondary_reason"] = self.detect_reasons(pd.Series(row))
            row["ad_assessment"] = self.assess_ads(pd.Series(row))
            row["price_assessment"] = self.assess_price(pd.Series(row))
            rows.append(row)
        out = pd.DataFrame(rows)
        sort_col = "gp_cur" if "gp_cur" in out.columns else out.columns[0]
        return out.sort_values(sort_col, ascending=False)

    def detect_reasons(self, r: pd.Series) -> Tuple[str, str]:
        # helpers
        def p(name, default=np.nan):
            return r.get(name, default)
        volume_dominant = abs(p("gp_volume_effect", 0)) >= abs(p("gp_economy_effect", 0))
        reasons = []

        if volume_dominant:
            if p("localization_coverage_weighted_delta", 0) <= -0.15:
                reasons.append("Локализация/наличие")
            if p("search_frequency_delta_pct", 0) <= -0.08:
                reasons.append("Снижение спроса")
            if p("visibility_pct_delta_pct", 0) <= -0.08 or (pd.notna(p("median_position_delta")) and p("median_position_delta", 0) > 1.0):
                reasons.append("Потеря позиций/видимости")
            if p("clicks_total_delta_pct", 0) <= -0.08 and p("ep_ctr_delta_pct", 0) <= -0.05:
                reasons.append("Падение CTR")
            if p("cr_click_to_order_delta_pct", 0) <= -0.08 or p("conv_to_cart_delta_pct", 0) <= -0.08 or p("conv_cart_to_order_delta_pct", 0) <= -0.08:
                reasons.append("Падение конверсии")
            if p("finished_price_avg_delta_pct", 0) >= 0.03:
                reasons.append("Рост цены для покупателя")
            if p("ad_spend_delta_pct", 0) >= 0.15 and p("gp_delta_pct", 0) < 0:
                reasons.append("Реклама не окупилась")
        else:
            if p("ads_unit_delta", 0) > 0 and p("gp_per_order_delta_pct", 0) <= -0.05:
                reasons.append("Рост рекламы на единицу")
            if p("logistics_direct_unit_delta_pct", 0) >= 0.10 or p("logistics_return_unit_delta_pct", 0) >= 0.10:
                reasons.append("Рост логистики")
            if p("commission_unit_delta_pct", 0) >= 0.05:
                reasons.append("Рост комиссии")
            if p("price_with_disc_avg_delta_pct", 0) <= -0.02 and p("gp_delta_pct", 0) < -0.05:
                reasons.append("Неудачное снижение priceWithDisc")
            if p("price_with_disc_avg_delta_pct", 0) >= 0.02 and p("abc_orders_delta_pct", 0) < -0.08 and p("gp_delta_pct", 0) < 0:
                reasons.append("Неудачное повышение priceWithDisc")
            if p("spp_avg_delta", 0) <= -2:
                reasons.append("Снижение SPP")

        if not reasons:
            if p("gp_delta", 0) >= 0:
                reasons = ["Стабильный рост", "—"]
            else:
                reasons = ["Смешанные факторы", "—"]
        elif len(reasons) == 1:
            reasons.append("—")
        return reasons[0], reasons[1]

    def assess_ads(self, r: pd.Series) -> str:
        spend_d = r.get("ad_spend_delta_pct", np.nan)
        clicks_d = r.get("ad_clicks_delta_pct", np.nan)
        orders_d = r.get("abc_orders_delta_pct", np.nan)
        gp_d = r.get("gp_delta_pct", np.nan)
        vis_d = r.get("visibility_pct_delta_pct", np.nan)
        if pd.isna(spend_d):
            return ""
        if spend_d >= 0.15 and clicks_d >= 0.08 and orders_d >= 0.08 and gp_d >= 0.05:
            return "Эффективно"
        if spend_d >= 0.15 and clicks_d >= 0.08 and gp_d > -0.02:
            return "Частично эффективно"
        if spend_d >= 0.15 and gp_d < 0:
            return "Неэффективно"
        if spend_d >= 0.05 and vis_d >= 0:
            return "Защитно"
        return "Нейтрально"

    def assess_price(self, r: pd.Series) -> str:
        pwd = r.get("price_with_disc_avg_delta_pct", np.nan)
        fin = r.get("finished_price_avg_delta_pct", np.nan)
        orders = r.get("abc_orders_delta_pct", np.nan)
        gp = r.get("gp_delta_pct", np.nan)
        if pd.isna(pwd):
            return ""
        if pwd <= -0.02 and orders < 0.08 and gp < -0.05:
            return "Снижение priceWithDisc не окупилось"
        if pwd <= -0.02 and orders >= 0.12 and gp > 0:
            return "Снижение priceWithDisc оправдано"
        if pwd >= 0.02 and orders > -0.05 and gp > 0:
            return "Повышение priceWithDisc оправдано"
        if pwd >= 0.02 and orders < -0.08 and gp < 0:
            return "Повышение priceWithDisc вредно"
        if fin >= 0.03 and gp <= 0:
            return "Рост finishedPrice давит на спрос"
        if fin <= -0.03 and gp <= 0:
            return "Снижение finishedPrice не дало эффекта"
        return "Нейтрально"

    def build_sku_contrib(self) -> pd.DataFrame:
        if self.article_period.empty:
            return pd.DataFrame()
        x = self.article_period.copy()
        x["gp_delta"] = x["gp_cur"] - x["gp_prev"]
        prod_delta = x.groupby("code", dropna=False)["gp_delta"].sum().rename("product_gp_delta")
        x = x.merge(prod_delta, on="code", how="left")
        x["contribution_to_product_gp"] = x.apply(lambda r: safe_div(r["gp_delta"], r["product_gp_delta"]), axis=1)
        keep = ["code","supplier_article","subject","gp_prev","gp_cur","gp_delta","abc_orders_prev","abc_orders_cur","revenue_prev","revenue_cur","contribution_to_product_gp","main_reason","price_assessment","ad_assessment"]
        return x[keep].sort_values(["code","gp_delta"], ascending=[True, False])

    def build_channels(self) -> pd.DataFrame:
        rows = []
        # categories from entry_cat
        if not self.data.entry_cat.empty:
            ec = self.data.entry_cat.copy()
            ec["period_name"] = np.where(
                ec["week_code"].isin({week_code_from_date(d) for d in self.windows["cur_dates"]}), "cur_14d",
                np.where(ec["week_code"].isin({week_code_from_date(d) for d in self.windows["prev_dates"]}), "prev_14d", "")
            )
            ec = ec[ec["period_name"] != ""].copy()
            for (subject, ep), g in ec.groupby(["subject","entry_point"], dropna=False):
                prev = g[g["period_name"]=="prev_14d"]
                cur = g[g["period_name"]=="cur_14d"]
                rows.append({
                    "Уровень":"Категория","Объект":subject,"Канал":ep,
                    "Клики_prev": prev["clicks"].sum(), "Клики_cur": cur["clicks"].sum(),
                    "Заказы_prev": prev["orders"].sum(), "Заказы_cur": cur["orders"].sum(),
                    "CTR_prev": prev["ctr"].mean(), "CTR_cur": cur["ctr"].mean(),
                    "CR_prev": prev["conv_order"].mean(), "CR_cur": cur["conv_order"].mean(),
                })
        # articles from entry_sku
        if not self.data.entry_sku.empty:
            es = self.data.entry_sku.copy()
            es["period_name"] = np.where(
                es["week_code"].isin({week_code_from_date(d) for d in self.windows["cur_dates"]}), "cur_14d",
                np.where(es["week_code"].isin({week_code_from_date(d) for d in self.windows["prev_dates"]}), "prev_14d", "")
            )
            es = es[es["period_name"] != ""].copy()
            for (art, ep), g in es.groupby(["supplier_article","entry_point"], dropna=False):
                prev = g[g["period_name"]=="prev_14d"]
                cur = g[g["period_name"]=="cur_14d"]
                rows.append({
                    "Уровень":"Артикул","Объект":art,"Канал":ep,
                    "Клики_prev": prev["clicks"].sum(), "Клики_cur": cur["clicks"].sum(),
                    "Заказы_prev": prev["orders"].sum(), "Заказы_cur": cur["orders"].sum(),
                    "CTR_prev": prev["ctr"].mean(), "CTR_cur": cur["ctr"].mean(),
                    "CR_prev": prev["conv_order"].mean(), "CR_cur": cur["conv_order"].mean(),
                })
        if not rows:
            return pd.DataFrame()
        ch = pd.DataFrame(rows)
        for c in ["Клики","Заказы","CTR","CR"]:
            ch[f"{c}_delta"] = ch[f"{c}_cur"] - ch[f"{c}_prev"]
            ch[f"{c}_delta_pct"] = [pct_delta(a,b) for a,b in zip(ch[f"{c}_cur"], ch[f"{c}_prev"])]
        return ch.sort_values(["Уровень","Объект","Заказы_cur"], ascending=[True,True,False])

    def make_matrix(self, group_col: str, label: str, gp_daily: pd.DataFrame, dates_desc: List[pd.Timestamp]) -> pd.DataFrame:
        if gp_daily.empty:
            return pd.DataFrame()
        x = gp_daily[gp_daily["day"].isin(dates_desc)].copy()
        p = x.pivot_table(index=group_col, columns="day", values="gp_day_fact", aggfunc="sum", fill_value=0)
        p = p.reindex(columns=dates_desc, fill_value=0)
        p["Итого 14д"] = p.sum(axis=1)
        # restrict categories via subject relation if needed
        if group_col in {"code","supplier_article"}:
            ref = x.groupby(group_col)["subject"].agg(lambda s: s.mode().iat[0] if not s.mode().empty else "").rename("Категория")
            p = p.merge(ref, left_index=True, right_index=True, how="left")
            cols = ["Категория", "Итого 14д"] + dates_desc
            p = p[cols]
        else:
            cols = ["Итого 14д"] + dates_desc
            p = p[cols]
        p = p.sort_values("Итого 14д", ascending=False)
        p.index.name = label
        p = p.reset_index()
        return p

    def make_week_matrix(self, group_col: str, label: str) -> pd.DataFrame:
        if self.article_gp_daily.empty:
            return pd.DataFrame()
        x = self.article_gp_daily.copy()
        x = x[(x["day"] >= self.windows["prev_start"]) & (x["day"] <= self.windows["cur_end"])].copy()
        x["week_code"] = x["day"].map(week_code_from_date)
        p = x.pivot_table(index=group_col, columns="week_code", values="gp_day_fact", aggfunc="sum", fill_value=0)
        cols = sorted(p.columns.tolist(), reverse=True)
        p = p.reindex(columns=cols, fill_value=0)
        p["Итого"] = p.sum(axis=1)
        p = p.sort_values("Итого", ascending=False).reset_index()
        p = p.rename(columns={group_col: label})
        return p

    def make_month_matrix(self, group_col: str, label: str) -> pd.DataFrame:
        if self.article_gp_daily.empty:
            return pd.DataFrame()
        x = self.article_gp_daily.copy()
        x["month_key"] = pd.to_datetime(x["day"]).dt.to_period("M").astype(str)
        p = x.pivot_table(index=group_col, columns="month_key", values="gp_day_fact", aggfunc="sum", fill_value=0)
        cols = sorted(p.columns.tolist(), reverse=True)
        p = p.reindex(columns=cols, fill_value=0)
        p["Итого"] = p.sum(axis=1)
        p = p.sort_values("Итого", ascending=False).reset_index()
        p = p.rename(columns={group_col: label})
        return p

    def build_example_tables(self, article: str) -> Dict[str, pd.DataFrame]:
        art = canonical_article(article)
        ap = self.article_period[self.article_period["supplier_article"].map(canonical_article) == art].copy()
        loc = self.localization_daily[self.localization_daily["supplier_article"].map(canonical_article) == art].copy()
        ch = self.channels[(self.channels["Уровень"] == "Артикул") & (self.channels["Объект"].map(canonical_article) == art)].copy() if not self.channels.empty else pd.DataFrame()
        money = pd.DataFrame([
            ["Валовая выручка, ₽", ap["revenue_prev"].sum(), ap["revenue_cur"].sum(), ap["revenue_delta"].sum()],
            ["Валовая прибыль, ₽", ap["gp_prev"].sum(), ap["gp_cur"].sum(), ap["gp_delta"].sum()],
            ["ABC заказы, шт", ap["abc_orders_prev"].sum(), ap["abc_orders_cur"].sum(), ap["abc_orders_delta"].sum()],
            ["ВП на 1 заказ, ₽", safe_div(ap["gp_prev"].sum(), ap["abc_orders_prev"].sum()), safe_div(ap["gp_cur"].sum(), ap["abc_orders_cur"].sum()), safe_div(ap["gp_cur"].sum(), ap["abc_orders_cur"].sum()) - safe_div(ap["gp_prev"].sum(), ap["abc_orders_prev"].sum())],
            ["Вклад объема, ₽", "", "", ap["gp_volume_effect"].sum()],
            ["Вклад экономики, ₽", "", "", ap["gp_economy_effect"].sum()],
        ], columns=["Метрика","Пред. 14д","Тек. 14д","Δ"])
        price = pd.DataFrame([
            ["finishedPrice, ₽", ap["finished_price_avg_prev"].mean(), ap["finished_price_avg_cur"].mean(), ap["finished_price_avg_delta"].mean()],
            ["priceWithDisc, ₽", ap["price_with_disc_avg_prev"].mean(), ap["price_with_disc_avg_cur"].mean(), ap["price_with_disc_avg_delta"].mean()],
            ["SPP, п.п.", ap["spp_avg_prev"].mean(), ap["spp_avg_cur"].mean(), ap["spp_avg_delta"].mean()],
            ["РРЦ, ₽", ap["rrp_prev"].mean(), ap["rrp_cur"].mean(), np.nan],
            ["finishedPrice / РРЦ", ap["finished_price_rrp_coeff_prev"].mean(), ap["finished_price_rrp_coeff_cur"].mean(), ap["finished_price_rrp_coeff_delta"].mean()],
            ["priceWithDisc / РРЦ", ap["price_with_disc_rrp_coeff_prev"].mean(), ap["price_with_disc_rrp_coeff_cur"].mean(), ap["price_with_disc_rrp_coeff_delta"].mean()],
        ], columns=["Метрика","Пред. 14д","Тек. 14д","Δ"])
        traffic = pd.DataFrame([
            ["Открытия карточки", ap["open_card_count_prev"].sum(), ap["open_card_count_cur"].sum(), ap["open_card_count_delta"].sum()],
            ["Клики total", ap["clicks_total_prev"].sum(), ap["clicks_total_cur"].sum(), ap["clicks_total_delta"].sum()],
            ["CR click→order", ap["cr_click_to_order_prev"].mean(), ap["cr_click_to_order_cur"].mean(), ap["cr_click_to_order_delta"].mean()],
            ["Search frequency", ap["search_frequency_prev"].sum(), ap["search_frequency_cur"].sum(), ap["search_frequency_delta"].sum()],
            ["Медианная позиция", ap["median_position_prev"].mean(), ap["median_position_cur"].mean(), ap["median_position_delta"].mean()],
            ["Видимость, %", ap["visibility_pct_prev"].mean(), ap["visibility_pct_cur"].mean(), ap["visibility_pct_delta"].mean()],
        ], columns=["Метрика","Пред. 14д","Тек. 14д","Δ"])
        ads = pd.DataFrame([
            ["Ad spend, ₽", ap["ad_spend_prev"].sum(), ap["ad_spend_cur"].sum(), ap["ad_spend_delta"].sum()],
            ["Ad clicks", ap["ad_clicks_prev"].sum(), ap["ad_clicks_cur"].sum(), ap["ad_clicks_delta"].sum()],
            ["Ad orders", ap["ad_orders_prev"].sum(), ap["ad_orders_cur"].sum(), ap["ad_orders_delta"].sum()],
            ["Оценка рекламы", ap["ad_assessment"].iloc[0] if not ap.empty else "", "", ""],
            ["Оценка цены", ap["price_assessment"].iloc[0] if not ap.empty else "", "", ""],
            ["Главная причина", ap["main_reason"].iloc[0] if not ap.empty else "", "", ""],
            ["Вторичная причина", ap["secondary_reason"].iloc[0] if not ap.empty else "", "", ""],
        ], columns=["Метрика","Пред. 14д","Тек. 14д","Δ"])
        if not loc.empty:
            loc = loc[(loc["day"] >= self.windows["cur_start"]) & (loc["day"] <= self.windows["cur_end"])].copy()
            loc = loc.sort_values(["day","warehouse"])
        return {"money": money, "price": price, "traffic": traffic, "ads": ads, "loc": loc, "channels": ch}

# =========================
# Writer
# =========================

class Writer:
    def __init__(self):
        self.wb = Workbook()
        self.wb.remove(self.wb.active)

    def add_df(self, name: str, df: pd.DataFrame, center=True) -> None:
        ws = self.wb.create_sheet(self.safe_title(name))
        if df is None or df.empty:
            ws["A1"] = "Нет данных"
            return
        x = df.copy()
        x = x.where(pd.notna(x), "")
        for j, c in enumerate(x.columns, 1):
            cell = ws.cell(1, j, c)
            cell.fill = HEADER_FILL
            cell.font = Font(bold=True)
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            cell.border = THIN_BORDER
        for i, row in enumerate(x.itertuples(index=False), 2):
            for j, val in enumerate(row, 1):
                cell = ws.cell(i, j, val)
                cell.border = THIN_BORDER
                cell.alignment = Alignment(horizontal="center" if center else "left", vertical="center")
        self.format_sheet(ws)

    def add_example(self, tables: Dict[str, pd.DataFrame]) -> None:
        ws = self.wb.create_sheet(self.safe_title("Пример_901_5"))
        row = 1
        sections = [
            ("Деньги", tables.get("money")),
            ("Цена / РРЦ / SPP", tables.get("price")),
            ("Трафик / спрос / позиции", tables.get("traffic")),
            ("Реклама / причины", tables.get("ads")),
            ("Каналы входа", tables.get("channels")),
            ("Локализация daily", tables.get("loc")),
        ]
        for title, df in sections:
            ws.cell(row, 1, title)
            ws.cell(row, 1).fill = HEADER_FILL
            ws.cell(row, 1).font = Font(bold=True, size=12)
            row += 1
            if df is None or df.empty:
                ws.cell(row, 1, "Нет данных")
                row += 3
                continue
            x = df.where(pd.notna(df), "")
            for j, c in enumerate(x.columns, 1):
                cell = ws.cell(row, j, c)
                cell.fill = SUBHEADER_FILL
                cell.font = Font(bold=True)
                cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
                cell.border = THIN_BORDER
            row += 1
            for rec in x.itertuples(index=False):
                for j, val in enumerate(rec, 1):
                    cell = ws.cell(row, j, val)
                    cell.border = THIN_BORDER
                    cell.alignment = Alignment(horizontal="center", vertical="center")
                row += 1
            row += 2
        self.format_sheet(ws)

    def format_sheet(self, ws) -> None:
        ws.freeze_panes = "A2"
        # width / formats
        for col in ws.columns:
            letter = get_column_letter(col[0].column)
            maxlen = 10
            for cell in col[:200]:
                maxlen = max(maxlen, len(normalize_text(cell.value)))
                # formats
                if isinstance(cell.value, (int, float)) and not isinstance(cell.value, bool):
                    header = normalize_text(ws.cell(1, cell.column).value)
                    if "дата" in header.lower():
                        pass
            ws.column_dimensions[letter].width = min(maxlen + 2, 22)
        for row in ws.iter_rows(min_row=2):
            for cell in row:
                header = normalize_text(ws.cell(1, cell.column).value)
                if isinstance(cell.value, (int, float)) and not isinstance(cell.value, bool):
                    h = header.lower()
                    if "дата" in h:
                        continue
                    if "%" in h or "видимость" in h or "ctr" in h or "cr" in h or "spp" in h or "коэф" in h or "coeff" in h or "рентабельность" in h or "маржинальность" in h:
                        cell.number_format = '0.00'
                    elif "позиц" in h:
                        cell.number_format = '0.00'
                    else:
                        cell.number_format = '#,##0 "₽"' if any(k in h for k in ["прибыль", "выруч", "расход", "price", "цена", "логист", "комис", "ррц", "spend", "gp", "revenue"]) else '0'
                if isinstance(cell.value, datetime):
                    cell.number_format = "DD.MM.YYYY"
        # center all
        for row in ws.iter_rows():
            for cell in row:
                cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    @staticmethod
    def safe_title(name: str) -> str:
        bad = '[]:*?/\\'
        return "".join("_" if c in bad else c for c in name)[:31]

def workbook_to_bytes(wb: Workbook) -> bytes:
    bio = io.BytesIO()
    wb.save(bio)
    return bio.getvalue()

# =========================
# CLI / main
# =========================

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--root", default=".")
    p.add_argument("--reports-root", default="Отчёты")
    p.add_argument("--store", default="TOPFACE")
    p.add_argument("--out-subdir", default="Отчёты/Объединенный отчет/TOPFACE")
    return p.parse_args()

def main() -> int:
    args = parse_args()
    storage = make_storage(args.root)
    loader = Loader(storage, args.store, args.reports_root)
    data = loader.load_all()
    log("Building analytics")
    an = Analyzer(data)

    log("Building workbook")
    w = Writer()
    # part 1
    w.add_df("Результаты", an.results_category)
    w.add_df("Товар_день", an.product_day)
    w.add_df("Артикул_день", an.article_day)
    w.add_df("Товар_неделя", an.product_week)
    w.add_df("Артикул_неделя", an.article_week)
    w.add_df("Товар_месяц", an.product_month)
    w.add_df("Артикул_месяц", an.article_month)
    # part 2 concise
    keep_cat = ["Категория","gp_prev","gp_cur","gp_delta","gp_delta_pct","revenue_prev","revenue_cur","revenue_delta","abc_orders_prev","abc_orders_cur","abc_orders_delta","finished_price_avg_prev","finished_price_avg_cur","price_with_disc_avg_prev","price_with_disc_avg_cur","spp_avg_prev","spp_avg_cur","search_frequency_prev","search_frequency_cur","clicks_total_prev","clicks_total_cur","localization_coverage_weighted_prev","localization_coverage_weighted_cur","main_reason","secondary_reason","ad_assessment","price_assessment"]
    keep_prod = ["Товар","Категория","gp_prev","gp_cur","gp_delta","gp_delta_pct","revenue_prev","revenue_cur","abc_orders_prev","abc_orders_cur","finished_price_avg_prev","finished_price_avg_cur","price_with_disc_avg_prev","price_with_disc_avg_cur","spp_avg_prev","spp_avg_cur","search_frequency_prev","search_frequency_cur","clicks_total_prev","clicks_total_cur","localization_coverage_weighted_prev","localization_coverage_weighted_cur","main_reason","secondary_reason","ad_assessment","price_assessment"]
    keep_art = ["supplier_article","subject","gp_prev","gp_cur","gp_delta","gp_delta_pct","revenue_prev","revenue_cur","abc_orders_prev","abc_orders_cur","abc_orders_delta","gp_per_order_prev","gp_per_order_cur","finished_price_avg_prev","finished_price_avg_cur","price_with_disc_avg_prev","price_with_disc_avg_cur","spp_avg_prev","spp_avg_cur","rrp_prev","rrp_cur","finished_price_rrp_coeff_prev","finished_price_rrp_coeff_cur","price_with_disc_rrp_coeff_prev","price_with_disc_rrp_coeff_cur","search_frequency_prev","search_frequency_cur","clicks_total_prev","clicks_total_cur","cr_click_to_order_prev","cr_click_to_order_cur","localization_coverage_weighted_prev","localization_coverage_weighted_cur","main_reason","secondary_reason","ad_assessment","price_assessment"]
    cat_df = an.category_period[keep_cat].rename(columns={"Категория":"Категория"}) if not an.category_period.empty else pd.DataFrame()
    prod_df = an.product_period[[c for c in keep_prod if c in an.product_period.columns]] if not an.product_period.empty else pd.DataFrame()
    art_df = an.article_period[[c for c in keep_art if c in an.article_period.columns]].rename(columns={"supplier_article":"Артикул","subject":"Категория"}) if not an.article_period.empty else pd.DataFrame()
    w.add_df("Причины_категории", cat_df)
    w.add_df("Причины_товары", prod_df)
    w.add_df("Причины_артикулы", art_df)
    w.add_df("Вклад_SKU_в_товар", an.sku_contrib.rename(columns={"code":"Товар","supplier_article":"Артикул","subject":"Категория"}))
    # price sheet
    if not an.article_period.empty:
        price_sheet = an.article_period[[
            "supplier_article","subject",
            "finished_price_avg_prev","finished_price_avg_cur","finished_price_avg_delta_pct",
            "price_with_disc_avg_prev","price_with_disc_avg_cur","price_with_disc_avg_delta_pct",
            "rrp_prev","rrp_cur","finished_price_rrp_coeff_prev","finished_price_rrp_coeff_cur",
            "price_with_disc_rrp_coeff_prev","price_with_disc_rrp_coeff_cur",
            "spp_avg_prev","spp_avg_cur","spp_avg_delta",
            "abc_orders_prev","abc_orders_cur","abc_orders_delta_pct",
            "gp_prev","gp_cur","gp_delta_pct","price_assessment"
        ]].rename(columns={"supplier_article":"Артикул","subject":"Категория"})
    else:
        price_sheet = pd.DataFrame()
    w.add_df("Цена_RRP_SPP", price_sheet)
    w.add_df("Каналы_входа", an.channels)
    loc_daily = an.localization_daily.copy()
    if not loc_daily.empty:
        loc_daily = loc_daily[(loc_daily["day"] >= an.windows["cur_start"]) & (loc_daily["day"] <= an.windows["cur_end"])].copy()
        loc_daily = loc_daily.rename(columns={"supplier_article":"Артикул","subject":"Категория","warehouse":"Склад","day":"Дата","stock_qty":"Остаток","avg_orders_per_day_warehouse":"Средние заказы/день","coverage_days":"Покрытие, дней","is_available_flag":"Флаг наличия","warehouse_weight":"Вес склада"})
        loc_daily = loc_daily[["Дата","Артикул","Категория","Склад","Остаток","Средние заказы/день","Покрытие, дней","Флаг наличия","Вес склада"]].sort_values(["Артикул","Дата","Склад"], ascending=[True,False,True])
    w.add_df("Локализация_daily", loc_daily)
    w.add_example(an.example_901_5)
    if data.warnings:
        w.add_df("Warnings", pd.DataFrame({"warning": data.warnings}))

    stamp = datetime.now().strftime("%Y-%m-%d")
    out = f"{args.out_subdir}/Объединенный_отчет_{args.store}_{stamp}.xlsx"
    log(f"Saving workbook: {out}")
    storage.write_bytes(out, workbook_to_bytes(w.wb))
    log(f"Saved: {out}")
    if data.warnings:
        log("Warnings:")
        for item in data.warnings:
            log(f" - {item}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
