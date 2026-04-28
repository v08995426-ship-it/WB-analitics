#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import calendar
import io
import math
import os
import re
from collections import defaultdict
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
from openpyxl.worksheet.dimensions import SheetFormatProperties

# -------------------------
# Constants
# -------------------------

TARGET_SUBJECTS = [
    "Кисти косметические",
    "Помады",
    "Блески",
    "Косметические карандаши",
]

EXCLUDED_ARTICLES = {
    "CZ420", "CZ420брови", "CZ420глаза", "DE49", "DE49глаза",
    "PT901", "cz420", "cz420глаза",
}

SUBJECT_ORDER = {
    "Кисти косметические": 1,
    "Помады": 2,
    "Блески": 3,
    "Косметические карандаши": 4,
}

THIN = Side(style="thin", color="D0D0D0")
BORDER = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)
FILL_HEADER = PatternFill("solid", fgColor="DCE6F1")
FILL_SECTION = PatternFill("solid", fgColor="EAF3E6")
FILL_TITLE = PatternFill("solid", fgColor="C6E0B4")
FILL_NEG = PatternFill("solid", fgColor="FCE4D6")
FILL_POS = PatternFill("solid", fgColor="E2F0D9")
FILL_ARTICLE = PatternFill("solid", fgColor="F4F6F8")

NUM_FMT_RUB = '# ##0" р."'
NUM_FMT_PCT = '0.00%'
NUM_FMT_PCT_PT = '0.00'
NUM_FMT_DATE = 'dd.mm.yyyy'

# -------------------------
# Helpers
# -------------------------

def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def normalize_text(v: Any) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return ""
    s = str(v).replace("\xa0", " ").strip()
    s = re.sub(r"\s+", " ", s)
    return s


def clean_article(v: Any) -> str:
    s = normalize_text(v)
    if not s or s.lower() in {"nan", "none"}:
        return ""
    return s


def norm_key(v: Any) -> str:
    s = normalize_text(v).lower().replace("ё", "е")
    s = s.replace("%", " pct ")
    s = re.sub(r"[^\w]+", " ", s, flags=re.UNICODE)
    s = re.sub(r"\s+", " ", s).strip()
    return s


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
    if b == 0 or pd.isna(b):
        return np.nan
    return a / b


def pct_delta(cur: Any, prev: Any) -> float:
    if pd.isna(cur) or pd.isna(prev) or prev == 0:
        return np.nan
    return (float(cur) - float(prev)) / float(prev)


def fmt_money(v: Any) -> str:
    if pd.isna(v):
        return "—"
    return f"{int(round(float(v))):,}".replace(",", " ") + " р."


def fmt_pct(v: Any, is_ratio: bool = True) -> str:
    if pd.isna(v):
        return "—"
    x = float(v) * 100 if is_ratio else float(v)
    return f"{x:.2f}%".replace(".", ",")


def fmt_pp(v: Any) -> str:
    if pd.isna(v):
        return "—"
    return f"{float(v):+.2f} п.п.".replace(".", ",")


def fmt_num(v: Any, digits: int = 1) -> str:
    if pd.isna(v):
        return "—"
    return f"{float(v):.{digits}f}".replace(".", ",")


def clean_code_from_article(article: str) -> str:
    a = clean_article(article)
    if not a:
        return ""
    low = a.lower().replace("_", "").replace(" ", "")
    if low.startswith("pt901"):
        return "901"
    m = re.search(r"(\d+)", a)
    return m.group(1) if m else a


def valid_article(article: str) -> bool:
    a = clean_article(article)
    if not a:
        return False
    if a in EXCLUDED_ARTICLES:
        return False
    if a.upper() in {x.upper() for x in EXCLUDED_ARTICLES}:
        return False
    return True


def parse_week_code_from_name(name: str) -> Optional[str]:
    m = re.search(r"(\d{4})-W(\d{2})", name)
    if not m:
        return None
    return f"{m.group(1)}-W{m.group(2)}"


def week_bounds_from_code(week_code: str) -> Tuple[Optional[date], Optional[date]]:
    m = re.match(r"^(\d{4})-W(\d{2})$", str(week_code))
    if not m:
        return None, None
    y, w = int(m.group(1)), int(m.group(2))
    return date.fromisocalendar(y, w, 1), date.fromisocalendar(y, w, 7)


def week_code_from_date(dt: Any) -> Optional[str]:
    if pd.isna(dt):
        return None
    ts = pd.Timestamp(dt)
    iso = ts.isocalendar()
    return f"{int(iso.year)}-W{int(iso.week):02d}"


def parse_abc_period_from_name(name: str) -> Tuple[Optional[date], Optional[date]]:
    m = re.search(r"__(\d{2})\.(\d{2})\.(\d{4})-(\d{2})\.(\d{2})\.(\d{4})__", name)
    if not m:
        return None, None
    return date(int(m.group(3)), int(m.group(2)), int(m.group(1))), date(int(m.group(6)), int(m.group(5)), int(m.group(4)))


def parse_entry_period_from_name(name: str) -> Tuple[Optional[date], Optional[date]]:
    m = re.search(r"с (\d{2})-(\d{2})-(\d{4}) по (\d{2})-(\d{2})-(\d{4})", name)
    if not m:
        return None, None
    return date(int(m.group(3)), int(m.group(2)), int(m.group(1))), date(int(m.group(6)), int(m.group(5)), int(m.group(4)))


def dedupe_columns(cols: Iterable[Any]) -> List[str]:
    out: List[str] = []
    seen: Dict[str, int] = {}
    for c in cols:
        base = normalize_text(c) or "unnamed"
        seen[base] = seen.get(base, 0) + 1
        out.append(base if seen[base] == 1 else f"{base}__{seen[base]}")
    return out


def pick_existing(df: pd.DataFrame, aliases: List[str]) -> Optional[str]:
    norm = {norm_key(c): c for c in df.columns}
    for a in aliases:
        if norm_key(a) in norm:
            return norm[norm_key(a)]
    for c in df.columns:
        kc = norm_key(c)
        for a in aliases:
            if norm_key(a) in kc:
                return c
    return None


def ensure_col(df: pd.DataFrame, target: str, aliases: List[str]) -> pd.DataFrame:
    found = pick_existing(df, aliases)
    if found is None:
        df[target] = np.nan
    elif found != target:
        df[target] = df[found]
    return df


def normalize_rrp_key(article: str) -> str:
    a = clean_article(article)
    if not a:
        return ""
    al = a.lower()
    if al.startswith("pt901.f"):
        m = re.search(r"f(\d+)", al)
        return f"PT901.F{int(m.group(1)):02d}" if m else a.upper()
    code = clean_code_from_article(a)
    m = re.search(r"/(\d+)", a)
    if m:
        return f"PT{int(code)}.{int(m.group(1)):03d}"
    return a.upper()


def is_target_subject(subject: str) -> bool:
    return normalize_text(subject) in TARGET_SUBJECTS


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
    def _abs(self, rel: str) -> Path:
        return self.root / rel
    def list_files(self, prefix: str) -> List[str]:
        prefix = prefix.replace("\\", "/").rstrip("/")
        p = self._abs(prefix)
        base = p if p.exists() else p.parent
        if not base.exists():
            base = self.root
        out = []
        for x in base.rglob("*"):
            if x.is_file():
                rel = str(x.relative_to(self.root)).replace("\\", "/")
                if rel.startswith(prefix) or prefix in rel or Path(rel).name.startswith(Path(prefix).name):
                    out.append(rel)
        return sorted(set(out))
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
        out, token = [], None
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
        except Exception:
            return False


def make_storage(root: str) -> BaseStorage:
    bucket = os.getenv("YC_BUCKET_NAME", "").strip()
    key = os.getenv("YC_ACCESS_KEY_ID", "").strip()
    secret = os.getenv("YC_SECRET_ACCESS_KEY", "").strip()
    if bucket and key and secret:
        log("Using Yandex Object Storage (S3)")
        return S3Storage(bucket, key, secret)
    log("Using local filesystem")
    return LocalStorage(root)


# -------------------------
# Reading helpers
# -------------------------

def read_excel_best(data: bytes, preferred_sheet: Optional[List[str]] = None, header_candidates: Tuple[int, ...] = (0, 1, 2)) -> Tuple[pd.DataFrame, str]:
    bio = io.BytesIO(data)
    xl = pd.ExcelFile(bio)
    if preferred_sheet:
        sheet = None
        norm = {norm_key(s): s for s in xl.sheet_names}
        for p in preferred_sheet:
            if norm_key(p) in norm:
                sheet = norm[norm_key(p)]
                break
        if sheet is None:
            sheet = xl.sheet_names[0]
    else:
        sheet = xl.sheet_names[0]
    best, best_score = None, -1
    for h in header_candidates:
        try:
            df = xl.parse(sheet_name=sheet, header=h, dtype=object)
        except Exception:
            continue
        df.columns = dedupe_columns(df.columns)
        df = df.dropna(axis=0, how="all").dropna(axis=1, how="all")
        score = len(df.columns) + len(df) * 0.001
        if score > best_score:
            best, best_score = df, score
    if best is None:
        raise ValueError(f"Не удалось прочитать {sheet}")
    return best, sheet


# -------------------------
# Data loading
# -------------------------

@dataclass
class Loaded:
    orders: pd.DataFrame
    stocks: pd.DataFrame
    search: pd.DataFrame
    funnel: pd.DataFrame
    ads: pd.DataFrame
    econ_unit: pd.DataFrame
    abc: pd.DataFrame
    entry: pd.DataFrame
    rrp: pd.DataFrame
    warnings: List[str]


class Loader:
    def __init__(self, storage: BaseStorage, root_reports: str = "Отчёты", store: str = "TOPFACE"):
        self.storage = storage
        self.root_reports = root_reports.rstrip("/")
        self.store = store
        self.warnings: List[str] = []

    def _list(self, prefixes: List[str], contains: Optional[List[str]] = None) -> List[str]:
        files = []
        for p in prefixes:
            try:
                files.extend(self.storage.list_files(p))
            except Exception as e:
                self.warnings.append(f"Не удалось получить список файлов {p}: {e}")
        uniq = []
        seen = set()
        for f in sorted(files):
            if not f.lower().endswith(".xlsx"):
                continue
            if contains and not all(c.lower() in Path(f).name.lower() for c in contains):
                continue
            if f not in seen:
                uniq.append(f); seen.add(f)
        return uniq

    def _filter_last_weeks(self, files: List[str], count: int = 8) -> List[str]:
        keyed = []
        for f in files:
            wc = parse_week_code_from_name(Path(f).name)
            if wc:
                keyed.append((wc, f))
        if keyed:
            return [f for _, f in sorted(keyed)[-count:]]
        return files[-count:]

    def load_orders(self) -> pd.DataFrame:
        files = self._filter_last_weeks(self._list([
            f"{self.root_reports}/Заказы/{self.store}/Недельные",
            f"{self.root_reports}/Заказы/{self.store}",
            "",
        ], contains=["Заказы_"]), 12)
        dfs = []
        for f in files:
            try:
                df, _ = read_excel_best(self.storage.read_bytes(f), preferred_sheet=["Заказы"], header_candidates=(0,1,2))
                for tgt, aliases in {
                    "day": ["date", "Дата", "Дата заказа"],
                    "supplier_article": ["supplierArticle", "Артикул продавца"],
                    "nm_id": ["nmId", "nmID", "Артикул WB"],
                    "subject": ["subject", "Предмет"],
                    "brand": ["brand", "Бренд"],
                    "warehouse": ["warehouseName", "Склад"],
                    "finishedPrice": ["finishedPrice", "Цена покупателя", "Цена с учетом всех скидок, кроме суммы по WB Кошельку"],
                    "priceWithDisc": ["priceWithDisc", "Цена со скидкой продавца, в том числе со скидкой WB Клуба", "Средняя цена продажи"],
                    "spp": ["spp", "СПП"],
                    "isCancel": ["isCancel", "Отмена заказа"],
                }.items():
                    df = ensure_col(df, tgt, aliases)
                df = df[["day","supplier_article","nm_id","subject","brand","warehouse","finishedPrice","priceWithDisc","spp","isCancel"]].copy()
                df["day"] = to_dt(df["day"]).dt.normalize()
                df["supplier_article"] = df["supplier_article"].map(clean_article)
                df["nm_id"] = to_numeric(df["nm_id"])
                df["subject"] = df["subject"].map(normalize_text)
                df["brand"] = df["brand"].map(normalize_text)
                df["warehouse"] = df["warehouse"].map(normalize_text)
                df["finishedPrice"] = to_numeric(df["finishedPrice"])
                df["priceWithDisc"] = to_numeric(df["priceWithDisc"])
                df["spp"] = to_numeric(df["spp"])
                df["isCancel"] = df["isCancel"].fillna(False)
                df = df[df["day"].notna()].copy()
                dfs.append(df)
            except Exception as e:
                self.warnings.append(f"Ошибка чтения orders {f}: {e}")
        if not dfs:
            return pd.DataFrame()
        out = pd.concat(dfs, ignore_index=True)
        out = out[out["subject"].map(is_target_subject)].copy()
        out = out[out["supplier_article"].map(valid_article)].copy()
        out["code"] = out["supplier_article"].map(clean_code_from_article)
        return out

    def load_stocks(self) -> pd.DataFrame:
        files = self._filter_last_weeks(self._list([
            f"{self.root_reports}/Остатки/{self.store}/Недельные",
            f"{self.root_reports}/Остатки/{self.store}",
            "",
        ], contains=["Остатки_"]), 12)
        dfs = []
        for f in files:
            try:
                df, _ = read_excel_best(self.storage.read_bytes(f), preferred_sheet=["Остатки"], header_candidates=(0,))
                for tgt, aliases in {
                    "day": ["Дата запроса", "Дата"],
                    "warehouse": ["Склад", "warehouseName"],
                    "supplier_article": ["Артикул продавца", "supplierArticle"],
                    "nm_id": ["Артикул WB", "nmId", "nmID"],
                    "subject": ["Предмет", "subject"],
                    "stock_available": ["Доступно для продажи", "Остаток"],
                    "stock_total": ["Полное количество"],
                }.items():
                    df = ensure_col(df, tgt, aliases)
                df = df[["day","warehouse","supplier_article","nm_id","subject","stock_available","stock_total"]].copy()
                df["day"] = to_dt(df["day"]).dt.normalize()
                if df["day"].isna().all():
                    wc = parse_week_code_from_name(Path(f).name)
                    _, end = week_bounds_from_code(wc) if wc else (None, None)
                    if end:
                        df["day"] = pd.Timestamp(end)
                df["warehouse"] = df["warehouse"].map(normalize_text)
                df["supplier_article"] = df["supplier_article"].map(clean_article)
                df["nm_id"] = to_numeric(df["nm_id"])
                df["subject"] = df["subject"].map(normalize_text)
                df["stock_available"] = to_numeric(df["stock_available"]).fillna(0)
                df["stock_total"] = to_numeric(df["stock_total"]).fillna(df["stock_available"]).fillna(0)
                dfs.append(df)
            except Exception as e:
                self.warnings.append(f"Ошибка чтения stocks {f}: {e}")
        if not dfs:
            return pd.DataFrame()
        out = pd.concat(dfs, ignore_index=True)
        out = out[out["subject"].map(is_target_subject)].copy()
        out = out[out["supplier_article"].map(valid_article)].copy()
        out["code"] = out["supplier_article"].map(clean_code_from_article)
        return out

    def load_search(self) -> pd.DataFrame:
        files = self._filter_last_weeks(self._list([
            f"{self.root_reports}/Поисковые запросы/{self.store}/Недельные",
            f"{self.root_reports}/Поисковые запросы/{self.store}",
            "",
        ], contains=["Неделя "]), 12)
        dfs = []
        for f in files:
            try:
                df, _ = read_excel_best(self.storage.read_bytes(f), header_candidates=(0,1,2))
                for tgt, aliases in {
                    "day": ["Дата", "date"],
                    "supplier_article": ["Артикул продавца", "supplierArticle"],
                    "nm_id": ["Артикул WB", "nmId", "nmID"],
                    "subject": ["Предмет", "subject"],
                    "query": ["Поисковый запрос", "Запрос", "query"],
                    "frequency": ["Частота запросов", "Частота"],
                    "median_position": ["Медианная позиция", "Средняя позиция"],
                    "visibility": ["Видимость", "Видимость, %"],
                }.items():
                    df = ensure_col(df, tgt, aliases)
                df = df[["day","supplier_article","nm_id","subject","query","frequency","median_position","visibility"]].copy()
                df["day"] = to_dt(df["day"]).dt.normalize()
                if df["day"].isna().all():
                    wc = parse_week_code_from_name(Path(f).name)
                    start, end = week_bounds_from_code(wc) if wc else (None, None)
                    if start and end:
                        dates = pd.date_range(start, end)
                        # if weekly file without day detail, replicate evenly by day
                        tmp = []
                        for _, r in df.iterrows():
                            for d in dates:
                                rr = r.copy(); rr["day"] = d
                                tmp.append(rr)
                        df = pd.DataFrame(tmp)
                df["supplier_article"] = df["supplier_article"].map(clean_article)
                df["nm_id"] = to_numeric(df["nm_id"])
                df["subject"] = df["subject"].map(normalize_text)
                df["query"] = df["query"].map(normalize_text)
                df["frequency"] = to_numeric(df["frequency"]).fillna(0)
                df["median_position"] = to_numeric(df["median_position"])
                df["visibility"] = to_numeric(df["visibility"])
                dfs.append(df)
            except Exception as e:
                self.warnings.append(f"Ошибка чтения search {f}: {e}")
        if not dfs:
            return pd.DataFrame()
        out = pd.concat(dfs, ignore_index=True)
        out = out[out["subject"].map(is_target_subject)].copy()
        out = out[out["supplier_article"].map(valid_article)].copy()
        out["code"] = out["supplier_article"].map(clean_code_from_article)
        return out

    def load_funnel(self) -> pd.DataFrame:
        cands = self._list([
            f"{self.root_reports}/Воронка продаж/{self.store}",
            f"{self.root_reports}/Воронка продаж",
            "",
        ], contains=["Воронка продаж"])
        path = cands[0] if cands else None
        if not path:
            return pd.DataFrame()
        try:
            df, _ = read_excel_best(self.storage.read_bytes(path), header_candidates=(0,1,2))
            for tgt, aliases in {
                "day": ["dt", "Дата", "date"],
                "nm_id": ["nmID", "nmId", "Артикул WB"],
                "open_card_count": ["openCardCount", "Открытие карточки"],
                "cart_count": ["addToCartCount", "Добавили в корзину"],
                "orders_funnel": ["ordersCount", "Заказали", "Заказы"],
                "buyouts": ["buyoutsCount"],
                "cancel_count": ["cancelCount"],
                "conv_to_cart": ["addToCartConversion", "Конверсия в корзину"],
                "conv_cart_to_order": ["cartToOrderConversion", "Конверсия в заказ"],
            }.items():
                df = ensure_col(df, tgt, aliases)
            df = df[["day","nm_id","open_card_count","cart_count","orders_funnel","buyouts","cancel_count","conv_to_cart","conv_cart_to_order"]].copy()
            df["day"] = to_dt(df["day"]).dt.normalize()
            df["nm_id"] = to_numeric(df["nm_id"])
            for c in ["open_card_count","cart_count","orders_funnel","buyouts","cancel_count","conv_to_cart","conv_cart_to_order"]:
                df[c] = to_numeric(df[c])
            return df[df["day"].notna()].copy()
        except Exception as e:
            self.warnings.append(f"Ошибка чтения funnel {path}: {e}")
            return pd.DataFrame()

    def load_ads(self) -> pd.DataFrame:
        files = self._filter_last_weeks(self._list([
            f"{self.root_reports}/Реклама/{self.store}/Недельные",
            f"{self.root_reports}/Реклама/{self.store}",
            "",
        ], contains=["Реклама"]), 12)
        dfs = []
        for f in files:
            try:
                df, sheet = read_excel_best(self.storage.read_bytes(f), preferred_sheet=["Статистика_Ежедневно", "Статистика_Итого"], header_candidates=(0,))
                for tgt, aliases in {
                    "day": ["Дата", "date"],
                    "nm_id": ["Артикул WB", "nmId", "nmID"],
                    "subject": ["Название предмета", "Предмет"],
                    "impressions": ["Показы"],
                    "clicks": ["Клики"],
                    "orders": ["Заказы"],
                    "spend": ["Расход", "Продвижение"],
                    "ctr": ["CTR"],
                    "cpc": ["CPC"],
                }.items():
                    df = ensure_col(df, tgt, aliases)
                if sheet == "Статистика_Итого" or df["day"].isna().all():
                    wc = parse_week_code_from_name(Path(f).name)
                    start, end = week_bounds_from_code(wc) if wc else (None, None)
                    if start and end:
                        dates = pd.date_range(start, end)
                        tmp = []
                        for _, r in df.iterrows():
                            for d in dates:
                                rr = r.copy(); rr["day"] = d
                                for c in ["impressions","clicks","orders","spend"]:
                                    rr[c] = safe_div(rr[c], len(dates)) if pd.notna(rr[c]) else np.nan
                                tmp.append(rr)
                        df = pd.DataFrame(tmp)
                df = df[["day","nm_id","subject","impressions","clicks","orders","spend","ctr","cpc"]].copy()
                df["day"] = to_dt(df["day"]).dt.normalize()
                df["nm_id"] = to_numeric(df["nm_id"])
                df["subject"] = df["subject"].map(normalize_text)
                for c in ["impressions","clicks","orders","spend","ctr","cpc"]:
                    df[c] = to_numeric(df[c])
                dfs.append(df)
            except Exception as e:
                self.warnings.append(f"Ошибка чтения ads {f}: {e}")
        if not dfs:
            return pd.DataFrame()
        return pd.concat(dfs, ignore_index=True)

    def load_econ(self) -> pd.DataFrame:
        cands = self._list([
            f"{self.root_reports}/Финансовые показатели/{self.store}",
            f"{self.root_reports}/Финансовые показатели/{self.store}/Недельные",
            "",
        ], contains=["Экономика"])
        path = cands[0] if cands else None
        if not path:
            return pd.DataFrame()
        try:
            df, _ = read_excel_best(self.storage.read_bytes(path), preferred_sheet=["Юнит экономика"], header_candidates=(0,1,2))
            colmap = {
                "week_code": ["Неделя"],
                "nm_id": ["Артикул WB"],
                "supplier_article": ["Артикул продавца"],
                "subject": ["Предмет"],
                "sales_qty": ["Чистые продажи, шт", "Продажи, шт"],
                "buyout_pct": ["Процент выкупа"],
                "avg_sale_price": ["Средняя цена продажи"],
                "avg_buyer_price": ["Средняя цена покупателя"],
                "spp": ["СПП, %"],
                "commission_unit": ["Комиссия WB, руб/ед"],
                "acquiring_unit": ["Эквайринг, руб/ед"],
                "log_direct_unit": ["Логистика прямая, руб/ед"],
                "log_return_unit": ["Логистика обратная, руб/ед"],
                "storage_unit": ["Хранение, руб/ед"],
                "acceptance_unit": ["Приёмка, руб/ед"],
                "ads_unit": ["Реклама, руб/ед"],
                "other_unit": ["Прочие расходы, руб/ед", "Штрафы и удержания, руб/ед"],
                "cost_unit": ["Себестоимость, руб"],
                "gp_unit": ["Валовая прибыль, руб/ед"],
                "np_unit": ["Чистая прибыль, руб/ед"],
                "margin_pct": ["Валовая рентабельность, %"],
                "profitability_pct": ["Чистая рентабельность, %"],
            }
            for tgt, aliases in colmap.items():
                df = ensure_col(df, tgt, aliases)
            keep = list(colmap.keys())
            df = df[keep].copy()
            df["supplier_article"] = df["supplier_article"].map(clean_article)
            df["nm_id"] = to_numeric(df["nm_id"])
            df["subject"] = df["subject"].map(normalize_text)
            for c in keep:
                if c not in {"week_code","supplier_article","subject"}:
                    df[c] = to_numeric(df[c])
            df = df[df["subject"].map(is_target_subject)].copy()
            df = df[df["supplier_article"].map(valid_article)].copy()
            df["code"] = df["supplier_article"].map(clean_code_from_article)
            return df
        except Exception as e:
            self.warnings.append(f"Ошибка чтения экономики {path}: {e}")
            return pd.DataFrame()

    def load_abc(self) -> pd.DataFrame:
        files = self._filter_last_weeks(self._list([
            f"{self.root_reports}/ABC",
            "",
        ], contains=["wb_abc_report_goods__"]), 20)
        dfs = []
        for f in files:
            try:
                df, _ = read_excel_best(self.storage.read_bytes(f), header_candidates=(0,))
                for tgt, aliases in {
                    "nm_id": ["Артикул WB", "nmId"],
                    "supplier_article": ["Артикул продавца", "supplierArticle"],
                    "subject": ["Предмет"],
                    "brand": ["Бренд"],
                    "orders": ["Кол-во продаж", "Заказы", "orders"],
                    "gross_profit": ["Валовая прибыль"],
                    "gross_revenue": ["Валовая выручка"],
                    "drr_pct": ["ДРР, %", "ДРР"],
                    "margin_pct": ["Маржинальность, %", "Маржинальность"],
                    "profitability_pct": ["Рентабельность, %", "Рентабельность"],
                }.items():
                    df = ensure_col(df, tgt, aliases)
                start, end = parse_abc_period_from_name(Path(f).name)
                wc = week_code_from_date(start) if start else parse_week_code_from_name(Path(f).name)
                df = df[["nm_id","supplier_article","subject","brand","orders","gross_profit","gross_revenue","drr_pct","margin_pct","profitability_pct"]].copy()
                df["week_code"] = wc
                df["week_start"] = pd.Timestamp(start) if start else pd.NaT
                df["week_end"] = pd.Timestamp(end) if end else pd.NaT
                df["nm_id"] = to_numeric(df["nm_id"])
                df["supplier_article"] = df["supplier_article"].map(clean_article)
                df["subject"] = df["subject"].map(normalize_text)
                df["brand"] = df["brand"].map(normalize_text)
                for c in ["orders","gross_profit","gross_revenue","drr_pct","margin_pct","profitability_pct"]:
                    df[c] = to_numeric(df[c])
                dfs.append(df)
            except Exception as e:
                self.warnings.append(f"Ошибка чтения ABC {f}: {e}")
        if not dfs:
            # fallback from economics overall weekly
            cands = self._list([
                f"{self.root_reports}/Финансовые показатели/{self.store}",
                "",
            ], contains=["Экономика"])
            if cands:
                try:
                    df, _ = read_excel_best(self.storage.read_bytes(cands[0]), preferred_sheet=["Общий факт за неделю"], header_candidates=(0,1,2))
                    for tgt, aliases in {
                        "week_code": ["Неделя"],
                        "nm_id": ["Артикул WB"],
                        "supplier_article": ["Артикул продавца"],
                        "subject": ["Предмет"],
                        "brand": ["Бренд"],
                        "orders": ["Чистые продажи, шт", "Продажи, шт"],
                        "gross_profit": ["Валовая прибыль"],
                        "gross_revenue": ["Валовая выручка"],
                        "drr_pct": ["Реклама", "ДРР, %"],
                        "margin_pct": ["Валовая рентабельность, %"],
                        "profitability_pct": ["Чистая рентабельность, %"],
                    }.items():
                        df = ensure_col(df, tgt, aliases)
                    df = df[["week_code","nm_id","supplier_article","subject","brand","orders","gross_profit","gross_revenue","drr_pct","margin_pct","profitability_pct"]].copy()
                    df["week_start"] = df["week_code"].map(lambda x: pd.Timestamp(week_bounds_from_code(x)[0]) if x else pd.NaT)
                    df["week_end"] = df["week_code"].map(lambda x: pd.Timestamp(week_bounds_from_code(x)[1]) if x else pd.NaT)
                    dfs = [df]
                except Exception as e:
                    self.warnings.append(f"Ошибка fallback ABC из экономики: {e}")
        if not dfs:
            return pd.DataFrame()
        out = pd.concat(dfs, ignore_index=True)
        out = out[out["subject"].map(is_target_subject)].copy()
        out = out[out["supplier_article"].map(valid_article)].copy()
        out["code"] = out["supplier_article"].map(clean_code_from_article)
        return out

    def load_entry(self) -> pd.DataFrame:
        files = self._filter_last_weeks(self._list([
            f"{self.root_reports}/Точки входа/{self.store}",
            "",
        ], contains=["Портрет покупателя. Точки входа"]), 12)
        dfs = []
        for f in files:
            try:
                data = self.storage.read_bytes(f)
                df, _ = read_excel_best(data, preferred_sheet=["Детализация по артикулам"], header_candidates=(1,0,2))
                for tgt, aliases in {
                    "section": ["Раздел"],
                    "entry_point": ["Точка входа"],
                    "nm_id": ["Артикул ВБ", "Артикул WB", "nmId"],
                    "supplier_article": ["Артикул продавца", "supplierArticle"],
                    "brand": ["Бренд"],
                    "title": ["Название"],
                    "subject": ["Предмет"],
                    "impressions": ["Показы"],
                    "clicks": ["Перешли в карточку", "Клики"],
                    "ctr": ["CTR"],
                    "cart": ["Добавили в корзину"],
                    "conv_cart": ["Конверсия в корзину"],
                    "orders": ["Заказали"],
                    "conv_order": ["Конверсия в заказ"],
                }.items():
                    df = ensure_col(df, tgt, aliases)
                start, end = parse_entry_period_from_name(Path(f).name)
                wc = week_code_from_date(start) if start else parse_week_code_from_name(Path(f).name)
                df = df[["section","entry_point","nm_id","supplier_article","brand","title","subject","impressions","clicks","ctr","cart","conv_cart","orders","conv_order"]].copy()
                df["week_code"] = wc
                df["week_start"] = pd.Timestamp(start) if start else pd.NaT
                df["week_end"] = pd.Timestamp(end) if end else pd.NaT
                df["section"] = df["section"].map(normalize_text)
                df["entry_point"] = df["entry_point"].map(normalize_text)
                df["nm_id"] = to_numeric(df["nm_id"])
                df["supplier_article"] = df["supplier_article"].map(clean_article)
                df["subject"] = df["subject"].map(normalize_text)
                for c in ["impressions","clicks","ctr","cart","conv_cart","orders","conv_order"]:
                    df[c] = to_numeric(df[c])
                dfs.append(df)
            except Exception as e:
                self.warnings.append(f"Ошибка чтения entry {f}: {e}")
        if not dfs:
            return pd.DataFrame()
        out = pd.concat(dfs, ignore_index=True)
        out = out[out["subject"].map(is_target_subject)].copy()
        out = out[out["supplier_article"].map(valid_article)].copy()
        out["code"] = out["supplier_article"].map(clean_code_from_article)
        return out

    def load_rrp(self) -> pd.DataFrame:
        cands = self._list([""], contains=["РРЦ"])
        path = cands[0] if cands else None
        if not path:
            return pd.DataFrame()
        try:
            df, _ = read_excel_best(self.storage.read_bytes(path), preferred_sheet=["TF"], header_candidates=(0,1))
            df = ensure_col(df, "rrp_key", ["ПРАВИЛЬНЫЙ АРТИКУЛ"])
            df = ensure_col(df, "rrp", ["РРЦ"])
            df = df[["rrp_key","rrp"]].copy()
            df["rrp_key"] = df["rrp_key"].map(normalize_text)
            df["rrp"] = to_numeric(df["rrp"])
            return df[df["rrp_key"] != ""].drop_duplicates("rrp_key")
        except Exception as e:
            self.warnings.append(f"Ошибка чтения РРЦ {path}: {e}")
            return pd.DataFrame()

    def load_all(self) -> Loaded:
        log("Loading orders")
        orders = self.load_orders()
        log("Loading stocks")
        stocks = self.load_stocks()
        log("Loading search")
        search = self.load_search()
        log("Loading funnel")
        funnel = self.load_funnel()
        log("Loading ads")
        ads = self.load_ads()
        log("Loading economics")
        econ = self.load_econ()
        log("Loading ABC")
        abc = self.load_abc()
        log("Loading entry points")
        entry = self.load_entry()
        log("Loading RRP")
        rrp = self.load_rrp()
        return Loaded(orders, stocks, search, funnel, ads, econ, abc, entry, rrp, self.warnings)


# -------------------------
# Analyzer
# -------------------------

class Analyzer:
    def __init__(self, data: Loaded):
        self.data = data
        self.latest_day = self._detect_latest_day()
        self.cur_start = self.latest_day - timedelta(days=13)
        self.prev_end = self.cur_start - timedelta(days=1)
        self.prev_start = self.prev_end - timedelta(days=13)
        self.lookback_loc_start = self.latest_day - timedelta(days=27)
        self.rrp_map = dict(zip(data.rrp["rrp_key"], data.rrp["rrp"])) if not data.rrp.empty else {}
        self.master = self._build_master()
        log(f"Analysis windows: prev {self.prev_start}..{self.prev_end}; cur {self.cur_start}..{self.latest_day}")

        self.article_day = self.build_article_day()
        self.localization_daily = self.build_localization_daily()
        self.article_period = self.build_article_period()
        self.product_period = self.aggregate_period(self.article_period, "code")
        self.category_period = self.aggregate_period(self.article_period, "subject")
        self.product_with_sku = self.build_product_with_articles()
        self.summary_struct = self.build_summary_struct()
        self.reasons_category = self.build_reason_rows(self.category_period, "category")
        self.reasons_product = self.build_reason_rows(self.product_period, "product")
        self.channels = self.build_channels()

    def _detect_latest_day(self) -> date:
        candidates = []
        for df, col in [
            (self.data.orders, "day"), (self.data.search, "day"), (self.data.funnel, "day"),
        ]:
            if not df.empty and col in df.columns and df[col].notna().any():
                candidates.append(pd.to_datetime(df[col], errors="coerce").max().date())
        if candidates:
            return max(candidates)
        return date.today()

    def _build_master(self) -> pd.DataFrame:
        frames = []
        for df in [self.data.orders, self.data.search, self.data.abc, self.data.econ_unit, self.data.entry]:
            if df.empty:
                continue
            cols = [c for c in ["supplier_article","nm_id","subject","brand","code"] if c in df.columns]
            if cols:
                frames.append(df[cols].copy())
        if not frames:
            return pd.DataFrame(columns=["supplier_article","nm_id","subject","brand","code"])
        m = pd.concat(frames, ignore_index=True)
        if "brand" not in m.columns:
            m["brand"] = ""
        m["supplier_article"] = m["supplier_article"].map(clean_article)
        m["subject"] = m["subject"].map(normalize_text)
        if "code" not in m.columns:
            m["code"] = m["supplier_article"].map(clean_code_from_article)
        m = m.drop_duplicates(subset=["supplier_article","nm_id"], keep="first")
        return m

    def build_article_day(self) -> pd.DataFrame:
        # orders daily
        o = self.data.orders.copy()
        if o.empty:
            return pd.DataFrame()
        o = o[(o["day"] >= pd.Timestamp(self.prev_start)) & (o["day"] <= pd.Timestamp(self.latest_day))].copy()
        o["is_valid_order"] = ~o["isCancel"].astype(bool)
        od = o[o["is_valid_order"]].groupby(["day","supplier_article","nm_id","subject","code"], dropna=False).agg(
            orders_day=("is_valid_order","sum"),
            finishedPrice_avg=("finishedPrice","mean"),
            priceWithDisc_avg=("priceWithDisc","mean"),
            spp_avg=("spp","mean"),
        ).reset_index()

        # funnel
        f = self.data.funnel.copy()
        if not f.empty:
            f = f[(f["day"] >= pd.Timestamp(self.prev_start)) & (f["day"] <= pd.Timestamp(self.latest_day))].copy()
            f = f.merge(self.master[["supplier_article","nm_id","subject","code"]].drop_duplicates("nm_id"), on="nm_id", how="left")
            fg = f.groupby(["day","supplier_article","nm_id","subject","code"], dropna=False).agg(
                open_card_count=("open_card_count","sum"),
                cart_count=("cart_count","sum"),
                orders_funnel=("orders_funnel","sum"),
                conv_to_cart=("conv_to_cart","mean"),
                conv_cart_to_order=("conv_cart_to_order","mean"),
            ).reset_index()
        else:
            fg = pd.DataFrame(columns=["day","supplier_article","nm_id","subject","code"])

        # search daily by article
        s = self.data.search.copy()
        if not s.empty:
            s = s[(s["day"] >= pd.Timestamp(self.prev_start)) & (s["day"] <= pd.Timestamp(self.latest_day))].copy()
            # demand by category/day from unique queries
            catd = s.groupby(["day","subject","query"], dropna=False).agg(freq=("frequency","max")).reset_index()
            catd = catd.groupby(["day","subject"], dropna=False).agg(demand_day=("freq","sum")).reset_index()
            sg = s.groupby(["day","supplier_article","nm_id","subject","code"], dropna=False).agg(
                search_freq=("frequency","sum"),
                visibility=("visibility","mean"),
                median_position=("median_position","median"),
            ).reset_index()
            sg = sg.merge(catd, on=["day","subject"], how="left")
        else:
            sg = pd.DataFrame(columns=["day","supplier_article","nm_id","subject","code"])

        # ads daily
        a = self.data.ads.copy()
        if not a.empty:
            a = a[(a["day"] >= pd.Timestamp(self.prev_start)) & (a["day"] <= pd.Timestamp(self.latest_day))].copy()
            a = a.merge(self.master[["supplier_article","nm_id","subject","code"]].drop_duplicates("nm_id"), on="nm_id", how="left")
            ag = a.groupby(["day","supplier_article","nm_id","subject","code"], dropna=False).agg(
                ad_spend=("spend","sum"),
                ad_clicks=("clicks","sum"),
                ad_orders=("orders","sum"),
                ad_impressions=("impressions","sum"),
            ).reset_index()
        else:
            ag = pd.DataFrame(columns=["day","supplier_article","nm_id","subject","code"])

        # combine
        keys = pd.concat([
            od[["day","supplier_article","nm_id","subject","code"]],
            fg[[c for c in ["day","supplier_article","nm_id","subject","code"] if c in fg.columns]],
            sg[[c for c in ["day","supplier_article","nm_id","subject","code"] if c in sg.columns]],
            ag[[c for c in ["day","supplier_article","nm_id","subject","code"] if c in ag.columns]],
        ], ignore_index=True).drop_duplicates()

        cur = keys.merge(od, on=["day","supplier_article","nm_id","subject","code"], how="left")
        if not fg.empty:
            cur = cur.merge(fg, on=["day","supplier_article","nm_id","subject","code"], how="left")
        if not sg.empty:
            cur = cur.merge(sg, on=["day","supplier_article","nm_id","subject","code"], how="left")
        if not ag.empty:
            cur = cur.merge(ag, on=["day","supplier_article","nm_id","subject","code"], how="left")

        for c in [
            "orders_day","finishedPrice_avg","priceWithDisc_avg","spp_avg","open_card_count","cart_count",
            "orders_funnel","conv_to_cart","conv_cart_to_order","search_freq","visibility","median_position",
            "demand_day","ad_spend","ad_clicks","ad_orders","ad_impressions"
        ]:
            if c not in cur.columns:
                cur[c] = np.nan

        # economics week mapping for daily GP forecast from economics
        e = self.data.econ_unit.copy()
        if not e.empty:
            em = e[["week_code","supplier_article","nm_id","gp_unit","buyout_pct"]].copy()
            cur["week_code"] = cur["day"].map(week_code_from_date)
            cur = cur.merge(em, on=["week_code","supplier_article","nm_id"], how="left")
            # fallback by article only
            miss = cur["gp_unit"].isna()
            if miss.any():
                em2 = e.groupby(["week_code","supplier_article"], dropna=False).agg(gp_unit=("gp_unit","mean"), buyout_pct=("buyout_pct","mean")).reset_index()
                tmp = cur.loc[miss, ["week_code","supplier_article"]].merge(em2, on=["week_code","supplier_article"], how="left")
                cur.loc[miss, "gp_unit"] = tmp["gp_unit"].values
                cur.loc[miss, "buyout_pct"] = tmp["buyout_pct"].values
            cur["gross_profit_day"] = cur["orders_day"].fillna(0) * cur["gp_unit"].fillna(0) * cur["buyout_pct"].fillna(100) / 100.0
        else:
            cur["gross_profit_day"] = np.nan

        cur["rrp_key"] = cur["supplier_article"].map(normalize_rrp_key)
        cur["rrp"] = cur["rrp_key"].map(self.rrp_map)
        cur["finished_rrp_coeff"] = cur.apply(lambda r: safe_div(r.get("finishedPrice_avg"), r.get("rrp")), axis=1)
        cur["pwd_rrp_coeff"] = cur.apply(lambda r: safe_div(r.get("priceWithDisc_avg"), r.get("rrp")), axis=1)
        return cur.sort_values(["day","subject","code","supplier_article"], ascending=[False,True,True,True])

    def build_localization_daily(self) -> pd.DataFrame:
        s = self.data.stocks.copy()
        o = self.data.orders.copy()
        if s.empty or o.empty:
            return pd.DataFrame(columns=["Артикул","Дата","Покрытие, %"])
        s = s[(s["day"] >= pd.Timestamp(self.prev_start)) & (s["day"] <= pd.Timestamp(self.latest_day))].copy()
        o = o[(o["day"] >= pd.Timestamp(self.lookback_loc_start)) & (o["day"] <= pd.Timestamp(self.latest_day)) & (~o["isCancel"].astype(bool))].copy()
        s = s[s["subject"].map(is_target_subject)].copy()
        o = o[o["subject"].map(is_target_subject)].copy()
        if s.empty or o.empty:
            return pd.DataFrame(columns=["Артикул","Дата","Покрытие, %"])

        latest_stock_day = s["day"].max()
        latest = s[s["day"] == latest_stock_day].copy()
        main_wh = []
        for art, g in latest.groupby("supplier_article", dropna=False):
            g = g.groupby("warehouse", as_index=False)["stock_total"].sum().sort_values("stock_total", ascending=False)
            total = g["stock_total"].sum()
            if total <= 0:
                continue
            g["share"] = g["stock_total"] / total
            g["cum"] = g["share"].cumsum()
            picked = g[g["cum"] <= 0.97].copy()
            if picked.empty:
                picked = g.head(1).copy()
            elif picked["cum"].max() < 0.97 and len(picked) < len(g):
                picked = g.head(len(picked) + 1).copy()
            picked["supplier_article"] = art
            main_wh.append(picked[["supplier_article","warehouse"]])
        if not main_wh:
            return pd.DataFrame(columns=["Артикул","Дата","Покрытие, %"])
        main_wh = pd.concat(main_wh, ignore_index=True).drop_duplicates()

        wh_orders = o.groupby(["supplier_article","warehouse"], dropna=False).size().reset_index(name="orders_28d")
        wh_orders["avg_orders_day_wh"] = wh_orders["orders_28d"] / 28.0
        wh_orders = wh_orders.merge(main_wh, on=["supplier_article","warehouse"], how="inner")
        # weights by warehouse order share, fallback equal
        total_ord = wh_orders.groupby("supplier_article", as_index=False)["orders_28d"].sum().rename(columns={"orders_28d":"orders_total"})
        wh_orders = wh_orders.merge(total_ord, on="supplier_article", how="left")
        wh_orders["weight"] = wh_orders.apply(lambda r: safe_div(r["orders_28d"], r["orders_total"]), axis=1)
        for art, idx in wh_orders.groupby("supplier_article").groups.items():
            if wh_orders.loc[idx, "weight"].isna().all() or wh_orders.loc[idx, "weight"].sum() == 0:
                wh_orders.loc[idx, "weight"] = 1 / len(idx)

        # repeat weekly stock snapshot for each day in week
        stock_rows = []
        for _, r in s.iterrows():
            d = pd.Timestamp(r["day"]).date()
            week_code = week_code_from_date(d)
            start, end = week_bounds_from_code(week_code)
            if not start or not end:
                continue
            for dd in pd.date_range(start, end):
                if dd.date() < self.prev_start or dd.date() > self.latest_day:
                    continue
                stock_rows.append({
                    "day": dd.normalize(),
                    "supplier_article": r["supplier_article"],
                    "warehouse": r["warehouse"],
                    "stock_qty": float(r["stock_available"]),
                })
        if not stock_rows:
            return pd.DataFrame(columns=["Артикул","Дата","Покрытие, %"])
        daily_stock = pd.DataFrame(stock_rows)
        daily_stock = daily_stock.merge(wh_orders[["supplier_article","warehouse","avg_orders_day_wh","weight"]], on=["supplier_article","warehouse"], how="inner")
        daily_stock["coverage_days"] = daily_stock.apply(lambda r: safe_div(r["stock_qty"], r["avg_orders_day_wh"]), axis=1)
        daily_stock["available_flag"] = np.where(daily_stock["stock_qty"] >= daily_stock["avg_orders_day_wh"], 1.0, 0.0)
        cov = daily_stock.groupby(["day","supplier_article"], dropna=False).agg(coverage=("available_flag", lambda x: np.average(x, weights=daily_stock.loc[x.index, "weight"]))).reset_index()
        cov["coverage"] = cov["coverage"].fillna(0)
        cov["Артикул"] = cov["supplier_article"]
        cov["Дата"] = cov["day"]
        cov["Покрытие, %"] = cov["coverage"]
        return cov[["Артикул","Дата","Покрытие, %"]].sort_values(["Артикул","Дата"], ascending=[True,False])

    def _window_df(self, df: pd.DataFrame, start: date, end: date, day_col: str = "day") -> pd.DataFrame:
        if df.empty or day_col not in df.columns:
            return df.iloc[0:0].copy()
        s = pd.Timestamp(start); e = pd.Timestamp(end)
        return df[(pd.to_datetime(df[day_col], errors="coerce") >= s) & (pd.to_datetime(df[day_col], errors="coerce") <= e)].copy()

    def _window_abc(self, start: date, end: date) -> pd.DataFrame:
        a = self.data.abc.copy()
        if a.empty:
            return a
        return a[(a["week_start"].dt.date >= start) & (a["week_end"].dt.date <= end)].copy()

    def _window_entry(self, start: date, end: date) -> pd.DataFrame:
        e = self.data.entry.copy()
        if e.empty:
            return e
        return e[(e["week_start"].dt.date >= start) & (e["week_end"].dt.date <= end)].copy()

    def _window_econ(self, start: date, end: date) -> pd.DataFrame:
        e = self.data.econ_unit.copy()
        if e.empty:
            return e
        weeks = set()
        for d in pd.date_range(start, end):
            weeks.add(week_code_from_date(d))
        return e[e["week_code"].isin(weeks)].copy()

    def build_article_period(self) -> pd.DataFrame:
        rows = []
        demand_map = {}
        if not self.data.search.empty:
            s = self.data.search.copy()
            catd = s.groupby(["day","subject","query"], dropna=False).agg(freq=("frequency","max")).reset_index()
            catd = catd.groupby(["day","subject"], dropna=False).agg(demand=("freq","sum")).reset_index()
            for period_name, start, end in [("prev_14d", self.prev_start, self.prev_end), ("cur_14d", self.cur_start, self.latest_day)]:
                x = self._window_df(catd, start, end).groupby("subject", as_index=False)["demand"].sum()
                for _, r in x.iterrows():
                    demand_map[(period_name, r["subject"])] = r["demand"]

        loc = self.localization_daily.copy()
        if not loc.empty:
            loc["Дата"] = pd.to_datetime(loc["Дата"]).dt.normalize()
            loc_prev = self._window_df(loc.rename(columns={"Дата":"day","Артикул":"supplier_article","Покрытие, %":"coverage"}), self.prev_start, self.prev_end)
            loc_cur = self._window_df(loc.rename(columns={"Дата":"day","Артикул":"supplier_article","Покрытие, %":"coverage"}), self.cur_start, self.latest_day)
            loc_prev = loc_prev.groupby("supplier_article", as_index=False)["coverage"].mean().rename(columns={"coverage":"loc_prev"})
            loc_cur = loc_cur.groupby("supplier_article", as_index=False)["coverage"].mean().rename(columns={"coverage":"loc_cur"})
        else:
            loc_prev = pd.DataFrame(columns=["supplier_article","loc_prev"])
            loc_cur = pd.DataFrame(columns=["supplier_article","loc_cur"])

        arts = sorted(set(self.article_day["supplier_article"].dropna().tolist()) | set(self.data.abc["supplier_article"].dropna().tolist()) | set(self.data.econ_unit["supplier_article"].dropna().tolist()))
        for art in arts:
            if not valid_article(art):
                continue
            code = clean_code_from_article(art)
            subj = ""
            brand = "TopFace"
            ms = self.master[self.master["supplier_article"] == art]
            if not ms.empty:
                subj = ms["subject"].dropna().astype(str).iloc[0]
                if "brand" in ms.columns and ms["brand"].dropna().any():
                    brand = ms["brand"].dropna().astype(str).iloc[0]
            if not is_target_subject(subj):
                continue
            ady = self.article_day[self.article_day["supplier_article"] == art].copy()
            abc_all = self.data.abc[self.data.abc["supplier_article"] == art].copy()
            entry_all = self.data.entry[self.data.entry["supplier_article"] == art].copy()
            for period_name, start, end in [("prev_14d", self.prev_start, self.prev_end), ("cur_14d", self.cur_start, self.latest_day)]:
                d = self._window_df(ady, start, end)
                a = abc_all[(abc_all["week_start"].dt.date >= start) & (abc_all["week_end"].dt.date <= end)]
                e = entry_all[(entry_all["week_start"].dt.date >= start) & (entry_all["week_end"].dt.date <= end)]
                eu = self._window_econ(start, end)
                eu = eu[eu["supplier_article"] == art]
                sales_w = to_numeric(eu["sales_qty"]).fillna(0)
                def wavg(col):
                    ser = to_numeric(eu[col]).fillna(np.nan)
                    if eu.empty:
                        return np.nan
                    if sales_w.sum() > 0 and ser.notna().any():
                        return np.average(np.nan_to_num(ser), weights=np.where(ser.notna(), sales_w, 0))
                    return ser.mean()

                row = {
                    "Период": period_name,
                    "Категория": subj,
                    "Товар": code,
                    "Артикул": art,
                    "Бренд": brand,
                    # daily economics-driven facts
                    "Валовая прибыль день, ₽": d["gross_profit_day"].sum(),
                    "Клики, шт": e["clicks"].sum() if not e.empty else d["open_card_count"].sum(),
                    "Показы, шт": e["impressions"].sum() if not e.empty else np.nan,
                    "CTR, %": safe_div((e["clicks"].sum() if not e.empty else np.nan), (e["impressions"].sum() if not e.empty else np.nan)) * 100,
                    "Заказы воронка, шт": d["orders_funnel"].sum(),
                    "Открытия карточки, шт": d["open_card_count"].sum(),
                    "Добавления в корзину, шт": d["cart_count"].sum(),
                    "Конверсия в корзину, %": d["conv_to_cart"].mean(),
                    "Конверсия корзина-заказ, %": d["conv_cart_to_order"].mean(),
                    # ABC weekly/monthly actuals
                    "Валовая прибыль ABC, ₽": a["gross_profit"].sum(),
                    "Валовая выручка ABC, ₽": a["gross_revenue"].sum(),
                    "Продажи ABC, шт": a["orders"].sum(),
                    "Валовая прибыль на 1 продажу, ₽": safe_div(a["gross_profit"].sum(), a["orders"].sum()),
                    # prices
                    "finishedPrice, ₽": d["finishedPrice_avg"].mean(),
                    "priceWithDisc, ₽": d["priceWithDisc_avg"].mean(),
                    "SPP, %": d["spp_avg"].mean(),
                    "РРЦ, ₽": self.rrp_map.get(normalize_rrp_key(art), np.nan),
                    "Коэф finishedPrice к РРЦ": safe_div(d["finishedPrice_avg"].mean(), self.rrp_map.get(normalize_rrp_key(art), np.nan)),
                    "Коэф priceWithDisc к РРЦ": safe_div(d["priceWithDisc_avg"].mean(), self.rrp_map.get(normalize_rrp_key(art), np.nan)),
                    # search / visibility
                    "Спрос категории, шт": demand_map.get((period_name, subj), np.nan),
                    "Видимость, %": d["visibility"].mean(),
                    "Медианная позиция": d["median_position"].mean(),
                    # ads
                    "Расходы на рекламу, ₽": d["ad_spend"].sum(),
                    "Рекламные клики, шт": d["ad_clicks"].sum(),
                    "Рекламные заказы, шт": d["ad_orders"].sum(),
                    "Рекламные показы, шт": d["ad_impressions"].sum(),
                    # localization
                    "Покрытие локализации, %": np.nan,
                    # economics units
                    "Комиссия WB, ₽/ед": wavg("commission_unit"),
                    "Эквайринг, ₽/ед": wavg("acquiring_unit"),
                    "Логистика прямая, ₽/ед": wavg("log_direct_unit"),
                    "Логистика обратная, ₽/ед": wavg("log_return_unit"),
                    "Хранение, ₽/ед": wavg("storage_unit"),
                    "Приемка, ₽/ед": wavg("acceptance_unit"),
                    "Реклама, ₽/ед": wavg("ads_unit"),
                    "Прочие расходы, ₽/ед": wavg("other_unit"),
                    "Себестоимость, ₽": wavg("cost_unit"),
                    "Валовая прибыль, ₽/ед": wavg("gp_unit"),
                    "Чистая прибыль, ₽/ед": wavg("np_unit"),
                    "Валовая рентабельность, %": wavg("margin_pct"),
                    "Чистая рентабельность, %": wavg("profitability_pct"),
                }
                rows.append(row)
        out = pd.DataFrame(rows)
        if out.empty:
            return out
        out = out.merge(loc_prev, left_on="Артикул", right_on="supplier_article", how="left").drop(columns=[c for c in ["supplier_article"] if c in out.columns])
        out = out.merge(loc_cur, left_on="Артикул", right_on="supplier_article", how="left").drop(columns=[c for c in ["supplier_article"] if c in out.columns])
        out["Покрытие локализации, %"] = np.where(out["Период"] == "prev_14d", out["loc_prev"], out["loc_cur"])
        out = out.drop(columns=[c for c in ["loc_prev","loc_cur"] if c in out.columns])
        return out

    def aggregate_period(self, df: pd.DataFrame, level: str) -> pd.DataFrame:
        if df.empty:
            return df
        key = {"subject":"Категория","code":"Товар"}[level]
        group_cols = ["Период", key]
        numeric_cols = [c for c in df.columns if c not in {"Период","Категория","Товар","Артикул","Бренд"}]
        agg_map = {}
        for c in numeric_cols:
            if c in {"finishedPrice, ₽","priceWithDisc, ₽","SPP, %","РРЦ, ₽","Коэф finishedPrice к РРЦ","Коэф priceWithDisc к РРЦ","Видимость, %","Медианная позиция","Комиссия WB, ₽/ед","Эквайринг, ₽/ед","Логистика прямая, ₽/ед","Логистика обратная, ₽/ед","Хранение, ₽/ед","Приемка, ₽/ед","Реклама, ₽/ед","Прочие расходы, ₽/ед","Себестоимость, ₽","Валовая прибыль, ₽/ед","Чистая прибыль, ₽/ед","Валовая рентабельность, %","Чистая рентабельность, %","Покрытие локализации, %"}:
                agg_map[c] = "mean"
            else:
                agg_map[c] = "sum"
        out = df.groupby(group_cols, dropna=False).agg(agg_map).reset_index()
        if level == "code":
            subj_map = df.groupby("Товар", as_index=False)["Категория"].first()
            out = out.merge(subj_map, on="Товар", how="left")
        return out

    def _pair_periods(self, df: pd.DataFrame, id_cols: List[str]) -> pd.DataFrame:
        prev = df[df["Период"] == "prev_14d"].copy().drop(columns=["Период"])
        cur = df[df["Период"] == "cur_14d"].copy().drop(columns=["Период"])
        paired = prev.merge(cur, on=id_cols, how="outer", suffixes=("_prev","_cur"))
        return paired

    def _ad_assessment(self, row: pd.Series) -> str:
        spend_d = pct_delta(row.get("Расходы на рекламу, ₽_cur"), row.get("Расходы на рекламу, ₽_prev"))
        gp_d = pct_delta(row.get("Валовая прибыль ABC, ₽_cur"), row.get("Валовая прибыль ABC, ₽_prev"))
        ord_d = pct_delta(row.get("Продажи ABC, шт_cur"), row.get("Продажи ABC, шт_prev"))
        click_d = pct_delta(row.get("Рекламные клики, шт_cur"), row.get("Рекламные клики, шт_prev"))
        vis_d = pct_delta(row.get("Видимость, %_cur"), row.get("Видимость, %_prev"))
        if pd.notna(spend_d) and spend_d >= 0.15 and pd.notna(gp_d) and gp_d < 0:
            return "Неэффективно"
        if pd.notna(spend_d) and spend_d >= 0.15 and pd.notna(ord_d) and ord_d >= 0.08 and pd.notna(gp_d) and gp_d >= 0.05:
            return "Эффективно"
        if pd.notna(spend_d) and spend_d >= 0.15 and pd.notna(click_d) and click_d >= 0.08:
            return "Частично эффективно"
        if pd.notna(spend_d) and spend_d >= 0.10 and pd.notna(vis_d) and vis_d >= 0:
            return "Защитно"
        return "Нейтрально"

    def _main_reason(self, row: pd.Series) -> Tuple[str, str]:
        gp_prev = row.get("Валовая прибыль ABC, ₽_prev", np.nan)
        gp_cur = row.get("Валовая прибыль ABC, ₽_cur", np.nan)
        ord_prev = row.get("Продажи ABC, шт_prev", np.nan)
        ord_cur = row.get("Продажи ABC, шт_cur", np.nan)
        gp1_prev = row.get("Валовая прибыль на 1 продажу, ₽_prev", np.nan)
        gp1_cur = row.get("Валовая прибыль на 1 продажу, ₽_cur", np.nan)
        volume_effect = (ord_cur - ord_prev) * (gp1_prev if pd.notna(gp1_prev) else 0)
        econ_effect = ((gp1_cur - gp1_prev) if pd.notna(gp1_cur) and pd.notna(gp1_prev) else 0) * (ord_cur if pd.notna(ord_cur) else 0)
        clicks_d = pct_delta(row.get("Клики, шт_cur"), row.get("Клики, шт_prev"))
        conv_prev = safe_div(row.get("Заказы воронка, шт_prev"), row.get("Клики, шт_prev"))
        conv_cur = safe_div(row.get("Заказы воронка, шт_cur"), row.get("Клики, шт_cur"))
        conv_d = pct_delta(conv_cur, conv_prev)
        demand_d = pct_delta(row.get("Спрос категории, шт_cur"), row.get("Спрос категории, шт_prev"))
        vis_d = pct_delta(row.get("Видимость, %_cur"), row.get("Видимость, %_prev"))
        pos_prev = row.get("Медианная позиция_prev", np.nan)
        pos_cur = row.get("Медианная позиция_cur", np.nan)
        fp_d = pct_delta(row.get("finishedPrice, ₽_cur"), row.get("finishedPrice, ₽_prev"))
        pwd_d = pct_delta(row.get("priceWithDisc, ₽_cur"), row.get("priceWithDisc, ₽_prev"))
        loc_prev = row.get("Покрытие локализации, %_prev", np.nan)
        loc_cur = row.get("Покрытие локализации, %_cur", np.nan)
        loc_d = loc_cur - loc_prev if pd.notna(loc_prev) and pd.notna(loc_cur) else np.nan
        ad_assessment = self._ad_assessment(row)
        gp1_d = pct_delta(gp1_cur, gp1_prev)
        # main
        if abs(volume_effect) >= abs(econ_effect):
            if pd.notna(loc_d) and loc_d <= -0.15:
                main = "Локализация"
            elif pd.notna(clicks_d) and clicks_d <= -0.08 and ((pd.notna(vis_d) and vis_d <= -0.08) or (pd.notna(pos_prev) and pd.notna(pos_cur) and pos_cur - pos_prev > 1.0)):
                main = "Потеря трафика из-за позиций"
            elif pd.notna(demand_d) and demand_d <= -0.08:
                main = "Снижение спроса"
            elif pd.notna(conv_d) and conv_d <= -0.08 and pd.notna(fp_d) and fp_d >= 0.03:
                main = "Цена для покупателя ухудшила конверсию"
            elif pd.notna(conv_d) and conv_d <= -0.08:
                main = "Снижение конверсии"
            elif pd.notna(clicks_d) and clicks_d <= -0.08:
                main = "Снижение трафика"
            else:
                main = "Снижение заказов"
        else:
            if pd.notna(pwd_d) and pwd_d <= -0.02 and pct_delta(gp_cur, gp_prev) < -0.05:
                main = "Снижение priceWithDisc не окупилось"
            elif ad_assessment == "Неэффективно":
                main = "Реклама съела прибыль"
            elif pd.notna(gp1_d) and gp1_d <= -0.05:
                main = "Снижение прибыли на единицу"
            else:
                main = "Экономика на единицу"
        # secondary
        secondary = ad_assessment
        if main == secondary:
            secondary = ""
        return main, secondary

    def _detail_text(self, row: pd.Series, level_name: str) -> str:
        rev_prev = row.get("Валовая выручка ABC, ₽_prev", np.nan)
        rev_cur = row.get("Валовая выручка ABC, ₽_cur", np.nan)
        ord_prev = row.get("Продажи ABC, шт_prev", np.nan)
        ord_cur = row.get("Продажи ABC, шт_cur", np.nan)
        gp_prev = row.get("Валовая прибыль ABC, ₽_prev", np.nan)
        gp_cur = row.get("Валовая прибыль ABC, ₽_cur", np.nan)
        clicks_prev = row.get("Клики, шт_prev", np.nan)
        clicks_cur = row.get("Клики, шт_cur", np.nan)
        fp_prev = row.get("finishedPrice, ₽_prev", np.nan)
        fp_cur = row.get("finishedPrice, ₽_cur", np.nan)
        pwd_prev = row.get("priceWithDisc, ₽_prev", np.nan)
        pwd_cur = row.get("priceWithDisc, ₽_cur", np.nan)
        spp_prev = row.get("SPP, %_prev", np.nan)
        spp_cur = row.get("SPP, %_cur", np.nan)
        loc_prev = row.get("Покрытие локализации, %_prev", np.nan)
        loc_cur = row.get("Покрытие локализации, %_cur", np.nan)
        d_prev = row.get("Спрос категории, шт_prev", np.nan)
        d_cur = row.get("Спрос категории, шт_cur", np.nan)
        vis_prev = row.get("Видимость, %_prev", np.nan)
        vis_cur = row.get("Видимость, %_cur", np.nan)
        ad_prev = row.get("Расходы на рекламу, ₽_prev", np.nan)
        ad_cur = row.get("Расходы на рекламу, ₽_cur", np.nan)
        ad_orders_prev = row.get("Рекламные заказы, шт_prev", np.nan)
        ad_orders_cur = row.get("Рекламные заказы, шт_cur", np.nan)
        main, secondary = row["Главная причина"], row["Вторичная причина"]
        parts = [
            f"Выручка {fmt_money(rev_prev)} -> {fmt_money(rev_cur)} ({fmt_pct(pct_delta(rev_cur, rev_prev))}), заказы {int(ord_prev) if pd.notna(ord_prev) else 0} -> {int(ord_cur) if pd.notna(ord_cur) else 0} ({fmt_pct(pct_delta(ord_cur, ord_prev))}), валовая прибыль {fmt_money(gp_prev)} -> {fmt_money(gp_cur)} ({fmt_pct(pct_delta(gp_cur, gp_prev))}).",
            f"Трафик {int(clicks_prev) if pd.notna(clicks_prev) else 0} -> {int(clicks_cur) if pd.notna(clicks_cur) else 0} ({fmt_pct(pct_delta(clicks_cur, clicks_prev))}), спрос категории {int(d_prev) if pd.notna(d_prev) else 0} -> {int(d_cur) if pd.notna(d_cur) else 0} ({fmt_pct(pct_delta(d_cur, d_prev))}), видимость {fmt_pct(vis_prev/100 if pd.notna(vis_prev) else np.nan)} -> {fmt_pct(vis_cur/100 if pd.notna(vis_cur) else np.nan)}, медианная позиция {fmt_num(row.get('Медианная позиция_prev'),1)} -> {fmt_num(row.get('Медианная позиция_cur'),1)}.",
            f"Цена для покупателя finishedPrice {fmt_money(fp_prev)} -> {fmt_money(fp_cur)}, цена продажи priceWithDisc {fmt_money(pwd_prev)} -> {fmt_money(pwd_cur)}, SPP {fmt_pct(spp_prev/100 if pd.notna(spp_prev) else np.nan)} -> {fmt_pct(spp_cur/100 if pd.notna(spp_cur) else np.nan)}.",
            f"Локализация {fmt_pct(loc_prev)} -> {fmt_pct(loc_cur)}. Реклама: расходы {fmt_money(ad_prev)} -> {fmt_money(ad_cur)}, рекламные заказы {int(ad_orders_prev) if pd.notna(ad_orders_prev) else 0} -> {int(ad_orders_cur) if pd.notna(ad_orders_cur) else 0}, оценка: {row['Оценка рекламы']}.",
            f"Главная причина: {main}." + (f" Вторичная: {secondary}." if secondary else ""),
        ]
        return " ".join(parts)

    def build_reason_rows(self, df: pd.DataFrame, level: str) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()
        id_cols = ["Категория"] if level == "category" else ["Товар"]
        paired = self._pair_periods(df, id_cols)
        if level == "product":
            subj_map = df.groupby("Товар", as_index=False)["Категория"].first()
            paired = paired.merge(subj_map, on="Товар", how="left")
        paired["Оценка рекламы"] = paired.apply(self._ad_assessment, axis=1)
        reasons = paired.apply(lambda r: pd.Series(self._main_reason(r), index=["Главная причина", "Вторичная причина"]), axis=1)
        paired = pd.concat([paired, reasons], axis=1)
        paired["Детальный вывод"] = paired.apply(lambda r: self._detail_text(r, level), axis=1)
        paired["Δ Выручка, ₽"] = paired["Валовая выручка ABC, ₽_cur"] - paired["Валовая выручка ABC, ₽_prev"]
        paired["Δ Валовая прибыль, ₽"] = paired["Валовая прибыль ABC, ₽_cur"] - paired["Валовая прибыль ABC, ₽_prev"]
        paired["Δ Заказы, шт"] = paired["Продажи ABC, шт_cur"] - paired["Продажи ABC, шт_prev"]
        if level == "category":
            paired = paired.sort_values("Валовая прибыль ABC, ₽_cur", ascending=False)
        else:
            paired = paired.sort_values(["Δ Выручка, ₽","Валовая прибыль ABC, ₽_cur"], ascending=[True,False])
        return paired

    def build_product_with_articles(self) -> pd.DataFrame:
        if self.article_period.empty:
            return pd.DataFrame()
        prod = self.build_reason_rows(self.product_period, "product")
        art = self.build_reason_rows(self.article_period[[c for c in self.article_period.columns if c not in {"Бренд"}]], "article") if False else None
        pair = self._pair_periods(self.article_period, ["Артикул"])
        pair["Товар"] = pair["Артикул"].map(clean_code_from_article)
        pair = pair.merge(self.article_period.groupby("Артикул", as_index=False)["Категория"].first(), on="Артикул", how="left")
        pair["Δ Выручка, ₽"] = pair["Валовая выручка ABC, ₽_cur"] - pair["Валовая выручка ABC, ₽_prev"]
        pair["Δ Валовая прибыль, ₽"] = pair["Валовая прибыль ABC, ₽_cur"] - pair["Валовая прибыль ABC, ₽_prev"]
        pair["Δ Заказы, шт"] = pair["Продажи ABC, шт_cur"] - pair["Продажи ABC, шт_prev"]
        # contribution within product
        prod_delta = pair.groupby("Товар", as_index=False)["Δ Валовая прибыль, ₽"].sum().rename(columns={"Δ Валовая прибыль, ₽":"prod_delta_gp"})
        pair = pair.merge(prod_delta, on="Товар", how="left")
        pair["Вклад SKU в товар, %"] = pair.apply(lambda r: safe_div(r["Δ Валовая прибыль, ₽"], r["prod_delta_gp"]), axis=1)
        # keep only products with decline or growth material
        neg_products = prod[(prod["Δ Выручка, ₽"] < 0) | (prod["Δ Заказы, шт"] < 0)]["Товар"].dropna().tolist()
        if not neg_products:
            neg_products = prod["Товар"].dropna().tolist()[:20]
        pair = pair[pair["Товар"].isin(neg_products)].copy()
        return pair.sort_values(["Товар","Δ Выручка, ₽"], ascending=[True,True])

    def build_summary_struct(self) -> Dict[str, pd.DataFrame]:
        daily = self.article_day.copy()
        if daily.empty:
            return {"daily": pd.DataFrame(), "weekly": pd.DataFrame(), "monthly": pd.DataFrame()}
        daily = daily[(daily["day"] >= pd.Timestamp(self.cur_start)) & (daily["day"] <= pd.Timestamp(self.latest_day))].copy()
        daily_cat = daily.groupby(["subject","day"], dropna=False)["gross_profit_day"].sum().reset_index()
        daily_pivot = daily_cat.pivot(index="subject", columns="day", values="gross_profit_day").reindex(TARGET_SUBJECTS).fillna(0)
        daily_pivot = daily_pivot[sorted(daily_pivot.columns, reverse=True)]

        # weekly from ABC actual last 8 weeks
        a = self.data.abc.copy()
        if a.empty:
            weekly_pivot = pd.DataFrame(index=TARGET_SUBJECTS)
            monthly_pivot = pd.DataFrame(index=TARGET_SUBJECTS)
        else:
            week_cat = a.groupby(["subject","week_code","week_start"], dropna=False)["gross_profit"].sum().reset_index()
            recent_weeks = week_cat.sort_values("week_start")["week_code"].drop_duplicates().tolist()[-8:]
            week_cat = week_cat[week_cat["week_code"].isin(recent_weeks)]
            weekly_pivot = week_cat.pivot(index="subject", columns="week_code", values="gross_profit").reindex(TARGET_SUBJECTS).fillna(0)
            weekly_pivot = weekly_pivot[[c for c in sorted(weekly_pivot.columns, reverse=True)]]

            a["month_key"] = a["week_end"].dt.to_period("M").astype(str)
            month_cat = a.groupby(["subject","month_key"], dropna=False)["gross_profit"].sum().reset_index()
            recent_months = month_cat["month_key"].drop_duplicates().tolist()[-6:]
            month_cat = month_cat[month_cat["month_key"].isin(recent_months)]
            monthly_pivot = month_cat.pivot(index="subject", columns="month_key", values="gross_profit").reindex(TARGET_SUBJECTS).fillna(0)
            monthly_pivot = monthly_pivot[[c for c in sorted(monthly_pivot.columns, reverse=True)]]
        return {"daily": daily_pivot, "weekly": weekly_pivot, "monthly": monthly_pivot}

    def build_channels(self) -> pd.DataFrame:
        e = self.data.entry.copy()
        if e.empty:
            return pd.DataFrame()
        e = e[e["week_start"].dt.date >= self.prev_start].copy()
        # only products with decline
        if self.reasons_product.empty:
            return pd.DataFrame()
        target_products = set(self.reasons_product[(self.reasons_product["Δ Выручка, ₽"] < 0) | (self.reasons_product["Δ Заказы, шт"] < 0)]["Товар"].dropna())
        if target_products:
            e = e[e["code"].isin(target_products)].copy()
        rows = []
        for code, g in e.groupby("code"):
            for ep, gg in g.groupby("entry_point"):
                prev = gg[(gg["week_start"].dt.date >= self.prev_start) & (gg["week_end"].dt.date <= self.prev_end)]
                cur = gg[(gg["week_start"].dt.date >= self.cur_start) & (gg["week_end"].dt.date <= self.latest_day)]
                rows.append({
                    "Товар": code,
                    "Канал": ep,
                    "Клики пред, шт": prev["clicks"].sum(),
                    "Клики тек, шт": cur["clicks"].sum(),
                    "CTR пред, %": safe_div(prev["clicks"].sum(), prev["impressions"].sum()) * 100,
                    "CTR тек, %": safe_div(cur["clicks"].sum(), cur["impressions"].sum()) * 100,
                    "Заказы пред, шт": prev["orders"].sum(),
                    "Заказы тек, шт": cur["orders"].sum(),
                    "Δ Заказы, шт": cur["orders"].sum() - prev["orders"].sum(),
                })
        out = pd.DataFrame(rows)
        if out.empty:
            return out
        return out.sort_values(["Товар","Δ Заказы, шт"], ascending=[True,True])


# -------------------------
# Workbook writer
# -------------------------

class Writer:
    def __init__(self):
        self.wb = Workbook()
        self.wb.remove(self.wb.active)

    def _base_style(self, ws):
        ws.freeze_panes = "B2"
        ws.sheet_format = SheetFormatProperties(defaultRowHeight=18)

    def _set_header(self, cell, title, fill=FILL_HEADER):
        cell.value = title
        cell.fill = fill
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = BORDER

    def _autofit(self, ws, max_width: int = 45):
        for col in ws.columns:
            letter = get_column_letter(col[0].column)
            max_len = 0
            for c in col[:300]:
                val = normalize_text(c.value)
                max_len = max(max_len, len(val))
            ws.column_dimensions[letter].width = min(max(max_len + 2, 12), max_width)

    def add_summary(self, summary: Dict[str, pd.DataFrame], latest_day: date, cur_start: date):
        ws = self.wb.create_sheet("Сводка")
        self._base_style(ws)
        ws["A1"] = f"Сводка по категориям • последние 2 недели ({cur_start:%d.%m.%Y} - {latest_day:%d.%m.%Y})"
        ws["A1"].font = Font(bold=True, size=14)
        ws["A1"].fill = FILL_TITLE

        # Daily block
        ws["A3"] = "Валовая прибыль по дням (дневная из Экономики)"
        ws["A3"].font = Font(bold=True)
        daily = summary["daily"]
        start_row = 4
        self._set_header(ws.cell(start_row,1), "Категория", FILL_SECTION)
        for j, d in enumerate(daily.columns, start=2):
            self._set_header(ws.cell(start_row,j), pd.Timestamp(d).strftime("%d.%m"), FILL_SECTION)
        for i, subj in enumerate(TARGET_SUBJECTS, start=start_row+1):
            self._set_header(ws.cell(i,1), subj, FILL_ARTICLE)
            for j, d in enumerate(daily.columns, start=2):
                c = ws.cell(i,j, float(daily.loc[subj, d]) if subj in daily.index else 0)
                c.number_format = NUM_FMT_RUB
                c.alignment = Alignment(horizontal="center", vertical="center")
                c.border = BORDER

        # Weekly block below
        weekly = summary["weekly"]
        r0 = start_row + len(TARGET_SUBJECTS) + 4
        ws.cell(r0,1).value = "Валовая прибыль по неделям (факт ABC)"
        ws.cell(r0,1).font = Font(bold=True)
        self._set_header(ws.cell(r0+1,1), "Категория", FILL_SECTION)
        for j, w in enumerate(weekly.columns, start=2):
            self._set_header(ws.cell(r0+1,j), w, FILL_SECTION)
        for i, subj in enumerate(TARGET_SUBJECTS, start=r0+2):
            self._set_header(ws.cell(i,1), subj, FILL_ARTICLE)
            for j, w in enumerate(weekly.columns, start=2):
                c = ws.cell(i,j, float(weekly.loc[subj,w]) if subj in weekly.index else 0)
                c.number_format = NUM_FMT_RUB
                c.alignment = Alignment(horizontal="center", vertical="center")
                c.border = BORDER

        # Monthly block right
        monthly = summary["monthly"]
        c0 = max(8, len(weekly.columns)+4)
        ws.cell(r0, c0).value = "Валовая прибыль по месяцам (факт ABC)"
        ws.cell(r0, c0).font = Font(bold=True)
        self._set_header(ws.cell(r0+1,c0), "Категория", FILL_SECTION)
        for j, m in enumerate(monthly.columns, start=c0+1):
            self._set_header(ws.cell(r0+1,j), m, FILL_SECTION)
        for i, subj in enumerate(TARGET_SUBJECTS, start=r0+2):
            self._set_header(ws.cell(i,c0), subj, FILL_ARTICLE)
            for j, m in enumerate(monthly.columns, start=c0+1):
                c = ws.cell(i,j, float(monthly.loc[subj,m]) if subj in monthly.index else 0)
                c.number_format = NUM_FMT_RUB
                c.alignment = Alignment(horizontal="center", vertical="center")
                c.border = BORDER
        self._autofit(ws)

    def add_reasons(self, cat: pd.DataFrame, prod: pd.DataFrame, sku: pd.DataFrame):
        ws = self.wb.create_sheet("Причины")
        self._base_style(ws)
        ws["A1"] = "Причины динамики: категории, товары и артикулы"
        ws["A1"].font = Font(bold=True, size=14)
        ws["A1"].fill = FILL_TITLE

        headers = [
            "Уровень","Категория","Товар","Артикул","Выручка пред, ₽","Выручка тек, ₽","Δ Выручка, ₽",
            "Заказы пред, шт","Заказы тек, шт","Δ Заказы, шт","Валовая прибыль пред, ₽","Валовая прибыль тек, ₽","Δ Валовая прибыль, ₽",
            "Клики пред, шт","Клики тек, шт","Конверсия пред","Конверсия тек","Спрос пред, шт","Спрос тек, шт",
            "finishedPrice пред, ₽","finishedPrice тек, ₽","priceWithDisc пред, ₽","priceWithDisc тек, ₽",
            "SPP пред, %","SPP тек, %","Локализация пред, %","Локализация тек, %",
            "Расходы рекламы пред, ₽","Расходы рекламы тек, ₽","Оценка рекламы","Главная причина","Вторичная причина","Детальный вывод"
        ]
        row = 3
        for j, h in enumerate(headers, start=1):
            self._set_header(ws.cell(row,j), h)
        row += 1

        def write_reason_rows(df: pd.DataFrame, level_name: str, group_articles: bool=False):
            nonlocal row
            for _, r in df.iterrows():
                vals = [
                    level_name,
                    r.get("Категория", "") if level_name != "Категория" else r.get("Категория", ""),
                    r.get("Товар", "") if level_name in {"Товар","Артикул"} else "",
                    r.get("Артикул", "") if level_name == "Артикул" else "",
                    r.get("Валовая выручка ABC, ₽_prev", np.nan), r.get("Валовая выручка ABC, ₽_cur", np.nan), r.get("Δ Выручка, ₽", np.nan),
                    r.get("Продажи ABC, шт_prev", np.nan), r.get("Продажи ABC, шт_cur", np.nan), r.get("Δ Заказы, шт", np.nan),
                    r.get("Валовая прибыль ABC, ₽_prev", np.nan), r.get("Валовая прибыль ABC, ₽_cur", np.nan), r.get("Δ Валовая прибыль, ₽", np.nan),
                    r.get("Клики, шт_prev", np.nan), r.get("Клики, шт_cur", np.nan),
                    safe_div(r.get("Заказы воронка, шт_prev"), r.get("Клики, шт_prev")), safe_div(r.get("Заказы воронка, шт_cur"), r.get("Клики, шт_cur")),
                    r.get("Спрос категории, шт_prev", np.nan), r.get("Спрос категории, шт_cur", np.nan),
                    r.get("finishedPrice, ₽_prev", np.nan), r.get("finishedPrice, ₽_cur", np.nan),
                    r.get("priceWithDisc, ₽_prev", np.nan), r.get("priceWithDisc, ₽_cur", np.nan),
                    r.get("SPP, %_prev", np.nan), r.get("SPP, %_cur", np.nan),
                    r.get("Покрытие локализации, %_prev", np.nan), r.get("Покрытие локализации, %_cur", np.nan),
                    r.get("Расходы на рекламу, ₽_prev", np.nan), r.get("Расходы на рекламу, ₽_cur", np.nan),
                    r.get("Оценка рекламы", ""), r.get("Главная причина", ""), r.get("Вторичная причина", ""), r.get("Детальный вывод", ""),
                ]
                for j, v in enumerate(vals, start=1):
                    c = ws.cell(row,j,v)
                    c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
                    c.border = BORDER
                    if j in {5,6,7,11,12,13,20,21,22,23,28,29} and pd.notna(v):
                        c.number_format = NUM_FMT_RUB
                    if j in {16,17,24,25,26,27} and pd.notna(v):
                        c.number_format = NUM_FMT_PCT
                if level_name == "Категория":
                    fill = FILL_SECTION
                    for j in range(1, len(headers)+1):
                        ws.cell(row,j).fill = fill
                        ws.cell(row,j).font = Font(bold=True)
                elif level_name == "Товар":
                    for j in range(1, len(headers)+1):
                        ws.cell(row,j).font = Font(bold=True)
                else:
                    for j in range(1, len(headers)+1):
                        ws.cell(row,j).fill = FILL_ARTICLE
                    ws.row_dimensions[row].outlineLevel = 1
                    ws.row_dimensions[row].hidden = True
                row += 1

        # categories
        write_reason_rows(cat, "Категория")
        row += 1
        # product rows then articles underneath
        product_order = prod[["Товар","Категория","Δ Выручка, ₽"]].copy().sort_values(["Категория","Δ Выручка, ₽"], ascending=[True,True])
        sku_map = defaultdict(list)
        for _, r in sku.iterrows():
            sku_map[r["Товар"]].append(r)
        for _, pr in product_order.iterrows():
            rr = prod[prod["Товар"] == pr["Товар"]].iloc[0]
            write_reason_rows(pd.DataFrame([rr]), "Товар")
            articles = pd.DataFrame(sku_map.get(pr["Товар"], []))
            if not articles.empty:
                # add lightweight reason text for articles
                pair = articles.copy()
                pair["Оценка рекламы"] = ""
                pair["Главная причина"] = articles.apply(lambda r: "Снижение заказов" if r.get("Δ Заказы, шт", 0) < 0 else "Рост заказов", axis=1)
                pair["Вторичная причина"] = ""
                pair["Детальный вывод"] = articles.apply(lambda r: f"Артикул дал изменение ВП {fmt_money(r.get('Δ Валовая прибыль, ₽'))}, выручки {fmt_money(r.get('Δ Выручка, ₽'))}, заказов {int(r.get('Δ Заказы, шт',0)) if pd.notna(r.get('Δ Заказы, шт')) else 0}.", axis=1)
                write_reason_rows(pair.sort_values("Δ Выручка, ₽"), "Артикул")
        ws.sheet_properties.outlinePr.summaryBelow = True
        self._autofit(ws, max_width=35)
        # widen detail text
        ws.column_dimensions[get_column_letter(33)].width = 80

    def add_localization(self, loc: pd.DataFrame):
        ws = self.wb.create_sheet("Локализация")
        self._base_style(ws)
        ws["A1"] = "Локализация: Артикул - Дата - Процент покрытия"
        ws["A1"].font = Font(bold=True, size=14)
        ws["A1"].fill = FILL_TITLE
        headers = ["Артикул","Дата","Покрытие, %"]
        for j,h in enumerate(headers, start=1):
            self._set_header(ws.cell(3,j), h)
        row = 4
        for _, r in loc.iterrows():
            ws.cell(row,1,r["Артикул"]).alignment = Alignment(horizontal="center", vertical="center")
            c = ws.cell(row,2,pd.Timestamp(r["Дата"]).to_pydatetime())
            c.number_format = NUM_FMT_DATE
            c.alignment = Alignment(horizontal="center", vertical="center")
            c = ws.cell(row,3,float(r["Покрытие, %"]))
            c.number_format = NUM_FMT_PCT
            c.alignment = Alignment(horizontal="center", vertical="center")
            for j in range(1,4):
                ws.cell(row,j).border = BORDER
            row += 1
        self._autofit(ws)

    def save(self, path: str):
        self.wb.save(path)


# -------------------------
# CLI
# -------------------------

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--root", default=".")
    p.add_argument("--reports-root", default="Отчёты")
    p.add_argument("--store", default="TOPFACE")
    p.add_argument("--out-subdir", default="Отчёты/Объединенный отчет/TOPFACE")
    return p.parse_args()


def main():
    args = parse_args()
    storage = make_storage(args.root)
    loader = Loader(storage, root_reports=args.reports_root, store=args.store)
    data = loader.load_all()
    log("Building analytics")
    analyzer = Analyzer(data)
    writer = Writer()
    writer.add_summary(analyzer.summary_struct, analyzer.latest_day, analyzer.cur_start)
    writer.add_reasons(analyzer.reasons_category, analyzer.reasons_product, analyzer.product_with_sku)
    writer.add_localization(analyzer.localization_daily)
    stamp = datetime.now().strftime("%Y-%m-%d")
    out = f"{args.out_subdir}/Объединенный_отчет_{args.store}_{stamp}.xlsx"
    log(f"Saving {out}")
    bio = io.BytesIO()
    writer.wb.save(bio)
    storage.write_bytes(out, bio.getvalue())
    log(f"Saved {out}")
    if data.warnings:
        log("Warnings:")
        for w in data.warnings[:50]:
            log(f" - {w}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
