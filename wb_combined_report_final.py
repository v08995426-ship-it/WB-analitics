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

TARGET_SUBJECTS = [
    "Кисти косметические",
    "Помады",
    "Блески",
    "Косметические карандаши",
]

EXCLUDE_ARTICLES = {
    "CZ420", "CZ420БРОВИ", "CZ420ГЛАЗА", "DE49", "DE49ГЛАЗА", "PT901", "CZ420", "CZ420ГЛАЗА"
}

TARGET_EXAMPLE_ARTICLES = ["901/5", "901/8", "901/14", "901/18"]

THIN = Side(style="thin", color="D9D9D9")
BORDER = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)
FILL_HEADER = PatternFill("solid", fgColor="DDEBF7")
FILL_SECTION = PatternFill("solid", fgColor="E2F0D9")
FILL_TOTAL = PatternFill("solid", fgColor="FFF2CC")
FILL_CATEGORY = PatternFill("solid", fgColor="EAF4FF")
FILL_PRODUCT = PatternFill("solid", fgColor="F7FBFF")


# -------------------------
# Helpers
# -------------------------

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
    if not text or text.lower() in {"nan", "none"}:
        return ""
    return text


def upper_article(value: Any) -> str:
    return clean_article(value).upper()


def clean_code_from_article(value: Any) -> str:
    text = upper_article(value)
    if not text or text in EXCLUDE_ARTICLES:
        return ""
    # PT901 / PT901.F25 / 901_/16 / 901/5 -> 901
    m = re.match(r"^PT(\d+)", text)
    if m:
        return m.group(1)
    m = re.match(r"^(\d+)", text)
    if m:
        return m.group(1)
    return ""


def to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def to_dt(series: pd.Series) -> pd.Series:
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


def safe_weighted_average(values: pd.Series, weights: pd.Series) -> float:
    v = pd.to_numeric(values, errors="coerce")
    w = pd.to_numeric(weights, errors="coerce").fillna(0)
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
    y = int(m.group(1))
    w = int(m.group(2))
    return date.fromisocalendar(y, w, 1), date.fromisocalendar(y, w, 7)


def parse_week_code_from_name(name: str) -> Optional[str]:
    m = re.search(r"(\d{4})-W(\d{2})", name)
    if not m:
        return None
    return f"{m.group(1)}-W{m.group(2)}"


def parse_abc_period_from_name(name: str) -> Tuple[Optional[date], Optional[date]]:
    m = re.search(r"__(\d{2})\.(\d{2})\.(\d{4})-(\d{2})\.(\d{2})\.(\d{4})__", name)
    if not m:
        return None, None
    return (
        date(int(m.group(3)), int(m.group(2)), int(m.group(1))),
        date(int(m.group(6)), int(m.group(5)), int(m.group(4))),
    )


def russian_month_name(month_num: int) -> str:
    names = {
        1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
        5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
        9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
    }
    return names[month_num]


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
        start = self._abs(prefix)
        base = start if start.exists() else start.parent
        if not base.exists():
            return []
        out = []
        for p in base.rglob("*"):
            if p.is_file():
                rel = str(p.relative_to(self.root)).replace("\\", "/")
                if rel.startswith(prefix):
                    out.append(rel)
        return sorted(out)

    def read_bytes(self, path: str) -> bytes:
        return self._abs(path).read_bytes()

    def write_bytes(self, path: str, data: bytes) -> None:
        abs_path = self._abs(path)
        abs_path.parent.mkdir(parents=True, exist_ok=True)
        abs_path.write_bytes(data)

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


# -------------------------
# Read helpers
# -------------------------
ALIASES = {
    "day": ["Дата", "Дата заказа", "date", "dt"],
    "nm_id": ["Артикул WB", "Артикул ВБ", "nmID", "nmId"],
    "supplier_article": ["Артикул продавца", "supplierArticle", "Артикул WB продавца"],
    "subject": ["Предмет", "subject", "Название предмета", "category"],
    "brand": ["Бренд", "brand"],
    "title": ["Название", "Название товара", "Товар"],
    "orders": ["Заказы", "orders", "ordersCount", "Кол-во продаж"],
    "buyouts_count": ["buyoutsCount"],
    "finished_price": ["finishedPrice", "Ср. цена продажи", "Цена с учетом всех скидок, кроме суммы по WB Кошельку"],
    "price_with_disc": ["priceWithDisc", "Цена со скидкой продавца, в том числе со скидкой WB Клуба"],
    "spp": ["СПП, %", "SPP", "Скидка WB, %", "spp"],
    "gross_profit": ["Валовая прибыль", "Валовая прибыль, руб/ед"],
    "gross_revenue": ["Валовая выручка"],
    "spend": ["Расход", "spend", "Продвижение"],
}


def rename_using_aliases(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    cols = {norm_key(c): c for c in out.columns}
    for target, aliases in ALIASES.items():
        if target in out.columns:
            continue
        found = None
        for a in aliases:
            k = norm_key(a)
            if k in cols:
                found = cols[k]
                break
        if found is not None:
            out[target] = out[found]
        else:
            out[target] = np.nan
    return out


def read_excel_best(data: bytes, preferred_sheet: Optional[str] = None, header_candidates: Iterable[int] = (0, 1, 2)) -> pd.DataFrame:
    bio = io.BytesIO(data)
    xl = pd.ExcelFile(bio)
    if preferred_sheet and preferred_sheet in xl.sheet_names:
        sheet = preferred_sheet
    else:
        sheet = xl.sheet_names[0]
    best = None
    best_score = -1
    for header in header_candidates:
        try:
            df = xl.parse(sheet_name=sheet, header=header, dtype=object)
        except Exception:
            continue
        df = df.dropna(axis=0, how="all").dropna(axis=1, how="all")
        score = len(df.columns)
        if score > best_score:
            best = df
            best_score = score
    if best is None:
        raise ValueError(f"Не удалось прочитать {sheet}")
    best.columns = [normalize_text(c) or f"col_{i}" for i, c in enumerate(best.columns)]
    return best


# -------------------------
# Data loading
# -------------------------
@dataclass
class LoadedData:
    orders: pd.DataFrame
    funnel: pd.DataFrame
    ads_daily: pd.DataFrame
    economics: pd.DataFrame
    abc_weekly: pd.DataFrame
    abc_monthly: pd.DataFrame
    plan: pd.DataFrame
    latest_date: pd.Timestamp


class Stage1Loader:
    def __init__(self, storage: BaseStorage, reports_root: str = "Отчёты", store: str = "TOPFACE"):
        self.storage = storage
        self.reports_root = reports_root.rstrip("/")
        self.store = store

    def _prefix(self, *parts: str) -> str:
        return "/".join([self.reports_root, *parts]).replace("//", "/")

    def _list_xlsx(self, prefix: str) -> List[str]:
        return [f for f in self.storage.list_files(prefix) if f.lower().endswith(".xlsx") and "/~$" not in f]

    def load_orders(self) -> pd.DataFrame:
        log("Loading orders")
        files = self._list_xlsx(self._prefix("Заказы", self.store, "Недельные"))
        frames = []
        for path in files:
            try:
                df = rename_using_aliases(read_excel_best(self.storage.read_bytes(path), preferred_sheet="Заказы", header_candidates=(0,)))
                df["day"] = to_dt(df["day"])
                df["nm_id"] = to_numeric(df["nm_id"])
                df["supplier_article"] = df["supplier_article"].map(clean_article)
                df["subject"] = df["subject"].map(normalize_text)
                df["finished_price"] = to_numeric(df["finished_price"])
                df["price_with_disc"] = to_numeric(df["price_with_disc"])
                df["spp"] = to_numeric(df["spp"])
                if "orders" in df.columns and not to_numeric(df["orders"]).isna().all():
                    df["orders"] = to_numeric(df["orders"]).fillna(0)
                else:
                    df["orders"] = 1.0
                if "warehouseName" in df.columns:
                    df["warehouse"] = df["warehouseName"].map(normalize_text)
                elif "warehouse" not in df.columns:
                    df["warehouse"] = ""
                frames.append(df[["day", "nm_id", "supplier_article", "subject", "finished_price", "price_with_disc", "spp", "orders", "warehouse"]])
            except Exception as e:
                log(f"WARN: orders read error {path}: {e}")
        out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["day"])
        out = out[out["day"].notna()].copy()
        return out

    def load_funnel(self) -> pd.DataFrame:
        log("Loading funnel")
        candidates = [
            self._prefix("Воронка продаж", self.store, "Воронка продаж.xlsx"),
            self._prefix("Воронка продаж", "Воронка продаж.xlsx"),
        ]
        path = None
        for c in candidates:
            if self.storage.exists(c):
                path = c
                break
        if path is None:
            return pd.DataFrame(columns=["day"])
        df = rename_using_aliases(read_excel_best(self.storage.read_bytes(path), header_candidates=(0,)))
        df["day"] = to_dt(df["day"])
        df["nm_id"] = to_numeric(df["nm_id"])
        df["orders"] = to_numeric(df["orders"])
        df["buyouts_count"] = to_numeric(df["buyouts_count"])
        return df[["day", "nm_id", "orders", "buyouts_count"]]

    def load_ads_daily(self) -> pd.DataFrame:
        log("Loading ads")
        files = self._list_xlsx(self._prefix("Реклама", self.store, "Недельные"))
        frames = []
        for path in files:
            try:
                df = rename_using_aliases(read_excel_best(self.storage.read_bytes(path), preferred_sheet="Статистика_Ежедневно", header_candidates=(0,)))
                df["day"] = to_dt(df["day"])
                df["nm_id"] = to_numeric(df["nm_id"])
                df["supplier_article"] = df["supplier_article"].map(clean_article)
                df["subject"] = df["subject"].map(normalize_text)
                df["spend"] = to_numeric(df["spend"])
                frames.append(df[["day", "nm_id", "supplier_article", "subject", "spend"]])
            except Exception as e:
                log(f"WARN: ads read error {path}: {e}")
        out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["day"])
        out = out[out["day"].notna()].copy()
        return out

    def load_economics(self) -> pd.DataFrame:
        log("Loading economics")
        path = None
        for c in [
            self._prefix("Финансовые показатели", self.store, "Экономика.xlsx"),
            self._prefix("Финансовые показатели", self.store, "Недельные", "Экономика.xlsx"),
        ]:
            if self.storage.exists(c):
                path = c
                break
        if path is None:
            return pd.DataFrame(columns=["week_code"])
        df = pd.read_excel(io.BytesIO(self.storage.read_bytes(path)), sheet_name="Юнит экономика")
        df.columns = [normalize_text(c) for c in df.columns]
        df = rename_using_aliases(df)
        df["supplier_article"] = df["supplier_article"].map(clean_article)
        df["nm_id"] = to_numeric(df["nm_id"])
        df["subject"] = df["subject"].map(normalize_text)
        for c in [
            "Процент выкупа", "Комиссия WB, %", "Эквайринг, %", "Логистика прямая, руб/ед",
            "Логистика обратная, руб/ед", "Хранение, руб/ед", "Прочие расходы, руб/ед", "Себестоимость, руб",
            "НДС, руб/ед", "Валовая прибыль, руб/ед"
        ]:
            if c not in df.columns:
                df[c] = np.nan
            df[c] = to_numeric(df[c])
        df["week_code"] = df.get("Неделя", pd.Series([None] * len(df))).astype(str).str.strip()
        return df[[
            "week_code", "supplier_article", "nm_id", "subject", "Процент выкупа", "Комиссия WB, %", "Эквайринг, %",
            "Логистика прямая, руб/ед", "Логистика обратная, руб/ед", "Хранение, руб/ед",
            "Прочие расходы, руб/ед", "Себестоимость, руб", "НДС, руб/ед", "Валовая прибыль, руб/ед"
        ]]

    def load_abc(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        log("Loading ABC")
        files = self._list_xlsx(self._prefix("ABC"))
        weekly_frames = []
        monthly_frames = []
        for path in files:
            name = Path(path).name
            if "wb_abc_report_goods__" not in name:
                continue
            try:
                df = rename_using_aliases(read_excel_best(self.storage.read_bytes(path), header_candidates=(0,)))
                df["supplier_article"] = df["supplier_article"].map(clean_article)
                df["nm_id"] = to_numeric(df["nm_id"])
                df["subject"] = df["subject"].map(normalize_text)
                df["gross_profit"] = to_numeric(df["gross_profit"])
                df["gross_revenue"] = to_numeric(df["gross_revenue"])
                df["orders"] = to_numeric(df["orders"])
                start, end = parse_abc_period_from_name(name)
                if not start or not end:
                    continue
                df["period_start"] = pd.Timestamp(start)
                df["period_end"] = pd.Timestamp(end)
                df["code"] = df["supplier_article"].map(clean_code_from_article)
                df["vat"] = df["gross_revenue"] * 7.0 / 107.0
                df["gp_minus_nds"] = df["gross_profit"] - df["vat"]
                month_end = (pd.Timestamp(start).to_period("M").end_time.normalize()).date()
                if start.day == 1 and end == month_end:
                    df["month_key"] = f"{start.year:04d}-{start.month:02d}"
                    monthly_frames.append(df[["month_key", "supplier_article", "nm_id", "subject", "code", "gross_profit", "gross_revenue", "vat", "gp_minus_nds", "orders"]])
                else:
                    df["week_code"] = week_code_from_date(start)
                    df["week_label"] = pd.Timestamp(start).strftime("%d.%m")
                    weekly_frames.append(df[["week_code", "week_label", "period_start", "period_end", "supplier_article", "nm_id", "subject", "code", "gross_profit", "gross_revenue", "vat", "gp_minus_nds", "orders"]])
            except Exception as e:
                log(f"WARN: abc read error {path}: {e}")
        weekly = pd.concat(weekly_frames, ignore_index=True) if weekly_frames else pd.DataFrame()
        monthly = pd.concat(monthly_frames, ignore_index=True) if monthly_frames else pd.DataFrame()
        return weekly, monthly

    def load_plan(self, current_month: pd.Timestamp) -> pd.DataFrame:
        log("Loading plan")
        path = self._prefix("Объединенный отчет", self.store, "План.xlsx")
        if not self.storage.exists(path):
            alt = "План.xlsx"
            if not self.storage.exists(alt):
                return pd.DataFrame(columns=["supplier_article", "subject", "plan_gp_minus_nds_month"])
            path = alt
        df = pd.read_excel(io.BytesIO(self.storage.read_bytes(path)), sheet_name="Итог_все_категории", header=2)
        df.columns = [normalize_text(c) for c in df.columns]
        df = df.rename(columns={normalize_text("Артикул продавца"): "supplier_article", normalize_text("Категория"): "subject"})
        target_col = f"ВП-НДС {russian_month_name(current_month.month)} {current_month.year}"
        col_map = {norm_key(c): c for c in df.columns}
        chosen = col_map.get(norm_key(target_col))
        if chosen is None:
            for c in df.columns:
                if norm_key(target_col) in norm_key(c):
                    chosen = c
                    break
        if chosen is None:
            return pd.DataFrame(columns=["supplier_article", "subject", "plan_gp_minus_nds_month"])
        out = df[["supplier_article", "subject", chosen]].copy()
        out["supplier_article"] = out["supplier_article"].map(clean_article)
        out["subject"] = out["subject"].map(normalize_text)
        out["plan_gp_minus_nds_month"] = to_numeric(out[chosen])
        return out[["supplier_article", "subject", "plan_gp_minus_nds_month"]]

    def load_all(self) -> LoadedData:
        orders = self.load_orders()
        funnel = self.load_funnel()
        ads_daily = self.load_ads_daily()
        economics = self.load_economics()
        abc_weekly, abc_monthly = self.load_abc()
        latest_candidates = []
        for df, col in [(orders, "day"), (funnel, "day"), (ads_daily, "day")]:
            if not df.empty:
                latest_candidates.append(pd.to_datetime(df[col]).max())
        if not abc_weekly.empty:
            latest_candidates.append(pd.to_datetime(abc_weekly["period_end"]).max())
        latest_date = max([x for x in latest_candidates if pd.notna(x)], default=pd.Timestamp(datetime.today().date()))
        plan = self.load_plan(pd.Timestamp(latest_date))
        return LoadedData(
            orders=orders,
            funnel=funnel,
            ads_daily=ads_daily,
            economics=economics,
            abc_weekly=abc_weekly,
            abc_monthly=abc_monthly,
            plan=plan,
            latest_date=pd.Timestamp(latest_date).normalize(),
        )


# -------------------------
# Stage 1 builder
# -------------------------
class Stage1Builder:
    def __init__(self, data: LoadedData):
        self.data = data
        self.latest_day = pd.Timestamp(data.latest_date).normalize()
        self.current_week_start = self.latest_day - pd.Timedelta(days=self.latest_day.weekday())
        self.current_week_days = [self.current_week_start + pd.Timedelta(days=i) for i in range((self.latest_day - self.current_week_start).days + 1)]
        self.current_month_key = self.latest_day.to_period("M").strftime("%Y-%m")
        self.current_month_start = self.latest_day.replace(day=1)
        self.days_in_month = calendar.monthrange(self.latest_day.year, self.latest_day.month)[1]
        self.subject_order = TARGET_SUBJECTS.copy()
        self.master = self.build_master()
        self.buyout90 = self.build_buyout90()
        self.econ_latest = self.build_econ_latest()

    def _filter_subjects(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df.copy()
        out = df.copy()
        if "subject" in out.columns:
            out["subject"] = out["subject"].map(normalize_text)
            out = out[out["subject"].isin(TARGET_SUBJECTS)].copy()
        if "supplier_article" in out.columns:
            out["supplier_article"] = out["supplier_article"].map(clean_article)
            out = out[~out["supplier_article"].map(upper_article).isin(EXCLUDE_ARTICLES)].copy()
        if "code" not in out.columns:
            out["code"] = out["supplier_article"].map(clean_code_from_article)
        out = out[out["code"] != ""].copy()
        return out

    def build_master(self) -> pd.DataFrame:
        frames = []
        for df in [self.data.orders, self.data.economics, self.data.abc_weekly, self.data.abc_monthly]:
            if df.empty:
                continue
            x = df.copy()
            for c in ["supplier_article", "nm_id", "subject", "brand", "title"]:
                if c not in x.columns:
                    x[c] = np.nan
            x = x[["supplier_article", "nm_id", "subject", "brand", "title"]].copy()
            frames.append(x)
        if not frames:
            return pd.DataFrame(columns=["supplier_article", "nm_id", "subject", "brand", "title", "code"])
        m = pd.concat(frames, ignore_index=True)
        m["supplier_article"] = m["supplier_article"].map(clean_article)
        m["nm_id"] = to_numeric(m["nm_id"])
        m["subject"] = m["subject"].map(normalize_text)
        m["brand"] = m["brand"].map(normalize_text)
        m["title"] = m["title"].map(normalize_text)
        m["code"] = m["supplier_article"].map(clean_code_from_article)
        m = self._filter_subjects(m)
        m["quality"] = m["supplier_article"].ne("").astype(int) * 4 + m["subject"].ne("").astype(int) * 2 + m["title"].ne("").astype(int)
        m = m.sort_values(["quality"], ascending=False).drop_duplicates(subset=["supplier_article", "nm_id"])
        return m[["supplier_article", "nm_id", "subject", "brand", "title", "code"]]

    def build_buyout90(self) -> pd.DataFrame:
        if self.data.funnel.empty:
            return pd.DataFrame(columns=["nm_id", "buyout_pct_90"])
        f = self.data.funnel.copy()
        f = f[(f["day"] >= self.latest_day - pd.Timedelta(days=89)) & (f["day"] <= self.latest_day)].copy()
        g = f.groupby("nm_id", dropna=False).agg(orders_90=("orders", "sum"), buyouts_90=("buyouts_count", "sum")).reset_index()
        g["buyout_pct_90"] = g.apply(lambda r: safe_div(r["buyouts_90"], r["orders_90"]), axis=1)
        return g[["nm_id", "buyout_pct_90"]]

    def build_econ_latest(self) -> pd.DataFrame:
        if self.data.economics.empty:
            return pd.DataFrame(columns=["supplier_article", "nm_id"])
        econ = self._filter_subjects(self.data.economics)
        # last available week per article
        econ["week_ord"] = econ["week_code"].astype(str)
        econ = econ.sort_values(["supplier_article", "week_ord"], ascending=[True, False])
        econ = econ.drop_duplicates(subset=["supplier_article", "nm_id"], keep="first")
        return econ.drop(columns=["week_ord"])

    def build_current_week_daily_article(self) -> pd.DataFrame:
        orders = self._filter_subjects(self.data.orders)
        if orders.empty:
            return pd.DataFrame()
        orders = orders[(orders["day"] >= self.current_week_start) & (orders["day"] <= self.latest_day)].copy()
        if orders.empty:
            return pd.DataFrame()

        orders = orders.merge(self.master, on=["supplier_article", "nm_id"], how="left", suffixes=("", "_m"))
        for c in ["subject", "brand", "title", "code"]:
            if f"{c}_m" in orders.columns:
                orders[c] = orders[c].where(orders[c].notna() & (orders[c] != ""), orders[f"{c}_m"])
        orders = orders.drop(columns=[c for c in orders.columns if c.endswith("_m")])
        orders = self._filter_subjects(orders)

        daily = orders.groupby(["day", "supplier_article", "nm_id", "subject", "code", "title"], dropna=False).agg(
            orders_day=("orders", "sum"),
            finished_price_day=("finished_price", "mean"),
            price_with_disc_day=("price_with_disc", "mean"),
            spp_day=("spp", "mean"),
        ).reset_index()

        daily = daily.merge(self.buyout90, on="nm_id", how="left")
        daily = daily.merge(
            self.econ_latest[[
                "supplier_article", "nm_id", "subject", "code", "title", "Процент выкупа", "Комиссия WB, %", "Эквайринг, %",
                "Логистика прямая, руб/ед", "Логистика обратная, руб/ед", "Хранение, руб/ед",
                "Прочие расходы, руб/ед", "Себестоимость, руб", "НДС, руб/ед", "Валовая прибыль, руб/ед"
            ]],
            on=["supplier_article", "nm_id"], how="left", suffixes=("", "_e")
        )
        for c in ["subject", "code", "title"]:
            if f"{c}_e" in daily.columns:
                daily[c] = daily[c].where(daily[c].notna() & (daily[c] != ""), daily[f"{c}_e"])
        daily = daily.drop(columns=[c for c in daily.columns if c.endswith("_e")])

        # ad spend by day/article
        ads = self._filter_subjects(self.data.ads_daily)
        if not ads.empty:
            ads = ads[(ads["day"] >= self.current_week_start) & (ads["day"] <= self.latest_day)].copy()
            ads = ads.merge(self.master[["supplier_article", "nm_id", "subject", "code"]], on=["supplier_article", "nm_id"], how="left", suffixes=("", "_m"))
            if "subject_m" in ads.columns:
                ads["subject"] = ads["subject"].where(ads["subject"].notna() & (ads["subject"] != ""), ads["subject_m"])
                ads["code"] = ads["code"].where(ads["code"].notna() & (ads["code"] != ""), ads["supplier_article"].map(clean_code_from_article))
            ads = ads.drop(columns=[c for c in ads.columns if c.endswith("_m")])
            ads = self._filter_subjects(ads)
            ads_g = ads.groupby(["day", "supplier_article", "nm_id"], dropna=False).agg(ad_spend_day=("spend", "sum")).reset_index()
            daily = daily.merge(ads_g, on=["day", "supplier_article", "nm_id"], how="left")
        else:
            daily["ad_spend_day"] = 0.0
        daily["ad_spend_day"] = daily["ad_spend_day"].fillna(0.0)

        daily["buyout_factor"] = daily["buyout_pct_90"].fillna(to_numeric(daily["Процент выкупа"]) / 100.0).fillna(1.0)
        daily["buyout_qty"] = daily["orders_day"] * daily["buyout_factor"]
        daily["revenue_pwd"] = daily["buyout_qty"] * daily["price_with_disc_day"].fillna(0)
        daily["commission_rub"] = daily["revenue_pwd"] * to_numeric(daily["Комиссия WB, %"]).fillna(0) / 100.0
        daily["acquiring_rub"] = daily["revenue_pwd"] * to_numeric(daily["Эквайринг, %"]).fillna(0) / 100.0
        daily["logistics_direct_rub"] = daily["buyout_qty"] * to_numeric(daily["Логистика прямая, руб/ед"]).fillna(0)
        daily["logistics_return_rub"] = daily["buyout_qty"] * to_numeric(daily["Логистика обратная, руб/ед"]).fillna(0)
        daily["storage_rub"] = daily["buyout_qty"] * to_numeric(daily["Хранение, руб/ед"]).fillna(0)
        daily["other_rub"] = daily["buyout_qty"] * to_numeric(daily["Прочие расходы, руб/ед"]).fillna(0)
        daily["cost_rub"] = daily["buyout_qty"] * to_numeric(daily["Себестоимость, руб"]).fillna(0)
        daily["vat_rub"] = daily["buyout_qty"] * daily["finished_price_day"].fillna(0) * 7.0 / 107.0
        daily["gross_profit_rub"] = (
            daily["revenue_pwd"] - daily["commission_rub"] - daily["acquiring_rub"]
            - daily["logistics_direct_rub"] - daily["logistics_return_rub"] - daily["storage_rub"]
            - daily["other_rub"] - daily["cost_rub"] - daily["ad_spend_day"]
        )
        daily["gp_minus_nds_rub"] = daily["gross_profit_rub"] - daily["vat_rub"]
        daily["day_label"] = pd.to_datetime(daily["day"]).dt.strftime("%d.%m")
        return daily

    def build_current_month_weekly_fact(self) -> pd.DataFrame:
        abc = self._filter_subjects(self.data.abc_weekly)
        if abc.empty:
            return pd.DataFrame()
        abc = abc[(abc["period_end"] >= self.current_month_start) & (abc["period_start"] <= self.latest_day)].copy()
        return abc

    def build_last3months_fact(self) -> pd.DataFrame:
        abc_month = self._filter_subjects(self.data.abc_monthly)
        abc_week = self._filter_subjects(self.data.abc_weekly)
        periods = [self.latest_day.to_period("M") - 2, self.latest_day.to_period("M") - 1, self.latest_day.to_period("M")]
        month_keys = [p.strftime("%Y-%m") for p in periods]
        frames = []
        if not abc_month.empty:
            frames.append(abc_month[abc_month["month_key"].isin(month_keys)].copy())
        # if current month not in monthly ABC, synthesize from weekly ABC
        if self.current_month_key not in set(abc_month.get("month_key", pd.Series(dtype=str)).astype(str)):
            if not abc_week.empty:
                wk = abc_week.copy()
                wk["month_key"] = pd.to_datetime(wk["period_start"]).dt.to_period("M").astype(str)
                wk = wk[wk["month_key"] == self.current_month_key].copy()
                if not wk.empty:
                    curm = wk.groupby(["month_key", "supplier_article", "nm_id", "subject", "code"], dropna=False).agg(
                        gross_profit=("gross_profit", "sum"),
                        gross_revenue=("gross_revenue", "sum"),
                        vat=("vat", "sum"),
                        gp_minus_nds=("gp_minus_nds", "sum"),
                        orders=("orders", "sum"),
                    ).reset_index()
                    frames = [f[f["month_key"] != self.current_month_key] for f in frames]
                    frames.append(curm)
        if not frames:
            return pd.DataFrame()
        out = pd.concat(frames, ignore_index=True)
        return out[out["month_key"].isin(month_keys)].copy()

    def build_plan_month(self) -> pd.DataFrame:
        plan = self._filter_subjects(self.data.plan)
        if plan.empty:
            return pd.DataFrame(columns=["supplier_article", "subject", "code", "plan_gp_minus_nds_month"])
        plan["code"] = plan["supplier_article"].map(clean_code_from_article)
        return plan[["supplier_article", "subject", "code", "plan_gp_minus_nds_month"]]

    def build_month_fact_by_entity(self, monthly: pd.DataFrame, keys: List[str]) -> Dict[Tuple[Any, ...], float]:
        if monthly.empty:
            return {}
        cur = monthly[monthly["month_key"] == self.current_month_key].copy()
        if cur.empty:
            return {}
        return cur.groupby(keys, dropna=False)["gp_minus_nds"].sum().to_dict()

    def aggregate_hierarchy(self, base: pd.DataFrame, value_col: str, label_col: str, labels: List[str], plan_mode: str) -> pd.DataFrame:
        rows: List[Dict[str, Any]] = []
        if base.empty:
            return pd.DataFrame(columns=["Наименование"] + labels + ["План"])

        plan = self.build_plan_month()
        monthly_fact = self.build_last3months_fact()
        article_fact_map = self.build_month_fact_by_entity(monthly_fact, ["supplier_article"])
        product_fact_map = self.build_month_fact_by_entity(monthly_fact, ["subject", "code"])
        category_fact_map = self.build_month_fact_by_entity(monthly_fact, ["subject"])
        article_plan_map = plan.set_index("supplier_article")["plan_gp_minus_nds_month"].to_dict() if not plan.empty else {}
        product_plan_map = plan.groupby(["subject", "code"], dropna=False)["plan_gp_minus_nds_month"].sum().to_dict() if not plan.empty else {}
        category_plan_map = plan.groupby(["subject"], dropna=False)["plan_gp_minus_nds_month"].sum().to_dict() if not plan.empty else {}

        def block_plan(level: str, subject: Optional[str], code: Optional[str], article: Optional[str], fact_values: List[float]) -> float:
            if plan_mode == "daily":
                if level == "article":
                    p = article_plan_map.get(article, np.nan)
                    if pd.isna(p):
                        return float(np.nanmean(fact_values)) if fact_values else 0.0
                    return float(p) / self.days_in_month
                if level == "product":
                    p = product_plan_map.get((subject, code), np.nan)
                    if pd.isna(p):
                        return float(np.nanmean(fact_values)) if fact_values else 0.0
                    return float(p) / self.days_in_month
                p = category_plan_map.get(subject, np.nan)
                if pd.isna(p):
                    return float(np.nanmean(fact_values)) if fact_values else 0.0
                return float(p) / self.days_in_month
            # month plan for weekly/monthly blocks
            if level == "article":
                p = article_plan_map.get(article, np.nan)
                if pd.isna(p):
                    return float(article_fact_map.get((article,), 0.0))
                return float(p)
            if level == "product":
                p = product_plan_map.get((subject, code), np.nan)
                if pd.isna(p):
                    return float(product_fact_map.get((subject, code), 0.0))
                return float(p)
            p = category_plan_map.get((subject,), np.nan)
            if pd.isna(p):
                return float(category_fact_map.get((subject,), 0.0))
            return float(p)

        for subject in self.subject_order:
            sg = base[base["subject"] == subject].copy()
            if sg.empty:
                continue
            fact_values = [float(sg.loc[sg[label_col] == lbl, value_col].sum()) for lbl in labels]
            row = {"Наименование": subject, "_level": "category"}
            for lbl, val in zip(labels, fact_values):
                row[lbl] = val
            row["План"] = block_plan("category", subject, None, None, fact_values)
            rows.append(row)

            prod_order = sg.groupby("code", dropna=False)[value_col].sum().sort_values(ascending=False).index.tolist()
            for code in prod_order:
                pg = sg[sg["code"] == code].copy()
                fact_values = [float(pg.loc[pg[label_col] == lbl, value_col].sum()) for lbl in labels]
                prow = {"Наименование": str(code), "_level": "product", "_subject": subject}
                for lbl, val in zip(labels, fact_values):
                    prow[lbl] = val
                prow["План"] = block_plan("product", subject, code, None, fact_values)
                rows.append(prow)

                art_order = pg.groupby("supplier_article", dropna=False)[value_col].sum().sort_values(ascending=False).index.tolist()
                for art in art_order:
                    ag = pg[pg["supplier_article"] == art].copy()
                    fact_values = [float(ag.loc[ag[label_col] == lbl, value_col].sum()) for lbl in labels]
                    arow = {"Наименование": art, "_level": "article", "_subject": subject, "_code": code}
                    for lbl, val in zip(labels, fact_values):
                        arow[lbl] = val
                    arow["План"] = block_plan("article", subject, code, art, fact_values)
                    rows.append(arow)

            total = {"Наименование": f"Итого {subject}", "_level": "subject_total"}
            for lbl in labels:
                total[lbl] = float(sg.loc[sg[label_col] == lbl, value_col].sum())
            total["План"] = block_plan("category", subject, None, None, [total[lbl] for lbl in labels])
            rows.append(total)

        grand = {"Наименование": "Итого по всем 4 категориям", "_level": "grand_total"}
        for lbl in labels:
            grand[lbl] = float(base.loc[base[label_col] == lbl, value_col].sum())
        if plan_mode == "daily":
            grand["План"] = float(sum(v for v in category_plan_map.values() if pd.notna(v))) / self.days_in_month if category_plan_map else float(np.nanmean([grand[lbl] for lbl in labels]))
        else:
            grand["План"] = float(sum(v for v in category_plan_map.values() if pd.notna(v))) if category_plan_map else float(sum(category_fact_map.values()))
        rows.append(grand)
        out = pd.DataFrame(rows)
        return out

    def build_main_blocks(self) -> Dict[str, pd.DataFrame]:
        daily = self.build_current_week_daily_article()
        daily = self._filter_subjects(daily)
        day_labels = [d.strftime("%d.%m") for d in self.current_week_days]
        block_daily_main = self.aggregate_hierarchy(daily, "gp_minus_nds_rub", "day_label", day_labels, plan_mode="daily") if not daily.empty else pd.DataFrame()
        block_daily_gp = self.aggregate_hierarchy(daily, "gross_profit_rub", "day_label", day_labels, plan_mode="daily") if not daily.empty else pd.DataFrame()
        block_daily_vat = self.aggregate_hierarchy(daily, "vat_rub", "day_label", day_labels, plan_mode="daily") if not daily.empty else pd.DataFrame()

        weekly = self.build_current_month_weekly_fact()
        weekly = self._filter_subjects(weekly)
        week_labels = sorted(weekly["week_label"].dropna().unique().tolist()) if not weekly.empty else []
        block_weekly_main = self.aggregate_hierarchy(weekly, "gp_minus_nds", "week_label", week_labels, plan_mode="month") if not weekly.empty else pd.DataFrame()
        block_weekly_gp = self.aggregate_hierarchy(weekly, "gross_profit", "week_label", week_labels, plan_mode="month") if not weekly.empty else pd.DataFrame()
        block_weekly_vat = self.aggregate_hierarchy(weekly, "vat", "week_label", week_labels, plan_mode="month") if not weekly.empty else pd.DataFrame()

        monthly = self.build_last3months_fact()
        monthly = self._filter_subjects(monthly)
        month_keys = [(self.latest_day.to_period("M") - 2).strftime("%Y-%m"), (self.latest_day.to_period("M") - 1).strftime("%Y-%m"), self.current_month_key]
        block_monthly_main = self.aggregate_hierarchy(monthly, "gp_minus_nds", "month_key", month_keys, plan_mode="month") if not monthly.empty else pd.DataFrame()
        block_monthly_gp = self.aggregate_hierarchy(monthly, "gross_profit", "month_key", month_keys, plan_mode="month") if not monthly.empty else pd.DataFrame()
        block_monthly_vat = self.aggregate_hierarchy(monthly, "vat", "month_key", month_keys, plan_mode="month") if not monthly.empty else pd.DataFrame()

        return {
            "daily_main": block_daily_main,
            "daily_gp": block_daily_gp,
            "daily_vat": block_daily_vat,
            "weekly_main": block_weekly_main,
            "weekly_gp": block_weekly_gp,
            "weekly_vat": block_weekly_vat,
            "monthly_main": block_monthly_main,
            "monthly_gp": block_monthly_gp,
            "monthly_vat": block_monthly_vat,
            "tech_daily": daily,
            "tech_weekly": weekly,
            "tech_monthly": monthly,
            "tech_buyout90": self.buyout90,
            "tech_econ_latest": self.econ_latest,
            "tech_plan": self.build_plan_month(),
            "tech_master": self.master,
        }

    def build_example_weekly(self, articles: List[str]) -> pd.DataFrame:
        # Detailed weekly forecast vs ABC for selected articles
        orders = self._filter_subjects(self.data.orders)
        if orders.empty:
            return pd.DataFrame()
        ads = self._filter_subjects(self.data.ads_daily)
        abc = self._filter_subjects(self.data.abc_weekly)
        econ = self._filter_subjects(self.data.economics)
        funnel = self.data.funnel.copy()

        orders = orders[orders["supplier_article"].isin(articles)].copy()
        if orders.empty:
            return pd.DataFrame()
        orders["week_code"] = orders["day"].map(week_code_from_date)
        # recent 4 weeks by orders
        recent_weeks = sorted(orders["week_code"].dropna().unique().tolist())[-4:]
        rows = []
        for art in articles:
            oa = orders[orders["supplier_article"] == art].copy()
            if oa.empty:
                continue
            nm_id = oa["nm_id"].dropna().iloc[0] if oa["nm_id"].notna().any() else np.nan
            subject = oa["subject"].dropna().iloc[0] if oa["subject"].notna().any() else ""
            for wk in recent_weeks:
                ws, we = week_bounds_from_code(wk)
                ws = pd.Timestamp(ws) if ws else pd.NaT
                we = pd.Timestamp(we) if we else pd.NaT
                w_orders = oa[oa["week_code"] == wk].copy()
                if w_orders.empty:
                    continue
                orders_week = w_orders["orders"].sum()
                fsub = funnel.copy()
                if not pd.isna(nm_id):
                    fsub = fsub[fsub["nm_id"] == nm_id].copy()
                if not fsub.empty and pd.notna(we):
                    fsub = fsub[(fsub["day"] >= we - pd.Timedelta(days=89)) & (fsub["day"] <= we)].copy()
                    buyout_pct_90 = safe_div(fsub["buyouts_count"].sum(), fsub["orders"].sum())
                else:
                    buyout_pct_90 = np.nan
                e = econ[(econ["supplier_article"] == art) & (econ["week_code"] == wk)].copy()
                if e.empty:
                    e = econ[econ["supplier_article"] == art].copy().sort_values("week_code", ascending=False).head(1)
                if e.empty:
                    continue
                e = e.iloc[0]
                pwd = w_orders["price_with_disc"].mean()
                fp = w_orders["finished_price"].mean()
                buyout_factor = buyout_pct_90 if pd.notna(buyout_pct_90) else safe_div(e.get("Процент выкупа", np.nan), 100)
                if pd.isna(buyout_factor):
                    buyout_factor = 1.0
                buyout_qty = orders_week * buyout_factor
                revenue_pwd = buyout_qty * pwd
                commission = revenue_pwd * float(e.get("Комиссия WB, %", 0) or 0) / 100.0
                acquiring = revenue_pwd * float(e.get("Эквайринг, %", 0) or 0) / 100.0
                logistics_direct = buyout_qty * float(e.get("Логистика прямая, руб/ед", 0) or 0)
                logistics_return = buyout_qty * float(e.get("Логистика обратная, руб/ед", 0) or 0)
                storage = buyout_qty * float(e.get("Хранение, руб/ед", 0) or 0)
                other = buyout_qty * float(e.get("Прочие расходы, руб/ед", 0) or 0)
                cost = buyout_qty * float(e.get("Себестоимость, руб", 0) or 0)
                if not ads.empty:
                    adw = ads[(ads["supplier_article"] == art) & (ads["day"] >= ws) & (ads["day"] <= we)]
                    ad_spend = adw["spend"].sum()
                else:
                    ad_spend = 0.0
                vat = buyout_qty * fp * 7.0 / 107.0
                gp_forecast = revenue_pwd - commission - acquiring - logistics_direct - logistics_return - storage - other - cost - ad_spend
                gp_minus_nds_forecast = gp_forecast - vat
                abcw = abc[(abc["supplier_article"] == art) & (abc["week_code"] == wk)]
                abc_gp = abcw["gross_profit"].sum() if not abcw.empty else np.nan
                abc_vat = abcw["vat"].sum() if not abcw.empty else np.nan
                abc_gp_minus_nds = abcw["gp_minus_nds"].sum() if not abcw.empty else np.nan
                rows.append({
                    "Артикул": art,
                    "Категория": subject,
                    "Неделя": wk,
                    "Заказы, шт": orders_week,
                    "% выкупа 90д": buyout_factor,
                    "Выкупленные продажи, шт": buyout_qty,
                    "Средний priceWithDisc": pwd,
                    "Средний finishedPrice": fp,
                    "Выручка по priceWithDisc, ₽": revenue_pwd,
                    "Комиссия WB, ₽": commission,
                    "Эквайринг, ₽": acquiring,
                    "Логистика прямая, ₽": logistics_direct,
                    "Логистика обратная, ₽": logistics_return,
                    "Хранение, ₽": storage,
                    "Прочие расходы, ₽": other,
                    "Себестоимость, ₽": cost,
                    "Реклама, ₽": ad_spend,
                    "НДС, ₽": vat,
                    "Валовая прибыль прогноз, ₽": gp_forecast,
                    "Валовая прибыль - НДС прогноз, ₽": gp_minus_nds_forecast,
                    "ABC Валовая прибыль, ₽": abc_gp,
                    "ABC НДС, ₽": abc_vat,
                    "ABC Валовая прибыль - НДС, ₽": abc_gp_minus_nds,
                    "Отклонение прогноза к ABC ВП-НДС, ₽": gp_minus_nds_forecast - abc_gp_minus_nds if pd.notna(abc_gp_minus_nds) else np.nan,
                })
        return pd.DataFrame(rows)


# -------------------------
# Export helpers
# -------------------------

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

    cols = [c for c in df.columns if not c.startswith("_")]
    style_title(ws, start_row, 1, len(cols), title)
    hdr = start_row + 1
    for j, col in enumerate(cols, start=1):
        set_header(ws.cell(hdr, j, col if col != "Наименование" else ""))

    row = hdr + 1
    cat_start = None
    prod_start = None
    prod_row = None
    cat_row = None

    for _, r in df.iterrows():
        level = r.get("_level", "")
        if level == "category":
            if prod_start and prod_row and row - 1 >= prod_start:
                ws.row_dimensions.group(prod_start, row - 1, outline_level=2, hidden=True)
                prod_start = None
                prod_row = None
            if cat_start and cat_row and row - 1 >= cat_start:
                ws.row_dimensions.group(cat_start, row - 1, outline_level=1, hidden=True)
                cat_start = None
            cat_row = row
            cat_start = row + 1
        elif level == "product":
            if prod_start and prod_row and row - 1 >= prod_start:
                ws.row_dimensions.group(prod_start, row - 1, outline_level=2, hidden=True)
            prod_row = row
            prod_start = row + 1

        for j, col in enumerate(cols, start=1):
            cell = ws.cell(row, j, r[col])
            cell.border = BORDER
            cell.alignment = Alignment(horizontal="center", vertical="center")
            if j >= 2 and isinstance(r[col], (int, float, np.integer, np.floating)) and not pd.isna(r[col]):
                fmt_money(cell)

        if level == "category":
            for j in range(1, len(cols) + 1):
                ws.cell(row, j).font = Font(bold=True)
                ws.cell(row, j).fill = FILL_CATEGORY
        elif level == "product":
            for j in range(1, len(cols) + 1):
                ws.cell(row, j).font = Font(bold=True, italic=True)
                ws.cell(row, j).fill = FILL_PRODUCT
            ws.row_dimensions[row].outlineLevel = 1
        elif level == "article":
            ws.row_dimensions[row].outlineLevel = 2
        elif level in {"subject_total", "grand_total"}:
            for j in range(1, len(cols) + 1):
                ws.cell(row, j).font = Font(bold=True)
                ws.cell(row, j).fill = FILL_TOTAL

        row += 1

    if prod_start and prod_row and row - 1 >= prod_start:
        ws.row_dimensions.group(prod_start, row - 1, outline_level=2, hidden=True)
    if cat_start and cat_row and row - 1 >= cat_start:
        ws.row_dimensions.group(cat_start, row - 1, outline_level=1, hidden=True)

    ws.sheet_properties.outlinePr.summaryBelow = False
    return row + 2


def write_dataframe_sheet(wb: Workbook, sheet_name: str, df: pd.DataFrame):
    ws = wb.create_sheet(sheet_name[:31])
    if df is None or df.empty:
        ws.cell(1, 1, "Нет данных")
        return
    for j, col in enumerate(df.columns, start=1):
        set_header(ws.cell(1, j, col))
    for i, row_vals in enumerate(df.itertuples(index=False), start=2):
        for j, val in enumerate(row_vals, start=1):
            c = ws.cell(i, j, val)
            c.border = BORDER
            c.alignment = Alignment(horizontal="center", vertical="center")
            if isinstance(val, (int, float, np.integer, np.floating)) and not pd.isna(val):
                # heuristic formatting
                col_name = df.columns[j-1].lower()
                if "%" in df.columns[j-1] or "процент" in df.columns[j-1].lower():
                    fmt_pct(c)
                elif "цена" in col_name or "руб" in col_name or "прибыль" in col_name or "ндс" in col_name or "выручка" in col_name or "расход" in col_name or "себестоимость" in col_name:
                    fmt_money(c)
                else:
                    fmt_num(c)
    autofit(ws)
    ws.freeze_panes = "A2"


def export_main_and_tech(main: Dict[str, pd.DataFrame], out_report: str, out_tech: str, out_example: str):
    wb = Workbook()
    ws = wb.active
    ws.title = "Сводка"
    row = 1
    row = write_hierarchy_block(ws, row, "Текущая неделя — Валовая прибыль - НДС", main["daily_main"])
    row = write_hierarchy_block(ws, row, "Текущая неделя — Валовая прибыль", main["daily_gp"])
    row = write_hierarchy_block(ws, row, "Текущая неделя — НДС", main["daily_vat"])
    row = write_hierarchy_block(ws, row, "Текущий месяц — Валовая прибыль - НДС по неделям", main["weekly_main"])
    row = write_hierarchy_block(ws, row, "Последние 3 месяца — Валовая прибыль - НДС", main["monthly_main"])
    ws.freeze_panes = "B3"
    autofit(ws)
    wb.save(out_report)

    twb = Workbook()
    twb.remove(twb.active)
    for sheet_name in [
        "tech_daily", "tech_weekly", "tech_monthly", "tech_buyout90", "tech_econ_latest", "tech_plan", "tech_master"
    ]:
        write_dataframe_sheet(twb, sheet_name.replace("tech_", ""), main.get(sheet_name, pd.DataFrame()))
    twb.save(out_tech)

    ex = main.get("example_weekly", pd.DataFrame())
    ewb = Workbook()
    ewb.remove(ewb.active)
    if ex is None or ex.empty:
        ws = ewb.create_sheet("Пример")
        ws.cell(1, 1, "Нет данных для примеров")
    else:
        for art in TARGET_EXAMPLE_ARTICLES:
            x = ex[ex["Артикул"] == art].copy()
            write_dataframe_sheet(ewb, art.replace("/", "_"), x)
    ewb.save(out_example)


# -------------------------
# CLI
# -------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="WB Stage 1 gross profit report")
    p.add_argument("--root", default=".")
    p.add_argument("--reports-root", default="Отчёты")
    p.add_argument("--store", default="TOPFACE")
    p.add_argument("--out-subdir", default="Отчёты/Объединенный отчет/TOPFACE")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    storage = make_storage(args.root)
    loader = Stage1Loader(storage, args.reports_root, args.store)
    log("Loading data")
    data = loader.load_all()
    log("Building stage 1")
    builder = Stage1Builder(data)
    main_blocks = builder.build_main_blocks()
    main_blocks["example_weekly"] = builder.build_example_weekly(TARGET_EXAMPLE_ARTICLES)

    stamp = datetime.now().strftime("%Y-%m-%d")
    out_report = f"{args.out_subdir}/Объединенный_отчет_{args.store}_{stamp}.xlsx"
    out_tech = f"{args.out_subdir}/Технические_расчеты_{args.store}_{stamp}.xlsx"
    out_example = f"{args.out_subdir}/Пример_расчета_901_{args.store}_{stamp}.xlsx"

    local_report = Path("/tmp") / f"wb_stage1_report_{stamp}.xlsx"
    local_tech = Path("/tmp") / f"wb_stage1_tech_{stamp}.xlsx"
    local_example = Path("/tmp") / f"wb_stage1_example_{stamp}.xlsx"
    export_main_and_tech(main_blocks, str(local_report), str(local_tech), str(local_example))

    storage.write_bytes(out_report, local_report.read_bytes())
    storage.write_bytes(out_tech, local_tech.read_bytes())
    storage.write_bytes(out_example, local_example.read_bytes())
    log(f"Saved report: {out_report}")
    log(f"Saved technical workbook: {out_tech}")
    log(f"Saved example workbook: {out_example}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
