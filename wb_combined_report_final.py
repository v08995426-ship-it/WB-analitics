#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import calendar
import io
import os
import re
from dataclasses import dataclass
from datetime import date, datetime
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


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("\xa0", " ").strip()
    text = re.sub(r"\s+", " ", text)
    return text


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
    if text in EXCLUDE_ARTICLES:
        return ""
    if text.startswith("PT901"):
        return "901"
    m = re.match(r"^([A-ZА-Я0-9]+)", text)
    if not m:
        return ""
    return re.sub(r"^PT", "", m.group(1))


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
    if b == 0 or pd.isna(a) or pd.isna(b):
        return np.nan
    return a / b


def week_code_from_date(dt_value: Any) -> Optional[str]:
    if pd.isna(dt_value):
        return None
    ts = pd.Timestamp(dt_value)
    iso = ts.isocalendar()
    return f"{int(iso.year)}-W{int(iso.week):02d}"


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


ALIASES = {
    "day": ["Дата", "Дата заказа", "dt", "date"],
    "nm_id": ["Артикул WB", "Артикул ВБ", "nmID", "nmId"],
    "supplier_article": ["Артикул продавца", "supplierArticle", "Артикул WB продавца"],
    "subject": ["Предмет", "subject", "Название предмета"],
    "brand": ["Бренд", "brand"],
    "title": ["Название", "Название товара", "Товар"],
    "orders": ["Заказы", "orders", "ordersCount", "Кол-во продаж"],
    "buyouts_count": ["buyoutsCount"],
    "finished_price": ["finishedPrice", "Ср. цена продажи", "Цена с учетом всех скидок, кроме суммы по WB Кошельку"],
    "price_with_disc": ["priceWithDisc", "Цена со скидкой продавца, в том числе со скидкой WB Клуба"],
    "spp": ["СПП, %", "SPP", "Скидка WB, %"],
    "gross_profit": ["Валовая прибыль", "Валовая прибыль, руб/ед"],
    "gross_revenue": ["Валовая выручка"],
}


def rename_using_aliases(df: pd.DataFrame) -> pd.DataFrame:
    cols = {norm_key(c): c for c in df.columns}
    out = df.copy()
    for target, aliases in ALIASES.items():
        found = None
        for a in aliases:
            k = norm_key(a)
            if k in cols:
                found = cols[k]
                break
        if found is not None and found != target:
            out[target] = out[found]
        elif found is None and target not in out.columns:
            out[target] = np.nan
    return out


def read_excel_best(data: bytes, preferred_sheet: Optional[str] = None, header_candidates: Iterable[int] = (0, 1, 2)) -> pd.DataFrame:
    bio = io.BytesIO(data)
    xl = pd.ExcelFile(bio)
    sheet = preferred_sheet if preferred_sheet in xl.sheet_names else xl.sheet_names[0]
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


@dataclass
class LoadedData:
    orders: pd.DataFrame
    funnel: pd.DataFrame
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
                df = rename_using_aliases(read_excel_best(self.storage.read_bytes(path)))
                df["day"] = to_dt(df["day"])
                df["nm_id"] = to_numeric(df["nm_id"])
                df["supplier_article"] = df["supplier_article"].map(clean_article)
                df["subject"] = df["subject"].map(normalize_text)
                df["finished_price"] = to_numeric(df["finished_price"])
                df["price_with_disc"] = to_numeric(df["price_with_disc"])
                df["spp"] = to_numeric(df["spp"])
                df["orders"] = to_numeric(df["orders"])
                if df["orders"].isna().all():
                    df["orders"] = 1
                frames.append(df[["day", "nm_id", "supplier_article", "subject", "finished_price", "price_with_disc", "spp", "orders"]])
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
        df = rename_using_aliases(read_excel_best(self.storage.read_bytes(path)))
        df["day"] = to_dt(df["day"])
        df["nm_id"] = to_numeric(df["nm_id"])
        if "buyoutsCount" in df.columns and "buyouts_count" not in df.columns:
            df["buyouts_count"] = to_numeric(df["buyoutsCount"])
        else:
            df["buyouts_count"] = to_numeric(df["buyouts_count"])
        df["orders"] = to_numeric(df["orders"])
        return df[["day", "nm_id", "orders", "buyouts_count"]]

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
        df["Процент выкупа"] = to_numeric(df.get("Процент выкупа", np.nan))
        df["НДС, руб/ед"] = to_numeric(df.get("НДС, руб/ед", np.nan))
        df["Валовая прибыль, руб/ед"] = to_numeric(df.get("Валовая прибыль, руб/ед", np.nan))
        df["Средняя цена продажи"] = to_numeric(df.get("Средняя цена продажи", np.nan))
        df["week_code"] = df.get("Неделя", pd.Series([None] * len(df))).astype(str).str.strip()
        df["gp_minus_nds_unit"] = df["Валовая прибыль, руб/ед"] - df["НДС, руб/ед"]
        return df[[
            "week_code", "supplier_article", "nm_id", "subject",
            "Процент выкупа", "НДС, руб/ед", "Валовая прибыль, руб/ед",
            "gp_minus_nds_unit", "Средняя цена продажи"
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
                df["vat"] = df["gross_revenue"] * 7.0 / 107.0
                df["gp_minus_nds"] = df["gross_profit"] - df["vat"]
                df["code"] = df["supplier_article"].map(clean_code_from_article)
                if start.day == 1 and end == (pd.Timestamp(end).to_period("M").end_time.normalize()).date():
                    df["month_key"] = f"{start.year:04d}-{start.month:02d}"
                    monthly_frames.append(df[["month_key", "supplier_article", "nm_id", "subject", "code", "gross_profit", "gross_revenue", "vat", "gp_minus_nds", "orders"]])
                else:
                    df["week_code"] = week_code_from_date(start)
                    weekly_frames.append(df[["week_code", "period_start", "period_end", "supplier_article", "nm_id", "subject", "code", "gross_profit", "gross_revenue", "vat", "gp_minus_nds", "orders"]])
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
                return pd.DataFrame(columns=["supplier_article", "plan_gp_minus_nds_month", "subject"])
            path = alt
        raw = pd.read_excel(io.BytesIO(self.storage.read_bytes(path)), sheet_name="Итог_все_категории", header=2)
        raw.columns = [normalize_text(c) for c in raw.columns]
        raw = raw.rename(columns={
            normalize_text("Артикул продавца"): "supplier_article",
            normalize_text("Категория"): "subject",
        })
        month_col = f"ВП-НДС {russian_month_name(current_month.month)} {current_month.year}"
        month_col_norm = normalize_text(month_col)
        chosen_col = None
        for c in raw.columns:
            if month_col_norm == norm_key(c) or month_col_norm in norm_key(c):
                chosen_col = c
                break
        if chosen_col is None:
            return pd.DataFrame(columns=["supplier_article", "plan_gp_minus_nds_month", "subject"])
        out = raw[["supplier_article", chosen_col, "subject"]].copy()
        out["supplier_article"] = out["supplier_article"].map(clean_article)
        out["subject"] = out["subject"].map(normalize_text)
        out["plan_gp_minus_nds_month"] = to_numeric(out[chosen_col])
        out["code"] = out["supplier_article"].map(clean_code_from_article)
        return out[["supplier_article", "subject", "code", "plan_gp_minus_nds_month"]]

    def load_all(self) -> LoadedData:
        orders = self.load_orders()
        funnel = self.load_funnel()
        economics = self.load_economics()
        abc_weekly, abc_monthly = self.load_abc()
        latest = []
        if not orders.empty:
            latest.append(pd.to_datetime(orders["day"]).max())
        if not funnel.empty:
            latest.append(pd.to_datetime(funnel["day"]).max())
        if not abc_weekly.empty:
            latest.append(pd.to_datetime(abc_weekly["period_end"]).max())
        latest_date = max([d for d in latest if pd.notna(d)], default=pd.Timestamp(datetime.today().date()))
        plan = self.load_plan(pd.Timestamp(latest_date))
        return LoadedData(orders, funnel, economics, abc_weekly, abc_monthly, plan, pd.Timestamp(latest_date))


class Stage1Builder:
    def __init__(self, data: LoadedData):
        self.data = data
        self.current_day = pd.Timestamp(data.latest_date).normalize()
        self.current_week_start = self.current_day - pd.Timedelta(days=self.current_day.weekday())
        self.current_week_days = [self.current_week_start + pd.Timedelta(days=i) for i in range((self.current_day - self.current_week_start).days + 1)]
        self.current_month_key = self.current_day.strftime("%Y-%m")
        self.current_month_start = pd.Timestamp(self.current_day.replace(day=1))
        self.days_in_month = calendar.monthrange(self.current_day.year, self.current_day.month)[1]

    def _filter(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df.copy()
        out = df.copy()
        if "subject" in out.columns:
            out["subject"] = out["subject"].map(normalize_text)
            out = out[out["subject"].isin(TARGET_SUBJECTS)].copy()
        if "supplier_article" in out.columns:
            out["supplier_article"] = out["supplier_article"].map(clean_article)
            out = out[~out["supplier_article"].map(upper_article).isin(EXCLUDE_ARTICLES)].copy()
        out["code"] = out["supplier_article"].map(clean_code_from_article)
        out = out[out["code"] != ""].copy()
        return out

    def build_buyout90(self) -> pd.DataFrame:
        f = self.data.funnel.copy()
        if f.empty:
            return pd.DataFrame(columns=["nm_id", "buyout_pct_90"])
        min_day = self.current_day - pd.Timedelta(days=89)
        f = f[(f["day"] >= min_day) & (f["day"] <= self.current_day)].copy()
        g = f.groupby("nm_id", dropna=False).agg(orders_90=("orders", "sum"), buyouts_90=("buyouts_count", "sum")).reset_index()
        g["buyout_pct_90"] = g.apply(lambda r: safe_div(r["buyouts_90"], r["orders_90"]), axis=1)
        return g[["nm_id", "buyout_pct_90"]]

    def build_master(self) -> pd.DataFrame:
        frames = []
        for df in [self.data.orders, self.data.economics, self.data.abc_weekly, self.data.abc_monthly]:
            if df.empty:
                continue
            cols = [c for c in ["supplier_article", "nm_id", "subject", "brand", "title"] if c in df.columns]
            x = df[cols].copy()
            for c in ["supplier_article", "nm_id", "subject", "brand", "title"]:
                if c not in x.columns:
                    x[c] = np.nan
            frames.append(x[["supplier_article", "nm_id", "subject", "brand", "title"]])
        if not frames:
            return pd.DataFrame(columns=["supplier_article", "nm_id", "subject", "brand", "title", "code"])
        m = pd.concat(frames, ignore_index=True)
        m["supplier_article"] = m["supplier_article"].map(clean_article)
        m["nm_id"] = to_numeric(m["nm_id"])
        m["subject"] = m["subject"].map(normalize_text)
        m["brand"] = m["brand"].map(normalize_text)
        m["title"] = m["title"].map(normalize_text)
        m["code"] = m["supplier_article"].map(clean_code_from_article)
        m = self._filter(m)
        m["quality"] = m["supplier_article"].ne("").astype(int) * 4 + m["subject"].ne("").astype(int) * 2 + m["title"].ne("").astype(int)
        m = m.sort_values(["quality"], ascending=False).drop_duplicates(subset=["supplier_article", "nm_id"])
        return m[["supplier_article", "nm_id", "subject", "brand", "title", "code"]]

    def build_daily_metrics(self) -> pd.DataFrame:
        orders = self._filter(self.data.orders)
        if orders.empty:
            return pd.DataFrame()
        orders = orders[(orders["day"] >= self.current_week_start) & (orders["day"] <= self.current_day)].copy()
        buyout = self.build_buyout90()
        master = self.build_master()
        econ = self._filter(self.data.economics)
        if econ.empty:
            return pd.DataFrame()
        orders = orders.merge(master[["supplier_article", "nm_id", "subject", "code", "title"]].drop_duplicates(), on=["supplier_article", "nm_id"], how="left", suffixes=("", "_m"))
        for fld in ["subject", "code", "title"]:
            if f"{fld}_m" in orders.columns:
                orders[fld] = orders[fld].where(orders[fld].notna() & (orders[fld] != ""), orders[f"{fld}_m"])
                orders.drop(columns=[f"{fld}_m"], inplace=True)
        orders["week_code"] = orders["day"].map(week_code_from_date)
        day_g = orders.groupby(["day", "week_code", "supplier_article", "nm_id", "subject", "code", "title"], dropna=False).agg(orders_day=("orders", "sum")).reset_index()
        day_g = day_g.merge(buyout, on="nm_id", how="left")
        econ_use = econ[["week_code", "supplier_article", "nm_id", "НДС, руб/ед", "Валовая прибыль, руб/ед", "gp_minus_nds_unit", "Процент выкупа"]].copy()
        out = day_g.merge(econ_use, on=["week_code", "supplier_article", "nm_id"], how="left")
        out["buyout_factor"] = out["buyout_pct_90"].fillna(to_numeric(out["Процент выкупа"]) / 100.0).fillna(1.0)
        out["gp_day"] = out["orders_day"] * out["buyout_factor"] * to_numeric(out["Валовая прибыль, руб/ед"]).fillna(0)
        out["vat_day"] = out["orders_day"] * out["buyout_factor"] * to_numeric(out["НДС, руб/ед"]).fillna(0)
        out["gp_minus_nds_day"] = out["orders_day"] * out["buyout_factor"] * to_numeric(out["gp_minus_nds_unit"]).fillna(0)
        out["day_label"] = pd.to_datetime(out["day"]).dt.strftime("%d.%m")
        return out

    def build_weekly_current_month(self) -> pd.DataFrame:
        abc = self._filter(self.data.abc_weekly)
        if abc.empty:
            return pd.DataFrame()
        abc = abc[(abc["period_end"] >= self.current_month_start) & (abc["period_start"] <= self.current_day)].copy()
        abc["week_label"] = pd.to_datetime(abc["period_start"]).dt.strftime("%d.%m")
        return abc

    def build_monthly_last3(self) -> pd.DataFrame:
        abc_month = self._filter(self.data.abc_monthly)
        abc_week = self._filter(self.data.abc_weekly)
        current = self.current_day.to_period("M")
        months = [(current - 2).strftime("%Y-%m"), (current - 1).strftime("%Y-%m"), current.strftime("%Y-%m")]
        frames = []
        if not abc_month.empty:
            frames.append(abc_month[abc_month["month_key"].isin(months)].copy())
        if abc_month.empty or current.strftime("%Y-%m") not in set(abc_month.get("month_key", pd.Series(dtype=str)).astype(str)):
            wk = abc_week.copy()
            if not wk.empty:
                wk["month_key"] = pd.to_datetime(wk["period_start"]).dt.to_period("M").astype(str)
                wk = wk[wk["month_key"] == current.strftime("%Y-%m")].copy()
                if not wk.empty:
                    agg = wk.groupby(["month_key", "supplier_article", "nm_id", "subject", "code"], dropna=False).agg(
                        gross_profit=("gross_profit", "sum"),
                        gross_revenue=("gross_revenue", "sum"),
                        vat=("vat", "sum"),
                        gp_minus_nds=("gp_minus_nds", "sum"),
                        orders=("orders", "sum"),
                    ).reset_index()
                    frames = [f[f["month_key"] != current.strftime("%Y-%m")] for f in frames]
                    frames.append(agg)
        if not frames:
            return pd.DataFrame()
        out = pd.concat(frames, ignore_index=True)
        return out[out["month_key"].isin(months)].copy()

    def plan_map(self):
        plan = self._filter(self.data.plan)
        if plan.empty:
            return {}, {}, {}
        article = plan.set_index("supplier_article")["plan_gp_minus_nds_month"].to_dict()
        product = plan.groupby(["subject", "code"], dropna=False)["plan_gp_minus_nds_month"].sum().to_dict()
        category = plan.groupby("subject", dropna=False)["plan_gp_minus_nds_month"].sum().to_dict()
        return article, product, category

    def month_fact_map(self):
        m = self.build_monthly_last3()
        if m.empty:
            return {}, {}, {}
        cur = m[m["month_key"] == self.current_month_key].copy()
        if cur.empty:
            return {}, {}, {}
        article = cur.groupby("supplier_article", dropna=False)["gp_minus_nds"].sum().to_dict()
        product = cur.groupby(["subject", "code"], dropna=False)["gp_minus_nds"].sum().to_dict()
        category = cur.groupby("subject", dropna=False)["gp_minus_nds"].sum().to_dict()
        return article, product, category

    def aggregate_hierarchy(self, base: pd.DataFrame, value_col: str, period_col: str, labels: List[str]) -> pd.DataFrame:
        if base.empty:
            return pd.DataFrame()
        rows = []
        art_plan, prod_plan, cat_plan = self.plan_map()
        art_fact, prod_fact, cat_fact = self.month_fact_map()

        for subject in TARGET_SUBJECTS:
            sg = base[base["subject"] == subject].copy()
            if sg.empty:
                continue
            cat_row = {"Уровень": "Категория", "Наименование": subject}
            for lbl in labels:
                cat_row[lbl] = float(sg.loc[sg[period_col] == lbl, value_col].sum())
            cat_row["План"] = float(cat_plan.get(subject, cat_fact.get(subject, 0.0)))
            rows.append(cat_row)

            product_order = sg.groupby("code", dropna=False)[value_col].sum().sort_values(ascending=False).index.tolist()
            for code in product_order:
                pg = sg[sg["code"] == code].copy()
                prow = {"Уровень": "Товар", "Наименование": str(code)}
                for lbl in labels:
                    prow[lbl] = float(pg.loc[pg[period_col] == lbl, value_col].sum())
                prow["План"] = float(prod_plan.get((subject, code), prod_fact.get((subject, code), 0.0)))
                rows.append(prow)

                art_order = pg.groupby("supplier_article", dropna=False)[value_col].sum().sort_values(ascending=False).index.tolist()
                for art in art_order:
                    ag = pg[pg["supplier_article"] == art].copy()
                    arow = {"Уровень": "Артикул", "Наименование": art}
                    for lbl in labels:
                        arow[lbl] = float(ag.loc[ag[period_col] == lbl, value_col].sum())
                    arow["План"] = float(art_plan.get(art, art_fact.get(art, 0.0)))
                    rows.append(arow)

            total = {"Уровень": "Итого", "Наименование": f"Итого {subject}"}
            for lbl in labels:
                total[lbl] = float(sg.loc[sg[period_col] == lbl, value_col].sum())
            total["План"] = float(cat_plan.get(subject, cat_fact.get(subject, 0.0)))
            rows.append(total)

        grand = {"Уровень": "Итого", "Наименование": "Итого по всем 4 категориям"}
        for lbl in labels:
            grand[lbl] = float(base.loc[base[period_col] == lbl, value_col].sum())
        grand["План"] = float(sum(cat_plan.values()) if cat_plan else sum(cat_fact.values()))
        rows.append(grand)
        return pd.DataFrame(rows)

    def build_summary_frames(self) -> Dict[str, Any]:
        daily = self.build_daily_metrics()
        daily_labels = [d.strftime("%d.%m") for d in self.current_week_days]
        daily_summary = self.aggregate_hierarchy(daily, "gp_minus_nds_day", "day_label", daily_labels) if not daily.empty else pd.DataFrame()

        weekly = self.build_weekly_current_month()
        week_labels = sorted(weekly["week_label"].dropna().unique().tolist()) if not weekly.empty else []
        weekly_summary = self.aggregate_hierarchy(weekly, "gp_minus_nds", "week_label", week_labels) if not weekly.empty else pd.DataFrame()

        monthly = self.build_monthly_last3()
        month_labels = [(self.current_day.to_period("M") - 2).strftime("%Y-%m"), (self.current_day.to_period("M") - 1).strftime("%Y-%m"), self.current_month_key]
        monthly_summary = self.aggregate_hierarchy(monthly, "gp_minus_nds", "month_key", month_labels) if not monthly.empty else pd.DataFrame()

        tech = {
            "daily_metrics": daily,
            "weekly_metrics": weekly,
            "monthly_metrics": monthly,
            "buyout90": self.build_buyout90(),
            "plan": self.data.plan,
            "master": self.build_master(),
        }
        return {"daily": daily_summary, "weekly": weekly_summary, "monthly": monthly_summary, "tech": tech}


THIN = Side(style="thin", color="D9D9D9")
BORDER = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)
FILL_HEADER = PatternFill("solid", fgColor="DDEBF7")
FILL_SECTION = PatternFill("solid", fgColor="E2F0D9")
FILL_TOTAL = PatternFill("solid", fgColor="FFF2CC")


def fmt_money(cell) -> None:
    cell.number_format = '# ##0 "₽"'


def style_block(ws, start_row: int, title: str, cols_count: int) -> int:
    ws.merge_cells(start_row=start_row, start_column=1, end_row=start_row, end_column=cols_count)
    c = ws.cell(start_row, 1, title)
    c.fill = FILL_SECTION
    c.font = Font(bold=True, size=12)
    c.alignment = Alignment(horizontal="center", vertical="center")
    return start_row + 1


def write_hier_block(ws, start_row: int, title: str, df: pd.DataFrame) -> int:
    if df.empty:
        ws.cell(start_row, 1, title).font = Font(bold=True)
        ws.cell(start_row + 1, 1, "Нет данных")
        return start_row + 3

    cols = ["Уровень", "Наименование"] + [c for c in df.columns if c not in {"Уровень", "Наименование"}]
    df = df[cols].copy()
    row = style_block(ws, start_row, title, len(cols))
    for j, col in enumerate(cols, start=1):
        cell = ws.cell(row, j, "" if col == "Наименование" else col)
        cell.fill = FILL_HEADER
        cell.font = Font(bold=True)
        cell.border = BORDER
        cell.alignment = Alignment(horizontal="center", vertical="center")
    row += 1

    current_category_start = None
    current_product_start = None

    for _, rec in df.iterrows():
        level = rec["Уровень"]
        if level == "Категория":
            if current_product_start and row - 1 >= current_product_start:
                ws.row_dimensions.group(current_product_start, row - 1, outline_level=2, hidden=True)
                current_product_start = None
            if current_category_start and row - 1 >= current_category_start:
                ws.row_dimensions.group(current_category_start, row - 1, outline_level=1, hidden=True)
                current_category_start = None
            current_category_start = row + 1
        elif level == "Товар":
            if current_product_start and row - 1 >= current_product_start:
                ws.row_dimensions.group(current_product_start, row - 1, outline_level=2, hidden=True)
            current_product_start = row + 1

        for j, col in enumerate(cols, start=1):
            v = rec[col]
            cell = ws.cell(row, j, v)
            cell.border = BORDER
            cell.alignment = Alignment(horizontal="center", vertical="center")
            if j >= 3 and pd.notna(v):
                fmt_money(cell)
        if level == "Категория":
            for j in range(1, len(cols) + 1):
                ws.cell(row, j).font = Font(bold=True)
        elif level == "Товар":
            for j in range(1, len(cols) + 1):
                ws.cell(row, j).font = Font(bold=True, italic=True)
            ws.row_dimensions[row].outlineLevel = 1
        elif level == "Артикул":
            ws.row_dimensions[row].outlineLevel = 2
        elif level == "Итого":
            for j in range(1, len(cols) + 1):
                ws.cell(row, j).fill = FILL_TOTAL
                ws.cell(row, j).font = Font(bold=True)
        row += 1

    if current_product_start and row - 1 >= current_product_start:
        ws.row_dimensions.group(current_product_start, row - 1, outline_level=2, hidden=True)
    if current_category_start and row - 1 >= current_category_start:
        ws.row_dimensions.group(current_category_start, row - 1, outline_level=1, hidden=True)

    ws.sheet_properties.outlinePr.summaryBelow = False
    return row + 2


def autofit(ws):
    widths = {}
    for row in ws.iter_rows():
        for c in row:
            if c.value is None:
                continue
            widths[c.column] = max(widths.get(c.column, 0), len(str(c.value)) + 2)
    for idx, w in widths.items():
        if idx == 1:
            ws.column_dimensions[get_column_letter(idx)].width = 14
        elif idx == 2:
            ws.column_dimensions[get_column_letter(idx)].width = 26
        else:
            ws.column_dimensions[get_column_letter(idx)].width = max(14, min(w, 18))


def export_stage1(result: Dict[str, Any], report_path: Path, tech_path: Path) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = "Сводка"
    row = 1
    row = write_hier_block(ws, row, "Текущая неделя — Валовая прибыль - НДС", result["daily"])
    row = write_hier_block(ws, row, "Текущий месяц — Валовая прибыль - НДС по неделям", result["weekly"])
    row = write_hier_block(ws, row, "Последние 3 месяца — Валовая прибыль - НДС", result["monthly"])
    ws.freeze_panes = "C3"
    autofit(ws)
    wb.save(report_path)

    tech_wb = Workbook()
    first = True
    for sheet_name, df in result["tech"].items():
        tws = tech_wb.active if first else tech_wb.create_sheet(sheet_name[:31])
        if first:
            tws.title = sheet_name[:31]
            first = False
        if isinstance(df, pd.DataFrame) and not df.empty:
            for j, col in enumerate(df.columns, start=1):
                c = tws.cell(1, j, col)
                c.fill = FILL_HEADER
                c.font = Font(bold=True)
                c.border = BORDER
                c.alignment = Alignment(horizontal="center", vertical="center")
            for i, row_vals in enumerate(df.itertuples(index=False), start=2):
                for j, val in enumerate(row_vals, start=1):
                    cell = tws.cell(i, j, val)
                    cell.border = BORDER
                    cell.alignment = Alignment(horizontal="center", vertical="center")
        else:
            tws.cell(1, 1, "Нет данных")
        autofit(tws)
    tech_wb.save(tech_path)


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
    loader = Stage1Loader(storage, args.reports_root, args.store)
    data = loader.load_all()
    log("Building stage 1")
    builder = Stage1Builder(data)
    result = builder.build_summary_frames()
    stamp = datetime.now().strftime("%Y-%m-%d")
    report_rel = f"{args.out_subdir}/Объединенный_отчет_{args.store}_{stamp}.xlsx"
    tech_rel = f"{args.out_subdir}/Технические_расчеты_{args.store}_{stamp}.xlsx"
    tmp_report = Path("/tmp") / f"stage1_report_{stamp}.xlsx"
    tmp_tech = Path("/tmp") / f"stage1_tech_{stamp}.xlsx"
    export_stage1(result, tmp_report, tmp_tech)
    storage.write_bytes(report_rel, tmp_report.read_bytes())
    storage.write_bytes(tech_rel, tmp_tech.read_bytes())
    log(f"Saved report: {report_rel}")
    log(f"Saved technical workbook: {tech_rel}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
