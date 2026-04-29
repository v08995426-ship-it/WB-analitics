#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import io
import math
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import boto3
import pandas as pd
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

STORE_NAME = "TOPFACE"
TARGET_SUBJECTS = {
    "кисти косметические": "Кисти косметические",
    "помады": "Помады",
    "блески": "Блески",
    "косметические карандаши": "Косметические карандаши",
}
EXCLUDED_ARTICLES = {
    "CZ420", "CZ420БРОВИ", "CZ420ГЛАЗА", "DE49", "DE49ГЛАЗА",
    "PT901", "CZ420ГЛАЗА", "CZ420", "CZ420БРОВИ", "CZ420ГЛАЗА",
}
WEEKLY_ORDERS_PREFIX = f"Отчёты/Заказы/{STORE_NAME}/Недельные/"
ADS_WEEKLY_PREFIX = f"Отчёты/Реклама/{STORE_NAME}/Недельные/"
FUNNEL_KEY = f"Отчёты/Воронка продаж/{STORE_NAME}/Воронка продаж.xlsx"
ECON_KEY = f"Отчёты/Финансовые показатели/{STORE_NAME}/Экономика.xlsx"
ABC_PREFIX = "Отчёты/ABC/"
PLAN_KEY = f"Отчёты/Объединенный отчет/{STORE_NAME}/План.xlsx"
OUT_DIR = f"Отчёты/Объединенный отчет/{STORE_NAME}/"

HEADER_FILL = PatternFill("solid", fgColor="1F4E78")
HEADER_FONT = Font(color="FFFFFF", bold=True, size=11)
BOLD_FONT = Font(bold=True)
WHITE_FONT = Font(color="FFFFFF", bold=True)
THIN = Side(style="thin", color="D9E2F3")
BORDER = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)
CATEGORY_FILLS = [
    PatternFill("solid", fgColor="1F4E78"),
    PatternFill("solid", fgColor="2F75B5"),
    PatternFill("solid", fgColor="5B9BD5"),
    PatternFill("solid", fgColor="9DC3E6"),
]
PRODUCT_FILL = PatternFill("solid", fgColor="D9EAF7")
ARTICLE_FILL = PatternFill("solid", fgColor="FFFFFF")
TOTAL_FILL = PatternFill("solid", fgColor="D9E2F3")


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return default
        if isinstance(v, str):
            s = v.replace("\xa0", " ").replace("%", "").replace(",", ".").strip()
            if not s:
                return default
            return float(s)
        return float(v)
    except Exception:
        return default


def normalize_text(v: Any) -> str:
    return str(v or "").strip()


def canonical_subject(v: Any) -> str:
    s = normalize_text(v).lower()
    return s


def is_target_subject(v: Any) -> bool:
    return canonical_subject(v) in TARGET_SUBJECTS


def clean_supplier_article(v: Any) -> str:
    s = normalize_text(v).upper()
    s = s.replace(" ", "")
    return s


def product_code_from_article(v: Any) -> str:
    s = clean_supplier_article(v)
    if not s:
        return ""
    s = re.sub(r"^PT(?=\d)", "", s)
    root = s.split("/")[0]
    m = re.search(r"(\d+)", root)
    return m.group(1) if m else root


def safe_date_series(series: pd.Series) -> pd.Series:
    s = pd.to_datetime(series, errors="coerce")
    return s.dt.date


def week_start(d: date) -> date:
    return d - timedelta(days=d.weekday())


def week_end(d: date) -> date:
    return week_start(d) + timedelta(days=6)


def iso_week_label(d: date) -> str:
    y, w, _ = d.isocalendar()
    return f"{y}-W{w:02d}"


def month_label(d: date) -> str:
    return d.strftime("%Y-%m")


def month_start(d: date) -> date:
    return d.replace(day=1)


def next_month_start(d: date) -> date:
    if d.month == 12:
        return date(d.year + 1, 1, 1)
    return date(d.year, d.month + 1, 1)


def month_days(d: date) -> int:
    return (next_month_start(month_start(d)) - month_start(d)).days


def russian_weekday_header(day: date) -> str:
    names = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    return f"{names[day.weekday()]} {day.strftime('%d.%m')}"


def money_fmt(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return round(float(value), 0)


def parse_period_from_name(name: str) -> Tuple[Optional[date], Optional[date]]:
    m = re.search(r"__(\d{2}\.\d{2}\.\d{4})-(\d{2}\.\d{2}\.\d{4})__", name)
    if not m:
        return None, None
    try:
        s = datetime.strptime(m.group(1), "%d.%m.%Y").date()
        e = datetime.strptime(m.group(2), "%d.%m.%Y").date()
        return s, e
    except Exception:
        return None, None


def guess_month_key(start: date, end: date) -> Optional[str]:
    if start.day != 1:
        return None
    if next_month_start(start) - timedelta(days=1) != end:
        return None
    return start.strftime("%Y-%m")


def find_column(df: pd.DataFrame, candidates: Iterable[str]) -> str:
    cols = {str(c).strip().lower(): c for c in df.columns}
    for cand in candidates:
        key = str(cand).strip().lower()
        if key in cols:
            return cols[key]
    normalized = {re.sub(r"[^a-zа-я0-9]+", "", str(c).lower()): c for c in df.columns}
    for cand in candidates:
        key = re.sub(r"[^a-zа-я0-9]+", "", str(cand).lower())
        if key in normalized:
            return normalized[key]
    return ""


class Provider:
    def read_excel(self, key: str, sheet_name: Any = 0) -> pd.DataFrame:
        raise NotImplementedError

    def read_excel_all(self, key: str) -> Dict[str, pd.DataFrame]:
        raise NotImplementedError

    def list_keys(self, prefix: str) -> List[str]:
        raise NotImplementedError

    def exists(self, key: str) -> bool:
        raise NotImplementedError

    def write_bytes(self, key: str, data: bytes) -> None:
        raise NotImplementedError


class S3Provider(Provider):
    def __init__(self, access_key: str, secret_key: str, bucket: str):
        self.bucket = bucket
        self.s3 = boto3.client(
            "s3",
            endpoint_url="https://storage.yandexcloud.net",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="ru-central1",
            config=BotoConfig(signature_version="s3v4", read_timeout=300, connect_timeout=60),
        )

    def read_bytes(self, key: str) -> bytes:
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        return obj["Body"].read()

    def read_excel(self, key: str, sheet_name: Any = 0) -> pd.DataFrame:
        return pd.read_excel(io.BytesIO(self.read_bytes(key)), sheet_name=sheet_name)

    def read_excel_all(self, key: str) -> Dict[str, pd.DataFrame]:
        raw = self.read_bytes(key)
        xls = pd.ExcelFile(io.BytesIO(raw))
        return {sh: pd.read_excel(io.BytesIO(raw), sheet_name=sh) for sh in xls.sheet_names}

    def list_keys(self, prefix: str) -> List[str]:
        out: List[str] = []
        token = None
        while True:
            kwargs = {"Bucket": self.bucket, "Prefix": prefix, "MaxKeys": 1000}
            if token:
                kwargs["ContinuationToken"] = token
            resp = self.s3.list_objects_v2(**kwargs)
            out.extend(x["Key"] for x in resp.get("Contents", []))
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        return out

    def exists(self, key: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False

    def write_bytes(self, key: str, data: bytes) -> None:
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=data)


class LocalProvider(Provider):
    def __init__(self, root: str):
        self.root = Path(root)

    def _resolve(self, key: str) -> Path:
        p = self.root / key
        if p.exists():
            return p
        return Path(key)

    def read_excel(self, key: str, sheet_name: Any = 0) -> pd.DataFrame:
        return pd.read_excel(self._resolve(key), sheet_name=sheet_name)

    def read_excel_all(self, key: str) -> Dict[str, pd.DataFrame]:
        path = self._resolve(key)
        xls = pd.ExcelFile(path)
        return {sh: pd.read_excel(path, sheet_name=sh) for sh in xls.sheet_names}

    def list_keys(self, prefix: str) -> List[str]:
        base = self.root / prefix
        if base.is_dir():
            return sorted(str(p.relative_to(self.root)).replace("\\", "/") for p in base.glob("*.xlsx"))
        parent = base.parent if base.suffix else base
        if parent.is_dir():
            return sorted(str(p.relative_to(self.root)).replace("\\", "/") for p in parent.glob("*.xlsx"))
        return []

    def exists(self, key: str) -> bool:
        return self._resolve(key).exists()

    def write_bytes(self, key: str, data: bytes) -> None:
        path = self._resolve(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)


@dataclass
class DataBundle:
    orders: pd.DataFrame
    funnel: pd.DataFrame
    ads: pd.DataFrame
    economics: pd.DataFrame
    abc_weekly: pd.DataFrame
    abc_monthly: pd.DataFrame
    plan: pd.DataFrame
    dictionary: pd.DataFrame
    buyout90: pd.DataFrame
    source_paths: pd.DataFrame


class Loader:
    def __init__(self, provider: Provider):
        self.provider = provider
        self.paths: List[Dict[str, Any]] = []

    def _remember(self, source_type: str, key: str, extra: str = "") -> None:
        self.paths.append({"Тип": source_type, "Путь": key, "Комментарий": extra})

    def load_orders(self) -> pd.DataFrame:
        log("Loading orders")
        keys = [k for k in self.provider.list_keys(WEEKLY_ORDERS_PREFIX) if k.lower().endswith(".xlsx")]
        frames = []
        for key in keys:
            try:
                sheets = self.provider.read_excel_all(key)
                df = next(iter(sheets.values())).copy()
                if df.empty:
                    continue
                df = df.rename(columns={
                    "supplierArticle": "supplier_article",
                    "nmId": "nm_id",
                    "category": "category_name",
                    "subject": "subject",
                    "date": "day",
                })
                if "day" not in df.columns:
                    dc = find_column(df, ["Дата заказа", "Дата", "day", "date"])
                    if dc:
                        df["day"] = df[dc]
                df["day"] = safe_date_series(df["day"])
                if "supplier_article" not in df.columns or "nm_id" not in df.columns:
                    continue
                df["supplier_article"] = df["supplier_article"].map(clean_supplier_article)
                df = df[~df["supplier_article"].isin(EXCLUDED_ARTICLES)].copy()
                df["nm_id"] = pd.to_numeric(df["nm_id"], errors="coerce")
                df["subject_norm"] = df.get("subject", "").map(canonical_subject)
                if "isCancel" in df.columns:
                    df["isCancel"] = df["isCancel"].fillna(False).astype(bool)
                else:
                    df["isCancel"] = False
                df["priceWithDisc"] = pd.to_numeric(df.get("priceWithDisc", 0), errors="coerce").fillna(0.0)
                df["finishedPrice"] = pd.to_numeric(df.get("finishedPrice", 0), errors="coerce").fillna(0.0)
                df["code"] = df["supplier_article"].map(product_code_from_article)
                frames.append(df[["day", "supplier_article", "nm_id", "subject", "subject_norm", "priceWithDisc", "finishedPrice", "isCancel", "code"]].copy())
                self._remember("orders", key)
            except Exception as e:
                log(f"WARN: orders read error {key}: {e}")
        out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["day", "supplier_article", "nm_id", "subject", "subject_norm", "priceWithDisc", "finishedPrice", "isCancel", "code"])
        out = out[out["day"].notna()].copy()
        out = out[out["nm_id"].notna()].copy()
        out["nm_id"] = out["nm_id"].astype("int64")
        out = out[out["subject_norm"].isin(TARGET_SUBJECTS.keys())].copy()
        if not out.empty:
            log(f"Orders rows loaded: {len(out):,}; date range {out['day'].min()} .. {out['day'].max()}")
        else:
            log("Orders rows loaded: 0")
        return out

    def load_funnel(self) -> pd.DataFrame:
        log("Loading funnel")
        if not self.provider.exists(FUNNEL_KEY):
            log("Funnel rows loaded: 0")
            return pd.DataFrame()
        df = self.provider.read_excel(FUNNEL_KEY).copy()
        self._remember("funnel", FUNNEL_KEY)
        df = df.rename(columns={"nmID": "nm_id", "dt": "day"})
        if "day" not in df.columns:
            dc = find_column(df, ["day", "date", "Дата"])
            if dc:
                df["day"] = df[dc]
        df["day"] = safe_date_series(df["day"])
        nm_col = find_column(df, ["nm_id", "nmid", "Артикул WB"])
        if nm_col and nm_col != "nm_id":
            df["nm_id"] = df[nm_col]
        df["nm_id"] = pd.to_numeric(df.get("nm_id"), errors="coerce")
        orders_col = find_column(df, ["ordersCount", "Заказы, шт", "Заказы"])
        buyouts_col = find_column(df, ["buyoutsCount", "Выкупы, шт"])
        buyout_pct_col = find_column(df, ["buyoutPercent", "Процент выкупа", "% выкупа"])
        if orders_col:
            df["orders_cnt"] = pd.to_numeric(df[orders_col], errors="coerce").fillna(0.0)
        else:
            df["orders_cnt"] = 0.0
        if buyouts_col:
            df["buyouts_cnt"] = pd.to_numeric(df[buyouts_col], errors="coerce").fillna(0.0)
        else:
            df["buyouts_cnt"] = 0.0
        if buyout_pct_col:
            pct = pd.to_numeric(df[buyout_pct_col], errors="coerce").fillna(0.0)
            df["buyout_rate_src"] = pct.where(pct <= 1, pct / 100.0)
        else:
            df["buyout_rate_src"] = 0.0
        df = df[df["day"].notna() & df["nm_id"].notna()].copy()
        df["nm_id"] = df["nm_id"].astype("int64")
        if not df.empty:
            log(f"Funnel rows loaded: {len(df):,}; date range {df['day'].min()} .. {df['day'].max()}")
        else:
            log("Funnel rows loaded: 0")
        return df[["day", "nm_id", "orders_cnt", "buyouts_cnt", "buyout_rate_src"]].copy()

    def load_ads(self) -> pd.DataFrame:
        log("Loading ads")
        keys = [k for k in self.provider.list_keys(ADS_WEEKLY_PREFIX) if k.lower().endswith(".xlsx")]
        frames = []
        total_spend = 0.0
        for key in keys:
            try:
                sheets = self.provider.read_excel_all(key)
                if "Статистика_Ежедневно" not in sheets:
                    continue
                df = sheets["Статистика_Ежедневно"].copy()
                if df.empty:
                    continue
                df = df.rename(columns={"Дата": "day", "Артикул WB": "nm_id", "Расход": "ad_spend", "Название предмета": "subject"})
                df["day"] = safe_date_series(df["day"])
                df["nm_id"] = pd.to_numeric(df["nm_id"], errors="coerce")
                df["ad_spend"] = pd.to_numeric(df["ad_spend"], errors="coerce").fillna(0.0)
                df["subject_norm"] = df.get("subject", "").map(canonical_subject)
                frames.append(df[["day", "nm_id", "ad_spend", "subject", "subject_norm"]].copy())
                total_spend += float(df["ad_spend"].sum())
                self._remember("ads", key, "Статистика_Ежедневно")
            except Exception as e:
                log(f"WARN: ads read error {key}: {e}")
        out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["day", "nm_id", "ad_spend", "subject", "subject_norm"])
        out = out[out["day"].notna() & out["nm_id"].notna()].copy()
        out["nm_id"] = out["nm_id"].astype("int64")
        if not out.empty:
            log(f"Ads rows loaded: {len(out):,}; date range {out['day'].min()} .. {out['day'].max()}; spend sum {out['ad_spend'].sum():,.0f}")
        else:
            log("Ads rows loaded: 0")
        return out

    def load_economics(self) -> pd.DataFrame:
        log("Loading economics")
        if not self.provider.exists(ECON_KEY):
            return pd.DataFrame()
        df = self.provider.read_excel(ECON_KEY, sheet_name="Юнит экономика").copy()
        self._remember("economics", ECON_KEY, "Юнит экономика")
        df = df.rename(columns={
            "Неделя": "week",
            "Артикул WB": "nm_id",
            "Артикул продавца": "supplier_article",
            "Предмет": "subject",
            "Комиссия WB, %": "commission_pct",
            "Эквайринг, %": "acquiring_pct",
            "Логистика прямая, руб/ед": "logistics_direct",
            "Логистика обратная, руб/ед": "logistics_reverse",
            "Хранение, руб/ед": "storage_unit",
            "Прочие расходы, руб/ед": "other_unit",
            "Себестоимость, руб": "cost_unit",
        })
        df["supplier_article"] = df["supplier_article"].map(clean_supplier_article)
        df = df[~df["supplier_article"].isin(EXCLUDED_ARTICLES)].copy()
        df["nm_id"] = pd.to_numeric(df["nm_id"], errors="coerce")
        df["subject_norm"] = df.get("subject", "").map(canonical_subject)
        df = df[df["subject_norm"].isin(TARGET_SUBJECTS.keys())].copy()
        for c in ["commission_pct", "acquiring_pct", "logistics_direct", "logistics_reverse", "storage_unit", "other_unit", "cost_unit"]:
            df[c] = pd.to_numeric(df.get(c, 0), errors="coerce").fillna(0.0)
        df["code"] = df["supplier_article"].map(product_code_from_article)
        weeks = ", ".join(sorted(df["week"].dropna().astype(str).unique().tolist())[:20])
        log(f"Economics rows loaded: {len(df):,}; weeks {weeks}")
        return df[["week", "nm_id", "supplier_article", "subject", "subject_norm", "commission_pct", "acquiring_pct", "logistics_direct", "logistics_reverse", "storage_unit", "other_unit", "cost_unit", "code"]].copy()

    def load_abc(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        log("Loading ABC")
        keys = [k for k in self.provider.list_keys(ABC_PREFIX) if Path(k).name.lower().endswith('.xlsx') and 'wb_abc_report_goods__' in Path(k).name]
        weekly_frames = []
        monthly_frames = []
        weekly_labels = set()
        monthly_labels = set()
        for key in keys:
            start, end = parse_period_from_name(Path(key).name)
            if start is None or end is None:
                continue
            try:
                df = self.provider.read_excel(key).copy()
            except Exception as e:
                log(f"WARN: abc read error {key}: {e}")
                continue
            df = df.rename(columns={
                "Артикул WB": "nm_id",
                "Артикул продавца": "supplier_article",
                "Предмет": "subject",
                "Валовая прибыль": "gross_profit",
                "НДС": "vat",
            })
            df["supplier_article"] = df.get("supplier_article", "").map(clean_supplier_article)
            df = df[~df["supplier_article"].isin(EXCLUDED_ARTICLES)].copy()
            df["nm_id"] = pd.to_numeric(df.get("nm_id"), errors="coerce")
            df["subject_norm"] = df.get("subject", "").map(canonical_subject)
            df = df[df["subject_norm"].isin(TARGET_SUBJECTS.keys())].copy()
            df["gross_profit"] = pd.to_numeric(df.get("gross_profit", 0), errors="coerce").fillna(0.0)
            df["vat"] = pd.to_numeric(df.get("vat", 0), errors="coerce").fillna(0.0)
            df["gp_minus_vat"] = df["gross_profit"] - df["vat"]
            df["code"] = df["supplier_article"].map(product_code_from_article)
            mkey = guess_month_key(start, end)
            if mkey:
                df["period_name"] = mkey
                monthly_labels.add(mkey)
                monthly_frames.append(df[["period_name", "nm_id", "supplier_article", "subject", "subject_norm", "gp_minus_vat", "code"]].copy())
            else:
                wk = iso_week_label(start)
                df["period_name"] = wk
                weekly_labels.add(wk)
                weekly_frames.append(df[["period_name", "nm_id", "supplier_article", "subject", "subject_norm", "gp_minus_vat", "code"]].copy())
            self._remember("abc", key)
        weekly = pd.concat(weekly_frames, ignore_index=True) if weekly_frames else pd.DataFrame(columns=["period_name", "nm_id", "supplier_article", "subject", "subject_norm", "gp_minus_vat", "code"])
        monthly = pd.concat(monthly_frames, ignore_index=True) if monthly_frames else pd.DataFrame(columns=["period_name", "nm_id", "supplier_article", "subject", "subject_norm", "gp_minus_vat", "code"])
        log(f"ABC weekly rows loaded: {len(weekly):,}; weeks {', '.join(sorted(weekly_labels))}")
        log(f"ABC monthly rows loaded: {len(monthly):,}; months {', '.join(sorted(monthly_labels))}")
        return weekly, monthly

    def load_plan(self) -> pd.DataFrame:
        log("Loading plan")
        if not self.provider.exists(PLAN_KEY):
            return pd.DataFrame(columns=["supplier_article", "code", "subject_norm", "plan_month"])
        try:
            sheets = self.provider.read_excel_all(PLAN_KEY)
        except Exception as e:
            log(f"WARN: plan read error: {e}")
            return pd.DataFrame(columns=["supplier_article", "code", "subject_norm", "plan_month"])
        self._remember("plan", PLAN_KEY)
        frames = []
        for sh_name, df in sheets.items():
            work = df.copy()
            art_col = find_column(work, ["Артикул продавца", "supplier_article", "Артикул", "SKU"])
            code_col = find_column(work, ["Товар", "code"])
            subj_col = find_column(work, ["Предмет", "Категория", "subject"])
            plan_col = find_column(work, ["План ВП", "План Валовая прибыль", "План Валовая прибыль-НДС", "План", "Валовая прибыль", "План ВП-НДС"])
            if not art_col and not code_col:
                continue
            tmp = pd.DataFrame()
            tmp["supplier_article"] = work[art_col].map(clean_supplier_article) if art_col else ""
            tmp["code"] = work[code_col].map(normalize_text) if code_col else tmp["supplier_article"].map(product_code_from_article)
            tmp["subject_norm"] = work[subj_col].map(canonical_subject) if subj_col else ""
            if plan_col:
                tmp["plan_month"] = pd.to_numeric(work[plan_col], errors="coerce").fillna(0.0)
            else:
                tmp["plan_month"] = 0.0
            frames.append(tmp)
        out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["supplier_article", "code", "subject_norm", "plan_month"])
        log(f"Plan rows loaded: {len(out):,}; non-null plan {(out['plan_month'] > 0).sum() if not out.empty else 0}")
        return out

    def build_dictionary(self, orders: pd.DataFrame, economics: pd.DataFrame, abc_weekly: pd.DataFrame, abc_monthly: pd.DataFrame) -> pd.DataFrame:
        parts = []
        for df in [orders, economics, abc_weekly, abc_monthly]:
            if df is None or df.empty:
                continue
            tmp = pd.DataFrame()
            tmp["supplier_article"] = df.get("supplier_article", "").map(clean_supplier_article)
            tmp["nm_id"] = pd.to_numeric(df.get("nm_id"), errors="coerce")
            tmp["subject_norm"] = df.get("subject_norm", "")
            tmp["subject"] = tmp["subject_norm"].map(lambda x: TARGET_SUBJECTS.get(canonical_subject(x), normalize_text(x)))
            tmp["code"] = df.get("code", tmp["supplier_article"].map(product_code_from_article))
            parts.append(tmp)
        dic = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=["supplier_article", "nm_id", "subject_norm", "subject", "code"])
        dic = dic[dic["supplier_article"].astype(str).str.strip() != ""].copy()
        dic = dic[~dic["supplier_article"].isin(EXCLUDED_ARTICLES)].copy()
        dic["code"] = dic["code"].astype(str).replace("", pd.NA)
        dic.loc[dic["code"].isna(), "code"] = dic.loc[dic["code"].isna(), "supplier_article"].map(product_code_from_article)
        dic["category"] = dic["subject_norm"].map(lambda x: TARGET_SUBJECTS.get(canonical_subject(x), normalize_text(x)))
        dic["title"] = ""
        dic = dic.drop_duplicates(subset=["supplier_article", "nm_id"], keep="first")
        return dic[["category", "code", "supplier_article", "nm_id", "title", "subject_norm"]].copy()

    def build_buyout90(self, funnel: pd.DataFrame, dictionary: pd.DataFrame) -> pd.DataFrame:
        if funnel.empty:
            return pd.DataFrame(columns=["nm_id", "buyout90", "subject_norm"])
        max_day = funnel["day"].max()
        start_day = max_day - timedelta(days=89)
        f = funnel[(funnel["day"] >= start_day) & (funnel["day"] <= max_day)].copy()
        grp = f.groupby("nm_id", as_index=False).agg(
            orders_cnt=("orders_cnt", "sum"),
            buyouts_cnt=("buyouts_cnt", "sum"),
            buyout_rate_src=("buyout_rate_src", "mean"),
        )
        grp["buyout90"] = grp.apply(lambda r: (r["buyouts_cnt"] / r["orders_cnt"]) if r["orders_cnt"] > 0 and r["buyouts_cnt"] > 0 else r["buyout_rate_src"], axis=1)
        grp["buyout90"] = grp["buyout90"].fillna(0).clip(lower=0, upper=1)
        grp = grp.merge(dictionary[["nm_id", "subject_norm"]].drop_duplicates(), on="nm_id", how="left")
        subj = grp.groupby("subject_norm", as_index=False)["buyout90"].median().rename(columns={"buyout90": "subject_buyout90"})
        grp = grp.merge(subj, on="subject_norm", how="left")
        grp["buyout90"] = grp["buyout90"].where(grp["buyout90"] > 0, grp["subject_buyout90"])
        fixed = {
            "кисти косметические": 0.85,
            "косметические карандаши": 0.95,
            "помады": 0.93,
            "блески": 0.90,
        }
        grp["buyout90"] = grp.apply(lambda r: r["buyout90"] if safe_float(r["buyout90"]) > 0 else fixed.get(canonical_subject(r["subject_norm"]), 0.85), axis=1)
        log(f"Buyout90 rows: {len(grp):,}; non-null ratios {(grp['buyout90'] > 0).sum()}")
        return grp[["nm_id", "buyout90", "subject_norm"]].copy()

    def load_all(self) -> DataBundle:
        orders = self.load_orders()
        funnel = self.load_funnel()
        ads = self.load_ads()
        economics = self.load_economics()
        abc_weekly, abc_monthly = self.load_abc()
        plan = self.load_plan()
        dictionary = self.build_dictionary(orders, economics, abc_weekly, abc_monthly)
        buyout90 = self.build_buyout90(funnel, dictionary)
        return DataBundle(
            orders=orders,
            funnel=funnel,
            ads=ads,
            economics=economics,
            abc_weekly=abc_weekly,
            abc_monthly=abc_monthly,
            plan=plan,
            dictionary=dictionary,
            buyout90=buyout90,
            source_paths=pd.DataFrame(self.paths),
        )


class Builder:
    def __init__(self, data: DataBundle):
        self.data = data
        self.diagnostics: List[Dict[str, Any]] = []
        self.latest_order_day = self.data.orders["day"].max() if not self.data.orders.empty else date.today()
        self.current_week_start = week_start(self.latest_order_day)
        self.current_week_days = [self.current_week_start + timedelta(days=i) for i in range(7)]
        self.current_week_end = self.current_week_days[-1]
        self.current_month = month_start(self.latest_order_day)
        self.dictionary = self.data.dictionary.copy()
        self.article_to_subject = self.dictionary.drop_duplicates("supplier_article").set_index("supplier_article")["subject_norm"].to_dict() if not self.dictionary.empty else {}
        self.article_to_code = self.dictionary.drop_duplicates("supplier_article").set_index("supplier_article")["code"].to_dict() if not self.dictionary.empty else {}
        self.nm_to_article = self.dictionary.dropna(subset=["nm_id"]).drop_duplicates("nm_id").set_index("nm_id")["supplier_article"].to_dict() if not self.dictionary.empty else {}
        self.nm_to_subject = self.dictionary.dropna(subset=["nm_id"]).drop_duplicates("nm_id").set_index("nm_id")["subject_norm"].to_dict() if not self.dictionary.empty else {}
        self.comm_subject_week, self.comm_subject_latest = self.build_commission_maps()
        self.econ_latest_by_article = self.data.economics.sort_values("week").drop_duplicates("supplier_article", keep="last") if not self.data.economics.empty else pd.DataFrame()

    def build_commission_maps(self) -> Tuple[Dict[Tuple[str, str], float], Dict[str, float]]:
        econ = self.data.economics.copy()
        if econ.empty:
            return {}, {}
        econ = econ[econ["commission_pct"] > 0].copy()
        by_week = {}
        by_latest = {}
        if not econ.empty:
            grp = econ.groupby(["subject_norm", "week"], as_index=False)["commission_pct"].median()
            by_week = {(r["subject_norm"], str(r["week"])): safe_float(r["commission_pct"]) for _, r in grp.iterrows()}
            latest = econ.sort_values("week").groupby("subject_norm", as_index=False)["commission_pct"].last()
            by_latest = {r["subject_norm"]: safe_float(r["commission_pct"]) for _, r in latest.iterrows()}
        return by_week, by_latest

    def attach_dictionary(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty or self.dictionary.empty:
            return df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
        out = df.copy()
        if "supplier_article" not in out.columns:
            out["supplier_article"] = out.get("supplier_article", "")
        if "nm_id" not in out.columns:
            out["nm_id"] = pd.to_numeric(out.get("nm_id"), errors="coerce")
        out["supplier_article"] = out["supplier_article"].map(clean_supplier_article)
        out["nm_id"] = pd.to_numeric(out["nm_id"], errors="coerce")
        dic = self.dictionary.copy()

        # first by supplier_article
        out = out.merge(dic[["supplier_article", "category", "code", "title", "subject_norm"]].drop_duplicates(), on="supplier_article", how="left", suffixes=("", "_sa"))
        # then by nm_id only for missing fields
        nm_dic = dic[["nm_id", "category", "code", "title", "subject_norm"]].dropna(subset=["nm_id"]).drop_duplicates("nm_id")
        out = out.merge(nm_dic, on="nm_id", how="left", suffixes=("", "_nm"))
        for c in ["category", "code", "title", "subject_norm"]:
            if c not in out.columns:
                out[c] = ""
            sa_col = f"{c}_sa"
            nm_col = f"{c}_nm"
            if sa_col in out.columns:
                mask = out[c].isna() | (out[c].astype(str).str.strip() == "")
                out.loc[mask, c] = out.loc[mask, sa_col]
            if nm_col in out.columns:
                mask = out[c].isna() | (out[c].astype(str).str.strip() == "")
                out.loc[mask, c] = out.loc[mask, nm_col]
        drop_cols = [c for c in out.columns if c.endswith("_sa") or c.endswith("_nm")]
        out = out.drop(columns=drop_cols, errors="ignore")
        out["category"] = out["category"].map(lambda x: TARGET_SUBJECTS.get(canonical_subject(x), x))
        return out

    def build_ads_daily(self) -> pd.DataFrame:
        ads = self.data.ads.copy()
        if ads.empty:
            return pd.DataFrame(columns=["day", "nm_id", "ad_spend"])
        ads = ads.groupby(["day", "nm_id"], as_index=False)["ad_spend"].sum()
        ads = self.attach_dictionary(ads)
        return ads

    def pick_econ_for_article_week(self, supplier_article: str, subject_norm: str, week_label: str) -> Dict[str, float]:
        econ = self.data.economics
        art = clean_supplier_article(supplier_article)
        exact = econ[(econ["supplier_article"] == art) & (econ["week"].astype(str) == str(week_label))]
        row = exact.iloc[-1] if not exact.empty else None
        latest = econ[econ["supplier_article"] == art].sort_values("week")
        latest_row = latest.iloc[-1] if not latest.empty else None
        def val(col: str, default: float = 0.0) -> float:
            if row is not None and col in row.index and safe_float(row[col]) > 0:
                return safe_float(row[col])
            if latest_row is not None and col in latest_row.index and safe_float(latest_row[col]) > 0:
                return safe_float(latest_row[col])
            if col == "commission_pct":
                wk = self.comm_subject_week.get((subject_norm, str(week_label)), 0.0)
                if wk > 0:
                    return wk
                return self.comm_subject_latest.get(subject_norm, default)
            if col == "acquiring_pct":
                sub = econ[(econ["subject_norm"] == subject_norm) & (econ["week"].astype(str) == str(week_label))]
                if not sub.empty and (sub["acquiring_pct"] > 0).any():
                    return float(sub.loc[sub["acquiring_pct"] > 0, "acquiring_pct"].median())
                sub = econ[(econ["subject_norm"] == subject_norm) & (econ["acquiring_pct"] > 0)]
                if not sub.empty:
                    return float(sub.sort_values("week").iloc[-1]["acquiring_pct"])
            if col in {"logistics_direct", "logistics_reverse", "storage_unit", "other_unit", "cost_unit"}:
                sub = econ[(econ["subject_norm"] == subject_norm) & (econ["week"].astype(str) == str(week_label))]
                if not sub.empty and (pd.to_numeric(sub[col], errors="coerce").fillna(0) > 0).any():
                    return float(pd.to_numeric(sub[col], errors="coerce").fillna(0).median())
                sub = econ[(econ["subject_norm"] == subject_norm)]
                if not sub.empty:
                    vals = pd.to_numeric(sub[col], errors="coerce").fillna(0)
                    if (vals > 0).any():
                        return float(vals[vals > 0].median())
            return default
        return {
            "commission_pct": val("commission_pct", 0.0),
            "acquiring_pct": val("acquiring_pct", 0.0),
            "logistics_direct": val("logistics_direct", 0.0),
            "logistics_reverse": val("logistics_reverse", 0.0),
            "storage_unit": val("storage_unit", 0.0),
            "other_unit": val("other_unit", 0.0),
            "cost_unit": val("cost_unit", 0.0),
        }

    def build_daily_formula(self) -> pd.DataFrame:
        orders = self.data.orders.copy()
        if orders.empty:
            return pd.DataFrame()
        orders = orders[~orders["isCancel"]].copy()
        daily = orders.groupby(["day", "supplier_article", "nm_id"], as_index=False).agg(
            orders_count=("nm_id", "count"),
            ordered_price_with_disc=("priceWithDisc", "sum"),
            ordered_finished_price=("finishedPrice", "sum"),
        )
        daily = self.attach_dictionary(daily)
        daily = daily[daily["category"].isin(TARGET_SUBJECTS.values())].copy()

        buyout = self.data.buyout90[["nm_id", "buyout90"]].drop_duplicates() if not self.data.buyout90.empty else pd.DataFrame(columns=["nm_id", "buyout90"])
        daily = daily.merge(buyout, on="nm_id", how="left")
        daily["buyout90"] = daily.apply(lambda r: safe_float(r["buyout90"]) if safe_float(r["buyout90"]) > 0 else {
            "кисти косметические": 0.85,
            "косметические карандаши": 0.95,
            "помады": 0.93,
            "блески": 0.90,
        }.get(canonical_subject(r["subject_norm"]), 0.85), axis=1)

        ads = self.build_ads_daily()
        daily = daily.merge(ads[["day", "nm_id", "ad_spend"]], on=["day", "nm_id"], how="left")
        daily["ad_spend"] = pd.to_numeric(daily.get("ad_spend"), errors="coerce").fillna(0.0)

        econ_rows = []
        for _, r in daily[["day", "supplier_article", "subject_norm"]].drop_duplicates().iterrows():
            wk = iso_week_label(r["day"])
            picked = self.pick_econ_for_article_week(r["supplier_article"], r["subject_norm"], wk)
            picked.update({"day": r["day"], "supplier_article": r["supplier_article"]})
            econ_rows.append(picked)
        econ_pick = pd.DataFrame(econ_rows)
        daily = daily.merge(econ_pick, on=["day", "supplier_article"], how="left")
        for c in ["commission_pct", "acquiring_pct", "logistics_direct", "logistics_reverse", "storage_unit", "other_unit", "cost_unit"]:
            daily[c] = pd.to_numeric(daily.get(c), errors="coerce").fillna(0.0)

        daily["buyout_orders"] = daily["orders_count"] * daily["buyout90"]
        daily["revenue_realized"] = daily["ordered_price_with_disc"] * daily["buyout90"]
        daily["commission_amount"] = daily["revenue_realized"] * daily["commission_pct"] / 100.0
        daily["acquiring_amount"] = daily["revenue_realized"] * daily["acquiring_pct"] / 100.0
        daily["logistics_direct_amount"] = daily["buyout_orders"] * daily["logistics_direct"]
        daily["logistics_reverse_amount"] = daily["buyout_orders"] * daily["logistics_reverse"]
        daily["storage_amount"] = daily["buyout_orders"] * daily["storage_unit"]
        daily["other_amount"] = daily["buyout_orders"] * daily["other_unit"]
        daily["cost_amount"] = daily["buyout_orders"] * daily["cost_unit"]
        daily["nds_amount"] = daily["ordered_finished_price"] * daily["buyout90"] * 7.0 / 107.0
        daily["gp_minus_vat_raw"] = (
            daily["revenue_realized"]
            - daily["commission_amount"]
            - daily["acquiring_amount"]
            - daily["logistics_direct_amount"]
            - daily["logistics_reverse_amount"]
            - daily["storage_amount"]
            - daily["other_amount"]
            - daily["cost_amount"]
            - daily["ad_spend"]
            - daily["nds_amount"]
        )
        negative = daily[daily["gp_minus_vat_raw"] < 0].copy()
        for _, r in negative.iterrows():
            self.diagnostics.append({
                "Тип": "negative_gp_minus_vat",
                "Дата": r["day"],
                "Артикул": r["supplier_article"],
                "nm_id": int(r["nm_id"]),
                "Значение": float(r["gp_minus_vat_raw"]),
            })
        daily["gp_minus_vat"] = daily["gp_minus_vat_raw"].clip(lower=0)
        return daily

    def build_weekly_facts(self) -> pd.DataFrame:
        abc = self.attach_dictionary(self.data.abc_weekly)
        if abc.empty:
            return abc
        abc = abc[abc["category"].isin(TARGET_SUBJECTS.values())].copy()
        return abc[["period_name", "category", "code", "supplier_article", "nm_id", "gp_minus_vat"]].copy()

    def build_monthly_facts(self) -> pd.DataFrame:
        monthly = self.attach_dictionary(self.data.abc_monthly)
        if monthly.empty:
            monthly = pd.DataFrame(columns=["period_name", "category", "code", "supplier_article", "nm_id", "gp_minus_vat"])
        monthly = monthly[monthly["category"].isin(TARGET_SUBJECTS.values())].copy()

        # current month from weekly ABC + current week forecast
        current_month_label = month_label(self.latest_order_day)
        prev1 = month_label(month_start(self.latest_order_day) - timedelta(days=1))
        prev2 = month_label(month_start(month_start(self.latest_order_day) - timedelta(days=1)) - timedelta(days=1))

        if current_month_label not in set(monthly["period_name"].astype(str)):
            current = self.build_current_month_fact_from_components(current_month_label)
            if not current.empty:
                monthly = pd.concat([monthly, current], ignore_index=True)
        keep = {current_month_label, prev1, prev2}
        monthly = monthly[monthly["period_name"].astype(str).isin(keep)].copy()
        return monthly

    def build_current_month_fact_from_components(self, current_month_label: str) -> pd.DataFrame:
        parts = []
        # full weeks in current month from ABC weekly
        weekly = self.build_weekly_facts()
        if not weekly.empty:
            for wk in sorted(weekly["period_name"].astype(str).unique()):
                try:
                    _, wnum = wk.split("-W")
                    # reconstruct week start from label using latest year
                    y = int(wk.split("-W")[0])
                    d = datetime.strptime(f"{y} {int(wnum)} 1", "%G %V %u").date()
                except Exception:
                    continue
                ws = week_start(d)
                we = ws + timedelta(days=6)
                if ws.month == self.current_month.month and we < self.current_week_start:
                    sub = weekly[weekly["period_name"].astype(str) == wk].copy()
                    sub["period_name"] = current_month_label
                    parts.append(sub)
        daily = self.build_daily_formula()
        if not daily.empty:
            sub = daily[(daily["day"] >= self.current_month) & (daily["day"] <= self.latest_order_day)].copy()
            if not sub.empty:
                g = sub.groupby(["category", "code", "supplier_article", "nm_id"], as_index=False)["gp_minus_vat"].sum()
                g["period_name"] = current_month_label
                parts.append(g[["period_name", "category", "code", "supplier_article", "nm_id", "gp_minus_vat"]])
        if not parts:
            return pd.DataFrame(columns=["period_name", "category", "code", "supplier_article", "nm_id", "gp_minus_vat"])
        out = pd.concat(parts, ignore_index=True)
        return out.groupby(["period_name", "category", "code", "supplier_article", "nm_id"], as_index=False)["gp_minus_vat"].sum()

    def build_plan_maps(self) -> Tuple[Dict[str, float], Dict[str, float], Dict[str, float]]:
        plan = self.data.plan.copy()
        if plan.empty:
            return {}, {}, {}
        plan["supplier_article"] = plan["supplier_article"].map(clean_supplier_article)
        plan["code"] = plan["code"].astype(str).replace("", pd.NA)
        plan.loc[plan["code"].isna(), "code"] = plan.loc[plan["code"].isna(), "supplier_article"].map(product_code_from_article)
        if "subject_norm" not in plan.columns:
            plan["subject_norm"] = plan["supplier_article"].map(lambda x: self.article_to_subject.get(x, ""))
        article_map = plan.groupby("supplier_article", as_index=False)["plan_month"].sum()
        code_map = plan.groupby("code", as_index=False)["plan_month"].sum()
        subj_map = plan.groupby("subject_norm", as_index=False)["plan_month"].sum()
        return (
            {r["supplier_article"]: float(r["plan_month"]) for _, r in article_map.iterrows()},
            {str(r["code"]): float(r["plan_month"]) for _, r in code_map.iterrows()},
            {str(r["subject_norm"]): float(r["plan_month"]) for _, r in subj_map.iterrows()},
        )

    def hierarchy_rows(self, fact_df: pd.DataFrame, value_cols: List[str], row_label_col: str, plan_value_map: Dict[str, float], row_type: str) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if fact_df.empty:
            return rows
        for category in [TARGET_SUBJECTS[k] for k in ["кисти косметические", "помады", "блески", "косметические карандаши"]]:
            cat_df = fact_df[fact_df["category"] == category].copy()
            if cat_df.empty:
                cat_vals = {c: 0.0 for c in value_cols}
            else:
                cat_vals = {c: float(cat_df[c].sum()) for c in value_cols}
            cat_key = canonical_subject(category)
            cat_plan = plan_value_map.get(cat_key)
            rows.append({"level": 0, "kind": "category", "label": category, **cat_vals, "plan": cat_plan})
            if cat_df.empty:
                continue
            for code in sorted(cat_df["code"].dropna().astype(str).unique().tolist(), key=lambda x: (not x.isdigit(), x)):
                prod_df = cat_df[cat_df["code"].astype(str) == str(code)].copy()
                prod_vals = {c: float(prod_df[c].sum()) for c in value_cols}
                prod_plan = plan_value_map.get(str(code))
                rows.append({"level": 1, "kind": "product", "label": str(code), **prod_vals, "plan": prod_plan})
                for art in sorted(prod_df["supplier_article"].dropna().astype(str).unique().tolist()):
                    art_df = prod_df[prod_df["supplier_article"] == art].copy()
                    art_vals = {c: float(art_df[c].sum()) for c in value_cols}
                    art_plan = plan_value_map.get(art)
                    rows.append({"level": 2, "kind": "article", "label": art, **art_vals, "plan": art_plan})
        total_vals = {c: float(fact_df[c].sum()) if not fact_df.empty else 0.0 for c in value_cols}
        total_plan = sum(v for v in plan_value_map.values()) if plan_value_map else None
        rows.append({"level": 0, "kind": "grand_total", "label": "Итого по всем 4 категориям", **total_vals, "plan": total_plan})
        # fallback missing plan => fact
        for r in rows:
            if r["kind"] == "grand_total":
                continue
            fact_sum = sum(float(r.get(c, 0.0) or 0.0) for c in value_cols)
            if r.get("plan") in (None, 0, 0.0, ""):
                r["plan"] = fact_sum
        gt = rows[-1]
        if gt.get("plan") in (None, 0, 0.0, ""):
            gt["plan"] = sum(float(gt.get(c, 0.0) or 0.0) for c in value_cols)
        return rows

    def build_current_week_matrix(self) -> Tuple[List[str], List[Dict[str, Any]]]:
        daily = self.build_daily_formula()
        current = daily[(daily["day"] >= self.current_week_start) & (daily["day"] <= self.current_week_end)].copy()
        day_cols = [russian_weekday_header(d) for d in self.current_week_days]
        if current.empty:
            empty_rows = []
            article_plan, code_plan, subj_plan = self.build_plan_maps()
            plan_map = {**subj_plan, **code_plan, **article_plan}
            return day_cols, self.hierarchy_rows(pd.DataFrame(columns=["category", "code", "supplier_article"] + day_cols), day_cols, "label", plan_map, "week")
        current["day_header"] = current["day"].map(russian_weekday_header)
        pivot = current.groupby(["category", "code", "supplier_article", "day_header"], as_index=False)["gp_minus_vat"].sum()
        wide = pivot.pivot_table(index=["category", "code", "supplier_article"], columns="day_header", values="gp_minus_vat", aggfunc="sum").reset_index()
        for c in day_cols:
            if c not in wide.columns:
                wide[c] = 0.0
        # future days blank handled in writer, values remain 0 for aggregation
        article_plan, code_plan, subj_plan = self.build_plan_maps()
        days_in_scope = sum(1 for d in self.current_week_days if month_start(d) == self.current_month)
        pr_article = {k: v / month_days(self.latest_order_day) * days_in_scope for k, v in article_plan.items()}
        pr_code = {k: v / month_days(self.latest_order_day) * days_in_scope for k, v in code_plan.items()}
        pr_subj = {k: v / month_days(self.latest_order_day) * days_in_scope for k, v in subj_plan.items()}
        plan_map = {}
        plan_map.update(pr_subj)
        plan_map.update(pr_code)
        plan_map.update(pr_article)
        rows = self.hierarchy_rows(wide, day_cols, "label", plan_map, "week")
        return day_cols, rows

    def build_past_weeks_matrix(self) -> Tuple[List[str], List[Dict[str, Any]]]:
        weekly = self.build_weekly_facts().copy()
        labels = sorted(weekly["period_name"].astype(str).unique().tolist()) if not weekly.empty else []
        # previous four full weeks before current week
        cur_label = iso_week_label(self.current_week_start)
        labels = [x for x in labels if x < cur_label]
        labels = labels[-4:]
        if not labels:
            labels = []
        if weekly.empty or not labels:
            article_plan, code_plan, subj_plan = self.build_plan_maps()
            plan_map = {**subj_plan, **code_plan, **article_plan}
            return labels, self.hierarchy_rows(pd.DataFrame(columns=["category", "code", "supplier_article"] + labels), labels, "label", plan_map, "week")
        sub = weekly[weekly["period_name"].astype(str).isin(labels)].copy()
        pivot = sub.pivot_table(index=["category", "code", "supplier_article"], columns="period_name", values="gp_minus_vat", aggfunc="sum").reset_index()
        for c in labels:
            if c not in pivot.columns:
                pivot[c] = 0.0
        article_plan, code_plan, subj_plan = self.build_plan_maps()
        plan_map = {**subj_plan, **code_plan, **article_plan}
        rows = self.hierarchy_rows(pivot, labels, "label", plan_map, "weeks")
        return labels, rows

    def build_months_matrix(self) -> Tuple[List[str], List[Dict[str, Any]]]:
        monthly = self.build_monthly_facts().copy()
        labels = [month_label(self.latest_order_day)]
        prev1 = month_label(month_start(self.latest_order_day) - timedelta(days=1))
        prev2 = month_label(month_start(month_start(self.latest_order_day) - timedelta(days=1)) - timedelta(days=1))
        labels.extend([prev1, prev2])
        labels = [x for x in labels if x]
        if monthly.empty:
            article_plan, code_plan, subj_plan = self.build_plan_maps()
            plan_map = {**subj_plan, **code_plan, **article_plan}
            return labels, self.hierarchy_rows(pd.DataFrame(columns=["category", "code", "supplier_article"] + labels), labels, "label", plan_map, "months")
        sub = monthly[monthly["period_name"].astype(str).isin(labels)].copy()
        pivot = sub.pivot_table(index=["category", "code", "supplier_article"], columns="period_name", values="gp_minus_vat", aggfunc="sum").reset_index()
        for c in labels:
            if c not in pivot.columns:
                pivot[c] = 0.0
        article_plan, code_plan, subj_plan = self.build_plan_maps()
        plan_map = {**subj_plan, **code_plan, **article_plan}
        rows = self.hierarchy_rows(pivot, labels, "label", plan_map, "months")
        return labels, rows

    def build_blocks(self) -> Dict[str, Any]:
        log("Building stage 1")
        day_cols, current_rows = self.build_current_week_matrix()
        week_cols, weekly_rows = self.build_past_weeks_matrix()
        month_cols, monthly_rows = self.build_months_matrix()
        return {
            "current_week": {"columns": day_cols, "rows": current_rows},
            "past_weeks": {"columns": week_cols, "rows": weekly_rows},
            "months": {"columns": month_cols, "rows": monthly_rows},
            "daily_formula": self.build_daily_formula(),
            "weekly_facts": self.build_weekly_facts(),
            "monthly_facts": self.build_monthly_facts(),
        }


class ExcelWriter:
    def __init__(self, data: DataBundle, blocks: Dict[str, Any], latest_day: date):
        self.data = data
        self.blocks = blocks
        self.latest_day = latest_day

    def write_main(self, path: str) -> None:
        wb = Workbook()
        ws = wb.active
        ws.title = "Сводка"
        current_row = 1
        current_row = self._write_title(ws, current_row, "Валовая Прибыль-НДС")
        current_row = self._write_block(ws, current_row, "Текущая неделя", self.blocks["current_week"]["columns"], self.blocks["current_week"]["rows"], current_week=True)
        current_row += 2
        current_row = self._write_block(ws, current_row, "Прошлые недели", self.blocks["past_weeks"]["columns"], self.blocks["past_weeks"]["rows"], current_week=False)
        current_row += 2
        self._write_block(ws, current_row, "Последние 3 месяца", self.blocks["months"]["columns"], self.blocks["months"]["rows"], current_week=False)
        ws.freeze_panes = "B3"
        ws.sheet_view.showOutlineSymbols = True
        self._finalize_sheet(ws)
        out = io.BytesIO()
        wb.save(out)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_bytes(out.getvalue())

    def write_technical(self, path: str, diagnostics: List[Dict[str, Any]]) -> None:
        wb = Workbook()
        wb.remove(wb.active)
        sheets = {
            "dictionary": self.data.dictionary,
            "orders_used": self.data.orders,
            "funnel_used": self.data.funnel,
            "ads_used": self.data.ads,
            "economics_used": self.data.economics,
            "abc_weekly_used": self.data.abc_weekly,
            "abc_monthly_used": self.data.abc_monthly,
            "plan_used": self.data.plan,
            "buyout90": self.data.buyout90,
            "daily_formula": self.blocks["daily_formula"],
            "weekly_facts": self.blocks["weekly_facts"],
            "monthly_facts": self.blocks["monthly_facts"],
            "paths": self.data.source_paths,
            "diagnostics": pd.DataFrame(diagnostics),
        }
        for name, df in sheets.items():
            ws = wb.create_sheet(title=name[:31])
            if df is None or df.empty:
                ws["A1"] = "Нет данных"
                continue
            for j, col in enumerate(df.columns, start=1):
                c = ws.cell(row=1, column=j, value=str(col))
                c.fill = HEADER_FILL
                c.font = HEADER_FONT
                c.alignment = Alignment(horizontal="center", vertical="center")
                c.border = BORDER
            for i, row in enumerate(df.itertuples(index=False), start=2):
                for j, val in enumerate(row, start=1):
                    cell = ws.cell(row=i, column=j, value=val)
                    cell.border = BORDER
                    cell.alignment = Alignment(vertical="center")
                    if isinstance(val, (int, float)):
                        cell.number_format = '# ##0.00'
            self._autofit(ws)
        out = io.BytesIO()
        wb.save(out)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_bytes(out.getvalue())

    def write_example(self, path: str) -> None:
        wb = Workbook()
        wb.remove(wb.active)
        daily = self.blocks["daily_formula"].copy()
        weekly = self.blocks["weekly_facts"].copy()
        arts = {"901/5", "901/8", "901/14", "901/18"}
        daily = daily[daily["supplier_article"].isin(arts)].copy() if not daily.empty else pd.DataFrame()
        weekly = weekly[weekly["supplier_article"].isin(arts)].copy() if not weekly.empty else pd.DataFrame()
        for name, df in {"daily": daily, "weekly": weekly}.items():
            ws = wb.create_sheet(title=name)
            if df.empty:
                ws["A1"] = "Нет данных"
                continue
            for j, col in enumerate(df.columns, start=1):
                c = ws.cell(row=1, column=j, value=str(col))
                c.fill = HEADER_FILL
                c.font = HEADER_FONT
                c.border = BORDER
            for i, row in enumerate(df.itertuples(index=False), start=2):
                for j, val in enumerate(row, start=1):
                    cell = ws.cell(row=i, column=j, value=val)
                    cell.border = BORDER
                    if isinstance(val, (int, float)):
                        cell.number_format = '# ##0.00'
            self._autofit(ws)
        out = io.BytesIO()
        wb.save(out)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_bytes(out.getvalue())

    def _write_title(self, ws, row: int, title: str) -> int:
        ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=10)
        c = ws.cell(row=row, column=1, value=title)
        c.fill = HEADER_FILL
        c.font = Font(color="FFFFFF", bold=True, size=14)
        c.alignment = Alignment(horizontal="center", vertical="center")
        return row + 1

    def _write_block(self, ws, start_row: int, block_title: str, cols: List[str], rows: List[Dict[str, Any]], current_week: bool) -> int:
        ws.cell(row=start_row, column=1, value=block_title).font = Font(bold=True, size=12)
        header_row = start_row + 1
        all_headers = ["Категория"] + cols + ["План"]
        for j, col in enumerate(all_headers, start=1):
            c = ws.cell(row=header_row, column=j, value=col)
            c.fill = HEADER_FILL
            c.font = HEADER_FONT
            c.alignment = Alignment(horizontal="center", vertical="center")
            c.border = BORDER
        row_i = header_row + 1
        category_color_index = 0
        for item in rows:
            level = int(item["level"])
            kind = item["kind"]
            label = item["label"]
            display = label
            if level == 1:
                display = f"    {label}"
            elif level == 2:
                display = f"        {label}"
            ws.cell(row=row_i, column=1, value=display)
            if kind == "category":
                fill = CATEGORY_FILLS[category_color_index % len(CATEGORY_FILLS)]
                category_color_index += 1
                for col in range(1, len(all_headers) + 1):
                    ws.cell(row=row_i, column=col).fill = fill
                    ws.cell(row=row_i, column=col).font = WHITE_FONT
            elif kind == "product":
                for col in range(1, len(all_headers) + 1):
                    ws.cell(row=row_i, column=col).fill = PRODUCT_FILL
                    ws.cell(row=row_i, column=col).font = BOLD_FONT
            elif kind == "article":
                for col in range(1, len(all_headers) + 1):
                    ws.cell(row=row_i, column=col).fill = ARTICLE_FILL
            elif kind == "grand_total":
                for col in range(1, len(all_headers) + 1):
                    ws.cell(row=row_i, column=col).fill = TOTAL_FILL
                    ws.cell(row=row_i, column=col).font = BOLD_FONT
            for idx, col_name in enumerate(cols, start=2):
                val = item.get(col_name)
                if current_week:
                    # future days blank
                    header_date = self._header_to_date(col_name)
                    if header_date and header_date > self.latest_day:
                        ws.cell(row=row_i, column=idx, value=None)
                    else:
                        ws.cell(row=row_i, column=idx, value=money_fmt(val if val is not None else 0.0))
                else:
                    ws.cell(row=row_i, column=idx, value=money_fmt(val if val is not None else 0.0))
                ws.cell(row=row_i, column=idx).number_format = '# ##0 ₽'
            plan_cell = ws.cell(row=row_i, column=len(cols) + 2, value=money_fmt(item.get("plan") if item.get("plan") is not None else 0.0))
            plan_cell.number_format = '# ##0 ₽'
            plan_cell.font = Font(bold=True)
            for col in range(1, len(all_headers) + 1):
                ws.cell(row=row_i, column=col).border = BORDER
                ws.cell(row=row_i, column=col).alignment = Alignment(vertical="center")
            if kind in {"product", "article"}:
                ws.row_dimensions[row_i].hidden = True
                ws.row_dimensions[row_i].outlineLevel = level
            else:
                ws.row_dimensions[row_i].outlineLevel = level
            row_i += 1
        return row_i

    def _header_to_date(self, header: str) -> Optional[date]:
        m = re.search(r"(\d{2})\.(\d{2})", header)
        if not m:
            return None
        dd, mm = int(m.group(1)), int(m.group(2))
        return date(self.latest_day.year, mm, dd)

    def _autofit(self, ws) -> None:
        widths: Dict[int, int] = {}
        for row in ws.iter_rows():
            for cell in row:
                if cell.value is None:
                    continue
                widths[cell.column] = max(widths.get(cell.column, 8), min(len(str(cell.value)) + 2, 36))
        for col_idx, width in widths.items():
            if col_idx == 1:
                width = max(width, 28)
            ws.column_dimensions[get_column_letter(col_idx)].width = width

    def _finalize_sheet(self, ws) -> None:
        self._autofit(ws)
        ws.column_dimensions["A"].width = 34
        for col in range(2, 9):
            ws.column_dimensions[get_column_letter(col)].width = 12
        ws.column_dimensions[get_column_letter(9)].width = 14
        for col in range(1, 11):
            for row in range(1, ws.max_row + 1):
                ws.cell(row=row, column=col).alignment = Alignment(vertical="center")


def choose_provider(root: str) -> Provider:
    access = os.getenv("YC_ACCESS_KEY_ID", "")
    secret = os.getenv("YC_SECRET_ACCESS_KEY", "")
    bucket = os.getenv("YC_BUCKET_NAME", "")
    if access and secret and bucket:
        log("Using Yandex Object Storage (S3)")
        return S3Provider(access, secret, bucket)
    log("Using local files")
    return LocalProvider(root or ".")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".")
    args = parser.parse_args()

    provider = choose_provider(args.root)
    loader = Loader(provider)
    log("Loading data")
    data = loader.load_all()
    builder = Builder(data)
    blocks = builder.build_blocks()

    latest = builder.latest_order_day
    out_main = Path(OUT_DIR) / f"Объединенный_отчет_{STORE_NAME}_{latest}.xlsx"
    out_tech = Path(OUT_DIR) / f"Технические_расчеты_{STORE_NAME}_{latest}.xlsx"
    out_ex = Path(OUT_DIR) / f"Пример_расчета_901_{STORE_NAME}_{latest}.xlsx"

    writer = ExcelWriter(data, blocks, latest)
    writer.write_main(str(out_main))
    writer.write_technical(str(out_tech), builder.diagnostics)
    writer.write_example(str(out_ex))

    log(f"Saved report: {out_main}")
    log(f"Saved technical workbook: {out_tech}")
    log(f"Saved example workbook: {out_ex}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
