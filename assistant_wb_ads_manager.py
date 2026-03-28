#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Ассистент WB Ads Manager v2

Новая логика управления рекламой для TOPFACE.
Главная цель — рост маркетинговой валовой прибыли по товару (product_root),
вторичная — рост общего числа заказов.

Режимы:
- preview: только рекомендации и сохранение отчёта
- report: только расчёт служебных таблиц
- run --apply: реальная отправка ставок в WB

Скрипт совместим с Yandex Object Storage и использует ту же схему окружения,
что и текущий алгоритм.
"""

from __future__ import annotations

import argparse
import io
import json
import math
import os
import re
import tempfile
import time
import traceback
from dataclasses import dataclass, asdict
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

import boto3
import pandas as pd
import pytz
import requests
from botocore.client import Config
from botocore.exceptions import ClientError

# =========================================================
# НАСТРОЙКИ
# =========================================================
STORE_NAME = "TOPFACE"
TIMEZONE = "Europe/Moscow"

ADS_ANALYSIS_KEY = f"Отчёты/Реклама/{STORE_NAME}/Анализ рекламы.xlsx"
ADS_WEEKLY_PREFIX = f"Отчёты/Реклама/{STORE_NAME}/Недельные/"
ECONOMICS_KEY = f"Отчёты/Финансовые показатели/{STORE_NAME}/Экономика.xlsx"
KEYWORDS_WEEKLY_PREFIX = f"Отчёты/Поисковые запросы/{STORE_NAME}/Недельные/"
ORDERS_WEEKLY_PREFIX = f"Отчёты/Заказы/{STORE_NAME}/Недельные/"
FUNNEL_KEY = f"Отчёты/Воронка продаж/{STORE_NAME}/Воронка продаж.xlsx"
ADS_HISTORY_KEY = f"Служебные файлы/Ассистент WB/{STORE_NAME}/История_рекламы_14дней.xlsx"

SERVICE_ROOT = f"Служебные файлы/Ассистент WB/{STORE_NAME}/"
SERVICE_PREVIEW_KEY = SERVICE_ROOT + "preview_last_run_v2.xlsx"
SERVICE_LOG_KEY = SERVICE_ROOT + "last_run_summary_v2.json"
SERVICE_DECISIONS_ARCHIVE_KEY = SERVICE_ROOT + "decision_archive_v2.xlsx"
SERVICE_BID_HISTORY_KEY = SERVICE_ROOT + "bid_history_v2.xlsx"
SERVICE_LIMITS_KEY = SERVICE_ROOT + "bid_limits_daily_v2.xlsx"
SERVICE_ROOT_METRICS_KEY = SERVICE_ROOT + "product_root_metrics_v2.xlsx"
SERVICE_BID_EFFICIENCY_KEY = SERVICE_ROOT + "bid_efficiency_daily_v2.xlsx"
SERVICE_WEAK_POSITION_KEY = SERVICE_ROOT + "weak_position_priority_v2.xlsx"
SERVICE_CHANGE_EFFECTS_KEY = SERVICE_ROOT + "change_effects_v2.xlsx"
SERVICE_EXPERIMENTS_KEY = SERVICE_ROOT + "bid_experiments_v2.xlsx"
SERVICE_CONFIG_KEY = SERVICE_ROOT + "strategy_config_v2.json"

WB_BIDS_URL = "https://advert-api.wildberries.ru/api/advert/v1/bids"
WB_BIDS_MIN_URL = "https://advert-api.wildberries.ru/api/advert/v1/bids/min"

TARGET_SUBJECTS = {
    "кисти косметические",
    "блески",
    "помады",
    "косметические карандаши",
}
EXPANSION_SUBJECTS = {"блески", "помады", "косметические карандаши"}

DEFAULT_CONFIG = {
    "comfort_blended_drr_min": 8.0,
    "comfort_blended_drr_max": 12.0,
    "growth_blended_drr_max": 15.0,
    "weekend_blended_drr_max": 20.0,
    "weekend_experiments_per_root_per_year": 2,
    "expansion_cap_multiplier": 2.5,
    "min_rating": 4.5,
    "ok_rating": 4.7,
    "good_rating": 4.8,
    "min_orders_for_stable": 5,
    "min_clicks_for_stable": 100,
    "learn_start_factor": 0.70,
    "up_step_cpc_pct": 0.05,
    "up_step_cpc_big_pct": 0.10,
    "up_step_cpm_search_pct": 0.08,
    "up_step_cpm_shelves_pct": 0.10,
    "down_step_pct": 0.07,
    "limit_reached_ratio": 0.95,
    "safety_hardcap_cpc": 1.40,
    "safety_hardcap_cpm": 1.35,
    "combined_min_kopecks": 8000,
    "recommendations_min_kopecks": 8000,
    "search_min_kopecks": 400,
}

# =========================================================
# ЛОГ
# =========================================================
def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)

# =========================================================
# УТИЛИТЫ
# =========================================================
def tz_now() -> datetime:
    return datetime.now(pytz.timezone(TIMEZONE))


def normalize_colname(name: str) -> str:
    return str(name).strip().lower().replace("ё", "е")


def safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if pd.isna(v):
            return default
        return float(v)
    except Exception:
        return default


def safe_int(v: Any, default: int = 0) -> int:
    try:
        if pd.isna(v):
            return default
        return int(float(v))
    except Exception:
        return default


def clamp(v: float, min_v: float, max_v: float) -> float:
    return max(min_v, min(v, max_v))


def pct_change(after: float, before: float) -> float:
    if before <= 0:
        return 0.0 if after <= 0 else 100.0
    return (after / before - 1.0) * 100.0


def iso_week_label(d: date) -> str:
    y, w, _ = d.isocalendar()
    return f"{y}-W{w:02d}"


def week_start_from_label(label: str) -> date:
    m = re.match(r"^(\d{4})-W(\d{2})$", label)
    if not m:
        raise ValueError(f"Некорректная неделя: {label}")
    return date.fromisocalendar(int(m.group(1)), int(m.group(2)), 1)


def previous_week_label(label: str, shift: int = 1) -> str:
    start = week_start_from_label(label) - timedelta(days=7 * shift)
    return iso_week_label(start)


def list_recent_weeks(end_dt: date, n_weeks: int = 6) -> List[str]:
    labels = []
    start = end_dt - timedelta(days=end_dt.weekday())
    for i in range(n_weeks):
        labels.append(iso_week_label(start - timedelta(days=7 * i)))
    return labels


def parse_product_root(supplier_article: Any) -> str:
    s = str(supplier_article or "").strip()
    if not s:
        return ""
    return s.split("/")[0].strip()


def uniq_concat(values: Iterable[Any], sep: str = ", ") -> str:
    vals = []
    seen = set()
    for v in values:
        s = str(v).strip()
        if s and s not in seen:
            vals.append(s)
            seen.add(s)
    return sep.join(vals)


def first_notnull(series: pd.Series, default: Any = None) -> Any:
    for v in series:
        if pd.notna(v) and str(v) != "":
            return v
    return default


def ensure_columns(df: pd.DataFrame, columns: Dict[str, Any]) -> pd.DataFrame:
    for c, default in columns.items():
        if c not in df.columns:
            df[c] = default
    return df

# =========================================================
# S3
# =========================================================
class S3Storage:
    def __init__(self, access_key: str, secret_key: str, bucket_name: str):
        self.bucket = bucket_name
        self.s3 = boto3.client(
            "s3",
            endpoint_url="https://storage.yandexcloud.net",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="ru-central1",
            config=Config(signature_version="s3v4", read_timeout=300, connect_timeout=60, retries={"max_attempts": 5}),
        )

    def file_exists(self, key: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False

    def read_excel(self, key: str, sheet_name=0) -> pd.DataFrame:
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        data = obj["Body"].read()
        return pd.read_excel(io.BytesIO(data), sheet_name=sheet_name)

    def read_excel_all_sheets(self, key: str) -> Dict[str, pd.DataFrame]:
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        data = obj["Body"].read()
        return pd.read_excel(io.BytesIO(data), sheet_name=None)

    def write_excel_sheets(self, key: str, sheets: Dict[str, pd.DataFrame]) -> None:
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            with pd.ExcelWriter(tmp_path, engine="openpyxl") as writer:
                for sheet_name, df in sheets.items():
                    (df if df is not None else pd.DataFrame()).to_excel(writer, sheet_name=str(sheet_name)[:31], index=False)
            self.s3.upload_file(tmp_path, self.bucket, key)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def read_text(self, key: str) -> str:
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        return obj["Body"].read().decode("utf-8")

    def write_text(self, key: str, text: str) -> None:
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=text.encode("utf-8"))

# =========================================================
# МОДЕЛИ
# =========================================================
@dataclass
class Decision:
    decision_date: str
    id_campaign: int
    nm_id: int
    product_root: str
    supplier_article: str
    subject: str
    campaign_type: str
    placement: str
    current_bid_kopecks: int
    comfort_bid_kopecks: int
    max_bid_kopecks: int
    experiment_bid_kopecks: int
    applied_max_bid_kopecks: int
    action: str
    new_bid_kopecks: int
    reason: str
    mode: str
    weak_position_flag: int = 0
    limit_reached_flag: int = 0

# =========================================================
# ПОДГОТОВКА РЕКЛАМЫ
# =========================================================
def prepare_daily_stats_sheet(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    aliases = {
        "ID кампании": ["ID кампании", "ID", "advert_id"],
        "Артикул WB": ["Артикул WB", "nmId", "nm_id"],
        "Название": ["Название", "Название кампании"],
        "Название предмета": ["Название предмета", "Предмет"],
        "Дата": ["Дата", "date"],
        "Показы": ["Показы", "impressions"],
        "Клики": ["Клики", "clicks"],
        "CTR": ["CTR"],
        "CPC": ["CPC"],
        "Заказы": ["Заказы", "orders"],
        "CR": ["CR"],
        "Расход": ["Расход", "spend"],
        "ATBS": ["ATBS"],
        "SHKS": ["SHKS"],
        "Сумма заказов": ["Сумма заказов", "revenue"],
        "Отменено": ["Отменено", "cancelled"],
        "ДРР": ["ДРР", "DRR"],
    }
    rename_map = {}
    lower_cols = {normalize_colname(c): c for c in df.columns}
    for target, variants in aliases.items():
        for v in variants:
            key = normalize_colname(v)
            if key in lower_cols:
                rename_map[lower_cols[key]] = target
                break
    df = df.rename(columns=rename_map)
    required = ["ID кампании", "Артикул WB", "Дата"]
    if not all(c in df.columns for c in required):
        return pd.DataFrame()
    df = ensure_columns(df, {
        "Показы": 0, "Клики": 0, "CTR": 0.0, "CPC": 0.0, "Заказы": 0, "CR": 0.0,
        "Расход": 0.0, "ATBS": 0.0, "SHKS": 0.0, "Сумма заказов": 0.0, "Отменено": 0.0, "ДРР": 0.0,
        "Название": "", "Название предмета": ""
    })
    df["Дата"] = pd.to_datetime(df["Дата"], errors="coerce").dt.date
    for col in ["ID кампании", "Артикул WB"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ["Показы", "Клики", "Заказы", "ATBS", "SHKS", "Отменено"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(float)
    for col in ["CTR", "CPC", "CR", "Расход", "Сумма заказов", "ДРР"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    df = df.dropna(subset=["ID кампании", "Артикул WB", "Дата"]).copy()
    df["ID кампании"] = df["ID кампании"].astype("int64")
    df["Артикул WB"] = df["Артикул WB"].astype("int64")
    df["Название предмета"] = df["Название предмета"].astype(str).str.strip().str.lower()
    return df


def prepare_campaigns_sheet(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    aliases = {
        "ID кампании": ["ID кампании", "ID", "Кампания ID", "advert_id"],
        "Тип оплаты": ["Тип оплаты", "payment_type"],
        "Тип ставки": ["Тип ставки", "bid_type"],
        "Ставка в поиске (руб)": ["Ставка в поиске (руб)", "Ставка", "bid"],
        "Ставка в рекомендациях (руб)": ["Ставка в рекомендациях (руб)", "Ставка в рекомендациях"],
        "Название": ["Название", "Название кампании", "Кампания"],
        "Статус": ["Статус", "status"],
        "Название предмета": ["Название предмета", "Предмет"],
        "Артикул WB": ["Артикул WB", "nmId", "nm_id"],
    }
    rename_map = {}
    lower_cols = {normalize_colname(c): c for c in df.columns}
    for target, variants in aliases.items():
        for v in variants:
            key = normalize_colname(v)
            if key in lower_cols:
                rename_map[lower_cols[key]] = target
                break
    df = df.rename(columns=rename_map)
    if "ID кампании" not in df.columns:
        return pd.DataFrame()
    df = ensure_columns(df, {
        "Тип оплаты": "", "Тип ставки": "", "Ставка в поиске (руб)": 0, "Ставка в рекомендациях (руб)": 0,
        "Название": "", "Статус": "", "Название предмета": "", "Артикул WB": None
    })
    df["ID кампании"] = pd.to_numeric(df["ID кампании"], errors="coerce")
    df = df.dropna(subset=["ID кампании"]).copy()
    df["ID кампании"] = df["ID кампании"].astype("int64")
    for c in ["Ставка в поиске (руб)", "Ставка в рекомендациях (руб)"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    if "Артикул WB" in df.columns:
        df["Артикул WB"] = pd.to_numeric(df["Артикул WB"], errors="coerce")
    df["Название предмета"] = df["Название предмета"].astype(str).str.strip().str.lower()
    return df


def classify_campaign(row: pd.Series) -> str:
    payment_type = str(row.get("Тип оплаты", "")).strip().lower()
    bid_type = str(row.get("Тип ставки", "")).strip().lower()
    if payment_type == "cpc":
        return "cpc_search"
    if payment_type == "cpm":
        if bid_type == "unified":
            return "cpm_shelves"
        return "cpm_search"
    return "unknown"


def detect_placement(campaign_type: str) -> str:
    if campaign_type == "cpm_shelves":
        return "recommendations"
    return "search"


def kopecks_from_rub(value: Any) -> int:
    return int(round(safe_float(value) * 100.0))


def load_ads_and_campaigns(s3: S3Storage) -> Tuple[pd.DataFrame, pd.DataFrame]:
    log("Загрузка рекламы и списка кампаний")
    sheets = s3.read_excel_all_sheets(ADS_ANALYSIS_KEY)
    stats_df = pd.DataFrame()
    campaigns_df = pd.DataFrame()
    if "Статистика_Ежедневно" in sheets:
        stats_df = prepare_daily_stats_sheet(sheets["Статистика_Ежедневно"])
    else:
        for _, df in sheets.items():
            tmp = prepare_daily_stats_sheet(df)
            if not tmp.empty:
                stats_df = tmp
                break
    if "Список_кампаний" in sheets:
        campaigns_df = prepare_campaigns_sheet(sheets["Список_кампаний"])
    else:
        for _, df in sheets.items():
            tmp = prepare_campaigns_sheet(df)
            if not tmp.empty:
                campaigns_df = tmp
                break
    if stats_df.empty:
        raise RuntimeError("Не удалось прочитать лист Статистика_Ежедневно из файла рекламы")
    if campaigns_df.empty:
        raise RuntimeError("Не удалось прочитать лист Список_кампаний из файла рекламы")
    campaigns_df["campaign_type"] = campaigns_df.apply(classify_campaign, axis=1)
    campaigns_df["placement"] = campaigns_df["campaign_type"].map(detect_placement)
    campaigns_df["current_search_bid_kopecks"] = campaigns_df["Ставка в поиске (руб)"].apply(kopecks_from_rub)
    campaigns_df["current_rec_bid_kopecks"] = campaigns_df["Ставка в рекомендациях (руб)"].apply(kopecks_from_rub)
    return stats_df, campaigns_df

# =========================================================
# ИСТОРИЯ ДОЗАГРУЗКИ
# =========================================================
def load_ads_history(s3: S3Storage, fallback_local: bool = False) -> pd.DataFrame:
    key = ADS_HISTORY_KEY
    try:
        if s3.file_exists(key):
            x = s3.read_excel(key)
        else:
            raise FileNotFoundError(key)
    except Exception:
        if fallback_local and os.path.exists("/mnt/data/История_рекламы_14дней.xlsx"):
            x = pd.read_excel("/mnt/data/История_рекламы_14дней.xlsx")
        else:
            return pd.DataFrame()
    x = prepare_daily_stats_sheet(x)
    if x.empty:
        return x
    if "Дата запроса" in x.columns:
        x["Дата запроса"] = pd.to_datetime(x["Дата запроса"], errors="coerce")
    return x

# =========================================================
# ЭКОНОМИКА
# =========================================================
def load_economics(s3: S3Storage) -> pd.DataFrame:
    log("Загрузка экономики")
    sheets = s3.read_excel_all_sheets(ECONOMICS_KEY)
    df = sheets.get("Юнит экономика")
    if df is None:
        raise RuntimeError("В файле экономики не найден лист 'Юнит экономика'")
    aliases = {
        "Неделя": ["Неделя"],
        "Артикул WB": ["Артикул WB", "nmId"],
        "Артикул продавца": ["Артикул продавца", "supplierArticle"],
        "Предмет": ["Предмет", "subject"],
        "Процент выкупа": ["Процент выкупа"],
        "Средняя цена покупателя": ["Средняя цена покупателя"],
        "Валовая прибыль, руб/ед": ["Валовая прибыль, руб/ед"],
        "Чистая прибыль, руб/ед": ["Чистая прибыль, руб/ед"],
        "Валовая рентабельность, %": ["Валовая рентабельность, %"],
        "Чистая рентабельность, %": ["Чистая рентабельность, %"],
    }
    lower_cols = {normalize_colname(c): c for c in df.columns}
    rename_map = {}
    for target, vars_ in aliases.items():
        for v in vars_:
            key = normalize_colname(v)
            if key in lower_cols:
                rename_map[lower_cols[key]] = target
                break
    df = df.rename(columns=rename_map)
    req = ["Артикул WB", "Артикул продавца", "Предмет"]
    if not all(c in df.columns for c in req):
        raise RuntimeError("Не хватает колонок в листе 'Юнит экономика'")
    df = ensure_columns(df, {
        "Процент выкупа": 0.0, "Средняя цена покупателя": 0.0,
        "Валовая прибыль, руб/ед": 0.0, "Чистая прибыль, руб/ед": 0.0,
        "Валовая рентабельность, %": 0.0, "Чистая рентабельность, %": 0.0,
        "Неделя": "",
    })
    df["Артикул WB"] = pd.to_numeric(df["Артикул WB"], errors="coerce")
    df = df.dropna(subset=["Артикул WB"]).copy()
    df["Артикул WB"] = df["Артикул WB"].astype("int64")
    for c in ["Процент выкупа", "Средняя цена покупателя", "Валовая прибыль, руб/ед", "Чистая прибыль, руб/ед", "Валовая рентабельность, %", "Чистая рентабельность, %"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    df["Предмет"] = df["Предмет"].astype(str).str.strip().str.lower()
    df["product_root"] = df["Артикул продавца"].apply(parse_product_root)
    df["BuyoutRate"] = df["Процент выкупа"] / 100.0
    df["GP_realized"] = df["Валовая прибыль, руб/ед"] * df["BuyoutRate"]
    df["NP_realized"] = df["Чистая прибыль, руб/ед"] * df["BuyoutRate"]
    return df

# =========================================================
# ЗАКАЗЫ
# =========================================================
def orders_weekly_key(week_label: str) -> str:
    return f"{ORDERS_WEEKLY_PREFIX}Заказы_{week_label}.xlsx"


def load_orders_for_period(s3: S3Storage, start_dt: date, end_dt: date) -> pd.DataFrame:
    log("Загрузка weekly-файлов заказов")
    weeks = list_recent_weeks(end_dt, n_weeks=8)
    frames: List[pd.DataFrame] = []
    for week in weeks:
        wk_start = week_start_from_label(week)
        wk_end = wk_start + timedelta(days=6)
        if wk_end < start_dt or wk_start > end_dt:
            continue
        key = orders_weekly_key(week)
        if not s3.file_exists(key):
            continue
        try:
            x = s3.read_excel_all_sheets(key)
            df = x.get("Заказы") or next(iter(x.values()))
            frames.append(df)
            log(f"  + {key}")
        except Exception as e:
            log(f"  ! ошибка чтения {key}: {e}")
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    lower_cols = {normalize_colname(c): c for c in df.columns}
    rename_map = {}
    aliases = {
        "date": ["date", "Дата"],
        "supplierArticle": ["supplierArticle", "Артикул продавца"],
        "nmId": ["nmId", "Артикул WB"],
        "subject": ["subject", "Предмет"],
        "finishedPrice": ["finishedPrice", "Цена покупателя", "finished_price"],
        "isCancel": ["isCancel", "Отмена"],
    }
    for target, vars_ in aliases.items():
        for v in vars_:
            key = normalize_colname(v)
            if key in lower_cols:
                rename_map[lower_cols[key]] = target
                break
    df = df.rename(columns=rename_map)
    req = ["date", "supplierArticle", "nmId", "subject", "finishedPrice"]
    if not all(c in df.columns for c in req):
        missing = [c for c in req if c not in df.columns]
        raise RuntimeError(f"Не хватает колонок в заказах: {missing}")
    df = ensure_columns(df, {"isCancel": False})
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df[(df["date"] >= start_dt) & (df["date"] <= end_dt)].copy()
    df["nmId"] = pd.to_numeric(df["nmId"], errors="coerce")
    df["finishedPrice"] = pd.to_numeric(df["finishedPrice"], errors="coerce").fillna(0.0)
    df = df.dropna(subset=["nmId"]).copy()
    df["nmId"] = df["nmId"].astype("int64")
    df["subject"] = df["subject"].astype(str).str.strip().str.lower()
    df["product_root"] = df["supplierArticle"].apply(parse_product_root)
    # заказы и выручка считаем по всем строкам, отдельно сохраняем отмены
    df["isCancel"] = df["isCancel"].astype(str).str.lower().isin(["true", "1", "yes"])
    return df

# =========================================================
# ВОРОНКА
# =========================================================
def load_funnel(s3: S3Storage, start_dt: date, end_dt: date) -> pd.DataFrame:
    try:
        sheets = s3.read_excel_all_sheets(FUNNEL_KEY)
    except Exception:
        return pd.DataFrame()
    df = sheets.get("Воронка продаж") or next(iter(sheets.values()))
    if df is None or df.empty:
        return pd.DataFrame()
    lower_cols = {normalize_colname(c): c for c in df.columns}
    rename_map = {}
    aliases = {
        "nmID": ["nmID", "nmId"],
        "dt": ["dt", "Дата"],
        "openCardCount": ["openCardCount"],
        "addToCartCount": ["addToCartCount"],
        "ordersCount": ["ordersCount"],
        "buyoutsCount": ["buyoutsCount"],
        "cancelCount": ["cancelCount"],
        "addToCartConversion": ["addToCartConversion"],
        "cartToOrderConversion": ["cartToOrderConversion"],
        "buyoutPercent": ["buyoutPercent"],
    }
    for target, vars_ in aliases.items():
        for v in vars_:
            key = normalize_colname(v)
            if key in lower_cols:
                rename_map[lower_cols[key]] = target
                break
    df = df.rename(columns=rename_map)
    if "nmID" not in df.columns or "dt" not in df.columns:
        return pd.DataFrame()
    df["dt"] = pd.to_datetime(df["dt"], errors="coerce").dt.date
    df = df[(df["dt"] >= start_dt) & (df["dt"] <= end_dt)].copy()
    df["nmID"] = pd.to_numeric(df["nmID"], errors="coerce")
    df = df.dropna(subset=["nmID"]).copy()
    df["nmID"] = df["nmID"].astype("int64")
    for c in ["openCardCount", "addToCartCount", "ordersCount", "buyoutsCount", "cancelCount", "addToCartConversion", "cartToOrderConversion", "buyoutPercent"]:
        if c not in df.columns:
            df[c] = 0.0
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    agg = df.groupby("nmID", as_index=False).agg({
        "openCardCount": "sum",
        "addToCartCount": "sum",
        "ordersCount": "sum",
        "buyoutsCount": "sum",
        "cancelCount": "sum",
        "addToCartConversion": "mean",
        "cartToOrderConversion": "mean",
        "buyoutPercent": "mean",
    })
    agg = agg.rename(columns={"nmID": "nm_id"})
    return agg

# =========================================================
# ПОИСКОВЫЕ ЗАПРОСЫ
# =========================================================
def keywords_weekly_key(week_label: str) -> str:
    return f"{KEYWORDS_WEEKLY_PREFIX}Неделя {week_label}.xlsx"


def load_keywords_for_period(s3: S3Storage, end_dt: date) -> pd.DataFrame:
    weeks = list_recent_weeks(end_dt, n_weeks=3)
    frames: List[pd.DataFrame] = []
    for week in weeks:
        key = keywords_weekly_key(week)
        if not s3.file_exists(key):
            continue
        try:
            sheets = s3.read_excel_all_sheets(key)
            df = sheets.get("Позиции по Ключам") or next(iter(sheets.values()))
            frames.append(df)
            log(f"Загружен файл поисковых запросов: {key}")
        except Exception as e:
            log(f"Ошибка чтения поисковых запросов {key}: {e}")
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    lower_cols = {normalize_colname(c): c for c in df.columns}
    rename_map = {}
    aliases = {
        "Дата": ["Дата"],
        "Артикул WB": ["Артикул WB", "nmId"],
        "Артикул продавца": ["Артикул продавца", "supplierArticle"],
        "Предмет": ["Предмет", "subject"],
        "Рейтинг отзывов": ["Рейтинг отзывов"],
        "Частота за неделю": ["Частота за неделю", "Частота запросов"],
        "Медианная позиция": ["Медианная позиция"],
        "Переходы в карточку": ["Переходы в карточку"],
        "Заказы": ["Заказы"],
        "Конверсия в заказ %": ["Конверсия в заказ %"],
        "Видимость %": ["Видимость %"],
        "Поисковый запрос": ["Поисковый запрос"],
    }
    for target, vars_ in aliases.items():
        for v in vars_:
            key = normalize_colname(v)
            if key in lower_cols:
                rename_map[lower_cols[key]] = target
                break
    df = df.rename(columns=rename_map)
    req = ["Артикул WB", "Предмет"]
    if not all(c in df.columns for c in req):
        return pd.DataFrame()
    df = ensure_columns(df, {
        "Рейтинг отзывов": 0.0, "Частота за неделю": 0.0, "Медианная позиция": 0.0,
        "Переходы в карточку": 0.0, "Заказы": 0.0, "Конверсия в заказ %": 0.0,
        "Видимость %": 0.0, "Поисковый запрос": "", "Артикул продавца": ""
    })
    df["Артикул WB"] = pd.to_numeric(df["Артикул WB"], errors="coerce")
    df = df.dropna(subset=["Артикул WB"]).copy()
    df["Артикул WB"] = df["Артикул WB"].astype("int64")
    df["Предмет"] = df["Предмет"].astype(str).str.strip().str.lower()
    for c in ["Рейтинг отзывов", "Частота за неделю", "Медианная позиция", "Переходы в карточку", "Заказы", "Конверсия в заказ %", "Видимость %"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    agg = df.groupby("Артикул WB", as_index=False).agg({
        "Предмет": lambda s: first_notnull(s, ""),
        "Артикул продавца": lambda s: first_notnull(s, ""),
        "Рейтинг отзывов": "mean",
        "Частота за неделю": "sum",
        "Медианная позиция": "median",
        "Переходы в карточку": "sum",
        "Заказы": "sum",
        "Конверсия в заказ %": "mean",
        "Видимость %": "mean",
        "Поисковый запрос": lambda s: uniq_concat(s[:10]),
    })
    agg = agg.rename(columns={"Артикул WB": "nm_id", "Артикул продавца": "supplier_article_kw", "Предмет": "subject_kw"})
    agg["product_root_kw"] = agg["supplier_article_kw"].apply(parse_product_root)
    return agg

# =========================================================
# ЗРЕЛОСТЬ ДАННЫХ
# =========================================================
def determine_mature_window(stats_df: pd.DataFrame) -> Tuple[date, date, date]:
    max_dt = stats_df["Дата"].max()
    if not isinstance(max_dt, date):
        raise RuntimeError("Не удалось определить максимальную дату в рекламной статистике")
    end_dt = max_dt - timedelta(days=3)
    start_dt = end_dt - timedelta(days=4)  # D-7 ... D-3 = 5 дней
    return max_dt, start_dt, end_dt

# =========================================================
# ОБЪЕДИНЕНИЕ И МЕТРИКИ
# =========================================================
def build_base_dataset(
    ads_df: pd.DataFrame,
    campaigns_df: pd.DataFrame,
    economics_df: pd.DataFrame,
    orders_df: pd.DataFrame,
    funnel_df: pd.DataFrame,
    keywords_df: pd.DataFrame,
    mature_start: date,
    mature_end: date,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    ads = ads_df[(ads_df["Дата"] >= mature_start) & (ads_df["Дата"] <= mature_end)].copy()
    ads = ads[ads["Название предмета"].isin(TARGET_SUBJECTS)].copy()

    # Маппинг кампаний
    camp = campaigns_df[[
        "ID кампании", "Тип оплаты", "Тип ставки", "Ставка в поиске (руб)", "Ставка в рекомендациях (руб)",
        "current_search_bid_kopecks", "current_rec_bid_kopecks", "campaign_type", "placement", "Статус", "Название"
    ]].drop_duplicates()
    ads = ads.merge(camp, on="ID кампании", how="left")
    ads["campaign_type"] = ads["campaign_type"].fillna("unknown")
    ads["placement"] = ads["placement"].fillna("search")

    ads_agg = ads.groupby(["ID кампании", "Артикул WB", "Название предмета", "campaign_type", "placement"], as_index=False).agg({
        "Показы": "sum",
        "Клики": "sum",
        "Заказы": "sum",
        "Расход": "sum",
        "ATBS": "sum",
        "SHKS": "sum",
        "Сумма заказов": "sum",
        "Отменено": "sum",
        "CTR": "mean",
        "CPC": "mean",
        "CR": "mean",
        "ДРР": "mean",
        "current_search_bid_kopecks": "max",
        "current_rec_bid_kopecks": "max",
        "Название": lambda s: first_notnull(s, ""),
        "Статус": lambda s: first_notnull(s, ""),
    })
    ads_agg = ads_agg.rename(columns={"Артикул WB": "nm_id", "Название предмета": "subject"})

    # Экономика
    eco = economics_df[[
        "Артикул WB", "Артикул продавца", "Предмет", "Процент выкупа", "Средняя цена покупателя",
        "Валовая прибыль, руб/ед", "Чистая прибыль, руб/ед", "Валовая рентабельность, %", "Чистая рентабельность, %",
        "product_root", "GP_realized", "NP_realized"
    ]].drop_duplicates(subset=["Артикул WB"])
    eco = eco.rename(columns={
        "Артикул WB": "nm_id", "Артикул продавца": "supplier_article", "Предмет": "subject_eco",
        "Процент выкупа": "buyout_rate_pct", "Средняя цена покупателя": "avg_buyer_price",
        "Валовая прибыль, руб/ед": "gp_unit", "Чистая прибыль, руб/ед": "np_unit",
        "Валовая рентабельность, %": "gp_margin_pct", "Чистая рентабельность, %": "np_margin_pct",
    })
    base = ads_agg.merge(eco, on="nm_id", how="left")
    base["supplier_article"] = base["supplier_article"].fillna("")
    base["product_root"] = base["product_root"].fillna(base["supplier_article"].apply(parse_product_root))
    base["subject"] = base["subject"].fillna(base["subject_eco"]).astype(str).str.lower()

    # Заказы -> SKU агрегат
    if not orders_df.empty:
        orders_f = orders_df[orders_df["subject"].isin(TARGET_SUBJECTS)].copy()
        orders_f["realized_order"] = (~orders_f["isCancel"]).astype(int)
        sku_orders = orders_f.groupby(["nmId", "supplierArticle", "product_root", "subject"], as_index=False).agg(
            total_orders=("nmId", "size"),
            net_orders=("realized_order", "sum"),
            total_revenue=("finishedPrice", "sum"),
            avg_finished_price=("finishedPrice", "mean"),
            cancel_orders=("isCancel", "sum"),
        ).rename(columns={"nmId": "nm_id", "supplierArticle": "supplier_article_orders", "subject": "subject_orders"})
        root_orders = orders_f.groupby(["product_root", "subject"], as_index=False).agg(
            root_total_orders=("nmId", "size"),
            root_net_orders=("realized_order", "sum"),
            root_total_revenue=("finishedPrice", "sum"),
            root_avg_finished_price=("finishedPrice", "mean"),
        ).rename(columns={"subject": "subject_root_orders"})
    else:
        sku_orders = pd.DataFrame(columns=["nm_id", "supplier_article_orders", "product_root", "subject_orders", "total_orders", "net_orders", "total_revenue", "avg_finished_price", "cancel_orders"])
        root_orders = pd.DataFrame(columns=["product_root", "subject_root_orders", "root_total_orders", "root_net_orders", "root_total_revenue", "root_avg_finished_price"])

    base = base.merge(sku_orders, on=["nm_id", "product_root"], how="left")
    base = base.merge(root_orders, on=["product_root"], how="left")

    # Воронка
    if not funnel_df.empty:
        base = base.merge(funnel_df, on="nm_id", how="left")
    # Поисковые запросы
    if not keywords_df.empty:
        base = base.merge(keywords_df, on="nm_id", how="left")

    # Дефолты
    defaults = {
        "total_orders": 0, "net_orders": 0, "total_revenue": 0.0, "avg_finished_price": 0.0, "cancel_orders": 0,
        "root_total_orders": 0, "root_net_orders": 0, "root_total_revenue": 0.0, "root_avg_finished_price": 0.0,
        "openCardCount": 0.0, "addToCartCount": 0.0, "ordersCount": 0.0, "buyoutsCount": 0.0, "cancelCount": 0.0,
        "addToCartConversion": 0.0, "cartToOrderConversion": 0.0, "buyoutPercent": 0.0,
        "Рейтинг отзывов": 0.0, "Частота за неделю": 0.0, "Медианная позиция": 0.0, "Переходы в карточку": 0.0,
        "Конверсия в заказ %": 0.0, "Видимость %": 0.0, "Заказы_y": 0.0,
    }
    base = ensure_columns(base, defaults)
    for c, default in defaults.items():
        if c in base.columns:
            if isinstance(default, float) or isinstance(default, int):
                base[c] = pd.to_numeric(base[c], errors="coerce").fillna(default)
            else:
                base[c] = base[c].fillna(default)
    base["supplier_article"] = base["supplier_article"].replace("", pd.NA).fillna(base.get("supplier_article_orders", "")).fillna("")

    # root рекламные метрики через агрегат base -> потом merge назад
    root_metrics = base.groupby("product_root", as_index=False).agg(
        root_ad_spend=("Расход", "sum"),
        root_ad_clicks=("Клики", "sum"),
        root_ad_impressions=("Показы", "sum"),
        root_ad_orders=("Заказы", "sum"),
        root_ad_revenue=("Сумма заказов", "sum"),
        root_ad_atbs=("ATBS", "sum"),
        root_ad_shks=("SHKS", "sum"),
        root_rating=("Рейтинг отзывов", "mean"),
        root_visibility=("Видимость %", "mean"),
        root_median_position=("Медианная позиция", "median"),
    )
    base = base.merge(root_metrics, on="product_root", how="left")

    return base, root_metrics, root_orders

# =========================================================
# РЕЖИМЫ ТОВАРОВ И ЛИМИТЫ СТАВОК
# =========================================================
def infer_mode(row: pd.Series, cfg: Dict[str, Any]) -> str:
    subject = str(row.get("subject", "")).strip().lower()
    rating = safe_float(row.get("root_rating") or row.get("Рейтинг отзывов"))
    gp = safe_float(row.get("gp_unit"))
    buyout_pct = safe_float(row.get("buyout_rate_pct"))
    pos = safe_float(row.get("root_median_position") or row.get("Медианная позиция"))
    atc = safe_float(row.get("addToCartConversion"))
    c2o = safe_float(row.get("cartToOrderConversion"))
    total_orders = safe_float(row.get("root_total_orders"))
    if gp <= 0 or buyout_pct < 70 or rating < cfg["min_rating"]:
        return "problem"
    if atc < 3 or c2o < 2:
        return "margin_guard"
    if subject in EXPANSION_SUBJECTS and pos > 15 and total_orders < 100 and rating >= cfg["ok_rating"]:
        return "hero_growth"
    if pos > 20 and rating >= cfg["ok_rating"]:
        return "hero_growth"
    return "balanced"


def mode_shares(mode: str) -> Tuple[float, float]:
    if mode == "hero_growth":
        return 0.50, 0.80
    if mode == "balanced":
        return 0.40, 0.65
    if mode == "margin_guard":
        return 0.30, 0.45
    return 0.15, 0.25


def estimate_ctr_for_cpm(row: pd.Series) -> float:
    ctr = safe_float(row.get("CTR"))
    if ctr > 0:
        return ctr / 100.0 if ctr > 1 else ctr
    # fallback by placement
    return 0.018 if row.get("placement") == "recommendations" else 0.012


def compute_bid_limits(base: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
    df = base.copy()
    df["mode"] = df.apply(lambda r: infer_mode(r, cfg), axis=1)
    shares = df["mode"].apply(mode_shares)
    df["comfort_share"] = shares.apply(lambda x: x[0])
    df["max_share"] = shares.apply(lambda x: x[1])
    df["GP_realized"] = pd.to_numeric(df["GP_realized"], errors="coerce").fillna(0.0)
    df["Comfort_CPO"] = df["GP_realized"] * df["comfort_share"]
    df["Max_CPO"] = df["GP_realized"] * df["max_share"]

    df["ClicksPerAdOrder"] = df.apply(lambda r: max(1.0, safe_float(r.get("Клики")) / max(1.0, safe_float(r.get("Заказы")))), axis=1)
    df["ClicksPerTotalOrder"] = df.apply(lambda r: max(1.0, safe_float(r.get("root_ad_clicks")) / max(1.0, safe_float(r.get("root_total_orders")))), axis=1)
    df["Comfort_CPC_ad"] = df["Comfort_CPO"] / df["ClicksPerAdOrder"]
    df["Max_CPC_ad"] = df["Max_CPO"] / df["ClicksPerAdOrder"]
    df["Comfort_CPC_total"] = df["Comfort_CPO"] / df["ClicksPerTotalOrder"]
    df["Max_CPC_total"] = df["Max_CPO"] / df["ClicksPerTotalOrder"]

    ctr_est = df.apply(estimate_ctr_for_cpm, axis=1)
    df["CTR_est"] = ctr_est
    df["Comfort_CPM_ad"] = df["Comfort_CPC_ad"] * 1000.0 * ctr_est
    df["Max_CPM_ad"] = df["Max_CPC_ad"] * 1000.0 * ctr_est
    df["Comfort_CPM_total"] = df["Comfort_CPC_total"] * 1000.0 * ctr_est
    df["Max_CPM_total"] = df["Max_CPC_total"] * 1000.0 * ctr_est

    df["Blended_DRR_root"] = df.apply(lambda r: (safe_float(r.get("root_ad_spend")) / max(1.0, safe_float(r.get("root_total_revenue")))) * 100.0, axis=1)
    df["Ad_DRR_root"] = df.apply(lambda r: (safe_float(r.get("root_ad_spend")) / max(1.0, safe_float(r.get("root_ad_revenue")))) * 100.0, axis=1)
    df["AdOrderShare"] = df.apply(lambda r: safe_float(r.get("root_ad_orders")) / max(1.0, safe_float(r.get("root_total_orders"))), axis=1)

    def pick_limits(r: pd.Series) -> pd.Series:
        subject = str(r.get("subject", "")).lower()
        placement = str(r.get("placement", "search"))
        if subject in EXPANSION_SUBJECTS and str(r.get("mode")) in {"hero_growth", "balanced"}:
            comfort_cpc = safe_float(r["Comfort_CPC_total"])
            max_cpc = min(safe_float(r["Max_CPC_total"]), safe_float(r["Max_CPC_ad"]) * cfg["expansion_cap_multiplier"])
            comfort_cpm = safe_float(r["Comfort_CPM_total"])
            max_cpm = min(safe_float(r["Max_CPM_total"]), safe_float(r["Max_CPM_ad"]) * cfg["expansion_cap_multiplier"])
        else:
            comfort_cpc = safe_float(r["Comfort_CPC_ad"])
            max_cpc = safe_float(r["Max_CPC_ad"])
            comfort_cpm = safe_float(r["Comfort_CPM_ad"])
            max_cpm = safe_float(r["Max_CPM_ad"])

        if placement == "recommendations":
            comfort = int(round(comfort_cpm * 100))
            max_bid = int(round(max_cpm * 100))
            hardcap = int(round(max_bid * cfg["safety_hardcap_cpm"]))
            floor = cfg["recommendations_min_kopecks"]
        else:
            if str(r.get("campaign_type")) == "cpc_search":
                comfort = int(round(comfort_cpc * 100))
                max_bid = int(round(max_cpc * 100))
                hardcap = int(round(max_bid * cfg["safety_hardcap_cpc"]))
                floor = cfg["search_min_kopecks"]
            else:
                comfort = int(round(comfort_cpm * 100))
                max_bid = int(round(max_cpm * 100))
                hardcap = int(round(max_bid * cfg["safety_hardcap_cpm"]))
                floor = cfg["search_min_kopecks"]
        comfort = max(comfort, floor)
        max_bid = max(max_bid, floor)
        hardcap = max(hardcap, floor)
        experiment = min(int(round(max_bid * 1.25)), hardcap)
        return pd.Series([comfort, max_bid, hardcap, experiment])

    df[["comfort_bid_kopecks", "max_bid_kopecks", "hardcap_bid_kopecks", "experiment_bid_kopecks"]] = df.apply(pick_limits, axis=1)
    return df

# =========================================================
# ЭФФЕКТИВНОСТЬ СТАВКИ
# =========================================================
def compute_bid_efficiency(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    demand = out["Частота за неделю"].replace(0, pd.NA)
    out["ImpressionCaptureRate"] = out["Показы"] / demand
    out["ClickCaptureRate"] = out["Клики"] / demand
    bid_rub = out.apply(lambda r: (safe_float(r.get("current_rec_bid_kopecks")) if r.get("placement") == "recommendations" else safe_float(r.get("current_search_bid_kopecks"))) / 100.0, axis=1)
    bid_rub = bid_rub.replace(0, pd.NA)
    out["BidEff_Imp"] = out["ImpressionCaptureRate"] / bid_rub
    out["BidEff_Click"] = out["ClickCaptureRate"] / bid_rub

    # baseline по subject + placement
    baseline = out.groupby(["subject", "placement"], as_index=False).agg(
        baseline_imp=("BidEff_Imp", "median"),
        baseline_click=("BidEff_Click", "median"),
    )
    out = out.merge(baseline, on=["subject", "placement"], how="left")
    out["BEI_Imp"] = out["BidEff_Imp"] / out["baseline_imp"].replace(0, pd.NA)
    out["BEI_Click"] = out["BidEff_Click"] / out["baseline_click"].replace(0, pd.NA)
    out["BEI_Imp"] = out["BEI_Imp"].fillna(1.0)
    out["BEI_Click"] = out["BEI_Click"].fillna(1.0)
    out["TotalBidEfficiency"] = 0.6 * out["BEI_Imp"] + 0.4 * out["BEI_Click"]
    out["bid_eff_comment"] = out.apply(_bid_eff_comment, axis=1)
    return out


def _bid_eff_comment(r: pd.Series) -> str:
    if safe_float(r.get("TotalBidEfficiency")) < 0.90:
        return "Ставка работает хуже нормы"
    if safe_float(r.get("TotalBidEfficiency")) > 1.15:
        return "Ставка работает лучше нормы"
    return "Ставка работает в пределах нормы"

# =========================================================
# ЭФФЕКТ ИЗМЕНЕНИЙ СТАВКИ
# =========================================================
def load_bid_history(s3: S3Storage) -> pd.DataFrame:
    if not s3.file_exists(SERVICE_BID_HISTORY_KEY):
        return pd.DataFrame(columns=[
            "Дата", "ID кампании", "Артикул WB", "product_root", "placement", "Старая ставка, коп", "Новая ставка, коп",
            "Action", "Reason"
        ])
    try:
        return s3.read_excel(SERVICE_BID_HISTORY_KEY)
    except Exception:
        return pd.DataFrame()


def compute_change_effects(current_df: pd.DataFrame, bid_history_df: pd.DataFrame) -> pd.DataFrame:
    # Упрощённая версия: считаем, что если по subject+placement эффективность ниже нормы и было повышение ставки раньше,
    # то последние повышения были слабые. Полная событийная модель требует ежедневной накопительной истории метрик.
    hist = bid_history_df.copy()
    if hist.empty:
        return pd.DataFrame(columns=["ID кампании", "Артикул WB", "placement", "last_action", "effect_label", "effect_score"])
    for c in ["ID кампании", "Артикул WB"]:
        if c in hist.columns:
            hist[c] = pd.to_numeric(hist[c], errors="coerce")
    hist["Дата"] = pd.to_datetime(hist.get("Дата"), errors="coerce")
    hist = hist.sort_values("Дата")
    last = hist.groupby(["ID кампании", "Артикул WB", "placement"], as_index=False).tail(2)
    last = last.merge(
        current_df[["ID кампании", "nm_id", "placement", "TotalBidEfficiency", "TotalBidEfficiency"]].rename(columns={"nm_id": "Артикул WB"}),
        on=["ID кампании", "Артикул WB", "placement"], how="left"
    )
    def eval_effect(g: pd.DataFrame) -> pd.Series:
        actions = uniq_concat(g.get("Action", []), sep=" | ")
        eff = safe_float(g["TotalBidEfficiency"].iloc[-1]) if not g.empty else 1.0
        if "UP" in actions and eff < 0.90:
            label = "weak_after_raise"
            score = -1
        elif "UP" in actions and eff > 1.05:
            label = "positive_after_raise"
            score = 1
        else:
            label = "neutral"
            score = 0
        return pd.Series({"last_action": actions, "effect_label": label, "effect_score": score})
    eff = last.groupby(["ID кампании", "Артикул WB", "placement"], as_index=False).apply(eval_effect).reset_index()
    if "level_0" in eff.columns:
        eff = eff.drop(columns=[c for c in ["level_0", "level_1"] if c in eff.columns])
    return eff

# =========================================================
# ЛОГИКА РЕШЕНИЙ
# =========================================================
def required_order_growth(spend_growth_pct: float, drr_growth_pp: float, blended_drr_after: float) -> float:
    if blended_drr_after <= 12.0:
        kdrr, kspend, floor = 2.0, 0.7, 3.0
    elif blended_drr_after <= 15.0:
        kdrr, kspend, floor = 3.0, 0.9, 6.0
    else:
        kdrr, kspend, floor = 4.0, 1.1, 10.0
    return max(kdrr * drr_growth_pp, kspend * spend_growth_pct, floor)


def is_weekend(dt: datetime) -> bool:
    return dt.weekday() >= 5


def decide_for_row(row: pd.Series, cfg: Dict[str, Any], effects_map: Dict[Tuple[int, int, str], Dict[str, Any]], experiment_usage: Dict[str, int]) -> Decision:
    today = tz_now().strftime("%Y-%m-%d %H:%M:%S")
    current_bid = safe_int(row.get("current_rec_bid_kopecks") if row.get("placement") == "recommendations" else row.get("current_search_bid_kopecks"))
    comfort_bid = safe_int(row.get("comfort_bid_kopecks"))
    max_bid = safe_int(row.get("max_bid_kopecks"))
    exp_bid = safe_int(row.get("experiment_bid_kopecks"))
    applied_max_bid = max_bid
    campaign_id = safe_int(row.get("ID кампании"))
    nm_id = safe_int(row.get("nm_id"))
    placement = str(row.get("placement", "search"))
    campaign_type = str(row.get("campaign_type", ""))
    product_root = str(row.get("product_root", ""))
    supplier_article = str(row.get("supplier_article", ""))
    subject = str(row.get("subject", "")).strip().lower()
    mode = str(row.get("mode", "balanced"))
    rating = safe_float(row.get("root_rating") or row.get("Рейтинг отзывов"))
    gp_realized = safe_float(row.get("GP_realized"))
    buyout_pct = safe_float(row.get("buyout_rate_pct"))
    position = safe_float(row.get("root_median_position") or row.get("Медианная позиция"))
    visibility = safe_float(row.get("root_visibility") or row.get("Видимость %"))
    blended_drr = safe_float(row.get("Blended_DRR_root"))
    total_eff = safe_float(row.get("TotalBidEfficiency"), 1.0)
    root_orders = safe_float(row.get("root_total_orders"))
    root_ad_orders = safe_float(row.get("root_ad_orders"))
    add_to_cart = safe_float(row.get("addToCartConversion"))
    c2o = safe_float(row.get("cartToOrderConversion"))
    effect = effects_map.get((campaign_id, nm_id, placement), {})
    effect_score = safe_int(effect.get("effect_score", 0))
    growth_limit = cfg["growth_blended_drr_max"] if subject in EXPANSION_SUBJECTS else cfg["comfort_blended_drr_max"]
    weekend_limit = cfg["weekend_blended_drr_max"] if subject in EXPANSION_SUBJECTS else cfg["comfort_blended_drr_max"]

    def mk(action: str, new_bid: int, reason: str, weak_flag: int = 0, limit_reached_flag: int = 0) -> Decision:
        return Decision(
            decision_date=today,
            id_campaign=campaign_id,
            nm_id=nm_id,
            product_root=product_root,
            supplier_article=supplier_article,
            subject=subject,
            campaign_type=campaign_type,
            placement=placement,
            current_bid_kopecks=current_bid,
            comfort_bid_kopecks=comfort_bid,
            max_bid_kopecks=max_bid,
            experiment_bid_kopecks=exp_bid,
            applied_max_bid_kopecks=applied_max_bid,
            action=action,
            new_bid_kopecks=max(0, new_bid),
            reason=reason,
            mode=mode,
            weak_position_flag=weak_flag,
            limit_reached_flag=limit_reached_flag,
        )

    # Жёсткие стопы
    if gp_realized <= 0:
        return mk("DOWN", int(round(current_bid * (1 - cfg["down_step_pct"]))), "GP_realized <= 0, товар убыточен")
    if rating and rating < cfg["min_rating"]:
        return mk("DOWN", int(round(current_bid * (1 - cfg["down_step_pct"]))), "Низкий рейтинг")
    if buyout_pct and buyout_pct < 70:
        return mk("DOWN", int(round(current_bid * (1 - cfg["down_step_pct"]))), "Слабый выкуп")
    if add_to_cart > 0 and c2o > 0 and (add_to_cart < 3 or c2o < 2):
        return mk("HOLD", current_bid, "Проблема скорее в карточке/воронке, а не в ставке")

    # LEARN
    if root_ad_orders < cfg["min_orders_for_stable"] or safe_float(row.get("Клики")) < cfg["min_clicks_for_stable"]:
        target = max(int(round(comfort_bid * cfg["learn_start_factor"])), current_bid)
        if target > current_bid:
            return mk("UP", target, "Режим LEARN: мало зрелых данных")
        return mk("HOLD", current_bid, "Режим LEARN: данных ещё мало")

    # LIMIT REACHED / повышение эффективности ставки
    if current_bid >= int(round(applied_max_bid * cfg["limit_reached_ratio"])) and position > 12 and total_eff < 0.95:
        return mk("LIMIT_REACHED", current_bid, "Повысить эффективность ставки — реклама работает на пределе", weak_flag=1, limit_reached_flag=1)

    # WEEKEND TEST
    if (
        is_weekend(tz_now())
        and subject in EXPANSION_SUBJECTS
        and mode == "hero_growth"
        and blended_drr < growth_limit
        and effect_score >= 0
        and experiment_usage.get(product_root, 0) < cfg["weekend_experiments_per_root_per_year"]
        and current_bid < exp_bid
        and position > 15
    ):
        new_bid = min(exp_bid, max(current_bid + int(round(current_bid * 0.08)), current_bid + 100))
        return mk("TEST_UP", new_bid, "Выходной growth-test выше обычного max")

    # Масштабирование
    if position > 12 or visibility < 65:
        if blended_drr <= growth_limit and total_eff >= 0.95 and effect_score >= 0:
            if current_bid < comfort_bid:
                new_bid = comfort_bid
                return mk("UP", new_bid, "Ниже комфортной ставки, слабая позиция / видимость")
            if current_bid < applied_max_bid:
                step_pct = cfg["up_step_cpm_shelves_pct"] if placement == "recommendations" else cfg["up_step_cpc_pct"]
                if mode == "hero_growth" and (position > 20 or visibility < 50):
                    step_pct = cfg["up_step_cpc_big_pct"] if placement == "search" else cfg["up_step_cpm_search_pct"]
                new_bid = min(applied_max_bid, int(round(current_bid * (1 + step_pct))))
                return mk("UP", new_bid, "Есть запас по позиции/видимости и ставка ещё не у max")

    # Охлаждение
    if blended_drr > growth_limit and subject in EXPANSION_SUBJECTS:
        new_bid = max(comfort_bid, int(round(current_bid * (1 - cfg["down_step_pct"]))))
        return mk("DOWN", new_bid, "Blended DRR выше growth-предела")
    if subject not in EXPANSION_SUBJECTS and blended_drr > cfg["comfort_blended_drr_max"]:
        new_bid = max(comfort_bid, int(round(current_bid * (1 - cfg["down_step_pct"]))))
        return mk("DOWN", new_bid, "Blended DRR выше комфортной зоны")

    if effect_score < 0 and current_bid > comfort_bid:
        new_bid = max(comfort_bid, int(round(current_bid * (1 - cfg["down_step_pct"]))))
        return mk("DOWN", new_bid, "Прошлые повышения не дали отклика")

    # Поддержание
    if current_bid > applied_max_bid:
        return mk("DOWN", applied_max_bid, "Ставка выше применимого max")
    return mk("HOLD", current_bid, "Ставка в рабочем диапазоне")


def build_decisions(df: pd.DataFrame, cfg: Dict[str, Any], change_effects_df: pd.DataFrame) -> Tuple[List[Decision], pd.DataFrame]:
    effects_map: Dict[Tuple[int, int, str], Dict[str, Any]] = {}
    if not change_effects_df.empty:
        for _, r in change_effects_df.iterrows():
            effects_map[(safe_int(r.get("ID кампании")), safe_int(r.get("Артикул WB")), str(r.get("placement", "")))] = r.to_dict()
    experiment_usage: Dict[str, int] = {}
    decisions: List[Decision] = []
    logic_rows: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        d = decide_for_row(row, cfg, effects_map, experiment_usage)
        if d.action == "TEST_UP":
            experiment_usage[d.product_root] = experiment_usage.get(d.product_root, 0) + 1
        decisions.append(d)
        logic_rows.append({
            "Дата расчёта": d.decision_date,
            "ID кампании": d.id_campaign,
            "Артикул WB": d.nm_id,
            "Товар": d.product_root,
            "Артикул продавца": d.supplier_article,
            "Предмет": d.subject,
            "Placement": d.placement,
            "Campaign type": d.campaign_type,
            "Режим": d.mode,
            "Текущая ставка, коп": d.current_bid_kopecks,
            "Comfort bid, коп": d.comfort_bid_kopecks,
            "Max bid, коп": d.max_bid_kopecks,
            "Experiment bid, коп": d.experiment_bid_kopecks,
            "Applied max, коп": d.applied_max_bid_kopecks,
            "Показы": safe_float(row.get("Показы")),
            "Клики": safe_float(row.get("Клики")),
            "Рекламные заказы": safe_float(row.get("Заказы")),
            "Рекламные расходы": safe_float(row.get("Расход")),
            "Рекламная выручка": safe_float(row.get("Сумма заказов")),
            "Все заказы товара": safe_float(row.get("root_total_orders")),
            "Вся выручка товара": safe_float(row.get("root_total_revenue")),
            "Blended DRR, %": safe_float(row.get("Blended_DRR_root")),
            "Ad DRR root, %": safe_float(row.get("Ad_DRR_root")),
            "Rating": safe_float(row.get("root_rating") or row.get("Рейтинг отзывов")),
            "Median position": safe_float(row.get("root_median_position") or row.get("Медианная позиция")),
            "Visibility %": safe_float(row.get("root_visibility") or row.get("Видимость %")),
            "ATC conversion %": safe_float(row.get("addToCartConversion")),
            "Cart-to-order %": safe_float(row.get("cartToOrderConversion")),
            "GP_realized": safe_float(row.get("GP_realized")),
            "Clicks per ad order": safe_float(row.get("ClicksPerAdOrder")),
            "Clicks per total order": safe_float(row.get("ClicksPerTotalOrder")),
            "Bid efficiency total": safe_float(row.get("TotalBidEfficiency")),
            "Действие": d.action,
            "Новая ставка, коп": d.new_bid_kopecks,
            "Причина": d.reason,
        })
    return decisions, pd.DataFrame(logic_rows)

# =========================================================
# WB API
# =========================================================
def placement_for_min_bids_api(placement: str) -> str:
    return "recommendation" if placement == "recommendations" else placement


def normalize_bid_for_wb(bid_kopecks: int, placement: str, cfg: Dict[str, Any], known_min_bid: Optional[int] = None) -> int:
    bid_kopecks = int(max(0, bid_kopecks))
    if known_min_bid is not None and known_min_bid > 0:
        return max(bid_kopecks, known_min_bid)
    if placement == "recommendations":
        return max(bid_kopecks, cfg["recommendations_min_kopecks"])
    return max(bid_kopecks, cfg["search_min_kopecks"])


def decisions_to_payload(decisions: List[Decision], cfg: Dict[str, Any]) -> Dict[str, Any]:
    grouped: Dict[int, List[Dict[str, Any]]] = {}
    for d in decisions:
        if d.action not in {"UP", "DOWN", "TEST_UP"}:
            continue
        if d.new_bid_kopecks <= 0 or d.new_bid_kopecks == d.current_bid_kopecks:
            continue
        grouped.setdefault(d.id_campaign, []).append({
            "nm_id": d.nm_id,
            "bid_kopecks": normalize_bid_for_wb(d.new_bid_kopecks, d.placement, cfg),
            "placement": d.placement,
        })
    return {"bids": [{"advert_id": advert_id, "nm_bids": nm_bids} for advert_id, nm_bids in grouped.items()]}


def send_batches(payload: Dict[str, Any], api_key: str, dry_run: bool = True) -> Tuple[int, int, pd.DataFrame]:
    rows: List[Dict[str, Any]] = []
    if dry_run:
        for advert in payload.get("bids", []):
            rows.append({
                "Дата": tz_now().strftime("%Y-%m-%d %H:%M:%S"),
                "ID кампании": advert.get("advert_id"),
                "Статус": "dry-run",
                "HTTP": "",
                "Ответ": json.dumps(advert, ensure_ascii=False),
            })
        return len(payload.get("bids", [])), 0, pd.DataFrame(rows)

    headers = {"Authorization": api_key.strip(), "Content-Type": "application/json"}
    success, failed = 0, 0
    for advert in payload.get("bids", []):
        body = {"bids": [advert]}
        status = "ok"
        http_status = ""
        response_txt = ""
        try:
            resp = requests.post(WB_BIDS_URL, headers=headers, json=body, timeout=120)
            http_status = resp.status_code
            response_txt = resp.text[:4000]
            if resp.status_code >= 400:
                failed += 1
                status = "failed"
            else:
                success += 1
        except Exception as e:
            failed += 1
            status = "exception"
            response_txt = str(e)
        rows.append({
            "Дата": tz_now().strftime("%Y-%m-%d %H:%M:%S"),
            "ID кампании": advert.get("advert_id"),
            "Статус": status,
            "HTTP": http_status,
            "Ответ": response_txt,
        })
        time.sleep(0.2)
    return success, failed, pd.DataFrame(rows)

# =========================================================
# СОХРАНЕНИЕ ИСТОРИИ / ОТЧЁТОВ
# =========================================================
def append_bid_history(s3: S3Storage, decisions: List[Decision]) -> pd.DataFrame:
    hist = load_bid_history(s3)
    add_rows = [
        {
            "Дата": d.decision_date,
            "ID кампании": d.id_campaign,
            "Артикул WB": d.nm_id,
            "product_root": d.product_root,
            "placement": d.placement,
            "Старая ставка, коп": d.current_bid_kopecks,
            "Новая ставка, коп": d.new_bid_kopecks,
            "Action": d.action,
            "Reason": d.reason,
        }
        for d in decisions if d.action in {"UP", "DOWN", "TEST_UP"} and d.new_bid_kopecks != d.current_bid_kopecks
    ]
    if add_rows:
        hist = pd.concat([hist, pd.DataFrame(add_rows)], ignore_index=True)
    s3.write_excel_sheets(SERVICE_BID_HISTORY_KEY, {"bid_history": hist})
    return hist


def save_outputs(
    s3: S3Storage,
    decisions: List[Decision],
    logic_df: pd.DataFrame,
    bid_limits_df: pd.DataFrame,
    root_metrics_df: pd.DataFrame,
    bid_eff_df: pd.DataFrame,
    weak_position_df: pd.DataFrame,
    change_effects_df: pd.DataFrame,
    send_log_df: pd.DataFrame,
    summary: Dict[str, Any],
) -> None:
    decisions_df = pd.DataFrame([asdict(d) for d in decisions])
    preview_sheets = {
        "Решения_по_ставкам": decisions_df,
        "Расчёт_логики": logic_df,
        "Статистика_по_товарам": root_metrics_df,
        "Эффективность_ставки": bid_eff_df,
        "Слабая_позиция": weak_position_df,
        "Эффект_изменений": change_effects_df,
        "Лимиты_ставок": bid_limits_df,
        "Отправка_WB": send_log_df,
    }
    s3.write_excel_sheets(SERVICE_PREVIEW_KEY, preview_sheets)
    s3.write_excel_sheets(SERVICE_DECISIONS_ARCHIVE_KEY, preview_sheets)
    s3.write_excel_sheets(SERVICE_LIMITS_KEY, {"bid_limits": bid_limits_df})
    s3.write_excel_sheets(SERVICE_ROOT_METRICS_KEY, {"product_root_metrics": root_metrics_df})
    s3.write_excel_sheets(SERVICE_BID_EFFICIENCY_KEY, {"bid_efficiency": bid_eff_df})
    s3.write_excel_sheets(SERVICE_WEAK_POSITION_KEY, {"weak_position": weak_position_df})
    s3.write_excel_sheets(SERVICE_CHANGE_EFFECTS_KEY, {"change_effects": change_effects_df})
    s3.write_text(SERVICE_LOG_KEY, json.dumps(summary, ensure_ascii=False, indent=2))

# =========================================================
# MAIN
# =========================================================
def load_config(s3: S3Storage) -> Dict[str, Any]:
    if not s3.file_exists(SERVICE_CONFIG_KEY):
        return DEFAULT_CONFIG.copy()
    try:
        cfg = json.loads(s3.read_text(SERVICE_CONFIG_KEY))
        out = DEFAULT_CONFIG.copy()
        out.update(cfg)
        return out
    except Exception:
        return DEFAULT_CONFIG.copy()


def save_config(s3: S3Storage, cfg: Dict[str, Any]) -> None:
    s3.write_text(SERVICE_CONFIG_KEY, json.dumps(cfg, ensure_ascii=False, indent=2))


def build_root_report(bid_limits_df: pd.DataFrame) -> pd.DataFrame:
    return bid_limits_df.groupby(["product_root", "subject"], as_index=False).agg(
        root_total_orders=("root_total_orders", "max"),
        root_ad_orders=("root_ad_orders", "max"),
        root_total_revenue=("root_total_revenue", "max"),
        root_ad_spend=("root_ad_spend", "max"),
        blended_drr=("Blended_DRR_root", "max"),
        ad_drr=("Ad_DRR_root", "max"),
        root_rating=("root_rating", "max"),
        root_median_position=("root_median_position", "max"),
        root_visibility=("root_visibility", "max"),
        mode=("mode", lambda s: first_notnull(s, "")),
        avg_max_bid_kopecks=("max_bid_kopecks", "mean"),
        avg_comfort_bid_kopecks=("comfort_bid_kopecks", "mean"),
    )


def build_weak_position_list(decisions: List[Decision], bid_eff_df: pd.DataFrame) -> pd.DataFrame:
    dec_df = pd.DataFrame([asdict(d) for d in decisions])
    if dec_df.empty:
        return pd.DataFrame()
    weak = dec_df[(dec_df["weak_position_flag"] == 1) | (dec_df["limit_reached_flag"] == 1)].copy()
    if weak.empty:
        weak = dec_df[(dec_df["action"] == "LIMIT_REACHED")].copy()
    if weak.empty:
        return weak
    cols = ["ID кампании", "nm_id", "placement", "TotalBidEfficiency", "bid_eff_comment", "Blended_DRR_root", "root_median_position", "root_visibility"]
    eff = bid_eff_df[[c for c in cols if c in bid_eff_df.columns]].rename(columns={"nm_id": "nm_id"})
    weak = weak.merge(eff, left_on=["id_campaign", "nm_id", "placement"], right_on=["ID кампании", "nm_id", "placement"], how="left")
    weak["Комментарий"] = "Повысить эффективность ставки — реклама работает на пределе"
    return weak


def run_pipeline(args: argparse.Namespace) -> Dict[str, Any]:
    access_key = os.environ.get("YC_ACCESS_KEY_ID", "")
    secret_key = os.environ.get("YC_SECRET_ACCESS_KEY", "")
    bucket_name = os.environ.get("YC_BUCKET_NAME", "")
    wb_api_key = os.environ.get("WB_PROMO_KEY_TOPFACE", "")
    if not all([access_key, secret_key, bucket_name]):
        raise RuntimeError("Не заданы YC_ACCESS_KEY_ID / YC_SECRET_ACCESS_KEY / YC_BUCKET_NAME")
    s3 = S3Storage(access_key, secret_key, bucket_name)
    cfg = load_config(s3)

    ads_df, campaigns_df = load_ads_and_campaigns(s3)
    max_ads_dt, mature_start, mature_end = determine_mature_window(ads_df)
    log(f"Берём зрелое окно: {mature_start} .. {mature_end} (max дата в рекламе: {max_ads_dt})")

    economics_df = load_economics(s3)
    orders_df = load_orders_for_period(s3, mature_start, mature_end)
    funnel_df = load_funnel(s3, mature_start, mature_end)
    keywords_df = load_keywords_for_period(s3, mature_end)
    base_df, _, _ = build_base_dataset(ads_df, campaigns_df, economics_df, orders_df, funnel_df, keywords_df, mature_start, mature_end)
    if base_df.empty:
        raise RuntimeError("После объединения данных не осталось строк для расчёта")
    bid_limits_df = compute_bid_limits(base_df, cfg)
    bid_eff_df = compute_bid_efficiency(bid_limits_df)
    bid_history_df = load_bid_history(s3)
    change_effects_df = compute_change_effects(bid_eff_df, bid_history_df)
    decisions, logic_df = build_decisions(bid_eff_df, cfg, change_effects_df)

    # Отчёты
    root_report_df = build_root_report(bid_eff_df)
    weak_position_df = build_weak_position_list(decisions, bid_eff_df)

    dry_run = True if args.mode in {"preview", "report"} or not args.apply else False
    payload = decisions_to_payload(decisions, cfg)
    send_log_df = pd.DataFrame()
    success = failed = 0
    if args.mode in {"preview", "run"}:
        success, failed, send_log_df = send_batches(payload, wb_api_key, dry_run=dry_run)
    hist_df = append_bid_history(s3, decisions)

    summary = {
        "generated_at": tz_now().isoformat(),
        "mode": args.mode,
        "apply": bool(args.apply),
        "dry_run": dry_run,
        "mature_start": str(mature_start),
        "mature_end": str(mature_end),
        "ads_rows": int(len(ads_df)),
        "base_rows": int(len(base_df)),
        "decisions_total": int(len(decisions)),
        "actions": pd.Series([d.action for d in decisions]).value_counts().to_dict(),
        "wb_send_success": int(success),
        "wb_send_failed": int(failed),
        "preview_key": SERVICE_PREVIEW_KEY,
    }

    save_outputs(
        s3=s3,
        decisions=decisions,
        logic_df=logic_df,
        bid_limits_df=bid_eff_df,
        root_metrics_df=root_report_df,
        bid_eff_df=bid_eff_df,
        weak_position_df=weak_position_df,
        change_effects_df=change_effects_df,
        send_log_df=send_log_df,
        summary=summary,
    )
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="WB Ads Manager v2 — новая логика управления ставками")
    parser.add_argument("mode", nargs="?", default="preview", choices=["preview", "report", "run", "set-config"], help="Режим работы")
    parser.add_argument("--apply", action="store_true", help="Реально отправлять ставки в WB")
    parser.add_argument("--config-json", default="", help="JSON для сохранения config в режиме set-config")
    args = parser.parse_args()

    access_key = os.environ.get("YC_ACCESS_KEY_ID", "")
    secret_key = os.environ.get("YC_SECRET_ACCESS_KEY", "")
    bucket_name = os.environ.get("YC_BUCKET_NAME", "")
    if args.mode == "set-config":
        s3 = S3Storage(access_key, secret_key, bucket_name)
        cfg = DEFAULT_CONFIG.copy()
        if args.config_json:
            cfg.update(json.loads(args.config_json))
        save_config(s3, cfg)
        log(f"Конфиг сохранён в {SERVICE_CONFIG_KEY}")
        return

    try:
        summary = run_pipeline(args)
        log("Готово")
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    except Exception as e:
        log(f"ОШИБКА: {e}")
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
