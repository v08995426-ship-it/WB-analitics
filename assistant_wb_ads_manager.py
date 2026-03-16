#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import io
import re
import json
import time
import argparse
import tempfile
import traceback
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple, Any, Union

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

SERVICE_CONFIG_KEY = f"Служебные файлы/Ассистент WB/{STORE_NAME}/strategy_config.json"
SERVICE_HISTORY_KEY = f"Служебные файлы/Ассистент WB/{STORE_NAME}/bid_history.xlsx"
SERVICE_PREVIEW_KEY = f"Служебные файлы/Ассистент WB/{STORE_NAME}/preview_last_run.xlsx"
SERVICE_LOG_KEY = f"Служебные файлы/Ассистент WB/{STORE_NAME}/last_run_summary.json"
SERVICE_SCHEDULE_KEY = f"Служебные файлы/Ассистент WB/{STORE_NAME}/strategy_schedule.xlsx"
SERVICE_EFFECTIVENESS_KEY = f"Служебные файлы/Ассистент WB/{STORE_NAME}/strategy_effectiveness.xlsx"
SERVICE_DECISIONS_ARCHIVE_KEY = f"Служебные файлы/Ассистент WB/{STORE_NAME}/decision_archive.xlsx"

WB_BIDS_URL = "https://advert-api.wildberries.ru/api/advert/v1/bids"
WB_BIDS_MIN_URL = "https://advert-api.wildberries.ru/api/advert/v1/bids/min"

ACTIVE_STATUS_VALUES = {"активна", "active", "запущена", "started", "4", "9", "11"}

STEP_CPC_SMALL = 100
STEP_CPC_MED = 200
STEP_CPC_BIG = 300

STEP_CPM_SMALL = 500
STEP_CPM_MED = 1000
STEP_CPM_BIG = 1500

MIN_CPC_SEARCH = 400
MAX_CPC_SEARCH = 15000

MIN_CPM_SEARCH = 4000
MAX_CPM_SEARCH = 70000

MIN_CPM_SHELVES = 8000
MAX_CPM_SHELVES = 120000

BAD_RATING_THRESHOLD = 4.6
OK_RATING_THRESHOLD = 4.7
GOOD_RATING_THRESHOLD = 4.8

TOP10_BORDER = 10
TOP20_BORDER = 20

MIN_CTR_SHELVES = 0.40
GOOD_CTR_SHELVES = 0.80

MIN_NET_PROFIT_PER_UNIT = 0.0
MIN_KEYWORD_ORDERS_FOR_PUSH = 2

MICRO_SPEND_IGNORE = 10.0
MICRO_AD_PROFIT_IGNORE = -5.0

DEFAULT_STRATEGY_SEQUENCE = [1, 2, 3, 4]

FALLBACK_BID_STEP_KOPECKS = 100
MAX_RETRY_ROUNDS = 10

STRATEGY_NAMES = {
    1: "Максимизация прибыли",
    2: "Удержание / рост позиции",
    3: "Контроль ДРР",
    4: "Доля трафика",
}

DEFAULT_CONFIG = {
    "mode": "rotation",
    "active_strategy": 1,
    "strategy_sequence": DEFAULT_STRATEGY_SEQUENCE,
    "evaluation_lag_weeks": 1,
    "final_evaluation_lag_weeks": 2,
    "target_drr_cpc": 15.0,
    "target_drr_cpm_search": 16.0,
    "target_drr_cpm_shelves": 12.0,
    "max_drr_cpc": 22.0,
    "max_drr_cpm_search": 24.0,
    "max_drr_cpm_shelves": 18.0,
}

# =========================================================
# ЛОГ
# =========================================================
def log(msg: str):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)

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

    def read_text(self, key: str) -> str:
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        return obj["Body"].read().decode("utf-8")

    def write_text(self, key: str, text: str):
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=text.encode("utf-8"))

    def read_excel(self, key: str, sheet_name=0):
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        data = obj["Body"].read()
        return pd.read_excel(io.BytesIO(data), sheet_name=sheet_name)

    def read_excel_all_sheets(self, key: str) -> Dict[str, pd.DataFrame]:
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        data = obj["Body"].read()
        return pd.read_excel(io.BytesIO(data), sheet_name=None)

    def write_excel_sheets(self, key: str, sheets: Dict[str, pd.DataFrame]):
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            with pd.ExcelWriter(tmp_path, engine="openpyxl") as writer:
                for sheet_name, df in sheets.items():
                    safe_sheet_name = str(sheet_name)[:31]
                    if df is None:
                        df = pd.DataFrame()
                    df.to_excel(writer, index=False, sheet_name=safe_sheet_name)
            self.s3.upload_file(tmp_path, self.bucket, key)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

# =========================================================
# УТИЛИТЫ
# =========================================================
def tz_now() -> datetime:
    return datetime.now(pytz.timezone(TIMEZONE))

def safe_float(v, default=0.0) -> float:
    try:
        if pd.isna(v):
            return default
        return float(v)
    except Exception:
        return default

def safe_int(v, default=0) -> int:
    try:
        if pd.isna(v):
            return default
        return int(float(v))
    except Exception:
        return default

def normalize_colname(name: str) -> str:
    return str(name).strip().lower().replace("ё", "е")

def percent(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)

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

def target_week_range(explicit_week: Optional[str] = None) -> Tuple[str, date, date]:
    if explicit_week:
        start = week_start_from_label(explicit_week)
        return explicit_week, start, start + timedelta(days=6)
    yesterday = tz_now().date() - timedelta(days=1)
    start = yesterday - timedelta(days=yesterday.weekday())
    return iso_week_label(start), start, start + timedelta(days=6)

def ads_weekly_key(week_label: str) -> str:
    return f"{ADS_WEEKLY_PREFIX}Реклама_{week_label}.xlsx"

def keywords_weekly_key(week_label: str) -> str:
    return f"{KEYWORDS_WEEKLY_PREFIX}Неделя {week_label}.xlsx"

def read_json_or_default(s3: S3Storage, key: str, default: dict) -> dict:
    try:
        if not s3.file_exists(key):
            return default
        return json.loads(s3.read_text(key))
    except Exception:
        return default

def clamp(v: int, min_v: int, max_v: int) -> int:
    return max(min_v, min(v, max_v))

def is_micro_noise(row: pd.Series) -> bool:
    spend = safe_float(row.get("Расход за неделю"))
    ad_profit = safe_float(row.get("Ожидаемая чистая прибыль рекламы, руб"))
    orders = safe_float(row.get("Заказы за неделю"))
    drr = safe_float(row.get("ДРР, % факт"))
    if spend < MICRO_SPEND_IGNORE and ad_profit > MICRO_AD_PROFIT_IGNORE:
        return True
    if spend < MICRO_SPEND_IGNORE and orders <= 0 and drr == 0:
        return True
    return False

# =========================================================
# КАМПАНИИ
# =========================================================
def classify_campaign(row: pd.Series) -> str:
    payment_type = str(row.get("Тип оплаты", "")).strip().lower()
    bid_type = str(row.get("Тип ставки", "")).strip().lower()
    rec_flag = str(row.get("Размещение в рекомендациях", "")).strip().lower()
    if payment_type == "cpc":
        return "cpc_search"
    if payment_type == "cpm":
        if bid_type == "unified":
            return "cpm_shelves"
        return "cpm_search"
    if bid_type == "manual" and payment_type == "cpc":
        if rec_flag in {"да", "true", "1"}:
            return "manual_mixed"
        return "manual_search"
    return "unknown"

def is_active_campaign(row: pd.Series) -> bool:
    status = str(row.get("Статус", "")).strip().lower()
    return status in ACTIVE_STATUS_VALUES

# =========================================================
# ЧТЕНИЕ РЕКЛАМЫ
# =========================================================
def prepare_daily_stats_sheet(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    aliases = {
        "ID кампании": ["ID кампании", "ID", "Кампания ID", "advert_id"],
        "Артикул WB": ["Артикул WB", "Артикул", "nm_id"],
        "Дата": ["Дата", "День", "date"],
        "Расход": ["Расход", "Затраты", "spent"],
        "Сумма заказов": ["Сумма заказов", "Выручка", "orders_sum"],
        "Показы": ["Показы", "Просмотры", "views"],
        "Клики": ["Клики", "clicks"],
        "Заказы": ["Заказы", "orders"],
        "Название": ["Название", "Название кампании", "Кампания"],
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
    required = ["ID кампании", "Артикул WB", "Дата", "Расход"]
    if not all(col in df.columns for col in required):
        return pd.DataFrame()
    for col in ["Показы", "Клики", "Заказы", "Сумма заказов"]:
        if col not in df.columns:
            df[col] = 0
    df["Дата"] = pd.to_datetime(df["Дата"], errors="coerce").dt.date
    for col in ["ID кампании", "Артикул WB"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ["Расход", "Показы", "Клики", "Заказы", "Сумма заказов"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    df = df.dropna(subset=["ID кампании", "Артикул WB", "Дата"]).copy()
    df["ID кампании"] = df["ID кампании"].astype("int64")
    df["Артикул WB"] = df["Артикул WB"].astype("int64")
    return df

def prepare_campaigns_sheet(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    aliases = {
        "ID кампании": ["ID кампании", "ID", "Кампания ID", "advert_id"],
        "Тип оплаты": ["Тип оплаты", "payment_type"],
        "Тип ставки": ["Тип ставки", "bid_type"],
        "Ставка в поиске (руб)": ["Ставка в поиске (руб)", "Ставка", "bid"],
        "Ставка в рекомендациях (руб)": ["Ставка в рекомендациях (руб)", "Ставка в рекомендациях"],
        "Название": ["Название", "Название кампании", "Кампания"],
        "Статус": ["Статус", "status"],
        "Размещение в рекомендациях": ["Размещение в рекомендациях", "recommendations"],
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
    required = ["ID кампании", "Тип оплаты", "Название"]
    if not all(col in df.columns for col in required):
        return pd.DataFrame()
    for col in ["Ставка в поиске (руб)", "Ставка в рекомендациях (руб)", "Статус", "Размещение в рекомендациях", "Тип ставки"]:
        if col not in df.columns:
            df[col] = ""
    df["ID кампании"] = pd.to_numeric(df["ID кампании"], errors="coerce")
    df = df.dropna(subset=["ID кампании"]).copy()
    df["ID кампании"] = df["ID кампании"].astype("int64")
    df["Ставка в поиске (руб)"] = pd.to_numeric(df["Ставка в поиске (руб)"], errors="coerce").fillna(0)
    df["Ставка в рекомендациях (руб)"] = pd.to_numeric(df["Ставка в рекомендациях (руб)"], errors="coerce").fillna(0)
    return df

def load_advertising_data(s3: S3Storage, week_label: str, week_start: date, week_end: date) -> Tuple[pd.DataFrame, pd.DataFrame]:
    log(f"📣 Загрузка рекламы за неделю {week_label} ({week_start} — {week_end})")
    sheets = {}
    try:
        if s3.file_exists(ADS_ANALYSIS_KEY):
            sheets = s3.read_excel_all_sheets(ADS_ANALYSIS_KEY)
    except Exception as e:
        log(f"⚠️ Не удалось прочитать основной файл рекламы: {e}")
    stats_df = pd.DataFrame()
    campaigns_df = pd.DataFrame()
    if sheets:
        if "Статистика_Ежедневно" in sheets:
            stats_df = prepare_daily_stats_sheet(sheets["Статистика_Ежедневно"].copy())
        else:
            for _, df in sheets.items():
                prepared = prepare_daily_stats_sheet(df.copy())
                if not prepared.empty:
                    stats_df = prepared
                    break
        if "Список_кампаний" in sheets:
            campaigns_df = prepare_campaigns_sheet(sheets["Список_кампаний"].copy())
        else:
            for _, df in sheets.items():
                prepared = prepare_campaigns_sheet(df.copy())
                if not prepared.empty:
                    campaigns_df = prepared
                    break
    if stats_df.empty:
        wk_key = ads_weekly_key(week_label)
        if s3.file_exists(wk_key):
            log(f"ℹ️ Беру статистику рекламы из weekly файла: {wk_key}")
            wk_sheets = s3.read_excel_all_sheets(wk_key)
            if "Статистика_Ежедневно" in wk_sheets:
                stats_df = prepare_daily_stats_sheet(wk_sheets["Статистика_Ежедневно"].copy())
            else:
                for _, df in wk_sheets.items():
                    prepared = prepare_daily_stats_sheet(df.copy())
                    if not prepared.empty:
                        stats_df = prepared
                        break
            if campaigns_df.empty and "Список_кампаний" in wk_sheets:
                campaigns_df = prepare_campaigns_sheet(wk_sheets["Список_кампаний"].copy())
    if stats_df.empty:
        raise RuntimeError("Не удалось найти лист статистики рекламы")
    if campaigns_df.empty:
        raise RuntimeError("Не удалось найти лист списка кампаний")
    stats_df = stats_df[(stats_df["Дата"] >= week_start) & (stats_df["Дата"] <= week_end)].copy()
    log(f"✅ Статистика рекламы за неделю: строк {len(stats_df)}")
    log(f"✅ Список кампаний: {len(campaigns_df)}")
    return stats_df, campaigns_df

# =========================================================
# ЭКОНОМИКА
# =========================================================
def load_unit_economics(s3: S3Storage, week_label: str) -> pd.DataFrame:
    if not s3.file_exists(ECONOMICS_KEY):
        raise RuntimeError(f"Не найден файл экономики: {ECONOMICS_KEY}")
    sheets = s3.read_excel_all_sheets(ECONOMICS_KEY)
    if "Юнит экономика" not in sheets:
        raise RuntimeError("В Экономика.xlsx нет листа 'Юнит экономика'")
    df = sheets["Юнит экономика"].copy()
    if df.empty:
        raise RuntimeError("Лист 'Юнит экономика' пустой")
    df["Неделя"] = df["Неделя"].astype(str)
    df["Артикул WB"] = pd.to_numeric(df["Артикул WB"], errors="coerce")
    df = df[df["Неделя"] == week_label].dropna(subset=["Артикул WB"]).copy()
    df["Артикул WB"] = df["Артикул WB"].astype("int64")
    numeric_cols = [
        "Продажи, шт", "Возвраты, шт", "Чистые продажи, шт", "Процент выкупа",
        "Средняя цена продажи", "Средняя цена покупателя", "СПП, %",
        "Комиссия WB, %", "Эквайринг, %", "Комиссия WB, руб/ед", "Эквайринг, руб/ед",
        "Логистика прямая, руб/ед", "Логистика обратная, руб/ед", "Хранение, руб/ед",
        "Приёмка, руб/ед", "Штрафы и удержания, руб/ед", "Реклама, руб/ед",
        "Себестоимость, руб", "НДС, руб/ед", "Валовая прибыль, руб/ед", "Чистая прибыль, руб/ед",
        "Валовая рентабельность, %", "Чистая рентабельность, %"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    log(f"✅ Юнит-экономика за {week_label}: SKU {len(df)}")
    return df

# =========================================================
# ПОИСКОВЫЕ ЗАПРОСЫ
# =========================================================
def load_keywords_weekly(s3: S3Storage, week_label: str) -> pd.DataFrame:
    key = keywords_weekly_key(week_label)
    if not s3.file_exists(key):
        log(f"ℹ️ Нет weekly файла поисковых запросов: {key}")
        return pd.DataFrame()
    try:
        df = s3.read_excel(key, sheet_name=0)
    except Exception as e:
        log(f"⚠️ Ошибка чтения weekly keywords: {e}")
        return pd.DataFrame()
    if df.empty:
        return pd.DataFrame()
    required = ["Артикул WB", "Рейтинг отзывов", "Частота запросов", "Переходы в карточку", "Заказы", "Медианная позиция", "Поисковый запрос"]
    if not all(col in df.columns for col in required):
        log("ℹ️ В weekly keywords нет полного набора колонок, продолжаю без анализа ключей")
        return pd.DataFrame()
    df["Артикул WB"] = pd.to_numeric(df["Артикул WB"], errors="coerce")
    df = df.dropna(subset=["Артикул WB"]).copy()
    df["Артикул WB"] = df["Артикул WB"].astype("int64")
    for col in ["Рейтинг отзывов", "Частота запросов", "Переходы в карточку", "Заказы", "Медианная позиция"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df

def aggregate_keywords(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=[
            "Артикул WB", "Рейтинг отзывов", "Частота запросов сумма", "Переходы в карточку сумма",
            "Заказы по ключам сумма", "Медианная позиция заказных ключей", "Доля трафика, %", "Флаг плохого рейтинга"
        ])
    rows = []
    for nm_id, g in df.groupby("Артикул WB"):
        total_freq = safe_float(g["Частота запросов"].sum())
        total_clicks = safe_float(g["Переходы в карточку"].sum())
        total_orders = safe_float(g["Заказы"].sum())
        g_orders = g[g["Заказы"] > 0].copy()
        if g_orders.empty:
            median_pos = safe_float(g["Медианная позиция"].replace(0, pd.NA).mean())
        else:
            weights = g_orders["Заказы"].replace(0, 1)
            median_pos = safe_float((g_orders["Медианная позиция"] * weights).sum() / weights.sum())
        rating = safe_float(g["Рейтинг отзывов"].replace(0, pd.NA).mean())
        traffic_share = percent(total_clicks, total_freq) if total_freq > 0 else 0.0
        rows.append({
            "Артикул WB": nm_id,
            "Рейтинг отзывов": round(rating, 2),
            "Частота запросов сумма": round(total_freq, 2),
            "Переходы в карточку сумма": round(total_clicks, 2),
            "Заказы по ключам сумма": round(total_orders, 2),
            "Медианная позиция заказных ключей": round(median_pos, 2),
            "Доля трафика, %": round(traffic_share, 2),
            "Флаг плохого рейтинга": "да" if rating and rating < BAD_RATING_THRESHOLD else "нет",
        })
    return pd.DataFrame(rows)

# =========================================================
# HISTORY / SCHEDULE / EFFECTIVENESS / ARCHIVE
# =========================================================
def load_bid_history(s3: S3Storage) -> pd.DataFrame:
    cols = [
        "Дата запуска", "Неделя", "ID кампании", "Артикул WB", "Тип кампании",
        "Ставка поиск, коп", "Ставка рекомендации, коп", "Стратегия"
    ]
    if not s3.file_exists(SERVICE_HISTORY_KEY):
        return pd.DataFrame(columns=cols)
    try:
        df = s3.read_excel(SERVICE_HISTORY_KEY, sheet_name=0)
        return df if not df.empty else pd.DataFrame(columns=cols)
    except Exception:
        return pd.DataFrame(columns=cols)

def save_bid_history(s3: S3Storage, history_df: pd.DataFrame):
    s3.write_excel_sheets(SERVICE_HISTORY_KEY, {"history": history_df})

def load_schedule(s3: S3Storage) -> pd.DataFrame:
    cols = ["Неделя", "Стратегия", "Название стратегии", "Статус", "Дата назначения"]
    if not s3.file_exists(SERVICE_SCHEDULE_KEY):
        return pd.DataFrame(columns=cols)
    try:
        df = s3.read_excel(SERVICE_SCHEDULE_KEY, sheet_name=0)
        return df if not df.empty else pd.DataFrame(columns=cols)
    except Exception:
        return pd.DataFrame(columns=cols)

def save_schedule(s3: S3Storage, df: pd.DataFrame):
    s3.write_excel_sheets(SERVICE_SCHEDULE_KEY, {"schedule": df})

def load_effectiveness(s3: S3Storage) -> pd.DataFrame:
    cols = [
        "Неделя", "Стратегия", "Название стратегии", "Тип оценки",
        "Расход за неделю", "Сумма заказов за неделю", "Заказы за неделю",
        "ДРР, % факт", "CTR, % факт", "CR, % факт",
        "Ожидаемая чистая прибыль рекламы, руб", "Средняя чистая прибыль, руб/ед",
        "Средняя позиция", "Средняя доля трафика, %", "Средний рейтинг", "Вывод"
    ]
    if not s3.file_exists(SERVICE_EFFECTIVENESS_KEY):
        return pd.DataFrame(columns=cols)
    try:
        df = s3.read_excel(SERVICE_EFFECTIVENESS_KEY, sheet_name=0)
        return df if not df.empty else pd.DataFrame(columns=cols)
    except Exception:
        return pd.DataFrame(columns=cols)

def save_effectiveness(s3: S3Storage, df: pd.DataFrame):
    s3.write_excel_sheets(SERVICE_EFFECTIVENESS_KEY, {"effectiveness": df})

def load_decision_archive(s3: S3Storage) -> Dict[str, pd.DataFrame]:
    if not s3.file_exists(SERVICE_DECISIONS_ARCHIVE_KEY):
        return {"Решения": pd.DataFrame(), "Расчёт логики": pd.DataFrame(), "Отправка WB": pd.DataFrame()}
    try:
        sheets = s3.read_excel_all_sheets(SERVICE_DECISIONS_ARCHIVE_KEY)
        for name in ["Решения", "Расчёт логики", "Отправка WB"]:
            if name not in sheets:
                sheets[name] = pd.DataFrame()
        return sheets
    except Exception:
        return {"Решения": pd.DataFrame(), "Расчёт логики": pd.DataFrame(), "Отправка WB": pd.DataFrame()}

def save_decision_archive(s3: S3Storage, decisions_df: pd.DataFrame, logic_df: pd.DataFrame, send_log_df: pd.DataFrame):
    current = load_decision_archive(s3)
    new_decisions = pd.concat([current["Решения"], decisions_df], ignore_index=True) if decisions_df is not None else current["Решения"]
    new_logic = pd.concat([current["Расчёт логики"], logic_df], ignore_index=True) if logic_df is not None else current["Расчёт логики"]
    new_send = pd.concat([current["Отправка WB"], send_log_df], ignore_index=True) if send_log_df is not None else current["Отправка WB"]
    if len(new_decisions) > 50000:
        new_decisions = new_decisions.tail(50000).reset_index(drop=True)
    if len(new_logic) > 50000:
        new_logic = new_logic.tail(50000).reset_index(drop=True)
    if len(new_send) > 50000:
        new_send = new_send.tail(50000).reset_index(drop=True)
    s3.write_excel_sheets(SERVICE_DECISIONS_ARCHIVE_KEY, {
        "Решения": new_decisions,
        "Расчёт логики": new_logic,
        "Отправка WB": new_send,
    })

# =========================================================
# METRICS
# =========================================================
def build_campaign_week_metrics(stats_df: pd.DataFrame, campaigns_df: pd.DataFrame, economics_df: pd.DataFrame, keywords_agg_df: pd.DataFrame) -> pd.DataFrame:
    g = stats_df.groupby(["ID кампании", "Артикул WB"], as_index=False).agg(
        **{
            "Расход за неделю": ("Расход", "sum"),
            "Сумма заказов за неделю": ("Сумма заказов", "sum"),
            "Показы за неделю": ("Показы", "sum"),
            "Клики за неделю": ("Клики", "sum"),
            "Заказы за неделю": ("Заказы", "sum"),
        }
    )
    g["CTR, % факт"] = g.apply(lambda r: percent(r["Клики за неделю"], r["Показы за неделю"]), axis=1)
    g["CR, % факт"] = g.apply(lambda r: percent(r["Заказы за неделю"], r["Клики за неделю"]), axis=1)
    g["CPC, руб факт"] = g.apply(lambda r: round(safe_float(r["Расход за неделю"]) / safe_float(r["Клики за неделю"]), 2) if r["Клики за неделю"] > 0 else 0.0, axis=1)
    g["ДРР, % факт"] = g.apply(lambda r: percent(r["Расход за неделю"], r["Сумма заказов за неделю"]) if r["Сумма заказов за неделю"] > 0 else 0.0, axis=1)
    c = campaigns_df.copy()
    c["Тип кампании"] = c.apply(classify_campaign, axis=1)
    c["Активна"] = c.apply(is_active_campaign, axis=1)
    keep_cols = ["ID кампании", "Название", "Тип оплаты", "Тип ставки", "Статус", "Размещение в рекомендациях", "Ставка в поиске (руб)", "Ставка в рекомендациях (руб)", "Тип кампании", "Активна"]
    c = c[keep_cols].drop_duplicates(subset=["ID кампании"])
    m = g.merge(c, on="ID кампании", how="left")
    econ_cols = ["Артикул WB", "Артикул продавца", "Предмет", "Бренд", "Средняя цена продажи", "Чистая прибыль, руб/ед", "Валовая прибыль, руб/ед", "Себестоимость, руб", "Реклама, руб/ед", "Комиссия WB, %", "Эквайринг, %", "Логистика прямая, руб/ед", "Логистика обратная, руб/ед"]
    e = economics_df[econ_cols].drop_duplicates(subset=["Артикул WB"])
    m = m.merge(e, on="Артикул WB", how="left")
    if not keywords_agg_df.empty:
        m = m.merge(keywords_agg_df, on="Артикул WB", how="left")
    else:
        for col in ["Рейтинг отзывов", "Частота запросов сумма", "Переходы в карточку сумма", "Заказы по ключам сумма", "Медианная позиция заказных ключей", "Доля трафика, %", "Флаг плохого рейтинга"]:
            m[col] = 0 if "Флаг" not in col else "нет"
    m["Ожидаемая чистая прибыль рекламы, руб"] = m["Заказы за неделю"].fillna(0) * m["Чистая прибыль, руб/ед"].fillna(0) - m["Расход за неделю"].fillna(0)
    m["Профит на заказ после рекламы, руб"] = m.apply(lambda r: round(safe_float(r["Чистая прибыль, руб/ед"]) - safe_float(r["Расход за неделю"]) / safe_float(r["Заказы за неделю"]), 2) if r["Заказы за неделю"] > 0 else round(-safe_float(r["Расход за неделю"]), 2), axis=1)
    m["Текущая ставка поиск, коп"] = (m["Ставка в поиске (руб)"].fillna(0) * 100).round().astype(int)
    m["Текущая ставка рекомендации, коп"] = (m["Ставка в рекомендациях (руб)"].fillna(0) * 100).round().astype(int)
    m = m[m["Активна"] == True].copy()
    m = m.sort_values(["Расход за неделю", "Заказы за неделю"], ascending=[False, False]).reset_index(drop=True)
    return m

# =========================================================
# STRATEGIES
# =========================================================
@dataclass
class Decision:
    strategy_id: int
    strategy_name: str
    id_campaign: int
    nm_id: int
    campaign_name: str
    campaign_type: str
    action: str
    reason: str
    current_search_bid_kop: int
    new_search_bid_kop: int
    current_rec_bid_kop: int
    new_rec_bid_kop: int
    week_label: str

def stop_factors(row: pd.Series) -> Optional[str]:
    rating = safe_float(row.get("Рейтинг отзывов"))
    profit_u = safe_float(row.get("Чистая прибыль, руб/ед"))
    if rating and rating < BAD_RATING_THRESHOLD:
        return f"Низкий рейтинг отзывов {rating:.2f}"
    if profit_u <= MIN_NET_PROFIT_PER_UNIT:
        return f"Чистая прибыль на единицу {profit_u:.2f} ≤ {MIN_NET_PROFIT_PER_UNIT:.2f}"
    return None

def make_logic_row(row: pd.Series, strategy_id: int, reason: str, action: str) -> Dict[str, Any]:
    return {
        "Дата расчёта": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Неделя": row.get("Неделя", ""),
        "Стратегия": strategy_id,
        "Название стратегии": STRATEGY_NAMES.get(strategy_id, ""),
        "ID кампании": safe_int(row.get("ID кампании")),
        "Название кампании": str(row.get("Название", "")),
        "Артикул WB": safe_int(row.get("Артикул WB")),
        "Тип кампании": str(row.get("Тип кампании", "")),
        "Расход за неделю": safe_float(row.get("Расход за неделю")),
        "Сумма заказов за неделю": safe_float(row.get("Сумма заказов за неделю")),
        "Заказы за неделю": safe_float(row.get("Заказы за неделю")),
        "CTR, % факт": safe_float(row.get("CTR, % факт")),
        "CR, % факт": safe_float(row.get("CR, % факт")),
        "ДРР, % факт": safe_float(row.get("ДРР, % факт")),
        "Ожидаемая чистая прибыль рекламы, руб": safe_float(row.get("Ожидаемая чистая прибыль рекламы, руб")),
        "Чистая прибыль, руб/ед": safe_float(row.get("Чистая прибыль, руб/ед")),
        "Рейтинг отзывов": safe_float(row.get("Рейтинг отзывов")),
        "Медианная позиция заказных ключей": safe_float(row.get("Медианная позиция заказных ключей")),
        "Доля трафика, %": safe_float(row.get("Доля трафика, %")),
        "Действие": action,
        "Причина": reason,
    }

def decide_profit_strategy(row: pd.Series, config: dict, week_label: str) -> Optional[Decision]:
    ctype = str(row.get("Тип кампании"))
    rating = safe_float(row.get("Рейтинг отзывов"))
    pos = safe_float(row.get("Медианная позиция заказных ключей"))
    ad_profit = safe_float(row.get("Ожидаемая чистая прибыль рекламы, руб"))
    drr = safe_float(row.get("ДРР, % факт"))
    profit_u = safe_float(row.get("Чистая прибыль, руб/ед"))
    orders_kw = safe_float(row.get("Заказы по ключам сумма"))
    if is_micro_noise(row):
        return None
    sf = stop_factors(row)
    if sf:
        if ctype == "cpm_shelves":
            current = safe_int(row.get("Текущая ставка рекомендации, коп"))
            new = clamp(current - STEP_CPM_BIG, MIN_CPM_SHELVES, MAX_CPM_SHELVES)
            if new == current:
                return None
            return Decision(1, STRATEGY_NAMES[1], safe_int(row["ID кампании"]), safe_int(row["Артикул WB"]),
                            str(row.get("Название", "")), ctype, "DOWN", sf, 0, 0, current, new, week_label)
        else:
            current = safe_int(row.get("Текущая ставка поиск, коп"))
            min_v = MIN_CPC_SEARCH if ctype == "cpc_search" else MIN_CPM_SEARCH
            max_v = MAX_CPC_SEARCH if ctype == "cpc_search" else MAX_CPM_SEARCH
            step = STEP_CPC_BIG if ctype == "cpc_search" else STEP_CPM_BIG
            new = clamp(current - step, min_v, max_v)
            if new == current:
                return None
            return Decision(1, STRATEGY_NAMES[1], safe_int(row["ID кампании"]), safe_int(row["Артикул WB"]),
                            str(row.get("Название", "")), ctype, "DOWN", sf, current, new, 0, 0, week_label)
    if ctype == "cpm_shelves":
        current = safe_int(row.get("Текущая ставка рекомендации, коп"))
        ctr = safe_float(row.get("CTR, % факт"))
        if ad_profit < 0 or drr > config["target_drr_cpm_shelves"]:
            new = clamp(current - STEP_CPM_MED, MIN_CPM_SHELVES, MAX_CPM_SHELVES)
            if new != current:
                return Decision(1, STRATEGY_NAMES[1], safe_int(row["ID кампании"]), safe_int(row["Артикул WB"]),
                                str(row.get("Название", "")), ctype, "DOWN",
                                f"Полки: прибыль рекламы {ad_profit:.2f}, ДРР {drr:.2f}%",
                                0, 0, current, new, week_label)
        elif ad_profit > 0 and ctr >= GOOD_CTR_SHELVES and profit_u > 0:
            new = clamp(current + STEP_CPM_SMALL, MIN_CPM_SHELVES, MAX_CPM_SHELVES)
            if new != current:
                return Decision(1, STRATEGY_NAMES[1], safe_int(row["ID кампании"]), safe_int(row["Артикул WB"]),
                                str(row.get("Название", "")), ctype, "UP",
                                f"Полки прибыльны, CTR {ctr:.2f}%",
                                0, 0, current, new, week_label)
        return None
    current = safe_int(row.get("Текущая ставка поиск, коп"))
    if ctype == "cpm_search":
        step_up = STEP_CPM_SMALL
        step_down = STEP_CPM_MED
        min_v, max_v = MIN_CPM_SEARCH, MAX_CPM_SEARCH
        drr_target = config["target_drr_cpm_search"]
        drr_max = config["max_drr_cpm_search"]
    else:
        step_up = STEP_CPC_SMALL
        step_down = STEP_CPC_MED
        min_v, max_v = MIN_CPC_SEARCH, MAX_CPC_SEARCH
        drr_target = config["target_drr_cpc"]
        drr_max = config["max_drr_cpc"]
    if ad_profit < 0 or drr > drr_max:
        new = clamp(current - step_down, min_v, max_v)
        if new != current:
            return Decision(1, STRATEGY_NAMES[1], safe_int(row["ID кампании"]), safe_int(row["Артикул WB"]),
                            str(row.get("Название", "")), ctype, "DOWN",
                            f"Убыточно/перегрето: прибыль рекламы {ad_profit:.2f}, ДРР {drr:.2f}%",
                            current, new, 0, 0, week_label)
    if ad_profit > 0 and profit_u > 0 and rating >= OK_RATING_THRESHOLD:
        if pos > TOP20_BORDER and orders_kw >= MIN_KEYWORD_ORDERS_FOR_PUSH:
            new = clamp(current + step_up * 2, min_v, max_v)
            if new != current:
                return Decision(1, STRATEGY_NAMES[1], safe_int(row["ID кампании"]), safe_int(row["Артикул WB"]),
                                str(row.get("Название", "")), ctype, "UP",
                                f"Нужно поднять видимость, позиция {pos:.1f}, реклама прибыльна",
                                current, new, 0, 0, week_label)
        if TOP10_BORDER < pos <= TOP20_BORDER and drr <= drr_target + 2:
            new = clamp(current + step_up, min_v, max_v)
            if new != current:
                return Decision(1, STRATEGY_NAMES[1], safe_int(row["ID кампании"]), safe_int(row["Артикул WB"]),
                                str(row.get("Название", "")), ctype, "UP",
                                f"Есть запас по прибыли, позиция {pos:.1f}",
                                current, new, 0, 0, week_label)
    return None

def decide_position_strategy(row: pd.Series, config: dict, week_label: str) -> Optional[Decision]:
    ctype = str(row.get("Тип кампании"))
    pos = safe_float(row.get("Медианная позиция заказных ключей"))
    rating = safe_float(row.get("Рейтинг отзывов"))
    drr = safe_float(row.get("ДРР, % факт"))
    ad_profit = safe_float(row.get("Ожидаемая чистая прибыль рекламы, руб"))
    if is_micro_noise(row):
        return None
    sf = stop_factors(row)
    if sf:
        if ctype == "cpm_shelves":
            current = safe_int(row.get("Текущая ставка рекомендации, коп"))
            new = clamp(current - STEP_CPM_BIG, MIN_CPM_SHELVES, MAX_CPM_SHELVES)
            if new != current:
                return Decision(2, STRATEGY_NAMES[2], safe_int(row["ID кампании"]), safe_int(row["Артикул WB"]),
                                str(row.get("Название", "")), ctype, "DOWN", sf, 0, 0, current, new, week_label)
            return None
        current = safe_int(row.get("Текущая ставка поиск, коп"))
        min_v = MIN_CPM_SEARCH if ctype == "cpm_search" else MIN_CPC_SEARCH
        max_v = MAX_CPM_SEARCH if ctype == "cpm_search" else MAX_CPC_SEARCH
        step = STEP_CPM_BIG if ctype == "cpm_search" else STEP_CPC_BIG
        new = clamp(current - step, min_v, max_v)
        if new != current:
            return Decision(2, STRATEGY_NAMES[2], safe_int(row["ID кампании"]), safe_int(row["Артикул WB"]),
                            str(row.get("Название", "")), ctype, "DOWN", sf, current, new, 0, 0, week_label)
        return None
    if ctype == "cpm_shelves":
        current = safe_int(row.get("Текущая ставка рекомендации, коп"))
        ctr = safe_float(row.get("CTR, % факт"))
        if ad_profit < 0 and ctr < MIN_CTR_SHELVES:
            new = clamp(current - STEP_CPM_MED, MIN_CPM_SHELVES, MAX_CPM_SHELVES)
            if new != current:
                return Decision(2, STRATEGY_NAMES[2], safe_int(row["ID кампании"]), safe_int(row["Артикул WB"]),
                                str(row.get("Название", "")), ctype, "DOWN",
                                f"Полки не дают нужного эффекта: CTR {ctr:.2f}%, прибыль {ad_profit:.2f}",
                                0, 0, current, new, week_label)
        elif ad_profit > 0 and ctr >= GOOD_CTR_SHELVES and rating >= GOOD_RATING_THRESHOLD:
            new = clamp(current + STEP_CPM_SMALL, MIN_CPM_SHELVES, MAX_CPM_SHELVES)
            if new != current:
                return Decision(2, STRATEGY_NAMES[2], safe_int(row["ID кампании"]), safe_int(row["Артикул WB"]),
                                str(row.get("Название", "")), ctype, "UP",
                                f"Полки стабильны: CTR {ctr:.2f}%, рейтинг {rating:.2f}",
                                0, 0, current, new, week_label)
        return None
    current = safe_int(row.get("Текущая ставка поиск, коп"))
    if ctype == "cpm_search":
        step_small, step_med, min_v, max_v = STEP_CPM_SMALL, STEP_CPM_MED, MIN_CPM_SEARCH, MAX_CPM_SEARCH
    else:
        step_small, step_med, min_v, max_v = STEP_CPC_SMALL, STEP_CPC_MED, MIN_CPC_SEARCH, MAX_CPC_SEARCH
    if ad_profit < 0 or drr > (config["target_drr_cpc"] + 5):
        new = clamp(current - step_small, min_v, max_v)
        if new != current:
            return Decision(2, STRATEGY_NAMES[2], safe_int(row["ID кампании"]), safe_int(row["Артикул WB"]),
                            str(row.get("Название", "")), ctype, "DOWN",
                            f"Позиция не должна покупаться в убыток: прибыль {ad_profit:.2f}, ДРР {drr:.2f}%",
                            current, new, 0, 0, week_label)
    if pos > TOP20_BORDER and rating >= OK_RATING_THRESHOLD:
        new = clamp(current + step_med, min_v, max_v)
        if new != current:
            return Decision(2, STRATEGY_NAMES[2], safe_int(row["ID кампании"]), safe_int(row["Артикул WB"]),
                            str(row.get("Название", "")), ctype, "UP",
                            f"Позиция {pos:.1f} хуже TOP20",
                            current, new, 0, 0, week_label)
    if TOP10_BORDER < pos <= TOP20_BORDER and rating >= GOOD_RATING_THRESHOLD:
        new = clamp(current + step_small, min_v, max_v)
        if new != current:
            return Decision(2, STRATEGY_NAMES[2], safe_int(row["ID кампании"]), safe_int(row["Артикул WB"]),
                            str(row.get("Название", "")), ctype, "UP",
                            f"Позиция {pos:.1f} вне TOP10",
                            current, new, 0, 0, week_label)
    if pos < 5 and drr > config["target_drr_cpc"]:
        new = clamp(current - step_small, min_v, max_v)
        if new != current:
            return Decision(2, STRATEGY_NAMES[2], safe_int(row["ID кампании"]), safe_int(row["Артикул WB"]),
                            str(row.get("Название", "")), ctype, "DOWN",
                            f"Позиция {pos:.1f} уже очень высокая, можно экономить",
                            current, new, 0, 0, week_label)
    return None

def decide_drr_strategy(row: pd.Series, config: dict, week_label: str) -> Optional[Decision]:
    ctype = str(row.get("Тип кампании"))
    drr = safe_float(row.get("ДРР, % факт"))
    if is_micro_noise(row):
        return None
    sf = stop_factors(row)
    if ctype == "cpm_shelves":
        current = safe_int(row.get("Текущая ставка рекомендации, коп"))
        target = config["target_drr_cpm_shelves"]
        max_drr = config["max_drr_cpm_shelves"]
        if sf:
            new = clamp(current - STEP_CPM_BIG, MIN_CPM_SHELVES, MAX_CPM_SHELVES)
            if new != current:
                return Decision(3, STRATEGY_NAMES[3], safe_int(row["ID кампании"]), safe_int(row["Артикул WB"]),
                                str(row.get("Название", "")), ctype, "DOWN", sf, 0, 0, current, new, week_label)
        if drr > max_drr:
            new = clamp(current - STEP_CPM_BIG, MIN_CPM_SHELVES, MAX_CPM_SHELVES)
            if new != current:
                return Decision(3, STRATEGY_NAMES[3], safe_int(row["ID кампании"]), safe_int(row["Артикул WB"]),
                                str(row.get("Название", "")), ctype, "DOWN",
                                f"ДРР {drr:.2f}% > {max_drr:.2f}%",
                                0, 0, current, new, week_label)
        elif target < drr <= max_drr:
            new = clamp(current - STEP_CPM_SMALL, MIN_CPM_SHELVES, MAX_CPM_SHELVES)
            if new != current:
                return Decision(3, STRATEGY_NAMES[3], safe_int(row["ID кампании"]), safe_int(row["Артикул WB"]),
                                str(row.get("Название", "")), ctype, "DOWN",
                                f"ДРР {drr:.2f}% выше цели {target:.2f}%",
                                0, 0, current, new, week_label)
        elif drr < target - 3 and safe_float(row.get("Ожидаемая чистая прибыль рекламы, руб")) > 0:
            new = clamp(current + STEP_CPM_SMALL, MIN_CPM_SHELVES, MAX_CPM_SHELVES)
            if new != current:
                return Decision(3, STRATEGY_NAMES[3], safe_int(row["ID кампании"]), safe_int(row["Артикул WB"]),
                                str(row.get("Название", "")), ctype, "UP",
                                f"ДРР {drr:.2f}% заметно ниже цели",
                                0, 0, current, new, week_label)
        return None
    current = safe_int(row.get("Текущая ставка поиск, коп"))
    if ctype == "cpm_search":
        target = config["target_drr_cpm_search"]
        max_drr = config["max_drr_cpm_search"]
        step_small, step_big, min_v, max_v = STEP_CPM_SMALL, STEP_CPM_BIG, MIN_CPM_SEARCH, MAX_CPM_SEARCH
    else:
        target = config["target_drr_cpc"]
        max_drr = config["max_drr_cpc"]
        step_small, step_big, min_v, max_v = STEP_CPC_SMALL, STEP_CPC_BIG, MIN_CPC_SEARCH, MAX_CPC_SEARCH
    if sf:
        new = clamp(current - step_big, min_v, max_v)
        if new != current:
            return Decision(3, STRATEGY_NAMES[3], safe_int(row["ID кампании"]), safe_int(row["Артикул WB"]),
                            str(row.get("Название", "")), ctype, "DOWN", sf, current, new, 0, 0, week_label)
    if drr > max_drr:
        new = clamp(current - step_big, min_v, max_v)
        if new != current:
            return Decision(3, STRATEGY_NAMES[3], safe_int(row["ID кампании"]), safe_int(row["Артикул WB"]),
                            str(row.get("Название", "")), ctype, "DOWN",
                            f"ДРР {drr:.2f}% > {max_drr:.2f}%",
                            current, new, 0, 0, week_label)
    elif target < drr <= max_drr:
        new = clamp(current - step_small, min_v, max_v)
        if new != current:
            return Decision(3, STRATEGY_NAMES[3], safe_int(row["ID кампании"]), safe_int(row["Артикул WB"]),
                            str(row.get("Название", "")), ctype, "DOWN",
                            f"ДРР {drr:.2f}% выше цели {target:.2f}%",
                            current, new, 0, 0, week_label)
    elif drr < target - 3 and safe_float(row.get("Ожидаемая чистая прибыль рекламы, руб")) > 0:
        new = clamp(current + step_small, min_v, max_v)
        if new != current:
            return Decision(3, STRATEGY_NAMES[3], safe_int(row["ID кампании"]), safe_int(row["Артикул WB"]),
                            str(row.get("Название", "")), ctype, "UP",
                            f"ДРР {drr:.2f}% ниже цели {target:.2f}%",
                            current, new, 0, 0, week_label)
    return None

def decide_traffic_share_strategy(row: pd.Series, config: dict, week_label: str) -> Optional[Decision]:
    ctype = str(row.get("Тип кампании"))
    traffic_share = safe_float(row.get("Доля трафика, %"))
    pos = safe_float(row.get("Медианная позиция заказных ключей"))
    rating = safe_float(row.get("Рейтинг отзывов"))
    ad_profit = safe_float(row.get("Ожидаемая чистая прибыль рекламы, руб"))
    drr = safe_float(row.get("ДРР, % факт"))
    if is_micro_noise(row):
        return None
    sf = stop_factors(row)
    if ctype == "cpm_shelves":
        current = safe_int(row.get("Текущая ставка рекомендации, коп"))
        ctr = safe_float(row.get("CTR, % факт"))
        if sf:
            new = clamp(current - STEP_CPM_BIG, MIN_CPM_SHELVES, MAX_CPM_SHELVES)
            if new != current:
                return Decision(4, STRATEGY_NAMES[4], safe_int(row["ID кампании"]), safe_int(row["Артикул WB"]),
                                str(row.get("Название", "")), ctype, "DOWN", sf, 0, 0, current, new, week_label)
        if ad_profit < 0 or ctr < MIN_CTR_SHELVES:
            new = clamp(current - STEP_CPM_MED, MIN_CPM_SHELVES, MAX_CPM_SHELVES)
            if new != current:
                return Decision(4, STRATEGY_NAMES[4], safe_int(row["ID кампании"]), safe_int(row["Артикул WB"]),
                                str(row.get("Название", "")), ctype, "DOWN",
                                f"Полки не увеличивают полезный трафик: CTR {ctr:.2f}%, прибыль {ad_profit:.2f}",
                                0, 0, current, new, week_label)
        elif ad_profit > 0 and ctr >= GOOD_CTR_SHELVES:
            new = clamp(current + STEP_CPM_SMALL, MIN_CPM_SHELVES, MAX_CPM_SHELVES)
            if new != current:
                return Decision(4, STRATEGY_NAMES[4], safe_int(row["ID кампании"]), safe_int(row["Артикул WB"]),
                                str(row.get("Название", "")), ctype, "UP",
                                f"Полки хорошо забирают трафик: CTR {ctr:.2f}%",
                                0, 0, current, new, week_label)
        return None
    current = safe_int(row.get("Текущая ставка поиск, коп"))
    if ctype == "cpm_search":
        step_up, step_down, min_v, max_v = STEP_CPM_SMALL, STEP_CPM_MED, MIN_CPM_SEARCH, MAX_CPM_SEARCH
    else:
        step_up, step_down, min_v, max_v = STEP_CPC_SMALL, STEP_CPC_MED, MIN_CPC_SEARCH, MAX_CPC_SEARCH
    if sf:
        new = clamp(current - step_down, min_v, max_v)
        if new != current:
            return Decision(4, STRATEGY_NAMES[4], safe_int(row["ID кампании"]), safe_int(row["Артикул WB"]),
                            str(row.get("Название", "")), ctype, "DOWN", sf, current, new, 0, 0, week_label)
    if traffic_share < 2.0 and pos > TOP20_BORDER and rating >= OK_RATING_THRESHOLD and ad_profit >= 0:
        new = clamp(current + step_up * 2, min_v, max_v)
        if new != current:
            return Decision(4, STRATEGY_NAMES[4], safe_int(row["ID кампании"]), safe_int(row["Артикул WB"]),
                            str(row.get("Название", "")), ctype, "UP",
                            f"Низкая доля трафика {traffic_share:.2f}% и слабая позиция {pos:.1f}",
                            current, new, 0, 0, week_label)
    if 2.0 <= traffic_share < 4.0 and pos > TOP10_BORDER and ad_profit >= 0 and drr <= config["max_drr_cpc"]:
        new = clamp(current + step_up, min_v, max_v)
        if new != current:
            return Decision(4, STRATEGY_NAMES[4], safe_int(row["ID кампании"]), safe_int(row["Артикул WB"]),
                            str(row.get("Название", "")), ctype, "UP",
                            f"Есть потенциал роста доли трафика: {traffic_share:.2f}%",
                            current, new, 0, 0, week_label)
    if traffic_share >= 6.0 and drr > config["target_drr_cpc"] + 2:
        new = clamp(current - step_down, min_v, max_v)
        if new != current:
            return Decision(4, STRATEGY_NAMES[4], safe_int(row["ID кампании"]), safe_int(row["Артикул WB"]),
                            str(row.get("Название", "")), ctype, "DOWN",
                            f"Доля трафика уже высокая ({traffic_share:.2f}%), можно экономить",
                            current, new, 0, 0, week_label)
    return None

def build_decisions(metrics_df: pd.DataFrame, strategy_id: int, config: dict, week_label: str) -> Tuple[List[Decision], pd.DataFrame]:
    decisions: List[Decision] = []
    logic_rows: List[Dict[str, Any]] = []
    metrics_df = metrics_df.copy()
    metrics_df["Неделя"] = week_label
    for _, row in metrics_df.iterrows():
        ctype = str(row.get("Тип кампании", "unknown"))
        if ctype in {"manual_mixed", "manual_search", "unknown"}:
            continue
        decision = None
        if strategy_id == 1:
            decision = decide_profit_strategy(row, config, week_label)
        elif strategy_id == 2:
            decision = decide_position_strategy(row, config, week_label)
        elif strategy_id == 3:
            decision = decide_drr_strategy(row, config, week_label)
        elif strategy_id == 4:
            decision = decide_traffic_share_strategy(row, config, week_label)
        if decision:
            decisions.append(decision)
            logic_rows.append(make_logic_row(row, strategy_id, decision.reason, decision.action))
    logic_df = pd.DataFrame(logic_rows)
    return decisions, logic_df

# =========================================================
# WB MIN BIDS / PAYLOAD
# =========================================================
def detect_wb_placement(row: pd.Series, decision: Decision) -> str:
    bid_type = str(row.get("Тип ставки", "")).strip().lower()
    campaign_type = str(row.get("Тип кампании", "")).strip().lower()
    if bid_type == "unified":
        return "combined"
    if campaign_type == "cpm_shelves":
        return "recommendations"
    return "search"

def placement_for_min_bids_api(placement: str) -> str:
    if placement == "recommendations":
        return "recommendation"
    return placement

def extract_payment_type_for_advert(metrics_df: pd.DataFrame, advert_id: int) -> str:
    rows = metrics_df[metrics_df["ID кампании"] == advert_id]
    if rows.empty:
        return "cpm"
    payment_type = str(rows.iloc[0].get("Тип оплаты", "")).strip().lower()
    if payment_type in {"cpm", "cpc"}:
        return payment_type
    campaign_type = str(rows.iloc[0].get("Тип кампании", "")).strip().lower()
    if campaign_type == "cpc_search":
        return "cpc"
    return "cpm"

def fetch_min_bids_for_advert(api_key: str, advert_id: int, nm_ids: List[int], payment_type: str, placements: List[str]) -> Dict[Tuple[int, str], int]:
    headers = {"Authorization": api_key.strip(), "Content-Type": "application/json"}
    req_payload = {
        "advert_id": int(advert_id),
        "nm_ids": [int(x) for x in nm_ids],
        "payment_type": payment_type,
        "placement_types": [placement_for_min_bids_api(p) for p in placements],
    }
    max_retries = 3
    for attempt in range(max_retries):
        try:
            resp = requests.post(WB_BIDS_MIN_URL, headers=headers, json=req_payload, timeout=120)
            if resp.status_code == 429:
                wait = 1 * (2 ** attempt)
                log(f"⚠️ 429 Too Many Requests для advert_id={advert_id}, повтор через {wait}с (попытка {attempt+1}/{max_retries})")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            data = resp.json()
            result: Dict[Tuple[int, str], int] = {}
            for item in data.get("bids", []):
                nm_id = safe_int(item.get("nm_id"))
                for bid_item in item.get("bids", []):
                    bid_type = str(bid_item.get("type", "")).strip().lower()
                    bid_val = safe_int(bid_item.get("value", 0))
                    if bid_type == "recommendation":
                        bid_type = "recommendations"
                    if nm_id > 0 and bid_type:
                        result[(nm_id, bid_type)] = bid_val
            return result
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            log(f"⚠️ Ошибка запроса min bids (попытка {attempt+1}): {e}, повтор через 1с")
            time.sleep(1)
    return {}

def normalize_bid_for_wb(bid_value: int, placement: str, known_min_bid: Optional[int] = None) -> int:
    bid_value = int(bid_value)
    if known_min_bid is not None and known_min_bid > 0:
        return max(bid_value, int(known_min_bid))
    if placement == "combined":
        return max(bid_value, 8000)
    if placement == "recommendations":
        return max(bid_value, 8000)
    if placement == "search":
        return max(bid_value, 400)
    return bid_value

def decisions_to_payload(decisions: List[Decision], metrics_df: pd.DataFrame) -> Dict[str, Any]:
    metrics_lookup = metrics_df.set_index(["ID кампании", "Артикул WB"], drop=False)
    grouped: Dict[int, Dict[str, Any]] = {}
    for d in decisions:
        key = (d.id_campaign, d.nm_id)
        if key not in metrics_lookup.index:
            continue
        row = metrics_lookup.loc[key]
        if isinstance(row, pd.DataFrame):
            row = row.iloc[0]
        placement = detect_wb_placement(row, d)
        if d.id_campaign not in grouped:
            grouped[d.id_campaign] = {"advert_id": int(d.id_campaign), "nm_bids": []}
        bid_value = d.new_rec_bid_kop if d.campaign_type == "cpm_shelves" else d.new_search_bid_kop
        if bid_value <= 0:
            continue
        grouped[d.id_campaign]["nm_bids"].append({
            "nm_id": int(d.nm_id),
            "bid_kopecks": normalize_bid_for_wb(int(bid_value), placement),
            "placement": placement,
        })
    bids = []
    for advert in grouped.values():
        nm_bids = advert["nm_bids"]
        if not nm_bids:
            continue
        for i in range(0, len(nm_bids), 50):
            bids.append({"advert_id": advert["advert_id"], "nm_bids": nm_bids[i:i+50]})
    return {"bids": bids}

def extract_bid_limits_from_error(response_text: str) -> Dict[str, Optional[Union[int, str]]]:
    """Извлекает из текста ошибки WB минимальную и максимальную ставку, а также placement."""
    limits = {}
    text = str(response_text).lower()
    m_min = re.search(r'min:\s*(\d+)', text)
    if m_min:
        limits['min'] = int(m_min.group(1))
    m_max = re.search(r'max:\s*(\d+)', text)
    if m_max:
        limits['max'] = int(m_max.group(1))
    m_place = re.search(r'placement:\s*(\w+)', text)
    if m_place:
        limits['placement'] = m_place.group(1).lower()
    return limits

def apply_limits_to_payload(payload: Dict[str, Any], limits: Dict[str, Optional[Union[int, str]]]) -> Dict[str, Any]:
    """Применяет ограничения min/max к ставкам в payload. Если указан placement, корректируются только записи с таким placement."""
    new_bids = []
    target_placement = limits.get('placement')
    min_val = limits.get('min')
    max_val = limits.get('max')
    for advert_block in payload.get('bids', []):
        adjusted_nm_bids = []
        for nm_bid in advert_block.get('nm_bids', []):
            placement = nm_bid.get('placement', '').strip().lower()
            if target_placement and placement != target_placement:
                adjusted_nm_bids.append(nm_bid)
                continue
            bid = nm_bid['bid_kopecks']
            if min_val is not None:
                bid = max(bid, min_val)
            if max_val is not None:
                bid = min(bid, max_val)
            adjusted_nm_bids.append({
                'nm_id': nm_bid['nm_id'],
                'bid_kopecks': bid,
                'placement': placement,
            })
        new_bids.append({'advert_id': advert_block['advert_id'], 'nm_bids': adjusted_nm_bids})
    return {'bids': new_bids}

def bump_payload_bids(payload: Dict[str, Any], step_kopecks: int = FALLBACK_BID_STEP_KOPECKS) -> Dict[str, Any]:
    new_bids = []
    for advert_block in payload.get("bids", []):
        adjusted_nm_bids = []
        for nm_bid in advert_block.get("nm_bids", []):
            bid_kopecks = safe_int(nm_bid.get("bid_kopecks"))
            placement = str(nm_bid.get("placement", "")).strip().lower()
            bumped = bid_kopecks + step_kopecks
            bumped = normalize_bid_for_wb(bumped, placement=placement, known_min_bid=None)
            adjusted_nm_bids.append({
                "nm_id": safe_int(nm_bid.get("nm_id")),
                "bid_kopecks": bumped,
                "placement": placement,
            })
        new_bids.append({
            "advert_id": safe_int(advert_block.get("advert_id")),
            "nm_bids": adjusted_nm_bids,
        })
    return {"bids": new_bids}

def is_wrong_bid_value_error(response_text: str) -> bool:
    txt = str(response_text).lower()
    return "wrong bid value" in txt

def enrich_payload_with_min_bids(payload: Dict[str, Any], metrics_df: pd.DataFrame, api_key: str) -> Tuple[Dict[str, Any], pd.DataFrame]:
    bids_list = payload.get("bids", [])
    if not bids_list:
        return payload, pd.DataFrame()
    new_bids = []
    log_rows = []
    for advert_block in bids_list:
        advert_id = safe_int(advert_block.get("advert_id"))
        nm_bids = advert_block.get("nm_bids", [])
        if not advert_id or not nm_bids:
            continue
        nm_ids = [safe_int(x.get("nm_id")) for x in nm_bids if safe_int(x.get("nm_id")) > 0]
        placements = list(sorted(set(str(x.get("placement", "")).strip().lower() for x in nm_bids if x.get("placement"))))
        payment_type = extract_payment_type_for_advert(metrics_df, advert_id)
        min_map: Dict[Tuple[int, str], int] = {}
        min_fetch_status = "ok"
        try:
            min_map = fetch_min_bids_for_advert(api_key=api_key, advert_id=advert_id, nm_ids=nm_ids, payment_type=payment_type, placements=placements)
            time.sleep(0.2)  # задержка между запросами
        except Exception as e:
            min_fetch_status = f"error: {e}"
            log(f"⚠️ Не удалось получить min bids для advert_id={advert_id}: {e}")
        adjusted_nm_bids = []
        for nm_bid in nm_bids:
            nm_id = safe_int(nm_bid.get("nm_id"))
            placement = str(nm_bid.get("placement", "")).strip().lower()
            old_bid = safe_int(nm_bid.get("bid_kopecks"))
            known_min = min_map.get((nm_id, placement))
            new_bid = normalize_bid_for_wb(old_bid, placement, known_min)
            adjusted_nm_bids.append({"nm_id": nm_id, "bid_kopecks": new_bid, "placement": placement})
            log_rows.append({
                "Дата": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "ID кампании": advert_id,
                "Артикул WB": nm_id,
                "Placement": placement,
                "Тип оплаты": payment_type,
                "Исходная ставка, коп": old_bid,
                "Минимальная ставка WB, коп": known_min if known_min is not None else "",
                "Ставка после нормализации, коп": new_bid,
                "Статус получения min": min_fetch_status,
            })
        new_bids.append({"advert_id": advert_id, "nm_bids": adjusted_nm_bids})
    return {"bids": new_bids}, pd.DataFrame(log_rows)

# =========================================================
# SEND WB
# =========================================================
def send_batches(payload: Dict[str, Any], api_key: str, metrics_df: pd.DataFrame, dry_run: bool = True) -> Tuple[int, int, pd.DataFrame]:
    send_log_rows = []
    if dry_run:
        log("🧪 dry-run: отправка ставок отключена")
        for advert in payload.get("bids", []):
            send_log_rows.append({
                "Дата": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "ID кампании": safe_int(advert.get("advert_id")),
                "Попытка": 0,
                "Статус": "dry-run",
                "HTTP статус": "",
                "Ответ WB": "",
            })
        return len(payload.get("bids", [])), 0, pd.DataFrame(send_log_rows)

    headers = {"Authorization": api_key.strip(), "Content-Type": "application/json"}
    all_bids = payload.get("bids", [])
    if not all_bids:
        log("ℹ️ Пустой payload, отправлять нечего")
        return 0, 0, pd.DataFrame(send_log_rows)

    success = 0
    failed = 0
    batch_size = 50
    batches = [all_bids[i:i+batch_size] for i in range(0, len(all_bids), batch_size)]

    for idx, batch in enumerate(batches, start=1):
        batch_payload = {"bids": batch}
        log(f"📤 Отправка батча {idx}/{len(batches)}: кампаний {len(batch)}")

        attempt_payload, min_log_df = enrich_payload_with_min_bids(payload=batch_payload, metrics_df=metrics_df, api_key=api_key)
        if not min_log_df.empty:
            for _, rr in min_log_df.iterrows():
                send_log_rows.append({
                    "Дата": rr["Дата"],
                    "ID кампании": rr["ID кампании"],
                    "Попытка": 0,
                    "Статус": "min_bids_loaded",
                    "HTTP статус": "",
                    "Ответ WB": f"placement={rr['Placement']}; old={rr['Исходная ставка, коп']}; min={rr['Минимальная ставка WB, коп']}; new={rr['Ставка после нормализации, коп']}; status={rr['Статус получения min']}",
                })

        sent_ok = False
        for attempt in range(1, MAX_RETRY_ROUNDS + 1):
            try:
                resp = requests.patch(WB_BIDS_URL, headers=headers, json=attempt_payload, timeout=120)
                if resp.status_code == 200:
                    success += len(batch)
                    log(f"✅ Батч {idx} успешно применён с попытки {attempt}")
                    send_log_rows.append({
                        "Дата": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "ID кампании": ",".join(str(x.get("advert_id")) for x in batch),
                        "Попытка": attempt,
                        "Статус": "success",
                        "HTTP статус": 200,
                        "Ответ WB": resp.text[:2000],
                    })
                    sent_ok = True
                    time.sleep(0.25)
                    break

                response_text = resp.text[:3000]
                log(f"⚠️ Ошибка WB {resp.status_code}: {response_text}")

                send_log_rows.append({
                    "Дата": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "ID кампании": ",".join(str(x.get("advert_id")) for x in batch),
                    "Попытка": attempt,
                    "Статус": "error",
                    "HTTP статус": resp.status_code,
                    "Ответ WB": response_text,
                })

                if resp.status_code == 400 and is_wrong_bid_value_error(response_text):
                    limits = extract_bid_limits_from_error(response_text)
                    if limits:
                        log(f"🔁 WB вернул лимиты: {limits}, корректирую payload")
                        attempt_payload = apply_limits_to_payload(attempt_payload, limits)
                    else:
                        log(f"🔁 Не удалось извлечь лимиты, повышаю ставки на +{FALLBACK_BID_STEP_KOPECKS} коп")
                        attempt_payload = bump_payload_bids(attempt_payload, step_kopecks=FALLBACK_BID_STEP_KOPECKS)
                    time.sleep(0.3)
                    continue

                break  # другая ошибка – выходим

            except Exception as e:
                log(f"⚠️ Исключение отправки батча {idx}, попытка {attempt}: {e}")
                send_log_rows.append({
                    "Дата": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "ID кампании": ",".join(str(x.get("advert_id")) for x in batch),
                    "Попытка": attempt,
                    "Статус": "exception",
                    "HTTP статус": "",
                    "Ответ WB": str(e),
                })
                if attempt < MAX_RETRY_ROUNDS:
                    attempt_payload = bump_payload_bids(attempt_payload, step_kopecks=FALLBACK_BID_STEP_KOPECKS)
                    time.sleep(0.5)
                    continue
                break

        if not sent_ok:
            failed += len(batch)
            log(f"⚠️ Батч {idx} не отправлен после {MAX_RETRY_ROUNDS} попыток")
            log(f"⚠️ Финальный payload батча {idx}: {json.dumps(attempt_payload, ensure_ascii=False)[:4000]}")
            send_log_rows.append({
                "Дата": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "ID кампании": ",".join(str(x.get("advert_id")) for x in batch),
                "Попытка": MAX_RETRY_ROUNDS,
                "Статус": "final_failed",
                "HTTP статус": "",
                "Ответ WB": json.dumps(attempt_payload, ensure_ascii=False)[:4000],
            })

    return success, failed, pd.DataFrame(send_log_rows)

# =========================================================
# PREVIEW / SUMMARY
# =========================================================
def decisions_to_df(decisions: List[Decision], metrics_df: pd.DataFrame) -> pd.DataFrame:
    if not decisions:
        return pd.DataFrame(columns=[
            "Неделя", "Стратегия", "Название стратегии", "ID кампании", "Название", "Артикул WB",
            "Тип кампании", "Действие", "Причина",
            "Текущая ставка поиск, коп", "Новая ставка поиск, коп",
            "Текущая ставка рекомендации, коп", "Новая ставка рекомендации, коп"
        ])
    ddf = pd.DataFrame([asdict(d) for d in decisions]).rename(columns={
        "strategy_id": "Стратегия",
        "strategy_name": "Название стратегии",
        "id_campaign": "ID кампании",
        "nm_id": "Артикул WB",
        "campaign_name": "Название",
        "campaign_type": "Тип кампании",
        "action": "Действие",
        "reason": "Причина",
        "current_search_bid_kop": "Текущая ставка поиск, коп",
        "new_search_bid_kop": "Новая ставка поиск, коп",
        "current_rec_bid_kop": "Текущая ставка рекомендации, коп",
        "new_rec_bid_kop": "Новая ставка рекомендации, коп",
        "week_label": "Неделя",
    })
    enrich_cols = [
        "ID кампании", "Артикул WB", "Расход за неделю", "Сумма заказов за неделю", "Заказы за неделю",
        "Показы за неделю", "Клики за неделю", "CTR, % факт", "CR, % факт", "ДРР, % факт",
        "Ожидаемая чистая прибыль рекламы, руб", "Профит на заказ после рекламы, руб",
        "Чистая прибыль, руб/ед", "Рейтинг отзывов", "Медианная позиция заказных ключей", "Доля трафика, %"
    ]
    enrich = metrics_df[enrich_cols].drop_duplicates(subset=["ID кампании", "Артикул WB"])
    return ddf.merge(enrich, on=["ID кампании", "Артикул WB"], how="left")

def save_preview_files(s3: S3Storage, decisions_df: pd.DataFrame, metrics_df: pd.DataFrame, week_label: str, strategy_id: int):
    sheets = {
        "Рекомендации": decisions_df if decisions_df is not None else pd.DataFrame(),
        "Метрики": metrics_df if metrics_df is not None else pd.DataFrame(),
    }
    s3.write_excel_sheets(SERVICE_PREVIEW_KEY, sheets)
    summary = {
        "generated_at": datetime.now().isoformat(),
        "week_label": week_label,
        "strategy_id": strategy_id,
        "strategy_name": STRATEGY_NAMES.get(strategy_id, f"Стратегия {strategy_id}"),
        "recommendations_count": 0 if decisions_df is None else int(len(decisions_df)),
        "metrics_count": 0 if metrics_df is None else int(len(metrics_df)),
    }
    s3.write_text(SERVICE_LOG_KEY, json.dumps(summary, ensure_ascii=False, indent=2))

# =========================================================
# CONFIG / ROTATION
# =========================================================
def load_strategy_config(s3: S3Storage) -> dict:
    cfg = read_json_or_default(s3, SERVICE_CONFIG_KEY, DEFAULT_CONFIG.copy())
    for k, v in DEFAULT_CONFIG.items():
        if k not in cfg:
            cfg[k] = v
    return cfg

def save_strategy_config(s3: S3Storage, cfg: dict):
    cfg = dict(cfg)
    cfg["updated_at"] = datetime.now().isoformat()
    s3.write_text(SERVICE_CONFIG_KEY, json.dumps(cfg, ensure_ascii=False, indent=2))

def determine_strategy_for_week(s3: S3Storage, week_label: str, cfg: dict) -> int:
    if cfg.get("mode") == "fixed":
        return int(cfg.get("active_strategy", 1))
    schedule_df = load_schedule(s3)
    if not schedule_df.empty and "Неделя" in schedule_df.columns:
        existing = schedule_df[schedule_df["Неделя"].astype(str) == week_label]
        if not existing.empty:
            return safe_int(existing.iloc[0]["Стратегия"], 1)
    seq = cfg.get("strategy_sequence", DEFAULT_STRATEGY_SEQUENCE)
    if not seq:
        seq = DEFAULT_STRATEGY_SEQUENCE
    prev_week = previous_week_label(week_label, 1)
    prev_rows = schedule_df[schedule_df["Неделя"].astype(str) == prev_week] if not schedule_df.empty else pd.DataFrame()
    if not prev_rows.empty:
        prev_strategy = safe_int(prev_rows.iloc[0]["Стратегия"], seq[0])
        if prev_strategy in seq:
            idx = seq.index(prev_strategy)
            strategy_id = seq[(idx + 1) % len(seq)]
        else:
            strategy_id = seq[0]
    else:
        strategy_id = seq[0]
    new_row = pd.DataFrame([{
        "Неделя": week_label,
        "Стратегия": strategy_id,
        "Название стратегии": STRATEGY_NAMES.get(strategy_id, str(strategy_id)),
        "Статус": "назначена",
        "Дата назначения": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }])
    schedule_df = pd.concat([schedule_df, new_row], ignore_index=True)
    schedule_df = schedule_df.drop_duplicates(subset=["Неделя"], keep="last")
    save_schedule(s3, schedule_df)
    return strategy_id

# =========================================================
# EFFECTIVENESS
# =========================================================
def evaluate_strategy_week(metrics_df: pd.DataFrame, week_label: str, strategy_id: int, eval_type: str) -> Dict[str, Any]:
    if metrics_df.empty:
        return {
            "Неделя": week_label,
            "Стратегия": strategy_id,
            "Название стратегии": STRATEGY_NAMES.get(strategy_id, str(strategy_id)),
            "Тип оценки": eval_type,
            "Расход за неделю": 0.0,
            "Сумма заказов за неделю": 0.0,
            "Заказы за неделю": 0.0,
            "ДРР, % факт": 0.0,
            "CTR, % факт": 0.0,
            "CR, % факт": 0.0,
            "Ожидаемая чистая прибыль рекламы, руб": 0.0,
            "Средняя чистая прибыль, руб/ед": 0.0,
            "Средняя позиция": 0.0,
            "Средняя доля трафика, %": 0.0,
            "Средний рейтинг": 0.0,
            "Вывод": "Нет данных",
        }
    spend = round(safe_float(metrics_df["Расход за неделю"].sum()), 2)
    revenue = round(safe_float(metrics_df["Сумма заказов за неделю"].sum()), 2)
    orders = round(safe_float(metrics_df["Заказы за неделю"].sum()), 2)
    clicks = safe_float(metrics_df["Клики за неделю"].sum())
    views = safe_float(metrics_df["Показы за неделю"].sum())
    ad_profit = round(safe_float(metrics_df["Ожидаемая чистая прибыль рекламы, руб"].sum()), 2)
    drr = percent(spend, revenue) if revenue > 0 else 0.0
    ctr = percent(clicks, views) if views > 0 else 0.0
    cr = percent(orders, clicks) if clicks > 0 else 0.0
    avg_net_u = round(safe_float(metrics_df["Чистая прибыль, руб/ед"].mean()), 2)
    avg_pos = round(safe_float(metrics_df["Медианная позиция заказных ключей"].replace(0, pd.NA).mean()), 2)
    avg_share = round(safe_float(metrics_df["Доля трафика, %"].replace(0, pd.NA).mean()), 2)
    avg_rating = round(safe_float(metrics_df["Рейтинг отзывов"].replace(0, pd.NA).mean()), 2)
    parts = []
    if ad_profit > 0:
        parts.append("стратегия дала положительную чистую прибыль рекламы")
    else:
        parts.append("стратегия не дала положительную чистую прибыль рекламы")
    if drr <= 15:
        parts.append("ДРР в хорошем диапазоне")
    elif drr <= 20:
        parts.append("ДРР приемлемый")
    else:
        parts.append("ДРР высокий")
    if avg_pos and avg_pos <= TOP10_BORDER:
        parts.append("позиции сильные")
    elif avg_pos and avg_pos <= TOP20_BORDER:
        parts.append("позиции средние")
    elif avg_pos:
        parts.append("позиции слабые")
    return {
        "Неделя": week_label,
        "Стратегия": strategy_id,
        "Название стратегии": STRATEGY_NAMES.get(strategy_id, str(strategy_id)),
        "Тип оценки": eval_type,
        "Расход за неделю": spend,
        "Сумма заказов за неделю": revenue,
        "Заказы за неделю": orders,
        "ДРР, % факт": drr,
        "CTR, % факт": ctr,
        "CR, % факт": cr,
        "Ожидаемая чистая прибыль рекламы, руб": ad_profit,
        "Средняя чистая прибыль, руб/ед": avg_net_u,
        "Средняя позиция": avg_pos,
        "Средняя доля трафика, %": avg_share,
        "Средний рейтинг": avg_rating,
        "Вывод": "; ".join(parts),
    }

def update_effectiveness_analytics(s3: S3Storage, cfg: dict):
    schedule_df = load_schedule(s3)
    if schedule_df.empty:
        return
    eff_df = load_effectiveness(s3)
    eval_lag = safe_int(cfg.get("evaluation_lag_weeks", 1), 1)
    final_lag = safe_int(cfg.get("final_evaluation_lag_weeks", 2), 2)
    weeks_to_check = []
    target_pre = previous_week_label(iso_week_label(tz_now().date()), eval_lag)
    weeks_to_check.append((target_pre, "предварительная"))
    target_final = previous_week_label(iso_week_label(tz_now().date()), final_lag)
    weeks_to_check.append((target_final, "финальная"))
    for week_label, eval_type in weeks_to_check:
        row_sched = schedule_df[schedule_df["Неделя"].astype(str) == week_label]
        if row_sched.empty:
            continue
        strategy_id = safe_int(row_sched.iloc[0]["Стратегия"], 1)
        exists = eff_df[(eff_df["Неделя"].astype(str) == week_label) & (eff_df["Тип оценки"].astype(str) == eval_type)]
        if not exists.empty:
            continue
        try:
            week_start = week_start_from_label(week_label)
            week_end = week_start + timedelta(days=6)
            stats_df, campaigns_df = load_advertising_data(s3, week_label, week_start, week_end)
            economics_df = load_unit_economics(s3, week_label)
            keywords_df = load_keywords_weekly(s3, week_label)
            keywords_agg_df = aggregate_keywords(keywords_df)
            metrics_df = build_campaign_week_metrics(stats_df, campaigns_df, economics_df, keywords_agg_df)
            row = evaluate_strategy_week(metrics_df, week_label, strategy_id, eval_type)
            eff_df = pd.concat([eff_df, pd.DataFrame([row])], ignore_index=True)
            log(f"✅ Добавлена {eval_type} оценка стратегии за {week_label}")
        except Exception as e:
            log(f"⚠️ Не удалось оценить стратегию за {week_label}: {e}")
    if not eff_df.empty:
        eff_df = eff_df.drop_duplicates(subset=["Неделя", "Тип оценки"], keep="last")
        eff_df = eff_df.sort_values(["Неделя", "Тип оценки"], ascending=[False, True]).reset_index(drop=True)
        save_effectiveness(s3, eff_df)

# =========================================================
# PIPELINE
# =========================================================
def run_pipeline(s3: S3Storage, dry_run: bool, explicit_week: Optional[str], forced_strategy_id: Optional[int] = None):
    cfg = load_strategy_config(s3)
    week_label, week_start, week_end = target_week_range(explicit_week)
    log(f"🎯 Целевая неделя: {week_label} ({week_start} — {week_end})")
    log("📌 Важно: расходы рекламы считаются только внутри этой недели")
    strategy_id = forced_strategy_id if forced_strategy_id else determine_strategy_for_week(s3, week_label, cfg)
    strategy_name = STRATEGY_NAMES.get(strategy_id, f"Стратегия {strategy_id}")
    log(f"🧠 Активная стратегия: {strategy_id} — {strategy_name}")
    stats_df, campaigns_df = load_advertising_data(s3, week_label, week_start, week_end)
    economics_df = load_unit_economics(s3, week_label)
    keywords_df = load_keywords_weekly(s3, week_label)
    keywords_agg_df = aggregate_keywords(keywords_df)
    metrics_df = build_campaign_week_metrics(stats_df, campaigns_df, economics_df, keywords_agg_df)
    decisions, logic_df = build_decisions(metrics_df, strategy_id, cfg, week_label)
    decisions_df = decisions_to_df(decisions, metrics_df)
    schedule_df = load_schedule(s3)
    if not schedule_df.empty:
        schedule_df.loc[schedule_df["Неделя"].astype(str) == week_label, "Статус"] = "в работе"
        save_schedule(s3, schedule_df)
    history_df = load_bid_history(s3)
    if decisions:
        hist_rows = []
        for d in decisions:
            hist_rows.append({
                "Дата запуска": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "Неделя": d.week_label,
                "ID кампании": d.id_campaign,
                "Артикул WB": d.nm_id,
                "Тип кампании": d.campaign_type,
                "Ставка поиск, коп": d.new_search_bid_kop,
                "Ставка рекомендации, коп": d.new_rec_bid_kop,
                "Стратегия": d.strategy_id,
            })
        history_df = pd.concat([history_df, pd.DataFrame(hist_rows)], ignore_index=True)
        save_bid_history(s3, history_df)
    save_preview_files(s3, decisions_df, metrics_df, week_label, strategy_id)
    log(f"📊 Кампаний/артикулов в расчёте: {len(metrics_df)}")
    log(f"📌 Рекомендаций: {len(decisions)}")
    if decisions_df is not None and not decisions_df.empty:
        show_cols = [
            "Неделя", "Стратегия", "Название стратегии", "ID кампании", "Название", "Артикул WB", "Тип кампании",
            "Действие", "Текущая ставка поиск, коп", "Новая ставка поиск, коп",
            "Текущая ставка рекомендации, коп", "Новая ставка рекомендации, коп",
            "ДРР, % факт", "Ожидаемая чистая прибыль рекламы, руб",
            "Чистая прибыль, руб/ед", "Рейтинг отзывов", "Медианная позиция заказных ключей",
            "Доля трафика, %", "Причина"
        ]
        show_cols = [c for c in show_cols if c in decisions_df.columns]
        print(decisions_df[show_cols].head(80).to_string(index=False))
    send_log_df = pd.DataFrame()
    if not decisions:
        log("ℹ️ Нет изменений ставок")
        update_effectiveness_analytics(s3, cfg)
        save_decision_archive(s3, decisions_df, logic_df, send_log_df)
        return
    payload = decisions_to_payload(decisions, metrics_df)
    wb_key = os.environ.get("WB_PROMO_KEY_TOPFACE", "").strip()
    if not wb_key:
        raise RuntimeError("Не задан секрет WB_PROMO_KEY_TOPFACE")
    success, failed, send_log_df = send_batches(payload, wb_key, metrics_df, dry_run=dry_run)
    log(f"✅ Успешно обработано кампаний: {success}")
    log(f"⚠️ Не обработано кампаний: {failed}")
    save_decision_archive(s3, decisions_df, logic_df, send_log_df)
    update_effectiveness_analytics(s3, cfg)

# =========================================================
# REPORT
# =========================================================
def print_effectiveness_report(s3: S3Storage):
    df = load_effectiveness(s3)
    if df.empty:
        print("Отчёт по эффективности стратегий пока пуст.")
        return
    print("\n==============================")
    print("Эффективность стратегий")
    print("==============================\n")
    show_cols = [
        "Неделя", "Стратегия", "Название стратегии", "Тип оценки",
        "Расход за неделю", "Сумма заказов за неделю", "Заказы за неделю",
        "ДРР, % факт", "CTR, % факт", "CR, % факт",
        "Ожидаемая чистая прибыль рекламы, руб",
        "Средняя позиция", "Средняя доля трафика, %", "Средний рейтинг", "Вывод"
    ]
    print(df[show_cols].to_string(index=False))
    agg = df.groupby(["Стратегия", "Название стратегии"], as_index=False).agg(
        **{
            "Недель в оценке": ("Неделя", "nunique"),
            "Средняя чистая прибыль рекламы, руб": ("Ожидаемая чистая прибыль рекламы, руб", "mean"),
            "Средний ДРР, %": ("ДРР, % факт", "mean"),
            "Средний CTR, %": ("CTR, % факт", "mean"),
            "Средний CR, %": ("CR, % факт", "mean"),
            "Средняя позиция": ("Средняя позиция", "mean"),
            "Средняя доля трафика, %": ("Средняя доля трафика, %", "mean"),
        }
    ).sort_values(["Средняя чистая прибыль рекламы, руб", "Средний ДРР, %"], ascending=[False, True])
    print("\n==============================")
    print("Сводка по стратегиям")
    print("==============================\n")
    print(agg.to_string(index=False))

# =========================================================
# MENU
# =========================================================
def interactive_menu(s3: S3Storage):
    while True:
        cfg = load_strategy_config(s3)
        current = int(cfg.get("active_strategy", 1))
        mode = cfg.get("mode", "rotation")
        print("\n==============================")
        print("Ассистент WB — управление рекламой")
        print("==============================")
        print(f"Режим: {mode}")
        print(f"Текущая активная стратегия: {current} — {STRATEGY_NAMES.get(current)}")
        print("1. Зафиксировать стратегию 1 — Максимизация прибыли")
        print("2. Зафиксировать стратегию 2 — Удержание / рост позиции")
        print("3. Зафиксировать стратегию 3 — Контроль ДРР")
        print("4. Зафиксировать стратегию 4 — Доля трафика")
        print("5. Включить недельную ротацию стратегий")
        print("6. Предпросмотр текущей недели")
        print("7. Запуск текущей недели dry-run")
        print("8. Запуск текущей недели боевой")
        print("9. Показать отчёт по эффективности стратегий")
        print("0. Выход")
        choice = input("Выберите действие: ").strip()
        if choice == "0":
            print("Выход.")
            return
        if choice in {"1", "2", "3", "4"}:
            cfg["mode"] = "fixed"
            cfg["active_strategy"] = int(choice)
            save_strategy_config(s3, cfg)
            print(f"Сохранена фиксированная стратегия: {choice}")
            continue
        if choice == "5":
            cfg["mode"] = "rotation"
            save_strategy_config(s3, cfg)
            print("Включена недельная ротация стратегий.")
            continue
        if choice == "6":
            run_pipeline(s3, dry_run=True, explicit_week=None)
            continue
        if choice == "7":
            run_pipeline(s3, dry_run=True, explicit_week=None)
            continue
        if choice == "8":
            run_pipeline(s3, dry_run=False, explicit_week=None)
            continue
        if choice == "9":
            print_effectiveness_report(s3)
            continue
        print("Неизвестная команда.")

# =========================================================
# MAIN
# =========================================================
def main():
    parser = argparse.ArgumentParser(description="Ассистент WB — управление рекламными ставками и тестом стратегий")
    sub = parser.add_subparsers(dest="command")

    p_set = sub.add_parser("set-strategy", help="Зафиксировать стратегию")
    p_set.add_argument("strategy_id", type=int, choices=[1, 2, 3, 4])

    sub.add_parser("enable-rotation", help="Включить недельную ротацию")

    p_preview = sub.add_parser("preview", help="Предпросмотр")
    p_preview.add_argument("--week", type=str, default=None)

    p_run = sub.add_parser("run", help="Запуск")
    p_run.add_argument("--dry-run", action="store_true")
    p_run.add_argument("--week", type=str, default=None)
    p_run.add_argument("--strategy-id", type=int, choices=[1, 2, 3, 4], default=None)

    sub.add_parser("report", help="Отчёт по эффективности стратегий")

    args = parser.parse_args()

    required_env = ["YC_ACCESS_KEY_ID", "YC_SECRET_ACCESS_KEY", "YC_BUCKET_NAME", "WB_PROMO_KEY_TOPFACE"]
    missing = [v for v in required_env if not os.environ.get(v)]
    if missing:
        raise RuntimeError(f"Отсутствуют переменные окружения: {missing}")

    s3 = S3Storage(
        access_key=os.environ["YC_ACCESS_KEY_ID"],
        secret_key=os.environ["YC_SECRET_ACCESS_KEY"],
        bucket_name=os.environ["YC_BUCKET_NAME"],
    )

    if args.command is None:
        interactive_menu(s3)
        return

    if args.command == "set-strategy":
        cfg = load_strategy_config(s3)
        cfg["mode"] = "fixed"
        cfg["active_strategy"] = int(args.strategy_id)
        save_strategy_config(s3, cfg)
        log(f"✅ Сохранена фиксированная стратегия: {args.strategy_id} — {STRATEGY_NAMES[args.strategy_id]}")
        return

    if args.command == "enable-rotation":
        cfg = load_strategy_config(s3)
        cfg["mode"] = "rotation"
        save_strategy_config(s3, cfg)
        log("✅ Включена недельная ротация стратегий")
        return

    if args.command == "preview":
        run_pipeline(s3, dry_run=True, explicit_week=args.week)
        return

    if args.command == "run":
        run_pipeline(s3, dry_run=bool(args.dry_run), explicit_week=args.week, forced_strategy_id=args.strategy_id)
        return

    if args.command == "report":
        print_effectiveness_report(s3)
        return

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"❌ Критическая ошибка: {e}")
        traceback.print_exc()
        raise
