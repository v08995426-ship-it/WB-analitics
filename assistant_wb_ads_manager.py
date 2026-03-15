#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import io
import re
import json
import time
import math
import argparse
import tempfile
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple, Any

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

WB_BIDS_URL = "https://advert-api.wildberries.ru/api/advert/v1/bids"

ACTIVE_STATUS_VALUES = {"активна", "active", "запущена", "started"}

# шаги ставок в копейках
STEP_CPC_SOFT = 100
STEP_CPC_HARD = 200
STEP_CPM_SOFT = 500
STEP_CPM_HARD = 1000

MIN_CPC_SEARCH = 400
MIN_CPM_SEARCH = 4000
MIN_CPM_SHELVES = 5000

MAX_CPC_SEARCH = 15000
MAX_CPM_SEARCH = 60000
MAX_CPM_SHELVES = 100000

# бизнес-пороги
BAD_RATING_THRESHOLD = 4.6
OK_RATING_THRESHOLD = 4.7
GOOD_RATING_THRESHOLD = 4.8

TOP10_BORDER = 10
TOP20_BORDER = 20

MIN_IMPRESSIONS_SHELVES = 5000
MIN_CLICKS_SHELVES = 40
MIN_CTR_SHELVES = 0.4   # %
GOOD_CTR_SHELVES = 0.8  # %

MIN_WEEK_SPEND = 300.0
MIN_WEEK_ORDERS = 3

# Порог DRR не как абсолютная цель бизнеса, а как защитный лимит
TARGET_DRR_CPC = 12.0
TARGET_DRR_CPM_SEARCH = 14.0
TARGET_DRR_CPM_SHELVES = 10.0

# Допустимая минимальная чистая прибыль на единицу
MIN_NET_PROFIT_PER_UNIT = 0.0

# если позиция низкая, но запрос почти не даёт заказов — не разгоняем
MIN_KEYWORD_ORDERS_FOR_PUSH = 2

# =========================================================
# ЛОГ
# =========================================================

def log(msg: str):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


# =========================================================
# S3 / OBJECT STORAGE
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
            config=Config(
                signature_version="s3v4",
                read_timeout=300,
                connect_timeout=60,
                retries={"max_attempts": 5},
            ),
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

    def list_files(self, prefix: str) -> List[str]:
        out = []
        token = None
        while True:
            kwargs = {"Bucket": self.bucket, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            resp = self.s3.list_objects_v2(**kwargs)
            for obj in resp.get("Contents", []):
                out.append(obj["Key"])
            if resp.get("IsTruncated"):
                token = resp.get("NextContinuationToken")
            else:
                break
        return out


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

def iso_week_label(d: date) -> str:
    y, w, _ = d.isocalendar()
    return f"{y}-W{w:02d}"

def week_start_from_label(label: str) -> date:
    m = re.match(r"^(\d{4})-W(\d{2})$", label)
    if not m:
        raise ValueError(f"Некорректная неделя: {label}")
    y = int(m.group(1))
    w = int(m.group(2))
    return date.fromisocalendar(y, w, 1)

def target_week_range(explicit_week: Optional[str] = None) -> Tuple[str, date, date]:
    if explicit_week:
        start = week_start_from_label(explicit_week)
        end = start + timedelta(days=6)
        return explicit_week, start, end

    # по умолчанию: неделя, в которую попадает вчера
    yesterday = tz_now().date() - timedelta(days=1)
    start = yesterday - timedelta(days=yesterday.weekday())
    end = start + timedelta(days=6)
    return iso_week_label(start), start, end

def keywords_weekly_key(week_label: str) -> str:
    return f"{KEYWORDS_WEEKLY_PREFIX}Неделя {week_label}.xlsx"

def ads_weekly_key(week_label: str) -> str:
    return f"{ADS_WEEKLY_PREFIX}Реклама_{week_label}.xlsx"

def normalize_colname(name: str) -> str:
    return str(name).strip().lower().replace("ё", "е")

def round_money(v: float) -> float:
    return round(safe_float(v), 2)

def clamp(v: int, min_v: int, max_v: int) -> int:
    return max(min_v, min(v, max_v))

def percent(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)

def read_json_or_default(s3: S3Storage, key: str, default: dict) -> dict:
    try:
        if not s3.file_exists(key):
            return default
        return json.loads(s3.read_text(key))
    except Exception:
        return default


# =========================================================
# КЛАССИФИКАЦИЯ КАМПАНИЙ
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
# ЗАГРУЗКА ДАННЫХ ИЗ РЕКЛАМЫ
# =========================================================

def prepare_daily_stats_sheet(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    aliases = {
        "ID кампании": ["ID кампании", "ID", "Кампания ID", "advert_id"],
        "Артикул WB": ["Артикул WB", "Артикул", "nm_id"],
        "Название предмета": ["Название предмета", "Предмет", "subject"],
        "Дата": ["Дата", "День", "date"],
        "Расход": ["Расход", "Затраты", "spent"],
        "Сумма заказов": ["Сумма заказов", "Выручка", "orders_sum"],
        "Показы": ["Показы", "Просмотры", "views"],
        "Клики": ["Клики", "clicks"],
        "Заказы": ["Заказы", "orders"],
        "CTR": ["CTR", "ctr"],
        "CPC": ["CPC", "cpc"],
        "CR": ["CR", "cr"],
        "ДРР": ["ДРР", "ДРР %", "drr"],
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

    for col in ["Показы", "Клики", "Заказы", "Сумма заказов", "CTR", "CPC", "CR", "ДРР"]:
        if col not in df.columns:
            df[col] = 0

    df["Дата"] = pd.to_datetime(df["Дата"], errors="coerce").dt.date
    for col in ["ID кампании", "Артикул WB"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ["Расход", "Показы", "Клики", "Заказы", "Сумма заказов", "CTR", "CPC", "CR", "ДРР"]:
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
        "Тип оплаты": ["Тип оплаты", "Тип", "payment_type"],
        "Тип ставки": ["Тип ставки", "bid_type", "Тип ставки (ручной/единый)"],
        "Ставка в поиске (руб)": ["Ставка в поиске (руб)", "Ставка", "current_bid", "bid"],
        "Ставка в рекомендациях (руб)": ["Ставка в рекомендациях (руб)", "Ставка в рекомендациях", "recommendations bid"],
        "Название": ["Название", "Название кампании", "Кампания"],
        "Статус": ["Статус", "status"],
        "Размещение в рекомендациях": ["Размещение в рекомендациях", "recommendations"],
        "Артикул WB": ["Артикул WB", "Артикул", "nm_id"],
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

    for col in ["Ставка в поиске (руб)", "Ставка в рекомендациях (руб)"]:
        if col not in df.columns:
            df[col] = 0

    for col in ["Статус", "Размещение в рекомендациях", "Тип ставки", "Артикул WB"]:
        if col not in df.columns:
            df[col] = ""

    df["ID кампании"] = pd.to_numeric(df["ID кампании"], errors="coerce")
    df = df.dropna(subset=["ID кампании"]).copy()
    df["ID кампании"] = df["ID кампании"].astype("int64")
    df["Ставка в поиске (руб)"] = pd.to_numeric(df["Ставка в поиске (руб)"], errors="coerce").fillna(0)
    df["Ставка в рекомендациях (руб)"] = pd.to_numeric(df["Ставка в рекомендациях (руб)"], errors="coerce").fillna(0)
    df["Артикул WB"] = pd.to_numeric(df["Артикул WB"], errors="coerce")
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

    # fallback на недельный файл
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

    required = ["Неделя", "Артикул WB", "Чистая прибыль, руб/ед", "Валовая прибыль, руб/ед"]
    for col in required:
        if col not in df.columns:
            raise RuntimeError(f"В 'Юнит экономика' нет колонки '{col}'")

    df["Неделя"] = df["Неделя"].astype(str)
    df["Артикул WB"] = pd.to_numeric(df["Артикул WB"], errors="coerce")
    df = df[df["Неделя"] == week_label].copy()
    df = df.dropna(subset=["Артикул WB"]).copy()
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

    required = ["Артикул WB", "Рейтинг отзывов", "Частота запросов", "Частота за неделю", "Медианная позиция", "Переходы в карточку", "Заказы", "Поисковый запрос"]
    if not all(col in df.columns for col in required):
        log("ℹ️ В weekly keywords нет полного набора колонок, продолжаю без анализа ключей")
        return pd.DataFrame()

    df["Артикул WB"] = pd.to_numeric(df["Артикул WB"], errors="coerce")
    df = df.dropna(subset=["Артикул WB"]).copy()
    df["Артикул WB"] = df["Артикул WB"].astype("int64")

    for col in ["Рейтинг отзывов", "Частота запросов", "Частота за неделю", "Медианная позиция", "Переходы в карточку", "Заказы"]:
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
# ИСТОРИЯ СТАВОК
# =========================================================

def load_bid_history(s3: S3Storage) -> pd.DataFrame:
    if not s3.file_exists(SERVICE_HISTORY_KEY):
        return pd.DataFrame(columns=[
            "Дата запуска", "Неделя", "ID кампании", "Артикул WB", "Тип кампании",
            "Ставка поиск, коп", "Ставка рекомендации, коп", "Стратегия"
        ])

    try:
        df = s3.read_excel(SERVICE_HISTORY_KEY, sheet_name=0)
        if df.empty:
            return pd.DataFrame(columns=[
                "Дата запуска", "Неделя", "ID кампании", "Артикул WB", "Тип кампании",
                "Ставка поиск, коп", "Ставка рекомендации, коп", "Стратегия"
            ])
        return df
    except Exception:
        return pd.DataFrame(columns=[
            "Дата запуска", "Неделя", "ID кампании", "Артикул WB", "Тип кампании",
            "Ставка поиск, коп", "Ставка рекомендации, коп", "Стратегия"
        ])


def save_bid_history(s3: S3Storage, history_df: pd.DataFrame):
    s3.write_excel_sheets(SERVICE_HISTORY_KEY, {"history": history_df})


# =========================================================
# АГРЕГАЦИЯ НЕДЕЛЬНЫХ РЕКЛАМНЫХ МЕТРИК
# =========================================================

def build_campaign_week_metrics(
    stats_df: pd.DataFrame,
    campaigns_df: pd.DataFrame,
    economics_df: pd.DataFrame,
    keywords_agg_df: pd.DataFrame,
) -> pd.DataFrame:
    g = (
        stats_df.groupby(["ID кампании", "Артикул WB"], as_index=False)
        .agg(
            **{
                "Расход за неделю": ("Расход", "sum"),
                "Сумма заказов за неделю": ("Сумма заказов", "sum"),
                "Показы за неделю": ("Показы", "sum"),
                "Клики за неделю": ("Клики", "sum"),
                "Заказы за неделю": ("Заказы", "sum"),
            }
        )
    )

    g["CTR, % факт"] = g.apply(lambda r: percent(r["Клики за неделю"], r["Показы за неделю"]), axis=1)
    g["CR, % факт"] = g.apply(lambda r: percent(r["Заказы за неделю"], r["Клики за неделю"]), axis=1)
    g["CPC, руб факт"] = g.apply(
        lambda r: round(safe_float(r["Расход за неделю"]) / safe_float(r["Клики за неделю"]), 2) if r["Клики за неделю"] > 0 else 0.0,
        axis=1
    )
    g["ДРР, % факт"] = g.apply(
        lambda r: percent(r["Расход за неделю"], r["Сумма заказов за неделю"]) if r["Сумма заказов за неделю"] > 0 else 0.0,
        axis=1
    )

    # из кампаний
    c = campaigns_df.copy()
    c["Тип кампании"] = c.apply(classify_campaign, axis=1)
    c["Активна"] = c.apply(is_active_campaign, axis=1)

    merge_cols = [
        "ID кампании", "Название", "Тип оплаты", "Тип ставки", "Статус",
        "Размещение в рекомендациях", "Ставка в поиске (руб)", "Ставка в рекомендациях (руб)",
        "Тип кампании", "Активна"
    ]
    c = c[merge_cols].drop_duplicates(subset=["ID кампании"])

    m = g.merge(c, on="ID кампании", how="left")

    # из экономики
    econ_cols = [
        "Артикул WB", "Артикул продавца", "Предмет", "Бренд",
        "Средняя цена продажи", "Чистая прибыль, руб/ед", "Валовая прибыль, руб/ед",
        "Себестоимость, руб", "Реклама, руб/ед", "Комиссия WB, %", "Эквайринг, %",
        "Логистика прямая, руб/ед", "Логистика обратная, руб/ед"
    ]
    e = economics_df[econ_cols].drop_duplicates(subset=["Артикул WB"])
    m = m.merge(e, on="Артикул WB", how="left")

    # из keywords
    if not keywords_agg_df.empty:
        m = m.merge(keywords_agg_df, on="Артикул WB", how="left")
    else:
        for col in ["Рейтинг отзывов", "Частота запросов сумма", "Переходы в карточку сумма",
                    "Заказы по ключам сумма", "Медианная позиция заказных ключей", "Доля трафика, %", "Флаг плохого рейтинга"]:
            m[col] = 0 if "Флаг" not in col else "нет"

    # derived business metrics
    m["Ожидаемая чистая прибыль рекламы, руб"] = (
        m["Заказы за неделю"].fillna(0) * m["Чистая прибыль, руб/ед"].fillna(0) - m["Расход за неделю"].fillna(0)
    )
    m["Ожидаемая валовая прибыль рекламы, руб"] = (
        m["Заказы за неделю"].fillna(0) * m["Валовая прибыль, руб/ед"].fillna(0) - m["Расход за неделю"].fillna(0)
    )
    m["Профит на заказ после рекламы, руб"] = m.apply(
        lambda r: round(safe_float(r["Чистая прибыль, руб/ед"]) - safe_float(r["Расход за неделю"]) / safe_float(r["Заказы за неделю"]), 2)
        if r["Заказы за неделю"] > 0 else round(-safe_float(r["Расход за неделю"]), 2),
        axis=1
    )
    m["Текущая ставка поиск, коп"] = (m["Ставка в поиске (руб)"].fillna(0) * 100).round().astype(int)
    m["Текущая ставка рекомендации, коп"] = (m["Ставка в рекомендациях (руб)"].fillna(0) * 100).round().astype(int)

    m = m[m["Активна"] == True].copy()
    m = m.sort_values(["Расход за неделю", "Заказы за неделю"], ascending=[False, False]).reset_index(drop=True)
    return m


# =========================================================
# СТРАТЕГИИ
# =========================================================

@dataclass
class Decision:
    strategy_id: int
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


def decide_cpc_search(row: pd.Series, strategy_id: int, week_label: str) -> Optional[Decision]:
    current_bid = safe_int(row.get("Текущая ставка поиск, коп"))
    if current_bid <= 0:
        return None

    drr = safe_float(row.get("ДРР, % факт"))
    profit_u = safe_float(row.get("Чистая прибыль, руб/ед"))
    pos = safe_float(row.get("Медианная позиция заказных ключей"))
    rating = safe_float(row.get("Рейтинг отзывов"))
    orders_kw = safe_float(row.get("Заказы по ключам сумма"))
    ad_profit = safe_float(row.get("Ожидаемая чистая прибыль рекламы, руб"))
    spend = safe_float(row.get("Расход за неделю"))

    step_up = STEP_CPC_SOFT if strategy_id == 1 else STEP_CPC_HARD
    step_down = STEP_CPC_SOFT if strategy_id == 1 else STEP_CPC_HARD

    new_bid = current_bid
    action = "KEEP"
    reason = "Без изменений"

    if rating and rating < BAD_RATING_THRESHOLD:
        new_bid = max(current_bid - step_down, MIN_CPC_SEARCH)
        action = "DOWN"
        reason = f"Низкий рейтинг отзывов {rating:.2f} (< {BAD_RATING_THRESHOLD})"
    elif profit_u <= MIN_NET_PROFIT_PER_UNIT:
        new_bid = max(current_bid - step_down, MIN_CPC_SEARCH)
        action = "DOWN"
        reason = f"Чистая прибыль на единицу {profit_u:.2f} ≤ {MIN_NET_PROFIT_PER_UNIT:.2f}"
    elif spend >= MIN_WEEK_SPEND and ad_profit < 0:
        new_bid = max(current_bid - step_down, MIN_CPC_SEARCH)
        action = "DOWN"
        reason = f"Реклама убыточна по неделе: {ad_profit:.2f} руб"
    elif drr > TARGET_DRR_CPC:
        new_bid = max(current_bid - step_down, MIN_CPC_SEARCH)
        action = "DOWN"
        reason = f"ДРР {drr:.2f}% > {TARGET_DRR_CPC:.2f}%"
    elif pos > TOP20_BORDER and profit_u > 0 and rating >= OK_RATING_THRESHOLD and orders_kw >= MIN_KEYWORD_ORDERS_FOR_PUSH:
        new_bid = min(current_bid + step_up, MAX_CPC_SEARCH)
        action = "UP"
        reason = f"Медианная позиция {pos:.1f} хуже TOP20, товар прибыльный"
    elif strategy_id == 2 and pos > TOP10_BORDER and profit_u > 0 and rating >= GOOD_RATING_THRESHOLD and orders_kw >= MIN_KEYWORD_ORDERS_FOR_PUSH:
        new_bid = min(current_bid + step_up, MAX_CPC_SEARCH)
        action = "UP"
        reason = f"Стратегия роста: позиция {pos:.1f} хуже TOP10, рейтинг {rating:.2f}"
    else:
        return None

    if new_bid == current_bid:
        return None

    return Decision(
        strategy_id=strategy_id,
        id_campaign=safe_int(row["ID кампании"]),
        nm_id=safe_int(row["Артикул WB"]),
        campaign_name=str(row.get("Название", "")),
        campaign_type="cpc_search",
        action=action,
        reason=reason,
        current_search_bid_kop=current_bid,
        new_search_bid_kop=new_bid,
        current_rec_bid_kop=0,
        new_rec_bid_kop=0,
        week_label=week_label,
    )


def decide_cpm_search(row: pd.Series, strategy_id: int, week_label: str) -> Optional[Decision]:
    current_bid = safe_int(row.get("Текущая ставка поиск, коп"))
    if current_bid <= 0:
        return None

    drr = safe_float(row.get("ДРР, % факт"))
    profit_u = safe_float(row.get("Чистая прибыль, руб/ед"))
    pos = safe_float(row.get("Медианная позиция заказных ключей"))
    rating = safe_float(row.get("Рейтинг отзывов"))
    ad_profit = safe_float(row.get("Ожидаемая чистая прибыль рекламы, руб"))
    spend = safe_float(row.get("Расход за неделю"))

    step_up = STEP_CPM_SOFT if strategy_id == 1 else STEP_CPM_HARD
    step_down = STEP_CPM_SOFT if strategy_id == 1 else STEP_CPM_HARD

    new_bid = current_bid
    action = "KEEP"
    reason = "Без изменений"

    if rating and rating < BAD_RATING_THRESHOLD:
        new_bid = max(current_bid - step_down, MIN_CPM_SEARCH)
        action = "DOWN"
        reason = f"Низкий рейтинг отзывов {rating:.2f}"
    elif profit_u <= MIN_NET_PROFIT_PER_UNIT:
        new_bid = max(current_bid - step_down, MIN_CPM_SEARCH)
        action = "DOWN"
        reason = f"Чистая прибыль на единицу {profit_u:.2f} ≤ {MIN_NET_PROFIT_PER_UNIT:.2f}"
    elif spend >= MIN_WEEK_SPEND and ad_profit < 0:
        new_bid = max(current_bid - step_down, MIN_CPM_SEARCH)
        action = "DOWN"
        reason = f"CPM-поиск убыточен по неделе: {ad_profit:.2f}"
    elif drr > TARGET_DRR_CPM_SEARCH:
        new_bid = max(current_bid - step_down, MIN_CPM_SEARCH)
        action = "DOWN"
        reason = f"ДРР {drr:.2f}% > {TARGET_DRR_CPM_SEARCH:.2f}%"
    elif pos > TOP20_BORDER and profit_u > 0 and rating >= OK_RATING_THRESHOLD:
        new_bid = min(current_bid + step_up, MAX_CPM_SEARCH)
        action = "UP"
        reason = f"CPM-поиск: позиция {pos:.1f} хуже TOP20 при положительной экономике"
    elif strategy_id == 2 and pos > TOP10_BORDER and profit_u > 0 and rating >= GOOD_RATING_THRESHOLD:
        new_bid = min(current_bid + step_up, MAX_CPM_SEARCH)
        action = "UP"
        reason = f"Стратегия роста CPM-поиска: позиция {pos:.1f} хуже TOP10"
    else:
        return None

    if new_bid == current_bid:
        return None

    return Decision(
        strategy_id=strategy_id,
        id_campaign=safe_int(row["ID кампании"]),
        nm_id=safe_int(row["Артикул WB"]),
        campaign_name=str(row.get("Название", "")),
        campaign_type="cpm_search",
        action=action,
        reason=reason,
        current_search_bid_kop=current_bid,
        new_search_bid_kop=new_bid,
        current_rec_bid_kop=0,
        new_rec_bid_kop=0,
        week_label=week_label,
    )


def decide_cpm_shelves(row: pd.Series, strategy_id: int, week_label: str) -> Optional[Decision]:
    current_bid = safe_int(row.get("Текущая ставка рекомендации, коп"))
    if current_bid <= 0:
        return None

    ctr = safe_float(row.get("CTR, % факт"))
    clicks = safe_float(row.get("Клики за неделю"))
    views = safe_float(row.get("Показы за неделю"))
    orders = safe_float(row.get("Заказы за неделю"))
    drr = safe_float(row.get("ДРР, % факт"))
    profit_u = safe_float(row.get("Чистая прибыль, руб/ед"))
    ad_profit = safe_float(row.get("Ожидаемая чистая прибыль рекламы, руб"))
    rating = safe_float(row.get("Рейтинг отзывов"))

    step_up = STEP_CPM_SOFT if strategy_id == 1 else STEP_CPM_HARD
    step_down = STEP_CPM_SOFT if strategy_id == 1 else STEP_CPM_HARD

    new_bid = current_bid
    action = "KEEP"
    reason = "Без изменений"

    if rating and rating < BAD_RATING_THRESHOLD:
        new_bid = max(current_bid - step_down, MIN_CPM_SHELVES)
        action = "DOWN"
        reason = f"Низкий рейтинг отзывов {rating:.2f}"
    elif profit_u <= MIN_NET_PROFIT_PER_UNIT:
        new_bid = max(current_bid - step_down, MIN_CPM_SHELVES)
        action = "DOWN"
        reason = f"Чистая прибыль на единицу {profit_u:.2f} ≤ {MIN_NET_PROFIT_PER_UNIT:.2f}"
    elif views >= MIN_IMPRESSIONS_SHELVES and clicks >= MIN_CLICKS_SHELVES and ctr < MIN_CTR_SHELVES:
        new_bid = max(current_bid - step_down, MIN_CPM_SHELVES)
        action = "DOWN"
        reason = f"Полки: низкий CTR {ctr:.2f}% при {views:.0f} показах"
    elif ad_profit < 0 and drr > TARGET_DRR_CPM_SHELVES:
        new_bid = max(current_bid - step_down, MIN_CPM_SHELVES)
        action = "DOWN"
        reason = f"Полки убыточны: ДРР {drr:.2f}% и прибыль рекламы {ad_profit:.2f}"
    elif strategy_id == 2 and profit_u > 0 and ctr >= GOOD_CTR_SHELVES and orders >= MIN_WEEK_ORDERS:
        new_bid = min(current_bid + step_up, MAX_CPM_SHELVES)
        action = "UP"
        reason = f"Полки: хороший CTR {ctr:.2f}% и положительная экономика"
    elif strategy_id == 3 and profit_u > 0 and ctr >= MIN_CTR_SHELVES and ad_profit > 0:
        new_bid = min(current_bid + step_up, MAX_CPM_SHELVES)
        action = "UP"
        reason = f"Агрессивный рост полок: реклама прибыльна, CTR {ctr:.2f}%"
    else:
        return None

    if new_bid == current_bid:
        return None

    return Decision(
        strategy_id=strategy_id,
        id_campaign=safe_int(row["ID кампании"]),
        nm_id=safe_int(row["Артикул WB"]),
        campaign_name=str(row.get("Название", "")),
        campaign_type="cpm_shelves",
        action=action,
        reason=reason,
        current_search_bid_kop=0,
        new_search_bid_kop=0,
        current_rec_bid_kop=current_bid,
        new_rec_bid_kop=new_bid,
        week_label=week_label,
    )


def build_decisions(metrics_df: pd.DataFrame, strategy_id: int, week_label: str) -> List[Decision]:
    decisions: List[Decision] = []

    for _, row in metrics_df.iterrows():
        ctype = str(row.get("Тип кампании", "unknown"))

        if ctype == "manual_mixed" or ctype == "manual_search":
            continue
        if ctype == "unknown":
            continue

        decision = None
        if ctype == "cpc_search":
            decision = decide_cpc_search(row, strategy_id, week_label)
        elif ctype == "cpm_search":
            decision = decide_cpm_search(row, strategy_id, week_label)
        elif ctype == "cpm_shelves":
            decision = decide_cpm_shelves(row, strategy_id, week_label)

        if decision:
            decisions.append(decision)

    return decisions


# =========================================================
# ПОДГОТОВКА PAYLOAD ДЛЯ WB
# =========================================================

def decisions_to_payload(decisions: List[Decision]) -> List[Dict[str, Any]]:
    grouped: Dict[int, Dict[str, Any]] = {}

    for d in decisions:
        if d.id_campaign not in grouped:
            grouped[d.id_campaign] = {
                "advert_id": d.id_campaign,
                "nm_bids": []
            }

        nm_bid = {
            "nm_id": d.nm_id
        }

        # используем ту же логику полей search/recommendations, что была в старом скрипте
        if d.campaign_type in {"cpc_search", "cpm_search"}:
            nm_bid["search"] = d.new_search_bid_kop
        elif d.campaign_type == "cpm_shelves":
            nm_bid["recommendations"] = d.new_rec_bid_kop

        grouped[d.id_campaign]["nm_bids"].append(nm_bid)

    return list(grouped.values())


def send_batches(payload: List[Dict[str, Any]], api_key: str, dry_run: bool = True) -> Tuple[int, int]:
    if dry_run:
        log("🧪 dry-run: отправка ставок отключена")
        return len(payload), 0

    headers = {
        "Authorization": api_key.strip(),
        "Content-Type": "application/json",
    }

    success = 0
    failed = 0

    batch_size = 50
    batches = [payload[i:i + batch_size] for i in range(0, len(payload), batch_size)]

    for idx, batch in enumerate(batches, start=1):
        log(f"📤 Отправка батча {idx}/{len(batches)}: кампаний {len(batch)}")
        try:
            resp = requests.post(WB_BIDS_URL, headers=headers, json=batch, timeout=120)
            if resp.status_code == 200:
                success += len(batch)
                time.sleep(0.2)
            else:
                failed += len(batch)
                log(f"⚠️ Ошибка WB {resp.status_code}: {resp.text[:500]}")
        except Exception as e:
            failed += len(batch)
            log(f"⚠️ Исключение отправки: {e}")

    return success, failed


# =========================================================
# СОХРАНЕНИЕ PREVIEW / SUMMARY
# =========================================================

def decisions_to_df(decisions: List[Decision], metrics_df: pd.DataFrame) -> pd.DataFrame:
    if not decisions:
        return pd.DataFrame(columns=[
            "Неделя", "ID кампании", "Название", "Артикул WB", "Тип кампании",
            "Действие", "Причина",
            "Текущая ставка поиск, коп", "Новая ставка поиск, коп",
            "Текущая ставка рекомендации, коп", "Новая ставка рекомендации, коп"
        ])

    ddf = pd.DataFrame([d.__dict__ for d in decisions]).rename(columns={
        "strategy_id": "Стратегия",
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
        "Чистая прибыль, руб/ед", "Валовая прибыль, руб/ед", "Рейтинг отзывов",
        "Медианная позиция заказных ключей", "Доля трафика, %"
    ]
    enrich = metrics_df[enrich_cols].drop_duplicates(subset=["ID кампании", "Артикул WB"])

    out = ddf.merge(enrich, on=["ID кампании", "Артикул WB"], how="left")
    return out


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
        "recommendations_count": 0 if decisions_df is None else int(len(decisions_df)),
        "metrics_count": 0 if metrics_df is None else int(len(metrics_df)),
    }
    s3.write_text(SERVICE_LOG_KEY, json.dumps(summary, ensure_ascii=False, indent=2))


# =========================================================
# КОНФИГ СТРАТЕГИИ
# =========================================================

def load_strategy_config(s3: S3Storage) -> dict:
    return read_json_or_default(s3, SERVICE_CONFIG_KEY, {"active_strategy": 1})


def save_strategy_config(s3: S3Storage, strategy_id: int):
    data = {"active_strategy": int(strategy_id), "updated_at": datetime.now().isoformat()}
    s3.write_text(SERVICE_CONFIG_KEY, json.dumps(data, ensure_ascii=False, indent=2))


# =========================================================
# ОСНОВНОЙ PIPELINE
# =========================================================

def run_pipeline(s3: S3Storage, strategy_id: int, dry_run: bool, explicit_week: Optional[str]):
    week_label, week_start, week_end = target_week_range(explicit_week)
    log(f"🎯 Целевая неделя: {week_label} ({week_start} — {week_end})")
    log("📌 Важно: расходы рекламы считаются только внутри этой недели")

    stats_df, campaigns_df = load_advertising_data(s3, week_label, week_start, week_end)
    economics_df = load_unit_economics(s3, week_label)
    keywords_df = load_keywords_weekly(s3, week_label)
    keywords_agg_df = aggregate_keywords(keywords_df)

    metrics_df = build_campaign_week_metrics(stats_df, campaigns_df, economics_df, keywords_agg_df)

    # оставляем только предметы, с которыми вы работаете
    if "Предмет" in metrics_df.columns:
        allowed = {"Кисти косметические", "Помады", "Косметические карандаши", "Блески"}
        metrics_df = metrics_df[metrics_df["Предмет"].astype(str).isin(allowed)].copy()

    decisions = build_decisions(metrics_df, strategy_id, week_label)
    decisions_df = decisions_to_df(decisions, metrics_df)

    # история ставок
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
            "Неделя", "ID кампании", "Название", "Артикул WB", "Тип кампании", "Действие",
            "Текущая ставка поиск, коп", "Новая ставка поиск, коп",
            "Текущая ставка рекомендации, коп", "Новая ставка рекомендации, коп",
            "ДРР, % факт", "Ожидаемая чистая прибыль рекламы, руб",
            "Чистая прибыль, руб/ед", "Рейтинг отзывов", "Медианная позиция заказных ключей", "Причина"
        ]
        show_cols = [c for c in show_cols if c in decisions_df.columns]
        print(decisions_df[show_cols].head(50).to_string(index=False))

    if not decisions:
        log("ℹ️ Нет изменений ставок")
        return

    payload = decisions_to_payload(decisions)
    wb_key = os.environ.get("WB_PROMO_KEY_TOPFACE", "").strip()
    if not wb_key:
        raise RuntimeError("Не задан секрет WB_PROMO_KEY_TOPFACE")

    success, failed = send_batches(payload, wb_key, dry_run=dry_run)
    log(f"✅ Успешно обработано кампаний: {success}")
    log(f"⚠️ Не обработано кампаний: {failed}")


# =========================================================
# ИНТЕРАКТИВНОЕ МЕНЮ
# =========================================================

def interactive_menu(s3: S3Storage):
    while True:
        cfg = load_strategy_config(s3)
        current = int(cfg.get("active_strategy", 1))

        print("\n==============================")
        print("Ассистент WB — управление рекламой")
        print("==============================")
        print(f"Текущая активная стратегия: {current}")
        print("1. Стратегия 1 — Защита прибыли")
        print("2. Стратегия 2 — Рост с контролем прибыли")
        print("3. Стратегия 3 — Полки / агрессивный рост")
        print("4. Предпросмотр активной стратегии")
        print("5. Запуск активной стратегии (dry-run)")
        print("6. Запуск активной стратегии (боевой)")
        print("0. Выход")

        choice = input("Выберите действие: ").strip()

        if choice == "0":
            print("Выход.")
            return

        if choice in {"1", "2", "3"}:
            save_strategy_config(s3, int(choice))
            print(f"Сохранена активная стратегия: {choice}")
            continue

        if choice == "4":
            cfg = load_strategy_config(s3)
            run_pipeline(s3, int(cfg.get("active_strategy", 1)), dry_run=True, explicit_week=None)
            continue

        if choice == "5":
            cfg = load_strategy_config(s3)
            run_pipeline(s3, int(cfg.get("active_strategy", 1)), dry_run=True, explicit_week=None)
            continue

        if choice == "6":
            cfg = load_strategy_config(s3)
            run_pipeline(s3, int(cfg.get("active_strategy", 1)), dry_run=False, explicit_week=None)
            continue

        print("Неизвестная команда.")


# =========================================================
# MAIN
# =========================================================

def main():
    parser = argparse.ArgumentParser(description="Ассистент WB — управление рекламными ставками")
    sub = parser.add_subparsers(dest="command")

    p_set = sub.add_parser("set-strategy", help="Сохранить активную стратегию")
    p_set.add_argument("strategy_id", type=int, choices=[1, 2, 3])

    p_preview = sub.add_parser("preview", help="Предпросмотр активной стратегии")
    p_preview.add_argument("--week", type=str, default=None, help="Неделя вида 2026-W11")

    p_run = sub.add_parser("run", help="Запуск стратегии")
    p_run.add_argument("--dry-run", action="store_true", help="Не отправлять ставки в WB")
    p_run.add_argument("--week", type=str, default=None, help="Неделя вида 2026-W11")

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
        save_strategy_config(s3, args.strategy_id)
        log(f"✅ Активная стратегия сохранена: {args.strategy_id}")
        return

    cfg = load_strategy_config(s3)
    strategy_id = int(cfg.get("active_strategy", 1))

    if args.command == "preview":
        run_pipeline(s3, strategy_id, dry_run=True, explicit_week=args.week)
        return

    if args.command == "run":
        run_pipeline(s3, strategy_id, dry_run=bool(args.dry_run), explicit_week=args.week)
        return


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"❌ Критическая ошибка: {e}")
        traceback.print_exc()
        raise
