#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
assistant_wb_ads_manager.py

Новый скрипт управления рекламными ставками WB для магазина TOPFACE.

Логика принятия решений строго бинарная:
- ДРР кампании >= 10.0% -> снизить ставку;
- ДРР кампании < 10.0% -> повысить ставку.

Активная управляемая строка ставки не удерживается без действия, кроме технических
исключений с фиксированным reason_code.
"""

from __future__ import annotations

import argparse
import io
import json
import math
import os
import re
import sys
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

import boto3
import pandas as pd
import requests
from botocore.exceptions import ClientError


# =============================
# Константы проекта
# =============================

SCRIPT_NAME = "assistant_wb_ads_manager.py"
SCRIPT_VERSION = "strict-drr-sheet-aware-v3-2026-05-13"
STORE_NAME = "TOPFACE"
DRR_LIMIT_PCT = 10.0
TECHNICAL_BID_FLOOR_RUB = 1.0

MANAGED_SUBJECTS = {
    "кисти косметические",
    "помады",
    "блески",
    "косметические карандаши",
}

SERVICE_PREFIX = "Служебные файлы/Ассистент WB/TOPFACE/"
ADS_MAIN_KEY = "Отчёты/Реклама/TOPFACE/Анализ рекламы.xlsx"
ADS_WEEKLY_PREFIX = "Отчёты/Реклама/TOPFACE/Недельные/"

BID_HISTORY_KEY = SERVICE_PREFIX + "История_ставок.xlsx"
PAUSE_HISTORY_KEY = SERVICE_PREFIX + "История_пауз.xlsx"
RUN_OUTPUT_KEY = SERVICE_PREFIX + "Итог_последнего_запуска.xlsx"
PREVIEW_OUTPUT_KEY = SERVICE_PREFIX + "Предпросмотр_последнего_запуска.xlsx"
SUMMARY_JSON_KEY = SERVICE_PREFIX + "Сводка_последнего_запуска.json"
API_LOG_KEY = SERVICE_PREFIX + "Лог_API.xlsx"

WB_ADVERT_BASE_URL = "https://advert-api.wildberries.ru"
WB_BIDS_ENDPOINT = "/api/advert/v1/bids"
WB_PAUSE_ENDPOINT = "/adv/v0/pause"
WB_START_ENDPOINT = "/adv/v0/start"

BID_HISTORY_COLUMNS = [
    "event_id",
    "run_datetime",
    "event_date",
    "campaign_id",
    "nm_id",
    "supplier_article",
    "subject_norm",
    "placement",
    "old_bid_rub",
    "new_bid_rub",
    "direction",
    "reason_code",
    "spend_before",
    "revenue_before",
    "orders_before",
    "impressions_before",
    "clicks_before",
    "drr_before",
    "gp_before",
    "postcheck_status",
    "final_verdict",
    "d1_verdict",
    "d3_verdict",
    "d1_check_date",
    "d3_check_date",
]

PAUSE_HISTORY_COLUMNS = [
    "pause_event_id",
    "pause_date",
    "campaign_id",
    "nm_id",
    "placement",
    "supplier_article",
    "reason_code",
    "spend_before_pause",
    "revenue_before_pause",
    "orders_before_pause",
    "drr_before_pause",
    "gp_before_pause",
    "status",
    "next_check_date",
    "api_status",
]

DECISION_COLUMNS = [
    "campaign_id",
    "nm_id",
    "supplier_article",
    "subject_norm",
    "placement",
    "campaign_status",
    "current_bid_rub",
    "new_bid_rub",
    "action",
    "reason_code",
    "reason_text",
    "spend",
    "revenue",
    "orders",
    "impressions",
    "clicks",
    "campaign_drr_pct",
    "cpo",
    "ctr_pct",
    "gp_after_ads",
    "previous_event_id",
    "postcheck_status",
    "pause_decision",
]

COLUMN_ALIASES: Dict[str, List[str]] = {
    "date": ["Дата", "date", "day", "День"],
    "campaign_id": ["ID кампании", "advertId", "advert_id", "campaign_id", "Кампания ID"],
    "campaign_name": ["Название", "Название кампании", "name", "campaign_name"],
    "campaign_status": ["Статус", "status", "Статус кампании"],
    "nm_id": ["nmId", "nm_id", "Номенклатура WB", "Артикул WB", "Товар"],
    "supplier_article": ["Артикул продавца", "supplier_article", "supplierArticle", "Артикул"],
    "subject_norm": ["Предмет", "subject", "subject_norm", "Название предмета"],
    "placement": ["Плейсмент", "placement", "Тип кампании", "Место размещения", "placement_norm"],
    "current_bid_rub": ["Текущая ставка, ₽", "Текущая ставка", "Ставка", "Ставка в поиске (руб)", "Ставка в рекомендациях (руб)", "bid", "cpc", "cpm"],
    "impressions": ["Показы", "views", "impressions"],
    "clicks": ["Клики", "clicks"],
    "orders": ["Заказы РК", "Заказы", "orders"],
    "spend": ["Расход", "Расходы", "Затраты", "Расход РК", "ad_spend"],
    "revenue": ["Выручка РК", "Продажи РК", "Сумма заказов", "Сумма заказов, ₽", "Заказано на сумму", "Заказано на сумму, ₽", "ordersSumRub", "sum_price", "sumPrice", "sales", "revenue", "GMV"],
    "gp_after_ads": ["ВП кампании", "Валовая прибыль после рекламы", "ВП после рекламы", "gross_profit"],
}


# =============================
# Конфигурация и S3
# =============================

@dataclass
class Config:
    yc_access_key_id: str
    yc_secret_access_key: str
    yc_bucket_name: str
    wb_promo_key: str
    s3_endpoint_url: str = "https://storage.yandexcloud.net"
    wb_base_url: str = WB_ADVERT_BASE_URL


@dataclass
class RunContext:
    mode: str
    dry_run: bool
    apply_pause: bool
    apply_start: bool
    run_datetime: datetime
    mature_end: date
    current_start: date
    current_end: date
    base_start: date
    base_end: date


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Не задан обязательный secret/env: {name}")
    return value


def load_config() -> Config:
    return Config(
        yc_access_key_id=require_env("YC_ACCESS_KEY_ID"),
        yc_secret_access_key=require_env("YC_SECRET_ACCESS_KEY"),
        yc_bucket_name=require_env("YC_BUCKET_NAME"),
        wb_promo_key=require_env("WB_PROMO_KEY_TOPFACE"),
    )


def make_s3_client(config: Config):
    return boto3.client(
        "s3",
        endpoint_url=config.s3_endpoint_url,
        aws_access_key_id=config.yc_access_key_id,
        aws_secret_access_key=config.yc_secret_access_key,
    )


def s3_key_exists(s3_client, bucket: str, key: str) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def read_s3_bytes(s3_client, bucket: str, key: str) -> bytes:
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read()


def upload_s3_bytes(s3_client, bucket: str, key: str, payload: bytes, content_type: Optional[str] = None) -> None:
    extra: Dict[str, Any] = {}
    if content_type:
        extra["ContentType"] = content_type
    s3_client.put_object(Bucket=bucket, Key=key, Body=payload, **extra)


def list_s3_keys(s3_client, bucket: str, prefix: str) -> List[str]:
    keys: List[str] = []
    continuation_token: Optional[str] = None
    while True:
        kwargs: Dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token
        resp = s3_client.list_objects_v2(**kwargs)
        for item in resp.get("Contents", []):
            key = item.get("Key", "")
            if key:
                keys.append(key)
        if not resp.get("IsTruncated"):
            break
        continuation_token = resp.get("NextContinuationToken")
    return keys


# =============================
# Helper-функции колонок
# =============================

def _norm_col_name(value: Any) -> str:
    text = str(value).strip().lower()
    text = re.sub(r"\s+", " ", text)
    text = text.replace("ё", "е")
    return text


def find_col(df: pd.DataFrame, aliases: Iterable[str]) -> Optional[str]:
    """Возвращает имя первой найденной колонки из списка aliases или None."""
    if df is None or df.empty and len(df.columns) == 0:
        return None
    by_norm = {_norm_col_name(col): col for col in df.columns}
    for alias in aliases:
        found = by_norm.get(_norm_col_name(alias))
        if found is not None:
            return found
    return None


def series_or_default(df: pd.DataFrame, aliases: Iterable[str], default: Any = "") -> pd.Series:
    """Всегда возвращает pandas.Series длины len(df), даже если колонка отсутствует."""
    col = find_col(df, aliases)
    if col is None:
        return pd.Series([default] * len(df), index=df.index)
    return df[col]


def numeric_series(df: pd.DataFrame, aliases: Iterable[str], default: float = 0.0) -> pd.Series:
    """Возвращает числовой Series; все нечисловые значения -> default."""
    src = series_or_default(df, aliases, default=default)
    text = src.astype(str).str.replace("\u00a0", "", regex=False).str.replace(" ", "", regex=False)
    text = text.str.replace(",", ".", regex=False)
    text = text.str.replace(r"[^0-9.\-]", "", regex=True)
    num = pd.to_numeric(text, errors="coerce")
    return num.fillna(default).astype(float)


def parse_date_series(values: pd.Series) -> pd.Series:
    """Без warning разбирает даты из WB-отчётов: ISO, dd.mm.yyyy и Excel datetime."""
    if not isinstance(values, pd.Series):
        values = pd.Series(values)
    raw = values.copy()
    result = pd.Series(pd.NaT, index=raw.index, dtype="datetime64[ns]")
    text = raw.astype(str).str.strip()

    iso_mask = text.str.fullmatch(r"\d{4}-\d{2}-\d{2}", na=False)
    if iso_mask.any():
        result.loc[iso_mask] = pd.to_datetime(text.loc[iso_mask], format="%Y-%m-%d", errors="coerce")

    dot_mask = result.isna() & text.str.fullmatch(r"\d{2}\.\d{2}\.\d{4}", na=False)
    if dot_mask.any():
        result.loc[dot_mask] = pd.to_datetime(text.loc[dot_mask], format="%d.%m.%Y", errors="coerce")

    slash_mask = result.isna() & text.str.fullmatch(r"\d{2}/\d{2}/\d{4}", na=False)
    if slash_mask.any():
        result.loc[slash_mask] = pd.to_datetime(text.loc[slash_mask], format="%d/%m/%Y", errors="coerce")

    remaining = result.isna() & raw.notna() & text.ne("") & text.ne("NaT") & text.ne("nan")
    if remaining.any():
        result.loc[remaining] = pd.to_datetime(raw.loc[remaining], errors="coerce")
    return result.dt.date


def _clean_id_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "nat"}:
        return ""
    text = text.replace("\u00a0", " ").strip()
    if re.fullmatch(r"\d+\.0", text):
        text = text[:-2]
    return text


def _text_series(df: pd.DataFrame, aliases: Iterable[str], default: str = "") -> pd.Series:
    src = series_or_default(df, aliases, default=default)
    return src.map(_clean_text_value)


def _clean_text_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none", "nat"}:
        return ""
    return re.sub(r"\s+", " ", text)


def normalize_subject_value(value: Any) -> str:
    text = _clean_text_value(value).replace("ё", "е").strip()
    return re.sub(r"\s+", " ", text)


def normalize_placement_value(value: Any) -> str:
    text = _clean_text_value(value).replace("ё", "е").lower()
    if not text:
        return ""
    if "search" in text or "поиск" in text:
        return "search"
    if "recommend" in text or "рекоменд" in text:
        return "recommendations"
    if "combined" in text or "комбин" in text or "cpm" in text:
        return "combined"
    return text


def normalize_columns(df: pd.DataFrame, source_type: str) -> pd.DataFrame:
    """Создаёт канонические поля и не удаляет исходные колонки."""
    result = df.copy()
    result["source_type"] = source_type
    result["_row_id"] = range(len(result))

    date_src = series_or_default(result, COLUMN_ALIASES["date"], default=pd.NaT)
    result["date"] = parse_date_series(date_src)

    result["campaign_id"] = series_or_default(result, COLUMN_ALIASES["campaign_id"], default="").map(_clean_id_value)
    result["campaign_name"] = _text_series(result, COLUMN_ALIASES["campaign_name"], default="")
    result["campaign_status"] = _text_series(result, COLUMN_ALIASES["campaign_status"], default="")
    result["nm_id"] = series_or_default(result, COLUMN_ALIASES["nm_id"], default="").map(_clean_id_value)
    result["supplier_article"] = _text_series(result, COLUMN_ALIASES["supplier_article"], default="")
    result["subject_norm"] = series_or_default(result, COLUMN_ALIASES["subject_norm"], default="").map(normalize_subject_value)
    result["placement"] = series_or_default(result, COLUMN_ALIASES["placement"], default="").map(normalize_placement_value)

    for metric in ["current_bid_rub", "impressions", "clicks", "orders", "spend", "revenue"]:
        result[metric] = numeric_series(result, COLUMN_ALIASES[metric], default=0.0)

    gp_col = find_col(result, COLUMN_ALIASES["gp_after_ads"])
    if gp_col is None:
        result["gp_after_ads"] = pd.Series([float("nan")] * len(result), index=result.index)
    else:
        gp_text = result[gp_col].astype(str).str.replace("\u00a0", "", regex=False).str.replace(" ", "", regex=False)
        gp_text = gp_text.str.replace(",", ".", regex=False)
        gp_text = gp_text.str.replace(r"[^0-9.\-]", "", regex=True)
        result["gp_after_ads"] = pd.to_numeric(gp_text, errors="coerce")

    return result


# =============================
# Загрузка Excel-данных
# =============================

def read_excel_bytes_as_sheets(payload: bytes) -> Dict[str, pd.DataFrame]:
    xls = pd.ExcelFile(io.BytesIO(payload))
    return {sheet_name: pd.read_excel(io.BytesIO(payload), sheet_name=sheet_name) for sheet_name in xls.sheet_names}


def first_sheet_by_name(sheets: Dict[str, pd.DataFrame], wanted_name: str) -> pd.DataFrame:
    wanted_norm = _norm_col_name(wanted_name)
    for name, df in sheets.items():
        if _norm_col_name(name) == wanted_norm:
            return df.copy()
    return pd.DataFrame()


def read_excel_sheets_as_frame(sheets: Dict[str, pd.DataFrame], source_name: str) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    for sheet_name, sheet_df in sheets.items():
        if sheet_df is None or sheet_df.empty:
            continue
        local_df = sheet_df.copy()
        local_df["source_file"] = source_name
        local_df["source_sheet"] = str(sheet_name)
        frames.append(local_df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True, sort=False)


def read_excel_bytes_as_frame(payload: bytes, source_name: str) -> pd.DataFrame:
    sheets = read_excel_bytes_as_sheets(payload)
    return read_excel_sheets_as_frame(sheets, source_name)


def derive_campaign_placement_and_bid(campaigns_norm: pd.DataFrame, campaigns_raw: pd.DataFrame) -> pd.DataFrame:
    result = campaigns_norm.copy()
    search_bid = numeric_series(campaigns_raw, [
        "Ставка в поиске (руб)", "Ставка поиск, руб", "Ставка поиск", "bid_search_rub", "search_bid",
        "Ставка в поиске", "Ставка в поиске ₽",
    ], default=0.0)
    reco_bid = numeric_series(campaigns_raw, [
        "Ставка в рекомендациях (руб)", "Ставка рекомендации, руб", "Ставка рекомендации", "bid_reco_rub",
        "reco_bid", "recommendation_bid", "Ставка в рекомендациях", "Ставка в рекомендациях ₽",
    ], default=0.0)
    direct_bid = numeric_series(campaigns_raw, COLUMN_ALIASES["current_bid_rub"], default=0.0)

    placements: List[str] = []
    bids: List[float] = []
    for idx in result.index:
        placement_raw = result.at[idx, "placement"] if "placement" in result.columns else ""
        placement = normalize_placement_value(placement_raw)
        s_bid = float(search_bid.loc[idx] if idx in search_bid.index else 0.0)
        r_bid = float(reco_bid.loc[idx] if idx in reco_bid.index else 0.0)
        d_bid = float(direct_bid.loc[idx] if idx in direct_bid.index else 0.0)

        if not placement:
            if s_bid > 0 and r_bid > 0:
                placement = "combined"
            elif s_bid > 0:
                placement = "search"
            elif r_bid > 0:
                placement = "recommendations"
            else:
                placement = "search"
        if placement == "recommendation":
            placement = "recommendations"

        if placement == "recommendations" and r_bid > 0:
            bid = r_bid
        elif placement in {"search", "combined"} and s_bid > 0:
            bid = s_bid
        elif d_bid > 0:
            bid = d_bid
        elif s_bid > 0:
            bid = s_bid
        else:
            bid = r_bid

        placements.append(placement)
        bids.append(float(bid or 0.0))
    result["placement"] = placements
    result["current_bid_rub"] = bids
    return result


def normalize_ads_analysis_sheets(sheets: Dict[str, pd.DataFrame], source_name: str) -> pd.DataFrame:
    """Специально читает Анализ рекламы.xlsx: метрики из Статистика_Ежедневно, ставки/status из Список_кампаний."""
    daily_raw = first_sheet_by_name(sheets, "Статистика_Ежедневно")
    campaigns_raw = first_sheet_by_name(sheets, "Список_кампаний")

    if daily_raw.empty:
        combined = read_excel_sheets_as_frame(sheets, source_name)
        return normalize_columns(combined, source_type="ads_generic")

    daily = normalize_columns(daily_raw, source_type="ads_daily")
    daily["source_file"] = source_name
    daily["source_sheet"] = "Статистика_Ежедневно"

    if "revenue" in daily.columns and float(pd.to_numeric(daily["revenue"], errors="coerce").fillna(0).sum()) == 0:
        sum_orders_col = find_col(daily_raw, ["Сумма заказов", "Сумма заказов, ₽", "Заказано на сумму", "Заказано на сумму, ₽"])
        if sum_orders_col:
            daily["revenue"] = numeric_series(daily_raw, [sum_orders_col], default=0.0)

    if campaigns_raw.empty:
        if daily["campaign_status"].astype(str).str.strip().eq("").all():
            daily["campaign_status"] = "Активна"
        if daily["placement"].astype(str).str.strip().eq("").all():
            daily["placement"] = "search"
        return daily

    campaigns = normalize_columns(campaigns_raw, source_type="ads_campaigns")
    campaigns = derive_campaign_placement_and_bid(campaigns, campaigns_raw)
    campaigns["source_file"] = source_name
    campaigns["source_sheet"] = "Список_кампаний"

    keep_cols = [
        "campaign_id", "nm_id", "placement", "campaign_status", "campaign_name",
        "current_bid_rub", "supplier_article", "subject_norm",
    ]
    for col in keep_cols:
        if col not in campaigns.columns:
            campaigns[col] = ""

    campaigns_dim = campaigns[keep_cols].copy()
    campaigns_dim = campaigns_dim[
        campaigns_dim["campaign_id"].map(_clean_id_value).ne("")
        & campaigns_dim["nm_id"].map(_clean_id_value).ne("")
    ].copy()
    if not campaigns_dim.empty:
        campaigns_dim["campaign_id"] = campaigns_dim["campaign_id"].map(_clean_id_value)
        campaigns_dim["nm_id"] = campaigns_dim["nm_id"].map(_clean_id_value)
        campaigns_dim["placement"] = campaigns_dim["placement"].map(normalize_placement_value).replace({"recommendation": "recommendations"})
        campaigns_dim["_status_rank"] = campaigns_dim["campaign_status"].map(lambda x: 1 if is_active_campaign(x) else 0)
        campaigns_dim = campaigns_dim.sort_values(["campaign_id", "nm_id", "_status_rank"], ascending=[True, True, False])
        campaigns_dim = campaigns_dim.drop_duplicates(["campaign_id", "nm_id", "placement"], keep="first")
        campaigns_dim = campaigns_dim.drop(columns=["_status_rank"], errors="ignore")

    daily["campaign_id"] = daily["campaign_id"].map(_clean_id_value)
    daily["nm_id"] = daily["nm_id"].map(_clean_id_value)

    if campaigns_dim.empty:
        if daily["campaign_status"].astype(str).str.strip().eq("").all():
            daily["campaign_status"] = "Активна"
        if daily["placement"].astype(str).str.strip().eq("").all():
            daily["placement"] = "search"
        return daily

    metric_cols = ["date", "campaign_id", "nm_id", "impressions", "clicks", "orders", "spend", "revenue", "gp_after_ads", "_row_id", "source_file", "source_sheet"]
    for col in ["supplier_article", "subject_norm", "campaign_name", "campaign_status", "placement", "current_bid_rub"]:
        if col not in daily.columns:
            daily[col] = "" if col != "current_bid_rub" else 0.0
    metric_cols.extend(["supplier_article", "subject_norm", "campaign_name", "campaign_status", "placement", "current_bid_rub"])
    metric_cols = [c for c in metric_cols if c in daily.columns]

    merged = daily[metric_cols].merge(
        campaigns_dim,
        on=["campaign_id", "nm_id"],
        how="left",
        suffixes=("", "_campaign"),
    )

    for col in ["placement", "campaign_status", "campaign_name", "supplier_article", "subject_norm"]:
        camp_col = f"{col}_campaign"
        if camp_col in merged.columns:
            base = merged[col].fillna("").astype(str) if col in merged.columns else pd.Series([""] * len(merged), index=merged.index)
            camp = merged[camp_col].fillna("").astype(str)
            merged[col] = base.where(base.str.strip().ne(""), camp)
    if "current_bid_rub_campaign" in merged.columns:
        base_bid = pd.to_numeric(merged.get("current_bid_rub", 0), errors="coerce").fillna(0.0)
        camp_bid = pd.to_numeric(merged["current_bid_rub_campaign"], errors="coerce").fillna(0.0)
        merged["current_bid_rub"] = base_bid.where(base_bid > 0, camp_bid)

    drop_cols = [c for c in merged.columns if c.endswith("_campaign")]
    merged = merged.drop(columns=drop_cols, errors="ignore")
    merged["placement"] = merged["placement"].map(normalize_placement_value).replace({"recommendation": "recommendations"})
    return merged


def load_ads_report(s3_client, config: Config) -> pd.DataFrame:
    if s3_key_exists(s3_client, config.yc_bucket_name, ADS_MAIN_KEY):
        payload = read_s3_bytes(s3_client, config.yc_bucket_name, ADS_MAIN_KEY)
        sheets = read_excel_bytes_as_sheets(payload)
        raw = normalize_ads_analysis_sheets(sheets, ADS_MAIN_KEY)
        if raw.empty:
            raise RuntimeError(f"Основной рекламный отчёт пустой: {ADS_MAIN_KEY}")
        print(
            "Диагностика загрузки рекламы: "
            f"листы={list(sheets.keys())}; "
            f"строк после нормализации={len(raw)}; "
            f"валидных campaign_id={raw['campaign_id'].map(_clean_id_value).ne('').sum() if 'campaign_id' in raw.columns else 0}; "
            f"валидных nm_id={raw['nm_id'].map(_clean_id_value).ne('').sum() if 'nm_id' in raw.columns else 0}; "
            f"валидных placement={raw['placement'].astype(str).str.strip().ne('').sum() if 'placement' in raw.columns else 0}; "
            f"валидных ставок={(pd.to_numeric(raw['current_bid_rub'], errors='coerce').fillna(0) > 0).sum() if 'current_bid_rub' in raw.columns else 0}; "
            f"активных={raw['campaign_status'].map(is_active_campaign).sum() if 'campaign_status' in raw.columns else 0}",
            flush=True,
        )
        return raw

    weekly_keys = [
        key for key in list_s3_keys(s3_client, config.yc_bucket_name, ADS_WEEKLY_PREFIX)
        if key.lower().endswith(".xlsx") and not key.endswith("/~$")
    ]
    weekly_keys = sorted(weekly_keys, reverse=True)[:8]
    frames: List[pd.DataFrame] = []
    for key in weekly_keys:
        payload = read_s3_bytes(s3_client, config.yc_bucket_name, key)
        sheets = read_excel_bytes_as_sheets(payload)
        raw = normalize_ads_analysis_sheets(sheets, key)
        if not raw.empty:
            frames.append(raw)
    if not frames:
        raise RuntimeError(
            "Не найден основной рекламный отчёт и нет непустых fallback-файлов: "
            f"{ADS_MAIN_KEY}; {ADS_WEEKLY_PREFIX}"
        )
    result = pd.concat(frames, ignore_index=True, sort=False)
    print(f"Диагностика fallback-рекламы: файлов={len(frames)}, строк={len(result)}", flush=True)
    return result


def load_excel_table_from_s3(s3_client, config: Config, key: str, columns: List[str]) -> pd.DataFrame:
    if not s3_key_exists(s3_client, config.yc_bucket_name, key):
        return pd.DataFrame(columns=columns)
    payload = read_s3_bytes(s3_client, config.yc_bucket_name, key)
    try:
        df = pd.read_excel(io.BytesIO(payload))
    except Exception:
        return pd.DataFrame(columns=columns)
    for col in columns:
        if col not in df.columns:
            df[col] = ""
    return df


def load_bid_history(s3_client, config: Config) -> pd.DataFrame:
    return load_excel_table_from_s3(s3_client, config, BID_HISTORY_KEY, BID_HISTORY_COLUMNS)


def load_pause_history(s3_client, config: Config) -> pd.DataFrame:
    return load_excel_table_from_s3(s3_client, config, PAUSE_HISTORY_KEY, PAUSE_HISTORY_COLUMNS)


# =============================
# Окна, агрегация и метрики
# =============================

def build_windows(run_date: Optional[date] = None) -> Tuple[date, date, date, date, date]:
    today = run_date or date.today()
    mature_end = today - timedelta(days=3)
    current_start = mature_end - timedelta(days=4)
    current_end = mature_end
    base_end = current_start - timedelta(days=1)
    base_start = base_end - timedelta(days=4)
    return mature_end, current_start, current_end, base_start, base_end


def build_run_context(args: argparse.Namespace) -> RunContext:
    mature_end, current_start, current_end, base_start, base_end = build_windows()
    return RunContext(
        mode=args.command,
        dry_run=bool(args.dry_run),
        apply_pause=bool(args.apply_pause),
        apply_start=bool(args.apply_start),
        run_datetime=datetime.now(),
        mature_end=mature_end,
        current_start=current_start,
        current_end=current_end,
        base_start=base_start,
        base_end=base_end,
    )


def has_valid_dates(df: pd.DataFrame) -> bool:
    return "date" in df.columns and df["date"].notna().any()


def filter_by_date_window(df: pd.DataFrame, start_date: date, end_date: date) -> pd.DataFrame:
    if not has_valid_dates(df):
        return df.copy()
    mask = (df["date"] >= start_date) & (df["date"] <= end_date)
    return df.loc[mask].copy()


def latest_nonempty_value(df: pd.DataFrame, group_keys: List[str], col: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=group_keys + [col])
    local = df[group_keys + ["date", "_row_id", col]].copy()
    local["_has_value"] = local[col].map(lambda x: _clean_text_value(x) != "" if not isinstance(x, (int, float)) else not pd.isna(x))
    local = local.sort_values(group_keys + ["_has_value", "date", "_row_id"])
    latest = local.groupby(group_keys, dropna=False).tail(1)
    return latest[group_keys + [col]]


def latest_numeric_value(df: pd.DataFrame, group_keys: List[str], col: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=group_keys + [col])
    local = df[group_keys + ["date", "_row_id", col]].copy()
    local = local.sort_values(group_keys + ["date", "_row_id"])
    latest = local.groupby(group_keys, dropna=False).tail(1)
    return latest[group_keys + [col]]


def aggregate_window_metrics(df: pd.DataFrame, group_keys: List[str], prefix: str = "") -> pd.DataFrame:
    if df.empty:
        cols = group_keys + [
            f"{prefix}spend",
            f"{prefix}revenue",
            f"{prefix}orders",
            f"{prefix}impressions",
            f"{prefix}clicks",
            f"{prefix}gp_after_ads",
        ]
        return pd.DataFrame(columns=cols)

    agg = df.groupby(group_keys, dropna=False).agg(
        spend=("spend", "sum"),
        revenue=("revenue", "sum"),
        orders=("orders", "sum"),
        impressions=("impressions", "sum"),
        clicks=("clicks", "sum"),
        gp_after_ads=("gp_after_ads", "sum"),
    ).reset_index()

    # Если ВП отсутствует во всех строках группы, sum даёт 0. Возвращаем NaN для таких групп.
    gp_present = df.groupby(group_keys, dropna=False)["gp_after_ads"].apply(lambda s: s.notna().any()).reset_index(name="_gp_present")
    agg = agg.merge(gp_present, on=group_keys, how="left")
    agg.loc[~agg["_gp_present"].fillna(False), "gp_after_ads"] = float("nan")
    agg = agg.drop(columns=["_gp_present"])

    if prefix:
        rename_map = {col: f"{prefix}{col}" for col in ["spend", "revenue", "orders", "impressions", "clicks", "gp_after_ads"]}
        agg = agg.rename(columns=rename_map)
    return agg


def safe_drr_pct(spend: float, revenue: float) -> float:
    spend = float(spend or 0)
    revenue = float(revenue or 0)
    if revenue == 0 and spend > 0:
        return 999.0
    if revenue == 0 and spend == 0:
        return 0.0
    return spend / revenue * 100.0


def safe_cpo(spend: float, orders: float) -> float:
    spend = float(spend or 0)
    orders = float(orders or 0)
    if orders == 0 and spend > 0:
        return 999999.0
    if orders == 0:
        return 0.0
    return spend / orders


def safe_ctr_pct(clicks: float, impressions: float) -> float:
    clicks = float(clicks or 0)
    impressions = float(impressions or 0)
    if impressions == 0:
        return 0.0
    return clicks / impressions * 100.0


def growth_pct_or_status(current: float, base: float) -> Tuple[Optional[float], str]:
    current = float(current or 0)
    base = float(base or 0)
    if base == 0 and current > 0:
        return None, "NEW_ACTIVITY"
    if base == 0 and current == 0:
        return None, "ZERO_BASE"
    return (current / base - 1.0) * 100.0, "OK"


def compute_metrics(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    result["campaign_drr_pct"] = [safe_drr_pct(s, r) for s, r in zip(result["spend"], result["revenue"])]
    result["cpo"] = [safe_cpo(s, o) for s, o in zip(result["spend"], result["orders"])]
    result["ctr_pct"] = [safe_ctr_pct(c, i) for c, i in zip(result["clicks"], result["impressions"])]

    for metric in ["spend", "revenue", "orders", "impressions", "clicks", "gp_after_ads"]:
        base_col = f"base_{metric}"
        if base_col in result.columns:
            growth_values: List[Optional[float]] = []
            growth_statuses: List[str] = []
            for current_value, base_value in zip(result[metric], result[base_col]):
                if pd.isna(current_value):
                    current_value = 0.0
                if pd.isna(base_value):
                    base_value = 0.0
                growth, status = growth_pct_or_status(float(current_value), float(base_value))
                growth_values.append(growth)
                growth_statuses.append(status)
            result[f"{metric}_growth_pct"] = growth_values
            result[f"{metric}_growth_status"] = growth_statuses
    return result


def aggregate_campaign_metrics(ads_df: pd.DataFrame, ctx: RunContext) -> pd.DataFrame:
    group_keys = ["campaign_id", "nm_id", "placement"]
    current_df = filter_by_date_window(ads_df, ctx.current_start, ctx.current_end)
    base_df = filter_by_date_window(ads_df, ctx.base_start, ctx.base_end)

    if current_df.empty and not ads_df.empty and not has_valid_dates(ads_df):
        current_df = ads_df.copy()

    current_metrics = aggregate_window_metrics(current_df, group_keys, prefix="")
    base_metrics = aggregate_window_metrics(base_df, group_keys, prefix="base_")

    if current_metrics.empty:
        return pd.DataFrame(columns=DECISION_COLUMNS)

    result = current_metrics.merge(base_metrics, on=group_keys, how="left")

    source_for_dims = current_df if not current_df.empty else ads_df
    for col in ["campaign_name", "campaign_status", "supplier_article", "subject_norm"]:
        result = result.merge(latest_nonempty_value(source_for_dims, group_keys, col), on=group_keys, how="left")
    result = result.merge(latest_numeric_value(source_for_dims, group_keys, "current_bid_rub"), on=group_keys, how="left")

    for metric in ["base_spend", "base_revenue", "base_orders", "base_impressions", "base_clicks"]:
        if metric not in result.columns:
            result[metric] = 0.0
        result[metric] = pd.to_numeric(result[metric], errors="coerce").fillna(0.0)

    if "base_gp_after_ads" not in result.columns:
        result["base_gp_after_ads"] = float("nan")

    result = compute_metrics(result)
    return result


# =============================
# Post-check изменений ставки
# =============================

def make_key(row: pd.Series | Dict[str, Any]) -> Tuple[str, str, str]:
    return (
        _clean_id_value(row.get("campaign_id", "")),
        _clean_id_value(row.get("nm_id", "")),
        normalize_placement_value(row.get("placement", "")),
    )


def load_pending_events(bid_history: pd.DataFrame) -> Dict[Tuple[str, str, str], Dict[str, Any]]:
    if bid_history.empty:
        return {}
    local = bid_history.copy()
    for col in BID_HISTORY_COLUMNS:
        if col not in local.columns:
            local[col] = ""
    local["event_date_parsed"] = pd.to_datetime(local["event_date"], errors="coerce").dt.date
    local["run_dt_parsed"] = pd.to_datetime(local["run_datetime"], errors="coerce")
    local = local.sort_values(["event_date_parsed", "run_dt_parsed"], na_position="first")
    pending: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for _, row in local.iterrows():
        status = _clean_text_value(row.get("postcheck_status", "")).lower()
        verdict = _clean_text_value(row.get("final_verdict", ""))
        if status != "resolved" and verdict not in {"RAISE_NO_TRAFFIC_GROWTH"}:
            pending[make_key(row)] = row.to_dict()
    return pending


def latest_postcheck_results(bid_history: pd.DataFrame) -> Dict[Tuple[str, str, str], Dict[str, Any]]:
    if bid_history.empty:
        return {}
    local = bid_history.copy()
    local["event_date_parsed"] = pd.to_datetime(local["event_date"], errors="coerce").dt.date
    local["run_dt_parsed"] = pd.to_datetime(local["run_datetime"], errors="coerce")
    local = local.sort_values(["event_date_parsed", "run_dt_parsed"], na_position="first")
    latest: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for _, row in local.iterrows():
        latest[make_key(row)] = row.to_dict()
    return latest


def aggregate_after_event(
    ads_df: pd.DataFrame,
    key: Tuple[str, str, str],
    start_date: date,
    end_date: date,
) -> Dict[str, float]:
    if ads_df.empty:
        return {"spend": 0.0, "revenue": 0.0, "orders": 0.0, "impressions": 0.0, "clicks": 0.0, "gp_after_ads": float("nan")}
    if not has_valid_dates(ads_df):
        return {"spend": 0.0, "revenue": 0.0, "orders": 0.0, "impressions": 0.0, "clicks": 0.0, "gp_after_ads": float("nan")}
    campaign_id, nm_id, placement = key
    mask = (
        (ads_df["campaign_id"] == campaign_id)
        & (ads_df["nm_id"] == nm_id)
        & (ads_df["placement"] == placement)
        & (ads_df["date"] >= start_date)
        & (ads_df["date"] <= end_date)
    )
    part = ads_df.loc[mask]
    if part.empty:
        return {"spend": 0.0, "revenue": 0.0, "orders": 0.0, "impressions": 0.0, "clicks": 0.0, "gp_after_ads": float("nan")}
    gp = part["gp_after_ads"].sum() if part["gp_after_ads"].notna().any() else float("nan")
    return {
        "spend": float(part["spend"].sum()),
        "revenue": float(part["revenue"].sum()),
        "orders": float(part["orders"].sum()),
        "impressions": float(part["impressions"].sum()),
        "clicks": float(part["clicks"].sum()),
        "gp_after_ads": gp,
    }


def grew_enough(after: float, before: float, factor: float) -> bool:
    before = float(before or 0)
    after = float(after or 0)
    if before == 0:
        return after > 0
    return after >= before * factor


def retained_enough(after: float, before: float, factor: float) -> bool:
    before = float(before or 0)
    after = float(after or 0)
    if before == 0:
        return after >= 0
    return after >= before * factor


def ge_metric(after: float, before: float) -> bool:
    if pd.isna(after) and pd.isna(before):
        return True
    if pd.isna(after) or pd.isna(before):
        return False
    return float(after) >= float(before)


def ge_metric_factor(after: float, before: float, factor: float) -> bool:
    if pd.isna(after) and pd.isna(before):
        return True
    if pd.isna(after) or pd.isna(before):
        return False
    return float(after) >= float(before) * factor


def lt_metric_factor(after: float, before: float, factor: float) -> bool:
    if pd.isna(after) or pd.isna(before):
        return False
    return float(after) < float(before) * factor


def evaluate_postchecks(ads_df: pd.DataFrame, bid_history: pd.DataFrame, ctx: RunContext) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if bid_history.empty:
        return bid_history.copy(), pd.DataFrame(columns=[
            "event_id", "campaign_id", "nm_id", "placement", "direction",
            "event_date", "d1_verdict", "d3_verdict", "final_verdict",
            "postcheck_status", "drr_before", "drr_after_d3",
            "impressions_after_d1", "clicks_after_d1", "orders_after_d3",
            "revenue_after_d3", "spend_after_d3", "gp_after_d3",
        ])

    updated = bid_history.copy()
    for col in BID_HISTORY_COLUMNS:
        if col not in updated.columns:
            updated[col] = ""

    effects: List[Dict[str, Any]] = []
    for idx, row in updated.iterrows():
        event_date = pd.to_datetime(row.get("event_date"), errors="coerce")
        if pd.isna(event_date):
            continue
        event_day = event_date.date()
        key = make_key(row)
        direction = _clean_text_value(row.get("direction", "")).lower()

        status = _clean_text_value(row.get("postcheck_status", "")) or "pending"
        final_verdict = _clean_text_value(row.get("final_verdict", ""))
        d1_verdict = _clean_text_value(row.get("d1_verdict", ""))
        d3_verdict = _clean_text_value(row.get("d3_verdict", ""))

        before = {
            "spend": float(pd.to_numeric(pd.Series([row.get("spend_before", 0)]), errors="coerce").fillna(0).iloc[0]),
            "revenue": float(pd.to_numeric(pd.Series([row.get("revenue_before", 0)]), errors="coerce").fillna(0).iloc[0]),
            "orders": float(pd.to_numeric(pd.Series([row.get("orders_before", 0)]), errors="coerce").fillna(0).iloc[0]),
            "impressions": float(pd.to_numeric(pd.Series([row.get("impressions_before", 0)]), errors="coerce").fillna(0).iloc[0]),
            "clicks": float(pd.to_numeric(pd.Series([row.get("clicks_before", 0)]), errors="coerce").fillna(0).iloc[0]),
            "drr": float(pd.to_numeric(pd.Series([row.get("drr_before", 0)]), errors="coerce").fillna(0).iloc[0]),
            "gp": float(pd.to_numeric(pd.Series([row.get("gp_before", float("nan"))]), errors="coerce").iloc[0]),
        }

        d1_after = {"impressions": 0.0, "clicks": 0.0}
        d3_after = {"spend": 0.0, "revenue": 0.0, "orders": 0.0, "gp_after_ads": float("nan")}
        drr_after_d3 = 0.0

        d1_day = event_day + timedelta(days=1)
        if ctx.mature_end >= d1_day and status not in {"resolved"}:
            d1_after = aggregate_after_event(ads_df, key, d1_day, d1_day)
            if direction == "raise":
                if grew_enough(d1_after["impressions"], before["impressions"], 1.05) or grew_enough(d1_after["clicks"], before["clicks"], 1.05):
                    d1_verdict = "RAISE_D1_OK"
                    status = "d1_done"
                else:
                    d1_verdict = "RAISE_NO_TRAFFIC_GROWTH"
                    final_verdict = "RAISE_NO_TRAFFIC_GROWTH"
                    status = "resolved"
            elif direction == "lower":
                if retained_enough(d1_after["impressions"], before["impressions"], 0.80) and retained_enough(d1_after["clicks"], before["clicks"], 0.80):
                    d1_verdict = "LOWER_D1_OK"
                else:
                    d1_verdict = "LOWER_TRAFFIC_DROP_RISK"
                status = "d1_done"
            updated.at[idx, "d1_verdict"] = d1_verdict
            updated.at[idx, "d1_check_date"] = str(ctx.mature_end)

        d3_end = event_day + timedelta(days=3)
        if ctx.mature_end >= d3_end and status not in {"resolved"}:
            d3_after = aggregate_after_event(ads_df, key, event_day + timedelta(days=1), d3_end)
            drr_after_d3 = safe_drr_pct(d3_after["spend"], d3_after["revenue"])
            if direction == "raise":
                if drr_after_d3 > before["drr"] + 2.0:
                    d3_verdict = "RAISE_BAD"
                    final_verdict = "RAISE_BAD"
                elif d3_after["spend"] > before["spend"] and d3_after["orders"] <= before["orders"] and ge_metric(before["gp"], d3_after["gp_after_ads"]):
                    d3_verdict = "RAISE_BAD"
                    final_verdict = "RAISE_BAD"
                elif ge_metric(d3_after["orders"], before["orders"]) and ge_metric(d3_after["revenue"], before["revenue"]) and ge_metric(d3_after["gp_after_ads"], before["gp"]):
                    d3_verdict = "RAISE_GOOD"
                    final_verdict = "RAISE_GOOD"
                else:
                    d3_verdict = "RAISE_D3_MIXED"
                    final_verdict = "RAISE_D3_MIXED"
            elif direction == "lower":
                if drr_after_d3 < before["drr"] and ge_metric_factor(d3_after["gp_after_ads"], before["gp"], 0.95) and ge_metric_factor(d3_after["orders"], before["orders"], 0.90):
                    d3_verdict = "LOWER_GOOD"
                    final_verdict = "LOWER_GOOD"
                elif lt_metric_factor(d3_after["orders"], before["orders"], 0.90) and lt_metric_factor(d3_after["gp_after_ads"], before["gp"], 0.95) and drr_after_d3 >= before["drr"]:
                    d3_verdict = "LOWER_BAD"
                    final_verdict = "LOWER_BAD"
                else:
                    d3_verdict = "LOWER_D3_MIXED"
                    final_verdict = "LOWER_D3_MIXED"
            status = "resolved"
            updated.at[idx, "d3_verdict"] = d3_verdict
            updated.at[idx, "d3_check_date"] = str(ctx.mature_end)

        updated.at[idx, "postcheck_status"] = status
        updated.at[idx, "final_verdict"] = final_verdict

        if not drr_after_d3 and ctx.mature_end >= event_day + timedelta(days=3):
            d3_after = aggregate_after_event(ads_df, key, event_day + timedelta(days=1), event_day + timedelta(days=3))
            drr_after_d3 = safe_drr_pct(d3_after["spend"], d3_after["revenue"])
        if ctx.mature_end >= d1_day:
            d1_after = aggregate_after_event(ads_df, key, d1_day, d1_day)
        if ctx.mature_end >= event_day + timedelta(days=3):
            d3_after = aggregate_after_event(ads_df, key, event_day + timedelta(days=1), event_day + timedelta(days=3))

        effects.append({
            "event_id": row.get("event_id", ""),
            "campaign_id": row.get("campaign_id", ""),
            "nm_id": row.get("nm_id", ""),
            "placement": row.get("placement", ""),
            "direction": direction,
            "event_date": row.get("event_date", ""),
            "d1_verdict": d1_verdict,
            "d3_verdict": d3_verdict,
            "final_verdict": final_verdict,
            "postcheck_status": status,
            "drr_before": before["drr"],
            "drr_after_d3": drr_after_d3,
            "impressions_after_d1": d1_after.get("impressions", 0.0),
            "clicks_after_d1": d1_after.get("clicks", 0.0),
            "orders_after_d3": d3_after.get("orders", 0.0),
            "revenue_after_d3": d3_after.get("revenue", 0.0),
            "spend_after_d3": d3_after.get("spend", 0.0),
            "gp_after_d3": d3_after.get("gp_after_ads", float("nan")),
        })

    return updated[BID_HISTORY_COLUMNS], pd.DataFrame(effects)


# =============================
# Решения по ставкам
# =============================

def is_active_campaign(status_value: Any) -> bool:
    text = _clean_text_value(status_value).replace("ё", "е").lower()
    if not text:
        return False
    if text in {"9", "9.0"}:
        return True
    if "active" in text:
        return True
    if "актив" in text and "неактив" not in text and "не актив" not in text:
        return True
    return False


def is_managed_subject(subject_value: Any) -> bool:
    text = normalize_subject_value(subject_value).lower()
    return text in MANAGED_SUBJECTS


def bid_step_rub(placement: Any) -> Tuple[float, str]:
    placement_norm = normalize_placement_value(placement)
    if placement_norm in {"search", "recommendations"}:
        return 1.0, ""
    if placement_norm == "combined":
        return 6.0, ""
    return 1.0, "UNKNOWN_PLACEMENT_DEFAULT_STEP"


def format_float(value: Any, digits: int = 2) -> str:
    if value is None or pd.isna(value):
        return "н/д"
    return f"{float(value):.{digits}f}"


def build_reason_text(row: pd.Series, action: str, new_bid: Optional[float], extra: str = "") -> str:
    old_bid = row.get("current_bid_rub", float("nan"))
    parts = [
        f"ДРР={format_float(row.get('campaign_drr_pct'), 2)}%",
        f"ставка={format_float(old_bid, 2)} ₽",
        f"новая ставка={format_float(new_bid, 2)} ₽" if new_bid is not None else "новая ставка=н/д",
        f"расход={format_float(row.get('spend'), 2)} ₽",
        f"выручка={format_float(row.get('revenue'), 2)} ₽",
        f"заказы={format_float(row.get('orders'), 0)}",
    ]
    if extra:
        parts.append(extra)
    return f"{action}: " + "; ".join(parts)


def technical_hold(reason_code: str, row: pd.Series, reason: str) -> Dict[str, Any]:
    return {
        "action": "Без изменений",
        "new_bid_rub": None,
        "reason_code": reason_code,
        "reason_text": build_reason_text(row, "Без изменений", None, reason),
        "pause_decision": "",
    }


def decide_action(row: pd.Series, pending_event: Optional[Dict[str, Any]] = None, postcheck_result: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Возвращает dict:
    {
        "action": "Повысить" | "Снизить" | "Без изменений",
        "new_bid_rub": float | None,
        "reason_code": str,
        "reason_text": str,
        "pause_decision": str | ""
    }
    """
    campaign_id = _clean_id_value(row.get("campaign_id", ""))
    nm_id = _clean_id_value(row.get("nm_id", ""))
    placement = normalize_placement_value(row.get("placement", ""))
    current_bid = row.get("current_bid_rub", float("nan"))

    if not campaign_id or not nm_id or not placement or pd.isna(current_bid) or float(current_bid) <= 0:
        return technical_hold("MISSING_KEY", row, "нет campaign_id / nm_id / placement или текущей ставки")

    if not is_active_campaign(row.get("campaign_status", "")):
        return technical_hold("NOT_ACTIVE", row, "кампания не активна")

    if not is_managed_subject(row.get("subject_norm", "")):
        return technical_hold("NOT_MANAGED_SUBJECT", row, "предмет не входит в управляемые")

    if float(row.get("spend", 0) or 0) == 0 and float(row.get("revenue", 0) or 0) == 0:
        return technical_hold("NO_TRAFFIC", row, "нет расхода и нет выручки")

    final_verdict = _clean_text_value((postcheck_result or {}).get("final_verdict", ""))
    previous_event_id = _clean_text_value((postcheck_result or {}).get("event_id", ""))

    step, step_reason = bid_step_rub(placement)
    current_bid_float = float(current_bid)

    if final_verdict in {"RAISE_BAD", "RAISE_NO_TRAFFIC_GROWTH"}:
        new_bid = round(current_bid_float - step, 2)
        reason_code = "RAISE_FAILED_REVERT"
        if step_reason:
            reason_code += f"__{step_reason}"
        if new_bid < TECHNICAL_BID_FLOOR_RUB:
            return {
                "action": "Без изменений",
                "new_bid_rub": None,
                "reason_code": "TECHNICAL_FLOOR_REACHED",
                "reason_text": build_reason_text(row, "Без изменений", None, f"откат после {final_verdict}; новая ставка ниже 1 ₽; previous_event_id={previous_event_id}"),
                "pause_decision": "PAUSE_CANDIDATE",
            }
        return {
            "action": "Снизить",
            "new_bid_rub": new_bid,
            "reason_code": reason_code,
            "reason_text": build_reason_text(row, "Снизить", new_bid, f"откат после {final_verdict}; previous_event_id={previous_event_id}"),
            "pause_decision": "",
        }

    if final_verdict == "LOWER_BAD":
        new_bid = round(current_bid_float + step, 2)
        reason_code = "LOWER_FAILED_REVERT"
        if step_reason:
            reason_code += f"__{step_reason}"
        return {
            "action": "Повысить",
            "new_bid_rub": new_bid,
            "reason_code": reason_code,
            "reason_text": build_reason_text(row, "Повысить", new_bid, f"откат после {final_verdict}; previous_event_id={previous_event_id}"),
            "pause_decision": "",
        }

    if pending_event is not None:
        return technical_hold("WAIT_POSTCHECK", row, f"есть незавершённый post-check event_id={pending_event.get('event_id', '')}")

    drr = float(row.get("campaign_drr_pct", 0) or 0)
    if drr >= DRR_LIMIT_PCT:
        new_bid = round(current_bid_float - step, 2)
        reason_code = "DRR_GE_10_REDUCE"
        if step_reason:
            reason_code += f"__{step_reason}"
        if new_bid < TECHNICAL_BID_FLOOR_RUB:
            return {
                "action": "Без изменений",
                "new_bid_rub": None,
                "reason_code": "TECHNICAL_FLOOR_REACHED",
                "reason_text": build_reason_text(row, "Без изменений", None, "требуется снижение, но новая ставка ниже 1 ₽"),
                "pause_decision": "PAUSE_CANDIDATE",
            }
        return {
            "action": "Снизить",
            "new_bid_rub": new_bid,
            "reason_code": reason_code,
            "reason_text": build_reason_text(row, "Снизить", new_bid, "ДРР >= 10.0%"),
            "pause_decision": "",
        }

    new_bid = round(current_bid_float + step, 2)
    reason_code = "DRR_LT_10_GROW"
    if step_reason:
        reason_code += f"__{step_reason}"
    return {
        "action": "Повысить",
        "new_bid_rub": new_bid,
        "reason_code": reason_code,
        "reason_text": build_reason_text(row, "Повысить", new_bid, "ДРР < 10.0%"),
        "pause_decision": "",
    }


def build_decisions(metrics_df: pd.DataFrame, pending_events: Dict[Tuple[str, str, str], Dict[str, Any]], postcheck_results: Dict[Tuple[str, str, str], Dict[str, Any]]) -> pd.DataFrame:
    if metrics_df.empty:
        return pd.DataFrame(columns=DECISION_COLUMNS)
    rows: List[Dict[str, Any]] = []
    for _, row in metrics_df.iterrows():
        key = make_key(row)
        pending = pending_events.get(key)
        latest_result = postcheck_results.get(key)
        decision = decide_action(row, pending_event=pending, postcheck_result=latest_result)
        previous_event_id = _clean_text_value((latest_result or {}).get("event_id", ""))
        postcheck_status = _clean_text_value((latest_result or {}).get("postcheck_status", ""))
        out = {
            "campaign_id": row.get("campaign_id", ""),
            "nm_id": row.get("nm_id", ""),
            "supplier_article": row.get("supplier_article", ""),
            "subject_norm": row.get("subject_norm", ""),
            "placement": row.get("placement", ""),
            "campaign_status": row.get("campaign_status", ""),
            "current_bid_rub": row.get("current_bid_rub", 0),
            "new_bid_rub": decision.get("new_bid_rub"),
            "action": decision.get("action", "Без изменений"),
            "reason_code": decision.get("reason_code", ""),
            "reason_text": decision.get("reason_text", ""),
            "spend": row.get("spend", 0),
            "revenue": row.get("revenue", 0),
            "orders": row.get("orders", 0),
            "impressions": row.get("impressions", 0),
            "clicks": row.get("clicks", 0),
            "campaign_drr_pct": row.get("campaign_drr_pct", 0),
            "cpo": row.get("cpo", 0),
            "ctr_pct": row.get("ctr_pct", 0),
            "gp_after_ads": row.get("gp_after_ads", float("nan")),
            "previous_event_id": previous_event_id,
            "postcheck_status": postcheck_status,
            "pause_decision": decision.get("pause_decision", ""),
        }
        rows.append(out)
    result = pd.DataFrame(rows)
    for col in DECISION_COLUMNS:
        if col not in result.columns:
            result[col] = ""
    return result[DECISION_COLUMNS]


# =============================
# API ставок и запись истории
# =============================

def wb_headers(config: Config) -> Dict[str, str]:
    return {
        "Authorization": config.wb_promo_key,
        "Content-Type": "application/json",
    }


def to_int_id(value: Any) -> Optional[int]:
    text = _clean_id_value(value)
    if re.fullmatch(r"\d+", text):
        return int(text)
    return None


def build_bid_payload(row: pd.Series) -> Optional[Dict[str, Any]]:
    advert_id = to_int_id(row.get("campaign_id", ""))
    nm_id = to_int_id(row.get("nm_id", ""))
    placement = normalize_placement_value(row.get("placement", ""))
    new_bid = row.get("new_bid_rub")
    if advert_id is None or nm_id is None or not placement or pd.isna(new_bid):
        return None
    bid_kopecks = int(round(float(new_bid) * 100))
    return {
        "bids": [
            {
                "advert_id": advert_id,
                "nm_bids": [
                    {
                        "nm_id": nm_id,
                        "bid_kopecks": bid_kopecks,
                        "placement": placement,
                    }
                ],
            }
        ]
    }


def api_log_row(run_datetime: datetime, method: str, endpoint: str, payload: Any, status: str, response_text: str, campaign_id: Any = "", nm_id: Any = "", placement: Any = "") -> Dict[str, Any]:
    return {
        "run_datetime": run_datetime.strftime("%Y-%m-%d %H:%M:%S"),
        "method": method,
        "endpoint": endpoint,
        "campaign_id": campaign_id,
        "nm_id": nm_id,
        "placement": placement,
        "payload": json.dumps(payload, ensure_ascii=False) if payload not in (None, "") else "",
        "api_status": status,
        "response_text": str(response_text)[:1000],
    }


def apply_bid_changes(decisions: pd.DataFrame, config: Config, ctx: RunContext) -> Tuple[pd.DataFrame, pd.DataFrame]:
    candidates = decisions[decisions["action"].isin(["Повысить", "Снизить"])].copy() if not decisions.empty else pd.DataFrame(columns=decisions.columns)
    api_logs: List[Dict[str, Any]] = []
    changed_rows: List[Dict[str, Any]] = []

    if candidates.empty:
        return pd.DataFrame(columns=decisions.columns.tolist() + ["api_status"]), pd.DataFrame(api_logs)

    url = config.wb_base_url.rstrip("/") + WB_BIDS_ENDPOINT
    for _, row in candidates.iterrows():
        payload = build_bid_payload(row)
        if payload is None:
            api_logs.append(api_log_row(ctx.run_datetime, "PATCH", WB_BIDS_ENDPOINT, {}, "payload_error", "Не удалось собрать payload", row.get("campaign_id"), row.get("nm_id"), row.get("placement")))
            continue

        if ctx.mode == "preview":
            api_logs.append(api_log_row(ctx.run_datetime, "PATCH", WB_BIDS_ENDPOINT, payload, "preview_no_call", "Предпросмотр без API-вызова", row.get("campaign_id"), row.get("nm_id"), row.get("placement")))
            continue

        if ctx.dry_run:
            api_logs.append(api_log_row(ctx.run_datetime, "PATCH", WB_BIDS_ENDPOINT, payload, "dry_run_no_call", "run --dry-run без API-вызова", row.get("campaign_id"), row.get("nm_id"), row.get("placement")))
            continue

        try:
            resp = requests.patch(url, headers=wb_headers(config), json=payload, timeout=60)
            status = str(resp.status_code)
            api_logs.append(api_log_row(ctx.run_datetime, "PATCH", WB_BIDS_ENDPOINT, payload, status, resp.text, row.get("campaign_id"), row.get("nm_id"), row.get("placement")))
            if 200 <= resp.status_code < 300:
                changed = row.to_dict()
                changed["api_status"] = status
                changed_rows.append(changed)
        except Exception as exc:
            api_logs.append(api_log_row(ctx.run_datetime, "PATCH", WB_BIDS_ENDPOINT, payload, "exception", repr(exc), row.get("campaign_id"), row.get("nm_id"), row.get("placement")))

    changed_df = pd.DataFrame(changed_rows)
    api_log_df = pd.DataFrame(api_logs)
    return changed_df, api_log_df


def record_bid_events(successful_changes: pd.DataFrame, bid_history: pd.DataFrame, ctx: RunContext) -> pd.DataFrame:
    if successful_changes.empty:
        return bid_history[BID_HISTORY_COLUMNS].copy() if not bid_history.empty else pd.DataFrame(columns=BID_HISTORY_COLUMNS)

    rows: List[Dict[str, Any]] = []
    for _, row in successful_changes.iterrows():
        action = _clean_text_value(row.get("action", ""))
        direction = "raise" if action == "Повысить" else "lower"
        rows.append({
            "event_id": str(uuid.uuid4()),
            "run_datetime": ctx.run_datetime.strftime("%Y-%m-%d %H:%M:%S"),
            "event_date": ctx.run_datetime.date().isoformat(),
            "campaign_id": row.get("campaign_id", ""),
            "nm_id": row.get("nm_id", ""),
            "supplier_article": row.get("supplier_article", ""),
            "subject_norm": row.get("subject_norm", ""),
            "placement": row.get("placement", ""),
            "old_bid_rub": row.get("current_bid_rub", 0),
            "new_bid_rub": row.get("new_bid_rub", 0),
            "direction": direction,
            "reason_code": row.get("reason_code", ""),
            "spend_before": row.get("spend", 0),
            "revenue_before": row.get("revenue", 0),
            "orders_before": row.get("orders", 0),
            "impressions_before": row.get("impressions", 0),
            "clicks_before": row.get("clicks", 0),
            "drr_before": row.get("campaign_drr_pct", 0),
            "gp_before": row.get("gp_after_ads", float("nan")),
            "postcheck_status": "pending",
            "final_verdict": "",
            "d1_verdict": "",
            "d3_verdict": "",
            "d1_check_date": "",
            "d3_check_date": "",
        })
    additions = pd.DataFrame(rows)
    base = bid_history.copy()
    for col in BID_HISTORY_COLUMNS:
        if col not in base.columns:
            base[col] = ""
        if col not in additions.columns:
            additions[col] = ""
    return pd.concat([base[BID_HISTORY_COLUMNS], additions[BID_HISTORY_COLUMNS]], ignore_index=True)


# =============================
# Паузы и запуск обратно
# =============================

def consecutive_lowers_for_key(bid_history: pd.DataFrame, key: Tuple[str, str, str]) -> int:
    if bid_history.empty:
        return 0
    local = bid_history.copy()
    local["event_date_parsed"] = pd.to_datetime(local["event_date"], errors="coerce").dt.date
    local["run_dt_parsed"] = pd.to_datetime(local["run_datetime"], errors="coerce")
    local = local[local.apply(lambda r: make_key(r) == key, axis=1)].sort_values(["event_date_parsed", "run_dt_parsed"], ascending=False)
    count = 0
    for _, row in local.iterrows():
        if _clean_text_value(row.get("direction", "")).lower() == "lower":
            count += 1
        else:
            break
    return count


def latest_lower_event_for_key(bid_history: pd.DataFrame, key: Tuple[str, str, str]) -> Optional[Dict[str, Any]]:
    if bid_history.empty:
        return None
    local = bid_history.copy()
    local = local[local.apply(lambda r: make_key(r) == key and _clean_text_value(r.get("direction", "")).lower() == "lower", axis=1)]
    if local.empty:
        return None
    local["event_date_parsed"] = pd.to_datetime(local["event_date"], errors="coerce").dt.date
    local["run_dt_parsed"] = pd.to_datetime(local["run_datetime"], errors="coerce")
    local = local.sort_values(["event_date_parsed", "run_dt_parsed"], ascending=False)
    return local.iloc[0].to_dict()


def build_pause_candidates(decisions: pd.DataFrame, bid_history: pd.DataFrame) -> pd.DataFrame:
    candidates: List[Dict[str, Any]] = []
    if decisions.empty:
        return pd.DataFrame(columns=PAUSE_HISTORY_COLUMNS)

    for _, row in decisions.iterrows():
        key = make_key(row)
        reason_code = ""
        drr = float(row.get("campaign_drr_pct", 0) or 0)
        spend = float(row.get("spend", 0) or 0)
        revenue = float(row.get("revenue", 0) or 0)
        orders = float(row.get("orders", 0) or 0)
        gp = row.get("gp_after_ads", float("nan"))

        if _clean_text_value(row.get("pause_decision", "")) == "PAUSE_CANDIDATE":
            reason_code = "BID_TECHNICAL_FLOOR_PAUSE"
        elif spend > 0 and revenue == 0 and orders == 0:
            reason_code = "SPEND_WITHOUT_REVENUE_ORDERS"
        else:
            lower_count = consecutive_lowers_for_key(bid_history, key)
            latest_lower = latest_lower_event_for_key(bid_history, key)
            latest_lower_verdict = _clean_text_value((latest_lower or {}).get("final_verdict", ""))
            if drr >= DRR_LIMIT_PCT and lower_count >= 2 and latest_lower is not None:
                reason_code = "TWO_LOWERS_DRR_STILL_GE_10"
            if not reason_code and not pd.isna(gp) and float(gp) < 0 and drr >= DRR_LIMIT_PCT and latest_lower_verdict in {"LOWER_D3_MIXED", "LOWER_BAD"}:
                reason_code = "NEGATIVE_GP_AFTER_LOWER"

        if not reason_code:
            continue

        candidates.append({
            "pause_event_id": str(uuid.uuid4()),
            "pause_date": date.today().isoformat(),
            "campaign_id": row.get("campaign_id", ""),
            "nm_id": row.get("nm_id", ""),
            "placement": row.get("placement", ""),
            "supplier_article": row.get("supplier_article", ""),
            "reason_code": reason_code,
            "spend_before_pause": spend,
            "revenue_before_pause": revenue,
            "orders_before_pause": orders,
            "drr_before_pause": drr,
            "gp_before_pause": gp,
            "status": "candidate",
            "next_check_date": (date.today() + timedelta(days=1)).isoformat(),
            "api_status": "",
        })
    return pd.DataFrame(candidates, columns=PAUSE_HISTORY_COLUMNS)


def apply_pause_actions(pause_candidates: pd.DataFrame, config: Config, ctx: RunContext) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if pause_candidates.empty:
        return pause_candidates.copy(), pd.DataFrame()

    result = pause_candidates.copy()
    api_logs: List[Dict[str, Any]] = []
    if ctx.mode == "preview":
        result["api_status"] = "preview_no_call"
        return result, pd.DataFrame(api_logs)
    if ctx.dry_run:
        result["api_status"] = "dry_run_no_call"
        return result, pd.DataFrame(api_logs)
    if not ctx.apply_pause:
        result["api_status"] = "not_applied_without_flag"
        return result, pd.DataFrame(api_logs)

    url_base = config.wb_base_url.rstrip("/") + WB_PAUSE_ENDPOINT
    status_by_campaign: Dict[str, Tuple[str, str]] = {}
    for campaign_id in sorted(result["campaign_id"].map(_clean_id_value).unique()):
        advert_id = to_int_id(campaign_id)
        if advert_id is None:
            status_by_campaign[campaign_id] = ("payload_error", "campaign_id не является числом")
            continue
        endpoint = f"{WB_PAUSE_ENDPOINT}?id={advert_id}"
        try:
            resp = requests.get(url_base, params={"id": advert_id}, headers=wb_headers(config), timeout=60)
            status_by_campaign[campaign_id] = (str(resp.status_code), resp.text)
            api_logs.append(api_log_row(ctx.run_datetime, "GET", endpoint, "", str(resp.status_code), resp.text, campaign_id=campaign_id))
        except Exception as exc:
            status_by_campaign[campaign_id] = ("exception", repr(exc))
            api_logs.append(api_log_row(ctx.run_datetime, "GET", endpoint, "", "exception", repr(exc), campaign_id=campaign_id))

    statuses: List[str] = []
    final_statuses: List[str] = []
    for _, row in result.iterrows():
        api_status, _ = status_by_campaign.get(_clean_id_value(row.get("campaign_id", "")), ("not_sent", ""))
        statuses.append(api_status)
        if api_status.isdigit() and 200 <= int(api_status) < 300:
            final_statuses.append("paused")
        else:
            final_statuses.append("candidate")
    result["api_status"] = statuses
    result["status"] = final_statuses
    return result, pd.DataFrame(api_logs)


def latest_pause_records(pause_history: pd.DataFrame) -> pd.DataFrame:
    if pause_history.empty:
        return pd.DataFrame(columns=PAUSE_HISTORY_COLUMNS)
    local = pause_history.copy()
    local["pause_date_parsed"] = pd.to_datetime(local["pause_date"], errors="coerce")
    local = local.sort_values(["campaign_id", "nm_id", "placement", "pause_date_parsed"])
    return local.groupby(["campaign_id", "nm_id", "placement"], dropna=False).tail(1)


def build_start_candidates(pause_history: pd.DataFrame, ads_df: pd.DataFrame, ctx: RunContext) -> pd.DataFrame:
    latest = latest_pause_records(pause_history)
    if latest.empty:
        return pd.DataFrame(columns=PAUSE_HISTORY_COLUMNS)
    rows: List[Dict[str, Any]] = []
    for _, row in latest.iterrows():
        status = _clean_text_value(row.get("status", "")).lower()
        if status not in {"paused", "keep_paused"}:
            continue
        pause_date = pd.to_datetime(row.get("pause_date", ""), errors="coerce")
        if pd.isna(pause_date):
            continue
        key = make_key(row)
        after = aggregate_after_event(ads_df, key, pause_date.date() + timedelta(days=1), ctx.mature_end)
        drr_after = safe_drr_pct(after["spend"], after["revenue"])
        gp_after = after["gp_after_ads"]
        if (after["revenue"] > 0 and after["orders"] > 0 and drr_after < DRR_LIMIT_PCT) or (not pd.isna(gp_after) and gp_after > 0):
            rows.append({
                "pause_event_id": str(uuid.uuid4()),
                "pause_date": date.today().isoformat(),
                "campaign_id": row.get("campaign_id", ""),
                "nm_id": row.get("nm_id", ""),
                "placement": row.get("placement", ""),
                "supplier_article": row.get("supplier_article", ""),
                "reason_code": "START_AFTER_ECONOMY_RECOVERY",
                "spend_before_pause": after["spend"],
                "revenue_before_pause": after["revenue"],
                "orders_before_pause": after["orders"],
                "drr_before_pause": drr_after,
                "gp_before_pause": gp_after,
                "status": "restart_candidate",
                "next_check_date": (date.today() + timedelta(days=1)).isoformat(),
                "api_status": "",
            })
    return pd.DataFrame(rows, columns=PAUSE_HISTORY_COLUMNS)


def apply_start_actions(start_candidates: pd.DataFrame, config: Config, ctx: RunContext) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if start_candidates.empty:
        return start_candidates.copy(), pd.DataFrame()
    result = start_candidates.copy()
    api_logs: List[Dict[str, Any]] = []
    if ctx.mode == "preview":
        result["api_status"] = "preview_no_call"
        return result, pd.DataFrame(api_logs)
    if ctx.dry_run:
        result["api_status"] = "dry_run_no_call"
        return result, pd.DataFrame(api_logs)
    if not ctx.apply_start:
        result["api_status"] = "not_applied_without_flag"
        return result, pd.DataFrame(api_logs)

    url_base = config.wb_base_url.rstrip("/") + WB_START_ENDPOINT
    status_by_campaign: Dict[str, Tuple[str, str]] = {}
    for campaign_id in sorted(result["campaign_id"].map(_clean_id_value).unique()):
        advert_id = to_int_id(campaign_id)
        if advert_id is None:
            status_by_campaign[campaign_id] = ("payload_error", "campaign_id не является числом")
            continue
        endpoint = f"{WB_START_ENDPOINT}?id={advert_id}"
        try:
            resp = requests.get(url_base, params={"id": advert_id}, headers=wb_headers(config), timeout=60)
            status_by_campaign[campaign_id] = (str(resp.status_code), resp.text)
            api_logs.append(api_log_row(ctx.run_datetime, "GET", endpoint, "", str(resp.status_code), resp.text, campaign_id=campaign_id))
        except Exception as exc:
            status_by_campaign[campaign_id] = ("exception", repr(exc))
            api_logs.append(api_log_row(ctx.run_datetime, "GET", endpoint, "", "exception", repr(exc), campaign_id=campaign_id))

    statuses: List[str] = []
    final_statuses: List[str] = []
    for _, row in result.iterrows():
        api_status, _ = status_by_campaign.get(_clean_id_value(row.get("campaign_id", "")), ("not_sent", ""))
        statuses.append(api_status)
        if api_status.isdigit() and 200 <= int(api_status) < 300:
            final_statuses.append("started")
        else:
            final_statuses.append("restart_candidate")
    result["api_status"] = statuses
    result["status"] = final_statuses
    return result, pd.DataFrame(api_logs)


# =============================
# Запись Excel / JSON
# =============================

def dataframe_to_excel_bytes(sheets: Dict[str, pd.DataFrame]) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            safe_name = sheet_name[:31]
            local_df = df.copy() if df is not None else pd.DataFrame()
            local_df.to_excel(writer, index=False, sheet_name=safe_name)
            ws = writer.book[safe_name]
            ws.freeze_panes = "A2"
            for col_cells in ws.columns:
                max_len = 0
                col_letter = col_cells[0].column_letter
                for cell in col_cells:
                    value = cell.value
                    if value is not None:
                        max_len = max(max_len, len(str(value)))
                ws.column_dimensions[col_letter].width = min(max(max_len + 2, 10), 60)
    return output.getvalue()


def save_table_to_s3_excel(s3_client, config: Config, key: str, df: pd.DataFrame) -> None:
    payload = dataframe_to_excel_bytes({"Лист1": df})
    upload_s3_bytes(s3_client, config.yc_bucket_name, key, payload, content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


def append_api_log_to_s3(s3_client, config: Config, new_log: pd.DataFrame) -> pd.DataFrame:
    existing = load_excel_table_from_s3(s3_client, config, API_LOG_KEY, [
        "run_datetime", "method", "endpoint", "campaign_id", "nm_id", "placement", "payload", "api_status", "response_text"
    ])
    if new_log is None or new_log.empty:
        combined = existing
    else:
        combined = pd.concat([existing, new_log], ignore_index=True, sort=False)
    save_table_to_s3_excel(s3_client, config, API_LOG_KEY, combined)
    return combined


def build_summary(ctx: RunContext, decisions: pd.DataFrame, successful_changes: pd.DataFrame, pause_candidates: pd.DataFrame, applied_pauses: pd.DataFrame, start_candidates: pd.DataFrame, applied_starts: pd.DataFrame) -> Dict[str, Any]:
    changed_count = int(len(successful_changes)) if successful_changes is not None else 0
    pause_applied_count = 0
    if applied_pauses is not None and not applied_pauses.empty:
        pause_applied_count = int((applied_pauses["status"] == "paused").sum())
    start_applied_count = 0
    if applied_starts is not None and not applied_starts.empty:
        start_applied_count = int((applied_starts["status"] == "started").sum())
    recommendation_count = 0
    if decisions is not None and not decisions.empty:
        recommendation_count = int(decisions["action"].isin(["Повысить", "Снизить"]).sum())

    return {
        "Режим": ctx.mode if not ctx.dry_run else f"{ctx.mode} --dry-run",
        "Дата формирования": ctx.run_datetime.strftime("%Y-%m-%d %H:%M:%S"),
        "Всего рекомендаций": recommendation_count,
        "Изменённых ставок": changed_count,
        "Блоков отправки ставок": recommendation_count if ctx.mode == "run" and not ctx.dry_run else 0,
        "Кандидатов на паузу": int(len(pause_candidates)) if pause_candidates is not None else 0,
        "Поставлено на паузу": pause_applied_count,
        "Кандидатов на запуск": int(len(start_candidates)) if start_candidates is not None else 0,
        "Запущено обратно": start_applied_count,
        "Текущее окно с": ctx.current_start.isoformat(),
        "Текущее окно по": ctx.current_end.isoformat(),
        "База с": ctx.base_start.isoformat(),
        "База по": ctx.base_end.isoformat(),
    }


def write_outputs(
    s3_client,
    config: Config,
    ctx: RunContext,
    decisions: pd.DataFrame,
    bid_history: pd.DataFrame,
    effect_df: pd.DataFrame,
    pause_candidates: pd.DataFrame,
    pause_history: pd.DataFrame,
    successful_changes: pd.DataFrame,
    api_log: pd.DataFrame,
    start_candidates: pd.DataFrame,
    applied_pauses: pd.DataFrame,
    applied_starts: pd.DataFrame,
) -> Dict[str, Any]:
    summary = build_summary(ctx, decisions, successful_changes, pause_candidates, applied_pauses, start_candidates, applied_starts)
    summary_df = pd.DataFrame([{"Показатель": k, "Значение": v} for k, v in summary.items()])

    sheets = {
        "Решения": decisions if decisions is not None else pd.DataFrame(columns=DECISION_COLUMNS),
        "История_изменений_ставок": bid_history if bid_history is not None else pd.DataFrame(columns=BID_HISTORY_COLUMNS),
        "Эффект_изменения_ставки": effect_df if effect_df is not None else pd.DataFrame(),
        "Кандидаты_на_паузу": pause_candidates if pause_candidates is not None else pd.DataFrame(columns=PAUSE_HISTORY_COLUMNS),
        "История_пауз": pause_history if pause_history is not None else pd.DataFrame(columns=PAUSE_HISTORY_COLUMNS),
        "Фактически_изменённые_ставки": successful_changes if successful_changes is not None else pd.DataFrame(),
        "Лог_API": api_log if api_log is not None else pd.DataFrame(),
        "Сводка": summary_df,
    }
    payload = dataframe_to_excel_bytes(sheets)
    output_key = PREVIEW_OUTPUT_KEY if ctx.mode == "preview" else RUN_OUTPUT_KEY
    upload_s3_bytes(s3_client, config.yc_bucket_name, output_key, payload, content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    upload_s3_bytes(
        s3_client,
        config.yc_bucket_name,
        SUMMARY_JSON_KEY,
        json.dumps(summary, ensure_ascii=False, indent=2).encode("utf-8"),
        content_type="application/json; charset=utf-8",
    )
    return summary


# =============================
# CLI и main
# =============================

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="WB TOPFACE strict ad bid manager")
    subparsers = parser.add_subparsers(dest="command", required=True)

    preview = subparsers.add_parser("preview", help="Предпросмотр без отправки изменений ставок")
    preview.set_defaults(dry_run=False, apply_pause=False, apply_start=False)

    run = subparsers.add_parser("run", help="Боевой расчёт и отправка изменений ставок")
    run.add_argument("--dry-run", action="store_true", help="Расчёт run-режима без реальных API-вызовов")
    run.add_argument("--apply-pause", action="store_true", help="Разрешить отправку pause для кандидатов")
    run.add_argument("--apply-start", action="store_true", help="Разрешить отправку start для кандидатов")
    return parser.parse_args(argv)


def print_summary(summary: Dict[str, Any]) -> None:
    print("=== Сводка запуска ===")
    for key, value in summary.items():
        print(f"{key}: {value}")


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    config = load_config()
    ctx = build_run_context(args)
    s3_client = make_s3_client(config)

    print(f"[{ctx.run_datetime:%Y-%m-%d %H:%M:%S}] Старт {SCRIPT_NAME}: версия={SCRIPT_VERSION}, режим={ctx.mode}, dry_run={ctx.dry_run}")
    print(f"Окна: база {ctx.base_start}..{ctx.base_end}; текущее {ctx.current_start}..{ctx.current_end}; mature_end={ctx.mature_end}")

    ads_df = load_ads_report(s3_client, config)
    print(f"Рекламный отчёт загружен: {len(ads_df):,} строк".replace(",", " "))

    bid_history = load_bid_history(s3_client, config)
    pause_history = load_pause_history(s3_client, config)
    print(f"История ставок: {len(bid_history):,} строк; история пауз: {len(pause_history):,} строк".replace(",", " "))

    bid_history, effect_df = evaluate_postchecks(ads_df, bid_history, ctx)
    pending_events = load_pending_events(bid_history)
    postcheck_results = latest_postcheck_results(bid_history)

    metrics_df = aggregate_campaign_metrics(ads_df, ctx)
    print(f"Диагностика агрегации: строк метрик={len(metrics_df)}", flush=True)
    if not metrics_df.empty:
        print("Диагностика метрик по статусам: " + json.dumps(metrics_df.get("campaign_status", pd.Series(dtype=str)).map(str).value_counts().head(10).to_dict(), ensure_ascii=False), flush=True)
        print("Диагностика метрик по предметам: " + json.dumps(metrics_df.get("subject_norm", pd.Series(dtype=str)).map(str).value_counts().head(10).to_dict(), ensure_ascii=False), flush=True)
    decisions = build_decisions(metrics_df, pending_events, postcheck_results)
    if not decisions.empty:
        print("Диагностика решений action: " + json.dumps(decisions["action"].value_counts().to_dict(), ensure_ascii=False), flush=True)
        print("Диагностика решений reason_code: " + json.dumps(decisions["reason_code"].value_counts().head(10).to_dict(), ensure_ascii=False), flush=True)
    pause_candidates = build_pause_candidates(decisions, bid_history)

    successful_changes, bid_api_log = apply_bid_changes(decisions, config, ctx)
    bid_history = record_bid_events(successful_changes, bid_history, ctx)

    applied_pauses, pause_api_log = apply_pause_actions(pause_candidates, config, ctx)
    if not applied_pauses.empty:
        pause_history = pd.concat([pause_history, applied_pauses[PAUSE_HISTORY_COLUMNS]], ignore_index=True, sort=False)

    start_candidates = build_start_candidates(pause_history, ads_df, ctx)
    applied_starts, start_api_log = apply_start_actions(start_candidates, config, ctx)
    if not applied_starts.empty:
        pause_history = pd.concat([pause_history, applied_starts[PAUSE_HISTORY_COLUMNS]], ignore_index=True, sort=False)

    all_api_log = pd.concat(
        [df for df in [bid_api_log, pause_api_log, start_api_log] if df is not None and not df.empty],
        ignore_index=True,
        sort=False,
    ) if any(df is not None and not df.empty for df in [bid_api_log, pause_api_log, start_api_log]) else pd.DataFrame()
    full_api_log = append_api_log_to_s3(s3_client, config, all_api_log)

    save_table_to_s3_excel(s3_client, config, BID_HISTORY_KEY, bid_history[BID_HISTORY_COLUMNS])
    save_table_to_s3_excel(s3_client, config, PAUSE_HISTORY_KEY, pause_history[PAUSE_HISTORY_COLUMNS] if not pause_history.empty else pd.DataFrame(columns=PAUSE_HISTORY_COLUMNS))

    summary = write_outputs(
        s3_client=s3_client,
        config=config,
        ctx=ctx,
        decisions=decisions,
        bid_history=bid_history[BID_HISTORY_COLUMNS],
        effect_df=effect_df,
        pause_candidates=pause_candidates,
        pause_history=pause_history[PAUSE_HISTORY_COLUMNS] if not pause_history.empty else pd.DataFrame(columns=PAUSE_HISTORY_COLUMNS),
        successful_changes=successful_changes,
        api_log=full_api_log,
        start_candidates=start_candidates,
        applied_pauses=applied_pauses,
        applied_starts=applied_starts,
    )
    print_summary(summary)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ОШИБКА: {exc}", file=sys.stderr)
        raise
