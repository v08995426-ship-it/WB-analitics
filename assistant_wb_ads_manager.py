
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import io
import json
import math
import os
import re
import time
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import boto3
import numpy as np
import pandas as pd
import requests
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError
from openpyxl import load_workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

STORE_NAME = "TOPFACE"
TARGET_SUBJECTS = {"кисти косметические", "блески", "помады", "косметические карандаши"}
GROWTH_SUBJECTS = {"блески", "помады", "косметические карандаши"}
CATEGORY_DRR_LIMITS = {
    "кисти косметические": 0.14,
    "косметические карандаши": 0.14,
    "помады": 0.16,
    "блески": 0.16,
}
SUBJECT_DISPLAY_NAMES = {
    "кисти косметические": "Кисти косметические",
    "косметические карандаши": "Косметические карандаши",
    "помады": "Помады",
    "блески": "Блески",
}

SUBJECT_FIXED_BUYOUT_RATES = {
    "кисти косметические": 0.85,
    "косметические карандаши": 0.95,
    "помады": 0.93,
    "блески": 0.90,
}
FUNNEL_SALES_CANDIDATES = [
    "ordersSumRub",
    "ordersSum",
    "ordersSumRur",
    "ordersAmountRub",
    "ordersAmount",
    "sumOrdersRub",
    "sumOrders",
    "sum_orders_rub",
    "sum_orders",
    "salesRub",
    "salesSumRub",
    "salesSum",
    "salesAmount",
    "revenueRub",
    "revenue",
    "Продажи, ₽",
    "Продажи",
    "Сумма продаж, ₽",
    "Сумма продаж",
    "Сумма заказов, ₽",
    "Сумма заказов",
    "Заказано на сумму, ₽",
    "Заказано на сумму",
]

FUNNEL_BUYOUT_CANDIDATES = [
    "buyoutPercent",
    "buyoutRate",
    "buyout_rate",
    "Процент выкупа",
    "% выкупа",
    "Выкуп, %",
]
ECON_BUYOUT_CANDIDATES = [
    "Процент выкупа",
    "% выкупа",
    "buyoutPercent",
    "buyout_rate",
]
ECON_GP_UNIT_CANDIDATES = [
    "Валовая прибыль, руб/ед",
    "Валовая прибыль на 1 товар, ₽",
    "Валовая прибыль на 1 товар",
    "ВП на 1 товар, ₽",
    "ВП на 1 товар",
    "gp_unit",
]
ECON_NP_UNIT_CANDIDATES = [
    "Чистая прибыль, руб/ед",
    "Чистая прибыль на 1 товар, ₽",
    "Чистая прибыль на 1 товар",
    "ЧП на 1 товар, ₽",
    "ЧП на 1 товар",
    "np_unit",
]
ECON_WEEK_CANDIDATES = [
    "Неделя",
    "week",
    "Дата",
    "date",
    "Период",
]

def get_category_drr_limit(subject_norm: Any) -> float:
    return CATEGORY_DRR_LIMITS.get(canonical_subject(subject_norm), 0.15)

def get_subject_display_name(subject_norm: Any) -> str:
    norm = canonical_subject(subject_norm)
    return SUBJECT_DISPLAY_NAMES.get(norm, str(subject_norm or "").strip() or norm)


def get_subject_buyout_rate(subject_norm: Any, default: float = 0.90) -> float:
    return SUBJECT_FIXED_BUYOUT_RATES.get(canonical_subject(subject_norm), default)

def get_bid_step_rub(payment_type: Any) -> float:
    return 1.0 if canonical_payment_type(payment_type) == 'cpc' else 6.0

def apply_bid_step(current_bid: float, payment_type: Any, direction: str, floor_bid: float, max_bid: float = 0.0) -> float:
    current_bid = safe_float(current_bid)
    step = get_bid_step_rub(payment_type)
    if direction == 'up':
        target = current_bid + step
        if max_bid > 0:
            target = min(target, max_bid)
        return round(max(target, floor_bid), 2)
    return round(max(current_bid - step, floor_bid), 2)

def is_drop_explained_by_demand(order_growth_pct: float, demand_growth_pct: float, tolerance_pp: float = 5.0) -> bool:
    order_growth_pct = safe_float(order_growth_pct)
    demand_growth_pct = safe_float(demand_growth_pct)
    if order_growth_pct >= 0 or demand_growth_pct >= 0:
        return False
    return demand_growth_pct <= order_growth_pct + tolerance_pp


def build_category_window_diagnostics(
    orders: pd.DataFrame,
    ads_daily: pd.DataFrame,
    funnel: pd.DataFrame,
    keywords: pd.DataFrame,
    master: pd.DataFrame,
    econ_latest: pd.DataFrame,
    window: Dict[str, date],
) -> pd.DataFrame:
    subjects = pd.DataFrame({'subject_norm': sorted(TARGET_SUBJECTS)})
    key_map = master[['nmId', 'subject_norm']].drop_duplicates() if not master.empty else pd.DataFrame(columns=['nmId', 'subject_norm'])
    key_map = key_map.copy()
    if 'nmId' in key_map.columns:
        key_map['nmId'] = pd.to_numeric(key_map['nmId'], errors='coerce')
        key_map = key_map.dropna(subset=['nmId']).copy()
    nm_to_subject: Dict[Any, Any] = {}
    if not key_map.empty:
        km = key_map.dropna(subset=['subject_norm']).copy()
        km['subject_norm'] = km['subject_norm'].map(canonical_subject)
        nm_to_subject = dict(zip(km['nmId'].tolist(), km['subject_norm'].tolist()))

    def _orders_slice(start: date, end: date, suffix: str) -> pd.DataFrame:
        if orders.empty:
            return pd.DataFrame(columns=['subject_norm', f'category_orders_{suffix}', f'category_gp_before_ads_{suffix}'])
        ords = orders[(orders['date'] >= start) & (orders['date'] <= end) & (~orders['isCancel'])].copy()
        if ords.empty:
            return pd.DataFrame(columns=['subject_norm', f'category_orders_{suffix}', f'category_gp_before_ads_{suffix}'])
        gp_map = econ_latest[['nmId', 'gp_realized']].drop_duplicates() if not econ_latest.empty else pd.DataFrame(columns=['nmId', 'gp_realized'])
        if not key_map.empty:
            ords = ords.merge(key_map, on='nmId', how='left', suffixes=('', '_m'))
        ords = with_resolved_subject_norm(ords, nm_to_subject)
        ords = ords.merge(gp_map, on='nmId', how='left')
        ords['gp_realized'] = pd.to_numeric(ords.get('gp_realized'), errors='coerce').fillna(0.0)
        ords = ords[ords['subject_norm'].isin(TARGET_SUBJECTS)].copy()
        if ords.empty:
            return pd.DataFrame(columns=['subject_norm', f'category_orders_{suffix}', f'category_gp_before_ads_{suffix}'])
        return ords.groupby('subject_norm', as_index=False).agg(
            **{
                f'category_orders_{suffix}': ('nmId', 'count'),
                f'category_gp_before_ads_{suffix}': ('gp_realized', 'sum'),
            }
        )

    def _spend_slice(start: date, end: date, suffix: str) -> pd.DataFrame:
        if ads_daily.empty:
            return pd.DataFrame(columns=['subject_norm', f'category_spend_{suffix}'])
        ad = ads_daily[(ads_daily['date'] >= start) & (ads_daily['date'] <= end)].copy()
        if ad.empty:
            return pd.DataFrame(columns=['subject_norm', f'category_spend_{suffix}'])
        if not key_map.empty:
            ad = ad.merge(key_map, on='nmId', how='left', suffixes=('', '_m'))
        ad = with_resolved_subject_norm(ad, nm_to_subject)
        ad['Расход'] = pd.to_numeric(ad.get('Расход'), errors='coerce').fillna(0.0)
        ad = ad[ad['subject_norm'].isin(TARGET_SUBJECTS)].copy()
        if ad.empty:
            return pd.DataFrame(columns=['subject_norm', f'category_spend_{suffix}'])
        return ad.groupby('subject_norm', as_index=False).agg(**{f'category_spend_{suffix}': ('Расход', 'sum')})

    def _demand_slice(start: date, end: date, suffix: str) -> pd.DataFrame:
        if keywords.empty:
            return pd.DataFrame(columns=['subject_norm', f'category_demand_{suffix}'])
        kw = keywords[(keywords['date'] >= start) & (keywords['date'] <= end)].copy()
        if kw.empty:
            return pd.DataFrame(columns=['subject_norm', f'category_demand_{suffix}'])
        kw = with_resolved_subject_norm(kw, nm_to_subject)
        kw['demand_week'] = pd.to_numeric(kw.get('demand_week'), errors='coerce').fillna(0.0)
        kw = kw[kw['subject_norm'].isin(TARGET_SUBJECTS)].copy()
        if kw.empty:
            return pd.DataFrame(columns=['subject_norm', f'category_demand_{suffix}'])
        return kw.groupby('subject_norm', as_index=False).agg(**{f'category_demand_{suffix}': ('demand_week', 'sum')})

    def _realized_sales_slice(start: date, end: date) -> pd.DataFrame:
        if funnel.empty:
            return pd.DataFrame(columns=['subject_norm', 'category_funnel_sales_cur', 'category_realized_sales_cur'])
        fw = funnel[(funnel['date'] >= start) & (funnel['date'] <= end)].copy()
        if fw.empty:
            return pd.DataFrame(columns=['subject_norm', 'category_funnel_sales_cur', 'category_realized_sales_cur'])
        sales_col = find_matching_column(fw, FUNNEL_SALES_CANDIDATES)
        if not key_map.empty:
            fw = fw.merge(key_map, on='nmId', how='left', suffixes=('', '_m'))
        fw = with_resolved_subject_norm(fw, nm_to_subject)
        fw['funnel_sales'] = pd.to_numeric(fw.get(sales_col if sales_col else 'ordersSumRub', 0), errors='coerce').fillna(0.0)
        fw = fw[fw['subject_norm'].isin(TARGET_SUBJECTS)].copy()
        if fw.empty:
            return pd.DataFrame(columns=['subject_norm', 'category_funnel_sales_cur', 'category_realized_sales_cur'])
        fw['buyout_rate'] = fw['subject_norm'].map(get_subject_buyout_rate)
        fw['realized_sales'] = fw['funnel_sales'] * fw['buyout_rate']
        return fw.groupby('subject_norm', as_index=False).agg(
            category_funnel_sales_cur=('funnel_sales', 'sum'),
            category_realized_sales_cur=('realized_sales', 'sum'),
        )

    out = subjects
    for df in [
        _orders_slice(window['cur_start'], window['cur_end'], 'cur'),
        _orders_slice(window['base_start'], window['base_end'], 'base'),
        _spend_slice(window['cur_start'], window['cur_end'], 'cur'),
        _spend_slice(window['base_start'], window['base_end'], 'base'),
        _demand_slice(window['cur_start'], window['cur_end'], 'cur'),
        _demand_slice(window['base_start'], window['base_end'], 'base'),
        _realized_sales_slice(window['cur_start'], window['cur_end']),
    ]:
        out = out.merge(df, on='subject_norm', how='left')
    for col in [
        'category_orders_cur', 'category_gp_before_ads_cur', 'category_orders_base', 'category_gp_before_ads_base',
        'category_spend_cur', 'category_spend_base', 'category_demand_cur', 'category_demand_base',
        'category_funnel_sales_cur', 'category_realized_sales_cur'
    ]:
        if col not in out.columns:
            out[col] = 0.0
        out[col] = pd.to_numeric(out[col], errors='coerce').fillna(0.0)
    out['category_gp_cur'] = out['category_gp_before_ads_cur'] - out['category_spend_cur']
    out['category_gp_base'] = out['category_gp_before_ads_base'] - out['category_spend_base']
    out['category_drr_cur'] = np.where(out['category_realized_sales_cur'] > 0, out['category_spend_cur'] / out['category_realized_sales_cur'], 0.0)
    out['category_orders_growth_pct'] = np.where(out['category_orders_base'] > 0, (out['category_orders_cur'] / out['category_orders_base'] - 1.0) * 100.0, np.where(out['category_orders_cur'] > 0, 100.0, 0.0))
    out['category_gp_growth_pct'] = np.where(out['category_gp_base'] != 0, (out['category_gp_cur'] - out['category_gp_base']) / np.abs(out['category_gp_base']) * 100.0, np.where(out['category_gp_cur'] > 0, 100.0, 0.0))
    out['category_demand_growth_pct'] = np.where(out['category_demand_base'] > 0, (out['category_demand_cur'] / out['category_demand_base'] - 1.0) * 100.0, np.where(out['category_demand_cur'] > 0, 100.0, 0.0))
    out['category_limit_drr'] = out['subject_norm'].map(get_category_drr_limit)
    out['Категория'] = out['subject_norm'].map(get_subject_display_name)
    out['Фиксированный % выкупа'] = out['subject_norm'].map(lambda x: round(get_subject_buyout_rate(x) * 100.0, 1))
    return out

def _normalize_name_for_match(value: Any) -> str:
    return re.sub(r"[^0-9a-zа-я]+", "", str(value or "").strip().lower())

def find_matching_column(df: pd.DataFrame, candidates: List[str]) -> str:
    if df is None or df.empty:
        return ""
    cols = list(df.columns)
    exact = {str(c): str(c) for c in cols}
    for cand in candidates:
        if cand in exact:
            return exact[cand]
    normalized = {_normalize_name_for_match(c): str(c) for c in cols}
    for cand in candidates:
        key = _normalize_name_for_match(cand)
        if key in normalized:
            return normalized[key]
    for cand in candidates:
        key = _normalize_name_for_match(cand)
        if not key:
            continue
        for norm_col, original_col in normalized.items():
            if key in norm_col or norm_col in key:
                return original_col
    return ""


def to_buyout_rate(series: pd.Series, default: float = 0.0) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    s = np.where(pd.Series(s).fillna(0) > 1, pd.Series(s).fillna(0) / 100.0, pd.Series(s).fillna(0))
    return pd.Series(s).fillna(default).clip(lower=0.0, upper=1.0)

def resolve_buyout_rate_from_funnel(df: pd.DataFrame, default: float = 0.85) -> pd.Series:
    if df is None or df.empty:
        return pd.Series(dtype=float)
    rate_col = find_matching_column(df, FUNNEL_BUYOUT_CANDIDATES)
    if rate_col:
        rate = to_buyout_rate(df[rate_col], default=np.nan)
    else:
        rate = pd.Series(np.nan, index=df.index, dtype=float)
    sales_col = find_matching_column(df, FUNNEL_SALES_CANDIDATES)
    if sales_col and "buyoutsSumRub" in df.columns:
        sales = pd.to_numeric(df[sales_col], errors="coerce").fillna(0.0)
        buyout_sum = pd.to_numeric(df["buyoutsSumRub"], errors="coerce").fillna(0.0)
        ratio_sum = np.where(sales > 0, buyout_sum / sales, np.nan)
        rate = rate.where(rate.fillna(0) > 0, pd.Series(ratio_sum, index=df.index))
    if "ordersCount" in df.columns and "buyoutsCount" in df.columns:
        orders_cnt = pd.to_numeric(df["ordersCount"], errors="coerce").fillna(0.0)
        buyouts_cnt = pd.to_numeric(df["buyoutsCount"], errors="coerce").fillna(0.0)
        ratio_cnt = np.where(orders_cnt > 0, buyouts_cnt / orders_cnt, np.nan)
        rate = rate.where(rate.fillna(0) > 0, pd.Series(ratio_cnt, index=df.index))
    return pd.to_numeric(rate, errors="coerce").fillna(default).clip(lower=0.0, upper=1.0)

def latest_econ_rows(econ: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    if econ is None or econ.empty:
        return pd.DataFrame(columns=columns)
    cols = [c for c in columns if c in econ.columns]
    if "nmId" not in cols:
        cols = ["nmId"] + cols
    work = econ.copy()
    week_col = find_matching_column(work, ECON_WEEK_CANDIDATES)
    if week_col:
        raw = work[week_col].fillna("").astype(str).str.strip()
        parsed = pd.Series(pd.NaT, index=work.index, dtype="datetime64[ns]")

        mask_iso_week = raw.str.fullmatch(r"\d{4}-W\d{2}")
        if mask_iso_week.any():
            parsed.loc[mask_iso_week] = pd.to_datetime(
                raw.loc[mask_iso_week] + "-1",
                format="%G-W%V-%u",
                errors="coerce",
            )

        mask_iso_date = raw.str.fullmatch(r"\d{4}-\d{2}-\d{2}")
        if mask_iso_date.any():
            parsed.loc[mask_iso_date] = pd.to_datetime(
                raw.loc[mask_iso_date],
                format="%Y-%m-%d",
                errors="coerce",
            )

        mask_dot_date = raw.str.fullmatch(r"\d{2}\.\d{2}\.\d{4}")
        if mask_dot_date.any():
            parsed.loc[mask_dot_date] = pd.to_datetime(
                raw.loc[mask_dot_date],
                format="%d.%m.%Y",
                errors="coerce",
            )

        mask_slash_date = raw.str.fullmatch(r"\d{2}/\d{2}/\d{4}")
        if mask_slash_date.any():
            parsed.loc[mask_slash_date] = pd.to_datetime(
                raw.loc[mask_slash_date],
                format="%d/%m/%Y",
                errors="coerce",
            )

        other_mask = parsed.isna() & raw.ne("")
        if other_mask.any():
            parsed.loc[other_mask] = pd.to_datetime(raw.loc[other_mask], errors="coerce")

        work["_econ_order"] = parsed.fillna(pd.Timestamp("1900-01-01"))
    else:
        work["_econ_order"] = np.arange(len(work))
    cols = [c for c in cols if c in work.columns]
    cols.append("_econ_order")
    return work[cols].sort_values("_econ_order").drop_duplicates("nmId", keep="last").drop(columns=["_econ_order"], errors="ignore")

def round_output_value(value: Any) -> Any:
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, (np.integer, int)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        if pd.isna(value):
            return np.nan
        abs_v = abs(float(value))
        if abs_v == 0:
            return 0
        if abs_v < 1:
            return round(float(value), 2)
        return int(round(float(value)))
    return value

def normalize_output_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return df
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_numeric_dtype(out[col]):
            out[col] = out[col].map(round_output_value)
    return out

def trim_to_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    if df is None or not isinstance(df, pd.DataFrame):
        return pd.DataFrame(columns=columns)
    out = df.copy()
    for c in columns:
        if c not in out.columns:
            out[c] = "" if "Дата" in c or "Предмет" in c or "Артикул" in c or "Тип" in c or "Плейсмент" in c or "Комментарий" in c or "Причина" in c else 0
    return out[columns]

def is_active_campaign_status(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return text in {"активна", "active", "running", "started", "в работе"}


def explain_limit_reason(row: pd.Series) -> str:
    gp_realized = safe_float(row.get("gp_realized"))
    local_clicks = safe_float(row.get("Клики"))
    local_orders = safe_float(row.get("Заказы"))
    inherited_clicks = safe_float(row.get("item_clicks_cur", row.get("control_ad_clicks", 0.0)))
    inherited_orders = safe_float(row.get("total_orders"))
    parts: List[str] = []
    if gp_realized <= 0:
        parts.append("нет положительной unit-экономики после выкупа")
    if local_clicks >= 50 and local_orders >= 3:
        parts.append(f"лимит рассчитан по факту: клики={local_clicks:.0f}, заказы РК={local_orders:.0f}")
    elif inherited_clicks >= 50 and inherited_orders >= 5:
        parts.append(f"лимит рассчитан по товару: клики={inherited_clicks:.0f}, все заказы={inherited_orders:.0f}")
    else:
        parts.append(
            "недостаточно данных для CPO: "
            f"по кампании клики={local_clicks:.0f} (<50) или заказы РК={local_orders:.0f} (<3); "
            f"по товару клики={inherited_clicks:.0f} (<50) или все заказы={inherited_orders:.0f} (<5)"
        )
    return "; ".join(parts)


def with_resolved_subject_norm(df: pd.DataFrame, nm_to_subject: Dict[Any, Any]) -> pd.DataFrame:
    out = df.copy()
    if out.empty:
        if "subject_norm" not in out.columns:
            out["subject_norm"] = pd.Series(dtype="object")
        return out

    resolved = pd.Series([""] * len(out), index=out.index, dtype="object")

    for col in ["subject_norm", "subject_norm_x", "subject_norm_y", "subject", "Предмет", "Название предмета"]:
        if col in out.columns:
            normalized = out[col].fillna("").astype(str).map(canonical_subject)
            resolved = resolved.where(resolved.astype(str).str.strip() != "", normalized)

    if "nmId" in out.columns and nm_to_subject:
        nm_series = pd.to_numeric(out["nmId"], errors="coerce")
        mapped = nm_series.map(nm_to_subject).fillna("").astype(str).map(canonical_subject)
        resolved = resolved.where(resolved.astype(str).str.strip() != "", mapped)

    out["subject_norm"] = resolved.fillna("").astype(str).map(canonical_subject)
    return out


def build_nm_to_subject_map(master: pd.DataFrame) -> Dict[Any, Any]:
    if master is None or master.empty:
        return {}
    work = master.copy()
    if 'nmId' not in work.columns:
        return {}
    work = with_resolved_subject_norm(work, {})
    nm_series = pd.to_numeric(work['nmId'], errors='coerce')
    subj_series = work.get('subject_norm', pd.Series('', index=work.index)).fillna('').astype(str).map(canonical_subject)
    mask = nm_series.notna() & subj_series.ne('')
    if not mask.any():
        return {}
    dedup = pd.DataFrame({'nmId': nm_series[mask].astype('int64'), 'subject_norm': subj_series[mask]})
    dedup = dedup.drop_duplicates(subset=['nmId'], keep='first')
    return dict(zip(dedup['nmId'], dedup['subject_norm']))
WB_BIDS_URL = "https://advert-api.wildberries.ru/api/advert/v1/bids"
WB_BIDS_MIN_URL = "https://advert-api.wildberries.ru/api/advert/v1/bids/min"
WB_NMS_URL = "https://advert-api.wildberries.ru/adv/v0/auction/nms"
WB_PAUSE_URL = "https://advert-api.wildberries.ru/adv/v0/pause"
WB_START_URL = "https://advert-api.wildberries.ru/adv/v0/start"

ADS_ANALYSIS_KEY = f"Отчёты/Реклама/{STORE_NAME}/Анализ рекламы.xlsx"
ECONOMICS_KEY = f"Отчёты/Финансовые показатели/{STORE_NAME}/Экономика.xlsx"
FUNNEL_KEY = f"Отчёты/Воронка продаж/{STORE_NAME}/Воронка продаж.xlsx"
ORDERS_WEEKLY_PREFIX = f"Отчёты/Заказы/{STORE_NAME}/Недельные/"
KEYWORDS_WEEKLY_PREFIX = f"Отчёты/Поисковые запросы/{STORE_NAME}/Недельные/"
ABC_PREFIX = "Отчёты/ABC/"
DYNAMICS_PREFIX = "Отчёты/ABC/"

SERVICE_ROOT = f"Служебные файлы/Ассистент WB/{STORE_NAME}/"
OUT_PREVIEW = SERVICE_ROOT + "Предпросмотр_последнего_запуска.xlsx"
OUT_SUMMARY = SERVICE_ROOT + "Сводка_последнего_запуска.json"
OUT_ARCHIVE = SERVICE_ROOT + "Архив_решений.xlsx"
OUT_BID_HISTORY = SERVICE_ROOT + "История_ставок.xlsx"
OUT_LIMITS = SERVICE_ROOT + "Лимиты_ставок_ежедневно.xlsx"
OUT_PRODUCT = SERVICE_ROOT + "Метрики_по_товарам.xlsx"
OUT_EFF = SERVICE_ROOT + "Эффективность_ставки_ежедневно.xlsx"
OUT_WEAK = SERVICE_ROOT + "Слабые_позиции_приоритет.xlsx"
OUT_EFFECTS = SERVICE_ROOT + "Эффект_изменений.xlsx"
# Блок работы с оттенками отключён: каждый оттенок должен вестись отдельной рекламной кампанией.
OUT_SHADE_ACTIONS = SERVICE_ROOT + "Рекомендации_по_оттенкам.xlsx"
OUT_SHADE_PORTFOLIO = SERVICE_ROOT + "Состав_кампаний_по_оттенкам.xlsx"
OUT_SHADE_TESTS = SERVICE_ROOT + "Тесты_оттенков.xlsx"
OUT_BENCHMARK = SERVICE_ROOT + "Сравнение_с_сильными_РК.xlsx"

# Единый итоговый файл. Все отчёты пишем только сюда.
OUT_SINGLE_REPORT = SERVICE_ROOT + "Итог_последнего_запуска.xlsx"

MIN_RATING_SHADE = 4.6
MATURE_START_OFFSET = 7
MATURE_END_OFFSET = 3
WINDOW_LEN = 5

API_CALL_LOGS: List[Dict[str, Any]] = []
MIN_BID_ROWS: List[Dict[str, Any]] = []
_LAST_API_CALL_AT: Dict[str, float] = {}
_API_MIN_INTERVAL_SEC = {
    WB_BIDS_MIN_URL: 3.1,   # 20 req/min, interval 3 sec
    WB_NMS_URL: 1.05,       # 1 req/sec
    WB_BIDS_URL: 0.25,      # 5 req/sec
}

def now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def json_dumps_safe(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, default=str)
    except Exception:
        return str(value)

def truncate_text(value: Any, limit: int = 4000) -> str:
    text_value = value if isinstance(value, str) else json_dumps_safe(value)
    return text_value[:limit]

def canonical_payment_type(value: Any) -> str:
    v = str(value or "").strip().lower()
    return "cpc" if v == "cpc" else "cpm"

def normalize_internal_placement(value: Any) -> str:
    v = str(value or "").strip().lower()
    mapping = {
        "combined": "combined",
        "search": "search",
        "recommendation": "recommendation",
        "recommendations": "recommendation",
    }
    return mapping.get(v, "search")

def placement_for_min_endpoint(value: Any) -> str:
    v = normalize_internal_placement(value)
    return "recommendation" if v == "recommendation" else v

def placement_for_bids_endpoint(value: Any) -> str:
    v = normalize_internal_placement(value)
    return "recommendations" if v == "recommendation" else v

def wait_for_rate_limit(url: str) -> None:
    delay = _API_MIN_INTERVAL_SEC.get(url, 0.0)
    if delay <= 0:
        return
    last = _LAST_API_CALL_AT.get(url, 0.0)
    now = time.time()
    sleep_for = delay - (now - last)
    if sleep_for > 0:
        time.sleep(sleep_for)

def extract_request_id(response_text: str) -> str:
    if not response_text:
        return ""
    try:
        data = json.loads(response_text)
        return str(data.get("requestId") or data.get("request_id") or "")
    except Exception:
        return ""

def append_api_log(
    *,
    method_name: str,
    http_method: str,
    url: str,
    request_body: Any,
    response_status: Any = "",
    response_text: Any = "",
    status: str = "",
    context: Optional[Dict[str, Any]] = None,
) -> None:
    row: Dict[str, Any] = {
        "timestamp": now_ts(),
        "Метод": method_name,
        "HTTP метод": http_method.upper(),
        "URL": url,
        "status": status,
        "http_status": response_status,
        "request_id": extract_request_id(str(response_text)),
        "request_body": truncate_text(request_body, 8000),
        "response": truncate_text(response_text, 8000),
    }
    if context:
        for k, v in context.items():
            row[k] = v
    API_CALL_LOGS.append(row)

def wb_api_request(
    http_method: str,
    url: str,
    api_key: str,
    body: Any,
    *,
    method_name: str,
    timeout: int = 120,
    dry_run: bool = False,
    context: Optional[Dict[str, Any]] = None,
) -> Optional[requests.Response]:
    if not api_key:
        append_api_log(
            method_name=method_name,
            http_method=http_method,
            url=url,
            request_body=body,
            response_status="",
            response_text="Нет WB_PROMO_KEY_TOPFACE, вызов не выполнен",
            status="skipped",
            context=context,
        )
        return None
    if dry_run:
        append_api_log(
            method_name=method_name,
            http_method=http_method,
            url=url,
            request_body=body,
            response_status="",
            response_text="dry-run",
            status="dry-run",
            context=context,
        )
        return None

    wait_for_rate_limit(url)
    headers = {"Authorization": api_key.strip(), "Content-Type": "application/json"}
    try:
        resp = requests.request(http_method.upper(), url, headers=headers, json=body, timeout=timeout)
        _LAST_API_CALL_AT[url] = time.time()
        append_api_log(
            method_name=method_name,
            http_method=http_method,
            url=url,
            request_body=body,
            response_status=resp.status_code,
            response_text=resp.text,
            status="ok" if resp.status_code == 200 else "failed",
            context=context,
        )
        return resp
    except Exception as e:
        _LAST_API_CALL_AT[url] = time.time()
        append_api_log(
            method_name=method_name,
            http_method=http_method,
            url=url,
            request_body=body,
            response_status="",
            response_text=str(e),
            status="failed",
            context=context,
        )
        return None


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)

def safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if pd.isna(v):
            return default
        if isinstance(v, str):
            v = v.replace("\xa0", " ").replace("%", "").replace(",", ".").strip()
            if not v:
                return default
        return float(v)
    except Exception:
        return default



def ensure_business_keys(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or not isinstance(df, pd.DataFrame):
        return df
    out = df.copy()
    if 'nmId' not in out.columns:
        for c in ['Артикул WB', 'nm_id', 'nmID']:
            if c in out.columns:
                out['nmId'] = out[c]
                break
    if 'supplier_article' not in out.columns:
        for c in ['Артикул продавца', 'supplier_article_x', 'supplier_article_y', 'supplierArticle', 'supplierArticle_x', 'supplierArticle_y', 'control_key']:
            if c in out.columns:
                out['supplier_article'] = out[c]
                break
    if 'subject' not in out.columns:
        for c in ['Предмет', 'subject_norm']:
            if c in out.columns:
                out['subject'] = out[c]
                break
    if 'Артикул WB' not in out.columns and 'nmId' in out.columns:
        out['Артикул WB'] = out['nmId']
    if 'Артикул продавца' not in out.columns and 'supplier_article' in out.columns:
        out['Артикул продавца'] = out['supplier_article']
    if 'Предмет' not in out.columns and 'subject' in out.columns:
        out['Предмет'] = out['subject']
    return out



def normalize_core_columns(df: pd.DataFrame) -> pd.DataFrame:
    return ensure_business_keys(df)

def safe_int(v: Any, default: int = 0) -> int:
    try:
        if pd.isna(v):
            return default
        if isinstance(v, str):
            v = v.replace("\xa0", " ").replace(",", ".").strip()
        return int(float(v))
    except Exception:
        return default


def series_or_default(df: pd.DataFrame, column: str, default: Any = 0.0) -> pd.Series:
    if isinstance(df, pd.DataFrame) and column in df.columns:
        return df[column]
    if isinstance(default, pd.Series):
        try:
            return default.reindex(df.index)
        except Exception:
            return default
    if isinstance(df, pd.DataFrame):
        return pd.Series(default, index=df.index)
    return pd.Series([default])


def numeric_series(df: pd.DataFrame, column: str, default: float = 0.0) -> pd.Series:
    return pd.to_numeric(series_or_default(df, column, default), errors="coerce").fillna(default)

def canonical_subject(v: Any) -> str:
    return str(v or "").strip().lower()

def product_root_from_supplier_article(v: Any) -> str:
    s = str(v or "").strip()
    if not s or s.lower() in {"nan", "none"}:
        return ""
    root = s.split("/")[0].strip()
    root = re.sub(r"[^0-9A-Za-zА-Яа-я_-]+", "", root)
    root = re.sub(r"[_-]+$", "", root)
    return root.upper()

def pct(a: float, b: float) -> float:
    return (safe_float(a) / safe_float(b) * 100.0) if safe_float(b) else 0.0

def growth_pct(cur: float, base: float) -> float:
    cur = safe_float(cur)
    base = safe_float(base)
    if base <= 0:
        return 100.0 if cur > 0 else 0.0
    return (cur / base - 1.0) * 100.0

def clamp(x: float, low: float, high: float) -> float:
    return max(low, min(high, x))

def daterange(start: date, end: date) -> Iterable[date]:
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)

def sanitize_sheet_name(name: str, used: Optional[set] = None) -> str:
    name = re.sub(r'[:\\/?*\[\]]', '_', str(name))
    name = re.sub(r'\s+', '_', name)
    name = re.sub(r'_+', '_', name).strip('_')
    name = name[:31] if len(name) > 31 else name
    if used is None:
        return name or "Лист"
    base = name or "Лист"
    candidate = base
    i = 2
    while candidate in used:
        suffix = f"_{i}"
        candidate = (base[:31-len(suffix)] + suffix) if len(base)+len(suffix) > 31 else base + suffix
        i += 1
    used.add(candidate)
    return candidate


def style_workbook(path: Path) -> None:
    try:
        wb = load_workbook(path)
        header_fill = PatternFill("solid", fgColor="1F4E78")
        header_font = Font(color="FFFFFF", bold=True)
        for ws in wb.worksheets:
            ws.freeze_panes = "A2"
            max_widths: Dict[int, int] = {}
            for row_idx, row in enumerate(ws.iter_rows(), start=1):
                for col_idx, cell in enumerate(row, start=1):
                    cell.alignment = Alignment(vertical="top", wrap_text=True)
                    if row_idx == 1:
                        cell.fill = header_fill
                        cell.font = header_font
                    else:
                        if isinstance(cell.value, (datetime, date)):
                            cell.number_format = "yyyy-mm-dd"
                        elif isinstance(cell.value, (int, float, np.integer, np.floating)) and not isinstance(cell.value, bool):
                            value = float(cell.value)
                            if value == 0:
                                cell.number_format = "#,##0"
                            elif abs(value) < 1:
                                cell.number_format = "0.00"
                            else:
                                cell.number_format = "#,##0"
                    val = "" if cell.value is None else str(cell.value)
                    width = min(max(len(val) + 2, 10), 42)
                    max_widths[col_idx] = max(max_widths.get(col_idx, 0), width)
            for col_idx, width in max_widths.items():
                ws.column_dimensions[get_column_letter(col_idx)].width = width
            ws.row_dimensions[1].height = 34
        wb.save(path)
    except Exception:
        pass

class BaseProvider:
    def read_excel(self, key: str, sheet_name: Any = 0) -> pd.DataFrame:
        raise NotImplementedError
    def read_excel_all_sheets(self, key: str) -> Dict[str, pd.DataFrame]:
        raise NotImplementedError
    def write_excel(self, key: str, sheets: Dict[str, pd.DataFrame]) -> None:
        raise NotImplementedError
    def read_text(self, key: str) -> str:
        raise NotImplementedError
    def write_text(self, key: str, text: str) -> None:
        raise NotImplementedError
    def file_exists(self, key: str) -> bool:
        raise NotImplementedError
    def list_keys(self, prefix: str) -> List[str]:
        raise NotImplementedError

class S3Provider(BaseProvider):
    def __init__(self, access_key: str, secret_key: str, bucket_name: str):
        self.bucket = bucket_name
        self.s3 = boto3.client(
            "s3",
            endpoint_url="https://storage.yandexcloud.net",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="ru-central1",
            config=BotoConfig(signature_version="s3v4", read_timeout=300, connect_timeout=60, retries={"max_attempts": 5}),
        )
    def read_bytes(self, key: str) -> bytes:
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        return obj["Body"].read()
    def read_excel(self, key: str, sheet_name: Any = 0) -> pd.DataFrame:
        return pd.read_excel(io.BytesIO(self.read_bytes(key)), sheet_name=sheet_name)
    def read_excel_all_sheets(self, key: str) -> Dict[str, pd.DataFrame]:
        data = self.read_bytes(key)
        xls = pd.ExcelFile(io.BytesIO(data))
        return {sh: pd.read_excel(io.BytesIO(data), sheet_name=sh) for sh in xls.sheet_names}
    def write_excel(self, key: str, sheets: Dict[str, pd.DataFrame]) -> None:
        tmp = Path("/tmp") / f"{int(time.time()*1000)}.xlsx"
        with pd.ExcelWriter(tmp, engine="openpyxl") as writer:
            for sh, df in sheets.items():
                (df if df is not None else pd.DataFrame()).to_excel(writer, sheet_name=sanitize_sheet_name(sh), index=False)
        style_workbook(tmp)
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=tmp.read_bytes())
        tmp.unlink(missing_ok=True)
    def read_text(self, key: str) -> str:
        return self.read_bytes(key).decode("utf-8")
    def write_text(self, key: str, text: str) -> None:
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=text.encode("utf-8"))
    def file_exists(self, key: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False
    def list_keys(self, prefix: str) -> List[str]:
        out: List[str] = []
        token = None
        while True:
            kwargs = {"Bucket": self.bucket, "Prefix": prefix, "MaxKeys": 1000}
            if token:
                kwargs["ContinuationToken"] = token
            resp = self.s3.list_objects_v2(**kwargs)
            out.extend([x["Key"] for x in resp.get("Contents", [])])
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        return out

class LocalProvider(BaseProvider):
    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)
    def _search(self, patterns: List[str]) -> List[Path]:
        out = []
        for child in self.base_dir.iterdir():
            if child.is_file():
                for p in patterns:
                    if re.search(p, child.name, flags=re.I):
                        out.append(child)
                        break
        return sorted(out)
    def _resolve(self, key: str) -> Path:
        p = Path(key)
        if p.exists():
            return p
        mappings = [
            (ADS_ANALYSIS_KEY, [r"^Анализ рекламы.*\.xlsx$"]),
            (ECONOMICS_KEY, [r"^Экономика.*\.xlsx$"]),
            (FUNNEL_KEY, [r"^Воронка продаж.*\.xlsx$"]),
            (OUT_BID_HISTORY, [r"^История_ставок.*\.xlsx$", r"^bid_history.*\.xlsx$"]),
            (OUT_PREVIEW, [r"^Предпросмотр_последнего_запуска.*\.xlsx$", r"^preview_last_run.*\.xlsx$"]),
            (OUT_SINGLE_REPORT, [r"^Итог_последнего_запуска.*\.xlsx$", r"^Предпросмотр_последнего_запуска.*\.xlsx$", r"^preview_last_run.*\.xlsx$"]),
            (OUT_SUMMARY, [r"^Сводка_последнего_запуска.*\.json$", r"^last_run_summary.*\.json$"]),
            (OUT_ARCHIVE, [r"^Архив_решений.*\.xlsx$", r"^decision_archive.*\.xlsx$"]),
        ]
        for logical, pats in mappings:
            if key == logical:
                found = self._search(pats)
                if found:
                    return found[0]
        return self.base_dir / Path(key).name
    def read_excel(self, key: str, sheet_name: Any = 0) -> pd.DataFrame:
        return pd.read_excel(self._resolve(key), sheet_name=sheet_name)
    def read_excel_all_sheets(self, key: str) -> Dict[str, pd.DataFrame]:
        path = self._resolve(key)
        xls = pd.ExcelFile(path)
        return {sh: pd.read_excel(path, sheet_name=sh) for sh in xls.sheet_names}
    def write_excel(self, key: str, sheets: Dict[str, pd.DataFrame]) -> None:
        path = self._resolve(key)
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            for sh, df in sheets.items():
                (df if df is not None else pd.DataFrame()).to_excel(writer, sheet_name=sanitize_sheet_name(sh), index=False)
        style_workbook(path)
    def read_text(self, key: str) -> str:
        return self._resolve(key).read_text(encoding="utf-8")
    def write_text(self, key: str, text: str) -> None:
        self._resolve(key).write_text(text, encoding="utf-8")
    def file_exists(self, key: str) -> bool:
        return self._resolve(key).exists()
    def list_keys(self, prefix: str) -> List[str]:
        if prefix == ORDERS_WEEKLY_PREFIX:
            return [str(p) for p in self._search([r"^Заказы_\d{4}-W\d{2}.*\.xlsx$"])]
        if prefix == KEYWORDS_WEEKLY_PREFIX:
            return [str(p) for p in self._search([r"^Неделя .*\.xlsx$", r"^W\d+.*\.xlsx$"])]
        if prefix == ABC_PREFIX:
            return [str(p) for p in self._search([r"^wb_abc_report_goods__.*\.xlsx$", r"^wb_dynamics__.*\.xlsx$"])]
        if prefix == DYNAMICS_PREFIX:
            return [str(p) for p in self._search([r"^wb_dynamics__.*\.xlsx$"])]
        return []

@dataclass
class Config:
    comfort_drr_min: float = 0.10
    comfort_drr_max: float = 0.12
    max_drr: float = 0.15
    max_up_step: float = 0.08
    test_up_step: float = 0.05
    down_step: float = 0.08

def compute_analysis_window(as_of_date: date) -> Dict[str, date]:
    cur_end = as_of_date - timedelta(days=MATURE_END_OFFSET)
    cur_start = cur_end - timedelta(days=WINDOW_LEN-1)
    base_end = cur_start - timedelta(days=1)
    base_start = base_end - timedelta(days=WINDOW_LEN-1)
    return {"cur_start": cur_start, "cur_end": cur_end, "base_start": base_start, "base_end": base_end}

def parse_date_col(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.date

def choose_provider(local_data_dir: str = "") -> BaseProvider:
    if local_data_dir:
        return LocalProvider(local_data_dir)
    access = os.getenv("YC_ACCESS_KEY_ID", "")
    secret = os.getenv("YC_SECRET_ACCESS_KEY", "")
    bucket = os.getenv("YC_BUCKET_NAME", "")
    if not (access and secret and bucket):
        raise RuntimeError("Не заданы YC_ACCESS_KEY_ID / YC_SECRET_ACCESS_KEY / YC_BUCKET_NAME")
    return S3Provider(access, secret, bucket)


def load_ads(provider: BaseProvider) -> Tuple[pd.DataFrame, pd.DataFrame]:
    sheets = provider.read_excel_all_sheets(ADS_ANALYSIS_KEY)
    daily = sheets.get("Статистика_Ежедневно", pd.DataFrame()).copy()
    campaigns = sheets.get("Список_кампаний", pd.DataFrame()).copy()
    if daily.empty:
        return daily, campaigns
    daily = daily.rename(columns={
        "ID кампании": "id_campaign",
        "Артикул WB": "nmId",
        "Название предмета": "subject",
        "Дата": "date",
    })
    daily["date"] = parse_date_col(daily["date"])
    for c in ["Показы", "Клики", "Заказы", "Расход", "Сумма заказов", "CTR", "CR", "ДРР"]:
        if c not in daily.columns:
            daily[c] = 0
    daily["Показы"] = daily["Показы"].map(safe_float)
    daily["Клики"] = daily["Клики"].map(safe_float)
    daily["Заказы"] = daily["Заказы"].map(safe_float)
    daily["Расход"] = daily["Расход"].map(safe_float)
    daily["Сумма заказов"] = daily["Сумма заказов"].map(safe_float)
    daily["subject_norm"] = daily["subject"].map(canonical_subject)
    daily = daily[daily["subject_norm"].isin(TARGET_SUBJECTS)].copy()

    if not campaigns.empty:
        campaigns = campaigns.rename(columns={"ID кампании": "id_campaign", "Артикул WB": "nmId", "Название предмета": "subject"})
        campaigns["subject_norm"] = campaigns["subject"].map(canonical_subject)
        campaigns = campaigns[campaigns["subject_norm"].isin(TARGET_SUBJECTS)].copy()
        campaigns["payment_type"] = campaigns["Тип оплаты"].astype(str).str.lower().str.strip()
        campaigns["bid_search_rub"] = campaigns.get("Ставка в поиске (руб)", 0).map(safe_float)
        campaigns["bid_reco_rub"] = campaigns.get("Ставка в рекомендациях (руб)", 0).map(safe_float)

        def _placement(r):
            s = safe_float(r["bid_search_rub"])
            rr = safe_float(r["bid_reco_rub"])
            if s > 0 and rr > 0:
                return "combined"
            if s > 0:
                return "search"
            if rr > 0:
                return "recommendation"
            return "search"

        campaigns["placement"] = campaigns.apply(_placement, axis=1)
        campaigns["current_bid_rub"] = campaigns.apply(lambda r: r["bid_search_rub"] if r["placement"] in {"search", "combined"} else r["bid_reco_rub"], axis=1)
        campaigns["campaign_status"] = campaigns.get("Статус", "").astype(str)
        campaigns["campaign_is_active"] = campaigns["campaign_status"].map(is_active_campaign_status)
        campaigns = campaigns[campaigns["campaign_is_active"]].copy()

        if not daily.empty:
            active_pairs = campaigns[["id_campaign", "nmId"]].drop_duplicates()
            daily = daily.merge(active_pairs, on=["id_campaign", "nmId"], how="inner")
    return daily, campaigns
    daily = daily.rename(columns={
        "ID кампании": "id_campaign",
        "Артикул WB": "nmId",
        "Название предмета": "subject",
        "Дата": "date",
    })
    daily["date"] = parse_date_col(daily["date"])
    for c in ["Показы","Клики","Заказы","Расход","Сумма заказов","CTR","CR","ДРР"]:
        if c not in daily.columns:
            daily[c] = 0
    daily["Показы"] = daily["Показы"].map(safe_float)
    daily["Клики"] = daily["Клики"].map(safe_float)
    daily["Заказы"] = daily["Заказы"].map(safe_float)
    daily["Расход"] = daily["Расход"].map(safe_float)
    daily["Сумма заказов"] = daily["Сумма заказов"].map(safe_float)
    daily["subject_norm"] = daily["subject"].map(canonical_subject)
    daily = daily[daily["subject_norm"].isin(TARGET_SUBJECTS)].copy()

    if not campaigns.empty:
        campaigns = campaigns.rename(columns={"ID кампании":"id_campaign","Артикул WB":"nmId","Название предмета":"subject"})
        campaigns["subject_norm"] = campaigns["subject"].map(canonical_subject)
        campaigns = campaigns[campaigns["subject_norm"].isin(TARGET_SUBJECTS)].copy()
        campaigns["payment_type"] = campaigns["Тип оплаты"].astype(str).str.lower().str.strip()
        campaigns["bid_search_rub"] = campaigns.get("Ставка в поиске (руб)", 0).map(safe_float)
        campaigns["bid_reco_rub"] = campaigns.get("Ставка в рекомендациях (руб)", 0).map(safe_float)
        def _placement(r):
            s = safe_float(r["bid_search_rub"])
            rr = safe_float(r["bid_reco_rub"])
            if s > 0 and rr > 0:
                return "combined"
            if s > 0:
                return "search"
            if rr > 0:
                return "recommendation"
            return "search"
        campaigns["placement"] = campaigns.apply(_placement, axis=1)
        campaigns["current_bid_rub"] = campaigns.apply(lambda r: r["bid_search_rub"] if r["placement"] in {"search","combined"} else r["bid_reco_rub"], axis=1)
        campaigns["campaign_status"] = campaigns.get("Статус", "").astype(str)
    return daily, campaigns


def load_economics(provider: BaseProvider) -> pd.DataFrame:
    df = provider.read_excel(ECONOMICS_KEY, sheet_name="Юнит экономика").copy()
    df = df.rename(columns={"Артикул WB": "nmId", "Артикул продавца": "supplier_article", "Предмет": "subject"})
    df["subject_norm"] = df.get("subject", "").map(canonical_subject)
    df = df[df["subject_norm"].isin(TARGET_SUBJECTS)].copy()
    df["product_root"] = df["supplier_article"].map(product_root_from_supplier_article)

    buyout_col = find_matching_column(df, ECON_BUYOUT_CANDIDATES)
    gp_col = find_matching_column(df, ECON_GP_UNIT_CANDIDATES)
    np_col = find_matching_column(df, ECON_NP_UNIT_CANDIDATES)

    df["buyout_rate"] = df["subject_norm"].map(get_subject_buyout_rate)
    df["gp_unit"] = pd.to_numeric(df[gp_col], errors="coerce").fillna(0.0) if gp_col else pd.Series(0.0, index=df.index)
    df["np_unit"] = pd.to_numeric(df[np_col], errors="coerce").fillna(0.0) if np_col else pd.Series(0.0, index=df.index)
    df["gp_realized"] = df["gp_unit"] * df["buyout_rate"].clip(lower=0, upper=1)

    week_col = find_matching_column(df, ECON_WEEK_CANDIDATES)
    if week_col:
        df["Неделя"] = df[week_col]
    elif "Неделя" not in df.columns:
        df["Неделя"] = np.arange(len(df))
    return df

def load_orders(provider: BaseProvider) -> pd.DataFrame:
    keys = provider.list_keys(ORDERS_WEEKLY_PREFIX)
    frames = []
    for key in keys:
        try:
            df = provider.read_excel(key).copy()
            if df.empty:
                continue
            df = df.rename(columns={"nmID":"nmId"})
            df["date"] = parse_date_col(df["date"])
            df["supplier_article"] = df.get("supplierArticle", "")
            df["subject"] = df.get("subject", "")
            df["subject_norm"] = df["subject"].map(canonical_subject)
            df = df[df["subject_norm"].isin(TARGET_SUBJECTS)].copy()
            df["product_root"] = df["supplier_article"].map(product_root_from_supplier_article)
            df["finishedPrice"] = df.get("finishedPrice", 0).map(safe_float)
            df["isCancel"] = df.get("isCancel", False).fillna(False).astype(bool)
            frames.append(df)
        except Exception:
            continue
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def load_funnel(provider: BaseProvider) -> pd.DataFrame:
    try:
        df = provider.read_excel(FUNNEL_KEY).copy()
    except Exception:
        return pd.DataFrame()
    df = df.rename(columns={"nmID": "nmId", "dt": "date"})
    df["date"] = parse_date_col(df["date"])

    numeric_cols = [
        "openCardCount", "addToCartCount", "ordersCount", "ordersSumRub",
        "buyoutsCount", "buyoutsSumRub", "cancelCount", "cancelSumRub",
        "addToCartConversion", "cartToOrderConversion", "buyoutPercent",
    ]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = df[c].map(safe_float)

    sales_col = find_matching_column(df, FUNNEL_SALES_CANDIDATES)
    if sales_col and sales_col != "ordersSumRub":
        df["ordersSumRub"] = pd.to_numeric(df[sales_col], errors="coerce").fillna(0.0)
    elif "ordersSumRub" not in df.columns:
        df["ordersSumRub"] = 0.0

    df["buyoutPercent"] = resolve_buyout_rate_from_funnel(df, default=0.85)
    return df

def load_keywords(provider: BaseProvider) -> pd.DataFrame:
    keys = provider.list_keys(KEYWORDS_WEEKLY_PREFIX)
    frames = []
    for key in keys:
        try:
            xls = provider.read_excel_all_sheets(key)
            sheet = xls.get("Позиции по Ключам", next(iter(xls.values())))
            df = sheet.copy()
            if df.empty:
                continue
            df = df.rename(columns={
                "Дата":"date",
                "Артикул WB":"nmId",
                "Артикул продавца":"supplier_article",
                "Предмет":"subject",
                "Рейтинг отзывов":"rating_reviews",
                "Рейтинг карточки":"rating_card",
                "Частота запросов":"query_freq",
                "Частота за неделю":"demand_week",
                "Медианная позиция":"median_position",
                "Переходы в карточку":"clicks_to_card",
                "Заказы":"keyword_orders",
                "Конверсия в заказ %":"keyword_conversion",
                "Видимость %":"visibility_pct",
            })
            df["date"] = parse_date_col(df["date"])
            df["subject_norm"] = df["subject"].map(canonical_subject)
            df = df[df["subject_norm"].isin(TARGET_SUBJECTS)].copy()
            df["product_root"] = df["supplier_article"].map(product_root_from_supplier_article)
            for c in ["query_freq","demand_week","median_position","clicks_to_card","keyword_orders","keyword_conversion","visibility_pct","rating_reviews","rating_card"]:
                if c in df.columns:
                    df[c] = df[c].map(safe_float)
            frames.append(df)
        except Exception:
            continue
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def load_bid_history(provider: BaseProvider) -> pd.DataFrame:
    if not provider.file_exists(OUT_BID_HISTORY):
        return pd.DataFrame()
    try:
        df = provider.read_excel(OUT_BID_HISTORY).copy()
    except Exception:
        return pd.DataFrame()
    df = df.rename(columns={"Дата запуска":"run_ts","ID кампании":"id_campaign","Артикул WB":"nmId","Тип кампании":"campaign_type"})
    if df.empty:
        return df

    df["run_ts"] = pd.to_datetime(df["run_ts"], errors="coerce")
    df["date"] = df["run_ts"].dt.normalize().astype("datetime64[ns]")

    search_col = pd.to_numeric(df.get("Ставка поиск, коп", 0), errors="coerce") if "Ставка поиск, коп" in df.columns else pd.Series(0, index=df.index, dtype=float)
    reco_col = pd.to_numeric(df.get("Ставка рекомендации, коп", 0), errors="coerce") if "Ставка рекомендации, коп" in df.columns else pd.Series(0, index=df.index, dtype=float)
    bid_kop = search_col.where(search_col.fillna(0) > 0, reco_col)
    df["bid_rub"] = (bid_kop.fillna(0) / 100.0).astype(float)

    df["id_campaign"] = pd.to_numeric(df.get("id_campaign"), errors="coerce")
    df["nmId"] = pd.to_numeric(df.get("nmId"), errors="coerce")
    df = df.dropna(subset=["run_ts", "date", "id_campaign", "nmId"]).copy()
    df["id_campaign"] = df["id_campaign"].astype("int64")
    df["nmId"] = df["nmId"].astype("int64")
    return df

def build_master(econ: pd.DataFrame, orders: pd.DataFrame, keywords: pd.DataFrame, campaigns: pd.DataFrame) -> pd.DataFrame:
    frames = []
    if not econ.empty:
        frames.append(econ[["nmId","supplier_article","product_root","subject","subject_norm","buyout_rate","gp_realized"]].copy())
    if not orders.empty:
        t = orders[["nmId","supplier_article","product_root","subject","subject_norm"]].copy()
        frames.append(t)
    if not keywords.empty:
        t = keywords[["nmId","supplier_article","product_root","subject","subject_norm","rating_reviews","rating_card"]].copy()
        frames.append(t)
    if not campaigns.empty:
        nm_map = campaigns[["id_campaign","nmId","subject","subject_norm"]].copy()
        frames.append(nm_map.rename(columns={"id_campaign":"_drop"}).drop(columns=["_drop"]))
    if not frames:
        return pd.DataFrame(columns=["nmId","supplier_article","product_root","subject","subject_norm","buyout_rate","gp_realized","rating_reviews","rating_card"])
    master = pd.concat(frames, ignore_index=True, sort=False)
    def first_non_empty(s):
        for v in s:
            if pd.notna(v) and str(v) != "":
                return v
        return None
    agg = master.groupby("nmId", as_index=False).agg({
        "supplier_article": first_non_empty,
        "product_root": first_non_empty,
        "subject": first_non_empty,
        "subject_norm": first_non_empty,
        "buyout_rate": "max",
        "gp_realized": "max",
        "rating_reviews": "max",
        "rating_card": "max",
    })
    agg["product_root"] = agg["product_root"].fillna(agg["supplier_article"].map(product_root_from_supplier_article))
    return agg

def aggregate_orders(orders: pd.DataFrame, start: date, end: date, control_field: str) -> pd.DataFrame:
    if orders.empty:
        return pd.DataFrame(columns=[control_field, "total_orders", "total_revenue", "total_orders_raw"])
    df = orders[(orders["date"] >= start) & (orders["date"] <= end) & (~orders["isCancel"])].copy()
    if df.empty:
        return pd.DataFrame(columns=[control_field, "total_orders", "total_revenue", "total_orders_raw"])
    out = df.groupby(control_field, as_index=False).agg(
        total_orders=("nmId", "count"),
        total_revenue=("finishedPrice", "sum"),
    )
    return out

def aggregate_ads_control(ads_daily: pd.DataFrame, start: date, end: date, mapping: pd.DataFrame, control_field: str) -> pd.DataFrame:
    if ads_daily.empty:
        return pd.DataFrame(columns=[control_field, "ad_spend", "ad_clicks", "ad_orders", "ad_impressions", "ad_revenue"])
    df = ads_daily[(ads_daily["date"] >= start) & (ads_daily["date"] <= end)].copy()
    if df.empty:
        return pd.DataFrame(columns=[control_field, "ad_spend", "ad_clicks", "ad_orders", "ad_impressions", "ad_revenue"])
    df = df.merge(mapping[["nmId", control_field]].drop_duplicates(), on="nmId", how="left")
    df = df[df[control_field].notna()].copy()
    if df.empty:
        return pd.DataFrame(columns=[control_field, "ad_spend", "ad_clicks", "ad_orders", "ad_impressions", "ad_revenue"])
    return df.groupby(control_field, as_index=False).agg(
        ad_spend=("Расход", "sum"),
        ad_clicks=("Клики", "sum"),
        ad_orders=("Заказы", "sum"),
        ad_impressions=("Показы", "sum"),
        ad_revenue=("Сумма заказов", "sum"),
    )

def aggregate_keyword_item(keywords: pd.DataFrame, start: date, end: date) -> pd.DataFrame:
    if keywords.empty:
        return pd.DataFrame(columns=["nmId","supplier_article","demand_week","median_position","visibility_pct","rating_reviews","rating_card"])
    df = keywords[(keywords["date"] >= start) & (keywords["date"] <= end)].copy()
    if df.empty:
        return pd.DataFrame(columns=["nmId","supplier_article","demand_week","median_position","visibility_pct","rating_reviews","rating_card"])
    return df.groupby(["nmId","supplier_article"], as_index=False).agg(
        demand_week=("demand_week", "sum"),
        median_position=("median_position", "median"),
        visibility_pct=("visibility_pct", "mean"),
        rating_reviews=("rating_reviews", "max"),
        rating_card=("rating_card", "max"),
        keyword_orders=("keyword_orders", "sum"),
        keyword_clicks=("clicks_to_card", "sum"),
    )

def aggregate_keyword_daily(keywords: pd.DataFrame) -> pd.DataFrame:
    if keywords.empty:
        return pd.DataFrame(columns=["date","nmId","supplier_article","demand","median_position","visibility_pct"])
    return keywords.groupby(["date","nmId","supplier_article"], as_index=False).agg(
        demand=("demand_week", "sum"),
        median_position=("median_position", "median"),
        visibility_pct=("visibility_pct", "mean"),
    )

def build_funnel_item(funnel: pd.DataFrame, master: pd.DataFrame, start: date, end: date) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if funnel.empty:
        cols1 = ["nmId","addToCartConversion","cartToOrderConversion","buyoutPercent"]
        cols2 = ["subject_norm","subj_addToCart","subj_cartToOrder"]
        return pd.DataFrame(columns=cols1), pd.DataFrame(columns=cols2)
    df = funnel[(funnel["date"] >= start) & (funnel["date"] <= end)].copy()
    if df.empty:
        cols1 = ["nmId","addToCartConversion","cartToOrderConversion","buyoutPercent"]
        cols2 = ["subject_norm","subj_addToCart","subj_cartToOrder"]
        return pd.DataFrame(columns=cols1), pd.DataFrame(columns=cols2)
    item = df.groupby("nmId", as_index=False).agg(
        addToCartConversion=("addToCartConversion", "mean"),
        cartToOrderConversion=("cartToOrderConversion", "mean"),
        buyoutPercent=("buyoutPercent", "mean"),
    )
    subj = item.merge(master[["nmId","subject_norm"]].drop_duplicates(), on="nmId", how="left")
    subj = subj.groupby("subject_norm", as_index=False).agg(
        subj_addToCart=("addToCartConversion", "median"),
        subj_cartToOrder=("cartToOrderConversion", "median"),
    )
    return item, subj

def compute_required_growth(blended_drr: float, spend_growth: float, subject_norm: str) -> float:
    sg = max(0.0, safe_float(spend_growth))
    if subject_norm in GROWTH_SUBJECTS:
        if blended_drr <= 0.12:
            return min(max(3.0, sg * 0.40), 15.0)
        if blended_drr <= 0.15:
            return min(max(6.0, sg * 0.60), 20.0)
        return min(max(10.0, sg * 0.80), 25.0)
    else:
        if blended_drr <= 0.12:
            return min(max(3.0, sg * 0.50), 12.0)
        if blended_drr <= 0.15:
            return min(max(6.0, sg * 0.75), 18.0)
        return min(max(10.0, sg * 1.00), 25.0)

def choose_control_key(subject_norm: str, supplier_article: str, product_root: str) -> str:
    supplier_article = str(supplier_article or '').strip()
    if supplier_article:
        return supplier_article
    return str(product_root or '').strip()

def build_subject_benchmarks(rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return pd.DataFrame(columns=["subject_norm","placement","bench_ctr","bench_capture_imp","bench_capture_click"])
    df = rows.copy()
    if "subject_norm" not in df.columns:
        df["subject_norm"] = df.get("subject", "")
    if "placement" not in df.columns:
        df["placement"] = ""
    if "ctr_pct" not in df.columns:
        if "Показы" in df.columns and "Клики" in df.columns:
            df["ctr_pct"] = np.where(pd.to_numeric(df["Показы"], errors="coerce").fillna(0) > 0,
                                     pd.to_numeric(df["Клики"], errors="coerce").fillna(0) / pd.to_numeric(df["Показы"], errors="coerce").fillna(0) * 100.0,
                                     0.0)
        else:
            df["ctr_pct"] = 0.0
    if "capture_imp" not in df.columns:
        if "Показы" in df.columns and "demand_week" in df.columns:
            df["capture_imp"] = np.where(pd.to_numeric(df["demand_week"], errors="coerce").fillna(0) > 0,
                                         pd.to_numeric(df["Показы"], errors="coerce").fillna(0) / pd.to_numeric(df["demand_week"], errors="coerce").fillna(0),
                                         0.0)
        else:
            df["capture_imp"] = 0.0
    if "capture_click" not in df.columns:
        base = "keyword_clicks" if "keyword_clicks" in df.columns else ("demand_week" if "demand_week" in df.columns else None)
        if "Клики" in df.columns and base:
            df["capture_click"] = np.where(pd.to_numeric(df[base], errors="coerce").fillna(0) > 0,
                                           pd.to_numeric(df["Клики"], errors="coerce").fillna(0) / pd.to_numeric(df[base], errors="coerce").fillna(0),
                                           0.0)
        else:
            df["capture_click"] = 0.0
    if "total_orders" not in df.columns:
        df["total_orders"] = pd.to_numeric(df.get("Заказы", 0), errors="coerce").fillna(0.0)
    df["capture_imp"] = pd.to_numeric(df["capture_imp"], errors="coerce").fillna(0.0)
    df["capture_click"] = pd.to_numeric(df["capture_click"], errors="coerce").fillna(0.0)
    df["ctr_pct"] = pd.to_numeric(df["ctr_pct"], errors="coerce").fillna(0.0)
    df["total_orders"] = pd.to_numeric(df["total_orders"], errors="coerce").fillna(0.0)
    eligible = df[df["total_orders"] > 0].copy()
    if eligible.empty:
        eligible = df.copy()
    out = eligible.groupby(["subject_norm","placement"], as_index=False).agg(
        bench_ctr=("ctr_pct","median"),
        bench_capture_imp=("capture_imp","median"),
        bench_capture_click=("capture_click","median"),
    )
    return out

def compute_bid_limits(row: pd.Series, subject_benchmarks: pd.DataFrame) -> Tuple[Optional[float], Optional[float], Optional[float], str]:
    subject_norm = row["subject_norm"]
    gp_realized = safe_float(row.get("gp_realized"))
    payment_type = str(row.get("payment_type","cpm"))
    placement = str(row.get("placement","search"))

    local_clicks = safe_float(row.get("Клики"))
    local_orders = safe_float(row.get("Заказы"))
    inherited_clicks = safe_float(row.get("item_clicks_cur", row.get("control_ad_clicks", 0.0)))
    inherited_orders = safe_float(row.get("total_orders"))

    limit_type = "Нет данных"
    cpo = None
    if local_clicks >= 50 and local_orders >= 3:
        cpo = local_clicks / max(local_orders, 1.0)
        limit_type = "Фактический"
    elif inherited_clicks >= 50 and inherited_orders >= 5:
        cpo = inherited_clicks / max(inherited_orders, 1.0)
        limit_type = "Наследуемый"

    if gp_realized <= 0 or cpo is None or cpo <= 0:
        return None, None, None, limit_type

    comfort_share, max_share = (0.50, 0.80) if subject_norm in GROWTH_SUBJECTS else (0.40, 0.65)
    comfort_cpo = gp_realized * comfort_share
    max_cpo = gp_realized * max_share
    comfort_cpc = comfort_cpo / cpo
    max_cpc = max_cpo / cpo

    if payment_type == "cpc":
        comfort_bid = round(comfort_cpc, 2)
        max_bid = round(max_cpc, 2)
    else:
        ctr = safe_float(row.get("ctr_pct")) / 100.0
        if ctr <= 0:
            bench = subject_benchmarks[
                (subject_benchmarks["subject_norm"] == subject_norm) &
                (subject_benchmarks["placement"] == placement)
            ]
            ctr = safe_float(bench["bench_ctr"].iloc[0]) / 100.0 if not bench.empty else 0.02
        ctr = max(ctr, 0.005)
        comfort_bid = round(comfort_cpc * 1000 * ctr, 2)
        max_bid = round(max_cpc * 1000 * ctr, 2)

    if payment_type == "cpc":
        comfort_bid = clamp(comfort_bid, 4.0, 150.0)
        max_bid = clamp(max_bid, 4.0, 300.0)
    else:
        low = 80.0
        high = 700.0 if placement in {"search","combined"} else 1200.0
        comfort_bid = clamp(comfort_bid, low, high)
        max_bid = clamp(max_bid, low, high)

    experiment_bid = round(max_bid, 2)
    return round(comfort_bid, 2), round(max_bid, 2), round(experiment_bid, 2), limit_type


def get_product_block_key(subject_norm: Any, supplier_article: Any, product_root: Any) -> str:
    subject_norm = canonical_subject(subject_norm)
    supplier_article = str(supplier_article or '').strip()
    product_root = str(product_root or '').strip()
    return product_root if subject_norm in GROWTH_SUBJECTS and product_root else supplier_article

def build_product_block_metrics(rows: pd.DataFrame) -> pd.DataFrame:
    if rows is None or rows.empty:
        return pd.DataFrame(columns=['product_block_key','subject_norm'])
    df = rows.copy()
    if 'product_block_key' not in df.columns:
        df['product_block_key'] = df.apply(lambda r: get_product_block_key(r.get('subject_norm',''), r.get('supplier_article',''), r.get('product_root','')), axis=1)
    for c in ['Показы','Заказы','base_Заказы','Расход','Сумма_заказов','buyout_rate','campaign_gp_cur','campaign_gp_base','campaign_cpo','campaign_drr_cur']:
        if c not in df.columns:
            df[c] = 0.0
        df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0.0)
    df['realized_revenue_cur'] = df['Сумма_заказов'] * df['buyout_rate']
    grp = df.groupby(['product_block_key','subject_norm'], as_index=False).agg(
        block_campaigns=('id_campaign','nunique'),
        block_impressions_cur=('Показы','sum'),
        block_orders_cur=('Заказы','sum'),
        block_orders_base=('base_Заказы','sum'),
        block_spend_cur=('Расход','sum'),
        block_realized_revenue_cur=('realized_revenue_cur','sum'),
        block_gp_cur=('campaign_gp_cur','sum'),
        block_gp_base=('campaign_gp_base','sum'),
    )
    grp['block_drr_cur'] = np.where(grp['block_realized_revenue_cur'] > 0, grp['block_spend_cur'] / grp['block_realized_revenue_cur'], 0.0)
    grp['block_orders_growth_pct'] = np.where(grp['block_orders_base'] > 0, (grp['block_orders_cur'] / grp['block_orders_base'] - 1.0) * 100.0, np.where(grp['block_orders_cur'] > 0, 100.0, 0.0))
    grp['block_gp_growth_pct'] = np.where(grp['block_gp_base'] != 0, (grp['block_gp_cur'] - grp['block_gp_base']) / np.abs(grp['block_gp_base']) * 100.0, np.where(grp['block_gp_cur'] > 0, 100.0, 0.0))

    pos = df[df['campaign_cpo'] > 0].groupby('product_block_key')['campaign_cpo']
    grp['block_avg_cpo'] = grp['product_block_key'].map(pos.mean()).fillna(0.0)
    grp['block_best_cpo'] = grp['product_block_key'].map(pos.min()).fillna(0.0)
    grp['block_median_cpo'] = grp['product_block_key'].map(pos.median()).fillna(0.0)

    posd = df[df['campaign_drr_cur'] > 0].groupby('product_block_key')['campaign_drr_cur']
    grp['block_avg_drr'] = grp['product_block_key'].map(posd.mean()).fillna(0.0)

    rank_df = df.copy()
    max_gp = max(rank_df['campaign_gp_cur'].abs().max(), 1.0) if not rank_df.empty else 1.0
    rank_df['score'] = np.where(rank_df['campaign_cpo'] > 0, 1.0 / rank_df['campaign_cpo'], 0.0) + np.where(rank_df['campaign_gp_cur'] > 0, rank_df['campaign_gp_cur'] / max_gp, 0.0)
    rank_df = rank_df.sort_values(['product_block_key','score','campaign_gp_cur'], ascending=[True, False, False])
    best = rank_df.drop_duplicates('product_block_key', keep='first')[['product_block_key','id_campaign','supplier_article','payment_type','placement']]
    best = best.rename(columns={'id_campaign':'block_best_campaign_id','supplier_article':'block_best_article','payment_type':'block_best_payment_type','placement':'block_best_placement'})
    grp = grp.merge(best, on='product_block_key', how='left')
    return grp

def determine_status_recommendation(row: pd.Series) -> str:
    block_campaigns = safe_int(row.get('block_campaigns'))
    campaign_drr = safe_float(row.get('campaign_drr_cur'))
    campaign_cpo = safe_float(row.get('campaign_cpo'))
    block_avg_cpo = safe_float(row.get('block_avg_cpo'))
    impressions = safe_float(row.get('Показы'))
    supplier_article = str(row.get('supplier_article', '') or '').strip()
    product_root = str(row.get('product_root', '') or '').strip().upper()
    subject_norm = canonical_subject(row.get('subject_norm', row.get('subject', '')))
    best_campaign_id = safe_int(row.get('block_best_campaign_id'))
    current_campaign_id = safe_int(row.get('id_campaign'))

    is_901_block = supplier_article.upper().startswith('901') or product_root.startswith('901') or subject_norm == 'кисти косметические'
    is_best_campaign = best_campaign_id > 0 and current_campaign_id == best_campaign_id
    is_alt_campaign = block_campaigns >= 2 and not is_best_campaign

    pause_candidate = (
        is_alt_campaign and
        not is_901_block and
        impressions >= 10000 and
        campaign_drr >= 0.18 and
        block_avg_cpo > 0 and
        campaign_cpo >= block_avg_cpo * 1.35
    )
    if pause_candidate:
        return 'Кандидат на паузу'
    if is_901_block:
        return 'Не паузить'
    if is_best_campaign:
        return 'Ядро трафика'
    return 'Оставить'

def determine_action(row: pd.Series, cfg: Config) -> Tuple[str, float, str, bool]:
    current_bid = safe_float(row.get('current_bid_rub'))
    max_bid = safe_float(row.get('max_bid_rub'))
    payment_type = canonical_payment_type(row.get('payment_type'))
    min_bid = safe_float(row.get('Минимальная ставка WB, ₽')) if pd.notna(row.get('Минимальная ставка WB, ₽')) else (4.0 if payment_type == 'cpc' else 80.0)
    floor_bid = max(min_bid, 4.0 if payment_type == 'cpc' else 80.0)
    step = get_bid_step_rub(payment_type)
    subject_norm = canonical_subject(row.get('subject_norm', row.get('subject', '')))
    subject_name = get_subject_display_name(subject_norm)

    category_drr = safe_float(row.get('category_drr_cur'))
    category_limit = safe_float(row.get('category_limit_drr')) or get_category_drr_limit(subject_norm)
    category_orders_growth = safe_float(row.get('category_orders_growth_pct'))
    category_gp_growth = safe_float(row.get('category_gp_growth_pct'))
    category_demand_growth = safe_float(row.get('category_demand_growth_pct'))
    category_plan_att = safe_float(row.get('category_plan_attainment_pct', row.get('plan_attainment_pct', 100.0)))

    product_block_key = str(row.get('product_block_key') or row.get('supplier_article') or '').strip()
    block_campaigns = safe_int(row.get('block_campaigns'))
    block_avg_cpo = safe_float(row.get('block_avg_cpo'))
    block_best_cpo = safe_float(row.get('block_best_cpo'))
    block_drr = safe_float(row.get('block_drr_cur'))
    block_orders_growth = safe_float(row.get('block_orders_growth_pct'))
    block_gp_growth = safe_float(row.get('block_gp_growth_pct'))

    campaign_drr = safe_float(row.get('campaign_drr_cur'))
    campaign_gp = safe_float(row.get('campaign_gp_cur'))
    campaign_gp_growth = safe_float(row.get('campaign_gp_growth_pct'))
    campaign_orders_growth = safe_float(row.get('campaign_order_growth_pct'))
    campaign_click_growth = safe_float(row.get('campaign_click_growth_pct'))
    campaign_impression_growth = safe_float(row.get('campaign_impression_growth_pct'))
    campaign_roi = safe_float(row.get('campaign_roi_cur'))
    campaign_cpo = safe_float(row.get('campaign_cpo'))
    impressions_3d = safe_float(row.get('campaign_impressions_3d'))
    position_3d = safe_float(row.get('median_position_3d', row.get('median_position')))

    item_gp_growth = safe_float(row.get('item_gp_growth_pct'))
    item_orders_growth = safe_float(row.get('item_order_growth_pct'))
    better_channel = str(row.get('better_channel', '') or '').strip().upper()
    row_channel = 'CPC' if payment_type == 'cpc' else 'CPM'
    campaign_is_active = bool(row.get('campaign_is_active', False))
    supplier_article = str(row.get('supplier_article', '') or '').strip()

    can_raise_more = (max_bid <= 0) or (current_bid + step <= max_bid + 1e-9)
    rate_limit = max_bid > 0 and current_bid >= max_bid - 1e-9
    positive_traffic = campaign_impression_growth > 0 or campaign_click_growth > 0
    positive_orders = campaign_orders_growth > 0
    positive_gp = campaign_gp_growth > 0
    trend_confirmed = positive_traffic and positive_orders and positive_gp
    doubtful_trend = (positive_traffic and not positive_orders) or (positive_orders and not positive_gp)
    demand_explains_drop = is_drop_explained_by_demand(category_orders_growth, category_demand_growth)

    cpo_vs_avg = (campaign_cpo / block_avg_cpo) if block_avg_cpo > 0 and campaign_cpo > 0 else 0.0
    cpo_vs_best = (campaign_cpo / block_best_cpo) if block_best_cpo > 0 and campaign_cpo > 0 else 0.0
    is_block_leader = block_campaigns >= 2 and campaign_cpo > 0 and block_best_cpo > 0 and cpo_vs_best <= 1.15
    is_block_weak = block_campaigns >= 2 and block_avg_cpo > 0 and campaign_cpo > 0 and cpo_vs_avg >= 1.20
    best_campaign_id = safe_int(row.get('block_best_campaign_id'))
    current_campaign_id = safe_int(row.get('id_campaign'))
    is_best_campaign = best_campaign_id > 0 and current_campaign_id == best_campaign_id
    is_alt_campaign = block_campaigns >= 2 and not is_best_campaign
    product_root = str(row.get('product_root', '') or '').strip().upper()
    is_901_block = supplier_article.upper().startswith('901') or product_root.startswith('901') or subject_norm == 'кисти косметические'
    pause_candidate = is_alt_campaign and (not is_901_block) and safe_float(row.get('Показы')) >= 10000 and campaign_drr >= 0.18 and block_avg_cpo > 0 and cpo_vs_avg >= 1.35
    low_impressions_probe = impressions_3d < 1000 and can_raise_more and (position_3d <= 0 or position_3d > 20) and campaign_drr <= max(category_limit, 0.12)

    def raise_bid() -> float:
        return apply_bid_step(current_bid, payment_type, 'up', floor_bid, max_bid)

    def lower_bid() -> float:
        return apply_bid_step(current_bid, payment_type, 'down', floor_bid, max_bid)

    level1 = f"Уровень 1 Категория: {subject_name}; план {category_plan_att:.0f}%, ДРР {category_drr*100:.1f}% при лимите {category_limit*100:.1f}%, заказы {category_orders_growth:.1f}%, ВП {category_gp_growth:.1f}%, спрос {category_demand_growth:.1f}%"
    level2 = f"Уровень 2 Товарный блок: {product_block_key}; кампаний {block_campaigns}, ДРР блока {block_drr*100:.1f}%, лучший CPO {block_best_cpo:.0f} ₽, средний CPO {block_avg_cpo:.0f} ₽, рост заказов {block_orders_growth:.1f}%, рост ВП {block_gp_growth:.1f}%"
    level3 = f"Уровень 3 Кампания: ДРР {campaign_drr*100:.1f}%, CPO {campaign_cpo:.0f} ₽, ВП {campaign_gp:.0f} ₽, рост показов {campaign_impression_growth:.1f}%, кликов {campaign_click_growth:.1f}%, заказов {campaign_orders_growth:.1f}%, ВП {campaign_gp_growth:.1f}%, показы 3д {impressions_3d:.0f}, позиция 3д {position_3d:.1f}"

    if not campaign_is_active:
        return 'Без изменений', round(current_bid, 2), f"{level1}; {level2}; {level3}; кампания не активна — ставку не меняем", False

    if supplier_article in {'901_/6', '901/6'} and campaign_roi < cfg.roi_9016_target and current_bid > floor_bid:
        return 'Снизить', lower_bid(), f"{level1}; {level2}; {level3}; ROI 901/6 ниже целевого {cfg.roi_9016_target*100:.0f}%", True

    if category_drr > category_limit:
        if campaign_drr > 0.10 and current_bid > floor_bid:
            return 'Снизить', lower_bid(), f"{level1}; {level2}; {level3}; категория выше лимита, эта кампания тоже >10% ДРР — снижаем на 1 шаг ({step:.0f} ₽)", True
        return 'Без изменений', round(current_bid, 2), f"{level1}; {level2}; {level3}; категория выше лимита, но у кампании ДРР <= 10% — не режем автоматически", False

    if pause_candidate:
        return 'Без изменений', round(current_bid, 2), f"{level1}; {level2}; {level3}; это альтернативная кампания внутри блока, лидер остаётся активным; >10 000 показов, CPO сильно выше среднего по блоку и ДРР > 18% — кандидат на паузу", True

    if campaign_gp <= 0 and current_bid > floor_bid:
        return 'Снизить', lower_bid(), f"{level1}; {level2}; {level3}; кампания убыточна — снижаем на 1 шаг ({step:.0f} ₽)", True

    if category_orders_growth < 0 and not demand_explains_drop and category_drr <= category_limit:
        if campaign_drr <= 0.10 and campaign_gp > 0 and can_raise_more:
            return 'Повысить', raise_bid(), f"{level1}; {level2}; {level3}; заказы категории падают быстрее спроса — усиливаем эффективную кампанию <=10% ДРР на 1 шаг ({step:.0f} ₽)", False

    if category_orders_growth > 0 and category_gp_growth < 0:
        if campaign_drr > 0.10 and current_bid > floor_bid:
            return 'Снизить', lower_bid(), f"{level1}; {level2}; {level3}; заказы категории растут, а ВП падает — режем кампанию >10% ДРР на 1 шаг ({step:.0f} ₽)", True

    if low_impressions_probe and campaign_gp >= 0:
        return 'Повысить', raise_bid(), f"{level1}; {level2}; {level3}; за 3 дня меньше 1000 показов и позиция слабая — аккуратно пробуем растить трафик на 1 шаг ({step:.0f} ₽)", False

    if block_campaigns >= 2 and is_block_leader and campaign_drr <= max(category_limit, 0.12) and campaign_gp > 0 and can_raise_more:
        return 'Повысить', raise_bid(), f"{level1}; {level2}; {level3}; внутри товарного блока это одна из лучших кампаний по CPO — отдаём ей больше трафика", False

    if block_campaigns >= 2 and is_block_weak and campaign_drr > 0.10 and current_bid > floor_bid:
        if campaign_gp_growth <= 0 or campaign_orders_growth <= 0:
            return 'Снизить', lower_bid(), f"{level1}; {level2}; {level3}; внутри товарного блока CPO выше среднего, а рост не подтверждён — сушим кампанию на 1 шаг ({step:.0f} ₽)", True

    if campaign_drr <= category_limit and campaign_gp > 0:
        if doubtful_trend:
            return 'Без изменений', round(current_bid, 2), f"{level1}; {level2}; {level3}; трафик меняется, но заказ/ВП не подтверждены — ждём 3 дня", False
        if trend_confirmed and can_raise_more:
            return 'Повысить', raise_bid(), f"{level1}; {level2}; {level3}; кампания прибыльная и растёт — повышаем на 1 шаг ({step:.0f} ₽)", False
        if category_plan_att < 95 and item_gp_growth <= 0 and can_raise_more:
            return 'Повысить', raise_bid(), f"{level1}; {level2}; {level3}; отстаём от плана, ДРР в норме, прибыль товара не растёт — повышаем на 1 шаг ({step:.0f} ₽)", False
        if better_channel and better_channel != row_channel:
            return 'Без изменений', round(current_bid, 2), f"{level1}; {level2}; {level3}; альтернативный канал сильнее, но эта кампания прибыльная — не режем автоматически", False
        return 'Без изменений', round(current_bid, 2), f"{level1}; {level2}; {level3}; недостаточно сигнала для изменения", rate_limit

    if campaign_drr > category_limit:
        if (campaign_gp_growth <= 0 or campaign_orders_growth <= 0) and current_bid > floor_bid:
            return 'Снизить', lower_bid(), f"{level1}; {level2}; {level3}; ДРР кампании выше верхнего значения и рост не подтверждён — снижаем на 1 шаг ({step:.0f} ₽)", True
        return 'Без изменений', round(current_bid, 2), f"{level1}; {level2}; {level3}; ДРР кампании выше верхнего значения, но кампания пока прибыльна — ждём подтверждение", False

    if rate_limit:
        return 'Предел эффективности ставки', round(current_bid, 2), f"{level1}; {level2}; {level3}; ставка упёрлась в расчётный максимум", True

    return 'Без изменений', round(current_bid, 2), f"{level1}; {level2}; {level3}; без изменений", False

def prepare_metrics(provider: BaseProvider, cfg: Config, as_of_date: date) -> Dict[str, Any]:

    window = compute_analysis_window(as_of_date)
    log(f"📅 Анализируем зрелое окно {window['cur_start']} .. {window['cur_end']}; база сравнения {window['base_start']} .. {window['base_end']}")
    ads_daily, campaigns = load_ads(provider)
    econ = load_economics(provider)
    orders = load_orders(provider)
    funnel = load_funnel(provider)
    keywords = load_keywords(provider)
    globals()['keywords_global_for_history'] = keywords.copy()
    bid_history = load_bid_history(provider)
    master = build_master(econ, orders, keywords, campaigns)
    log(f"📣 Реклама: {len(ads_daily):,} строк; кампании: {campaigns['id_campaign'].nunique() if not campaigns.empty else 0}; placement-строк: {len(campaigns):,}")
    plan_df, category_plan = build_previous_month_plan(orders, funnel, ads_daily, keywords, econ, as_of_date, master)
    log(f"💰 Экономика: {len(econ):,} SKU; Заказы: {len(orders):,} строк; Воронка: {len(funnel):,}; Keywords: {len(keywords):,}; План строк: {0 if 'Комментарий' in plan_df.columns else len(plan_df):,}")

    daily_item = build_daily_item_history(orders, ads_daily, funnel, econ, master)
    daily_campaign = build_daily_campaign_history(ads_daily, campaigns, funnel, econ, master)

    econ_latest = latest_econ_rows(econ, ['nmId','supplier_article','product_root','subject','subject_norm','buyout_rate','gp_realized','np_unit']).copy() if not econ.empty else pd.DataFrame(columns=['nmId','supplier_article','product_root','subject','subject_norm','buyout_rate','gp_realized','np_unit'])
    category_diag = build_category_window_diagnostics(orders, ads_daily, funnel, keywords, master, econ_latest, window)
    keywords_current = aggregate_keyword_item(keywords, window['cur_start'], window['cur_end'])
    funnel_item, funnel_subject = build_funnel_item(funnel, master, window['cur_start'], window['cur_end'])

    campaigns_base = campaigns[['id_campaign','nmId','placement','payment_type','current_bid_rub','campaign_status']].drop_duplicates().merge(master[['nmId','supplier_article','product_root','subject','subject_norm','rating_reviews','rating_card']].drop_duplicates(), on='nmId', how='left').merge(econ_latest[['nmId','buyout_rate','gp_realized','np_unit']], on='nmId', how='left').drop_duplicates(['id_campaign','nmId','placement','payment_type'])
    campaigns_base['buyout_rate'] = campaigns_base['subject_norm'].map(get_subject_buyout_rate)

    cur = ads_daily[(ads_daily['date'] >= window['cur_start']) & (ads_daily['date'] <= window['cur_end'])].groupby(['id_campaign','nmId'], as_index=False).agg(
        Показы=('Показы','sum'), Клики=('Клики','sum'), Заказы=('Заказы','sum'), Расход=('Расход','sum'), Сумма_заказов=('Сумма заказов','sum')
    ) if not ads_daily.empty else pd.DataFrame(columns=['id_campaign','nmId','Показы','Клики','Заказы','Расход','Сумма_заказов'])
    base = ads_daily[(ads_daily['date'] >= window['base_start']) & (ads_daily['date'] <= window['base_end'])].groupby(['id_campaign','nmId'], as_index=False).agg(
        base_Показы=('Показы','sum'), base_Клики=('Клики','sum'), base_Заказы=('Заказы','sum'), base_Расход=('Расход','sum'), base_Сумма_заказов=('Сумма заказов','sum')
    ) if not ads_daily.empty else pd.DataFrame(columns=['id_campaign','nmId','base_Показы','base_Клики','base_Заказы','base_Расход','base_Сумма_заказов'])

    rows = campaigns_base.merge(cur, on=['id_campaign','nmId'], how='left').merge(base, on=['id_campaign','nmId'], how='left').fillna(0)
    rows = ensure_business_keys(rows)
    rows['supplier_article'] = series_or_default(rows, 'supplier_article', '').fillna('').astype(str)
    rows['subject'] = series_or_default(rows, 'subject', series_or_default(rows, 'subject_norm', '')).fillna('').astype(str)
    rows['subject_norm'] = series_or_default(rows, 'subject_norm', rows['subject'].map(canonical_subject)).fillna('').astype(str).map(canonical_subject)
    rows['campaign_status'] = series_or_default(rows, 'campaign_status', '').fillna('').astype(str)
    rows['campaign_is_active'] = rows['campaign_status'].map(is_active_campaign_status)

    rows = rows.merge(keywords_current[['nmId','supplier_article','demand_week','median_position','visibility_pct','keyword_orders','keyword_clicks']].drop_duplicates(), on=['nmId','supplier_article'], how='left')
    rows = rows.merge(funnel_item[['nmId','addToCartConversion','cartToOrderConversion','buyoutPercent']], on='nmId', how='left')

    if not daily_item.empty and 'Комментарий' not in daily_item.columns:
        daily_item = ensure_business_keys(daily_item)
        item_metrics = build_item_current_metrics(daily_item, window['cur_start'], window['cur_end'], window['base_start'], window['base_end'])
        item_metrics = ensure_business_keys(item_metrics)
        if 'Артикул продавца' in rows.columns and 'Артикул продавца' in item_metrics.columns:
            rows = rows.merge(item_metrics, on='Артикул продавца', how='left')
        elif 'supplier_article' in rows.columns and 'supplier_article' in item_metrics.columns:
            rows = rows.merge(item_metrics, on='supplier_article', how='left')

    if not plan_df.empty and 'Комментарий' not in plan_df.columns:
        plan_df = ensure_business_keys(plan_df)
        plan_cols = [c for c in ['Артикул WB','Артикул продавца','План ВП MTD, ₽','Факт ВП MTD, ₽','Темп плана ВП, %','Причина плана'] if c in plan_df.columns]
        rows = rows.merge(plan_df[plan_cols].drop_duplicates(), on=[c for c in ['Артикул WB','Артикул продавца'] if c in plan_cols], how='left')
    else:
        rows['План ВП MTD, ₽'] = 0.0
        rows['Факт ВП MTD, ₽'] = 0.0
        rows['Темп плана ВП, %'] = 0.0
        rows['Причина плана'] = ''

    rows['ctr_pct'] = np.where(rows['Показы'] > 0, rows['Клики'] / rows['Показы'] * 100.0, 0.0)
    rows['campaign_drr_cur'] = np.where(rows['Сумма_заказов'] * rows['buyout_rate'] > 0, rows['Расход'] / (rows['Сумма_заказов'] * rows['buyout_rate']), 0.0)
    rows['campaign_drr_base'] = np.where(rows['base_Сумма_заказов'] * rows['buyout_rate'] > 0, rows['base_Расход'] / (rows['base_Сумма_заказов'] * rows['buyout_rate']), 0.0)
    rows['campaign_gp_cur'] = rows['Заказы'] * rows['gp_realized'] - rows['Расход']
    rows['campaign_gp_base'] = rows['base_Заказы'] * rows['gp_realized'] - rows['base_Расход']
    rows['campaign_gp_growth_pct'] = np.where(rows['campaign_gp_base'] != 0, (rows['campaign_gp_cur'] - rows['campaign_gp_base']) / np.abs(rows['campaign_gp_base']) * 100.0, np.where(rows['campaign_gp_cur'] > 0, 100.0, 0.0))
    rows['campaign_order_growth_pct'] = np.where(rows['base_Заказы'] > 0, (rows['Заказы'] / rows['base_Заказы'] - 1) * 100.0, np.where(rows['Заказы'] > 0, 100.0, 0.0))
    rows['campaign_click_growth_pct'] = np.where(rows['base_Клики'] > 0, (rows['Клики'] / rows['base_Клики'] - 1) * 100.0, np.where(rows['Клики'] > 0, 100.0, 0.0))
    rows['campaign_impression_growth_pct'] = np.where(rows['base_Показы'] > 0, (rows['Показы'] / rows['base_Показы'] - 1) * 100.0, np.where(rows['Показы'] > 0, 100.0, 0.0))
    rows['campaign_cpo'] = np.where(rows['Заказы'] > 0, rows['Расход'] / rows['Заказы'], 0.0)
    rows['campaign_roi_cur'] = np.where(rows['Расход'] > 0, ((rows['Заказы'] * rows['buyout_rate'] * rows['np_unit']) - rows['Расход']) / rows['Расход'], 0.0)
    item_revenue_cur = numeric_series(rows, 'item_revenue_cur', 0.0)
    item_spend_cur = numeric_series(rows, 'item_spend_cur', 0.0)
    item_orders_cur = numeric_series(rows, 'item_orders_cur', 0.0)
    item_order_growth_pct = numeric_series(rows, 'item_order_growth_pct', 0.0)
    base_item_spend_cur = numeric_series(rows, 'base_item_spend_cur', 0.0)
    campaign_orders_cur = numeric_series(rows, 'Заказы', 0.0)
    add_to_cart_conv = numeric_series(rows, 'addToCartConversion', 0.0)
    subj_add_to_cart = numeric_series(rows, 'subj_addToCart', 0.0)
    cart_to_order_conv = numeric_series(rows, 'cartToOrderConversion', 0.0)
    subj_cart_to_order = numeric_series(rows, 'subj_cartToOrder', 0.0)

    rows['blended_drr'] = np.where(item_revenue_cur * rows['buyout_rate'] > 0, item_spend_cur / (item_revenue_cur * rows['buyout_rate']), 0.0)
    rows['total_orders'] = item_orders_cur
    rows['total_revenue'] = item_revenue_cur
    rows['ad_spend'] = item_spend_cur
    rows['ad_orders'] = campaign_orders_cur
    rows['order_growth_pct'] = item_order_growth_pct
    rows['spend_growth_pct'] = np.where(base_item_spend_cur > 0, (item_spend_cur / base_item_spend_cur.replace(0, np.nan) - 1.0) * 100.0, 0.0)
    rows['required_growth_pct'] = rows.apply(lambda r: compute_required_growth(safe_float(r.get('campaign_drr_cur')), safe_float(r.get('spend_growth_pct')), str(r.get('subject_norm',''))), axis=1)
    rows['card_issue'] = ((add_to_cart_conv < 0.5 * subj_add_to_cart) | (cart_to_order_conv < 0.5 * subj_cart_to_order)) if 'subj_addToCart' in rows.columns else False

    if category_plan is not None and not category_plan.empty and 'subject_norm' in category_plan.columns:
        cat_cols = [c for c in ['subject_norm','Факт ВП MTD, ₽','План ВП MTD, ₽','Темп плана ВП, %','Лимит ДРР категории, доля'] if c in category_plan.columns]
        cat_merge = category_plan[cat_cols].drop_duplicates('subject_norm').copy()
        cat_merge = cat_merge.rename(columns={
            'Факт ВП MTD, ₽': 'category_gp_mtd',
            'План ВП MTD, ₽': 'category_gp_plan',
            'Темп плана ВП, %': 'category_plan_attainment_pct',
            'Лимит ДРР категории, доля': 'category_limit_drr_plan',
        })
        rows = rows.merge(cat_merge, on='subject_norm', how='left')
    else:
        rows['category_gp_mtd'] = 0.0
        rows['category_gp_plan'] = 0.0
        rows['category_plan_attainment_pct'] = 100.0
        rows['category_limit_drr_plan'] = np.nan

    if category_diag is not None and not category_diag.empty:
        rows = rows.merge(category_diag[['subject_norm','category_drr_cur','category_gp_cur','category_gp_base','category_orders_cur','category_orders_base','category_orders_growth_pct','category_gp_growth_pct','category_demand_cur','category_demand_base','category_demand_growth_pct','category_limit_drr']], on='subject_norm', how='left')
    else:
        rows['category_drr_cur'] = 0.0
        rows['category_gp_cur'] = 0.0
        rows['category_gp_base'] = 0.0
        rows['category_orders_cur'] = 0.0
        rows['category_orders_base'] = 0.0
        rows['category_orders_growth_pct'] = 0.0
        rows['category_gp_growth_pct'] = 0.0
        rows['category_demand_cur'] = 0.0
        rows['category_demand_base'] = 0.0
        rows['category_demand_growth_pct'] = 0.0
        rows['category_limit_drr'] = rows['subject_norm'].map(get_category_drr_limit)

    rows['category_limit_drr'] = numeric_series(rows, 'category_limit_drr', np.nan).fillna(rows['subject_norm'].map(get_category_drr_limit))
    _category_gp_plan = numeric_series(rows, 'category_gp_plan', 0.0)
    _category_gp_mtd = numeric_series(rows, 'category_gp_mtd', 0.0)
    _category_plan_att_default = pd.Series(
        np.where(_category_gp_plan > 0, _category_gp_mtd / _category_gp_plan.replace(0, np.nan).fillna(1.0) * 100.0, 100.0),
        index=rows.index,
    )
    rows['category_plan_attainment_pct'] = numeric_series(rows, 'category_plan_attainment_pct', np.nan).fillna(_category_plan_att_default)

    category_plan = category_diag.copy() if category_diag is not None else pd.DataFrame(columns=['subject_norm'])
    if category_plan is not None and not category_plan.empty:
        category_plan['Категория'] = category_plan['subject_norm'].map(get_subject_display_name)
        category_plan['Лимит ДРР категории, доля'] = category_plan['category_limit_drr']
        category_plan['Лимит ДРР категории, %'] = category_plan['category_limit_drr'] * 100.0
        category_plan['ДРР категории, доля'] = category_plan['category_drr_cur']
        category_plan['ДРР категории, %'] = category_plan['category_drr_cur'] * 100.0
        category_plan['Факт ВП MTD, ₽'] = category_plan['category_gp_cur']
        category_plan['ВП базовое окно, ₽'] = category_plan['category_gp_base']
        category_plan['Темп категории, %'] = category_plan['category_gp_growth_pct']
        category_plan['Заказы окно'] = category_plan['category_orders_cur']
        category_plan['Заказы база'] = category_plan['category_orders_base']
        category_plan['Рост заказов окна, %'] = category_plan['category_orders_growth_pct']
        category_plan['Спрос окно'] = category_plan['category_demand_cur']
        category_plan['Спрос база'] = category_plan['category_demand_base']
        category_plan['Рост спроса окна, %'] = category_plan['category_demand_growth_pct']
        category_plan['Фиксированный % выкупа'] = category_plan['subject_norm'].map(lambda x: get_subject_buyout_rate(x) * 100.0)
        category_plan['Рабочее окно с'] = window['cur_start']
        category_plan['Рабочее окно по'] = window['cur_end']
        category_plan = category_plan[['Категория','subject_norm','Фиксированный % выкупа','Заказы окно','Заказы база','Рост заказов окна, %','Спрос окно','Спрос база','Рост спроса окна, %','Факт ВП MTD, ₽','ВП базовое окно, ₽','Темп категории, %','ДРР категории, доля','ДРР категории, %','Лимит ДРР категории, доля','Лимит ДРР категории, %','Рабочее окно с','Рабочее окно по']]

    rows = normalize_core_columns(rows)
    if 'supplier_article' not in rows.columns:
        rows['supplier_article'] = rows.get('Артикул продавца', '')
    rows['supplier_article'] = rows['supplier_article'].fillna('').astype(str)
    rows = rows.merge(build_channel_balance(ads_daily, campaigns, master, econ_latest, window, funnel), on='supplier_article', how='left')
    for c in ['cpo_cpc','cpo_cpm','drr_cpc','drr_cpm','gp_after_ads_cpc','gp_after_ads_cpm','orders_cpc','orders_cpm']:
        rows[c] = numeric_series(rows, c, 0.0)
    rows['plan_attainment_pct'] = numeric_series(rows, 'Темп плана ВП, %', 0.0)
    rows['product_block_key'] = rows.apply(lambda r: get_product_block_key(r.get('subject_norm',''), r.get('supplier_article',''), r.get('product_root','')), axis=1)

    recent_start = max(window['cur_start'], window['cur_end'] - timedelta(days=2))
    recent_ads = ads_daily[(ads_daily['date'] >= recent_start) & (ads_daily['date'] <= window['cur_end'])].groupby(['id_campaign','nmId'], as_index=False).agg(
        campaign_impressions_3d=('Показы','sum'),
        campaign_clicks_3d=('Клики','sum'),
        campaign_orders_3d=('Заказы','sum'),
    ) if not ads_daily.empty else pd.DataFrame(columns=['id_campaign','nmId','campaign_impressions_3d','campaign_clicks_3d','campaign_orders_3d'])
    rows = rows.merge(recent_ads, on=['id_campaign','nmId'], how='left')
    recent_kw = keywords[(keywords['date'] >= recent_start) & (keywords['date'] <= window['cur_end'])].groupby(['nmId','supplier_article'], as_index=False).agg(
        median_position_3d=('median_position','median'),
        visibility_3d=('visibility_pct','mean'),
        query_freq_3d=('query_freq','sum'),
    ) if not keywords.empty else pd.DataFrame(columns=['nmId','supplier_article','median_position_3d','visibility_3d','query_freq_3d'])
    if not recent_kw.empty:
        rows = rows.merge(recent_kw, on=['nmId','supplier_article'], how='left')
    block_report = build_product_block_metrics(rows)
    rows = rows.merge(block_report, on=['product_block_key','subject_norm'], how='left')
    rows['campaign_cpo_to_block_avg_x'] = np.where(numeric_series(rows, 'block_avg_cpo', 0.0) > 0, numeric_series(rows, 'campaign_cpo', 0.0) / numeric_series(rows, 'block_avg_cpo', 1.0), 0.0)
    rows['campaign_cpo_to_block_best_x'] = np.where(numeric_series(rows, 'block_best_cpo', 0.0) > 0, numeric_series(rows, 'campaign_cpo', 0.0) / numeric_series(rows, 'block_best_cpo', 1.0), 0.0)
    rows['status_recommendation'] = rows.apply(determine_status_recommendation, axis=1)

    subject_benchmarks = build_subject_benchmarks(rows)
    rows = rows.merge(subject_benchmarks, on=['subject_norm','placement'], how='left')
    rows['capture_imp'] = np.where(rows['demand_week'] > 0, rows['Показы'] / rows['demand_week'], 0.0)
    rows['capture_click'] = np.where(rows['keyword_clicks'] > 0, rows['Клики'] / rows['keyword_clicks'], 0.0)
    bench_capture_imp = numeric_series(rows, 'bench_capture_imp', 0.0)
    rows['eff_index_imp'] = np.where(bench_capture_imp > 0, rows['capture_imp'] / bench_capture_imp.replace(0, np.nan), 1.0)
    bench_capture_click = numeric_series(rows, 'bench_capture_click', 0.0)
    rows['eff_index_click'] = np.where(bench_capture_click > 0, rows['capture_click'] / bench_capture_click.replace(0, np.nan), 1.0)

    limits = rows.apply(lambda r: pd.Series(compute_bid_limits(r, subject_benchmarks), index=['comfort_bid_rub','max_bid_rub','experiment_bid_rub','limit_type']), axis=1)
    rows = pd.concat([rows, limits], axis=1)
    rows['Причина лимита'] = rows.apply(explain_limit_reason, axis=1)

    rows = rows.sort_values(['id_campaign','nmId','placement']).drop_duplicates(['id_campaign','nmId','placement','payment_type'])

    decisions = []
    for _, r in rows.iterrows():
        action, new_bid, reason, rate_limit = determine_action(r, cfg)
        max_bid = safe_float(r.get('max_bid_rub'))
        if max_bid > 0 and new_bid > max_bid:
            new_bid = max_bid
        decisions.append({
            'Дата запуска': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'ID кампании': safe_int(r.get('id_campaign')),
            'Артикул WB': safe_int(r.get('nmId')),
            'Артикул продавца': r.get('supplier_article',''),
            'Товар': r.get('supplier_article',''),
            'Предмет': r.get('subject',''),
            'Плейсмент': r.get('placement',''),
            'Тип кампании': f"{r.get('payment_type','')}_{r.get('placement','')}",
            'Текущая ставка, ₽': round(safe_float(r.get('current_bid_rub')), 2),
            'Комфортная ставка, ₽': round(safe_float(r.get('comfort_bid_rub')), 2) if pd.notna(r.get('comfort_bid_rub')) else None,
            'Максимальная ставка, ₽': round(safe_float(r.get('max_bid_rub')), 2) if pd.notna(r.get('max_bid_rub')) else None,
            'Экспериментальная ставка, ₽': round(safe_float(r.get('experiment_bid_rub')), 2) if pd.notna(r.get('experiment_bid_rub')) else None,
            'Тип лимита': r.get('limit_type',''),
            'Причина лимита': r.get('Причина лимита',''),
            'Статус кампании': r.get('campaign_status',''),
            'Активна для API': bool(r.get('campaign_is_active', False)),
            'Действие': action,
            'Новая ставка, ₽': round(safe_float(new_bid), 2),
            'Причина': reason,
            'Показы': round(safe_float(r.get('Показы')), 0),
            'Клики': round(safe_float(r.get('Клики')), 0),
            'CTR, %': round(safe_float(r.get('ctr_pct')), 2),
            'Заказы РК': round(safe_float(r.get('Заказы')), 2),
            'Выручка РК, ₽': round(safe_float(r.get('Сумма_заказов')), 2),
            'Расход РК, ₽': round(safe_float(r.get('Расход')), 2),
            'ДРР кампании, %': round(safe_float(r.get('campaign_drr_cur')) * 100, 2),
            'ВП кампании текущее окно после рекламы, ₽': round(safe_float(r.get('campaign_gp_cur')), 2),
            'ВП кампании базовое окно после рекламы, ₽': round(safe_float(r.get('campaign_gp_base')), 2),
            'Рост ВП кампании, %': round(safe_float(r.get('campaign_gp_growth_pct')), 2),
            'ROI кампании, %': round(safe_float(r.get('campaign_roi_cur')) * 100, 2),
            'CPO кампании, ₽': round(safe_float(r.get('campaign_cpo')), 2),
            'Все заказы товара': round(safe_float(r.get('total_orders')), 2),
            'Выручка товара, ₽': round(safe_float(r.get('total_revenue')), 2),
            'Общий ДРР товара, %': round(safe_float(r.get('blended_drr')) * 100, 2),
            'ВП товара, % рост': round(safe_float(r.get('item_gp_growth_pct')), 2),
            'План ВП MTD, ₽': round(safe_float(r.get('План ВП MTD, ₽')), 2),
            'Факт ВП MTD, ₽': round(safe_float(r.get('Факт ВП MTD, ₽')), 2),
            'Темп плана ВП, %': round(safe_float(r.get('Темп плана ВП, %')), 2),
            'Причина плана': r.get('Причина плана',''),
            'ДРР категории, %': round(safe_float(r.get('category_drr_cur')) * 100, 2),
            'Лимит ДРР категории, %': round(safe_float(r.get('category_limit_drr')) * 100, 2),
            'Факт ВП категории MTD, ₽': round(safe_float(r.get('category_gp_cur')), 2),
            'Темп категории, %': round(safe_float(r.get('category_gp_growth_pct')), 2),
            'Рост заказов категории, %': round(safe_float(r.get('category_orders_growth_pct')), 2),
            'Рост спроса категории, %': round(safe_float(r.get('category_demand_growth_pct')), 2),
            'Темп плана категории, %': round(safe_float(r.get('category_plan_attainment_pct')), 2),
            'Рост показов кампании, %': round(safe_float(r.get('campaign_impression_growth_pct')), 2),
            'Рост кликов кампании, %': round(safe_float(r.get('campaign_click_growth_pct')), 2),
            'Рост заказов кампании, %': round(safe_float(r.get('campaign_order_growth_pct')), 2),
            'CPO CPC, ₽': round(safe_float(r.get('cpo_cpc')), 2),
            'CPO Полок, ₽': round(safe_float(r.get('cpo_cpm')), 2),
            'ДРР CPC, %': round(safe_float(r.get('drr_cpc')) * 100, 2),
            'ДРР Полок, %': round(safe_float(r.get('drr_cpm')) * 100, 2),
            'ВП CPC, ₽': round(safe_float(r.get('gp_after_ads_cpc')), 2),
            'ВП Полок, ₽': round(safe_float(r.get('gp_after_ads_cpm')), 2),
            'Лучший канал': r.get('better_channel',''),
            'Товарный блок': r.get('product_block_key',''),
            'Кампаний в блоке': safe_int(r.get('block_campaigns')),
            'CPO блока средний, ₽': round(safe_float(r.get('block_avg_cpo')), 2),
            'CPO блока лучший, ₽': round(safe_float(r.get('block_best_cpo')), 2),
            'ДРР блока, %': round(safe_float(r.get('block_drr_cur')) * 100, 2),
            'Рост заказов блока, %': round(safe_float(r.get('block_orders_growth_pct')), 2),
            'Рост ВП блока, %': round(safe_float(r.get('block_gp_growth_pct')), 2),
            'Показы 3д': round(safe_float(r.get('campaign_impressions_3d')), 0),
            'Позиция 3д': round(safe_float(r.get('median_position_3d')), 2),
            'CPO к среднему блока, x': round(safe_float(r.get('campaign_cpo_to_block_avg_x')), 2),
            'CPO к лучшему блока, x': round(safe_float(r.get('campaign_cpo_to_block_best_x')), 2),
            'Рекомендация по статусу': r.get('status_recommendation','Оставить'),
            'Статус риска': ('Критический' if safe_float(r.get('campaign_drr_cur')) > cfg.campaign_hard_drr else ('Высокий' if safe_float(r.get('campaign_drr_cur')) > cfg.campaign_target_drr or safe_float(r.get('campaign_gp_cur')) <= 0 else 'Низкий')),
            'Потенциал роста': ('Высокий' if r.get('better_channel','') == ('CPC' if str(r.get('payment_type','')).lower() == 'cpc' else 'CPM') and safe_float(r.get('campaign_gp_cur')) > 0 and safe_float(r.get('campaign_drr_cur')) <= cfg.campaign_target_drr else 'Низкий')
        })
    decisions_df = pd.DataFrame(decisions).drop_duplicates(['ID кампании','Артикул WB','Плейсмент'])

    weak = decisions_df[(decisions_df['Действие'].isin(['Снизить','Предел эффективности ставки'])) | (decisions_df.get('Статус риска', '').astype(str).eq('Критический'))].copy() if not decisions_df.empty else pd.DataFrame()
    if not weak.empty:
        weak['Комментарий'] = weak['Причина']
        weak = weak[['Артикул продавца','Артикул WB','ID кампании','Тип кампании','Плейсмент','Действие','Комментарий']].drop_duplicates()
    else:
        weak = pd.DataFrame([{'Комментарий':'Нет слабых позиций'}])

    product_metrics = daily_item.groupby('Артикул продавца', as_index=False).agg(
        Заказы=('Заказы','sum'), ДРР=('ДРР, доля','mean'), Валовая_прибыль_после_рекламы=('Валовая прибыль после рекламы, ₽','sum'), Клики=('Клики','sum'), CPO=('CPO, ₽','mean'), Чистая_прибыль=('Прогнозная чистая прибыль, ₽','sum')
    ) if not daily_item.empty and 'Комментарий' not in daily_item.columns else pd.DataFrame([{'Комментарий':'Нет дневной товарной истории'}])

    campaign_profit = daily_campaign.groupby(['ID кампании','Тип'], as_index=False).agg(
        Заказы=('Заказы_РК','sum'), ДРР=('ДРР кампании, доля','mean'), Валовая_прибыль_после_рекламы=('ВП кампании после рекламы, ₽','sum'), ROI=('ROI кампании','mean'), CPO=('CPO кампании, ₽','mean')
    ) if not daily_campaign.empty and 'Комментарий' not in daily_campaign.columns else pd.DataFrame([{'Комментарий':'Нет дневной кампанийной истории'}])

    effects = decisions_df[decisions_df['Действие'].isin(['Повысить','Снизить']) & (decisions_df['Новая ставка, ₽'] != decisions_df['Текущая ставка, ₽'])].copy()
    effects = effects[['Дата запуска','Артикул продавца','ID кампании','Тип кампании','Текущая ставка, ₽','Новая ставка, ₽','Действие','Причина']] if not effects.empty else pd.DataFrame([{'Комментарий':'Нет изменений ставок'}])

    return {
        'rows': rows,
        'decisions': decisions_df,
        'weak': weak,
        'product_metrics': product_metrics,
        'campaign_profit': campaign_profit,
        'effects': effects,
        'product_block_report': block_report if 'block_report' in locals() else pd.DataFrame(),
        'eff_history_sheets': build_efficiency_history(ads_daily, campaigns, aggregate_keyword_daily(keywords), master, bid_history, as_of_date),
        'window': window,
        'daily_item_history': daily_item,
        'daily_campaign_history': daily_campaign,
        'plan_sheet': plan_df,
        'category_plan': category_plan,
    }


def decisions_to_payload(decisions_df: pd.DataFrame) -> Dict[str, Any]:
    if decisions_df.empty:
        return {'bids': []}
    work = decisions_df.copy()
    if 'Активна для API' in work.columns:
        work = work[work['Активна для API'].fillna(False).astype(bool)].copy()
    elif 'Статус кампании' in work.columns:
        work = work[work['Статус кампании'].map(is_active_campaign_status)].copy()
    changed = work[work['Действие'].isin(['Повысить','Снизить']) & (work['Новая ставка, ₽'] != work['Текущая ставка, ₽'])].copy()
    changed = changed.drop_duplicates(['ID кампании','Артикул WB','Плейсмент'])
    grouped = {}
    for _, r in changed.iterrows():
        advert = safe_int(r['ID кампании'])
        nm_id = safe_int(r['Артикул WB'])
        payment_type = 'cpc' if 'cpc' in str(r['Тип кампании']).lower() else 'cpm'
        placement = str(r['Плейсмент'])
        grouped.setdefault((advert, payment_type), []).append({
            'nm_id': nm_id,
            'placement': placement_for_bids_endpoint(placement),
            'bid_kopecks': normalize_bid_for_wb(r['Новая ставка, ₽'], payment_type, placement),
        })
    out = []
    for (advert, payment_type), items in grouped.items():
        uniq = {(it['nm_id'], it['placement']): it for it in items}
        out.append({'advert_id': advert, 'payment_type': payment_type, 'nm_bids': list(uniq.values())})
    return {'bids': out}


def apply_shade_actions(actions_df: pd.DataFrame, api_key: str, dry_run: bool) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    empty_log = pd.DataFrame([{'Комментарий':'Блок оттенков отключён: каждый оттенок теперь ведётся отдельной рекламной кампанией'}])
    empty_actions = pd.DataFrame([{'Комментарий':'Блок оттенков отключён'}])
    empty_tests = pd.DataFrame([{'Комментарий':'Блок оттенков отключён'}])
    return empty_log, empty_actions, empty_tests


def build_history_append(decisions: pd.DataFrame, as_of_date: date) -> pd.DataFrame:
    if decisions.empty:
        return pd.DataFrame()
    hist = decisions.copy()
    hist['Дата запуска'] = now_ts()
    hist['Дата дня'] = as_of_date.isoformat()
    return hist



def save_outputs(provider: BaseProvider, results: Dict[str, Any], run_mode: str, bid_send_log: Optional[pd.DataFrame], shade_apply_log: Optional[pd.DataFrame] = None, history_append: Optional[pd.DataFrame] = None) -> None:
    decisions = results.get('decisions', pd.DataFrame()).copy()
    plan_sheet = results.get('plan_sheet', pd.DataFrame()).copy()
    category_plan = results.get('category_plan', pd.DataFrame()).copy()
    daily_item = results.get('daily_item_history', pd.DataFrame()).copy()
    daily_campaign = results.get('daily_campaign_history', pd.DataFrame()).copy()
    product_metrics = results.get('product_metrics', pd.DataFrame()).copy()
    campaign_profit = results.get('campaign_profit', pd.DataFrame()).copy()
    effects = results.get('effects', pd.DataFrame()).copy()
    weak = results.get('weak', pd.DataFrame()).copy()
    min_bids_df = results.get('min_bids_df', pd.DataFrame()).copy()
    history_append = history_append.copy() if isinstance(history_append, pd.DataFrame) else pd.DataFrame()

    item_cols = ['Дата', 'Товар', 'Артикул WB', 'Артикул продавца', 'Предмет', 'Заказы', 'Сумма_заказов', '% выкупа', 'Расходы_РК',
                 'Валовая прибыль после рекламы, ₽', 'Клики', 'CPO, ₽', 'Прогнозная чистая прибыль, ₽', 'Показы', 'Заказы_РК', 'Выручка_РК',
                 'ДРР, доля', 'Конверсия в корзину, %', 'Конверсия в заказ, %', 'Частота запросов', 'Спрос по ключам', 'Медианная позиция', 'Видимость, %', 'День зрелый']
    campaign_cols = ['Дата', 'ID кампании', 'Артикул WB', 'Артикул продавца', 'Предмет', 'Тип кампании', 'Тип', 'Плейсмент', 'Ставка, ₽', 'Показы', 'Клики',
                     'Заказы_РК', 'Выручка_РК', 'Расходы_РК', 'ДРР кампании, доля', 'ВП кампании после рекламы, ₽', 'ROI кампании',
                     'CPO кампании, ₽', 'Спрос по ключам', 'Частота запросов', 'День зрелый']

    daily_item = trim_to_columns(daily_item, item_cols)
    daily_campaign = trim_to_columns(daily_campaign, campaign_cols)

    summary = pd.DataFrame([{
        'Режим': 'run',
        'Дата формирования': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'Всего рекомендаций': int(len(decisions)),
        'Изменённых ставок': int(len(decisions[decisions['Действие'].isin(['Повысить', 'Снизить']) & (decisions['Новая ставка, ₽'] != decisions['Текущая ставка, ₽'])])) if not decisions.empty else 0,
        'Блоков отправки ставок': 0 if bid_send_log is None or bid_send_log.empty else int(len(bid_send_log)),
        'Текущее окно с': results['window']['cur_start'],
        'Текущее окно по': results['window']['cur_end'],
        'База с': results['window']['base_start'],
        'База по': results['window']['base_end'],
    }])

    old_sheets = {}
    try:
        if provider.file_exists(OUT_SINGLE_REPORT):
            old_sheets = provider.read_excel_all_sheets(OUT_SINGLE_REPORT)
    except Exception:
        old_sheets = {}

    def append_dedup(old_df: pd.DataFrame, new_df: pd.DataFrame, keys: List[str], final_cols: Optional[List[str]] = None) -> pd.DataFrame:
        old_df = old_df.copy() if isinstance(old_df, pd.DataFrame) else pd.DataFrame()
        new_df = new_df.copy() if isinstance(new_df, pd.DataFrame) else pd.DataFrame()
        if final_cols:
            old_df = trim_to_columns(old_df, final_cols)
            new_df = trim_to_columns(new_df, final_cols)
        if new_df.empty:
            base = old_df
        elif old_df.empty:
            base = new_df
        else:
            base = pd.concat([old_df, new_df], ignore_index=True, sort=False)
        real_keys = [k for k in keys if k in base.columns]
        base = base.drop_duplicates(real_keys, keep='last') if real_keys else base
        if final_cols:
            base = trim_to_columns(base, final_cols)
        return base

    decisions_hist = append_dedup(old_sheets.get('История решений день', pd.DataFrame()), history_append, ['Дата запуска', 'ID кампании', 'Артикул WB', 'Плейсмент'])
    item_hist_all = append_dedup(old_sheets.get('История день товар', pd.DataFrame()), daily_item, ['Дата', 'Артикул WB'], item_cols)
    campaign_hist_all = append_dedup(old_sheets.get('История день кампания', pd.DataFrame()), daily_campaign, ['Дата', 'ID кампании', 'Артикул WB'], campaign_cols)
    old_bid_hist = old_sheets.get('История ставок', pd.DataFrame())
    bid_hist_all = append_dedup(old_bid_hist, decisions[decisions['Действие'].isin(['Повысить', 'Снизить']) & (decisions['Новая ставка, ₽'] != decisions['Текущая ставка, ₽'])].copy(), ['ID кампании', 'Артикул WB', 'Плейсмент', 'Дата запуска'])
    archive_all = append_dedup(old_sheets.get('Архив решений', pd.DataFrame()), decisions, ['Дата запуска', 'ID кампании', 'Артикул WB', 'Плейсмент'])

    eff_stub = pd.DataFrame([{'Комментарий': 'Детальная эффективность ставки вынесена в отдельный файл Эффективность_ставки_ежедневно.xlsx'}])

    sheets = {
        'Сводка': summary,
        'Решения': decisions,
        'План': plan_sheet,
        'План категорий': category_plan,
        'Товар день': daily_item,
        'Кампания день': daily_campaign,
        'Товар итог': product_metrics,
        'Кампании прибыль': campaign_profit,
        'Фактически изменённые ставки': bid_send_log if bid_send_log is not None and not bid_send_log.empty else pd.DataFrame([{'Комментарий': 'Нет отправленных блоков ставок'}]),
        'Лимиты ставок': decisions[[c for c in ['Артикул продавца', 'ID кампании', 'Тип кампании', 'Статус кампании', 'Активна для API', 'Текущая ставка, ₽', 'Комфортная ставка, ₽', 'Максимальная ставка, ₽', 'Экспериментальная ставка, ₽', 'Тип лимита', 'Причина лимита'] if c in decisions.columns]].copy() if not decisions.empty else pd.DataFrame(),
        'Минимальные ставки WB': min_bids_df if not min_bids_df.empty else pd.DataFrame([{'Комментарий': 'Нет данных по min bid'}]),
        'Слабые позиции': weak,
        'Эффект изменений': effects,
        'Лог API': pd.DataFrame(API_CALL_LOGS) if API_CALL_LOGS else pd.DataFrame([{'Комментарий': 'Нет вызовов API'}]),
        'История решений день': decisions_hist,
        'История ставок': bid_hist_all,
        'Архив решений': archive_all,
        'Эффективность ставки': eff_stub,
        'История день товар': item_hist_all,
        'История день кампания': campaign_hist_all,
    }

    sheets = {name: normalize_output_df(df) for name, df in sheets.items()}
    provider.write_excel(OUT_SINGLE_REPORT, sheets)
    provider.write_excel(OUT_PREVIEW, sheets)
    provider.write_text(OUT_SUMMARY, json.dumps(summary.iloc[0].to_dict(), ensure_ascii=False, default=str, indent=2))

    eff_sheets = results.get('eff_history_sheets', {})
    if not eff_sheets:
        eff_sheets = {'Комментарий': pd.DataFrame([{'Комментарий': 'Нет данных по эффективности ставки'}])}
    eff_sheets = {name: normalize_output_df(df) for name, df in eff_sheets.items()}
    provider.write_excel(OUT_EFF, eff_sheets)

def run_manager(args: argparse.Namespace) -> None:
    API_CALL_LOGS.clear()
    MIN_BID_ROWS.clear()
    provider = choose_provider(args.local_data_dir)
    as_of_date = datetime.strptime(args.as_of_date, '%Y-%m-%d').date() if args.as_of_date else datetime.now().date()
    cfg = Config()
    results = prepare_metrics(provider, cfg, as_of_date)
    api_key = os.getenv('WB_PROMO_KEY_TOPFACE','').strip()
    results = enrich_with_min_bids(results, api_key)
    decisions = results['decisions'].copy()
    log(f'✅ Всего строк решений: {len(decisions)}')
    changed = decisions[decisions['Действие'].isin(['Повысить','Снизить']) & (decisions['Новая ставка, ₽'] != decisions['Текущая ставка, ₽'])].copy()
    log(f'🔁 Изменённых ставок: {len(changed)}')
    if not changed.empty:
        print(changed[['Товар','Артикул продавца','Предмет','ID кампании','Плейсмент','Текущая ставка, ₽','Новая ставка, ₽','Действие','Причина']].head(30).to_string(index=False), flush=True)
    payload = decisions_to_payload(decisions)
    bid_send_log = send_payload(payload, api_key, dry_run=False)
    log(f"📤 Отправлено блоков в WB: {0 if bid_send_log is None or bid_send_log.empty else len(bid_send_log)}")
    history_append = build_history_append(decisions, as_of_date)
    save_outputs(provider, results, 'run', bid_send_log, None, history_append)




def parse_abc_snapshot_dt(key: str) -> datetime:
    text = str(key or '')
    m = re.search(r'_at_(\d{4}-\d{2}-\d{2})_(\d{2})-(\d{2})', text)
    if m:
        try:
            return datetime.strptime(f"{m.group(1)} {m.group(2)}:{m.group(3)}", "%Y-%m-%d %H:%M")
        except Exception:
            pass
    return datetime.min


def load_latest_abc_reference(provider: BaseProvider) -> Tuple[pd.DataFrame, Dict[str, float]]:
    try:
        keys = provider.list_keys(ABC_PREFIX)
    except Exception:
        return pd.DataFrame(), {}
    abc_keys = [k for k in keys if re.search(r'wb_abc_report_goods__.*\.xlsx$', Path(str(k)).name, flags=re.I)]
    if not abc_keys:
        return pd.DataFrame(), {}
    best_key = max(abc_keys, key=parse_abc_snapshot_dt)
    try:
        df = provider.read_excel(best_key).copy()
    except Exception:
        return pd.DataFrame(), {}
    if df.empty:
        return pd.DataFrame(), {}

    df = df.rename(columns={
        'Артикул WB': 'nmId',
        'Артикул продавца': 'supplier_article',
        'Предмет': 'subject',
        'Ваша категория': 'manager_category',
        'Процент выкупов, %': 'buyout_pct',
        'Заказы': 'orders_cnt',
    })
    if 'nmId' in df.columns:
        df['nmId'] = pd.to_numeric(df['nmId'], errors='coerce')
    else:
        df['nmId'] = np.nan
    df['supplier_article'] = series_or_default(df, 'supplier_article', '').fillna('').astype(str)
    df['subject'] = series_or_default(df, 'subject', '').fillna('').astype(str)
    df['subject_norm'] = df['subject'].map(canonical_subject)
    df['manager_category'] = series_or_default(df, 'manager_category', '').fillna('').astype(str).str.strip()
    df['manager_category'] = df['manager_category'].replace({'': 'Без менеджера'})
    df['buyout_pct'] = pd.to_numeric(df.get('buyout_pct'), errors='coerce')
    df['orders_cnt'] = numeric_series(df, 'orders_cnt', 0.0)
    df['buyout_pct_clipped'] = df['buyout_pct'].clip(lower=0.0, upper=100.0)

    rates: Dict[str, float] = {}
    for subject_norm, grp in df.groupby('subject_norm'):
        if not subject_norm:
            continue
        valid = grp['buyout_pct_clipped'].notna()
        if not valid.any():
            continue
        w = grp.loc[valid, 'orders_cnt'].fillna(0.0)
        x = grp.loc[valid, 'buyout_pct_clipped'].fillna(0.0)
        if float(w.sum()) > 0:
            pct_value = float((x * w).sum() / w.sum())
        else:
            pct_value = float(x.mean())
        rates[subject_norm] = round(clamp(pct_value / 100.0, 0.0, 1.0), 4)

    ref_cols = [c for c in ['nmId', 'supplier_article', 'subject', 'subject_norm', 'manager_category', 'buyout_pct_clipped', 'orders_cnt'] if c in df.columns]
    ref = df[ref_cols].drop_duplicates().copy()
    ref.attrs['source_key'] = best_key
    return ref, rates


def _normalize_funnel_subject_sheet(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df is None or raw_df.empty:
        return pd.DataFrame(columns=['supplier_article', 'nmId', 'subject', 'subject_norm', 'buyout_rate', 'orders_qty', 'orders_sum'])
    df = raw_df.copy()
    first_row = df.iloc[0].astype(str).tolist() if len(df.index) > 0 else []
    if 'Предмет' in first_row and ('Процент выкупа' in first_row or 'Процент выкупа (предыдущий период)' in first_row):
        df.columns = [str(x).strip() for x in df.iloc[0].tolist()]
        df = df.iloc[1:].copy()
    df = df.rename(columns={
        'Артикул продавца': 'supplier_article',
        'Артикул WB': 'nmId',
        'Предмет': 'subject',
        'Процент выкупа': 'buyout_pct',
        'Заказали, шт': 'orders_qty',
        'Заказали на сумму, ₽': 'orders_sum',
        'Выкупили на сумму, ₽': 'buyout_sum',
        'ordersSumRub': 'orders_sum',
        'buyoutsSumRub': 'buyout_sum',
        'ordersCount': 'orders_qty',
        'buyoutPercent': 'buyout_pct',
    })
    if 'subject' not in df.columns:
        return pd.DataFrame(columns=['supplier_article', 'nmId', 'subject', 'subject_norm', 'buyout_rate', 'orders_qty', 'orders_sum'])
    out = df.copy()
    out['supplier_article'] = series_or_default(out, 'supplier_article', '').fillna('').astype(str).str.strip()
    out['nmId'] = pd.to_numeric(out.get('nmId'), errors='coerce')
    out['subject'] = series_or_default(out, 'subject', '').fillna('').astype(str).str.strip()
    out['subject_norm'] = out['subject'].map(canonical_subject)
    direct_rate = to_buyout_rate(out.get('buyout_pct', pd.Series(index=out.index, dtype=float)), default=np.nan)
    if 'orders_sum' in out.columns and 'buyout_sum' in out.columns:
        orders_sum = numeric_series(out, 'orders_sum', 0.0)
        buyout_sum = numeric_series(out, 'buyout_sum', 0.0)
        ratio_sum = pd.Series(np.where(orders_sum > 0, buyout_sum / orders_sum, np.nan), index=out.index)
        direct_rate = direct_rate.where(~direct_rate.isna(), ratio_sum)
    out['buyout_rate'] = pd.to_numeric(direct_rate, errors='coerce').clip(lower=0.0, upper=1.0)
    out['orders_qty'] = numeric_series(out, 'orders_qty', 0.0)
    out['orders_sum'] = numeric_series(out, 'orders_sum', 0.0)
    out = out[out['subject_norm'].ne('') & out['buyout_rate'].notna()].copy()
    return out[['supplier_article', 'nmId', 'subject', 'subject_norm', 'buyout_rate', 'orders_qty', 'orders_sum']]


def load_latest_funnel_subject_reference(provider: BaseProvider) -> Tuple[pd.DataFrame, Dict[str, float]]:
    candidates: List[Tuple[str, pd.DataFrame]] = []

    def _append_candidate(name: str, raw_df: pd.DataFrame) -> None:
        norm = _normalize_funnel_subject_sheet(raw_df)
        if not norm.empty:
            candidates.append((name, norm))

    try:
        sheets = provider.read_excel_all_sheets(FUNNEL_KEY)
        if isinstance(sheets, dict):
            for sheet_name, raw_df in sheets.items():
                if str(sheet_name).strip().lower() in {'товары', 'products'}:
                    _append_candidate(f'{FUNNEL_KEY}::{sheet_name}', raw_df)
                    break
            if not candidates:
                for sheet_name, raw_df in sheets.items():
                    _append_candidate(f'{FUNNEL_KEY}::{sheet_name}', raw_df)
                    if candidates:
                        break
    except Exception:
        pass

    if not candidates and isinstance(provider, LocalProvider):
        local_candidates = sorted(provider.base_dir.glob('*Воронка продаж*.zip')) + sorted(provider.base_dir.glob('*Воронка продаж*.xlsx'))
        for path in local_candidates:
            try:
                if path.suffix.lower() == '.zip':
                    with zipfile.ZipFile(path) as zf:
                        for member in zf.namelist():
                            if member.lower().endswith('.xlsx'):
                                data = io.BytesIO(zf.read(member))
                                xls = pd.ExcelFile(data)
                                for sheet_name in xls.sheet_names:
                                    raw_df = pd.read_excel(data, sheet_name=sheet_name)
                                    _append_candidate(f'{path.name}::{sheet_name}', raw_df)
                                    if candidates:
                                        break
                                if candidates:
                                    break
                else:
                    xls = pd.ExcelFile(path)
                    for sheet_name in xls.sheet_names:
                        raw_df = pd.read_excel(path, sheet_name=sheet_name)
                        _append_candidate(f'{path.name}::{sheet_name}', raw_df)
                        if candidates:
                            break
                if candidates:
                    break
            except Exception:
                continue

    if not candidates:
        return pd.DataFrame(), {}

    source_name, ref = candidates[0]
    rates: Dict[str, float] = {}
    for subject_norm, grp in ref.groupby('subject_norm'):
        if not subject_norm:
            continue
        weights = grp['orders_qty'].fillna(0.0)
        if float(weights.sum()) <= 0:
            weights = grp['orders_sum'].fillna(0.0)
        values = grp['buyout_rate'].fillna(np.nan)
        valid = values.notna()
        if not valid.any():
            continue
        values = values[valid]
        weights = weights[valid]
        if float(weights.sum()) > 0:
            rate_value = float((values * weights).sum() / weights.sum())
        else:
            rate_value = float(values.mean())
        rates[subject_norm] = round(clamp(rate_value, 0.0, 1.0), 4)

    ref = ref.drop_duplicates().copy()
    ref.attrs['source_key'] = source_name
    return ref, rates


def write_binary_provider(provider: BaseProvider, key: str, data: bytes, content_type: str = 'application/octet-stream') -> None:
    if isinstance(provider, S3Provider):
        provider.s3.put_object(Bucket=provider.bucket, Key=key, Body=data, ContentType=content_type)
        return
    if isinstance(provider, LocalProvider):
        path = provider._resolve(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return
    raise TypeError(f'Unsupported provider type for binary write: {type(provider)}')


def article_natural_key(value: Any) -> List[Any]:
    text = str(value or '').strip()
    parts = re.split(r'(\d+)', text)
    out: List[Any] = []
    for part in parts:
        if not part:
            continue
        out.append(int(part) if part.isdigit() else part.lower())
    return out


def wrap_text_to_width(draw: Any, text: str, font: Any, max_width: int) -> List[str]:
    text = str(text or '').strip()
    if not text:
        return ['']
    words = text.split()
    if not words:
        return ['']
    lines: List[str] = []
    current = words[0]
    for word in words[1:]:
        candidate = f'{current} {word}'
        width = draw.textbbox((0, 0), candidate, font=font)[2]
        if width <= max_width:
            current = candidate
        else:
            lines.append(current)
            current = word
    lines.append(current)
    return lines



def build_manager_pdf_bytes(manager_name: str, manager_df: pd.DataFrame, run_dt_text: str, source_key: str = '') -> bytes:
    """
    Build PDF bytes for manager recommendations.

    Fallback order:
    1) reportlab
    2) matplotlib PdfPages
    3) minimal built-in PDF (ASCII-safe transliterated text)
    """
    try:
        from reportlab.lib import colors
        from reportlab.lib.enums import TA_LEFT
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
        from reportlab.lib.units import mm
        from reportlab.platypus import HRFlowable, KeepTogether, Paragraph, SimpleDocTemplate, Spacer

        buf = io.BytesIO()
        doc = SimpleDocTemplate(
            buf,
            pagesize=A4,
            leftMargin=14 * mm,
            rightMargin=14 * mm,
            topMargin=14 * mm,
            bottomMargin=14 * mm,
            title=f'Рекомендации по ставкам - {manager_name}',
            author='Ассистент WB',
        )

        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            'ManagerTitle',
            parent=styles['Title'],
            fontName='Helvetica-Bold',
            fontSize=16,
            leading=20,
            alignment=TA_LEFT,
            textColor=colors.black,
            spaceAfter=6,
        )
        meta_style = ParagraphStyle(
            'ManagerMeta',
            parent=styles['BodyText'],
            fontName='Helvetica',
            fontSize=9,
            leading=12,
            textColor=colors.HexColor('#444444'),
            spaceAfter=2,
        )
        head_style = ParagraphStyle(
            'ManagerHead',
            parent=styles['Heading4'],
            fontName='Helvetica-Bold',
            fontSize=10.5,
            leading=13,
            textColor=colors.black,
            spaceAfter=4,
        )
        body_style = ParagraphStyle(
            'ManagerBody',
            parent=styles['BodyText'],
            fontName='Helvetica',
            fontSize=9.5,
            leading=12,
            textColor=colors.black,
            spaceAfter=3,
        )
        reason_style = ParagraphStyle(
            'ManagerReason',
            parent=styles['BodyText'],
            fontName='Helvetica',
            fontSize=8.8,
            leading=11,
            textColor=colors.HexColor('#222222'),
            spaceAfter=0,
        )

        def esc(value: Any) -> str:
            text_value = '' if value is None else str(value)
            return (
                text_value.replace('&', '&amp;')
                .replace('<', '&lt;')
                .replace('>', '&gt;')
                .replace('\n', '<br/>')
            )

        story = [
            Paragraph(esc(f'Рекомендации по ставкам - {manager_name}'), title_style),
            Paragraph(esc(f'Дата формирования: {run_dt_text}'), meta_style),
            Paragraph(esc(f'Строк рекомендаций: {len(manager_df)}'), meta_style),
        ]
        if source_key:
            story.append(Paragraph(esc(f'Источник менеджеров/выкупа: {Path(source_key).name}'), meta_style))
        story.extend([Spacer(1, 4), HRFlowable(width='100%', thickness=1, color=colors.black), Spacer(1, 8)])

        for _, row in manager_df.iterrows():
            current_bid = safe_float(row.get('Текущая ставка, ₽'))
            new_bid = safe_float(row.get('Новая ставка, ₽'))
            drr_campaign = safe_float(row.get('ДРР кампании, %'))
            drr_category = safe_float(row.get('ДРР категории, %'))
            spend = safe_float(row.get('Расход РК, ₽'))
            orders = safe_float(row.get('Заказы РК'))
            gp = safe_float(row.get('ВП кампании текущее окно после рекламы, ₽'))

            head = (
                f"Артикул {esc(row.get('Артикул продавца', ''))} | "
                f"WB {safe_int(row.get('Артикул WB'))} | "
                f"ID кампании {safe_int(row.get('ID кампании'))} | "
                f"{esc(row.get('Тип кампании', ''))}"
            )
            rec = (
                f"<b>Рекомендация:</b> {esc(row.get('Действие', ''))} | "
                f"ставка {current_bid:.0f} -> {new_bid:.0f} ₽"
            )
            metrics = (
                f"<b>Цифры:</b> расход {spend:.0f} ₽; "
                f"заказы РК {orders:.0f}; "
                f"ДРР кампании {drr_campaign:.1f}%; "
                f"ВП кампании {gp:.0f} ₽; "
                f"ДРР категории {drr_category:.1f}%"
            )
            reason = f"<b>Почему:</b> {esc(row.get('Причина', ''))}"

            block = [
                Paragraph(head, head_style),
                Paragraph(rec, body_style),
                Paragraph(metrics, body_style),
                Paragraph(reason, reason_style),
                Spacer(1, 6),
                HRFlowable(width='100%', thickness=0.6, color=colors.HexColor('#B5B5B5')),
                Spacer(1, 7),
            ]
            story.append(KeepTogether(block))

        doc.build(story)
        return buf.getvalue()
    except Exception:
        pass

    try:
        import math
        import textwrap
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        from matplotlib.backends.backend_pdf import PdfPages

        lines: List[str] = []
        lines.append(f'Рекомендации по ставкам - {manager_name}')
        lines.append(f'Дата формирования: {run_dt_text}')
        lines.append(f'Строк рекомендаций: {len(manager_df)}')
        if source_key:
            lines.append(f'Источник менеджеров/выкупа: {Path(source_key).name}')
        lines.append('')

        for _, row in manager_df.iterrows():
            current_bid = safe_float(row.get('Текущая ставка, ₽'))
            new_bid = safe_float(row.get('Новая ставка, ₽'))
            drr_campaign = safe_float(row.get('ДРР кампании, %'))
            drr_category = safe_float(row.get('ДРР категории, %'))
            spend = safe_float(row.get('Расход РК, ₽'))
            orders = safe_float(row.get('Заказы РК'))
            gp = safe_float(row.get('ВП кампании текущее окно после рекламы, ₽'))

            block = [
                f"Артикул {row.get('Артикул продавца', '')} | WB {safe_int(row.get('Артикул WB'))} | ID кампании {safe_int(row.get('ID кампании'))} | {row.get('Тип кампании', '')}",
                f"Рекомендация: {row.get('Действие', '')} | ставка {current_bid:.0f} -> {new_bid:.0f} ₽",
                f"Цифры: расход {spend:.0f} ₽; заказы РК {orders:.0f}; ДРР кампании {drr_campaign:.1f}%; ВП кампании {gp:.0f} ₽; ДРР категории {drr_category:.1f}%",
                f"Почему: {row.get('Причина', '')}",
                "-" * 110,
            ]
            for b in block:
                wrapped = textwrap.wrap(str(b), width=115, break_long_words=False, break_on_hyphens=False) or ['']
                lines.extend(wrapped)

        line_height = 0.022
        usable_lines = 42
        total_pages = max(1, math.ceil(len(lines) / usable_lines))
        buf = io.BytesIO()
        with PdfPages(buf) as pdf:
            for page_idx in range(total_pages):
                page_lines = lines[page_idx * usable_lines:(page_idx + 1) * usable_lines]
                fig = plt.figure(figsize=(8.27, 11.69))
                ax = fig.add_axes([0, 0, 1, 1])
                ax.axis('off')
                y = 0.97
                for i, line in enumerate(page_lines):
                    fontsize = 12 if (page_idx == 0 and i == 0) else 8.5
                    weight = 'bold' if (page_idx == 0 and i == 0) else 'normal'
                    ax.text(0.04, y, str(line), ha='left', va='top', fontsize=fontsize, fontweight=weight, family='DejaVu Sans')
                    y -= line_height
                pdf.savefig(fig)
                plt.close(fig)
        return buf.getvalue()
    except Exception:
        pass

    def _translit(value: Any) -> str:
        mapping = {
            'А':'A','Б':'B','В':'V','Г':'G','Д':'D','Е':'E','Ё':'E','Ж':'Zh','З':'Z','И':'I','Й':'Y','К':'K','Л':'L','М':'M','Н':'N','О':'O','П':'P','Р':'R','С':'S','Т':'T','У':'U','Ф':'F','Х':'Kh','Ц':'Ts','Ч':'Ch','Ш':'Sh','Щ':'Sch','Ъ':'','Ы':'Y','Ь':'','Э':'E','Ю':'Yu','Я':'Ya',
            'а':'a','б':'b','в':'v','г':'g','д':'d','е':'e','ё':'e','ж':'zh','з':'z','и':'i','й':'y','к':'k','л':'l','м':'m','н':'n','о':'o','п':'p','р':'r','с':'s','т':'t','у':'u','ф':'f','х':'kh','ц':'ts','ч':'ch','ш':'sh','щ':'sch','ъ':'','ы':'y','ь':'','э':'e','ю':'yu','я':'ya',
            '₽':'RUB','—':'-','–':'-','№':'No '
        }
        s = '' if value is None else str(value)
        return ''.join(mapping.get(ch, ch if ord(ch) < 128 else '?') for ch in s)

    def _pdf_escape(text_value: str) -> str:
        return text_value.replace('\\', '\\\\').replace('(', '\\(').replace(')', '\\)')

    lines = [
        _translit(f'Rekomendatsii po stavkam - {manager_name}'),
        _translit(f'Data formirovaniya: {run_dt_text}'),
        _translit(f'Strok rekomendatsiy: {len(manager_df)}'),
    ]
    if source_key:
        lines.append(_translit(f'Istochnik menedzherov/vykupa: {Path(source_key).name}'))
    lines.append('')

    import textwrap
    for _, row in manager_df.iterrows():
        current_bid = safe_float(row.get('Текущая ставка, ₽'))
        new_bid = safe_float(row.get('Новая ставка, ₽'))
        drr_campaign = safe_float(row.get('ДРР кампании, %'))
        drr_category = safe_float(row.get('ДРР категории, %'))
        spend = safe_float(row.get('Расход РК, ₽'))
        orders = safe_float(row.get('Заказы РК'))
        gp = safe_float(row.get('ВП кампании текущее окно после рекламы, ₽'))
        block = [
            _translit(f"Artikul {row.get('Артикул продавца', '')} | WB {safe_int(row.get('Артикул WB'))} | ID kampanii {safe_int(row.get('ID кампании'))} | {row.get('Тип кампании', '')}"),
            _translit(f"Rekomendatsiya: {row.get('Действие', '')} | stavka {current_bid:.0f} -> {new_bid:.0f} RUB"),
            _translit(f"Tsifry: rashod {spend:.0f} RUB; zakazy RK {orders:.0f}; DRR kampanii {drr_campaign:.1f}%; VP kampanii {gp:.0f} RUB; DRR kategorii {drr_category:.1f}%"),
            _translit(f"Pochemu: {row.get('Причина', '')}"),
            "-" * 110,
        ]
        for b in block:
            lines.extend(textwrap.wrap(str(b), width=110, break_long_words=False, break_on_hyphens=False) or [''])

    lines_per_page = 46
    page_width = 595
    page_height = 842
    content = []
    page_objects = []
    font_obj_num = 3
    obj_num = 4

    pages = [lines[i:i+lines_per_page] for i in range(0, len(lines), lines_per_page)] or [[]]
    for page_lines in pages:
        stream_lines = ['BT', '/F1 9 Tf', '14 TL', '40 800 Td']
        for i, line in enumerate(page_lines):
            if i > 0:
                stream_lines.append('T*')
            stream_lines.append(f'({_pdf_escape(line)}) Tj')
        stream_lines.append('ET')
        stream = '\n'.join(stream_lines).encode('latin-1', errors='replace')
        content_obj = obj_num
        page_obj = obj_num + 1
        content.append((content_obj, f"<< /Length {len(stream)} >>\nstream\n".encode('latin-1') + stream + b"\nendstream"))
        page_objects.append(page_obj)
        content.append((page_obj, f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 {page_width} {page_height}] /Contents {content_obj} 0 R /Resources << /Font << /F1 {font_obj_num} 0 R >> >> >>".encode('latin-1')))
        obj_num += 2

    objects = [
        (1, b"<< /Type /Catalog /Pages 2 0 R >>"),
        (2, f"<< /Type /Pages /Kids [{' '.join(f'{n} 0 R' for n in page_objects)}] /Count {len(page_objects)} >>".encode('latin-1')),
        (3, b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"),
    ] + content

    pdf = io.BytesIO()
    pdf.write(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets = {0: 0}
    for num, body in objects:
        offsets[num] = pdf.tell()
        pdf.write(f"{num} 0 obj\n".encode('latin-1'))
        pdf.write(body)
        pdf.write(b"\nendobj\n")
    xref_pos = pdf.tell()
    max_obj = max(offsets)
    pdf.write(f"xref\n0 {max_obj + 1}\n".encode('latin-1'))
    pdf.write(b"0000000000 65535 f \n")
    for i in range(1, max_obj + 1):
        pdf.write(f"{offsets.get(i, 0):010d} 00000 n \n".encode('latin-1'))
    pdf.write(f"trailer\n<< /Size {max_obj + 1} /Root 1 0 R >>\nstartxref\n{xref_pos}\n%%EOF".encode('latin-1'))
    return pdf.getvalue()


def build_manager_recommendations(decisions_df: pd.DataFrame, abc_ref: pd.DataFrame) -> pd.DataFrame:
    work = decisions_df.copy()
    if work.empty:
        return work
    if abc_ref is not None and not abc_ref.empty:
        manager_map = abc_ref[['nmId', 'manager_category']].dropna().drop_duplicates('nmId').copy()
        manager_map = manager_map.rename(columns={'nmId': 'Артикул WB', 'manager_category': 'Менеджер'})
        work = work.merge(manager_map, on='Артикул WB', how='left')
    if 'Менеджер' not in work.columns:
        work['Менеджер'] = 'Без менеджера'
    work['Менеджер'] = work['Менеджер'].fillna('Без менеджера').astype(str).str.strip().replace({'': 'Без менеджера'})
    work['_article_sort'] = work['Артикул продавца'].map(lambda x: '|'.join(f"{p:010d}" if isinstance(p, int) else str(p) for p in article_natural_key(x)))
    work = work.sort_values(['Менеджер', '_article_sort', 'ID кампании', 'Тип кампании'], kind='stable').drop(columns=['_article_sort'])
    return work


def save_manager_recommendation_pdfs(provider: BaseProvider, decisions_df: pd.DataFrame, abc_ref: pd.DataFrame, run_dt_text: str) -> pd.DataFrame:
    work = build_manager_recommendations(decisions_df, abc_ref)
    if work.empty:
        return pd.DataFrame([{'Менеджер': 'Нет данных', 'Файл': '', 'Строк рекомендаций': 0}])

    source_key = ''
    if abc_ref is not None and hasattr(abc_ref, 'attrs'):
        source_key = str(abc_ref.attrs.get('source_key') or '')

    index_rows: List[Dict[str, Any]] = []
    base_prefix = SERVICE_ROOT + 'Рекомендации_менеджерам/'
    for manager_name, grp in work.groupby('Менеджер', dropna=False):
        manager_name = str(manager_name or 'Без менеджера').strip() or 'Без менеджера'
        pdf_bytes = build_manager_pdf_bytes(manager_name, grp, run_dt_text, source_key)
        safe_name = re.sub(r'[^0-9A-Za-zА-Яа-я_-]+', '_', manager_name).strip('_') or 'manager'
        key = base_prefix + f'Рекомендации_{safe_name}.pdf'
        write_binary_provider(provider, key, pdf_bytes, content_type='application/pdf')
        index_rows.append({
            'Менеджер': manager_name,
            'Файл': key,
            'Строк рекомендаций': int(len(grp)),
        })
    return pd.DataFrame(index_rows).sort_values(['Менеджер']).reset_index(drop=True)


def save_outputs(provider: BaseProvider, results: Dict[str, Any], run_mode: str, bid_send_log: Optional[pd.DataFrame], shade_apply_log: Optional[pd.DataFrame] = None, history_append: Optional[pd.DataFrame] = None) -> None:
    decisions = results.get('decisions', pd.DataFrame()).copy()
    plan_sheet = results.get('plan_sheet', pd.DataFrame()).copy()
    category_plan = results.get('category_plan', pd.DataFrame()).copy()
    daily_item = results.get('daily_item_history', pd.DataFrame()).copy()
    daily_campaign = results.get('daily_campaign_history', pd.DataFrame()).copy()
    product_metrics = results.get('product_metrics', pd.DataFrame()).copy()
    campaign_profit = results.get('campaign_profit', pd.DataFrame()).copy()
    effects = results.get('effects', pd.DataFrame()).copy()
    weak = results.get('weak', pd.DataFrame()).copy()
    min_bids_df = results.get('min_bids_df', pd.DataFrame()).copy()
    history_append = history_append.copy() if isinstance(history_append, pd.DataFrame) else pd.DataFrame()

    abc_ref = globals().get('ABC_REFERENCE_DF', pd.DataFrame())
    if abc_ref is None or not isinstance(abc_ref, pd.DataFrame):
        abc_ref = pd.DataFrame()
    manager_index = save_manager_recommendation_pdfs(provider, decisions, abc_ref, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    if not decisions.empty:
        decisions = build_manager_recommendations(decisions, abc_ref)

    item_cols = ['Дата', 'Товар', 'Артикул WB', 'Артикул продавца', 'Предмет', 'Заказы', 'Сумма_заказов', '% выкупа', 'Расходы_РК',
                 'Валовая прибыль после рекламы, ₽', 'Клики', 'CPO, ₽', 'Прогнозная чистая прибыль, ₽', 'Показы', 'Заказы_РК', 'Выручка_РК',
                 'ДРР, доля', 'Конверсия в корзину, %', 'Конверсия в заказ, %', 'Частота запросов', 'Спрос по ключам', 'Медианная позиция', 'Видимость, %', 'День зрелый']
    campaign_cols = ['Дата', 'ID кампании', 'Артикул WB', 'Артикул продавца', 'Предмет', 'Тип кампании', 'Тип', 'Плейсмент', 'Ставка, ₽', 'Показы', 'Клики',
                     'Заказы_РК', 'Выручка_РК', 'Расходы_РК', 'ДРР кампании, доля', 'ВП кампании после рекламы, ₽', 'ROI кампании',
                     'CPO кампании, ₽', 'Спрос по ключам', 'Частота запросов', 'День зрелый']

    daily_item = trim_to_columns(daily_item, item_cols)
    daily_campaign = trim_to_columns(daily_campaign, campaign_cols)

    summary = pd.DataFrame([{
        'Режим': 'run',
        'Дата формирования': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'Всего рекомендаций': int(len(decisions)),
        'Изменённых ставок': int(len(decisions[decisions['Действие'].isin(['Повысить', 'Снизить']) & (decisions['Новая ставка, ₽'] != decisions['Текущая ставка, ₽'])])) if not decisions.empty else 0,
        'Блоков отправки ставок': 0 if bid_send_log is None or bid_send_log.empty else int(len(bid_send_log)),
        'PDF менеджерам': int(len(manager_index)) if not manager_index.empty else 0,
        'Текущее окно с': results['window']['cur_start'],
        'Текущее окно по': results['window']['cur_end'],
        'База с': results['window']['base_start'],
        'База по': results['window']['base_end'],
    }])

    old_sheets = {}
    try:
        if provider.file_exists(OUT_SINGLE_REPORT):
            old_sheets = provider.read_excel_all_sheets(OUT_SINGLE_REPORT)
    except Exception:
        old_sheets = {}

    def append_dedup(old_df: pd.DataFrame, new_df: pd.DataFrame, keys: List[str], final_cols: Optional[List[str]] = None) -> pd.DataFrame:
        old_df = old_df.copy() if isinstance(old_df, pd.DataFrame) else pd.DataFrame()
        new_df = new_df.copy() if isinstance(new_df, pd.DataFrame) else pd.DataFrame()
        if final_cols:
            old_df = trim_to_columns(old_df, final_cols)
            new_df = trim_to_columns(new_df, final_cols)
        if new_df.empty:
            base = old_df
        elif old_df.empty:
            base = new_df
        else:
            base = pd.concat([old_df, new_df], ignore_index=True, sort=False)
        real_keys = [k for k in keys if k in base.columns]
        base = base.drop_duplicates(real_keys, keep='last') if real_keys else base
        if final_cols:
            base = trim_to_columns(base, final_cols)
        return base

    decisions_hist = append_dedup(old_sheets.get('История решений день', pd.DataFrame()), history_append, ['Дата запуска', 'ID кампании', 'Артикул WB', 'Плейсмент'])
    item_hist_all = append_dedup(old_sheets.get('История день товар', pd.DataFrame()), daily_item, ['Дата', 'Артикул WB'], item_cols)
    campaign_hist_all = append_dedup(old_sheets.get('История день кампания', pd.DataFrame()), daily_campaign, ['Дата', 'ID кампании', 'Артикул WB'], campaign_cols)
    old_bid_hist = old_sheets.get('История ставок', pd.DataFrame())
    bid_hist_all = append_dedup(old_bid_hist, decisions[decisions['Действие'].isin(['Повысить', 'Снизить']) & (decisions['Новая ставка, ₽'] != decisions['Текущая ставка, ₽'])].copy(), ['ID кампании', 'Артикул WB', 'Плейсмент', 'Дата запуска'])
    archive_all = append_dedup(old_sheets.get('Архив решений', pd.DataFrame()), decisions, ['Дата запуска', 'ID кампании', 'Артикул WB', 'Плейсмент'])

    eff_stub = pd.DataFrame([{'Комментарий': 'Детальная эффективность ставки вынесена в отдельный файл Эффективность_ставки_ежедневно.xlsx'}])

    sheets = {
        'Сводка': summary,
        'Решения': decisions,
        'План': plan_sheet,
        'План категорий': category_plan,
        'Товар день': daily_item,
        'Кампания день': daily_campaign,
        'Товар итог': product_metrics,
        'Кампании прибыль': campaign_profit,
        'Фактически изменённые ставки': bid_send_log if bid_send_log is not None and not bid_send_log.empty else pd.DataFrame([{'Комментарий': 'Нет отправленных блоков ставок'}]),
        'Лимиты ставок': decisions[[c for c in ['Менеджер', 'Артикул продавца', 'ID кампании', 'Тип кампании', 'Статус кампании', 'Активна для API', 'Текущая ставка, ₽', 'Комфортная ставка, ₽', 'Максимальная ставка, ₽', 'Экспериментальная ставка, ₽', 'Тип лимита', 'Причина лимита'] if c in decisions.columns]].copy() if not decisions.empty else pd.DataFrame(),
        'Минимальные ставки WB': min_bids_df if not min_bids_df.empty else pd.DataFrame([{'Комментарий': 'Нет данных по min bid'}]),
        'Слабые позиции': weak,
        'Эффект изменений': effects,
        'Рекомендации менеджерам': manager_index if not manager_index.empty else pd.DataFrame([{'Комментарий': 'PDF не сформированы'}]),
        'Лог API': pd.DataFrame(API_CALL_LOGS) if API_CALL_LOGS else pd.DataFrame([{'Комментарий': 'Нет вызовов API'}]),
        'История решений день': decisions_hist,
        'История ставок': bid_hist_all,
        'Архив решений': archive_all,
        'Эффективность ставки': eff_stub,
        'История день товар': item_hist_all,
        'История день кампания': campaign_hist_all,
    }

    sheets = {name: normalize_output_df(df) for name, df in sheets.items()}
    provider.write_excel(OUT_SINGLE_REPORT, sheets)
    provider.write_excel(OUT_PREVIEW, sheets)
    provider.write_text(OUT_SUMMARY, json.dumps(summary.iloc[0].to_dict(), ensure_ascii=False, default=str, indent=2))

    eff_sheets = results.get('eff_history_sheets', {})
    if not eff_sheets:
        eff_sheets = {'Комментарий': pd.DataFrame([{'Комментарий': 'Нет данных по эффективности ставки'}])}
    eff_sheets = {name: normalize_output_df(df) for name, df in eff_sheets.items()}
    provider.write_excel(OUT_EFF, eff_sheets)



def wb_api_get_request(url: str, api_key: str, params: Dict[str, Any], *, method_name: str, dry_run: bool = False, context: Optional[Dict[str, Any]] = None) -> Optional[requests.Response]:
    if not api_key:
        append_api_log(method_name=method_name, http_method='GET', url=url, request_body=params, response_text='Нет WB_PROMO_KEY_TOPFACE, вызов не выполнен', status='skipped', context=context)
        return None
    if dry_run:
        append_api_log(method_name=method_name, http_method='GET', url=url, request_body=params, response_text='dry-run', status='dry-run', context=context)
        return None
    wait_for_rate_limit(url)
    headers = {'Authorization': api_key.strip()}
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=60)
        _LAST_API_CALL_AT[url] = time.time()
        append_api_log(method_name=method_name, http_method='GET', url=url, request_body=params, response_status=resp.status_code, response_text=resp.text, status='ok' if resp.status_code == 200 else 'failed', context=context)
        return resp
    except Exception as e:
        _LAST_API_CALL_AT[url] = time.time()
        append_api_log(method_name=method_name, http_method='GET', url=url, request_body=params, response_text=str(e), status='failed', context=context)
        return None

def control_campaign_status(campaign_ids: List[int], api_key: str, action: str, dry_run: bool = False) -> pd.DataFrame:
    url = WB_PAUSE_URL if action == 'pause' else WB_START_URL
    method_name = 'Пауза кампании' if action == 'pause' else 'Запуск кампании'
    rows = []
    for campaign_id in campaign_ids:
        resp = wb_api_get_request(url, api_key, {'id': safe_int(campaign_id)}, method_name=method_name, dry_run=dry_run, context={'advert_id': safe_int(campaign_id)})
        rows.append({
            'timestamp': now_ts(),
            'advert_id': safe_int(campaign_id),
            'action': action,
            'status': 'dry-run' if dry_run and api_key else ('skipped' if not api_key else ('ok' if resp is not None and resp.status_code == 200 else 'failed')),
            'http_status': resp.status_code if resp is not None else '',
            'response': truncate_text(resp.text if resp is not None else ('dry-run' if api_key else 'Нет WB_PROMO_KEY_TOPFACE'), 4000),
        })
    return pd.DataFrame(rows)

def save_manual_status_log(provider: BaseProvider, log_df: pd.DataFrame) -> None:
    if log_df is None or log_df.empty:
        return
    key = SERVICE_ROOT + 'Управление_статусом_кампаний.xlsx'
    old = pd.DataFrame()
    try:
        if provider.file_exists(key):
            old = provider.read_excel(key)
    except Exception:
        old = pd.DataFrame()
    out = pd.concat([old, log_df], ignore_index=True) if not old.empty else log_df.copy()
    provider.write_excel(key, {'Управление статусом': out})

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description='TOPFACE WB Ads Manager')
    p.add_argument('mode', nargs='?', default='run', choices=['run','pause','start'])
    p.add_argument('--local-data-dir', default=None)
    p.add_argument('--as-of-date', default=None)
    p.add_argument('--campaign-ids', default='', help='Список ID кампаний через запятую для pause/start')
    p.add_argument('--dry-run', action='store_true', help='Не отправлять реальные изменения')
    return p

def run_manager(args: argparse.Namespace) -> None:
    API_CALL_LOGS.clear()
    MIN_BID_ROWS.clear()
    provider = choose_provider(args.local_data_dir)
    api_key = os.getenv('WB_PROMO_KEY_TOPFACE','').strip()

    if args.mode in {'pause','start'}:
        campaign_ids = [safe_int(x) for x in re.split(r'[;,\s]+', str(args.campaign_ids or '').strip()) if safe_int(x) > 0]
        if not campaign_ids:
            raise SystemExit('Не переданы campaign-ids для pause/start')
        log_df = control_campaign_status(campaign_ids, api_key, args.mode, dry_run=args.dry_run)
        save_manual_status_log(provider, log_df)
        print(log_df.to_string(index=False), flush=True)
        return

    abc_ref, abc_rates = load_latest_abc_reference(provider)
    funnel_ref, funnel_rates = load_latest_funnel_subject_reference(provider)
    globals()['ABC_REFERENCE_DF'] = abc_ref
    globals()['FUNNEL_SUBJECT_REFERENCE_DF'] = funnel_ref

    merged_rates = dict(SUBJECT_FIXED_BUYOUT_RATES)
    if abc_rates:
        merged_rates.update(abc_rates)
    if funnel_rates:
        merged_rates.update(funnel_rates)
    globals()['SUBJECT_FIXED_BUYOUT_RATES'] = merged_rates

    if merged_rates:
        target_rate_text = ', '.join(
            f"{get_subject_display_name(k)}={v*100:.1f}%"
            for k, v in sorted(merged_rates.items())
            if k in TARGET_SUBJECTS
        )
        all_subjects_loaded = len([k for k in merged_rates.keys() if str(k).strip()])
        source_parts = []
        if abc_rates:
            source_parts.append(f'ABC={len(abc_rates)}')
        if funnel_rates:
            source_parts.append(f'Воронка={len(funnel_rates)}')
        source_text = ', '.join(source_parts) if source_parts else 'fallback'
        log(f'📦 Выкуп по всем предметам загружен: {all_subjects_loaded} предметов ({source_text})')
        if target_rate_text:
            log(f'📦 Целевые категории: {target_rate_text}')

    as_of_date = datetime.strptime(args.as_of_date, '%Y-%m-%d').date() if args.as_of_date else datetime.now().date()
    cfg = Config()
    results = prepare_metrics(provider, cfg, as_of_date)
    results = enrich_with_min_bids(results, api_key)
    decisions = results['decisions'].copy()
    log(f'✅ Всего строк решений: {len(decisions)}')
    changed = decisions[decisions['Действие'].isin(['Повысить','Снизить']) & (decisions['Новая ставка, ₽'] != decisions['Текущая ставка, ₽'])].copy()
    log(f'🔁 Изменённых ставок: {len(changed)}')
    if not changed.empty:
        print(changed[['Товарный блок','Артикул продавца','Предмет','ID кампании','Плейсмент','Текущая ставка, ₽','Новая ставка, ₽','Действие','Рекомендация по статусу','Причина']].head(50).to_string(index=False), flush=True)
    payload = decisions_to_payload(decisions)
    bid_send_log = send_payload(payload, api_key, dry_run=args.dry_run)
    log(f"📤 Отправлено блоков в WB: {0 if bid_send_log is None or bid_send_log.empty else len(bid_send_log)}")
    history_append = build_history_append(decisions, as_of_date)
    save_outputs(provider, results, 'run', bid_send_log, None, history_append)

def main() -> None:
    args = build_parser().parse_args()
    run_manager(args)

if __name__ == '__main__':
    main()
