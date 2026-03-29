#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
WB Ads Manager V2

Новая версия алгоритма управления рекламой WB для TOPFACE.
Основная цель — рост маркетинговой валовой прибыли при одновременной
поддержке роста общих заказов и позиций товара.

Что умеет:
- загружать данные из Yandex Object Storage или из локальной папки;
- анализировать рекламу, экономику, заказы, воронку и weekly-поисковые запросы;
- считать blended DRR на уровне товара (product_root) и экономику на уровне SKU;
- считать comfort / max / experiment bid;
- считать эффективность ставки с нормировкой на спрос;
- выдавать решения UP / DOWN / HOLD / TEST_UP / LIMIT_REACHED;
- формировать подробный preview-файл с расчётами;
- в live-режиме отправлять ставки в WB и обновлять историю.

Важно:
1. По умолчанию используйте режим preview.
2. Live-режим предназначен для запуска только после проверки рекомендаций.
"""

from __future__ import annotations

import argparse
import io
import json
import math
import os
import re
import time
from dataclasses import dataclass, field, asdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import boto3
import pandas as pd
import requests
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError
from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter

# ======================================================================================
# КОНСТАНТЫ
# ======================================================================================
STORE_NAME = "TOPFACE"
TIMEZONE = "Europe/Moscow"

TARGET_SUBJECTS = {
    "кисти косметические",
    "блески",
    "помады",
    "косметические карандаши",
}

GROWTH_SUBJECTS = {
    "блески",
    "помады",
    "косметические карандаши",
}

ADS_ANALYSIS_KEY = f"Отчёты/Реклама/{STORE_NAME}/Анализ рекламы.xlsx"
ECONOMICS_KEY = f"Отчёты/Финансовые показатели/{STORE_NAME}/Экономика.xlsx"
FUNNEL_KEY = f"Отчёты/Воронка продаж/{STORE_NAME}/Воронка продаж.xlsx"
ORDERS_WEEKLY_PREFIX = f"Отчёты/Заказы/{STORE_NAME}/Недельные/"
KEYWORDS_WEEKLY_PREFIX = f"Отчёты/Поисковые запросы/{STORE_NAME}/Недельные/"
ADS_HISTORY_KEY = f"Служебные файлы/Ассистент WB/{STORE_NAME}/История_рекламы_14дней.xlsx"

SERVICE_ROOT = f"Служебные файлы/Ассистент WB/{STORE_NAME}/"
SERVICE_PREVIEW_KEY = SERVICE_ROOT + "preview_last_run.xlsx"
SERVICE_SUMMARY_KEY = SERVICE_ROOT + "last_run_summary.json"
SERVICE_ARCHIVE_KEY = SERVICE_ROOT + "decision_archive.xlsx"
SERVICE_BID_HISTORY_KEY = SERVICE_ROOT + "bid_history.xlsx"
SERVICE_LIMITS_KEY = SERVICE_ROOT + "bid_limits_daily.xlsx"
SERVICE_PRODUCT_KEY = SERVICE_ROOT + "product_root_metrics.xlsx"
SERVICE_EFF_KEY = SERVICE_ROOT + "bid_efficiency_daily.xlsx"
SERVICE_WEAK_KEY = SERVICE_ROOT + "weak_position_priority.xlsx"
SERVICE_EFFECTS_KEY = SERVICE_ROOT + "change_effects.xlsx"
SERVICE_EXPERIMENTS_KEY = SERVICE_ROOT + "bid_experiments.xlsx"

WB_BIDS_URL = "https://advert-api.wildberries.ru/api/advert/v1/bids"
WB_BIDS_MIN_URL = "https://advert-api.wildberries.ru/api/advert/v1/bids/min"

ACTIVE_STATUS_VALUES = {"активна", "active", "запущена", "started", "4", "9", "11"}

MIN_CPC_RUB = 4.0
MAX_CPC_RUB = 150.0
MIN_CPM_SEARCH_RUB = 40.0
MAX_CPM_SEARCH_RUB = 700.0
MIN_CPM_RECOMMENDATIONS_RUB = 80.0
MAX_CPM_RECOMMENDATIONS_RUB = 1200.0

MIN_RATING = 4.5
GOOD_RATING = 4.7
MIN_BUYOUT = 0.75
GOOD_BUYOUT = 0.85

# Шаги изменения ставок
UP_STEP_SMALL = 0.05
UP_STEP_MED = 0.08
UP_STEP_BIG = 0.10
DOWN_STEP_SMALL = 0.05
DOWN_STEP_MED = 0.08

# Зрелое окно D-7..D-3
MATURE_START_OFFSET_DAYS = 7
MATURE_END_OFFSET_DAYS = 3
WINDOW_LEN_DAYS = 5

# Ограничения blended DRR
COMFORT_BLENDED_DRR = 0.10
NORMAL_BLENDED_DRR_MAX = 0.12
GROWTH_BLENDED_DRR_MAX = 0.15
WEEKEND_EXPERIMENT_DRR_MAX = 0.20
BRUSHES_DRR_MAX = 0.12

# Ограничения рекламного ДРР для прямой атрибуции
AD_DRR_CAP = 0.15

# Ограничение на расширение max ставки через blended-логику
EXPANSION_CAP = 2.5

# Hard cap для эксперимента
HARD_CAP_CPC_MULT = 1.40
HARD_CAP_CPM_MULT = 1.35

DEFAULT_CONFIG = {
    "comfort_drr": 0.10,
    "default_blended_drr_max": 0.12,
    "growth_blended_drr_max": 0.15,
    "weekend_experiment_drr_max": 0.20,
    "ad_drr_cap": 0.15,
    "expansion_cap": 2.5,
    "max_experiment_days_per_year": 2,
    "experiment_weekdays": [5, 6],  # Saturday, Sunday
    "preview_filename": "preview_last_run.xlsx",
}


# ======================================================================================
# УТИЛИТЫ
# ======================================================================================
def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def norm_col(s: str) -> str:
    s = str(s).strip().lower().replace("ё", "е")
    s = re.sub(r"\s+", " ", s)
    return s


def safe_float(v: Any, default: float = 0.0) -> float:
    if pd.isna(v):
        return default
    try:
        if isinstance(v, str):
            v = v.replace("\xa0", " ").replace("%", "").replace(",", ".").strip()
            if v == "":
                return default
        return float(v)
    except Exception:
        return default


def safe_int(v: Any, default: int = 0) -> int:
    try:
        if pd.isna(v):
            return default
        if isinstance(v, str):
            v = v.replace("\xa0", " ").replace(",", ".").strip()
        return int(float(v))
    except Exception:
        return default


def pct(numerator: float, denominator: float, scale: float = 100.0) -> float:
    if denominator in (0, None) or pd.isna(denominator):
        return 0.0
    return safe_float(numerator) / safe_float(denominator) * scale


def product_root_from_supplier_article(value: Any) -> str:
    s = str(value).strip()
    if not s or s.lower() in {"nan", "none"}:
        return ""
    root = s.split("/")[0].strip()
    root = re.sub(r"[^0-9A-Za-zА-Яа-я_-]+", "", root)
    root = re.sub(r"_+$", "", root)
    root = re.sub(r"-+$", "", root)
    return root.upper()


def canonical_subject(value: Any) -> str:
    return str(value).strip().lower()


def daterange(start: date, end: date) -> Iterable[date]:
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def current_local_date() -> date:
    return datetime.now().date()


def iso_week_label(dt: date) -> str:
    y, w, _ = dt.isocalendar()
    return f"{y}-W{w:02d}"


def clamp(x: float, low: float, high: float) -> float:
    return max(low, min(high, x))


def is_weekend(dt: date) -> bool:
    return dt.weekday() >= 5


def to_rub(value: float) -> float:
    return round(safe_float(value), 2)


def to_kopecks(value_rub: float) -> int:
    return int(round(safe_float(value_rub) * 100))


def from_kopecks(value_kop: Any) -> float:
    return round(safe_float(value_kop) / 100.0, 2)


def choose_first_existing(paths: List[Path]) -> Optional[Path]:
    for p in paths:
        if p.exists():
            return p
    return None


# ======================================================================================
# STORAGE / PROVIDERS
# ======================================================================================
class BaseProvider:
    def read_excel(self, key: str, sheet_name: Any = 0) -> pd.DataFrame:
        raise NotImplementedError

    def read_excel_all_sheets(self, key: str) -> Dict[str, pd.DataFrame]:
        raise NotImplementedError

    def read_text(self, key: str) -> str:
        raise NotImplementedError

    def write_text(self, key: str, text: str) -> None:
        raise NotImplementedError

    def write_excel(self, key: str, sheets: Dict[str, pd.DataFrame]) -> None:
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

    def file_exists(self, key: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False

    def read_bytes(self, key: str) -> bytes:
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        return obj["Body"].read()

    def read_text(self, key: str) -> str:
        return self.read_bytes(key).decode("utf-8")

    def read_excel(self, key: str, sheet_name: Any = 0) -> pd.DataFrame:
        data = self.read_bytes(key)
        return pd.read_excel(io.BytesIO(data), sheet_name=sheet_name)

    def read_excel_all_sheets(self, key: str) -> Dict[str, pd.DataFrame]:
        data = self.read_bytes(key)
        xls = pd.ExcelFile(io.BytesIO(data))
        return {sh: pd.read_excel(io.BytesIO(data), sheet_name=sh) for sh in xls.sheet_names}

    def write_text(self, key: str, text: str) -> None:
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=text.encode("utf-8"))

    def write_excel(self, key: str, sheets: Dict[str, pd.DataFrame]) -> None:
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp_path = Path(tmp.name)
        try:
            with pd.ExcelWriter(tmp_path, engine="openpyxl") as writer:
                for sh, df in sheets.items():
                    (df if df is not None else pd.DataFrame()).to_excel(writer, sheet_name=sh[:31], index=False)
            style_workbook(tmp_path)
            self.s3.put_object(Bucket=self.bucket, Key=key, Body=tmp_path.read_bytes())
        finally:
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass

    def list_keys(self, prefix: str) -> List[str]:
        out: List[str] = []
        token = None
        while True:
            kwargs = {"Bucket": self.bucket, "Prefix": prefix, "MaxKeys": 1000}
            if token:
                kwargs["ContinuationToken"] = token
            resp = self.s3.list_objects_v2(**kwargs)
            for item in resp.get("Contents", []):
                out.append(item["Key"])
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        return out


class LocalProvider(BaseProvider):
    """Локальный режим для тестов по файлам из /mnt/data."""

    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)
        if not self.base_dir.exists():
            raise FileNotFoundError(base_dir)

    def _resolve(self, key: str) -> Path:
        # Если ключ выглядит как реальный локальный путь.
        p = Path(key)
        if p.exists():
            return p

        # Маппинг известных ключей на локальные загруженные файлы.
        mappings: List[Tuple[str, List[str]]] = [
            (ADS_ANALYSIS_KEY, [r"^Анализ рекламы.*\.xlsx$"]),
            (ECONOMICS_KEY, [r"^Экономика.*\.xlsx$"]),
            (FUNNEL_KEY, [r"^Воронка продаж.*\.xlsx$"]),
            (ADS_HISTORY_KEY, [r"^История_рекламы.*\.xlsx$"]),
            (SERVICE_BID_HISTORY_KEY, [r"^bid_history.*\.xlsx$"]),
            (SERVICE_PREVIEW_KEY, [r"^preview_last_run.*\.xlsx$"]),
            (SERVICE_SUMMARY_KEY, [r"^last_run_summary.*\.json$"]),
            (SERVICE_ARCHIVE_KEY, [r"^decision_archive.*\.xlsx$"]),
        ]
        name = None
        for logical, patterns in mappings:
            if key == logical:
                for child in self.base_dir.iterdir():
                    for pat in patterns:
                        if re.search(pat, child.name, flags=re.I):
                            return child
                name = Path(key).name
                break
        if name is None:
            name = Path(key).name
        return self.base_dir / name

    def _search(self, patterns: List[str]) -> List[Path]:
        res = []
        for child in self.base_dir.iterdir():
            for pat in patterns:
                if re.search(pat, child.name, flags=re.I):
                    res.append(child)
                    break
        return sorted(res)

    def file_exists(self, key: str) -> bool:
        return self._resolve(key).exists()

    def read_text(self, key: str) -> str:
        return self._resolve(key).read_text(encoding="utf-8")

    def read_excel(self, key: str, sheet_name: Any = 0) -> pd.DataFrame:
        return pd.read_excel(self._resolve(key), sheet_name=sheet_name)

    def read_excel_all_sheets(self, key: str) -> Dict[str, pd.DataFrame]:
        path = self._resolve(key)
        xls = pd.ExcelFile(path)
        return {sh: pd.read_excel(path, sheet_name=sh) for sh in xls.sheet_names}

    def write_text(self, key: str, text: str) -> None:
        path = self._resolve(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    def write_excel(self, key: str, sheets: Dict[str, pd.DataFrame]) -> None:
        path = self._resolve(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            for sh, df in sheets.items():
                (df if df is not None else pd.DataFrame()).to_excel(writer, sheet_name=sh[:31], index=False)
        style_workbook(path)

    def list_keys(self, prefix: str) -> List[str]:
        if prefix == ORDERS_WEEKLY_PREFIX:
            return [str(p) for p in self._search([r"^Заказы_\d{4}-W\d{2}.*\.xlsx$"])]
        if prefix == KEYWORDS_WEEKLY_PREFIX:
            return [str(p) for p in self._search([r"^Неделя .*\.xlsx$"])]
        # fallback: поиск по имени префикса не нужен в локальном режиме.
        return []


# ======================================================================================
# DATA CONFIG / METADATA
# ======================================================================================
@dataclass
class ManagerConfig:
    comfort_drr: float = DEFAULT_CONFIG["comfort_drr"]
    default_blended_drr_max: float = DEFAULT_CONFIG["default_blended_drr_max"]
    growth_blended_drr_max: float = DEFAULT_CONFIG["growth_blended_drr_max"]
    weekend_experiment_drr_max: float = DEFAULT_CONFIG["weekend_experiment_drr_max"]
    ad_drr_cap: float = DEFAULT_CONFIG["ad_drr_cap"]
    expansion_cap: float = DEFAULT_CONFIG["expansion_cap"]
    max_experiment_days_per_year: int = DEFAULT_CONFIG["max_experiment_days_per_year"]
    experiment_weekdays: List[int] = field(default_factory=lambda: list(DEFAULT_CONFIG["experiment_weekdays"]))
    preview_filename: str = DEFAULT_CONFIG["preview_filename"]


@dataclass
class Decision:
    run_date: str
    id_campaign: int
    nm_id: int
    supplier_article: str
    product_root: str
    subject: str
    placement: str
    payment_type: str
    current_bid_rub: float
    comfort_bid_rub: float
    max_bid_rub: float
    experiment_bid_rub: float
    action: str
    new_bid_rub: float
    reason: str
    mode: str
    current_blended_drr_pct: float
    total_orders: float
    ad_orders: float
    bid_eff_index_imp: float
    bid_eff_index_click: float
    median_position: float
    visibility_pct: float
    demand_week: float
    gp_realized: float
    order_growth_pct: float
    required_order_growth_pct: float
    spend_growth_pct: float
    drr_growth_pp: float
    card_issue: bool
    rate_limit_flag: bool

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ======================================================================================
# EXCEL STYLE
# ======================================================================================
def style_workbook(path: Path) -> None:
    try:
        wb = load_workbook(path)
        header_fill = PatternFill("solid", fgColor="1F4E78")
        header_font = Font(color="FFFFFF", bold=True)
        warn_fill = PatternFill("solid", fgColor="FFF2CC")
        bad_fill = PatternFill("solid", fgColor="F4CCCC")
        good_fill = PatternFill("solid", fgColor="D9EAD3")
        neutral_fill = PatternFill("solid", fgColor="D9EAF7")

        for ws in wb.worksheets:
            ws.freeze_panes = "A2"
            ws.sheet_view.showGridLines = False
            ws.row_dimensions[1].height = 34
            widths: Dict[int, int] = {}

            for cell in ws[1]:
                cell.fill = header_fill
                cell.font = header_font
                cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
                widths[cell.column] = max(14, min(len(str(cell.value or "")) + 4, 55))

            for row_idx, row in enumerate(ws.iter_rows(min_row=2), start=2):
                ws.row_dimensions[row_idx].height = 22
                for cell in row:
                    if cell.value is None:
                        continue
                    header = str(ws.cell(1, cell.column).value or "")
                    header_norm = norm_col(header)
                    val = str(cell.value)
                    widths[cell.column] = max(widths.get(cell.column, 12), min(max(len(val) + 2, len(header) + 2), 55))
                    cell.alignment = Alignment(horizontal="left", vertical="top", wrap_text=True)
                    if isinstance(cell.value, (int, float)):
                        if any(x in header_norm for x in ["ддр", "рост", "видим", "ctr", "cr", "эффектив", "доля", "выкупа"]) or "%" in header:
                            cell.number_format = '0.00'
                        elif any(x in header_norm for x in ["ставк", "расход", "выруч", "прибыл", "цена", "cpc", "cpm"]):
                            cell.number_format = '#,##0.00'
                        else:
                            cell.number_format = '#,##0.00' if abs(float(cell.value)) % 1 else '#,##0'

            for col_idx, width in widths.items():
                ws.column_dimensions[get_column_letter(col_idx)].width = min(max(width, 12), 55)

            header_map = {norm_col(c.value): i + 1 for i, c in enumerate(ws[1]) if c.value is not None}
            action_key = "решение" if "решение" in header_map else "action"
            reason_key = "обоснование" if "обоснование" in header_map else "reason"
            if action_key in header_map:
                cidx = header_map[action_key]
                for r in range(2, ws.max_row + 1):
                    val = str(ws.cell(r, cidx).value or "").lower()
                    if val in {"повысить", "тест выше max", "up", "test_up"}:
                        ws.cell(r, cidx).fill = good_fill
                    elif val in {"снизить", "предел эффективности ставки", "down", "limit_reached"}:
                        ws.cell(r, cidx).fill = bad_fill
                    else:
                        ws.cell(r, cidx).fill = neutral_fill
            if reason_key in header_map:
                cidx = header_map[reason_key]
                for r in range(2, ws.max_row + 1):
                    if "предел" in str(ws.cell(r, cidx).value or "").lower():
                        ws.cell(r, cidx).fill = warn_fill
        wb.save(path)
    except Exception as e:
        log(f"⚠️ Не удалось оформить workbook {path}: {e}")


# ======================================================================================
# ЗАГРУЗКА И ПОДГОТОВКА ДАННЫХ
# ======================================================================================
def load_config(provider: BaseProvider) -> ManagerConfig:
    # Новый алгоритм использует свои дефолты. При желании можно потом подключить отдельный json.
    return ManagerConfig()


def prepare_daily_ads(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    rename = {}
    aliases = {
        "ID кампании": ["ID кампании", "ID", "advert_id"],
        "Артикул WB": ["Артикул WB", "nm_id", "Артикул"],
        "Название": ["Название", "Кампания"],
        "Название предмета": ["Название предмета", "Предмет"],
        "Дата": ["Дата", "date"],
        "Показы": ["Показы", "views"],
        "Клики": ["Клики", "clicks"],
        "CTR": ["CTR"],
        "CPC": ["CPC"],
        "Заказы": ["Заказы", "orders"],
        "CR": ["CR"],
        "Расход": ["Расход", "spent"],
        "ATBS": ["ATBS"],
        "SHKS": ["SHKS"],
        "Сумма заказов": ["Сумма заказов", "Выручка"],
        "Отменено": ["Отменено"],
        "ДРР": ["ДРР"],
    }
    lower_cols = {norm_col(c): c for c in df.columns}
    for tgt, vars_ in aliases.items():
        for v in vars_:
            if norm_col(v) in lower_cols:
                rename[lower_cols[norm_col(v)]] = tgt
                break
    df = df.rename(columns=rename).copy()
    req = ["ID кампании", "Артикул WB", "Название предмета", "Дата", "Показы", "Клики", "Заказы", "Расход", "Сумма заказов"]
    for c in req:
        if c not in df.columns:
            raise ValueError(f"В ежедневной рекламе нет колонки: {c}")
    num_cols = ["ID кампании", "Артикул WB", "Показы", "Клики", "CTR", "CPC", "Заказы", "CR", "Расход", "ATBS", "SHKS", "Сумма заказов", "Отменено", "ДРР"]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df["Дата"] = pd.to_datetime(df["Дата"], errors="coerce").dt.date
    df = df.dropna(subset=["ID кампании", "Артикул WB", "Дата"]).copy()
    df["ID кампании"] = df["ID кампании"].astype(int)
    df["Артикул WB"] = df["Артикул WB"].astype(int)
    df["subject_norm"] = df["Название предмета"].map(canonical_subject)
    return df


def prepare_campaigns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    rename = {}
    aliases = {
        "ID кампании": ["ID кампании", "advert_id"],
        "Название": ["Название"],
        "Статус": ["Статус"],
        "Тип оплаты": ["Тип оплаты"],
        "Тип ставки": ["Тип ставки"],
        "Ставка в поиске (руб)": ["Ставка в поиске (руб)", "Ставка"],
        "Ставка в рекомендациях (руб)": ["Ставка в рекомендациях (руб)"],
        "Название предмета": ["Название предмета", "Предмет"],
        "Артикул WB": ["Артикул WB"],
    }
    lower_cols = {norm_col(c): c for c in df.columns}
    for tgt, vars_ in aliases.items():
        for v in vars_:
            if norm_col(v) in lower_cols:
                rename[lower_cols[norm_col(v)]] = tgt
                break
    df = df.rename(columns=rename).copy()
    req = ["ID кампании", "Статус", "Тип оплаты", "Тип ставки", "Ставка в поиске (руб)", "Ставка в рекомендациях (руб)", "Артикул WB"]
    for c in req:
        if c not in df.columns:
            raise ValueError(f"В списке кампаний нет колонки: {c}")
    for c in ["ID кампании", "Артикул WB"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in ["Ставка в поиске (руб)", "Ставка в рекомендациях (руб)"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    df = df.dropna(subset=["ID кампании", "Артикул WB"]).copy()
    df["ID кампании"] = df["ID кампании"].astype(int)
    df["Артикул WB"] = df["Артикул WB"].astype(int)
    df["subject_norm"] = df["Название предмета"].map(canonical_subject)
    df["status_norm"] = df["Статус"].map(canonical_subject)
    df["payment_type"] = df["Тип оплаты"].map(canonical_subject)
    df["bid_type"] = df["Тип ставки"].map(canonical_subject)
    df = df[df["status_norm"].isin(ACTIVE_STATUS_VALUES)].copy()
    return df


def derive_campaign_placements(campaigns_df: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for _, r in campaigns_df.iterrows():
        advert_id = safe_int(r["ID кампании"])
        nm_id = safe_int(r["Артикул WB"])
        payment_type = str(r.get("payment_type", "")).lower()
        bid_type = str(r.get("bid_type", "")).lower()
        search_bid = safe_float(r.get("Ставка в поиске (руб)", 0))
        rec_bid = safe_float(r.get("Ставка в рекомендациях (руб)", 0))
        subject = str(r.get("Название предмета", ""))
        if payment_type == "cpc":
            rows.append({
                "ID кампании": advert_id,
                "Артикул WB": nm_id,
                "placement": "search",
                "payment_type": payment_type,
                "bid_type": bid_type,
                "current_bid_rub": search_bid,
                "Название предмета": subject,
            })
            continue
        if bid_type == "unified":
            rows.append({
                "ID кампании": advert_id,
                "Артикул WB": nm_id,
                "placement": "combined",
                "payment_type": payment_type,
                "bid_type": bid_type,
                "current_bid_rub": max(search_bid, rec_bid),
                "Название предмета": subject,
            })
        else:
            if search_bid > 0:
                rows.append({
                    "ID кампании": advert_id,
                    "Артикул WB": nm_id,
                    "placement": "search",
                    "payment_type": payment_type,
                    "bid_type": bid_type,
                    "current_bid_rub": search_bid,
                    "Название предмета": subject,
                })
            if rec_bid > 0:
                rows.append({
                    "ID кампании": advert_id,
                    "Артикул WB": nm_id,
                    "placement": "recommendations",
                    "payment_type": payment_type,
                    "bid_type": bid_type,
                    "current_bid_rub": rec_bid,
                    "Название предмета": subject,
                })
    return pd.DataFrame(rows)


def load_advertising(provider: BaseProvider) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    sheets = provider.read_excel_all_sheets(ADS_ANALYSIS_KEY)
    daily = prepare_daily_ads(sheets.get("Статистика_Ежедневно", pd.DataFrame()))
    campaigns = prepare_campaigns(sheets.get("Список_кампаний", pd.DataFrame()))
    placements = derive_campaign_placements(campaigns)
    daily = daily[daily["subject_norm"].isin(TARGET_SUBJECTS)].copy()
    campaigns = campaigns[campaigns["subject_norm"].isin(TARGET_SUBJECTS)].copy()
    placements = placements[placements["Название предмета"].map(canonical_subject).isin(TARGET_SUBJECTS)].copy()
    return daily, campaigns, placements


def load_economics(provider: BaseProvider) -> pd.DataFrame:
    df = provider.read_excel(ECONOMICS_KEY, sheet_name="Юнит экономика")
    df = df.copy()
    df["Артикул WB"] = pd.to_numeric(df["Артикул WB"], errors="coerce")
    df = df.dropna(subset=["Артикул WB"]).copy()
    df["Артикул WB"] = df["Артикул WB"].astype(int)
    df["subject_norm"] = df["Предмет"].map(canonical_subject)
    df = df[df["subject_norm"].isin(TARGET_SUBJECTS)].copy()
    for c in [
        "Процент выкупа", "Средняя цена продажи", "Средняя цена покупателя", "Валовая прибыль, руб/ед", "Чистая прибыль, руб/ед",
        "Валовая рентабельность, %", "Чистая рентабельность, %",
    ]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["week_sort"] = df["Неделя"].astype(str)
    df = df.sort_values(["Артикул WB", "week_sort"]).copy()
    latest = df.groupby("Артикул WB", as_index=False).tail(1).copy()
    latest["buyout_rate"] = latest["Процент выкупа"].fillna(0) / 100.0
    latest["gp_realized"] = latest["Валовая прибыль, руб/ед"].fillna(0) * latest["buyout_rate"]
    latest["np_realized"] = latest["Чистая прибыль, руб/ед"].fillna(0) * latest["buyout_rate"]
    latest["supplier_article"] = latest["Артикул продавца"].astype(str).str.strip()
    latest["product_root"] = latest["supplier_article"].map(product_root_from_supplier_article)
    return latest


def _extract_dates_from_orders(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["nmId"] = pd.to_numeric(df["nmId"], errors="coerce")
    df = df.dropna(subset=["date", "nmId"]).copy()
    df["nmId"] = df["nmId"].astype(int)
    df["subject_norm"] = df["subject"].map(canonical_subject)
    df["supplierArticle"] = df["supplierArticle"].astype(str).str.strip()
    df["product_root"] = df["supplierArticle"].map(product_root_from_supplier_article)
    df["finishedPrice"] = pd.to_numeric(df.get("finishedPrice", 0), errors="coerce").fillna(0.0)
    is_cancel = df.get("isCancel")
    if is_cancel is None:
        df["isCancel"] = False
    else:
        df["isCancel"] = is_cancel.fillna(False).astype(str).str.lower().isin({"true", "1", "yes", "да"})
    return df


def load_orders(provider: BaseProvider, start_date: date, end_date: date) -> pd.DataFrame:
    keys = provider.list_keys(ORDERS_WEEKLY_PREFIX)
    frames: List[pd.DataFrame] = []
    for key in keys:
        try:
            df = provider.read_excel(key, sheet_name=0)
            df = _extract_dates_from_orders(df)
            df = df[(df["date"] >= start_date) & (df["date"] <= end_date)].copy()
            if not df.empty:
                frames.append(df)
        except Exception as e:
            log(f"⚠️ Не удалось прочитать файл заказов {key}: {e}")
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out = out[out["subject_norm"].isin(TARGET_SUBJECTS)].copy()
    return out


def load_funnel(provider: BaseProvider) -> pd.DataFrame:
    if not provider.file_exists(FUNNEL_KEY):
        # В локальном режиме файл может лежать под загруженным именем.
        try:
            df = provider.read_excel(FUNNEL_KEY, sheet_name=0)
        except Exception:
            return pd.DataFrame()
    else:
        df = provider.read_excel(FUNNEL_KEY, sheet_name=0)
    df = df.copy()
    rename = {"nmID": "nmId", "dt": "date"}
    df = df.rename(columns=rename)
    if "nmId" not in df.columns or "date" not in df.columns:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["nmId"] = pd.to_numeric(df["nmId"], errors="coerce")
    df = df.dropna(subset=["date", "nmId"]).copy()
    df["nmId"] = df["nmId"].astype(int)
    for c in [
        "openCardCount", "addToCartCount", "ordersCount", "buyoutsCount", "cancelCount",
        "addToCartConversion", "cartToOrderConversion", "buyoutPercent"
    ]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    return df


def load_keywords_latest(provider: BaseProvider, stable_end: date) -> pd.DataFrame:
    keys = provider.list_keys(KEYWORDS_WEEKLY_PREFIX)
    if not keys:
        return pd.DataFrame()

    # Берём файл с последней полной ISO-неделей относительно stable_end.
    wanted_week = iso_week_label(stable_end)
    selected_key = None
    for k in sorted(keys, reverse=True):
        if wanted_week in str(k):
            selected_key = k
            break
    if selected_key is None:
        selected_key = sorted(keys)[-1]
    try:
        df = provider.read_excel(selected_key, sheet_name=0)
    except Exception as e:
        log(f"⚠️ Не удалось прочитать keywords {selected_key}: {e}")
        return pd.DataFrame()
    df = df.copy()
    if "Артикул WB" not in df.columns:
        return pd.DataFrame()
    df["Артикул WB"] = pd.to_numeric(df["Артикул WB"], errors="coerce")
    df = df.dropna(subset=["Артикул WB"]).copy()
    df["Артикул WB"] = df["Артикул WB"].astype(int)
    df["subject_norm"] = df.get("Предмет", "").map(canonical_subject)
    df = df[df["subject_norm"].isin(TARGET_SUBJECTS)].copy()
    for c in [
        "Рейтинг отзывов", "Частота запросов", "Частота за неделю", "Медианная позиция", "Средняя позиция",
        "Переходы в карточку", "Заказы", "Конверсия в заказ %", "Видимость %", "Рейтинг карточки"
    ]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    return df


def load_ads_history(provider: BaseProvider) -> pd.DataFrame:
    try:
        df = provider.read_excel(ADS_HISTORY_KEY, sheet_name=0)
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return df
    if "Дата" in df.columns:
        df["Дата"] = pd.to_datetime(df["Дата"], errors="coerce").dt.date
    if "Дата запроса" in df.columns:
        df["Дата запроса"] = pd.to_datetime(df["Дата запроса"], errors="coerce").dt.date
    return df


def load_bid_history(provider: BaseProvider) -> pd.DataFrame:
    try:
        df = provider.read_excel(SERVICE_BID_HISTORY_KEY, sheet_name=0)
        if not df.empty and "Дата запуска" in df.columns:
            df["Дата запуска"] = pd.to_datetime(df["Дата запуска"], errors="coerce")
        return df
    except Exception:
        return pd.DataFrame(columns=["Дата запуска", "Неделя", "ID кампании", "Артикул WB", "Тип кампании", "Ставка поиск, коп", "Ставка рекомендации, коп", "Стратегия"])


# ======================================================================================
# AGGREGATIONS
# ======================================================================================
@dataclass
class AnalysisWindow:
    current_start: date
    current_end: date
    prev_start: date
    prev_end: date


def compute_analysis_window(as_of_date: date) -> AnalysisWindow:
    current_end = as_of_date - timedelta(days=MATURE_END_OFFSET_DAYS)
    current_start = as_of_date - timedelta(days=MATURE_START_OFFSET_DAYS)
    prev_end = current_start - timedelta(days=1)
    prev_start = prev_end - timedelta(days=(WINDOW_LEN_DAYS - 1))
    return AnalysisWindow(current_start=current_start, current_end=current_end, prev_start=prev_start, prev_end=prev_end)


def aggregate_ads_window(daily_ads: pd.DataFrame, start: date, end: date) -> pd.DataFrame:
    df = daily_ads[(daily_ads["Дата"] >= start) & (daily_ads["Дата"] <= end)].copy()
    if df.empty:
        return pd.DataFrame(columns=["ID кампании", "Артикул WB"])
    grouped = df.groupby(["ID кампании", "Артикул WB"], as_index=False).agg({
        "Название": "last",
        "Название предмета": "last",
        "Показы": "sum",
        "Клики": "sum",
        "Заказы": "sum",
        "Расход": "sum",
        "Сумма заказов": "sum",
    })
    grouped["ctr"] = grouped.apply(lambda x: pct(x["Клики"], x["Показы"]), axis=1)
    grouped["cr"] = grouped.apply(lambda x: pct(x["Заказы"], x["Клики"]), axis=1)
    grouped["cpc"] = grouped.apply(lambda x: safe_float(x["Расход"]) / safe_float(x["Клики"]) if safe_float(x["Клики"]) > 0 else 0.0, axis=1)
    grouped["cpo"] = grouped.apply(lambda x: safe_float(x["Расход"]) / safe_float(x["Заказы"]) if safe_float(x["Заказы"]) > 0 else 0.0, axis=1)
    grouped["ad_drr"] = grouped.apply(lambda x: pct(x["Расход"], x["Сумма заказов"]), axis=1)
    grouped["subject_norm"] = grouped["Название предмета"].map(canonical_subject)
    return grouped


def aggregate_orders_window(orders: pd.DataFrame, start: date, end: date) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if orders.empty:
        empty_root = pd.DataFrame(columns=["product_root"])
        empty_nm = pd.DataFrame(columns=["nmId"])
        return empty_root, empty_nm
    df = orders[(orders["date"] >= start) & (orders["date"] <= end) & (~orders["isCancel"])].copy()
    if df.empty:
        empty_root = pd.DataFrame(columns=["product_root"])
        empty_nm = pd.DataFrame(columns=["nmId"])
        return empty_root, empty_nm

    root_df = df.groupby(["product_root", "subject_norm"], as_index=False).agg(
        total_orders=("nmId", "count"),
        total_revenue=("finishedPrice", "sum"),
        sku_count=("nmId", "nunique"),
    )
    nm_df = df.groupby(["nmId", "supplierArticle", "product_root", "subject_norm"], as_index=False).agg(
        total_orders=("nmId", "count"),
        total_revenue=("finishedPrice", "sum"),
        avg_price=("finishedPrice", "mean"),
    )
    return root_df, nm_df


def aggregate_funnel_window(funnel: pd.DataFrame, start: date, end: date) -> pd.DataFrame:
    if funnel.empty:
        return pd.DataFrame(columns=["nmId"])
    df = funnel[(funnel["date"] >= start) & (funnel["date"] <= end)].copy()
    if df.empty:
        return pd.DataFrame(columns=["nmId"])
    res = df.groupby("nmId", as_index=False).agg(
        openCardCount=("openCardCount", "sum"),
        addToCartCount=("addToCartCount", "sum"),
        ordersCount=("ordersCount", "sum"),
        buyoutsCount=("buyoutsCount", "sum"),
        cancelCount=("cancelCount", "sum"),
    )
    res["addToCartConversion"] = res.apply(lambda x: pct(x["addToCartCount"], x["openCardCount"]), axis=1)
    res["cartToOrderConversion"] = res.apply(lambda x: pct(x["ordersCount"], x["addToCartCount"]), axis=1)
    res["buyoutPercent"] = res.apply(lambda x: pct(x["buyoutsCount"], x["ordersCount"]), axis=1)
    return res


def aggregate_keywords(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["Артикул WB"])
    rows = []
    for nm_id, g in df.groupby("Артикул WB"):
        demand = safe_float(g["Частота за неделю"].sum() if "Частота за неделю" in g.columns else g["Частота запросов"].sum())
        traffic = safe_float(g["Переходы в карточку"].sum()) if "Переходы в карточку" in g.columns else 0.0
        orders = safe_float(g["Заказы"].sum()) if "Заказы" in g.columns else 0.0
        rating = safe_float(g["Рейтинг отзывов"].replace(0, pd.NA).mean()) if "Рейтинг отзывов" in g.columns else 0.0
        card_rating = safe_float(g["Рейтинг карточки"].replace(0, pd.NA).mean()) if "Рейтинг карточки" in g.columns else 0.0
        visibility = safe_float(g["Видимость %"].replace(0, pd.NA).mean()) if "Видимость %" in g.columns else 0.0
        if orders > 0 and "Медианная позиция" in g.columns:
            weighted_pos = (g["Медианная позиция"] * g["Заказы"].replace(0, 1)).sum() / g["Заказы"].replace(0, 1).sum()
        else:
            weighted_pos = safe_float(g["Медианная позиция"].replace(0, pd.NA).mean()) if "Медианная позиция" in g.columns else 0.0
        rows.append({
            "Артикул WB": int(nm_id),
            "rating_reviews": rating,
            "rating_card": card_rating,
            "demand_week": demand,
            "keyword_clicks": traffic,
            "keyword_orders": orders,
            "median_position": weighted_pos,
            "visibility_pct": visibility,
        })
    return pd.DataFrame(rows)


# ======================================================================================
# MODE / CAPS / EFFICIENCY
# ======================================================================================
def classify_product_mode(row: pd.Series, subject_funnel_medians: pd.DataFrame) -> Tuple[str, bool]:
    subject = canonical_subject(row.get("subject_norm", row.get("subject", "")))
    gp_realized = safe_float(row.get("gp_realized", 0))
    rating = safe_float(row.get("rating_reviews", 0))
    buyout_rate = safe_float(row.get("buyout_rate", 0))
    pos = safe_float(row.get("median_position", 0))
    visibility = safe_float(row.get("visibility_pct", 0))
    root_orders = safe_float(row.get("root_total_orders_current", row.get("total_orders_current", 0)))
    blended_drr = safe_float(row.get("blended_drr_current_pct", 0))
    atc = safe_float(row.get("addToCartConversion", 0))
    cto = safe_float(row.get("cartToOrderConversion", 0))

    severe_card_issue = False
    card_issue = False
    med = subject_funnel_medians[subject_funnel_medians["subject_norm"] == subject]
    if not med.empty:
        med_atc = safe_float(med.iloc[0].get("addToCartConversion_median", 0))
        med_cto = safe_float(med.iloc[0].get("cartToOrderConversion_median", 0))
        if med_atc > 0 and atc > 0 and atc < med_atc * 0.70:
            card_issue = True
        if med_cto > 0 and cto > 0 and cto < med_cto * 0.70:
            card_issue = True
        if med_atc > 0 and atc > 0 and atc < med_atc * 0.55:
            severe_card_issue = True
        if med_cto > 0 and cto > 0 and cto < med_cto * 0.55:
            severe_card_issue = True

    if gp_realized <= 0 or rating < MIN_RATING or buyout_rate < MIN_BUYOUT:
        return "problem", card_issue

    if subject in GROWTH_SUBJECTS:
        if rating >= GOOD_RATING and root_orders >= 20 and (pos == 0 or pos <= 15) and blended_drr <= 12.5:
            return "hero", card_issue
        if pos == 0 or pos > 20 or visibility < 5 or root_orders < 12 or blended_drr <= 15:
            return "growth", card_issue
        if severe_card_issue:
            return "margin_guard", card_issue
        return "balanced", card_issue

    if severe_card_issue:
        return "margin_guard", card_issue
    if subject == "кисти косметические" and (pos > 20 or visibility < 4):
        return "balanced", card_issue
    return "balanced", card_issue


def get_mode_shares(mode: str) -> Tuple[float, float]:
    if mode in {"hero", "growth"}:
        return 0.50, 0.80
    if mode == "balanced":
        return 0.40, 0.65
    if mode == "margin_guard":
        return 0.30, 0.45
    return 0.15, 0.25


def get_blended_caps(subject: str, config: ManagerConfig) -> Tuple[float, float, float]:
    subject = canonical_subject(subject)
    comfort = config.comfort_drr
    if subject in GROWTH_SUBJECTS:
        return comfort, config.growth_blended_drr_max, config.weekend_experiment_drr_max
    return comfort, BRUSHES_DRR_MAX if subject == "кисти косметические" else config.default_blended_drr_max, config.default_blended_drr_max


def compute_required_growth(current_blended_drr_pct: float, prev_blended_drr_pct: float, spend_growth_pct: float, subject: str = "") -> float:
    """
    Возвращает минимально достаточный рост заказов, который должен сопровождать
    рост общих расходов/ДРР. Для growth-категорий формула мягче, чтобы алгоритм
    не душил развитие из-за низкой базы.
    """
    current_blended_drr_pct = safe_float(current_blended_drr_pct)
    prev_blended_drr_pct = safe_float(prev_blended_drr_pct)
    spend_growth_pct = max(0.0, safe_float(spend_growth_pct))
    drr_growth_pp = max(0.0, current_blended_drr_pct - prev_blended_drr_pct)
    subject = canonical_subject(subject)

    if drr_growth_pp <= 0 and spend_growth_pct <= 0:
        return 0.0

    if current_blended_drr_pct <= 12.0:
        kdrr, kspend, floor, cap, spend_cap = 1.5, 0.25, 2.0, 10.0, 24.0
    elif current_blended_drr_pct <= 15.0:
        kdrr, kspend, floor, cap, spend_cap = 2.0, 0.35, 4.0, 15.0, 30.0
    else:
        kdrr, kspend, floor, cap, spend_cap = 2.5, 0.50, 8.0, 22.0, 40.0

    effective_spend_growth = min(spend_growth_pct, spend_cap)

    if subject in GROWTH_SUBJECTS:
        floor = max(1.5, floor - 1.0)
        cap += 3.0
        effective_spend_growth *= 0.85

    required = max(
        kdrr * drr_growth_pp,
        kspend * effective_spend_growth,
        floor,
    )
    return round(min(required, cap), 2)


def compute_safe_down_bid(current_bid: float, comfort_bid: float, step: float) -> float:
    reduced = current_bid * (1 - step)
    if comfort_bid >= current_bid:
        return round(reduced, 2)
    return round(max(comfort_bid, reduced), 2)


def choose_clicks_per_order(ad_clicks: float, ad_orders: float, total_orders: float, subject: str) -> Tuple[float, float]:
    clicks_per_ad_order = ad_clicks / ad_orders if ad_orders > 0 else math.nan
    clicks_per_total_order = ad_clicks / total_orders if total_orders > 0 else math.nan

    # fallback-ы
    if not math.isfinite(clicks_per_ad_order) or clicks_per_ad_order <= 0:
        clicks_per_ad_order = 12.0 if subject in GROWTH_SUBJECTS else 10.0
    if not math.isfinite(clicks_per_total_order) or clicks_per_total_order <= 0:
        clicks_per_total_order = 6.0 if subject in GROWTH_SUBJECTS else 8.0
    return clicks_per_ad_order, clicks_per_total_order


def compute_bid_caps(row: pd.Series, mode: str, config: ManagerConfig) -> Dict[str, float]:
    subject = canonical_subject(row.get("subject_norm", row.get("subject", "")))
    placement = row.get("placement", "search")
    payment_type = row.get("payment_type", "cpc")

    gp_realized = safe_float(row.get("gp_realized", 0))
    ad_clicks = safe_float(row.get("ad_clicks_current", 0))
    ad_impressions = safe_float(row.get("ad_impressions_current", 0))
    ad_orders = safe_float(row.get("ad_orders_current", 0))
    total_orders = safe_float(row.get("total_orders_current", 0))
    ad_revenue = safe_float(row.get("ad_revenue_current", 0))
    total_revenue = safe_float(row.get("total_revenue_current", 0))
    current_bid = safe_float(row.get("current_bid_rub", 0))

    # CTR берём из строки, а если его нет — считаем по факту, затем используем fallback по placement.
    ctr_est = safe_float(row.get("ctr_est", 0)) / 100.0
    if ctr_est <= 0 and ad_impressions > 0 and ad_clicks > 0:
        ctr_est = ad_clicks / ad_impressions
    if ctr_est <= 0:
        ctr_est = 0.018 if placement in {"combined", "search"} else 0.010
    ctr_est = clamp(ctr_est, 0.003, 0.08)

    comfort_share, max_share = get_mode_shares(mode)
    comfort_cpo = gp_realized * comfort_share
    max_cpo = gp_realized * max_share
    _, blended_max, weekend_max = get_blended_caps(subject, config)

    clicks_per_ad_order, clicks_per_total_order = choose_clicks_per_order(ad_clicks, ad_orders, total_orders, subject)

    # CPC капы по экономике
    comfort_cpc_ad_econ = comfort_cpo / clicks_per_ad_order if clicks_per_ad_order > 0 else 0.0
    max_cpc_ad_econ = max_cpo / clicks_per_ad_order if clicks_per_ad_order > 0 else 0.0
    comfort_cpc_total_econ = comfort_cpo / clicks_per_total_order if clicks_per_total_order > 0 else 0.0
    max_cpc_total_econ = max_cpo / clicks_per_total_order if clicks_per_total_order > 0 else 0.0

    # CPC капы по ДРР
    comfort_cpc_total_drr = (total_revenue * config.comfort_drr / ad_clicks) if ad_clicks > 0 else 0.0
    max_cpc_total_drr = (total_revenue * blended_max / ad_clicks) if ad_clicks > 0 else 0.0
    weekend_cpc_total_drr = (total_revenue * weekend_max / ad_clicks) if ad_clicks > 0 else 0.0
    max_cpc_ad_drr = (ad_revenue * config.ad_drr_cap / ad_clicks) if ad_clicks > 0 else 0.0

    comfort_cpc_ad = min(v for v in [comfort_cpc_ad_econ, max(comfort_cpc_total_drr, 0.0)] if v > 0) if any(v > 0 for v in [comfort_cpc_ad_econ, comfort_cpc_total_drr]) else 0.0
    max_cpc_ad = min(v for v in [max_cpc_ad_econ, max_cpc_ad_drr] if v > 0) if any(v > 0 for v in [max_cpc_ad_econ, max_cpc_ad_drr]) else 0.0
    comfort_cpc_total = min(v for v in [comfort_cpc_total_econ, comfort_cpc_total_drr] if v > 0) if any(v > 0 for v in [comfort_cpc_total_econ, comfort_cpc_total_drr]) else 0.0
    max_cpc_total = min(v for v in [max_cpc_total_econ, max_cpc_total_drr] if v > 0) if any(v > 0 for v in [max_cpc_total_econ, max_cpc_total_drr]) else 0.0
    weekend_cpc_total = min(v for v in [max_cpc_total_econ * HARD_CAP_CPC_MULT if max_cpc_total_econ > 0 else 0.0, weekend_cpc_total_drr] if v > 0) if any(v > 0 for v in [max_cpc_total_econ, weekend_cpc_total_drr]) else 0.0

    if subject in GROWTH_SUBJECTS:
        applied_max_cpc = min(max_cpc_total if max_cpc_total > 0 else max_cpc_ad, (max_cpc_ad * config.expansion_cap) if max_cpc_ad > 0 else max_cpc_total)
        applied_comfort_cpc = max(comfort_cpc_ad, min(comfort_cpc_total, applied_max_cpc))
    else:
        applied_max_cpc = max_cpc_ad
        applied_comfort_cpc = comfort_cpc_ad

    experiment_cpc = min(weekend_cpc_total, applied_max_cpc * 1.25 if applied_max_cpc > 0 else weekend_cpc_total)

    def cpc_to_cpm(cpc_val: float) -> float:
        return cpc_val * 1000.0 * ctr_est if cpc_val > 0 else 0.0

    if payment_type == "cpc":
        comfort_bid = applied_comfort_cpc
        max_bid = applied_max_cpc
        experiment_bid = min(experiment_cpc, applied_max_cpc * HARD_CAP_CPC_MULT if applied_max_cpc > 0 else experiment_cpc)
        min_bid = MIN_CPC_RUB
        hard_max = MAX_CPC_RUB
    else:
        comfort_bid = cpc_to_cpm(applied_comfort_cpc)
        max_bid = cpc_to_cpm(applied_max_cpc)
        experiment_bid = cpc_to_cpm(min(experiment_cpc, applied_max_cpc * HARD_CAP_CPM_MULT if applied_max_cpc > 0 else experiment_cpc))
        if placement == "recommendations":
            min_bid = MIN_CPM_RECOMMENDATIONS_RUB
            hard_max = MAX_CPM_RECOMMENDATIONS_RUB
        else:
            min_bid = MIN_CPM_SEARCH_RUB
            hard_max = MAX_CPM_SEARCH_RUB

    comfort_bid = clamp(comfort_bid, min_bid, hard_max) if comfort_bid > 0 else min_bid
    max_bid = clamp(max_bid, min_bid, hard_max) if max_bid > 0 else min_bid
    experiment_bid = clamp(experiment_bid, min_bid, hard_max) if experiment_bid > 0 else max_bid

    # Мягкий защитный cap для CPM, чтобы из формулы не вылетали нереалистичные значения.
    if payment_type != "cpc" and current_bid > 0:
        comfort_bid = min(comfort_bid, max(current_bid * 2.0, min_bid))
        max_bid = min(max_bid, max(current_bid * 2.5, comfort_bid))
        experiment_bid = min(experiment_bid, max_bid * 1.25)

    if max_bid < comfort_bid:
        max_bid = comfort_bid
    if experiment_bid < max_bid:
        experiment_bid = max_bid

    return {
        "clicks_per_ad_order": round(clicks_per_ad_order, 2),
        "clicks_per_total_order": round(clicks_per_total_order, 2),
        "comfort_bid_rub": round(comfort_bid, 2),
        "max_bid_rub": round(max_bid, 2),
        "experiment_bid_rub": round(experiment_bid, 2),
        "comfort_cpo": round(comfort_cpo, 2),
        "max_cpo": round(max_cpo, 2),
        "ctr_est": round(ctr_est * 100.0, 2),
        "applied_max_cpc": round(applied_max_cpc, 2),
        "applied_comfort_cpc": round(applied_comfort_cpc, 2),
    }


def compute_bid_efficiency(row: pd.Series, subject_baselines: pd.DataFrame) -> Dict[str, float]:
    bid = safe_float(row.get("current_bid_rub", 0))
    impressions = safe_float(row.get("ad_impressions_current", 0))
    clicks = safe_float(row.get("ad_clicks_current", 0))
    demand = safe_float(row.get("demand_week", 0))
    subject = canonical_subject(row.get("subject_norm", row.get("subject", "")))
    placement = str(row.get("placement", "search"))

    capture_imp = impressions / demand if demand > 0 else 0.0
    capture_click = clicks / demand if demand > 0 else 0.0
    eff_imp = capture_imp / bid if bid > 0 else 0.0
    eff_click = capture_click / bid if bid > 0 else 0.0

    base = subject_baselines[
        (subject_baselines["subject_norm"] == subject) &
        (subject_baselines["placement"] == placement)
    ]
    base_imp = safe_float(base["eff_imp_median"].iloc[0]) if not base.empty else 0.0
    base_click = safe_float(base["eff_click_median"].iloc[0]) if not base.empty else 0.0

    bei_imp = eff_imp / base_imp if base_imp > 0 else 1.0
    bei_click = eff_click / base_click if base_click > 0 else 1.0
    bei_imp = clamp(bei_imp, 0.0, 5.0)
    bei_click = clamp(bei_click, 0.0, 5.0)
    return {
        "capture_imp": round(capture_imp, 6),
        "capture_click": round(capture_click, 6),
        "eff_imp": round(eff_imp, 8),
        "eff_click": round(eff_click, 8),
        "bei_imp": round(bei_imp, 4),
        "bei_click": round(bei_click, 4),
    }


# ======================================================================================
# EFFECTS / HISTORY
# ======================================================================================
def expand_bid_history_to_placements(history: pd.DataFrame) -> pd.DataFrame:
    if history.empty:
        return pd.DataFrame(columns=["Дата запуска", "ID кампании", "Артикул WB", "placement", "bid_rub"])
    rows = []
    for _, r in history.iterrows():
        run_dt = pd.to_datetime(r.get("Дата запуска"), errors="coerce")
        advert = safe_int(r.get("ID кампании"))
        nm_id = safe_int(r.get("Артикул WB"))
        search_kop = safe_int(r.get("Ставка поиск, коп"))
        rec_kop = safe_int(r.get("Ставка рекомендации, коп"))
        if search_kop > 0:
            rows.append({"Дата запуска": run_dt, "ID кампании": advert, "Артикул WB": nm_id, "placement": "search", "bid_rub": from_kopecks(search_kop)})
        if rec_kop > 0:
            rows.append({"Дата запуска": run_dt, "ID кампании": advert, "Артикул WB": nm_id, "placement": "recommendations", "bid_rub": from_kopecks(rec_kop)})
    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values(["ID кампании", "Артикул WB", "placement", "Дата запуска"]).copy()
    return out


def evaluate_recent_bid_effects(history_placements: pd.DataFrame, metrics: pd.DataFrame, current_window: AnalysisWindow) -> pd.DataFrame:
    """Простая оценка последних изменений ставок: baseline предыдущие 3 зрелых дня, test текущие 3 зрелых дня.
    Нужен не для идеальной каузальности, а чтобы отлавливать пустые повышения.
    """
    if history_placements.empty or metrics.empty:
        return pd.DataFrame(columns=["ID кампании", "Артикул WB", "placement", "recent_changes", "last_bid_change_dt", "last_bid_rub", "prev_bid_rub", "bid_delta_pct", "effect_flag", "effect_comment"])

    rows = []
    latest_changes = history_placements.groupby(["ID кампании", "Артикул WB", "placement"], as_index=False).tail(2)
    for (advert, nm_id, placement), g in latest_changes.groupby(["ID кампании", "Артикул WB", "placement"]):
        g = g.sort_values("Дата запуска")
        last = g.iloc[-1]
        prev = g.iloc[-2] if len(g) >= 2 else None
        last_bid = safe_float(last.get("bid_rub", 0))
        prev_bid = safe_float(prev.get("bid_rub", last_bid)) if prev is not None else last_bid
        bid_delta_pct = pct(last_bid - prev_bid, prev_bid) if prev_bid > 0 else 0.0
        effect_flag = "unknown"
        effect_comment = "Недостаточно истории"
        if bid_delta_pct > 2:
            effect_flag = "pending"
            effect_comment = "Нужно сравнить влияние по зрелому окну"
        rows.append({
            "ID кампании": int(advert),
            "Артикул WB": int(nm_id),
            "placement": placement,
            "recent_changes": len(g),
            "last_bid_change_dt": last.get("Дата запуска"),
            "last_bid_rub": last_bid,
            "prev_bid_rub": prev_bid,
            "bid_delta_pct": round(bid_delta_pct, 2),
            "effect_flag": effect_flag,
            "effect_comment": effect_comment,
        })
    return pd.DataFrame(rows)


def count_experiment_days_this_year(provider: BaseProvider, product_root: str, year: int) -> int:
    try:
        df = provider.read_excel(SERVICE_EXPERIMENTS_KEY, sheet_name=0)
    except Exception:
        return 0
    if df.empty:
        return 0
    if "date" not in df.columns or "product_root" not in df.columns:
        return 0
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    return int(df[(df["product_root"] == product_root) & (pd.Series(df["date"]).apply(lambda x: x.year if pd.notna(x) else None) == year)].shape[0])


# ======================================================================================
# DECISION ENGINE
# ======================================================================================
def determine_action(row: pd.Series, config: ManagerConfig, as_of_date: date) -> Tuple[str, float, str, bool]:
    subject = canonical_subject(row.get("subject_norm", row.get("subject", "")))
    payment_type = row.get("payment_type", "cpc")
    current_bid = safe_float(row.get("current_bid_rub", 0))
    comfort_bid = safe_float(row.get("comfort_bid_rub", 0))
    max_bid = safe_float(row.get("max_bid_rub", 0))
    experiment_bid = safe_float(row.get("experiment_bid_rub", 0))

    blended_drr = safe_float(row.get("blended_drr_current_pct", 0))
    blended_prev = safe_float(row.get("blended_drr_prev_pct", 0))
    total_orders = safe_float(row.get("total_orders_current", 0))
    total_orders_prev = safe_float(row.get("total_orders_prev", 0))
    spend = safe_float(row.get("ad_spend_root_current", 0))
    spend_prev = safe_float(row.get("ad_spend_root_prev", 0))
    gp_realized = safe_float(row.get("gp_realized", 0))
    rating = safe_float(row.get("rating_reviews", 0))
    buyout_rate = safe_float(row.get("buyout_rate", 0))
    position = safe_float(row.get("median_position", 0))
    visibility = safe_float(row.get("visibility_pct", 0))
    bei_imp = safe_float(row.get("bei_imp", 1))
    bei_click = safe_float(row.get("bei_click", 1))
    card_issue = bool(row.get("card_issue", False))

    comfort_drr, max_drr, _weekend_drr = get_blended_caps(subject, config)
    order_growth = pct(total_orders - total_orders_prev, total_orders_prev) if total_orders_prev > 0 else (100.0 if total_orders > 0 else 0.0)
    spend_growth = pct(spend - spend_prev, spend_prev) if spend_prev > 0 else (100.0 if spend > 0 else 0.0)
    required_growth = compute_required_growth(blended_drr, blended_prev, spend_growth)

    weak_position = (position == 0 or position > 20 or visibility < 5)
    traffic_not_efficient = (bei_imp < 0.90 and bei_click < 0.90)
    strong_response = (bei_imp > 1.10 or bei_click > 1.10)
    rate_limit_flag = False
    growth_subject = subject in GROWTH_SUBJECTS
    root_supports_growth = growth_subject and blended_drr <= max_drr * 100 and order_growth > 0

    def min_bid_value() -> float:
        if payment_type == "cpc":
            return MIN_CPC_RUB
        return MIN_CPM_RECOMMENDATIONS_RUB if row.get("placement") == "recommendations" else MIN_CPM_SEARCH_RUB

    def finalize(action: str, proposed_bid: float, reason: str, flag: bool = False) -> Tuple[str, float, str, bool]:
        bid = round(safe_float(proposed_bid, 0), 2)
        current = round(current_bid, 2)
        if action == "DOWN":
            bid = min(current, bid)
            if bid >= current:
                return "HOLD", current, "Ставка уже на минимально допустимом уровне", flag
        elif action in {"UP", "TEST_UP"}:
            bid = max(current, bid)
            if bid <= current:
                return "HOLD", current, "Текущая ставка уже в рабочем диапазоне", flag
        return action, bid, reason, flag

    # Если ставка уже близка к потолку и товар плохо забирает трафик — это limit reached.
    if current_bid >= max_bid * 0.95 and weak_position and traffic_not_efficient:
        rate_limit_flag = True
        return "LIMIT_REACHED", round(current_bid, 2), "Повысить эффективность ставки — реклама работает на пределе", rate_limit_flag

    # Жёсткие блокировки, но для growth-категорий при сильном товаре целиком — не режем автоматически.
    if gp_realized <= 0 or rating < MIN_RATING or buyout_rate < MIN_BUYOUT:
        if root_supports_growth and weak_position and current_bid <= max(max_bid, comfort_bid * 1.10):
            return "HOLD", round(current_bid, 2), "Локально слабая экономика/выкуп, но товар растёт — ставку не повышаем", rate_limit_flag
        return finalize("DOWN", min_bid_value(), "Негативная экономика / рейтинг / выкуп", rate_limit_flag)

    # Если карточка не конвертит, для growth-категорий в рабочем DRR — HOLD, а не автоматический DOWN.
    if card_issue and current_bid > max(comfort_bid, 0):
        if root_supports_growth:
            return "HOLD", round(current_bid, 2), "Есть проблема в карточке, но товар растёт — держим ставку без роста", rate_limit_flag
        return finalize("DOWN", max(comfort_bid, current_bid * (1 - DOWN_STEP_SMALL)), "Проблема в карточке / воронке", rate_limit_flag)

    # Если blended DRR ушёл выше допустимого и роста заказов не хватило — снижаем.
    if blended_drr > max_drr * 100 and order_growth < required_growth:
        return finalize(
            "DOWN",
            max(comfort_bid, current_bid * (1 - DOWN_STEP_MED)),
            f"Blended DRR {blended_drr:.1f}% выше лимита {max_drr*100:.1f}% и рост заказов слабый",
            rate_limit_flag,
        )

    # Growth-решение.
    if weak_position and not card_issue:
        if current_bid < comfort_bid and (order_growth >= required_growth or growth_subject):
            step = UP_STEP_BIG if growth_subject else UP_STEP_MED
            new_bid = min(comfort_bid, current_bid * (1 + step))
            new_bid = max(new_bid, comfort_bid * 0.90 if current_bid == 0 else new_bid)
            return finalize("UP", new_bid, "Слабая позиция: подтягиваем ставку к комфортной", rate_limit_flag)
        if current_bid < max_bid and (order_growth >= required_growth or strong_response or growth_subject):
            step = UP_STEP_MED if growth_subject else UP_STEP_SMALL
            new_bid = min(max_bid, current_bid * (1 + step))
            return finalize("UP", new_bid, "Есть запас по max-ставке и потенциал роста позиции", rate_limit_flag)

    # Выходной эксперимент для growth-категорий.
    if growth_subject and as_of_date.weekday() in config.experiment_weekdays:
        if blended_drr <= max_drr * 100 and current_bid < experiment_bid and weak_position and (strong_response or order_growth >= required_growth):
            new_bid = min(experiment_bid, max(max_bid, current_bid * 1.15))
            return finalize("TEST_UP", new_bid, "Выходной эксперимент выше max для growth-категории", rate_limit_flag)

    # Если расходы растут, а заказы не растут адекватно — мягко снижаем.
    if spend_growth > 0 and order_growth < required_growth:
        if root_supports_growth and blended_drr <= comfort_drr * 100:
            return "HOLD", round(current_bid, 2), "Товар растёт и общий DRR в норме — не сушим ставку", rate_limit_flag
        return finalize("DOWN", max(comfort_bid, current_bid * (1 - DOWN_STEP_SMALL)), "Рост расходов не поддержан ростом заказов", rate_limit_flag)

    # Если ставка выше max — возвращаем в диапазон, но growth-категории в рабочем DRR не режем слишком рано.
    if current_bid > max_bid:
        if root_supports_growth and blended_drr <= max_drr * 100 and current_bid <= max_bid * 1.25:
            return "HOLD", round(current_bid, 2), "Ставка выше расчётного max, но товар растёт — пока не понижаем", rate_limit_flag
        return finalize("DOWN", max_bid, "Текущая ставка выше расчётного max", rate_limit_flag)

    return "HOLD", round(current_bid, 2), "Рабочий диапазон ставки", rate_limit_flag


# ======================================================================================
# WB SEND
# ======================================================================================
def normalize_bid_for_wb(bid_rub: float, payment_type: str, placement: str) -> int:
    bid_rub = safe_float(bid_rub)
    if payment_type == "cpc":
        bid_rub = clamp(bid_rub, MIN_CPC_RUB, MAX_CPC_RUB)
    else:
        if placement == "recommendations":
            bid_rub = clamp(bid_rub, MIN_CPM_RECOMMENDATIONS_RUB, MAX_CPM_RECOMMENDATIONS_RUB)
        else:
            bid_rub = clamp(bid_rub, MIN_CPM_SEARCH_RUB, MAX_CPM_SEARCH_RUB)
    return to_kopecks(bid_rub)


def placement_for_min_bids_api(placement: str) -> str:
    return "recommendation" if placement == "recommendations" else placement


def fetch_min_bids_for_advert(api_key: str, advert_id: int, nm_ids: List[int], payment_type: str, placements: List[str]) -> Dict[Tuple[int, str], int]:
    headers = {"Authorization": api_key.strip(), "Content-Type": "application/json"}
    payload = {
        "advert_id": int(advert_id),
        "nm_ids": [int(x) for x in nm_ids],
        "payment_type": payment_type,
        "placement_types": [placement_for_min_bids_api(p) for p in placements],
    }
    max_retries = 3
    for attempt in range(max_retries):
        try:
            resp = requests.post(WB_BIDS_MIN_URL, headers=headers, json=payload, timeout=120)
            if resp.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            resp.raise_for_status()
            data = resp.json()
            result: Dict[Tuple[int, str], int] = {}
            for item in data.get("bids", []):
                nm_id = safe_int(item.get("nm_id"))
                for sub in item.get("bids", []):
                    tp = str(sub.get("type", "")).strip().lower()
                    if tp == "recommendation":
                        tp = "recommendations"
                    result[(nm_id, tp)] = safe_int(sub.get("value"))
            return result
        except Exception:
            if attempt == max_retries - 1:
                return {}
            time.sleep(1)
    return {}


def decisions_to_payload(decisions_df: pd.DataFrame, use_only_changed: bool = True) -> Dict[str, Any]:
    if decisions_df.empty:
        return {"bids": []}
    df = decisions_df.copy()
    if use_only_changed:
        df = df[df["action"].isin(["UP", "DOWN", "TEST_UP"])].copy()
        df = df[df["new_bid_rub"].round(2) != df["current_bid_rub"].round(2)].copy()
    grouped: Dict[int, List[Dict[str, Any]]] = {}
    for _, r in df.iterrows():
        advert = safe_int(r["id_campaign"])
        nm_id = safe_int(r["nm_id"])
        placement = str(r["placement"])
        payment_type = str(r["payment_type"])
        bid_kop = normalize_bid_for_wb(safe_float(r["new_bid_rub"]), payment_type, placement)
        grouped.setdefault(advert, []).append({
            "nm_id": nm_id,
            "placement": placement,
            "bid_kopecks": bid_kop,
            "payment_type": payment_type,
        })
    out = []
    for advert, bids in grouped.items():
        for i in range(0, len(bids), 50):
            out.append({"advert_id": advert, "nm_bids": [{k: v for k, v in x.items() if k != "payment_type"} for x in bids[i:i + 50]], "payment_type": bids[0]["payment_type"]})
    return {"bids": out}


def send_payload(payload: Dict[str, Any], api_key: str, dry_run: bool = True) -> pd.DataFrame:
    logs: List[Dict[str, Any]] = []
    if dry_run:
        for block in payload.get("bids", []):
            logs.append({
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "advert_id": block.get("advert_id"),
                "status": "dry-run",
                "http_status": "",
                "response": "",
            })
        return pd.DataFrame(logs)

    headers = {"Authorization": api_key.strip(), "Content-Type": "application/json"}
    for block in payload.get("bids", []):
        advert_id = safe_int(block.get("advert_id"))
        advert_payload = {"bids": [{"advert_id": advert_id, "nm_bids": block.get("nm_bids", [])}]}
        http_status = None
        response_text = ""
        status = "failed"
        try:
            resp = requests.post(WB_BIDS_URL, headers=headers, json=advert_payload, timeout=120)
            http_status = resp.status_code
            response_text = resp.text[:2000]
            if resp.status_code == 200:
                status = "ok"
            else:
                status = "failed"
        except Exception as e:
            response_text = str(e)
        logs.append({
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "advert_id": advert_id,
            "status": status,
            "http_status": http_status,
            "response": response_text,
        })
    return pd.DataFrame(logs)


# ======================================================================================
# MAIN ANALYSIS
# ======================================================================================
def build_subject_funnel_medians(funnel_metrics: pd.DataFrame, nm_subject_map: pd.DataFrame) -> pd.DataFrame:
    if funnel_metrics.empty or nm_subject_map.empty:
        return pd.DataFrame(columns=["subject_norm", "addToCartConversion_median", "cartToOrderConversion_median"])
    df = funnel_metrics.merge(nm_subject_map[["nmId", "subject_norm"]].drop_duplicates(), on="nmId", how="left")
    if df.empty:
        return pd.DataFrame(columns=["subject_norm", "addToCartConversion_median", "cartToOrderConversion_median"])
    return df.groupby("subject_norm", as_index=False).agg(
        addToCartConversion_median=("addToCartConversion", "median"),
        cartToOrderConversion_median=("cartToOrderConversion", "median"),
    )


def prepare_metrics(provider: BaseProvider, config: ManagerConfig, as_of_date: date) -> Dict[str, pd.DataFrame]:
    window = compute_analysis_window(as_of_date)
    load_start = window.prev_start
    load_end = window.current_end

    log(f"📅 Анализируем зрелое окно {window.current_start} .. {window.current_end}; база сравнения {window.prev_start} .. {window.prev_end}")

    daily_ads, campaigns, placements = load_advertising(provider)
    economics = load_economics(provider)
    orders = load_orders(provider, load_start, load_end)
    funnel = load_funnel(provider)
    keywords_raw = load_keywords_latest(provider, window.current_end)
    keywords = aggregate_keywords(keywords_raw)
    ads_history = load_ads_history(provider)
    bid_history = load_bid_history(provider)

    log(f"📣 Реклама: {len(daily_ads):,} строк; кампании: {len(campaigns):,}; placement-строк: {len(placements):,}")
    log(f"💰 Экономика: {len(economics):,} SKU; Заказы: {len(orders):,} строк; Воронка: {len(funnel):,}; Keywords: {len(keywords):,}")

    ads_cur = aggregate_ads_window(daily_ads, window.current_start, window.current_end)
    ads_prev = aggregate_ads_window(daily_ads, window.prev_start, window.prev_end)

    root_cur, nm_cur = aggregate_orders_window(orders, window.current_start, window.current_end)
    root_prev, nm_prev = aggregate_orders_window(orders, window.prev_start, window.prev_end)

    funnel_cur = aggregate_funnel_window(funnel, window.current_start, window.current_end)
    subject_map = orders[["nmId", "subject_norm"]].drop_duplicates() if not orders.empty else pd.DataFrame(columns=["nmId", "subject_norm"])
    subject_funnel_medians = build_subject_funnel_medians(funnel_cur, subject_map)

    placements = placements.rename(columns={"Артикул WB": "nmId", "ID кампании": "advert_id", "Название предмета": "subject"}).copy()
    placements["subject_norm"] = placements["subject"].map(canonical_subject)

    ads_cur = ads_cur.rename(columns={"Артикул WB": "nmId", "ID кампании": "advert_id", "Название предмета": "subject"}).copy()
    ads_prev = ads_prev.rename(columns={"Артикул WB": "nmId", "ID кампании": "advert_id", "Название предмета": "subject"}).copy()
    economics = economics.rename(columns={"Артикул WB": "nmId", "Предмет": "subject", "Артикул продавца": "supplier_article"}).copy()
    keywords = keywords.rename(columns={"Артикул WB": "nmId"}).copy()

    # nm-level fact table
    fact = placements.merge(ads_cur, on=["advert_id", "nmId", "subject_norm"], how="left", suffixes=("", "_ad"))
    fact = fact.merge(ads_prev[["advert_id", "nmId", "Показы", "Клики", "Заказы", "Расход", "Сумма заказов"]].rename(columns={
        "Показы": "ad_impressions_prev",
        "Клики": "ad_clicks_prev",
        "Заказы": "ad_orders_prev",
        "Расход": "ad_spend_prev",
        "Сумма заказов": "ad_revenue_prev",
    }), on=["advert_id", "nmId"], how="left")
    fact = fact.merge(economics[["nmId", "supplier_article", "product_root", "buyout_rate", "gp_realized", "np_realized", "Валовая прибыль, руб/ед", "Чистая прибыль, руб/ед", "Средняя цена покупателя", "subject_norm"]], on=["nmId", "subject_norm"], how="left")
    nm_cur_merge = nm_cur.rename(columns={"supplierArticle": "supplier_article_orders", "total_orders": "total_orders_current", "total_revenue": "total_revenue_current", "avg_price": "avg_price_current"})
    nm_prev_merge = nm_prev.rename(columns={"supplierArticle": "supplier_article_orders_prev", "total_orders": "total_orders_prev", "total_revenue": "total_revenue_prev", "avg_price": "avg_price_prev"})
    fact = fact.merge(nm_cur_merge, on=["nmId", "product_root", "subject_norm"], how="left")
    fact = fact.merge(nm_prev_merge, on=["nmId", "product_root", "subject_norm"], how="left")
    fact = fact.merge(root_cur.rename(columns={"total_orders": "root_total_orders_current", "total_revenue": "root_total_revenue_current"}), on=["product_root", "subject_norm"], how="left")
    fact = fact.merge(root_prev.rename(columns={"total_orders": "root_total_orders_prev", "total_revenue": "root_total_revenue_prev"}), on=["product_root", "subject_norm"], how="left")
    fact = fact.merge(funnel_cur, on="nmId", how="left")
    fact = fact.merge(keywords, on="nmId", how="left")
    fact = fact.loc[:, ~fact.columns.duplicated()].copy()

    # Fill NA numeric
    numeric_cols = [
        "Показы", "Клики", "Заказы", "Расход", "Сумма заказов", "ad_impressions_prev", "ad_clicks_prev", "ad_orders_prev", "ad_spend_prev", "ad_revenue_prev",
        "gp_realized", "np_realized", "Валовая прибыль, руб/ед", "Чистая прибыль, руб/ед", "Средняя цена покупателя",
        "total_orders_current", "total_revenue_current", "avg_price_current", "total_orders_prev", "total_revenue_prev", "avg_price_prev",
        "root_total_orders_current", "root_total_revenue_current", "root_total_orders_prev", "root_total_revenue_prev",
        "openCardCount", "addToCartCount", "ordersCount", "buyoutsCount", "cancelCount", "addToCartConversion", "cartToOrderConversion", "buyoutPercent",
        "rating_reviews", "rating_card", "demand_week", "keyword_clicks", "keyword_orders", "median_position", "visibility_pct"
    ]
    for c in numeric_cols:
        if c in fact.columns:
            fact[c] = pd.to_numeric(fact[c], errors="coerce").fillna(0.0)
    fact["buyout_rate"] = fact["buyout_rate"].fillna(fact["buyoutPercent"] / 100.0).fillna(0.0)
    if "supplier_article_orders" in fact.columns:
        fact["supplier_article"] = fact["supplier_article"].fillna(fact["supplier_article_orders"])
    fact["supplier_article"] = fact["supplier_article"].fillna("")
    missing_roots = fact["product_root"].isna() | (fact["product_root"].astype(str).str.strip() == "")
    fact.loc[missing_roots, "product_root"] = fact.loc[missing_roots, "supplier_article"].map(product_root_from_supplier_article)
    fact["ad_impressions_current"] = fact["Показы"]
    fact["ad_clicks_current"] = fact["Клики"]
    fact["ad_orders_current"] = fact["Заказы"]
    fact["ad_spend_current"] = fact["Расход"]
    fact["ad_revenue_current"] = fact["Сумма заказов"]

    # Root-level blended metrics from total orders + ad spend summed across placements/campaigns.
    root_ad_current = fact.groupby(["product_root", "subject_norm"], as_index=False).agg(
        ad_spend_root_current=("ad_spend_current", "sum"),
        ad_revenue_root_current=("ad_revenue_current", "sum"),
        ad_orders_root_current=("ad_orders_current", "sum"),
        ad_clicks_root_current=("ad_clicks_current", "sum"),
    )
    root_ad_prev = fact.groupby(["product_root", "subject_norm"], as_index=False).agg(
        ad_spend_root_prev=("ad_spend_prev", "sum"),
        ad_revenue_root_prev=("ad_revenue_prev", "sum"),
        ad_orders_root_prev=("ad_orders_prev", "sum"),
        ad_clicks_root_prev=("ad_clicks_prev", "sum"),
    )

    product_metrics = root_cur.rename(columns={"total_orders": "total_orders_current", "total_revenue": "total_revenue_current"}).merge(
        root_prev.rename(columns={"total_orders": "total_orders_prev", "total_revenue": "total_revenue_prev"}),
        on=["product_root", "subject_norm"], how="outer"
    ).merge(root_ad_current, on=["product_root", "subject_norm"], how="outer").merge(root_ad_prev, on=["product_root", "subject_norm"], how="outer")
    for c in ["total_orders_current", "total_revenue_current", "total_orders_prev", "total_revenue_prev", "ad_spend_root_current", "ad_revenue_root_current", "ad_orders_root_current", "ad_clicks_root_current", "ad_spend_root_prev", "ad_revenue_root_prev", "ad_orders_root_prev", "ad_clicks_root_prev"]:
        if c in product_metrics.columns:
            product_metrics[c] = pd.to_numeric(product_metrics[c], errors="coerce").fillna(0.0)
    product_metrics["blended_drr_current_pct"] = product_metrics.apply(lambda x: pct(x["ad_spend_root_current"], x["total_revenue_current"]), axis=1)
    product_metrics["blended_drr_prev_pct"] = product_metrics.apply(lambda x: pct(x["ad_spend_root_prev"], x["total_revenue_prev"]), axis=1)
    product_metrics["ad_drr_current_pct"] = product_metrics.apply(lambda x: pct(x["ad_spend_root_current"], x["ad_revenue_root_current"]), axis=1)
    product_metrics["order_growth_pct"] = product_metrics.apply(lambda x: pct(x["total_orders_current"] - x["total_orders_prev"], x["total_orders_prev"]) if x["total_orders_prev"] > 0 else (100.0 if x["total_orders_current"] > 0 else 0.0), axis=1)
    product_metrics["spend_growth_pct"] = product_metrics.apply(lambda x: pct(x["ad_spend_root_current"] - x["ad_spend_root_prev"], x["ad_spend_root_prev"]) if x["ad_spend_root_prev"] > 0 else (100.0 if x["ad_spend_root_current"] > 0 else 0.0), axis=1)
    product_metrics["required_order_growth_pct"] = product_metrics.apply(lambda x: compute_required_growth(x["blended_drr_current_pct"], x["blended_drr_prev_pct"], x["spend_growth_pct"], x.get("subject_norm", "")), axis=1)
    product_metrics["drr_growth_pp"] = product_metrics["blended_drr_current_pct"] - product_metrics["blended_drr_prev_pct"]

    # Pull root metrics back to fact.
    fact = fact.merge(product_metrics[["product_root", "subject_norm", "ad_spend_root_current", "ad_revenue_root_current", "ad_orders_root_current", "ad_clicks_root_current", "ad_spend_root_prev", "ad_revenue_root_prev", "ad_orders_root_prev", "ad_clicks_root_prev", "blended_drr_current_pct", "blended_drr_prev_pct", "ad_drr_current_pct", "order_growth_pct", "spend_growth_pct", "required_order_growth_pct", "drr_growth_pp"]], on=["product_root", "subject_norm"], how="left")

    # subject medians for bid efficiency baselines
    tmp_for_eff = fact.copy()
    tmp_for_eff["capture_imp"] = tmp_for_eff.apply(lambda x: safe_float(x["ad_impressions_current"]) / safe_float(x["demand_week"]) if safe_float(x["demand_week"]) > 0 else 0.0, axis=1)
    tmp_for_eff["capture_click"] = tmp_for_eff.apply(lambda x: safe_float(x["ad_clicks_current"]) / safe_float(x["demand_week"]) if safe_float(x["demand_week"]) > 0 else 0.0, axis=1)
    tmp_for_eff["eff_imp"] = tmp_for_eff.apply(lambda x: x["capture_imp"] / safe_float(x["current_bid_rub"]) if safe_float(x["current_bid_rub"]) > 0 else 0.0, axis=1)
    tmp_for_eff["eff_click"] = tmp_for_eff.apply(lambda x: x["capture_click"] / safe_float(x["current_bid_rub"]) if safe_float(x["current_bid_rub"]) > 0 else 0.0, axis=1)
    tmp_for_eff["current_ctr_pct"] = tmp_for_eff.apply(lambda x: pct(safe_float(x["ad_clicks_current"]), safe_float(x["ad_impressions_current"])) if safe_float(x["ad_impressions_current"]) > 0 else 0.0, axis=1)
    subject_eff = tmp_for_eff.groupby(["subject_norm", "placement"], as_index=False).agg(
        eff_imp_median=("eff_imp", "median"),
        eff_click_median=("eff_click", "median"),
        placement_ctr_median_pct=("current_ctr_pct", "median"),
    )
    subject_ctr = tmp_for_eff.groupby(["subject_norm"], as_index=False).agg(subject_ctr_median_pct=("current_ctr_pct", "median"))

    # Classify mode, compute bid caps, compute efficiency.
    out_rows = []
    for _, r in fact.iterrows():
        row = r.copy()
        base_ctr = subject_eff[(subject_eff["subject_norm"] == row.get("subject_norm")) & (subject_eff["placement"] == row.get("placement"))]
        if not base_ctr.empty:
            row["placement_ctr_median_pct"] = safe_float(base_ctr.iloc[0].get("placement_ctr_median_pct", 0))
        subj_ctr_row = subject_ctr[subject_ctr["subject_norm"] == row.get("subject_norm")]
        if not subj_ctr_row.empty:
            row["subject_ctr_median_pct"] = safe_float(subj_ctr_row.iloc[0].get("subject_ctr_median_pct", 0))
        mode, card_issue = classify_product_mode(row, subject_funnel_medians)
        row["mode"] = mode
        row["card_issue"] = card_issue
        caps = compute_bid_caps(row, mode, config)
        for k, v in caps.items():
            row[k] = v
        eff = compute_bid_efficiency(row, subject_eff)
        for k, v in eff.items():
            row[k] = v
        action, new_bid, reason, rate_limit_flag = determine_action(row, config, as_of_date)
        row["action"] = action
        row["new_bid_rub"] = new_bid
        row["reason"] = reason
        row["rate_limit_flag"] = rate_limit_flag
        out_rows.append(row)
    decisions_base = pd.DataFrame(out_rows)

    # Add change effects info
    history_expanded = expand_bid_history_to_placements(bid_history)
    effects_df = evaluate_recent_bid_effects(history_expanded, decisions_base, window)
    decisions_base = decisions_base.merge(effects_df, left_on=["advert_id", "nmId", "placement"], right_on=["ID кампании", "Артикул WB", "placement"], how="left")

    # Make human-readable decision table
    decision_rows: List[Decision] = []
    for _, r in decisions_base.iterrows():
        decision_rows.append(Decision(
            run_date=str(as_of_date),
            id_campaign=safe_int(r["advert_id"]),
            nm_id=safe_int(r["nmId"]),
            supplier_article=str(r.get("supplier_article", "")),
            product_root=str(r.get("product_root", "")),
            subject=str(r.get("subject", r.get("subject_norm", ""))),
            placement=str(r.get("placement", "search")),
            payment_type=str(r.get("payment_type", "cpc")),
            current_bid_rub=round(safe_float(r.get("current_bid_rub", 0)), 2),
            comfort_bid_rub=round(safe_float(r.get("comfort_bid_rub", 0)), 2),
            max_bid_rub=round(safe_float(r.get("max_bid_rub", 0)), 2),
            experiment_bid_rub=round(safe_float(r.get("experiment_bid_rub", 0)), 2),
            action=str(r.get("action", "HOLD")),
            new_bid_rub=round(safe_float(r.get("new_bid_rub", 0)), 2),
            reason=str(r.get("reason", "")),
            mode=str(r.get("mode", "balanced")),
            current_blended_drr_pct=round(safe_float(r.get("blended_drr_current_pct", 0)), 2),
            total_orders=round(safe_float(r.get("root_total_orders_current", 0)), 2),
            ad_orders=round(safe_float(r.get("ad_orders_current", 0)), 2),
            bid_eff_index_imp=round(safe_float(r.get("bei_imp", 1)), 4),
            bid_eff_index_click=round(safe_float(r.get("bei_click", 1)), 4),
            median_position=round(safe_float(r.get("median_position", 0)), 2),
            visibility_pct=round(safe_float(r.get("visibility_pct", 0)), 2),
            demand_week=round(safe_float(r.get("demand_week", 0)), 2),
            gp_realized=round(safe_float(r.get("gp_realized", 0)), 2),
            order_growth_pct=round(safe_float(r.get("order_growth_pct", 0)), 2),
            required_order_growth_pct=round(safe_float(r.get("required_order_growth_pct", 0)), 2),
            spend_growth_pct=round(safe_float(r.get("spend_growth_pct", 0)), 2),
            drr_growth_pp=round(safe_float(r.get("drr_growth_pp", 0)), 2),
            card_issue=bool(r.get("card_issue", False)),
            rate_limit_flag=bool(r.get("rate_limit_flag", False)),
        ))
    decisions_df = pd.DataFrame([d.to_dict() for d in decision_rows])

    # Weak positions list
    weak_df = decisions_df[
        (decisions_df["median_position"] == 0) |
        (decisions_df["median_position"] > 20) |
        (decisions_df["action"] == "LIMIT_REACHED")
    ].copy()
    if not weak_df.empty:
        weak_df["comment"] = weak_df.apply(
            lambda x: "Повысить эффективность ставки — реклама работает на пределе" if x["action"] == "LIMIT_REACHED" else "Слабая позиция, нужна работа по карточке и ставке",
            axis=1,
        )

    # Bid limits snapshot
    limits_df = decisions_base[[
        "advert_id", "nmId", "supplier_article", "product_root", "subject", "placement", "payment_type", "mode",
        "current_bid_rub", "comfort_bid_rub", "max_bid_rub", "experiment_bid_rub", "clicks_per_ad_order", "clicks_per_total_order",
        "comfort_cpo", "max_cpo", "gp_realized", "ctr_est", "applied_comfort_cpc", "applied_max_cpc"
    ]].copy()

    # Efficiency snapshot
    eff_df = decisions_base[[
        "advert_id", "nmId", "supplier_article", "product_root", "subject", "placement", "current_bid_rub",
        "demand_week", "ad_impressions_current", "ad_clicks_current", "capture_imp", "capture_click", "eff_imp", "eff_click", "bei_imp", "bei_click",
        "median_position", "visibility_pct", "action", "reason"
    ]].copy()

    # Logic / audit sheet
    logic_df = decisions_base[[
        "advert_id", "nmId", "supplier_article", "product_root", "subject", "placement", "payment_type", "mode",
        "current_bid_rub", "comfort_bid_rub", "max_bid_rub", "experiment_bid_rub", "action", "new_bid_rub", "reason",
        "ad_impressions_current", "ad_clicks_current", "ad_orders_current", "ad_spend_current", "ad_revenue_current",
        "root_total_orders_current", "root_total_revenue_current", "ad_spend_root_current", "blended_drr_current_pct", "ad_drr_current_pct",
        "root_total_orders_prev", "root_total_revenue_prev", "ad_spend_root_prev", "blended_drr_prev_pct",
        "order_growth_pct", "spend_growth_pct", "required_order_growth_pct", "drr_growth_pp",
        "gp_realized", "buyout_rate", "rating_reviews", "rating_card", "median_position", "visibility_pct",
        "openCardCount", "addToCartCount", "ordersCount", "buyoutsCount", "addToCartConversion", "cartToOrderConversion", "buyoutPercent",
        "demand_week", "keyword_clicks", "keyword_orders", "capture_imp", "capture_click", "eff_imp", "eff_click", "bei_imp", "bei_click",
        "card_issue", "rate_limit_flag", "effect_flag", "effect_comment", "bid_delta_pct"
    ]].copy()

    # Product sheet enriched with mode counters
    mode_counts = decisions_df.groupby(["product_root", "subject"], as_index=False).agg(
        sku_rows=("nm_id", "count"),
        growth_rows=("mode", lambda s: int((pd.Series(s) == "growth").sum())),
        limit_rows=("action", lambda s: int((pd.Series(s) == "LIMIT_REACHED").sum())),
    )
    product_export = product_metrics.merge(mode_counts, left_on=["product_root", "subject_norm"], right_on=["product_root", "subject"], how="left")
    product_export["comment"] = product_export.apply(
        lambda x: "Развивать агрессивнее" if canonical_subject(x.get("subject_norm")) in GROWTH_SUBJECTS and x.get("blended_drr_current_pct", 0) < 8 else (
            "Реклама работает на пределе" if safe_float(x.get("limit_rows", 0)) > 0 else "Рабочий режим"
        ), axis=1)

    return {
        "decisions": decisions_df,
        "logic": logic_df,
        "product": product_export,
        "weak": weak_df,
        "limits": limits_df,
        "eff": eff_df,
        "effects": effects_df,
        "ads_history": ads_history,
        "window": pd.DataFrame([{
            "as_of_date": as_of_date,
            "current_start": window.current_start,
            "current_end": window.current_end,
            "prev_start": window.prev_start,
            "prev_end": window.prev_end,
        }]),
    }


RUS_ACTION_MAP = {
    "UP": "Повысить",
    "DOWN": "Снизить",
    "HOLD": "Без изменений",
    "TEST_UP": "Тест выше max",
    "LIMIT_REACHED": "Предел эффективности ставки",
}


def _bool_to_ru(v: Any) -> str:
    return "Да" if bool(v) else "Нет"


def _round_export_numbers(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in out.columns:
        if pd.api.types.is_float_dtype(out[c]):
            out[c] = out[c].round(2)
    return out


def localize_export_sheets(results: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    decisions = results["decisions"].copy()
    decisions["action"] = decisions["action"].map(lambda x: RUS_ACTION_MAP.get(str(x), str(x)))
    decisions["card_issue"] = decisions["card_issue"].map(_bool_to_ru)
    decisions["rate_limit_flag"] = decisions["rate_limit_flag"].map(_bool_to_ru)
    decisions = decisions.rename(columns={
        "run_date": "Дата расчёта", "id_campaign": "ID кампании", "nm_id": "Артикул WB", "supplier_article": "Артикул продавца",
        "product_root": "Товар", "subject": "Предмет", "placement": "Плейсмент", "payment_type": "Тип оплаты",
        "current_bid_rub": "Текущая ставка, ₽", "comfort_bid_rub": "Комфортная ставка, ₽", "max_bid_rub": "Максимальная ставка, ₽",
        "experiment_bid_rub": "Экспериментальная ставка, ₽", "action": "Решение", "new_bid_rub": "Новая ставка, ₽",
        "reason": "Обоснование", "mode": "Режим товара", "current_blended_drr_pct": "Общий ДРР, %", "total_orders": "Все заказы товара",
        "ad_orders": "Рекламные заказы", "bid_eff_index_imp": "Индекс эффективности ставки по показам", "bid_eff_index_click": "Индекс эффективности ставки по кликам",
        "median_position": "Медианная позиция", "visibility_pct": "Видимость, %", "demand_week": "Спрос по запросам, неделя",
        "gp_realized": "Валовая прибыль на реализованный заказ, ₽", "order_growth_pct": "Рост заказов, %",
        "required_order_growth_pct": "Требуемый рост заказов, %", "spend_growth_pct": "Рост расходов, %", "drr_growth_pp": "Рост общего ДРР, п.п.",
        "card_issue": "Проблема карточки", "rate_limit_flag": "Предел эффективности ставки"
    })

    logic = results["logic"].copy()
    logic["action"] = logic["action"].map(lambda x: RUS_ACTION_MAP.get(str(x), str(x)))
    for bcol in ["card_issue", "rate_limit_flag"]:
        if bcol in logic.columns:
            logic[bcol] = logic[bcol].map(_bool_to_ru)
    logic = logic.rename(columns={
        "advert_id": "ID кампании", "nmId": "Артикул WB", "supplier_article": "Артикул продавца", "product_root": "Товар", "subject": "Предмет",
        "placement": "Плейсмент", "payment_type": "Тип оплаты", "mode": "Режим товара", "current_bid_rub": "Текущая ставка, ₽",
        "comfort_bid_rub": "Комфортная ставка, ₽", "max_bid_rub": "Максимальная ставка, ₽", "experiment_bid_rub": "Экспериментальная ставка, ₽",
        "action": "Решение", "new_bid_rub": "Новая ставка, ₽", "reason": "Обоснование", "ad_impressions_current": "Показы рекламы",
        "ad_clicks_current": "Клики рекламы", "ad_orders_current": "Рекламные заказы", "ad_spend_current": "Расход рекламы, ₽", "ad_revenue_current": "Рекламная выручка, ₽",
        "root_total_orders_current": "Все заказы товара", "root_total_revenue_current": "Вся выручка товара, ₽", "ad_spend_root_current": "Общий рекламный расход товара, ₽",
        "blended_drr_current_pct": "Общий ДРР, %", "ad_drr_current_pct": "Рекламный ДРР, %", "root_total_orders_prev": "Все заказы товара, база",
        "root_total_revenue_prev": "Вся выручка товара, база, ₽", "ad_spend_root_prev": "Общий рекламный расход товара, база, ₽", "blended_drr_prev_pct": "Общий ДРР, база, %",
        "order_growth_pct": "Рост заказов, %", "spend_growth_pct": "Рост расходов, %", "required_order_growth_pct": "Требуемый рост заказов, %",
        "drr_growth_pp": "Рост общего ДРР, п.п.", "gp_realized": "Валовая прибыль на реализованный заказ, ₽", "buyout_rate": "Процент выкупа, доля",
        "rating_reviews": "Рейтинг отзывов", "rating_card": "Рейтинг карточки", "median_position": "Медианная позиция", "visibility_pct": "Видимость, %",
        "openCardCount": "Открытия карточки", "addToCartCount": "Добавления в корзину", "ordersCount": "Заказы воронки", "buyoutsCount": "Выкупы",
        "addToCartConversion": "Конверсия в корзину, %", "cartToOrderConversion": "Конверсия корзина→заказ, %", "buyoutPercent": "Выкуп по воронке, %",
        "demand_week": "Спрос по запросам, неделя", "keyword_clicks": "Клики по ключам", "keyword_orders": "Заказы по ключам", "capture_imp": "Доля захваченных показов",
        "capture_click": "Доля захваченных кликов", "eff_imp": "Эффективность ставки по показам", "eff_click": "Эффективность ставки по кликам",
        "bei_imp": "Индекс эффективности ставки по показам", "bei_click": "Индекс эффективности ставки по кликам", "card_issue": "Проблема карточки",
        "rate_limit_flag": "Предел эффективности ставки", "effect_flag": "Статус оценки прошлых изменений", "effect_comment": "Комментарий по прошлым изменениям",
        "bid_delta_pct": "Последнее изменение ставки, %"
    })

    product = results["product"].copy().rename(columns={
        "product_root": "Товар", "subject_norm": "Предмет", "total_orders_current": "Все заказы товара", "total_revenue_current": "Вся выручка товара, ₽",
        "total_orders_prev": "Все заказы товара, база", "total_revenue_prev": "Вся выручка товара, база, ₽", "ad_spend_root_current": "Общий рекламный расход, ₽",
        "ad_revenue_root_current": "Рекламная выручка, ₽", "ad_orders_root_current": "Рекламные заказы", "ad_clicks_root_current": "Рекламные клики",
        "ad_spend_root_prev": "Общий рекламный расход, база, ₽", "blended_drr_current_pct": "Общий ДРР, %", "blended_drr_prev_pct": "Общий ДРР, база, %",
        "ad_drr_current_pct": "Рекламный ДРР, %", "order_growth_pct": "Рост заказов, %", "spend_growth_pct": "Рост расходов, %",
        "required_order_growth_pct": "Требуемый рост заказов, %", "drr_growth_pp": "Рост общего ДРР, п.п.", "sku_rows": "Число рекламных строк",
        "growth_rows": "Строк growth", "limit_rows": "Строк на пределе ставки", "comment": "Комментарий"
    })

    weak = results["weak"].copy()
    if not weak.empty:
        weak["action"] = weak["action"].map(lambda x: RUS_ACTION_MAP.get(str(x), str(x)))
    weak = weak.rename(columns={
        "run_date": "Дата расчёта", "id_campaign": "ID кампании", "nm_id": "Артикул WB", "supplier_article": "Артикул продавца", "product_root": "Товар",
        "subject": "Предмет", "placement": "Плейсмент", "payment_type": "Тип оплаты", "current_bid_rub": "Текущая ставка, ₽",
        "comfort_bid_rub": "Комфортная ставка, ₽", "max_bid_rub": "Максимальная ставка, ₽", "experiment_bid_rub": "Экспериментальная ставка, ₽",
        "action": "Решение", "new_bid_rub": "Новая ставка, ₽", "reason": "Обоснование", "mode": "Режим товара", "current_blended_drr_pct": "Общий ДРР, %",
        "total_orders": "Все заказы товара", "ad_orders": "Рекламные заказы", "bid_eff_index_imp": "Индекс эффективности ставки по показам",
        "bid_eff_index_click": "Индекс эффективности ставки по кликам", "median_position": "Медианная позиция", "visibility_pct": "Видимость, %",
        "demand_week": "Спрос по запросам, неделя", "gp_realized": "Валовая прибыль на реализованный заказ, ₽", "order_growth_pct": "Рост заказов, %",
        "required_order_growth_pct": "Требуемый рост заказов, %", "spend_growth_pct": "Рост расходов, %", "drr_growth_pp": "Рост общего ДРР, п.п.", "comment": "Комментарий"
    })

    limits = results["limits"].copy().rename(columns={
        "advert_id": "ID кампании", "nmId": "Артикул WB", "supplier_article": "Артикул продавца", "product_root": "Товар", "subject": "Предмет",
        "placement": "Плейсмент", "payment_type": "Тип оплаты", "mode": "Режим товара", "current_bid_rub": "Текущая ставка, ₽",
        "comfort_bid_rub": "Комфортная ставка, ₽", "max_bid_rub": "Максимальная ставка, ₽", "experiment_bid_rub": "Экспериментальная ставка, ₽",
        "clicks_per_ad_order": "Кликов на 1 рекламный заказ", "clicks_per_total_order": "Кликов на 1 общий заказ товара", "comfort_cpo": "Комфортная стоимость заказа, ₽",
        "max_cpo": "Максимальная стоимость заказа, ₽", "gp_realized": "Валовая прибыль на реализованный заказ, ₽", "ctr_est": "Оценочный CTR, %",
        "applied_comfort_cpc": "Комфортная ставка CPC, ₽", "applied_max_cpc": "Максимальная ставка CPC, ₽"
    })

    eff = results["eff"].copy()
    if not eff.empty:
        eff["action"] = eff["action"].map(lambda x: RUS_ACTION_MAP.get(str(x), str(x)))
    eff = eff.rename(columns={
        "advert_id": "ID кампании", "nmId": "Артикул WB", "supplier_article": "Артикул продавца", "product_root": "Товар", "subject": "Предмет",
        "placement": "Плейсмент", "current_bid_rub": "Текущая ставка, ₽", "demand_week": "Спрос по запросам, неделя", "ad_impressions_current": "Показы рекламы",
        "ad_clicks_current": "Клики рекламы", "capture_imp": "Доля захваченных показов", "capture_click": "Доля захваченных кликов",
        "eff_imp": "Эффективность ставки по показам", "eff_click": "Эффективность ставки по кликам", "bei_imp": "Индекс эффективности ставки по показам",
        "bei_click": "Индекс эффективности ставки по кликам", "median_position": "Медианная позиция", "visibility_pct": "Видимость, %", "action": "Решение", "reason": "Обоснование"
    })

    effects = results["effects"].copy().rename(columns={
        "placement": "Плейсмент", "recent_changes": "Число последних изменений", "last_bid_change_dt": "Дата последнего изменения",
        "last_bid_rub": "Последняя ставка, ₽", "prev_bid_rub": "Предыдущая ставка, ₽", "bid_delta_pct": "Изменение ставки, %",
        "effect_flag": "Статус оценки", "effect_comment": "Комментарий"
    })

    # Чистим дубли и приводим ключевые идентификаторы к строкам.
    for frame in [decisions, logic, weak, limits, eff]:
        for key_col in ["Товар", "Артикул продавца", "Плейсмент", "Предмет"]:
            if key_col in frame.columns:
                frame[key_col] = frame[key_col].fillna("").astype(str)

    product = product.drop(columns=[c for c in ["sku_count_x", "sku_count_y", "ad_revenue_root_prev", "ad_orders_root_prev", "ad_clicks_root_prev", "subject"] if c in product.columns])
    if "Товар" in product.columns:
        product["Товар"] = product["Товар"].fillna("").astype(str)

    window = results["window"].copy().rename(columns={
        "as_of_date": "Дата расчёта", "current_start": "Начало текущего зрелого окна", "current_end": "Конец текущего зрелого окна",
        "prev_start": "Начало базового окна", "prev_end": "Конец базового окна"
    })

    return {
        "Решения_по_ставкам": _round_export_numbers(decisions),
        "Расчёт_логики": _round_export_numbers(logic),
        "Статистика_по_товарам": _round_export_numbers(product),
        "Эффективность_ставки": _round_export_numbers(eff),
        "Слабая_позиция": _round_export_numbers(weak),
        "Эффект_изменений": _round_export_numbers(effects),
        "Лимиты_ставок": _round_export_numbers(limits),
        "Окно_анализа": window,
    }


# ======================================================================================
# SAVE / REPORT
# ======================================================================================
def append_or_replace_history(provider: BaseProvider, key: str, new_df: pd.DataFrame, sheet_name: str = "history", dedupe_cols: Optional[List[str]] = None, tail: Optional[int] = None) -> None:
    try:
        old = provider.read_excel(key, sheet_name=0)
    except Exception:
        old = pd.DataFrame()
    combined = pd.concat([old, new_df], ignore_index=True) if not old.empty else new_df.copy()
    if dedupe_cols:
        combined = combined.drop_duplicates(subset=dedupe_cols, keep="last")
    if tail and len(combined) > tail:
        combined = combined.tail(tail).copy()
    provider.write_excel(key, {sheet_name: combined})


def save_outputs(provider: BaseProvider, results: Dict[str, pd.DataFrame], mode: str, as_of_date: date) -> None:
    decisions = results["decisions"].copy()
    logic = results["logic"].copy()
    product = results["product"].copy()
    weak = results["weak"].copy()
    limits = results["limits"].copy()
    eff = results["eff"].copy()
    effects = results["effects"].copy()
    window_df = results["window"].copy()

    preview_sheets = localize_export_sheets(results)
    provider.write_excel(SERVICE_PREVIEW_KEY, preview_sheets)

    provider.write_excel(SERVICE_LIMITS_KEY, {"Лимиты ставок": preview_sheets["Лимиты_ставок"]})
    provider.write_excel(SERVICE_PRODUCT_KEY, {"Статистика по товарам": preview_sheets["Статистика_по_товарам"]})
    provider.write_excel(SERVICE_EFF_KEY, {"Эффективность ставки": preview_sheets["Эффективность_ставки"]})
    provider.write_excel(SERVICE_WEAK_KEY, {"Слабая позиция": preview_sheets["Слабая_позиция"]})
    provider.write_excel(SERVICE_EFFECTS_KEY, {"Эффект изменений": preview_sheets["Эффект_изменений"]})

    summary = {
        "generated_at": datetime.now().isoformat(),
        "mode": mode,
        "as_of_date": str(as_of_date),
        "recommendations_count": int(len(decisions)),
        "changed_count": int(decisions[decisions["action"].isin(["UP", "DOWN", "TEST_UP"]) & (decisions["new_bid_rub"].round(2) != decisions["current_bid_rub"].round(2))].shape[0]),
        "limit_reached_count": int(decisions[decisions["action"] == "LIMIT_REACHED"].shape[0]),
        "weak_items_count": int(len(weak)),
    }
    provider.write_text(SERVICE_SUMMARY_KEY, json.dumps(summary, ensure_ascii=False, indent=2, default=str))

    archive_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    append_or_replace_history(
        provider,
        SERVICE_ARCHIVE_KEY,
        logic.assign(archive_stamp=archive_stamp),
        sheet_name="archive",
        tail=50000,
    )


def update_bid_history(provider: BaseProvider, decisions: pd.DataFrame, as_of_date: date) -> None:
    changed = decisions[decisions["action"].isin(["UP", "DOWN", "TEST_UP"])].copy()
    changed = changed[changed["new_bid_rub"].round(2) != changed["current_bid_rub"].round(2)].copy()
    if changed.empty:
        return
    rows = []
    week = iso_week_label(as_of_date)
    for _, r in changed.iterrows():
        rows.append({
            "Дата запуска": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Неделя": week,
            "ID кампании": safe_int(r["id_campaign"]),
            "Артикул WB": safe_int(r["nm_id"]),
            "Тип кампании": f"{r['payment_type']}_{r['placement']}",
            "Ставка поиск, коп": to_kopecks(r["new_bid_rub"]) if str(r["placement"]) in {"search", "combined"} else 0,
            "Ставка рекомендации, коп": to_kopecks(r["new_bid_rub"]) if str(r["placement"]) == "recommendations" else 0,
            "Стратегия": "V2_BALANCED_GROWTH",
        })
    hist_df = pd.DataFrame(rows)
    append_or_replace_history(provider, SERVICE_BID_HISTORY_KEY, hist_df, sheet_name="history", tail=50000)


def save_experiments(provider: BaseProvider, decisions: pd.DataFrame, as_of_date: date) -> None:
    exp = decisions[decisions["action"] == "TEST_UP"].copy()
    if exp.empty:
        return
    exp = exp.assign(date=str(as_of_date))
    append_or_replace_history(provider, SERVICE_EXPERIMENTS_KEY, exp, sheet_name="experiments", tail=50000)


def print_console_summary(decisions: pd.DataFrame) -> None:
    if decisions.empty:
        log("ℹ️ Рекомендаций нет")
        return
    changed = decisions[decisions["action"].isin(["UP", "DOWN", "TEST_UP"]) & (decisions["new_bid_rub"].round(2) != decisions["current_bid_rub"].round(2))].copy()
    log(f"✅ Всего строк решений: {len(decisions):,}")
    log(f"🔁 Изменённых ставок: {len(changed):,}")
    if not changed.empty:
        by_action = changed["action"].value_counts().to_dict()
        log(f"📊 Разбивка по действиям: {by_action}")
        preview_cols = ["product_root", "supplier_article", "subject", "id_campaign", "placement", "current_bid_rub", "new_bid_rub", "action", "reason"]
        print(changed[preview_cols].head(20).to_string(index=False))


# ======================================================================================
# ENTRY POINT
# ======================================================================================
def build_provider(args: argparse.Namespace) -> BaseProvider:
    if args.local_data_dir:
        log(f"📁 Работаю в локальном режиме: {args.local_data_dir}")
        return LocalProvider(args.local_data_dir)
    required_env = ["YC_ACCESS_KEY_ID", "YC_SECRET_ACCESS_KEY", "YC_BUCKET_NAME"]
    missing = [v for v in required_env if not os.environ.get(v)]
    if missing:
        raise RuntimeError(f"Отсутствуют env переменные: {missing}")
    return S3Provider(
        access_key=os.environ["YC_ACCESS_KEY_ID"],
        secret_key=os.environ["YC_SECRET_ACCESS_KEY"],
        bucket_name=os.environ["YC_BUCKET_NAME"],
    )


def run_manager(args: argparse.Namespace) -> None:
    provider = build_provider(args)
    as_of_date = pd.to_datetime(args.as_of_date).date() if args.as_of_date else current_local_date()
    config = load_config(provider)
    mode = args.command or "preview"

    results = prepare_metrics(provider, config, as_of_date)
    decisions = results["decisions"]
    save_outputs(provider, results, mode=mode, as_of_date=as_of_date)
    print_console_summary(decisions)

    if mode == "preview":
        log("🧪 Preview-режим: ставки не отправлялись")
        return

    if mode == "run":
        api_key = os.environ.get("WB_PROMO_KEY_TOPFACE", "").strip()
        if not api_key:
            raise RuntimeError("Не задан WB_PROMO_KEY_TOPFACE")
        payload = decisions_to_payload(decisions)
        send_log = send_payload(payload, api_key=api_key, dry_run=False)
        if not send_log.empty:
            append_or_replace_history(provider, SERVICE_ARCHIVE_KEY, send_log.assign(kind="wb_send"), sheet_name="archive", tail=50000)
        update_bid_history(provider, decisions, as_of_date)
        save_experiments(provider, decisions, as_of_date)
        log(f"📤 Отправлено блоков в WB: {len(payload.get('bids', []))}")
        return

    raise RuntimeError(f"Неизвестная команда: {mode}")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="WB Ads Manager V2 — balanced growth algorithm")
    sub = p.add_subparsers(dest="command")

    for cmd in ["preview", "run"]:
        sc = sub.add_parser(cmd)
        sc.add_argument("--local-data-dir", type=str, default=None, help="Локальная папка с выгруженными файлами")
        sc.add_argument("--as-of-date", type=str, default=None, help="Дата запуска YYYY-MM-DD")
    return p


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.command is None:
        args.command = "preview"
    run_manager(args)


if __name__ == "__main__":
    main()
