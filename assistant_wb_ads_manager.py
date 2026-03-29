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
SERVICE_PREVIEW_KEY = SERVICE_ROOT + "Предпросмотр_последнего_запуска.xlsx"
SERVICE_SUMMARY_KEY = SERVICE_ROOT + "Сводка_последнего_запуска.json"
SERVICE_ARCHIVE_KEY = SERVICE_ROOT + "Архив_решений.xlsx"
SERVICE_BID_HISTORY_KEY = SERVICE_ROOT + "История_ставок.xlsx"
SERVICE_LIMITS_KEY = SERVICE_ROOT + "Лимиты_ставок_ежедневно.xlsx"
SERVICE_PRODUCT_KEY = SERVICE_ROOT + "Метрики_по_товарам.xlsx"
SERVICE_EFF_KEY = SERVICE_ROOT + "Эффективность_ставки_ежедневно.xlsx"
SERVICE_WEAK_KEY = SERVICE_ROOT + "Слабые_позиции_приоритет.xlsx"
SERVICE_EFFECTS_KEY = SERVICE_ROOT + "Эффект_изменений.xlsx"
SERVICE_EXPERIMENTS_KEY = SERVICE_ROOT + "Эксперименты_ставок.xlsx"
SERVICE_SHADE_ACTIONS_KEY = SERVICE_ROOT + "Рекомендации_по_оттенкам.xlsx"
SERVICE_SHADE_PORTFOLIO_KEY = SERVICE_ROOT + "Состав_кампаний_по_оттенкам.xlsx"
SERVICE_SHADE_TESTS_KEY = SERVICE_ROOT + "Тесты_оттенков.xlsx"
SERVICE_BENCHMARKS_KEY = SERVICE_ROOT + "Сравнение_с_сильными_РК.xlsx"

WB_BIDS_URL = "https://advert-api.wildberries.ru/api/advert/v1/bids"
WB_BIDS_MIN_URL = "https://advert-api.wildberries.ru/api/advert/v1/bids/min"
WB_NMS_URL = "https://advert-api.wildberries.ru/adv/v0/auction/nms"

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
MIN_SHADE_RATING = 4.6
SHADE_TEST_MIN_IMPRESSIONS = 4000
SHADE_TEST_MIN_DAYS = 5
SHADE_TEST_COOLDOWN_DAYS = 21

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
    "preview_filename": "Предпросмотр_последнего_запуска.xlsx",
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


def parse_placement_types(value: Any, fallback: str = "search") -> List[str]:
    raw = str(value or "").strip().lower()
    if not raw:
        return [fallback]
    raw = raw.replace("recommendation", "recommendations")
    tokens = re.split(r"[,;/|]+", raw)
    out: List[str] = []
    for t in tokens:
        t = t.strip()
        if t in {"search", "combined", "recommendations"} and t not in out:
            out.append(t)
    return out or [fallback]


def human_subject(value: Any) -> str:
    return str(value or "").strip()


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
    """Лёгкая оценка последних изменений ставок. Если зрелого сравнения нет, всё равно возвращаем историю последних правок,
    чтобы файл не был пустым и было видно, что именно менять оценивать позже."""
    columns = ["ID кампании", "Артикул WB", "placement", "recent_changes", "last_bid_change_dt", "last_bid_rub", "prev_bid_rub", "bid_delta_pct", "effect_flag", "effect_comment"]
    if metrics.empty:
        return pd.DataFrame(columns=columns)
    if history_placements.empty:
        # Возвращаем заглушку по строкам, где есть изменение ставки в текущем решении
        rows = []
        for _, r in metrics.iterrows():
            current = safe_float(r.get("current_bid_rub", 0))
            new = safe_float(r.get("new_bid_rub", current))
            if abs(new - current) < 0.01:
                continue
            rows.append({
                "ID кампании": safe_int(r.get("advert_id", 0)),
                "Артикул WB": safe_int(r.get("nmId", 0)),
                "placement": str(r.get("placement", "search")),
                "recent_changes": 1,
                "last_bid_change_dt": pd.NaT,
                "last_bid_rub": new,
                "prev_bid_rub": current,
                "bid_delta_pct": round(pct(new - current, current) if current > 0 else 0.0, 2),
                "effect_flag": "planned",
                "effect_comment": "Изменение запланировано, зрелого окна после изменения ещё нет",
            })
        return pd.DataFrame(rows, columns=columns)

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
        elif bid_delta_pct < -2:
            effect_flag = "pending"
            effect_comment = "Снижение ставки — ждём зрелое окно для оценки"
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
    return pd.DataFrame(rows, columns=columns)



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


def build_subject_benchmarks(decisions_base: pd.DataFrame) -> pd.DataFrame:
    if decisions_base.empty:
        return pd.DataFrame(columns=["subject_norm","placement","peer_capture_imp_median","peer_capture_click_median","peer_eff_imp_median","peer_eff_click_median","peer_ctr_median_pct","peer_total_orders_median"])
    df = decisions_base.copy()
    df["current_ctr_pct"] = df.apply(lambda x: pct(safe_float(x.get("ad_clicks_current", 0)), safe_float(x.get("ad_impressions_current", 0))) if safe_float(x.get("ad_impressions_current", 0)) > 0 else 0.0, axis=1)
    rows=[]
    for (subject, placement), g in df.groupby(["subject_norm","placement"]):
        cap = get_blended_caps(subject, ManagerConfig())[1] * 100.0
        good = g[g["blended_drr_current_pct"] <= cap].copy()
        if good.empty: good = g.copy()
        order_thr = good["root_total_orders_current"].quantile(0.50) if "root_total_orders_current" in good.columns else 0
        click_thr = good["ad_clicks_current"].quantile(0.50) if "ad_clicks_current" in good.columns else 0
        leaders = good[(good["root_total_orders_current"] >= order_thr) | (good["ad_clicks_current"] >= click_thr)].copy()
        if leaders.empty: leaders = good.copy()
        rows.append({"subject_norm": subject, "placement": placement, "peer_capture_imp_median": round(leaders["capture_imp"].median(), 6) if "capture_imp" in leaders else 0.0, "peer_capture_click_median": round(leaders["capture_click"].median(), 6) if "capture_click" in leaders else 0.0, "peer_eff_imp_median": round(leaders["eff_imp"].median(), 8) if "eff_imp" in leaders else 0.0, "peer_eff_click_median": round(leaders["eff_click"].median(), 8) if "eff_click" in leaders else 0.0, "peer_ctr_median_pct": round(leaders["current_ctr_pct"].median(), 4), "peer_total_orders_median": round(leaders["root_total_orders_current"].median(), 2) if "root_total_orders_current" in leaders else 0.0})
    return pd.DataFrame(rows)


def build_shade_universe(fact_df: pd.DataFrame) -> pd.DataFrame:
    if fact_df.empty:
        return pd.DataFrame()
    keep = [c for c in ["nmId","supplier_article","product_root","subject","subject_norm","rating_reviews","buyout_rate","gp_realized","total_orders_current","total_revenue_current","ad_impressions_current","ad_clicks_current","ad_orders_current","ad_spend_current","median_position","visibility_pct"] if c in fact_df.columns]
    df = fact_df[keep].copy()
    grp = df.groupby(["nmId","supplier_article","product_root","subject_norm"], as_index=False).agg(subject=("subject","first"), rating_reviews=("rating_reviews","max"), buyout_rate=("buyout_rate","max"), gp_realized=("gp_realized","max"), total_orders_current=("total_orders_current","max"), total_revenue_current=("total_revenue_current","max"), ad_impressions_current=("ad_impressions_current","sum"), ad_clicks_current=("ad_clicks_current","sum"), ad_orders_current=("ad_orders_current","sum"), ad_spend_current=("ad_spend_current","sum"), median_position=("median_position","median"), visibility_pct=("visibility_pct","median"))
    grp["ctr_pct"] = grp.apply(lambda x: pct(x["ad_clicks_current"], x["ad_impressions_current"]) if x["ad_impressions_current"] > 0 else 0.0, axis=1)
    grp["cr_pct"] = grp.apply(lambda x: pct(x["ad_orders_current"], x["ad_clicks_current"]) if x["ad_clicks_current"] > 0 else 0.0, axis=1)
    grp["shade_candidate_score"] = (grp["total_orders_current"].rank(pct=True).fillna(0) * 0.35 + grp["ctr_pct"].rank(pct=True).fillna(0) * 0.20 + grp["cr_pct"].rank(pct=True).fillna(0) * 0.20 + grp["rating_reviews"].rank(pct=True).fillna(0) * 0.15 + grp["buyout_rate"].rank(pct=True).fillna(0) * 0.10)
    return grp


def build_shade_portfolio(campaigns_df: pd.DataFrame, shade_universe: pd.DataFrame, decisions_base: pd.DataFrame) -> pd.DataFrame:
    if campaigns_df.empty or shade_universe.empty:
        return pd.DataFrame()
    base = campaigns_df.rename(columns={
        "ID кампании": "advert_id",
        "id_campaign": "advert_id",
        "advertId": "advert_id",
        "advert": "advert_id",
        "Артикул WB": "nmId",
        "nm_id": "nmId",
        "nmID": "nmId",
    }).copy()
    if "advert_id" not in base.columns or "nmId" not in base.columns:
        return pd.DataFrame()
    keep = [c for c in ["advert_id","nmId","payment_type","bid_type","status_norm","Название предмета"] if c in base.columns]
    base = base[keep].copy().merge(shade_universe, on="nmId", how="left")
    if base.empty:
        return pd.DataFrame()
    if decisions_base is not None and not decisions_base.empty and {"advert_id","nmId"}.issubset(decisions_base.columns):
        placement_map = decisions_base.groupby(["advert_id","nmId"], as_index=False).agg(
            placement=("placement", lambda s: ",".join(sorted(set(map(str, s))))),
            current_bid_rub=("current_bid_rub", "max")
        )
        base = base.merge(placement_map, on=["advert_id","nmId"], how="left")
    base["subject"] = base.get("subject", pd.Series(index=base.index, dtype=object)).fillna(base.get("Название предмета", ""))
    if "subject_norm" not in base.columns:
        base["subject_norm"] = base["subject"].map(canonical_subject)
    else:
        base["subject_norm"] = base["subject_norm"].fillna(base["subject"].map(canonical_subject))
    base = base[base["product_root"].astype(str).str.strip() != ""].copy()
    if base.empty:
        return pd.DataFrame()

    def _pick_core(g: pd.DataFrame) -> pd.DataFrame:
        g = g.copy()
        for col in ["total_orders_current", "rating_reviews", "buyout_rate", "ctr_pct", "cr_pct"]:
            if col not in g.columns:
                g[col] = 0.0
        g["core_score"] = (
            g["total_orders_current"].rank(pct=True).fillna(0) * 0.45 +
            g["rating_reviews"].rank(pct=True).fillna(0) * 0.20 +
            g["buyout_rate"].rank(pct=True).fillna(0) * 0.15 +
            g["ctr_pct"].rank(pct=True).fillna(0) * 0.10 +
            g["cr_pct"].rank(pct=True).fillna(0) * 0.10
        )
        core_nm = safe_int(g.sort_values(["core_score","total_orders_current","rating_reviews"], ascending=False).iloc[0]["nmId"])
        g["shade_status"] = g["nmId"].map(lambda x: "CORE" if safe_int(x) == core_nm else "WORKING")
        g["core_nm_id"] = core_nm
        return g

    return base.groupby("advert_id", group_keys=False).apply(_pick_core).reset_index(drop=True)



def load_existing_shade_tests(provider: BaseProvider) -> pd.DataFrame:
    try:
        df = provider.read_excel(SERVICE_SHADE_TESTS_KEY, sheet_name=0)
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return pd.DataFrame()
    for c in ["start_date","last_eval_date","remove_date"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.date
    return df


def build_shade_actions(provider: BaseProvider, shade_portfolio: pd.DataFrame, shade_universe: pd.DataFrame, product_metrics: pd.DataFrame, config: ManagerConfig, as_of_date: date, api_key: str = "") -> Tuple[pd.DataFrame, pd.DataFrame]:
    existing_tests = load_existing_shade_tests(provider)
    if shade_portfolio.empty or shade_universe.empty:
        return pd.DataFrame(), existing_tests

    portfolio = shade_portfolio.copy()
    if "advert_id" not in portfolio.columns:
        for candidate in ["ID кампании", "id_campaign", "advertId", "advert"]:
            if candidate in portfolio.columns:
                portfolio = portfolio.rename(columns={candidate: "advert_id"})
                break
    if "nmId" not in portfolio.columns:
        for candidate in ["Артикул WB", "nm_id", "nmID"]:
            if candidate in portfolio.columns:
                portfolio = portfolio.rename(columns={candidate: "nmId"})
                break
    if "advert_id" not in portfolio.columns or "nmId" not in portfolio.columns:
        return pd.DataFrame(), existing_tests
    if "subject" not in portfolio.columns:
        portfolio["subject"] = ""
    if "subject_norm" not in portfolio.columns:
        portfolio["subject_norm"] = portfolio["subject"].map(canonical_subject)
    else:
        portfolio["subject_norm"] = portfolio["subject_norm"].fillna(portfolio["subject"].map(canonical_subject))

    actions: List[Dict[str, Any]] = []
    reserved_by_root: Dict[str, set] = {}

    def _append_no_action(advert_id: int, product_root: str, subject: str, core_nm: int, core_supplier: str, payment_type: str, placements: List[str], reason: str):
        actions.append({
            "advert_id": safe_int(advert_id),
            "product_root": product_root,
            "subject": human_subject(subject),
            "subject_norm": subject,
            "core_nm_id": core_nm,
            "core_supplier_article": core_supplier,
            "candidate_nm_id": None,
            "candidate_supplier_article": "",
            "shade_action": "NO_ACTION",
            "reason": reason,
            "payment_type": payment_type,
            "placement": ",".join(placements),
            "placement_primary": placements[0] if placements else "search",
            "rating_reviews": None,
            "candidate_score": None,
            "root_blended_drr_pct": None,
            "root_order_growth_pct": None,
            "min_wb_bid_rub": None,
            "min_bid_status": "n/a",
        })

    for advert_id, g in portfolio.groupby("advert_id"):
        subject = canonical_subject(g["subject_norm"].iloc[0])
        if subject not in GROWTH_SUBJECTS:
            continue
        product_root = str(g["product_root"].dropna().astype(str).iloc[0]) if g["product_root"].notna().any() else ""
        if not product_root:
            continue
        current_nms = {safe_int(x) for x in g["nmId"].tolist() if safe_int(x) > 0}
        core_nm = safe_int(g["core_nm_id"].iloc[0]) if "core_nm_id" in g.columns else (next(iter(current_nms)) if current_nms else 0)
        core_supplier = str(g.loc[g["nmId"] == core_nm, "supplier_article"].iloc[0]) if (g["nmId"] == core_nm).any() and "supplier_article" in g.columns else ""
        placements = parse_placement_types(g["placement"].iloc[0] if "placement" in g.columns else "", "search")
        payment_type = str(g["payment_type"].iloc[0] if "payment_type" in g.columns else "cpc")
        root_metrics = product_metrics[(product_metrics["product_root"].astype(str) == product_root) & (product_metrics["subject_norm"] == subject)]
        blended = safe_float(root_metrics["blended_drr_current_pct"].iloc[0]) if not root_metrics.empty else 0.0
        order_growth = safe_float(root_metrics["order_growth_pct"].iloc[0]) if not root_metrics.empty else 0.0
        cap = get_blended_caps(subject, config)[1] * 100.0
        if blended > cap and order_growth <= 0:
            _append_no_action(advert_id, product_root, subject, core_nm, core_supplier, payment_type, placements, "Общий ДРР товара выше допустимого и роста заказов нет — новые оттенки не добавляем")
            continue

        same_root_tests = existing_tests[existing_tests.get("product_root", pd.Series(dtype=str)).astype(str) == product_root].copy() if not existing_tests.empty and "product_root" in existing_tests.columns else pd.DataFrame()
        active_test_nms = {safe_int(x) for x in same_root_tests[same_root_tests.get("status", pd.Series(dtype=str)).astype(str).isin(["TEST","PENDING_MIN_BID"])] .get("candidate_nm_id", pd.Series(dtype=float)).tolist()} if not same_root_tests.empty else set()
        if active_test_nms:
            _append_no_action(advert_id, product_root, subject, core_nm, core_supplier, payment_type, placements, "По товару уже идёт тест оттенка — ждём накопления показов")
            continue

        candidates = shade_universe[(shade_universe["product_root"].astype(str) == product_root) & (shade_universe["subject_norm"] == subject)].copy()
        if candidates.empty:
            _append_no_action(advert_id, product_root, subject, core_nm, core_supplier, payment_type, placements, "Для товара нет доступных оттенков-кандидатов")
            continue
        candidates = candidates[(candidates["rating_reviews"] > MIN_SHADE_RATING) & (~candidates["nmId"].isin(current_nms))].copy()
        if candidates.empty:
            _append_no_action(advert_id, product_root, subject, core_nm, core_supplier, payment_type, placements, "Нет подходящих оттенков с рейтингом выше 4.6 вне текущего состава кампании")
            continue
        reserved = reserved_by_root.setdefault(product_root, set())
        candidates = candidates[~candidates["nmId"].isin(reserved)].copy()
        if candidates.empty:
            _append_no_action(advert_id, product_root, subject, core_nm, core_supplier, payment_type, placements, "Кандидаты уже зарезервированы в другом тесте по этому товару")
            continue
        cand = candidates.sort_values(["shade_candidate_score", "total_orders_current", "rating_reviews"], ascending=False).iloc[0]
        reserved.add(safe_int(cand["nmId"]))
        actions.append({
            "advert_id": safe_int(advert_id),
            "product_root": product_root,
            "subject": human_subject(cand["subject"]),
            "subject_norm": subject,
            "core_nm_id": core_nm,
            "core_supplier_article": core_supplier,
            "candidate_nm_id": safe_int(cand["nmId"]),
            "candidate_supplier_article": str(cand["supplier_article"]),
            "shade_action": "ADD_TEST",
            "reason": "Добавляем новый оттенок на минимальной ставке WB и собираем тест до 4000 показов",
            "payment_type": payment_type,
            "placement": ",".join(placements),
            "placement_primary": placements[0] if placements else "search",
            "rating_reviews": round(safe_float(cand.get("rating_reviews", 0)), 2),
            "candidate_score": round(safe_float(cand.get("shade_candidate_score", 0)), 4),
            "root_blended_drr_pct": round(blended, 2),
            "root_order_growth_pct": round(order_growth, 2),
            "min_wb_bid_rub": None,
            "min_bid_status": "pending",
        })

    actions_df = pd.DataFrame(actions)
    if actions_df.empty:
        return actions_df, existing_tests

    add_mask = actions_df["shade_action"] == "ADD_TEST"
    if add_mask.any() and api_key.strip():
        query_rows = []
        for _, r in actions_df[add_mask].iterrows():
            for pl in parse_placement_types(r.get("placement", ""), r.get("placement_primary", "search")):
                query_rows.append({
                    "advert_id": safe_int(r["advert_id"]),
                    "nm_id": safe_int(r["candidate_nm_id"]),
                    "payment_type": str(r.get("payment_type", "cpc")),
                    "placement": pl,
                })
        min_bids_df = fetch_min_bids_for_rows(api_key, pd.DataFrame(query_rows))
        if not min_bids_df.empty:
            agg = min_bids_df.groupby(["advert_id","nm_id"], as_index=False).agg(
                min_wb_bid_rub=("min_wb_bid_rub","max"),
                min_bid_status=("min_bid_status", lambda s: "ok" if "ok" in set(map(str,s)) else "not_found")
            )
            actions_df = actions_df.merge(agg, left_on=["advert_id","candidate_nm_id"], right_on=["advert_id","nm_id"], how="left").drop(columns=["nm_id"], errors="ignore")
        actions_df.loc[add_mask & (actions_df.get("min_bid_status", "not_found") != "ok"), "shade_action"] = "NEED_MIN_BID_CHECK"
        actions_df.loc[actions_df["shade_action"] == "NEED_MIN_BID_CHECK", "reason"] = "Не удалось получить минимальную ставку WB — автодобавление запрещено"
    elif add_mask.any():
        actions_df.loc[add_mask, "shade_action"] = "NEED_MIN_BID_CHECK"
        actions_df.loc[add_mask, "reason"] = "Нет WB_PROMO_KEY_TOPFACE — минимальная ставка WB не проверена"
        actions_df.loc[add_mask, "min_bid_status"] = "no_api_key"

    tests_rows = []
    for _, r in actions_df.iterrows():
        if r.get("shade_action") not in {"ADD_TEST", "NEED_MIN_BID_CHECK"}:
            continue
        tests_rows.append({
            "start_date": as_of_date,
            "last_eval_date": as_of_date,
            "advert_id": safe_int(r.get("advert_id")),
            "product_root": str(r.get("product_root", "")),
            "subject": str(r.get("subject", "")),
            "core_nm_id": safe_int(r.get("core_nm_id")),
            "candidate_nm_id": safe_int(r.get("candidate_nm_id")),
            "candidate_supplier_article": str(r.get("candidate_supplier_article", "")),
            "status": "TEST" if r.get("shade_action") == "ADD_TEST" else "PENDING_MIN_BID",
            "test_target_impressions": SHADE_TEST_MIN_IMPRESSIONS,
            "collected_impressions": 0,
            "collected_clicks": 0,
            "collected_orders": 0,
            "min_wb_bid_rub": safe_float(r.get("min_wb_bid_rub", 0)),
            "reason": str(r.get("reason", "")),
        })
    tests_df = pd.concat([existing_tests, pd.DataFrame(tests_rows)], ignore_index=True) if (not existing_tests.empty or tests_rows) else pd.DataFrame()
    if not tests_df.empty:
        tests_df = tests_df.drop_duplicates(subset=["advert_id","candidate_nm_id"], keep="last")
    return actions_df, tests_df



def build_benchmark_comparison(decisions_base: pd.DataFrame, subject_benchmarks: pd.DataFrame) -> pd.DataFrame:
    if decisions_base.empty:
        return pd.DataFrame()
    df = decisions_base.merge(subject_benchmarks, on=["subject_norm","placement"], how="left")
    for col in ["peer_capture_imp_median","peer_capture_click_median","peer_eff_imp_median","peer_eff_click_median","peer_ctr_median_pct","peer_total_orders_median"]:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    df["vs_peer_capture_imp"] = df.apply(lambda x: safe_float(x.get("capture_imp",0)) / safe_float(x["peer_capture_imp_median"]) if safe_float(x["peer_capture_imp_median"]) > 0 else 1.0, axis=1)
    df["vs_peer_capture_click"] = df.apply(lambda x: safe_float(x.get("capture_click",0)) / safe_float(x["peer_capture_click_median"]) if safe_float(x["peer_capture_click_median"]) > 0 else 1.0, axis=1)
    df["current_ctr_pct"] = df.apply(lambda x: pct(safe_float(x.get("ad_clicks_current",0)), safe_float(x.get("ad_impressions_current",0))) if safe_float(x.get("ad_impressions_current",0)) > 0 else 0.0, axis=1)
    df["vs_peer_ctr"] = df.apply(lambda x: safe_float(x["current_ctr_pct"]) / safe_float(x["peer_ctr_median_pct"]) if safe_float(x["peer_ctr_median_pct"]) > 0 else 1.0, axis=1)
    df["benchmark_problem_flag"] = (df.get("demand_week", 0) > 0) & (df["vs_peer_capture_imp"] < 0.6) & (df["vs_peer_ctr"] < 0.8)
    df["problem_bucket"] = df.apply(lambda x: "РК на пределе" if safe_float(x.get("bei_imp",1)) < 0.9 and safe_float(x.get("bei_click",1)) < 0.9 else ("Growth-РК: нужен трафик и портфель оттенков" if bool(x.get("benchmark_problem_flag", False)) else "Нет"), axis=1)
    # Возвращаем исходный df с добавленными полями — он нужен decision engine и weak list.
    return df



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


def fetch_min_bids_for_rows(api_key: str, rows_df: pd.DataFrame) -> pd.DataFrame:
    if rows_df is None or rows_df.empty or not api_key.strip():
        return pd.DataFrame(columns=["advert_id", "nm_id", "placement", "min_wb_bid_rub", "min_wb_bid_kop", "min_bid_status"])
    rows = rows_df.copy()
    out_rows: List[Dict[str, Any]] = []
    for (advert_id, payment_type), g in rows.groupby(["advert_id", "payment_type"]):
        advert_id = safe_int(advert_id)
        payment_type = str(payment_type)
        nm_ids = sorted({safe_int(x) for x in g["nm_id"].tolist() if safe_int(x) > 0})
        placements = sorted({str(x) for x in g["placement"].tolist() if str(x)})
        result = fetch_min_bids_for_advert(api_key, advert_id, nm_ids, payment_type, placements)
        for _, r in g.iterrows():
            nm_id = safe_int(r["nm_id"]); placement = str(r["placement"])
            min_kop = result.get((nm_id, placement))
            out_rows.append({"advert_id": advert_id, "nm_id": nm_id, "placement": placement, "min_wb_bid_kop": min_kop if min_kop is not None else None, "min_wb_bid_rub": from_kopecks(min_kop) if min_kop is not None else None, "min_bid_status": "ok" if min_kop is not None else "not_found"})
    return pd.DataFrame(out_rows)


def patch_campaign_nms(api_key: str, actions_df: pd.DataFrame, dry_run: bool = True) -> pd.DataFrame:
    if actions_df is None or actions_df.empty:
        return pd.DataFrame(columns=["timestamp", "advert_id", "status", "http_status", "response"])
    logs: List[Dict[str, Any]] = []
    headers = {"Authorization": api_key.strip(), "Content-Type": "application/json"}
    for advert_id, g in actions_df.groupby("advert_id"):
        add_ids = sorted({safe_int(x) for x in g.loc[g["shade_action"] == "ADD_TEST", "candidate_nm_id"].tolist() if safe_int(x) > 0})
        del_ids = sorted({safe_int(x) for x in g.loc[g["shade_action"] == "REMOVE_SHADE", "candidate_nm_id"].tolist() if safe_int(x) > 0})
        payload = {"nms": [{"advert_id": safe_int(advert_id), "nms": {"add": add_ids, "delete": del_ids}}]}
        if dry_run:
            logs.append({"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "advert_id": safe_int(advert_id), "status": "dry-run", "http_status": "", "response": json.dumps(payload, ensure_ascii=False)})
            continue
        try:
            resp = requests.patch(WB_NMS_URL, headers=headers, json=payload, timeout=120)
            logs.append({"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "advert_id": safe_int(advert_id), "status": "ok" if resp.status_code == 200 else "failed", "http_status": resp.status_code, "response": resp.text[:2000]})
        except Exception as e:
            logs.append({"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "advert_id": safe_int(advert_id), "status": "failed", "http_status": "", "response": str(e)})
        time.sleep(1.05)
    return pd.DataFrame(logs)


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

    # Сравнение с сильными РК и контур оттенков
    subject_benchmarks = build_subject_benchmarks(decisions_base)
    decisions_base = build_benchmark_comparison(decisions_base, subject_benchmarks)
    api_key_for_min = os.environ.get("WB_PROMO_KEY_TOPFACE", "").strip()
    shade_universe = build_shade_universe(fact)
    shade_portfolio = build_shade_portfolio(campaigns, shade_universe, decisions_base)
    shade_actions_df, shade_tests_df = build_shade_actions(provider, shade_portfolio, shade_universe, product_metrics, config, as_of_date, api_key=api_key_for_min)

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

    # Weak positions / слабые РК по эффективности ставки
    weak_mask = (
        (decisions_df["median_position"] == 0) |
        (decisions_df["median_position"] > 20) |
        (decisions_df["action"] == "LIMIT_REACHED")
    )
    if "benchmark_problem_flag" in decisions_base.columns:
        weak_mask = weak_mask | decisions_base["benchmark_problem_flag"].fillna(False).values
    weak_df = decisions_df[weak_mask].copy()
    if not weak_df.empty:
        enrich_cols = [c for c in ["advert_id","nmId","placement","capture_imp","capture_click","eff_imp","eff_click","bei_imp","bei_click","peer_capture_imp_median","peer_capture_click_median","peer_eff_imp_median","peer_eff_click_median","peer_ctr_median_pct","vs_peer_capture_imp","vs_peer_capture_click","vs_peer_ctr","benchmark_problem_flag","problem_bucket"] if c in decisions_base.columns]
        weak_df = weak_df.merge(decisions_base[enrich_cols].rename(columns={"advert_id":"id_campaign","nmId":"nm_id"}), on=["id_campaign","nm_id","placement"], how="left")
        weak_df["comment"] = weak_df.apply(lambda x: "Повысить эффективность ставки — реклама работает на пределе" if x["action"] == "LIMIT_REACHED" else (str(x.get("problem_bucket")) if str(x.get("problem_bucket","")).strip() else "Слабая позиция, нужна работа по карточке и ставке"), axis=1)

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
        "shade_portfolio": shade_portfolio,
        "shade_actions": shade_actions_df,
        "shade_tests": shade_tests_df,
        "benchmark_cmp": build_benchmark_report(build_benchmark_comparison(decisions_base, subject_benchmarks)),
        "subject_benchmarks": subject_benchmarks,
        "daily_ads": daily_ads,
        "campaigns": campaigns,
        "placements": placements,
        "bid_history": bid_history,
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
    "TEST_UP": "Тест роста",
    "LIMIT_REACHED": "Предел эффективности ставки",
}


def _eff_conclusion(ctr: float, clicks: float, impressions: float) -> str:
    if impressions <= 0:
        return "Нет показов"
    if clicks <= 0:
        return "Нет кликов"
    if ctr < 1.0:
        return "CTR низкий — ставку выше жать рано, сначала карточка/запросы"
    if ctr < 2.5:
        return "CTR средний — тестировать рост ставки осторожно"
    return "CTR хороший — ставка работает нормально"


def build_bid_efficiency_history_sheets(results: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    daily_ads = results.get("daily_ads", pd.DataFrame()).copy()
    campaigns = results.get("campaigns", pd.DataFrame()).copy()
    if daily_ads.empty:
        return {"Пусто": pd.DataFrame([{"Комментарий": "Нет ежедневной истории рекламы"}])}
    daily = daily_ads.copy()
    if not campaigns.empty:
        camp = campaigns.rename(columns={"ID кампании": "advert_id", "Артикул WB": "nmId", "Название": "campaign_name", "Ставка в поиске (руб)": "search_bid_rub", "Ставка в рекомендациях (руб)": "rec_bid_rub"}).copy()
        keep = [c for c in ["advert_id", "nmId", "payment_type", "bid_type", "search_bid_rub", "rec_bid_rub", "campaign_name"] if c in camp.columns]
        daily = daily.merge(camp[keep], left_on=["ID кампании", "Артикул WB"], right_on=["advert_id", "nmId"], how="left")
    else:
        daily["payment_type"] = ""
        daily["bid_type"] = ""
        daily["search_bid_rub"] = 0.0
        daily["rec_bid_rub"] = 0.0
    # resolve type/placement and bid
    def _ptype(row):
        p = str(row.get("payment_type", "")).lower()
        b = str(row.get("bid_type", "")).lower()
        if p == "cpc":
            return "Поиск"
        if "recommend" in b:
            return "Полки"
        if "combined" in b or "поиск" in b:
            return "Поиск+Полки"
        return "Поиск+Полки" if safe_float(row.get("rec_bid_rub", 0)) > 0 and safe_float(row.get("search_bid_rub", 0)) > 0 else ("Поиск" if safe_float(row.get("search_bid_rub", 0)) > 0 else "Полки")
    daily["Тип кампании"] = daily.apply(_ptype, axis=1)
    def _bid(row):
        t = row.get("Тип кампании")
        if t == "Полки":
            return safe_float(row.get("rec_bid_rub", 0))
        if t == "Поиск":
            return safe_float(row.get("search_bid_rub", 0))
        return max(safe_float(row.get("search_bid_rub", 0)), safe_float(row.get("rec_bid_rub", 0)))
    daily["Ставка, ₽"] = daily.apply(_bid, axis=1)
    daily["CTR, %"] = daily.apply(lambda x: pct(safe_float(x.get("Клики", 0)), safe_float(x.get("Показы", 0))), axis=1)
    daily["Вывод"] = daily.apply(lambda x: _eff_conclusion(safe_float(x.get("CTR, %", 0)), safe_float(x.get("Клики", 0)), safe_float(x.get("Показы", 0))), axis=1)
    daily["Артикул продавца"] = daily.get("Артикул WB", pd.Series(dtype=int)).astype(str)
    if "supplier_article" in results.get("logic", pd.DataFrame()).columns:
        mp = results["logic"][["nmId", "supplier_article"]].drop_duplicates().rename(columns={"nmId": "Артикул WB", "supplier_article": "Артикул продавца"})
        daily = daily.merge(mp, on="Артикул WB", how="left", suffixes=("", "_y"))
        daily["Артикул продавца"] = daily["Артикул продавца_y"].fillna(daily["Артикул продавца"])
        daily = daily.drop(columns=[c for c in ["Артикул продавца_y"] if c in daily.columns])
    daily = daily.sort_values(["Артикул WB", "Дата", "ID кампании"])
    sheets = {}
    for nm_id, g in daily.groupby("Артикул WB"):
        art = str(g["Артикул продавца"].dropna().astype(str).iloc[0] if "Артикул продавца" in g.columns and not g.empty else nm_id)
        df = g[["Дата", "ID кампании", "Тип кампании", "Ставка, ₽", "Показы", "Клики", "CTR, %", "Вывод"]].copy()
        # add small summary row block at top? keep just history
        sheet = art[:31] if art else str(nm_id)[:31]
        sheets[sheet] = _round_export_numbers(df)
    return sheets or {"Пусто": pd.DataFrame([{"Комментарий": "Нет данных по эффективности ставки"}])}


def build_simple_weak_export(results: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    weak = results.get("weak", pd.DataFrame()).copy()
    if weak.empty:
        return pd.DataFrame([{"Комментарий": "Слабых артикулов не выявлено"}])
    cols = []
    mapping = {"product_root":"Товар", "supplier_article":"Артикул продавца", "nm_id":"Артикул WB", "id_campaign":"ID кампании", "placement":"Тип кампании", "comment":"Комментарий", "reason":"Причина"}
    for src,dst in mapping.items():
        if src in weak.columns:
            weak = weak.rename(columns={src: dst})
            cols.append(dst)
    if "Решение" not in weak.columns and "action" in weak.columns:
        weak["Решение"] = weak["action"].map(lambda x: RUS_ACTION_MAP.get(str(x), str(x)))
        cols.append("Решение")
    cols = [c for c in ["Товар","Артикул продавца","Артикул WB","ID кампании","Тип кампании","Решение","Комментарий","Причина"] if c in weak.columns]
    weak = weak[cols].drop_duplicates().sort_values([c for c in ["Товар","Артикул продавца","ID кампании"] if c in cols])
    return weak.reset_index(drop=True)


def build_benchmark_clean_export(results: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    df = results.get("benchmark_cmp", pd.DataFrame()).copy()
    if df.empty:
        return pd.DataFrame([{"Комментарий": "Недостаточно данных для сравнения с сильными РК"}])
    keep = [c for c in ["advert_id","nmId","supplier_article","product_root","subject","placement","current_bid_rub","capture_imp","capture_click","bei_imp","bei_click","peer_capture_imp_median","peer_capture_click_median","peer_ctr_median_pct","vs_peer_capture_imp","vs_peer_capture_click","vs_peer_ctr","benchmark_problem_flag","problem_bucket","action"] if c in df.columns]
    df = df[keep].copy()
    df = df.rename(columns={
        "advert_id":"ID кампании","nmId":"Артикул WB","supplier_article":"Артикул продавца","product_root":"Товар","subject":"Предмет","placement":"Плейсмент","current_bid_rub":"Текущая ставка, ₽","capture_imp":"Наша доля показов","capture_click":"Наша доля кликов","bei_imp":"Индекс эфф. по показам","bei_click":"Индекс эфф. по кликам","peer_capture_imp_median":"Эталон доли показов","peer_capture_click_median":"Эталон доли кликов","peer_ctr_median_pct":"Эталон CTR, %","vs_peer_capture_imp":"Отн. к эталону по показам","vs_peer_capture_click":"Отн. к эталону по кликам","vs_peer_ctr":"Отн. к эталону по CTR","benchmark_problem_flag":"Флаг проблемы","problem_bucket":"Тип проблемы","action":"Решение"
    })
    if "Решение" in df.columns:
        df["Решение"] = df["Решение"].map(lambda x: RUS_ACTION_MAP.get(str(x), str(x)))
    if "Флаг проблемы" in df.columns:
        df["Флаг проблемы"] = df["Флаг проблемы"].map(_bool_to_ru)
    return _round_export_numbers(df)


def _bool_to_ru(v: Any) -> str:
    return "Да" if bool(v) else "Нет"


def _round_export_numbers(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in out.columns:
        if pd.api.types.is_float_dtype(out[c]):
            cl = str(c).lower()
            if any(token in cl for token in ["eff_", "bei_", "capture", "эталон", "отн."]):
                out[c] = out[c].round(6)
            elif "ctr" in cl or "cr" in cl:
                out[c] = out[c].round(4)
            else:
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

    weak = build_simple_weak_export(results)

    limits = results["limits"].copy().rename(columns={
        "advert_id": "ID кампании", "nmId": "Артикул WB", "supplier_article": "Артикул продавца", "product_root": "Товар", "subject": "Предмет",
        "placement": "Плейсмент", "payment_type": "Тип оплаты", "mode": "Режим товара", "current_bid_rub": "Текущая ставка, ₽",
        "comfort_bid_rub": "Комфортная ставка, ₽", "max_bid_rub": "Максимальная ставка, ₽", "experiment_bid_rub": "Экспериментальная ставка, ₽",
        "clicks_per_ad_order": "Кликов на 1 рекламный заказ", "clicks_per_total_order": "Кликов на 1 общий заказ товара", "comfort_cpo": "Комфортная стоимость заказа, ₽",
        "max_cpo": "Максимальная стоимость заказа, ₽", "gp_realized": "Валовая прибыль на реализованный заказ, ₽", "ctr_est": "Оценочный CTR, %",
        "applied_comfort_cpc": "Комфортная ставка CPC, ₽", "applied_max_cpc": "Максимальная ставка CPC, ₽"
    })

    eff = results["eff"].copy()
    if eff.empty:
        eff = pd.DataFrame([{"Комментарий": "Нет данных по эффективности ставки"}])
    else:
        if "action" in eff.columns:
            eff["action"] = eff["action"].map(lambda x: RUS_ACTION_MAP.get(str(x), str(x)))
        eff = eff.rename(columns={
            "advert_id": "ID кампании", "nmId": "Артикул WB", "supplier_article": "Артикул продавца", "product_root": "Товар", "subject": "Предмет",
            "placement": "Плейсмент", "current_bid_rub": "Текущая ставка, ₽", "demand_week": "Спрос по запросам, неделя", "ad_impressions_current": "Показы рекламы",
            "ad_clicks_current": "Клики рекламы", "capture_imp": "Доля захваченных показов", "capture_click": "Доля захваченных кликов",
            "eff_imp": "Эффективность ставки по показам", "eff_click": "Эффективность ставки по кликам", "bei_imp": "Индекс эффективности ставки по показам",
            "bei_click": "Индекс эффективности ставки по кликам", "median_position": "Медианная позиция", "visibility_pct": "Видимость, %", "action": "Решение", "reason": "Обоснование"
        })
        keep_cols = [c for c in ["Артикул продавца","Артикул WB","ID кампании","Плейсмент","Текущая ставка, ₽","Показы рекламы","Клики рекламы","Эффективность ставки по показам","Эффективность ставки по кликам","Индекс эффективности ставки по показам","Индекс эффективности ставки по кликам","Решение","Обоснование"] if c in eff.columns]
        eff = eff[keep_cols].copy()

    effects_src = results["effects"].copy()
    if effects_src.empty:
        planned = results["decisions"].copy()
        planned = planned[planned["action"].isin(["UP", "DOWN", "TEST_UP"]) & (planned["new_bid_rub"].round(2) != planned["current_bid_rub"].round(2))].copy() if not planned.empty else pd.DataFrame()
        if planned.empty:
            effects = pd.DataFrame([{"Комментарий": "Нет созревших изменений для оценки"}])
        else:
            effects = planned.rename(columns={"id_campaign":"ID кампании","nm_id":"Артикул WB","placement":"Плейсмент","current_bid_rub":"Предыдущая ставка, ₽","new_bid_rub":"Последняя ставка, ₽","reason":"Комментарий"})[[c for c in ["ID кампании","Артикул WB","Плейсмент","Предыдущая ставка, ₽","Последняя ставка, ₽","Комментарий"] if c in planned.rename(columns={"id_campaign":"ID кампании","nm_id":"Артикул WB","placement":"Плейсмент","current_bid_rub":"Предыдущая ставка, ₽","new_bid_rub":"Последняя ставка, ₽","reason":"Комментарий"}).columns]]
    else:
        effects = effects_src.rename(columns={
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

    shade_portfolio = results.get("shade_portfolio", pd.DataFrame()).copy()
    if not shade_portfolio.empty:
        shade_portfolio = shade_portfolio.rename(columns={
            "advert_id": "ID кампании", "nmId": "Артикул WB", "supplier_article": "Артикул продавца", "product_root": "Товар",
            "subject": "Предмет", "subject_norm": "Предмет (норм.)", "payment_type": "Тип оплаты", "bid_type": "Тип кампании",
            "status_norm": "Статус РК", "placement": "Плейсменты", "current_bid_rub": "Текущая ставка, ₽",
            "rating_reviews": "Рейтинг", "buyout_rate": "Выкуп", "gp_realized": "Валовая прибыль на реализованный заказ, ₽",
            "total_orders_current": "Все заказы оттенка", "total_revenue_current": "Выручка оттенка, ₽",
            "ad_impressions_current": "Показы рекламы", "ad_clicks_current": "Клики рекламы", "ad_orders_current": "Рекламные заказы",
            "ad_spend_current": "Расход рекламы, ₽", "median_position": "Медианная позиция", "visibility_pct": "Видимость, %",
            "ctr_pct": "CTR, %", "cr_pct": "CR, %", "core_score": "Оценка core", "shade_status": "Статус оттенка", "core_nm_id": "Core Артикул WB"
        })
    shade_actions = results.get("shade_actions", pd.DataFrame()).copy()
    if shade_actions.empty:
        shade_actions = pd.DataFrame([{"Комментарий": "По текущему запуску действий по оттенкам нет"}])
    else:
        shade_actions = shade_actions.rename(columns={
            "advert_id": "ID кампании", "product_root": "Товар", "subject": "Предмет", "subject_norm": "Предмет (норм.)",
            "core_nm_id": "Core Артикул WB", "core_supplier_article": "Core артикул продавца", "candidate_nm_id": "Кандидат Артикул WB",
            "candidate_supplier_article": "Кандидат артикул продавца", "shade_action": "Действие по оттенку", "reason": "Обоснование",
            "payment_type": "Тип оплаты", "placement": "Плейсменты", "placement_primary": "Основной плейсмент",
            "rating_reviews": "Рейтинг", "candidate_score": "Оценка кандидата", "root_blended_drr_pct": "Общий ДРР товара, %",
            "root_order_growth_pct": "Рост заказов товара, %", "min_wb_bid_rub": "Минимальная ставка WB, ₽", "min_bid_status": "Статус проверки минимума"
        })
    shade_tests = results.get("shade_tests", pd.DataFrame()).copy()
    if shade_tests.empty:
        shade_tests = pd.DataFrame([{"Комментарий": "Активных тестов оттенков нет"}])
    else:
        shade_tests = shade_tests.rename(columns={
            "start_date": "Дата старта", "last_eval_date": "Дата последней оценки", "advert_id": "ID кампании", "product_root": "Товар",
            "subject": "Предмет", "core_nm_id": "Core Артикул WB", "candidate_nm_id": "Кандидат Артикул WB",
            "candidate_supplier_article": "Кандидат артикул продавца", "status": "Статус теста", "test_target_impressions": "Целевые показы",
            "collected_impressions": "Собрано показов", "collected_clicks": "Собрано кликов", "collected_orders": "Собрано заказов",
            "min_wb_bid_rub": "Минимальная ставка WB, ₽", "reason": "Обоснование", "remove_date": "Дата удаления"
        })
    benchmark_cmp = build_benchmark_clean_export(results)
    return {
        "Решения_по_ставкам": _round_export_numbers(decisions),
        "Расчёт_логики": _round_export_numbers(logic),
        "Статистика_по_товарам": _round_export_numbers(product),
        "Эффективность_ставки": _round_export_numbers(eff),
        "Слабая_позиция": _round_export_numbers(weak),
        "Эффект_изменений": _round_export_numbers(effects),
        "Лимиты_ставок": _round_export_numbers(limits),
        "Состав_кампаний_по_оттенкам": _round_export_numbers(shade_portfolio),
        "Рекомендации_по_оттенкам": _round_export_numbers(shade_actions),
        "Тесты_оттенков": _round_export_numbers(shade_tests),
        "Сравнение_с_сильными_РК": _round_export_numbers(benchmark_cmp),
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
    provider.write_excel(SERVICE_EFF_KEY, build_bid_efficiency_history_sheets(results))
    provider.write_excel(SERVICE_WEAK_KEY, {"Список артикулов": build_simple_weak_export(results)})
    provider.write_excel(SERVICE_EFFECTS_KEY, {"Эффект изменений": preview_sheets["Эффект_изменений"]})
    if "Состав_кампаний_по_оттенкам" in preview_sheets:
        provider.write_excel(SERVICE_SHADE_PORTFOLIO_KEY, {"Состав кампаний по оттенкам": preview_sheets["Состав_кампаний_по_оттенкам"]})
    if "Рекомендации_по_оттенкам" in preview_sheets:
        provider.write_excel(SERVICE_SHADE_ACTIONS_KEY, {"Рекомендации по оттенкам": preview_sheets["Рекомендации_по_оттенкам"]})
    if "Тесты_оттенков" in preview_sheets:
        provider.write_excel(SERVICE_SHADE_TESTS_KEY, {"Тесты оттенков": preview_sheets["Тесты_оттенков"]})
    if "Сравнение_с_сильными_РК" in preview_sheets:
        provider.write_excel(SERVICE_BENCHMARKS_KEY, {"Сравнение с сильными РК": preview_sheets["Сравнение_с_сильными_РК"]})

    shade_actions = results.get("shade_actions", pd.DataFrame())
    summary = {
        "generated_at": datetime.now().isoformat(),
        "mode": mode,
        "as_of_date": str(as_of_date),
        "recommendations_count": int(len(decisions)),
        "changed_count": int(decisions[decisions["action"].isin(["UP", "DOWN", "TEST_UP"]) & (decisions["new_bid_rub"].round(2) != decisions["current_bid_rub"].round(2))].shape[0]),
        "limit_reached_count": int(decisions[decisions["action"] == "LIMIT_REACHED"].shape[0]),
        "weak_items_count": int(len(weak)),
        "shade_actions_count": int(len(shade_actions)),
        "shade_add_test_count": int((shade_actions.get("shade_action", pd.Series(dtype=str)) == "ADD_TEST").sum()) if isinstance(shade_actions, pd.DataFrame) and not shade_actions.empty else 0,
        "shade_remove_count": int((shade_actions.get("shade_action", pd.Series(dtype=str)) == "REMOVE_SHADE").sum()) if isinstance(shade_actions, pd.DataFrame) and not shade_actions.empty else 0,
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

    if mode in {"preview", "report"}:
        log("🧪 Preview/report-режим: ставки и изменения оттенков не отправлялись")
        return

    if mode == "run":
        api_key = os.environ.get("WB_PROMO_KEY_TOPFACE", "").strip()
        if not api_key:
            raise RuntimeError("Не задан WB_PROMO_KEY_TOPFACE")
        payload = decisions_to_payload(decisions)
        send_log = send_payload(payload, api_key=api_key, dry_run=False)
        if not send_log.empty:
            append_or_replace_history(provider, SERVICE_ARCHIVE_KEY, send_log.assign(kind="wb_send"), sheet_name="archive", tail=50000)
        if getattr(args, "apply_shades", False):
            shade_actions = results.get("shade_actions", pd.DataFrame())
            shade_apply_df = shade_actions[shade_actions.get("shade_action", pd.Series(dtype=str)) == "ADD_TEST"].copy() if isinstance(shade_actions, pd.DataFrame) and not shade_actions.empty else pd.DataFrame()
            shade_send_log = patch_campaign_nms(api_key, shade_apply_df, dry_run=False)
            if not shade_send_log.empty:
                append_or_replace_history(provider, SERVICE_ARCHIVE_KEY, shade_send_log.assign(kind="wb_shade_send"), sheet_name="archive", tail=50000)
            log(f"🧩 Отправлено действий по оттенкам: {len(shade_apply_df)}")
        update_bid_history(provider, decisions, as_of_date)
        save_experiments(provider, decisions, as_of_date)
        log(f"📤 Отправлено блоков ставок в WB: {len(payload.get('bids', []))}")
        return

    raise RuntimeError(f"Неизвестная команда: {mode}")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="WB Ads Manager V2 — balanced growth algorithm")
    sub = p.add_subparsers(dest="command")

    for cmd in ["preview", "run", "report"]:
        sc = sub.add_parser(cmd)
        sc.add_argument("--local-data-dir", type=str, default=None, help="Локальная папка с выгруженными файлами")
        sc.add_argument("--as-of-date", type=str, default=None, help="Дата запуска YYYY-MM-DD")
        if cmd == "run":
            sc.add_argument("--apply-shades", action="store_true", help="Применять изменения состава кампаний по оттенкам")
    return p


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.command is None:
        args.command = "preview"
    run_manager(args)


def determine_action(row: pd.Series, config: ManagerConfig, as_of_date: date) -> Tuple[str, float, str, bool]:
    subject = canonical_subject(row.get("subject_norm", row.get("subject", "")))
    payment_type = str(row.get("payment_type", "cpc")).lower()
    placement = str(row.get("placement", "search"))
    current_bid = safe_float(row.get("current_bid_rub", 0))
    comfort_bid = safe_float(row.get("comfort_bid_rub", 0))
    max_bid = safe_float(row.get("max_bid_rub", 0))
    experiment_bid = safe_float(row.get("experiment_bid_rub", 0))
    mode = str(row.get("mode", "balanced")).strip().lower()

    blended_drr = safe_float(row.get("blended_drr_current_pct", row.get("current_blended_drr_pct", 0)))
    blended_prev = safe_float(row.get("blended_drr_prev_pct", 0))
    total_orders = safe_float(row.get("root_total_orders_current", row.get("total_orders_current", 0)))
    total_orders_prev = safe_float(row.get("root_total_orders_prev", row.get("total_orders_prev", 0)))
    spend = safe_float(row.get("ad_spend_root_current", 0))
    spend_prev = safe_float(row.get("ad_spend_root_prev", 0))
    gp_realized = safe_float(row.get("gp_realized", 0))
    rating = safe_float(row.get("rating_reviews", 0))
    buyout_rate = safe_float(row.get("buyout_rate", 0))
    position = safe_float(row.get("median_position", 0))
    visibility = safe_float(row.get("visibility_pct", 0))
    bei_imp = safe_float(row.get("bei_imp", 1))
    bei_click = safe_float(row.get("bei_click", 1))
    capture_imp = safe_float(row.get("capture_imp", 0))
    capture_click = safe_float(row.get("capture_click", 0))
    vs_peer_capture_imp = safe_float(row.get("vs_peer_capture_imp", 1))
    vs_peer_capture_click = safe_float(row.get("vs_peer_capture_click", 1))
    vs_peer_ctr = safe_float(row.get("vs_peer_ctr", 1))
    benchmark_problem_flag = bool(row.get("benchmark_problem_flag", False))
    card_issue = bool(row.get("card_issue", False))
    effect_flag = str(row.get("effect_flag", "") or "").strip().lower()

    comfort_drr, max_drr, weekend_drr = get_blended_caps(subject, config)
    order_growth = pct(total_orders - total_orders_prev, total_orders_prev) if total_orders_prev > 0 else (100.0 if total_orders > 0 else 0.0)
    spend_growth = pct(spend - spend_prev, spend_prev) if spend_prev > 0 else (100.0 if spend > 0 else 0.0)
    required_growth = compute_required_growth(blended_drr, blended_prev, spend_growth, subject)

    weak_position = (position == 0 or position > 20 or visibility < 5)
    poor_capture_vs_peers = (vs_peer_capture_imp < 0.70 and vs_peer_capture_click < 0.70)
    poor_ctr_vs_peers = vs_peer_ctr < 0.80
    traffic_not_efficient = (bei_imp < 0.90 and bei_click < 0.90)
    strong_response = (bei_imp > 1.10 or bei_click > 1.10 or capture_imp > 0.03 or capture_click > 0.002)
    empty_previous_changes = effect_flag in {"no_effect", "negative", "weak", "пусто", "без эффекта"}
    growth_subject = subject in GROWTH_SUBJECTS
    growth_like = growth_subject or mode in {"growth", "hero"}
    root_supports_growth = growth_like and blended_drr <= max_drr * 100 and (order_growth > 0 or blended_drr <= comfort_drr * 100)
    headroom_to_comfort = current_bid < comfort_bid * 0.98 if comfort_bid > 0 else False
    headroom_to_max = current_bid < max_bid * 0.98 if max_bid > 0 else False

    def min_bid_value() -> float:
        if payment_type == "cpc":
            return MIN_CPC_RUB
        return MIN_CPM_RECOMMENDATIONS_RUB if placement == "recommendations" else MIN_CPM_SEARCH_RUB

    def finalize(action: str, proposed_bid: float, reason: str, flag: bool = False) -> Tuple[str, float, str, bool]:
        bid = round(safe_float(proposed_bid, 0), 2)
        current = round(current_bid, 2)
        min_bid = round(min_bid_value(), 2)
        # финальный фильтр по ДРР > 15%
        if growth_subject:
            control_limit = GROWTH_BLENDED_DRR_MAX * 100
        elif subject == canonical_subject("кисти косметические"):
            control_limit = GROWTH_BLENDED_DRR_MAX * 100
        else:
            control_limit = max_drr * 100
        if action in {"UP", "TEST_UP"} and blended_drr > control_limit:
            if current >= max(min_bid, max_bid * 0.95 if max_bid > 0 else current) or traffic_not_efficient:
                return "LIMIT_REACHED", current, f"Общий ДРР {blended_drr:.1f}% выше лимита {control_limit:.1f}% — повышение ставки запрещено", True
            return "HOLD", current, f"Общий ДРР {blended_drr:.1f}% выше лимита {control_limit:.1f}% — повышение ставки запрещено", False
        if action == "DOWN":
            bid = min(current, max(min_bid, bid))
            if bid >= current or abs(bid - current) < 0.01:
                return "HOLD", current, "Ставка уже находится на минимально допустимом уровне", flag
        elif action in {"UP", "TEST_UP"}:
            bid = max(current, bid)
            if bid <= current or abs(bid - current) < 0.01:
                return "HOLD", current, "Текущая ставка уже находится в рабочем диапазоне", flag
        return action, bid, reason, flag

    if weak_position and traffic_not_efficient and poor_capture_vs_peers and current_bid >= max(min_bid_value(), max_bid * 0.90 if max_bid > 0 else current_bid):
        return "LIMIT_REACHED", round(current_bid, 2), "Повысить эффективность ставки — реклама работает на пределе", True

    local_economy_problem = gp_realized <= 0 or rating < MIN_RATING or buyout_rate < MIN_BUYOUT
    if local_economy_problem:
        if growth_like and blended_drr <= comfort_drr * 100:
            return "HOLD", round(current_bid, 2), "Локально слабая экономика/выкуп, но общий ДРР рабочий — товар не сушим", False
        if root_supports_growth and weak_position:
            return "HOLD", round(current_bid, 2), "Локально слабая экономика, но товар целиком растёт — ставку не сушим", False
        if not growth_like:
            return finalize("DOWN", min_bid_value(), "Негативная экономика / рейтинг / выкуп", False)

    if card_issue:
        if growth_like and blended_drr <= comfort_drr * 100:
            return "HOLD", round(current_bid, 2), "Есть проблема в карточке, но общий ДРР рабочий — сначала исправляем карточку", False
        if growth_like and root_supports_growth and weak_position:
            return "HOLD", round(current_bid, 2), "Есть проблема в карточке, но товар целиком растёт — ставку не сушим", False
        if not growth_like and current_bid > max(comfort_bid, min_bid_value()):
            return finalize("DOWN", max(comfort_bid, current_bid * (1 - DOWN_STEP_SMALL)), "Проблема в карточке / воронке", False)

    if growth_like and weak_position and benchmark_problem_flag and blended_drr <= max_drr * 100:
        if headroom_to_comfort:
            return finalize("UP", min(comfort_bid if comfort_bid > 0 else current_bid * (1 + UP_STEP_MED), current_bid * (1 + UP_STEP_MED)), "Есть большой спрос и отставание от сильных РК — подтягиваем ставку к комфортной", False)
        if headroom_to_max and not empty_previous_changes:
            return finalize("TEST_UP", min(max_bid if max_bid > 0 else current_bid * (1 + UP_STEP_SMALL), current_bid * (1 + UP_STEP_SMALL)), "Есть большой спрос и отставание от сильных РК — запускаем тест роста", False)

    hard_negatives = 0
    if blended_drr > max_drr * 100:
        hard_negatives += 1
    if order_growth < required_growth:
        hard_negatives += 1
    if empty_previous_changes:
        hard_negatives += 1
    if traffic_not_efficient or poor_capture_vs_peers or poor_ctr_vs_peers:
        hard_negatives += 1
    if card_issue or local_economy_problem:
        hard_negatives += 1

    if blended_drr > max_drr * 100 and order_growth < required_growth:
        if growth_like:
            if hard_negatives >= 3:
                return finalize("DOWN", max(comfort_bid, current_bid * (1 - DOWN_STEP_MED)), f"Общий ДРР {blended_drr:.1f}% выше лимита {max_drr*100:.1f}% и рост заказов недостаточный", False)
            return "HOLD", round(current_bid, 2), "Общий ДРР повышен, но для growth-категории пока сохраняем ставку и наблюдаем", False
        return finalize("DOWN", max(comfort_bid, current_bid * (1 - DOWN_STEP_MED)), f"Общий ДРР {blended_drr:.1f}% выше лимита {max_drr*100:.1f}% и рост заказов недостаточный", False)

    if weak_position and not card_issue:
        if headroom_to_comfort and (order_growth >= required_growth or growth_like or benchmark_problem_flag):
            step = UP_STEP_BIG if growth_like else UP_STEP_MED
            target = current_bid * (1 + step)
            if comfort_bid > 0:
                target = min(comfort_bid, max(target, comfort_bid * 0.90 if current_bid == 0 else target))
            return finalize("UP", target, "Слабая позиция: подтягиваем ставку к комфортной", False)
        if headroom_to_max and (order_growth >= required_growth or strong_response or benchmark_problem_flag):
            step = UP_STEP_MED if growth_like else UP_STEP_SMALL
            target = current_bid * (1 + step)
            if max_bid > 0:
                target = min(max_bid, target)
            return finalize("UP", target, "Есть запас по max-ставке и потенциал роста позиции", False)
        if growth_like and blended_drr <= max_drr * 100:
            return "HOLD", round(current_bid, 2), "Слабая позиция, но товар ростовый — держим ставку и копим сигнал", False

    if growth_like and as_of_date.weekday() in config.experiment_weekdays:
        experiment_cap = weekend_drr * 100
        if blended_drr <= experiment_cap and current_bid < experiment_bid and weak_position and not empty_previous_changes:
            new_bid = min(experiment_bid if experiment_bid > 0 else current_bid * 1.12, max(current_bid * 1.12, max_bid))
            return finalize("TEST_UP", new_bid, "Выходной эксперимент выше max для growth-категории", False)

    if spend_growth > 0 and order_growth < required_growth:
        if growth_like and blended_drr <= comfort_drr * 100:
            return "HOLD", round(current_bid, 2), "Общий ДРР в рабочей зоне — ставку не сушим, несмотря на слабый рост", False
        if growth_like and benchmark_problem_flag and weak_position and headroom_to_max:
            target = min(max_bid if max_bid > 0 else current_bid * 1.05, current_bid * 1.05)
            return finalize("TEST_UP", target, "Есть рынок, но рост пока слабый — проводим тест роста", False)
        return finalize("DOWN", max(comfort_bid, current_bid * (1 - DOWN_STEP_SMALL)), "Рост расходов не поддержан ростом заказов", False)

    if current_bid > max_bid and max_bid > 0:
        if growth_like and blended_drr <= max_drr * 100 and current_bid <= max_bid * 1.20:
            return "HOLD", round(current_bid, 2), "Ставка немного выше расчётного max, но товар ещё в growth-режиме", False
        return finalize("DOWN", max_bid, "Текущая ставка выше расчётного max", False)

    if growth_like:
        return "HOLD", round(current_bid, 2), "Growth-категория: базовый режим удержания ставки", False

    return "HOLD", round(current_bid, 2), "Рабочий диапазон ставки", False

if __name__ == "__main__":
    main()
