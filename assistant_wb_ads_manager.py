
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
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import boto3
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
WB_BIDS_URL = "https://advert-api.wildberries.ru/api/advert/v1/bids"
WB_BIDS_MIN_URL = "https://advert-api.wildberries.ru/api/advert/v1/bids/min"
WB_NMS_URL = "https://advert-api.wildberries.ru/adv/v0/auction/nms"

ADS_ANALYSIS_KEY = f"Отчёты/Реклама/{STORE_NAME}/Анализ рекламы.xlsx"
ECONOMICS_KEY = f"Отчёты/Финансовые показатели/{STORE_NAME}/Экономика.xlsx"
FUNNEL_KEY = f"Отчёты/Воронка продаж/{STORE_NAME}/Воронка продаж.xlsx"
ORDERS_WEEKLY_PREFIX = f"Отчёты/Заказы/{STORE_NAME}/Недельные/"
KEYWORDS_WEEKLY_PREFIX = f"Отчёты/Поисковые запросы/{STORE_NAME}/Недельные/"

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
OUT_SHADE_ACTIONS = SERVICE_ROOT + "Рекомендации_по_оттенкам.xlsx"
OUT_SHADE_PORTFOLIO = SERVICE_ROOT + "Состав_кампаний_по_оттенкам.xlsx"
OUT_SHADE_TESTS = SERVICE_ROOT + "Тесты_оттенков.xlsx"
OUT_BENCHMARK = SERVICE_ROOT + "Сравнение_с_сильными_РК.xlsx"

MIN_RATING_SHADE = 4.6
MATURE_START_OFFSET = 7
MATURE_END_OFFSET = 3
WINDOW_LEN = 5

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

def safe_int(v: Any, default: int = 0) -> int:
    try:
        if pd.isna(v):
            return default
        if isinstance(v, str):
            v = v.replace("\xa0", " ").replace(",", ".").strip()
        return int(float(v))
    except Exception:
        return default

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
                    val = "" if cell.value is None else str(cell.value)
                    width = min(max(len(val) + 2, 10), 40)
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
    df = df.rename(columns={"Артикул WB":"nmId","Артикул продавца":"supplier_article","Предмет":"subject"})
    df["subject_norm"] = df["subject"].map(canonical_subject)
    df = df[df["subject_norm"].isin(TARGET_SUBJECTS)].copy()
    df["product_root"] = df["supplier_article"].map(product_root_from_supplier_article)
    df["buyout_rate"] = df.get("Процент выкупа", 0).map(lambda x: safe_float(x) / 100.0 if safe_float(x) > 1 else safe_float(x))
    df["gp_unit"] = df.get("Валовая прибыль, руб/ед", 0).map(safe_float)
    df["np_unit"] = df.get("Чистая прибыль, руб/ед", 0).map(safe_float)
    df["gp_realized"] = df["gp_unit"] * df["buyout_rate"].clip(lower=0, upper=1)
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
    df = df.rename(columns={"nmID":"nmId","dt":"date"})
    df["date"] = parse_date_col(df["date"])
    for c in ["openCardCount","addToCartCount","ordersCount","buyoutsCount","addToCartConversion","cartToOrderConversion","buyoutPercent"]:
        if c in df.columns:
            df[c] = df[c].map(safe_float)
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
    df["date"] = df["run_ts"].dt.date
    df["bid_rub"] = df.get("Ставка поиск, коп", 0).map(lambda x: safe_float(x) / 100.0 if safe_float(x) else safe_float(df.get("Ставка рекомендации, коп", 0)))
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
    return product_root if subject_norm in GROWTH_SUBJECTS else supplier_article

def build_subject_benchmarks(rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return pd.DataFrame(columns=["subject_norm","placement","bench_ctr","bench_capture_imp","bench_capture_click"])
    df = rows.copy()
    df["capture_imp"] = df["capture_imp"].map(safe_float)
    df["capture_click"] = df["capture_click"].map(safe_float)
    df["ctr_pct"] = df["ctr_pct"].map(safe_float)
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
    current_bid = safe_float(row.get("current_bid_rub"))
    payment_type = str(row.get("payment_type","cpm"))
    placement = str(row.get("placement","search"))
    # choose reliable clicks per order
    limit_type = "Нет данных"
    cpo = None
    if safe_float(row.get("Клики")) >= 50 and safe_float(row.get("Заказы")) >= 3:
        cpo = safe_float(row.get("Клики")) / max(safe_float(row.get("Заказы")), 1.0)
        limit_type = "Фактический"
    elif safe_float(row.get("control_ad_clicks")) >= 50 and safe_float(row.get("total_orders")) >= 5:
        cpo = safe_float(row.get("control_ad_clicks")) / max(safe_float(row.get("total_orders")), 1.0)
        limit_type = "Наследуемый"
    if gp_realized <= 0 or not cpo or cpo <= 0:
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
            bench = subject_benchmarks[(subject_benchmarks["subject_norm"] == subject_norm) & (subject_benchmarks["placement"] == placement)]
            ctr = safe_float(bench["bench_ctr"].iloc[0]) / 100.0 if not bench.empty else 0.02
        ctr = max(ctr, 0.005)
        comfort_bid = round(comfort_cpc * 1000 * ctr, 2)
        max_bid = round(max_cpc * 1000 * ctr, 2)
    if payment_type == "cpc":
        comfort_bid = clamp(comfort_bid, 4.0, 150.0)
        max_bid = clamp(max_bid, 4.0, 300.0)
    else:
        low = 80.0 if placement == "recommendation" else 80.0
        high = 700.0 if placement in {"search","combined"} else 1200.0
        comfort_bid = clamp(comfort_bid, low, high)
        max_bid = clamp(max_bid, low, high)
    experiment_bid = round(min(max_bid * 1.15, max_bid), 2)
    return round(comfort_bid, 2), round(max_bid, 2), round(experiment_bid, 2), limit_type

def determine_action(row: pd.Series, cfg: Config) -> Tuple[str, float, str, bool]:
    subject_norm = row["subject_norm"]
    current_bid = safe_float(row["current_bid_rub"])
    comfort_bid = row.get("comfort_bid_rub")
    max_bid = row.get("max_bid_rub")
    total_orders = safe_float(row.get("total_orders"))
    ad_orders = safe_float(row.get("Заказы"))
    blended_drr = safe_float(row.get("blended_drr"))
    order_growth = safe_float(row.get("order_growth_pct"))
    required_growth = safe_float(row.get("required_growth_pct"))
    position = safe_float(row.get("median_position"))
    demand = safe_float(row.get("demand_week"))
    rating = safe_float(row.get("rating_reviews"))
    buyout = safe_float(row.get("buyout_rate"))
    gp_realized = safe_float(row.get("gp_realized"))
    weak_card = bool(row.get("card_issue"))
    weak_eff = safe_float(row.get("eff_index_click")) < 0.7 if pd.notna(row.get("eff_index_click")) else False
    growth = subject_norm in GROWTH_SUBJECTS
    rate_limit = False

    if pd.notna(max_bid) and safe_float(max_bid) > 0:
        rate_limit = current_bid >= safe_float(max_bid) * 0.95

    # If no reliable limits and no sales, collect data only
    if (pd.isna(max_bid) or safe_float(max_bid) <= 0) and total_orders <= 0 and ad_orders <= 0:
        return "Без изменений", current_bid, "Недостаточно данных для расчёта лимитов, собираем статистику", rate_limit

    # Final hard filter by blended DRR > 15%
    if blended_drr > cfg.max_drr:
        if rate_limit or weak_eff:
            return "Предел эффективности ставки", current_bid, f"Общий ДРР {blended_drr*100:.1f}% выше 15%: дальше ставкой расти нецелесообразно", True
        if current_bid > 0 and order_growth < required_growth:
            new_bid = round(current_bid * (1 - cfg.down_step), 2)
            return "Снизить", max(new_bid, 4.0 if str(row.get("payment_type")) == "cpc" else 80.0), f"Общий ДРР {blended_drr*100:.1f}% выше 15% и рост заказов слабый", rate_limit
        return "Без изменений", current_bid, f"Общий ДРР {blended_drr*100:.1f}% выше 15%: рост запрещён финальным фильтром", rate_limit

    if gp_realized <= 0 or rating and rating < 4.5 or buyout and buyout < 0.70:
        if growth:
            return "Без изменений", current_bid, "Локальная экономика слабая: для growth-товара не режем автоматически, наблюдаем", rate_limit
        if current_bid > 0:
            new_bid = round(current_bid * (1 - cfg.down_step), 2)
            return "Снизить", max(new_bid, 4.0 if str(row.get("payment_type")) == "cpc" else 80.0), "Негативная экономика / рейтинг / выкуп", rate_limit
        return "Без изменений", current_bid, "Негативная экономика / рейтинг / выкуп", rate_limit

    weak_position = position <= 0 or position > 15
    demand_high = demand >= 3000
    can_raise = pd.notna(max_bid) and safe_float(max_bid) > current_bid + 0.01

    # Strong sign that ставка уже не помогает
    if weak_eff and rate_limit and weak_position:
        return "Предел эффективности ставки", current_bid, "Ставка близка к максимуму, а трафик/позиция не улучшаются", True

    if growth:
        # default to HOLD for growth categories
        if weak_position and demand_high and can_raise and not weak_card:
            step = cfg.test_up_step if blended_drr >= cfg.comfort_drr_max else cfg.max_up_step
            proposed = round(current_bid * (1 + step), 2)
            new_bid = min(round(safe_float(max_bid), 2), proposed)
            if blended_drr <= cfg.comfort_drr_max:
                return "Повысить", new_bid, "Есть запас по max-ставке и потенциал роста позиции", rate_limit
            return "Тест роста", new_bid, "Запускаем осторожный тест роста в зоне 12–15%", rate_limit
        if weak_card and order_growth < required_growth:
            return "Предел эффективности ставки", current_bid, "Проблема в карточке / воронке: ставкой дальше не лечится", True
        if current_bid > safe_float(max_bid) > 0 and order_growth < required_growth:
            return "Без изменений", current_bid, "Ставка выше расчётного max, но товар ростовый: не режем автоматически", rate_limit
        return "Без изменений", current_bid, "Growth-товар: удерживаем ставку, пока нет сильного сигнала на снижение", rate_limit

    # Brushes and others
    severe = 0
    severe += 1 if weak_card else 0
    severe += 1 if weak_eff else 0
    severe += 1 if order_growth < required_growth else 0
    severe += 1 if weak_position and demand_high else 0

    if weak_position and demand_high and can_raise and order_growth >= 0:
        proposed = round(current_bid * (1 + cfg.max_up_step), 2)
        return "Повысить", min(round(safe_float(max_bid), 2), proposed), "Слабая позиция: подтягиваем ставку к комфортной", rate_limit
    if severe >= 3 and current_bid > 0:
        new_bid = round(current_bid * (1 - cfg.down_step), 2)
        return "Снизить", max(new_bid, 4.0 if str(row.get("payment_type")) == "cpc" else 80.0), "Проблема в карточке / воронке или рост заказов слабый", rate_limit
    return "Без изменений", current_bid, "Без изменений", rate_limit

def fetch_wb_min_bids(api_key: str, advert_id: int, nm_ids: List[int], payment_type: str, placement_types: List[str]) -> Dict[int, float]:
    if not api_key or not nm_ids:
        return {}
    headers = {"Authorization": api_key.strip(), "Content-Type": "application/json"}
    body = {"advert_id": advert_id, "nm_ids": nm_ids[:100], "payment_type": payment_type, "placement_types": placement_types}
    try:
        resp = requests.post(WB_BIDS_MIN_URL, headers=headers, json=body, timeout=60)
        if resp.status_code != 200:
            return {}
        data = resp.json()
        out: Dict[int, float] = {}
        for item in data.get("bids", []):
            nm = safe_int(item.get("nm_id"))
            bids = item.get("bids", []) or []
            vals = [safe_float(x.get("value")) / 100.0 for x in bids if safe_float(x.get("value")) > 0]
            if vals:
                out[nm] = min(vals)
        return out
    except Exception:
        return {}

def build_shade_portfolio(campaigns: pd.DataFrame, master: pd.DataFrame, orders_60: pd.DataFrame) -> pd.DataFrame:
    if campaigns.empty:
        return pd.DataFrame()
    df = campaigns[campaigns["subject_norm"].isin(GROWTH_SUBJECTS)].copy()
    if df.empty:
        return pd.DataFrame()
    df = df.merge(master[["nmId","supplier_article","product_root","rating_reviews"]].drop_duplicates(), on="nmId", how="left")
    ord_map = orders_60.groupby("supplier_article", as_index=False).agg(total_orders_60=("nmId", "count"))
    df = df.merge(ord_map, on="supplier_article", how="left")
    df["total_orders_60"] = df["total_orders_60"].fillna(0)
    core_rows = []
    for advert_id, g in df.groupby("id_campaign"):
        g = g.sort_values(["total_orders_60","rating_reviews"], ascending=[False, False]).copy()
        core_article = g["supplier_article"].iloc[0] if not g.empty else ""
        g["роль"] = g["supplier_article"].eq(core_article).map({True:"CORE", False:"WORKING"})
        core_rows.append(g)
    return pd.concat(core_rows, ignore_index=True) if core_rows else pd.DataFrame()

def build_shade_actions(campaigns: pd.DataFrame, portfolio: pd.DataFrame, master: pd.DataFrame, orders_60: pd.DataFrame, product_metrics: pd.DataFrame, api_key: str = "") -> Tuple[pd.DataFrame, pd.DataFrame]:
    if campaigns.empty or portfolio.empty:
        return pd.DataFrame([{"Комментарий":"Нет подходящих кампаний для анализа оттенков"}]), pd.DataFrame([{"Комментарий":"Нет активных тестов оттенков"}])
    actions = []
    tests_rows = []
    ord_map = orders_60.groupby("supplier_article", as_index=False).agg(
        total_orders_60=("nmId","count"),
        revenue_60=("finishedPrice","sum"),
        nmId=("nmId","first"),
        subject=("subject","first"),
    )
    ord_map["product_root"] = ord_map["supplier_article"].map(product_root_from_supplier_article)
    bench_rating = master[["supplier_article","rating_reviews","product_root","nmId"]].drop_duplicates()
    universe = ord_map.merge(bench_rating, on=["supplier_article","product_root","nmId"], how="left")
    control_drr = product_metrics[["control_key","blended_drr","subject_norm"]].drop_duplicates()

    for advert_id, g in portfolio.groupby("id_campaign"):
        current = g.iloc[0]
        product_root = current["product_root"]
        subject_norm = current["subject_norm"]
        control = control_drr[control_drr["control_key"] == product_root]
        blended = safe_float(control["blended_drr"].iloc[0]) if not control.empty else 0.0
        if blended > 0.15:
            actions.append({"ID кампании": advert_id, "Товар": product_root, "Действие":"Нет действий", "Причина":"Общий ДРР товара выше 15%, новые оттенки не добавляем"})
            continue
        used = set(g["supplier_article"].dropna().astype(str))
        cand = universe[(universe["product_root"] == product_root) & (~universe["supplier_article"].isin(used))].copy()
        cand = cand[cand["rating_reviews"].fillna(0) > MIN_RATING_SHADE]
        if cand.empty:
            actions.append({"ID кампании": advert_id, "Товар": product_root, "Действие":"Нет действий", "Причина":"Нет подходящих оттенков с рейтингом > 4.6"})
            continue
        cand = cand.sort_values(["total_orders_60","rating_reviews"], ascending=[False, False])
        best = cand.iloc[0]
        actions.append({
            "ID кампании": advert_id,
            "Товар": product_root,
            "Предмет": current["subject"],
            "Текущий CORE": current["supplier_article"],
            "Новый оттенок": best["supplier_article"],
            "Артикул WB": safe_int(best["nmId"]),
            "Действие":"Добавить тестовый оттенок",
            "Минимальная ставка WB, ₽": None,
            "Причина":"Расширяем охват товара новым оттенком, старт с минимальной ставки WB",
            "Действие API":"add",
            "Статус применения":"ожидает",
            "Тип кампании": f'{current["payment_type"]}_{current["placement"]}',
        })
    actions_df = pd.DataFrame(actions)
    if actions_df.empty:
        actions_df = pd.DataFrame([{"Комментарий":"Нет действий по оттенкам"}])
    if api_key and not actions_df.empty and "Артикул WB" in actions_df.columns:
        for idx, r in actions_df[actions_df["Действие"].eq("Добавить тестовый оттенок")].iterrows():
            mins = fetch_wb_min_bids(api_key, safe_int(r["ID кампании"]), [safe_int(r["Артикул WB"])], "cpm" if "cpm" in str(r.get("Тип кампании","")) else "cpc", ["combined" if "combined" in str(r.get("Тип кампании","")) else ("search" if "search" in str(r.get("Тип кампании","")) else "recommendation")])
            if mins:
                actions_df.at[idx, "Минимальная ставка WB, ₽"] = list(mins.values())[0]
                actions_df.at[idx, "Статус применения"] = "готово к применению"
    return actions_df, pd.DataFrame([{"Комментарий":"История тестов оттенков начнёт копиться после первого успешного добавления"}])

def apply_shade_actions(actions_df: pd.DataFrame, api_key: str, dry_run: bool) -> pd.DataFrame:
    if actions_df.empty or "Действие API" not in actions_df.columns:
        return pd.DataFrame([{"Комментарий":"Нет действий по оттенкам"}])
    add_rows = actions_df[(actions_df["Действие API"] == "add") & (actions_df["Минимальная ставка WB, ₽"].notna())].copy()
    if add_rows.empty:
        return pd.DataFrame([{"Комментарий":"Нет оттенков с подтверждённой минимальной ставкой WB"}])
    logs = []
    headers = {"Authorization": api_key.strip(), "Content-Type": "application/json"}
    for advert_id, g in add_rows.groupby("ID кампании"):
        payload = {"nms":[{"advert_id": safe_int(advert_id), "nms": {"add": [safe_int(x) for x in g["Артикул WB"].tolist()], "delete": []}}]}
        if dry_run:
            logs.append({"ID кампании": advert_id, "Статус":"dry-run", "Ответ": json.dumps(payload, ensure_ascii=False)})
            continue
        try:
            resp = requests.patch(WB_NMS_URL, headers=headers, json=payload, timeout=120)
            logs.append({"ID кампании": advert_id, "Статус":"ok" if resp.status_code == 200 else "failed", "Ответ": resp.text[:1000]})
        except Exception as e:
            logs.append({"ID кампании": advert_id, "Статус":"failed", "Ответ": str(e)})
        time.sleep(1.05)
    return pd.DataFrame(logs)

def build_efficiency_history(ads_daily: pd.DataFrame, campaigns: pd.DataFrame, keywords_daily: pd.DataFrame, master: pd.DataFrame, bid_history: pd.DataFrame, as_of_date: date) -> Dict[str, pd.DataFrame]:
    if ads_daily.empty:
        return {"Нет данных": pd.DataFrame([{"Комментарий":"Нет рекламной дневной статистики"}])}
    hist = ads_daily.merge(campaigns[["id_campaign","nmId","placement","payment_type","current_bid_rub"]].drop_duplicates(), on=["id_campaign","nmId"], how="left")
    hist = hist.merge(master[["nmId","supplier_article","subject"]].drop_duplicates(), on="nmId", how="left")
    hist = hist.merge(keywords_daily, on=["date","nmId","supplier_article"], how="left")
    hist["demand"] = hist.get("demand", 0).map(safe_float)
    hist["current_bid_rub"] = hist["current_bid_rub"].map(safe_float)
    # bid history merge_asof
    if not bid_history.empty:
        events = bid_history[["id_campaign","nmId","date","bid_rub"]].dropna().copy().sort_values(["id_campaign","nmId","date"])
        out_parts = []
        for (cid, nm), g in hist.groupby(["id_campaign","nmId"], dropna=False):
            gg = g.sort_values("date").copy()
            ev = events[(events["id_campaign"] == cid) & (events["nmId"] == nm)].copy()
            if not ev.empty:
                gg = pd.merge_asof(gg.sort_values("date"), ev[["date","bid_rub"]].sort_values("date"), on="date", direction="backward")
                gg["bid_rub"] = gg["bid_rub"].fillna(gg["current_bid_rub"])
            else:
                gg["bid_rub"] = gg["current_bid_rub"]
            out_parts.append(gg)
        hist = pd.concat(out_parts, ignore_index=True)
    else:
        hist["bid_rub"] = hist["current_bid_rub"]
    hist["ctr_pct"] = hist["CTR"].map(safe_float)
    hist["capture_imp"] = hist.apply(lambda r: safe_float(r["Показы"]) / safe_float(r["demand"]) if safe_float(r["demand"]) else math.nan, axis=1)
    hist["capture_click"] = hist.apply(lambda r: safe_float(r["Клики"]) / safe_float(r["demand"]) if safe_float(r["demand"]) else math.nan, axis=1)
    hist["eff_imp"] = hist.apply(lambda r: (safe_float(r["Показы"]) / safe_float(r["demand"]) / safe_float(r["bid_rub"])) if safe_float(r["demand"]) and safe_float(r["bid_rub"]) else math.nan, axis=1)
    hist["eff_click"] = hist.apply(lambda r: (safe_float(r["Клики"]) / safe_float(r["demand"]) / safe_float(r["bid_rub"])) if safe_float(r["demand"]) and safe_float(r["bid_rub"]) else math.nan, axis=1)
    hist["Тип кампании"] = hist["payment_type"].astype(str) + "_" + hist["placement"].astype(str)
    hist = hist.sort_values(["supplier_article","date","id_campaign"])

    # conclusions
    out_sheets: Dict[str, pd.DataFrame] = {}
    used_names = set()
    for article, g in hist.groupby("supplier_article"):
        if not str(article):
            continue
        g = g.copy().sort_values(["date","id_campaign"])
        conclusions = []
        prev_eff = {}
        for _, r in g.iterrows():
            key = (r["id_campaign"], r["Тип кампании"])
            cur = safe_float(r["eff_click"], math.nan)
            if math.isnan(cur):
                conclusions.append("Нет спроса или данных")
                continue
            prior = prev_eff.get(key, [])
            prior_valid = [x for x in prior if not math.isnan(x)]
            if len(prior_valid) >= 3:
                base = float(pd.Series(prior_valid[-7:]).median())
                if base > 0:
                    ratio = cur / base
                    if ratio >= 1.10:
                        conclusions.append("За ту же ставку начали получать больше кликов")
                    elif ratio <= 0.90:
                        conclusions.append("Эффективность ставки снижается")
                    else:
                        conclusions.append("Без существенных изменений")
                else:
                    conclusions.append("Недостаточно истории")
            else:
                conclusions.append("Недостаточно истории")
            prev_eff.setdefault(key, []).append(cur)
        sheet = pd.DataFrame({
            "Дата": g["date"],
            "ID кампании": g["id_campaign"],
            "Тип кампании": g["Тип кампании"],
            "Плейсмент": g["placement"],
            "Ставка, ₽": g["bid_rub"].round(2),
            "Показы": g["Показы"].round(0),
            "Клики": g["Клики"].round(0),
            "CTR, %": g["ctr_pct"].round(2),
            "Спрос": g["demand"].round(0),
            "Доля показов": (g["capture_imp"] * 100).round(4),
            "Доля кликов": (g["capture_click"] * 100).round(4),
            "Эффективность ставки по показам": g["eff_imp"].round(6),
            "Эффективность ставки по кликам": g["eff_click"].round(6),
            "Вывод": conclusions,
        })
        out_sheets[sanitize_sheet_name(str(article), used_names)] = sheet
    if not out_sheets:
        out_sheets = {"Нет данных": pd.DataFrame([{"Комментарий":"Нет истории эффективности ставки"}])}
    return out_sheets

def prepare_metrics(provider: BaseProvider, cfg: Config, as_of_date: date) -> Dict[str, Any]:
    window = compute_analysis_window(as_of_date)
    log(f"📅 Анализируем зрелое окно {window['cur_start']} .. {window['cur_end']}; база сравнения {window['base_start']} .. {window['base_end']}")
    ads_daily, campaigns = load_ads(provider)
    econ = load_economics(provider)
    orders = load_orders(provider)
    funnel = load_funnel(provider)
    keywords = load_keywords(provider)
    bid_history = load_bid_history(provider)
    log(f"📣 Реклама: {len(ads_daily):,} строк; кампании: {campaigns['id_campaign'].nunique() if not campaigns.empty else 0}; placement-строк: {len(campaigns):,}")
    log(f"💰 Экономика: {len(econ):,} SKU; Заказы: {len(orders):,} строк; Воронка: {len(funnel):,}; Keywords: {len(keywords):,}")

    master = build_master(econ, orders, keywords, campaigns)
    keywords_current = aggregate_keyword_item(keywords, window["cur_start"], window["cur_end"])
    keywords_daily = aggregate_keyword_daily(keywords)
    funnel_item, funnel_subject = build_funnel_item(funnel, master, window["cur_start"], window["cur_end"])

    econ_latest = econ.sort_values("Неделя").drop_duplicates("nmId", keep="last")[["nmId","supplier_article","product_root","subject","subject_norm","buyout_rate","gp_realized"]]
    campaign_base = campaigns.merge(master[["nmId","supplier_article","product_root","subject","subject_norm","rating_reviews","rating_card"]].drop_duplicates(), on="nmId", how="left")
    campaign_base = campaign_base.merge(econ_latest[["nmId","buyout_rate","gp_realized"]], on="nmId", how="left")
    if campaign_base.empty:
        raise RuntimeError("Нет кампаний целевых предметов в файле рекламы")

    campaign_cur = ads_daily[(ads_daily["date"] >= window["cur_start"]) & (ads_daily["date"] <= window["cur_end"])].groupby(["id_campaign","nmId"], as_index=False).agg(
        Показы=("Показы","sum"), Клики=("Клики","sum"), Заказы=("Заказы","sum"), Расход=("Расход","sum"), Сумма_заказов=("Сумма заказов","sum")
    )
    campaign_base_stats = ads_daily[(ads_daily["date"] >= window["base_start"]) & (ads_daily["date"] <= window["base_end"])].groupby(["id_campaign","nmId"], as_index=False).agg(
        base_Показы=("Показы","sum"), base_Клики=("Клики","sum"), base_Заказы=("Заказы","sum"), base_Расход=("Расход","sum"), base_Сумма_заказов=("Сумма заказов","sum")
    )
    rows = campaign_base.merge(campaign_cur, on=["id_campaign","nmId"], how="left").merge(campaign_base_stats, on=["id_campaign","nmId"], how="left").fillna(0)

    # control metrics
    rows["control_key"] = rows.apply(lambda r: choose_control_key(r["subject_norm"], r["supplier_article"], r["product_root"]), axis=1)
    orders_cur_root = aggregate_orders(orders, window["cur_start"], window["cur_end"], "product_root")
    orders_base_root = aggregate_orders(orders, window["base_start"], window["base_end"], "product_root").rename(columns={"total_orders":"base_total_orders","total_revenue":"base_total_revenue"})
    orders_cur_article = aggregate_orders(orders, window["cur_start"], window["cur_end"], "supplier_article")
    orders_base_article = aggregate_orders(orders, window["base_start"], window["base_end"], "supplier_article").rename(columns={"total_orders":"base_total_orders","total_revenue":"base_total_revenue"})

    ads_cur_root = aggregate_ads_control(ads_daily, window["cur_start"], window["cur_end"], master, "product_root")
    ads_base_root = aggregate_ads_control(ads_daily, window["base_start"], window["base_end"], master, "product_root").rename(columns={
        "ad_spend":"base_ad_spend","ad_clicks":"base_ad_clicks","ad_orders":"base_ad_orders","ad_impressions":"base_ad_impressions","ad_revenue":"base_ad_revenue"})
    ads_cur_article = aggregate_ads_control(ads_daily, window["cur_start"], window["cur_end"], master, "supplier_article")
    ads_base_article = aggregate_ads_control(ads_daily, window["base_start"], window["base_end"], master, "supplier_article").rename(columns={
        "ad_spend":"base_ad_spend","ad_clicks":"base_ad_clicks","ad_orders":"base_ad_orders","ad_impressions":"base_ad_impressions","base_ad_revenue":"ad_revenue","ad_revenue":"base_ad_revenue"})

    # attach based on control type
    root_rows = rows["subject_norm"].isin(GROWTH_SUBJECTS)
    growth_part = rows[root_rows].merge(orders_cur_root, left_on="control_key", right_on="product_root", how="left").merge(orders_base_root, left_on="control_key", right_on="product_root", how="left", suffixes=("","_b")).merge(ads_cur_root, left_on="control_key", right_on="product_root", how="left").merge(ads_base_root, left_on="control_key", right_on="product_root", how="left", suffixes=("","_ab"))
    brush_part = rows[~root_rows].merge(orders_cur_article, left_on="control_key", right_on="supplier_article", how="left").merge(orders_base_article, left_on="control_key", right_on="supplier_article", how="left", suffixes=("","_b")).merge(ads_cur_article, left_on="control_key", right_on="supplier_article", how="left").merge(ads_base_article, left_on="control_key", right_on="supplier_article", how="left", suffixes=("","_ab"))
    rows = pd.concat([growth_part, brush_part], ignore_index=True, sort=False).fillna(0)

    rows = rows.merge(keywords_current, on=["nmId","supplier_article"], how="left")
    rows = rows.merge(funnel_item, on="nmId", how="left").merge(funnel_subject, on="subject_norm", how="left")
    rows["ctr_pct"] = rows.apply(lambda r: pct(r["Клики"], r["Показы"]), axis=1)
    rows["capture_imp"] = rows.apply(lambda r: safe_float(r["Показы"]) / safe_float(r["demand_week"]) if safe_float(r["demand_week"]) else 0.0, axis=1)
    rows["capture_click"] = rows.apply(lambda r: safe_float(r["Клики"]) / safe_float(r["demand_week"]) if safe_float(r["demand_week"]) else 0.0, axis=1)
    rows["blended_drr"] = rows.apply(lambda r: safe_float(r["ad_spend"]) / safe_float(r["total_revenue"]) if safe_float(r["total_revenue"]) else 0.0, axis=1)
    rows["ad_drr"] = rows.apply(lambda r: safe_float(r["Расход"]) / safe_float(r["Сумма_заказов"]) if safe_float(r["Сумма_заказов"]) else 0.0, axis=1)
    rows["order_growth_pct"] = rows.apply(lambda r: growth_pct(r["total_orders"], r["base_total_orders"]), axis=1)
    rows["spend_growth_pct"] = rows.apply(lambda r: growth_pct(r["ad_spend"], r["base_ad_spend"]), axis=1)
    rows["drr_growth_pp"] = rows.apply(lambda r: (safe_float(r["blended_drr"]) - (safe_float(r["base_ad_spend"]) / safe_float(r["base_total_revenue"]) if safe_float(r["base_total_revenue"]) else 0.0))*100.0, axis=1)
    rows["required_growth_pct"] = rows.apply(lambda r: compute_required_growth(safe_float(r["blended_drr"]), safe_float(r["spend_growth_pct"]), r["subject_norm"]), axis=1)
    rows["card_issue"] = rows.apply(lambda r: (safe_float(r.get("addToCartConversion")) > 0 and safe_float(r.get("subj_addToCart")) > 0 and safe_float(r["addToCartConversion"]) < safe_float(r["subj_addToCart"]) * 0.7) or (safe_float(r.get("cartToOrderConversion")) > 0 and safe_float(r.get("subj_cartToOrder")) > 0 and safe_float(r["cartToOrderConversion"]) < safe_float(r["subj_cartToOrder"]) * 0.7), axis=1)

    # preliminary rows for benchmarks
    rows["bid_eff_imp"] = rows.apply(lambda r: (safe_float(r["capture_imp"]) / safe_float(r["current_bid_rub"])) if safe_float(r["current_bid_rub"]) else 0.0, axis=1)
    rows["bid_eff_click"] = rows.apply(lambda r: (safe_float(r["capture_click"]) / safe_float(r["current_bid_rub"])) if safe_float(r["current_bid_rub"]) else 0.0, axis=1)
    subject_benchmarks = build_subject_benchmarks(rows)
    rows = rows.merge(subject_benchmarks, on=["subject_norm","placement"], how="left")
    rows["eff_index_imp"] = rows.apply(lambda r: safe_float(r["capture_imp"]) / safe_float(r["bench_capture_imp"]) if safe_float(r["bench_capture_imp"]) else 1.0, axis=1)
    rows["eff_index_click"] = rows.apply(lambda r: safe_float(r["capture_click"]) / safe_float(r["bench_capture_click"]) if safe_float(r["bench_capture_click"]) else 1.0, axis=1)

    # limits and decisions
    limits = rows.apply(lambda r: pd.Series(compute_bid_limits(r, subject_benchmarks), index=["comfort_bid_rub","max_bid_rub","experiment_bid_rub","limit_type"]), axis=1)
    rows = pd.concat([rows, limits], axis=1)
    decisions = []
    for _, r in rows.iterrows():
        action, new_bid, reason, rate_limit = determine_action(r, cfg)
        decisions.append({
            "Дата запуска": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "ID кампании": safe_int(r["id_campaign"]),
            "Артикул WB": safe_int(r["nmId"]),
            "Артикул продавца": r["supplier_article"],
            "Товар": r["control_key"],
            "Предмет": r["subject"],
            "Плейсмент": r["placement"],
            "Тип кампании": f'{r["payment_type"]}_{r["placement"]}',
            "Текущая ставка, ₽": round(safe_float(r["current_bid_rub"]), 2),
            "Комфортная ставка, ₽": round(safe_float(r["comfort_bid_rub"]), 2) if pd.notna(r["comfort_bid_rub"]) else None,
            "Максимальная ставка, ₽": round(safe_float(r["max_bid_rub"]), 2) if pd.notna(r["max_bid_rub"]) else None,
            "Экспериментальная ставка, ₽": round(safe_float(r["experiment_bid_rub"]), 2) if pd.notna(r["experiment_bid_rub"]) else None,
            "Тип лимита": r["limit_type"],
            "Действие": action,
            "Новая ставка, ₽": round(safe_float(new_bid), 2),
            "Причина": reason,
            "Показы": round(safe_float(r["Показы"]), 0),
            "Клики": round(safe_float(r["Клики"]), 0),
            "CTR, %": round(safe_float(r["ctr_pct"]), 2),
            "Заказы РК": round(safe_float(r["Заказы"]), 2),
            "Все заказы товара": round(safe_float(r["total_orders"]), 2),
            "Расход РК, ₽": round(safe_float(r["Расход"]), 2),
            "Выручка РК, ₽": round(safe_float(r["Сумма_заказов"]), 2),
            "Выручка товара, ₽": round(safe_float(r["total_revenue"]), 2),
            "Общий ДРР товара, %": round(safe_float(r["blended_drr"]) * 100, 2),
            "Рекламный ДРР, %": round(safe_float(r["ad_drr"]) * 100, 2),
            "Рост заказов, %": round(safe_float(r["order_growth_pct"]), 2),
            "Рост расходов, %": round(safe_float(r["spend_growth_pct"]), 2),
            "Требуемый рост заказов, %": round(safe_float(r["required_growth_pct"]), 2),
            "Спрос за окно": round(safe_float(r["demand_week"]), 0),
            "Медианная позиция": round(safe_float(r["median_position"]), 2),
            "Видимость, %": round(safe_float(r["visibility_pct"]), 2),
            "Индекс эффективности ставки по показам": round(safe_float(r["eff_index_imp"]), 4),
            "Индекс эффективности ставки по кликам": round(safe_float(r["eff_index_click"]), 4),
            "Предел эффективности": "Да" if rate_limit or action == "Предел эффективности ставки" else "Нет",
            "Проблема карточки": "Да" if bool(r["card_issue"]) else "Нет",
        })
    decisions_df = pd.DataFrame(decisions)
    # weak positions simple
    weak = decisions_df[(decisions_df["Действие"].isin(["Снизить","Предел эффективности ставки"])) | (decisions_df["Медианная позиция"] > 20)].copy()
    weak["Комментарий"] = weak["Причина"]
    weak = weak[["Артикул продавца","Артикул WB","ID кампании","Тип кампании","Плейсмент","Действие","Комментарий"]].drop_duplicates()

    # product metrics
    product_metrics = rows.groupby(["control_key","subject_norm"], as_index=False).agg(
        total_orders=("total_orders","max"),
        total_revenue=("total_revenue","max"),
        ad_spend=("ad_spend","max"),
        ad_orders=("ad_orders","max"),
        ad_clicks=("ad_clicks","max"),
        blended_drr=("blended_drr","max"),
        order_growth_pct=("order_growth_pct","max"),
        spend_growth_pct=("spend_growth_pct","max"),
        required_growth_pct=("required_growth_pct","max"),
    ).rename(columns={"control_key":"Товар","subject_norm":"Предмет код"})
    product_metrics["Общий ДРР товара, %"] = (product_metrics["blended_drr"]*100).round(2)

    # benchmark comparison clean
    bench_cmp = decisions_df.merge(subject_benchmarks, left_on=["Предмет","Плейсмент"], right_on=["subject_norm","placement"], how="left")
    bench_cmp = bench_cmp[["Артикул продавца","ID кампании","Тип кампании","Плейсмент","CTR, %","Индекс эффективности ставки по показам","Индекс эффективности ставки по кликам","Причина","bench_ctr","bench_capture_imp","bench_capture_click"]].copy()
    bench_cmp = bench_cmp.rename(columns={"bench_ctr":"Эталон CTR, %","bench_capture_imp":"Эталон доля показов","bench_capture_click":"Эталон доля кликов"})

    # effects: simple from changed decisions
    changed = decisions_df[decisions_df["Действие"].isin(["Повысить","Снизить","Тест роста"]) & (decisions_df["Текущая ставка, ₽"] != decisions_df["Новая ставка, ₽"])].copy()
    if changed.empty:
        effects = pd.DataFrame([{"Комментарий":"В этом запуске не было изменений ставок"}])
    else:
        effects = changed[["Дата запуска","Артикул продавца","ID кампании","Тип кампании","Текущая ставка, ₽","Новая ставка, ₽","Действие","Причина"]].copy()
        effects["Комментарий"] = "Ожидаем накопление зрелых данных после изменения"

    orders_60 = orders[(orders["date"] >= as_of_date - timedelta(days=60)) & (orders["date"] <= as_of_date) & (~orders["isCancel"])].copy() if not orders.empty else pd.DataFrame()
    shade_portfolio = build_shade_portfolio(campaigns, master, orders_60)
    shade_actions, shade_tests = build_shade_actions(campaigns, shade_portfolio, master, orders_60, product_metrics.rename(columns={"Товар":"control_key","Предмет код":"subject_norm","Общий ДРР товара, %":"blended_drr"}), api_key=os.getenv("WB_PROMO_KEY_TOPFACE",""))
    if shade_actions.empty:
        shade_actions = pd.DataFrame([{"Комментарий":"Нет действий по оттенкам"}])

    return {
        "rows": rows,
        "decisions": decisions_df,
        "weak": weak,
        "product_metrics": product_metrics,
        "bench_cmp": bench_cmp,
        "effects": effects,
        "shade_portfolio": shade_portfolio if not shade_portfolio.empty else pd.DataFrame([{"Комментарий":"Нет кампаний по оттенкам"}]),
        "shade_actions": shade_actions,
        "shade_tests": shade_tests,
        "eff_history_sheets": build_efficiency_history(ads_daily, campaigns, keywords_daily, master, bid_history, as_of_date),
        "window": window,
    }

def normalize_bid_for_wb(value_rub: float, payment_type: str, placement: str) -> int:
    value_rub = safe_float(value_rub)
    if payment_type == "cpc":
        return int(round(value_rub * 100))
    # cpm in WB examples also in kopecks
    return int(round(value_rub * 100))

def decisions_to_payload(decisions_df: pd.DataFrame) -> Dict[str, Any]:
    changed = decisions_df[decisions_df["Действие"].isin(["Повысить","Снизить","Тест роста"]) & (decisions_df["Новая ставка, ₽"] != decisions_df["Текущая ставка, ₽"])].copy()
    grouped = {}
    for _, r in changed.iterrows():
        advert = safe_int(r["ID кампании"])
        nm_id = safe_int(r["Артикул WB"])
        payment_type = "cpc" if "cpc" in str(r["Тип кампании"]).lower() else "cpm"
        placement = str(r["Плейсмент"])
        grouped.setdefault((advert, payment_type), []).append({
            "nm_id": nm_id,
            "placement": placement,
            "bid_kopecks": normalize_bid_for_wb(r["Новая ставка, ₽"], payment_type, placement),
        })
    out = []
    for (advert, payment_type), items in grouped.items():
        out.append({"advert_id": advert, "payment_type": payment_type, "nm_bids": items})
    return {"bids": out}

def send_payload(payload: Dict[str, Any], api_key: str, dry_run: bool) -> pd.DataFrame:
    logs = []
    headers = {"Authorization": api_key.strip(), "Content-Type": "application/json"}
    for block in payload.get("bids", []):
        advert_id = safe_int(block["advert_id"])
        body = {"bids": [{"advert_id": advert_id, "nm_bids": block["nm_bids"]}]}
        if dry_run:
            logs.append({"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "advert_id": advert_id, "status":"dry-run", "http_status":"", "response":json.dumps(body, ensure_ascii=False)})
            continue
        try:
            resp = requests.post(WB_BIDS_URL, headers=headers, json=body, timeout=120)
            logs.append({"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "advert_id": advert_id, "status":"ok" if resp.status_code == 200 else "failed", "http_status":resp.status_code, "response":resp.text[:1000]})
        except Exception as e:
            logs.append({"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "advert_id": advert_id, "status":"failed", "http_status":"", "response":str(e)})
    return pd.DataFrame(logs)

def save_outputs(provider: BaseProvider, results: Dict[str, Any], run_mode: str, bid_send_log: Optional[pd.DataFrame], shade_apply_log: Optional[pd.DataFrame], history_append: pd.DataFrame) -> None:
    decisions = results["decisions"]
    preview_sheets = {
        "Решения_по_ставкам": decisions,
        "Расчёт_логики": results["rows"],
        "Статистика_по_товарам": results["product_metrics"],
        "Слабая_позиция": results["weak"],
        "Лимиты_ставок": decisions[["Артикул продавца","ID кампании","Тип кампании","Текущая ставка, ₽","Комфортная ставка, ₽","Максимальная ставка, ₽","Экспериментальная ставка, ₽","Тип лимита"]],
        "Рекомендации_по_оттенкам": results["shade_actions"],
        "Состав_кампаний_по_оттенкам": results["shade_portfolio"],
        "Тесты_оттенков": results["shade_tests"],
        "Сравнение_с_сильными_РК": results["bench_cmp"],
        "Эффект_изменений": results["effects"],
        "Окно_анализа": pd.DataFrame([{
            "Текущее окно с": results["window"]["cur_start"],
            "Текущее окно по": results["window"]["cur_end"],
            "База с": results["window"]["base_start"],
            "База по": results["window"]["base_end"],
            "Режим": run_mode,
        }]),
    }
    provider.write_excel(OUT_PREVIEW, preview_sheets)
    provider.write_excel(OUT_PRODUCT, {"Метрики_по_товарам": results["product_metrics"]})
    provider.write_excel(OUT_LIMITS, {"Лимиты_ставок": preview_sheets["Лимиты_ставок"]})
    provider.write_excel(OUT_WEAK, {"Слабые_позиции": results["weak"] if not results["weak"].empty else pd.DataFrame([{"Комментарий":"Нет слабых позиций"}])})
    provider.write_excel(OUT_EFFECTS, {"Эффект_изменений": results["effects"]})
    provider.write_excel(OUT_SHADE_ACTIONS, {"Рекомендации_по_оттенкам": results["shade_actions"]})
    provider.write_excel(OUT_SHADE_PORTFOLIO, {"Состав_кампаний_по_оттенкам": results["shade_portfolio"]})
    provider.write_excel(OUT_SHADE_TESTS, {"Тесты_оттенков": results["shade_tests"]})
    provider.write_excel(OUT_BENCHMARK, {"Сравнение_с_сильными_РК": results["bench_cmp"]})
    provider.write_excel(OUT_EFF, results["eff_history_sheets"])
    # append archive and bid history
    try:
        old_archive = provider.read_excel(OUT_ARCHIVE)
    except Exception:
        old_archive = pd.DataFrame()
    new_archive = pd.concat([old_archive, decisions], ignore_index=True)
    provider.write_excel(OUT_ARCHIVE, {"Архив_решений": new_archive})

    if history_append is not None and not history_append.empty:
        try:
            old_hist = provider.read_excel(OUT_BID_HISTORY)
        except Exception:
            old_hist = pd.DataFrame()
        hist = pd.concat([old_hist, history_append], ignore_index=True)
        provider.write_excel(OUT_BID_HISTORY, {"История_ставок": hist})

    summary = {
        "mode": run_mode,
        "generated_at": datetime.now().isoformat(),
        "recommendations_count": int(len(decisions)),
        "changed_count": int(len(decisions[(decisions["Действие"].isin(["Повысить","Снизить","Тест роста"])) & (decisions["Текущая ставка, ₽"] != decisions["Новая ставка, ₽"])])),
        "limit_reached_count": int((decisions["Действие"] == "Предел эффективности ставки").sum()),
        "weak_positions_count": int(len(results["weak"])),
        "shade_actions_count": 0 if results["shade_actions"].empty else int((results["shade_actions"].get("Действие") == "Добавить тестовый оттенок").sum()) if "Действие" in results["shade_actions"].columns else 0,
        "shade_add_test_count": 0 if results["shade_actions"].empty else int((results["shade_actions"].get("Действие") == "Добавить тестовый оттенок").sum()) if "Действие" in results["shade_actions"].columns else 0,
        "shade_remove_count": 0,
        "bid_send_blocks": 0 if bid_send_log is None or bid_send_log.empty else int(len(bid_send_log)),
        "shade_apply_blocks": 0 if shade_apply_log is None or shade_apply_log.empty else int(len(shade_apply_log)),
    }
    provider.write_text(OUT_SUMMARY, json.dumps(summary, ensure_ascii=False, indent=2))

def run_manager(args: argparse.Namespace) -> None:
    provider = choose_provider(args.local_data_dir)
    as_of_date = datetime.strptime(args.as_of_date, "%Y-%m-%d").date() if args.as_of_date else datetime.now().date()
    cfg = Config()
    results = prepare_metrics(provider, cfg, as_of_date)

    decisions = results["decisions"].copy()
    log(f"✅ Всего строк решений: {len(decisions)}")
    changed = decisions[(decisions["Действие"].isin(["Повысить","Снизить","Тест роста"])) & (decisions["Текущая ставка, ₽"] != decisions["Новая ставка, ₽"])].copy()
    log(f"🔁 Изменённых ставок: {len(changed)}")
    log(f"📊 Разбивка по действиям: {dict(decisions['Действие'].value_counts())}")
    if not changed.empty:
        print(changed[["Товар","Артикул продавца","Предмет","ID кампании","Плейсмент","Текущая ставка, ₽","Новая ставка, ₽","Действие","Причина"]].head(20).to_string(index=False), flush=True)

    api_key = os.getenv("WB_PROMO_KEY_TOPFACE","").strip()
    bid_send_log = pd.DataFrame()
    shade_apply_log = pd.DataFrame()
    history_append = pd.DataFrame()

    if args.mode == "run":
        payload = decisions_to_payload(decisions)
        bid_send_log = send_payload(payload, api_key, dry_run=not bool(api_key))
        log(f"📤 Отправлено блоков в WB: {len(payload.get('bids', []))}")
        if not changed.empty:
            history_append = pd.DataFrame({
                "Дата запуска": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "Неделя": f"{as_of_date.isocalendar().year}-W{as_of_date.isocalendar().week:02d}",
                "ID кампании": changed["ID кампании"],
                "Артикул WB": changed["Артикул WB"],
                "Тип кампании": changed["Тип кампании"],
                "Ставка поиск, коп": changed["Новая ставка, ₽"].map(lambda x: int(round(safe_float(x)*100))),
                "Ставка рекомендации, коп": 0,
                "Стратегия": "STABLE_V1",
            })
        if args.apply_shades:
            shade_apply_log = apply_shade_actions(results["shade_actions"], api_key, dry_run=not bool(api_key))
            log(f"🎨 Блоков оттенков к применению: {0 if shade_apply_log.empty else len(shade_apply_log)}")
    else:
        log("🧪 Preview-режим: ставки не отправлялись")

    save_outputs(provider, results, args.mode, bid_send_log, shade_apply_log, history_append)

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Стабильный менеджер ставок WB для TOPFACE")
    p.add_argument("mode", choices=["preview","run"], help="preview = только рекомендации, run = применить ставки")
    p.add_argument("--apply-shades", action="store_true", help="Применить рекомендации по оттенкам через API")
    p.add_argument("--local-data-dir", default="", help="Локальная папка с файлами")
    p.add_argument("--as-of-date", default="", help="Дата расчёта YYYY-MM-DD")
    return p

def main() -> None:
    args = build_parser().parse_args()
    run_manager(args)

if __name__ == "__main__":
    main()
