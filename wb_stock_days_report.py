#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import io
import math
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import boto3
import pandas as pd
import requests
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side


# =========================
# БАЗОВЫЕ УТИЛИТЫ
# =========================


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}



def env_first(*names: str, default: str = "") -> str:
    for name in names:
        value = os.getenv(name)
        if value is not None and str(value).strip() != "":
            return str(value).strip()
    return default



def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)



def normalize_text(value: object) -> str:
    if pd.isna(value) or value is None:
        return ""
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text



def safe_float(value: object) -> float:
    if pd.isna(value) or value is None or value == "":
        return 0.0
    try:
        return float(value)
    except Exception:
        return 0.0



def ceil_int(value: object) -> int:
    return int(math.ceil(safe_float(value)))



def days_of_stock(stock_qty: float, avg_daily_sales: float) -> Optional[float]:
    if avg_daily_sales <= 0:
        if stock_qty <= 0:
            return 0.0
        return None
    return stock_qty / avg_daily_sales



def parse_week_key_date(key: str) -> Optional[datetime]:
    match = re.search(r"_(\d{4}-W\d{2})\.xlsx$", key)
    if not match:
        return None
    year, week = match.group(1).split("-W")
    return datetime.fromisocalendar(int(year), int(week), 1)



def find_first_existing_column(df: pd.DataFrame, candidates: List[str], context: str) -> str:
    normalized = {str(col).strip(): col for col in df.columns}
    for candidate in candidates:
        if candidate in normalized:
            return normalized[candidate]
    raise KeyError(f"Не найдена колонка для {context}. Доступные колонки: {list(df.columns)}")


# =========================
# КОНФИГ
# =========================


@dataclass
class AppConfig:
    bucket_name: str = env_first("WB_S3_BUCKET", "YC_BUCKET_NAME", "CLOUD_RU_BUCKET")
    access_key: str = env_first("WB_S3_ACCESS_KEY", "YC_ACCESS_KEY_ID", "CLOUD_RU_ACCESS_KEY")
    secret_key: str = env_first("WB_S3_SECRET_KEY", "YC_SECRET_ACCESS_KEY", "CLOUD_RU_SECRET_KEY")
    endpoint_url: str = env_first("WB_S3_ENDPOINT", "YC_ENDPOINT_URL", default="https://storage.yandexcloud.net")
    region_name: str = env_first("WB_S3_REGION", default="ru-central1")

    store_name: str = os.getenv("WB_STORE", "TOPFACE").strip()
    run_date: datetime = datetime.strptime(
        os.getenv("WB_RUN_DATE", datetime.now().strftime("%Y-%m-%d")),
        "%Y-%m-%d",
    )
    sales_window_days: int = int(os.getenv("WB_SALES_WINDOW_DAYS", "7"))
    days_threshold: float = float(os.getenv("WB_LOW_STOCK_DAYS_THRESHOLD", "14"))
    output_dir: str = os.getenv("WB_OUTPUT_DIR", "output")

    telegram_bot_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    telegram_chat_id: str = os.getenv("TELEGRAM_CHAT_ID", "")
    send_telegram: bool = env_bool("WB_SEND_TELEGRAM", True)

    orders_prefix_tpl: str = os.getenv("WB_ORDERS_PREFIX", "Отчёты/Заказы/{store}/Недельные/")
    wb_stocks_prefix_tpl: str = os.getenv("WB_STOCKS_PREFIX", "Отчёты/Остатки/{store}/Недельные/")
    stocks_1c_key: str = os.getenv("WB_STOCKS_1C_KEY", "Отчёты/Остатки/1С/Остатки 1С.xlsx")
    article_map_1c_key: str = os.getenv("WB_ARTICLE_MAP_1C_KEY", "Отчёты/Остатки/1С/Артикулы 1с.xlsx")
    stop_list_key: str = os.getenv("WB_STOP_LIST_KEY", "Отчёты/Остатки/1С/СТОП к заказам.xlsx")
    stop_list_local_path: str = os.getenv("WB_STOP_LIST_LOCAL_PATH", "")

    wb_code_column_1c: str = os.getenv("WB_1C_WB_CODE_COLUMN", "Код_WB")
    article_1c_column: str = os.getenv("WB_1C_ARTICLE_COLUMN", "Артикул")
    mp_stock_column: str = os.getenv("WB_1C_MP_STOCK_COLUMN", "Остатки МП")

    upload_result_to_s3: bool = env_bool("WB_UPLOAD_RESULT_TO_S3", False)
    result_prefix: str = os.getenv("WB_RESULT_PREFIX", "Отчёты/Контроль остатков/{store}/")


CONFIG = AppConfig()


# =========================
# ХРАНИЛИЩЕ S3
# =========================


class S3Storage:
    def __init__(self, cfg: AppConfig):
        if not cfg.bucket_name or not cfg.access_key or not cfg.secret_key:
            raise ValueError(
                "Не заданы параметры Object Storage. Нужны env из одной группы: "
                "WB_S3_*, YC_* или CLOUD_RU_*."
            )

        self.bucket = cfg.bucket_name
        self.s3 = boto3.client(
            "s3",
            endpoint_url=cfg.endpoint_url,
            aws_access_key_id=cfg.access_key,
            aws_secret_access_key=cfg.secret_key,
            region_name=cfg.region_name,
            config=BotoConfig(signature_version="s3v4", read_timeout=300, connect_timeout=60),
        )

    def list_keys(self, prefix: str) -> List[str]:
        keys: List[str] = []
        continuation_token = None
        while True:
            kwargs = {"Bucket": self.bucket, "Prefix": prefix, "MaxKeys": 1000}
            if continuation_token:
                kwargs["ContinuationToken"] = continuation_token
            resp = self.s3.list_objects_v2(**kwargs)
            keys.extend([obj["Key"] for obj in resp.get("Contents", [])])
            if not resp.get("IsTruncated"):
                break
            continuation_token = resp.get("NextContinuationToken")
        return keys

    def key_exists(self, key: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False

    def read_bytes(self, key: str) -> bytes:
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        return obj["Body"].read()

    def read_excel(self, key: str, sheet_name: Optional[str] = None):
        return pd.read_excel(io.BytesIO(self.read_bytes(key)), sheet_name=sheet_name)

    def upload_file(self, local_path: str, key: str) -> None:
        self.s3.upload_file(local_path, self.bucket, key)


# =========================
# ЗАГРУЗКА ДАННЫХ
# =========================


def load_latest_weekly_file(storage: S3Storage, prefix: str, expected_sheet: Optional[str] = None) -> Tuple[pd.DataFrame, str]:
    keys = [k for k in storage.list_keys(prefix) if k.lower().endswith(".xlsx")]
    if not keys:
        raise FileNotFoundError(f"В Object Storage не найдено weekly-файлов по префиксу: {prefix}")

    keys_with_dates = [(parse_week_key_date(k) or datetime.min, k) for k in keys]
    _, latest_key = max(keys_with_dates, key=lambda x: x[0])
    df = storage.read_excel(latest_key, sheet_name=expected_sheet)
    if isinstance(df, dict):
        df = next(iter(df.values()))
    log(f"Загружен последний weekly-файл: {latest_key}")
    return df, latest_key



def load_weekly_window(storage: S3Storage, prefix: str, run_date: datetime, lookback_days: int, expected_sheet: Optional[str] = None) -> pd.DataFrame:
    keys = [k for k in storage.list_keys(prefix) if k.lower().endswith(".xlsx")]
    if not keys:
        raise FileNotFoundError(f"В Object Storage не найдено файлов по префиксу: {prefix}")

    cutoff = run_date - timedelta(days=lookback_days + 21)
    selected = [(parse_week_key_date(k) or datetime.min, k) for k in keys if (parse_week_key_date(k) or datetime.min) >= cutoff]
    if not selected:
        selected = [(parse_week_key_date(k) or datetime.min, k) for k in keys]

    parts: List[pd.DataFrame] = []
    for _, key in sorted(selected, key=lambda x: x[0]):
        try:
            df = storage.read_excel(key, sheet_name=expected_sheet)
            if isinstance(df, dict):
                df = next(iter(df.values()))
            if not df.empty:
                parts.append(df)
                log(f"Загружен файл: {key}")
        except Exception as exc:
            log(f"⚠️ Не удалось прочитать {key}: {exc}")

    if not parts:
        raise ValueError(f"Не удалось прочитать ни одного файла по префиксу {prefix}")
    return pd.concat(parts, ignore_index=True)



def load_orders(storage: S3Storage, cfg: AppConfig) -> pd.DataFrame:
    df = load_weekly_window(
        storage,
        cfg.orders_prefix_tpl.format(store=cfg.store_name),
        cfg.run_date,
        cfg.sales_window_days,
        expected_sheet="Заказы",
    )

    date_col = find_first_existing_column(df, ["date", "Дата", "Дата заказа"], "даты заказа")
    nmid_col = find_first_existing_column(df, ["nmId", "nm_id", "Артикул WB"], "nmId в заказах")
    article_col = find_first_existing_column(df, ["supplierArticle", "Артикул продавца", "vendorCode"], "артикула продавца")

    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.normalize()
    start_date = cfg.run_date - timedelta(days=cfg.sales_window_days - 1)
    df = df[(df[date_col] >= start_date) & (df[date_col] <= cfg.run_date)].copy()

    if "isCancel" in df.columns:
        df = df[~df["isCancel"].fillna(False)].copy()

    df["nmId"] = df[nmid_col].map(normalize_text)
    df["supplierArticle"] = df[article_col].map(normalize_text)
    df = df[df["nmId"] != ""].copy()
    df["qty"] = 1
    return df[["nmId", "supplierArticle", "qty"]]



def load_wb_stocks(storage: S3Storage, cfg: AppConfig) -> pd.DataFrame:
    df, stock_key = load_latest_weekly_file(
        storage,
        cfg.wb_stocks_prefix_tpl.format(store=cfg.store_name),
        expected_sheet=None,
    )

    nmid_col = find_first_existing_column(df, ["nmId", "Артикул WB", "nm_id"], "nmId в остатках WB")
    stock_col = find_first_existing_column(df, ["Количество", "Доступно", "Остаток", "остаток", "quantity"], "остатка WB")
    supplier_col = None
    for candidate in ["supplierArticle", "Артикул продавца", "vendorCode"]:
        if candidate in df.columns:
            supplier_col = candidate
            break

    result = pd.DataFrame()
    result["nmId"] = df[nmid_col].map(normalize_text)
    result["stock_wb_qty"] = pd.to_numeric(df[stock_col], errors="coerce").fillna(0)
    if supplier_col:
        result["supplierArticle"] = df[supplier_col].map(normalize_text)
    else:
        result["supplierArticle"] = ""

    result = (
        result[result["nmId"] != ""]
        .groupby("nmId", as_index=False)
        .agg(stock_wb_qty=("stock_wb_qty", "sum"), supplierArticle=("supplierArticle", "first"))
    )
    log(f"Источник WB остатков: {stock_key}")
    return result



def load_article_map_1c(storage: S3Storage, cfg: AppConfig) -> pd.DataFrame:
    df = storage.read_excel(cfg.article_map_1c_key)
    if isinstance(df, dict):
        df = next(iter(df.values()))
    if df.shape[1] < 3:
        raise ValueError(
            f"Файл карты артикулов {cfg.article_map_1c_key} имеет меньше 3 колонок, не могу построить mapping WB -> 1С"
        )

    mapped = pd.DataFrame()
    mapped["nmId"] = df.iloc[:, 0].map(normalize_text)
    mapped["article_1c"] = df.iloc[:, 2].map(normalize_text)
    mapped = mapped[(mapped["nmId"] != "") & (mapped["article_1c"] != "")].drop_duplicates()
    log(f"Загружена карта WB -> 1С: {cfg.article_map_1c_key}")
    return mapped



def load_1c_stocks(storage: S3Storage, cfg: AppConfig) -> pd.DataFrame:
    df = storage.read_excel(cfg.stocks_1c_key)
    if isinstance(df, dict):
        df = next(iter(df.values()))
    log(f"Загружен файл 1С: {cfg.stocks_1c_key}")
    return df



def load_stop_list(storage: S3Storage, cfg: AppConfig) -> pd.DataFrame:
    if cfg.stop_list_local_path:
        local_path = Path(cfg.stop_list_local_path)
        if local_path.exists():
            df = pd.read_excel(local_path)
            log(f"Загружен локальный стоп-лист: {local_path}")
            return df

    candidate_keys = [cfg.stop_list_key]
    stop_prefix = "Отчёты/Остатки/1С/"
    try:
        prefix_keys = storage.list_keys(stop_prefix)
        candidate_keys.extend([k for k in prefix_keys if "стоп" in k.lower() and k.lower().endswith(".xlsx")])
    except Exception:
        pass

    seen = set()
    for key in candidate_keys:
        if not key or key in seen:
            continue
        seen.add(key)
        try:
            if storage.key_exists(key):
                df = storage.read_excel(key)
                if isinstance(df, dict):
                    df = next(iter(df.values()))
                log(f"Загружен стоп-лист: {key}")
                return df
        except Exception as exc:
            log(f"⚠️ Не удалось прочитать стоп-лист {key}: {exc}")

    log("⚠️ Стоп-лист не найден. Продолжаю без пометки Delist.")
    return pd.DataFrame(columns=["АРТ", "Статус"])


# =========================
# ПОДГОТОВКА ДАННЫХ
# =========================


def build_sales_metrics(orders: pd.DataFrame, cfg: AppConfig) -> pd.DataFrame:
    if orders.empty:
        return pd.DataFrame(columns=["nmId", "supplierArticle", "sales_7d", "avg_daily_sales_7d"])

    grouped = (
        orders.groupby(["nmId", "supplierArticle"], as_index=False)
        .agg(sales_7d=("qty", "sum"))
    )
    grouped["avg_daily_sales_7d"] = grouped["sales_7d"] / float(cfg.sales_window_days)
    return grouped



def prepare_1c_dataset(df_1c: pd.DataFrame, cfg: AppConfig) -> pd.DataFrame:
    df = df_1c.copy()
    df.columns = [str(c).strip() for c in df.columns]

    required = [cfg.article_1c_column, cfg.mp_stock_column]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"В файле 1С отсутствуют обязательные колонки: {missing}")

    result = pd.DataFrame()
    result["article_1c"] = df[cfg.article_1c_column].map(normalize_text)
    result["wb_mp_qty"] = pd.to_numeric(df[cfg.mp_stock_column], errors="coerce").fillna(0).map(ceil_int)

    if cfg.wb_code_column_1c in df.columns:
        result["nmId_from_1c"] = df[cfg.wb_code_column_1c].map(normalize_text)
    else:
        result["nmId_from_1c"] = ""

    result = (
        result[result["article_1c"] != ""]
        .groupby(["article_1c", "nmId_from_1c"], as_index=False)
        .agg(wb_mp_qty=("wb_mp_qty", "sum"))
    )
    return result



def prepare_stop_dataset(stop_df: pd.DataFrame) -> pd.DataFrame:
    if stop_df.empty:
        return pd.DataFrame(columns=["article_1c", "delist_flag"])

    df = stop_df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    art_col = find_first_existing_column(df, ["АРТ", "Арт", "Артикул", "article_1c"], "артикула в стоп-листе")
    status_col = find_first_existing_column(df, ["Unnamed: 2", "Статус", "status"], "статуса в стоп-листе")

    prepared = pd.DataFrame()
    prepared["article_1c"] = df[art_col].map(normalize_text)
    prepared["status"] = df[status_col].map(normalize_text).str.lower()
    prepared = prepared[(prepared["article_1c"] != "") & (prepared["status"] == "delist")].copy()
    prepared["delist_flag"] = "Delist"
    return prepared[["article_1c", "delist_flag"]].drop_duplicates()



def build_report_dataframe(
    sales_df: pd.DataFrame,
    wb_stocks_df: pd.DataFrame,
    article_map_df: pd.DataFrame,
    stocks_1c_df: pd.DataFrame,
    stop_df: pd.DataFrame,
) -> pd.DataFrame:
    base = wb_stocks_df.merge(sales_df, on="nmId", how="left", suffixes=("", "_sales"))
    base["supplierArticle"] = base["supplierArticle"].fillna("")
    if "supplierArticle_sales" in base.columns:
        base["supplierArticle"] = base["supplierArticle"].where(base["supplierArticle"] != "", base["supplierArticle_sales"].fillna(""))
        base = base.drop(columns=["supplierArticle_sales"])

    base["sales_7d"] = base["sales_7d"].fillna(0).astype(int)
    base["avg_daily_sales_7d"] = base["avg_daily_sales_7d"].fillna(0.0)

    article_map_df = article_map_df.drop_duplicates(subset=["nmId"])
    stocks_1c_df = stocks_1c_df.drop_duplicates(subset=["article_1c"])
    stop_df = stop_df.drop_duplicates(subset=["article_1c"])

    merged = base.merge(article_map_df, on="nmId", how="left")
    merged = merged.merge(stocks_1c_df[["article_1c", "wb_mp_qty"]], on="article_1c", how="left")
    merged = merged.merge(stop_df, on="article_1c", how="left")

    # fallback: если mapping-файл не дал Артикул 1С, попробуем связать по Код_WB из файла 1С
    fallback_1c = stocks_1c_df[stocks_1c_df["nmId_from_1c"] != ""][ ["nmId_from_1c", "article_1c", "wb_mp_qty"] ].drop_duplicates(subset=["nmId_from_1c"])
    fallback_1c = fallback_1c.rename(columns={"nmId_from_1c": "nmId", "article_1c": "article_1c_fallback", "wb_mp_qty": "wb_mp_qty_fallback"})
    merged = merged.merge(fallback_1c, on="nmId", how="left")

    merged["article_1c"] = merged["article_1c"].fillna(merged["article_1c_fallback"])
    merged["wb_mp_qty"] = merged["wb_mp_qty"].fillna(merged["wb_mp_qty_fallback"])
    merged["wb_mp_qty"] = merged["wb_mp_qty"].fillna(0).astype(int)
    merged["delist_flag"] = merged["delist_flag"].fillna("")
    merged = merged.drop(columns=[c for c in ["article_1c_fallback", "wb_mp_qty_fallback"] if c in merged.columns])

    merged["days_wb"] = merged.apply(lambda x: days_of_stock(safe_float(x["stock_wb_qty"]), safe_float(x["avg_daily_sales_7d"])), axis=1)
    merged["days_lipetsk"] = merged.apply(lambda x: days_of_stock(safe_float(x["wb_mp_qty"]), safe_float(x["avg_daily_sales_7d"])), axis=1)
    merged["days_total"] = merged.apply(
        lambda x: days_of_stock(safe_float(x["stock_wb_qty"]) + safe_float(x["wb_mp_qty"]), safe_float(x["avg_daily_sales_7d"])),
        axis=1,
    )

    merged["article_1c"] = merged["article_1c"].fillna("")
    merged = merged.sort_values(by=["days_wb"], key=lambda s: s.fillna(10**9), ascending=True).reset_index(drop=True)
    return merged


# =========================
# EXCEL ОТЧЁТ
# =========================


HEADER_FILL = PatternFill("solid", fgColor="1F4E78")
ALERT_FILL = PatternFill("solid", fgColor="FFF2CC")
CRITICAL_FILL = PatternFill("solid", fgColor="F4CCCC")
DELIST_FILL = PatternFill("solid", fgColor="E6B8AF")
BORDER = Border(
    left=Side(style="thin", color="D9D9D9"),
    right=Side(style="thin", color="D9D9D9"),
    top=Side(style="thin", color="D9D9D9"),
    bottom=Side(style="thin", color="D9D9D9"),
)



def style_header_row(ws, row_idx: int) -> None:
    for cell in ws[row_idx]:
        if cell.value is None:
            continue
        cell.fill = HEADER_FILL
        cell.font = Font(name="Aptos", size=14, bold=True, color="FFFFFF")
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = BORDER



def style_body(ws, start_row: int, end_row: int, numeric_cols: List[int], threshold_col: int, delist_col: Optional[int] = None) -> None:
    for row in ws.iter_rows(min_row=start_row, max_row=end_row):
        for cell in row:
            cell.font = Font(name="Aptos", size=14)
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            cell.border = BORDER

        row_idx = row[0].row
        fill = None

        target = ws.cell(row=row_idx, column=threshold_col).value
        try:
            target = float(target)
        except Exception:
            target = None

        if target is not None:
            if target < 7:
                fill = CRITICAL_FILL
            elif target < 14:
                fill = ALERT_FILL

        if delist_col:
            delist_value = normalize_text(ws.cell(row=row_idx, column=delist_col).value).lower()
            if delist_value == "delist":
                fill = DELIST_FILL

        if fill is not None:
            for cell in row:
                cell.fill = fill

    for col_idx in numeric_cols:
        for r in range(start_row, end_row + 1):
            ws.cell(r, col_idx).number_format = "0.0"



def autofit_layout(ws, widths: Dict[str, float], row_heights: Dict[int, float]) -> None:
    for col_letter, width in widths.items():
        ws.column_dimensions[col_letter].width = width
    for row_idx, height in row_heights.items():
        ws.row_dimensions[row_idx].height = height



def apply_freeze_and_filter(ws, ref: str) -> None:
    ws.freeze_panes = "A3"
    ws.auto_filter.ref = ref
    ws.sheet_view.showGridLines = False



def create_excel_report(report_df: pd.DataFrame, cfg: AppConfig, output_path: str) -> str:
    wb = Workbook()
    ws_short = wb.active
    ws_short.title = "Критично <14 дней"
    ws_calc = wb.create_sheet("Расчёт")

    report_date_text = cfg.run_date.strftime("%d.%m.%Y")

    ws_short["A1"] = f"Контроль остатка WB — товары менее {int(cfg.days_threshold)} дней на {report_date_text}"
    ws_short.merge_cells("A1:F1")
    ws_short["A1"].font = Font(name="Aptos", size=16, bold=True, color="FFFFFF")
    ws_short["A1"].fill = HEADER_FILL
    ws_short["A1"].alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    short_headers = [
        "Артикул 1С",
        "Код WB",
        "Остатка WB\nхватит, дней",
        "В Липецке есть\nзапас, дней",
        "WB + Липецк\nхватит, дней",
        "Статус",
    ]
    ws_short.append([])
    ws_short.append(short_headers)
    style_header_row(ws_short, 3)

    short_df = report_df[
        (report_df["days_wb"].notna())
        & (report_df["days_wb"] < cfg.days_threshold)
    ].copy()

    for _, row in short_df.iterrows():
        ws_short.append([
            row["article_1c"],
            row["nmId"],
            row["days_wb"],
            row["days_lipetsk"],
            row["days_total"],
            row["delist_flag"],
        ])

    if ws_short.max_row >= 4:
        style_body(ws_short, 4, ws_short.max_row, numeric_cols=[3, 4, 5], threshold_col=3, delist_col=6)
    autofit_layout(
        ws_short,
        widths={"A": 28, "B": 16, "C": 18, "D": 18, "E": 18, "F": 14},
        row_heights={1: 28, 3: 42},
    )
    apply_freeze_and_filter(ws_short, f"A3:F{max(ws_short.max_row, 3)}")

    ws_calc["A1"] = f"Расчёт дней до конца остатка WB — {cfg.store_name} — {report_date_text}"
    ws_calc.merge_cells("A1:J1")
    ws_calc["A1"].font = Font(name="Aptos", size=16, bold=True, color="FFFFFF")
    ws_calc["A1"].fill = HEADER_FILL
    ws_calc["A1"].alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    calc_headers = [
        "Артикул 1С",
        "Код WB",
        "Артикул WB",
        "Остаток WB,\nшт",
        "Продажи за 7 дней,\nшт",
        "Среднесуточные продажи,\nшт",
        "Остатки МП\n(Липецк), шт",
        "WB хватит,\nдней",
        "Липецк хватит,\nдней",
        "Статус",
    ]
    ws_calc.append([])
    ws_calc.append(calc_headers)
    style_header_row(ws_calc, 3)

    for _, row in report_df.iterrows():
        ws_calc.append([
            row["article_1c"],
            row["nmId"],
            row["supplierArticle"],
            row["stock_wb_qty"],
            row["sales_7d"],
            row["avg_daily_sales_7d"],
            row["wb_mp_qty"],
            row["days_wb"],
            row["days_lipetsk"],
            row["delist_flag"],
        ])

    if ws_calc.max_row >= 4:
        style_body(ws_calc, 4, ws_calc.max_row, numeric_cols=[4, 5, 6, 7, 8, 9], threshold_col=8, delist_col=10)
    autofit_layout(
        ws_calc,
        widths={"A": 28, "B": 16, "C": 22, "D": 14, "E": 16, "F": 20, "G": 18, "H": 14, "I": 16, "J": 12},
        row_heights={1: 28, 3: 42},
    )
    apply_freeze_and_filter(ws_calc, f"A3:J{max(ws_calc.max_row, 3)}")

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    wb.save(output_path)
    return output_path


# =========================
# TELEGRAM
# =========================



def send_telegram_document(bot_token: str, chat_id: str, file_path: str, caption: str = "") -> None:
    if not bot_token or not chat_id:
        raise ValueError("Не заданы TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID")

    url = f"https://api.telegram.org/bot{bot_token}/sendDocument"
    with open(file_path, "rb") as f:
        files = {"document": (Path(file_path).name, f)}
        data = {"chat_id": chat_id, "caption": caption[:1024]}
        response = requests.post(url, data=data, files=files, timeout=300)

    if response.status_code != 200:
        raise RuntimeError(f"Ошибка отправки в Telegram: {response.status_code} {response.text}")


# =========================
# MAIN
# =========================



def run(cfg: AppConfig = CONFIG, force_send: bool = False) -> str:
    Path(cfg.output_dir).mkdir(parents=True, exist_ok=True)
    storage = S3Storage(cfg)

    log(
        f"Старт отчёта по остаткам. store={cfg.store_name}, "
        f"run_date={cfg.run_date:%Y-%m-%d}, threshold_days={cfg.days_threshold}, sales_window_days={cfg.sales_window_days}"
    )

    orders = load_orders(storage, cfg)
    wb_stocks = load_wb_stocks(storage, cfg)
    article_map = load_article_map_1c(storage, cfg)
    stocks_1c_raw = load_1c_stocks(storage, cfg)
    stop_raw = load_stop_list(storage, cfg)

    sales_df = build_sales_metrics(orders, cfg)
    stocks_1c_df = prepare_1c_dataset(stocks_1c_raw, cfg)
    stop_df = prepare_stop_dataset(stop_raw)
    report_df = build_report_dataframe(sales_df, wb_stocks, article_map, stocks_1c_df, stop_df)

    output_path = str(Path(cfg.output_dir) / f"WB_остаток_по_дням_{cfg.store_name}_{cfg.run_date:%Y%m%d}.xlsx")
    create_excel_report(report_df, cfg, output_path)
    log(f"Сформирован отчёт: {output_path}")

    if cfg.upload_result_to_s3:
        result_key = cfg.result_prefix.format(store=cfg.store_name) + Path(output_path).name
        storage.upload_file(output_path, result_key)
        log(f"Отчёт загружен в Object Storage: {result_key}")

    if cfg.send_telegram or force_send:
        critical_count = int(((report_df["days_wb"].notna()) & (report_df["days_wb"] < cfg.days_threshold)).sum())
        delist_count = int((report_df["delist_flag"] == "Delist").sum())
        caption = (
            f"{cfg.store_name} | Остаток WB в днях | {cfg.run_date:%Y-%m-%d}\n"
            f"Товаров < {int(cfg.days_threshold)} дней: {critical_count}\n"
            f"Delist: {delist_count}"
        )
        send_telegram_document(cfg.telegram_bot_token, cfg.telegram_chat_id, output_path, caption)
        log("Отчёт отправлен в Telegram")

    return output_path



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Отчёт по дням до конца остатка WB")
    parser.add_argument("command", nargs="?", default="run", choices=["run"], help="Доступна команда run")
    parser.add_argument("--force-send", action="store_true", help="Отправить в Telegram, даже если WB_SEND_TELEGRAM=false")
    parser.add_argument("--run-date", help="Дата запуска в формате YYYY-MM-DD")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    cfg = CONFIG
    if args.run_date:
        cfg.run_date = datetime.strptime(args.run_date, "%Y-%m-%d")
    run(cfg=cfg, force_send=args.force_send)
