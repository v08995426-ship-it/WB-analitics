#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import io
import math
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional

import boto3
import pandas as pd
import requests
from botocore.client import Config as BotoConfig
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter


# =========================
# Базовые утилиты
# =========================


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)



def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}



def normalize_text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text



def normalize_key(value: object) -> str:
    return normalize_text(value).upper()



def safe_float(value: object) -> float:
    if value is None or value == "" or pd.isna(value):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    text = normalize_text(value).replace(" ", "").replace(",", ".")
    return float(text) if text else 0.0



def ceil_int(value: object) -> int:
    return int(math.ceil(max(0.0, safe_float(value))))



def int_or_zero(value: object) -> int:
    return int(round(safe_float(value)))



def calculate_days(stock_qty: float, avg_daily_sales: float) -> int:
    if avg_daily_sales <= 0:
        return 999999 if stock_qty > 0 else 0
    return int(math.floor(stock_qty / avg_daily_sales))



def choose_existing_column(df: pd.DataFrame, candidates: Iterable[str], label: str) -> str:
    normalized = {str(col).strip().lower(): col for col in df.columns}
    for candidate in candidates:
        real = normalized.get(str(candidate).strip().lower())
        if real is not None:
            return real
    raise KeyError(f"Не найдена колонка для '{label}'. Доступные колонки: {list(df.columns)}")



def try_choose_column(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    normalized = {str(col).strip().lower(): col for col in df.columns}
    for candidate in candidates:
        real = normalized.get(str(candidate).strip().lower())
        if real is not None:
            return real
    return None



def parse_stop_articles(raw: str) -> set[str]:
    if not raw:
        return set()
    normalized = raw.replace("\r", "\n").replace(";", "\n").replace(",", "\n")
    return {normalize_key(item) for item in normalized.split("\n") if normalize_text(item)}



def parse_iso_week_from_key(key: str) -> Optional[tuple[int, int]]:
    match = re.search(r"_(\d{4})-W(\d{2})\.xlsx$", key, flags=re.IGNORECASE)
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))



def iso_week_start(year: int, week: int) -> datetime:
    return datetime.fromisocalendar(year, week, 1)


# =========================
# Конфиг
# =========================


@dataclass
class AppConfig:
    bucket_name: str = (
        os.getenv("WB_S3_BUCKET")
        or os.getenv("YC_BUCKET_NAME")
        or os.getenv("CLOUD_RU_BUCKET")
        or ""
    )
    access_key: str = (
        os.getenv("WB_S3_ACCESS_KEY")
        or os.getenv("YC_ACCESS_KEY_ID")
        or os.getenv("CLOUD_RU_ACCESS_KEY")
        or ""
    )
    secret_key: str = (
        os.getenv("WB_S3_SECRET_KEY")
        or os.getenv("YC_SECRET_ACCESS_KEY")
        or os.getenv("CLOUD_RU_SECRET_KEY")
        or ""
    )
    endpoint_url: str = (
        os.getenv("WB_S3_ENDPOINT")
        or os.getenv("YC_ENDPOINT")
        or os.getenv("CLOUD_RU_ENDPOINT")
        or "https://storage.yandexcloud.net"
    )
    region_name: str = os.getenv("WB_S3_REGION") or os.getenv("YC_REGION") or "ru-central1"

    store_name: str = os.getenv("WB_STORE", "TOPFACE").strip()
    run_date: datetime = datetime.strptime(os.getenv("WB_RUN_DATE", datetime.now().strftime("%Y-%m-%d")), "%Y-%m-%d")

    sales_window_days: int = int(os.getenv("WB_SALES_WINDOW_DAYS", "7"))
    activity_window_days: int = int(os.getenv("WB_ACTIVITY_WINDOW_DAYS", "60"))
    low_stock_days_threshold: int = int(os.getenv("WB_LOW_STOCK_DAYS_THRESHOLD", "14"))
    dead_stock_days_threshold: int = int(os.getenv("WB_DEAD_STOCK_DAYS_THRESHOLD", "120"))
    output_dir: str = os.getenv("WB_OUTPUT_DIR", "output")

    telegram_bot_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    telegram_chat_id: str = os.getenv("TELEGRAM_CHAT_ID", "")
    send_telegram: bool = env_bool("WB_SEND_TELEGRAM", True)
    force_send_env: bool = env_bool("WB_FORCE_SEND", False)

    stocks_prefix: str = os.getenv("WB_STOCKS_PREFIX", "Отчёты/Остатки/{store}/Недельные/")
    orders_prefix: str = os.getenv("WB_ORDERS_PREFIX", "Отчёты/Заказы/{store}/Недельные/")
    article_map_key: str = os.getenv("WB_ARTICLE_MAP_KEY", "Отчёты/Остатки/1С/Артикулы 1с.xlsx")
    stocks_1c_key: str = os.getenv("WB_STOCKS_1C_KEY", "Отчёты/Остатки/1С/Остатки 1С.xlsx")
    stop_articles_raw: str = os.getenv("WB_STOP_LIST_KEY", "")
    exclude_article_prefixes: tuple[str, ...] = ("PT104",)


# =========================
# Object Storage
# =========================


class S3Storage:
    def __init__(self, cfg: AppConfig):
        if not cfg.bucket_name or not cfg.access_key or not cfg.secret_key:
            raise ValueError(
                "Не заданы параметры Object Storage. Нужны env из одной из схем: WB_S3_*, YC_* или CLOUD_RU_*"
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

    def list_keys(self, prefix: str) -> list[str]:
        keys: list[str] = []
        token = None
        while True:
            params = {"Bucket": self.bucket, "Prefix": prefix, "MaxKeys": 1000}
            if token:
                params["ContinuationToken"] = token
            resp = self.s3.list_objects_v2(**params)
            keys.extend(obj["Key"] for obj in resp.get("Contents", []))
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        return keys

    def read_bytes(self, key: str) -> bytes:
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        return obj["Body"].read()

    def read_excel(self, key: str, sheet_name: Optional[str] = None) -> pd.DataFrame:
        content = self.read_bytes(key)
        df = pd.read_excel(io.BytesIO(content), sheet_name=sheet_name)
        if isinstance(df, dict):
            return next(iter(df.values()))
        return df


# =========================
# Источники
# =========================


def find_latest_stock_file(storage: S3Storage, cfg: AppConfig) -> str:
    prefix = cfg.stocks_prefix.format(store=cfg.store_name)
    keys = [k for k in storage.list_keys(prefix) if k.lower().endswith(".xlsx")]
    if not keys:
        raise FileNotFoundError(f"Не найдены weekly-файлы остатков по префиксу: {prefix}")

    dated = []
    for key in keys:
        parsed = parse_iso_week_from_key(key)
        week_start = iso_week_start(*parsed) if parsed else datetime.min
        dated.append((week_start, key))
    latest_key = max(dated, key=lambda x: x[0])[1]
    log(f"Берём остатки WB из файла: {latest_key}")
    return latest_key



def find_order_files_for_window(storage: S3Storage, cfg: AppConfig) -> list[str]:
    prefix = cfg.orders_prefix.format(store=cfg.store_name)
    keys = [k for k in storage.list_keys(prefix) if k.lower().endswith(".xlsx")]
    if not keys:
        raise FileNotFoundError(f"Не найдены weekly-файлы заказов по префиксу: {prefix}")

    history_days = max(cfg.sales_window_days, cfg.activity_window_days)
    lower_bound = cfg.run_date - timedelta(days=history_days + 14)
    selected: list[tuple[datetime, str]] = []
    for key in keys:
        parsed = parse_iso_week_from_key(key)
        if not parsed:
            continue
        week_start = iso_week_start(*parsed)
        if week_start >= lower_bound:
            selected.append((week_start, key))

    if not selected:
        selected = sorted(
            [(iso_week_start(*p), k) for k in keys if (p := parse_iso_week_from_key(k))],
            key=lambda x: x[0],
        )[-10:]

    selected_keys = [key for _, key in sorted(selected, key=lambda x: x[0])]
    log(f"Берём заказы WB из файлов: {selected_keys}")
    return selected_keys



def find_stock_history_files_for_month(storage: S3Storage, cfg: AppConfig) -> list[str]:
    prefix = cfg.stocks_prefix.format(store=cfg.store_name)
    keys = [k for k in storage.list_keys(prefix) if k.lower().endswith(".xlsx")]
    month_start = cfg.run_date.replace(day=1)
    selected: list[tuple[datetime, str]] = []
    for key in keys:
        parsed = parse_iso_week_from_key(key)
        if not parsed:
            continue
        week_start = iso_week_start(*parsed)
        if week_start >= month_start - timedelta(days=7):
            selected.append((week_start, key))
    selected_keys = [k for _, k in sorted(selected, key=lambda x: x[0])]
    return selected_keys



def load_article_map(storage: S3Storage, cfg: AppConfig) -> pd.DataFrame:
    df = storage.read_excel(cfg.article_map_key)
    if df.shape[1] < 3:
        raise ValueError("Файл Артикулы 1с.xlsx должен содержать минимум 3 колонки")

    mapped = pd.DataFrame(
        {
            "nmId": df.iloc[:, 0].map(normalize_text),
            "article_1c": df.iloc[:, 2].map(normalize_text),
        }
    )
    mapped = mapped[(mapped["nmId"] != "") & (mapped["article_1c"] != "")].drop_duplicates("nmId")
    log(f"Загружено соответствий WB -> 1С: {len(mapped)}")
    return mapped



def load_1c_stocks(storage: S3Storage, cfg: AppConfig) -> pd.DataFrame:
    df = storage.read_excel(cfg.stocks_1c_key)
    df.columns = [str(c).strip() for c in df.columns]

    article_col = choose_existing_column(df, ["Артикул", "АРТ"], "Артикул 1С")
    mp_col = choose_existing_column(df, ["Остатки МП", "Остатки МП (Липецк), шт"], "Остатки МП")

    result = pd.DataFrame(
        {
            "article_1c": df[article_col].map(normalize_text),
            "stock_company_qty": df[mp_col].map(ceil_int),
        }
    )
    result = result[result["article_1c"] != ""].drop_duplicates("article_1c", keep="first")
    return result



def load_wb_stocks(storage: S3Storage, stock_key: str) -> pd.DataFrame:
    df = storage.read_excel(stock_key)
    df.columns = [str(c).strip() for c in df.columns]

    nm_col = choose_existing_column(df, ["nmId", "Артикул WB", "Артикул wb"], "идентификатор товара WB")
    qty_col = choose_existing_column(
        df,
        [
            "Доступно для продажи",
            "Полное количество",
            "Остатки МП",
            "Количество",
            "Доступно",
            "Остаток",
            "Остатки",
        ],
        "остаток WB",
    )
    article_wb_col = try_choose_column(df, ["supplierArticle", "Артикул продавца", "Артикул поставщика", "Артикул WB"])

    out = pd.DataFrame(
        {
            "nmId": df[nm_col].map(normalize_text),
            "stock_wb_qty": df[qty_col].map(ceil_int),
            "supplierArticle": df[article_wb_col].map(normalize_text) if article_wb_col else "",
        }
    )
    out = out[out["nmId"] != ""].copy()
    out = out.groupby(["nmId", "supplierArticle"], as_index=False).agg(stock_wb_qty=("stock_wb_qty", "sum"))
    return out



def load_orders(storage: S3Storage, cfg: AppConfig, order_keys: list[str]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for key in order_keys:
        try:
            df = storage.read_excel(key, sheet_name="Заказы")
        except Exception:
            df = storage.read_excel(key)
        df.columns = [str(c).strip() for c in df.columns]
        frames.append(df)

    if not frames:
        return pd.DataFrame(columns=["nmId", "supplierArticle", "sales_7d", "sales_60d", "avg_daily_sales_7d"])

    orders = pd.concat(frames, ignore_index=True)
    if orders.empty:
        return pd.DataFrame(columns=["nmId", "supplierArticle", "sales_7d", "sales_60d", "avg_daily_sales_7d"])

    date_col = choose_existing_column(orders, ["date", "Дата заказа", "Дата"], "дата заказа")
    nm_col = choose_existing_column(orders, ["nmId", "Артикул WB", "Артикул wb"], "идентификатор товара WB")
    art_col = try_choose_column(orders, ["supplierArticle", "Артикул продавца", "Артикул поставщика"])
    qty_col = try_choose_column(orders, ["quantity", "qty", "Количество", "Кол-во", "Количество, шт"])
    cancel_col = try_choose_column(orders, ["isCancel", "cancel", "Отмена", "is_cancel"])

    orders = orders.copy()
    orders[date_col] = pd.to_datetime(orders[date_col], errors="coerce").dt.normalize()
    orders = orders[orders[date_col].notna()].copy()

    if cancel_col:
        orders = orders[~orders[cancel_col].fillna(False).astype(bool)].copy()

    orders["nmId"] = orders[nm_col].map(normalize_text)
    orders["supplierArticle"] = orders[art_col].map(normalize_text) if art_col else ""
    orders["qty"] = orders[qty_col].map(safe_float) if qty_col else 1.0

    start_60d = cfg.run_date - timedelta(days=cfg.activity_window_days - 1)
    start_7d = cfg.run_date - timedelta(days=cfg.sales_window_days - 1)

    orders_60d = orders[(orders[date_col] >= start_60d) & (orders[date_col] <= cfg.run_date)].copy()
    orders_7d = orders[(orders[date_col] >= start_7d) & (orders[date_col] <= cfg.run_date)].copy()

    agg_60d = orders_60d.groupby(["nmId", "supplierArticle"], as_index=False).agg(sales_60d=("qty", "sum"))
    agg_7d = orders_7d.groupby(["nmId", "supplierArticle"], as_index=False).agg(sales_7d=("qty", "sum"))

    sales = agg_60d.merge(agg_7d, on=["nmId", "supplierArticle"], how="outer").fillna(0.0)
    sales["avg_daily_sales_7d"] = sales["sales_7d"] / float(cfg.sales_window_days)
    return sales



def count_zero_stock_days_current_month(
    storage: S3Storage,
    cfg: AppConfig,
    history_keys: list[str],
    current_zero_nmids: set[str],
) -> pd.DataFrame:
    if not current_zero_nmids:
        return pd.DataFrame(columns=["nmId", "days_zero_in_month"])

    month_start = cfg.run_date.replace(day=1)
    dates_by_nmid: dict[str, set[datetime.date]] = {nmid: set() for nmid in current_zero_nmids}

    for key in history_keys:
        try:
            df = storage.read_excel(key)
        except Exception:
            continue
        if df.empty:
            continue
        df.columns = [str(c).strip() for c in df.columns]

        try:
            nm_col = choose_existing_column(df, ["nmId", "Артикул WB", "Артикул wb"], "идентификатор товара WB")
            qty_col = choose_existing_column(df, ["Доступно для продажи", "Полное количество", "Количество", "Остаток", "Остатки"], "остаток WB")
        except Exception:
            continue

        date_col = try_choose_column(df, ["Дата сбора", "Дата запроса", "Дата последнего изменения", "Дата"])
        if date_col:
            stock_dates = pd.to_datetime(df[date_col], errors="coerce").dt.normalize()
        else:
            parsed = parse_iso_week_from_key(key)
            fallback_date = iso_week_start(*parsed) if parsed else cfg.run_date
            stock_dates = pd.Series([fallback_date] * len(df))

        tmp = pd.DataFrame(
            {
                "nmId": df[nm_col].map(normalize_text),
                "stock_wb_qty": df[qty_col].map(ceil_int),
                "stock_date": stock_dates,
            }
        )
        tmp = tmp[tmp["nmId"].isin(current_zero_nmids)].copy()
        tmp = tmp[tmp["stock_date"].notna()].copy()
        tmp = tmp[tmp["stock_date"] >= month_start].copy()
        tmp = tmp[tmp["stock_wb_qty"] <= 0].copy()

        for row in tmp.itertuples(index=False):
            dates_by_nmid[row.nmId].add(row.stock_date.date())

    result = pd.DataFrame(
        {"nmId": list(dates_by_nmid.keys()), "days_zero_in_month": [len(v) for v in dates_by_nmid.values()]}
    )
    return result


# =========================
# Расчёт
# =========================


def build_report_dataframe(
    wb_stocks: pd.DataFrame,
    sales: pd.DataFrame,
    article_map: pd.DataFrame,
    stocks_1c: pd.DataFrame,
    zero_days_df: pd.DataFrame,
    stop_articles: set[str],
    cfg: AppConfig,
) -> pd.DataFrame:
    df = wb_stocks.merge(sales, on=["nmId", "supplierArticle"], how="left")
    for col in ["sales_7d", "sales_60d", "avg_daily_sales_7d"]:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = df[col].fillna(0.0)

    df = df.merge(article_map, on="nmId", how="left")
    df["article_1c"] = df["article_1c"].fillna("").map(normalize_text)

    df = df.merge(stocks_1c, on="article_1c", how="left")
    df["stock_company_qty"] = df.get("stock_company_qty", 0).fillna(0).map(ceil_int)

    if not zero_days_df.empty:
        df = df.merge(zero_days_df, on="nmId", how="left")
    if "days_zero_in_month" not in df.columns:
        df["days_zero_in_month"] = 0
    df["days_zero_in_month"] = df["days_zero_in_month"].fillna(0).astype(int)

    df["days_wb"] = df.apply(lambda x: calculate_days(safe_float(x["stock_wb_qty"]), safe_float(x["avg_daily_sales_7d"])), axis=1)
    df["days_company"] = df.apply(lambda x: calculate_days(safe_float(x["stock_company_qty"]), safe_float(x["avg_daily_sales_7d"])), axis=1)
    df["days_total"] = df.apply(
        lambda x: calculate_days(safe_float(x["stock_wb_qty"]) + safe_float(x["stock_company_qty"]), safe_float(x["avg_daily_sales_7d"])),
        axis=1,
    )
    df["status"] = df["article_1c"].map(lambda x: "Delist" if normalize_key(x) in stop_articles else "")

    for col in ["stock_wb_qty", "stock_company_qty", "sales_7d", "sales_60d"]:
        df[col] = df[col].map(int_or_zero)
    for col in ["days_wb", "days_company", "days_total"]:
        df[col] = df[col].map(int_or_zero)

    exclude_mask = pd.Series(False, index=df.index)
    for prefix in cfg.exclude_article_prefixes:
        exclude_mask = exclude_mask | df["article_1c"].map(normalize_key).str.startswith(prefix)
    df = df[~exclude_mask].copy()

    sort_days = df["days_wb"].replace(0, 0).fillna(999999)
    df = df.assign(_sort_days=sort_days).sort_values(["_sort_days", "stock_wb_qty", "article_1c"], ascending=[True, True, True])
    df = df.drop(columns=["_sort_days"]).reset_index(drop=True)
    return df


# =========================
# Excel
# =========================


HEADER_FILL = PatternFill("solid", fgColor="1F4E78")
WARNING_FILL = PatternFill("solid", fgColor="FFF2CC")
CRITICAL_FILL = PatternFill("solid", fgColor="F4CCCC")
DELIST_FILL = PatternFill("solid", fgColor="D9D2E9")
DEAD_FILL = PatternFill("solid", fgColor="000000")
WHITE_FONT = Font(name="Calibri", size=14, color="FFFFFF")
BASE_FONT = Font(name="Calibri", size=14)
HEADER_FONT = Font(name="Calibri", size=14, bold=True, color="FFFFFF")
TITLE_FONT = Font(name="Calibri", size=15, bold=True, color="FFFFFF")
BORDER = Border(
    left=Side(style="thin", color="D9D9D9"),
    right=Side(style="thin", color="D9D9D9"),
    top=Side(style="thin", color="D9D9D9"),
    bottom=Side(style="thin", color="D9D9D9"),
)
CENTER = Alignment(horizontal="center", vertical="center", wrap_text=True)



def write_title(ws, text: str, columns: int) -> None:
    ws.cell(1, 1, text)
    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=columns)
    cell = ws.cell(1, 1)
    cell.fill = HEADER_FILL
    cell.font = TITLE_FONT
    cell.alignment = CENTER
    ws.row_dimensions[1].height = 28



def write_headers(ws, headers: list[str]) -> None:
    for col_idx, header in enumerate(headers, start=1):
        cell = ws.cell(2, col_idx, header)
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = CENTER
        cell.border = BORDER
    ws.row_dimensions[2].height = 42



def apply_row_style(ws, row_idx: int, days_col: int, status_col: int, dead: bool = False) -> None:
    row_fill = None
    row_font = BASE_FONT

    if dead:
        row_fill = DEAD_FILL
        row_font = WHITE_FONT
    else:
        days_value = ws.cell(row_idx, days_col).value
        status_value = normalize_text(ws.cell(row_idx, status_col).value)
        numeric_days = safe_float(days_value)
        if status_value == "Delist":
            row_fill = DELIST_FILL
        if numeric_days < 7:
            row_fill = CRITICAL_FILL
        elif numeric_days < 14 and row_fill is None:
            row_fill = WARNING_FILL

    for cell in ws[row_idx]:
        cell.font = row_font
        cell.alignment = CENTER
        cell.border = BORDER
        if row_fill is not None:
            cell.fill = row_fill



def autofit_worksheet(ws) -> None:
    for col_cells in ws.columns:
        col_idx = col_cells[0].column
        max_len = 0
        for cell in col_cells:
            value = "" if cell.value is None else str(cell.value)
            max_len = max(max_len, max((len(x) for x in value.split("\n")), default=0))
        ws.column_dimensions[get_column_letter(col_idx)].width = min(max(max_len + 3, 12), 36)
    ws.freeze_panes = "A3"
    ws.sheet_view.showGridLines = False
    ws.auto_filter.ref = f"A2:{get_column_letter(ws.max_column)}{max(ws.max_row, 2)}"



def dataframe_rows(df: pd.DataFrame, columns: list[str]) -> list[list[object]]:
    rows = []
    for _, row in df.iterrows():
        rows.append([row.get(col, "") for col in columns])
    return rows



def save_excel_report(df: pd.DataFrame, cfg: AppConfig, output_path: str) -> None:
    wb = Workbook()
    ws_short = wb.active
    ws_short.title = "Критично <14 дней"
    ws_calc = wb.create_sheet("Расчёт")
    ws_dead = wb.create_sheet("Dead_Stock")

    report_date_text = cfg.run_date.strftime("%d.%m.%Y")

    short_df = df[(df["sales_60d"] > 0) & ((df["stock_wb_qty"] <= 0) | (df["days_wb"] < cfg.low_stock_days_threshold))].copy()
    dead_df = df[(df["sales_60d"] > 0) & (df["days_total"] > cfg.dead_stock_days_threshold)].copy()

    # Лист 1
    short_headers = [
        "Артикул 1С",
        "Продажи за 60 дней, шт",
        "Остаток WB, шт",
        "WB хватит, дней",
        "Остатки МП (Липецк), шт",
        "Липецк хватит, дней",
        "WB + Липецк, дней",
        "Дней без остатка в текущем месяце",
        "Статус",
    ]
    write_title(
        ws_short,
        f"Товары с продажами за {cfg.activity_window_days} дней и текущим остатком WB = 0 или < {cfg.low_stock_days_threshold} дней на {report_date_text}",
        len(short_headers),
    )
    write_headers(ws_short, short_headers)
    short_export = short_df.rename(columns={
        "article_1c": "Артикул 1С",
        "sales_60d": "Продажи за 60 дней, шт",
        "stock_wb_qty": "Остаток WB, шт",
        "days_wb": "WB хватит, дней",
        "stock_company_qty": "Остатки МП (Липецк), шт",
        "days_company": "Липецк хватит, дней",
        "days_total": "WB + Липецк, дней",
        "days_zero_in_month": "Дней без остатка в текущем месяце",
        "status": "Статус",
    })
    for values in dataframe_rows(short_export, short_headers):
        ws_short.append(values)
    for r in range(3, ws_short.max_row + 1):
        apply_row_style(ws_short, r, days_col=4, status_col=9)
    autofit_worksheet(ws_short)

    # Лист 2
    calc_headers = [
        "nmId",
        "Артикул WB",
        "Артикул 1С",
        "Остаток WB, шт",
        "Продажи за 7 дней, шт",
        "Продажи за 60 дней, шт",
        "Среднесуточные продажи за 7 дней, шт",
        "Остатки МП (Липецк), шт",
        "WB хватит, дней",
        "Липецк хватит, дней",
        "WB + Липецк, дней",
        "Дней без остатка в текущем месяце",
        "Статус",
    ]
    write_title(ws_calc, f"Полный расчёт дней остатка WB — {cfg.store_name} — {report_date_text}", len(calc_headers))
    write_headers(ws_calc, calc_headers)
    calc_export = df.rename(columns={
        "nmId": "nmId",
        "supplierArticle": "Артикул WB",
        "article_1c": "Артикул 1С",
        "stock_wb_qty": "Остаток WB, шт",
        "sales_7d": "Продажи за 7 дней, шт",
        "sales_60d": "Продажи за 60 дней, шт",
        "avg_daily_sales_7d": "Среднесуточные продажи за 7 дней, шт",
        "stock_company_qty": "Остатки МП (Липецк), шт",
        "days_wb": "WB хватит, дней",
        "days_company": "Липецк хватит, дней",
        "days_total": "WB + Липецк, дней",
        "days_zero_in_month": "Дней без остатка в текущем месяце",
        "status": "Статус",
    }).copy()
    calc_export["Среднесуточные продажи за 7 дней, шт"] = calc_export["Среднесуточные продажи за 7 дней, шт"].map(int_or_zero)
    for values in dataframe_rows(calc_export, calc_headers):
        ws_calc.append(values)
    for r in range(3, ws_calc.max_row + 1):
        apply_row_style(ws_calc, r, days_col=9, status_col=13)
    autofit_worksheet(ws_calc)

    # Лист 3
    dead_headers = [
        "Артикул 1С",
        "Продажи за 60 дней, шт",
        "Остаток WB, шт",
        "Остатки МП (Липецк), шт",
        "WB + Липецк, дней",
        "Статус",
    ]
    write_title(ws_dead, f"Dead Stock — запас более {cfg.dead_stock_days_threshold} дней на {report_date_text}", len(dead_headers))
    write_headers(ws_dead, dead_headers)
    dead_export = dead_df.rename(columns={
        "article_1c": "Артикул 1С",
        "sales_60d": "Продажи за 60 дней, шт",
        "stock_wb_qty": "Остаток WB, шт",
        "stock_company_qty": "Остатки МП (Липецк), шт",
        "days_total": "WB + Липецк, дней",
        "status": "Статус",
    })
    for values in dataframe_rows(dead_export, dead_headers):
        ws_dead.append(values)
    for r in range(3, ws_dead.max_row + 1):
        apply_row_style(ws_dead, r, days_col=5, status_col=6, dead=True)
    autofit_worksheet(ws_dead)

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    wb.save(output_path)


# =========================
# Telegram
# =========================



def should_send_to_telegram(run_date: datetime, force_send: bool) -> bool:
    if force_send:
        log("Ручной запуск — отчёт будет отправлен в Telegram")
        return True
    return run_date.weekday() in {0, 4}



def send_telegram_document(bot_token: str, chat_id: str, file_path: str, caption: str) -> None:
    if not bot_token or not chat_id:
        raise ValueError("Не заданы TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID")
    url = f"https://api.telegram.org/bot{bot_token}/sendDocument"
    with open(file_path, "rb") as fh:
        files = {"document": (Path(file_path).name, fh)}
        data = {"chat_id": chat_id, "caption": caption[:1024]}
        response = requests.post(url, data=data, files=files, timeout=300)
    if response.status_code != 200:
        raise RuntimeError(f"Ошибка отправки в Telegram: {response.status_code} {response.text}")


# =========================
# Main
# =========================



def run() -> str:
    cfg = AppConfig()
    storage = S3Storage(cfg)

    stock_key = find_latest_stock_file(storage, cfg)
    order_keys = find_order_files_for_window(storage, cfg)
    stock_history_keys = find_stock_history_files_for_month(storage, cfg)

    wb_stocks = load_wb_stocks(storage, stock_key)
    sales = load_orders(storage, cfg, order_keys)
    article_map = load_article_map(storage, cfg)
    stocks_1c = load_1c_stocks(storage, cfg)
    stop_articles = parse_stop_articles(cfg.stop_articles_raw)

    current_zero_nmids = set(wb_stocks.loc[wb_stocks["stock_wb_qty"] <= 0, "nmId"].astype(str).tolist())
    zero_days_df = count_zero_stock_days_current_month(storage, cfg, stock_history_keys, current_zero_nmids)

    report_df = build_report_dataframe(wb_stocks, sales, article_map, stocks_1c, zero_days_df, stop_articles, cfg)

    output_path = str(Path(cfg.output_dir) / f"Отчёт_дни_остатка_WB_{cfg.store_name}_{cfg.run_date:%Y%m%d}.xlsx")
    save_excel_report(report_df, cfg, output_path)
    log(f"Отчёт сохранён: {output_path}")

    short_df = report_df[(report_df["sales_60d"] > 0) & ((report_df["stock_wb_qty"] <= 0) | (report_df["days_wb"] < cfg.low_stock_days_threshold))]
    dead_df = report_df[(report_df["sales_60d"] > 0) & (report_df["days_total"] > cfg.dead_stock_days_threshold)]

    if cfg.send_telegram and should_send_to_telegram(cfg.run_date, cfg.force_send_env):
        delist_count = int((short_df["status"] == "Delist").sum())
        caption = (
            f"📦 Отчёт по остаткам WB {cfg.store_name}\n"
            f"Дата: {cfg.run_date:%d.%m.%Y}\n"
            f"Критичных товаров: {len(short_df)}\n"
            f"Dead Stock: {len(dead_df)}\n"
            f"Delist в критичных: {delist_count}"
        )
        send_telegram_document(cfg.telegram_bot_token, cfg.telegram_chat_id, output_path, caption)
        log("Отчёт отправлен в Telegram")
    else:
        log("Отправка в Telegram пропущена по расписанию")

    log(f"Источник остатков: {stock_key}")
    log(f"Источники заказов: {', '.join(order_keys)}")
    log(f"Delist-артикулов из env: {len(stop_articles)}")
    return output_path


if __name__ == "__main__":
    run()
