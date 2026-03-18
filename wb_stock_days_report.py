#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import io
import math
import os
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
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
# Общие утилиты
# =========================


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)



def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}



def normalize_text(value: object) -> str:
    if pd.isna(value) or value is None:
        return ""
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text



def normalize_key(value: object) -> str:
    return normalize_text(value).upper()



def safe_float(value: object) -> float:
    if pd.isna(value) or value is None or value == "":
        return 0.0
    try:
        return float(value)
    except Exception:
        text = normalize_text(value).replace(" ", "").replace(",", ".")
        return float(text) if text else 0.0



def ceil_int(value: object) -> int:
    return int(math.ceil(safe_float(value)))



def round_int(value: object) -> int:
    return int(round(safe_float(value)))



def floor_positive_int(value: Optional[float]) -> Optional[int]:
    if value is None:
        return None
    if value <= 0:
        return 0
    return int(math.floor(value))



def calculate_days(stock_qty: float, avg_daily_sales: float) -> Optional[float]:
    if avg_daily_sales <= 0:
        return None if stock_qty > 0 else 0.0
    return stock_qty / avg_daily_sales



def should_send_to_telegram(run_date: datetime, force_send: bool) -> bool:
    if force_send:
        log("Ручной запуск — отчёт будет отправлен в Telegram")
        return True
    if run_date.weekday() in {0, 4}:  # Пн и Пт
        return True
    return False



def choose_existing_column(df: pd.DataFrame, candidates: Iterable[str], label: str) -> str:
    normalized = {str(col).strip().lower(): col for col in df.columns}
    for candidate in candidates:
        real = normalized.get(candidate.strip().lower())
        if real is not None:
            return real
    raise KeyError(f"Не найдена колонка для '{label}'. Доступные колонки: {list(df.columns)}")



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



def parse_calendar_date_from_key(key: str) -> Optional[date]:
    patterns = [
        r"(20\d{2})[-_\.](\d{2})[-_\.](\d{2})",
        r"(\d{2})[-_\.](\d{2})[-_\.](20\d{2})",
        r"(20\d{2})(\d{2})(\d{2})",
    ]
    filename = Path(key).name
    for pattern in patterns:
        m = re.search(pattern, filename)
        if not m:
            continue
        parts = m.groups()
        try:
            if len(parts[0]) == 4:
                y, mth, d = int(parts[0]), int(parts[1]), int(parts[2])
            else:
                d, mth, y = int(parts[0]), int(parts[1]), int(parts[2])
            return date(y, mth, d)
        except Exception:
            continue
    return None



def contains_pt104(*values: object) -> bool:
    for value in values:
        txt = normalize_key(value)
        if txt.startswith("PT104"):
            return True
    return False


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
    run_date: datetime = datetime.strptime(
        os.getenv("WB_RUN_DATE", datetime.now().strftime("%Y-%m-%d")),
        "%Y-%m-%d",
    )
    sales_window_days: int = int(os.getenv("WB_SALES_WINDOW_DAYS", "7"))
    days_threshold: int = int(os.getenv("WB_LOW_STOCK_DAYS_THRESHOLD", "14"))
    dead_stock_threshold: int = int(os.getenv("WB_DEAD_STOCK_DAYS_THRESHOLD", "120"))
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
    stock_history_prefix: str = field(init=False)

    def __post_init__(self) -> None:
        weekly_prefix = self.stocks_prefix.format(store=self.store_name)
        self.stock_history_prefix = weekly_prefix.replace("Недельные/", "")


# =========================
# S3 / Object Storage
# =========================


class S3Storage:
    def __init__(self, cfg: AppConfig):
        if not cfg.bucket_name or not cfg.access_key or not cfg.secret_key:
            raise ValueError(
                "Не заданы параметры Object Storage. Нужны env из одной из схем: "
                "WB_S3_*, YC_* или CLOUD_RU_*"
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
# Загрузка источников
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

    lower_bound = cfg.run_date - timedelta(days=cfg.sales_window_days + 10)
    selected: list[tuple[datetime, str]] = []
    for key in keys:
        parsed = parse_iso_week_from_key(key)
        if not parsed:
            continue
        week_start = iso_week_start(*parsed)
        if week_start >= lower_bound:
            selected.append((week_start, key))

    if not selected:
        all_dated = []
        for key in keys:
            parsed = parse_iso_week_from_key(key)
            week_start = iso_week_start(*parsed) if parsed else datetime.min
            all_dated.append((week_start, key))
        selected = sorted(all_dated, key=lambda x: x[0])[-2:]

    selected_keys = [key for _, key in sorted(selected, key=lambda x: x[0])]
    log(f"Берём заказы WB из файлов: {selected_keys}")
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
    mp_col = choose_existing_column(df, ["Остатки МП"], "Остатки МП")

    result = pd.DataFrame(
        {
            "article_1c": df[article_col].map(normalize_text),
            "stock_company_qty": df[mp_col].map(ceil_int),
        }
    )
    result = result[result["article_1c"] != ""].drop_duplicates("article_1c", keep="first")
    return result



def extract_stock_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
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
    article_wb_col = None
    for candidate in ["supplierArticle", "Артикул продавца", "Артикул поставщика", "Артикул WB"]:
        try:
            article_wb_col = choose_existing_column(df, [candidate], "Артикул WB")
            break
        except Exception:
            continue

    out = pd.DataFrame(
        {
            "nmId": df[nm_col].map(normalize_text),
            "stock_wb_qty": df[qty_col].map(ceil_int),
        }
    )
    out["supplierArticle"] = df[article_wb_col].map(normalize_text) if article_wb_col else ""
    out = out[out["nmId"] != ""].copy()
    out = out.groupby(["nmId", "supplierArticle"], as_index=False).agg(stock_wb_qty=("stock_wb_qty", "sum"))
    return out



def load_wb_stocks(storage: S3Storage, cfg: AppConfig, stock_key: str) -> pd.DataFrame:
    df = storage.read_excel(stock_key)
    return extract_stock_snapshot(df)



def load_orders(storage: S3Storage, cfg: AppConfig, order_keys: list[str]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for key in order_keys:
        try:
            df = storage.read_excel(key, sheet_name="Заказы")
        except Exception:
            df = storage.read_excel(key)
        df.columns = [str(c).strip() for c in df.columns]
        frames.append(df)

    orders = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if orders.empty:
        return pd.DataFrame(columns=["nmId", "supplierArticle", "sales_7d", "avg_daily_sales_7d"])

    date_col = choose_existing_column(orders, ["date", "Дата заказа", "Дата"], "дата заказа")
    nm_col = choose_existing_column(orders, ["nmId", "Артикул WB", "Артикул wb"], "идентификатор товара WB")
    art_col = None
    for candidate in ["supplierArticle", "Артикул продавца", "Артикул поставщика"]:
        try:
            art_col = choose_existing_column(orders, [candidate], "Артикул продавца")
            break
        except Exception:
            continue

    qty_col = None
    for candidate in ["quantity", "qty", "Количество", "Кол-во", "Количество, шт"]:
        try:
            qty_col = choose_existing_column(orders, [candidate], "количество")
            break
        except Exception:
            continue

    cancel_col = None
    for candidate in ["isCancel", "cancel", "Отмена", "is_cancel"]:
        try:
            cancel_col = choose_existing_column(orders, [candidate], "признак отмены")
            break
        except Exception:
            continue

    orders = orders.copy()
    orders[date_col] = pd.to_datetime(orders[date_col], errors="coerce").dt.normalize()
    start_date = cfg.run_date - timedelta(days=cfg.sales_window_days - 1)
    orders = orders[(orders[date_col] >= start_date) & (orders[date_col] <= cfg.run_date)].copy()

    if cancel_col:
        orders = orders[~orders[cancel_col].fillna(False).astype(bool)].copy()

    orders["nmId"] = orders[nm_col].map(normalize_text)
    orders["supplierArticle"] = orders[art_col].map(normalize_text) if art_col else ""
    orders["qty"] = orders[qty_col].map(safe_float) if qty_col else 1.0

    grouped = orders.groupby(["nmId", "supplierArticle"], as_index=False).agg(sales_7d=("qty", "sum"))
    grouped["avg_daily_sales_7d"] = grouped["sales_7d"] / float(cfg.sales_window_days)
    return grouped



def load_zero_stock_days_current_month(
    storage: S3Storage,
    cfg: AppConfig,
    current_zero_nmids: set[str],
) -> pd.DataFrame:
    if not current_zero_nmids:
        return pd.DataFrame(columns=["nmId", "zero_stock_days_month"])

    all_keys = [k for k in storage.list_keys(cfg.stock_history_prefix) if k.lower().endswith(".xlsx")]
    month_start = date(cfg.run_date.year, cfg.run_date.month, 1)
    dated_keys: list[tuple[date, str]] = []
    for key in all_keys:
        file_date = parse_calendar_date_from_key(key)
        if not file_date:
            continue
        if month_start <= file_date <= cfg.run_date.date():
            dated_keys.append((file_date, key))

    dated_keys = sorted(dated_keys, key=lambda x: (x[0], x[1]))
    if not dated_keys:
        log("Дневные файлы остатков за текущий месяц не найдены — дни без остатка будут = 0")
        return pd.DataFrame(columns=["nmId", "zero_stock_days_month"])

    zero_days: dict[str, set[date]] = {nmid: set() for nmid in current_zero_nmids}

    for file_date, key in dated_keys:
        try:
            snapshot = extract_stock_snapshot(storage.read_excel(key))
        except Exception as exc:
            log(f"Пропускаем файл истории остатков {key}: {exc}")
            continue

        snapshot = snapshot[snapshot["nmId"].isin(current_zero_nmids)]
        if snapshot.empty:
            continue

        zero_nmids = set(snapshot.loc[snapshot["stock_wb_qty"] <= 0, "nmId"].astype(str))
        for nmid in zero_nmids:
            zero_days.setdefault(nmid, set()).add(file_date)

    result = pd.DataFrame(
        {
            "nmId": list(zero_days.keys()),
            "zero_stock_days_month": [len(days) for days in zero_days.values()],
        }
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
    stop_articles: set[str],
    zero_stock_days: pd.DataFrame,
    cfg: AppConfig,
) -> pd.DataFrame:
    df = wb_stocks.merge(sales, on=["nmId", "supplierArticle"], how="left")
    df["sales_7d"] = df["sales_7d"].fillna(0.0)
    df["avg_daily_sales_7d"] = df["avg_daily_sales_7d"].fillna(0.0)

    df = df.merge(article_map, on="nmId", how="left")
    df["article_1c"] = df["article_1c"].fillna("").map(normalize_text)

    df = df.merge(stocks_1c, on="article_1c", how="left")
    df["stock_company_qty"] = df["stock_company_qty"].fillna(0).astype(int)

    df = df.merge(zero_stock_days, on="nmId", how="left")
    df["zero_stock_days_month"] = df["zero_stock_days_month"].fillna(0).astype(int)

    df["days_wb_float"] = df.apply(
        lambda x: calculate_days(safe_float(x["stock_wb_qty"]), safe_float(x["avg_daily_sales_7d"])),
        axis=1,
    )
    df["days_company_float"] = df.apply(
        lambda x: calculate_days(safe_float(x["stock_company_qty"]), safe_float(x["avg_daily_sales_7d"])),
        axis=1,
    )
    df["days_total_float"] = df.apply(
        lambda x: calculate_days(
            safe_float(x["stock_wb_qty"]) + safe_float(x["stock_company_qty"]),
            safe_float(x["avg_daily_sales_7d"]),
        ),
        axis=1,
    )

    df["days_wb"] = df["days_wb_float"].map(floor_positive_int)
    df["days_company"] = df["days_company_float"].map(floor_positive_int)
    df["days_total"] = df["days_total_float"].map(floor_positive_int)

    df["status"] = df["article_1c"].map(lambda x: "Delist" if normalize_key(x) in stop_articles else "")

    df["sales_7d"] = df["sales_7d"].map(round_int).astype(int)
    df["avg_daily_sales_7d"] = df["avg_daily_sales_7d"].map(lambda x: int(math.ceil(x)) if safe_float(x) > 0 else 0)
    df["stock_wb_qty"] = df["stock_wb_qty"].fillna(0).astype(int)

    df = df[~df.apply(lambda r: contains_pt104(r["supplierArticle"], r["article_1c"]), axis=1)].copy()

    df["is_dead_stock"] = (
        ((df["stock_wb_qty"] + df["stock_company_qty"]) > 0)
        & (
            (df["avg_daily_sales_7d"] <= 0)
            | (df["days_total_float"].fillna(float("inf")) > cfg.dead_stock_threshold)
        )
    )

    sort_key = df["days_wb_float"].copy()
    sort_key = sort_key.where(sort_key.notna(), 999999.0)
    df = (
        df.assign(_sort_days=sort_key)
        .sort_values(["_sort_days", "stock_wb_qty", "article_1c", "supplierArticle"], ascending=[True, True, True, True])
        .drop(columns=["_sort_days"])
        .reset_index(drop=True)
    )
    return df


# =========================
# Excel
# =========================


HEADER_FILL = PatternFill("solid", fgColor="1F4E78")
WARNING_FILL = PatternFill("solid", fgColor="FFF2CC")
CRITICAL_FILL = PatternFill("solid", fgColor="F4CCCC")
DELIST_FILL = PatternFill("solid", fgColor="D9D2E9")
DEAD_FILL = PatternFill("solid", fgColor="000000")
WHITE_FONT = Font(name="Aptos", size=14, bold=True, color="FFFFFF")
BASE_FONT = Font(name="Aptos", size=14)
HEADER_FONT = Font(name="Aptos", size=14, bold=True, color="FFFFFF")
TITLE_FONT = Font(name="Aptos", size=16, bold=True, color="FFFFFF")
BORDER = Border(
    left=Side(style="thin", color="D9D9D9"),
    right=Side(style="thin", color="D9D9D9"),
    top=Side(style="thin", color="D9D9D9"),
    bottom=Side(style="thin", color="D9D9D9"),
)
CENTER_WRAP = Alignment(horizontal="center", vertical="center", wrap_text=True)



def style_title(ws, cell_range: str, text: str) -> None:
    first_cell = cell_range.split(":")[0]
    ws[first_cell] = text
    ws.merge_cells(cell_range)
    cell = ws[first_cell]
    cell.fill = HEADER_FILL
    cell.font = TITLE_FONT
    cell.alignment = CENTER_WRAP
    cell.border = BORDER



def style_header(ws, row_idx: int, fill: PatternFill = HEADER_FILL, font: Font = HEADER_FONT) -> None:
    for cell in ws[row_idx]:
        if cell.value is None:
            continue
        cell.fill = fill
        cell.font = font
        cell.alignment = CENTER_WRAP
        cell.border = BORDER



def apply_row_style(ws, row_idx: int, fill: Optional[PatternFill] = None, font: Optional[Font] = None) -> None:
    for cell in ws[row_idx]:
        cell.font = font or BASE_FONT
        cell.alignment = CENTER_WRAP
        cell.border = BORDER
        if fill is not None:
            cell.fill = fill



def style_conditional_row(ws, row_idx: int, days_wb_col: int, status_col: int, dead: bool = False) -> None:
    if dead:
        apply_row_style(ws, row_idx, fill=DEAD_FILL, font=WHITE_FONT)
        return

    row_fill = None
    status_value = normalize_text(ws.cell(row_idx, status_col).value)
    days_value = ws.cell(row_idx, days_wb_col).value
    try:
        numeric_days = int(days_value) if days_value is not None and days_value != "" else None
    except Exception:
        numeric_days = None

    if status_value == "Delist":
        row_fill = DELIST_FILL
    if numeric_days is not None and numeric_days < 7:
        row_fill = CRITICAL_FILL
    elif numeric_days is not None and numeric_days < 14 and row_fill is None:
        row_fill = WARNING_FILL

    apply_row_style(ws, row_idx, fill=row_fill, font=BASE_FONT)



def autosize_columns(ws, min_width: int = 12, max_width: int = 42) -> None:
    for col_idx in range(1, ws.max_column + 1):
        col_letter = get_column_letter(col_idx)
        max_len = 0
        for cell in ws[col_letter]:
            value = "" if cell.value is None else str(cell.value)
            longest_line = max((len(line) for line in value.split("\n")), default=0)
            max_len = max(max_len, longest_line)
        width = min(max(max_len + 2, min_width), max_width)
        ws.column_dimensions[col_letter].width = width



def set_sheet_layout(ws, title_row: int, header_row: int) -> None:
    ws.row_dimensions[title_row].height = 28
    ws.row_dimensions[header_row].height = 42
    ws.freeze_panes = f"A{header_row + 1}"
    ws.sheet_view.showGridLines = False
    ws.auto_filter.ref = f"A{header_row}:{get_column_letter(ws.max_column)}{max(ws.max_row, header_row)}"
    autosize_columns(ws)



def append_int_row(ws, values: list[object]) -> None:
    ws.append(values)



def save_excel_report(df: pd.DataFrame, cfg: AppConfig, output_path: str) -> None:
    wb = Workbook()
    ws_short = wb.active
    ws_short.title = "Критично <14 дней"
    ws_calc = wb.create_sheet("Расчёт")
    ws_dead = wb.create_sheet("Dead_Stock")

    report_date_text = cfg.run_date.strftime("%d.%m.%Y")

    short_df = df[(df["days_wb"].notna()) & (df["days_wb"] < cfg.days_threshold)].copy()
    dead_df = df[df["is_dead_stock"]].copy().sort_values(["days_total", "article_1c"], ascending=[False, True])

    # Лист 1
    style_title(ws_short, "A1:F1", f"Контроль остатка WB — товары менее {cfg.days_threshold} дней на {report_date_text}")
    short_headers = [
        "Артикул 1С",
        "Артикул WB",
        "Остатка WB хватит, дней",
        "В Липецке есть запас, дней",
        "Дней без остатка WB в текущем месяце",
        "Статус",
    ]
    ws_short.append(short_headers)
    style_header(ws_short, 2)
    for _, row in short_df.iterrows():
        append_int_row(
            ws_short,
            [
                row["article_1c"],
                row["supplierArticle"],
                row["days_wb"],
                row["days_company"],
                row["zero_stock_days_month"] if row["stock_wb_qty"] == 0 else 0,
                row["status"],
            ],
        )
    for r in range(3, ws_short.max_row + 1):
        style_conditional_row(ws_short, r, days_wb_col=3, status_col=6)
    set_sheet_layout(ws_short, title_row=1, header_row=2)

    # Лист 2
    style_title(ws_calc, "A1:L1", f"Расчёт дней до конца остатка WB — {cfg.store_name} — {report_date_text}")
    calc_headers = [
        "nmId",
        "Артикул WB",
        "Артикул 1С",
        "Остаток WB, шт",
        "Продажи за 7 дней, шт",
        "Среднесуточные продажи за 7 дней, шт",
        "Остатки МП (Липецк), шт",
        "WB хватит, дней",
        "Липецк хватит, дней",
        "WB + Липецк, дней",
        "Дней без остатка WB в текущем месяце",
        "Статус",
    ]
    ws_calc.append(calc_headers)
    style_header(ws_calc, 2)
    for _, row in df.iterrows():
        append_int_row(
            ws_calc,
            [
                row["nmId"],
                row["supplierArticle"],
                row["article_1c"],
                int(row["stock_wb_qty"]),
                int(row["sales_7d"]),
                int(row["avg_daily_sales_7d"]),
                int(row["stock_company_qty"]),
                row["days_wb"] if row["days_wb"] is not None else "",
                row["days_company"] if row["days_company"] is not None else "",
                row["days_total"] if row["days_total"] is not None else "",
                int(row["zero_stock_days_month"]) if int(row["stock_wb_qty"]) == 0 else 0,
                row["status"],
            ],
        )
    for r in range(3, ws_calc.max_row + 1):
        style_conditional_row(ws_calc, r, days_wb_col=8, status_col=12)
    set_sheet_layout(ws_calc, title_row=1, header_row=2)

    # Лист 3 Dead_Stock
    style_title(ws_dead, "A1:H1", f"Dead Stock — товаров более {cfg.dead_stock_threshold} дней на {report_date_text}")
    dead_headers = [
        "Артикул 1С",
        "Артикул WB",
        "Остаток WB, шт",
        "Остатки МП (Липецк), шт",
        "WB + Липецк, дней",
        "Продажи за 7 дней, шт",
        "Дней без остатка WB в текущем месяце",
        "Статус",
    ]
    ws_dead.append(dead_headers)
    style_header(ws_dead, 2, fill=DEAD_FILL, font=WHITE_FONT)
    for _, row in dead_df.iterrows():
        append_int_row(
            ws_dead,
            [
                row["article_1c"],
                row["supplierArticle"],
                int(row["stock_wb_qty"]),
                int(row["stock_company_qty"]),
                row["days_total"] if row["days_total"] is not None else cfg.dead_stock_threshold + 1,
                int(row["sales_7d"]),
                int(row["zero_stock_days_month"]) if int(row["stock_wb_qty"]) == 0 else 0,
                row["status"],
            ],
        )
    for r in range(3, ws_dead.max_row + 1):
        style_conditional_row(ws_dead, r, days_wb_col=5, status_col=8, dead=True)
    set_sheet_layout(ws_dead, title_row=1, header_row=2)

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    wb.save(output_path)


# =========================
# Telegram
# =========================



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

    wb_stocks = load_wb_stocks(storage, cfg, stock_key)
    sales = load_orders(storage, cfg, order_keys)
    article_map = load_article_map(storage, cfg)
    stocks_1c = load_1c_stocks(storage, cfg)
    stop_articles = parse_stop_articles(cfg.stop_articles_raw)

    current_zero_nmids = set(wb_stocks.loc[wb_stocks["stock_wb_qty"] <= 0, "nmId"].astype(str))
    zero_stock_days = load_zero_stock_days_current_month(storage, cfg, current_zero_nmids)

    report_df = build_report_dataframe(
        wb_stocks=wb_stocks,
        sales=sales,
        article_map=article_map,
        stocks_1c=stocks_1c,
        stop_articles=stop_articles,
        zero_stock_days=zero_stock_days,
        cfg=cfg,
    )

    output_path = str(Path(cfg.output_dir) / f"Отчёт_дни_остатка_WB_{cfg.store_name}_{cfg.run_date:%Y%m%d}.xlsx")
    save_excel_report(report_df, cfg, output_path)
    log(f"Отчёт сохранён: {output_path}")

    if cfg.send_telegram and should_send_to_telegram(cfg.run_date, cfg.force_send_env):
        short_df = report_df[(report_df["days_wb"].notna()) & (report_df["days_wb"] < cfg.days_threshold)]
        dead_df = report_df[report_df["is_dead_stock"]]
        delist_count = int((short_df["status"] == "Delist").sum())
        caption = (
            f"📦 Отчёт по остаткам WB {cfg.store_name}\n"
            f"Дата: {cfg.run_date:%d.%m.%Y}\n"
            f"Товаров < {cfg.days_threshold} дней: {len(short_df)}\n"
            f"Dead_Stock > {cfg.dead_stock_threshold} дней: {len(dead_df)}\n"
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
