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
    if value is None or pd.isna(value) or value == "":
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(" ", "").replace(",", ".")
    return float(text) if text else 0.0


def ceil_int(value: object) -> int:
    return int(math.ceil(safe_float(value)))


def floor_nonneg(value: Optional[float]) -> int:
    if value is None or pd.isna(value):
        return 0
    value = float(value)
    if value <= 0:
        return 0
    return int(math.floor(value))


def choose_existing_column(df: pd.DataFrame, candidates: Iterable[str], label: str) -> str:
    mapping = {str(col).strip().lower(): col for col in df.columns}
    for candidate in candidates:
        real = mapping.get(candidate.strip().lower())
        if real is not None:
            return real
    raise KeyError(f"Не найдена колонка для '{label}'. Доступные колонки: {list(df.columns)}")


def parse_stop_articles(raw: str) -> set[str]:
    if not raw:
        return set()
    text = raw.replace("\r", "\n").replace(";", "\n").replace(",", "\n")
    return {normalize_key(x) for x in text.split("\n") if normalize_text(x)}


def parse_iso_week_from_key(key: str) -> Optional[tuple[int, int]]:
    m = re.search(r"_(\d{4})-W(\d{2})\.xlsx$", key, flags=re.IGNORECASE)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def iso_week_start(year: int, week: int) -> datetime:
    return datetime.fromisocalendar(year, week, 1)


def parse_snapshot_date_from_key(key: str) -> Optional[datetime]:
    m = re.search(r"(\d{4}-\d{2}-\d{2})", key)
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y-%m-%d")
        except Exception:
            pass
    m = re.search(r"(\d{8})", key)
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y%m%d")
        except Exception:
            pass
    parsed_week = parse_iso_week_from_key(key)
    if parsed_week:
        return iso_week_start(*parsed_week)
    return None


def natural_sort_key(text: str):
    parts = re.split(r"(\d+)", normalize_text(text).upper())
    out = []
    for part in parts:
        if part.isdigit():
            out.append((0, int(part)))
        else:
            out.append((1, part))
    return out


def format_ratio_rrc(value: float) -> str:
    if value <= 0:
        return ""
    return f"{value:.2f}".replace(".", ",") + "_РРЦ"


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
    long_window_days: int = int(os.getenv("WB_ACTIVITY_WINDOW_DAYS", "60"))
    critical_threshold_days: float = float(os.getenv("WB_LOW_STOCK_DAYS_THRESHOLD", "14"))
    dead_stock_threshold_days: float = float(os.getenv("WB_DEAD_STOCK_DAYS_THRESHOLD", "120"))
    black_cell_threshold_days: float = float(os.getenv("WB_BLACK_CELL_DAYS_THRESHOLD", "180"))
    output_dir: str = os.getenv("WB_OUTPUT_DIR", "output")

    telegram_bot_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    telegram_chat_id: str = os.getenv("TELEGRAM_CHAT_ID", "")
    send_telegram: bool = env_bool("WB_SEND_TELEGRAM", True)
    force_send_env: bool = env_bool("WB_FORCE_SEND", False)

    stocks_prefix: str = os.getenv("WB_STOCKS_PREFIX", "Отчёты/Остатки/{store}/Недельные/")
    orders_prefix: str = os.getenv("WB_ORDERS_PREFIX", "Отчёты/Заказы/{store}/Недельные/")
    article_map_key: str = os.getenv("WB_ARTICLE_MAP_KEY", "Отчёты/Остатки/1С/Артикулы 1с.xlsx")
    stocks_1c_key: str = os.getenv("WB_STOCKS_1C_KEY", "Отчёты/Остатки/1С/Остатки 1С.xlsx")
    rrc_key: str = os.getenv("WB_RRC_KEY", "Отчёты/Финансовые показатели/{store}/РРЦ.xlsx")

    stop_articles_raw: str = os.getenv("WB_STOP_LIST_KEY", "")


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
            config=BotoConfig(signature_version="s3v4"),
        )

    def list_keys(self, prefix: str) -> list[str]:
        keys: list[str] = []
        token = None
        while True:
            params = {"Bucket": self.bucket, "Prefix": prefix}
            if token:
                params["ContinuationToken"] = token
            resp = self.s3.list_objects_v2(**params)
            for item in resp.get("Contents", []):
                keys.append(item["Key"])
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        return keys

    def read_bytes(self, key: str) -> bytes:
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        return obj["Body"].read()

    def read_excel(self, key: str, sheet_name=0) -> pd.DataFrame:
        return pd.read_excel(io.BytesIO(self.read_bytes(key)), sheet_name=sheet_name)


# -------------------- loading --------------------

def should_send_to_telegram(run_date: datetime, force_send: bool) -> bool:
    if force_send:
        log("Ручной запуск — отчёт будет отправлен в Telegram")
        return True
    return run_date.weekday() in {0, 4}


def find_latest_stock_file(storage: S3Storage, cfg: AppConfig) -> str:
    prefix = cfg.stocks_prefix.format(store=cfg.store_name)
    keys = [k for k in storage.list_keys(prefix) if k.lower().endswith(".xlsx")]
    if not keys:
        raise FileNotFoundError(f"Не найдены weekly-файлы остатков по префиксу: {prefix}")
    dated = [(parse_snapshot_date_from_key(k) or datetime.min, k) for k in keys]
    latest_key = max(dated, key=lambda x: x[0])[1]
    log(f"Берём остатки WB из файла: {latest_key}")
    return latest_key


def find_order_files_for_window(storage: S3Storage, cfg: AppConfig) -> list[str]:
    prefix = cfg.orders_prefix.format(store=cfg.store_name)
    keys = [k for k in storage.list_keys(prefix) if k.lower().endswith(".xlsx")]
    if not keys:
        raise FileNotFoundError(f"Не найдены weekly-файлы заказов по префиксу: {prefix}")
    lower_bound = cfg.run_date - timedelta(days=cfg.long_window_days + 10)
    selected: list[tuple[datetime, str]] = []
    for key in keys:
        dt = parse_snapshot_date_from_key(key)
        if dt and dt >= lower_bound:
            selected.append((dt, key))
    if not selected:
        selected = sorted([(parse_snapshot_date_from_key(k) or datetime.min, k) for k in keys], key=lambda x: x[0])[-10:]
    selected_keys = [k for _, k in sorted(selected, key=lambda x: x[0])]
    log(f"Берём заказы WB из файлов: {selected_keys}")
    return selected_keys


def load_article_map(storage: S3Storage, cfg: AppConfig) -> pd.DataFrame:
    df = storage.read_excel(cfg.article_map_key)
    if df.shape[1] < 3:
        raise ValueError("Файл Артикулы 1с.xlsx должен содержать минимум 3 колонки")
    result = pd.DataFrame(
        {
            "nmId": df.iloc[:, 0].map(normalize_text),
            "article_1c": df.iloc[:, 2].map(normalize_text),
        }
    )
    result = result[(result["nmId"] != "") & (result["article_1c"] != "")].drop_duplicates("nmId")
    log(f"Загружено соответствий WB -> 1С: {len(result)}")
    return result


def load_1c_stocks(storage: S3Storage, cfg: AppConfig) -> pd.DataFrame:
    df = storage.read_excel(cfg.stocks_1c_key)
    df.columns = [str(c).strip() for c in df.columns]
    article_col = choose_existing_column(df, ["Артикул", "АРТ"], "Артикул 1С")
    mp_col = choose_existing_column(df, ["Остатки МП (Липецк), шт", "Остатки МП"], "Остатки МП")
    result = pd.DataFrame(
        {
            "article_1c": df[article_col].map(normalize_text),
            "stock_company_qty": df[mp_col].map(ceil_int),
        }
    )
    return result[result["article_1c"] != ""].drop_duplicates("article_1c")


def load_rrc(storage: S3Storage, cfg: AppConfig) -> pd.DataFrame:
    key = cfg.rrc_key.format(store=cfg.store_name)
    # TOPFACE -> лист TF
    preferred_sheets = ["TF", cfg.store_name, 0]
    last_err = None
    for sheet in preferred_sheets:
        try:
            df = storage.read_excel(key, sheet_name=sheet)
            break
        except Exception as e:
            last_err = e
            df = None
    if df is None:
        raise last_err
    df.columns = [str(c).strip() for c in df.columns]
    article_col = choose_existing_column(df, ["ПРАВИЛЬНЫЙ АРТИКУЛ", "Артикул", "АРТ"], "Артикул 1С в РРЦ")
    rrc_col = choose_existing_column(df, ["РРЦ"], "РРЦ")
    out = pd.DataFrame(
        {
            "article_1c": df[article_col].map(normalize_text),
            "rrc": df[rrc_col].map(lambda x: round(safe_float(x), 2)),
        }
    )
    out = out[(out["article_1c"] != "") & (out["rrc"] > 0)].drop_duplicates("article_1c")
    return out


def load_wb_stocks(storage: S3Storage, stock_key: str) -> pd.DataFrame:
    df = storage.read_excel(stock_key)
    df.columns = [str(c).strip() for c in df.columns]
    nm_col = choose_existing_column(df, ["nmId", "Артикул WB", "Артикул wb"], "идентификатор товара WB")
    qty_col = choose_existing_column(
        df,
        ["Доступно для продажи", "Полное количество", "Остатки МП", "Количество", "Доступно", "Остаток", "Остатки"],
        "остаток WB",
    )
    article_wb_col = None
    for candidate in ["Артикул продавца", "supplierArticle", "Артикул поставщика", "Артикул WB"]:
        try:
            article_wb_col = choose_existing_column(df, [candidate], "Артикул продавца")
            break
        except Exception:
            pass
    out = pd.DataFrame(
        {
            "nmId": df[nm_col].map(normalize_text),
            "stock_wb_qty": df[qty_col].map(ceil_int),
            "supplierArticle": df[article_wb_col].map(normalize_text) if article_wb_col else "",
        }
    )
    out = out[out["nmId"] != ""].copy()
    return out.groupby(["nmId", "supplierArticle"], as_index=False).agg(stock_wb_qty=("stock_wb_qty", "sum"))


def load_orders(storage: S3Storage, cfg: AppConfig, order_keys: list[str]) -> pd.DataFrame:
    frames = []
    for key in order_keys:
        try:
            df = storage.read_excel(key, sheet_name="Заказы")
        except Exception:
            df = storage.read_excel(key)
        df.columns = [str(c).strip() for c in df.columns]
        frames.append(df)
    orders = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if orders.empty:
        return pd.DataFrame(columns=["nmId", "supplierArticle", "sales_7d", "sales_60d", "avg_daily_sales_7d", "avg_daily_sales_60d", "avg_price_last_day"])

    date_col = choose_existing_column(orders, ["Дата заказа", "Дата", "date"], "дата заказа")
    nm_col = choose_existing_column(orders, ["nmId", "Артикул WB", "Артикул wb"], "идентификатор товара WB")
    art_col = None
    for candidate in ["Артикул продавца", "supplierArticle", "Артикул поставщика"]:
        try:
            art_col = choose_existing_column(orders, [candidate], "Артикул продавца")
            break
        except Exception:
            pass
    qty_col = None
    for candidate in ["Количество", "Кол-во", "Количество, шт", "quantity", "qty"]:
        try:
            qty_col = choose_existing_column(orders, [candidate], "количество")
            break
        except Exception:
            pass
    cancel_col = None
    for candidate in ["Отмена", "isCancel", "cancel", "is_cancel"]:
        try:
            cancel_col = choose_existing_column(orders, [candidate], "признак отмены")
            break
        except Exception:
            pass
    price_col = None
    for candidate in ["finishedPrice", "FinishedPrice", "Цена покупателя", "Цена продажи", "priceWithDisc"]:
        try:
            price_col = choose_existing_column(orders, [candidate], "finishedPrice")
            break
        except Exception:
            pass

    orders = orders.copy()
    orders[date_col] = pd.to_datetime(orders[date_col], errors="coerce").dt.normalize()
    orders = orders[orders[date_col].notna()].copy()
    if cancel_col:
        try:
            orders = orders[~orders[cancel_col].fillna(False).astype(bool)].copy()
        except Exception:
            pass

    orders["nmId"] = orders[nm_col].map(normalize_text)
    orders["supplierArticle"] = orders[art_col].map(normalize_text) if art_col else ""
    orders["qty"] = orders[qty_col].map(safe_float) if qty_col else 1.0
    if price_col:
        orders["finishedPrice"] = orders[price_col].map(safe_float)
    else:
        orders["finishedPrice"] = 0.0

    start_60 = cfg.run_date - timedelta(days=cfg.long_window_days - 1)
    start_7 = cfg.run_date - timedelta(days=cfg.sales_window_days - 1)
    orders_60 = orders[(orders[date_col] >= start_60) & (orders[date_col] <= cfg.run_date)].copy()
    orders_7 = orders[(orders[date_col] >= start_7) & (orders[date_col] <= cfg.run_date)].copy()

    g60 = orders_60.groupby(["nmId", "supplierArticle"], as_index=False).agg(sales_60d=("qty", "sum"))
    g7 = orders_7.groupby(["nmId", "supplierArticle"], as_index=False).agg(sales_7d=("qty", "sum"))
    result = pd.merge(g60, g7, on=["nmId", "supplierArticle"], how="outer").fillna(0.0)
    result["avg_daily_sales_60d"] = result["sales_60d"] / float(cfg.long_window_days)
    result["avg_daily_sales_7d"] = result["sales_7d"] / float(cfg.sales_window_days)

    # средняя цена продажи за последний день, где есть заказы
    last_order_day = orders_60[date_col].max() if not orders_60.empty else pd.NaT
    if pd.notna(last_order_day) and price_col:
        last_day = orders_60[orders_60[date_col] == last_order_day].copy()
        grp = last_day.groupby(["nmId", "supplierArticle"], as_index=False).agg(
            price_sum=("finishedPrice", lambda s: float((s).sum())),
            qty_sum=("qty", "sum"),
        )
        grp["avg_price_last_day"] = grp.apply(
            lambda x: round(safe_float(x["price_sum"]) / safe_float(x["qty_sum"]), 2) if safe_float(x["qty_sum"]) > 0 else 0.0,
            axis=1,
        )
        result = result.merge(grp[["nmId", "supplierArticle", "avg_price_last_day"]], on=["nmId", "supplierArticle"], how="left")
    else:
        result["avg_price_last_day"] = 0.0

    return result


# -------------------- calculations --------------------

def calculate_days(stock_qty: float, avg_daily_sales: float) -> float:
    if avg_daily_sales <= 0:
        return 0.0
    return stock_qty / avg_daily_sales


def build_report_dataframe(
    wb_stocks: pd.DataFrame,
    sales: pd.DataFrame,
    article_map: pd.DataFrame,
    stocks_1c: pd.DataFrame,
    rrc_df: pd.DataFrame,
    stop_articles: set[str],
) -> pd.DataFrame:
    df = wb_stocks.merge(sales, on=["nmId", "supplierArticle"], how="left")
    for col in ["sales_7d", "sales_60d", "avg_daily_sales_7d", "avg_daily_sales_60d", "avg_price_last_day"]:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = df[col].fillna(0.0)

    df = df.merge(article_map, on="nmId", how="left")
    df["article_1c"] = df["article_1c"].fillna("").map(normalize_text)
    df = df[df["article_1c"] != ""].copy()
    df = df[~df["article_1c"].str.upper().str.startswith("PT104", na=False)].copy()

    df = df.merge(stocks_1c, on="article_1c", how="left")
    df["stock_company_qty"] = df["stock_company_qty"].fillna(0).map(ceil_int)

    df = df.merge(rrc_df, on="article_1c", how="left")
    df["rrc"] = df["rrc"].fillna(0.0)

    # если на WB 0 или за 7 дней нет продаж — считаем по 60 дням
    df["use_60d_logic"] = (df["stock_wb_qty"] <= 0) | (df["avg_daily_sales_7d"] <= 0)
    df["effective_avg_daily_sales"] = df["avg_daily_sales_7d"]
    df.loc[df["use_60d_logic"], "effective_avg_daily_sales"] = df.loc[df["use_60d_logic"], "avg_daily_sales_60d"]
    df["effective_avg_daily_sales"] = df["effective_avg_daily_sales"].fillna(0.0)

    df["days_wb_raw"] = df.apply(lambda x: calculate_days(x["stock_wb_qty"], x["effective_avg_daily_sales"]), axis=1)
    df["days_company_raw"] = df.apply(lambda x: calculate_days(x["stock_company_qty"], x["effective_avg_daily_sales"]), axis=1)
    df["days_total_raw"] = df.apply(lambda x: calculate_days(x["stock_wb_qty"] + x["stock_company_qty"], x["effective_avg_daily_sales"]), axis=1)

    df["days_wb"] = df["days_wb_raw"].map(floor_nonneg)
    df["days_company"] = df["days_company_raw"].map(floor_nonneg)
    df["days_total"] = df["days_total_raw"].map(floor_nonneg)

    df["sales_7d"] = df["sales_7d"].round().astype(int)
    df["sales_60d"] = df["sales_60d"].round().astype(int)
    df["stock_wb_qty"] = df["stock_wb_qty"].fillna(0).map(ceil_int)
    df["stock_company_qty"] = df["stock_company_qty"].fillna(0).map(ceil_int)
    df["avg_price_last_day"] = df["avg_price_last_day"].round(2)
    df["ratio_rrc"] = df.apply(lambda x: round(x["avg_price_last_day"] / x["rrc"], 2) if x["rrc"] > 0 and x["avg_price_last_day"] > 0 else 0.0, axis=1)
    df["ratio_rrc_label"] = df["ratio_rrc"].map(format_ratio_rrc)
    df["status"] = df["article_1c"].map(lambda x: "Delist" if normalize_key(x) in stop_articles else "")

    df = df[df["sales_60d"] > 0].copy()
    df = df.sort_values(by="article_1c", key=lambda s: s.map(natural_sort_key)).reset_index(drop=True)
    return df


def list_stock_history_files_for_month(storage: S3Storage, cfg: AppConfig) -> list[tuple[datetime, str]]:
    store_root = cfg.stocks_prefix.format(store=cfg.store_name)
    if store_root.endswith("Недельные/"):
        store_root = store_root[: -len("Недельные/")]
    keys = [k for k in storage.list_keys(store_root) if k.lower().endswith(".xlsx")]
    out = []
    month_start = cfg.run_date.replace(day=1)
    for key in keys:
        dt = parse_snapshot_date_from_key(key)
        if dt and month_start <= dt <= cfg.run_date:
            out.append((dt, key))
    out.sort(key=lambda x: (x[0], x[1]))
    return out


def count_zero_stock_days_current_month(storage: S3Storage, cfg: AppConfig, zero_nmids: set[str]) -> dict[str, int]:
    if not zero_nmids:
        return {}
    files = list_stock_history_files_for_month(storage, cfg)
    if not files:
        return {}
    seen_dates: dict[str, set[datetime.date]] = {nmid: set() for nmid in zero_nmids}
    for dt, key in files:
        try:
            df = storage.read_excel(key)
        except Exception:
            continue
        df.columns = [str(c).strip() for c in df.columns]
        try:
            nm_col = choose_existing_column(df, ["nmId", "Артикул WB", "Артикул wb"], "идентификатор товара WB")
            qty_col = choose_existing_column(df, ["Доступно для продажи", "Полное количество", "Остатки МП", "Количество", "Доступно", "Остаток", "Остатки"], "остаток WB")
        except Exception:
            continue
        snap = pd.DataFrame({"nmId": df[nm_col].map(normalize_text), "qty": df[qty_col].map(ceil_int)})
        snap = snap[snap["nmId"].isin(zero_nmids)].groupby("nmId", as_index=False).agg(qty=("qty", "sum"))
        for _, row in snap.iterrows():
            if int(row["qty"]) == 0:
                seen_dates[row["nmId"]].add(dt.date())
    return {k: len(v) for k, v in seen_dates.items()}


# -------------------- excel --------------------

HEADER_FILL = PatternFill("solid", fgColor="1F4E78")
HEADER_GREEN_FILL = PatternFill("solid", fgColor="C6E0B4")
CRITICAL_FILL = PatternFill("solid", fgColor="F4CCCC")
WARNING_FILL = PatternFill("solid", fgColor="FFF2CC")
DELIST_FILL = PatternFill("solid", fgColor="D9D2E9")
BLACK_FILL = PatternFill("solid", fgColor="000000")
BASE_FONT = Font(name="Calibri", size=14, color="000000")
HEADER_FONT = Font(name="Calibri", size=14, color="FFFFFF", bold=True)
HEADER_DARK_FONT = Font(name="Calibri", size=14, color="000000", bold=True)
THIN_BORDER = Border(
    left=Side(style="thin", color="D9D9D9"),
    right=Side(style="thin", color="D9D9D9"),
    top=Side(style="thin", color="D9D9D9"),
    bottom=Side(style="thin", color="D9D9D9"),
)


def auto_fit_widths(ws) -> None:
    widths = {}
    for row in ws.iter_rows():
        for cell in row:
            if cell.value is None:
                continue
            text = str(cell.value)
            length = max(len(x) for x in text.split("\n"))
            if cell.row == 1 and "Артикул" in text:
                length = max(length, 14)
            widths[cell.column] = max(widths.get(cell.column, 0), length)
    for col_idx, length in widths.items():
        width = min(max(length + 2, 12), 32)
        ws.column_dimensions[get_column_letter(col_idx)].width = width


def style_sheet(ws) -> None:
    ws.freeze_panes = "A2"
    ws.sheet_view.showGridLines = False
    for cell in ws[1]:
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = THIN_BORDER
    for row in ws.iter_rows(min_row=2):
        for cell in row:
            cell.font = BASE_FONT
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            cell.border = THIN_BORDER
    ws.row_dimensions[1].height = 38
    for r in range(2, ws.max_row + 1):
        ws.row_dimensions[r].height = 24
    auto_fit_widths(ws)


def append_dataframe(ws, df: pd.DataFrame) -> None:
    ws.append(list(df.columns))
    for row in df.itertuples(index=False):
        ws.append(list(row))


def save_excel_report(df: pd.DataFrame, cfg: AppConfig, storage: S3Storage, output_path: str) -> None:
    zero_now_nmids = set(df.loc[df["stock_wb_qty"] == 0, "nmId"].astype(str))
    zero_days_map = count_zero_stock_days_current_month(storage, cfg, zero_now_nmids)
    df = df.copy()
    df["zero_days_current_month"] = df["nmId"].map(lambda x: int(zero_days_map.get(str(x), 0)))
    df.loc[df["stock_wb_qty"] > 0, "zero_days_current_month"] = 0

    critical_df = df[(df["sales_60d"] > 0) & ((df["stock_wb_qty"] == 0) | (df["days_wb_raw"] < cfg.critical_threshold_days))].copy()
    dead_df = df[(df["sales_60d"] > 0) & (df["days_total_raw"] > cfg.dead_stock_threshold_days)].copy()

    critical_df = critical_df.sort_values(by="article_1c", key=lambda s: s.map(natural_sort_key)).reset_index(drop=True)
    dead_df = dead_df.sort_values(by="article_1c", key=lambda s: s.map(natural_sort_key)).reset_index(drop=True)
    calc_df = df.sort_values(by="article_1c", key=lambda s: s.map(natural_sort_key)).reset_index(drop=True)

    critical_view = critical_df[[
        "article_1c", "sales_60d", "stock_wb_qty", "days_wb", "stock_company_qty", "days_company",
        "days_total", "zero_days_current_month", "status"
    ]].rename(columns={
        "article_1c": "Артикул 1С",
        "sales_60d": "Продажи за 60 дней, шт",
        "stock_wb_qty": "Остаток WB, шт",
        "days_wb": "WB хватит, дней",
        "stock_company_qty": "Остатки МП (Липецк), шт",
        "days_company": "Липецк хватит, дней",
        "days_total": "WB + Липецк, дней",
        "zero_days_current_month": "Дней без остатка в текущем месяце",
        "status": "Статус",
    })

    dead_view = dead_df[[
        "article_1c", "sales_60d", "stock_wb_qty", "days_wb", "stock_company_qty", "days_company",
        "days_total", "avg_price_last_day", "rrc", "ratio_rrc_label", "zero_days_current_month", "status"
    ]].rename(columns={
        "article_1c": "Артикул 1С",
        "sales_60d": "Продажи за 60 дней, шт",
        "stock_wb_qty": "Остаток WB, шт",
        "days_wb": "WB хватит, дней",
        "stock_company_qty": "Остатки МП (Липецк), шт",
        "days_company": "Липецк хватит, дней",
        "days_total": "WB + Липецк, дней",
        "avg_price_last_day": "Цена покупателя",
        "rrc": "РРЦ",
        "ratio_rrc_label": "Коэффициент",
        "zero_days_current_month": "Дней без остатка в текущем месяце",
        "status": "Статус",
    })

    calc_view = calc_df[[
        "nmId", "supplierArticle", "article_1c", "stock_wb_qty", "sales_7d", "sales_60d",
        "avg_daily_sales_7d", "avg_daily_sales_60d", "effective_avg_daily_sales", "stock_company_qty",
        "days_wb", "days_company", "days_total", "avg_price_last_day", "rrc", "ratio_rrc_label",
        "zero_days_current_month", "status"
    ]].copy().rename(columns={
        "nmId": "nmId",
        "supplierArticle": "Артикул WB",
        "article_1c": "Артикул 1С",
        "stock_wb_qty": "Остаток WB, шт",
        "sales_7d": "Продажи за 7 дней, шт",
        "sales_60d": "Продажи за 60 дней, шт",
        "avg_daily_sales_7d": "Среднесуточные продажи за 7 дней, шт",
        "avg_daily_sales_60d": "Среднесуточные продажи за 60 дней, шт",
        "effective_avg_daily_sales": "Среднесуточные продажи для расчёта, шт",
        "stock_company_qty": "Остатки МП (Липецк), шт",
        "days_wb": "WB хватит, дней",
        "days_company": "Липецк хватит, дней",
        "days_total": "WB + Липецк, дней",
        "avg_price_last_day": "Цена покупателя",
        "rrc": "РРЦ",
        "ratio_rrc_label": "Коэффициент",
        "zero_days_current_month": "Дней без остатка в текущем месяце",
        "status": "Статус",
    })

    # в calc оставляем целые по требованию, кроме коэффициента, который текстовый
    for col in ["Среднесуточные продажи за 7 дней, шт", "Среднесуточные продажи за 60 дней, шт", "Среднесуточные продажи для расчёта, шт", "Цена покупателя", "РРЦ"]:
        if col in calc_view.columns:
            calc_view[col] = calc_view[col].map(lambda x: floor_nonneg(round(safe_float(x), 0)))
    for col in ["Цена покупателя", "РРЦ"]:
        if col in dead_view.columns:
            dead_view[col] = dead_view[col].map(lambda x: floor_nonneg(round(safe_float(x), 0)))

    wb = Workbook()
    ws1 = wb.active
    ws1.title = "Критично <14 дней"
    append_dataframe(ws1, critical_view)

    ws2 = wb.create_sheet("Dead_Stock")
    append_dataframe(ws2, dead_view)

    ws3 = wb.create_sheet("Расчёт")
    append_dataframe(ws3, calc_view)

    style_sheet(ws1)
    style_sheet(ws2)
    style_sheet(ws3)

    # первый лист: подсветка строк
    headers1 = {ws1.cell(1, c).value: c for c in range(1, ws1.max_column + 1)}
    days_col1 = headers1.get("WB хватит, дней")
    status_col1 = headers1.get("Статус")
    for r in range(2, ws1.max_row + 1):
        days = safe_float(ws1.cell(r, days_col1).value) if days_col1 else 0
        fill = CRITICAL_FILL if days < 7 else WARNING_FILL
        for c in range(1, ws1.max_column + 1):
            ws1.cell(r, c).fill = fill
        if status_col1 and ws1.cell(r, status_col1).value == "Delist":
            ws1.cell(r, status_col1).fill = DELIST_FILL

    # Dead_Stock: салатовые колонки + чёрная только ячейка WB + Липецк, дней > 180
    headers2 = {ws2.cell(1, c).value: c for c in range(1, ws2.max_column + 1)}
    green_cols = [headers2.get("Цена покупателя"), headers2.get("РРЦ"), headers2.get("Коэффициент")]
    total_col2 = headers2.get("WB + Липецк, дней")
    status_col2 = headers2.get("Статус")

    for c in green_cols:
        if c:
            ws2.cell(1, c).fill = HEADER_GREEN_FILL
            ws2.cell(1, c).font = HEADER_DARK_FONT
            for r in range(2, ws2.max_row + 1):
                ws2.cell(r, c).fill = HEADER_GREEN_FILL

    for r in range(2, ws2.max_row + 1):
        if total_col2 and safe_float(ws2.cell(r, total_col2).value) > cfg.black_cell_threshold_days:
            ws2.cell(r, total_col2).fill = BLACK_FILL
            ws2.cell(r, total_col2).font = Font(name="Calibri", size=14, color="FFFFFF", bold=True)
        if status_col2 and ws2.cell(r, status_col2).value == "Delist":
            ws2.cell(r, status_col2).fill = DELIST_FILL

    # Расчёт: подсветка статуса и ценовых колонок
    headers3 = {ws3.cell(1, c).value: c for c in range(1, ws3.max_column + 1)}
    status_col3 = headers3.get("Статус")
    for key in ["Цена покупателя", "РРЦ", "Коэффициент"]:
        c = headers3.get(key)
        if c:
            ws3.cell(1, c).fill = HEADER_GREEN_FILL
            ws3.cell(1, c).font = HEADER_DARK_FONT
            for r in range(2, ws3.max_row + 1):
                ws3.cell(r, c).fill = HEADER_GREEN_FILL
    if status_col3:
        for r in range(2, ws3.max_row + 1):
            if ws3.cell(r, status_col3).value == "Delist":
                ws3.cell(r, status_col3).fill = DELIST_FILL

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    wb.save(output_path)


# -------------------- telegram and run --------------------

def send_telegram_document(bot_token: str, chat_id: str, file_path: str, caption: str) -> None:
    if not bot_token or not chat_id:
        log("Пропуск отправки в Telegram: TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID не заданы")
        return
    with open(file_path, "rb") as f:
        resp = requests.post(
            f"https://api.telegram.org/bot{bot_token}/sendDocument",
            data={"chat_id": chat_id, "caption": caption},
            files={"document": (Path(file_path).name, f)},
            timeout=120,
        )
    resp.raise_for_status()


def run() -> str:
    cfg = AppConfig()
    storage = S3Storage(cfg)

    stock_key = find_latest_stock_file(storage, cfg)
    order_keys = find_order_files_for_window(storage, cfg)

    wb_stocks = load_wb_stocks(storage, stock_key)
    sales = load_orders(storage, cfg, order_keys)
    article_map = load_article_map(storage, cfg)
    stocks_1c = load_1c_stocks(storage, cfg)
    rrc_df = load_rrc(storage, cfg)
    stop_articles = parse_stop_articles(cfg.stop_articles_raw)

    report_df = build_report_dataframe(wb_stocks, sales, article_map, stocks_1c, rrc_df, stop_articles)

    output_path = str(Path(cfg.output_dir) / f"Отчёт_дни_остатка_WB_{cfg.store_name}_{cfg.run_date:%Y%m%d}.xlsx")
    save_excel_report(report_df, cfg, storage, output_path)
    log(f"Отчёт сохранён: {output_path}")

    if cfg.send_telegram and should_send_to_telegram(cfg.run_date, cfg.force_send_env):
        critical_df = report_df[(report_df["sales_60d"] > 0) & ((report_df["stock_wb_qty"] == 0) | (report_df["days_wb_raw"] < cfg.critical_threshold_days))]
        delist_count = int((critical_df["status"] == "Delist").sum())
        caption = (
            f"📦 Отчёт по остаткам WB {cfg.store_name}\n"
            f"Дата: {cfg.run_date:%d.%m.%Y}\n"
            f"Товаров в критичном листе: {len(critical_df)}\n"
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
