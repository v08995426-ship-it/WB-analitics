#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import io
import math
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Iterable, Optional

import boto3
import pandas as pd
import requests
from botocore.client import Config as BotoConfig
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter


STORE_NAME = "TOPFACE"
WB_STOCKS_PREFIX = f"Отчёты/Остатки/{STORE_NAME}/Недельные/"
WB_ORDERS_PREFIX = f"Отчёты/Заказы/{STORE_NAME}/Недельные/"
ARTICLE_MAP_KEY = "Отчёты/Остатки/1С/Артикулы 1с.xlsx"
STOCKS_1C_KEY = "Отчёты/Остатки/1С/Остатки 1С.xlsx"
RRC_KEY = f"Отчёты/Финансовые показатели/{STORE_NAME}/РРЦ.xlsx"
OUT_DIR = "output"

HEADER_FILL = PatternFill("solid", fgColor="1F4E78")
WARNING_FILL = PatternFill("solid", fgColor="FFF2CC")
CRITICAL_FILL = PatternFill("solid", fgColor="F4CCCC")
DELIST_FILL = PatternFill("solid", fgColor="D9D2E9")
LIGHT_GREEN_FILL = PatternFill("solid", fgColor="CCFFCC")
BLACK_FILL = PatternFill("solid", fgColor="000000")
BORDER = Border(
    left=Side(style="thin", color="D9D9D9"),
    right=Side(style="thin", color="D9D9D9"),
    top=Side(style="thin", color="D9D9D9"),
    bottom=Side(style="thin", color="D9D9D9"),
)
BASE_FONT = Font(name="Calibri", size=14)
HEADER_FONT = Font(name="Calibri", size=14, bold=True, color="FFFFFF")
TITLE_FONT = Font(name="Calibri", size=16, bold=True, color="FFFFFF")
CENTER_WRAP = Alignment(horizontal="center", vertical="center", wrap_text=True)
LEFT_WRAP = Alignment(horizontal="left", vertical="center", wrap_text=True)


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
    if isinstance(value, int):
        return str(value).strip()
    if isinstance(value, float):
        if float(value).is_integer():
            return str(int(value))
        return str(value).strip().replace(",", ".")
    text = str(value).strip()
    if re.fullmatch(r"\d+\.0+", text):
        text = text.split(".")[0]
    return text


def normalize_key(value: object) -> str:
    return normalize_text(value).upper()


def safe_float(value: object) -> float:
    if value is None or pd.isna(value) or value == "":
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


def calculate_days(stock_qty: float, avg_daily_sales: float) -> Optional[float]:
    if avg_daily_sales <= 0:
        return None if stock_qty > 0 else 0.0
    return stock_qty / avg_daily_sales


def floor_positive_int(value: Optional[float]) -> Optional[int]:
    if value is None or pd.isna(value):
        return None
    return int(math.floor(float(value)))


def natural_sort_key(value: object) -> list[object]:
    text = normalize_text(value)
    return [int(part) if part.isdigit() else part.lower() for part in re.split(r"(\d+)", text)]


def format_rrc_coef(price: object, rrc: object) -> str:
    p = safe_float(price)
    r = safe_float(rrc)
    if r <= 0:
        return ""
    return f"{(p / r):.2f}".replace(".", ",") + "_РРЦ"


def should_send_to_telegram(run_date: datetime, force_send: bool) -> bool:
    if force_send:
        log("Ручной запуск — отчёт будет отправлен в Telegram")
        return True
    if run_date.weekday() in {0, 4}:
        return True
    log("Отправка в Telegram пропущена по расписанию")
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


@dataclass
class AppConfig:
    bucket_name: str
    access_key: str
    secret_key: str
    endpoint_url: str
    region_name: str
    telegram_bot_token: str
    telegram_chat_id: str
    run_date: datetime
    force_send: bool
    store_name: str = STORE_NAME
    stock_prefix: str = WB_STOCKS_PREFIX
    stock_history_prefix: str = WB_STOCKS_PREFIX
    orders_prefix: str = WB_ORDERS_PREFIX
    article_map_key: str = ARTICLE_MAP_KEY
    stocks_1c_key: str = STOCKS_1C_KEY
    rrc_key: str = RRC_KEY
    days_threshold: int = 14
    sales_window_days: int = 7
    sales_window_days_60: int = 60
    dead_stock_threshold: int = 120
    dead_stock_black_threshold: int = 180
    stop_articles: str = ""


class S3Storage:
    def __init__(self, cfg: AppConfig) -> None:
        self.bucket = cfg.bucket_name
        self.client = boto3.client(
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
            kwargs = {"Bucket": self.bucket, "Prefix": prefix, "MaxKeys": 1000}
            if token:
                kwargs["ContinuationToken"] = token
            resp = self.client.list_objects_v2(**kwargs)
            for item in resp.get("Contents", []):
                key = item.get("Key")
                if key and not key.endswith("/"):
                    keys.append(key)
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        return keys

    def read_excel(self, key: str, **kwargs) -> pd.DataFrame:
        body = self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        return pd.read_excel(io.BytesIO(body), **kwargs)


def build_config() -> AppConfig:
    bucket_name = (os.getenv("YC_BUCKET_NAME") or os.getenv("CLOUD_RU_BUCKET") or os.getenv("WB_S3_BUCKET") or "").strip()
    access_key = (os.getenv("YC_ACCESS_KEY_ID") or os.getenv("CLOUD_RU_ACCESS_KEY") or os.getenv("WB_S3_ACCESS_KEY") or "").strip()
    secret_key = (os.getenv("YC_SECRET_ACCESS_KEY") or os.getenv("CLOUD_RU_SECRET_KEY") or os.getenv("WB_S3_SECRET_KEY") or "").strip()
    endpoint_url = (os.getenv("YC_ENDPOINT_URL") or os.getenv("WB_S3_ENDPOINT") or "https://storage.yandexcloud.net").strip()
    region_name = (os.getenv("WB_S3_REGION") or "ru-central1").strip()

    if not bucket_name or not access_key or not secret_key:
        raise ValueError("Не заданы параметры Object Storage. Нужны env: YC_* или CLOUD_RU_* или WB_S3_*.")

    return AppConfig(
        bucket_name=bucket_name,
        access_key=access_key,
        secret_key=secret_key,
        endpoint_url=endpoint_url,
        region_name=region_name,
        telegram_bot_token=(os.getenv("TELEGRAM_BOT_TOKEN") or "").strip(),
        telegram_chat_id=(os.getenv("TELEGRAM_CHAT_ID") or "").strip(),
        run_date=datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
        force_send=env_bool("WB_FORCE_SEND", False),
        stop_articles=os.getenv("WB_STOP_LIST_KEY", ""),
    )


def load_latest_stock_key(storage: S3Storage, cfg: AppConfig) -> str:
    keys = [k for k in storage.list_keys(cfg.stock_prefix) if k.lower().endswith(".xlsx")]
    dated = [(parse_iso_week_from_key(k), k) for k in keys]
    dated = [(w, k) for w, k in dated if w is not None]
    if not dated:
        raise FileNotFoundError("Не найдены weekly-файлы остатков")
    dated.sort(key=lambda x: x[0])
    latest_key = dated[-1][1]
    log(f"Берём остатки WB из файла: {latest_key}")
    return latest_key


def load_order_keys(storage: S3Storage, cfg: AppConfig) -> list[str]:
    keys = [k for k in storage.list_keys(cfg.orders_prefix) if k.lower().endswith(".xlsx")]
    dated = [(parse_iso_week_from_key(k), k) for k in keys]
    dated = [(w, k) for w, k in dated if w is not None]
    dated.sort(key=lambda x: x[0])
    selected = [k for _, k in dated[-10:]]
    log(f"Берём заказы WB из файлов: {selected}")
    return selected


def load_article_map(storage: S3Storage, cfg: AppConfig) -> pd.DataFrame:
    df = storage.read_excel(cfg.article_map_key)
    if df.shape[1] < 3:
        raise ValueError("Файл Артикулы 1с.xlsx должен содержать минимум 3 колонки")

    temp = pd.DataFrame({
        "map_key": df.iloc[:, 0].map(normalize_key),
        "article_1c": df.iloc[:, 2].map(normalize_text),
    })
    temp = temp[(temp["map_key"] != "") & (temp["article_1c"] != "")].drop_duplicates("map_key")
    log(f"Загружено соответствий WB -> 1С: {len(temp)}")
    return temp


def load_1c_stocks(storage: S3Storage, cfg: AppConfig) -> pd.DataFrame:
    df = storage.read_excel(cfg.stocks_1c_key)
    df.columns = [str(c).strip() for c in df.columns]
    article_col = choose_existing_column(df, ["Артикул", "АРТ", "Артикул 1С"], "Артикул 1С")
    mp_col = choose_existing_column(df, ["Остатки МП", "Остатки МП (Липецк), шт"], "Остатки МП")
    result = pd.DataFrame({
        "article_1c": df[article_col].map(normalize_text),
        "stock_company_qty": df[mp_col].map(ceil_int),
    })
    result = result[result["article_1c"] != ""].drop_duplicates("article_1c", keep="first")
    return result


def load_rrc(storage: S3Storage, cfg: AppConfig) -> pd.DataFrame:
    try:
        df = storage.read_excel(cfg.rrc_key)
    except Exception:
        return pd.DataFrame(columns=["article_1c", "rrc"])
    if df.shape[1] < 4:
        return pd.DataFrame(columns=["article_1c", "rrc"])
    temp = pd.DataFrame({
        "article_1c": df.iloc[:, 0].map(normalize_text),
        "rrc": df.iloc[:, 3].map(round_int),
    })
    temp = temp[temp["article_1c"] != ""].drop_duplicates("article_1c", keep="first")
    return temp


def extract_stock_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    nm_col = choose_existing_column(df, ["nmId", "Артикул WB", "Артикул wb"], "идентификатор товара WB")
    qty_col = choose_existing_column(df, ["Доступно для продажи", "Полное количество", "Остатки МП", "Количество", "Доступно", "Остаток", "Остатки"], "остаток WB")

    supplier_col = None
    for candidate in ["supplierArticle", "Артикул продавца", "Артикул поставщика", "Артикул WB"]:
        try:
            supplier_col = choose_existing_column(df, [candidate], "Артикул продавца")
            break
        except Exception:
            continue

    out = pd.DataFrame({
        "nmId": df[nm_col].map(normalize_text),
        "supplierArticle": df[supplier_col].map(normalize_text) if supplier_col else "",
        "stock_wb_qty": df[qty_col].map(ceil_int),
    })
    out = out[out["nmId"] != ""].copy()
    out = out.groupby(["nmId", "supplierArticle"], as_index=False).agg(stock_wb_qty=("stock_wb_qty", "sum"))
    return out


def load_wb_stocks(storage: S3Storage, cfg: AppConfig, stock_key: str) -> pd.DataFrame:
    return extract_stock_snapshot(storage.read_excel(stock_key))


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
        return pd.DataFrame(columns=["nmId", "supplierArticle", "sales_7d", "avg_daily_sales_7d", "sales_60d", "avg_daily_sales_60d", "last_finished_price"])

    date_col = choose_existing_column(orders, ["date", "Дата заказа", "Дата", "lastChangeDate"], "дата заказа")
    nm_col = choose_existing_column(orders, ["nmId", "Артикул WB", "Артикул wb"], "идентификатор товара WB")
    price_col = None
    for candidate in ["finishedPrice", "finishPrice", "Цена покупателя", "Цена"]:
        try:
            price_col = choose_existing_column(orders, [candidate], "finishedPrice")
            break
        except Exception:
            continue

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
    orders = orders[orders[date_col].notna()].copy()
    if cancel_col:
        orders = orders[~orders[cancel_col].fillna(False).astype(bool)].copy()

    orders["nmId"] = orders[nm_col].map(normalize_text)
    orders["supplierArticle"] = orders[art_col].map(normalize_text) if art_col else ""
    orders["qty"] = orders[qty_col].map(safe_float) if qty_col else 1.0
    orders["finished_price"] = orders[price_col].map(safe_float) if price_col else 0.0

    start_7 = cfg.run_date - timedelta(days=cfg.sales_window_days - 1)
    start_60 = cfg.run_date - timedelta(days=cfg.sales_window_days_60 - 1)

    orders_7 = orders[(orders[date_col] >= start_7) & (orders[date_col] <= cfg.run_date)].copy()
    orders_60 = orders[(orders[date_col] >= start_60) & (orders[date_col] <= cfg.run_date)].copy()

    g7 = orders_7.groupby(["nmId", "supplierArticle"], as_index=False).agg(sales_7d=("qty", "sum"))
    g7["avg_daily_sales_7d"] = g7["sales_7d"] / float(cfg.sales_window_days)

    g60 = orders_60.groupby(["nmId", "supplierArticle"], as_index=False).agg(sales_60d=("qty", "sum"))
    g60["avg_daily_sales_60d"] = g60["sales_60d"] / float(cfg.sales_window_days_60)

    latest_day = orders[date_col].max()
    price_last = orders[orders[date_col] == latest_day].groupby(["nmId", "supplierArticle"], as_index=False).agg(last_finished_price=("finished_price", "mean"))

    result = g60.merge(g7, on=["nmId", "supplierArticle"], how="left").merge(price_last, on=["nmId", "supplierArticle"], how="left")
    result["sales_7d"] = result["sales_7d"].fillna(0.0)
    result["avg_daily_sales_7d"] = result["avg_daily_sales_7d"].fillna(0.0)
    result["sales_60d"] = result["sales_60d"].fillna(0.0)
    result["avg_daily_sales_60d"] = result["avg_daily_sales_60d"].fillna(0.0)
    result["last_finished_price"] = result["last_finished_price"].fillna(0.0)
    return result


def load_zero_stock_days_current_month(storage: S3Storage, cfg: AppConfig, current_zero_nmids: set[str]) -> pd.DataFrame:
    if not current_zero_nmids:
        return pd.DataFrame(columns=["nmId", "zero_stock_days_month"])

    month_start = date(cfg.run_date.year, cfg.run_date.month, 1)
    all_keys = [k for k in storage.list_keys(cfg.stock_history_prefix) if k.lower().endswith(".xlsx")]
    if not all_keys:
        return pd.DataFrame(columns=["nmId", "zero_stock_days_month"])

    zero_days: dict[str, set[date]] = {nmid: set() for nmid in current_zero_nmids}

    for key in sorted(all_keys, key=lambda x: (parse_iso_week_from_key(x) or (0, 0), x)):
        try:
            raw = storage.read_excel(key)
        except Exception:
            continue
        try:
            snapshot = extract_stock_snapshot(raw)
        except Exception:
            continue

        dt_col = None
        try:
            dt_col = choose_existing_column(raw, ["Дата сбора", "Дата запроса"], "дата среза")
        except Exception:
            dt_col = None
        if not dt_col:
            continue

        snapshot_dates = pd.to_datetime(raw[dt_col], errors="coerce").dt.date
        if snapshot_dates.isna().all():
            continue
        snap_date = max([d for d in snapshot_dates.dropna().tolist()])
        if snap_date < month_start or snap_date > cfg.run_date.date():
            continue

        subset = snapshot[snapshot["nmId"].isin(current_zero_nmids)].copy()
        if subset.empty:
            continue
        subset = subset.groupby("nmId", as_index=False).agg(stock_wb_qty=("stock_wb_qty", "sum"))
        subset = subset[subset["stock_wb_qty"] <= 0]
        for nmid in subset["nmId"].tolist():
            zero_days.setdefault(nmid, set()).add(snap_date)

    result = pd.DataFrame({
        "nmId": list(zero_days.keys()),
        "zero_stock_days_month": [len(v) for v in zero_days.values()],
    })
    return result


def resolve_article_1c(row: pd.Series, article_map_dict: dict[str, str]) -> str:
    for key in [normalize_key(row.get("nmId")), normalize_key(row.get("supplierArticle"))]:
        if key and key in article_map_dict:
            return article_map_dict[key]
    return ""


def choose_avg_daily_sales(row: pd.Series) -> float:
    avg7 = safe_float(row.get("avg_daily_sales_7d"))
    avg60 = safe_float(row.get("avg_daily_sales_60d"))
    stock_wb = safe_float(row.get("stock_wb_qty"))
    if stock_wb <= 0 or avg7 <= 0:
        return avg60
    return avg7


def contains_pt104(article_wb: object, article_1c: object) -> bool:
    return normalize_text(article_wb).startswith("PT104") or normalize_text(article_1c).startswith("PT104")


def build_report_dataframe(wb_stocks: pd.DataFrame, sales: pd.DataFrame, article_map: pd.DataFrame, stocks_1c: pd.DataFrame, stop_articles: set[str], zero_stock_days: pd.DataFrame, rrc_df: pd.DataFrame, cfg: AppConfig) -> pd.DataFrame:
    df = wb_stocks.merge(sales, on=["nmId", "supplierArticle"], how="left")
    for col in ["sales_7d", "avg_daily_sales_7d", "sales_60d", "avg_daily_sales_60d", "last_finished_price"]:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = df[col].fillna(0.0)

    article_map_dict = dict(zip(article_map["map_key"], article_map["article_1c"]))
    df["article_1c"] = df.apply(lambda r: resolve_article_1c(r, article_map_dict), axis=1)
    df = df[df["article_1c"] != ""].copy()

    df = df.merge(stocks_1c, on="article_1c", how="left")
    df["stock_company_qty"] = df["stock_company_qty"].fillna(0).astype(int)

    df = df.merge(zero_stock_days, on="nmId", how="left")
    df["zero_stock_days_month"] = df["zero_stock_days_month"].fillna(0).astype(int)

    df["avg_daily_sales_used"] = df.apply(choose_avg_daily_sales, axis=1)
    df["days_wb_float"] = df.apply(lambda x: calculate_days(safe_float(x["stock_wb_qty"]), safe_float(x["avg_daily_sales_used"])), axis=1)
    df["days_company_float"] = df.apply(lambda x: calculate_days(safe_float(x["stock_company_qty"]), safe_float(x["avg_daily_sales_used"])), axis=1)
    df["days_total_float"] = df.apply(lambda x: calculate_days(safe_float(x["stock_wb_qty"]) + safe_float(x["stock_company_qty"]), safe_float(x["avg_daily_sales_used"])), axis=1)

    df["days_wb"] = df["days_wb_float"].map(floor_positive_int)
    df["days_company"] = df["days_company_float"].map(floor_positive_int)
    df["days_total"] = df["days_total_float"].map(floor_positive_int)

    df["status"] = df["article_1c"].map(lambda x: "Delist" if normalize_key(x) in stop_articles else "")
    df["sales_7d"] = df["sales_7d"].map(round_int).astype(int)
    df["sales_60d"] = df["sales_60d"].map(round_int).astype(int)
    df["avg_daily_sales_7d"] = df["avg_daily_sales_7d"].map(lambda x: int(math.ceil(x)) if safe_float(x) > 0 else 0)
    df["avg_daily_sales_60d"] = df["avg_daily_sales_60d"].map(lambda x: int(math.ceil(x)) if safe_float(x) > 0 else 0)
    df["avg_daily_sales_used"] = df["avg_daily_sales_used"].map(lambda x: int(math.ceil(x)) if safe_float(x) > 0 else 0)
    df["stock_wb_qty"] = df["stock_wb_qty"].fillna(0).astype(int)
    df["last_finished_price"] = df["last_finished_price"].map(round_int)

    df = df[~df.apply(lambda r: contains_pt104(r["supplierArticle"], r["article_1c"]), axis=1)].copy()

    df = df.merge(rrc_df, on="article_1c", how="left")
    df["rrc"] = df["rrc"].fillna(0).map(round_int)
    df["rrc_coef"] = df.apply(lambda r: format_rrc_coef(r["last_finished_price"], r["rrc"]), axis=1)

    df["is_dead_stock"] = ((df["stock_wb_qty"] + df["stock_company_qty"]) > 0) & (((df["avg_daily_sales_used"] <= 0)) | (df["days_total_float"].fillna(float("inf")) > cfg.dead_stock_threshold))

    # Финальная защита от дублей по 1С-артикулу
    df = df.groupby("article_1c", as_index=False).agg(
        supplierArticle=("supplierArticle", "first"),
        stock_wb_qty=("stock_wb_qty", "sum"),
        stock_company_qty=("stock_company_qty", "max"),
        sales_7d=("sales_7d", "sum"),
        sales_60d=("sales_60d", "sum"),
        avg_daily_sales_7d=("avg_daily_sales_7d", "max"),
        avg_daily_sales_60d=("avg_daily_sales_60d", "max"),
        avg_daily_sales_used=("avg_daily_sales_used", "max"),
        days_wb_float=("days_wb_float", "max"),
        days_company_float=("days_company_float", "max"),
        days_total_float=("days_total_float", "max"),
        days_wb=("days_wb", "max"),
        days_company=("days_company", "max"),
        days_total=("days_total", "max"),
        zero_stock_days_month=("zero_stock_days_month", "max"),
        last_finished_price=("last_finished_price", "max"),
        rrc=("rrc", "max"),
        rrc_coef=("rrc_coef", "first"),
        status=("status", "first"),
        is_dead_stock=("is_dead_stock", "max"),
    )

    df = df.sort_values(by="article_1c", key=lambda s: s.map(natural_sort_key)).reset_index(drop=True)
    return df


def style_title(ws, cell_range: str, text: str) -> None:
    first_cell = cell_range.split(":")[0]
    ws[first_cell] = text
    ws.merge_cells(cell_range)
    cell = ws[first_cell]
    cell.fill = HEADER_FILL
    cell.font = TITLE_FONT
    cell.alignment = CENTER_WRAP
    cell.border = BORDER


def style_header(ws, row_idx: int) -> None:
    for cell in ws[row_idx]:
        if cell.value is None:
            continue
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = CENTER_WRAP
        cell.border = BORDER


def apply_row_style(ws, row_idx: int, fill: Optional[PatternFill] = None) -> None:
    for cell in ws[row_idx]:
        cell.font = BASE_FONT
        cell.alignment = CENTER_WRAP
        cell.border = BORDER
        if fill is not None:
            cell.fill = fill
    ws.cell(row_idx, 1).alignment = LEFT_WRAP


def style_conditional_row(ws, row_idx: int, days_wb_col: int, status_col: int) -> None:
    status_value = normalize_text(ws.cell(row_idx, status_col).value)
    days_value = ws.cell(row_idx, days_wb_col).value
    fill = None
    try:
        numeric_days = int(days_value) if days_value is not None and days_value != "" else None
    except Exception:
        numeric_days = None
    if status_value == "Delist":
        fill = DELIST_FILL
    if numeric_days is not None and numeric_days < 7:
        fill = CRITICAL_FILL
    elif numeric_days is not None and numeric_days < 14 and fill is None:
        fill = WARNING_FILL
    apply_row_style(ws, row_idx, fill=fill)


def autosize_columns(ws, min_width: int = 12, max_width: int = 42) -> None:
    for col_idx in range(1, ws.max_column + 1):
        letter = get_column_letter(col_idx)
        max_len = 0
        for cell in ws[letter]:
            value = "" if cell.value is None else str(cell.value)
            line_len = max((len(line) for line in value.split("\n")), default=0)
            max_len = max(max_len, line_len)
        ws.column_dimensions[letter].width = min(max(max_len + 2, min_width), max_width)


def set_sheet_layout(ws, title_row: int, header_row: int) -> None:
    ws.row_dimensions[title_row].height = 28
    ws.row_dimensions[header_row].height = 42
    ws.freeze_panes = f"A{header_row + 1}"
    ws.sheet_view.showGridLines = False
    ws.auto_filter.ref = f"A{header_row}:{get_column_letter(ws.max_column)}{max(ws.max_row, header_row)}"
    autosize_columns(ws)


def save_excel_report(df: pd.DataFrame, cfg: AppConfig, output_path: str) -> None:
    wb = Workbook()
    ws_short = wb.active
    ws_short.title = "Критично <14 дней"
    ws_calc = wb.create_sheet("Расчёт")
    ws_dead = wb.create_sheet("Dead_Stock")

    report_date_text = cfg.run_date.strftime("%d.%m.%Y")

    short_df = df[(df["sales_60d"] > 0) & (((df["stock_wb_qty"] <= 0) | (df["days_wb"].fillna(999999) < cfg.days_threshold)))].copy()
    dead_df = df[df["is_dead_stock"]].copy().sort_values(["days_total", "article_1c"], ascending=[False, True])

    style_title(ws_short, "A1:H1", f"Контроль остатка WB — товары менее {cfg.days_threshold} дней на {report_date_text}")
    short_headers = [
        "Артикул 1С",
        "WB хватит, дней",
        "Липецк хватит, дней",
        "Остаток WB, шт",
        "Остатки МП (Липецк), шт",
        "Продажи 60 дней, шт",
        "Дней без остатка WB в текущем месяце",
        "Статус",
    ]
    ws_short.append(short_headers)
    style_header(ws_short, 2)
    for _, row in short_df.iterrows():
        ws_short.append([
            row["article_1c"],
            row["days_wb"],
            row["days_company"],
            row["stock_wb_qty"],
            row["stock_company_qty"],
            row["sales_60d"],
            row["zero_stock_days_month"] if row["stock_wb_qty"] == 0 else 0,
            row["status"],
        ])
    for r in range(3, ws_short.max_row + 1):
        style_conditional_row(ws_short, r, 2, 8)
    set_sheet_layout(ws_short, 1, 2)

    style_title(ws_calc, "A1:O1", f"Расчёт дней до конца остатка WB — {cfg.store_name} — {report_date_text}")
    calc_headers = [
        "Артикул 1С",
        "Артикул WB",
        "Остаток WB, шт",
        "Остатки МП (Липецк), шт",
        "Продажи 7 дней, шт",
        "Продажи 60 дней, шт",
        "Среднесуточные продажи 7д",
        "Среднесуточные продажи 60д",
        "Расчётный спрос в день, шт",
        "WB хватит, дней",
        "Липецк хватит, дней",
        "WB + Липецк, дней",
        "Дней без остатка WB в текущем месяце",
        "Цена покупателя",
        "Статус",
    ]
    ws_calc.append(calc_headers)
    style_header(ws_calc, 2)
    for _, row in df.iterrows():
        ws_calc.append([
            row["article_1c"],
            row["supplierArticle"],
            row["stock_wb_qty"],
            row["stock_company_qty"],
            row["sales_7d"],
            row["sales_60d"],
            row["avg_daily_sales_7d"],
            row["avg_daily_sales_60d"],
            row["avg_daily_sales_used"],
            row["days_wb"],
            row["days_company"],
            row["days_total"],
            row["zero_stock_days_month"] if row["stock_wb_qty"] == 0 else 0,
            row["last_finished_price"],
            row["status"],
        ])
    for r in range(3, ws_calc.max_row + 1):
        style_conditional_row(ws_calc, r, 10, 15)
    set_sheet_layout(ws_calc, 1, 2)

    style_title(ws_dead, "A1:K1", f"Dead Stock — {cfg.store_name} — {report_date_text}")
    dead_headers = [
        "Артикул 1С",
        "WB хватит, дней",
        "Липецк хватит, дней",
        "WB + Липецк, дней",
        "Остаток WB, шт",
        "Остатки МП (Липецк), шт",
        "Продажи 60 дней, шт",
        "Цена покупателя",
        "РРЦ",
        "Коэффициент",
        "Статус",
    ]
    ws_dead.append(dead_headers)
    style_header(ws_dead, 2)
    for _, row in dead_df.iterrows():
        ws_dead.append([
            row["article_1c"],
            row["days_wb"],
            row["days_company"],
            row["days_total"],
            row["stock_wb_qty"],
            row["stock_company_qty"],
            row["sales_60d"],
            row["last_finished_price"],
            row["rrc"],
            row["rrc_coef"],
            row["status"],
        ])
    for r in range(3, ws_dead.max_row + 1):
        apply_row_style(ws_dead, r)
        for c in (8, 9, 10):
            ws_dead.cell(r, c).fill = LIGHT_GREEN_FILL
        total_cell = ws_dead.cell(r, 4)
        try:
            if int(total_cell.value) > cfg.dead_stock_black_threshold:
                total_cell.fill = BLACK_FILL
                total_cell.font = BASE_FONT
        except Exception:
            pass
    set_sheet_layout(ws_dead, 1, 2)

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    wb.save(output_path)


def send_telegram_document(cfg: AppConfig, file_path: str, caption: str) -> None:
    if not cfg.telegram_bot_token or not cfg.telegram_chat_id:
        log("TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID не заданы — отправку пропускаем")
        return
    url = f"https://api.telegram.org/bot{cfg.telegram_bot_token}/sendDocument"
    with open(file_path, "rb") as f:
        response = requests.post(url, data={"chat_id": cfg.telegram_chat_id, "caption": caption}, files={"document": f}, timeout=120)
    response.raise_for_status()
    log("Отчёт отправлен в Telegram")


def run() -> str:
    cfg = build_config()
    storage = S3Storage(cfg)
    stop_articles = parse_stop_articles(cfg.stop_articles)
    log(f"Delist-артикулов из env: {len(stop_articles)}")

    stock_key = load_latest_stock_key(storage, cfg)
    order_keys = load_order_keys(storage, cfg)

    wb_stocks = load_wb_stocks(storage, cfg, stock_key)
    sales = load_orders(storage, cfg, order_keys)
    article_map = load_article_map(storage, cfg)
    stocks_1c = load_1c_stocks(storage, cfg)
    rrc_df = load_rrc(storage, cfg)

    current_zero_nmids = set(wb_stocks.loc[wb_stocks["stock_wb_qty"] <= 0, "nmId"].tolist())
    zero_stock_days = load_zero_stock_days_current_month(storage, cfg, current_zero_nmids)

    report_df = build_report_dataframe(wb_stocks, sales, article_map, stocks_1c, stop_articles, zero_stock_days, rrc_df, cfg)
    log(f"Строк в финальном расчёте: {len(report_df)}")

    output_path = str(Path(OUT_DIR) / f"Отчёт_дни_остатка_WB_{cfg.store_name}_{cfg.run_date.strftime('%Y%m%d')}.xlsx")
    save_excel_report(report_df, cfg, output_path)
    log(f"Отчёт сохранён: {output_path}")
    log(f"Источник остатков: {stock_key}")
    log(f"Источники заказов: {', '.join(order_keys)}")

    if should_send_to_telegram(cfg.run_date, cfg.force_send):
        caption = (
            f"📦 Отчёт по остаткам WB {cfg.store_name}\n"
            f"Критично <14 дней: {len(report_df[(report_df['sales_60d'] > 0) & ((report_df['stock_wb_qty'] <= 0) | (report_df['days_wb'].fillna(999999) < cfg.days_threshold))])}\n"
            f"Dead_Stock: {len(report_df[report_df['is_dead_stock']])}"
        )
        send_telegram_document(cfg, output_path, caption)

    return output_path


if __name__ == "__main__":
    run()
