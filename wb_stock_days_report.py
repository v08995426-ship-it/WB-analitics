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
from openpyxl import load_workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter


STORE_NAME = os.getenv("WB_STORE", "TOPFACE").strip()
WB_STOCKS_PREFIX = os.getenv("WB_STOCKS_PREFIX", f"Отчёты/Остатки/{STORE_NAME}/Недельные/")
WB_ORDERS_PREFIX = os.getenv("WB_ORDERS_PREFIX", f"Отчёты/Заказы/{STORE_NAME}/Недельные/")
ARTICLE_MAP_KEY = os.getenv("WB_ARTICLE_MAP_KEY", "Отчёты/Остатки/1С/Артикулы 1с.xlsx")
STOCKS_1C_KEY = os.getenv("WB_STOCKS_1C_KEY", "Отчёты/Остатки/1С/Остатки 1С.xlsx")
RRC_KEY = os.getenv("WB_RRC_KEY", f"Отчёты/Финансовые показатели/{STORE_NAME}/РРЦ.xlsx")
INBOUND_PREFIX = os.getenv("WB_INBOUND_PREFIX", "Отчёты/Остатки/1С/")
OUTPUT_DIR = os.getenv("WB_OUTPUT_DIR", "output")

SHEET_CRITICAL = "Критично <14 дней"
SHEET_CALC = "Расчёт"
SHEET_DEAD = "Dead_Stock"
SHEET_MONITOR = "Мониторинг остатков"

FONT_NAME = "Calibri"
FONT_SIZE = 14
HEADER_FILL = PatternFill("solid", fgColor="1F4E78")
GREEN_FILL = PatternFill("solid", fgColor="CCFFCC")
BLACK_FILL = PatternFill("solid", fgColor="000000")
ORANGE_FILL = PatternFill("solid", fgColor="FCE4D6")
BLUE_FILL = PatternFill("solid", fgColor="DDEBF7")
BORDER = Border(
    left=Side(style="thin", color="D9D9D9"),
    right=Side(style="thin", color="D9D9D9"),
    top=Side(style="thin", color="D9D9D9"),
    bottom=Side(style="thin", color="D9D9D9"),
)
ALIGN_CENTER = Alignment(horizontal="center", vertical="center", wrap_text=True)
ALIGN_LEFT = Alignment(horizontal="left", vertical="center", wrap_text=True)


@dataclass
class AppConfig:
    bucket_name: str
    access_key: str
    secret_key: str
    endpoint_url: str
    region_name: str
    run_date: date
    critical_threshold_days: int
    dead_stock_threshold_days: int
    black_cell_threshold_days: int
    sales_window_days: int
    activity_window_days: int
    telegram_bot_token: str
    telegram_chat_id: str
    stop_articles_raw: str
    force_send: bool


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def get_config() -> AppConfig:
    bucket_name = (os.getenv("WB_S3_BUCKET") or os.getenv("YC_BUCKET_NAME") or os.getenv("CLOUD_RU_BUCKET") or "").strip()
    access_key = (os.getenv("WB_S3_ACCESS_KEY") or os.getenv("YC_ACCESS_KEY_ID") or os.getenv("CLOUD_RU_ACCESS_KEY") or "").strip()
    secret_key = (os.getenv("WB_S3_SECRET_KEY") or os.getenv("YC_SECRET_ACCESS_KEY") or os.getenv("CLOUD_RU_SECRET_KEY") or "").strip()
    endpoint_url = (os.getenv("WB_S3_ENDPOINT") or os.getenv("YC_ENDPOINT") or os.getenv("CLOUD_RU_ENDPOINT") or "https://storage.yandexcloud.net").strip()
    region_name = (os.getenv("WB_S3_REGION") or os.getenv("YC_REGION") or "ru-central1").strip()

    if not bucket_name or not access_key or not secret_key:
        raise ValueError("Не заданы параметры Object Storage.")

    run_date = datetime.strptime(os.getenv("WB_RUN_DATE", datetime.now().strftime("%Y-%m-%d")), "%Y-%m-%d").date()
    return AppConfig(
        bucket_name=bucket_name,
        access_key=access_key,
        secret_key=secret_key,
        endpoint_url=endpoint_url,
        region_name=region_name,
        run_date=run_date,
        critical_threshold_days=int(os.getenv("WB_LOW_STOCK_DAYS_THRESHOLD", "14")),
        dead_stock_threshold_days=int(os.getenv("WB_DEAD_STOCK_DAYS_THRESHOLD", "120")),
        black_cell_threshold_days=int(os.getenv("WB_BLACK_CELL_DAYS_THRESHOLD", "180")),
        sales_window_days=int(os.getenv("WB_SALES_WINDOW_DAYS", "7")),
        activity_window_days=int(os.getenv("WB_ACTIVITY_WINDOW_DAYS", "60")),
        telegram_bot_token=(os.getenv("TELEGRAM_BOT_TOKEN") or "").strip(),
        telegram_chat_id=(os.getenv("TELEGRAM_CHAT_ID") or "").strip(),
        stop_articles_raw=os.getenv("WB_STOP_LIST_KEY", ""),
        force_send=env_bool("WB_FORCE_SEND", False),
    )


class S3Storage:
    def __init__(self, cfg: AppConfig):
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
            keys.extend([x["Key"] for x in resp.get("Contents", [])])
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        return keys

    def read_bytes(self, key: str) -> bytes:
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        return obj["Body"].read()

    def read_excel(self, key: str, sheet_name=0) -> pd.DataFrame:
        return pd.read_excel(io.BytesIO(self.read_bytes(key)), sheet_name=sheet_name)


def normalize_text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    if isinstance(value, float) and float(value).is_integer():
        return str(int(value)).strip()
    text = str(value).strip()
    if re.fullmatch(r"\d+\.0+", text):
        text = text.split(".")[0]
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


def round_int(value: object) -> int:
    return int(round(safe_float(value)))


def choose_existing_column(df: pd.DataFrame, candidates: Iterable[str], label: str) -> str:
    mapping = {str(col).strip().lower(): col for col in df.columns}
    for candidate in candidates:
        real = mapping.get(candidate.strip().lower())
        if real is not None:
            return real
    raise KeyError(f"Не найдена колонка для '{label}'. Доступные колонки: {list(df.columns)}")


def try_choose_column(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    mapping = {str(col).strip().lower(): col for col in df.columns}
    for candidate in candidates:
        real = mapping.get(candidate.strip().lower())
        if real is not None:
            return real
    return None


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


def latest_weekly_key(keys: list[str]) -> str:
    dated = []
    for key in keys:
        parsed = parse_iso_week_from_key(key)
        week_start = iso_week_start(*parsed) if parsed else datetime.min
        dated.append((week_start, key))
    if not dated:
        raise FileNotFoundError("Не найдены weekly xlsx файлы")
    return max(dated, key=lambda x: x[0])[1]


def natural_sort_key(text: str):
    parts = re.split(r"(\d+)", normalize_text(text).upper())
    out = []
    for part in parts:
        if part.isdigit():
            out.append((0, int(part)))
        else:
            out.append((1, part))
    return out


def should_send_to_telegram(cfg: AppConfig) -> bool:
    if cfg.force_send:
        return True
    return cfg.run_date.weekday() in {0, 4}


def calculate_days(stock_qty: float, daily_sales: float) -> float:
    if daily_sales <= 0:
        return 9999.0 if stock_qty > 0 else 0.0
    return stock_qty / daily_sales


# ---------------- loaders ----------------

def find_latest_stock_file(storage: S3Storage) -> str:
    keys = [k for k in storage.list_keys(WB_STOCKS_PREFIX) if k.lower().endswith(".xlsx")]
    latest = latest_weekly_key(keys)
    log(f"Берём остатки WB из файла: {latest}")
    return latest


def find_order_files(storage: S3Storage, cfg: AppConfig) -> list[str]:
    keys = [k for k in storage.list_keys(WB_ORDERS_PREFIX) if k.lower().endswith(".xlsx")]
    if not keys:
        raise FileNotFoundError("Не найдены weekly-файлы заказов")
    selected = sorted(keys, key=lambda k: iso_week_start(*parse_iso_week_from_key(k)) if parse_iso_week_from_key(k) else datetime.min)[-10:]
    log(f"Берём заказы WB из файлов: {selected}")
    return selected


def load_article_map(storage: S3Storage) -> pd.DataFrame:
    df = storage.read_excel(ARTICLE_MAP_KEY)
    mapped = pd.DataFrame({
        "Артикул WB": df.iloc[:, 0].map(normalize_key),
        "Артикул 1С": df.iloc[:, 2].map(normalize_text),
    })
    mapped = mapped[(mapped["Артикул WB"] != "") & (mapped["Артикул 1С"] != "")].drop_duplicates("Артикул WB")
    log(f"Загружено соответствий WB -> 1С: {len(mapped)}")
    return mapped


def load_1c_stocks(storage: S3Storage) -> pd.DataFrame:
    df = storage.read_excel(STOCKS_1C_KEY)
    df.columns = [str(c).strip() for c in df.columns]
    article_col = choose_existing_column(df, ["Артикул", "АРТ", "Артикул 1С"], "Артикул 1С")
    mp_col = choose_existing_column(df, ["Остатки МП", "Остатки МП (Липецк), шт"], "Остатки МП")
    result = pd.DataFrame({
        "Артикул 1С": df[article_col].map(normalize_text),
        "Остатки МП (Липецк), шт": df[mp_col].map(ceil_int),
    })
    return result[result["Артикул 1С"] != ""].drop_duplicates("Артикул 1С", keep="first")


def load_rrc(storage: S3Storage) -> pd.DataFrame:
    df = storage.read_excel(RRC_KEY)
    out = pd.DataFrame({
        "Артикул 1С": df.iloc[:, 0].map(normalize_text),
        "РРЦ": df.iloc[:, 3].map(round_int),
    })
    return out[(out["Артикул 1С"] != "")].drop_duplicates("Артикул 1С", keep="first")


def load_abc(storage: S3Storage) -> pd.DataFrame:
    keys = [k for k in storage.list_keys("") if k.lower().endswith(".xlsx") and "abc_report_goods" in os.path.basename(k).lower()]
    if not keys:
        log("ABC-отчёт не найден — Менеджер может быть пустым")
        return pd.DataFrame(columns=["Артикул WB", "Артикул продавца", "Менеджер"])
    key = sorted(keys)[-1]
    log(f"Берём ABC-отчёт: {key}")
    df = storage.read_excel(key)
    wb_col = choose_existing_column(df, ["Артикул WB"], "Артикул WB в ABC")
    seller_col = choose_existing_column(df, ["Артикул продавца"], "Артикул продавца в ABC")
    mgr_col = choose_existing_column(df, ["Ваша категория"], "Ваша категория")
    out = pd.DataFrame({
        "Артикул WB": df[wb_col].map(normalize_key),
        "Артикул продавца": df[seller_col].map(normalize_text),
        "Менеджер": df[mgr_col].map(normalize_text),
    })
    out = out[out["Менеджер"] != ""].drop_duplicates(["Артикул WB", "Артикул продавца"], keep="first")
    return out


def load_inbound(storage: S3Storage, run_date: date) -> pd.DataFrame:
    keys = [k for k in storage.list_keys(INBOUND_PREFIX) if k.lower().endswith(".xlsx") and "в пути" in os.path.basename(k).lower()]
    frames = []
    for key in keys:
        fname = os.path.basename(key)
        m = re.search(r"(\d{2})-(\d{2})-(\d{2})", fname)
        if not m:
            continue
        eta = datetime.strptime(m.group(0), "%d-%m-%y").date() + timedelta(days=14)
        df = storage.read_excel(key)
        if "CODES" not in df.columns:
            continue
        qty_col = try_choose_column(df, ["Заказ МП", "заказ мп", "Unnamed: 6"])
        if not qty_col:
            continue
        tmp = pd.DataFrame({
            "Артикул 1С": df["CODES"].map(normalize_text),
            "Товары в пути, шт": df[qty_col].map(round_int),
        })
        tmp = tmp[(tmp["Артикул 1С"] != "") & (tmp["Товары в пути, шт"] > 0)]
        if tmp.empty:
            continue
        tmp["Дата поступления"] = eta
        frames.append(tmp)
    if not frames:
        return pd.DataFrame(columns=["Артикул 1С", "Товары в пути, шт", "Дата поступления", "Дней до поступления"])
    inbound = pd.concat(frames, ignore_index=True)
    qty = inbound.groupby("Артикул 1С", as_index=False)["Товары в пути, шт"].sum()
    eta = inbound.groupby("Артикул 1С", as_index=False)["Дата поступления"].min()
    out = qty.merge(eta, on="Артикул 1С", how="left")
    out["Дней до поступления"] = out["Дата поступления"].map(lambda x: max((x - run_date).days, 0) if pd.notna(x) else None)
    return out


def load_wb_stocks(storage: S3Storage, stock_key: str) -> pd.DataFrame:
    df = storage.read_excel(stock_key)
    df.columns = [str(c).strip() for c in df.columns]
    date_col = choose_existing_column(df, ["Дата сбора", "Дата запроса"], "дата среза остатков")
    snapshot = pd.to_datetime(df[date_col], errors="coerce")
    latest_snapshot = snapshot.max()
    df = df[snapshot == latest_snapshot].copy()

    wb_col = choose_existing_column(df, ["Артикул WB", "nmId", "Артикул wb"], "Артикул WB")
    seller_col = choose_existing_column(df, ["Артикул продавца", "supplierArticle"], "Артикул продавца")
    stock_col = choose_existing_column(df, ["Доступно для продажи", "Полное количество", "Количество", "Остаток", "Остатки"], "остаток WB")

    out = pd.DataFrame({
        "Артикул WB": df[wb_col].map(normalize_key),
        "Артикул продавца": df[seller_col].map(normalize_text),
        "Остаток WB, шт": df[stock_col].map(round_int),
    })
    out = out[(out["Артикул WB"] != "") | (out["Артикул продавца"] != "")].copy()
    out = out.groupby(["Артикул WB", "Артикул продавца"], as_index=False).agg({"Остаток WB, шт": "sum"})
    return out


def load_orders(storage: S3Storage, cfg: AppConfig, order_keys: list[str]) -> pd.DataFrame:
    frames = []
    for key in order_keys:
        try:
            df = storage.read_excel(key, sheet_name="Заказы")
        except Exception:
            df = storage.read_excel(key)
        df.columns = [str(c).strip() for c in df.columns]
        frames.append(df)
    if not frames:
        return pd.DataFrame(columns=["Артикул WB", "Артикул продавца", "Продажи 7 дней, шт", "Продажи 60 дней, шт", "Среднесуточные продажи 7д", "Среднесуточные продажи 60д", "Цена покупателя"])

    orders = pd.concat(frames, ignore_index=True)
    if orders.empty:
        return pd.DataFrame(columns=["Артикул WB", "Артикул продавца", "Продажи 7 дней, шт", "Продажи 60 дней, шт", "Среднесуточные продажи 7д", "Среднесуточные продажи 60д", "Цена покупателя"])

    date_col = choose_existing_column(orders, ["date", "Дата заказа", "Дата", "lastChangeDate", "Дата продажи"], "дата заказа")
    wb_col = choose_existing_column(orders, ["nmId", "Артикул WB", "Артикул wb"], "Артикул WB")
    seller_col = try_choose_column(orders, ["supplierArticle", "Артикул продавца", "Артикул поставщика"])
    qty_col = try_choose_column(orders, ["quantity", "qty", "Количество", "Кол-во", "Количество, шт"])
    cancel_col = try_choose_column(orders, ["isCancel", "cancel", "Отмена", "is_cancel"])
    price_col = try_choose_column(orders, ["finishedPrice", "finishedprice", "Цена покупателя", "priceWithDisc"]) 

    orders = orders.copy()
    orders[date_col] = pd.to_datetime(orders[date_col], errors="coerce").dt.date
    orders = orders[orders[date_col].notna()].copy()
    if cancel_col:
        orders = orders[~orders[cancel_col].fillna(False).astype(bool)].copy()

    orders["Артикул WB"] = orders[wb_col].map(normalize_key)
    orders["Артикул продавца"] = orders[seller_col].map(normalize_text) if seller_col else ""
    orders["qty"] = orders[qty_col].map(safe_float) if qty_col else 1.0
    orders["finishedPrice"] = orders[price_col].map(safe_float) if price_col else 0.0

    start_60 = cfg.run_date - timedelta(days=cfg.activity_window_days - 1)
    start_7 = cfg.run_date - timedelta(days=cfg.sales_window_days - 1)

    ord60 = orders[(orders[date_col] >= start_60) & (orders[date_col] <= cfg.run_date)].copy()
    ord7 = orders[(orders[date_col] >= start_7) & (orders[date_col] <= cfg.run_date)].copy()

    key_cols = ["Артикул WB", "Артикул продавца"]
    s60 = ord60.groupby(key_cols, as_index=False).agg(**{"Продажи 60 дней, шт": ("qty", "sum")})
    s7 = ord7.groupby(key_cols, as_index=False).agg(**{"Продажи 7 дней, шт": ("qty", "sum")})

    sales = s60.merge(s7, on=key_cols, how="outer").fillna(0.0)
    sales["Среднесуточные продажи 7д"] = sales["Продажи 7 дней, шт"] / float(cfg.sales_window_days)
    sales["Среднесуточные продажи 60д"] = sales["Продажи 60 дней, шт"] / float(cfg.activity_window_days)

    if not ord60.empty and price_col:
        max_date = max(ord60[date_col])
        price = ord60[ord60[date_col] == max_date].groupby(key_cols, as_index=False).agg(**{"Цена покупателя": ("finishedPrice", "mean")})
        price["Цена покупателя"] = price["Цена покупателя"].map(round_int)
        sales = sales.merge(price, on=key_cols, how="left")
    else:
        sales["Цена покупателя"] = 0
    sales["Цена покупателя"] = sales["Цена покупателя"].fillna(0).map(round_int)
    return sales


def count_zero_like_days(storage: S3Storage, cfg: AppConfig, current_zero_wb: set[str], avg7_map: dict[str, float]) -> pd.DataFrame:
    if not current_zero_wb:
        return pd.DataFrame(columns=["Артикул WB", "Дней без остатка WB в текущем месяце"])
    keys = [k for k in storage.list_keys(WB_STOCKS_PREFIX) if k.lower().endswith(".xlsx")]
    month_start = cfg.run_date.replace(day=1)
    rows = []
    for key in keys:
        df = storage.read_excel(key)
        df.columns = [str(c).strip() for c in df.columns]
        try:
            wb_col = choose_existing_column(df, ["Артикул WB", "nmId"], "Артикул WB")
            stock_col = choose_existing_column(df, ["Доступно для продажи", "Полное количество"], "остаток WB")
            date_col = choose_existing_column(df, ["Дата сбора", "Дата запроса"], "дата среза")
        except Exception:
            continue
        tmp = pd.DataFrame({
            "Артикул WB": df[wb_col].map(normalize_key),
            "stock": df[stock_col].map(safe_float),
            "dt": pd.to_datetime(df[date_col], errors="coerce").dt.date,
        })
        tmp = tmp[tmp["Артикул WB"].isin(current_zero_wb)]
        tmp = tmp[tmp["dt"].notna() & (tmp["dt"] >= month_start)]
        if tmp.empty:
            continue
        tmp = tmp.groupby(["Артикул WB", "dt"], as_index=False).agg(stock=("stock", "sum"))
        rows.append(tmp)
    if not rows:
        return pd.DataFrame(columns=["Артикул WB", "Дней без остатка WB в текущем месяце"])
    month_df = pd.concat(rows, ignore_index=True)
    def is_zero_like(row):
        threshold = float(avg7_map.get(row["Артикул WB"], 0.0)) * 0.5
        return row["stock"] <= threshold
    month_df["zero_like"] = month_df.apply(is_zero_like, axis=1)
    out = month_df.groupby("Артикул WB", as_index=False).agg(**{"Дней без остатка WB в текущем месяце": ("zero_like", "sum")})
    out["Дней без остатка WB в текущем месяце"] = out["Дней без остатка WB в текущем месяце"].astype(int)
    return out


# ---------------- calculations ----------------

def build_manager_maps(abc_df: pd.DataFrame):
    by_pair = {(normalize_key(r["Артикул WB"]), normalize_text(r["Артикул продавца"])): normalize_text(r["Менеджер"]) for _, r in abc_df.iterrows()}
    by_wb = {normalize_key(r["Артикул WB"]): normalize_text(r["Менеджер"]) for _, r in abc_df.iterrows() if normalize_key(r["Артикул WB"])}
    by_seller = {normalize_text(r["Артикул продавца"]): normalize_text(r["Менеджер"]) for _, r in abc_df.iterrows() if normalize_text(r["Артикул продавца"])}
    return by_pair, by_wb, by_seller


def build_report_dataframe(
    wb_stocks: pd.DataFrame,
    sales: pd.DataFrame,
    article_map: pd.DataFrame,
    stocks_1c: pd.DataFrame,
    zero_days_df: pd.DataFrame,
    stop_articles: set[str],
    rrc_df: pd.DataFrame,
    inbound_df: pd.DataFrame,
    abc_df: pd.DataFrame,
    cfg: AppConfig,
) -> pd.DataFrame:
    df = wb_stocks.merge(sales, on=["Артикул WB", "Артикул продавца"], how="left")
    for col in ["Продажи 7 дней, шт", "Продажи 60 дней, шт", "Среднесуточные продажи 7д", "Среднесуточные продажи 60д", "Цена покупателя"]:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = df[col].fillna(0.0)

    df = df.merge(article_map, on="Артикул WB", how="left")
    df["Артикул 1С"] = df["Артикул 1С"].fillna("")
    df.loc[df["Артикул 1С"] == "", "Артикул 1С"] = df.loc[df["Артикул 1С"] == "", "Артикул продавца"]
    df["Артикул 1С"] = df["Артикул 1С"].map(normalize_text)

    df = df.merge(stocks_1c, on="Артикул 1С", how="left")
    df["Остатки МП (Липецк), шт"] = df["Остатки МП (Липецк), шт"].fillna(0).map(ceil_int)

    df = df.merge(inbound_df, on="Артикул 1С", how="left")
    if "Товары в пути, шт" not in df.columns:
        df["Товары в пути, шт"] = 0
    df["Товары в пути, шт"] = df["Товары в пути, шт"].fillna(0).map(round_int)
    if "Дата поступления" not in df.columns:
        df["Дата поступления"] = None
    if "Дней до поступления" not in df.columns:
        df["Дней до поступления"] = None
    df.loc[df["Товары в пути, шт"] <= 0, ["Дата поступления", "Дней до поступления"]] = None

    if not zero_days_df.empty:
        df = df.merge(zero_days_df, on="Артикул WB", how="left")
    if "Дней без остатка WB в текущем месяце" not in df.columns:
        df["Дней без остатка WB в текущем месяце"] = 0
    df["Дней без остатка WB в текущем месяце"] = df["Дней без остатка WB в текущем месяце"].fillna(0).astype(int)
    df.loc[df["Остаток WB, шт"] > 0, "Дней без остатка WB в текущем месяце"] = 0

    by_pair, by_wb, by_seller = build_manager_maps(abc_df)
    def resolve_manager(row):
        m = by_pair.get((normalize_key(row["Артикул WB"]), normalize_text(row["Артикул продавца"])), "")
        if not m:
            m = by_wb.get(normalize_key(row["Артикул WB"]), "")
        if not m:
            m = by_seller.get(normalize_text(row["Артикул продавца"]), "")
        if not m:
            m = by_seller.get(normalize_text(row["Артикул 1С"]), "")
        return m
    df["Менеджер"] = df.apply(resolve_manager, axis=1)

    # Global filters
    df = df[(df["Артикул 1С"] != "") & (~df["Артикул 1С"].map(normalize_key).str.startswith("PT104"))].copy()
    df["Статус"] = df["Артикул 1С"].map(lambda x: "Delist" if normalize_key(x) in stop_articles else "")
    df = df[df["Продажи 60 дней, шт"] >= 20].copy()

    # Demand logic
    def demand(row):
        avg7 = safe_float(row["Среднесуточные продажи 7д"])
        avg60 = safe_float(row["Среднесуточные продажи 60д"])
        stock_wb = safe_float(row["Остаток WB, шт"])
        if stock_wb <= 0 or avg7 <= 0:
            return avg60
        return avg7

    df["Расчётный спрос в день, шт"] = df.apply(demand, axis=1)

    def days_or_blank(stock, daily):
        if daily <= 0:
            return 9999.0 if stock > 0 else 0.0
        return stock / daily

    df["WB хватит, дней"] = df.apply(lambda r: days_or_blank(safe_float(r["Остаток WB, шт"]), safe_float(r["Расчётный спрос в день, шт"])), axis=1)
    df["WB + Липецк, дней"] = df.apply(lambda r: days_or_blank(safe_float(r["Остаток WB, шт"]) + safe_float(r["Остатки МП (Липецк), шт"]), safe_float(r["Расчётный спрос в день, шт"])), axis=1)
    df["WB + Липецк + в пути, дней"] = df.apply(lambda r: days_or_blank(safe_float(r["Остаток WB, шт"]) + safe_float(r["Остатки МП (Липецк), шт"]) + safe_float(r["Товары в пути, шт"]), safe_float(r["Расчётный спрос в день, шт"])), axis=1)

    # Out of stock on 60-day horizon
    def oos_days(row):
        daily = safe_float(row["Расчётный спрос в день, шт"])
        if daily <= 0:
            return 0
        total_days = safe_float(row["WB + Липецк + в пути, дней"])
        return round_int(max(60 - total_days, 0))
    df["Out of stock, days"] = df.apply(oos_days, axis=1)
    df["Хватит на 60 дней"] = df["Out of stock, days"].map(lambda x: "Да" if round_int(x) <= 0 else f"Дефицит {round_int(x)} дн.")

    def enough_to_arrival(row):
        if pd.isna(row["Дней до поступления"]):
            return ""
        if safe_float(row["Среднесуточные продажи 7д"]) <= 0:
            return "Да"
        return "Да" if safe_float(row["WB + Липецк, дней"]) >= safe_float(row["Дней до поступления"]) else "Нет"
    df["Хватит до поступления"] = df.apply(enough_to_arrival, axis=1)
    df["Комментарий"] = df["Хватит до поступления"].map(lambda x: "Не хватает до поставки" if x == "Нет" else "")

    df = df.merge(rrc_df, on="Артикул 1С", how="left")
    df["РРЦ"] = df["РРЦ"].fillna(0).map(round_int)
    df["Цена покупателя"] = df["Цена покупателя"].fillna(0).map(round_int)
    df["Коэффициент"] = df.apply(lambda r: (f"{(safe_float(r['Цена покупателя'])/safe_float(r['РРЦ'])):.2f}".replace(".", ",") + "_РРЦ") if safe_float(r['РРЦ']) > 0 else "", axis=1)

    # Round numerics for output
    int_cols = [
        "Остаток WB, шт", "Остатки МП (Липецк), шт", "Товары в пути, шт", "Продажи 60 дней, шт", "Продажи 7 дней, шт",
        "Среднесуточные продажи 60д", "Среднесуточные продажи 7д", "Расчётный спрос в день, шт",
        "WB хватит, дней", "WB + Липецк, дней", "WB + Липецк + в пути, дней", "Out of stock, days",
        "Дней без остатка WB в текущем месяце", "Цена покупателя", "РРЦ"
    ]
    for col in int_cols:
        df[col] = df[col].map(round_int)

    # Final collapse by article_1c
    agg = {
        "Менеджер": "first",
        "Артикул WB": "first",
        "Артикул продавца": "first",
        "Остаток WB, шт": "sum",
        "Остатки МП (Липецк), шт": "max",
        "Товары в пути, шт": "max",
        "Дата поступления": "min",
        "Дней до поступления": "min",
        "Продажи 60 дней, шт": "sum",
        "Продажи 7 дней, шт": "sum",
        "Среднесуточные продажи 60д": "max",
        "Среднесуточные продажи 7д": "max",
        "Расчётный спрос в день, шт": "max",
        "WB хватит, дней": "max",
        "WB + Липецк, дней": "max",
        "WB + Липецк + в пути, дней": "max",
        "Out of stock, days": "max",
        "Хватит на 60 дней": "first",
        "Хватит до поступления": "first",
        "Комментарий": "first",
        "Дней без остатка WB в текущем месяце": "max",
        "Цена покупателя": "max",
        "РРЦ": "max",
        "Коэффициент": "first",
        "Статус": "first",
    }
    df = df.groupby("Артикул 1С", as_index=False).agg(agg)
    df = df.sort_values(by="Артикул 1С", key=lambda s: s.map(natural_sort_key)).reset_index(drop=True)
    return df


# ---------------- sheets ----------------

def split_sheets(df: pd.DataFrame, cfg: AppConfig):
    critical = df[
        (df["Остаток WB, шт"] <= 0)
        | (df["WB хватит, дней"] < cfg.critical_threshold_days)
        | (df["Комментарий"] != "")
    ].copy()

    calc = df.copy()
    dead = df[df["WB + Липецк + в пути, дней"] > cfg.dead_stock_threshold_days].copy()
    monitor = df[df["Статус"] != "Delist"].copy()

    critical = critical[[
        "Артикул 1С", "Продажи 60 дней, шт", "WB хватит, дней", "WB + Липецк, дней", "Out of stock, days",
        "Товары в пути, шт", "Остаток WB, шт", "Остатки МП (Липецк), шт",
        "Дней без остатка WB в текущем месяце", "Менеджер", "Комментарий", "Статус"
    ]].copy()

    calc = calc[[
        "Артикул 1С", "Менеджер", "Артикул WB", "Артикул продавца", "Остаток WB, шт", "Остатки МП (Липецк), шт",
        "Товары в пути, шт", "Дата поступления", "Дней до поступления", "Продажи 60 дней, шт", "Продажи 7 дней, шт",
        "Среднесуточные продажи 60д", "Среднесуточные продажи 7д", "Расчётный спрос в день, шт",
        "WB хватит, дней", "WB + Липецк, дней", "WB + Липецк + в пути, дней", "Out of stock, days",
        "Хватит на 60 дней", "Хватит до поступления", "Дней без остатка WB в текущем месяце",
        "Цена покупателя", "РРЦ", "Коэффициент", "Статус"
    ]].copy()

    dead = dead[[
        "Артикул 1С", "Менеджер", "WB хватит, дней", "WB + Липецк, дней", "WB + Липецк + в пути, дней",
        "Остаток WB, шт", "Остатки МП (Липецк), шт", "Товары в пути, шт", "Продажи 60 дней, шт",
        "Цена покупателя", "РРЦ", "Коэффициент", "Статус"
    ]].copy()

    monitor = monitor[[
        "Артикул 1С", "Продажи 60 дней, шт", "Out of stock, days", "Хватит на 60 дней",
        "WB + Липецк, дней", "WB + Липецк + в пути, дней", "Товары в пути, шт", "Хватит до поступления",
        "Остаток WB, шт", "Остатки МП (Липецк), шт", "Дней без остатка WB в текущем месяце", "Менеджер", "Статус"
    ]].copy()
    return critical, calc, dead, monitor


# ---------------- excel formatting ----------------

def set_widths(ws):
    for col_cells in ws.columns:
        max_len = 0
        col_idx = col_cells[0].column
        for cell in col_cells:
            val = "" if cell.value is None else str(cell.value)
            max_len = max(max_len, max((len(x) for x in val.split("\n")), default=0))
        width = min(max(max_len + 3, 14), 42)
        ws.column_dimensions[get_column_letter(col_idx)].width = width


def style_sheet(ws, kind: str, cfg: AppConfig):
    for row in ws.iter_rows():
        for cell in row:
            cell.border = BORDER
            cell.alignment = ALIGN_CENTER
            cell.font = Font(name=FONT_NAME, size=FONT_SIZE, color="000000")
    for cell in ws[1]:
        cell.fill = HEADER_FILL
        cell.font = Font(name=FONT_NAME, size=FONT_SIZE, color="FFFFFF", bold=True)
    for row in ws.iter_rows(min_row=2):
        row[0].alignment = ALIGN_LEFT

    headers = [c.value for c in ws[1]]
    header_index = {h: i + 1 for i, h in enumerate(headers)}

    if kind == "critical":
        wb_days_col = header_index.get("WB хватит, дней")
        comment_col = header_index.get("Комментарий")
        for r in range(2, ws.max_row + 1):
            if wb_days_col:
                val = ws.cell(r, wb_days_col).value
                try:
                    if float(val) == 0:
                        for c in range(1, ws.max_column + 1):
                            ws.cell(r, c).fill = ORANGE_FILL
                    
                except Exception:
                    pass
            if comment_col:
                comment = str(ws.cell(r, comment_col).value or "")
                if "Не хватает до поставки" in comment:
                    for c in range(1, ws.max_column + 1):
                        ws.cell(r, c).fill = BLUE_FILL

    if kind == "dead":
        for name in ["Цена покупателя", "РРЦ", "Коэффициент"]:
            idx = header_index.get(name)
            if idx:
                for r in range(1, ws.max_row + 1):
                    ws.cell(r, idx).fill = GREEN_FILL if r > 1 else HEADER_FILL
        black_col = header_index.get("WB + Липецк + в пути, дней")
        if black_col:
            for r in range(2, ws.max_row + 1):
                try:
                    val = float(ws.cell(r, black_col).value)
                except Exception:
                    val = 0.0
                if val > cfg.black_cell_threshold_days:
                    ws.cell(r, black_col).fill = BLACK_FILL
                    ws.cell(r, black_col).font = Font(name=FONT_NAME, size=FONT_SIZE, color="FFFFFF", bold=True)

    if kind == "monitor":
        enough_col = header_index.get("Хватит на 60 дней")
        if enough_col:
            for r in range(2, ws.max_row + 1):
                text = str(ws.cell(r, enough_col).value or "")
                if text.startswith("Дефицит"):
                    for c in range(1, ws.max_column + 1):
                        ws.cell(r, c).fill = ORANGE_FILL

    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions
    set_widths(ws)
    for r in range(2, ws.max_row + 1):
        ws.row_dimensions[r].height = 24
    ws.row_dimensions[1].height = 32


# ---------------- output ----------------

def save_report(report_path: Path, critical: pd.DataFrame, calc: pd.DataFrame, dead: pd.DataFrame, monitor: pd.DataFrame, cfg: AppConfig):
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(report_path, engine="openpyxl") as writer:
        critical.to_excel(writer, sheet_name=SHEET_CRITICAL, index=False)
        calc.to_excel(writer, sheet_name=SHEET_CALC, index=False)
        dead.to_excel(writer, sheet_name=SHEET_DEAD, index=False)
        monitor.to_excel(writer, sheet_name=SHEET_MONITOR, index=False)
    wb = load_workbook(report_path)
    style_sheet(wb[SHEET_CRITICAL], "critical", cfg)
    style_sheet(wb[SHEET_CALC], "calc", cfg)
    style_sheet(wb[SHEET_DEAD], "dead", cfg)
    style_sheet(wb[SHEET_MONITOR], "monitor", cfg)
    wb.save(report_path)


def send_telegram(cfg: AppConfig, report_path: Path, critical_count: int, dead_count: int):
    if not cfg.telegram_bot_token or not cfg.telegram_chat_id:
        log("TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID не заданы — отправку пропускаем")
        return
    url = f"https://api.telegram.org/bot{cfg.telegram_bot_token}/sendDocument"
    caption = f"📦 Отчёт по остаткам WB {STORE_NAME}\nКритично: {critical_count}\nDead_Stock: {dead_count}"
    with report_path.open("rb") as f:
        resp = requests.post(
            url,
            data={"chat_id": cfg.telegram_chat_id, "caption": caption},
            files={"document": (report_path.name, f)},
            timeout=120,
        )
    resp.raise_for_status()
    log("Отчёт отправлен в Telegram")


def run():
    cfg = get_config()
    storage = S3Storage(cfg)
    stop_articles = parse_stop_articles(cfg.stop_articles_raw)

    stock_key = find_latest_stock_file(storage)
    order_keys = find_order_files(storage, cfg)

    wb_stocks = load_wb_stocks(storage, stock_key)
    orders = load_orders(storage, cfg, order_keys)
    article_map = load_article_map(storage)
    stocks_1c = load_1c_stocks(storage)
    rrc_df = load_rrc(storage)
    inbound_df = load_inbound(storage, cfg.run_date)
    abc_df = load_abc(storage)

    avg7_map = {}
    for _, row in orders.iterrows():
        avg7_map[normalize_key(row["Артикул WB"])] = safe_float(row.get("Среднесуточные продажи 7д", 0))
    current_zero_wb = set(wb_stocks.loc[wb_stocks["Остаток WB, шт"] <= 0, "Артикул WB"].tolist())
    zero_days_df = count_zero_like_days(storage, cfg, current_zero_wb, avg7_map)

    report_df = build_report_dataframe(
        wb_stocks=wb_stocks,
        sales=orders,
        article_map=article_map,
        stocks_1c=stocks_1c,
        zero_days_df=zero_days_df,
        stop_articles=stop_articles,
        rrc_df=rrc_df,
        inbound_df=inbound_df,
        abc_df=abc_df,
        cfg=cfg,
    )

    critical_df, calc_df, dead_df, monitor_df = split_sheets(report_df, cfg)

    report_path = Path(OUTPUT_DIR) / f"Отчёт_дни_остатка_WB_{STORE_NAME}_{cfg.run_date.strftime('%Y%m%d')}.xlsx"
    save_report(report_path, critical_df, calc_df, dead_df, monitor_df, cfg)

    log(f"Отчёт сохранён: {report_path}")
    if should_send_to_telegram(cfg):
        send_telegram(cfg, report_path, len(critical_df), len(dead_df))
    return report_path


if __name__ == "__main__":
    run()
