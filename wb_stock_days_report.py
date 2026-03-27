#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import io
import math
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional

import boto3
import pandas as pd
import requests
from openpyxl import load_workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

STORE_NAME = "TOPFACE"
WB_STOCKS_PREFIX = f"Отчёты/Остатки/{STORE_NAME}/Недельные/"
WB_ORDERS_PREFIX = f"Отчёты/Заказы/{STORE_NAME}/Недельные/"
ARTICLE_MAP_KEY = "Отчёты/Остатки/1С/Артикулы 1с.xlsx"
STOCKS_1C_KEY = "Отчёты/Остатки/1С/Остатки 1С.xlsx"
RRC_KEY = f"Отчёты/Финансовые показатели/{STORE_NAME}/РРЦ.xlsx"
INBOUND_PREFIX = "Отчёты/Остатки/1С/"
ABC_NAME_FRAGMENT = "abc_report_goods"
OUT_DIR = "output"

SHEET_CRITICAL = "Критично <14 дней"
SHEET_CALC = "Расчёт"
SHEET_DEAD = "Dead_Stock"
SHEET_MONITOR = "Мониторинг остатков"

FONT_NAME = "Calibri"
FONT_SIZE = 14

FILL_HEADER = PatternFill("solid", fgColor="1F4E78")
FILL_LIGHT_GREEN = PatternFill("solid", fgColor="CCFFCC")
FILL_BLACK = PatternFill("solid", fgColor="000000")
FILL_ORANGE = PatternFill("solid", fgColor="FCE4D6")
FILL_BLUE_ROW = PatternFill("solid", fgColor="DDEBF7")

BORDER_THIN = Border(
    left=Side(style="thin", color="D9D9D9"),
    right=Side(style="thin", color="D9D9D9"),
    top=Side(style="thin", color="D9D9D9"),
    bottom=Side(style="thin", color="D9D9D9"),
)
ALIGN_CENTER = Alignment(horizontal="center", vertical="center", wrap_text=True)
ALIGN_LEFT = Alignment(horizontal="left", vertical="center", wrap_text=True)


@dataclass
class Config:
    bucket: str
    access_key: str
    secret_key: str
    endpoint_url: str
    region_name: str
    telegram_bot_token: str
    telegram_chat_id: str
    stop_articles_raw: str
    force_send: bool
    run_date: date


class S3Storage:
    def __init__(self, cfg: Config) -> None:
        self.bucket = cfg.bucket
        self.client = boto3.client(
            "s3",
            endpoint_url=cfg.endpoint_url,
            aws_access_key_id=cfg.access_key,
            aws_secret_access_key=cfg.secret_key,
            region_name=cfg.region_name,
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
                key = item["Key"]
                if not key.endswith("/"):
                    keys.append(key)
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        return keys

    def read_excel(self, key: str, **kwargs) -> pd.DataFrame:
        obj = self.client.get_object(Bucket=self.bucket, Key=key)
        return pd.read_excel(io.BytesIO(obj["Body"].read()), **kwargs)


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


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
    if value is None or pd.isna(value):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(" ", "").replace(",", ".")
    if not text:
        return 0.0
    try:
        return float(text)
    except Exception:
        return 0.0


def round_int(value: object) -> int:
    return int(round(safe_float(value)))


def ceil_int(value: object) -> int:
    return int(math.ceil(safe_float(value)))


def choose_existing_column(df: pd.DataFrame, candidates: Iterable[str], label: str) -> str:
    mapping = {str(c).strip().lower(): c for c in df.columns}
    for candidate in candidates:
        real = mapping.get(candidate.strip().lower())
        if real is not None:
            return real
    raise KeyError(f"Не найдена колонка для '{label}'. Доступные колонки: {list(df.columns)}")


def try_choose_column(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    mapping = {str(c).strip().lower(): c for c in df.columns}
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


def parse_iso_week_from_key(key: str) -> tuple[int, int]:
    m = re.search(r"_(\d{4})-W(\d{2})\.xlsx$", key, flags=re.IGNORECASE)
    if not m:
        return (0, 0)
    return int(m.group(1)), int(m.group(2))


def latest_weekly_key(keys: list[str]) -> str:
    xlsx = [k for k in keys if k.lower().endswith(".xlsx")]
    if not xlsx:
        raise FileNotFoundError("Не найдены weekly xlsx файлы")
    return sorted(xlsx, key=parse_iso_week_from_key)[-1]


def latest_n_weekly_keys(keys: list[str], n: int) -> list[str]:
    xlsx = [k for k in keys if k.lower().endswith(".xlsx")]
    return sorted(xlsx, key=parse_iso_week_from_key)[-n:]


def get_config() -> Config:
    bucket = (os.getenv("YC_BUCKET_NAME") or os.getenv("CLOUD_RU_BUCKET") or os.getenv("WB_S3_BUCKET") or "").strip()
    access_key = (os.getenv("YC_ACCESS_KEY_ID") or os.getenv("CLOUD_RU_ACCESS_KEY") or os.getenv("WB_S3_ACCESS_KEY") or "").strip()
    secret_key = (os.getenv("YC_SECRET_ACCESS_KEY") or os.getenv("CLOUD_RU_SECRET_KEY") or os.getenv("WB_S3_SECRET_KEY") or "").strip()
    endpoint_url = (os.getenv("YC_ENDPOINT_URL") or os.getenv("WB_S3_ENDPOINT") or "https://storage.yandexcloud.net").strip()
    region_name = (os.getenv("WB_S3_REGION") or "ru-central1").strip()
    if not bucket or not access_key or not secret_key:
        raise ValueError("Не заданы параметры Object Storage")
    return Config(
        bucket=bucket,
        access_key=access_key,
        secret_key=secret_key,
        endpoint_url=endpoint_url,
        region_name=region_name,
        telegram_bot_token=(os.getenv("TELEGRAM_BOT_TOKEN") or "").strip(),
        telegram_chat_id=(os.getenv("TELEGRAM_CHAT_ID") or "").strip(),
        stop_articles_raw=os.getenv("WB_STOP_LIST_KEY", ""),
        force_send=(os.getenv("WB_FORCE_SEND", "false").strip().lower() == "true"),
        run_date=date.today(),
    )


def should_send_report(cfg: Config) -> bool:
    if cfg.force_send:
        return True
    return cfg.run_date.weekday() in (0, 4)


def load_article_map(storage: S3Storage) -> dict[str, str]:
    df = storage.read_excel(ARTICLE_MAP_KEY)
    wb_col = df.columns[0]
    article_col = df.columns[2]
    temp = df[[wb_col, article_col]].copy()
    temp.columns = ["Артикул WB", "Артикул 1С"]
    temp["Артикул WB"] = temp["Артикул WB"].map(normalize_key)
    temp["Артикул 1С"] = temp["Артикул 1С"].map(normalize_text)
    temp = temp[(temp["Артикул WB"] != "") & (temp["Артикул 1С"] != "")]
    temp = temp.drop_duplicates(subset=["Артикул WB"], keep="first")
    mapping = dict(zip(temp["Артикул WB"], temp["Артикул 1С"]))
    log(f"Загружено соответствий WB -> 1С: {len(mapping)}")
    return mapping


def load_stocks_1c(storage: S3Storage) -> pd.DataFrame:
    df = storage.read_excel(STOCKS_1C_KEY)
    article_col = choose_existing_column(df, ["Артикул", "АРТ", "Артикул 1С"], "Артикул 1С")
    lipetsk_col = choose_existing_column(df, ["Остатки МП", "Остатки МП (Липецк), шт", "Остатки МП(Липецк), шт"], "Остатки МП")
    temp = pd.DataFrame({
        "Артикул 1С": df[article_col].map(normalize_text),
        "Остатки МП (Липецк), шт": df[lipetsk_col].map(ceil_int),
    })
    temp = temp[temp["Артикул 1С"] != ""]
    return temp.drop_duplicates(subset=["Артикул 1С"], keep="first")


def load_rrc(storage: S3Storage) -> pd.DataFrame:
    df = storage.read_excel(RRC_KEY)
    article_col = df.columns[0]
    rrc_col = df.columns[3]
    temp = pd.DataFrame({
        "Артикул 1С": df[article_col].map(normalize_text),
        "РРЦ": df[rrc_col].map(round_int),
    })
    temp = temp[temp["Артикул 1С"] != ""]
    return temp.drop_duplicates(subset=["Артикул 1С"], keep="first")


def load_abc_managers(storage: S3Storage) -> pd.DataFrame:
    try:
        keys = [k for k in storage.list_keys("") if k.lower().endswith(".xlsx") and ABC_NAME_FRAGMENT in os.path.basename(k).lower()]
        if not keys:
            return pd.DataFrame(columns=["Артикул WB", "Артикул WB продавца", "Менеджер"])
        key = sorted(keys)[-1]
        log(f"Берём ABC-отчёт: {key}")
        df = storage.read_excel(key)
        wb_col = choose_existing_column(df, ["Артикул WB"], "Артикул WB в ABC")
        seller_col = choose_existing_column(df, ["Артикул продавца"], "Артикул продавца в ABC")
        mgr_col = choose_existing_column(df, ["Ваша категория"], "Ваша категория в ABC")
        temp = pd.DataFrame({
            "Артикул WB": df[wb_col].map(normalize_key),
            "Артикул WB продавца": df[seller_col].map(normalize_text),
            "Менеджер": df[mgr_col].map(normalize_text),
        })
        temp = temp[temp["Менеджер"] != ""]
        return temp.drop_duplicates(subset=["Артикул WB", "Артикул WB продавца"], keep="first")
    except Exception as exc:
        log(f"ABC-отчёт не загружен: {exc}")
        return pd.DataFrame(columns=["Артикул WB", "Артикул WB продавца", "Менеджер"])


def load_latest_wb_stocks(storage: S3Storage) -> tuple[pd.DataFrame, str]:
    latest_key = latest_weekly_key(storage.list_keys(WB_STOCKS_PREFIX))
    log(f"Берём остатки WB из файла: {latest_key}")
    df = storage.read_excel(latest_key)

    sample_col = choose_existing_column(df, ["Дата сбора", "Дата запроса"], "дата среза")
    df["_sample_dt"] = pd.to_datetime(df[sample_col], errors="coerce")
    latest_dt = df["_sample_dt"].max()
    if pd.notna(latest_dt):
        df = df[df["_sample_dt"] == latest_dt].copy()

    wb_col = choose_existing_column(df, ["Артикул WB", "nmId"], "Артикул WB")
    seller_col = choose_existing_column(df, ["Артикул продавца"], "Артикул продавца")
    stock_col = choose_existing_column(df, ["Доступно для продажи", "Полное количество", "Количество", "Доступно", "Остаток", "Остатки"], "остатка WB")

    temp = pd.DataFrame({
        "Артикул WB": df[wb_col].map(normalize_key),
        "Артикул WB продавца": df[seller_col].map(normalize_text),
        "Остаток WB, шт": df[stock_col].map(round_int),
    })
    temp = temp[(temp["Артикул WB"] != "") | (temp["Артикул WB продавца"] != "")]
    temp = temp.groupby(["Артикул WB", "Артикул WB продавца"], as_index=False)["Остаток WB, шт"].sum()
    return temp, latest_key


def load_orders_metrics(storage: S3Storage) -> tuple[pd.DataFrame, list[str]]:
    keys = latest_n_weekly_keys(storage.list_keys(WB_ORDERS_PREFIX), 10)
    log(f"Берём заказы WB из файлов: {keys}")
    frames: list[pd.DataFrame] = []
    for key in keys:
        df = storage.read_excel(key)
        frames.append(df)
    orders = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if orders.empty:
        return pd.DataFrame(columns=[
            "Артикул WB", "Артикул WB продавца", "Продажи 7 дней, шт", "Продажи 60 дней, шт",
            "Среднесуточные продажи 7д", "Среднесуточные продажи 60д", "Цена покупателя"
        ]), keys

    wb_col = choose_existing_column(orders, ["nmId", "Артикул WB"], "Артикул WB в заказах")
    seller_col = choose_existing_column(orders, ["supplierArticle", "Артикул продавца"], "Артикул продавца в заказах")
    date_col = choose_existing_column(orders, ["date", "Дата", "Дата заказа", "lastChangeDate", "Дата продажи"], "дата в заказах")

    work = pd.DataFrame({
        "Артикул WB": orders[wb_col].map(normalize_key),
        "Артикул WB продавца": orders[seller_col].map(normalize_text),
        "dt": pd.to_datetime(orders[date_col], errors="coerce").dt.normalize(),
    })
    if "finishedPrice" in orders.columns:
        work["finishedPrice"] = orders["finishedPrice"].map(safe_float)
    else:
        work["finishedPrice"] = 0.0

    work = work[((work["Артикул WB"] != "") | (work["Артикул WB продавца"] != "")) & work["dt"].notna()].copy()
    if work.empty:
        return pd.DataFrame(columns=[
            "Артикул WB", "Артикул WB продавца", "Продажи 7 дней, шт", "Продажи 60 дней, шт",
            "Среднесуточные продажи 7д", "Среднесуточные продажи 60д", "Цена покупателя"
        ]), keys

    max_dt = work["dt"].max()
    start_7 = max_dt - pd.Timedelta(days=6)
    start_60 = max_dt - pd.Timedelta(days=59)
    group_cols = ["Артикул WB", "Артикул WB продавца"]

    sales_7 = work[work["dt"] >= start_7].groupby(group_cols).size().rename("sales_7d")
    sales_60 = work[work["dt"] >= start_60].groupby(group_cols).size().rename("sales_60d")
    metrics = pd.concat([sales_7, sales_60], axis=1).fillna(0).reset_index()
    metrics["sales_7d"] = metrics["sales_7d"].astype(int)
    metrics["sales_60d"] = metrics["sales_60d"].astype(int)
    metrics["avg_daily_sales_7d"] = metrics["sales_7d"] / 7.0
    metrics["avg_daily_sales_60d"] = metrics["sales_60d"] / 60.0

    price_last = work[work["dt"] == max_dt].groupby(group_cols)["finishedPrice"].mean().rename("Цена покупателя").reset_index()
    price_last["Цена покупателя"] = price_last["Цена покупателя"].map(round_int)
    metrics = metrics.merge(price_last, on=group_cols, how="left")
    return metrics, keys


def load_inbound(storage: S3Storage, run_date: date) -> pd.DataFrame:
    keys = [k for k in storage.list_keys(INBOUND_PREFIX) if k.lower().endswith(".xlsx") and "в пути" in os.path.basename(k).lower()]
    frames: list[pd.DataFrame] = []
    for key in keys:
        fname = os.path.basename(key)
        m = re.search(r"(\d{2})-(\d{2})-(\d{2})", fname)
        if not m:
            continue
        arrival_date = datetime.strptime(m.group(0), "%d-%m-%y").date() + timedelta(days=14)
        df = storage.read_excel(key)
        if df.empty or "CODES" not in df.columns:
            continue
        qty_col = try_choose_column(df, ["Заказ МП", "ЗаказМП", "Unnamed: 6"])
        if qty_col is None:
            continue
        temp = pd.DataFrame({
            "Артикул 1С": df["CODES"].map(normalize_text),
            "qty_raw": df[qty_col],
        })
        temp["Товары в пути, шт"] = pd.to_numeric(temp["qty_raw"], errors="coerce").fillna(0).map(round_int)
        temp = temp[(temp["Артикул 1С"] != "") & (temp["Товары в пути, шт"] > 0)]
        if temp.empty:
            continue
        temp = temp[["Артикул 1С", "Товары в пути, шт"]].copy()
        temp["Дата поступления"] = arrival_date
        temp["Дней до поступления"] = max((arrival_date - run_date).days, 0)
        frames.append(temp)

    if not frames:
        return pd.DataFrame(columns=["Артикул 1С", "Товары в пути, шт", "Дата поступления", "Дней до поступления"])

    all_inbound = pd.concat(frames, ignore_index=True)
    qty = all_inbound.groupby("Артикул 1С", as_index=False)["Товары в пути, шт"].sum()
    eta = all_inbound.groupby("Артикул 1С", as_index=False).agg({"Дата поступления": "min", "Дней до поступления": "min"})
    return qty.merge(eta, on="Артикул 1С", how="left")


def load_current_month_zero_days(storage: S3Storage, zero_articles: set[str], avg7_map: dict[str, float], run_date: date) -> dict[str, int]:
    if not zero_articles:
        return {}
    month_start = run_date.replace(day=1)
    rows: list[pd.DataFrame] = []
    for key in sorted(storage.list_keys(WB_STOCKS_PREFIX), key=parse_iso_week_from_key):
        if not key.lower().endswith(".xlsx"):
            continue
        try:
            df = storage.read_excel(key)
        except Exception:
            continue
        wb_col = choose_existing_column(df, ["Артикул WB", "nmId"], "Артикул WB")
        stock_col = choose_existing_column(df, ["Доступно для продажи", "Полное количество"], "остаток WB")
        sample_col = choose_existing_column(df, ["Дата сбора", "Дата запроса"], "дата среза")
        temp = pd.DataFrame({
            "Артикул WB": df[wb_col].map(normalize_key),
            "stock_wb": df[stock_col].map(safe_float),
            "sample_dt": pd.to_datetime(df[sample_col], errors="coerce").dt.normalize(),
        })
        temp = temp[(temp["Артикул WB"].isin(zero_articles)) & temp["sample_dt"].notna()]
        temp = temp[temp["sample_dt"].dt.date >= month_start]
        if temp.empty:
            continue
        temp = temp.groupby(["Артикул WB", "sample_dt"], as_index=False)["stock_wb"].sum()
        rows.append(temp)
    if not rows:
        return {}
    month_df = pd.concat(rows, ignore_index=True)

    def is_zero_like(row: pd.Series) -> bool:
        threshold = 0.5 * float(avg7_map.get(row["Артикул WB"], 0.0) or 0.0)
        return float(row["stock_wb"]) <= threshold

    month_df["is_zero_like"] = month_df.apply(is_zero_like, axis=1)
    return {k: int(v) for k, v in month_df.groupby("Артикул WB")["is_zero_like"].sum().to_dict().items()}


def compute_coef_rrc(price: int, rrc: int) -> str:
    if rrc <= 0 or price <= 0:
        return ""
    return f"{price / rrc:.2f}".replace(".", ",") + "_РРЦ"


def build_report_dataframe(
    wb_stocks: pd.DataFrame,
    sales: pd.DataFrame,
    article_map: dict[str, str],
    stocks_1c: pd.DataFrame,
    stop_articles: set[str],
    rrc_df: pd.DataFrame,
    inbound_df: pd.DataFrame,
    zero_days_map: dict[str, int],
    abc_df: pd.DataFrame,
) -> pd.DataFrame:
    df = wb_stocks.merge(sales, on=["Артикул WB", "Артикул WB продавца"], how="left")
    for col, default in {
        "sales_7d": 0,
        "sales_60d": 0,
        "avg_daily_sales_7d": 0.0,
        "avg_daily_sales_60d": 0.0,
        "Цена покупателя": 0,
    }.items():
        if col not in df.columns:
            df[col] = default
        df[col] = df[col].fillna(default)

    df["Артикул 1С"] = df["Артикул WB"].map(article_map)
    missing = df["Артикул 1С"].isna() | (df["Артикул 1С"].astype(str).str.strip() == "")
    df.loc[missing, "Артикул 1С"] = df.loc[missing, "Артикул WB продавца"]
    df["Артикул 1С"] = df["Артикул 1С"].map(normalize_text)
    df = df[(df["Артикул 1С"] != "") & (~df["Артикул 1С"].str.startswith("PT104", na=False))].copy()

    df = df.merge(stocks_1c, on="Артикул 1С", how="left")
    df["Остатки МП (Липецк), шт"] = df["Остатки МП (Липецк), шт"].fillna(0).map(ceil_int)

    df = df.merge(inbound_df, on="Артикул 1С", how="left")
    df["Товары в пути, шт"] = df["Товары в пути, шт"].fillna(0).map(round_int)
    df["Дней до поступления"] = pd.to_numeric(df.get("Дней до поступления"), errors="coerce")
    df.loc[df["Товары в пути, шт"] <= 0, "Дней до поступления"] = pd.NA
    df.loc[df["Товары в пути, шт"] <= 0, "Дата поступления"] = pd.NaT

    df = df.merge(abc_df, on=["Артикул WB", "Артикул WB продавца"], how="left")
    if "Менеджер" not in df.columns:
        df["Менеджер"] = ""
    df["Менеджер"] = df["Менеджер"].fillna("")

    df["Продажи 7 дней, шт"] = df["sales_7d"].map(round_int)
    df["Продажи 60 дней, шт"] = df["sales_60d"].map(round_int)
    df["Среднесуточные продажи 7д"] = df["avg_daily_sales_7d"].map(safe_float)
    df["Среднесуточные продажи 60д"] = df["avg_daily_sales_60d"].map(safe_float)

    def daily_demand(row: pd.Series) -> float:
        stock = safe_float(row["Остаток WB, шт"])
        avg7 = safe_float(row["Среднесуточные продажи 7д"])
        avg60 = safe_float(row["Среднесуточные продажи 60д"])
        if stock <= 0 or avg7 <= 0:
            return avg60
        return avg7

    df["Расчётный спрос в день, шт"] = df.apply(daily_demand, axis=1)
    df["WB хватит, дней"] = df.apply(lambda r: safe_float(r["Остаток WB, шт"]) / safe_float(r["Расчётный спрос в день, шт"]) if safe_float(r["Расчётный спрос в день, шт"]) > 0 else 0.0, axis=1)
    df["WB + Липецк, дней"] = df.apply(lambda r: (safe_float(r["Остаток WB, шт"]) + safe_float(r["Остатки МП (Липецк), шт"])) / safe_float(r["Расчётный спрос в день, шт"]) if safe_float(r["Расчётный спрос в день, шт"]) > 0 else 0.0, axis=1)
    df["WB + Липецк + в пути, дней"] = df.apply(lambda r: (safe_float(r["Остаток WB, шт"]) + safe_float(r["Остатки МП (Липецк), шт"]) + safe_float(r["Товары в пути, шт"])) / safe_float(r["Расчётный спрос в день, шт"]) if safe_float(r["Расчётный спрос в день, шт"]) > 0 else 0.0, axis=1)

    def enough_to_arrival(row: pd.Series) -> str:
        if pd.isna(row["Дней до поступления"]):
            return ""
        if safe_float(row["Расчётный спрос в день, шт"]) <= 0:
            return "Да"
        return "Да" if safe_float(row["WB + Липецк, дней"]) >= safe_float(row["Дней до поступления"]) else "Нет"

    df["Хватит до поступления"] = df.apply(enough_to_arrival, axis=1)
    df["Out of stock, days"] = df["WB + Липецк + в пути, дней"].map(lambda x: round_int(max(60 - safe_float(x), 0)))
    df["Хватит на 60 дней"] = df["WB + Липецк + в пути, дней"].map(lambda x: "Да" if safe_float(x) >= 60 else f"Дефицит {round_int(60 - safe_float(x))} дн.")

    df["Дней без остатка WB в текущем месяце"] = df["Артикул WB"].map(zero_days_map).fillna(0).astype(int)
    df.loc[df["Остаток WB, шт"] > 0, "Дней без остатка WB в текущем месяце"] = 0
    df["Delist"] = df["Артикул 1С"].map(lambda x: "Delist" if normalize_key(x) in stop_articles else "")

    df = df.merge(rrc_df, on="Артикул 1С", how="left")
    df["РРЦ"] = df["РРЦ"].fillna(0).map(round_int)
    df["Цена покупателя"] = df["Цена покупателя"].fillna(0).map(round_int)
    df["Коэффициент"] = df.apply(lambda r: compute_coef_rrc(round_int(r["Цена покупателя"]), round_int(r["РРЦ"])), axis=1)

    # глобальный фильтр: продажи за 60 дней >= 20 во всех листах/расчётах
    df = df[df["Продажи 60 дней, шт"] >= 20].copy()

    for col in ["Среднесуточные продажи 7д", "Среднесуточные продажи 60д", "Расчётный спрос в день, шт", "WB хватит, дней", "WB + Липецк, дней", "WB + Липецк + в пути, дней"]:
        df[col] = df[col].map(round_int)

    return df.sort_values(by="Артикул 1С", key=lambda s: s.map(lambda x: [int(t) if t.isdigit() else t.lower() for t in re.split(r"(\d+)", str(x))])).reset_index(drop=True)


def split_sheets(report_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    crit_mask = (
        (report_df["Остаток WB, шт"] <= 0)
        | (report_df["WB + Липецк, дней"] < 14)
        | ((report_df["Товары в пути, шт"] > 0) & (report_df["Хватит до поступления"] == "Нет"))
    )
    critical = report_df[crit_mask].copy()
    critical["Комментарий"] = critical.apply(
        lambda r: "Не хватает до поставки" if (safe_float(r["Товары в пути, шт"]) > 0 and r["Хватит до поступления"] == "Нет") else "",
        axis=1,
    )
    critical = critical[[
        "Артикул 1С", "Продажи 60 дней, шт", "WB хватит, дней", "Out of stock, days", "WB + Липецк, дней",
        "Товары в пути, шт", "Остаток WB, шт", "Остатки МП (Липецк), шт", "Дней без остатка WB в текущем месяце",
        "Комментарий", "Менеджер", "Delist",
    ]].copy()

    calc = report_df[[
        "Артикул 1С", "Менеджер", "Артикул WB", "Артикул WB продавца", "Остаток WB, шт",
        "Остатки МП (Липецк), шт", "Товары в пути, шт", "Дата поступления", "Дней до поступления",
        "Продажи 7 дней, шт", "Продажи 60 дней, шт", "Среднесуточные продажи 7д", "Среднесуточные продажи 60д",
        "Расчётный спрос в день, шт", "WB хватит, дней", "WB + Липецк, дней", "WB + Липецк + в пути, дней",
        "Хватит до поступления", "Out of stock, days", "Хватит на 60 дней", "Дней без остатка WB в текущем месяце",
        "Цена покупателя", "РРЦ", "Коэффициент", "Delist",
    ]].copy()

    dead = report_df[report_df["WB + Липецк + в пути, дней"] > 120].copy()
    dead = dead[[
        "Артикул 1С", "Менеджер", "WB хватит, дней", "WB + Липецк, дней", "WB + Липецк + в пути, дней",
        "Остаток WB, шт", "Остатки МП (Липецк), шт", "Товары в пути, шт", "Продажи 60 дней, шт",
        "Цена покупателя", "РРЦ", "Коэффициент", "Delist",
    ]].copy()

    monitor = report_df[report_df["Delist"] != "Delist"].copy()
    monitor = monitor[[
        "Артикул 1С", "Продажи 60 дней, шт", "Out of stock, days", "Хватит на 60 дней",
        "WB + Липецк, дней", "WB + Липецк + в пути, дней", "Товары в пути, шт", "Хватит до поступления",
        "Остаток WB, шт", "Остатки МП (Липецк), шт", "Дней без остатка WB в текущем месяце", "Менеджер",
    ]].copy()
    return critical, calc, dead, monitor


def auto_fit_columns(ws) -> None:
    widths: dict[int, int] = {}
    for row in ws.iter_rows():
        for cell in row:
            text = "" if cell.value is None else str(cell.value)
            max_len = max((len(part) for part in text.split("\n")), default=0)
            widths[cell.column] = max(widths.get(cell.column, 0), max_len)
    for idx, width in widths.items():
        ws.column_dimensions[get_column_letter(idx)].width = min(max(width + 3, 16), 42)


def style_sheet(ws, monitor: bool = False, dead_days_col: Optional[int] = None) -> None:
    for row in ws.iter_rows():
        for cell in row:
            cell.alignment = ALIGN_CENTER
            cell.border = BORDER_THIN
            cell.font = Font(name=FONT_NAME, size=FONT_SIZE, color="000000")
    for cell in ws[1]:
        cell.fill = FILL_HEADER
        cell.font = Font(name=FONT_NAME, size=FONT_SIZE, bold=True, color="FFFFFF")
    for row in ws.iter_rows(min_row=2):
        row[0].alignment = ALIGN_LEFT
    auto_fit_columns(ws)
    ws.auto_filter.ref = ws.dimensions
    ws.freeze_panes = "A2"

    headers = [c.value for c in ws[1]]

    if ws.title == SHEET_CRITICAL:
        wb_days_idx = headers.index("WB хватит, дней") + 1 if "WB хватит, дней" in headers else None
        comment_idx = headers.index("Комментарий") + 1 if "Комментарий" in headers else None
        for r in range(2, ws.max_row + 1):
            if wb_days_idx is not None and safe_float(ws.cell(r, wb_days_idx).value) == 0:
                for c in range(1, ws.max_column + 1):
                    ws.cell(r, c).fill = FILL_ORANGE
            if comment_idx is not None and str(ws.cell(r, comment_idx).value or "").strip() == "Не хватает до поставки":
                for c in range(1, ws.max_column + 1):
                    ws.cell(r, c).fill = FILL_BLUE_ROW

    if ws.title == SHEET_DEAD and dead_days_col is not None:
        for r in range(2, ws.max_row + 1):
            cell = ws.cell(r, dead_days_col)
            if safe_float(cell.value) > 180:
                cell.fill = FILL_BLACK
                cell.font = Font(name=FONT_NAME, size=FONT_SIZE, bold=True, color="FFFFFF")

    if monitor and "Хватит на 60 дней" in headers:
        idx = headers.index("Хватит на 60 дней") + 1
        for r in range(2, ws.max_row + 1):
            value = str(ws.cell(r, idx).value or "")
            if value.startswith("Дефицит"):
                for c in range(1, ws.max_column + 1):
                    ws.cell(r, c).fill = FILL_ORANGE


def save_report(report_path: Path, critical: pd.DataFrame, calc: pd.DataFrame, dead: pd.DataFrame, monitor: pd.DataFrame) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(report_path, engine="openpyxl") as writer:
        critical.to_excel(writer, sheet_name=SHEET_CRITICAL, index=False)
        calc.to_excel(writer, sheet_name=SHEET_CALC, index=False)
        dead.to_excel(writer, sheet_name=SHEET_DEAD, index=False)
        monitor.to_excel(writer, sheet_name=SHEET_MONITOR, index=False)
    wb = load_workbook(report_path)
    style_sheet(wb[SHEET_CRITICAL])
    style_sheet(wb[SHEET_CALC])
    dead_headers = [c.value for c in wb[SHEET_DEAD][1]]
    dead_days_col = dead_headers.index("WB + Липецк + в пути, дней") + 1 if "WB + Липецк + в пути, дней" in dead_headers else None
    style_sheet(wb[SHEET_DEAD], dead_days_col=dead_days_col)
    style_sheet(wb[SHEET_MONITOR], monitor=True)
    wb.save(report_path)


def send_to_telegram(cfg: Config, path: Path, critical_count: int, dead_count: int) -> None:
    if not cfg.telegram_bot_token or not cfg.telegram_chat_id:
        log("Telegram env не заданы — отправку пропускаем")
        return
    url = f"https://api.telegram.org/bot{cfg.telegram_bot_token}/sendDocument"
    caption = f"📦 Отчёт по остаткам WB {STORE_NAME}\nКритично: {critical_count}\nDead_Stock: {dead_count}"
    with open(path, "rb") as f:
        resp = requests.post(url, data={"chat_id": cfg.telegram_chat_id, "caption": caption}, files={"document": (path.name, f)}, timeout=120)
    resp.raise_for_status()
    log("Отчёт отправлен в Telegram")


def run() -> Path:
    cfg = get_config()
    storage = S3Storage(cfg)
    stop_articles = parse_stop_articles(cfg.stop_articles_raw)

    wb_stocks, stock_source = load_latest_wb_stocks(storage)
    sales_df, order_sources = load_orders_metrics(storage)
    article_map = load_article_map(storage)
    stocks_1c = load_stocks_1c(storage)
    rrc_df = load_rrc(storage)
    inbound_df = load_inbound(storage, cfg.run_date)
    abc_df = load_abc_managers(storage)

    avg7_map: dict[str, float] = {}
    for _, row in sales_df.iterrows():
        wb_key = normalize_key(row.get("Артикул WB"))
        avg7_map[wb_key] = safe_float(row.get("avg_daily_sales_7d"))

    current_zero_articles = set(wb_stocks.loc[wb_stocks["Остаток WB, шт"] <= 0, "Артикул WB"].tolist())
    zero_days_map = load_current_month_zero_days(storage, current_zero_articles, avg7_map, cfg.run_date)

    report_df = build_report_dataframe(
        wb_stocks=wb_stocks,
        sales=sales_df,
        article_map=article_map,
        stocks_1c=stocks_1c,
        stop_articles=stop_articles,
        rrc_df=rrc_df,
        inbound_df=inbound_df,
        zero_days_map=zero_days_map,
        abc_df=abc_df,
    )

    critical, calc, dead, monitor = split_sheets(report_df)
    report_path = Path(OUT_DIR) / f"Отчёт_дни_остатка_WB_{STORE_NAME}_{cfg.run_date.strftime('%Y%m%d')}.xlsx"
    save_report(report_path, critical, calc, dead, monitor)

    log(f"Отчёт сохранён: {report_path}")
    log(f"Источник остатков: {stock_source}")
    log(f"Источники заказов: {', '.join(order_sources)}")

    if should_send_report(cfg):
        send_to_telegram(cfg, report_path, len(critical), len(dead))
    else:
        log("Отправка в Telegram пропущена по расписанию")
    return report_path


if __name__ == "__main__":
    run()
