#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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
from openpyxl import load_workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter


STORE_NAME_DEFAULT = "TOPFACE"
OUT_DIR_DEFAULT = "output"
HEADER_FILL = PatternFill("solid", fgColor="1F4E78")
LIGHT_GREEN_FILL = PatternFill("solid", fgColor="CCFFCC")
BLACK_FILL = PatternFill("solid", fgColor="000000")
ORANGE_FILL = PatternFill("solid", fgColor="FCE4D6")
THIN_BORDER = Border(
    left=Side(style="thin", color="D9D9D9"),
    right=Side(style="thin", color="D9D9D9"),
    top=Side(style="thin", color="D9D9D9"),
    bottom=Side(style="thin", color="D9D9D9"),
)
ALIGN_CENTER = Alignment(horizontal="center", vertical="center", wrap_text=True)
ALIGN_LEFT = Alignment(horizontal="left", vertical="center", wrap_text=True)
FONT_NAME = "Calibri"
FONT_SIZE = 14

SHEET_CRITICAL = "Критично <14 дней"
SHEET_CALC = "Расчёт"
SHEET_DEAD = "Dead_Stock"
SHEET_MONITOR = "Мониторинг остатков"


CYR_TO_LAT_LOOKALIKE = str.maketrans({
    "А": "A", "В": "B", "С": "C", "Е": "E", "Н": "H", "К": "K", "М": "M",
    "О": "O", "Р": "P", "Т": "T", "У": "Y", "Х": "X",
    "а": "a", "е": "e", "о": "o", "р": "p", "с": "c", "у": "y", "х": "x", "к": "k", "м": "m", "т": "t", "в": "b", "н": "h",
})


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def normalize_text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    if isinstance(value, float) and float(value).is_integer():
        return str(int(value)).strip()
    text = str(value).strip()
    if re.fullmatch(r"\d+\.0+", text):
        return text.split(".")[0]
    return text


def normalize_key(value: object) -> str:
    return normalize_text(value).upper()


def normalize_article_code(value: object) -> str:
    text = normalize_text(value)
    return text.translate(CYR_TO_LAT_LOOKALIKE).upper()


def safe_float(value: object) -> float:
    if value is None or pd.isna(value) or value == "":
        return 0.0
    try:
        return float(value)
    except Exception:
        text = str(value).replace(" ", "").replace(",", ".")
        return float(text) if text else 0.0


def round_int(value: object) -> int:
    if value is None or pd.isna(value):
        return 0
    return int(round(safe_float(value)))


def ceil_int(value: object) -> int:
    if value is None or pd.isna(value):
        return 0
    return int(math.ceil(safe_float(value)))


def safe_div(num: float, den: float) -> Optional[float]:
    if den is None or pd.isna(den) or float(den) <= 0:
        if float(num or 0) > 0:
            return None
        return 0.0
    return float(num) / float(den)


def natural_sort_key(article: object):
    s = normalize_text(article)
    return [int(t) if t.isdigit() else t.lower() for t in re.split(r"(\d+)", s)]


def parse_stop_articles(raw: str) -> set[str]:
    if not raw:
        return set()
    norm = raw.replace(";", "\n").replace(",", "\n").replace("\r", "\n")
    return {normalize_article_code(x) for x in norm.split("\n") if normalize_text(x)}


def choose_existing_column(df: pd.DataFrame, candidates: Iterable[str], label: str) -> str:
    lower_map = {str(c).strip().lower(): c for c in df.columns}
    for c in candidates:
        real = lower_map.get(c.strip().lower())
        if real is not None:
            return real
    raise KeyError(f"Не найдена колонка для '{label}'. Доступные колонки: {list(df.columns)}")


def parse_iso_week_from_key(key: str) -> tuple[int, int]:
    m = re.search(r"(\d{4})-W(\d{2})", key)
    if not m:
        return (0, 0)
    return (int(m.group(1)), int(m.group(2)))


def should_send_to_telegram(run_date: datetime, force_send: bool) -> bool:
    if force_send:
        return True
    return run_date.weekday() in (0, 4)


def format_coef_rrc(price: object, rrc: object) -> str:
    p = safe_float(price)
    r = safe_float(rrc)
    if r <= 0:
        return ""
    coef = p / r
    return f"{coef:.2f}".replace(".", ",") + "_РРЦ"


@dataclass
class AppConfig:
    bucket_name: str
    access_key: str
    secret_key: str
    endpoint_url: str
    region_name: str
    store_name: str
    run_date: datetime
    low_stock_days_threshold: float
    force_send: bool
    send_telegram: bool
    output_dir: str
    telegram_bot_token: str
    telegram_chat_id: str
    stop_articles_raw: str
    stocks_prefix: str
    orders_prefix: str
    article_map_key: str
    stocks_1c_key: str
    rrc_key: str
    inbound_prefix: str
    abc_key: str
    abc_search_substring: str

    @staticmethod
    def from_env() -> "AppConfig":
        bucket_name = (os.getenv("YC_BUCKET_NAME") or os.getenv("CLOUD_RU_BUCKET") or os.getenv("WB_S3_BUCKET") or "").strip()
        access_key = (os.getenv("YC_ACCESS_KEY_ID") or os.getenv("CLOUD_RU_ACCESS_KEY") or os.getenv("WB_S3_ACCESS_KEY") or "").strip()
        secret_key = (os.getenv("YC_SECRET_ACCESS_KEY") or os.getenv("CLOUD_RU_SECRET_KEY") or os.getenv("WB_S3_SECRET_KEY") or "").strip()
        endpoint_url = (os.getenv("YC_ENDPOINT_URL") or os.getenv("YC_ENDPOINT") or os.getenv("WB_S3_ENDPOINT") or os.getenv("CLOUD_RU_ENDPOINT") or "https://storage.yandexcloud.net").strip()
        region_name = (os.getenv("WB_S3_REGION") or os.getenv("YC_REGION") or "ru-central1").strip()
        if not bucket_name or not access_key or not secret_key:
            raise ValueError("Не заданы параметры Object Storage.")
        run_date = datetime.strptime(os.getenv("WB_RUN_DATE", datetime.now().strftime("%Y-%m-%d")), "%Y-%m-%d")
        store = (os.getenv("WB_STORE") or STORE_NAME_DEFAULT).strip()
        return AppConfig(
            bucket_name=bucket_name,
            access_key=access_key,
            secret_key=secret_key,
            endpoint_url=endpoint_url,
            region_name=region_name,
            store_name=store,
            run_date=run_date,
            low_stock_days_threshold=float(os.getenv("WB_LOW_STOCK_DAYS_THRESHOLD", "14")),
            force_send=str(os.getenv("WB_FORCE_SEND", "false")).strip().lower() in {"1", "true", "yes", "on"},
            send_telegram=str(os.getenv("WB_SEND_TELEGRAM", "true")).strip().lower() in {"1", "true", "yes", "on"},
            output_dir=(os.getenv("WB_OUTPUT_DIR") or OUT_DIR_DEFAULT).strip(),
            telegram_bot_token=(os.getenv("TELEGRAM_BOT_TOKEN") or "").strip(),
            telegram_chat_id=(os.getenv("TELEGRAM_CHAT_ID") or "").strip(),
            stop_articles_raw=os.getenv("WB_STOP_LIST_KEY", ""),
            stocks_prefix=os.getenv("WB_STOCKS_PREFIX", f"Отчёты/Остатки/{store}/Недельные/"),
            orders_prefix=os.getenv("WB_ORDERS_PREFIX", f"Отчёты/Заказы/{store}/Недельные/"),
            article_map_key=os.getenv("WB_ARTICLE_MAP_KEY", "Отчёты/Остатки/1С/Артикулы 1с.xlsx"),
            stocks_1c_key=os.getenv("WB_STOCKS_1C_KEY", "Отчёты/Остатки/1С/Остатки 1С.xlsx"),
            rrc_key=os.getenv("WB_RRC_KEY", f"Отчёты/Финансовые показатели/{store}/РРЦ.xlsx"),
            inbound_prefix=os.getenv("WB_INBOUND_PREFIX", "Отчёты/Остатки/1С/"),
            abc_key=os.getenv("WB_ABC_KEY", ""),
            abc_search_substring=(os.getenv("WB_ABC_SEARCH", "abc_report_goods") or "abc_report_goods").strip().lower(),
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
        )

    def list_keys(self, prefix: str) -> list[str]:
        keys: list[str] = []
        token = None
        while True:
            params = {"Bucket": self.bucket, "Prefix": prefix, "MaxKeys": 1000}
            if token:
                params["ContinuationToken"] = token
            resp = self.s3.list_objects_v2(**params)
            keys.extend([obj["Key"] for obj in resp.get("Contents", []) if not obj["Key"].endswith("/")])
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        return keys

    def read_bytes(self, key: str) -> bytes:
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        return obj["Body"].read()

    def read_excel(self, key: str, sheet_name: Optional[str] = None) -> pd.DataFrame:
        df = pd.read_excel(io.BytesIO(self.read_bytes(key)), sheet_name=sheet_name)
        if isinstance(df, dict):
            return next(iter(df.values()))
        return df


def load_latest_weekly_file(storage: S3Storage, prefix: str) -> tuple[pd.DataFrame, str]:
    keys = [k for k in storage.list_keys(prefix) if k.lower().endswith(".xlsx")]
    if not keys:
        raise FileNotFoundError(f"Не найдены xlsx по префиксу {prefix}")
    latest_key = sorted(keys, key=parse_iso_week_from_key)[-1]
    df = storage.read_excel(latest_key)
    log(f"Берём остатки WB из файла: {latest_key}")
    return df, latest_key


def load_orders_window(storage: S3Storage, cfg: AppConfig) -> tuple[pd.DataFrame, list[str]]:
    keys = [k for k in storage.list_keys(cfg.orders_prefix) if k.lower().endswith(".xlsx")]
    if not keys:
        raise FileNotFoundError(f"Не найдены weekly-файлы заказов по префиксу {cfg.orders_prefix}")
    recent = sorted(keys, key=parse_iso_week_from_key)[-10:]
    log(f"Берём заказы WB из файлов: {recent}")
    parts = []
    for key in recent:
        try:
            parts.append(storage.read_excel(key))
        except Exception as exc:
            log(f"⚠️ Не удалось прочитать {key}: {exc}")
    if not parts:
        raise ValueError("Не удалось прочитать weekly-файлы заказов")
    return pd.concat(parts, ignore_index=True), recent


def load_wb_stocks(storage: S3Storage, cfg: AppConfig) -> pd.DataFrame:
    df, latest_key = load_latest_weekly_file(storage, cfg.stocks_prefix)
    sample_col = choose_existing_column(df, ["Дата сбора", "Дата запроса"], "дата среза остатков")
    wb_col = choose_existing_column(df, ["Артикул WB", "nmId"], "Артикул WB")
    seller_col = choose_existing_column(df, ["Артикул продавца", "supplierArticle", "vendorCode"], "Артикул продавца")
    stock_col = choose_existing_column(df, ["Доступно для продажи", "Полное количество", "Количество", "Доступно", "Остаток", "Остатки"], "остатка WB")

    work = df.copy()
    work[sample_col] = pd.to_datetime(work[sample_col], errors="coerce")
    max_sample = work[sample_col].max()
    work = work[work[sample_col] == max_sample].copy()

    out = pd.DataFrame()
    out["wb_id"] = work[wb_col].map(normalize_key)
    out["seller_article_key"] = work[seller_col].map(normalize_article_code)
    out["seller_article"] = work[seller_col].map(normalize_text)
    out["stock_wb_qty"] = pd.to_numeric(work[stock_col], errors="coerce").fillna(0)
    out = out[(out["wb_id"] != "") | (out["seller_article_key"] != "")].copy()
    out = out.groupby(["wb_id", "seller_article_key", "seller_article"], as_index=False).agg(stock_wb_qty=("stock_wb_qty", "sum"))
    out["stock_wb_qty"] = out["stock_wb_qty"].map(round_int)
    log(f"Источник WB остатков: {latest_key}")
    return out


def load_orders_metrics(storage: S3Storage, cfg: AppConfig) -> pd.DataFrame:
    orders, source_keys = load_orders_window(storage, cfg)
    date_col = choose_existing_column(orders, ["date", "Дата", "Дата заказа", "lastChangeDate", "Дата продажи"], "даты заказов")
    wb_col = choose_existing_column(orders, ["nmId", "Артикул WB"], "Артикул WB в заказах")
    seller_col = choose_existing_column(orders, ["supplierArticle", "Артикул продавца", "vendorCode"], "Артикул продавца в заказах")

    work = orders.copy()
    work[date_col] = pd.to_datetime(work[date_col], errors="coerce").dt.normalize()
    work = work[work[date_col].notna()].copy()
    if "isCancel" in work.columns:
        work = work[~work["isCancel"].fillna(False)].copy()

    if work.empty:
        return pd.DataFrame(columns=["wb_id", "seller_article_key", "seller_article", "sales_7d", "sales_60d", "avg_daily_sales_7d", "avg_daily_sales_60d", "last_day_price"])

    max_day = work[date_col].max()
    start_7 = max_day - timedelta(days=6)
    start_60 = max_day - timedelta(days=59)

    work["wb_id"] = work[wb_col].map(normalize_key)
    work["seller_article_key"] = work[seller_col].map(normalize_article_code)
    work["seller_article"] = work[seller_col].map(normalize_text)
    work = work[(work["wb_id"] != "") | (work["seller_article_key"] != "")].copy()
    work["qty"] = 1

    gkeys = ["wb_id", "seller_article_key", "seller_article"]
    s7 = work[work[date_col] >= start_7].groupby(gkeys, as_index=False).agg(sales_7d=("qty", "sum"))
    s60 = work[work[date_col] >= start_60].groupby(gkeys, as_index=False).agg(sales_60d=("qty", "sum"))
    sales = s60.merge(s7, on=gkeys, how="outer").fillna({"sales_60d": 0, "sales_7d": 0})
    sales["sales_60d"] = sales["sales_60d"].astype(int)
    sales["sales_7d"] = sales["sales_7d"].astype(int)
    sales["avg_daily_sales_60d"] = sales["sales_60d"] / 60.0
    sales["avg_daily_sales_7d"] = sales["sales_7d"] / 7.0

    if "finishedPrice" in work.columns:
        price = work[work[date_col] == max_day].groupby(gkeys, as_index=False).agg(last_day_price=("finishedPrice", "mean"))
        sales = sales.merge(price, on=gkeys, how="left")
    else:
        sales["last_day_price"] = 0.0

    log(f"Источники заказов: {', '.join(source_keys)}")
    return sales


def load_article_map_1c(storage: S3Storage, cfg: AppConfig) -> pd.DataFrame:
    df = storage.read_excel(cfg.article_map_key)
    if df.shape[1] < 3:
        raise ValueError("Файл Артикулы 1с.xlsx должен содержать минимум 3 колонки")
    mapped = pd.DataFrame()
    mapped["wb_id"] = df.iloc[:, 0].map(normalize_key)
    mapped["article_1c"] = df.iloc[:, 2].map(normalize_text)
    mapped = mapped[(mapped["wb_id"] != "") & (mapped["article_1c"] != "")].drop_duplicates(subset=["wb_id"])
    log(f"Загружено соответствий WB -> 1С: {len(mapped)}")
    return mapped


def load_1c_stocks(storage: S3Storage, cfg: AppConfig) -> pd.DataFrame:
    df = storage.read_excel(cfg.stocks_1c_key)
    article_col = choose_existing_column(df, ["Артикул", "АРТ", "Артикул 1С"], "Артикул 1С в Остатки 1С")
    mp_col = choose_existing_column(df, ["Остатки МП", "Остатки МП (Липецк), шт", "Остатки МП(Липецк), шт"], "Остатки МП")
    wb_code_col = None
    for c in df.columns:
        if str(c).strip().lower() in {"код_wb", "код wb", "nmid", "артикул wb"}:
            wb_code_col = c
            break
    out = pd.DataFrame()
    out["article_1c"] = df[article_col].map(normalize_text)
    out["wb_mp_qty"] = pd.to_numeric(df[mp_col], errors="coerce").fillna(0).map(ceil_int)
    if wb_code_col:
        out["wb_id_from_1c"] = df[wb_code_col].map(normalize_key)
    else:
        out["wb_id_from_1c"] = ""
    out = out[(out["article_1c"] != "")].drop_duplicates(subset=["article_1c"])
    return out


def load_rrc(storage: S3Storage, cfg: AppConfig) -> pd.DataFrame:
    try:
        df = storage.read_excel(cfg.rrc_key)
    except Exception as exc:
        log(f"⚠️ Не удалось загрузить РРЦ: {exc}")
        return pd.DataFrame(columns=["article_1c", "rrc"])
    if df.shape[1] < 4:
        return pd.DataFrame(columns=["article_1c", "rrc"])
    out = pd.DataFrame()
    out["article_1c"] = df.iloc[:, 0].map(normalize_text)
    out["rrc"] = pd.to_numeric(df.iloc[:, 3], errors="coerce").fillna(0).map(round_int)
    out = out[(out["article_1c"] != "")].drop_duplicates(subset=["article_1c"])
    return out


def load_inbound(storage: S3Storage, cfg: AppConfig) -> pd.DataFrame:
    keys = [k for k in storage.list_keys(cfg.inbound_prefix) if k.lower().endswith(".xlsx") and "в пути" in os.path.basename(k).lower()]
    frames = []
    for key in keys:
        fname = os.path.basename(key)
        m = re.search(r"(\d{2})-(\d{2})-(\d{2})", fname)
        if not m:
            continue
        eta = datetime.strptime(m.group(0), "%d-%m-%y").date() + timedelta(days=14)
        try:
            df = storage.read_excel(key)
        except Exception as exc:
            log(f"⚠️ Не удалось прочитать файл в пути {key}: {exc}")
            continue
        if "CODES" not in df.columns:
            continue
        qty_col = "Заказ МП" if "Заказ МП" in df.columns else ("Unnamed: 6" if "Unnamed: 6" in df.columns else None)
        if qty_col is None:
            continue
        tmp = pd.DataFrame()
        tmp["article_1c"] = df["CODES"].map(normalize_text)
        tmp["inbound_qty"] = pd.to_numeric(df[qty_col], errors="coerce").fillna(0).map(round_int)
        tmp = tmp[(tmp["article_1c"] != "") & (tmp["inbound_qty"] > 0)]
        if tmp.empty:
            continue
        tmp["arrival_date"] = eta
        frames.append(tmp)
    if not frames:
        return pd.DataFrame(columns=["article_1c", "inbound_qty", "arrival_date", "days_to_arrival"])
    all_inb = pd.concat(frames, ignore_index=True)
    total = all_inb.groupby("article_1c", as_index=False).agg(inbound_qty=("inbound_qty", "sum"))
    nearest = all_inb.groupby("article_1c", as_index=False).agg(arrival_date=("arrival_date", "min"))
    out = total.merge(nearest, on="article_1c", how="left")
    today = date.today()
    out["days_to_arrival"] = out["arrival_date"].map(lambda d: max((d - today).days, 0) if pd.notna(d) else None)
    return out


def load_abc(storage: S3Storage, cfg: AppConfig) -> pd.DataFrame:
    try:
        if cfg.abc_key:
            df = storage.read_excel(cfg.abc_key)
            source = cfg.abc_key
        else:
            keys = [k for k in storage.list_keys("") if k.lower().endswith(".xlsx") and cfg.abc_search_substring in os.path.basename(k).lower()]
            if not keys:
                return pd.DataFrame(columns=["wb_id", "seller_article_key", "manager"])
            source = sorted(keys)[-1]
            df = storage.read_excel(source)
        wb_col = choose_existing_column(df, ["Артикул WB"], "Артикул WB в ABC")
        seller_col = choose_existing_column(df, ["Артикул продавца"], "Артикул продавца в ABC")
        mgr_col = choose_existing_column(df, ["Ваша категория"], "Ваша категория в ABC")
        out = pd.DataFrame()
        out["wb_id"] = df[wb_col].map(normalize_key)
        out["seller_article_key"] = df[seller_col].map(normalize_article_code)
        out["manager"] = df[mgr_col].map(normalize_text)
        out = out[(out["manager"] != "") & ((out["wb_id"] != "") | (out["seller_article_key"] != ""))]
        out = out.drop_duplicates(subset=["wb_id", "seller_article_key"])
        log(f"Загружен ABC-отчёт: {source}")
        return out
    except Exception as exc:
        log(f"⚠️ Не удалось загрузить ABC-отчёт: {exc}")
        return pd.DataFrame(columns=["wb_id", "seller_article_key", "manager"])


def build_metrics(wb_stocks: pd.DataFrame, sales: pd.DataFrame, article_map: pd.DataFrame, stocks_1c: pd.DataFrame, rrc: pd.DataFrame, inbound: pd.DataFrame, abc: pd.DataFrame, stop_articles: set[str]) -> pd.DataFrame:
    df = wb_stocks.merge(sales, on=["wb_id", "seller_article_key", "seller_article"], how="left")
    for col in ["sales_7d", "sales_60d", "avg_daily_sales_7d", "avg_daily_sales_60d", "last_day_price"]:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df = df.merge(article_map, on="wb_id", how="left")
    # fallback на артикул продавца, если mapping WB->1С не нашёлся
    df["article_1c"] = df["article_1c"].fillna("")
    missing = df["article_1c"].astype(str).str.strip() == ""
    df.loc[missing, "article_1c"] = df.loc[missing, "seller_article"].fillna("")

    # fallback по wb_id из файла Остатки 1С
    fb = stocks_1c[(stocks_1c["wb_id_from_1c"] != "") & (stocks_1c["article_1c"] != "")][["wb_id_from_1c", "article_1c"]].drop_duplicates(subset=["wb_id_from_1c"]).rename(columns={"wb_id_from_1c": "wb_id", "article_1c": "article_1c_fb"})
    df = df.merge(fb, on="wb_id", how="left")
    missing = df["article_1c"].astype(str).str.strip() == ""
    df.loc[missing, "article_1c"] = df.loc[missing, "article_1c_fb"].fillna("")
    df = df.drop(columns=[c for c in ["article_1c_fb"] if c in df.columns])

    df["article_1c"] = df["article_1c"].map(normalize_text)
    df = df[(df["article_1c"] != "") & (~df["article_1c"].str.startswith("PT104", na=False))].copy()

    df = df.merge(stocks_1c[["article_1c", "wb_mp_qty"]], on="article_1c", how="left")
    df["wb_mp_qty"] = pd.to_numeric(df["wb_mp_qty"], errors="coerce").fillna(0).map(round_int)

    df = df.merge(inbound, on="article_1c", how="left")
    df["inbound_qty"] = pd.to_numeric(df.get("inbound_qty", 0), errors="coerce").fillna(0).map(round_int)
    if "days_to_arrival" not in df.columns:
        df["days_to_arrival"] = None
    df["days_to_arrival"] = pd.to_numeric(df["days_to_arrival"], errors="coerce")
    df.loc[df["inbound_qty"] <= 0, "days_to_arrival"] = pd.NA
    if "arrival_date" not in df.columns:
        df["arrival_date"] = pd.NaT

    df = df.merge(abc, on=["wb_id", "seller_article_key"], how="left")
    df["manager"] = df.get("manager", "").fillna("")

    def pick_daily_demand(row):
        avg7 = safe_float(row["avg_daily_sales_7d"])
        avg60 = safe_float(row["avg_daily_sales_60d"])
        if safe_float(row["stock_wb_qty"]) <= 0 or avg7 <= 0:
            return avg60
        return avg7

    df["daily_demand"] = df.apply(pick_daily_demand, axis=1)

    df["days_wb"] = df.apply(lambda r: safe_div(safe_float(r["stock_wb_qty"]), safe_float(r["daily_demand"])), axis=1)
    df["days_total"] = df.apply(lambda r: safe_div(safe_float(r["stock_wb_qty"]) + safe_float(r["wb_mp_qty"]), safe_float(r["daily_demand"])), axis=1)
    df["days_total_inbound"] = df.apply(lambda r: safe_div(safe_float(r["stock_wb_qty"]) + safe_float(r["wb_mp_qty"]) + safe_float(r["inbound_qty"]), safe_float(r["daily_demand"])), axis=1)

    def out_of_stock_days(row):
        days = row["days_total_inbound"]
        if days is None or pd.isna(days):
            return 0
        return round_int(max(60 - float(days), 0))

    def enough_60_text(row):
        days = row["days_total_inbound"]
        if days is None or pd.isna(days) or float(days) >= 60:
            return "Да"
        return f"Дефицит {round_int(60 - float(days))} дн."

    def enough_until_arrival(row):
        if pd.isna(row["days_to_arrival"]):
            return ""
        if safe_float(row["daily_demand"]) <= 0:
            return "Да"
        return "Да" if (row["days_total"] is not None and not pd.isna(row["days_total"]) and float(row["days_total"]) >= float(row["days_to_arrival"])) else "Нет"

    df["out_of_stock_days"] = df.apply(out_of_stock_days, axis=1)
    df["enough_60_text"] = df.apply(enough_60_text, axis=1)
    df["enough_until_arrival"] = df.apply(enough_until_arrival, axis=1)

    df = df.merge(rrc, on="article_1c", how="left")
    df["rrc"] = pd.to_numeric(df.get("rrc", 0), errors="coerce").fillna(0).map(round_int)
    df["buyer_price"] = pd.to_numeric(df.get("last_day_price", 0), errors="coerce").fillna(0).map(round_int)
    df["rrc_coef"] = df.apply(lambda r: format_coef_rrc(r["buyer_price"], r["rrc"]), axis=1)
    df["delist_flag"] = df["article_1c"].map(lambda x: "Delist" if normalize_article_code(x) in stop_articles else "")

    # monthly zero-like days threshold = 50% of avg7 demand; filled later by caller merge
    return df


def build_zero_days_current_month(storage: S3Storage, cfg: AppConfig, wb_ids: set[str], avg7_map: dict[str, float]) -> pd.DataFrame:
    keys = [k for k in storage.list_keys(cfg.stocks_prefix) if k.lower().endswith(".xlsx")]
    rows = []
    month_start = cfg.run_date.date().replace(day=1)
    for key in sorted(keys, key=parse_iso_week_from_key):
        try:
            df = storage.read_excel(key)
        except Exception as exc:
            log(f"⚠️ Не удалось прочитать остатки для zero-days {key}: {exc}")
            continue
        wb_col = choose_existing_column(df, ["Артикул WB", "nmId"], "Артикул WB для zero-days")
        stock_col = choose_existing_column(df, ["Доступно для продажи", "Полное количество"], "остаток WB для zero-days")
        sample_col = choose_existing_column(df, ["Дата сбора", "Дата запроса"], "дата среза zero-days")
        tmp = pd.DataFrame()
        tmp["wb_id"] = df[wb_col].map(normalize_key)
        tmp["sample_date"] = pd.to_datetime(df[sample_col], errors="coerce").dt.normalize()
        tmp["stock_wb_qty"] = pd.to_numeric(df[stock_col], errors="coerce").fillna(0)
        tmp = tmp[(tmp["wb_id"].isin(wb_ids)) & tmp["sample_date"].notna()]
        tmp = tmp[tmp["sample_date"].dt.date >= month_start]
        if tmp.empty:
            continue
        tmp = tmp.groupby(["wb_id", "sample_date"], as_index=False).agg(stock_wb_qty=("stock_wb_qty", "sum"))
        rows.append(tmp)
    if not rows:
        return pd.DataFrame(columns=["wb_id", "days_zero_like_month"])
    all_rows = pd.concat(rows, ignore_index=True)

    def is_zero_like(row):
        threshold = 0.5 * float(avg7_map.get(row["wb_id"], 0.0) or 0.0)
        return float(row["stock_wb_qty"]) <= threshold

    all_rows["is_zero_like"] = all_rows.apply(is_zero_like, axis=1)
    out = all_rows.groupby("wb_id", as_index=False).agg(days_zero_like_month=("is_zero_like", "sum"))
    out["days_zero_like_month"] = out["days_zero_like_month"].astype(int)
    return out


def finalize_report(metrics: pd.DataFrame, zero_days_df: pd.DataFrame, cfg: AppConfig) -> pd.DataFrame:
    df = metrics.merge(zero_days_df, on="wb_id", how="left")
    df["days_zero_like_month"] = df["days_zero_like_month"].fillna(0).astype(int)

    # numbers to ints where needed
    int_cols = ["stock_wb_qty", "wb_mp_qty", "inbound_qty", "sales_7d", "sales_60d", "buyer_price", "rrc", "out_of_stock_days"]
    for c in int_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).map(round_int)

    day_cols = ["days_wb", "days_total", "days_total_inbound", "avg_daily_sales_7d", "avg_daily_sales_60d", "daily_demand"]
    for c in day_cols:
        if c in df.columns:
            df[c] = df[c].map(lambda x: None if x is None or pd.isna(x) else round_int(x))

    df = df.sort_values(by="article_1c", key=lambda s: s.map(natural_sort_key)).reset_index(drop=True)
    return df


def split_sheets(df: pd.DataFrame, cfg: AppConfig):
    critical_mask = (
        (df["sales_60d"] > 0)
        & (
            (df["stock_wb_qty"] <= 0)
            | ((df["days_wb"].notna()) & (df["days_wb"] < cfg.low_stock_days_threshold))
            | ((df["inbound_qty"] > 0) & (df["enough_until_arrival"] == "Нет"))
        )
    )
    critical = df[critical_mask].copy()
    critical["Комментарий"] = critical.apply(
        lambda r: f"Не хватает до поставки" if (r["inbound_qty"] > 0 and r["enough_until_arrival"] == "Нет") else "",
        axis=1,
    )

    dead = df[(df["days_total_inbound"].notna()) & (df["days_total_inbound"] > 120)].copy()
    monitor = df[df["sales_60d"] >= 20].copy()
    calc = df.copy()

    critical_out = critical[[
        "article_1c", "sales_60d", "out_of_stock_days", "days_wb", "days_total", "inbound_qty",
        "stock_wb_qty", "wb_mp_qty", "days_zero_like_month", "manager", "Комментарий", "delist_flag"
    ]].copy()
    critical_out.columns = [
        "Артикул 1С", "Продажи 60 дней, шт", "Out of stock, days", "WB хватит, дней", "WB + Липецк, дней", "Товары в пути, шт",
        "Остаток WB, шт", "Остатки МП (Липецк), шт", "Дней без остатка WB в текущем месяце", "Менеджер", "Комментарий", "Статус"
    ]

    calc_out = calc[[
        "article_1c", "manager", "wb_id", "seller_article", "stock_wb_qty", "wb_mp_qty", "inbound_qty", "arrival_date", "days_to_arrival",
        "sales_60d", "sales_7d", "avg_daily_sales_60d", "avg_daily_sales_7d", "daily_demand",
        "days_wb", "days_total", "days_total_inbound", "out_of_stock_days", "enough_60_text", "enough_until_arrival",
        "days_zero_like_month", "buyer_price", "rrc", "rrc_coef", "delist_flag"
    ]].copy()
    calc_out.columns = [
        "Артикул 1С", "Менеджер", "Артикул WB", "Артикул продавца", "Остаток WB, шт", "Остатки МП (Липецк), шт", "Товары в пути, шт", "Дата поступления", "Дней до поступления",
        "Продажи 60 дней, шт", "Продажи 7 дней, шт", "Среднесуточные продажи 60д", "Среднесуточные продажи 7д", "Расчётный спрос в день, шт",
        "WB хватит, дней", "WB + Липецк, дней", "WB + Липецк + в пути, дней", "Out of stock, days", "Хватит на 60 дней", "Хватит до поступления",
        "Дней без остатка WB в текущем месяце", "Цена покупателя", "РРЦ", "Коэффициент", "Статус"
    ]
    calc_out["Дней до поступления"] = calc_out["Дней до поступления"].where(calc_out["Товары в пути, шт"] > 0, "")
    calc_out["Дата поступления"] = calc_out["Дата поступления"].where(calc_out["Товары в пути, шт"] > 0, "")

    dead_out = dead[[
        "article_1c", "manager", "days_wb", "days_total", "days_total_inbound", "stock_wb_qty", "wb_mp_qty", "inbound_qty",
        "sales_60d", "buyer_price", "rrc", "rrc_coef", "delist_flag"
    ]].copy()
    dead_out.columns = [
        "Артикул 1С", "Менеджер", "WB хватит, дней", "WB + Липецк, дней", "WB + Липецк + в пути, дней",
        "Остаток WB, шт", "Остатки МП (Липецк), шт", "Товары в пути, шт",
        "Продажи 60 дней, шт", "Цена покупателя", "РРЦ", "Коэффициент", "Статус"
    ]

    monitor_out = monitor[[
        "article_1c", "sales_60d", "out_of_stock_days", "enough_60_text", "days_total", "days_total_inbound", "inbound_qty",
        "enough_until_arrival", "stock_wb_qty", "wb_mp_qty", "days_zero_like_month", "manager", "delist_flag"
    ]].copy()
    monitor_out.columns = [
        "Артикул 1С", "Продажи 60 дней, шт", "Out of stock, days", "Хватит на 60 дней", "WB + Липецк, дней", "WB + Липецк + в пути, дней",
        "Товары в пути, шт", "Хватит до поступления", "Остаток WB, шт", "Остатки МП (Липецк), шт", "Дней без остатка WB в текущем месяце", "Менеджер", "Статус"
    ]

    return critical_out, calc_out, dead_out, monitor_out


def set_autowidth(ws) -> None:
    for col_cells in ws.columns:
        col_letter = get_column_letter(col_cells[0].column)
        max_len = 0
        for cell in col_cells:
            val = "" if cell.value is None else str(cell.value)
            max_len = max(max_len, max((len(x) for x in val.split("\n")), default=0))
        width = min(max(max_len + 3, 16), 42)
        ws.column_dimensions[col_letter].width = width


def style_sheet(ws, *, price_cols: Optional[set[int]] = None, black_col: Optional[int] = None, orange_trigger_col: Optional[int] = None) -> None:
    price_cols = price_cols or set()
    for row in ws.iter_rows():
        for cell in row:
            cell.border = THIN_BORDER
            cell.alignment = ALIGN_CENTER
            cell.font = Font(name=FONT_NAME, size=FONT_SIZE, color="000000")
    for cell in ws[1]:
        cell.fill = HEADER_FILL
        cell.font = Font(name=FONT_NAME, size=FONT_SIZE, bold=True, color="FFFFFF")
        cell.alignment = ALIGN_CENTER
    for row in ws.iter_rows(min_row=2):
        row[0].alignment = ALIGN_LEFT
    for idx in price_cols:
        for r in range(1, ws.max_row + 1):
            ws.cell(r, idx).fill = LIGHT_GREEN_FILL
    if black_col is not None:
        for r in range(2, ws.max_row + 1):
            cell = ws.cell(r, black_col)
            try:
                v = float(cell.value)
            except Exception:
                v = 0.0
            if v > 180:
                cell.fill = BLACK_FILL
                cell.font = Font(name=FONT_NAME, size=FONT_SIZE, bold=True, color="FFFFFF")
    if orange_trigger_col is not None:
        for r in range(2, ws.max_row + 1):
            text = str(ws.cell(r, orange_trigger_col).value or "")
            if text.startswith("Дефицит"):
                for c in range(1, ws.max_column + 1):
                    ws.cell(r, c).fill = ORANGE_FILL
                    ws.cell(r, c).font = Font(name=FONT_NAME, size=FONT_SIZE, color="000000")
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions
    set_autowidth(ws)


def save_report(critical: pd.DataFrame, calc: pd.DataFrame, dead: pd.DataFrame, monitor: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        critical.to_excel(writer, sheet_name=SHEET_CRITICAL, index=False)
        calc.to_excel(writer, sheet_name=SHEET_CALC, index=False)
        dead.to_excel(writer, sheet_name=SHEET_DEAD, index=False)
        monitor.to_excel(writer, sheet_name=SHEET_MONITOR, index=False)

    wb = load_workbook(out_path)
    ws_critical = wb[SHEET_CRITICAL]
    ws_calc = wb[SHEET_CALC]
    ws_dead = wb[SHEET_DEAD]
    ws_monitor = wb[SHEET_MONITOR]

    style_sheet(ws_critical)
    style_sheet(ws_calc)
    dead_header = [c.value for c in ws_dead[1]]
    price_cols = {dead_header.index(name) + 1 for name in ["Цена покупателя", "РРЦ", "Коэффициент"] if name in dead_header}
    black_col = dead_header.index("WB + Липецк + в пути, дней") + 1 if "WB + Липецк + в пути, дней" in dead_header else None
    style_sheet(ws_dead, price_cols=price_cols, black_col=black_col)
    monitor_header = [c.value for c in ws_monitor[1]]
    orange_col = monitor_header.index("Хватит на 60 дней") + 1 if "Хватит на 60 дней" in monitor_header else None
    style_sheet(ws_monitor, orange_trigger_col=orange_col)

    for ws in [ws_critical, ws_calc, ws_dead, ws_monitor]:
        ws.row_dimensions[1].height = 34
        for r in range(2, ws.max_row + 1):
            ws.row_dimensions[r].height = 24

    wb.save(out_path)


def send_telegram(cfg: AppConfig, file_path: Path, critical_count: int, dead_count: int) -> None:
    if not cfg.send_telegram or not cfg.telegram_bot_token or not cfg.telegram_chat_id:
        log("Отправка в Telegram пропущена")
        return
    url = f"https://api.telegram.org/bot{cfg.telegram_bot_token}/sendDocument"
    caption = f"📦 Отчёт по остаткам WB {cfg.store_name}\nКритично: {critical_count}\nDead_Stock: {dead_count}"
    with open(file_path, "rb") as f:
        resp = requests.post(
            url,
            data={"chat_id": cfg.telegram_chat_id, "caption": caption},
            files={"document": (file_path.name, f)},
            timeout=120,
        )
    resp.raise_for_status()
    log("Отчёт отправлен в Telegram")


def run() -> Path:
    cfg = AppConfig.from_env()
    storage = S3Storage(cfg)
    stop_articles = parse_stop_articles(cfg.stop_articles_raw)

    wb_stocks = load_wb_stocks(storage, cfg)
    sales = load_orders_metrics(storage, cfg)
    article_map = load_article_map_1c(storage, cfg)
    stocks_1c = load_1c_stocks(storage, cfg)
    rrc = load_rrc(storage, cfg)
    inbound = load_inbound(storage, cfg)
    abc = load_abc(storage, cfg)

    metrics = build_metrics(wb_stocks, sales, article_map, stocks_1c, rrc, inbound, abc, stop_articles)
    avg7_map = metrics.groupby("wb_id", as_index=False).agg(avg7=("avg_daily_sales_7d", "max"))
    avg7_map = dict(zip(avg7_map["wb_id"], avg7_map["avg7"]))
    zero_days = build_zero_days_current_month(storage, cfg, set(metrics["wb_id"].dropna().astype(str)), avg7_map)
    report_df = finalize_report(metrics, zero_days, cfg)
    critical, calc, dead, monitor = split_sheets(report_df, cfg)

    out_path = Path(cfg.output_dir) / f"Отчёт_дни_остатка_WB_{cfg.store_name}_{cfg.run_date.strftime('%Y%m%d')}.xlsx"
    save_report(critical, calc, dead, monitor, out_path)
    log(f"Отчёт сохранён: {out_path}")

    if should_send_to_telegram(cfg.run_date, cfg.force_send):
        send_telegram(cfg, out_path, len(critical), len(dead))
    else:
        log("Отправка в Telegram пропущена по расписанию")
    return out_path


if __name__ == "__main__":
    run()
