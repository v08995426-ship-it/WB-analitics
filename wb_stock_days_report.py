#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import io
import math
import os
import re
from dataclasses import dataclass
from datetime import datetime

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
OUT_DIR = "output"

SHEET_CRITICAL = "Критично <14 дней"
SHEET_CALC = "Расчёт"
SHEET_DEAD = "Dead_Stock"

FONT_NAME = "Calibri"
FONT_SIZE = 14

FILL_HEADER = PatternFill("solid", fgColor="D9EAF7")
FILL_LIGHT_GREEN = PatternFill("solid", fgColor="CCFFCC")
FILL_BLACK = PatternFill("solid", fgColor="000000")
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


def now_log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


def get_config() -> Config:
    bucket = (os.getenv("YC_BUCKET_NAME") or os.getenv("CLOUD_RU_BUCKET") or os.getenv("WB_S3_BUCKET") or "").strip()
    access_key = (os.getenv("YC_ACCESS_KEY_ID") or os.getenv("CLOUD_RU_ACCESS_KEY") or os.getenv("WB_S3_ACCESS_KEY") or "").strip()
    secret_key = (os.getenv("YC_SECRET_ACCESS_KEY") or os.getenv("CLOUD_RU_SECRET_KEY") or os.getenv("WB_S3_SECRET_KEY") or "").strip()
    endpoint_url = (os.getenv("YC_ENDPOINT_URL") or os.getenv("WB_S3_ENDPOINT") or "https://storage.yandexcloud.net").strip()
    region_name = (os.getenv("WB_S3_REGION") or "ru-central1").strip()

    if not bucket or not access_key or not secret_key:
        raise ValueError("Не заданы параметры Object Storage.")

    telegram_bot_token = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    telegram_chat_id = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()
    stop_articles_raw = os.getenv("WB_STOP_LIST_KEY", "")
    force_send = (os.getenv("WB_FORCE_SEND", "false").strip().lower() == "true")

    return Config(
        bucket=bucket,
        access_key=access_key,
        secret_key=secret_key,
        endpoint_url=endpoint_url,
        region_name=region_name,
        telegram_bot_token=telegram_bot_token,
        telegram_chat_id=telegram_chat_id,
        stop_articles_raw=stop_articles_raw,
        force_send=force_send,
    )


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

    def list_keys(self, prefix: str):
        keys = []
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

    def read_bytes(self, key: str) -> bytes:
        obj = self.client.get_object(Bucket=self.bucket, Key=key)
        return obj["Body"].read()

    def read_excel(self, key: str, **kwargs) -> pd.DataFrame:
        return pd.read_excel(io.BytesIO(self.read_bytes(key)), **kwargs)


def normalize_str(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip().upper()


def split_stop_articles(raw: str):
    items = re.split(r"[,;\n\t]+", raw or "")
    return {normalize_str(x) for x in items if normalize_str(x)}


def choose_existing_column(df: pd.DataFrame, candidates, label: str) -> str:
    for c in candidates:
        if c in df.columns:
            return c
    raise KeyError(f"Не найдена колонка для '{label}'. Доступные колонки: {list(df.columns)}")


def parse_week_from_key(key: str):
    m = re.search(r"(\d{4})-W(\d{1,2})", key)
    if not m:
        return (0, 0)
    return (int(m.group(1)), int(m.group(2)))


def latest_weekly_key(keys):
    keys = [k for k in keys if k.lower().endswith(".xlsx")]
    if not keys:
        raise FileNotFoundError("Не найдены xlsx файлы.")
    return sorted(keys, key=parse_week_from_key)[-1]


def latest_n_weekly_keys(keys, n):
    keys = [k for k in keys if k.lower().endswith(".xlsx")]
    return sorted(keys, key=parse_week_from_key)[-n:]


def to_numeric(series):
    return pd.to_numeric(series, errors="coerce").fillna(0.0)


def safe_div(num, den):
    if den is None or pd.isna(den) or float(den) <= 0:
        return 0.0
    return float(num) / float(den)


def natural_sort_key(article: str):
    s = str(article)
    return [int(t) if t.isdigit() else t.lower() for t in re.split(r"(\d+)", s)]


def round_up_int(x):
    if pd.isna(x):
        return 0
    return int(math.ceil(float(x)))


def round_int(x):
    if pd.isna(x):
        return 0
    return int(round(float(x)))


def format_coef_rrc(price, rrc):
    if pd.isna(price) or pd.isna(rrc) or float(rrc) <= 0:
        return ""
    coef = float(price) / float(rrc)
    return f"{coef:.2f}".replace(".", ",") + "_РРЦ"


def should_send_report(cfg: Config) -> bool:
    if cfg.force_send:
        now_log("Ручной запуск — отправляем всегда")
        return True
    wd = datetime.now().weekday()
    if wd in (0, 4):
        return True
    now_log("Отправка в Telegram пропущена по расписанию")
    return False


def load_article_map(storage: S3Storage):
    df = storage.read_excel(ARTICLE_MAP_KEY)
    wb_col = df.columns[0]
    article_col = df.columns[2]
    temp = df[[wb_col, article_col]].copy()
    temp.columns = ["wb_key", "Артикул 1С"]
    temp["wb_key"] = temp["wb_key"].map(normalize_str)
    temp["Артикул 1С"] = temp["Артикул 1С"].astype(str).str.strip()
    temp = temp[(temp["wb_key"] != "") & (temp["Артикул 1С"] != "")]
    temp = temp.drop_duplicates(subset=["wb_key"], keep="first")
    res = dict(zip(temp["wb_key"], temp["Артикул 1С"]))
    now_log(f"Загружено соответствий WB -> 1С: {len(res)}")
    return res


def load_stocks_1c(storage: S3Storage):
    df = storage.read_excel(STOCKS_1C_KEY)
    article_col = choose_existing_column(df, ["Артикул", "АРТ", "Артикул 1С"], "Артикул 1С")
    lipetsk_col = choose_existing_column(df, ["Остатки МП", "Остатки МП (Липецк), шт", "Остатки МП(Липецк), шт"], "Остатки МП")
    temp = df[[article_col, lipetsk_col]].copy()
    temp.columns = ["Артикул 1С", "Остатки МП (Липецк), шт"]
    temp["Артикул 1С"] = temp["Артикул 1С"].astype(str).str.strip()
    temp["Остатки МП (Липецк), шт"] = to_numeric(temp["Остатки МП (Липецк), шт"]).map(round_up_int)
    temp = temp[(temp["Артикул 1С"] != "")].drop_duplicates(subset=["Артикул 1С"], keep="first")
    return temp


def load_rrc(storage: S3Storage):
    df = storage.read_excel(RRC_KEY)
    article_col = df.columns[0]
    rrc_col = df.columns[3]
    temp = df[[article_col, rrc_col]].copy()
    temp.columns = ["Артикул 1С", "РРЦ"]
    temp["Артикул 1С"] = temp["Артикул 1С"].astype(str).str.strip()
    temp["РРЦ"] = to_numeric(temp["РРЦ"]).map(round_int)
    temp = temp[(temp["Артикул 1С"] != "")].drop_duplicates(subset=["Артикул 1С"], keep="first")
    return temp


def load_latest_wb_stocks(storage: S3Storage):
    latest_key = latest_weekly_key(storage.list_keys(WB_STOCKS_PREFIX))
    now_log(f"Берём остатки WB из файла: {latest_key}")
    df = storage.read_excel(latest_key)
    stock_key = choose_existing_column(df, ["nmId", "Артикул WB"], "ключ WB товара")
    qty_col = choose_existing_column(df, ["Доступно для продажи", "Полное количество", "Остатки", "Остаток", "Количество"], "остаток WB")
    temp = df[[stock_key, qty_col]].copy()
    temp.columns = ["wb_key", "Остаток WB, шт"]
    temp["wb_key"] = temp["wb_key"].map(normalize_str)
    temp["Остаток WB, шт"] = to_numeric(temp["Остаток WB, шт"])
    temp = temp[temp["wb_key"] != ""]
    temp = temp.groupby("wb_key", as_index=False)["Остаток WB, шт"].sum()
    temp["Остаток WB, шт"] = temp["Остаток WB, шт"].map(round_int)
    return temp, latest_key


def load_orders_windows(storage: S3Storage):
    order_keys = latest_n_weekly_keys(storage.list_keys(WB_ORDERS_PREFIX), 10)
    now_log(f"Берём заказы WB из файлов: {order_keys}")
    frames = []
    for key in order_keys:
        df = storage.read_excel(key)
        df["_source_key"] = key
        frames.append(df)
    orders = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return orders, order_keys


def build_sales_by_windows(orders: pd.DataFrame):
    empty_cols = [
        "wb_key",
        "Продажи 7 дней, шт",
        "Продажи 60 дней, шт",
        "Среднесуточные продажи 7д",
        "Среднесуточные продажи 60д",
        "Цена покупателя",
    ]
    if orders.empty:
        return pd.DataFrame(columns=empty_cols)

    key_col = choose_existing_column(orders, ["nmId", "Артикул WB"], "ключ товара в заказах")
    date_col = choose_existing_column(orders, ["Дата", "date", "Дата заказа", "lastChangeDate", "Дата продажи"], "дата в заказах")

    work = orders.copy()
    work["wb_key"] = work[key_col].map(normalize_str)
    work["dt"] = pd.to_datetime(work[date_col], errors="coerce").dt.normalize()
    work = work[(work["wb_key"] != "") & work["dt"].notna()].copy()
    if work.empty:
        return pd.DataFrame(columns=empty_cols)

    max_dt = work["dt"].max()
    start_7 = max_dt - pd.Timedelta(days=6)
    start_60 = max_dt - pd.Timedelta(days=59)

    s7 = work[work["dt"] >= start_7].groupby("wb_key").size().rename("Продажи 7 дней, шт")
    s60 = work[work["dt"] >= start_60].groupby("wb_key").size().rename("Продажи 60 дней, шт")
    res = pd.concat([s7, s60], axis=1).fillna(0).reset_index()
    res["Продажи 7 дней, шт"] = res["Продажи 7 дней, шт"].astype(int)
    res["Продажи 60 дней, шт"] = res["Продажи 60 дней, шт"].astype(int)
    res["Среднесуточные продажи 7д"] = res["Продажи 7 дней, шт"] / 7.0
    res["Среднесуточные продажи 60д"] = res["Продажи 60 дней, шт"] / 60.0

    if "finishedPrice" in work.columns:
        price_last = (
            work[work["dt"] == max_dt]
            .groupby("wb_key")["finishedPrice"]
            .mean()
            .rename("Цена покупателя")
            .reset_index()
        )
        price_last["Цена покупателя"] = to_numeric(price_last["Цена покупателя"]).map(round_int)
        res = res.merge(price_last, on="wb_key", how="left")
    else:
        res["Цена покупателя"] = 0

    return res


def load_current_month_zero_days(storage: S3Storage, zero_wb_keys):
    if not zero_wb_keys:
        return {}

    month_start = datetime.now().date().replace(day=1)
    rows = []

    for key in sorted(storage.list_keys(WB_STOCKS_PREFIX), key=parse_week_from_key):
        if not key.lower().endswith(".xlsx"):
            continue
        try:
            df = storage.read_excel(key)
        except Exception:
            continue

        key_col = choose_existing_column(df, ["nmId", "Артикул WB"], "ключ товара в остатках месяца")
        qty_col = choose_existing_column(df, ["Доступно для продажи", "Полное количество"], "остаток WB в остатках месяца")
        dt_col = choose_existing_column(df, ["Дата сбора", "Дата запроса"], "дата среза остатков")

        temp = df[[key_col, qty_col, dt_col]].copy()
        temp.columns = ["wb_key", "stock_wb", "sample_dt"]
        temp["wb_key"] = temp["wb_key"].map(normalize_str)
        temp["stock_wb"] = to_numeric(temp["stock_wb"])
        temp["sample_dt"] = pd.to_datetime(temp["sample_dt"], errors="coerce").dt.normalize()

        temp = temp[(temp["wb_key"].isin(zero_wb_keys)) & temp["sample_dt"].notna()]
        temp = temp[temp["sample_dt"].dt.date >= month_start]
        if temp.empty:
            continue

        temp = temp.groupby(["wb_key", "sample_dt"], as_index=False)["stock_wb"].sum()
        rows.append(temp)

    if not rows:
        return {}

    month_df = pd.concat(rows, ignore_index=True)
    month_df = month_df.groupby(["wb_key", "sample_dt"], as_index=False)["stock_wb"].sum()
    month_df["is_zero"] = month_df["stock_wb"] <= 0
    out = month_df.groupby("wb_key")["is_zero"].sum().to_dict()
    return {k: int(v) for k, v in out.items()}


def select_daily_sales(row):
    stock_wb = float(row.get("Остаток WB, шт", 0) or 0)
    avg7 = float(row.get("Среднесуточные продажи 7д", 0) or 0)
    avg60 = float(row.get("Среднесуточные продажи 60д", 0) or 0)
    if stock_wb <= 0 or avg7 <= 0:
        return avg60
    return avg7


def build_report_dataframe(wb_stocks, sales, article_map, stocks_1c, stop_articles, rrc_df, zero_days_map):
    df = wb_stocks.merge(sales, on="wb_key", how="left")

    for col in [
        "Продажи 7 дней, шт",
        "Продажи 60 дней, шт",
        "Среднесуточные продажи 7д",
        "Среднесуточные продажи 60д",
        "Цена покупателя",
    ]:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["Артикул 1С"] = df["wb_key"].map(article_map).fillna("").astype(str).str.strip()
    df = df[(df["Артикул 1С"] != "") & (~df["Артикул 1С"].str.startswith("PT104", na=False))].copy()

    df = df.merge(stocks_1c, on="Артикул 1С", how="left")
    df["Остатки МП (Липецк), шт"] = pd.to_numeric(df["Остатки МП (Липецк), шт"], errors="coerce").fillna(0).map(round_up_int)

    df["Расчётный спрос в день, шт"] = df.apply(select_daily_sales, axis=1)
    df["WB хватит, дней"] = df.apply(lambda r: safe_div(r["Остаток WB, шт"], r["Расчётный спрос в день, шт"]), axis=1).map(round_int)
    df["Липецк хватит, дней"] = df.apply(lambda r: safe_div(r["Остатки МП (Липецк), шт"], r["Расчётный спрос в день, шт"]), axis=1).map(round_int)
    df["WB + Липецк, дней"] = df.apply(
        lambda r: safe_div(r["Остаток WB, шт"] + r["Остатки МП (Липецк), шт"], r["Расчётный спрос в день, шт"]),
        axis=1,
    ).map(round_int)

    df["Дней без остатка WB в текущем месяце"] = df["wb_key"].map(zero_days_map).fillna(0).astype(int)
    df.loc[df["Остаток WB, шт"] > 0, "Дней без остатка WB в текущем месяце"] = 0
    df["Delist"] = df["Артикул 1С"].map(lambda x: "Delist" if normalize_str(x) in stop_articles else "")

    df = df.merge(rrc_df, on="Артикул 1С", how="left")
    df["РРЦ"] = pd.to_numeric(df["РРЦ"], errors="coerce").fillna(0).map(round_int)
    df["Цена покупателя"] = pd.to_numeric(df["Цена покупателя"], errors="coerce").fillna(0).map(round_int)
    df["Коэффициент"] = df.apply(lambda r: format_coef_rrc(r["Цена покупателя"], r["РРЦ"]), axis=1)

    result_cols = [
        "Артикул 1С",
        "Остаток WB, шт",
        "Продажи 7 дней, шт",
        "Продажи 60 дней, шт",
        "Среднесуточные продажи 7д",
        "Среднесуточные продажи 60д",
        "Расчётный спрос в день, шт",
        "WB хватит, дней",
        "Остатки МП (Липецк), шт",
        "Липецк хватит, дней",
        "WB + Липецк, дней",
        "Дней без остатка WB в текущем месяце",
        "Цена покупателя",
        "РРЦ",
        "Коэффициент",
        "Delist",
    ]
    df = df[result_cols].copy()

    agg = {
        "Остаток WB, шт": "sum",
        "Продажи 7 дней, шт": "sum",
        "Продажи 60 дней, шт": "sum",
        "Среднесуточные продажи 7д": "max",
        "Среднесуточные продажи 60д": "max",
        "Расчётный спрос в день, шт": "max",
        "WB хватит, дней": "max",
        "Остатки МП (Липецк), шт": "max",
        "Липецк хватит, дней": "max",
        "WB + Липецк, дней": "max",
        "Дней без остатка WB в текущем месяце": "max",
        "Цена покупателя": "max",
        "РРЦ": "max",
        "Коэффициент": "first",
        "Delist": "first",
    }
    df = df.groupby("Артикул 1С", as_index=False).agg(agg)

    for col in ["Среднесуточные продажи 7д", "Среднесуточные продажи 60д", "Расчётный спрос в день, шт"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).map(round_int)

    df = df.sort_values(by="Артикул 1С", key=lambda s: s.map(natural_sort_key)).reset_index(drop=True)
    return df


def split_report_sheets(df):
    critical = df[
        (df["Продажи 60 дней, шт"] > 0)
        & ((df["Остаток WB, шт"] <= 0) | (df["WB хватит, дней"] < 14))
    ].copy()

    calc = df.copy()
    dead = df[df["WB + Липецк, дней"] > 120].copy()

    critical = critical[
        [
            "Артикул 1С",
            "WB хватит, дней",
            "Липецк хватит, дней",
            "Остаток WB, шт",
            "Остатки МП (Липецк), шт",
            "Продажи 60 дней, шт",
            "Дней без остатка WB в текущем месяце",
            "Delist",
        ]
    ].copy()

    dead = dead[
        [
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
            "Delist",
        ]
    ].copy()

    dead = dead.sort_values(by="Артикул 1С", key=lambda s: s.map(natural_sort_key)).reset_index(drop=True)
    return critical, calc, dead


def auto_fit_ws(ws):
    widths = {}
    for row in ws.iter_rows():
        for cell in row:
            text = "" if cell.value is None else str(cell.value)
            cell_len = max((len(x) for x in text.split("\n")), default=0)
            widths[cell.column] = max(widths.get(cell.column, 0), cell_len)
    for col_idx, width in widths.items():
        ws.column_dimensions[get_column_letter(col_idx)].width = min(max(width + 2, 14), 36)


def style_ws(ws, price_cols=None, dead_stock_days_col=None):
    price_cols = price_cols or set()

    for row in ws.iter_rows():
        for cell in row:
            cell.font = Font(name=FONT_NAME, size=FONT_SIZE, color="000000")
            cell.alignment = ALIGN_CENTER
            cell.border = BORDER_THIN

    for cell in ws[1]:
        cell.fill = FILL_HEADER
        cell.font = Font(name=FONT_NAME, size=FONT_SIZE, bold=True, color="000000")

    for row in ws.iter_rows(min_row=2):
        row[0].alignment = ALIGN_LEFT

    for idx in price_cols:
        for r in range(1, ws.max_row + 1):
            ws.cell(r, idx).fill = FILL_LIGHT_GREEN

    if dead_stock_days_col:
        for r in range(2, ws.max_row + 1):
            cell = ws.cell(r, dead_stock_days_col)
            try:
                val = float(cell.value)
            except Exception:
                val = 0.0
            if val > 180:
                cell.fill = FILL_BLACK
                cell.font = Font(name=FONT_NAME, size=FONT_SIZE, color="000000")

    auto_fit_ws(ws)
    ws.freeze_panes = "A2"


def save_report_xlsx(critical_df, calc_df, dead_df, out_path: str):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        critical_df.to_excel(writer, sheet_name=SHEET_CRITICAL, index=False, startrow=0)
        calc_df.to_excel(writer, sheet_name=SHEET_CALC, index=False, startrow=0)
        dead_df.to_excel(writer, sheet_name=SHEET_DEAD, index=False, startrow=0)

    wb = load_workbook(out_path)
    ws_critical = wb[SHEET_CRITICAL]
    ws_calc = wb[SHEET_CALC]
    ws_dead = wb[SHEET_DEAD]

    style_ws(ws_critical)
    style_ws(ws_calc)

    header = [c.value for c in ws_dead[1]]
    price_cols = set()
    for name in ["Цена покупателя", "РРЦ", "Коэффициент"]:
        if name in header:
            price_cols.add(header.index(name) + 1)

    days_col = header.index("WB + Липецк, дней") + 1 if "WB + Липецк, дней" in header else None
    style_ws(ws_dead, price_cols=price_cols, dead_stock_days_col=days_col)

    for ws
