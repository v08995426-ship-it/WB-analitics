#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import io
import math
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import boto3
import pandas as pd
import requests
from botocore.client import Config as BotoConfig
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.dimensions import ColumnDimension


# =========================
# БАЗОВЫЕ УТИЛИТЫ
# =========================


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}



def env_list(name: str, default: List[str]) -> List[str]:
    value = os.getenv(name)
    if not value:
        return default
    return [item.strip() for item in str(value).split("|") if item.strip()]



def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)



def normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text



def ceil_int(value: object) -> int:
    if pd.isna(value) or value is None or value == "":
        return 0
    return int(math.ceil(float(value)))



def safe_float(value: object) -> float:
    if pd.isna(value) or value is None or value == "":
        return 0.0
    return float(value)



def days_value(stock_qty: float, daily_sales: float) -> Optional[float]:
    if daily_sales <= 0:
        if stock_qty <= 0:
            return 0.0
        return None
    return stock_qty / daily_sales



def fmt_days(value: Optional[float]) -> str:
    if value is None:
        return "∞"
    return f"{value:.1f}"


# =========================
# КОНФИГ
# =========================


@dataclass
class AppConfig:
    bucket_name: str = os.getenv("WB_S3_BUCKET", "")
    access_key: str = os.getenv("WB_S3_ACCESS_KEY", "")
    secret_key: str = os.getenv("WB_S3_SECRET_KEY", "")
    endpoint_url: str = os.getenv("WB_S3_ENDPOINT", "https://storage.yandexcloud.net")
    region_name: str = os.getenv("WB_S3_REGION", "ru-central1")

    store_name: str = os.getenv("WB_STORE", "TOPFACE").strip()
    run_date: datetime = datetime.strptime(
        os.getenv("WB_RUN_DATE", datetime.now().strftime("%Y-%m-%d")),
        "%Y-%m-%d",
    )

    output_dir: str = os.getenv("WB_OUTPUT_DIR", "output")
    days_threshold: float = float(os.getenv("WB_LOW_STOCK_DAYS_THRESHOLD", "14"))
    sales_window_days: int = int(os.getenv("WB_SALES_WINDOW_DAYS", "7"))

    telegram_bot_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    telegram_chat_id: str = os.getenv("TELEGRAM_CHAT_ID", "")
    send_telegram: bool = env_bool("WB_SEND_TELEGRAM", True)

    orders_prefix_tpl: str = os.getenv("WB_ORDERS_PREFIX", "Отчёты/Заказы/{store}/Недельные/")
    stocks_1c_key: str = os.getenv("WB_STOCKS_1C_KEY", "Отчёты/Остатки/1С/Остатки 1С.xlsx")
    local_1c_path: str = os.getenv("WB_LOCAL_1C_PATH", "")

    wb_code_column: str = os.getenv("WB_1C_WB_CODE_COLUMN", "Код_WB")
    article_1c_column: str = os.getenv("WB_1C_ARTICLE_COLUMN", "Артикул")
    mp_stock_column: str = os.getenv("WB_1C_MP_STOCK_COLUMN", "Остатки МП")

    company_stock_columns: List[str] = None
    lipetsk_stock_columns: List[str] = None

    upload_result_to_s3: bool = env_bool("WB_UPLOAD_RESULT_TO_S3", False)
    result_prefix: str = os.getenv("WB_RESULT_PREFIX", "Отчёты/Контроль остатков/{store}/")

    def __post_init__(self) -> None:
        if self.company_stock_columns is None:
            self.company_stock_columns = env_list(
                "WB_COMPANY_STOCK_COLUMNS",
                [
                    "Адресный склад",
                    'Оптовый склад Луганск- ООО "Хайлер"',
                    'Основной склад - ИП Куканянц И.Ю.',
                    'Основной склад - ООО "Хайлер"',
                ],
            )
        if self.lipetsk_stock_columns is None:
            self.lipetsk_stock_columns = env_list(
                "WB_LIPETSK_STOCK_COLUMNS",
                ['Основной склад - ООО "Хайлер"'],
            )


CONFIG = AppConfig()


# =========================
# ХРАНИЛИЩЕ S3
# =========================


class S3Storage:
    def __init__(self, cfg: AppConfig):
        if not cfg.bucket_name or not cfg.access_key or not cfg.secret_key:
            raise ValueError(
                "Не заданы параметры Object Storage. Нужны env: WB_S3_BUCKET, WB_S3_ACCESS_KEY, WB_S3_SECRET_KEY."
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


def parse_week_key_date(key: str) -> Optional[datetime]:
    import re

    match = re.search(r"_(\d{4}-W\d{2})\.xlsx$", key)
    if not match:
        return None
    year, week = match.group(1).split("-W")
    return datetime.fromisocalendar(int(year), int(week), 1)



def load_weekly_window(storage: S3Storage, prefix: str, run_date: datetime, lookback_days: int, expected_sheet: Optional[str] = None) -> pd.DataFrame:
    keys = storage.list_keys(prefix)
    if not keys:
        raise FileNotFoundError(f"В Object Storage не найдено файлов по префиксу: {prefix}")

    cutoff = run_date - timedelta(days=lookback_days + 14)
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
    required = ["date", "supplierArticle", "nmId"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"В заказах отсутствуют обязательные колонки: {missing}")

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
    start_date = cfg.run_date - timedelta(days=cfg.sales_window_days - 1)
    df = df[(df["date"] >= start_date) & (df["date"] <= cfg.run_date)].copy()

    if "isCancel" in df.columns:
        df = df[~df["isCancel"].fillna(False)].copy()

    df["nmId"] = df["nmId"].map(normalize_text)
    df["supplierArticle"] = df["supplierArticle"].map(normalize_text)
    df["qty"] = 1
    return df



def load_1c_stocks(storage: Optional[S3Storage], cfg: AppConfig) -> pd.DataFrame:
    if cfg.local_1c_path:
        path = Path(cfg.local_1c_path)
        if not path.exists():
            raise FileNotFoundError(f"Локальный файл 1С не найден: {path}")
        df = pd.read_excel(path)
        log(f"Загружен локальный файл 1С: {path}")
        return df

    if storage is None:
        raise ValueError("storage должен быть задан, если локальный файл 1С не используется")

    df = storage.read_excel(cfg.stocks_1c_key)
    if isinstance(df, dict):
        df = next(iter(df.values()))
    log(f"Загружен файл 1С из Object Storage: {cfg.stocks_1c_key}")
    return df


# =========================
# РАСЧЁТЫ
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

    required = [cfg.wb_code_column, cfg.article_1c_column, cfg.mp_stock_column]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"В файле 1С отсутствуют обязательные колонки: {missing}")

    for col in set(cfg.company_stock_columns + cfg.lipetsk_stock_columns):
        if col not in df.columns:
            df[col] = 0

    df["nmId"] = df[cfg.wb_code_column].map(normalize_text)
    df["article_1c"] = df[cfg.article_1c_column].map(normalize_text)
    df["wb_mp_qty"] = pd.to_numeric(df[cfg.mp_stock_column], errors="coerce").fillna(0).map(ceil_int)

    company_cols_existing = [col for col in cfg.company_stock_columns if col in df.columns]
    lipetsk_cols_existing = [col for col in cfg.lipetsk_stock_columns if col in df.columns]

    df["company_stock_qty"] = (
        df[company_cols_existing].apply(pd.to_numeric, errors="coerce").fillna(0).sum(axis=1)
        if company_cols_existing else 0
    )
    df["lipetsk_stock_qty"] = (
        df[lipetsk_cols_existing].apply(pd.to_numeric, errors="coerce").fillna(0).sum(axis=1)
        if lipetsk_cols_existing else 0
    )

    # если один и тот же Код_WB встречается несколько раз, агрегируем
    agg = (
        df[df["nmId"] != ""]
        .groupby(["nmId", "article_1c"], as_index=False)
        .agg(
            wb_mp_qty=("wb_mp_qty", "sum"),
            company_stock_qty=("company_stock_qty", "sum"),
            lipetsk_stock_qty=("lipetsk_stock_qty", "sum"),
        )
    )
    return agg



def build_report_dataframe(sales_df: pd.DataFrame, stocks_1c_df: pd.DataFrame) -> pd.DataFrame:
    merged = stocks_1c_df.merge(sales_df, on="nmId", how="left")
    merged["supplierArticle"] = merged["supplierArticle"].fillna("")
    merged["sales_7d"] = merged["sales_7d"].fillna(0).astype(int)
    merged["avg_daily_sales_7d"] = merged["avg_daily_sales_7d"].fillna(0.0)

    merged["days_wb"] = merged.apply(lambda x: days_value(x["wb_mp_qty"], x["avg_daily_sales_7d"]), axis=1)
    merged["days_lipetsk"] = merged.apply(lambda x: days_value(x["lipetsk_stock_qty"], x["avg_daily_sales_7d"]), axis=1)
    merged["days_total"] = merged.apply(
        lambda x: days_value(x["wb_mp_qty"] + x["company_stock_qty"], x["avg_daily_sales_7d"]),
        axis=1,
    )

    merged["days_wb_text"] = merged["days_wb"].map(fmt_days)
    merged["days_lipetsk_text"] = merged["days_lipetsk"].map(fmt_days)
    merged["days_total_text"] = merged["days_total"].map(fmt_days)

    merged = merged.sort_values(
        by=["days_wb"],
        key=lambda s: s.fillna(10**9),
        ascending=True,
    ).reset_index(drop=True)
    return merged


# =========================
# EXCEL ОТЧЁТ
# =========================


HEADER_FILL = PatternFill("solid", fgColor="1F4E78")
SUBHEADER_FILL = PatternFill("solid", fgColor="D9EAF7")
ALERT_FILL = PatternFill("solid", fgColor="FDE9D9")
CRITICAL_FILL = PatternFill("solid", fgColor="F4CCCC")
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
        cell.font = Font(name="Calibri", size=14, bold=True, color="FFFFFF")
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = BORDER



def style_body(ws, start_row: int, end_row: int, numeric_cols: List[int], threshold_col: Optional[int] = None) -> None:
    for row in ws.iter_rows(min_row=start_row, max_row=end_row):
        for cell in row:
            cell.font = Font(name="Calibri", size=14)
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            cell.border = BORDER

        if threshold_col:
            target = ws.cell(row=row[0].row, column=threshold_col).value
            try:
                target = float(target)
            except Exception:
                target = None
            if target is not None:
                fill = CRITICAL_FILL if target < 7 else ALERT_FILL if target < 14 else None
                if fill:
                    for cell in row:
                        cell.fill = fill

    for col_idx in numeric_cols:
        for r in range(start_row, end_row + 1):
            ws.cell(r, col_idx).number_format = '0.0'



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

    # Лист 1 — только критичные товары
    ws_short["A1"] = f"Контроль остатка WB — товары менее {int(cfg.days_threshold)} дней на {report_date_text}"
    ws_short.merge_cells("A1:G1")
    ws_short["A1"].font = Font(name="Calibri", size=16, bold=True, color="FFFFFF")
    ws_short["A1"].fill = HEADER_FILL
    ws_short["A1"].alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    short_headers = [
        "Артикул 1С",
        "Код WB",
        "Среднесуточные продажи\nза 7 дней, шт",
        "Остатка WB\nхватит, дней",
        "С учётом склада\nкомпании, дней",
        "В Липецке есть\nзапас, дней",
        "Остатки МП\nWB, шт",
    ]
    ws_short.append([])
    ws_short.append(short_headers)
    style_header_row(ws_short, 3)

    short_df = report_df[
        (report_df["avg_daily_sales_7d"] > 0)
        & (report_df["days_wb"].notna())
        & (report_df["days_wb"] < cfg.days_threshold)
    ].copy()

    for _, row in short_df.iterrows():
        ws_short.append([
            row["article_1c"],
            row["nmId"],
            row["avg_daily_sales_7d"],
            row["days_wb"],
            row["days_total"],
            row["days_lipetsk"],
            row["wb_mp_qty"],
        ])

    if ws_short.max_row >= 4:
        style_body(ws_short, 4, ws_short.max_row, numeric_cols=[3, 4, 5, 6], threshold_col=4)
    autofit_layout(
        ws_short,
        widths={"A": 28, "B": 15, "C": 20, "D": 18, "E": 20, "F": 18, "G": 14},
        row_heights={1: 28, 3: 38},
    )
    apply_freeze_and_filter(ws_short, f"A3:G{max(ws_short.max_row, 3)}")

    # Лист 2 — полный расчёт
    ws_calc["A1"] = f"Расчёт дней до конца остатка WB — {cfg.store_name} — {report_date_text}"
    ws_calc.merge_cells("A1:K1")
    ws_calc["A1"].font = Font(name="Calibri", size=16, bold=True, color="FFFFFF")
    ws_calc["A1"].fill = HEADER_FILL
    ws_calc["A1"].alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    calc_headers = [
        "Артикул 1С",
        "Код WB",
        "Артикул WB",
        "Продажи\nза 7 дней, шт",
        "Среднесуточные продажи\nза 7 дней, шт",
        "Остатки МП\nWB, шт",
        "Склад компании,\nшт",
        "Липецк,\nшт",
        "WB хватит,\nдней",
        "С Липецком,\nдней",
        "Склад компании + WB,\nдней",
    ]
    ws_calc.append([])
    ws_calc.append(calc_headers)
    style_header_row(ws_calc, 3)

    for _, row in report_df.iterrows():
        ws_calc.append([
            row["article_1c"],
            row["nmId"],
            row["supplierArticle"],
            row["sales_7d"],
            row["avg_daily_sales_7d"],
            row["wb_mp_qty"],
            row["company_stock_qty"],
            row["lipetsk_stock_qty"],
            row["days_wb"],
            row["days_lipetsk"],
            row["days_total"],
        ])

    if ws_calc.max_row >= 4:
        style_body(ws_calc, 4, ws_calc.max_row, numeric_cols=[4, 5, 6, 7, 8, 9, 10, 11], threshold_col=9)
    autofit_layout(
        ws_calc,
        widths={"A": 28, "B": 15, "C": 20, "D": 14, "E": 20, "F": 14, "G": 16, "H": 12, "I": 14, "J": 14, "K": 18},
        row_heights={1: 28, 3: 42},
    )
    apply_freeze_and_filter(ws_calc, f"A3:K{max(ws_calc.max_row, 3)}")

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
    storage = None

    if not cfg.local_1c_path:
        storage = S3Storage(cfg)

    log(
        f"Старт отчёта по остаткам. store={cfg.store_name}, "
        f"run_date={cfg.run_date:%Y-%m-%d}, threshold_days={cfg.days_threshold}, sales_window_days={cfg.sales_window_days}"
    )

    if storage is None:
        raise ValueError("Для расчёта нужен доступ к Object Storage, потому что продажи берутся из отчётов WB в S3")

    orders = load_orders(storage, cfg)
    stocks_1c_raw = load_1c_stocks(storage, cfg)

    sales_df = build_sales_metrics(orders, cfg)
    stocks_1c_df = prepare_1c_dataset(stocks_1c_raw, cfg)
    report_df = build_report_dataframe(sales_df, stocks_1c_df)

    output_path = str(Path(cfg.output_dir) / f"WB_остаток_по_дням_{cfg.store_name}_{cfg.run_date:%Y%m%d}.xlsx")
    create_excel_report(report_df, cfg, output_path)
    log(f"Сформирован отчёт: {output_path}")

    if cfg.upload_result_to_s3:
        result_key = cfg.result_prefix.format(store=cfg.store_name) + Path(output_path).name
        storage.upload_file(output_path, result_key)
        log(f"Отчёт загружен в Object Storage: {result_key}")

    if cfg.send_telegram or force_send:
        caption = f"{cfg.store_name} | Остаток WB в днях | {cfg.run_date:%Y-%m-%d}"
        send_telegram_document(cfg.telegram_bot_token, cfg.telegram_chat_id, output_path, caption)
        log("Отчёт отправлен в Telegram")

    return output_path



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Отчёт по дням до конца остатка WB")
    parser.add_argument("command", nargs="?", default="run", choices=["run"], help="Доступна команда run")
    parser.add_argument("--force-send", action="store_true", help="Отправить в Telegram, даже если WB_SEND_TELEGRAM=false")
    parser.add_argument("--local-1c-path", help="Локальный путь к файлу Остатки 1С.xlsx")
    parser.add_argument("--run-date", help="Дата запуска в формате YYYY-MM-DD")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    cfg = CONFIG
    if args.local_1c_path:
        cfg.local_1c_path = args.local_1c_path
    if args.run_date:
        cfg.run_date = datetime.strptime(args.run_date, "%Y-%m-%d")

    run(cfg=cfg, force_send=args.force_send)
