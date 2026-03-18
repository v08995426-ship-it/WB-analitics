#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import io
import math
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple

import boto3
import pandas as pd
import requests
from botocore.client import Config as BotoConfig
from openpyxl import load_workbook
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
    if pd.isna(value):
        return ""
    return str(value).strip()



def normalize_nmid(value: object) -> str:
    text = normalize_text(value)
    return text[:-2] if text.endswith(".0") else text



def normalize_article_1c(value: object) -> str:
    return normalize_text(value).upper()



def ceil_int(value: object) -> int:
    if pd.isna(value) or normalize_text(value) == "":
        return 0
    return int(math.ceil(float(value)))



def find_first_existing(columns: Sequence[str], candidates: Sequence[str]) -> Optional[str]:
    lowered = {normalize_text(c).lower(): c for c in columns}
    for candidate in candidates:
        original = lowered.get(candidate.lower())
        if original is not None:
            return original
    return None



def parse_stop_list_env(raw: str) -> Set[str]:
    if not raw:
        return set()
    normalized = raw.replace(";", "\n").replace(",", "\n")
    result: Set[str] = set()
    for item in normalized.splitlines():
        val = normalize_article_1c(item)
        if val:
            result.add(val)
    return result



def iso_week_monday(year: int, week: int) -> datetime:
    return datetime.fromisocalendar(year, week, 1)



def extract_week_from_key(key: str) -> Optional[datetime]:
    match = re.search(r"(\d{4})-W(\d{2})", key)
    if not match:
        return None
    year = int(match.group(1))
    week = int(match.group(2))
    return iso_week_monday(year, week)


@dataclass
class AppConfig:
    bucket_name: str
    access_key: str
    secret_key: str
    endpoint_url: str
    region_name: str
    store_name: str
    telegram_bot_token: str
    telegram_chat_id: str
    send_telegram: bool
    upload_result_to_s3: bool
    result_prefix: str
    run_date: datetime
    output_dir: str
    stock_prefix_tpl: str
    orders_prefix_tpl: str
    article_map_1c_key: str
    stocks_1c_key: str
    stop_list_articles: Set[str]
    critical_days_threshold: float
    sheet_stocks_name: str
    sheet_orders_name: str

    @classmethod
    def from_env(cls, run_date: Optional[str] = None) -> "AppConfig":
        bucket_name = (
            os.getenv("WB_S3_BUCKET")
            or os.getenv("YC_BUCKET_NAME")
            or os.getenv("CLOUD_RU_BUCKET")
            or ""
        ).strip()
        access_key = (
            os.getenv("WB_S3_ACCESS_KEY")
            or os.getenv("YC_ACCESS_KEY_ID")
            or os.getenv("CLOUD_RU_ACCESS_KEY")
            or ""
        ).strip()
        secret_key = (
            os.getenv("WB_S3_SECRET_KEY")
            or os.getenv("YC_SECRET_ACCESS_KEY")
            or os.getenv("CLOUD_RU_SECRET_KEY")
            or ""
        ).strip()

        endpoint_url = (
            os.getenv("WB_S3_ENDPOINT")
            or os.getenv("YC_ENDPOINT_URL")
            or os.getenv("CLOUD_RU_ENDPOINT_URL")
            or "https://storage.yandexcloud.net"
        ).strip()
        region_name = os.getenv("WB_S3_REGION", "ru-central1").strip()
        store_name = os.getenv("WB_STORE", "TOPFACE").strip() or "TOPFACE"

        run_date_value = (
            datetime.strptime(run_date, "%Y-%m-%d")
            if run_date
            else datetime.strptime(
                os.getenv("WB_RUN_DATE", datetime.now().strftime("%Y-%m-%d")),
                "%Y-%m-%d",
            )
        )

        return cls(
            bucket_name=bucket_name,
            access_key=access_key,
            secret_key=secret_key,
            endpoint_url=endpoint_url,
            region_name=region_name,
            store_name=store_name,
            telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
            telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", "").strip(),
            send_telegram=env_bool("WB_SEND_TELEGRAM", True),
            upload_result_to_s3=env_bool("WB_UPLOAD_RESULT_TO_S3", False),
            result_prefix=os.getenv("WB_RESULT_PREFIX", f"Отчёты/Остатки/{store_name}/Отчёт дней остатка/").strip(),
            run_date=run_date_value,
            output_dir=os.getenv("WB_OUTPUT_DIR", "output").strip() or "output",
            stock_prefix_tpl=os.getenv("WB_STOCKS_PREFIX_TPL", "Отчёты/Остатки/{store}/Недельные/"),
            orders_prefix_tpl=os.getenv("WB_ORDERS_PREFIX_TPL", "Отчёты/Заказы/{store}/Недельные/"),
            article_map_1c_key=os.getenv("WB_ARTICLE_MAP_1C_KEY", "Отчёты/Остатки/1С/Артикулы 1с.xlsx").strip(),
            stocks_1c_key=os.getenv("WB_STOCKS_1C_KEY", "Отчёты/Остатки/1С/Остатки 1С.xlsx").strip(),
            stop_list_articles=parse_stop_list_env(os.getenv("WB_STOP_LIST_KEY", "")),
            critical_days_threshold=float(os.getenv("WB_CRITICAL_DAYS_THRESHOLD", "14")),
            sheet_stocks_name=os.getenv("WB_STOCKS_SHEET_NAME", "Остатки").strip(),
            sheet_orders_name=os.getenv("WB_ORDERS_SHEET_NAME", "Заказы").strip(),
        )


class S3Storage:
    def __init__(self, cfg: AppConfig) -> None:
        if not cfg.bucket_name or not cfg.access_key or not cfg.secret_key:
            raise ValueError(
                "Не заданы параметры Object Storage. "
                "Нужны env: YC_BUCKET_NAME/YC_ACCESS_KEY_ID/YC_SECRET_ACCESS_KEY "
                "или CLOUD_RU_* / WB_S3_*.")

        self.bucket = cfg.bucket_name
        self.client = boto3.client(
            "s3",
            endpoint_url=cfg.endpoint_url,
            aws_access_key_id=cfg.access_key,
            aws_secret_access_key=cfg.secret_key,
            region_name=cfg.region_name,
            config=BotoConfig(signature_version="s3v4", read_timeout=300, connect_timeout=60),
        )

    def list_keys(self, prefix: str) -> List[dict]:
        items: List[dict] = []
        token = None
        while True:
            kwargs = {"Bucket": self.bucket, "Prefix": prefix, "MaxKeys": 1000}
            if token:
                kwargs["ContinuationToken"] = token
            response = self.client.list_objects_v2(**kwargs)
            items.extend(response.get("Contents", []))
            if not response.get("IsTruncated"):
                break
            token = response.get("NextContinuationToken")
        return items

    def read_bytes(self, key: str) -> bytes:
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        return response["Body"].read()

    def read_excel(self, key: str, sheet_name: Optional[str] = None) -> pd.DataFrame:
        result = pd.read_excel(io.BytesIO(self.read_bytes(key)), sheet_name=sheet_name)
        if isinstance(result, dict):
            return next(iter(result.values()))
        return result

    def upload_file(self, local_path: str, key: str) -> None:
        self.client.upload_file(local_path, self.bucket, key)



def choose_latest_weekly_key(storage: S3Storage, prefix: str) -> str:
    items = storage.list_keys(prefix)
    if not items:
        raise FileNotFoundError(f"В Object Storage нет файлов по префиксу: {prefix}")

    xlsx_items = [x for x in items if x["Key"].lower().endswith(".xlsx")]
    if not xlsx_items:
        raise FileNotFoundError(f"По префиксу {prefix} не найдено xlsx-файлов")

    def sort_key(item: dict) -> Tuple[datetime, datetime]:
        week_dt = extract_week_from_key(item["Key"]) or datetime.min
        last_modified = item.get("LastModified")
        if hasattr(last_modified, "replace"):
            last_modified = last_modified.replace(tzinfo=None)
        else:
            last_modified = datetime.min
        return week_dt, last_modified

    latest = sorted(xlsx_items, key=sort_key)[-1]
    return latest["Key"]



def choose_order_keys_for_last_7_days(storage: S3Storage, prefix: str, run_date: datetime) -> List[str]:
    items = storage.list_keys(prefix)
    xlsx_items = [x for x in items if x["Key"].lower().endswith(".xlsx")]
    if not xlsx_items:
        raise FileNotFoundError(f"По префиксу {prefix} не найдено xlsx-файлов")

    start_date = run_date - timedelta(days=6)
    selected: List[str] = []
    for item in xlsx_items:
        week_dt = extract_week_from_key(item["Key"])
        if week_dt is None:
            continue
        week_end = week_dt + timedelta(days=6)
        if week_end >= start_date and week_dt <= run_date:
            selected.append(item["Key"])

    if not selected:
        selected = [choose_latest_weekly_key(storage, prefix)]

    return sorted(set(selected))



def load_article_map_1c(storage: S3Storage, cfg: AppConfig) -> Dict[str, str]:
    df = storage.read_excel(cfg.article_map_1c_key)
    df.columns = [normalize_text(c) for c in df.columns]

    wb_col = df.columns[0]
    one_c_col = df.columns[2] if len(df.columns) >= 3 else df.columns[-1]

    mapping: Dict[str, str] = {}
    for _, row in df.iterrows():
        wb_article = normalize_nmid(row.get(wb_col))
        article_1c = normalize_text(row.get(one_c_col))
        if wb_article and article_1c:
            mapping[wb_article] = article_1c

    log(f"Загружено соответствий WB -> 1С: {len(mapping):,}")
    return mapping



def load_1c_stocks(storage: S3Storage, cfg: AppConfig) -> pd.DataFrame:
    df = storage.read_excel(cfg.stocks_1c_key)
    df.columns = [normalize_text(c) for c in df.columns]

    required = ["Артикул", "Остатки МП"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"В файле 1С отсутствуют колонки: {missing}")

    out = df.copy()
    out["Артикул 1С"] = out["Артикул"].map(normalize_text)
    out["Артикул 1С_norm"] = out["Артикул 1С"].map(normalize_article_1c)
    out["Остатки МП"] = pd.to_numeric(out["Остатки МП"], errors="coerce").fillna(0).map(ceil_int)

    return out[["Артикул 1С", "Артикул 1С_norm", "Остатки МП"]].drop_duplicates(subset=["Артикул 1С_norm"], keep="first")



def load_wb_stocks(storage: S3Storage, cfg: AppConfig) -> Tuple[pd.DataFrame, str]:
    prefix = cfg.stock_prefix_tpl.format(store=cfg.store_name)
    key = choose_latest_weekly_key(storage, prefix)
    log(f"Берём остатки WB из файла: {key}")

    df = storage.read_excel(key, sheet_name=cfg.sheet_stocks_name)
    df.columns = [normalize_text(c) for c in df.columns]

    id_col = find_first_existing(df.columns, ["nmId", "Артикул WB", "Код номенклатуры"])
    wb_article_col = find_first_existing(df.columns, ["Артикул WB", "nmId", "Код номенклатуры"])
    qty_col = find_first_existing(df.columns, ["Доступно для продажи", "Полное количество", "Количество", "Остаток"])
    subject_col = find_first_existing(df.columns, ["Предмет", "subject"])

    if id_col is None or qty_col is None:
        raise KeyError(
            f"Не найдены нужные колонки в weekly-файле остатков. "
            f"id_col={id_col}, qty_col={qty_col}, колонки={list(df.columns)}"
        )

    out = df.copy()
    out["nmId"] = out[id_col].map(normalize_nmid)
    out["Артикул WB"] = out[wb_article_col].map(normalize_nmid) if wb_article_col else out["nmId"]
    out["stock_wb"] = pd.to_numeric(out[qty_col], errors="coerce").fillna(0)
    out["subject"] = out[subject_col].map(normalize_text) if subject_col else ""

    out = out.groupby(["nmId", "Артикул WB", "subject"], dropna=False, as_index=False).agg(stock_wb=("stock_wb", "sum"))
    return out, key



def load_orders_last_7_days(storage: S3Storage, cfg: AppConfig) -> Tuple[pd.DataFrame, List[str]]:
    prefix = cfg.orders_prefix_tpl.format(store=cfg.store_name)
    keys = choose_order_keys_for_last_7_days(storage, prefix, cfg.run_date)
    log(f"Берём заказы WB из файлов: {keys}")

    parts: List[pd.DataFrame] = []
    for key in keys:
        df = storage.read_excel(key, sheet_name=cfg.sheet_orders_name)
        df.columns = [normalize_text(c) for c in df.columns]
        parts.append(df)

    orders = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    if orders.empty:
        raise ValueError("Файлы заказов прочитаны, но данных в них нет.")

    date_col = find_first_existing(orders.columns, ["date", "Дата", "Дата заказа"])
    nmid_col = find_first_existing(orders.columns, ["nmId", "Артикул WB", "Код номенклатуры"])
    if date_col is None or nmid_col is None:
        raise KeyError(
            f"В weekly-файле заказов не найдены колонки date/nmId. Колонки: {list(orders.columns)}"
        )

    out = orders.copy()
    out["date"] = pd.to_datetime(out[date_col], errors="coerce").dt.normalize()
    out["nmId"] = out[nmid_col].map(normalize_nmid)

    if "isCancel" in out.columns:
        out = out[~out["isCancel"].fillna(False)].copy()

    start_date = cfg.run_date - timedelta(days=6)
    out = out[(out["date"] >= start_date) & (out["date"] <= cfg.run_date)].copy()

    sales = out.groupby("nmId", as_index=False).size().rename(columns={"size": "sales_7d"})
    sales["avg_sales_per_day"] = sales["sales_7d"] / 7.0
    return sales, keys



def prepare_report_dataframe(
    wb_stocks: pd.DataFrame,
    sales: pd.DataFrame,
    article_map_1c: Dict[str, str],
    stocks_1c: pd.DataFrame,
    stop_list_articles: Set[str],
) -> pd.DataFrame:
    df = wb_stocks.merge(sales, on="nmId", how="left").copy()
    df["sales_7d"] = df["sales_7d"].fillna(0).astype(int)
    df["avg_sales_per_day"] = df["avg_sales_per_day"].fillna(0.0)

    df["Артикул 1С"] = df["nmId"].map(article_map_1c).fillna("")
    df["Артикул 1С_norm"] = df["Артикул 1С"].map(normalize_article_1c)

    df = df.merge(stocks_1c[["Артикул 1С_norm", "Остатки МП"]], on="Артикул 1С_norm", how="left")
    df["Остатки МП"] = df["Остатки МП"].fillna(0).astype(int)

    zero_sales_mask = df["avg_sales_per_day"] <= 0
    denominator = df["avg_sales_per_day"].where(~zero_sales_mask, other=pd.NA)
    df["Дней остатка WB"] = df["stock_wb"] / denominator
    df["Дней запаса Липецк"] = df["Остатки МП"] / denominator
    df["Дней суммарно WB+Липецк"] = (df["stock_wb"] + df["Остатки МП"]) / denominator
    df.loc[zero_sales_mask, ["Дней остатка WB", "Дней запаса Липецк", "Дней суммарно WB+Липецк"]] = pd.NA

    df["Delist"] = df["Артикул 1С_norm"].apply(lambda x: "Delist" if x in stop_list_articles else "")
    df["Комментарий"] = df["Delist"].apply(lambda x: "Товар на вывод, пополнения не будет" if x == "Delist" else "")

    df = df.sort_values(by=["Дней остатка WB", "sales_7d", "stock_wb"], ascending=[True, False, True], na_position="last").reset_index(drop=True)
    return df



def build_export_frames(df: pd.DataFrame, threshold: float) -> Tuple[pd.DataFrame, pd.DataFrame]:
    report = df.copy()
    critical = report[(report["Дней остатка WB"].notna()) & (report["Дней остатка WB"] < threshold)].copy()

    public_df = critical[["Артикул 1С", "Дней остатка WB", "Дней запаса Липецк", "Delist", "Комментарий"]].copy()
    public_df.rename(columns={
        "Дней остатка WB": "Остатка WB хватит на, дней",
        "Дней запаса Липецк": "В Липецке есть запас на, дней",
        "Delist": "Статус",
        "Комментарий": "Примечание",
    }, inplace=True)

    calc_df = report[[
        "nmId",
        "Артикул WB",
        "Артикул 1С",
        "subject",
        "stock_wb",
        "sales_7d",
        "avg_sales_per_day",
        "Дней остатка WB",
        "Остатки МП",
        "Дней запаса Липецк",
        "Дней суммарно WB+Липецк",
        "Delist",
        "Комментарий",
    ]].copy()
    calc_df.rename(columns={
        "subject": "Предмет",
        "stock_wb": "Остаток WB",
        "sales_7d": "Продажи за 7 дней",
        "avg_sales_per_day": "Среднесуточные продажи",
        "Остатки МП": "Остатки МП (Липецк)",
        "Delist": "Статус",
        "Комментарий": "Примечание",
    }, inplace=True)

    return public_df, calc_df



def autosize_and_style_workbook(file_path: str, threshold: float) -> None:
    wb = load_workbook(file_path)

    header_fill = PatternFill("solid", fgColor="1F4E78")
    header_font = Font(name="Calibri", size=14, bold=True, color="FFFFFF")
    body_font = Font(name="Calibri", size=14)
    center_alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
    thin_side = Side(border_style="thin", color="D9D9D9")
    border = Border(left=thin_side, right=thin_side, top=thin_side, bottom=thin_side)

    red_fill = PatternFill("solid", fgColor="F4CCCC")
    yellow_fill = PatternFill("solid", fgColor="FFF2CC")
    delist_fill = PatternFill("solid", fgColor="D9EAD3")

    for ws in wb.worksheets:
        ws.freeze_panes = "A2"

        for row in ws.iter_rows():
            for cell in row:
                cell.font = body_font
                cell.alignment = center_alignment
                cell.border = border

        for cell in ws[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = center_alignment
            cell.border = border

        ws.row_dimensions[1].height = 34
        for row_idx in range(2, ws.max_row + 1):
            ws.row_dimensions[row_idx].height = 28

        for col_idx in range(1, ws.max_column + 1):
            max_len = 0
            for row_idx in range(1, ws.max_row + 1):
                value = ws.cell(row=row_idx, column=col_idx).value
                text = "" if value is None else str(value)
                max_len = max(max_len, max((len(part) for part in text.split("\n")), default=0))
            ws.column_dimensions[get_column_letter(col_idx)].width = min(max(max_len + 4, 16), 42)

        headers = {str(ws.cell(1, c).value): c for c in range(1, ws.max_column + 1)}
        day_col = headers.get("Остатка WB хватит на, дней") or headers.get("Дней остатка WB")
        status_col = headers.get("Статус")

        for row_idx in range(2, ws.max_row + 1):
            if day_col:
                value = ws.cell(row_idx, day_col).value
                try:
                    days = float(value)
                except Exception:
                    days = None
                if days is not None:
                    fill = red_fill if days < 7 else yellow_fill if days < threshold else None
                    if fill:
                        for col_idx in range(1, ws.max_column + 1):
                            ws.cell(row_idx, col_idx).fill = fill

            if status_col and normalize_text(ws.cell(row_idx, status_col).value).lower() == "delist":
                for col_idx in range(1, ws.max_column + 1):
                    ws.cell(row_idx, col_idx).fill = delist_fill

    wb.save(file_path)



def save_report_xlsx(public_df: pd.DataFrame, calc_df: pd.DataFrame, output_path: str, threshold: float) -> str:
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        public_df.to_excel(writer, index=False, sheet_name="Критично <14 дней")
        calc_df.to_excel(writer, index=False, sheet_name="Расчёт")
    autosize_and_style_workbook(output_path, threshold)
    return output_path



def send_telegram_document(bot_token: str, chat_id: str, file_path: str, caption: str) -> None:
    if not bot_token or not chat_id:
        raise ValueError("Не заданы TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID")

    url = f"https://api.telegram.org/bot{bot_token}/sendDocument"
    with open(file_path, "rb") as f:
        response = requests.post(
            url,
            data={"chat_id": chat_id, "caption": caption},
            files={"document": (Path(file_path).name, f)},
            timeout=300,
        )

    if response.status_code != 200:
        raise RuntimeError(f"Ошибка отправки в Telegram: {response.status_code} {response.text}")



def upload_report_if_needed(storage: S3Storage, cfg: AppConfig, file_path: str) -> None:
    if not cfg.upload_result_to_s3:
        return
    key = f"{cfg.result_prefix.rstrip('/')}/{Path(file_path).name}"
    storage.upload_file(file_path, key)
    log(f"Отчёт загружен в Object Storage: {key}")



def should_send_today(run_date: datetime) -> bool:
    return run_date.weekday() in {0, 4}



def run_report(cfg: AppConfig, force_send: bool = False) -> str:
    storage = S3Storage(cfg)

    wb_stocks, stock_key = load_wb_stocks(storage, cfg)
    sales_7d, order_keys = load_orders_last_7_days(storage, cfg)
    article_map_1c = load_article_map_1c(storage, cfg)
    stocks_1c = load_1c_stocks(storage, cfg)

    df = prepare_report_dataframe(
        wb_stocks=wb_stocks,
        sales=sales_7d,
        article_map_1c=article_map_1c,
        stocks_1c=stocks_1c,
        stop_list_articles=cfg.stop_list_articles,
    )

    public_df, calc_df = build_export_frames(df, cfg.critical_days_threshold)

    report_name = f"Отчёт_дни_остатка_WB_{cfg.store_name}_{cfg.run_date:%Y%m%d}.xlsx"
    output_path = str(Path(cfg.output_dir) / report_name)
    save_report_xlsx(public_df, calc_df, output_path, cfg.critical_days_threshold)
    log(f"Отчёт сохранён: {output_path}")

    upload_report_if_needed(storage, cfg, output_path)

    if cfg.send_telegram and (force_send or should_send_today(cfg.run_date)):
        delist_count = int((calc_df["Статус"] == "Delist").sum()) if "Статус" in calc_df.columns else 0
        critical_count = len(public_df)
        caption = (
            f"{cfg.store_name} | {cfg.run_date:%Y-%m-%d}\n"
            f"Товаров < {int(cfg.critical_days_threshold)} дней: {critical_count}\n"
            f"Delist: {delist_count}"
        )
        send_telegram_document(cfg.telegram_bot_token, cfg.telegram_chat_id, output_path, caption)
        log("Отчёт отправлен в Telegram")
    else:
        log("Отправка в Telegram пропущена по расписанию")

    log(f"Источник остатков: {stock_key}")
    log(f"Источники заказов: {', '.join(order_keys)}")
    log(f"Delist-артикулов из env: {len(cfg.stop_list_articles)}")
    return output_path



def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Отчёт по дням остатка WB")
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Сформировать отчёт")
    run_parser.add_argument("--run-date", dest="run_date", help="Дата запуска YYYY-MM-DD")
    run_parser.add_argument("--force-send", action="store_true", help="Принудительно отправить в Telegram")

    parser.set_defaults(command="run")
    return parser



def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()

    cfg = AppConfig.from_env(run_date=getattr(args, "run_date", None))
    if args.command == "run":
        run_report(cfg, force_send=getattr(args, "force_send", False))
        return

    parser.print_help()
    sys.exit(1)


if __name__ == "__main__":
    main()
