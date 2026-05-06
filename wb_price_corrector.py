#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Корректировка цен Wildberries под целевую цену покупателя = РРЦ * 0.9.

Логика:
- каждые 3 часа забирает заказы за сегодняшний день через statistics-api;
- считает средний SPP за последние 3 часа по nmID;
- если по nmID нет заказов за 3 часа, использует fallback: сегодня -> последние недельные файлы -> subject/global;
- читает РРЦ из S3: Отчёты/Финансовые показатели/<STORE>/РРЦ.xlsx;
- читает справочник артикулов 1С из S3;
- исключает subject: Помады, Блески;
- считает новую WB price при фиксированной скидке продавца 26%;
- отправляет price + discount в /api/v2/upload/task только в режиме --apply;
- в режиме --dry-run только сохраняет расчёт.

Важно:
- штатные недельные файлы "Отчёты/Заказы/..." по умолчанию НЕ перезаписываются,
  чтобы ежедневный сборщик не принял сегодняшний оперативный срез за закрытый день.
- свежие заказы за сегодня сохраняются в служебной папке корректировщика.

Переменные окружения:
- YC_ACCESS_KEY_ID
- YC_SECRET_ACCESS_KEY
- YC_BUCKET_NAME
- WB_PROMO_KEY_TOPFACE
"""

from __future__ import annotations

import argparse
import io
import json
import math
import os
import re
import sys
import tempfile
import time
import traceback
from collections import defaultdict
from copy import copy
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import boto3
import pandas as pd
import pytz
import requests
from botocore.client import Config
from botocore.exceptions import ClientError


MOSCOW_TZ = pytz.timezone("Europe/Moscow")

ORDERS_API_URL = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
PRICES_LIST_API_URL = "https://discounts-prices-api.wildberries.ru/api/v2/list/goods/filter"
PRICE_UPLOAD_API_URL = "https://discounts-prices-api.wildberries.ru/api/v2/upload/task"

DEFAULT_STORE = "TOPFACE"
DEFAULT_TARGET_FACTOR = 0.90
DEFAULT_SELLER_DISCOUNT = 26
DEFAULT_PRICE_TOLERANCE_RUB = 1
DEFAULT_MAX_PRICE_CHANGE_PCT = 80.0  # 0 = отключить ограничение
DEFAULT_FALLBACK_DAYS = 21

EXCLUDED_SUBJECTS = {"помады", "блески"}

# Когда WB price API не отдаёт subject, запрещённые группы дополнительно определяем
# по названию в файле РРЦ. Не используем общий признак "губ", потому что
# карандаши для губ должны остаться в обработке.
EXCLUDED_RRC_NAME_KEYWORDS = (
    "помада",
    "lipstick",
    "lip stick",
    "lip paint",
    "lippaint",
    "блеск для губ",
    "блеск-бустер",
    "блеск бустер",
    "lipgloss",
    "lip gloss",
)


def excluded_by_rrc_name(value: Any) -> bool:
    text = normalize_text(value)
    if not text:
        return False
    return any(k in text for k in EXCLUDED_RRC_NAME_KEYWORDS)


def excluded_rrc_keyword(value: Any) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    for k in EXCLUDED_RRC_NAME_KEYWORDS:
        if k in text:
            return k
    return ""


# ========================== S3 / Yandex Object Storage ==========================

class S3Storage:
    """Клиент для работы с S3-совместимым хранилищем Yandex Cloud."""

    def __init__(self, access_key: str, secret_key: str, bucket_name: str):
        self.bucket = bucket_name
        self.s3 = boto3.client(
            "s3",
            endpoint_url="https://storage.yandexcloud.net",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="ru-central1",
            config=Config(
                signature_version="s3v4",
                read_timeout=300,
                connect_timeout=60,
                retries={"max_attempts": 5},
            ),
        )
        self.log(f"Подключение к Yandex Cloud OK, bucket={bucket_name}")

    @staticmethod
    def log(message: str):
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [S3] {message}", flush=True)

    def read_bytes(self, key: str) -> bytes:
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        return obj["Body"].read()

    def read_excel(self, key: str, sheet_name: Any = 0) -> pd.DataFrame:
        try:
            data = self.read_bytes(key)
            return pd.read_excel(io.BytesIO(data), sheet_name=sheet_name)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in {"NoSuchKey", "404", "NotFound"}:
                print(f"⚠️ Файл не найден в S3: {key}")
                return pd.DataFrame()
            raise
        except ValueError as e:
            print(f"⚠️ Ошибка чтения Excel {key}, sheet={sheet_name}: {e}")
            return pd.DataFrame()
        except Exception as e:
            print(f"⚠️ Ошибка чтения Excel {key}: {e}")
            return pd.DataFrame()

    def read_excel_first_existing_sheet(self, key: str, preferred_sheets: Sequence[Any]) -> pd.DataFrame:
        """Пробует прочитать один из листов; если не вышло — первый лист."""
        try:
            data = self.read_bytes(key)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in {"NoSuchKey", "404", "NotFound"}:
                print(f"⚠️ Файл не найден в S3: {key}")
                return pd.DataFrame()
            raise

        for sheet in preferred_sheets:
            try:
                return pd.read_excel(io.BytesIO(data), sheet_name=sheet)
            except Exception:
                continue
        try:
            return pd.read_excel(io.BytesIO(data), sheet_name=0)
        except Exception as e:
            print(f"⚠️ Ошибка чтения Excel {key}: {e}")
            return pd.DataFrame()

    def write_excel(self, key: str, df: pd.DataFrame, sheet_name: str = "Data"):
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            with pd.ExcelWriter(tmp_path, engine="openpyxl") as writer:
                df.to_excel(writer, sheet_name=safe_sheet_name(sheet_name), index=False)
                autofit_openpyxl(writer, safe_sheet_name(sheet_name), df)
            self.upload_file(tmp_path, key)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def write_excel_multi(self, key: str, sheets: Dict[str, pd.DataFrame]):
        if not sheets:
            return
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            with pd.ExcelWriter(tmp_path, engine="openpyxl") as writer:
                wrote_any = False
                for sheet_name, df in sheets.items():
                    if df is None:
                        continue
                    safe_name = safe_sheet_name(sheet_name)
                    df_out = df.copy()
                    df_out.to_excel(writer, sheet_name=safe_name, index=False)
                    autofit_openpyxl(writer, safe_name, df_out)
                    wrote_any = True
                if not wrote_any:
                    pd.DataFrame({"message": ["Нет данных"]}).to_excel(writer, sheet_name="Data", index=False)
            self.upload_file(tmp_path, key)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def upload_file(self, local_path: str, key: str):
        self.s3.upload_file(local_path, self.bucket, key)
        print(f"✅ Сохранено в S3: {key}", flush=True)

    def file_exists(self, key: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False

    def list_files(self, prefix: str = "") -> List[str]:
        keys: List[str] = []
        continuation_token = None
        while True:
            kwargs: Dict[str, Any] = {"Bucket": self.bucket, "Prefix": prefix}
            if continuation_token:
                kwargs["ContinuationToken"] = continuation_token
            resp = self.s3.list_objects_v2(**kwargs)
            for obj in resp.get("Contents", []):
                keys.append(obj["Key"])
            if not resp.get("IsTruncated"):
                break
            continuation_token = resp.get("NextContinuationToken")
        return keys

    def find_first_key(self, prefixes: Sequence[str], keywords: Sequence[str], suffix: str = ".xlsx") -> Optional[str]:
        kw = [normalize_text(x) for x in keywords]
        for prefix in prefixes:
            try:
                keys = self.list_files(prefix)
            except Exception as e:
                print(f"⚠️ Не удалось просмотреть prefix={prefix}: {e}")
                continue
            for key in keys:
                key_norm = normalize_text(key)
                if suffix and not key_norm.endswith(normalize_text(suffix)):
                    continue
                if all(x in key_norm for x in kw):
                    return key
        return None


# ========================== Helpers ==========================

def now_msk() -> datetime:
    return datetime.now(MOSCOW_TZ)


def log(message: str, level: str = "INFO"):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [{level}] {message}", flush=True)


def safe_sheet_name(name: str) -> str:
    name = re.sub(r"[\\/*?:\[\]]", "_", str(name))
    return name[:31] or "Data"


def autofit_openpyxl(writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame):
    """Лёгкое автоформатирование Excel-вывода."""
    try:
        ws = writer.sheets[sheet_name]
        ws.freeze_panes = "A2"
        for idx, col in enumerate(df.columns, start=1):
            values = [str(col)] + ["" if pd.isna(v) else str(v) for v in df[col].head(500).tolist()]
            width = min(max(max(len(v) for v in values) + 2, 10), 45)
            ws.column_dimensions[ws.cell(row=1, column=idx).column_letter].width = width
        for cell in ws[1]:
            new_font = copy(cell.font)
            new_font.bold = True
            cell.font = new_font
            new_alignment = copy(cell.alignment)
            new_alignment.wrap_text = True
            new_alignment.horizontal = "center"
            cell.alignment = new_alignment
    except Exception:
        pass


def normalize_text(value: Any) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    return str(value).strip().lower().replace("ё", "е")


def normalize_article(value: Any) -> str:
    """Нормализация артикула для сопоставления PT901.F05, 901_/5, 901/5 и т.п."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    s = str(value).strip().upper()
    if s.endswith(".0") and re.fullmatch(r"\d+\.0", s):
        s = s[:-2]
    s = s.replace(" ", "")
    s = s.replace("_", "")
    s = s.replace("\\", "/")
    s = s.replace("-", "-")
    # Часто в отчётах встречается 901_/10 -> после удаления _ станет 901/10.
    s = re.sub(r"/+", "/", s)
    return s


def to_int_or_none(value: Any) -> Optional[int]:
    try:
        if value is None or pd.isna(value):
            return None
        return int(float(str(value).replace(" ", "").replace(",", ".")))
    except Exception:
        return None


def to_float_or_none(value: Any) -> Optional[float]:
    try:
        if value is None or pd.isna(value):
            return None
        if isinstance(value, str):
            value = value.replace("\u00a0", " ").replace(" ", "").replace(",", ".")
            value = re.sub(r"[^0-9.\-]", "", value)
            if value == "":
                return None
        out = float(value)
        if math.isnan(out):
            return None
        return out
    except Exception:
        return None


def first_existing_col(df: pd.DataFrame, candidates: Sequence[str]) -> Optional[str]:
    if df.empty:
        return None
    lower_map = {normalize_text(c): c for c in df.columns}
    for cand in candidates:
        key = normalize_text(cand)
        if key in lower_map:
            return lower_map[key]
    # мягкий поиск по вхождению
    for cand in candidates:
        cand_norm = normalize_text(cand)
        for c in df.columns:
            if cand_norm and cand_norm in normalize_text(c):
                return c
    return None


def is_cancelled_series(series: pd.Series) -> pd.Series:
    return series.astype(str).str.lower().isin(["true", "1", "да", "yes", "истина"])


def get_week_start(dt: datetime) -> datetime:
    return dt - timedelta(days=dt.weekday())


def get_weekly_orders_key(store: str, dt: datetime) -> str:
    year, week, _ = dt.isocalendar()
    return f"Отчёты/Заказы/{store}/Недельные/Заказы_{year}-W{week:02d}.xlsx"


# ========================== Price corrector ==========================

@dataclass
class PriceCorrectorConfig:
    store: str = DEFAULT_STORE
    target_factor: float = DEFAULT_TARGET_FACTOR
    seller_discount: int = DEFAULT_SELLER_DISCOUNT
    price_tolerance_rub: int = DEFAULT_PRICE_TOLERANCE_RUB
    max_price_change_pct: float = DEFAULT_MAX_PRICE_CHANGE_PCT
    fallback_days: int = DEFAULT_FALLBACK_DAYS
    allow_unknown_subject: bool = False
    update_weekly_orders: bool = False


class WBPriceCorrector:
    def __init__(self, s3: S3Storage, wb_key: str, cfg: PriceCorrectorConfig):
        self.s3 = s3
        self.wb_key = wb_key.strip()
        self.cfg = cfg
        self.session = requests.Session()
        self.service_prefix = f"Служебные файлы/Корректировка цен/{self.cfg.store}"

    # ---------- API ----------

    def _request_with_retry(
        self,
        method: str,
        url: str,
        headers: Dict[str, str],
        params: Optional[Dict[str, Any]] = None,
        json_payload: Optional[Dict[str, Any]] = None,
        timeout: int = 120,
        max_attempts: int = 5,
        rate_limit_wait_sec: int = 65,
    ) -> Optional[requests.Response]:
        for attempt in range(1, max_attempts + 1):
            try:
                if method.upper() == "GET":
                    resp = self.session.get(url, headers=headers, params=params, timeout=timeout)
                elif method.upper() == "POST":
                    resp = self.session.post(url, headers=headers, params=params, json=json_payload, timeout=timeout)
                else:
                    raise ValueError(f"Unsupported method: {method}")

                if resp.status_code in (200, 201, 202, 204):
                    return resp

                if resp.status_code == 429:
                    wait = rate_limit_wait_sec * attempt
                    log(f"Лимит WB 429, попытка {attempt}/{max_attempts}, ждём {wait} сек", "WARN")
                    time.sleep(wait)
                    continue

                if resp.status_code in (500, 502, 503, 504):
                    wait = 20 * attempt
                    log(f"WB {resp.status_code}, попытка {attempt}/{max_attempts}, ждём {wait} сек", "WARN")
                    time.sleep(wait)
                    continue

                log(f"WB API ошибка {resp.status_code}: {resp.text[:500]}", "ERROR")
                return resp

            except requests.RequestException as e:
                wait = 15 * attempt
                log(f"Ошибка соединения: {e}; попытка {attempt}/{max_attempts}, ждём {wait} сек", "WARN")
                time.sleep(wait)
        return None

    def fetch_today_orders(self) -> pd.DataFrame:
        """Получает все заказы сегодняшнего дня. date сохраняется с временем."""
        today_str = now_msk().strftime("%Y-%m-%d")
        headers = {"Authorization": self.wb_key}
        params = {"dateFrom": today_str, "flag": 1}
        log(f"Загружаю заказы за сегодня: {today_str}, flag=1")
        resp = self._request_with_retry("GET", ORDERS_API_URL, headers=headers, params=params, rate_limit_wait_sec=65)
        if resp is None:
            raise RuntimeError("Не удалось получить ответ от API заказов WB")
        if resp.status_code == 204:
            return pd.DataFrame()
        if resp.status_code != 200:
            raise RuntimeError(f"Ошибка заказов WB {resp.status_code}: {resp.text[:1000]}")
        data = resp.json()
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        df["store"] = self.cfg.store
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
        if "lastChangeDate" in df.columns:
            df["lastChangeDate"] = pd.to_datetime(df["lastChangeDate"], errors="coerce")
        return df

    def fetch_current_goods_prices(self) -> pd.DataFrame:
        """Получает текущие цены всех товаров продавца из Discounts & Prices API."""
        headers = {"Authorization": self.wb_key}
        limit = 1000
        offset = 0
        all_rows: List[Dict[str, Any]] = []
        page = 1

        while True:
            params = {"limit": limit, "offset": offset}
            log(f"Загружаю текущие цены WB: page={page}, offset={offset}")
            resp = self._request_with_retry(
                "GET",
                PRICES_LIST_API_URL,
                headers=headers,
                params=params,
                timeout=120,
                max_attempts=5,
                rate_limit_wait_sec=10,
            )
            if resp is None:
                raise RuntimeError("Не удалось получить текущие цены WB")
            if resp.status_code != 200:
                raise RuntimeError(f"Ошибка текущих цен WB {resp.status_code}: {resp.text[:1000]}")
            payload = resp.json()
            items = self._extract_goods_items(payload)
            if not items:
                break

            for item in items:
                all_rows.append(self._normalize_goods_item(item))

            if len(items) < limit:
                break
            offset += limit
            page += 1
            time.sleep(0.7)

        df = pd.DataFrame(all_rows)
        if not df.empty:
            df = df.drop_duplicates(subset=["nmID"], keep="last")
        log(f"Текущие цены WB: товаров={len(df)}")
        return df

    @staticmethod
    def _extract_goods_items(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        data = payload.get("data", payload)
        if isinstance(data, list):
            return data
        if not isinstance(data, dict):
            return []
        for key in ["listGoods", "goods", "items", "list", "products", "data"]:
            value = data.get(key)
            if isinstance(value, list):
                return value
        return []

    @staticmethod
    def _normalize_goods_item(item: Dict[str, Any]) -> Dict[str, Any]:
        sizes = item.get("sizes") if isinstance(item.get("sizes"), list) else []
        first_size = sizes[0] if sizes else {}

        nm_id = item.get("nmID", item.get("nmId", item.get("nm_id")))
        vendor_code = item.get("vendorCode", item.get("supplierArticle", item.get("article", "")))

        # В разных ответах WB price может лежать на уровне товара или размера.
        price = item.get("price", first_size.get("price"))
        discount = item.get("discount", first_size.get("discount"))
        discounted_price = item.get("discountedPrice", first_size.get("discountedPrice", first_size.get("priceWithDisc")))

        subject = item.get("subjectName", item.get("subject", item.get("subjectNameRu", "")))
        name = item.get("name", item.get("title", ""))

        return {
            "nmID": to_int_or_none(nm_id),
            "supplierArticle_api": vendor_code,
            "supplierArticle_api_norm": normalize_article(vendor_code),
            "current_wb_price": to_float_or_none(price),
            "current_wb_discount": to_float_or_none(discount),
            "current_wb_discounted_price": to_float_or_none(discounted_price),
            "currencyIsoCode4217": item.get("currencyIsoCode4217", item.get("currency", "")),
            "subject_api": subject,
            "name_api": name,
        }

    def upload_prices(self, to_upload: pd.DataFrame) -> pd.DataFrame:
        if to_upload.empty:
            return pd.DataFrame([{"status": "nothing_to_upload", "message": "Нет товаров для отправки"}])

        rows = []
        payload_rows = []
        for _, r in to_upload.iterrows():
            nm_id = to_int_or_none(r.get("nmID"))
            price = to_int_or_none(r.get("new_price"))
            discount = to_int_or_none(r.get("new_discount"))
            if nm_id and price and discount is not None:
                payload_rows.append({"nmID": nm_id, "price": price, "discount": discount})

        headers = {"Authorization": self.wb_key, "Content-Type": "application/json"}
        batch_size = 1000
        for batch_idx, start in enumerate(range(0, len(payload_rows), batch_size), start=1):
            batch = payload_rows[start:start + batch_size]
            payload = {"data": batch}
            log(f"Отправка цен WB: батч {batch_idx}, товаров={len(batch)}")
            resp = self._request_with_retry(
                "POST",
                PRICE_UPLOAD_API_URL,
                headers=headers,
                json_payload=payload,
                timeout=120,
                max_attempts=5,
                rate_limit_wait_sec=10,
            )
            if resp is None:
                rows.append({
                    "batch": batch_idx,
                    "status_code": None,
                    "ok": False,
                    "response": "no response",
                    "sent_count": len(batch),
                })
                continue

            text = resp.text[:5000]
            task_id = ""
            try:
                js = resp.json()
                task_id = (
                    js.get("data", {}).get("id")
                    or js.get("data", {}).get("taskId")
                    or js.get("id")
                    or js.get("taskId")
                    or ""
                )
                text = json.dumps(js, ensure_ascii=False)[:5000]
            except Exception:
                pass

            rows.append({
                "batch": batch_idx,
                "status_code": resp.status_code,
                "ok": resp.status_code in (200, 201, 202, 204),
                "task_id": task_id,
                "sent_count": len(batch),
                "response": text,
            })
            time.sleep(1.0)

        return pd.DataFrame(rows)

    # ---------- Sources ----------

    def save_today_orders_snapshot(self, orders_df: pd.DataFrame):
        key = f"{self.service_prefix}/Заказы_сегодня.xlsx"
        self.s3.write_excel(key, orders_df, sheet_name="Заказы_сегодня")

        if self.cfg.update_weekly_orders:
            self._upsert_today_into_weekly_orders(orders_df)
        else:
            log("Штатный недельный файл заказов не трогаю: update_weekly_orders=False")

    def _upsert_today_into_weekly_orders(self, orders_df: pd.DataFrame):
        """Опционально обновляет сегодняшний день в обычном недельном файле заказов."""
        if orders_df.empty:
            return
        dt = now_msk()
        today_d = dt.date()
        key = get_weekly_orders_key(self.cfg.store, dt)
        old = self.s3.read_excel(key, sheet_name=0)
        if not old.empty and "date" in old.columns:
            old_dates = pd.to_datetime(old["date"], errors="coerce").dt.date
            old = old.loc[old_dates != today_d].copy()
        combined = pd.concat([old, orders_df], ignore_index=True) if not old.empty else orders_df.copy()
        if "srid" in combined.columns:
            combined = combined.drop_duplicates(subset=["srid"], keep="last")
        self.s3.write_excel(key, combined, sheet_name="Заказы")

    def load_rrc(self) -> pd.DataFrame:
        key = f"Отчёты/Финансовые показатели/{self.cfg.store}/РРЦ.xlsx"
        df = self.s3.read_excel_first_existing_sheet(key, preferred_sheets=["TF", 0])
        if df.empty:
            raise RuntimeError(f"Не удалось прочитать РРЦ: {key}")

        article_col = first_existing_col(df, ["ПРАВИЛЬНЫЙ АРТИКУЛ", "Артикул 1С", "Артикул", "article", "vendorCode"])
        rrc_col = first_existing_col(df, ["РРЦ", "RRC", "rrc", "Цена РРЦ"])
        name_col = first_existing_col(df, ["Наименование", "Название", "name"])
        barcode_col = first_existing_col(df, ["ШК", "Баркод", "barcode"])

        if not article_col or not rrc_col:
            raise RuntimeError(f"В РРЦ не найдены обязательные колонки. Колонки файла: {list(df.columns)}")

        out = pd.DataFrame()
        out["article_rrc"] = df[article_col].astype(str).str.strip()
        out["article_rrc_norm"] = out["article_rrc"].map(normalize_article)
        out["rrc"] = df[rrc_col].map(to_float_or_none)
        out["rrc_name"] = df[name_col] if name_col else ""
        out["rrc_barcode"] = df[barcode_col] if barcode_col else ""
        out = out[(out["article_rrc_norm"] != "") & (out["rrc"].notna()) & (out["rrc"] > 0)].copy()
        out = out.drop_duplicates(subset=["article_rrc_norm"], keep="last")
        log(f"РРЦ загружен: строк={len(out)}")
        return out

    def load_article_reference(self) -> pd.DataFrame:
        candidates = [
            f"Отчёты/Финансовые показатели/{self.cfg.store}/Артикулы 1с.xlsx",
            f"Отчёты/Финансовые показатели/{self.cfg.store}/Артикулы 1С.xlsx",
            f"Служебные файлы/Ассистент WB/{self.cfg.store}/Артикулы 1с.xlsx",
            f"Служебные файлы/Корректировка цен/{self.cfg.store}/Артикулы 1с.xlsx",
            f"Отчёты/Остатки/{self.cfg.store}/Артикулы 1с.xlsx",
            "Артикулы 1с.xlsx",
            "Артикулы 1С.xlsx",
        ]
        key = next((k for k in candidates if self.s3.file_exists(k)), None)
        if not key:
            key = self.s3.find_first_key(
                prefixes=[
                    f"Отчёты/Финансовые показатели/{self.cfg.store}/",
                    f"Служебные файлы/Ассистент WB/{self.cfg.store}/",
                    f"Служебные файлы/Корректировка цен/{self.cfg.store}/",
                    "",
                ],
                keywords=["Артикулы", "1"],
                suffix=".xlsx",
            )
        if not key:
            raise RuntimeError("Не найден файл справочника Артикулы 1с.xlsx в S3")

        log(f"Читаю справочник артикулов: {key}")
        df = self.s3.read_excel(key, sheet_name=0)
        if df.empty:
            raise RuntimeError(f"Справочник артикулов пустой: {key}")

        nm_col = first_existing_col(df, ["Артикул WB", "nmID", "nmId", "nm_id"])
        vendor_col = first_existing_col(df, ["Артикул", "Артикул продавца", "supplierArticle", "vendorCode"])
        art1c_col = first_existing_col(df, ["Артикул 1С", "Артикул 1с", "article_1c", "ПРАВИЛЬНЫЙ АРТИКУЛ"])

        if not nm_col or not art1c_col:
            raise RuntimeError(f"В справочнике не найдены Артикул WB / Артикул 1С. Колонки: {list(df.columns)}")

        out = pd.DataFrame()
        out["nmID"] = df[nm_col].map(to_int_or_none)
        out["supplierArticle_ref"] = df[vendor_col].astype(str).str.strip() if vendor_col else ""
        out["article_1c"] = df[art1c_col].astype(str).str.strip()
        out["supplierArticle_ref_norm"] = out["supplierArticle_ref"].map(normalize_article)
        out["article_1c_norm"] = out["article_1c"].map(normalize_article)
        out = out[out["nmID"].notna()].copy()
        out["nmID"] = out["nmID"].astype(int)
        out = out.drop_duplicates(subset=["nmID"], keep="last")
        log(f"Справочник артикулов загружен: строк={len(out)}")
        return out

    def load_recent_orders_history(self, today_orders: pd.DataFrame) -> pd.DataFrame:
        """Берёт заказы из последних недельных файлов для fallback по SPP и subject."""
        prefix = f"Отчёты/Заказы/{self.cfg.store}/Недельные/"
        keys = self.s3.list_files(prefix)
        keys = [k for k in keys if k.lower().endswith(".xlsx")]
        keys = sorted(keys)[-8:]  # последние недели, без лишней тяжести
        frames = []
        for key in keys:
            try:
                df = self.s3.read_excel(key, sheet_name=0)
                if not df.empty:
                    frames.append(df)
            except Exception as e:
                log(f"Не удалось прочитать недельный файл заказов {key}: {e}", "WARN")
        if not today_orders.empty:
            frames.append(today_orders)
        if not frames:
            return pd.DataFrame()
        hist = pd.concat(frames, ignore_index=True, sort=False)
        if "date" in hist.columns:
            hist["date"] = pd.to_datetime(hist["date"], errors="coerce")
            min_dt = now_msk().replace(tzinfo=None) - timedelta(days=self.cfg.fallback_days)
            hist = hist[(hist["date"].isna()) | (hist["date"] >= min_dt)].copy()
        if "nmId" in hist.columns and "nmID" not in hist.columns:
            hist["nmID"] = hist["nmId"]
        elif "nmID" not in hist.columns and "Артикул WB" in hist.columns:
            hist["nmID"] = hist["Артикул WB"]
        hist["nmID"] = hist["nmID"].map(to_int_or_none) if "nmID" in hist.columns else None
        hist = hist[hist["nmID"].notna()].copy()
        hist["nmID"] = hist["nmID"].astype(int)
        log(f"История заказов для fallback: строк={len(hist)}")
        return hist

    # ---------- Calculations ----------

    def build_subject_map(self, orders_history: pd.DataFrame, goods_df: pd.DataFrame) -> pd.DataFrame:
        rows = []
        if not goods_df.empty:
            subj_col = first_existing_col(goods_df, ["subject_api", "subject", "Предмет"])
            if subj_col:
                tmp = goods_df[["nmID", subj_col]].copy()
                tmp.columns = ["nmID", "subject"]
                tmp = tmp[tmp["subject"].notna() & (tmp["subject"].astype(str).str.strip() != "")].copy()
                if not tmp.empty:
                    tmp["subject_source"] = "prices_api"
                    rows.append(tmp)

        if not orders_history.empty:
            subj_col = first_existing_col(orders_history, ["subject", "Предмет", "subjectName", "Название предмета"])
            if subj_col:
                tmp = orders_history[["nmID", subj_col]].copy()
                tmp.columns = ["nmID", "subject"]
                tmp["subject_source"] = "orders_history"
                tmp = tmp[tmp["subject"].notna() & (tmp["subject"].astype(str).str.strip() != "")]
                # последняя известная категория по товару
                tmp = tmp.drop_duplicates(subset=["nmID"], keep="last")
                rows.append(tmp)

        if not rows:
            return pd.DataFrame(columns=["nmID", "subject", "subject_source"])
        out = pd.concat(rows, ignore_index=True)
        out["subject_norm"] = out["subject"].map(normalize_text)
        out = out[out["nmID"].notna()].copy()
        out["nmID"] = out["nmID"].astype(int)
        # prices_api приоритетнее history, поэтому keep first
        out["priority"] = out["subject_source"].map({"prices_api": 0, "orders_history": 1}).fillna(9)
        out = out.sort_values(["nmID", "priority"]).drop_duplicates(subset=["nmID"], keep="first")
        return out.drop(columns=["priority"])

    def build_spp_table(self, today_orders: pd.DataFrame, orders_history: pd.DataFrame) -> pd.DataFrame:
        """Возвращает avg_spp по nmID с fallback: 3h -> today -> history -> subject/global."""
        all_rows = orders_history.copy() if not orders_history.empty else today_orders.copy()
        if all_rows.empty:
            return pd.DataFrame(columns=["nmID", "avg_spp", "spp_source", "orders_3h", "orders_today", "orders_history"])

        if "nmID" not in all_rows.columns:
            if "nmId" in all_rows.columns:
                all_rows["nmID"] = all_rows["nmId"]
            elif "Артикул WB" in all_rows.columns:
                all_rows["nmID"] = all_rows["Артикул WB"]
        all_rows["nmID"] = all_rows["nmID"].map(to_int_or_none)
        all_rows = all_rows[all_rows["nmID"].notna()].copy()
        all_rows["nmID"] = all_rows["nmID"].astype(int)

        if "spp" not in all_rows.columns:
            all_rows["spp"] = None
        all_rows["spp_num"] = all_rows["spp"].map(to_float_or_none)
        all_rows = all_rows[all_rows["spp_num"].notna()].copy()

        if "date" in all_rows.columns:
            all_rows["date"] = pd.to_datetime(all_rows["date"], errors="coerce")
        else:
            all_rows["date"] = pd.NaT

        if "isCancel" in all_rows.columns:
            all_rows = all_rows[~is_cancelled_series(all_rows["isCancel"])].copy()

        current_naive = now_msk().replace(tzinfo=None)
        three_hours_ago = current_naive - timedelta(hours=3)
        today_d = now_msk().date()

        by3 = all_rows[(all_rows["date"].notna()) & (all_rows["date"] >= three_hours_ago)].copy()
        byt = all_rows[(all_rows["date"].notna()) & (all_rows["date"].dt.date == today_d)].copy()
        hist = all_rows.copy()

        sku_3h = self._agg_spp(by3, "spp_3h")
        sku_today = self._agg_spp(byt, "spp_today")
        sku_hist = self._agg_spp(hist, "spp_history")

        base = pd.DataFrame({"nmID": sorted(hist["nmID"].dropna().astype(int).unique())})
        out = base.merge(sku_3h, on="nmID", how="left").merge(sku_today, on="nmID", how="left").merge(sku_hist, on="nmID", how="left")

        def choose(row):
            if pd.notna(row.get("avg_spp_spp_3h")):
                return row.get("avg_spp_spp_3h"), "sku_3h"
            if pd.notna(row.get("avg_spp_spp_today")):
                return row.get("avg_spp_spp_today"), "sku_today"
            if pd.notna(row.get("avg_spp_spp_history")):
                return row.get("avg_spp_spp_history"), "sku_history"
            return None, "no_spp"

        chosen = out.apply(choose, axis=1, result_type="expand")
        out["avg_spp"] = chosen[0]
        out["spp_source"] = chosen[1]
        out["orders_3h"] = out.get("orders_spp_3h", 0).fillna(0).astype(int)
        out["orders_today"] = out.get("orders_spp_today", 0).fillna(0).astype(int)
        out["orders_history"] = out.get("orders_spp_history", 0).fillna(0).astype(int)

        # subject/global fallback добавим позднее после merge с category, здесь оставляем SKU fallback.
        return out[["nmID", "avg_spp", "spp_source", "orders_3h", "orders_today", "orders_history"]]

    @staticmethod
    def _agg_spp(df: pd.DataFrame, suffix: str) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=["nmID", f"avg_spp_{suffix}", f"orders_{suffix}"])
        g = df.groupby("nmID", as_index=False).agg(
            **{
                f"avg_spp_{suffix}": ("spp_num", "mean"),
                f"orders_{suffix}": ("spp_num", "size"),
            }
        )
        return g

    def add_subject_and_global_spp_fallback(self, calc: pd.DataFrame, orders_history: pd.DataFrame) -> pd.DataFrame:
        """Для товаров без SKU-SPP добавляет fallback по subject/global."""
        if calc.empty or orders_history.empty:
            return calc
        if "spp" not in orders_history.columns:
            return calc

        hist = orders_history.copy()
        if "nmID" not in hist.columns and "nmId" in hist.columns:
            hist["nmID"] = hist["nmId"]
        hist["spp_num"] = hist["spp"].map(to_float_or_none)
        hist = hist[hist["spp_num"].notna()].copy()
        if "isCancel" in hist.columns:
            hist = hist[~is_cancelled_series(hist["isCancel"])].copy()
        subj_col = first_existing_col(hist, ["subject", "Предмет", "subjectName", "Название предмета"])
        if subj_col:
            hist["subject_norm"] = hist[subj_col].map(normalize_text)
            subject_spp = hist.groupby("subject_norm", as_index=False).agg(
                avg_spp_subject=("spp_num", "mean"),
                orders_subject=("spp_num", "size"),
            )
            calc = calc.merge(subject_spp, on="subject_norm", how="left")
        else:
            calc["avg_spp_subject"] = None
            calc["orders_subject"] = 0

        global_spp = hist["spp_num"].mean() if not hist.empty else None
        missing = calc["avg_spp"].isna()
        use_subject = missing & calc["avg_spp_subject"].notna()
        calc.loc[use_subject, "avg_spp"] = calc.loc[use_subject, "avg_spp_subject"]
        calc.loc[use_subject, "spp_source"] = "subject_history"

        missing = calc["avg_spp"].isna()
        if global_spp is not None and not math.isnan(float(global_spp)):
            calc.loc[missing, "avg_spp"] = float(global_spp)
            calc.loc[missing, "spp_source"] = "global_history"
        return calc

    def build_calculation(self, today_orders: pd.DataFrame, goods_df: pd.DataFrame, rrc_df: pd.DataFrame, ref_df: pd.DataFrame, orders_history: pd.DataFrame) -> pd.DataFrame:
        # Универс товаров: текущие цены WB + справочник + РРЦ.
        if goods_df.empty:
            log("Текущие цены WB не получены; строю универс по справочнику Артикулы 1С", "WARN")
            universe = ref_df.copy()
            universe["current_wb_price"] = None
            universe["current_wb_discount"] = None
            universe["current_wb_discounted_price"] = None
            universe["supplierArticle_api"] = ""
            universe["subject_api"] = ""
            universe["name_api"] = ""
        else:
            universe = goods_df.merge(ref_df, on="nmID", how="left")
            # если в API нет supplierArticle, берём из справочника
            universe["supplierArticle_ref"] = universe.get("supplierArticle_ref", "")
            universe["article_1c"] = universe.get("article_1c", "")
            universe["article_1c_norm"] = universe.get("article_1c_norm", "")

        # Если справочник не подтянулся по nmID, попробуем подтянуть по supplierArticle из API.
        if not goods_df.empty and "supplierArticle_api_norm" in universe.columns:
            missing_ref = universe["article_1c_norm"].isna() | (universe["article_1c_norm"].astype(str) == "")
            ref_by_vendor = ref_df[ref_df["supplierArticle_ref_norm"] != ""].drop_duplicates("supplierArticle_ref_norm", keep="last")
            add = universe.loc[missing_ref, ["supplierArticle_api_norm"]].merge(
                ref_by_vendor,
                left_on="supplierArticle_api_norm",
                right_on="supplierArticle_ref_norm",
                how="left",
                suffixes=("", "_ref2"),
            )
            if not add.empty:
                # add имеет тот же порядок строк, что universe.loc[missing_ref].
                # Заполняем только пустые значения справочника без нарушения индексов.
                target_index = universe.index[missing_ref]
                add = add.reset_index(drop=True)
                for col in ["article_1c", "article_1c_norm", "supplierArticle_ref", "supplierArticle_ref_norm"]:
                    ref2 = f"{col}_ref2" if f"{col}_ref2" in add.columns else col
                    if ref2 in add.columns and col in universe.columns:
                        series_to_add = pd.Series(add[ref2].values, index=target_index)
                        empty_col = universe.loc[target_index, col].isna() | (universe.loc[target_index, col].astype(str) == "")
                        universe.loc[target_index[empty_col], col] = series_to_add.loc[target_index[empty_col]]

        # РРЦ по article_1c_norm.
        calc = universe.merge(rrc_df, left_on="article_1c_norm", right_on="article_rrc_norm", how="left")

        # Subject.
        subject_map = self.build_subject_map(orders_history, goods_df)
        if not subject_map.empty:
            calc = calc.merge(subject_map[["nmID", "subject", "subject_norm", "subject_source"]], on="nmID", how="left")
        else:
            calc["subject"] = calc.get("subject_api", "")
            calc["subject_norm"] = calc["subject"].map(normalize_text)
            calc["subject_source"] = "unknown"

        # Если subject из goods API есть, а subject_map не дал, используем API.
        if "subject_api" in calc.columns:
            empty_subject = calc["subject"].isna() | (calc["subject"].astype(str).str.strip() == "")
            api_subject_present = calc["subject_api"].notna() & (calc["subject_api"].astype(str).str.strip() != "")
            fill_from_api = empty_subject & api_subject_present
            calc.loc[fill_from_api, "subject"] = calc.loc[fill_from_api, "subject_api"]
            if "subject_source" in calc.columns:
                calc.loc[fill_from_api, "subject_source"] = "prices_api"
            calc["subject_norm"] = calc["subject"].map(normalize_text)

        # Если subject всё равно неизвестен, не блокируем товар автоматически:
        # помады/блески дополнительно отсекаются по названию из РРЦ.
        calc["excluded_by_rrc_name"] = calc["rrc_name"].map(excluded_by_rrc_name) if "rrc_name" in calc.columns else False
        calc["excluded_rrc_keyword"] = calc["rrc_name"].map(excluded_rrc_keyword) if "rrc_name" in calc.columns else ""

        # SPP.
        spp = self.build_spp_table(today_orders, orders_history)
        calc = calc.merge(spp, on="nmID", how="left")
        calc = self.add_subject_and_global_spp_fallback(calc, orders_history)

        # Расчёт цен.
        calc["target_factor"] = self.cfg.target_factor
        calc["target_finishedPrice"] = calc["rrc"].apply(lambda x: int(round(float(x) * self.cfg.target_factor)) if pd.notna(x) else None)
        calc["new_discount"] = int(self.cfg.seller_discount)
        calc["target_priceWithDisc"] = calc.apply(self._calc_target_price_with_disc, axis=1)
        calc["new_price"] = calc.apply(self._calc_new_price, axis=1)

        calc["current_wb_price"] = calc["current_wb_price"].map(to_float_or_none) if "current_wb_price" in calc.columns else None
        calc["current_wb_discount"] = calc["current_wb_discount"].map(to_float_or_none) if "current_wb_discount" in calc.columns else None
        calc["old_priceWithDisc_calc"] = calc.apply(self._calc_old_price_with_disc, axis=1)
        calc["delta_price"] = calc.apply(lambda r: (r["new_price"] - r["current_wb_price"]) if pd.notna(r.get("new_price")) and pd.notna(r.get("current_wb_price")) else None, axis=1)
        calc["delta_price_pct"] = calc.apply(self._calc_delta_pct, axis=1)

        calc["decision"] = ""
        calc["reason"] = ""
        calc = self.apply_decision_rules(calc)

        # Удобный порядок колонок.
        columns_order = [
            "decision", "reason", "nmID", "supplierArticle_api", "supplierArticle_ref", "article_1c", "article_rrc",
            "subject", "subject_source", "rrc", "target_factor", "target_finishedPrice", "avg_spp", "spp_source",
            "target_priceWithDisc", "new_price", "new_discount", "current_wb_price", "current_wb_discount",
            "current_wb_discounted_price", "old_priceWithDisc_calc", "delta_price", "delta_price_pct",
            "orders_3h", "orders_today", "orders_history", "name_api", "rrc_name", "excluded_by_rrc_name",
            "excluded_rrc_keyword", "currencyIsoCode4217",
        ]
        existing = [c for c in columns_order if c in calc.columns]
        rest = [c for c in calc.columns if c not in existing]
        return calc[existing + rest]

    def _calc_target_price_with_disc(self, row: pd.Series) -> Optional[int]:
        target = to_float_or_none(row.get("target_finishedPrice"))
        spp = to_float_or_none(row.get("avg_spp"))
        if target is None or spp is None or spp >= 95 or spp < 0:
            return None
        return int(round(target / (1 - spp / 100)))

    def _calc_new_price(self, row: pd.Series) -> Optional[int]:
        price_with_disc = to_float_or_none(row.get("target_priceWithDisc"))
        discount = to_float_or_none(row.get("new_discount"))
        if price_with_disc is None or discount is None or discount >= 95 or discount < 0:
            return None
        return int(round(price_with_disc / (1 - discount / 100)))

    def _calc_old_price_with_disc(self, row: pd.Series) -> Optional[float]:
        price = to_float_or_none(row.get("current_wb_price"))
        discount = to_float_or_none(row.get("current_wb_discount"))
        if price is None or discount is None or discount >= 100:
            return None
        return round(price * (1 - discount / 100), 2)

    def _calc_delta_pct(self, row: pd.Series) -> Optional[float]:
        old = to_float_or_none(row.get("current_wb_price"))
        new = to_float_or_none(row.get("new_price"))
        if old is None or old == 0 or new is None:
            return None
        return round((new - old) / old * 100, 2)

    def apply_decision_rules(self, calc: pd.DataFrame) -> pd.DataFrame:
        calc = calc.copy()
        reasons: List[str] = []
        decisions: List[str] = []
        for _, r in calc.iterrows():
            decision = "send"
            reason_parts = []

            nm_id = to_int_or_none(r.get("nmID"))
            subject_norm = normalize_text(r.get("subject"))
            current_price = to_float_or_none(r.get("current_wb_price"))
            current_discount = to_float_or_none(r.get("current_wb_discount"))
            old_price_with_disc = to_float_or_none(r.get("old_priceWithDisc_calc"))
            new_price = to_float_or_none(r.get("new_price"))
            new_discount = to_float_or_none(r.get("new_discount"))
            target_price_with_disc = to_float_or_none(r.get("target_priceWithDisc"))
            rrc = to_float_or_none(r.get("rrc"))
            spp = to_float_or_none(r.get("avg_spp"))
            delta_pct = to_float_or_none(r.get("delta_price_pct"))

            if not nm_id:
                decision = "skip"
                reason_parts.append("нет nmID")
            if rrc is None or rrc <= 0:
                decision = "skip"
                reason_parts.append("нет РРЦ")
            excluded_by_name = bool(r.get("excluded_by_rrc_name")) or excluded_by_rrc_name(r.get("rrc_name")) or excluded_by_rrc_name(r.get("name_api"))
            excluded_keyword = str(r.get("excluded_rrc_keyword") or excluded_rrc_keyword(r.get("rrc_name")) or excluded_rrc_keyword(r.get("name_api")) or "")

            if subject_norm in EXCLUDED_SUBJECTS:
                decision = "skip"
                reason_parts.append("исключённый subject: Помады/Блески")
            elif excluded_by_name:
                decision = "skip"
                if excluded_keyword:
                    reason_parts.append(f"исключено по названию РРЦ/товара: {excluded_keyword}")
                else:
                    reason_parts.append("исключено по названию РРЦ/товара: Помады/Блески")
            elif not subject_norm:
                # WB price API часто не отдаёт subject. Если РРЦ есть и название не похоже
                # на помаду/блеск, товар можно корректировать: иначе весь прайс-лист будет пропущен.
                if rrc is None or rrc <= 0:
                    decision = "skip"
                    reason_parts.append("неизвестный subject")
                elif not self.cfg.allow_unknown_subject:
                    reason_parts.append("subject неизвестен, разрешено по РРЦ: не Помады/Блески")
                else:
                    reason_parts.append("subject неизвестен, но разрешён параметром")
            if spp is None or spp < 0 or spp >= 95:
                decision = "skip"
                reason_parts.append("нет корректного SPP")
            if new_price is None or new_price <= 0 or new_discount is None:
                decision = "skip"
                reason_parts.append("не рассчитана новая цена")

            # Если цена уже корректная и скидка уже 26%, не отправляем.
            if decision == "send" and current_price is not None and current_discount is not None:
                same_price = abs(float(new_price) - float(current_price)) <= self.cfg.price_tolerance_rub
                same_discount = int(round(float(current_discount))) == int(self.cfg.seller_discount)
                if same_price and same_discount:
                    decision = "skip"
                    reason_parts.append("цена уже корректная")

            # Защита от карантина: новая цена после скидки продавца не должна быть в 3 раза ниже старой.
            if decision == "send" and old_price_with_disc is not None and target_price_with_disc is not None:
                if target_price_with_disc < old_price_with_disc / 3:
                    decision = "skip"
                    reason_parts.append("риск карантина WB: цена со скидкой >3 раза ниже старой")

            # Защита от явных выбросов.
            if decision == "send" and self.cfg.max_price_change_pct and self.cfg.max_price_change_pct > 0:
                if delta_pct is not None and abs(delta_pct) > self.cfg.max_price_change_pct:
                    decision = "skip"
                    reason_parts.append(f"изменение price {delta_pct}% больше лимита {self.cfg.max_price_change_pct}%")

            if decision == "send":
                reason_parts.append(f"РРЦ*{self.cfg.target_factor:.2f}, discount={self.cfg.seller_discount}%, SPP={spp:.2f}%")

            decisions.append(decision)
            reasons.append("; ".join(reason_parts))

        calc["decision"] = decisions
        calc["reason"] = reasons
        return calc

    # ---------- Save reports ----------

    def save_result_files(self, calc: pd.DataFrame, response_df: Optional[pd.DataFrame] = None):
        response_df = response_df if response_df is not None else pd.DataFrame()
        run_dt = now_msk().strftime("%Y-%m-%d %H:%M:%S")
        calc_out = calc.copy()
        calc_out.insert(0, "datetime_run", run_dt)

        to_send = calc_out[calc_out["decision"] == "send"].copy()
        skipped = calc_out[calc_out["decision"] != "send"].copy()
        errors = skipped[skipped["reason"].astype(str).str.contains("нет|риск|больше лимита|не", case=False, na=False)].copy()

        key_last = f"{self.service_prefix}/Расчёт_цен_последний.xlsx"
        sheets = {
            "Расчёт": calc_out,
            "К_отправке": to_send,
            "Пропущено": skipped,
            "Ошибки": errors,
            "Ответ_WB": response_df,
        }
        self.s3.write_excel_multi(key_last, sheets)

        # История изменений: добавляем только отправленные / рассчитанные к отправке строки.
        hist_key = f"{self.service_prefix}/История_изменений_цен.xlsx"
        hist_new = to_send.copy()
        if not hist_new.empty:
            if not response_df.empty:
                task_ids = ", ".join([str(x) for x in response_df.get("task_id", pd.Series(dtype=str)).dropna().astype(str).unique() if str(x)])
                ok = bool(response_df.get("ok", pd.Series(dtype=bool)).all()) if "ok" in response_df.columns else False
                hist_new["wb_task_id"] = task_ids
                hist_new["wb_response_ok"] = ok
            else:
                hist_new["wb_task_id"] = ""
                hist_new["wb_response_ok"] = "dry_run"

            old_hist = self.s3.read_excel(hist_key, sheet_name=0) if self.s3.file_exists(hist_key) else pd.DataFrame()
            hist_combined = pd.concat([old_hist, hist_new], ignore_index=True, sort=False) if not old_hist.empty else hist_new
            self.s3.write_excel(hist_key, hist_combined, sheet_name="История")

    def run(self, apply: bool = False) -> Tuple[pd.DataFrame, pd.DataFrame]:
        log(f"Старт корректировки цен: store={self.cfg.store}, apply={apply}")

        today_orders = self.fetch_today_orders()
        log(f"Заказы сегодня: строк={len(today_orders)}")
        self.save_today_orders_snapshot(today_orders)

        rrc_df = self.load_rrc()
        ref_df = self.load_article_reference()
        goods_df = self.fetch_current_goods_prices()
        orders_history = self.load_recent_orders_history(today_orders)

        calc = self.build_calculation(today_orders, goods_df, rrc_df, ref_df, orders_history)
        send_df = calc[calc["decision"] == "send"].copy()
        log(f"Расчёт готов: всего={len(calc)}, к отправке={len(send_df)}, пропущено={len(calc) - len(send_df)}")

        response_df = pd.DataFrame()
        if apply:
            response_df = self.upload_prices(send_df)
        else:
            response_df = pd.DataFrame([{
                "status": "dry_run",
                "ok": True,
                "sent_count": 0,
                "message": "Цены не отправлялись. Для отправки используйте --apply",
            }])

        self.save_result_files(calc, response_df)
        log("Готово")
        return calc, response_df


# ========================== CLI ==========================

def build_s3_from_env() -> S3Storage:
    required = ["YC_ACCESS_KEY_ID", "YC_SECRET_ACCESS_KEY", "YC_BUCKET_NAME", "WB_PROMO_KEY_TOPFACE"]
    missing = [x for x in required if not os.environ.get(x)]
    if missing:
        raise RuntimeError(f"Отсутствуют переменные окружения: {missing}")
    return S3Storage(
        access_key=os.environ["YC_ACCESS_KEY_ID"],
        secret_key=os.environ["YC_SECRET_ACCESS_KEY"],
        bucket_name=os.environ["YC_BUCKET_NAME"],
    )


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="WB price corrector: target finishedPrice = РРЦ * 0.9")
    sub = parser.add_subparsers(dest="command")

    run_parser = sub.add_parser("run", help="Сделать расчёт и при --apply отправить цены в WB")
    run_parser.add_argument("--store", default=DEFAULT_STORE, help="Магазин, по умолчанию TOPFACE")
    mode = run_parser.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true", help="Отправить изменения цен в WB")
    mode.add_argument("--dry-run", action="store_true", help="Только расчёт без отправки")
    run_parser.add_argument("--target-factor", type=float, default=DEFAULT_TARGET_FACTOR, help="Целевой коэффициент к РРЦ, по умолчанию 0.9")
    run_parser.add_argument("--seller-discount", type=int, default=DEFAULT_SELLER_DISCOUNT, help="Скидка продавца, %, по умолчанию 26")
    run_parser.add_argument("--price-tolerance-rub", type=int, default=DEFAULT_PRICE_TOLERANCE_RUB, help="Не отправлять, если отличие price <= N рублей")
    run_parser.add_argument("--max-price-change-pct", type=float, default=DEFAULT_MAX_PRICE_CHANGE_PCT, help="Макс. изменение price за запуск, 0 = отключить")
    run_parser.add_argument("--fallback-days", type=int, default=DEFAULT_FALLBACK_DAYS, help="Сколько последних дней заказов читать для fallback SPP")
    run_parser.add_argument("--allow-unknown-subject", action="store_true", help="Разрешить менять товары без известного subject")
    run_parser.add_argument("--update-weekly-orders", action="store_true", help="Опционально обновлять сегодняшний день в обычном недельном файле заказов")

    # Удобство: если запустили без команды — ведём как run --dry-run.
    args = parser.parse_args(argv)
    if args.command is None:
        args.command = "run"
        args.store = DEFAULT_STORE
        args.apply = False
        args.dry_run = True
        args.target_factor = DEFAULT_TARGET_FACTOR
        args.seller_discount = DEFAULT_SELLER_DISCOUNT
        args.price_tolerance_rub = DEFAULT_PRICE_TOLERANCE_RUB
        args.max_price_change_pct = DEFAULT_MAX_PRICE_CHANGE_PCT
        args.fallback_days = DEFAULT_FALLBACK_DAYS
        args.allow_unknown_subject = False
        args.update_weekly_orders = False
    return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    if args.command != "run":
        raise RuntimeError(f"Неизвестная команда: {args.command}")

    s3 = build_s3_from_env()
    cfg = PriceCorrectorConfig(
        store=args.store,
        target_factor=args.target_factor,
        seller_discount=args.seller_discount,
        price_tolerance_rub=args.price_tolerance_rub,
        max_price_change_pct=args.max_price_change_pct,
        fallback_days=args.fallback_days,
        allow_unknown_subject=args.allow_unknown_subject,
        update_weekly_orders=args.update_weekly_orders,
    )
    corrector = WBPriceCorrector(s3=s3, wb_key=os.environ["WB_PROMO_KEY_TOPFACE"], cfg=cfg)
    apply_changes = bool(args.apply)
    corrector.run(apply=apply_changes)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        log(f"Критическая ошибка: {exc}", "ERROR")
        traceback.print_exc()
        raise SystemExit(1)
