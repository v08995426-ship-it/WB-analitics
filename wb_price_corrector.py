#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Корректировка цен Wildberries под целевую цену покупателя:
- с 23:00 до 04:59 по Москве: РРЦ * 0.8;
- с 05:00 до 22:59 по Москве: РРЦ * 0.9.

Логика:
- плановый запуск: 23:00 и 05:00 по Москве;
- забирает заказы за сегодняшний и предыдущий день через statistics-api;
- SPP считает самостоятельно по строкам заказов: (1 - finishedPrice / priceWithDisc) * 100;
- базовый SPP берёт как spp_raw_max, округлённый вверх;
- к найденному SPP добавляет поправку +1.5 п.п. по умолчанию;
- для оттенков одной группы, например 501/5 и 501/19, сначала пытается использовать общий SPP группы;
- group fallback применяется только если средний SPP между оттенками отличается не больше чем на 3 п.п.;
- если по товару/группе меньше 10 значений SPP, период не используется.
- читает РРЦ из S3: Отчёты/Финансовые показатели/<STORE>/РРЦ.xlsx;
- читает справочник артикулов 1С из S3;
- исключает subject: Помады, Блески;
- выбирает целевой коэффициент по московскому времени: ночь 0.8, день 0.9;
- считает новую WB price при фиксированной скидке продавца 26%;
- отправляет price + discount в /api/v2/upload/task только в режиме --apply;
- в режиме --dry-run только сохраняет расчёт.

Важно:
- штатные недельные файлы "Отчёты/Заказы/..." по умолчанию НЕ перезаписываются,
  чтобы ежедневный сборщик не принял сегодняшний оперативный срез за закрытый день.
- свежие заказы за сегодня сохраняются в служебной папке корректировщика;
- заказы за предыдущий день хранятся отдельно и используются как основной fallback для SPP.

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
PUBLIC_CARD_DETAIL_API_URL = "https://card.wb.ru/cards/v2/detail"

DEFAULT_STORE = "TOPFACE"
DEFAULT_DAY_TARGET_FACTOR = 0.90
DEFAULT_NIGHT_TARGET_FACTOR = 0.80
DEFAULT_NIGHT_START_HOUR = 23
DEFAULT_NIGHT_END_HOUR = 5
DEFAULT_TARGET_FACTOR = DEFAULT_DAY_TARGET_FACTOR  # legacy/fixed override value
DEFAULT_SELLER_DISCOUNT = 26
DEFAULT_PRICE_TOLERANCE_RUB = 1
DEFAULT_MAX_PRICE_CHANGE_PCT = 80.0  # 0 = отключить ограничение
DEFAULT_FALLBACK_DAYS = 21
DEFAULT_MIN_SPP_SAMPLE_ORDERS = 10
DEFAULT_PRICE_SOURCE = "orders-spp"  # боевой режим: считаем по заказам, сайт WB не используем
DEFAULT_SITE_PRICE_TOLERANCE_RUB = 3
# Экстренная защита от занижения: если фактическая WB-скидка/SPP из заказов
# меньше этого значения, в расчёте используется floor. 0 = отключить.
# Рекомендованный аварийный режим: 45.
DEFAULT_SAFE_WB_DISCOUNT_FLOOR_PCT = float(os.environ.get("WB_SAFE_WB_DISCOUNT_FLOOR_PCT", "0") or 0)
DEFAULT_SPP_ADJUSTMENT_PCT = float(os.environ.get("WB_SPP_ADJUSTMENT_PCT", "1.5") or 0)
DEFAULT_USE_SAFE_FLOOR_FOR_MISSING = (os.environ.get("WB_USE_SAFE_FLOOR_FOR_MISSING", "false").strip().lower() in {"1", "true", "yes", "y", "да"})
DEFAULT_PUBLIC_DEST = os.environ.get("WB_PUBLIC_DEST", "-1257786")
DEFAULT_PUBLIC_PRICE_CHUNK_SIZE = 80

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


def extract_shade_group(value: Any) -> str:
    """Возвращает базовую группу оттенков: 501/5 -> 501, PT501R.005K -> 501, PT901.F05 -> 901."""
    s = normalize_article(value)
    if not s:
        return ""
    m = re.match(r"^PT(\d+)", s)
    if m:
        return m.group(1).lstrip("0") or m.group(1)
    m = re.match(r"^(\d+)[/\.]", s)
    if m:
        return m.group(1).lstrip("0") or m.group(1)
    m = re.match(r"^(\d+)", s)
    if m:
        return m.group(1).lstrip("0") or m.group(1)
    return ""


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


def chunked_list(values: Sequence[Any], size: int) -> Iterable[List[Any]]:
    for i in range(0, len(values), size):
        yield list(values[i:i + size])


def wb_public_price_to_rub(value: Any) -> Optional[float]:
    """Преобразует цену WB public API в рубли. В старых/новых ответах цена может быть в копейках."""
    v = to_float_or_none(value)
    if v is None or v <= 0:
        return None
    # В ответах WB часто встречаются поля priceU/salePriceU или price.total в копейках.
    # Для косметики рублёвая цена почти всегда меньше 10 000, поэтому >10 000 считаем копейками.
    if v > 10000:
        v = v / 100.0
    return round(float(v), 2)


def extract_public_price_from_product(product: Dict[str, Any]) -> Dict[str, Any]:
    """Достаёт финальную цену карточки из ответа card.wb.ru/cards/v2/detail."""
    nm_id = to_int_or_none(product.get("id") or product.get("nmID") or product.get("nmId"))
    name = product.get("name") or product.get("title") or ""
    brand = product.get("brand") or product.get("brandName") or ""

    price_objs: List[Dict[str, Any]] = []
    if isinstance(product.get("price"), dict):
        price_objs.append(product.get("price"))
    sizes = product.get("sizes") if isinstance(product.get("sizes"), list) else []
    for size in sizes:
        if isinstance(size, dict):
            if isinstance(size.get("price"), dict):
                price_objs.append(size.get("price"))
            opts = size.get("options") if isinstance(size.get("options"), list) else []
            for opt in opts:
                if isinstance(opt, dict) and isinstance(opt.get("price"), dict):
                    price_objs.append(opt.get("price"))

    site_final = None
    site_product = None
    site_basic = None
    raw_price_obj = {}
    for price_obj in price_objs:
        if not isinstance(price_obj, dict):
            continue
        # total — обычно финальная цена покупателя, product — цена после скидки продавца, basic — до скидок.
        site_final = wb_public_price_to_rub(
            price_obj.get("total") or price_obj.get("salePriceU") or price_obj.get("salePrice")
            or price_obj.get("totalPrice") or price_obj.get("price")
        )
        site_product = wb_public_price_to_rub(price_obj.get("product") or price_obj.get("productPrice"))
        site_basic = wb_public_price_to_rub(price_obj.get("basic") or price_obj.get("basicPrice") or price_obj.get("priceU"))
        raw_price_obj = price_obj
        if site_final is not None:
            break

    # Старый формат иногда лежит напрямую на product.
    if site_final is None:
        site_final = wb_public_price_to_rub(product.get("salePriceU") or product.get("salePrice") or product.get("priceU"))
    if site_basic is None:
        site_basic = wb_public_price_to_rub(product.get("priceU") or product.get("price"))

    return {
        "nmID": nm_id,
        "site_final_price": site_final,
        "site_product_price": site_product,
        "site_basic_price": site_basic,
        "site_name": name,
        "site_brand": brand,
        "site_price_raw": json.dumps(raw_price_obj, ensure_ascii=False)[:1000] if raw_price_obj else "",
    }


def fetch_public_site_prices_for_nmids(
    nmids: Sequence[int],
    dest: str = DEFAULT_PUBLIC_DEST,
    session: Optional[requests.Session] = None,
    chunk_size: int = DEFAULT_PUBLIC_PRICE_CHUNK_SIZE,
    timeout: int = 30,
) -> pd.DataFrame:
    """Получает фактические цены карточек WB по nmID через публичный endpoint сайта.

    Это не seller API и не официальный метод WB API. Используем только как контроль/обратную связь
    по цене, которую видит покупатель для выбранного dest.
    """
    ids = []
    for x in nmids:
        nm = to_int_or_none(x)
        if nm:
            ids.append(nm)
    ids = sorted(set(ids))
    if not ids:
        return pd.DataFrame(columns=[
            "nmID", "site_final_price", "site_product_price", "site_basic_price",
            "site_name", "site_brand", "site_price_raw", "site_price_source", "site_dest", "site_checked_at"
        ])

    sess = session or requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
        "Accept": "application/json,text/plain,*/*",
        "Origin": "https://www.wildberries.ru",
        "Referer": "https://www.wildberries.ru/",
    }
    rows: List[Dict[str, Any]] = []
    for chunk in chunked_list(ids, max(1, int(chunk_size))):
        params = {
            "appType": 1,
            "curr": "rub",
            "dest": str(dest),
            "lang": "ru",
            "ab_testing": "false",
            "nm": ",".join(str(x) for x in chunk),
        }
        try:
            resp = sess.get(PUBLIC_CARD_DETAIL_API_URL, headers=headers, params=params, timeout=timeout)
            if resp.status_code != 200:
                for nm in chunk:
                    rows.append({
                        "nmID": nm,
                        "site_final_price": None,
                        "site_price_source": f"card.wb.ru_http_{resp.status_code}",
                        "site_dest": str(dest),
                        "site_checked_at": now_msk().strftime("%Y-%m-%d %H:%M:%S"),
                    })
                continue
            payload = resp.json()
            products = payload.get("data", {}).get("products", []) if isinstance(payload, dict) else []
            by_id = {}
            for product in products:
                if isinstance(product, dict):
                    row = extract_public_price_from_product(product)
                    if row.get("nmID"):
                        by_id[int(row["nmID"])] = row
            for nm in chunk:
                row = by_id.get(nm, {"nmID": nm, "site_final_price": None})
                row["site_price_source"] = "card.wb.ru/cards/v2/detail"
                row["site_dest"] = str(dest)
                row["site_checked_at"] = now_msk().strftime("%Y-%m-%d %H:%M:%S")
                rows.append(row)
        except Exception as e:
            for nm in chunk:
                rows.append({
                    "nmID": nm,
                    "site_final_price": None,
                    "site_price_source": f"card.wb.ru_error: {str(e)[:120]}",
                    "site_dest": str(dest),
                    "site_checked_at": now_msk().strftime("%Y-%m-%d %H:%M:%S"),
                })
        time.sleep(0.25)
    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.drop_duplicates(subset=["nmID"], keep="last")
    return out


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
    # Если target_factor задан явно, скрипт работает в фиксированном режиме.
    # Если None — выбирает коэффициент автоматически по московскому времени:
    # 23:00..04:59 => night_target_factor, 05:00..22:59 => day_target_factor.
    target_factor: Optional[float] = None
    day_target_factor: float = DEFAULT_DAY_TARGET_FACTOR
    night_target_factor: float = DEFAULT_NIGHT_TARGET_FACTOR
    night_start_hour: int = DEFAULT_NIGHT_START_HOUR
    night_end_hour: int = DEFAULT_NIGHT_END_HOUR
    seller_discount: int = DEFAULT_SELLER_DISCOUNT
    price_tolerance_rub: int = DEFAULT_PRICE_TOLERANCE_RUB
    max_price_change_pct: float = DEFAULT_MAX_PRICE_CHANGE_PCT
    fallback_days: int = DEFAULT_FALLBACK_DAYS
    min_spp_sample_orders: int = DEFAULT_MIN_SPP_SAMPLE_ORDERS
    price_source: str = DEFAULT_PRICE_SOURCE
    public_dest: str = DEFAULT_PUBLIC_DEST
    site_price_tolerance_rub: int = DEFAULT_SITE_PRICE_TOLERANCE_RUB
    safe_wb_discount_floor_pct: float = DEFAULT_SAFE_WB_DISCOUNT_FLOOR_PCT
    spp_adjustment_pct: float = DEFAULT_SPP_ADJUSTMENT_PCT
    use_safe_floor_for_missing: bool = DEFAULT_USE_SAFE_FLOOR_FOR_MISSING
    allow_unknown_subject: bool = False
    update_weekly_orders: bool = False


class WBPriceCorrector:
    def __init__(self, s3: S3Storage, wb_key: str, cfg: PriceCorrectorConfig):
        self.s3 = s3
        self.wb_key = wb_key.strip()
        self.cfg = cfg
        self.session = requests.Session()
        self.service_prefix = f"Служебные файлы/Корректировка цен/{self.cfg.store}"
        self.run_datetime_msk = now_msk()
        self.active_target_factor, self.active_pricing_period = self.get_active_target_factor(self.run_datetime_msk)

    def get_active_target_factor(self, dt_msk: Optional[datetime] = None) -> Tuple[float, str]:
        """
        Возвращает целевой коэффициент к РРЦ для текущего московского времени.

        Ночной режим действует с night_start_hour:00 включительно до
        night_end_hour:00 НЕ включительно. При стандартных настройках:
        23:00..04:59 => 0.8, 05:00..22:59 => 0.9.

        Если --target-factor задан явно, автоматический график отключается.
        """
        if self.cfg.target_factor is not None:
            return float(self.cfg.target_factor), "fixed_override"

        dt_msk = dt_msk or now_msk()
        hour = int(dt_msk.hour)
        start = int(self.cfg.night_start_hour) % 24
        end = int(self.cfg.night_end_hour) % 24

        if start == end:
            is_night = True
        elif start < end:
            is_night = start <= hour < end
        else:
            # Период через полночь: например 23..5.
            is_night = hour >= start or hour < end

        if is_night:
            return float(self.cfg.night_target_factor), f"night_{start:02d}_to_{end:02d}"
        return float(self.cfg.day_target_factor), f"day_{end:02d}_to_{start:02d}"

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

    def fetch_orders_for_date(self, target_date: date, label: str = "") -> pd.DataFrame:
        """Получает все заказы за указанную дату. date сохраняется с временем."""
        date_str = target_date.strftime("%Y-%m-%d")
        headers = {"Authorization": self.wb_key}
        params = {"dateFrom": date_str, "flag": 1}
        label_text = f" ({label})" if label else ""
        log(f"Загружаю заказы за дату{label_text}: {date_str}, flag=1")
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

    def fetch_today_orders(self) -> pd.DataFrame:
        """Получает все заказы сегодняшнего дня. date сохраняется с временем."""
        return self.fetch_orders_for_date(now_msk().date(), label="сегодня")

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

    def fetch_public_site_prices(self, nmids: Sequence[int]) -> pd.DataFrame:
        price_source = normalize_text(self.cfg.price_source or DEFAULT_PRICE_SOURCE)
        if price_source in {"orders-spp", "orders", "spp"}:
            log("Публичные цены сайта WB не загружаю: price_source=orders-spp")
            return pd.DataFrame()
        ids = [x for x in nmids if to_int_or_none(x)]
        if not ids:
            return pd.DataFrame()
        log(f"Загружаю цены карточек WB с сайта: товаров={len(set(ids))}, dest={self.cfg.public_dest}")
        df = fetch_public_site_prices_for_nmids(
            ids,
            dest=str(self.cfg.public_dest),
            session=self.session,
            chunk_size=DEFAULT_PUBLIC_PRICE_CHUNK_SIZE,
            timeout=30,
        )
        ok_count = int(df["site_final_price"].notna().sum()) if not df.empty and "site_final_price" in df.columns else 0
        log(f"Цены сайта WB: найдено={ok_count}/{len(set(ids))}")
        return df

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

    def save_orders_snapshot(self, orders_df: pd.DataFrame, key: str, sheet_name: str):
        self.s3.write_excel(key, orders_df, sheet_name=sheet_name)

    def save_today_orders_snapshot(self, orders_df: pd.DataFrame):
        today_d = self.run_datetime_msk.date()
        key = f"{self.service_prefix}/Заказы_сегодня.xlsx"
        self.save_orders_snapshot(orders_df, key, "Заказы_сегодня")

        archive_key = f"{self.service_prefix}/Архив/Заказы_{today_d.strftime('%Y-%m-%d')}.xlsx"
        self.save_orders_snapshot(orders_df, archive_key, "Заказы")

        if self.cfg.update_weekly_orders:
            self._upsert_today_into_weekly_orders(orders_df)
        else:
            log("Штатный недельный файл заказов не трогаю: update_weekly_orders=False")

    def load_previous_day_orders(self) -> pd.DataFrame:
        """Загружает предыдущий день для fallback SPP: служебный архив -> файл предыдущего дня -> недельный файл -> API."""
        prev_d = self.run_datetime_msk.date() - timedelta(days=1)
        archive_key = f"{self.service_prefix}/Архив/Заказы_{prev_d.strftime('%Y-%m-%d')}.xlsx"
        prev_key = f"{self.service_prefix}/Заказы_предыдущий_день.xlsx"

        df = pd.DataFrame()
        if self.s3.file_exists(archive_key):
            log(f"Читаю заказы предыдущего дня из архива: {archive_key}")
            df = self.s3.read_excel(archive_key, sheet_name=0)
        elif self.s3.file_exists(prev_key):
            log(f"Читаю заказы предыдущего дня из служебного файла: {prev_key}")
            df = self.s3.read_excel(prev_key, sheet_name=0)

        if df.empty:
            weekly_key = get_weekly_orders_key(self.cfg.store, datetime.combine(prev_d, datetime.min.time()))
            if self.s3.file_exists(weekly_key):
                log(f"Ищу предыдущий день в недельном файле заказов: {weekly_key}")
                wk = self.s3.read_excel(weekly_key, sheet_name=0)
                if not wk.empty and "date" in wk.columns:
                    wk["date"] = pd.to_datetime(wk["date"], errors="coerce")
                    df = wk[wk["date"].dt.date == prev_d].copy()

        if df.empty:
            try:
                # API fallback нужен на случай, когда обычный ежедневный сборщик ещё не успел закрыть вчерашний день.
                df = self.fetch_orders_for_date(prev_d, label="предыдущий день")
            except Exception as e:
                log(f"Не удалось получить предыдущий день через API: {e}", "WARN")
                df = pd.DataFrame()

        if not df.empty:
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"], errors="coerce")
            if "lastChangeDate" in df.columns:
                df["lastChangeDate"] = pd.to_datetime(df["lastChangeDate"], errors="coerce")
            self.save_orders_snapshot(df, prev_key, "Заказы_предыдущий_день")
            self.save_orders_snapshot(df, archive_key, "Заказы")
            log(f"Заказы предыдущего дня: дата={prev_d}, строк={len(df)}")
        else:
            log(f"Заказы предыдущего дня не найдены: дата={prev_d}", "WARN")
        return df

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

    def load_recent_orders_history(self, today_orders: pd.DataFrame, previous_day_orders: Optional[pd.DataFrame] = None) -> pd.DataFrame:
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
        if previous_day_orders is not None and not previous_day_orders.empty:
            frames.append(previous_day_orders)
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

    def build_spp_table(self, today_orders: pd.DataFrame, previous_day_orders: pd.DataFrame, orders_history: pd.DataFrame) -> pd.DataFrame:
        """
        Возвращает avg_spp по nmID с правилом минимальной выборки.

        Важно: для расчёта используется не только поле spp. Если в заказах есть
        finishedPrice и priceWithDisc, сначала считаем фактическую WB-скидку:
            effective_discount = 1 - finishedPrice / priceWithDisc
        Это надёжнее, чем blindly верить полю spp, потому что нам важен именно
        итоговый коэффициент между ценой до WB-скидки и ценой покупателя.

        Правило:
        1) Последние 3 часа используем только если по nmID >= min_spp_sample_orders заказов.
        2) Если за 3 часа заказов меньше минимума, берём средний коэффициент по этому же nmID за предыдущий день.
        3) Если предыдущего дня недостаточно, берём SKU-историю за прошлый период только при выборке >= min_spp_sample_orders.
        4) Subject/global fallback не применяется.
        5) Если задан safe_wb_discount_floor_pct, коэффициент не может быть ниже floor — это защита от занижения цен.
        """
        min_n = max(1, int(self.cfg.min_spp_sample_orders or 1))

        frames = []
        if not today_orders.empty:
            t = today_orders.copy()
            t["_spp_frame"] = "today"
            frames.append(t)
        if previous_day_orders is not None and not previous_day_orders.empty:
            p = previous_day_orders.copy()
            p["_spp_frame"] = "previous_day"
            frames.append(p)
        if not orders_history.empty:
            h = orders_history.copy()
            h["_spp_frame"] = h.get("_spp_frame", "history")
            frames.append(h)

        if not frames:
            return pd.DataFrame(columns=[
                "nmID", "avg_spp", "spp_source", "spp_sample_min",
                "orders_3h", "orders_today", "orders_previous_day", "orders_history"
            ])

        all_rows = pd.concat(frames, ignore_index=True, sort=False)

        if "nmID" not in all_rows.columns:
            if "nmId" in all_rows.columns:
                all_rows["nmID"] = all_rows["nmId"]
            elif "Артикул WB" in all_rows.columns:
                all_rows["nmID"] = all_rows["Артикул WB"]
        all_rows["nmID"] = all_rows["nmID"].map(to_int_or_none) if "nmID" in all_rows.columns else None
        all_rows = all_rows[all_rows["nmID"].notna()].copy()
        all_rows["nmID"] = all_rows["nmID"].astype(int)

        # Основной показатель для расчёта — фактическая WB-скидка между priceWithDisc
        # и finishedPrice. Поле spp используем только как запасной источник.
        if "spp" not in all_rows.columns:
            all_rows["spp"] = None
        all_rows["spp_api_num"] = all_rows["spp"].map(to_float_or_none)

        if "finishedPrice" in all_rows.columns and "priceWithDisc" in all_rows.columns:
            all_rows["finished_num"] = all_rows["finishedPrice"].map(to_float_or_none)
            all_rows["price_with_disc_num"] = all_rows["priceWithDisc"].map(to_float_or_none)
            valid_coef = (
                all_rows["finished_num"].notna()
                & all_rows["price_with_disc_num"].notna()
                & (all_rows["finished_num"] > 0)
                & (all_rows["price_with_disc_num"] > 0)
                # допускаем небольшие расхождения/округления, но не абсурдные строки
                & (all_rows["finished_num"] <= all_rows["price_with_disc_num"] * 1.05)
            )
            all_rows["spp_effective_num"] = None
            all_rows.loc[valid_coef, "spp_effective_num"] = (
                (1 - all_rows.loc[valid_coef, "finished_num"] / all_rows.loc[valid_coef, "price_with_disc_num"]) * 100
            )
        else:
            all_rows["spp_effective_num"] = None

        all_rows["spp_num"] = all_rows["spp_effective_num"].map(to_float_or_none)
        missing_eff = all_rows["spp_num"].isna()
        all_rows.loc[missing_eff, "spp_num"] = all_rows.loc[missing_eff, "spp_api_num"].map(to_float_or_none)

        # Убираем явный мусор. Верхнюю границу 95 оставляем, чтобы не делить почти на ноль.
        all_rows = all_rows[all_rows["spp_num"].notna()].copy()
        all_rows = all_rows[(all_rows["spp_num"] >= 0) & (all_rows["spp_num"] < 95)].copy()

        floor = float(self.cfg.safe_wb_discount_floor_pct or 0)
        if floor > 0:
            floor = min(max(floor, 0.0), 94.0)
            below_floor = all_rows["spp_num"] < floor
            if below_floor.any():
                all_rows.loc[below_floor, "spp_num_raw_before_floor"] = all_rows.loc[below_floor, "spp_num"]
                all_rows.loc[below_floor, "spp_num"] = floor

        if "date" in all_rows.columns:
            all_rows["date"] = pd.to_datetime(all_rows["date"], errors="coerce")
        else:
            all_rows["date"] = pd.NaT

        if "isCancel" in all_rows.columns:
            all_rows = all_rows[~is_cancelled_series(all_rows["isCancel"])].copy()

        if all_rows.empty:
            return pd.DataFrame(columns=[
                "nmID", "avg_spp", "spp_source", "spp_sample_min",
                "orders_3h", "orders_today", "orders_previous_day", "orders_history"
            ])

        current_naive = self.run_datetime_msk.replace(tzinfo=None)
        three_hours_ago = current_naive - timedelta(hours=3)
        today_d = self.run_datetime_msk.date()
        prev_d = today_d - timedelta(days=1)

        by3 = all_rows[(all_rows["date"].notna()) & (all_rows["date"] >= three_hours_ago) & (all_rows["date"] <= current_naive)].copy()
        byt = all_rows[(all_rows["date"].notna()) & (all_rows["date"].dt.date == today_d)].copy()
        prev = all_rows[(all_rows["date"].notna()) & (all_rows["date"].dt.date == prev_d)].copy()
        hist_prev_period = all_rows[(all_rows["date"].isna()) | ((all_rows["date"].notna()) & (all_rows["date"].dt.date < today_d))].copy()

        # Группа оттенков: 501/5, 501/19 -> 501; PT501R.005K -> 501.
        article_col = first_existing_col(all_rows, ["supplierArticle", "Артикул продавца", "vendorCode", "Артикул", "article", "article_1c", "Артикул 1С"])
        if article_col:
            all_rows["spp_shade_group"] = all_rows[article_col].map(extract_shade_group)
            for _frame in (by3, byt, prev, hist_prev_period):
                if article_col in _frame.columns:
                    _frame["spp_shade_group"] = _frame[article_col].map(extract_shade_group)
                else:
                    _frame["spp_shade_group"] = ""
        else:
            all_rows["spp_shade_group"] = ""
            for _frame in (by3, byt, prev, hist_prev_period):
                _frame["spp_shade_group"] = ""

        sku_3h = self._agg_spp(by3, "spp_3h")
        sku_today = self._agg_spp(byt, "spp_today")
        sku_prev = self._agg_spp(prev, "spp_previous_day")
        sku_hist = self._agg_spp(hist_prev_period, "spp_history")

        group_3h = self._agg_spp_group(by3, "group_3h", min_n)
        group_today = self._agg_spp_group(byt, "group_today", min_n)
        group_prev = self._agg_spp_group(prev, "group_previous_day", min_n)
        group_hist = self._agg_spp_group(hist_prev_period, "group_history", min_n)

        nm_group = (
            all_rows[["nmID", "spp_shade_group"]]
            .dropna()
            .drop_duplicates(subset=["nmID"], keep="last")
        )

        base_ids = sorted(all_rows["nmID"].dropna().astype(int).unique())
        base = pd.DataFrame({"nmID": base_ids})
        out = (
            base
            .merge(nm_group, on="nmID", how="left")
            .merge(sku_3h, on="nmID", how="left")
            .merge(sku_today, on="nmID", how="left")
            .merge(sku_prev, on="nmID", how="left")
            .merge(sku_hist, on="nmID", how="left")
            .merge(group_3h, on="spp_shade_group", how="left")
            .merge(group_today, on="spp_shade_group", how="left")
            .merge(group_prev, on="spp_shade_group", how="left")
            .merge(group_hist, on="spp_shade_group", how="left")
        )

        def safe_int(value: Any) -> int:
            try:
                if value is None or pd.isna(value):
                    return 0
                return int(float(value))
            except Exception:
                return 0

        def choose(row):
            # Сначала общий SPP группы оттенков. Это фиксирует одинаковую цену для оттенков
            # вроде 501/5, 501/19 при одинаковом РРЦ, если разбег между оттенками <= 3 п.п.
            ng3 = safe_int(row.get("orders_group_3h"))
            ngt = safe_int(row.get("orders_group_today"))
            ngprev = safe_int(row.get("orders_group_previous_day"))
            nghist = safe_int(row.get("orders_group_history"))

            if pd.notna(row.get("avg_spp_group_3h")) and ng3 >= min_n:
                return row.get("avg_spp_group_3h"), "shade_group_3h_raw_max", "group_3h"
            if pd.notna(row.get("avg_spp_group_today")) and ngt >= min_n:
                return row.get("avg_spp_group_today"), "shade_group_today_raw_max", "group_today"
            if pd.notna(row.get("avg_spp_group_previous_day")) and ngprev >= min_n:
                return row.get("avg_spp_group_previous_day"), "shade_group_previous_day_raw_max", "group_previous_day"
            if pd.notna(row.get("avg_spp_group_history")) and nghist >= min_n:
                return row.get("avg_spp_group_history"), "shade_group_history_raw_max", "group_history"

            n3 = safe_int(row.get("orders_spp_3h"))
            nt = safe_int(row.get("orders_spp_today"))
            n_prev = safe_int(row.get("orders_spp_previous_day"))
            n_hist = safe_int(row.get("orders_spp_history"))

            if pd.notna(row.get("avg_spp_spp_3h")) and n3 >= min_n:
                return row.get("avg_spp_spp_3h"), "sku_3h_raw_max", "3h"
            if pd.notna(row.get("avg_spp_spp_today")) and nt >= min_n:
                return row.get("avg_spp_spp_today"), "sku_today_raw_max", "today"
            if pd.notna(row.get("avg_spp_spp_previous_day")) and n_prev >= min_n:
                return row.get("avg_spp_spp_previous_day"), "sku_previous_day_raw_max", "previous_day"
            if pd.notna(row.get("avg_spp_spp_history")) and n_hist >= min_n:
                return row.get("avg_spp_spp_history"), "sku_history_raw_max", "history"
            return None, "no_spp_min10", "none"

        chosen = out.apply(choose, axis=1, result_type="expand")
        out["avg_spp"] = chosen[0]
        out["spp_source"] = chosen[1]
        out["spp_reference_period"] = chosen[2]
        out["spp_sample_min"] = min_n
        for c, src in [
            ("orders_3h", "orders_spp_3h"),
            ("orders_today", "orders_spp_today"),
            ("orders_previous_day", "orders_spp_previous_day"),
            ("orders_history", "orders_spp_history"),
            ("orders_group_3h", "orders_group_3h"),
            ("orders_group_today", "orders_group_today"),
            ("orders_group_previous_day", "orders_group_previous_day"),
            ("orders_group_history", "orders_group_history"),
        ]:
            if src in out.columns:
                out[c] = out[src].fillna(0).astype(int)
            else:
                out[c] = 0

        cols = [
            "nmID", "avg_spp", "spp_source", "spp_reference_period", "spp_sample_min",
            "spp_shade_group", "orders_3h", "orders_today", "orders_previous_day", "orders_history",
            "orders_group_3h", "orders_group_today", "orders_group_previous_day", "orders_group_history",
        ]
        for c in ["group_mean_spread_group_3h", "group_mean_spread_group_today", "group_mean_spread_group_previous_day", "group_mean_spread_group_history"]:
            if c in out.columns:
                cols.append(c)
        return out[[c for c in cols if c in out.columns]]

    @staticmethod
    def _agg_spp(df: pd.DataFrame, suffix: str) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=["nmID", f"avg_spp_{suffix}", f"orders_{suffix}", f"spp_raw_max_{suffix}"])
        g = df.groupby("nmID", as_index=False).agg(
            **{
                # Базовое значение SPP — максимум по фактическому SPP из строк заказов, округлённый вверх.
                f"avg_spp_{suffix}": ("spp_num", lambda x: int(math.ceil(float(x.max())))),
                f"spp_raw_max_{suffix}": ("spp_num", "max"),
                f"spp_raw_min_{suffix}": ("spp_num", "min"),
                f"orders_{suffix}": ("spp_num", "size"),
            }
        )
        return g

    @staticmethod
    def _agg_spp_group(df: pd.DataFrame, suffix: str, min_n: int) -> pd.DataFrame:
        if df.empty or "spp_shade_group" not in df.columns:
            return pd.DataFrame(columns=["spp_shade_group", f"avg_spp_{suffix}", f"orders_{suffix}"])
        tmp = df[df["spp_shade_group"].astype(str).str.strip() != ""].copy()
        if tmp.empty:
            return pd.DataFrame(columns=["spp_shade_group", f"avg_spp_{suffix}", f"orders_{suffix}"])

        shade = tmp.groupby(["spp_shade_group", "nmID"], as_index=False).agg(
            shade_mean=("spp_num", "mean"),
            shade_orders=("spp_num", "size"),
        )
        eligible = shade[shade["shade_orders"] >= min_n].copy()

        rows = []
        for group, gdf in tmp.groupby("spp_shade_group"):
            total_orders = int(len(gdf))
            elig = eligible[eligible["spp_shade_group"] == group]
            if total_orders < min_n or elig.empty:
                continue
            mean_min = float(elig["shade_mean"].min())
            mean_max = float(elig["shade_mean"].max())
            mean_spread = mean_max - mean_min
            # Склеиваем оттенки только если средний SPP по оттенкам расходится не больше чем на 3 п.п.
            if mean_spread > 3.0:
                continue
            rows.append({
                "spp_shade_group": group,
                f"avg_spp_{suffix}": int(math.ceil(float(gdf["spp_num"].max()))),
                f"orders_{suffix}": total_orders,
                f"group_mean_min_{suffix}": mean_min,
                f"group_mean_max_{suffix}": mean_max,
                f"group_mean_spread_{suffix}": mean_spread,
                f"group_shades_count_{suffix}": int(elig["nmID"].nunique()),
            })
        return pd.DataFrame(rows)

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

    def build_calculation(self, today_orders: pd.DataFrame, previous_day_orders: pd.DataFrame, goods_df: pd.DataFrame, site_prices_df: pd.DataFrame, rrc_df: pd.DataFrame, ref_df: pd.DataFrame, orders_history: pd.DataFrame) -> pd.DataFrame:
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

        # Фактическая цена карточки WB с сайта по nmID. Это контрольная цена покупателя
        # для выбранного dest; если она доступна, используем её как более надёжную обратную связь,
        # чем SPP по 1-2 заказам.
        if site_prices_df is not None and not site_prices_df.empty:
            calc = calc.merge(site_prices_df, on="nmID", how="left")
        else:
            for col in [
                "site_final_price", "site_product_price", "site_basic_price", "site_name",
                "site_brand", "site_price_raw", "site_price_source", "site_dest", "site_checked_at"
            ]:
                calc[col] = None

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

        # WB-скидка/SPP. Используем только SKU-уровень: 3h при выборке >= min_n, иначе предыдущий день/история.
        spp = self.build_spp_table(today_orders, previous_day_orders, orders_history)
        calc = calc.merge(spp, on="nmID", how="left")

        # Калибровка SPP по фактической проверке сайта: добавляем +1.5 п.п.
        # к найденному значению SPP. Применяется одинаково к SKU-SPP и к групповому
        # SPP оттенков, поэтому 501-е и аналогичные группы остаются синхронизированы.
        spp_adj = float(self.cfg.spp_adjustment_pct or 0)
        if abs(spp_adj) > 1e-9 and "avg_spp" in calc.columns:
            spp_mask = calc["avg_spp"].map(to_float_or_none).notna()
            if spp_mask.any():
                calc.loc[spp_mask, "avg_spp_before_adjustment"] = calc.loc[spp_mask, "avg_spp"]
                calc.loc[spp_mask, "spp_adjustment_pct"] = spp_adj
                calc.loc[spp_mask, "avg_spp"] = calc.loc[spp_mask, "avg_spp"].map(
                    lambda v: min(max(float(v) + spp_adj, 0.0), 94.0) if to_float_or_none(v) is not None else v
                )
                if "spp_source" not in calc.columns:
                    calc["spp_source"] = ""
                calc.loc[spp_mask, "spp_source"] = calc.loc[spp_mask, "spp_source"].astype(str) + f"+adj_{spp_adj:g}pp"

        # Экстренный защитный режим: если по товару вообще нет достаточной выборки,
        # можно использовать консервативный floor, чтобы цены не оказались ниже цели.
        safe_floor = float(self.cfg.safe_wb_discount_floor_pct or 0)
        if safe_floor > 0:
            safe_floor = min(max(safe_floor, 0.0), 94.0)
            if "avg_spp" not in calc.columns:
                calc["avg_spp"] = None
            if "spp_source" not in calc.columns:
                calc["spp_source"] = ""
            has_spp = calc["avg_spp"].map(to_float_or_none).notna()
            low_spp = has_spp & (calc["avg_spp"].map(to_float_or_none) < safe_floor)
            if low_spp.any():
                calc.loc[low_spp, "avg_spp_raw_before_floor"] = calc.loc[low_spp, "avg_spp"]
                calc.loc[low_spp, "avg_spp"] = safe_floor
                calc.loc[low_spp, "spp_source"] = calc.loc[low_spp, "spp_source"].astype(str) + f"+safe_floor_{safe_floor:g}"
            if self.cfg.use_safe_floor_for_missing:
                missing_spp = calc["avg_spp"].map(to_float_or_none).isna()
                if missing_spp.any():
                    calc.loc[missing_spp, "avg_spp"] = safe_floor
                    calc.loc[missing_spp, "spp_source"] = f"safe_floor_missing_{safe_floor:g}"
                    calc.loc[missing_spp, "spp_reference_period"] = "safe_floor"
                    calc.loc[missing_spp, "spp_sample_min"] = int(self.cfg.min_spp_sample_orders or 0)
                    for c in ["orders_3h", "orders_today", "orders_previous_day", "orders_history"]:
                        if c not in calc.columns:
                            calc[c] = 0

        # Расчёт цен.
        target_factor = self.active_target_factor
        calc["pricing_period"] = self.active_pricing_period
        calc["target_factor"] = target_factor
        calc["target_finishedPrice"] = calc["rrc"].apply(lambda x: int(round(float(x) * target_factor)) if pd.notna(x) else None)
        calc["new_discount"] = int(self.cfg.seller_discount)

        calc["current_wb_price"] = calc["current_wb_price"].map(to_float_or_none) if "current_wb_price" in calc.columns else None
        calc["current_wb_discount"] = calc["current_wb_discount"].map(to_float_or_none) if "current_wb_discount" in calc.columns else None
        calc["site_final_price"] = calc["site_final_price"].map(to_float_or_none) if "site_final_price" in calc.columns else None
        calc["old_priceWithDisc_calc"] = calc.apply(self._calc_old_price_with_disc, axis=1)

        # Вариант 1: старый расчёт через SPP из заказов, но только при достаточной выборке.
        calc["target_priceWithDisc_orders_spp"] = calc.apply(self._calc_target_price_with_disc, axis=1)
        calc["new_price_orders_spp"] = calc.apply(lambda r: self._calc_new_price_from_price_with_disc(r, "target_priceWithDisc_orders_spp"), axis=1)

        # Вариант 2: расчёт от фактической цены на сайте WB.
        calc["site_effective_spp"] = calc.apply(self._calc_site_effective_spp, axis=1)
        calc["target_priceWithDisc_site"] = calc.apply(self._calc_target_price_with_disc_from_site, axis=1)
        calc["new_price_site"] = calc.apply(lambda r: self._calc_new_price_from_price_with_disc(r, "target_priceWithDisc_site"), axis=1)
        calc["site_price_delta_to_target"] = calc.apply(
            lambda r: round(float(r["site_final_price"]) - float(r["target_finishedPrice"]), 2)
            if pd.notna(r.get("site_final_price")) and pd.notna(r.get("target_finishedPrice")) else None,
            axis=1,
        )

        price_source = normalize_text(self.cfg.price_source or DEFAULT_PRICE_SOURCE)
        site_allowed = price_source in {"hybrid", "site", "site-price", "public-site"}
        orders_allowed = price_source in {"hybrid", "orders-spp", "orders", "spp"}
        use_site = (
            site_allowed
            & calc["new_price_site"].notna()
            & calc["site_final_price"].notna()
            & (calc["site_final_price"] > 0)
        )
        use_orders = (~use_site) & orders_allowed & calc["new_price_orders_spp"].notna()

        calc["price_calc_source"] = "no_price_source"
        calc.loc[use_site, "price_calc_source"] = "public_site_price"
        calc.loc[use_orders, "price_calc_source"] = "orders_spp_min_sample"
        calc["target_priceWithDisc"] = None
        calc["new_price"] = None
        calc.loc[use_site, "target_priceWithDisc"] = calc.loc[use_site, "target_priceWithDisc_site"]
        calc.loc[use_site, "new_price"] = calc.loc[use_site, "new_price_site"]
        calc.loc[use_orders, "target_priceWithDisc"] = calc.loc[use_orders, "target_priceWithDisc_orders_spp"]
        calc.loc[use_orders, "new_price"] = calc.loc[use_orders, "new_price_orders_spp"]

        calc["delta_price"] = calc.apply(lambda r: (r["new_price"] - r["current_wb_price"]) if pd.notna(r.get("new_price")) and pd.notna(r.get("current_wb_price")) else None, axis=1)
        calc["delta_price_pct"] = calc.apply(self._calc_delta_pct, axis=1)

        calc["decision"] = ""
        calc["reason"] = ""
        calc = self.apply_decision_rules(calc)

        # Удобный порядок колонок.
        columns_order = [
            "decision", "reason", "nmID", "supplierArticle_api", "supplierArticle_ref", "article_1c", "article_rrc",
            "subject", "subject_source", "rrc", "pricing_period", "target_factor", "target_finishedPrice",
            "price_calc_source", "site_final_price", "site_price_delta_to_target", "site_effective_spp",
            "avg_spp", "avg_spp_before_adjustment", "spp_adjustment_pct", "avg_spp_raw_before_floor", "spp_source",
            "target_priceWithDisc", "new_price", "new_discount", "current_wb_price", "current_wb_discount",
            "current_wb_discounted_price", "old_priceWithDisc_calc",
            "target_priceWithDisc_site", "new_price_site", "target_priceWithDisc_orders_spp", "new_price_orders_spp",
            "delta_price", "delta_price_pct",
            "orders_3h", "orders_today", "orders_previous_day", "orders_history",
            "orders_group_3h", "orders_group_today", "orders_group_previous_day", "orders_group_history",
            "spp_shade_group", "spp_reference_period", "spp_sample_min",
            "site_price_source", "site_dest", "site_checked_at", "site_name",
            "name_api", "rrc_name", "excluded_by_rrc_name",
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

    def _calc_new_price_from_price_with_disc(self, row: pd.Series, price_with_disc_col: str = "target_priceWithDisc") -> Optional[int]:
        price_with_disc = to_float_or_none(row.get(price_with_disc_col))
        discount = to_float_or_none(row.get("new_discount"))
        if price_with_disc is None or discount is None or discount >= 95 or discount < 0:
            return None
        return int(round(price_with_disc / (1 - discount / 100)))

    def _calc_site_effective_spp(self, row: pd.Series) -> Optional[float]:
        site_final = to_float_or_none(row.get("site_final_price"))
        old_pwd = to_float_or_none(row.get("old_priceWithDisc_calc"))
        if site_final is None or old_pwd is None or old_pwd <= 0 or site_final <= 0:
            return None
        spp = (1 - site_final / old_pwd) * 100
        if spp < -20 or spp >= 95:
            return None
        return round(float(spp), 2)

    def _calc_target_price_with_disc_from_site(self, row: pd.Series) -> Optional[int]:
        target = to_float_or_none(row.get("target_finishedPrice"))
        site_final = to_float_or_none(row.get("site_final_price"))
        old_pwd = to_float_or_none(row.get("old_priceWithDisc_calc"))
        if target is None or site_final is None or old_pwd is None or site_final <= 0 or old_pwd <= 0:
            return None
        # Сохраняем текущий фактический коэффициент WB/SPP из сайта: site_final / old_priceWithDisc.
        return int(round(target * old_pwd / site_final))

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
            price_calc_source = str(r.get("price_calc_source") or "")
            if price_calc_source != "public_site_price" and (spp is None or spp < 0 or spp >= 95):
                decision = "skip"
                reason_parts.append(f"нет корректного SPP с выборкой >= {self.cfg.min_spp_sample_orders}")
            if price_calc_source == "no_price_source":
                decision = "skip"
                reason_parts.append("нет цены сайта и нет SPP с достаточной выборкой")
            if new_price is None or new_price <= 0 or new_discount is None:
                decision = "skip"
                reason_parts.append("не рассчитана новая цена")

            # Если фактическая цена сайта уже близка к целевой и скидка продавца уже 26%, не отправляем.
            if decision == "send" and price_calc_source == "public_site_price" and current_discount is not None:
                site_final = to_float_or_none(r.get("site_final_price"))
                target_finished = to_float_or_none(r.get("target_finishedPrice"))
                same_site_price = (
                    site_final is not None and target_finished is not None
                    and abs(site_final - target_finished) <= self.cfg.site_price_tolerance_rub
                )
                same_discount = int(round(float(current_discount))) == int(self.cfg.seller_discount)
                if same_site_price and same_discount:
                    decision = "skip"
                    reason_parts.append("цена сайта уже близка к целевой")

            # Если базовая WB price уже корректная и скидка уже 26%, не отправляем.
            if decision == "send" and current_price is not None and current_discount is not None:
                same_price = abs(float(new_price) - float(current_price)) <= self.cfg.price_tolerance_rub
                same_discount = int(round(float(current_discount))) == int(self.cfg.seller_discount)
                if same_price and same_discount:
                    decision = "skip"
                    reason_parts.append("базовая цена уже корректная")

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
                factor = to_float_or_none(r.get("target_factor")) or self.active_target_factor
                period = str(r.get("pricing_period") or self.active_pricing_period)
                src = str(r.get("price_calc_source") or "")
                if src == "public_site_price":
                    site_final = to_float_or_none(r.get("site_final_price"))
                    target_finished = to_float_or_none(r.get("target_finishedPrice"))
                    eff_spp = to_float_or_none(r.get("site_effective_spp"))
                    reason_parts.append(
                        f"РРЦ*{factor:.2f}, режим={period}, расчёт от цены сайта={site_final}, "
                        f"цель={target_finished}, effective_spp={eff_spp}, discount={self.cfg.seller_discount}%"
                    )
                else:
                    reason_parts.append(f"РРЦ*{factor:.2f}, режим={period}, discount={self.cfg.seller_discount}%, SPP={spp:.2f}%")

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
        log(
            f"Старт корректировки цен: store={self.cfg.store}, apply={apply}, "
            f"target_factor={self.active_target_factor:.2f}, режим={self.active_pricing_period}, "
            f"min_spp_sample_orders={self.cfg.min_spp_sample_orders}, "
            f"price_source={self.cfg.price_source}, site_dest={self.cfg.public_dest}, "
            f"safe_wb_discount_floor={self.cfg.safe_wb_discount_floor_pct:g}, "
            f"spp_adjustment_pct={self.cfg.spp_adjustment_pct:g}, "
            f"use_safe_floor_for_missing={self.cfg.use_safe_floor_for_missing}, "
            f"время МСК={self.run_datetime_msk.strftime('%Y-%m-%d %H:%M:%S')}"
        )

        today_orders = self.fetch_today_orders()
        log(f"Заказы сегодня: строк={len(today_orders)}")
        self.save_today_orders_snapshot(today_orders)

        previous_day_orders = self.load_previous_day_orders()

        rrc_df = self.load_rrc()
        ref_df = self.load_article_reference()
        goods_df = self.fetch_current_goods_prices()
        nmids_for_site = goods_df["nmID"].dropna().astype(int).tolist() if not goods_df.empty and "nmID" in goods_df.columns else ref_df["nmID"].dropna().astype(int).tolist()
        site_prices_df = self.fetch_public_site_prices(nmids_for_site)
        orders_history = self.load_recent_orders_history(today_orders, previous_day_orders)

        calc = self.build_calculation(today_orders, previous_day_orders, goods_df, site_prices_df, rrc_df, ref_df, orders_history)
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
    parser = argparse.ArgumentParser(description="WB price corrector: day РРЦ*0.9, night 23:00-04:59 МСК РРЦ*0.8")
    sub = parser.add_subparsers(dest="command")

    run_parser = sub.add_parser("run", help="Сделать расчёт и при --apply отправить цены в WB")
    run_parser.add_argument("--store", default=DEFAULT_STORE, help="Магазин, по умолчанию TOPFACE")
    mode = run_parser.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true", help="Отправить изменения цен в WB")
    mode.add_argument("--dry-run", action="store_true", help="Только расчёт без отправки")
    run_parser.add_argument("--target-factor", type=float, default=None, help="Фиксированный коэффициент к РРЦ. Если не задан, работает график: 23:00-04:59 МСК = 0.8, остальное время = 0.9")
    run_parser.add_argument("--day-target-factor", type=float, default=DEFAULT_DAY_TARGET_FACTOR, help="Дневной коэффициент к РРЦ, по умолчанию 0.9")
    run_parser.add_argument("--night-target-factor", type=float, default=DEFAULT_NIGHT_TARGET_FACTOR, help="Ночной коэффициент к РРЦ, по умолчанию 0.8")
    run_parser.add_argument("--night-start-hour", type=int, default=DEFAULT_NIGHT_START_HOUR, help="Час начала ночного режима по Москве, по умолчанию 23")
    run_parser.add_argument("--night-end-hour", type=int, default=DEFAULT_NIGHT_END_HOUR, help="Час окончания ночного режима по Москве, по умолчанию 5; в 05:00 уже дневной коэффициент")
    run_parser.add_argument("--seller-discount", type=int, default=DEFAULT_SELLER_DISCOUNT, help="Скидка продавца, %, по умолчанию 26")
    run_parser.add_argument("--price-tolerance-rub", type=int, default=DEFAULT_PRICE_TOLERANCE_RUB, help="Не отправлять, если отличие price <= N рублей")
    run_parser.add_argument("--max-price-change-pct", type=float, default=DEFAULT_MAX_PRICE_CHANGE_PCT, help="Макс. изменение price за запуск, 0 = отключить")
    run_parser.add_argument("--fallback-days", type=int, default=DEFAULT_FALLBACK_DAYS, help="Сколько последних дней заказов читать для fallback SPP")
    run_parser.add_argument("--min-spp-sample-orders", type=int, default=DEFAULT_MIN_SPP_SAMPLE_ORDERS, help="Минимум заказов для использования SPP из периода")
    run_parser.add_argument("--price-source", choices=["hybrid", "site", "orders-spp"], default=DEFAULT_PRICE_SOURCE, help="Источник расчёта: hybrid=сначала цена сайта, потом SPP; site=только цена сайта; orders-spp=только SPP из заказов")
    run_parser.add_argument("--public-dest", default=DEFAULT_PUBLIC_DEST, help="dest для публичной цены WB, по умолчанию WB_PUBLIC_DEST или -1257786")
    run_parser.add_argument("--site-price-tolerance-rub", type=int, default=DEFAULT_SITE_PRICE_TOLERANCE_RUB, help="Допуск по фактической цене сайта до целевой, рублей")
    run_parser.add_argument("--safe-wb-discount-floor-pct", type=float, default=DEFAULT_SAFE_WB_DISCOUNT_FLOOR_PCT, help="Консервативный floor WB-скидки/SPP для расчёта price. 0 = отключить")
    run_parser.add_argument("--spp-adjustment-pct", type=float, default=DEFAULT_SPP_ADJUSTMENT_PCT, help="Корректировка к рассчитанному SPP, п.п. По умолчанию +1.5")
    run_parser.add_argument("--use-safe-floor-for-missing", action="store_true", default=DEFAULT_USE_SAFE_FLOOR_FOR_MISSING, help="Если по товару нет достаточной выборки, использовать safe-wb-discount-floor-pct вместо пропуска")
    run_parser.add_argument("--allow-unknown-subject", action="store_true", help="Разрешить менять товары без известного subject")
    run_parser.add_argument("--update-weekly-orders", action="store_true", help="Опционально обновлять сегодняшний день в обычном недельном файле заказов")

    check_parser = sub.add_parser("check-price", help="Проверить фактическую цену карточки WB по nmID через публичный endpoint сайта")
    check_parser.add_argument("--nmID", "--nmid", dest="nmids", action="append", required=True, help="Артикул WB/nmID. Можно указать несколько раз или через запятую")
    check_parser.add_argument("--public-dest", default=DEFAULT_PUBLIC_DEST, help="dest для публичной цены WB")

    # Удобство: если запустили без команды — ведём как run --dry-run.
    args = parser.parse_args(argv)
    if args.command is None:
        args.command = "run"
        args.store = DEFAULT_STORE
        args.apply = False
        args.dry_run = True
        args.target_factor = None
        args.day_target_factor = DEFAULT_DAY_TARGET_FACTOR
        args.night_target_factor = DEFAULT_NIGHT_TARGET_FACTOR
        args.night_start_hour = DEFAULT_NIGHT_START_HOUR
        args.night_end_hour = DEFAULT_NIGHT_END_HOUR
        args.seller_discount = DEFAULT_SELLER_DISCOUNT
        args.price_tolerance_rub = DEFAULT_PRICE_TOLERANCE_RUB
        args.max_price_change_pct = DEFAULT_MAX_PRICE_CHANGE_PCT
        args.fallback_days = DEFAULT_FALLBACK_DAYS
        args.min_spp_sample_orders = DEFAULT_MIN_SPP_SAMPLE_ORDERS
        args.price_source = DEFAULT_PRICE_SOURCE
        args.public_dest = DEFAULT_PUBLIC_DEST
        args.site_price_tolerance_rub = DEFAULT_SITE_PRICE_TOLERANCE_RUB
        args.safe_wb_discount_floor_pct = DEFAULT_SAFE_WB_DISCOUNT_FLOOR_PCT
        args.spp_adjustment_pct = DEFAULT_SPP_ADJUSTMENT_PCT
        args.use_safe_floor_for_missing = DEFAULT_USE_SAFE_FLOOR_FOR_MISSING
        args.allow_unknown_subject = False
        args.update_weekly_orders = False
    return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    if args.command == "check-price":
        raw_ids: List[int] = []
        for part in args.nmids:
            for token in str(part).split(","):
                nm = to_int_or_none(token.strip())
                if nm:
                    raw_ids.append(nm)
        df = fetch_public_site_prices_for_nmids(raw_ids, dest=str(args.public_dest))
        if df.empty:
            print("Цены не найдены")
            return 2
        print(df.to_string(index=False))
        return 0

    if args.command != "run":
        raise RuntimeError(f"Неизвестная команда: {args.command}")

    s3 = build_s3_from_env()
    cfg = PriceCorrectorConfig(
        store=args.store,
        target_factor=args.target_factor,
        day_target_factor=args.day_target_factor,
        night_target_factor=args.night_target_factor,
        night_start_hour=args.night_start_hour,
        night_end_hour=args.night_end_hour,
        seller_discount=args.seller_discount,
        price_tolerance_rub=args.price_tolerance_rub,
        max_price_change_pct=args.max_price_change_pct,
        fallback_days=args.fallback_days,
        min_spp_sample_orders=args.min_spp_sample_orders,
        price_source=args.price_source,
        public_dest=args.public_dest,
        site_price_tolerance_rub=args.site_price_tolerance_rub,
        safe_wb_discount_floor_pct=args.safe_wb_discount_floor_pct,
        spp_adjustment_pct=args.spp_adjustment_pct,
        use_safe_floor_for_missing=args.use_safe_floor_for_missing,
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
