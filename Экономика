import os
import io
import re
import math
import json
import traceback
from datetime import datetime, timedelta, date
from typing import Dict, List, Tuple, Optional

import pandas as pd
import numpy as np
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
import pytz


# ============================================================
# НАСТРОЙКИ
# ============================================================

STORE_NAME = "TOPFACE"
TIMEZONE = "Europe/Moscow"

VAT_RATE = 7.0                # НДС, включён в цену
PROFIT_TAX_RATE = 15.0        # налог на прибыль
MIN_DLV_PRC = 0.8
EXPENSIVE_WAREHOUSE_THRESHOLD = 1.6
TARGET_DLV_PRC_CAP = 1.4

ACCEPTANCE_LOOKBACK_WEEKS = 9
RETENTION_WEEKS = 13

FINANCE_FOLDER = f"Отчёты/Финансовые показатели/{STORE_NAME}/Недельные"
STOCKS_FOLDER = f"Отчёты/Остатки/{STORE_NAME}/Недельные"
ADVERT_ANALYTICS_KEY = f"Отчёты/Реклама/{STORE_NAME}/Анализ рекламы.xlsx"
COST_KEY = "Отчёты/Себестоимость/Себестоимость.xlsx"
OUTPUT_KEY = f"Отчёты/Финансовые показатели/{STORE_NAME}/Экономика.xlsx"

SHEET_UNIT = "Юнит экономика"
SHEET_FACT = "Общий факт за неделю"
SHEET_WOW = "Анализ неделя к неделе"
SHEET_WH = "Склады_Коэффициенты"

# Если хотите расширить список, добавите сюда:
POSITIVE_COMPENSATION_OPS = {
    "Компенсация ущерба",
    "Добровольная компенсация при возврате",
}
NEGATIVE_COMPENSATION_OPS = {
    "Компенсация ущерба",
    "Добровольная компенсация при возврате",
}

# Общемагазинные расходы
STOREWIDE_EXPENSE_NAMES = {
    "Штраф",
    "Удержания",
    "Разовое изменение срока перечисления денежных средств",
}

# Логистика
DIRECT_LOGISTICS_HINTS = {
    "к клиенту при продаже",
    "клиенту при продаже",
}
REVERSE_LOGISTICS_HINTS = {
    "к клиенту при отмене",
    "от клиента при отмене",
    "от клиента при возврате",
    "возврат товара",
}

# ============================================================
# S3 STORAGE
# ============================================================

class S3Storage:
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

    def file_exists(self, key: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False

    def list_files(self, prefix: str) -> List[str]:
        try:
            resp = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
            if "Contents" not in resp:
                return []
            return [x["Key"] for x in resp["Contents"]]
        except Exception:
            return []

    def read_excel(self, key: str, sheet_name=0) -> pd.DataFrame:
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=key)
            data = obj["Body"].read()
            return pd.read_excel(io.BytesIO(data), sheet_name=sheet_name)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return pd.DataFrame()
            raise
        except Exception:
            print(f"Ошибка чтения Excel: {key}")
            traceback.print_exc()
            return pd.DataFrame()

    def read_excel_all_sheets(self, key: str) -> Dict[str, pd.DataFrame]:
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=key)
            data = obj["Body"].read()
            return pd.read_excel(io.BytesIO(data), sheet_name=None)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return {}
            raise
        except Exception:
            print(f"Ошибка чтения всех листов Excel: {key}")
            traceback.print_exc()
            return {}

    def write_excel_sheets(self, key: str, sheets: Dict[str, pd.DataFrame]) -> None:
        out = io.BytesIO()
        with pd.ExcelWriter(out, engine="openpyxl") as writer:
            for sheet_name, df in sheets.items():
                safe_name = str(sheet_name)[:31] if sheet_name else "Sheet1"
                if df is None:
                    df = pd.DataFrame()
                df.to_excel(writer, index=False, sheet_name=safe_name)
        out.seek(0)
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=out.getvalue())


# ============================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================

def log(msg: str):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {msg}", flush=True)


def week_id_from_date(d: date) -> str:
    y, w, _ = d.isocalendar()
    return f"{y}-W{w:02d}"


def week_start_from_week_id(week_id: str) -> date:
    year, week = week_id.split("-W")
    return date.fromisocalendar(int(year), int(week), 1)


def week_end_from_week_id(week_id: str) -> date:
    year, week = week_id.split("-W")
    return date.fromisocalendar(int(year), int(week), 7)


def get_last_complete_week_id() -> str:
    tz = pytz.timezone(TIMEZONE)
    today = datetime.now(tz).date()
    last_sunday = today - timedelta(days=today.weekday() + 1)
    return week_id_from_date(last_sunday)


def get_previous_week_id(week_id: str) -> str:
    start = week_start_from_week_id(week_id)
    prev = start - timedelta(days=7)
    return week_id_from_date(prev)


def get_last_n_week_ids(anchor_week_id: str, n: int) -> List[str]:
    start = week_start_from_week_id(anchor_week_id)
    result = []
    for i in range(n):
        d = start - timedelta(days=7 * i)
        result.append(week_id_from_date(d))
    return result


def retention_filter(df: pd.DataFrame, week_col: str = "Неделя", keep_weeks: int = RETENTION_WEEKS) -> pd.DataFrame:
    if df.empty or week_col not in df.columns:
        return df
    unique_weeks = sorted(df[week_col].dropna().astype(str).unique())
    keep = unique_weeks[-keep_weeks:]
    return df[df[week_col].astype(str).isin(keep)].copy()


def to_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0.0)


def to_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0).astype(int)


def safe_div(a, b):
    try:
        if b in (0, None) or pd.isna(b):
            return 0.0
        return float(a) / float(b)
    except Exception:
        return 0.0


def safe_round(x, n=2):
    try:
        if pd.isna(x):
            return 0
        return round(float(x), n)
    except Exception:
        return 0


def normalize_text(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()


def normalize_lower(x) -> str:
    return normalize_text(x).lower()


def first_non_empty(*values):
    for v in values:
        if pd.notna(v) and str(v).strip() != "":
            return v
    return None


def cols_exist(df: pd.DataFrame, cols: List[str]) -> bool:
    return all(c in df.columns for c in cols)


def ensure_columns(df: pd.DataFrame, cols: List[str]):
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan


def pick_first_existing(columns: List[str], candidates: List[str]) -> Optional[str]:
    low_map = {str(c).strip().lower(): c for c in columns}
    for c in candidates:
        if c.strip().lower() in low_map:
            return low_map[c.strip().lower()]
    return None


# ============================================================
# НОРМАЛИЗАЦИЯ СЕБЕСТОИМОСТИ
# ============================================================

def load_costs(s3: S3Storage) -> pd.DataFrame:
    df = s3.read_excel(COST_KEY, sheet_name=0)
    if df.empty:
        log("⚠️ Файл себестоимости пуст или не найден.")
        return pd.DataFrame(columns=["nm_id", "cost_price"])

    normalized = {col: str(col).strip().lower().replace("ё", "е") for col in df.columns}
    nm_col = None
    cost_col = None

    for col, norm in normalized.items():
        if norm in {"nm_id", "nmid", "артикул wb", "артикул", "id товара"} or "артикул wb" in norm:
            nm_col = col
            break

    for col, norm in normalized.items():
        if norm in {"cost_price", "себестоимость", "закупочная цена", "cost"} or "себестоим" in norm:
            cost_col = col
            break

    if nm_col is None or cost_col is None:
        log(f"⚠️ Не удалось определить колонки в Себестоимости. Колонки: {list(df.columns)}")
        return pd.DataFrame(columns=["nm_id", "cost_price"])

    out = df.rename(columns={nm_col: "nm_id", cost_col: "cost_price"}).copy()
    out["nm_id"] = pd.to_numeric(out["nm_id"], errors="coerce")
    out["cost_price"] = pd.to_numeric(out["cost_price"], errors="coerce").fillna(0.0)
    out = out.dropna(subset=["nm_id"])
    out["nm_id"] = out["nm_id"].astype("int64")
    out = out[["nm_id", "cost_price"]].drop_duplicates(subset=["nm_id"], keep="last")
    return out


# ============================================================
# ЗАГРУЗКА ФИНАНСОВ
# ============================================================

def load_finance_week(s3: S3Storage, week_id: str) -> pd.DataFrame:
    key = f"{FINANCE_FOLDER}/Финансовые показатели_{week_id}.xlsx"
    df = s3.read_excel(key, sheet_name=0)
    if df.empty:
        log(f"⚠️ Не найден финансовый файл: {key}")
        return pd.DataFrame()

    # обязательные поля
    must_cols = [
        "rr_dt", "nm_id", "sa_name", "subject_name", "brand_name",
        "doc_type_name", "supplier_oper_name", "quantity", "retail_amount",
        "retail_price_withdisc_rub", "commission_percent", "acquiring_fee",
        "acquiring_percent", "delivery_amount", "return_amount", "delivery_rub",
        "bonus_type_name", "penalty", "additional_payment", "rebill_logistic_cost",
        "storage_fee", "deduction", "acceptance", "ppvz_for_pay", "ppvz_vw",
        "ppvz_vw_nds", "ppvz_spp_prc", "office_name", "dlv_prc",
    ]
    ensure_columns(df, must_cols)

    # типы
    numeric_cols = [
        "nm_id", "quantity", "retail_amount", "retail_price_withdisc_rub",
        "commission_percent", "acquiring_fee", "acquiring_percent", "delivery_amount",
        "return_amount", "delivery_rub", "penalty", "additional_payment",
        "rebill_logistic_cost", "storage_fee", "deduction", "acceptance",
        "ppvz_for_pay", "ppvz_vw", "ppvz_vw_nds", "ppvz_spp_prc", "dlv_prc",
    ]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["rr_dt"] = pd.to_datetime(df["rr_dt"], errors="coerce").dt.date
    df["sale_dt"] = pd.to_datetime(df.get("sale_dt"), errors="coerce")
    df["order_dt"] = pd.to_datetime(df.get("order_dt"), errors="coerce")

    # строковые
    for c in ["doc_type_name", "supplier_oper_name", "bonus_type_name", "office_name", "sa_name", "subject_name", "brand_name"]:
        df[c] = df[c].astype(str).fillna("").str.strip()

    df["nm_id"] = pd.to_numeric(df["nm_id"], errors="coerce")
    return df


# ============================================================
# ЗАГРУЗКА ОСТАТКОВ
# ============================================================

def load_stocks_week(s3: S3Storage, week_id: str) -> pd.DataFrame:
    key = f"{STOCKS_FOLDER}/Остатки_{week_id}.xlsx"
    df = s3.read_excel(key, sheet_name=0)
    if df.empty:
        log(f"⚠️ Файл остатков не найден: {key}")
        return pd.DataFrame()

    ensure_columns(df, ["Дата сбора", "Дата запроса", "Артикул WB", "Доступно для продажи", "Склад"])
    if "Дата сбора" in df.columns:
        df["Дата сбора"] = pd.to_datetime(df["Дата сбора"], errors="coerce")
    if "Дата запроса" in df.columns:
        df["Дата запроса"] = pd.to_datetime(df["Дата запроса"], errors="coerce")
    df["Артикул WB"] = pd.to_numeric(df["Артикул WB"], errors="coerce")
    df["Доступно для продажи"] = pd.to_numeric(df["Доступно для продажи"], errors="coerce").fillna(0)
    return df


def get_stock_snapshot_for_week(stocks_df: pd.DataFrame) -> pd.DataFrame:
    """
    Берём первую доступную дату внутри файла.
    Сначала пытаемся 'Дата сбора', если пусто — 'Дата запроса'.
    """
    if stocks_df.empty:
        return pd.DataFrame(columns=["nm_id", "stock_units"])

    date_col = None
    if "Дата сбора" in stocks_df.columns and stocks_df["Дата сбора"].notna().any():
        date_col = "Дата сбора"
    elif "Дата запроса" in stocks_df.columns and stocks_df["Дата запроса"].notna().any():
        date_col = "Дата запроса"

    if date_col is None:
        log("⚠️ В остатках нет валидных дат.")
        return pd.DataFrame(columns=["nm_id", "stock_units"])

    first_dt = stocks_df[date_col].dropna().min()
    snap = stocks_df[stocks_df[date_col] == first_dt].copy()
    snap["nm_id"] = pd.to_numeric(snap["Артикул WB"], errors="coerce")
    snap["stock_units"] = pd.to_numeric(snap["Доступно для продажи"], errors="coerce").fillna(0)
    snap = snap.dropna(subset=["nm_id"])

    out = snap.groupby("nm_id", as_index=False)["stock_units"].sum()
    out["nm_id"] = out["nm_id"].astype("int64")
    return out


# ============================================================
# ЗАГРУЗКА РЕКЛАМЫ
# ============================================================

def load_advert_spend_week(s3: S3Storage, week_id: str) -> pd.DataFrame:
    sheets = s3.read_excel_all_sheets(ADVERT_ANALYTICS_KEY)
    if not sheets:
        log(f"⚠️ Не найден файл рекламы: {ADVERT_ANALYTICS_KEY}")
        return pd.DataFrame(columns=["nm_id", "advert_spend_week"])

    # основной лист
    if "Статистика_Ежедневно" in sheets:
        df = sheets["Статистика_Ежедневно"].copy()
    else:
        # fallback - первый лист
        first_sheet = next(iter(sheets.keys()))
        df = sheets[first_sheet].copy()

    if df.empty:
        return pd.DataFrame(columns=["nm_id", "advert_spend_week"])

    ensure_columns(df, ["Дата", "Артикул WB", "Расход"])
    df["Дата"] = pd.to_datetime(df["Дата"], errors="coerce").dt.date
    df["Артикул WB"] = pd.to_numeric(df["Артикул WB"], errors="coerce")
    df["Расход"] = pd.to_numeric(df["Расход"], errors="coerce").fillna(0)

    start = week_start_from_week_id(week_id)
    end = week_end_from_week_id(week_id)
    df = df[(df["Дата"] >= start) & (df["Дата"] <= end)].copy()

    if df.empty:
        return pd.DataFrame(columns=["nm_id", "advert_spend_week"])

    out = df.groupby("Артикул WB", as_index=False)["Расход"].sum()
    out.columns = ["nm_id", "advert_spend_week"]
    out["nm_id"] = out["nm_id"].astype("int64")
    return out


# ============================================================
# КЛАССИФИКАЦИЯ СТРОК ФИНОТЧЁТА
# ============================================================

def classify_finance_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()

    out["is_sale"] = (
        (out["doc_type_name"].str.lower() == "продажа") &
        (~out["supplier_oper_name"].isin(POSITIVE_COMPENSATION_OPS))
    )

    out["is_return"] = (
        (out["doc_type_name"].str.lower() == "возврат") &
        (~out["supplier_oper_name"].isin(NEGATIVE_COMPENSATION_OPS))
    )

    out["is_positive_compensation"] = (
        out["supplier_oper_name"].isin(POSITIVE_COMPENSATION_OPS) &
        (out["doc_type_name"].str.lower() == "продажа")
    )

    out["is_negative_compensation"] = (
        out["supplier_oper_name"].isin(NEGATIVE_COMPENSATION_OPS) &
        (out["doc_type_name"].str.lower() == "возврат")
    )

    out["is_storage"] = out["supplier_oper_name"].str.lower().eq("хранение")
    out["is_acceptance"] = (
        out["supplier_oper_name"].str.lower().isin({"обработка товара", "операции на приемке", "операции при приемке"})
        | (to_float(out["acceptance"]) != 0)
    )
    out["is_penalty"] = (out["supplier_oper_name"].str.lower().eq("штраф")) | (to_float(out["penalty"]) != 0)
    out["is_deduction"] = (out["supplier_oper_name"].str.lower().eq("удержания")) | (to_float(out["deduction"]) != 0)

    # классификация логистики
    bonus_lower = out["bonus_type_name"].astype(str).str.lower().fillna("")
    oper_lower = out["supplier_oper_name"].astype(str).str.lower().fillna("")

    out["is_logistics"] = (
        oper_lower.eq("логистика")
        | oper_lower.eq("возмещение издержек по перевозке/по складским операциям с товаром")
        | (to_float(out["delivery_rub"]) != 0)
        | (to_float(out["rebill_logistic_cost"]) != 0)
    )

    out["is_direct_logistics"] = (
        out["is_logistics"] &
        (
            bonus_lower.apply(lambda x: any(h in x for h in DIRECT_LOGISTICS_HINTS))
            | ((to_float(out["delivery_amount"]) > 0) & (to_float(out["return_amount"]) <= 0))
        )
    )

    out["is_reverse_logistics"] = (
        out["is_logistics"] &
        (
            bonus_lower.apply(lambda x: any(h in x for h in REVERSE_LOGISTICS_HINTS))
            | (to_float(out["return_amount"]) > 0)
        )
    )

    out["is_storewide_expense"] = (
        out["supplier_oper_name"].isin(STOREWIDE_EXPENSE_NAMES)
        | out["is_penalty"]
        | out["is_deduction"]
    )

    return out


# ============================================================
# РАСЧЁТ СТРОКОВОЙ КОМИССИИ
# ============================================================

def calculate_commission_rub(row: pd.Series) -> float:
    """
    Приоритет:
    1) ppvz_vw + ppvz_vw_nds — явное вознаграждение WB
    2) fallback: retail_price_withdisc_rub - ppvz_for_pay - acquiring_fee
    """
    ppvz_vw = float(row.get("ppvz_vw", 0) or 0)
    ppvz_vw_nds = float(row.get("ppvz_vw_nds", 0) or 0)
    explicit = ppvz_vw + ppvz_vw_nds
    if explicit != 0:
        return explicit

    retail = float(row.get("retail_price_withdisc_rub", 0) or 0)
    pay = float(row.get("ppvz_for_pay", 0) or 0)
    acquiring = float(row.get("acquiring_fee", 0) or 0)
    fallback = retail - pay - acquiring
    return max(fallback, 0.0)


# ============================================================
# АГРЕГАЦИЯ ФИНАНСОВ ПО SKU
# ============================================================

def build_weekly_fact_by_sku(fin_df: pd.DataFrame, cost_df: pd.DataFrame, advert_df: pd.DataFrame, stock_snapshot_df: pd.DataFrame, week_id: str) -> pd.DataFrame:
    if fin_df.empty:
        return pd.DataFrame()

    fin_df = classify_finance_rows(fin_df).copy()
    fin_df["commission_rub_row"] = fin_df.apply(calculate_commission_rub, axis=1)

    # База SKU — всё, где есть nm_id
    sku_df = fin_df[fin_df["nm_id"].notna() & (fin_df["nm_id"] != 0)].copy()
    if sku_df.empty:
        return pd.DataFrame()

    sku_df["nm_id"] = sku_df["nm_id"].astype("int64")

    # Продажи / возвраты / компенсации
    sold = sku_df[sku_df["is_sale"]].groupby("nm_id").agg(
        sold_units=("quantity", "sum"),
        revenue_sale=("retail_amount", "sum"),
        retail_price_withdisc_sale=("retail_price_withdisc_rub", "sum"),
        avg_spp_sale=("ppvz_spp_prc", "mean"),
        commission_rub_sale=("commission_rub_row", "sum"),
        acquiring_fee_sale=("acquiring_fee", "sum"),
        avg_commission_percent=("commission_percent", "mean"),
        avg_acquiring_percent=("acquiring_percent", "mean"),
    ).reset_index()

    returned = sku_df[sku_df["is_return"]].groupby("nm_id").agg(
        returned_units=("quantity", "sum"),
        revenue_return=("retail_amount", "sum"),
        retail_price_withdisc_return=("retail_price_withdisc_rub", "sum"),
        commission_rub_return=("commission_rub_row", "sum"),
        acquiring_fee_return=("acquiring_fee", "sum"),
    ).reset_index()

    pos_comp = sku_df[sku_df["is_positive_compensation"]].groupby("nm_id").agg(
        positive_comp_units=("quantity", "sum"),
        revenue_positive_comp=("retail_amount", "sum"),
    ).reset_index()

    neg_comp = sku_df[sku_df["is_negative_compensation"]].groupby("nm_id").agg(
        negative_comp_units=("quantity", "sum"),
        revenue_negative_comp=("retail_amount", "sum"),
    ).reset_index()

    # Прямая и обратная логистика
    direct_log = sku_df[sku_df["is_direct_logistics"]].copy()
    direct_log["direct_log_rub_row"] = to_float(direct_log["delivery_rub"]) + to_float(direct_log["rebill_logistic_cost"])
    direct_log_agg = direct_log.groupby("nm_id").agg(
        direct_logistics=("direct_log_rub_row", "sum"),
        direct_logistics_rows=("direct_log_rub_row", "count"),
    ).reset_index()

    reverse_log = sku_df[sku_df["is_reverse_logistics"]].copy()
    reverse_log["reverse_log_rub_row"] = to_float(reverse_log["delivery_rub"]) + to_float(reverse_log["rebill_logistic_cost"])
    reverse_log_agg = reverse_log.groupby("nm_id").agg(
        reverse_logistics=("reverse_log_rub_row", "sum"),
        reverse_logistics_rows=("reverse_log_rub_row", "count"),
    ).reset_index()

    # Справочник SKU
    info = sku_df.groupby("nm_id").agg(
        Артикул_продавца=("sa_name", lambda x: next((i for i in x if str(i).strip()), "")),
        Предмет=("subject_name", lambda x: next((i for i in x if str(i).strip()), "")),
        Бренд=("brand_name", lambda x: next((i for i in x if str(i).strip()), "")),
        last_sale_dt=("sale_dt", "max"),
    ).reset_index()

    # Последняя комиссия по SKU — по последним продажам недели
    sale_rows = sku_df[sku_df["is_sale"]].copy()
    sale_rows = sale_rows.sort_values(["nm_id", "sale_dt", "rr_dt"])
    last_commission = sale_rows.groupby("nm_id").tail(1)[["nm_id", "commission_percent", "acquiring_percent"]].copy()
    last_commission.columns = ["nm_id", "commission_percent_last", "acquiring_percent_last"]

    # Собираем базу
    base = info.copy()
    for part in [sold, returned, pos_comp, neg_comp, direct_log_agg, reverse_log_agg, advert_df, stock_snapshot_df, cost_df, last_commission]:
        if part is not None and not part.empty:
            base = base.merge(part, on="nm_id", how="left")

    # fillna
    numeric_cols = [
        "sold_units", "revenue_sale", "retail_price_withdisc_sale", "avg_spp_sale",
        "commission_rub_sale", "acquiring_fee_sale", "avg_commission_percent",
        "avg_acquiring_percent", "returned_units", "revenue_return", "retail_price_withdisc_return",
        "commission_rub_return", "acquiring_fee_return", "positive_comp_units",
        "revenue_positive_comp", "negative_comp_units", "revenue_negative_comp",
        "direct_logistics", "reverse_logistics", "advert_spend_week", "stock_units",
        "cost_price", "commission_percent_last", "acquiring_percent_last",
    ]
    for c in numeric_cols:
        if c in base.columns:
            base[c] = pd.to_numeric(base[c], errors="coerce").fillna(0.0)

    # NET UNITS и REVENUE
    base["sold_units"] = base.get("sold_units", 0)
    base["returned_units"] = base.get("returned_units", 0)
    base["positive_comp_units"] = base.get("positive_comp_units", 0)
    base["negative_comp_units"] = base.get("negative_comp_units", 0)

    base["Net units"] = (
        base["sold_units"]
        - base["returned_units"]
        + base["positive_comp_units"]
        - base["negative_comp_units"]
    )

    base["Валовая выручка"] = (
        base.get("revenue_sale", 0)
        - base.get("revenue_return", 0)
        + base.get("revenue_positive_comp", 0)
        - base.get("revenue_negative_comp", 0)
    )

    # Комиссия и эквайринг
    base["Комиссия WB"] = base.get("commission_rub_sale", 0) - base.get("commission_rub_return", 0)
    base["Эквайринг"] = base.get("acquiring_fee_sale", 0) - base.get("acquiring_fee_return", 0)

    # Себестоимость
    base["Себестоимость"] = base["Net units"] * base.get("cost_price", 0)

    # Средние цены
    retail_price_net = base.get("retail_price_withdisc_sale", 0) - base.get("retail_price_withdisc_return", 0)
    base["Средняя retail_amount"] = base.apply(lambda r: safe_div(r["Валовая выручка"], r["Net units"]), axis=1)
    base["Средняя retail_price_withdisc_rub"] = base.apply(lambda r: safe_div(retail_price_net.loc[r.name], r["Net units"]), axis=1)
    base["Средняя СПП"] = base.get("avg_spp_sale", 0).fillna(0)

    # Buyout rate proxy:
    # Берём sold_units / (sold_units + reverse logistics rows)
    base["Buyout rate"] = base.apply(
        lambda r: safe_div(r["sold_units"], (r["sold_units"] + r.get("reverse_logistics_rows", 0))),
        axis=1
    )

    # НДС
    base["НДС"] = base["Валовая выручка"] * VAT_RATE / (100.0 + VAT_RATE)

    # Пока без хранения/приёмки/штрафов — добавим дальше
    base["Хранение"] = 0.0
    base["Приёмка"] = 0.0
    base["Штрафы"] = 0.0
    base["Удержания"] = 0.0
    base["Логистика прямая"] = base.get("direct_logistics", 0)
    base["Логистика обратная"] = base.get("reverse_logistics", 0)
    base["Реклама"] = base.get("advert_spend_week", 0)

    # Week
    base["Неделя"] = week_id

    # Оставим нужное
    out = base[
        [
            "Неделя", "nm_id", "Артикул_продавца", "Предмет", "Бренд",
            "sold_units", "returned_units", "positive_comp_units", "negative_comp_units",
            "Net units", "Buyout rate", "Средняя retail_amount", "Средняя retail_price_withdisc_rub",
            "Средняя СПП", "commission_percent_last", "acquiring_percent_last",
            "Валовая выручка", "Себестоимость", "Комиссия WB", "Эквайринг",
            "Логистика прямая", "Логистика обратная", "Хранение", "Приёмка",
            "Штрафы", "Удержания", "Реклама", "cost_price", "stock_units"
        ]
    ].copy()

    # Переименуем чуть аккуратнее
    out = out.rename(columns={
        "sold_units": "Продажи, шт",
        "returned_units": "Возвраты, шт",
        "positive_comp_units": "Компенсации+, шт",
        "negative_comp_units": "Компенсации-, шт",
        "commission_percent_last": "Комиссия WB, % актуальная",
        "acquiring_percent_last": "Эквайринг, % актуальный",
        "cost_price": "Себестоимость, руб/ед",
        "stock_units": "Остаток, шт"
    })

    return out


# ============================================================
# ОБЩЕМАГАЗИННЫЕ РАСХОДЫ
# ============================================================

def calculate_storewide_expenses(fin_df: pd.DataFrame) -> Dict[str, float]:
    fin_df = classify_finance_rows(fin_df).copy()
    result = {
        "total_storage_week": 0.0,
        "total_acceptance_week": 0.0,
        "total_penalties_week": 0.0,
        "total_deductions_week": 0.0,
    }

    # Хранение
    storage_rows = fin_df[fin_df["is_storage"]].copy()
    if not storage_rows.empty:
        result["total_storage_week"] = float(to_float(storage_rows["storage_fee"]).sum())

    # Приёмка
    acceptance_rows = fin_df[fin_df["is_acceptance"]].copy()
    if not acceptance_rows.empty:
        result["total_acceptance_week"] = float(to_float(acceptance_rows["acceptance"]).sum())

    # Штрафы
    penalty_rows = fin_df[fin_df["is_penalty"]].copy()
    if not penalty_rows.empty:
        result["total_penalties_week"] = float(to_float(penalty_rows["penalty"]).sum())

    # Удержания
    deduction_rows = fin_df[fin_df["is_deduction"]].copy()
    if not deduction_rows.empty:
        result["total_deductions_week"] = float(to_float(deduction_rows["deduction"]).sum())

    return result


def allocate_storage_by_stock(fact_df: pd.DataFrame, total_storage_week: float) -> pd.DataFrame:
    out = fact_df.copy()
    total_stock = to_float(out["Остаток, шт"]).sum()
    if total_stock <= 0 or total_storage_week == 0:
        out["Хранение"] = 0.0
        return out
    out["Хранение"] = out["Остаток, шт"].apply(lambda x: total_storage_week * safe_div(x, total_stock))
    return out


def allocate_storewide_cost_by_sales_units(fact_df: pd.DataFrame, value: float, target_col: str) -> pd.DataFrame:
    out = fact_df.copy()
    total_units = to_float(out["Продажи, шт"]).sum()
    if total_units <= 0 or value == 0:
        out[target_col] = 0.0
        return out
    out[target_col] = out["Продажи, шт"].apply(lambda x: value * safe_div(x, total_units))
    return out


# ============================================================
# ПРИЁМКА ЗА 9 НЕДЕЛЬ
# ============================================================

def calculate_acceptance_per_unit_9w(s3: S3Storage, anchor_week_id: str) -> float:
    week_ids = get_last_n_week_ids(anchor_week_id, ACCEPTANCE_LOOKBACK_WEEKS)
    total_acceptance = 0.0
    total_sold_units = 0.0

    for w in week_ids:
        fin_df = load_finance_week(s3, w)
        if fin_df.empty:
            continue
        fin_df = classify_finance_rows(fin_df)
        total_acceptance += float(to_float(fin_df["acceptance"]).sum())

        sold_units = fin_df[fin_df["doc_type_name"].str.lower().eq("продажа")]["quantity"]
        total_sold_units += float(to_float(sold_units).sum())

    return safe_div(total_acceptance, total_sold_units)


def apply_acceptance_norm_to_fact(fact_df: pd.DataFrame, acceptance_per_unit_9w: float) -> pd.DataFrame:
    out = fact_df.copy()
    out["Приёмка"] = out["Продажи, шт"] * acceptance_per_unit_9w
    return out


# ============================================================
# ПРИБЫЛЬ
# ============================================================

def calculate_profit_columns(fact_df: pd.DataFrame) -> pd.DataFrame:
    out = fact_df.copy()

    # Валовая прибыль — до налогов
    out["Валовая прибыль"] = (
        out["Валовая выручка"]
        - out["Себестоимость"]
        - out["Комиссия WB"]
        - out["Эквайринг"]
        - out["Логистика прямая"]
        - out["Логистика обратная"]
        - out["Хранение"]
        - out["Приёмка"]
        - out["Штрафы"]
        - out["Удержания"]
        - out["Реклама"]
    )

    out["Прибыль до налога"] = out["Валовая прибыль"] - out["НДС"]
    out["Налог на прибыль"] = out["Прибыль до налога"].apply(lambda x: max(float(x), 0.0) * PROFIT_TAX_RATE / 100.0)
    out["Чистая прибыль"] = out["Прибыль до налога"] - out["Налог на прибыль"]

    return out


def build_unit_economics(fact_df: pd.DataFrame, acceptance_per_unit_9w: float) -> pd.DataFrame:
    out = fact_df.copy()

    out["Комиссия WB, руб/ед"] = out.apply(lambda r: safe_div(r["Комиссия WB"], r["Net units"]), axis=1)
    out["Эквайринг, руб/ед"] = out.apply(lambda r: safe_div(r["Эквайринг"], r["Net units"]), axis=1)
    out["Прямая логистика, руб/ед"] = out.apply(lambda r: safe_div(r["Логистика прямая"], r["Net units"]), axis=1)
    out["Обратная логистика, руб/ед"] = out.apply(lambda r: safe_div(r["Логистика обратная"], r["Net units"]), axis=1)
    out["Хранение, руб/ед"] = out.apply(lambda r: safe_div(r["Хранение"], r["Net units"]), axis=1)
    out["Приёмка, руб/ед (9 недель)"] = acceptance_per_unit_9w
    out["Штрафы и удержания, руб/ед"] = out.apply(lambda r: safe_div(r["Штрафы"] + r["Удержания"], r["Net units"]), axis=1)
    out["Реклама, руб/ед"] = out.apply(lambda r: safe_div(r["Реклама"], r["Net units"]), axis=1)
    out["Валовая прибыль, руб/ед"] = out.apply(lambda r: safe_div(r["Валовая прибыль"], r["Net units"]), axis=1)
    out["Чистая прибыль, руб/ед"] = out.apply(lambda r: safe_div(r["Чистая прибыль"], r["Net units"]), axis=1)
    out["Валовая маржа, %"] = out.apply(lambda r: safe_div(r["Валовая прибыль"], r["Валовая выручка"]) * 100, axis=1)
    out["Чистая маржа, %"] = out.apply(lambda r: safe_div(r["Чистая прибыль"], r["Валовая выручка"]) * 100, axis=1)

    cols = [
        "Неделя", "nm_id", "Артикул_продавца", "Предмет", "Бренд",
        "Продажи, шт", "Возвраты, шт", "Net units", "Buyout rate",
        "Средняя retail_amount", "Средняя retail_price_withdisc_rub", "Средняя СПП",
        "Комиссия WB, % актуальная", "Комиссия WB, руб/ед",
        "Эквайринг, % актуальный", "Эквайринг, руб/ед",
        "Прямая логистика, руб/ед", "Обратная логистика, руб/ед",
        "Хранение, руб/ед", "Приёмка, руб/ед (9 недель)",
        "Штрафы и удержания, руб/ед", "Реклама, руб/ед",
        "Себестоимость, руб/ед", "Валовая прибыль, руб/ед",
        "Чистая прибыль, руб/ед", "Валовая маржа, %", "Чистая маржа, %",
        "Остаток, шт"
    ]
    return out[cols].copy()


# ============================================================
# АНАЛИЗ НЕДЕЛЯ К НЕДЕЛЕ
# ============================================================

def explain_change(row: pd.Series) -> str:
    reasons = []

    delta_net = row.get("ΔЧистая прибыль", 0)
    delta_rev = row.get("ΔВаловая выручка", 0)
    delta_adv = row.get("ΔРеклама", 0)
    delta_comm = row.get("ΔКомиссия WB", 0)
    delta_log = row.get("ΔЛогистика итого", 0)
    delta_price = row.get("ΔСредняя retail_price_withdisc_rub", 0)
    delta_amount = row.get("ΔСредняя retail_amount", 0)
    delta_spp = row.get("ΔСредняя СПП", 0)
    delta_units = row.get("ΔПродажи, шт", 0)

    if delta_units > 0 and delta_rev > 0:
        reasons.append("рост объёма продаж")
    elif delta_units < 0 and delta_rev < 0:
        reasons.append("снижение объёма продаж")

    if delta_price > 0:
        reasons.append("выросла ваша цена")
    elif delta_price < 0:
        reasons.append("снизилась ваша цена")

    if abs(delta_amount) > 0 and abs(delta_price) < abs(delta_amount) * 0.3:
        if delta_amount < 0 and delta_spp > 0:
            reasons.append("снизилась цена покупателя из-за роста СПП WB")
        elif delta_amount > 0 and delta_spp < 0:
            reasons.append("цена покупателя выросла за счёт снижения СПП WB")

    if delta_adv > 0 and delta_rev <= 0:
        reasons.append("рекламные расходы выросли без роста выручки")
    elif delta_adv < 0 and delta_net > 0:
        reasons.append("снизились рекламные расходы")

    if delta_comm > 0:
        reasons.append("выросла комиссия WB")

    if delta_log > 0:
        reasons.append("выросли логистические расходы")

    if not reasons:
        if delta_net > 0:
            reasons.append("смешанный положительный эффект")
        elif delta_net < 0:
            reasons.append("смешанный отрицательный эффект")
        else:
            reasons.append("без существенных изменений")

    return "; ".join(reasons)


def build_wow_analysis(current_fact: pd.DataFrame, previous_fact: pd.DataFrame, week_id: str) -> pd.DataFrame:
    if current_fact.empty:
        return pd.DataFrame()

    if previous_fact.empty:
        out = current_fact.copy()
        out["Предыдущая неделя"] = ""
        out["ΔЧистая прибыль"] = 0.0
        out["Комментарий"] = "предыдущая неделя отсутствует"
        return out[["Неделя", "nm_id", "Артикул_продавца", "Предмет", "Бренд", "Комментарий"]].copy()

    cur = current_fact.copy()
    prev = previous_fact.copy()

    cur["Логистика итого"] = cur["Логистика прямая"] + cur["Логистика обратная"]
    prev["Логистика итого"] = prev["Логистика прямая"] + prev["Логистика обратная"]

    merge_cols = [
        "nm_id", "Артикул_продавца", "Предмет", "Бренд",
        "Продажи, шт", "Возвраты, шт", "Net units",
        "Средняя retail_amount", "Средняя retail_price_withdisc_rub", "Средняя СПП",
        "Валовая выручка", "Комиссия WB", "Эквайринг", "Логистика итого",
        "Хранение", "Приёмка", "Штрафы", "Удержания", "Реклама",
        "Валовая прибыль", "НДС", "Налог на прибыль", "Чистая прибыль"
    ]
    cur = cur[merge_cols].copy()
    prev = prev[merge_cols].copy()

    merged = cur.merge(prev, on="nm_id", how="outer", suffixes=("_cur", "_prev"))

    text_cols = ["Артикул_продавца", "Предмет", "Бренд"]
    for c in text_cols:
        merged[c] = merged[f"{c}_cur"].combine_first(merged[f"{c}_prev"])

    num_base = [
        "Продажи, шт", "Возвраты, шт", "Net units",
        "Средняя retail_amount", "Средняя retail_price_withdisc_rub", "Средняя СПП",
        "Валовая выручка", "Комиссия WB", "Эквайринг", "Логистика итого",
        "Хранение", "Приёмка", "Штрафы", "Удержания", "Реклама",
        "Валовая прибыль", "НДС", "Налог на прибыль", "Чистая прибыль"
    ]
    for c in num_base:
        merged[f"{c}_cur"] = pd.to_numeric(merged.get(f"{c}_cur"), errors="coerce").fillna(0)
        merged[f"{c}_prev"] = pd.to_numeric(merged.get(f"{c}_prev"), errors="coerce").fillna(0)
        merged[f"Δ{c}"] = merged[f"{c}_cur"] - merged[f"{c}_prev"]

    merged["Неделя"] = week_id
    merged["Предыдущая неделя"] = get_previous_week_id(week_id)
    merged["Комментарий"] = merged.apply(explain_change, axis=1)

    cols = [
        "Неделя", "Предыдущая неделя", "nm_id", "Артикул_продавца", "Предмет", "Бренд",
        "ΔЧистая прибыль", "ΔВаловая прибыль", "ΔВаловая выручка", "ΔПродажи, шт",
        "ΔСредняя retail_amount", "ΔСредняя retail_price_withdisc_rub", "ΔСредняя СПП",
        "ΔРеклама", "ΔКомиссия WB", "ΔЭквайринг", "ΔЛогистика итого", "ΔХранение",
        "ΔПриёмка", "ΔШтрафы", "ΔУдержания", "Комментарий"
    ]
    out = merged[cols].copy()
    out = out.sort_values("ΔЧистая прибыль", ascending=False).reset_index(drop=True)
    return out


# ============================================================
# СКЛАДЫ И КОЭФФИЦИЕНТЫ
# ============================================================

def build_warehouse_analysis(fin_df: pd.DataFrame, week_id: str) -> pd.DataFrame:
    if fin_df.empty:
        return pd.DataFrame()

    df = classify_finance_rows(fin_df).copy()
    df = df[df["is_direct_logistics"]].copy()
    if df.empty:
        return pd.DataFrame()

    df["dlv_prc"] = pd.to_numeric(df["dlv_prc"], errors="coerce")
    df["delivery_rub"] = pd.to_numeric(df["delivery_rub"], errors="coerce").fillna(0)
    df["rebill_logistic_cost"] = pd.to_numeric(df["rebill_logistic_cost"], errors="coerce").fillna(0)
    df["nm_id"] = pd.to_numeric(df["nm_id"], errors="coerce")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)

    df = df[df["dlv_prc"].notna()]
    df = df[df["dlv_prc"] > 0]
    df = df[df["dlv_prc"] >= MIN_DLV_PRC]

    if df.empty:
        return pd.DataFrame()

    df["actual_direct_log"] = df["delivery_rub"] + df["rebill_logistic_cost"]

    # Переплата только для дорогих складов
    def calc_overpay(row):
        coeff = float(row["dlv_prc"])
        actual = float(row["actual_direct_log"])
        if coeff <= EXPENSIVE_WAREHOUSE_THRESHOLD or coeff <= 0:
            return 0.0
        base = actual / coeff
        recalc = base * TARGET_DLV_PRC_CAP
        return max(actual - recalc, 0.0)

    df["overpay"] = df.apply(calc_overpay, axis=1)

    # Пересчёт логистики при cap
    def calc_capped_log(row):
        coeff = float(row["dlv_prc"])
        actual = float(row["actual_direct_log"])
        if coeff <= 0:
            return actual
        if coeff <= EXPENSIVE_WAREHOUSE_THRESHOLD:
            return actual
        base = actual / coeff
        return base * TARGET_DLV_PRC_CAP

    df["capped_logistics"] = df.apply(calc_capped_log, axis=1)

    wh = df.groupby("office_name", as_index=False).agg(
        **{
            "Средний коэффициент": ("dlv_prc", "mean"),
            "Количество продаж": ("quantity", "sum"),
            "Логистика факт": ("actual_direct_log", "sum"),
            "Логистика при cap = 1.4": ("capped_logistics", "sum"),
            "Переплата": ("overpay", "sum"),
        }
    )
    wh["Переплата на единицу"] = wh.apply(lambda r: safe_div(r["Переплата"], r["Количество продаж"]), axis=1)
    wh["Неделя"] = week_id

    wh = wh[
        ["Неделя", "office_name", "Средний коэффициент", "Количество продаж",
         "Логистика факт", "Логистика при cap = 1.4", "Переплата", "Переплата на единицу"]
    ].rename(columns={"office_name": "Склад"})

    wh = wh.sort_values("Переплата", ascending=False).reset_index(drop=True)
    return wh


# ============================================================
# ИТОГ ПО МАГАЗИНУ
# ============================================================

def build_store_total_row(fact_df: pd.DataFrame) -> pd.DataFrame:
    if fact_df.empty:
        return pd.DataFrame()

    total_numeric = [
        "Продажи, шт", "Возвраты, шт", "Компенсации+, шт", "Компенсации-, шт", "Net units",
        "Валовая выручка", "Себестоимость", "Комиссия WB", "Эквайринг",
        "Логистика прямая", "Логистика обратная", "Хранение", "Приёмка",
        "Штрафы", "Удержания", "Реклама", "Валовая прибыль",
        "НДС", "Прибыль до налога", "Налог на прибыль", "Чистая прибыль",
        "Остаток, шт"
    ]
    row = {}
    for c in total_numeric:
        if c in fact_df.columns:
            row[c] = pd.to_numeric(fact_df[c], errors="coerce").fillna(0).sum()

    row["Неделя"] = fact_df["Неделя"].iloc[0]
    row["nm_id"] = "ИТОГО"
    row["Артикул_продавца"] = ""
    row["Предмет"] = "ИТОГО ПО МАГАЗИНУ"
    row["Бренд"] = ""
    row["Buyout rate"] = safe_div(row.get("Продажи, шт", 0), row.get("Продажи, шт", 0) + 0)
    row["Средняя retail_amount"] = safe_div(row.get("Валовая выручка", 0), row.get("Net units", 0))
    row["Средняя retail_price_withdisc_rub"] = np.nan
    row["Средняя СПП"] = np.nan
    row["Комиссия WB, % актуальная"] = np.nan
    row["Эквайринг, % актуальный"] = np.nan
    row["Себестоимость, руб/ед"] = safe_div(row.get("Себестоимость", 0), row.get("Net units", 0))
    return pd.DataFrame([row])


# ============================================================
# ОБНОВЛЕНИЕ ИСТОРИИ В OUTPUT
# ============================================================

def upsert_history(existing: pd.DataFrame, new_rows: pd.DataFrame, key_cols: List[str]) -> pd.DataFrame:
    if existing is None or existing.empty:
        out = new_rows.copy()
    elif new_rows is None or new_rows.empty:
        out = existing.copy()
    else:
        out = pd.concat([existing, new_rows], ignore_index=True)
        existing_keys = [c for c in key_cols if c in out.columns]
        if existing_keys:
            out = out.drop_duplicates(subset=existing_keys, keep="last")
    if "Неделя" in out.columns:
        out = retention_filter(out, "Неделя", RETENTION_WEEKS)
    return out


# ============================================================
# MAIN
# ============================================================

def main():
    required_env = [
        "YC_ACCESS_KEY_ID",
        "YC_SECRET_ACCESS_KEY",
        "YC_BUCKET_NAME",
    ]
    missing = [x for x in required_env if not os.environ.get(x)]
    if missing:
        raise RuntimeError(f"Не заданы переменные окружения: {missing}")

    s3 = S3Storage(
        access_key=os.environ["YC_ACCESS_KEY_ID"],
        secret_key=os.environ["YC_SECRET_ACCESS_KEY"],
        bucket_name=os.environ["YC_BUCKET_NAME"],
    )

    week_id = get_last_complete_week_id()
    prev_week_id = get_previous_week_id(week_id)

    log("=" * 80)
    log(f"📌 Запуск weekly-экономики для магазина {STORE_NAME}")
    log(f"📅 Целевая неделя: {week_id}")
    log("=" * 80)

    # 1. Загружаем данные
    fin_df = load_finance_week(s3, week_id)
    if fin_df.empty:
        raise RuntimeError(f"Не удалось загрузить финансы за {week_id}")

    prev_fin_df = load_finance_week(s3, prev_week_id)
    stocks_df = load_stocks_week(s3, week_id)
    stock_snapshot_df = get_stock_snapshot_for_week(stocks_df)
    advert_df = load_advert_spend_week(s3, week_id)
    cost_df = load_costs(s3)

    # 2. Базовый факт по SKU
    fact_df = build_weekly_fact_by_sku(
        fin_df=fin_df,
        cost_df=cost_df,
        advert_df=advert_df,
        stock_snapshot_df=stock_snapshot_df,
        week_id=week_id
    )
    if fact_df.empty:
        raise RuntimeError("Не удалось собрать weekly fact по SKU")

    # 3. Общемагазинные расходы
    storewide = calculate_storewide_expenses(fin_df)

    fact_df = allocate_storage_by_stock(fact_df, storewide["total_storage_week"])
    fact_df = allocate_storewide_cost_by_sales_units(fact_df, storewide["total_penalties_week"], "Штрафы")
    fact_df = allocate_storewide_cost_by_sales_units(fact_df, storewide["total_deductions_week"], "Удержания")

    acceptance_per_unit_9w = calculate_acceptance_per_unit_9w(s3, week_id)
    fact_df = apply_acceptance_norm_to_fact(fact_df, acceptance_per_unit_9w)

    # 4. Прибыль
    fact_df = calculate_profit_columns(fact_df)

    # 5. Добавляем итог магазина
    total_row = build_store_total_row(fact_df)
    fact_df_full = pd.concat([fact_df, total_row], ignore_index=True)

    # 6. Юнит экономика
    unit_df = build_unit_economics(fact_df, acceptance_per_unit_9w)

    # 7. Анализ неделя к неделе
    prev_fact_df = pd.DataFrame()
    if not prev_fin_df.empty:
        prev_stocks_df = load_stocks_week(s3, prev_week_id)
        prev_stock_snapshot_df = get_stock_snapshot_for_week(prev_stocks_df)
        prev_advert_df = load_advert_spend_week(s3, prev_week_id)
        prev_fact_df = build_weekly_fact_by_sku(
            fin_df=prev_fin_df,
            cost_df=cost_df,
            advert_df=prev_advert_df,
            stock_snapshot_df=prev_stock_snapshot_df,
            week_id=prev_week_id
        )
        prev_storewide = calculate_storewide_expenses(prev_fin_df)
        prev_fact_df = allocate_storage_by_stock(prev_fact_df, prev_storewide["total_storage_week"])
        prev_fact_df = allocate_storewide_cost_by_sales_units(prev_fact_df, prev_storewide["total_penalties_week"], "Штрафы")
        prev_fact_df = allocate_storewide_cost_by_sales_units(prev_fact_df, prev_storewide["total_deductions_week"], "Удержания")
        prev_acc_norm = calculate_acceptance_per_unit_9w(s3, prev_week_id)
        prev_fact_df = apply_acceptance_norm_to_fact(prev_fact_df, prev_acc_norm)
        prev_fact_df = calculate_profit_columns(prev_fact_df)

    wow_df = build_wow_analysis(fact_df, prev_fact_df, week_id)

    # 8. Склады и коэффициенты
    warehouse_df = build_warehouse_analysis(fin_df, week_id)

    # 9. Загружаем существующую историю output
    existing_sheets = s3.read_excel_all_sheets(OUTPUT_KEY)

    existing_unit = existing_sheets.get(SHEET_UNIT, pd.DataFrame())
    existing_fact = existing_sheets.get(SHEET_FACT, pd.DataFrame())
    existing_wow = existing_sheets.get(SHEET_WOW, pd.DataFrame())
    existing_wh = existing_sheets.get(SHEET_WH, pd.DataFrame())

    # 10. Upsert history
    unit_hist = upsert_history(existing_unit, unit_df, ["Неделя", "nm_id"])
    fact_hist = upsert_history(existing_fact, fact_df_full, ["Неделя", "nm_id"])
    wow_hist = upsert_history(existing_wow, wow_df, ["Неделя", "nm_id"])
    wh_hist = upsert_history(existing_wh, warehouse_df, ["Неделя", "Склад"])

    # 11. Красивое округление
    def round_df(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        out = df.copy()
        for c in out.columns:
            if pd.api.types.is_float_dtype(out[c]) or pd.api.types.is_numeric_dtype(out[c]):
                out[c] = out[c].apply(lambda x: safe_round(x, 4) if abs(float(x)) < 1 else safe_round(x, 2))
        return out

    unit_hist = round_df(unit_hist)
    fact_hist = round_df(fact_hist)
    wow_hist = round_df(wow_hist)
    wh_hist = round_df(wh_hist)

    # 12. Сохраняем
    s3.write_excel_sheets(
        OUTPUT_KEY,
        {
            SHEET_UNIT: unit_hist,
            SHEET_FACT: fact_hist,
            SHEET_WOW: wow_hist,
            SHEET_WH: wh_hist,
        }
    )

    log(f"✅ Экономика сохранена: {OUTPUT_KEY}")
    log(f"   - {SHEET_UNIT}: {len(unit_hist)} строк")
    log(f"   - {SHEET_FACT}: {len(fact_hist)} строк")
    log(f"   - {SHEET_WOW}: {len(wow_hist)} строк")
    log(f"   - {SHEET_WH}: {len(wh_hist)} строк")
    log("✅ Готово")


if __name__ == "__main__":
    main()
