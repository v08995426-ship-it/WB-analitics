import os
import io
import tempfile
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import boto3
import pandas as pd
import pytz
from botocore.client import Config
from botocore.exceptions import ClientError


# =========================================================
# НАСТРОЙКИ
# =========================================================

STORE_NAME = "TOPFACE"
TIMEZONE = "Europe/Moscow"

VAT_RATE = 7.0
PROFIT_TAX_RATE = 15.0

MIN_DLV_PRC = 0.8
EXPENSIVE_WAREHOUSE_THRESHOLD = 1.6
TARGET_DLV_PRC_CAP = 1.4

ACCEPTANCE_LOOKBACK_WEEKS = 9
RETENTION_WEEKS = 13

ECONOMICS_KEY = f"Отчёты/Финансовые показатели/{STORE_NAME}/Экономика.xlsx"
FINANCE_PREFIX = f"Отчёты/Финансовые показатели/{STORE_NAME}/Недельные/"
STOCKS_PREFIX = f"Отчёты/Остатки/{STORE_NAME}/Недельные/"
ADVERT_ANALYTICS_KEY = f"Отчёты/Реклама/{STORE_NAME}/Анализ рекламы.xlsx"
ADVERT_WEEKLY_PREFIX = f"Отчёты/Реклама/{STORE_NAME}/Недельные/"
COST_KEY = "Отчёты/Себестоимость/Себестоимость.xlsx"

WEEK_SHEET_UNIT = "Юнит экономика"
WEEK_SHEET_FACT = "Общий факт за неделю"
WEEK_SHEET_ANALYSIS = "Анализ неделя к неделе"
WEEK_SHEET_WAREHOUSES = "Склады_Коэффициенты"

SALE_LIKE_OPERATIONS = {
    "Продажа",
    "Компенсация ущерба",
    "Добровольная компенсация при возврате",
    "Компенсация скидки по программе лояльности",
}

RETURN_LIKE_OPERATIONS = {
    "Возврат",
}

DIRECT_LOGISTIC_HINTS = [
    "к клиенту при продаже",
    "к клиенту",
]

REVERSE_LOGISTIC_HINTS = [
    "от клиента при отмене",
    "от клиента при возврате",
    "к клиенту при отмене",
    "возврат товара",
    "возврат",
    "от клиента",
]


# =========================================================
# LOGGING / UTILS
# =========================================================

def log(msg: str):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def moscow_now():
    return datetime.now(pytz.timezone(TIMEZONE))


def safe_round(x, digits=6):
    try:
        if pd.isna(x):
            return 0.0
        return round(float(x), digits)
    except Exception:
        return 0.0


def safe_div(a, b, digits=6):
    try:
        a = float(a)
        b = float(b)
        if b == 0:
            return 0.0
        return round(a / b, digits)
    except Exception:
        return 0.0


def norm_text(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip().lower()


def week_label(dt: datetime.date) -> str:
    year, week_num, _ = dt.isocalendar()
    return f"{year}-W{week_num:02d}"


def get_last_full_week_range() -> Tuple[datetime.date, datetime.date]:
    today = moscow_now().date()
    current_week_monday = today - timedelta(days=today.weekday())
    end_prev_week = current_week_monday - timedelta(days=1)
    start_prev_week = end_prev_week - timedelta(days=6)
    return start_prev_week, end_prev_week


def get_weekly_finance_key(week_start: datetime.date) -> str:
    return f"{FINANCE_PREFIX}Финансовые показатели_{week_label(week_start)}.xlsx"


def get_weekly_stocks_key(week_start: datetime.date) -> str:
    return f"{STOCKS_PREFIX}Остатки_{week_label(week_start)}.xlsx"


def get_weekly_advert_key(week_start: datetime.date) -> str:
    return f"{ADVERT_WEEKLY_PREFIX}Реклама_{week_label(week_start)}.xlsx"


def to_date_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.date


def to_datetime_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def ensure_numeric(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


def mode_or_last(series: pd.Series):
    s = series.dropna().astype(str).str.strip()
    if s.empty:
        return ""
    mode = s.mode()
    if not mode.empty:
        return mode.iloc[0]
    return s.iloc[-1]


def extract_vat_from_gross(revenue_with_vat: float, vat_rate: float) -> float:
    return safe_round(revenue_with_vat * vat_rate / (100.0 + vat_rate), 6)


def get_sign_for_row(doc_type_name: str, supplier_oper_name: str) -> int:
    doc = norm_text(doc_type_name)
    oper = norm_text(supplier_oper_name)

    if supplier_oper_name in SALE_LIKE_OPERATIONS or doc == "продажа":
        return 1
    if supplier_oper_name in RETURN_LIKE_OPERATIONS or doc == "возврат":
        return -1
    if oper in {x.lower() for x in SALE_LIKE_OPERATIONS}:
        return 1
    if oper in {x.lower() for x in RETURN_LIKE_OPERATIONS}:
        return -1
    return 0


def classify_logistics_row(row) -> str:
    supplier_oper = norm_text(row.get("supplier_oper_name", ""))
    bonus_type = norm_text(row.get("bonus_type_name", ""))
    delivery_amount = float(row.get("delivery_amount", 0) or 0)
    return_amount = float(row.get("return_amount", 0) or 0)

    if supplier_oper != "логистика":
        return "none"

    for hint in REVERSE_LOGISTIC_HINTS:
        if hint in bonus_type:
            return "reverse"

    for hint in DIRECT_LOGISTIC_HINTS:
        if hint in bonus_type:
            return "direct"

    if return_amount > 0:
        return "reverse"
    if delivery_amount > 0:
        return "direct"
    return "direct"


def build_week_list(last_week_start: datetime.date, lookback_weeks: int) -> List[datetime.date]:
    weeks = []
    current = last_week_start
    for _ in range(lookback_weeks):
        weeks.append(current)
        current = current - timedelta(days=7)
    return sorted(weeks)


# =========================================================
# S3 / YANDEX OBJECT STORAGE
# =========================================================

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
        out = []
        token = None
        while True:
            kwargs = {"Bucket": self.bucket, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            resp = self.s3.list_objects_v2(**kwargs)
            for obj in resp.get("Contents", []):
                out.append(obj["Key"])
            if resp.get("IsTruncated"):
                token = resp.get("NextContinuationToken")
            else:
                break
        return out

    def read_excel(self, key: str, sheet_name=0):
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        data = obj["Body"].read()
        return pd.read_excel(io.BytesIO(data), sheet_name=sheet_name)

    def read_excel_all_sheets(self, key: str) -> Dict[str, pd.DataFrame]:
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        data = obj["Body"].read()
        return pd.read_excel(io.BytesIO(data), sheet_name=None)

    def write_excel_sheets(self, key: str, sheets: Dict[str, pd.DataFrame]):
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            with pd.ExcelWriter(tmp_path, engine="openpyxl") as writer:
                for sheet_name, df in sheets.items():
                    safe_sheet = str(sheet_name)[:31]
                    if df is None:
                        df = pd.DataFrame()
                    df.to_excel(writer, index=False, sheet_name=safe_sheet)
            self.s3.upload_file(tmp_path, self.bucket, key)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)


# =========================================================
# COSTS NORMALIZATION
# =========================================================

def normalize_cost_dataframe(cost_df: pd.DataFrame) -> pd.DataFrame:
    if cost_df.empty:
        return pd.DataFrame(columns=["nm_id", "cost_price"])

    df = cost_df.copy()
    original_columns = list(df.columns)

    normalized = {}
    for col in df.columns:
        norm = str(col).strip().lower().replace("ё", "е")
        normalized[col] = norm

    nm_col = None
    cost_col = None

    nm_priority = [
        "nm_id",
        "nmid",
        "артикул wb",
        "артикул вб",
        "wb article",
        "wb id",
        "код wb",
    ]

    for col, norm in normalized.items():
        if norm in nm_priority:
            nm_col = col
            break

    if nm_col is None:
        for col, norm in normalized.items():
            if "артикул wb" in norm or "артикул вб" in norm:
                nm_col = col
                break

    cost_priority = [
        "cost_price",
        "себестоимость",
        "стоимость",
        "cost",
        "cost price",
        "закупочная цена",
    ]

    for col, norm in normalized.items():
        if norm in cost_priority:
            cost_col = col
            break

    if cost_col is None:
        for col, norm in normalized.items():
            if "себестоим" in norm or "стоимость" in norm or norm.startswith("cost"):
                cost_col = col
                break

    if nm_col is None:
        for col in original_columns:
            if "вб" in str(col).lower():
                nm_col = col
                break

    if cost_col is None and len(original_columns) >= 4:
        cost_col = original_columns[-1]

    if nm_col is None or cost_col is None:
        log(f"⚠️ Не удалось определить колонки в Себестоимости. Колонки: {original_columns}")
        return pd.DataFrame(columns=["nm_id", "cost_price"])

    df = df.rename(columns={nm_col: "nm_id", cost_col: "cost_price"})
    df["nm_id"] = pd.to_numeric(df["nm_id"], errors="coerce")
    df["cost_price"] = pd.to_numeric(df["cost_price"], errors="coerce")

    df = df[["nm_id", "cost_price"]].dropna(subset=["nm_id"])
    df["nm_id"] = df["nm_id"].astype("int64")
    df["cost_price"] = df["cost_price"].fillna(0.0)

    return df.drop_duplicates(subset="nm_id", keep="last")


# =========================================================
# READ INPUTS
# =========================================================

def read_finance_week(s3: S3Storage, week_start: datetime.date) -> pd.DataFrame:
    key = get_weekly_finance_key(week_start)
    if not s3.file_exists(key):
        log(f"⚠️ Не найден фин. отчёт: {key}")
        return pd.DataFrame()

    df = s3.read_excel(key, sheet_name=0)
    if df.empty:
        return df

    numeric_cols = [
        "nm_id", "quantity", "retail_price", "retail_amount",
        "retail_price_withdisc_rub", "commission_percent", "ppvz_for_pay",
        "acquiring_fee", "acquiring_percent", "delivery_rub", "delivery_amount",
        "return_amount", "penalty", "additional_payment", "rebill_logistic_cost",
        "storage_fee", "deduction", "acceptance", "ppvz_spp_prc", "dlv_prc"
    ]
    df = ensure_numeric(df, numeric_cols)

    if "rr_dt" in df.columns:
        df["rr_dt"] = to_date_series(df["rr_dt"])
    if "sale_dt" in df.columns:
        df["sale_dt"] = to_datetime_series(df["sale_dt"])
    if "order_dt" in df.columns:
        df["order_dt"] = to_datetime_series(df["order_dt"])
    if "nm_id" in df.columns:
        df["nm_id"] = pd.to_numeric(df["nm_id"], errors="coerce")

    return df


def read_stocks_week(s3: S3Storage, week_start: datetime.date) -> pd.DataFrame:
    key = get_weekly_stocks_key(week_start)
    if not s3.file_exists(key):
        log(f"⚠️ Не найден отчёт остатков: {key}")
        return pd.DataFrame()

    df = s3.read_excel(key, sheet_name=0)
    if df.empty:
        return df

    if "Дата сбора" in df.columns:
        df["Дата сбора"] = to_date_series(df["Дата сбора"])
    elif "Дата запроса" in df.columns:
        df["Дата сбора"] = to_date_series(df["Дата запроса"])

    df = ensure_numeric(df, ["Артикул WB", "Доступно для продажи", "Полное количество"])
    if "Артикул WB" in df.columns:
        df["Артикул WB"] = pd.to_numeric(df["Артикул WB"], errors="coerce")

    return df


def read_advert_week(s3: S3Storage, week_start: datetime.date, week_end: datetime.date) -> pd.DataFrame:
    try:
        sheets = s3.read_excel_all_sheets(ADVERT_ANALYTICS_KEY)
        if "Статистика_Ежедневно" in sheets:
            df = sheets["Статистика_Ежедневно"].copy()
        else:
            df = next(iter(sheets.values())).copy()

        if not df.empty and "Дата" in df.columns:
            df["Дата"] = to_date_series(df["Дата"])
            df = ensure_numeric(df, ["Артикул WB", "Расход", "Сумма заказов", "Показы", "Клики", "Заказы"])
            df["Артикул WB"] = pd.to_numeric(df["Артикул WB"], errors="coerce")
            df = df[(df["Дата"] >= week_start) & (df["Дата"] <= week_end)]
            return df
    except Exception as e:
        log(f"⚠️ Не удалось прочитать Анализ рекламы.xlsx: {e}")

    weekly_key = get_weekly_advert_key(week_start)
    if not s3.file_exists(weekly_key):
        log(f"⚠️ Не найден weekly-рекламный файл: {weekly_key}")
        return pd.DataFrame()

    try:
        sheets = s3.read_excel_all_sheets(weekly_key)
        if "Статистика_Ежедневно" in sheets:
            df = sheets["Статистика_Ежедневно"].copy()
        else:
            df = next(iter(sheets.values())).copy()

        if not df.empty and "Дата" in df.columns:
            df["Дата"] = to_date_series(df["Дата"])
            df = ensure_numeric(df, ["Артикул WB", "Расход", "Сумма заказов", "Показы", "Клики", "Заказы"])
            df["Артикул WB"] = pd.to_numeric(df["Артикул WB"], errors="coerce")
            df = df[(df["Дата"] >= week_start) & (df["Дата"] <= week_end)]
            return df
    except Exception as e:
        log(f"⚠️ Не удалось прочитать weekly рекламу: {e}")

    return pd.DataFrame()


def read_costs(s3: S3Storage) -> pd.DataFrame:
    if not s3.file_exists(COST_KEY):
        log(f"⚠️ Не найден файл себестоимости: {COST_KEY}")
        return pd.DataFrame(columns=["nm_id", "cost_price"])

    raw = s3.read_excel(COST_KEY, sheet_name=0)
    if raw.empty:
        return pd.DataFrame(columns=["nm_id", "cost_price"])

    return normalize_cost_dataframe(raw)


# =========================================================
# PREP FINANCE
# =========================================================

def build_finance_rows(fin_df: pd.DataFrame) -> pd.DataFrame:
    if fin_df.empty:
        return pd.DataFrame()

    df = fin_df.copy()

    df["supplier_oper_name_norm"] = df.get("supplier_oper_name", "").astype(str).str.strip()
    df["doc_type_name_norm"] = df.get("doc_type_name", "").astype(str).str.strip()
    df["subject_name"] = df.get("subject_name", "").astype(str)
    df["brand_name"] = df.get("brand_name", "").astype(str)
    df["sa_name"] = df.get("sa_name", "").astype(str)
    df["office_name"] = df.get("office_name", "").astype(str)
    df["bonus_type_name"] = df.get("bonus_type_name", "").astype(str)

    df["sign"] = df.apply(
        lambda r: get_sign_for_row(r.get("doc_type_name_norm", ""), r.get("supplier_oper_name_norm", "")),
        axis=1
    )

    df["signed_quantity"] = df["quantity"] * df["sign"]
    df["signed_retail_amount"] = df["retail_amount"] * df["sign"]
    df["signed_retail_price_withdisc_rub"] = df["retail_price_withdisc_rub"] * df["sign"]

    commission_raw = (df["retail_price_withdisc_rub"] - df["ppvz_for_pay"] - df["acquiring_fee"]).fillna(0)
    df["signed_commission"] = commission_raw.abs() * df["sign"]
    df["signed_acquiring"] = df["acquiring_fee"].abs() * df["sign"]

    return df


# =========================================================
# STORAGE / WAREHOUSES
# =========================================================

def calc_storage_allocation(
    stocks_df: pd.DataFrame,
    total_storage_week: float,
    week_start: datetime.date,
    week_end: datetime.date
) -> pd.DataFrame:
    if stocks_df.empty or "Дата сбора" not in stocks_df.columns or "Артикул WB" not in stocks_df.columns:
        return pd.DataFrame(columns=["nm_id", "Хранение"])

    df = stocks_df.copy()
    df = df[df["Дата сбора"].notna()].copy()

    if df.empty:
        return pd.DataFrame(columns=["nm_id", "Хранение"])

    in_week = df[(df["Дата сбора"] >= week_start) & (df["Дата сбора"] <= week_end)].copy()
    if in_week.empty:
        target_date = df["Дата сбора"].min()
        in_week = df[df["Дата сбора"] == target_date].copy()
    else:
        target_date = in_week["Дата сбора"].min()
        in_week = in_week[in_week["Дата сбора"] == target_date].copy()

    qty_col = "Доступно для продажи" if "Доступно для продажи" in in_week.columns else "Полное количество"
    in_week[qty_col] = pd.to_numeric(in_week[qty_col], errors="coerce").fillna(0)
    in_week["Артикул WB"] = pd.to_numeric(in_week["Артикул WB"], errors="coerce")
    in_week = in_week.dropna(subset=["Артикул WB"]).copy()
    in_week["Артикул WB"] = in_week["Артикул WB"].astype("int64")

    agg = (
        in_week.groupby("Артикул WB", as_index=False)[qty_col]
        .sum()
        .rename(columns={"Артикул WB": "nm_id", qty_col: "stock_units"})
    )

    total_stock_units = agg["stock_units"].sum()
    if total_stock_units <= 0:
        agg["Хранение"] = 0.0
        return agg[["nm_id", "Хранение"]]

    agg["Хранение"] = agg["stock_units"] * total_storage_week / total_stock_units
    return agg[["nm_id", "Хранение"]]


def calc_warehouse_analysis(logistic_rows: pd.DataFrame, week_start: datetime.date) -> pd.DataFrame:
    if logistic_rows.empty:
        return pd.DataFrame(columns=[
            "Неделя", "Склад", "Средний коэффициент", "Количество продаж",
            "Логистика факт", "Логистика при cap=1.4", "Переплата", "Переплата на единицу"
        ])

    df = logistic_rows.copy()
    df["dlv_prc"] = pd.to_numeric(df["dlv_prc"], errors="coerce")
    df["delivery_rub"] = pd.to_numeric(df["delivery_rub"], errors="coerce").fillna(0)
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)

    df["log_type"] = df.apply(classify_logistics_row, axis=1)
    df = df[df["log_type"] == "direct"].copy()
    df = df[df["dlv_prc"].notna()].copy()
    df = df[df["dlv_prc"] >= MIN_DLV_PRC].copy()

    if df.empty:
        return pd.DataFrame(columns=[
            "Неделя", "Склад", "Средний коэффициент", "Количество продаж",
            "Логистика факт", "Логистика при cap=1.4", "Переплата", "Переплата на единицу"
        ])

    def recalc_row(row):
        actual = abs(float(row["delivery_rub"]))
        coeff = float(row["dlv_prc"])
        if coeff <= 0:
            return actual, actual, 0.0
        if coeff > EXPENSIVE_WAREHOUSE_THRESHOLD:
            base = actual / coeff
            recalc = base * TARGET_DLV_PRC_CAP
            overpay = max(0.0, actual - recalc)
            return actual, recalc, overpay
        return actual, actual, 0.0

    tmp = df.apply(lambda r: pd.Series(recalc_row(r), index=["actual_delivery", "recalc_delivery", "overpay"]), axis=1)
    df = pd.concat([df, tmp], axis=1)

    out = (
        df.groupby("office_name", as_index=False)
        .agg(
            **{
                "Средний коэффициент": ("dlv_prc", "mean"),
                "Количество продаж": ("quantity", "sum"),
                "Логистика факт": ("actual_delivery", "sum"),
                "Логистика при cap=1.4": ("recalc_delivery", "sum"),
                "Переплата": ("overpay", "sum"),
            }
        )
        .rename(columns={"office_name": "Склад"})
    )

    out["Переплата на единицу"] = out.apply(
        lambda r: safe_div(r["Переплата"], r["Количество продаж"], 6), axis=1
    )
    out["Неделя"] = week_label(week_start)

    out = out[[
        "Неделя", "Склад", "Средний коэффициент", "Количество продаж",
        "Логистика факт", "Логистика при cap=1.4", "Переплата", "Переплата на единицу"
    ]].copy()

    return out.sort_values("Переплата", ascending=False)


# =========================================================
# WEEKLY FACT / UNIT ECONOMICS
# =========================================================

def calc_weekly_facts(
    fin_df: pd.DataFrame,
    advert_df: pd.DataFrame,
    cost_df: pd.DataFrame,
    stocks_df: pd.DataFrame,
    week_start: datetime.date,
    week_end: datetime.date,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if fin_df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    df = build_finance_rows(fin_df)

    product_rows = df[df["nm_id"].notna()].copy()
    product_rows["nm_id"] = product_rows["nm_id"].astype("int64")

    meta_agg = (
        product_rows.groupby("nm_id", as_index=False)
        .agg(
            subject_name=("subject_name", lambda x: mode_or_last(x)),
            brand_name=("brand_name", lambda x: mode_or_last(x)),
            sa_name=("sa_name", lambda x: mode_or_last(x)),
        )
    )

    signed_rows = product_rows[product_rows["sign"] != 0].copy()

    sold_units = (
        signed_rows[signed_rows["sign"] == 1]
        .groupby("nm_id")["quantity"]
        .sum()
        .rename("Продажи, шт")
    )
    return_units = (
        signed_rows[signed_rows["sign"] == -1]
        .groupby("nm_id")["quantity"]
        .sum()
        .rename("Возвраты, шт")
    )
    net_units = (
        signed_rows.groupby("nm_id")["signed_quantity"]
        .sum()
        .rename("Net units")
    )
    revenue = (
        signed_rows.groupby("nm_id")["signed_retail_amount"]
        .sum()
        .rename("Валовая выручка")
    )
    retail_price_withdisc_total = (
        signed_rows.groupby("nm_id")["signed_retail_price_withdisc_rub"]
        .sum()
        .rename("Сумма retail_price_withdisc_rub")
    )
    commission_total = (
        signed_rows.groupby("nm_id")["signed_commission"]
        .sum()
        .rename("Комиссия WB")
    )
    acquiring_total = (
        signed_rows.groupby("nm_id")["signed_acquiring"]
        .sum()
        .rename("Эквайринг")
    )
    avg_retail_amount = (
        revenue / net_units.replace(0, pd.NA)
    ).rename("Средняя retail_amount")
    avg_retail_price_withdisc = (
        retail_price_withdisc_total / net_units.replace(0, pd.NA)
    ).rename("Средняя retail_price_withdisc_rub")

    sale_like = product_rows[
        product_rows.apply(
            lambda r: get_sign_for_row(r.get("doc_type_name_norm", ""), r.get("supplier_oper_name_norm", "")) == 1,
            axis=1
        )
    ].copy()

    sale_like = sale_like.sort_values(["nm_id", "sale_dt", "rr_dt"], ascending=[True, False, False])

    last_commission_pct = (
        sale_like.groupby("nm_id")["commission_percent"]
        .first()
        .rename("Комиссия WB, % актуальная")
    )

    avg_spp = (
        sale_like.groupby("nm_id")["ppvz_spp_prc"]
        .mean()
        .rename("Средняя СПП")
    )

    avg_acquiring_pct = (
        sale_like.groupby("nm_id")["acquiring_percent"]
        .mean()
        .rename("Эквайринг, % средний")
    )

    logistic_rows = product_rows[product_rows["supplier_oper_name_norm"].str.lower() == "логистика"].copy()
    if not logistic_rows.empty:
        logistic_rows["log_type"] = logistic_rows.apply(classify_logistics_row, axis=1)
    else:
        logistic_rows["log_type"] = []

    direct_logistics = (
        logistic_rows[logistic_rows["log_type"] == "direct"]
        .groupby("nm_id")["delivery_rub"]
        .sum()
        .abs()
        .rename("Логистика прямая")
    )

    reverse_logistics = (
        logistic_rows[logistic_rows["log_type"] == "reverse"]
        .groupby("nm_id")["delivery_rub"]
        .sum()
        .abs()
        .rename("Логистика обратная")
    )

    reverse_events = (
        logistic_rows[logistic_rows["log_type"] == "reverse"]
        .groupby("nm_id")["return_amount"]
        .sum()
        .rename("_reverse_events")
    )

    storage_total_store = abs(pd.to_numeric(df.get("storage_fee", 0), errors="coerce").fillna(0).sum())
    acceptance_total_store = abs(pd.to_numeric(df.get("acceptance", 0), errors="coerce").fillna(0).sum())
    penalty_total_store = abs(pd.to_numeric(df.get("penalty", 0), errors="coerce").fillna(0).sum())
    deduction_total_store = abs(pd.to_numeric(df.get("deduction", 0), errors="coerce").fillna(0).sum())

    if not advert_df.empty and "Артикул WB" in advert_df.columns:
        advert_agg = (
            advert_df.groupby("Артикул WB", as_index=False)
            .agg(
                Реклама=("Расход", "sum"),
                Рекламные_заказы=("Заказы", "sum"),
                Рекламная_выручка=("Сумма заказов", "sum"),
                Рекламные_показы=("Показы", "sum"),
                Рекламные_клики=("Клики", "sum"),
            )
        )
        advert_agg["Артикул WB"] = pd.to_numeric(advert_agg["Артикул WB"], errors="coerce").fillna(0).astype("int64")
        advert_agg = advert_agg.rename(columns={"Артикул WB": "nm_id"})
    else:
        advert_agg = pd.DataFrame(columns=["nm_id", "Реклама", "Рекламные_заказы", "Рекламная_выручка", "Рекламные_показы", "Рекламные_клики"])

    if cost_df.empty:
        cost_df = pd.DataFrame(columns=["nm_id", "cost_price"])

    fact = meta_agg.copy()
    series_list = [
        sold_units, return_units, net_units, revenue, retail_price_withdisc_total,
        commission_total, acquiring_total, avg_retail_amount, avg_retail_price_withdisc,
        last_commission_pct, avg_spp, avg_acquiring_pct,
        direct_logistics, reverse_logistics, reverse_events
    ]

    for s in series_list:
        fact = fact.merge(s.reset_index(), on="nm_id", how="left")

    fact = fact.merge(cost_df, on="nm_id", how="left")
    fact = fact.merge(advert_agg, on="nm_id", how="left")

    if "cost_price" not in fact.columns:
        fact["cost_price"] = 0.0

    num_fill_cols = [
        "Продажи, шт", "Возвраты, шт", "Net units", "Валовая выручка",
        "Сумма retail_price_withdisc_rub", "Комиссия WB", "Эквайринг",
        "Средняя retail_amount", "Средняя retail_price_withdisc_rub",
        "Комиссия WB, % актуальная", "Средняя СПП", "Эквайринг, % средний",
        "Логистика прямая", "Логистика обратная", "_reverse_events",
        "cost_price", "Реклама", "Рекламные_заказы", "Рекламная_выручка",
        "Рекламные_показы", "Рекламные_клики"
    ]
    for c in num_fill_cols:
        if c in fact.columns:
            fact[c] = pd.to_numeric(fact[c], errors="coerce").fillna(0)

    fact["Оценка заказанных, шт"] = fact["Продажи, шт"] + fact["_reverse_events"]
    fact["Buyout rate"] = fact.apply(
        lambda r: safe_round(safe_div(r["Продажи, шт"], r["Оценка заказанных, шт"], 6), 6)
        if r["Оценка заказанных, шт"] > 0 else 0.0,
        axis=1
    )

    fact["Себестоимость"] = fact["Net units"] * fact["cost_price"]

    storage_alloc = calc_storage_allocation(stocks_df, storage_total_store, week_start, week_end)
    fact = fact.merge(storage_alloc, on="nm_id", how="left")
    if "Хранение" not in fact.columns:
        fact["Хранение"] = 0.0
    fact["Хранение"] = pd.to_numeric(fact["Хранение"], errors="coerce").fillna(0)

    total_units_sold_store = fact["Продажи, шт"].sum()
    acceptance_per_unit = safe_div(acceptance_total_store, total_units_sold_store, 6)
    penalty_per_unit = safe_div(penalty_total_store, total_units_sold_store, 6)
    deduction_per_unit = safe_div(deduction_total_store, total_units_sold_store, 6)

    fact["Приёмка"] = fact["Продажи, шт"] * acceptance_per_unit
    fact["Штрафы"] = fact["Продажи, шт"] * penalty_per_unit
    fact["Удержания"] = fact["Продажи, шт"] * deduction_per_unit

    fact["НДС"] = fact["Валовая выручка"].apply(lambda x: extract_vat_from_gross(x, VAT_RATE))

    fact["Валовая прибыль"] = (
        fact["Валовая выручка"]
        - fact["Себестоимость"]
        - fact["Комиссия WB"]
        - fact["Эквайринг"]
        - fact["Логистика прямая"]
        - fact["Логистика обратная"]
        - fact["Хранение"]
        - fact["Приёмка"]
        - fact["Штрафы"]
        - fact["Удержания"]
        - fact["Реклама"]
    )

    fact["Прибыль до налога"] = fact["Валовая прибыль"] - fact["НДС"]
    fact["Налог на прибыль"] = fact["Прибыль до налога"].apply(
        lambda x: max(0.0, x) * PROFIT_TAX_RATE / 100.0
    )
    fact["Чистая прибыль"] = fact["Прибыль до налога"] - fact["Налог на прибыль"]

    fact["Валовая маржа, %"] = fact.apply(
        lambda r: safe_round(safe_div(r["Валовая прибыль"] * 100.0, r["Валовая выручка"], 6), 6)
        if r["Валовая выручка"] else 0.0,
        axis=1
    )
    fact["Чистая маржа, %"] = fact.apply(
        lambda r: safe_round(safe_div(r["Чистая прибыль"] * 100.0, r["Валовая выручка"], 6), 6)
        if r["Валовая выручка"] else 0.0,
        axis=1
    )

    unit = fact.copy()
    unit["Неделя"] = week_label(week_start)
    unit["Комиссия WB, руб/ед"] = unit.apply(lambda r: safe_div(r["Комиссия WB"], r["Net units"], 6), axis=1)
    unit["Эквайринг, руб/ед"] = unit.apply(lambda r: safe_div(r["Эквайринг"], r["Net units"], 6), axis=1)
    unit["Прямая логистика, руб/ед"] = unit.apply(lambda r: safe_div(r["Логистика прямая"], r["Net units"], 6), axis=1)
    unit["Обратная логистика, руб/ед"] = unit.apply(lambda r: safe_div(r["Логистика обратная"], r["Net units"], 6), axis=1)
    unit["Хранение, руб/ед"] = unit.apply(lambda r: safe_div(r["Хранение"], r["Net units"], 6), axis=1)
    unit["Приёмка, руб/ед"] = unit.apply(lambda r: safe_div(r["Приёмка"], r["Net units"], 6), axis=1)
    unit["Штрафы и удержания, руб/ед"] = unit.apply(lambda r: safe_div(r["Штрафы"] + r["Удержания"], r["Net units"], 6), axis=1)
    unit["Реклама, руб/ед"] = unit.apply(lambda r: safe_div(r["Реклама"], r["Net units"], 6), axis=1)
    unit["Себестоимость, руб/ед"] = unit["cost_price"]
    unit["Валовая прибыль, руб/ед"] = unit.apply(lambda r: safe_div(r["Валовая прибыль"], r["Net units"], 6), axis=1)
    unit["Чистая прибыль, руб/ед"] = unit.apply(lambda r: safe_div(r["Чистая прибыль"], r["Net units"], 6), axis=1)

    unit = unit[[
        "Неделя", "nm_id", "sa_name", "subject_name", "brand_name",
        "Продажи, шт", "Возвраты, шт", "Net units", "Buyout rate",
        "Средняя retail_amount", "Средняя retail_price_withdisc_rub", "Средняя СПП",
        "Комиссия WB, % актуальная", "Эквайринг, % средний",
        "Комиссия WB, руб/ед", "Эквайринг, руб/ед",
        "Прямая логистика, руб/ед", "Обратная логистика, руб/ед",
        "Хранение, руб/ед", "Приёмка, руб/ед",
        "Штрафы и удержания, руб/ед", "Реклама, руб/ед",
        "Себестоимость, руб/ед",
        "Валовая прибыль, руб/ед", "Чистая прибыль, руб/ед",
        "Валовая маржа, %", "Чистая маржа, %"
    ]].copy()

    fact["Неделя"] = week_label(week_start)
    fact["VAT_RATE"] = VAT_RATE
    fact["PROFIT_TAX_RATE"] = PROFIT_TAX_RATE

    fact = fact[[
        "Неделя", "nm_id", "sa_name", "subject_name", "brand_name",
        "Продажи, шт", "Возвраты, шт", "Net units", "Buyout rate",
        "Валовая выручка", "Средняя retail_amount", "Средняя retail_price_withdisc_rub", "Средняя СПП",
        "Комиссия WB", "Комиссия WB, % актуальная",
        "Эквайринг", "Эквайринг, % средний",
        "Логистика прямая", "Логистика обратная",
        "Хранение", "Приёмка", "Штрафы", "Удержания",
        "Реклама", "Рекламные_заказы", "Рекламная_выручка", "Рекламные_показы", "Рекламные_клики",
        "cost_price", "Себестоимость",
        "Валовая прибыль", "НДС", "Прибыль до налога", "Налог на прибыль", "Чистая прибыль",
        "Валовая маржа, %", "Чистая маржа, %", "VAT_RATE", "PROFIT_TAX_RATE"
    ]].copy()

    warehouse_df = calc_warehouse_analysis(logistic_rows, week_start)
    return fact, unit, warehouse_df


# =========================================================
# ACCEPTANCE NORM 9W
# =========================================================

def calc_acceptance_per_unit_9w(s3: S3Storage, last_week_start: datetime.date) -> float:
    weeks = build_week_list(last_week_start, ACCEPTANCE_LOOKBACK_WEEKS)
    total_acceptance = 0.0
    total_units_sold = 0.0

    for ws in weeks:
        df = read_finance_week(s3, ws)
        if df.empty:
            continue

        df = build_finance_rows(df)
        signed_rows = df[df["sign"] != 0].copy()
        sales = signed_rows[signed_rows["sign"] == 1]
        units = pd.to_numeric(sales["quantity"], errors="coerce").fillna(0).sum()

        total_units_sold += units
        total_acceptance += abs(pd.to_numeric(df.get("acceptance", 0), errors="coerce").fillna(0).sum())

    return safe_div(total_acceptance, total_units_sold, 6)


def apply_acceptance_norm_to_unit(unit_df: pd.DataFrame, acceptance_per_unit_9w: float) -> pd.DataFrame:
    if unit_df.empty:
        return unit_df

    df = unit_df.copy()
    old_acceptance = df["Приёмка, руб/ед"].copy()
    df["Приёмка, руб/ед"] = acceptance_per_unit_9w

    delta = df["Приёмка, руб/ед"] - old_acceptance
    df["Валовая прибыль, руб/ед"] = df["Валовая прибыль, руб/ед"] - delta
    df["Чистая прибыль, руб/ед"] = df["Чистая прибыль, руб/ед"] - delta
    return df


# =========================================================
# ANALYSIS
# =========================================================

def explain_store_change(cur_store: Dict[str, float], prev_store: Dict[str, float]) -> str:
    deltas = {
        "выручка": cur_store["Валовая выручка"] - prev_store["Валовая выручка"],
        "реклама": cur_store["Реклама"] - prev_store["Реклама"],
        "комиссия": cur_store["Комиссия WB"] - prev_store["Комиссия WB"],
        "логистика": (cur_store["Логистика прямая"] + cur_store["Логистика обратная"]) -
                     (prev_store["Логистика прямая"] + prev_store["Логистика обратная"]),
        "себестоимость": cur_store["Себестоимость"] - prev_store["Себестоимость"],
        "хранение": cur_store["Хранение"] - prev_store["Хранение"],
        "приёмка": cur_store["Приёмка"] - prev_store["Приёмка"],
    }

    top_negative = max(deltas, key=lambda k: abs(deltas[k]))
    if deltas["выручка"] > 0 and cur_store["Чистая прибыль"] > prev_store["Чистая прибыль"]:
        return f"Прибыль выросла. Основной драйвер — выручка. Крупнейшее изменение среди статей: {top_negative} ({safe_round(deltas[top_negative], 2)})."
    if deltas["выручка"] < 0 and cur_store["Чистая прибыль"] < prev_store["Чистая прибыль"]:
        return f"Прибыль снизилась вместе с выручкой. Крупнейшее изменение среди статей: {top_negative} ({safe_round(deltas[top_negative], 2)})."
    if cur_store["Реклама"] > prev_store["Реклама"] and cur_store["Валовая выручка"] <= prev_store["Валовая выручка"]:
        return "Прибыль снизилась: рекламные расходы выросли быстрее выручки."
    if (cur_store["Логистика прямая"] + cur_store["Логистика обратная"]) > (prev_store["Логистика прямая"] + prev_store["Логистика обратная"]):
        return "Прибыль снизилась: выросли логистические расходы."
    return f"Изменение прибыли смешанное. Наиболее сильное влияние оказала статья: {top_negative}."


def explain_sku_change(row) -> str:
    delta_profit = row["Δ Чистая прибыль"]
    delta_revenue = row["Δ Валовая выручка"]
    delta_ads = row["Δ Реклама"]
    delta_comm = row["Δ Комиссия WB"]
    delta_log = row["Δ Логистика"]
    delta_spp = row["Δ СПП"]
    delta_price = row["Δ вашей цены"]

    reasons = []

    if delta_revenue > 0 and delta_profit > 0:
        reasons.append("рост выручки")
    if delta_revenue < 0 and delta_profit < 0:
        reasons.append("снижение выручки")
    if delta_ads > 0 and delta_profit < 0:
        reasons.append("рост рекламных расходов")
    if delta_comm > 0 and delta_profit < 0:
        reasons.append("рост комиссии WB")
    if delta_log > 0 and delta_profit < 0:
        reasons.append("рост логистики")
    if delta_price > 0:
        reasons.append("рост вашей цены")
    if delta_price < 0:
        reasons.append("снижение вашей цены")
    if delta_spp > 0:
        reasons.append("рост СПП WB")
    if delta_spp < 0:
        reasons.append("снижение СПП WB")

    if not reasons:
        if delta_profit >= 0:
            return "Положительная динамика без явного одного драйвера."
        return "Негативная динамика без явного одного драйвера."

    return "; ".join(reasons[:3])


def build_week_to_week_analysis(current_fact: pd.DataFrame, prev_fact: pd.DataFrame) -> pd.DataFrame:
    rows = []

    if current_fact.empty:
        return pd.DataFrame()

    cur = current_fact.copy()
    prev = prev_fact.copy() if prev_fact is not None else pd.DataFrame()
    current_week = cur["Неделя"].iloc[0]

    summary_cols = [
        "Валовая выручка", "Комиссия WB", "Эквайринг", "Логистика прямая",
        "Логистика обратная", "Хранение", "Приёмка", "Штрафы", "Удержания",
        "Реклама", "Себестоимость", "Валовая прибыль", "НДС", "Налог на прибыль", "Чистая прибыль"
    ]

    cur_store = {c: cur[c].sum() for c in summary_cols}
    if not prev.empty:
        prev_store = {c: prev[c].sum() for c in summary_cols}
    else:
        prev_store = {c: 0.0 for c in summary_cols}

    store_delta_profit = cur_store["Чистая прибыль"] - prev_store["Чистая прибыль"]
    store_reason = explain_store_change(cur_store, prev_store)

    rows.append({
        "section": "summary_store",
        "Неделя": current_week,
        "nm_id": "",
        "Артикул продавца": "",
        "Предмет": "",
        "Показатель": "Итог по магазину",
        "Чистая прибыль_тек": safe_round(cur_store["Чистая прибыль"], 6),
        "Чистая прибыль_пред": safe_round(prev_store["Чистая прибыль"], 6),
        "Δ Чистая прибыль": safe_round(store_delta_profit, 6),
        "Валовая выручка_тек": safe_round(cur_store["Валовая выручка"], 6),
        "Валовая выручка_пред": safe_round(prev_store["Валовая выручка"], 6),
        "Δ Реклама": safe_round(cur_store["Реклама"] - prev_store["Реклама"], 6),
        "Δ Комиссия WB": safe_round(cur_store["Комиссия WB"] - prev_store["Комиссия WB"], 6),
        "Δ Логистика": safe_round(
            (cur_store["Логистика прямая"] + cur_store["Логистика обратная"]) -
            (prev_store["Логистика прямая"] + prev_store["Логистика обратная"]), 6
        ),
        "Δ СПП": "",
        "Δ вашей цены": "",
        "Комментарий": store_reason
    })

    if prev.empty:
        return pd.DataFrame(rows)

    merge_cols = [
        "nm_id", "sa_name", "subject_name",
        "Чистая прибыль", "Валовая прибыль", "Валовая выручка", "Реклама", "Комиссия WB",
        "Логистика прямая", "Логистика обратная", "Средняя СПП", "Средняя retail_price_withdisc_rub",
        "Средняя retail_amount", "Продажи, шт", "Buyout rate"
    ]

    left = cur[merge_cols].copy()
    right = prev[merge_cols].copy()
    merged = left.merge(right, on="nm_id", how="outer", suffixes=("_cur", "_prev")).fillna(0)

    merged["Δ Чистая прибыль"] = merged["Чистая прибыль_cur"] - merged["Чистая прибыль_prev"]
    merged["Δ Валовая выручка"] = merged["Валовая выручка_cur"] - merged["Валовая выручка_prev"]
    merged["Δ Реклама"] = merged["Реклама_cur"] - merged["Реклама_prev"]
    merged["Δ Комиссия WB"] = merged["Комиссия WB_cur"] - merged["Комиссия WB_prev"]
    merged["Δ Логистика"] = (
        (merged["Логистика прямая_cur"] + merged["Логистика обратная_cur"]) -
        (merged["Логистика прямая_prev"] + merged["Логистика обратная_prev"])
    )
    merged["Δ СПП"] = merged["Средняя СПП_cur"] - merged["Средняя СПП_prev"]
    merged["Δ вашей цены"] = merged["Средняя retail_price_withdisc_rub_cur"] - merged["Средняя retail_price_withdisc_rub_prev"]
    merged["Комментарий"] = merged.apply(explain_sku_change, axis=1)

    gainers = merged.sort_values("Δ Чистая прибыль", ascending=False).head(20)
    losers = merged.sort_values("Δ Чистая прибыль", ascending=True).head(20)

    for _, r in gainers.iterrows():
        rows.append({
            "section": "top_gainers",
            "Неделя": current_week,
            "nm_id": int(r["nm_id"]) if pd.notna(r["nm_id"]) else "",
            "Артикул продавца": r.get("sa_name_cur") or r.get("sa_name_prev") or "",
            "Предмет": r.get("subject_name_cur") or r.get("subject_name_prev") or "",
            "Показатель": "Рост прибыли",
            "Чистая прибыль_тек": safe_round(r["Чистая прибыль_cur"], 6),
            "Чистая прибыль_пред": safe_round(r["Чистая прибыль_prev"], 6),
            "Δ Чистая прибыль": safe_round(r["Δ Чистая прибыль"], 6),
            "Валовая выручка_тек": safe_round(r["Валовая выручка_cur"], 6),
            "Валовая выручка_пред": safe_round(r["Валовая выручка_prev"], 6),
            "Δ Реклама": safe_round(r["Δ Реклама"], 6),
            "Δ Комиссия WB": safe_round(r["Δ Комиссия WB"], 6),
            "Δ Логистика": safe_round(r["Δ Логистика"], 6),
            "Δ СПП": safe_round(r["Δ СПП"], 6),
            "Δ вашей цены": safe_round(r["Δ вашей цены"], 6),
            "Комментарий": r["Комментарий"]
        })

    for _, r in losers.iterrows():
        rows.append({
            "section": "top_losers",
            "Неделя": current_week,
            "nm_id": int(r["nm_id"]) if pd.notna(r["nm_id"]) else "",
            "Артикул продавца": r.get("sa_name_cur") or r.get("sa_name_prev") or "",
            "Предмет": r.get("subject_name_cur") or r.get("subject_name_prev") or "",
            "Показатель": "Падение прибыли",
            "Чистая прибыль_тек": safe_round(r["Чистая прибыль_cur"], 6),
            "Чистая прибыль_пред": safe_round(r["Чистая прибыль_prev"], 6),
            "Δ Чистая прибыль": safe_round(r["Δ Чистая прибыль"], 6),
            "Валовая выручка_тек": safe_round(r["Валовая выручка_cur"], 6),
            "Валовая выручка_пред": safe_round(r["Валовая выручка_prev"], 6),
            "Δ Реклама": safe_round(r["Δ Реклама"], 6),
            "Δ Комиссия WB": safe_round(r["Δ Комиссия WB"], 6),
            "Δ Логистика": safe_round(r["Δ Логистика"], 6),
            "Δ СПП": safe_round(r["Δ СПП"], 6),
            "Δ вашей цены": safe_round(r["Δ вашей цены"], 6),
            "Комментарий": r["Комментарий"]
        })

    return pd.DataFrame(rows)


# =========================================================
# HISTORY / RETENTION
# =========================================================

def append_with_retention(
    existing_df: pd.DataFrame,
    new_df: pd.DataFrame,
    key_cols: List[str],
    retention_weeks: int = RETENTION_WEEKS
) -> pd.DataFrame:
    if new_df is None or new_df.empty:
        return existing_df if existing_df is not None else pd.DataFrame()

    if existing_df is None or existing_df.empty:
        combined = new_df.copy()
    else:
        combined = pd.concat([existing_df, new_df], ignore_index=True)

    if "Неделя" in combined.columns:
        combined = combined.drop_duplicates(subset=key_cols, keep="last")
        weeks_sorted = sorted([w for w in combined["Неделя"].dropna().astype(str).unique()])
        if len(weeks_sorted) > retention_weeks:
            keep_weeks = set(weeks_sorted[-retention_weeks:])
            combined = combined[combined["Неделя"].astype(str).isin(keep_weeks)].copy()

    return combined.reset_index(drop=True)


def load_existing_economics(s3: S3Storage) -> Dict[str, pd.DataFrame]:
    if not s3.file_exists(ECONOMICS_KEY):
        return {
            WEEK_SHEET_UNIT: pd.DataFrame(),
            WEEK_SHEET_FACT: pd.DataFrame(),
            WEEK_SHEET_ANALYSIS: pd.DataFrame(),
            WEEK_SHEET_WAREHOUSES: pd.DataFrame(),
        }

    try:
        sheets = s3.read_excel_all_sheets(ECONOMICS_KEY)
        return {
            WEEK_SHEET_UNIT: sheets.get(WEEK_SHEET_UNIT, pd.DataFrame()),
            WEEK_SHEET_FACT: sheets.get(WEEK_SHEET_FACT, pd.DataFrame()),
            WEEK_SHEET_ANALYSIS: sheets.get(WEEK_SHEET_ANALYSIS, pd.DataFrame()),
            WEEK_SHEET_WAREHOUSES: sheets.get(WEEK_SHEET_WAREHOUSES, pd.DataFrame()),
        }
    except Exception as e:
        log(f"⚠️ Не удалось прочитать существующий Экономика.xlsx: {e}")
        return {
            WEEK_SHEET_UNIT: pd.DataFrame(),
            WEEK_SHEET_FACT: pd.DataFrame(),
            WEEK_SHEET_ANALYSIS: pd.DataFrame(),
            WEEK_SHEET_WAREHOUSES: pd.DataFrame(),
        }


# =========================================================
# MAIN CALCULATOR
# =========================================================

class WeeklyEconomicsCalculator:
    def __init__(self, s3: S3Storage):
        self.s3 = s3

    def run(self):
        week_start, week_end = get_last_full_week_range()
        prev_week_start = week_start - timedelta(days=7)
        prev_week_end = week_end - timedelta(days=7)

        current_week_code = week_label(week_start)

        log("=" * 80)
        log(f"📌 Запуск weekly-экономики для магазина {STORE_NAME}")
        log(f"📅 Целевая неделя: {current_week_code}")
        log("=" * 80)

        fin_df = read_finance_week(self.s3, week_start)
        if fin_df.empty:
            raise RuntimeError(f"Нет финансовых данных за неделю {current_week_code}")

        stocks_df = read_stocks_week(self.s3, week_start)
        advert_df = read_advert_week(self.s3, week_start, week_end)
        cost_df = read_costs(self.s3)

        fact_df, unit_df, warehouse_df = calc_weekly_facts(
            fin_df=fin_df,
            advert_df=advert_df,
            cost_df=cost_df,
            stocks_df=stocks_df,
            week_start=week_start,
            week_end=week_end,
        )

        if fact_df.empty:
            raise RuntimeError("Не удалось сформировать weekly fact")

        acceptance_norm = calc_acceptance_per_unit_9w(self.s3, week_start)
        log(f"📦 Норматив приёмки за {ACCEPTANCE_LOOKBACK_WEEKS} недель: {acceptance_norm:.6f} руб/ед")

        unit_df = apply_acceptance_norm_to_unit(unit_df, acceptance_norm)

        prev_fin_df = read_finance_week(self.s3, prev_week_start)
        if not prev_fin_df.empty:
            prev_stocks_df = read_stocks_week(self.s3, prev_week_start)
            prev_advert_df = read_advert_week(self.s3, prev_week_start, prev_week_end)
            prev_fact_df, _, _ = calc_weekly_facts(
                fin_df=prev_fin_df,
                advert_df=prev_advert_df,
                cost_df=cost_df,
                stocks_df=prev_stocks_df,
                week_start=prev_week_start,
                week_end=prev_week_end,
            )
        else:
            prev_fact_df = pd.DataFrame()

        analysis_df = build_week_to_week_analysis(fact_df, prev_fact_df)

        existing = load_existing_economics(self.s3)

        unit_all = append_with_retention(
            existing[WEEK_SHEET_UNIT],
            unit_df,
            key_cols=["Неделя", "nm_id"],
            retention_weeks=RETENTION_WEEKS
        )

        fact_all = append_with_retention(
            existing[WEEK_SHEET_FACT],
            fact_df,
            key_cols=["Неделя", "nm_id"],
            retention_weeks=RETENTION_WEEKS
        )

        analysis_all = append_with_retention(
            existing[WEEK_SHEET_ANALYSIS],
            analysis_df,
            key_cols=["Неделя", "section", "nm_id", "Показатель"],
            retention_weeks=RETENTION_WEEKS
        )

        warehouses_all = append_with_retention(
            existing[WEEK_SHEET_WAREHOUSES],
            warehouse_df,
            key_cols=["Неделя", "Склад"],
            retention_weeks=RETENTION_WEEKS
        )

        sheets_to_write = {
            WEEK_SHEET_UNIT: unit_all.sort_values(["Неделя", "Чистая прибыль, руб/ед"], ascending=[True, False]).reset_index(drop=True),
            WEEK_SHEET_FACT: fact_all.sort_values(["Неделя", "Чистая прибыль"], ascending=[True, False]).reset_index(drop=True),
            WEEK_SHEET_ANALYSIS: analysis_all.reset_index(drop=True),
            WEEK_SHEET_WAREHOUSES: warehouses_all.sort_values(["Неделя", "Переплата"], ascending=[True, False]).reset_index(drop=True),
        }

        self.s3.write_excel_sheets(ECONOMICS_KEY, sheets_to_write)

        total_revenue = fact_df["Валовая выручка"].sum()
        total_gp = fact_df["Валовая прибыль"].sum()
        total_net = fact_df["Чистая прибыль"].sum()

        log(f"✅ Экономика сохранена: {ECONOMICS_KEY}")
        log(f"📊 Выручка недели: {total_revenue:,.2f}")
        log(f"📊 Валовая прибыль недели: {total_gp:,.2f}")
        log(f"📊 Чистая прибыль недели: {total_net:,.2f}")


# =========================================================
# ENTRYPOINT
# =========================================================

def main():
    required_env = [
        "YC_ACCESS_KEY_ID",
        "YC_SECRET_ACCESS_KEY",
        "YC_BUCKET_NAME",
    ]
    missing = [var for var in required_env if not os.environ.get(var)]
    if missing:
        raise RuntimeError(f"Отсутствуют переменные окружения: {missing}")

    s3 = S3Storage(
        access_key=os.environ["YC_ACCESS_KEY_ID"],
        secret_key=os.environ["YC_SECRET_ACCESS_KEY"],
        bucket_name=os.environ["YC_BUCKET_NAME"],
    )

    calc = WeeklyEconomicsCalculator(s3)
    calc.run()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"❌ Критическая ошибка: {e}")
        traceback.print_exc()
        raise
