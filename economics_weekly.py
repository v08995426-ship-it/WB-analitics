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

МАГАЗИН = "TOPFACE"
ЧАСОВОЙ_ПОЯС = "Europe/Moscow"

СТАВКА_НДС = 7.0
СТАВКА_НАЛОГА_НА_ПРИБЫЛЬ = 15.0

МИН_КОЭФ_ЛОГИСТИКИ = 0.8
ПОРОГ_ДОРОГОГО_СКЛАДА = 1.6
ЦЕЛЕВОЙ_КОЭФ_ПЕРЕСЧЁТА = 1.4

НЕДЕЛЬ_ДЛЯ_ПРИЁМКИ = 9
НЕДЕЛЬ_ХРАНЕНИЯ_ИСТОРИИ = 13

ПУТЬ_ЭКОНОМИКА = f"Отчёты/Финансовые показатели/{МАГАЗИН}/Экономика.xlsx"
ПРЕФИКС_ФИНАНСЫ = f"Отчёты/Финансовые показатели/{МАГАЗИН}/Недельные/"
ПРЕФИКС_ОСТАТКИ = f"Отчёты/Остатки/{МАГАЗИН}/Недельные/"
ПУТЬ_РЕКЛАМА_АНАЛИЗ = f"Отчёты/Реклама/{МАГАЗИН}/Анализ рекламы.xlsx"
ПРЕФИКС_РЕКЛАМА_НЕДЕЛЬНЫЕ = f"Отчёты/Реклама/{МАГАЗИН}/Недельные/"
ПУТЬ_СЕБЕСТОИМОСТЬ = "Отчёты/Себестоимость/Себестоимость.xlsx"

ЛИСТ_ЮНИТ = "Юнит экономика"
ЛИСТ_ФАКТ = "Общий факт за неделю"
ЛИСТ_АНАЛИЗ = "Анализ неделя к неделе"
ЛИСТ_СКЛАДЫ = "Склады_Коэффициенты"

ОПЕРАЦИИ_ПРОДАЖА = {
    "Продажа",
    "Компенсация ущерба",
    "Добровольная компенсация при возврате",
}

ОПЕРАЦИИ_ВОЗВРАТ = {
    "Возврат",
}

ПОДСКАЗКИ_ПРЯМАЯ_ЛОГИСТИКА = [
    "к клиенту при продаже",
    "к клиенту",
]

ПОДСКАЗКИ_ОБРАТНАЯ_ЛОГИСТИКА = [
    "от клиента при отмене",
    "от клиента при возврате",
    "к клиенту при отмене",
    "возврат товара",
    "возврат",
    "от клиента",
]


# =========================================================
# ОБЩИЕ ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =========================================================

def лог(msg: str):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def сейчас_мск():
    return datetime.now(pytz.timezone(ЧАСОВОЙ_ПОЯС))


def безопасное_округление(x, знаков=6):
    try:
        if pd.isna(x):
            return 0.0
        return round(float(x), знаков)
    except Exception:
        return 0.0


def деление(a, b, знаков=6):
    try:
        a = float(a)
        b = float(b)
        if b == 0:
            return 0.0
        return round(a / b, знаков)
    except Exception:
        return 0.0


def текст(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip().lower()


def дата_в_неделю(dt: datetime.date) -> str:
    year, week_num, _ = dt.isocalendar()
    return f"{year}-W{week_num:02d}"


def получить_последнюю_полную_неделю() -> Tuple[datetime.date, datetime.date]:
    today = сейчас_мск().date()
    monday_current = today - timedelta(days=today.weekday())
    end_prev = monday_current - timedelta(days=1)
    start_prev = end_prev - timedelta(days=6)
    return start_prev, end_prev


def ключ_финансы(week_start: datetime.date) -> str:
    return f"{ПРЕФИКС_ФИНАНСЫ}Финансовые показатели_{дата_в_неделю(week_start)}.xlsx"


def ключ_остатки(week_start: datetime.date) -> str:
    return f"{ПРЕФИКС_ОСТАТКИ}Остатки_{дата_в_неделю(week_start)}.xlsx"


def ключ_реклама_недельный(week_start: datetime.date) -> str:
    return f"{ПРЕФИКС_РЕКЛАМА_НЕДЕЛЬНЫЕ}Реклама_{дата_в_неделю(week_start)}.xlsx"


def в_дату(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.date


def в_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def привести_к_числам(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


def мода_или_последнее(series: pd.Series):
    s = series.dropna().astype(str).str.strip()
    if s.empty:
        return ""
    m = s.mode()
    if not m.empty:
        return m.iloc[0]
    return s.iloc[-1]


def выделить_ндс_из_цены(сумма_с_ндс: float, ставка_ндс: float) -> float:
    return безопасное_округление(сумма_с_ндс * ставка_ндс / (100.0 + ставка_ндс), 6)


def знак_операции(тип_документа: str, обоснование: str) -> int:
    doc = текст(тип_документа)
    oper = текст(обоснование)

    if обоснование in ОПЕРАЦИИ_ПРОДАЖА or doc == "продажа":
        return 1
    if обоснование in ОПЕРАЦИИ_ВОЗВРАТ or doc == "возврат":
        return -1
    if oper in {x.lower() for x in ОПЕРАЦИИ_ПРОДАЖА}:
        return 1
    if oper in {x.lower() for x in ОПЕРАЦИИ_ВОЗВРАТ}:
        return -1

    return 0


def тип_логистики(row) -> str:
    обоснование = текст(row.get("supplier_oper_name", ""))
    вид = текст(row.get("bonus_type_name", ""))
    delivery_amount = float(row.get("delivery_amount", 0) or 0)
    return_amount = float(row.get("return_amount", 0) or 0)

    if обоснование != "логистика":
        return "нет"

    for hint in ПОДСКАЗКИ_ОБРАТНАЯ_ЛОГИСТИКА:
        if hint in вид:
            return "обратная"

    for hint in ПОДСКАЗКИ_ПРЯМАЯ_ЛОГИСТИКА:
        if hint in вид:
            return "прямая"

    if return_amount > 0:
        return "обратная"
    if delivery_amount > 0:
        return "прямая"

    return "прямая"


def список_недель(last_week_start: datetime.date, count_weeks: int) -> List[datetime.date]:
    weeks = []
    current = last_week_start
    for _ in range(count_weeks):
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
# СЕБЕСТОИМОСТЬ
# =========================================================

def нормализовать_себестоимость(cost_df: pd.DataFrame) -> pd.DataFrame:
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

    варианты_nm = [
        "nm_id",
        "nmid",
        "артикул wb",
        "артикул вб",
        "wb article",
        "wb id",
        "код wb",
    ]

    for col, norm in normalized.items():
        if norm in варианты_nm:
            nm_col = col
            break

    if nm_col is None:
        for col, norm in normalized.items():
            if "артикул wb" in norm or "артикул вб" in norm:
                nm_col = col
                break

    варианты_cost = [
        "cost_price",
        "себестоимость",
        "стоимость",
        "cost",
        "cost price",
        "закупочная цена",
    ]

    for col, norm in normalized.items():
        if norm in варианты_cost:
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
        лог(f"⚠️ Не удалось определить колонки в Себестоимости. Колонки: {original_columns}")
        return pd.DataFrame(columns=["nm_id", "cost_price"])

    df = df.rename(columns={nm_col: "nm_id", cost_col: "cost_price"})
    df["nm_id"] = pd.to_numeric(df["nm_id"], errors="coerce")
    df["cost_price"] = pd.to_numeric(df["cost_price"], errors="coerce")

    df = df[["nm_id", "cost_price"]].dropna(subset=["nm_id"])
    df["nm_id"] = df["nm_id"].astype("int64")
    df["cost_price"] = df["cost_price"].fillna(0.0)

    return df.drop_duplicates(subset="nm_id", keep="last")


# =========================================================
# ЧТЕНИЕ ИСХОДНИКОВ
# =========================================================

def прочитать_финансы_недели(s3: S3Storage, week_start: datetime.date) -> pd.DataFrame:
    key = ключ_финансы(week_start)
    if not s3.file_exists(key):
        лог(f"⚠️ Не найден фин. отчёт: {key}")
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
    df = привести_к_числам(df, numeric_cols)

    if "rr_dt" in df.columns:
        df["rr_dt"] = в_дату(df["rr_dt"])
    if "sale_dt" in df.columns:
        df["sale_dt"] = в_datetime(df["sale_dt"])
    if "order_dt" in df.columns:
        df["order_dt"] = в_datetime(df["order_dt"])
    if "nm_id" in df.columns:
        df["nm_id"] = pd.to_numeric(df["nm_id"], errors="coerce")

    return df


def прочитать_остатки_недели(s3: S3Storage, week_start: datetime.date) -> pd.DataFrame:
    key = ключ_остатки(week_start)
    if not s3.file_exists(key):
        лог(f"⚠️ Не найден отчёт остатков: {key}")
        return pd.DataFrame()

    df = s3.read_excel(key, sheet_name=0)
    if df.empty:
        return df

    if "Дата сбора" in df.columns:
        df["Дата сбора"] = в_дату(df["Дата сбора"])
    elif "Дата запроса" in df.columns:
        df["Дата сбора"] = в_дату(df["Дата запроса"])

    df = привести_к_числам(df, ["Артикул WB", "Доступно для продажи", "Полное количество"])
    if "Артикул WB" in df.columns:
        df["Артикул WB"] = pd.to_numeric(df["Артикул WB"], errors="coerce")

    return df


def привести_колонки_рекламы(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    rename_map = {}
    columns_norm = {c: str(c).strip().lower() for c in df.columns}

    for col, norm in columns_norm.items():
        if norm in ["артикул wb", "артикул", "nm_id", "nmid"]:
            rename_map[col] = "Артикул WB"
        elif norm in ["дата", "day"]:
            rename_map[col] = "Дата"
        elif norm in ["расход", "затраты", "spent"]:
            rename_map[col] = "Расход"
        elif norm in ["сумма заказов", "выручка", "orders_sum"]:
            rename_map[col] = "Сумма заказов"
        elif norm in ["показы", "просмотры", "views"]:
            rename_map[col] = "Показы"
        elif norm in ["клики", "clicks"]:
            rename_map[col] = "Клики"
        elif norm in ["заказы", "orders"]:
            rename_map[col] = "Заказы"

    df = df.rename(columns=rename_map)
    return df


def прочитать_рекламу_недели(s3: S3Storage, week_start: datetime.date, week_end: datetime.date) -> pd.DataFrame:
    try:
        sheets = s3.read_excel_all_sheets(ПУТЬ_РЕКЛАМА_АНАЛИЗ)
        if "Статистика_Ежедневно" in sheets:
            df = sheets["Статистика_Ежедневно"].copy()
        else:
            df = next(iter(sheets.values())).copy()

        df = привести_колонки_рекламы(df)

        if not df.empty and "Дата" in df.columns and "Артикул WB" in df.columns:
            df["Дата"] = в_дату(df["Дата"])
            df = привести_к_числам(df, ["Артикул WB", "Расход", "Сумма заказ
