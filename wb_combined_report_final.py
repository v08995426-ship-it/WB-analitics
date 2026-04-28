
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import calendar
import io
import math
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import boto3
import numpy as np
import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

# =========================
# Logging
# =========================

def log(message: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {message}", flush=True)


# =========================
# Constants
# =========================

TARGET_SUBJECTS = [
    "Кисти косметические",
    "Помады",
    "Блески",
    "Косметические карандаши",
]

EXCLUDED_ARTICLES = {
    "CZ420", "CZ420БРОВИ", "CZ420ГЛАЗА",
    "DE49", "DE49ГЛАЗА",
    "PT901",
}

TITLE_FILL = PatternFill("solid", fgColor="D9EAF7")
HEADER_FILL = PatternFill("solid", fgColor="EAF2F8")
SECTION_FILL = PatternFill("solid", fgColor="F8F9FA")
TOTAL_FILL = PatternFill("solid", fgColor="FFF2CC")
THIN = Side(style="thin", color="C0C0C0")
BORDER = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)

MONEY_FMT = '# ##0" р."'
INT_FMT = '# ##0'
PCT_FMT = '0.00%'

# =========================
# Helpers
# =========================

def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("\xa0", " ").strip()
    text = re.sub(r"\s+", " ", text)
    return text

def norm_key(value: Any) -> str:
    text = normalize_text(value).lower()
    text = text.replace("ё", "е")
    text = re.sub(r"[^\w]+", " ", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def clean_article(value: Any) -> str:
    text = normalize_text(value)
    if not text or text.lower() in {"nan", "none"}:
        return ""
    return text

def article_upper(value: Any) -> str:
    return clean_article(value).upper()

def normalize_supplier_article(value: Any) -> str:
    art = clean_article(value)
    if not art:
        return ""
    up = art.upper()

    # exact excluded tokens
    if up in EXCLUDED_ARTICLES:
        return ""

    # pt901 variants should belong to 901 group, but exact PT901 excluded above
    if re.match(r"^PT901[\./].+", up):
        return art.lower()  # keep specific sku, code extractor will map to 901

    return art

def extract_code(supplier_article: Any) -> str:
    art = clean_article(supplier_article)
    if not art:
        return ""
    up = art.upper()

    # pt901.f25 / PT901.F25 => 901
    m = re.search(r"(\d+)", up)
    if m:
        return m.group(1).lstrip("0") or "0"
    return up

def to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")

def to_dt(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")

def safe_div(a: Any, b: Any) -> float:
    try:
        a = float(a)
        b = float(b)
    except Exception:
        return np.nan
    if b == 0 or math.isnan(b):
        return np.nan
    return a / b

def week_code_from_date(dt_value: Any) -> Optional[str]:
    if pd.isna(dt_value):
        return None
    ts = pd.Timestamp(dt_value)
    iso = ts.isocalendar()
    return f"{int(iso.year)}-W{int(iso.week):02d}"

def week_bounds_from_code(week_code: str) -> Tuple[Optional[date], Optional[date]]:
    m = re.match(r"^(\d{4})-W(\d{2})$", str(week_code))
    if not m:
        return None, None
    year = int(m.group(1))
    week = int(m.group(2))
    start = date.fromisocalendar(year, week, 1)
    end = date.fromisocalendar(year, week, 7)
    return start, end

def parse_week_code_from_name(name: str) -> Optional[str]:
    m = re.search(r"(\d{4})-W(\d{2})", name)
    if not m:
        return None
    return f"{m.group(1)}-W{m.group(2)}"

def parse_abc_period_from_name(name: str) -> Tuple[Optional[date], Optional[date]]:
    m = re.search(r"__(\d{2})\.(\d{2})\.(\d{4})-(\d{2})\.(\d{2})\.(\d{4})__", name)
    if not m:
        return None, None
    start = date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    end = date(int(m.group(6)), int(m.group(5)), int(m.group(4)))
    return start, end

def dedupe_columns(cols: Iterable[Any]) -> List[str]:
    out: List[str] = []
    counts: Dict[str, int] = {}
    for c in cols:
        base = normalize_text(c) or "unnamed"
        counts[base] = counts.get(base, 0) + 1
        if counts[base] == 1:
            out.append(base)
        else:
            out.append(f"{base}__{counts[base]}")
    return out

def pick_best_sheet(sheet_names: List[str], preferred: Iterable[str]) -> Any:
    if not sheet_names:
        return 0
    norm_map = {norm_key(s): s for s in sheet_names}
    for name in preferred:
        k = norm_key(name)
        if k in norm_map:
            return norm_map[k]
    return sheet_names[0]

def required_score(columns: Iterable[Any], expected_aliases: Dict[str, List[str]]) -> int:
    norm_cols = {norm_key(c) for c in columns if normalize_text(c)}
    score = 0
    for aliases in expected_aliases.values():
        for alias in aliases:
            if norm_key(alias) in norm_cols:
                score += 1
                break
    return score

def read_excel_flexible(
    data: bytes,
    filename: str,
    preferred_sheets: Optional[Iterable[str]] = None,
    header_candidates: Iterable[int] = (0, 1, 2),
    expected_aliases: Optional[Dict[str, List[str]]] = None,
) -> Tuple[pd.DataFrame, str, int]:
    bio = io.BytesIO(data)
    xl = pd.ExcelFile(bio)
    sheet = pick_best_sheet(xl.sheet_names, preferred_sheets or [])
    best_df: Optional[pd.DataFrame] = None
    best_header = 0
    best_score = -10**9
    for header in header_candidates:
        try:
            df = xl.parse(sheet_name=sheet, header=header, dtype=object)
        except Exception:
            continue
        df = df.copy()
        df.columns = dedupe_columns(df.columns)
        df = df.dropna(axis=0, how="all").dropna(axis=1, how="all")
        if df.empty:
            score = -1000
        else:
            score = len([c for c in df.columns if normalize_text(c)])
            if expected_aliases:
                score += required_score(df.columns, expected_aliases) * 100
        if score > best_score:
            best_score = score
            best_df = df
            best_header = header
    if best_df is None:
        raise ValueError(f"Не удалось прочитать Excel: {filename}")
    best_df.columns = dedupe_columns(best_df.columns)
    return best_df, str(sheet), best_header

def rename_using_aliases(df: pd.DataFrame, alias_map: Dict[str, List[str]]) -> pd.DataFrame:
    norm_existing: Dict[str, str] = {}
    for col in df.columns:
        norm_existing.setdefault(norm_key(col), col)
    out = df.copy()
    for target, aliases in alias_map.items():
        chosen = None
        for alias in aliases:
            k = norm_key(alias)
            if k in norm_existing:
                chosen = norm_existing[k]
                break
        if chosen is None:
            out[target] = np.nan
        elif chosen != target:
            out[target] = out[chosen]
    return out

def russian_weekday_name(ts: pd.Timestamp) -> str:
    names = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    return names[int(ts.weekday())]

def month_label(period: pd.Timestamp) -> str:
    months = {
        1:"Янв",2:"Фев",3:"Мар",4:"Апр",5:"Май",6:"Июн",
        7:"Июл",8:"Авг",9:"Сен",10:"Окт",11:"Ноя",12:"Дек"
    }
    return f"{months[period.month]} {period.year}"

def week_label(start_dt: pd.Timestamp, end_dt: pd.Timestamp) -> str:
    return f"{start_dt.strftime('%d.%m')}-{end_dt.strftime('%d.%m')}"

def target_from_series(values: pd.Series) -> float:
    s = to_numeric(values).dropna()
    if s.empty:
        return 0.0
    med = s.median()
    strong = s[s >= med]
    if strong.empty:
        return float(s.mean())
    return float(strong.mean())

def ensure_columns(df: pd.DataFrame, cols: Dict[str, Any]) -> pd.DataFrame:
    out = df.copy()
    for c, default in cols.items():
        if c not in out.columns:
            out[c] = default
    return out

# =========================
# Storage
# =========================

class BaseStorage:
    def list_files(self, prefix: str) -> List[str]:
        raise NotImplementedError
    def read_bytes(self, path: str) -> bytes:
        raise NotImplementedError
    def write_bytes(self, path: str, data: bytes) -> None:
        raise NotImplementedError
    def exists(self, path: str) -> bool:
        raise NotImplementedError

class LocalStorage(BaseStorage):
    def __init__(self, root: str):
        self.root = Path(root)
    def _abs(self, rel_path: str) -> Path:
        return self.root / rel_path
    def list_files(self, prefix: str) -> List[str]:
        prefix = prefix.replace("\\", "/").rstrip("/")
        prefix_path = self._abs(prefix)
        base = prefix_path if prefix_path.exists() else prefix_path.parent
        if not base.exists():
            return []
        files = []
        for p in base.rglob("*"):
            if p.is_file():
                rel = str(p.relative_to(self.root)).replace("\\", "/")
                if rel.startswith(prefix):
                    files.append(rel)
        return sorted(files)
    def read_bytes(self, path: str) -> bytes:
        return self._abs(path).read_bytes()
    def write_bytes(self, path: str, data: bytes) -> None:
        out = self._abs(path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(data)
    def exists(self, path: str) -> bool:
        return self._abs(path).exists()

class S3Storage(BaseStorage):
    def __init__(self, bucket: str, access_key: str, secret_key: str):
        self.bucket = bucket
        self.s3 = boto3.client(
            "s3",
            endpoint_url="https://storage.yandexcloud.net",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
    def list_files(self, prefix: str) -> List[str]:
        files = []
        token = None
        while True:
            kwargs = {"Bucket": self.bucket, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            resp = self.s3.list_objects_v2(**kwargs)
            for item in resp.get("Contents", []):
                key = item["Key"]
                if not key.endswith("/"):
                    files.append(key)
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        return sorted(files)
    def read_bytes(self, path: str) -> bytes:
        obj = self.s3.get_object(Bucket=self.bucket, Key=path)
        return obj["Body"].read()
    def write_bytes(self, path: str, data: bytes) -> None:
        self.s3.put_object(Bucket=self.bucket, Key=path, Body=data)
    def exists(self, path: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=path)
            return True
        except Exception:
            return False

def make_storage(root: str) -> BaseStorage:
    bucket = os.getenv("YC_BUCKET_NAME", "").strip()
    access_key = os.getenv("YC_ACCESS_KEY_ID", "").strip()
    secret_key = os.getenv("YC_SECRET_ACCESS_KEY", "").strip()
    if bucket and access_key and secret_key:
        log("Using Yandex Object Storage (S3)")
        return S3Storage(bucket=bucket, access_key=access_key, secret_key=secret_key)
    log("Using local filesystem")
    return LocalStorage(root=root)

# =========================
# Aliases
# =========================

ALIASES = {
    "day": ["Дата", "dt", "date", "Дата заказа"],
    "week_code": ["Неделя"],
    "nm_id": ["Артикул WB", "Артикул ВБ", "nmID", "nmId"],
    "supplier_article": ["Артикул продавца", "supplierArticle", "Артикул WB продавца"],
    "subject": ["Предмет", "subject", "Название предмета"],
    "brand": ["Бренд", "brand"],
    "title": ["Название", "Название товара", "Товар"],
    "orders": ["Заказы", "Заказали", "orders", "ordersCount", "Кол-во продаж", "Чистые продажи, шт"],
    "gross_profit_unit": ["Валовая прибыль, руб/ед"],
    "gross_profit": ["Валовая прибыль"],
}

# =========================
# Loaded data
# =========================

@dataclass
class LoadedData:
    orders: pd.DataFrame
    economics: pd.DataFrame
    abc: pd.DataFrame

# =========================
# Loader
# =========================

class Loader:
    def __init__(self, storage: BaseStorage, store: str, reports_root: str = "Отчёты"):
        self.storage = storage
        self.store = store
        self.reports_root = reports_root.rstrip("/")

    def _prefix(self, *parts: str) -> str:
        return "/".join([self.reports_root, *parts]).replace("//", "/")

    def _list_xlsx(self, prefix: str) -> List[str]:
        return [p for p in self.storage.list_files(prefix) if p.lower().endswith(".xlsx") and "/~$" not in p]

    def load_orders(self) -> pd.DataFrame:
        log("Этап 1.1. Загружаю заказы")
        files = self._list_xlsx(self._prefix("Заказы", self.store, "Недельные"))
        if not files:
            files = self._list_xlsx(self._prefix("Заказы", self.store))
        dfs = []
        for path in files:
            try:
                raw, _, _ = read_excel_flexible(
                    self.storage.read_bytes(path),
                    path,
                    preferred_sheets=None,
                    header_candidates=(0, 1, 2),
                    expected_aliases={
                        "day": ALIASES["day"],
                        "supplier_article": ALIASES["supplier_article"],
                        "nm_id": ALIASES["nm_id"],
                        "subject": ALIASES["subject"],
                    },
                )
                df = rename_using_aliases(raw, {
                    **ALIASES,
                    "warehouse": ["Склад", "warehouseName"],
                    "finished_price": ["finishedPrice", "Ср. цена продажи", "Цена с учетом всех скидок, кроме суммы по WB Кошельку"],
                    "price_with_disc": ["priceWithDisc", "Цена со скидкой продавца, в том числе со скидкой WB Клуба"],
                    "spp": ["SPP", "СПП", "Скидка WB, %"],
                    "is_cancel": ["isCancel", "Отмена заказа"],
                })
                df["day"] = to_dt(df["day"]).dt.normalize()
                df["supplier_article"] = df["supplier_article"].map(normalize_supplier_article)
                df["nm_id"] = to_numeric(df["nm_id"])
                df["subject"] = df["subject"].map(normalize_text)
                df["brand"] = df["brand"].map(normalize_text)
                df["warehouse"] = df["warehouse"].map(normalize_text)
                df["finished_price"] = to_numeric(df["finished_price"])
                df["price_with_disc"] = to_numeric(df["price_with_disc"])
                df["spp"] = to_numeric(df["spp"])
                if "is_cancel" in df.columns:
                    df["is_cancel"] = df["is_cancel"].fillna(False).astype(str).str.lower().isin({"true", "1"})
                else:
                    df["is_cancel"] = False
                df = df[df["day"].notna()].copy()
                df["code"] = df["supplier_article"].map(extract_code)
                dfs.append(df[["day","supplier_article","nm_id","subject","brand","warehouse","finished_price","price_with_disc","spp","is_cancel","code"]])
            except Exception as e:
                log(f"WARN: orders read error {path}: {e}")
        if not dfs:
            return pd.DataFrame(columns=["day","supplier_article","nm_id","subject","brand","warehouse","finished_price","price_with_disc","spp","is_cancel","code"])
        out = pd.concat(dfs, ignore_index=True)
        out = out[(out["supplier_article"] != "") & out["subject"].isin(TARGET_SUBJECTS)].copy()
        out = out[~out["is_cancel"]].copy()
        log(f"  orders rows: {len(out):,}")
        return out

    def load_economics(self) -> pd.DataFrame:
        log("Этап 1.2. Загружаю Экономику")
        candidates = [
            self._prefix("Финансовые показатели", self.store, "Экономика.xlsx"),
            self._prefix("Финансовые показатели", self.store, "Недельные", "Экономика.xlsx"),
        ]
        path = None
        for c in candidates:
            if self.storage.exists(c):
                path = c
                break
        if path is None:
            raise FileNotFoundError("Не найден файл Экономика.xlsx")
        raw, sheet, _ = read_excel_flexible(
            self.storage.read_bytes(path),
            path,
            preferred_sheets=["Юнит экономика"],
            header_candidates=(0, 1, 2),
            expected_aliases={
                "week_code": ALIASES["week_code"],
                "supplier_article": ALIASES["supplier_article"],
                "nm_id": ALIASES["nm_id"],
                "subject": ALIASES["subject"],
                "orders": ["Чистые продажи, шт", "Заказы"],
                "gross_profit_unit": ["Валовая прибыль, руб/ед"],
            },
        )
        df = rename_using_aliases(raw, {
            **ALIASES,
        })
        df["week_code"] = df["week_code"].map(normalize_text)
        df["supplier_article"] = df["supplier_article"].map(normalize_supplier_article)
        df["nm_id"] = to_numeric(df["nm_id"])
        df["subject"] = df["subject"].map(normalize_text)
        df["brand"] = df["brand"].map(normalize_text)
        df["orders"] = to_numeric(df["orders"]).fillna(0)
        df["gross_profit_unit"] = to_numeric(df["gross_profit_unit"]).fillna(0)
        df["code"] = df["supplier_article"].map(extract_code)
        df = df[(df["supplier_article"] != "") & df["subject"].isin(TARGET_SUBJECTS)].copy()
        log(f"  economics rows: {len(df):,}; sheet={sheet}")
        return df[["week_code","supplier_article","nm_id","subject","brand","orders","gross_profit_unit","code"]]

    def load_abc(self) -> pd.DataFrame:
        log("Этап 1.3. Загружаю ABC")
        files = self._list_xlsx(self._prefix("ABC"))
        files = [f for f in files if "wb_abc_report_goods__" in Path(f).name]
        dfs = []
        for path in files:
            try:
                raw, _, _ = read_excel_flexible(
                    self.storage.read_bytes(path),
                    path,
                    preferred_sheets=None,
                    header_candidates=(0, 1, 2),
                    expected_aliases={
                        "supplier_article": ALIASES["supplier_article"],
                        "nm_id": ALIASES["nm_id"],
                        "subject": ALIASES["subject"],
                        "gross_profit": ALIASES["gross_profit"],
                    },
                )
                df = rename_using_aliases(raw, {
                    **ALIASES,
                    "gross_revenue": ["Валовая выручка"],
                })
                start, end = parse_abc_period_from_name(Path(path).name)
                week_code = week_code_from_date(pd.Timestamp(start)) if start else None
                df["week_code"] = week_code
                df["week_start"] = pd.Timestamp(start) if start else pd.NaT
                df["week_end"] = pd.Timestamp(end) if end else pd.NaT
                df["supplier_article"] = df["supplier_article"].map(normalize_supplier_article)
                df["nm_id"] = to_numeric(df["nm_id"])
                df["subject"] = df["subject"].map(normalize_text)
                df["brand"] = df["brand"].map(normalize_text)
                df["gross_profit"] = to_numeric(df["gross_profit"]).fillna(0)
                df["gross_revenue"] = to_numeric(df["gross_revenue"]).fillna(0)
                df["orders"] = to_numeric(df["orders"]).fillna(0)
                df["code"] = df["supplier_article"].map(extract_code)
                df = df[(df["supplier_article"] != "") & df["subject"].isin(TARGET_SUBJECTS)].copy()
                dfs.append(df[["week_code","week_start","week_end","supplier_article","nm_id","subject","brand","gross_profit","gross_revenue","orders","code"]])
            except Exception as e:
                log(f"WARN: abc read error {path}: {e}")
        if not dfs:
            return pd.DataFrame(columns=["week_code","week_start","week_end","supplier_article","nm_id","subject","brand","gross_profit","gross_revenue","orders","code"])
        out = pd.concat(dfs, ignore_index=True)
        log(f"  abc rows: {len(out):,}")
        return out

    def load_all(self) -> LoadedData:
        orders = self.load_orders()
        economics = self.load_economics()
        abc = self.load_abc()
        return LoadedData(orders=orders, economics=economics, abc=abc)

# =========================
# Stage 1 builder
# =========================

class Stage1Builder:
    def __init__(self, data: LoadedData):
        self.data = data
        self.daily = self.build_daily_gp()
        self.current_week_dates = self.get_current_week_dates()
        self.current_month_weeks = self.get_current_month_weeks()
        self.last_three_months = self.get_last_three_months()
        self.daily_plan = self.build_daily_plan()
        self.weekly_actual = self.build_weekly_actual()
        self.weekly_plan = self.build_weekly_plan()
        self.monthly_actual = self.build_monthly_actual()
        self.monthly_plan = self.build_monthly_plan()

    def build_daily_gp(self) -> pd.DataFrame:
        log("Этап 1.4. Считаю дневную валовую прибыль (Экономика × заказы)")
        o = self.data.orders.copy()
        e = self.data.economics.copy()
        if o.empty or e.empty:
            return pd.DataFrame(columns=["day","supplier_article","nm_id","subject","brand","code","gross_profit_day"])
        o["week_code"] = o["day"].map(week_code_from_date)
        o["one_order"] = 1

        daily_orders = (
            o.groupby(["day","week_code","supplier_article","nm_id","subject","brand","code"], dropna=False)
             .agg(order_count=("one_order","sum"))
             .reset_index()
        )

        econ = (
            e.groupby(["week_code","supplier_article","nm_id","subject","brand","code"], dropna=False)
             .agg(gross_profit_unit=("gross_profit_unit","mean"))
             .reset_index()
        )

        cur = daily_orders.merge(
            econ[["week_code","supplier_article","gross_profit_unit"]],
            on=["week_code","supplier_article"],
            how="left",
        )
        missing = cur["gross_profit_unit"].isna()
        if missing.any():
            fallback = daily_orders.loc[missing, ["week_code","nm_id"]].merge(
                econ[["week_code","nm_id","gross_profit_unit"]].dropna(subset=["nm_id"]),
                on=["week_code","nm_id"],
                how="left",
            )
            cur.loc[missing, "gross_profit_unit"] = fallback["gross_profit_unit"].values

        cur["gross_profit_unit"] = cur["gross_profit_unit"].fillna(0)
        cur["gross_profit_day"] = cur["order_count"] * cur["gross_profit_unit"]
        log(f"  daily gp rows: {len(cur):,}")
        return cur

    def get_current_week_dates(self) -> List[pd.Timestamp]:
        if self.daily.empty:
            return []
        max_day = pd.to_datetime(self.daily["day"], errors="coerce").max()
        week_start = max_day - pd.Timedelta(days=int(max_day.weekday()))
        return [week_start + pd.Timedelta(days=i) for i in range(7)]

    def get_current_month_weeks(self) -> List[Tuple[pd.Timestamp, pd.Timestamp, str]]:
        if self.data.abc.empty:
            return []
        max_week_end = pd.to_datetime(self.data.abc["week_end"], errors="coerce").max()
        month = max_week_end.month
        year = max_week_end.year
        abc = self.data.abc.copy()
        abc["week_start"] = to_dt(abc["week_start"])
        abc["week_end"] = to_dt(abc["week_end"])
        weeks = (
            abc.loc[(abc["week_start"].dt.month == month) | (abc["week_end"].dt.month == month), ["week_code","week_start","week_end"]]
               .drop_duplicates()
               .sort_values("week_start", ascending=False)
        )
        result = []
        seen = set()
        for _, r in weeks.iterrows():
            wk = r["week_code"]
            if wk in seen:
                continue
            seen.add(wk)
            result.append((pd.Timestamp(r["week_start"]), pd.Timestamp(r["week_end"]), wk))
        return result

    def get_last_three_months(self) -> List[pd.Timestamp]:
        if self.data.abc.empty:
            return []
        max_week_end = pd.to_datetime(self.data.abc["week_end"], errors="coerce").max()
        current_month = pd.Timestamp(max_week_end.year, max_week_end.month, 1)
        months = [current_month]
        for i in range(1, 3):
            prev = (current_month.to_period("M") - i).to_timestamp()
            months.append(prev)
        return months

    def build_daily_plan(self) -> pd.DataFrame:
        log("Этап 1.5. Считаю дневной план по валовой прибыли")
        df = self.daily.copy()
        if df.empty:
            return pd.DataFrame(columns=["level","entity","weekday","plan_day"])
        rows = []

        def plans_for(group_df: pd.DataFrame, entity: str, level: str):
            g = group_df.copy()
            g["weekday"] = pd.to_datetime(g["day"]).dt.weekday
            for wd, s in g.groupby("weekday")["gross_profit_day"]:
                rows.append({
                    "level": level,
                    "entity": entity,
                    "weekday": int(wd),
                    "plan_day": target_from_series(s.tail(90)),
                })

        for subject, g in df.groupby("subject", dropna=False):
            plans_for(g, subject, "category")
        for code, g in df.groupby("code", dropna=False):
            plans_for(g, code, "product")
        for art, g in df.groupby("supplier_article", dropna=False):
            plans_for(g, art, "article")

        return pd.DataFrame(rows)

    def build_weekly_actual(self) -> pd.DataFrame:
        log("Этап 1.6. Считаю недельную валовую прибыль по ABC")
        a = self.data.abc.copy()
        if a.empty:
            return pd.DataFrame(columns=["week_code","week_start","week_end","supplier_article","code","subject","gross_profit_week"])
        g = (
            a.groupby(["week_code","week_start","week_end","supplier_article","code","subject"], dropna=False)
             .agg(gross_profit_week=("gross_profit","sum"))
             .reset_index()
        )
        return g

    def build_weekly_plan(self) -> pd.DataFrame:
        log("Этап 1.7. Считаю недельный план")
        w = self.weekly_actual.copy()
        if w.empty:
            return pd.DataFrame(columns=["level","entity","plan_week"])
        rows = []
        for subject, g in w.groupby("subject", dropna=False):
            rows.append({"level":"category","entity":subject,"plan_week":target_from_series(g["gross_profit_week"].tail(12))})
        for code, g in w.groupby("code", dropna=False):
            rows.append({"level":"product","entity":code,"plan_week":target_from_series(g["gross_profit_week"].tail(12))})
        for art, g in w.groupby("supplier_article", dropna=False):
            rows.append({"level":"article","entity":art,"plan_week":target_from_series(g["gross_profit_week"].tail(12))})
        return pd.DataFrame(rows)

    def build_monthly_actual(self) -> pd.DataFrame:
        log("Этап 1.8. Считаю месячную валовую прибыль по ABC")
        w = self.weekly_actual.copy()
        if w.empty:
            return pd.DataFrame(columns=["month_start","supplier_article","code","subject","gross_profit_month"])
        w["month_start"] = pd.to_datetime(w["week_end"]).dt.to_period("M").dt.to_timestamp()
        g = (
            w.groupby(["month_start","supplier_article","code","subject"], dropna=False)
             .agg(gross_profit_month=("gross_profit_week","sum"))
             .reset_index()
        )
        return g

    def build_monthly_plan(self) -> pd.DataFrame:
        log("Этап 1.9. Считаю месячный план")
        m = self.monthly_actual.copy()
        if m.empty:
            return pd.DataFrame(columns=["level","entity","plan_month"])
        rows = []
        for subject, g in m.groupby("subject", dropna=False):
            rows.append({"level":"category","entity":subject,"plan_month":target_from_series(g["gross_profit_month"].tail(6))})
        for code, g in m.groupby("code", dropna=False):
            rows.append({"level":"product","entity":code,"plan_month":target_from_series(g["gross_profit_month"].tail(6))})
        for art, g in m.groupby("supplier_article", dropna=False):
            rows.append({"level":"article","entity":art,"plan_month":target_from_series(g["gross_profit_month"].tail(6))})
        return pd.DataFrame(rows)

    def build_hierarchy(self) -> List[Dict[str, str]]:
        log("Этап 1.10. Собираю иерархию категория → товар → артикул")
        articles = (
            self.daily[["supplier_article","code","subject"]]
            .drop_duplicates()
            .query("supplier_article != '' and code != '' and subject != ''")
        )
        if articles.empty:
            # fallback from abc/economics
            frames = []
            for df in [self.weekly_actual, self.data.economics]:
                cols = [c for c in ["supplier_article","code","subject"] if c in df.columns]
                if cols:
                    frames.append(df[cols].drop_duplicates())
            if frames:
                articles = pd.concat(frames, ignore_index=True).drop_duplicates()
                articles = articles.query("supplier_article != '' and code != '' and subject != ''")
        rows: List[Dict[str, str]] = []
        for subject in TARGET_SUBJECTS:
            sub = articles[articles["subject"] == subject].copy()
            if sub.empty:
                # still keep category
                rows.append({"level":"category","subject":subject,"code":"","supplier_article":"","label":subject})
                continue
            rows.append({"level":"category","subject":subject,"code":"","supplier_article":"","label":subject})
            for code in sorted(sub["code"].dropna().astype(str).unique(), key=lambda x: (len(x), x)):
                rows.append({"level":"product","subject":subject,"code":code,"supplier_article":"","label":code})
                arts = sorted(sub.loc[sub["code"] == code, "supplier_article"].dropna().astype(str).unique())
                for art in arts:
                    rows.append({"level":"article","subject":subject,"code":code,"supplier_article":art,"label":art})
        return rows

# =========================
# Writer
# =========================

class Stage1Workbook:
    def __init__(self, builder: Stage1Builder):
        self.b = builder
        self.wb = Workbook()
        self.ws = self.wb.active
        self.ws.title = "Сводка"
        self.row = 1
        self.rows_meta: List[Tuple[int, str, str, str]] = []  # row, level, subject, code

    def write(self) -> Workbook:
        ws = self.ws
        ws.sheet_properties.outlinePr.summaryBelow = True
        self.write_title("Показатели Валовой прибыли")
        self.row += 1
        self.write_daily_section()
        self.row += 2
        self.write_weekly_section()
        self.row += 2
        self.write_monthly_section()
        self.apply_formatting()
        return self.wb

    def write_title(self, title: str) -> None:
        ws = self.ws
        ws.cell(self.row, 1, title)
        ws.cell(self.row, 1).font = Font(bold=True, size=14)
        ws.cell(self.row, 1).fill = TITLE_FILL
        self.row += 1
        subtitle = f"Сформировано: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"
        ws.cell(self.row, 1, subtitle)
        self.row += 1

    def _entity_value(self, level: str, subject: str, code: str, article: str, metric_df: pd.DataFrame, key_cols: List[str], value_col: str) -> pd.DataFrame:
        df = metric_df.copy()
        if df.empty:
            return df
        if level == "category":
            return df[df["subject"] == subject]
        if level == "product":
            return df[df["code"] == code]
        return df[df["supplier_article"] == article]

    def _plan_value(self, level: str, entity: str, plan_df: pd.DataFrame, col: str, weekdays: Optional[List[int]] = None) -> float:
        if plan_df.empty:
            return 0.0
        x = plan_df[(plan_df["level"] == level) & (plan_df["entity"] == entity)]
        if x.empty:
            return 0.0
        if weekdays is not None and "weekday" in x.columns:
            x = x[x["weekday"].isin(weekdays)]
            if x.empty:
                return 0.0
        return float(x[col].mean()) if col in x.columns else 0.0

    def write_daily_section(self) -> None:
        ws = self.ws
        start_row = self.row
        ws.cell(self.row, 1, "Текущая неделя по дням (дневная валовая прибыль из Экономики)")
        ws.cell(self.row, 1).font = Font(bold=True)
        ws.cell(self.row, 1).fill = SECTION_FILL
        self.row += 1

        dates = self.b.current_week_dates
        headers = ["Категория / Товар / Артикул"] + [f"{russian_weekday_name(d)}\n{d.strftime('%d.%m')}" for d in dates] + ["План по валовой прибыли, день"]
        self._write_header(headers)

        hierarchy = self.b.build_hierarchy()
        for item in hierarchy:
            row_idx = self.row
            ws.cell(self.row, 1, item["label"])
            level = item["level"]
            if level == "category":
                vals = []
                df = self._entity_value(level, item["subject"], item["code"], item["supplier_article"], self.b.daily, [], "gross_profit_day")
                for d in dates:
                    vals.append(float(df.loc[df["day"] == d, "gross_profit_day"].sum()))
                plan = self._plan_value("category", item["subject"], self.b.daily_plan, "plan_day", weekdays=[int(d.weekday()) for d in dates])
            elif level == "product":
                df = self._entity_value(level, item["subject"], item["code"], item["supplier_article"], self.b.daily, [], "gross_profit_day")
                vals = [float(df.loc[df["day"] == d, "gross_profit_day"].sum()) for d in dates]
                plan = self._plan_value("product", item["code"], self.b.daily_plan, "plan_day", weekdays=[int(d.weekday()) for d in dates])
            else:
                df = self._entity_value(level, item["subject"], item["code"], item["supplier_article"], self.b.daily, [], "gross_profit_day")
                vals = [float(df.loc[df["day"] == d, "gross_profit_day"].sum()) for d in dates]
                plan = self._plan_value("article", item["supplier_article"], self.b.daily_plan, "plan_day", weekdays=[int(d.weekday()) for d in dates])
            for i, val in enumerate(vals, start=2):
                ws.cell(self.row, i, val)
            ws.cell(self.row, len(headers), plan)
            self._style_data_row(self.row, len(headers), level)
            self.rows_meta.append((row_idx, level, item["subject"], item["code"]))
            self.row += 1

        # total
        total_row = self.row
        ws.cell(self.row, 1, "ИТОГО")
        for idx, d in enumerate(dates, start=2):
            total = float(self.b.daily.loc[self.b.daily["day"] == d, "gross_profit_day"].sum())
            ws.cell(self.row, idx, total)
        total_plan = sum(self._plan_value("category", s, self.b.daily_plan, "plan_day", weekdays=[int(d.weekday()) for d in dates]) for s in TARGET_SUBJECTS)
        ws.cell(self.row, len(headers), total_plan)
        self._style_total_row(self.row, len(headers))
        self.row += 1

    def write_weekly_section(self) -> None:
        ws = self.ws
        ws.cell(self.row, 1, "Текущий месяц по неделям (недельная валовая прибыль из ABC)")
        ws.cell(self.row, 1).font = Font(bold=True)
        ws.cell(self.row, 1).fill = SECTION_FILL
        self.row += 1

        weeks = self.b.current_month_weeks
        headers = ["Категория / Товар / Артикул"] + [week_label(s, e) for s, e, _ in weeks] + ["План по валовой прибыли, неделя"]
        self._write_header(headers)

        hierarchy = self.b.build_hierarchy()
        for item in hierarchy:
            row_idx = self.row
            ws.cell(self.row, 1, item["label"])
            level = item["level"]
            vals = []
            if level == "category":
                df = self.b.weekly_actual[self.b.weekly_actual["subject"] == item["subject"]]
                plan = self._plan_value("category", item["subject"], self.b.weekly_plan, "plan_week")
            elif level == "product":
                df = self.b.weekly_actual[self.b.weekly_actual["code"] == item["code"]]
                plan = self._plan_value("product", item["code"], self.b.weekly_plan, "plan_week")
            else:
                df = self.b.weekly_actual[self.b.weekly_actual["supplier_article"] == item["supplier_article"]]
                plan = self._plan_value("article", item["supplier_article"], self.b.weekly_plan, "plan_week")
            for s, e, wk in weeks:
                vals.append(float(df.loc[df["week_code"] == wk, "gross_profit_week"].sum()))
            for i, val in enumerate(vals, start=2):
                ws.cell(self.row, i, val)
            ws.cell(self.row, len(headers), plan)
            self._style_data_row(self.row, len(headers), level)
            self.rows_meta.append((row_idx, level, item["subject"], item["code"]))
            self.row += 1

        ws.cell(self.row, 1, "ИТОГО")
        for idx, (_, _, wk) in enumerate(weeks, start=2):
            total = float(self.b.weekly_actual.loc[self.b.weekly_actual["week_code"] == wk, "gross_profit_week"].sum())
            ws.cell(self.row, idx, total)
        total_plan = sum(self._plan_value("category", s, self.b.weekly_plan, "plan_week") for s in TARGET_SUBJECTS)
        ws.cell(self.row, len(headers), total_plan)
        self._style_total_row(self.row, len(headers))
        self.row += 1

    def write_monthly_section(self) -> None:
        ws = self.ws
        ws.cell(self.row, 1, "Последние 3 месяца (месячная валовая прибыль из ABC)")
        ws.cell(self.row, 1).font = Font(bold=True)
        ws.cell(self.row, 1).fill = SECTION_FILL
        self.row += 1

        months = self.b.last_three_months
        headers = ["Категория / Товар / Артикул"] + [month_label(m) for m in months] + ["План по валовой прибыли, месяц"]
        self._write_header(headers)

        hierarchy = self.b.build_hierarchy()
        for item in hierarchy:
            row_idx = self.row
            ws.cell(self.row, 1, item["label"])
            level = item["level"]
            vals = []
            if level == "category":
                df = self.b.monthly_actual[self.b.monthly_actual["subject"] == item["subject"]]
                plan = self._plan_value("category", item["subject"], self.b.monthly_plan, "plan_month")
            elif level == "product":
                df = self.b.monthly_actual[self.b.monthly_actual["code"] == item["code"]]
                plan = self._plan_value("product", item["code"], self.b.monthly_plan, "plan_month")
            else:
                df = self.b.monthly_actual[self.b.monthly_actual["supplier_article"] == item["supplier_article"]]
                plan = self._plan_value("article", item["supplier_article"], self.b.monthly_plan, "plan_month")
            for m in months:
                vals.append(float(df.loc[df["month_start"] == m, "gross_profit_month"].sum()))
            for i, val in enumerate(vals, start=2):
                ws.cell(self.row, i, val)
            ws.cell(self.row, len(headers), plan)
            self._style_data_row(self.row, len(headers), level)
            self.rows_meta.append((row_idx, level, item["subject"], item["code"]))
            self.row += 1

        ws.cell(self.row, 1, "ИТОГО")
        for idx, m in enumerate(months, start=2):
            total = float(self.b.monthly_actual.loc[self.b.monthly_actual["month_start"] == m, "gross_profit_month"].sum())
            ws.cell(self.row, idx, total)
        total_plan = sum(self._plan_value("category", s, self.b.monthly_plan, "plan_month") for s in TARGET_SUBJECTS)
        ws.cell(self.row, len(headers), total_plan)
        self._style_total_row(self.row, len(headers))
        self.row += 1

    def _write_header(self, headers: List[str]) -> None:
        for col_idx, header in enumerate(headers, start=1):
            c = self.ws.cell(self.row, col_idx, header)
            c.fill = HEADER_FILL
            c.font = Font(bold=True)
            c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            c.border = BORDER
        self.row += 1

    def _style_data_row(self, row_idx: int, max_col: int, level: str) -> None:
        ws = self.ws
        indent = {"category":0, "product":1, "article":2}[level]
        for c in range(1, max_col + 1):
            cell = ws.cell(row_idx, c)
            cell.border = BORDER
            cell.alignment = Alignment(horizontal="center", vertical="center")
            if c == 1:
                cell.alignment = Alignment(horizontal="left", vertical="center", indent=indent)
                if level == "category":
                    cell.font = Font(bold=True)
                elif level == "product":
                    cell.font = Font(bold=True, italic=True)
            else:
                cell.number_format = MONEY_FMT
        if level == "product":
            ws.row_dimensions[row_idx].outlineLevel = 1
            ws.row_dimensions[row_idx].hidden = True
        elif level == "article":
            ws.row_dimensions[row_idx].outlineLevel = 2
            ws.row_dimensions[row_idx].hidden = True

    def _style_total_row(self, row_idx: int, max_col: int) -> None:
        ws = self.ws
        for c in range(1, max_col + 1):
            cell = ws.cell(row_idx, c)
            cell.fill = TOTAL_FILL
            cell.font = Font(bold=True)
            cell.border = BORDER
            cell.alignment = Alignment(horizontal="center", vertical="center")
            if c > 1:
                cell.number_format = MONEY_FMT

    def apply_formatting(self) -> None:
        ws = self.ws
        ws.freeze_panes = "A4"
        widths = {}
        for row in ws.iter_rows():
            for cell in row:
                val = "" if cell.value is None else str(cell.value)
                widths[cell.column] = max(widths.get(cell.column, 0), len(val))
        for idx, width in widths.items():
            if idx == 1:
                ws.column_dimensions[get_column_letter(idx)].width = 28
            else:
                ws.column_dimensions[get_column_letter(idx)].width = max(13, min(width + 2, 18))
        # group rows are already hidden, Excel will show outline controls

def workbook_to_bytes(wb: Workbook) -> bytes:
    bio = io.BytesIO()
    wb.save(bio)
    return bio.getvalue()

# =========================
# CLI
# =========================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="WB report - stage 1 only")
    p.add_argument("--root", default=".", help="Project root for local mode")
    p.add_argument("--reports-root", default="Отчёты", help="Root folder with reports")
    p.add_argument("--store", default="TOPFACE", help="Store name")
    p.add_argument("--out-subdir", default="Отчёты/Объединенный отчет/TOPFACE", help="Output folder")
    return p.parse_args()

def main() -> int:
    args = parse_args()
    storage = make_storage(args.root)
    loader = Loader(storage=storage, store=args.store, reports_root=args.reports_root)

    log("Loading data")
    data = loader.load_all()

    log("Building stage 1 analytics")
    builder = Stage1Builder(data)
    wb = Stage1Workbook(builder).write()

    stamp = datetime.now().strftime("%Y-%m-%d")
    out_path = f"{args.out_subdir}/Объединенный_отчет_{args.store}_{stamp}.xlsx"
    log(f"Saving workbook: {out_path}")
    storage.write_bytes(out_path, workbook_to_bytes(wb))
    log(f"Saved: {out_path}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
