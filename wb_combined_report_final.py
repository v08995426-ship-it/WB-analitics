
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WB TOPFACE analytics report.

Main goals:
- оперативная валовая прибыль по заказам с корректным % выкупа WB = buyouts / (buyouts + cancels);
- недельный факторный анализ по товарам и артикулам;
- средние и целевые значения за 90 дней;
- анализ рекламы manual/unified;
- поисковые запросы, % трафика, ядро ключей 80% заказов, позиции и рейтинги;
- точки входа / каналы продаж;
- локализация по складам с региональными заменами;
- выводы о причинах отклонения от плана/цели.

Outputs are overwritten every run:
1. Отчёты/Объединенный отчет/TOPFACE/Объединенный_отчет_TOPFACE.xlsx
2. Отчёты/Объединенный отчет/TOPFACE/Технические_расчеты_TOPFACE.xlsx
3. Отчёты/Объединенный отчет/TOPFACE/Пример_расчета_901_TOPFACE.xlsx
4. Отчёты/Объединенный отчет/TOPFACE/Средние_и_целевые_значения_TOPFACE.xlsx
5. Отчёты/Объединенный отчет/TOPFACE/Каналы_продаж_и_реклама_TOPFACE.xlsx
6. Отчёты/Объединенный отчет/TOPFACE/Поисковые_запросы_и_позиции_TOPFACE.xlsx
7. Отчёты/Объединенный отчет/TOPFACE/Локализация_TOPFACE.xlsx
8. Отчёты/Объединенный отчет/TOPFACE/Выводы_по_причинам_TOPFACE.xlsx
"""

from __future__ import annotations

import argparse
import calendar
import io
import math
import os
import re
import shutil
import zipfile
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import boto3
import numpy as np
import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

TARGET_SUBJECTS = [
    "Кисти косметические",
    "Помады",
    "Блески",
    "Косметические карандаши",
]

EXCLUDE_ARTICLES_UPPER = {
    "CZ420", "CZ420БРОВИ", "CZ420ГЛАЗА", "DE49", "DE49ГЛАЗА", "PT901",
}

EXAMPLE_ARTICLES = ["901/5", "901/8", "901/14", "901/18"]

OUT_DIR = "Отчёты/Объединенный отчет/TOPFACE"
MAIN_REPORT_NAME = "Объединенный_отчет_TOPFACE.xlsx"
TECH_REPORT_NAME = "Технические_расчеты_TOPFACE.xlsx"
EXAMPLE_REPORT_NAME = "Пример_расчета_901_TOPFACE.xlsx"
POTENTIAL_REPORT_NAME = "Средние_и_целевые_значения_TOPFACE.xlsx"
CHANNEL_REPORT_NAME = "Каналы_продаж_и_реклама_TOPFACE.xlsx"
SEARCH_REPORT_NAME = "Поисковые_запросы_и_позиции_TOPFACE.xlsx"
LOCALIZATION_REPORT_NAME = "Локализация_TOPFACE.xlsx"
CONCLUSIONS_REPORT_NAME = "Выводы_по_причинам_TOPFACE.xlsx"

HEADER_FILL = PatternFill("solid", fgColor="17365D")
HEADER_FONT = Font(color="FFFFFF", bold=True)
TITLE_FILL = PatternFill("solid", fgColor="1F4E79")
SECTION_FILL = PatternFill("solid", fgColor="D9EAF7")
SUBSECTION_FILL = PatternFill("solid", fgColor="EAF4FF")
GOOD_FILL = PatternFill("solid", fgColor="C6EFCE")
WARN_FILL = PatternFill("solid", fgColor="FFF2CC")
BAD_FILL = PatternFill("solid", fgColor="FFC7CE")
THIN = Side(style="thin", color="D9D9D9")
BORDER = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)

WEEKDAY_RU = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
MONTH_RU = {
    1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель", 5: "Май", 6: "Июнь",
    7: "Июль", 8: "Август", 9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
}

# Broad alias dictionary. The code never fails because of one missing cosmetic column: it writes diagnostics instead.
ALIASES: Dict[str, Sequence[str]] = {
    "day": ["Дата", "Дата заказа", "Дата сбора", "Дата запроса", "dt", "date", "День"],
    "nm_id": ["Артикул WB", "Артикул ВБ", "Артикул ВБ", "nmID", "nmId", "nm_id", "Артикул WB", "Номенклатура", "Артикул ВБ"],
    "supplier_article": ["Артикул продавца", "supplierArticle", "supplier_article", "Артикул", "Артикул WB продавца"],
    "subject": ["Предмет", "subject", "Название предмета", "Категория", "category"],
    "brand": ["Бренд", "brand"],
    "title": ["Название", "Название товара", "Товар", "Наименование"],
    "warehouse": ["Склад", "warehouseName", "warehouse"],
    "orders": ["Заказы", "orders", "ordersCount", "Количество заказов", "Кол-во заказов", "Заказали товаров, шт", "Заказали, шт", "Заказали"],
    "order_sum": ["Сумма заказов", "ordersSumRub", "Сумма заказов, руб", "Сумма заказов со скидкой", "Сумма заказов (со скидкой)"],
    "open_cards": ["Открытия карточки", "openCardCount", "Переходы в карточку", "Клики", "Клики карточки"],
    "add_to_cart": ["Добавления в корзину", "addToCartCount", "Корзины", "ATBS"],
    "cart_conv": ["Конверсия в корзину", "addToCartConversion", "Конверсия в корзину %"],
    "order_conv": ["Конверсия в заказ", "cartToOrderConversion", "Конверсия в заказ %"],
    "buyouts_count": ["buyoutsCount", "Выкупы", "Выкупили, шт", "Выкупили", "Кол-во выкупов"],
    "buyout_sum": ["buyoutsSumRub", "Выкупили, руб", "Сумма выкупов"],
    "cancels_count": ["cancelCount", "cancelsCount", "Отменили, шт", "Отменили", "Отмены", "Отменено"],
    "finished_price": ["finishedPrice", "Средняя конечная цена", "Средняя цена покупателя", "Цена с учетом всех скидок, кроме суммы по WB Кошельку", "Ср. цена продажи"],
    "price_with_disc": ["priceWithDisc", "Средняя цена продажи", "Цена со скидкой продавца, в том числе со скидкой WB Клуба"],
    "spp": ["СПП, %", "SPP", "Скидка WB, %", "spp"],
    "spend": ["Расход", "spend", "Продвижение", "Затраты", "Расходы"],
    "impressions": ["Показы", "shows", "views", "impressions"],
    "clicks": ["Клики", "Клики РК", "clicks", "clicksCount", "Переходы в карточку"],
    "ctr": ["CTR", "CTR РК", "ctr"],
    "cpc": ["CPC", "cpc"],
    "cr": ["CR", "cr"],
    "ad_orders": ["Заказы", "Заказы РК", "Заказы из рекламы", "orders"],
    "ad_order_sum": ["Сумма заказов", "Сумма заказов РК", "ordersSumRub"],
    "drr": ["ДРР", "drr"],
    "campaign_id": ["ID кампании", "advertId", "campaignId"],
    "bid_type": ["Тип ставки", "bidType"],
    "search_query": ["Поисковый запрос", "Запрос", "Ключевой запрос", "Ключевая фраза", "keyword", "query"],
    "filter": ["Фильтр"],
    "frequency": ["Частота запросов", "Частотность", "Частота", "frequency"],
    "median_position": ["Медианная позиция", "medianPosition"],
    "avg_position": ["Средняя позиция", "averagePosition"],
    "visibility": ["Видимость %", "Видимость", "visibility"],
    "rating_card": ["Рейтинг карточки"],
    "rating_reviews": ["Рейтинг отзывов"],
    "entry_section": ["Раздел", "Источник", "Группа источника"],
    "entry_point": ["Точка входа", "Канал", "Источник перехода"],
    "stock": ["Доступно для продажи", "Остаток", "Остатки", "Доступный остаток", "Остаток, шт", "Количество", "Полное количество", "Всего", "stock", "quantity", "qty"],
    "gross_profit": ["Валовая прибыль", "Валовая прибыль, руб", "Валовая прибыль, руб/ед"],
    "gross_revenue": ["Валовая выручка", "Валовая выручка, руб", "Выручка"],
    "commission_pct": ["Комиссия WB, %", "Комиссия ВБ, %", "Комиссия, %"],
    "acquiring_pct": ["Эквайринг, %", "Эквайринг WB, %"],
    "logistics_direct": ["Логистика прямая, руб/ед", "Логистика прямая"],
    "logistics_return": ["Логистика обратная, руб/ед", "Логистика обратная"],
    "storage": ["Хранение, руб/ед", "Хранение"],
    "other_costs": ["Прочие расходы, руб/ед", "Прочие расходы"],
    "cost": ["Себестоимость, руб", "Себестоимость", "Себестоимость, руб/ед"],
    "week": ["Неделя", "week", "week_code"],
}

REGIONAL_REPLACEMENT_POOLS = {
    "ЦФО": ["КОЛЕДИНО", "ЭЛЕКТРОСТАЛ", "БЕЛАЯ ДАЧ", "ВЕШК", "ВЁШК", "РЯЗАН", "ТУЛ", "АЛЕКСИН", "ВЛАДИМИР", "КОТОВСК", "ВОРОНЕЖ"],
    "ЮГ": ["КРАСНОДАР", "НЕВИННОМЫССК", "ВОЛГОГРАД", "РОСТОВ", "АКСАЙ"],
    "ПОВОЛЖЬЕ": ["КАЗАН", "ПЕНЗ", "САРАПУЛ", "НОВОСЕМЕЙКИНО", "САМАР"],
    "СЗФО": ["ШУШАР", "САНКТ", "ПЕТЕРБУРГ", "УТКИН", "СПБ"],
    "УРАЛ": ["ЕКАТЕРИНБУРГ", "ЧЕЛЯБИНСК", "ПЕРМ"],
    "СИБИРЬ": ["НОВОСИБИРСК", "КРАСНОЯРСК", "КЕМЕРОВО"],
}

# ------------------------- helpers -------------------------
def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def normalize_text(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return re.sub(r"\s+", " ", str(value).replace("\xa0", " ").strip())


def norm_key(value: Any) -> str:
    text = normalize_text(value).lower().replace("ё", "е")
    text = re.sub(r"[^a-zа-я0-9%]+", " ", text, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", text).strip()


def clean_article(value: Any) -> str:
    text = normalize_text(value)
    if text.lower() in {"", "nan", "none", "null"}:
        return ""
    return text


def article_upper(value: Any) -> str:
    return clean_article(value).upper().replace(" ", "")


def product_code(article: Any) -> str:
    text = article_upper(article).replace("_", "/")
    if not text or text in EXCLUDE_ARTICLES_UPPER:
        return ""
    m = re.match(r"^PT(\d+)", text)
    if m:
        return m.group(1)
    m = re.match(r"^(\d+)", text)
    if m:
        return m.group(1)
    m = re.match(r"^([A-ZА-Я]+\d+)", text)
    if m:
        return m.group(1)
    return text.split("/")[0].split(".")[0]


def to_number(value: Any) -> float:
    if value is None:
        return np.nan
    if isinstance(value, str):
        value = value.replace("\xa0", " ").replace(" ", "").replace("₽", "").replace("%", "").replace(",", ".")
    return pd.to_numeric(value, errors="coerce")


def num_series(series: pd.Series) -> pd.Series:
    if series is None:
        return pd.Series(dtype=float)
    return series.map(to_number)


def date_series(series: pd.Series) -> pd.Series:
    """Parse dates safely: ISO yyyy-mm-dd as ISO, Russian dd.mm.yyyy as day-first."""
    def parse_one(v: Any) -> Any:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return pd.NaT
        if isinstance(v, (pd.Timestamp, datetime, date)):
            return pd.Timestamp(v).normalize()
        txt = normalize_text(v)
        if not txt or txt.lower() in {"nan", "none", "null"}:
            return pd.NaT
        try:
            if re.match(r"^\d{4}[-/.]\d{1,2}[-/.]\d{1,2}", txt):
                return pd.to_datetime(txt, errors="coerce", dayfirst=False).normalize()
            if re.match(r"^\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4}", txt):
                return pd.to_datetime(txt, errors="coerce", dayfirst=True).normalize()
            return pd.to_datetime(txt, errors="coerce").normalize()
        except Exception:
            return pd.NaT
    return series.map(parse_one)


def safe_div(a: Any, b: Any, default: float = np.nan) -> float:
    a = to_number(a)
    b = to_number(b)
    if pd.isna(a) or pd.isna(b) or b == 0:
        return default
    return float(a) / float(b)


def money_format() -> str:
    return '# ##0 ₽;[Red]-# ##0 ₽;0 ₽'


def pct_format() -> str:
    return '0.0%'


def parse_week_from_name(name: str) -> Optional[str]:
    m = re.search(r"(\d{4})-W(\d{2})", name)
    if m:
        return f"{m.group(1)}-W{m.group(2)}"
    return None


def week_bounds(week_code: str) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    m = re.match(r"^(\d{4})-W(\d{2})$", str(week_code))
    if not m:
        return None, None
    start = pd.Timestamp(date.fromisocalendar(int(m.group(1)), int(m.group(2)), 1))
    return start, start + pd.Timedelta(days=6)


def week_code(ts: Any) -> str:
    if pd.isna(ts):
        return ""
    d = pd.Timestamp(ts)
    iso = d.isocalendar()
    return f"{int(iso.year)}-W{int(iso.week):02d}"


def parse_period_from_name(name: str) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    patterns = [
        r"(\d{2})\.(\d{2})\.(\d{4})-(\d{2})\.(\d{2})\.(\d{4})",
        r"(\d{2})-(\d{2})-(\d{4}).*?(\d{2})-(\d{2})-(\d{4})",
        r"(\d{4})-(\d{2})-(\d{2}).*?(\d{4})-(\d{2})-(\d{2})",
    ]
    for p in patterns:
        m = re.search(p, name)
        if not m:
            continue
        if len(m.group(1)) == 4:
            return pd.Timestamp(date(int(m.group(1)), int(m.group(2)), int(m.group(3)))), pd.Timestamp(date(int(m.group(4)), int(m.group(5)), int(m.group(6))))
        return pd.Timestamp(date(int(m.group(3)), int(m.group(2)), int(m.group(1)))), pd.Timestamp(date(int(m.group(6)), int(m.group(5)), int(m.group(4))))
    wk = parse_week_from_name(name)
    if wk:
        return week_bounds(wk)
    return None, None




def filter_recent_report_files(files: List[str], latest_day: pd.Timestamp, lookback_days: int = 110, keep_unknown: bool = True) -> List[str]:
    """Keep report files whose period intersects the requested lookback window.
    This avoids parsing old weekly search/stock/entry reports from S3.
    """
    if latest_day is None or pd.isna(latest_day):
        return sorted(set(files))
    cutoff = pd.Timestamp(latest_day).normalize() - pd.Timedelta(days=lookback_days)
    out: List[str] = []
    skipped_old = 0
    skipped_unknown = 0
    for f in sorted(set(files)):
        start, end = parse_period_from_name(Path(f).name)
        if start is None or end is None:
            if keep_unknown:
                out.append(f)
            else:
                skipped_unknown += 1
            continue
        if pd.Timestamp(end).normalize() >= cutoff and pd.Timestamp(start).normalize() <= pd.Timestamp(latest_day).normalize():
            out.append(f)
        else:
            skipped_old += 1
    if skipped_old or skipped_unknown:
        log(f"recent_file_filter: input={len(set(files))}, kept={len(out)}, skipped_old={skipped_old}, skipped_unknown={skipped_unknown}, cutoff={cutoff.date()}")
    return out



def limit_recent_report_files(files: List[str], max_files: int) -> List[str]:
    """Keep unknown support files and the newest dated report files by parsed period end.

    Using lexical sorting for names like "7-4-2026 ..." and "29-3-2026 ..." can pull
    old weeks instead of the latest ones. This helper uses dates parsed from the file
    name, so max-file limits reduce runtime without accidentally keeping older reports.
    """
    unique = sorted(set(files))
    if max_files is None or int(max_files) <= 0 or len(unique) <= int(max_files):
        known = []
        unknown = []
        for f in unique:
            start, end = parse_period_from_name(Path(f).name)
            if start is None or end is None:
                unknown.append(f)
            else:
                known.append((pd.Timestamp(end).normalize(), pd.Timestamp(start).normalize(), f))
        return sorted(unknown) + [f for _, _, f in sorted(known)]

    max_files = int(max_files)
    known = []
    unknown = []
    for f in unique:
        start, end = parse_period_from_name(Path(f).name)
        if start is None or end is None:
            unknown.append(f)
        else:
            known.append((pd.Timestamp(end).normalize(), pd.Timestamp(start).normalize(), f))
    known_sorted = [f for _, _, f in sorted(known)]

    # Preserve service/reference files without dates when possible; use the remaining
    # quota for the latest dated weekly/monthly files.
    if len(unknown) >= max_files:
        return sorted(unknown)[-max_files:]
    keep_known = max_files - len(unknown)
    return sorted(unknown) + known_sorted[-keep_known:]

def is_month_file(start: pd.Timestamp, end: pd.Timestamp) -> bool:
    if start is None or end is None or pd.isna(start) or pd.isna(end):
        return False
    return start.day == 1 and start.year == end.year and start.month == end.month and end.day == calendar.monthrange(start.year, start.month)[1]


def month_key(ts: Any) -> str:
    d = pd.Timestamp(ts)
    return f"{d.year:04d}-{d.month:02d}"


def unwrap_excel_bytes(data: bytes) -> bytes:
    """If uploaded report is a zip with xlsx inside, return the first xlsx bytes."""
    if data[:2] != b"PK":
        return data
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            names = [n for n in zf.namelist() if n.lower().endswith((".xlsx", ".xlsm")) and not Path(n).name.startswith("~$")]
            if names:
                # prefer files not inside __MACOSX
                names = sorted(names, key=lambda x: ("__MACOSX" in x, len(x)))
                return zf.read(names[0])
    except zipfile.BadZipFile:
        pass
    return data


def get_col(df: pd.DataFrame, logical_name: str) -> pd.Series:
    if logical_name in df.columns:
        return df[logical_name]
    return pd.Series([np.nan] * len(df))


def add_alias_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    col_by_key = {norm_key(c): c for c in out.columns}
    for target, variants in ALIASES.items():
        if target in out.columns:
            continue
        found = None
        for v in variants:
            key = norm_key(v)
            if key in col_by_key:
                found = col_by_key[key]
                break
        out[target] = out[found] if found is not None else np.nan
    return out


def read_excel_table(data: bytes, preferred_sheet: Optional[str] = None, header_rows: Iterable[int] = (0, 1, 2, 3, 4)) -> pd.DataFrame:
    data = unwrap_excel_bytes(data)
    xl = pd.ExcelFile(io.BytesIO(data))
    if preferred_sheet and preferred_sheet in xl.sheet_names:
        sheets = [preferred_sheet]
    else:
        sheets = xl.sheet_names
    best_df = None
    best_score = -1.0
    for sheet in sheets:
        for h in header_rows:
            try:
                df = xl.parse(sheet_name=sheet, header=h, dtype=object)
            except Exception:
                continue
            df = df.dropna(how="all").dropna(axis=1, how="all")
            if df.empty:
                continue
            df.columns = [normalize_text(c) or f"col_{i}" for i, c in enumerate(df.columns)]
            aliased = add_alias_columns(df)
            score = min(len(df.columns), 50) / 100
            for c in ["day", "nm_id", "supplier_article", "subject", "orders", "order_sum", "search_query", "entry_section", "spend", "stock", "gross_profit"]:
                if c in aliased.columns and not aliased[c].isna().all():
                    score += 1
            if score > best_score:
                best_score = score
                best_df = aliased
    if best_df is None:
        return pd.DataFrame()
    return best_df


def safe_sheet_name(name: str, used: set) -> str:
    raw = re.sub(r"[\\/*?:\[\]]", "_", normalize_text(name))[:31] or "Sheet"
    name = raw
    i = 1
    while name in used:
        suffix = f"_{i}"
        name = raw[:31 - len(suffix)] + suffix
        i += 1
    used.add(name)
    return name


def clean_warehouse_name(name: Any) -> str:
    return normalize_text(name).upper().replace("Ё", "Е")


def warehouse_pool(name: Any) -> str:
    n = clean_warehouse_name(name)
    for pool, keys in REGIONAL_REPLACEMENT_POOLS.items():
        if any(k in n for k in keys):
            return pool
    return "ДРУГИЕ"


def weighted_mean(values: pd.Series, weights: pd.Series) -> float:
    v = pd.to_numeric(values, errors="coerce")
    w = pd.to_numeric(weights, errors="coerce").fillna(0)
    mask = v.notna() & (w > 0)
    if mask.any():
        return float(np.average(v[mask], weights=w[mask]))
    if v.notna().any():
        return float(v.mean())
    return np.nan


def classify_ad_type(value: Any) -> str:
    t = norm_key(value)
    if "manual" in t or "руч" in t or "поиск" in t or "каталог" in t:
        return "manual"
    if "unified" in t or "авто" in t or "един" in t or "карточ" in t or "полк" in t or "рекомен" in t:
        return "unified"
    return "unknown"


# ------------------------- business cleanup helpers -------------------------
EXCLUDE_ARTICLE_PREFIXES = tuple(sorted(EXCLUDE_ARTICLES_UPPER, key=len, reverse=True))
EXCLUDE_PRODUCT_PREFIXES = ("CZ420", "DE49", "PT901", "FL", "PE")
WAREHOUSE_EXCLUDE_KEYWORDS = (
    "ВИРТУАЛ", "АСТАН", "АЛМАТ", "АТАКЕНТ", "КАРАГАНД", "КАЗАХ", "БЕЛАРУС", "МИНСК",
    "ДАЛЬНЕГОРСК", "МАХАЧКАЛА ВИРТ", "ВИРТУАЛЬНЫЙ",
)


def is_excluded_article(value: Any) -> bool:
    t = article_upper(value)
    if not t:
        return True
    return any(t.startswith(prefix) for prefix in EXCLUDE_ARTICLE_PREFIXES)


def is_valid_product_code(value: Any) -> bool:
    t = normalize_text(value).upper().replace(" ", "")
    if not t:
        return False
    if any(t.startswith(prefix) for prefix in EXCLUDE_PRODUCT_PREFIXES):
        return False
    # Основные товары TOPFACE в этих категориях обычно числовые; F-серия кистей допускается отдельно.
    if re.match(r"^\d{2,5}$", t):
        return True
    if re.match(r"^F\d{1,3}$", t):
        return True
    return False


def canonical_warehouse_name(name: Any) -> str:
    n = clean_warehouse_name(name)
    rules = [
        ("КОЛЕДИНО", "Коледино"), ("ЭЛЕКТРОСТАЛ", "Электросталь"), ("БЕЛАЯ ДАЧ", "Белая Дача"),
        ("ВЕШК", "Вёшки"), ("ВЁШК", "Вёшки"), ("РЯЗАН", "Рязань"), ("ТУЛ", "Тула"), ("АЛЕКСИН", "Тула"),
        ("ВЛАДИМИР", "Владимир"), ("КОТОВСК", "Котовск"), ("ВОРОНЕЖ", "Воронеж"),
        ("КРАСНОДАР", "Краснодар"), ("НЕВИННОМЫССК", "Невинномысск"), ("ВОЛГОГРАД", "Волгоград"),
        ("РОСТОВ", "Ростов/Аксай"), ("АКСАЙ", "Ростов/Аксай"),
        ("КАЗАН", "Казань"), ("ПЕНЗ", "Пенза"), ("САРАПУЛ", "Сарапул"), ("НОВОСЕМЕЙКИНО", "Новосемейкино"), ("САМАР", "Самара"),
        ("ШУШАР", "СПБ Шушары"), ("УТКИН", "СПБ Уткина Заводь"), ("САНКТ", "Санкт-Петербург"), ("ПЕТЕРБУРГ", "Санкт-Петербург"),
        ("ЕКАТЕРИНБУРГ", "Екатеринбург"), ("ЧЕЛЯБИНСК", "Челябинск"), ("ПЕРМ", "Пермь"),
        ("НОВОСИБИРСК", "Новосибирск"), ("КРАСНОЯРСК", "Красноярск"), ("КЕМЕРОВО", "Кемерово"),
    ]
    for key, canon in rules:
        if key in n:
            return canon
    return normalize_text(name)


def is_relevant_warehouse(name: Any) -> bool:
    n = clean_warehouse_name(name)
    if not n:
        return False
    if any(k in n for k in WAREHOUSE_EXCLUDE_KEYWORDS):
        return False
    return True


def classify_entry_channel(section: Any, point: Any = "") -> str:
    t = norm_key(f"{section} {point}")
    if "поиск" in t or "каталог" in t:
        return "Поиск/Каталог"
    if "карточ" in t or "полк" in t or "рекомен" in t:
        return "Карточка товара / полки"
    if "реклам" in t:
        return "Реклама / прочее"
    if "внеш" in t:
        return "Внешние переходы"
    return "Другие точки входа"


def pct_gap(fact: Any, target: Any) -> float:
    f = to_number(fact)
    t = to_number(target)
    if pd.isna(f) or pd.isna(t) or t == 0:
        return np.nan
    return (f / t - 1) * 100



@dataclass
class Diagnostics:
    rows: List[Dict[str, Any]] = field(default_factory=list)

    def add(self, level: str, source: str, message: str, details: Any = "") -> None:
        self.rows.append({
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "level": level,
            "source": source,
            "message": message,
            "details": normalize_text(details),
        })

    def frame(self) -> pd.DataFrame:
        return pd.DataFrame(self.rows, columns=["timestamp", "level", "source", "message", "details"])


# ------------------------- storage -------------------------
class Storage:
    is_s3 = False
    def list_files(self, prefix: str) -> List[str]:
        raise NotImplementedError
    def read_bytes(self, key: str) -> bytes:
        raise NotImplementedError
    def write_bytes(self, key: str, data: bytes) -> None:
        raise NotImplementedError
    def exists(self, key: str) -> bool:
        raise NotImplementedError


class LocalStorage(Storage):
    def __init__(self, root: str):
        self.root = Path(root)
    def _full(self, key: str) -> Path:
        return self.root / key
    def list_files(self, prefix: str) -> List[str]:
        prefix = prefix.replace("\\", "/").strip("/")
        start = self._full(prefix)
        base = start if start.is_dir() else start.parent
        if not base.exists():
            return []
        out = []
        for p in base.rglob("*"):
            if p.is_file():
                rel = str(p.relative_to(self.root)).replace("\\", "/")
                if rel.startswith(prefix) and not Path(rel).name.startswith("~$"):
                    out.append(rel)
        return sorted(out)
    def read_bytes(self, key: str) -> bytes:
        return self._full(key).read_bytes()
    def write_bytes(self, key: str, data: bytes) -> None:
        p = self._full(key)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
    def exists(self, key: str) -> bool:
        return self._full(key).exists()


class S3Storage(Storage):
    is_s3 = True
    def __init__(self, bucket: str, access_key: str, secret_key: str, endpoint_url: str):
        self.bucket = bucket
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
    def list_files(self, prefix: str) -> List[str]:
        out = []
        token = None
        while True:
            kwargs = {"Bucket": self.bucket, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            resp = self.client.list_objects_v2(**kwargs)
            for item in resp.get("Contents", []):
                key = item.get("Key", "")
                if key and not key.endswith("/") and not Path(key).name.startswith("~$"):
                    out.append(key)
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        return sorted(out)
    def read_bytes(self, key: str) -> bytes:
        return self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()
    def write_bytes(self, key: str, data: bytes) -> None:
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data)
    def exists(self, key: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except Exception:
            return False


def make_storage(root: str) -> Storage:
    bucket = os.getenv("YC_BUCKET_NAME", "").strip()
    access = os.getenv("YC_ACCESS_KEY_ID", "").strip()
    secret = os.getenv("YC_SECRET_ACCESS_KEY", "").strip()
    endpoint = os.getenv("YC_ENDPOINT_URL", "https://storage.yandexcloud.net").strip()
    if bucket and access and secret:
        log(f"Storage: Yandex Object Storage bucket={bucket}")
        return S3Storage(bucket, access, secret, endpoint)
    log(f"Storage: local root={Path(root).resolve()}")
    return LocalStorage(root)


# ------------------------- loader -------------------------
@dataclass
class DataPack:
    orders: pd.DataFrame
    funnel: pd.DataFrame
    ads_daily: pd.DataFrame
    ads_raw: pd.DataFrame
    campaigns: pd.DataFrame
    search_queries: pd.DataFrame
    entry_points: pd.DataFrame
    stock: pd.DataFrame
    abc_weekly: pd.DataFrame
    abc_monthly: pd.DataFrame
    economics: pd.DataFrame
    latest_day: pd.Timestamp
    diagnostics: Diagnostics


class Loader:
    def __init__(self, storage: Storage, reports_root: str, store: str, diagnostics: Diagnostics):
        self.storage = storage
        self.reports_root = reports_root.strip("/")
        self.store = store
        self.diag = diagnostics

    def path(self, *parts: str) -> str:
        return "/".join([self.reports_root, *parts]).replace("//", "/")

    def list_reports(self, *parts: str) -> List[str]:
        prefix = self.path(*parts)
        files = self.storage.list_files(prefix)
        return [f for f in files if f.lower().endswith((".xlsx", ".xlsm", ".zip")) and not Path(f).name.startswith("~$")]

    def _log(self, name: str, df: pd.DataFrame, date_col: Optional[str] = None) -> None:
        if date_col and not df.empty and date_col in df.columns:
            mn, mx = pd.to_datetime(df[date_col], errors="coerce").min(), pd.to_datetime(df[date_col], errors="coerce").max()
            log(f"{name}: rows={len(df):,}, dates={mn.date() if pd.notna(mn) else '-'}..{mx.date() if pd.notna(mx) else '-'}")
        else:
            log(f"{name}: rows={len(df):,}")

    def _read_candidates(self, paths: Iterable[str]) -> Iterable[Tuple[str, bytes]]:
        for key in sorted(set(paths)):
            try:
                yield key, self.storage.read_bytes(key)
            except Exception as exc:
                self.diag.add("ERROR", "read", f"Не удалось прочитать {key}", exc)

    def load_orders(self) -> pd.DataFrame:
        files = self.list_reports("Заказы", self.store, "Недельные")
        frames = []
        for key, data in self._read_candidates(files):
            try:
                df = read_excel_table(data, "Заказы")
                if df.empty:
                    continue
                out = pd.DataFrame({
                    "day": date_series(get_col(df, "day")),
                    "nm_id": num_series(get_col(df, "nm_id")),
                    "supplier_article": get_col(df, "supplier_article").map(clean_article),
                    "subject": get_col(df, "subject").map(normalize_text),
                    "warehouse": get_col(df, "warehouse").map(normalize_text),
                    "orders": num_series(get_col(df, "orders")),
                    "finished_price": num_series(get_col(df, "finished_price")),
                    "price_with_disc": num_series(get_col(df, "price_with_disc")),
                    "spp": num_series(get_col(df, "spp")),
                    "source_file": key,
                })
                if out["orders"].isna().all():
                    out["orders"] = 1.0
                out["orders"] = out["orders"].fillna(1.0)
                frames.append(out[out["day"].notna()])
            except Exception as exc:
                self.diag.add("ERROR", "orders", f"Не прочитан файл заказов {key}", exc)
        out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        self._log("orders", out, "day")
        return out

    def load_funnel(self) -> pd.DataFrame:
        files = []
        direct = self.path("Воронка продаж", self.store, "Воронка продаж.xlsx")
        if self.storage.exists(direct):
            files.append(direct)
        files += self.list_reports("Воронка продаж", self.store)
        files += self.list_reports("Воронка продаж")
        frames = []
        for key, data in self._read_candidates(files):
            try:
                period_start, period_end = parse_period_from_name(Path(key).name)
                preferred = "Воронка продаж"
                df = read_excel_table(data, preferred)
                if df.empty:
                    continue
                day = date_series(get_col(df, "day"))
                if day.isna().all() and period_end is not None:
                    day = pd.Series([period_end] * len(df))
                out = pd.DataFrame({
                    "day": day,
                    "nm_id": num_series(get_col(df, "nm_id")),
                    "supplier_article": get_col(df, "supplier_article").map(clean_article),
                    "subject": get_col(df, "subject").map(normalize_text),
                    "orders": num_series(get_col(df, "orders")),
                    "order_sum": num_series(get_col(df, "order_sum")),
                    "open_cards": num_series(get_col(df, "open_cards")),
                    "add_to_cart": num_series(get_col(df, "add_to_cart")),
                    "cart_conv_pct": num_series(get_col(df, "cart_conv")),
                    "order_conv_pct": num_series(get_col(df, "order_conv")),
                    "buyouts_count": num_series(get_col(df, "buyouts_count")),
                    "buyout_sum": num_series(get_col(df, "buyout_sum")),
                    "cancels_count": num_series(get_col(df, "cancels_count")),
                    "finished_price": num_series(get_col(df, "finished_price")),
                    "spp": num_series(get_col(df, "spp")),
                    "source_file": key,
                })
                out = out[out["nm_id"].notna()]
                out = out[out["day"].notna()]
                frames.append(out)
            except Exception as exc:
                self.diag.add("ERROR", "funnel", f"Не прочитана воронка {key}", exc)
        out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        self._log("funnel", out, "day")
        return out

    def load_ads(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        files = self.list_reports("Реклама", self.store, "Недельные")
        consolidated = self.path("Реклама", self.store, "Анализ рекламы.xlsx")
        if self.storage.exists(consolidated):
            files.append(consolidated)
        raw_frames, campaign_frames = [], []
        for key, data in self._read_candidates(files):
            try:
                book = pd.ExcelFile(io.BytesIO(unwrap_excel_bytes(data)))
                if "Список_кампаний" in book.sheet_names:
                    camp = read_excel_table(data, "Список_кампаний")
                    if not camp.empty:
                        cdf = pd.DataFrame({
                            "campaign_id": get_col(camp, "campaign_id").map(lambda x: str(int(x)) if pd.notna(to_number(x)) else normalize_text(x)),
                            "nm_id": num_series(get_col(camp, "nm_id")),
                            "subject": get_col(camp, "subject").map(normalize_text),
                            "bid_type_raw": get_col(camp, "bid_type").map(normalize_text),
                            "source_file": key,
                        })
                        cdf["ad_type"] = cdf["bid_type_raw"].map(classify_ad_type)
                        campaign_frames.append(cdf[cdf["campaign_id"].ne("")])
                sheets = ["Статистика_Ежедневно"] if "Статистика_Ежедневно" in book.sheet_names else book.sheet_names
                for sheet in sheets:
                    df = read_excel_table(data, sheet)
                    if df.empty or get_col(df, "spend").isna().all():
                        continue
                    out = pd.DataFrame({
                        "day": date_series(get_col(df, "day")),
                        "campaign_id": get_col(df, "campaign_id").map(lambda x: str(int(x)) if pd.notna(to_number(x)) else normalize_text(x)),
                        "nm_id": num_series(get_col(df, "nm_id")),
                        "subject": get_col(df, "subject").map(normalize_text),
                        "impressions": num_series(get_col(df, "impressions")).fillna(0),
                        "clicks": num_series(get_col(df, "clicks")).fillna(0),
                        "orders": num_series(get_col(df, "ad_orders")).fillna(0),
                        "order_sum": num_series(get_col(df, "ad_order_sum")).fillna(0),
                        "spend": num_series(get_col(df, "spend")).fillna(0),
                        "ctr_pct_src": num_series(get_col(df, "ctr")),
                        "cpc_src": num_series(get_col(df, "cpc")),
                        "cr_pct_src": num_series(get_col(df, "cr")),
                        "drr_pct_src": num_series(get_col(df, "drr")),
                        "source_file": key,
                        "source_sheet": sheet,
                    })
                    raw_frames.append(out[out["day"].notna() & out["nm_id"].notna()])
            except Exception as exc:
                self.diag.add("ERROR", "ads", f"Не прочитана реклама {key}", exc)
        raw = pd.concat(raw_frames, ignore_index=True) if raw_frames else pd.DataFrame()
        campaigns = pd.concat(campaign_frames, ignore_index=True) if campaign_frames else pd.DataFrame(columns=["campaign_id", "nm_id", "subject", "ad_type"])
        if not raw.empty:
            if not campaigns.empty:
                cmap = campaigns.drop_duplicates(["campaign_id", "nm_id"])[["campaign_id", "nm_id", "ad_type", "bid_type_raw"]]
                raw = raw.merge(cmap, on=["campaign_id", "nm_id"], how="left")
            raw["ad_type"] = raw.get("ad_type", "unknown")
            raw["ad_type"] = raw["ad_type"].fillna("unknown").replace("", "unknown")
            raw["ctr_pct"] = np.where(raw["impressions"] > 0, raw["clicks"] / raw["impressions"] * 100, raw["ctr_pct_src"])
            raw["cpc"] = np.where(raw["clicks"] > 0, raw["spend"] / raw["clicks"], raw["cpc_src"])
            raw["cr_pct"] = np.where(raw["clicks"] > 0, raw["orders"] / raw["clicks"] * 100, raw["cr_pct_src"])
            raw["drr_pct"] = np.where(raw["order_sum"] > 0, raw["spend"] / raw["order_sum"] * 100, raw["drr_pct_src"])
            daily = raw.groupby(["day", "nm_id", "ad_type"], dropna=False, as_index=False).agg(
                impressions=("impressions", "sum"), clicks=("clicks", "sum"), orders=("orders", "sum"),
                order_sum=("order_sum", "sum"), spend=("spend", "sum"),
            )
            daily["ctr_pct"] = np.where(daily["impressions"] > 0, daily["clicks"] / daily["impressions"] * 100, np.nan)
            daily["cpc"] = np.where(daily["clicks"] > 0, daily["spend"] / daily["clicks"], np.nan)
            daily["cr_pct"] = np.where(daily["clicks"] > 0, daily["orders"] / daily["clicks"] * 100, np.nan)
            daily["drr_pct"] = np.where(daily["order_sum"] > 0, daily["spend"] / daily["order_sum"] * 100, np.nan)
        else:
            daily = pd.DataFrame()
        self._log("ads_raw", raw, "day")
        self._log("ads_daily", daily, "day")
        return raw, daily, campaigns

    def load_search_queries(self, latest_day: Optional[pd.Timestamp] = None) -> pd.DataFrame:
        files_all = self.list_reports("Поисковые запросы", self.store, "Недельные")
        files = filter_recent_report_files(files_all, latest_day, lookback_days=110, keep_unknown=False)
        max_files = int(os.getenv("WB_MAX_SEARCH_QUERY_FILES", "8"))
        files = limit_recent_report_files(files, max_files)
        log(f"search_queries: start, files_all={len(files_all)}, files_to_read={len(files)}")
        frames = []
        for idx, (key, data) in enumerate(self._read_candidates(files), start=1):
            log(f"search_queries: reading {idx}/{len(files)} {Path(key).name}")
            try:
                df = read_excel_table(data, "Позиции по Ключам")
                if df.empty:
                    continue
                out = pd.DataFrame({
                    "day": date_series(get_col(df, "day")),
                    "nm_id": num_series(get_col(df, "nm_id")),
                    "supplier_article": get_col(df, "supplier_article").map(clean_article),
                    "subject": get_col(df, "subject").map(normalize_text),
                    "search_query": get_col(df, "search_query").map(normalize_text),
                    "filter": get_col(df, "filter").map(normalize_text),
                    "frequency": num_series(get_col(df, "frequency")),
                    "median_position": num_series(get_col(df, "median_position")),
                    "avg_position": num_series(get_col(df, "avg_position")),
                    "transitions": num_series(get_col(df, "open_cards")).fillna(num_series(get_col(df, "clicks"))).fillna(0),
                    "add_to_cart": num_series(get_col(df, "add_to_cart")).fillna(0),
                    "orders": num_series(get_col(df, "orders")).fillna(0),
                    "cart_conv_pct": num_series(get_col(df, "cart_conv")),
                    "order_conv_pct": num_series(get_col(df, "order_conv")),
                    "rating_card": num_series(get_col(df, "rating_card")),
                    "rating_reviews": num_series(get_col(df, "rating_reviews")),
                    "visibility_pct": num_series(get_col(df, "visibility")),
                    "source_file": key,
                })
                out = out[out["day"].notna() & out["search_query"].ne("")]
                # Deduplicate same query due to filters: frequency once, commercial metrics summed.
                group_cols = ["day", "nm_id", "supplier_article", "subject", "search_query"]
                agg = out.groupby(group_cols, dropna=False, as_index=False).agg(
                    frequency=("frequency", "max"),
                    transitions=("transitions", "sum"), add_to_cart=("add_to_cart", "sum"), orders=("orders", "sum"),
                    median_position=("median_position", "mean"), avg_position=("avg_position", "mean"),
                    cart_conv_pct=("cart_conv_pct", "mean"), order_conv_pct=("order_conv_pct", "mean"),
                    rating_card=("rating_card", "mean"), rating_reviews=("rating_reviews", "mean"),
                    visibility_pct=("visibility_pct", "mean"), source_file=("source_file", "first"),
                )
                agg["traffic_capture_pct"] = np.where(agg["frequency"] > 0, agg["transitions"] / agg["frequency"] * 100, np.nan)
                frames.append(agg)
            except Exception as exc:
                self.diag.add("ERROR", "search_queries", f"Не прочитан файл поисковых запросов {key}", exc)
        out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        self._log("search_queries", out, "day")
        return out

    def load_entry_points(self, latest_day: Optional[pd.Timestamp] = None) -> pd.DataFrame:
        files_all = []
        files_all += self.list_reports("Точки входа", self.store)
        files_all += self.list_reports("Портрет покупателя", self.store)
        # Broad fallback is expensive on S3, so it is used only when targeted folders are empty.
        if not files_all:
            files_all += [f for f in self.storage.list_files(self.reports_root) if "Точки входа" in f and f.lower().endswith((".xlsx", ".zip"))]
        files = filter_recent_report_files(files_all, latest_day, lookback_days=110, keep_unknown=True)
        max_files = int(os.getenv("WB_MAX_ENTRY_POINT_FILES", "8"))
        files = limit_recent_report_files(files, max_files)
        log(f"entry_points: start, files_all={len(files_all)}, files_to_read={len(files)}")
        frames = []
        for idx, (key, data) in enumerate(self._read_candidates(files), start=1):
            log(f"entry_points: reading {idx}/{len(files)} {Path(key).name}")
            try:
                _, period_end = parse_period_from_name(Path(key).name)
                df = read_excel_table(data, "Детализация по артикулам")
                if df.empty or get_col(df, "entry_section").isna().all():
                    continue
                day = date_series(get_col(df, "day")) if "day" in df.columns else pd.Series([period_end] * len(df))
                if day.isna().all() and period_end is not None:
                    day = pd.Series([period_end] * len(df))
                out = pd.DataFrame({
                    "day": day,
                    "entry_section": get_col(df, "entry_section").map(normalize_text),
                    "entry_point": get_col(df, "entry_point").map(normalize_text),
                    "nm_id": num_series(get_col(df, "nm_id")),
                    "supplier_article": get_col(df, "supplier_article").map(clean_article),
                    "subject": get_col(df, "subject").map(normalize_text),
                    "impressions": num_series(get_col(df, "impressions")).fillna(0),
                    "transitions": num_series(get_col(df, "open_cards")).fillna(0),
                    "ctr_pct": num_series(get_col(df, "ctr")),
                    "add_to_cart": num_series(get_col(df, "add_to_cart")).fillna(0),
                    "cart_conv_pct": num_series(get_col(df, "cart_conv")),
                    "orders": num_series(get_col(df, "orders")).fillna(0),
                    "order_conv_pct": num_series(get_col(df, "order_conv")),
                    "source_file": key,
                })
                out = out[out["nm_id"].notna() | out["supplier_article"].ne("")]
                frames.append(out)
            except Exception as exc:
                self.diag.add("ERROR", "entry_points", f"Не прочитаны точки входа {key}", exc)
        out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        self._log("entry_points", out, "day")
        return out

    def load_stock(self, latest_day: Optional[pd.Timestamp] = None) -> pd.DataFrame:
        files_all = []
        for parts in [("Остатки", self.store), ("Остатки", self.store, "Недельные"), ("Остатки",), ("Остатки и товары в пути", self.store), ("Остатки и товары в пути",)]:
            files_all += self.list_reports(*parts)
        files = filter_recent_report_files(files_all, latest_day, lookback_days=110, keep_unknown=True)
        max_files = int(os.getenv("WB_MAX_STOCK_FILES", "10"))
        files = limit_recent_report_files(files, max_files)
        log(f"stock: start, files_all={len(files_all)}, files_to_read={len(files)}")
        frames = []
        for idx, (key, data) in enumerate(self._read_candidates(files), start=1):
            log(f"stock: reading {idx}/{len(files)} {Path(key).name}")
            try:
                _, period_end = parse_period_from_name(Path(key).name)
                df = read_excel_table(data)
                if df.empty:
                    continue
                day = date_series(get_col(df, "day"))
                if day.isna().all() and period_end is not None:
                    day = pd.Series([period_end] * len(df))
                out = pd.DataFrame({
                    "day": day,
                    "nm_id": num_series(get_col(df, "nm_id")),
                    "supplier_article": get_col(df, "supplier_article").map(clean_article),
                    "subject": get_col(df, "subject").map(normalize_text),
                    "warehouse": get_col(df, "warehouse").map(normalize_text),
                    "stock": num_series(get_col(df, "stock")).fillna(0),
                    "source_file": key,
                })
                out = out[(out["nm_id"].notna() | out["supplier_article"].ne("")) & out["warehouse"].ne("")]
                frames.append(out)
            except Exception as exc:
                self.diag.add("ERROR", "stock", f"Не прочитаны остатки {key}", exc)
        out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        self._log("stock", out, "day")
        return out

    def load_abc(self, current_year: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
        files = self.list_reports("ABC")
        weekly_frames, monthly_frames = [], []
        for key, data in self._read_candidates(files):
            if "abc" not in key.lower():
                continue
            start, end = parse_period_from_name(Path(key).name)
            if start is None or end is None:
                continue
            try:
                df = read_excel_table(data)
                if df.empty:
                    continue
                out = pd.DataFrame({
                    "period_start": start,
                    "period_end": end,
                    "week_code": week_code(start),
                    "week_label": f"{start.strftime('%d.%m')}-{end.strftime('%d.%m')}",
                    "month_key": month_key(start),
                    "nm_id": num_series(get_col(df, "nm_id")),
                    "supplier_article": get_col(df, "supplier_article").map(clean_article),
                    "subject": get_col(df, "subject").map(normalize_text),
                    "gross_profit": num_series(get_col(df, "gross_profit")).fillna(0),
                    "gross_revenue": num_series(get_col(df, "gross_revenue")).fillna(0),
                    "orders": num_series(get_col(df, "orders")).fillna(0),
                    "source_file": key,
                })
                out["product"] = out["supplier_article"].map(product_code)
                if is_month_file(start, end) and start.year == current_year:
                    monthly_frames.append(out)
                else:
                    weekly_frames.append(out)
            except Exception as exc:
                self.diag.add("ERROR", "abc", f"Не прочитан ABC {key}", exc)
        weekly = pd.concat(weekly_frames, ignore_index=True) if weekly_frames else pd.DataFrame()
        monthly = pd.concat(monthly_frames, ignore_index=True) if monthly_frames else pd.DataFrame()
        self._log("abc_weekly", weekly, "period_start")
        self._log("abc_monthly", monthly, "period_start")
        return weekly, monthly

    def load_economics(self) -> pd.DataFrame:
        candidates = [
            self.path("Финансовые показатели", self.store, "Экономика.xlsx"),
            self.path("Финансовые показатели", self.store, "Недельные", "Экономика.xlsx"),
        ]
        files = [f for f in candidates if self.storage.exists(f)]
        frames = []
        for key, data in self._read_candidates(files):
            try:
                df = read_excel_table(data, "Юнит экономика")
                if df.empty:
                    continue
                out = pd.DataFrame({
                    "week_code": get_col(df, "week").map(normalize_text),
                    "nm_id": num_series(get_col(df, "nm_id")),
                    "supplier_article": get_col(df, "supplier_article").map(clean_article),
                    "subject": get_col(df, "subject").map(normalize_text),
                    "commission_pct": num_series(get_col(df, "commission_pct")),
                    "acquiring_pct": num_series(get_col(df, "acquiring_pct")),
                    "logistics_direct": num_series(get_col(df, "logistics_direct")),
                    "logistics_return": num_series(get_col(df, "logistics_return")),
                    "storage": num_series(get_col(df, "storage")),
                    "other_costs": num_series(get_col(df, "other_costs")),
                    "cost": num_series(get_col(df, "cost")),
                    "source_file": key,
                })
                out["product"] = out["supplier_article"].map(product_code)
                frames.append(out)
            except Exception as exc:
                self.diag.add("ERROR", "economics", f"Не прочитана экономика {key}", exc)
        out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        self._log("economics", out)
        return out

    def load_all(self) -> DataPack:
        orders = self.load_orders()
        funnel = self.load_funnel()
        ads_raw, ads_daily, campaigns = self.load_ads()

        # Preliminary latest date is known before heavy Stage 2 sources.
        # Use it to read only the recent 90-110 day window and avoid silent 20+ minute parsing of old files.
        pre_candidates = []
        for df, col in [(orders, "day"), (funnel, "day"), (ads_daily, "day")]:
            if not df.empty and col in df.columns:
                mx = pd.to_datetime(df[col], errors="coerce").max()
                if pd.notna(mx):
                    pre_candidates.append(pd.Timestamp(mx).normalize())
        preliminary_latest_day = max(pre_candidates) if pre_candidates else pd.Timestamp(datetime.today().date())
        log(f"preliminary_latest_day: {preliminary_latest_day.date()}")

        search_queries = self.load_search_queries(preliminary_latest_day)
        entry_points = self.load_entry_points(preliminary_latest_day)
        stock = self.load_stock(preliminary_latest_day)
        economics = self.load_economics()
        candidates = []
        for df, col in [(orders, "day"), (funnel, "day"), (ads_daily, "day"), (search_queries, "day"), (stock, "day")]:
            if not df.empty and col in df.columns:
                mx = pd.to_datetime(df[col], errors="coerce").max()
                if pd.notna(mx):
                    candidates.append(pd.Timestamp(mx).normalize())
        latest_day = max(candidates) if candidates else preliminary_latest_day
        abc_weekly, abc_monthly = self.load_abc(latest_day.year)
        if not abc_weekly.empty:
            latest_day = max(latest_day, pd.to_datetime(abc_weekly["period_end"], errors="coerce").max())
        return DataPack(
            orders=orders, funnel=funnel, ads_daily=ads_daily, ads_raw=ads_raw, campaigns=campaigns,
            search_queries=search_queries, entry_points=entry_points, stock=stock, abc_weekly=abc_weekly,
            abc_monthly=abc_monthly, economics=economics, latest_day=pd.Timestamp(latest_day).normalize(), diagnostics=self.diag,
        )


# ------------------------- analytics -------------------------
class AnalyticsBuilder:
    def __init__(self, pack: DataPack):
        self.pack = pack
        self.diag = pack.diagnostics
        self.latest_day = pack.latest_day
        self.cutoff_90 = self.latest_day - pd.Timedelta(days=89)
        # Last full week. If latest day is Sunday, current week is full; otherwise previous Mon-Sun.
        last_monday = self.latest_day - pd.Timedelta(days=int(self.latest_day.weekday()))
        if self.latest_day.weekday() == 6:
            self.week_start = last_monday
            self.week_end = self.latest_day
        else:
            self.week_start = last_monday - pd.Timedelta(days=7)
            self.week_end = last_monday - pd.Timedelta(days=1)
        self.dictionary = self.build_dictionary()

    def build_dictionary(self) -> pd.DataFrame:
        frames = []
        for name in ["orders", "funnel", "ads_raw", "search_queries", "entry_points", "stock", "abc_weekly", "abc_monthly", "economics"]:
            df = getattr(self.pack, name)
            if df is None or df.empty:
                continue
            x = df.copy()
            for col in ["subject", "product", "supplier_article", "nm_id"]:
                if col not in x.columns:
                    x[col] = "" if col != "nm_id" else np.nan
            x["supplier_article"] = x["supplier_article"].map(clean_article)
            x["subject"] = x["subject"].map(normalize_text)
            x["nm_id"] = num_series(x["nm_id"])
            x["product"] = x["supplier_article"].map(product_code).where(x["product"].map(normalize_text).eq(""), x["product"].map(normalize_text))
            x["source"] = name
            frames.append(x[["subject", "product", "supplier_article", "nm_id", "source"]])
        if not frames:
            return pd.DataFrame(columns=["subject", "product", "supplier_article", "nm_id", "source"])
        d = pd.concat(frames, ignore_index=True)
        d = d[d["subject"].isin(TARGET_SUBJECTS)]
        d = d[d["supplier_article"].ne("") & d["product"].ne("")]
        d = d[~d["supplier_article"].map(is_excluded_article)]
        d = d[d["product"].map(is_valid_product_code)]
        d = d.drop_duplicates(["supplier_article", "nm_id"])
        log(f"dictionary: rows={len(d):,}, articles={d['supplier_article'].nunique():,}, nm_ids={d['nm_id'].nunique(dropna=True):,}")
        return d

    def enrich(self, df: pd.DataFrame, source: str = "") -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        out = df.copy()
        for col in ["subject", "product", "supplier_article", "nm_id"]:
            if col not in out.columns:
                out[col] = "" if col != "nm_id" else np.nan
        if "nm_id" in out.columns and not self.dictionary.empty:
            d_nm = self.dictionary.dropna(subset=["nm_id"]).drop_duplicates("nm_id")[["nm_id", "subject", "product", "supplier_article"]]
            out = out.merge(d_nm, on="nm_id", how="left", suffixes=("", "_dict"))
            for col in ["subject", "product", "supplier_article"]:
                out[col] = out[col].where(out[col].map(normalize_text).ne(""), out[f"{col}_dict"])
            out = out.drop(columns=[c for c in out.columns if c.endswith("_dict")], errors="ignore")
        out["supplier_article"] = out["supplier_article"].map(clean_article)
        out["subject"] = out["subject"].map(normalize_text)
        out["product"] = out["product"].map(normalize_text).where(out["product"].map(normalize_text).ne(""), out["supplier_article"].map(product_code))
        out = out[out["subject"].isin(TARGET_SUBJECTS)]
        out = out[out["supplier_article"].ne("") & out["product"].ne("")]
        out = out[~out["supplier_article"].map(is_excluded_article)]
        out = out[out["product"].map(is_valid_product_code)]
        return out

    def buyout_rates(self) -> pd.DataFrame:
        f = self.enrich(self.pack.funnel, "funnel")
        if f.empty:
            return pd.DataFrame()
        f90 = f[(f["day"] >= self.cutoff_90) & (f["day"] <= self.latest_day)].copy()
        g = f90.groupby(["subject", "product", "supplier_article", "nm_id"], dropna=False, as_index=False).agg(
            orders_90=("orders", "sum"), buyouts_90=("buyouts_count", "sum"), cancels_90=("cancels_count", "sum"),
        )
        g["resolved_90"] = g["buyouts_90"].fillna(0) + g["cancels_90"].fillna(0)
        g["buyout_pct_90"] = np.where(g["resolved_90"] > 0, g["buyouts_90"] / g["resolved_90"], np.nan)
        g["buyout_pct_wrong_orders"] = np.where(g["orders_90"] > 0, g["buyouts_90"] / g["orders_90"], np.nan)
        # fallback by product, then category.
        prod = g.groupby(["subject", "product"], as_index=False).agg(prod_buyouts=("buyouts_90", "sum"), prod_cancels=("cancels_90", "sum"))
        prod["product_buyout_pct_90"] = np.where(prod["prod_buyouts"] + prod["prod_cancels"] > 0, prod["prod_buyouts"] / (prod["prod_buyouts"] + prod["prod_cancels"]), np.nan)
        cat = g.groupby(["subject"], as_index=False).agg(cat_buyouts=("buyouts_90", "sum"), cat_cancels=("cancels_90", "sum"))
        cat["category_buyout_pct_90"] = np.where(cat["cat_buyouts"] + cat["cat_cancels"] > 0, cat["cat_buyouts"] / (cat["cat_buyouts"] + cat["cat_cancels"]), np.nan)
        g = g.merge(prod[["subject", "product", "product_buyout_pct_90"]], on=["subject", "product"], how="left").merge(cat[["subject", "category_buyout_pct_90"]], on="subject", how="left")
        g["used_buyout_pct_90"] = g["buyout_pct_90"].fillna(g["product_buyout_pct_90"]).fillna(g["category_buyout_pct_90"]).fillna(1.0)
        g["used_buyout_pct_90"] = g["used_buyout_pct_90"].clip(0, 1)
        return g

    def order_prices_daily(self) -> pd.DataFrame:
        orders = self.enrich(self.pack.orders, "orders")
        if orders.empty:
            return pd.DataFrame()
        orders = orders[(orders["day"] >= self.cutoff_90) & (orders["day"] <= self.latest_day)].copy()
        g = orders.groupby(["day", "subject", "product", "supplier_article", "nm_id"], dropna=False, as_index=False).agg(
            orders_rows=("orders", "sum"),
            finished_price=("finished_price", "mean"), price_with_disc=("price_with_disc", "mean"), spp=("spp", "mean"),
        )
        return g

    def funnel_daily(self) -> pd.DataFrame:
        f = self.enrich(self.pack.funnel, "funnel")
        if f.empty:
            return pd.DataFrame()
        f = f[(f["day"] >= self.cutoff_90) & (f["day"] <= self.latest_day)].copy()
        g = f.groupby(["day", "subject", "product", "supplier_article", "nm_id"], dropna=False, as_index=False).agg(
            orders=("orders", "sum"), order_sum=("order_sum", "sum"), open_cards=("open_cards", "sum"),
            add_to_cart=("add_to_cart", "sum"), buyouts_count=("buyouts_count", "sum"), cancels_count=("cancels_count", "sum"),
            finished_price_funnel=("finished_price", "mean"), spp_funnel=("spp", "mean"),
        )
        g["cart_conv_pct"] = np.where(g["open_cards"] > 0, g["add_to_cart"] / g["open_cards"] * 100, np.nan)
        g["order_conv_pct"] = np.where(g["add_to_cart"] > 0, g["orders"] / g["add_to_cart"] * 100, np.nan)
        return g

    def ads_daily_pivot(self) -> pd.DataFrame:
        ads = self.enrich(self.pack.ads_daily, "ads")
        if ads.empty:
            return pd.DataFrame()
        ads = ads[(ads["day"] >= self.cutoff_90) & (ads["day"] <= self.latest_day)].copy()
        # pivot manual/unified/unknown into columns.
        grouped = ads.groupby(["day", "subject", "product", "supplier_article", "nm_id", "ad_type"], dropna=False, as_index=False).agg(
            impressions=("impressions", "sum"), clicks=("clicks", "sum"), orders=("orders", "sum"),
            order_sum=("order_sum", "sum"), spend=("spend", "sum"),
        )
        rows = []
        for keys, part in grouped.groupby(["day", "subject", "product", "supplier_article", "nm_id"], dropna=False):
            rec = dict(zip(["day", "subject", "product", "supplier_article", "nm_id"], keys))
            for typ in ["manual", "unified", "unknown"]:
                p = part[part["ad_type"] == typ]
                imps, clicks, orders, order_sum, spend = [float(p[c].sum()) for c in ["impressions", "clicks", "orders", "order_sum", "spend"]]
                rec[f"{typ}_impressions"] = imps
                rec[f"{typ}_clicks"] = clicks
                rec[f"{typ}_orders"] = orders
                rec[f"{typ}_order_sum"] = order_sum
                rec[f"{typ}_spend"] = spend
                rec[f"{typ}_ctr_pct"] = clicks / imps * 100 if imps else np.nan
                rec[f"{typ}_cpc"] = spend / clicks if clicks else np.nan
                rec[f"{typ}_cr_pct"] = orders / clicks * 100 if clicks else np.nan
                rec[f"{typ}_drr_pct"] = spend / order_sum * 100 if order_sum else np.nan
            rows.append(rec)
        out = pd.DataFrame(rows)
        return out

    def search_daily_summary(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        q = self.enrich(self.pack.search_queries, "search_queries")
        if q.empty:
            return pd.DataFrame(), pd.DataFrame()
        q = q[(q["day"] >= self.cutoff_90) & (q["day"] <= self.latest_day)].copy()
        # Query data is already deduped by loader. Article-day summary.
        summary = q.groupby(["day", "subject", "product", "supplier_article", "nm_id"], dropna=False, as_index=False).agg(
            search_frequency=("frequency", "sum"), search_transitions=("transitions", "sum"),
            search_add_to_cart=("add_to_cart", "sum"), search_orders=("orders", "sum"),
            search_avg_position=("avg_position", lambda s: weighted_mean(s, q.loc[s.index, "orders"].fillna(0) + q.loc[s.index, "frequency"].fillna(0) / 1000)),
            search_median_position=("median_position", "mean"),
            rating_card=("rating_card", "mean"), rating_reviews=("rating_reviews", "mean"), visibility_pct=("visibility_pct", "mean"),
        )
        summary["search_traffic_capture_pct"] = np.where(summary["search_frequency"] > 0, summary["search_transitions"] / summary["search_frequency"] * 100, np.nan)
        # Core queries that give 80%+ orders over period.
        core_rows = []
        for keys, part in q.groupby(["subject", "product", "supplier_article", "nm_id"], dropna=False):
            part2 = part.groupby("search_query", as_index=False).agg(
                frequency=("frequency", "sum"), transitions=("transitions", "sum"), add_to_cart=("add_to_cart", "sum"), orders=("orders", "sum"),
                avg_position=("avg_position", lambda s: weighted_mean(s, part.loc[s.index, "orders"].fillna(0) + part.loc[s.index, "frequency"].fillna(0) / 1000)),
                median_position=("median_position", "mean"), rating_card=("rating_card", "mean"), rating_reviews=("rating_reviews", "mean"),
                visibility_pct=("visibility_pct", "mean"),
            ).sort_values("orders", ascending=False)
            total_orders = part2["orders"].sum()
            part2["orders_share_pct"] = np.where(total_orders > 0, part2["orders"] / total_orders * 100, 0)
            part2["cum_orders_share_pct"] = part2["orders_share_pct"].cumsum()
            if total_orders > 0:
                core = part2[(part2["cum_orders_share_pct"] <= 80) | (part2["orders_share_pct"] == part2["orders_share_pct"].max())].copy()
                # Ensure the row crossing 80 is included.
                crossing = part2[part2["cum_orders_share_pct"] > 80].head(1)
                core = pd.concat([core, crossing], ignore_index=True).drop_duplicates("search_query")
            else:
                core = part2.head(10).copy()
            for _, r in core.iterrows():
                rec = dict(zip(["subject", "product", "supplier_article", "nm_id"], keys))
                rec.update(r.to_dict())
                rec["traffic_capture_pct"] = r["transitions"] / r["frequency"] * 100 if r["frequency"] else np.nan
                core_rows.append(rec)
        core = pd.DataFrame(core_rows)
        return summary, core

    def entry_points_summary(self) -> pd.DataFrame:
        e = self.enrich(self.pack.entry_points, "entry_points")
        if e.empty:
            return pd.DataFrame()
        e = e[(e["day"].isna()) | ((e["day"] >= self.cutoff_90) & (e["day"] <= self.latest_day))].copy()
        g = e.groupby(["subject", "product", "supplier_article", "nm_id", "entry_section", "entry_point"], dropna=False, as_index=False).agg(
            impressions=("impressions", "sum"), transitions=("transitions", "sum"), add_to_cart=("add_to_cart", "sum"), orders=("orders", "sum"),
        )
        g["ctr_pct"] = np.where(g["impressions"] > 0, g["transitions"] / g["impressions"] * 100, np.nan)
        g["cart_conv_pct"] = np.where(g["transitions"] > 0, g["add_to_cart"] / g["transitions"] * 100, np.nan)
        g["order_conv_pct"] = np.where(g["add_to_cart"] > 0, g["orders"] / g["add_to_cart"] * 100, np.nan)
        totals = g.groupby(["subject", "product", "supplier_article", "nm_id"], dropna=False)["orders"].sum().rename("orders_total").reset_index()
        g = g.merge(totals, on=["subject", "product", "supplier_article", "nm_id"], how="left")
        g["orders_share_pct"] = np.where(g["orders_total"] > 0, g["orders"] / g["orders_total"] * 100, np.nan)
        return g.sort_values(["subject", "product", "supplier_article", "orders"], ascending=[True, True, True, False])

    def localization(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        stock = self.enrich(self.pack.stock, "stock")
        orders = self.enrich(self.pack.orders, "orders")
        if stock.empty or orders.empty:
            return pd.DataFrame(), pd.DataFrame()
        orders = orders[(orders["day"] >= self.cutoff_90) & (orders["day"] <= self.latest_day)].copy()
        orders["warehouse"] = orders["warehouse"].map(canonical_warehouse_name)
        orders = orders[orders["warehouse"].map(is_relevant_warehouse)].copy()
        stock["warehouse"] = stock["warehouse"].map(canonical_warehouse_name)
        stock = stock[stock["warehouse"].map(is_relevant_warehouse)].copy()
        if stock.empty or orders.empty:
            return pd.DataFrame(), pd.DataFrame()
        weights = orders.groupby(["subject", "product", "supplier_article", "nm_id", "warehouse"], dropna=False, as_index=False).agg(orders_90=("orders", "sum"))
        totals = weights.groupby(["subject", "product", "supplier_article", "nm_id"], dropna=False)["orders_90"].sum().rename("orders_total").reset_index()
        weights = weights.merge(totals, on=["subject", "product", "supplier_article", "nm_id"], how="left")
        weights["warehouse_weight_pct"] = np.where(weights["orders_total"] > 0, weights["orders_90"] / weights["orders_total"] * 100, 0)
        # Оставляем ключевые склады, которые суммарно дают 97% заказов артикула.
        weights = weights.sort_values(["subject", "product", "supplier_article", "nm_id", "warehouse_weight_pct"], ascending=[True, True, True, True, False])
        weights["cum_weight_pct"] = weights.groupby(["subject", "product", "supplier_article", "nm_id"], dropna=False)["warehouse_weight_pct"].cumsum()
        weights = weights[(weights["cum_weight_pct"] <= 97) | (weights["warehouse_weight_pct"] >= 0.5)].copy()
        weights["avg_daily_orders_wh"] = weights["orders_90"] / 90.0
        weights["needed_stock_2d"] = weights["avg_daily_orders_wh"] * 2
        weights["warehouse_pool"] = weights["warehouse"].map(warehouse_pool)
        # Last stock date per article/warehouse.
        stock = stock[stock["day"].notna()].copy()
        stock = stock.sort_values("day").groupby(["subject", "product", "supplier_article", "nm_id", "warehouse"], dropna=False, as_index=False).tail(1)
        st = stock.groupby(["subject", "product", "supplier_article", "nm_id", "warehouse"], dropna=False, as_index=False).agg(stock_qty=("stock", "sum"), stock_day=("day", "max"))
        detail = weights.merge(st, on=["subject", "product", "supplier_article", "nm_id", "warehouse"], how="left")
        detail["stock_qty"] = detail["stock_qty"].fillna(0)
        detail["is_direct_covered"] = detail["stock_qty"] >= detail["needed_stock_2d"]
        # Replacement: same regional pool has enough stock in aggregate.
        pool_stock = detail.groupby(["subject", "product", "supplier_article", "nm_id", "warehouse_pool"], dropna=False)["stock_qty"].sum().rename("pool_stock_qty").reset_index()
        pool_need = detail.groupby(["subject", "product", "supplier_article", "nm_id", "warehouse_pool"], dropna=False)["needed_stock_2d"].sum().rename("pool_need_qty").reset_index()
        detail = detail.merge(pool_stock, on=["subject", "product", "supplier_article", "nm_id", "warehouse_pool"], how="left").merge(pool_need, on=["subject", "product", "supplier_article", "nm_id", "warehouse_pool"], how="left")
        detail["is_covered_with_replacement"] = detail["is_direct_covered"] | (detail["pool_stock_qty"] >= detail["needed_stock_2d"])
        detail["direct_coverage_weight_pct"] = np.where(detail["is_direct_covered"], detail["warehouse_weight_pct"], 0)
        detail["replacement_coverage_weight_pct"] = np.where(detail["is_covered_with_replacement"], detail["warehouse_weight_pct"], 0)
        summary = detail.groupby(["subject", "product", "supplier_article", "nm_id"], dropna=False, as_index=False).agg(
            direct_localization_pct=("direct_coverage_weight_pct", "sum"),
            localization_with_replacements_pct=("replacement_coverage_weight_pct", "sum"),
            stock_qty_total=("stock_qty", "sum"),
            key_warehouses=("warehouse", "nunique"),
            stock_day=("stock_day", "max"),
        )
        summary["localization_status"] = np.select(
            [summary["localization_with_replacements_pct"] >= 85, summary["localization_with_replacements_pct"] >= 60, summary["localization_with_replacements_pct"] >= 30],
            ["Норма", "Риск", "Плохая локализация"], default="Критично",
        )
        uncovered = detail[~detail["is_covered_with_replacement"]].groupby(["supplier_article", "nm_id"], dropna=False)["warehouse"].apply(lambda s: "; ".join(s.astype(str).head(8))).rename("uncovered_warehouses").reset_index()
        summary = summary.merge(uncovered, on=["supplier_article", "nm_id"], how="left")
        return detail, summary

    def gross_profit_potential(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        abc = self.enrich(self.pack.abc_weekly, "abc_weekly")
        if abc.empty:
            return pd.DataFrame(), pd.DataFrame()
        abc = abc[(abc["period_end"] >= self.cutoff_90) & (abc["period_start"] <= self.latest_day)].copy()
        abc["days_in_period"] = (pd.to_datetime(abc["period_end"]) - pd.to_datetime(abc["period_start"])).dt.days + 1
        abc["gp_per_day"] = np.where(abc["days_in_period"] > 0, abc["gross_profit"] / abc["days_in_period"], np.nan)
        weekly = abc[["subject", "product", "supplier_article", "nm_id", "week_code", "week_label", "period_start", "period_end", "gross_profit", "gp_per_day", "orders", "gross_revenue"]].copy()
        rows = []
        prev_month = (self.latest_day.to_period("M") - 1).strftime("%Y-%m")
        cur_month = self.latest_day.to_period("M").strftime("%Y-%m")
        for keys, part in weekly.groupby(["subject", "product", "supplier_article", "nm_id"], dropna=False):
            values = part["gp_per_day"].dropna()
            avg = values.mean() if len(values) else np.nan
            above = values[values > avg]
            target = above.mean() if len(above) else avg
            best = values.max() if len(values) else np.nan
            prev_gp = part.loc[pd.to_datetime(part["period_start"]).dt.to_period("M").astype(str) == prev_month, "gross_profit"].sum()
            cur_gp = part.loc[pd.to_datetime(part["period_start"]).dt.to_period("M").astype(str) == cur_month, "gross_profit"].sum()
            plan = prev_gp * 1.1 if prev_gp else cur_gp
            elapsed = min(self.latest_day.day, calendar.monthrange(self.latest_day.year, self.latest_day.month)[1])
            plan_to_date = plan / calendar.monthrange(self.latest_day.year, self.latest_day.month)[1] * elapsed if plan else np.nan
            rec = dict(zip(["subject", "product", "supplier_article", "nm_id"], keys))
            rec.update({
                "weeks_count": part["week_code"].nunique(), "gross_profit_90d": part["gross_profit"].sum(),
                "avg_gp_per_day": avg, "target_gp_per_day": target, "best_week_gp_per_day": best,
                "prev_month_gross_profit": prev_gp, "plan_month": plan, "current_month_gross_profit": cur_gp,
                "plan_completion_pct": cur_gp / plan * 100 if plan else np.nan,
                "plan_to_date": plan_to_date,
                "plan_to_date_completion_pct": cur_gp / plan_to_date * 100 if plan_to_date else np.nan,
            })
            rows.append(rec)
        return weekly, pd.DataFrame(rows)

    def article_day_fact(self) -> pd.DataFrame:
        base = self.funnel_daily()
        if base.empty:
            base = self.order_prices_daily()
        prices = self.order_prices_daily()
        ads = self.ads_daily_pivot()
        search_summary, _ = self.search_daily_summary()
        loc_detail, loc_summary = self.localization()
        gp_weekly, gp_potential = self.gross_profit_potential()
        buyouts = self.buyout_rates()
        # Merge all by day+article.
        out = base.copy()
        if out.empty:
            return pd.DataFrame()
        keys = ["day", "subject", "product", "supplier_article", "nm_id"]
        for df in [prices, ads, search_summary]:
            if df is not None and not df.empty:
                out = out.merge(df, on=keys, how="outer", suffixes=("", "_dup"))
                for c in [c for c in out.columns if c.endswith("_dup")]:
                    basec = c[:-4]
                    if basec in out.columns:
                        out[basec] = out[basec].fillna(out[c])
                    out = out.drop(columns=[c])
        if not buyouts.empty:
            out = out.merge(buyouts[["supplier_article", "nm_id", "used_buyout_pct_90", "buyout_pct_90", "buyout_pct_wrong_orders"]], on=["supplier_article", "nm_id"], how="left")
        if not loc_summary.empty:
            out = out.merge(loc_summary[["supplier_article", "nm_id", "direct_localization_pct", "localization_with_replacements_pct", "localization_status", "stock_qty_total"]], on=["supplier_article", "nm_id"], how="left")
        # Fill price fields
        out["finished_price"] = out.get("finished_price", np.nan)
        if "finished_price_funnel" in out.columns:
            out["finished_price"] = out["finished_price"].fillna(out["finished_price_funnel"])
        out["spp"] = out.get("spp", np.nan)
        if "spp_funnel" in out.columns:
            out["spp"] = out["spp"].fillna(out["spp_funnel"])
        # General traffic capture from funnel opens vs search demand.
        search_freq = out["search_frequency"].fillna(0) if "search_frequency" in out.columns else pd.Series([0] * len(out), index=out.index)
        open_cards = out["open_cards"].fillna(0) if "open_cards" in out.columns else pd.Series([0] * len(out), index=out.index)
        out["total_traffic_capture_pct"] = np.where(search_freq > 0, open_cards / search_freq * 100, np.nan)
        # Gross profit forecast using economics and buyout.
        econ = self.enrich(self.pack.economics, "economics")
        if not econ.empty:
            econ_latest = econ.sort_values("week_code").drop_duplicates(["supplier_article", "nm_id"], keep="last")
            out = out.merge(econ_latest[["supplier_article", "nm_id", "commission_pct", "acquiring_pct", "logistics_direct", "logistics_return", "storage", "other_costs", "cost"]], on=["supplier_article", "nm_id"], how="left")
        for c in ["orders", "order_sum", "open_cards", "add_to_cart", "manual_spend", "unified_spend", "unknown_spend"]:
            if c not in out.columns:
                out[c] = 0
        out["used_buyout_pct_90"] = out.get("used_buyout_pct_90", np.nan).fillna(1.0)
        out["buyout_qty_model"] = out["orders"].fillna(0) * out["used_buyout_pct_90"]
        # Formula: revenue = order_sum * buyout pct. Direct logistics from all orders, others as agreed.
        out["revenue_model"] = out["order_sum"].fillna(0) * out["used_buyout_pct_90"]
        for c in ["commission_pct", "acquiring_pct", "logistics_direct", "logistics_return", "storage", "other_costs", "cost"]:
            if c not in out.columns:
                out[c] = 0
            out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0)
        out["commission_model"] = out["revenue_model"] * out["commission_pct"] / 100
        out["acquiring_model"] = out["revenue_model"] * out["acquiring_pct"] / 100
        out["logistics_direct_model"] = out["orders"].fillna(0) * out["logistics_direct"]
        out["logistics_return_model"] = out["buyout_qty_model"] * out["logistics_return"]
        out["storage_model"] = out["buyout_qty_model"] * out["storage"]
        out["other_costs_model"] = out["buyout_qty_model"] * out["other_costs"]
        out["cost_model"] = out["buyout_qty_model"] * out["cost"]
        ad_spend_cols = [c for c in ["manual_spend", "unified_spend", "unknown_spend"] if c in out.columns]
        out["ad_spend_model"] = out[ad_spend_cols].sum(axis=1) if ad_spend_cols else 0
        out["gross_profit_model"] = out["revenue_model"] - out["commission_model"] - out["acquiring_model"] - out["logistics_direct_model"] - out["logistics_return_model"] - out["storage_model"] - out["other_costs_model"] - out["cost_model"] - out["ad_spend_model"]
        out = out.sort_values(["subject", "product", "supplier_article", "day"])
        return out

    @staticmethod
    def target_value(series: pd.Series) -> float:
        s = pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        if s.empty:
            return np.nan
        avg = s.mean()
        above = s[s > avg]
        return float(above.mean()) if not above.empty else float(avg)

    def metrics_summary(self, daily: pd.DataFrame) -> pd.DataFrame:
        if daily.empty:
            return pd.DataFrame()
        metric_defs = [
            ("orders", "Заказы в день"), ("order_sum", "Сумма заказов"), ("gross_profit_model", "Валовая прибыль модель"),
            ("open_cards", "Открытия карточки / клики"), ("add_to_cart", "Добавления в корзину"),
            ("cart_conv_pct", "Конверсия в корзину, %"), ("order_conv_pct", "Конверсия в заказ, %"),
            ("finished_price", "finishedPrice"), ("spp", "СПП, %"),
            ("manual_impressions", "manual показы"), ("manual_clicks", "manual клики"), ("manual_ctr_pct", "manual CTR, %"), ("manual_cpc", "manual CPC"), ("manual_drr_pct", "manual ДРР, %"),
            ("unified_impressions", "unified показы"), ("unified_clicks", "unified клики"), ("unified_ctr_pct", "unified CTR, %"), ("unified_cpc", "unified CPC"), ("unified_drr_pct", "unified ДРР, %"),
            ("search_frequency", "Спрос / частотность"), ("search_transitions", "Переходы из поиска"), ("search_traffic_capture_pct", "% поискового трафика"), ("total_traffic_capture_pct", "% общего захвата спроса"),
            ("search_avg_position", "Средняя позиция"), ("rating_card", "Рейтинг карточки"), ("rating_reviews", "Рейтинг отзывов"),
            ("direct_localization_pct", "Прямая локализация, %"), ("localization_with_replacements_pct", "Локализация с заменами, %"),
        ]
        rows = []
        for keys, part in daily.groupby(["subject", "product", "supplier_article", "nm_id"], dropna=False):
            avg_order_sum = pd.to_numeric(part.get("order_sum", pd.Series(dtype=float)), errors="coerce").mean()
            best_days = part[pd.to_numeric(part.get("order_sum", 0), errors="coerce") > avg_order_sum].copy() if pd.notna(avg_order_sum) else part.iloc[0:0]
            last_week = part[(part["day"] >= self.week_start) & (part["day"] <= self.week_end)].copy()
            for col, label in metric_defs:
                if col not in part.columns:
                    continue
                s = pd.to_numeric(part[col], errors="coerce")
                nonzero = s.replace(0, np.nan)
                rec = dict(zip(["subject", "product", "supplier_article", "nm_id"], keys))
                rec.update({
                    "metric": label,
                    "avg_90d_all_days": s.mean(),
                    "avg_90d_nonzero_days": nonzero.mean(),
                    "target_above_mean_90d": self.target_value(nonzero),
                    "best_days_avg": pd.to_numeric(best_days[col], errors="coerce").mean() if col in best_days.columns and not best_days.empty else np.nan,
                    "last_full_week_avg": pd.to_numeric(last_week[col], errors="coerce").mean() if col in last_week.columns and not last_week.empty else np.nan,
                    "last_full_week_sum": pd.to_numeric(last_week[col], errors="coerce").sum() if col in last_week.columns and not last_week.empty else np.nan,
                    "days_count": part["day"].nunique(),
                    "best_days_count": best_days["day"].nunique(),
                })
                target = rec["target_above_mean_90d"]
                fact = rec["last_full_week_avg"]
                rec["gap_to_target_pct"] = (fact / target - 1) * 100 if pd.notna(target) and target != 0 and pd.notna(fact) else np.nan
                rows.append(rec)
        return pd.DataFrame(rows)

    def best_days(self, daily: pd.DataFrame) -> pd.DataFrame:
        rows = []
        if daily.empty:
            return pd.DataFrame()
        for keys, part in daily.groupby(["subject", "product", "supplier_article", "nm_id"], dropna=False):
            avg_order_sum = pd.to_numeric(part["order_sum"], errors="coerce").mean() if "order_sum" in part.columns else np.nan
            best = part[pd.to_numeric(part.get("order_sum", 0), errors="coerce") > avg_order_sum].copy() if pd.notna(avg_order_sum) else part.head(0)
            best = best.sort_values("order_sum", ascending=False).head(15)
            for _, r in best.iterrows():
                rows.append({
                    "subject": keys[0], "product": keys[1], "supplier_article": keys[2], "nm_id": keys[3],
                    "day": r.get("day"), "orders": r.get("orders"), "order_sum": r.get("order_sum"), "gross_profit_model": r.get("gross_profit_model"),
                    "finished_price": r.get("finished_price"), "spp": r.get("spp"), "open_cards": r.get("open_cards"), "add_to_cart": r.get("add_to_cart"),
                    "cart_conv_pct": r.get("cart_conv_pct"), "order_conv_pct": r.get("order_conv_pct"),
                    "manual_ctr_pct": r.get("manual_ctr_pct"), "manual_drr_pct": r.get("manual_drr_pct"),
                    "unified_ctr_pct": r.get("unified_ctr_pct"), "unified_drr_pct": r.get("unified_drr_pct"),
                    "search_frequency": r.get("search_frequency"), "search_traffic_capture_pct": r.get("search_traffic_capture_pct"),
                    "total_traffic_capture_pct": r.get("total_traffic_capture_pct"), "search_avg_position": r.get("search_avg_position"),
                    "rating_reviews": r.get("rating_reviews"), "localization_with_replacements_pct": r.get("localization_with_replacements_pct"),
                })
        return pd.DataFrame(rows)

    def price_ranges(self, daily: pd.DataFrame) -> pd.DataFrame:
        if daily.empty or "finished_price" not in daily.columns:
            return pd.DataFrame()
        df = daily.copy()
        def bucket(p):
            if pd.isna(p):
                return "нет цены"
            step = 10 if p < 300 else 20 if p < 700 else 50
            lo = math.floor(p / step) * step
            hi = lo + step
            return f"{lo:.0f}-{hi:.0f}"
        df["price_range"] = df["finished_price"].map(bucket)
        g = df.groupby(["subject", "product", "supplier_article", "nm_id", "price_range"], dropna=False, as_index=False).agg(
            days=("day", "nunique"), order_sum=("order_sum", "sum"), orders=("orders", "sum"),
            avg_finished_price=("finished_price", "mean"), avg_gross_profit=("gross_profit_model", "mean"),
            avg_drr_manual=("manual_drr_pct", "mean"), avg_drr_unified=("unified_drr_pct", "mean"),
            avg_cart_conv_pct=("cart_conv_pct", "mean"), avg_order_conv_pct=("order_conv_pct", "mean"),
        )
        # Mark recommended range: max order_sum per article.
        g["is_recommended"] = False
        idx = g.groupby(["supplier_article", "nm_id"], dropna=False)["order_sum"].idxmax()
        g.loc[idx.dropna().astype(int), "is_recommended"] = True
        return g.sort_values(["subject", "product", "supplier_article", "order_sum"], ascending=[True, True, True, False])

    def channel_summary(self, daily: pd.DataFrame) -> pd.DataFrame:
        rows = []
        if daily.empty:
            return pd.DataFrame()
        # средний чек нужен, чтобы оценить сумму заказов по точкам входа, где WB отдаёт только шт.
        avg_check = daily.copy()
        avg_check["avg_order_value"] = np.where(avg_check.get("orders", 0).fillna(0) > 0, avg_check.get("order_sum", 0).fillna(0) / avg_check.get("orders", 0).replace(0, np.nan), np.nan)
        avg_check_map = avg_check.groupby(["subject", "product", "supplier_article", "nm_id"], dropna=False)["avg_order_value"].mean().to_dict()
        ad_sums = {}
        for keys, part in daily.groupby(["subject", "product", "supplier_article", "nm_id"], dropna=False):
            ad_sums[keys] = {}
            for typ, name in [("manual", "Реклама manual: Поиск/Каталог"), ("unified", "Реклама unified: Карточка товара/полки"), ("unknown", "Реклама без типа ставки")]:
                imps = part.get(f"{typ}_impressions", pd.Series(dtype=float)).sum()
                clicks = part.get(f"{typ}_clicks", pd.Series(dtype=float)).sum()
                orders = part.get(f"{typ}_orders", pd.Series(dtype=float)).sum()
                order_sum = part.get(f"{typ}_order_sum", pd.Series(dtype=float)).sum()
                spend = part.get(f"{typ}_spend", pd.Series(dtype=float)).sum()
                ad_sums[keys][typ] = {"spend": spend, "impressions": imps, "clicks": clicks, "orders": orders, "order_sum": order_sum}
                rows.append({
                    "subject": keys[0], "product": keys[1], "supplier_article": keys[2], "nm_id": keys[3], "channel": name,
                    "channel_group": "Реклама", "impressions": imps, "clicks": clicks,
                    "ctr_pct": clicks / imps * 100 if imps else np.nan,
                    "orders": orders, "order_sum": order_sum, "estimated_order_sum": np.nan, "spend": spend,
                    "cpc": spend / clicks if clicks else np.nan,
                    "cr_pct": orders / clicks * 100 if clicks else np.nan,
                    "drr_pct": spend / order_sum * 100 if order_sum else np.nan,
                    "comment": "Факт по рекламному отчёту",
                })
        entry = self.entry_points_summary()
        entry_agg_rows = []
        if not entry.empty:
            entry["entry_channel"] = entry.apply(lambda r: classify_entry_channel(r.get("entry_section"), r.get("entry_point")), axis=1)
            # Детальные точки входа
            for _, r in entry.iterrows():
                keys = (r["subject"], r["product"], r["supplier_article"], r["nm_id"])
                aov = avg_check_map.get(keys, np.nan)
                est_sum = r["orders"] * aov if pd.notna(aov) else np.nan
                rows.append({
                    "subject": r["subject"], "product": r["product"], "supplier_article": r["supplier_article"], "nm_id": r["nm_id"],
                    "channel": f"Точка входа: {r['entry_section']} / {r['entry_point']}", "channel_group": r["entry_channel"],
                    "impressions": r["impressions"], "clicks": r["transitions"], "ctr_pct": np.nan,
                    "orders": r["orders"], "order_sum": np.nan, "estimated_order_sum": est_sum, "spend": np.nan,
                    "cpc": np.nan, "cr_pct": r["order_conv_pct"], "drr_pct": np.nan,
                    "orders_share_pct": r.get("orders_share_pct", np.nan),
                    "comment": "CTR для точек входа не сравниваем с рекламным CTR; показы/переходы могут быть разными сущностями",
                })
            # Агрегация каналов точек входа + привязка расходов рекламы для оценки ДРР канала
            ep = entry.groupby(["subject", "product", "supplier_article", "nm_id", "entry_channel"], dropna=False, as_index=False).agg(
                impressions=("impressions", "sum"), transitions=("transitions", "sum"), add_to_cart=("add_to_cart", "sum"), orders=("orders", "sum")
            )
            for _, r in ep.iterrows():
                keys = (r["subject"], r["product"], r["supplier_article"], r["nm_id"])
                aov = avg_check_map.get(keys, np.nan)
                est_sum = r["orders"] * aov if pd.notna(aov) else np.nan
                spend = np.nan
                if r["entry_channel"] == "Поиск/Каталог":
                    spend = ad_sums.get(keys, {}).get("manual", {}).get("spend", np.nan)
                    channel = "Канал Поиск/Каталог: заказы точки входа + расход manual"
                elif r["entry_channel"] == "Карточка товара / полки":
                    spend = ad_sums.get(keys, {}).get("unified", {}).get("spend", np.nan)
                    channel = "Канал Карточка товара/полки: заказы точки входа + расход unified"
                else:
                    channel = f"Канал {r['entry_channel']}: точки входа"
                rows.append({
                    "subject": r["subject"], "product": r["product"], "supplier_article": r["supplier_article"], "nm_id": r["nm_id"],
                    "channel": channel, "channel_group": r["entry_channel"],
                    "impressions": r["impressions"], "clicks": r["transitions"], "ctr_pct": np.nan,
                    "orders": r["orders"], "order_sum": np.nan, "estimated_order_sum": est_sum, "spend": spend,
                    "cpc": np.nan, "cr_pct": r["orders"] / r["transitions"] * 100 if r["transitions"] else np.nan,
                    "drr_pct": spend / est_sum * 100 if pd.notna(spend) and pd.notna(est_sum) and est_sum else np.nan,
                    "orders_share_pct": np.nan,
                    "comment": "ДРР канала оценочный: сумма заказов канала = заказы канала × средний чек артикула",
                })
        out = pd.DataFrame(rows)
        return out.sort_values(["subject", "product", "supplier_article", "orders"], ascending=[True, True, True, False]) if not out.empty else out

    def best_day_factors(self, daily: pd.DataFrame) -> pd.DataFrame:
        """Сравнение обычных дней и дней, где сумма заказов выше среднего."""
        if daily.empty:
            return pd.DataFrame()
        factors = [
            ("orders", "Заказы"), ("order_sum", "Сумма заказов"), ("open_cards", "Открытия карточки / клики"),
            ("add_to_cart", "Добавления в корзину"), ("cart_conv_pct", "Конверсия в корзину, %"), ("order_conv_pct", "Конверсия в заказ, %"),
            ("finished_price", "finishedPrice"), ("spp", "СПП, %"),
            ("manual_impressions", "Показы manual"), ("manual_clicks", "Клики manual"), ("manual_ctr_pct", "CTR manual, %"), ("manual_drr_pct", "ДРР manual, %"),
            ("unified_impressions", "Показы unified"), ("unified_clicks", "Клики unified"), ("unified_ctr_pct", "CTR unified, %"), ("unified_drr_pct", "ДРР unified, %"),
            ("search_frequency", "Спрос / частотность"), ("search_transitions", "Переходы из поиска"), ("search_traffic_capture_pct", "% поискового трафика"),
            ("total_traffic_capture_pct", "% общего захвата спроса"), ("search_avg_position", "Средняя позиция"),
            ("rating_reviews", "Рейтинг отзывов"), ("localization_with_replacements_pct", "Локализация с заменами, %"),
        ]
        rows = []
        for keys, part in daily.groupby(["subject", "product", "supplier_article", "nm_id"], dropna=False):
            order_sum = pd.to_numeric(part.get("order_sum", 0), errors="coerce")
            avg_order_sum = order_sum.mean()
            best = part[order_sum > avg_order_sum].copy() if pd.notna(avg_order_sum) else part.iloc[0:0]
            normal = part[~part.index.isin(best.index)].copy()
            if best.empty:
                continue
            for col, label in factors:
                if col not in part.columns:
                    continue
                best_avg = pd.to_numeric(best[col], errors="coerce").replace([np.inf, -np.inf], np.nan).mean()
                norm_avg = pd.to_numeric(normal[col], errors="coerce").replace([np.inf, -np.inf], np.nan).mean()
                diff = pct_gap(best_avg, norm_avg)
                if pd.isna(diff):
                    conclusion = "недостаточно данных"
                elif label in ["Средняя позиция", "ДРР manual, %", "ДРР unified, %"]:
                    conclusion = "лучше в сильные дни" if diff < -5 else "хуже в сильные дни" if diff > 5 else "примерно без изменений"
                else:
                    conclusion = "выше в сильные дни" if diff > 5 else "ниже в сильные дни" if diff < -5 else "примерно без изменений"
                rows.append({
                    "subject": keys[0], "product": keys[1], "supplier_article": keys[2], "nm_id": keys[3],
                    "factor": label, "normal_days_avg": norm_avg, "best_days_avg": best_avg,
                    "difference_pct": diff, "best_days_count": best["day"].nunique(), "conclusion": conclusion,
                })
        return pd.DataFrame(rows)

    def conclusions(self, summary: pd.DataFrame, daily: pd.DataFrame, loc_summary: pd.DataFrame) -> pd.DataFrame:
        if summary.empty:
            return pd.DataFrame()
        piv = summary.pivot_table(index=["subject", "product", "supplier_article", "nm_id"], columns="metric", values=["last_full_week_avg", "target_above_mean_90d", "gap_to_target_pct"], aggfunc="first")
        piv.columns = [f"{a}__{b}" for a, b in piv.columns]
        piv = piv.reset_index()
        if loc_summary is not None and not loc_summary.empty:
            piv = piv.merge(loc_summary[["supplier_article", "nm_id", "direct_localization_pct", "localization_with_replacements_pct", "localization_status", "uncovered_warehouses"]], on=["supplier_article", "nm_id"], how="left")
        rows = []
        for _, r in piv.iterrows():
            sales_gap = r.get("gap_to_target_pct__Сумма заказов", np.nan)
            if pd.notna(sales_gap) and sales_gap >= 5:
                status = "Опережаем целевой уровень"
            elif pd.notna(sales_gap) and sales_gap < -10:
                status = "Отстаём от целевого уровня"
            else:
                status = "Около целевого уровня"
            candidates = []
            def add_factor(name, severity, evidence, recommendation):
                if pd.notna(severity):
                    candidates.append((abs(float(severity)), name, evidence, recommendation))
            loc = r.get("localization_with_replacements_pct", np.nan)
            if status.startswith("Отстаём"):
                if pd.notna(loc) and loc < 85:
                    add_factor("Локализация / остатки", 85 - loc, f"локализация с заменами {loc:.1f}%", "восстановить остатки на ключевых складах и в региональных заменителях")
                traffic_gap = r.get("gap_to_target_pct__% поискового трафика", np.nan)
                if pd.notna(traffic_gap) and traffic_gap < -10:
                    add_factor("Забираем меньше поискового трафика", traffic_gap, f"% поискового трафика ниже цели на {abs(traffic_gap):.1f}%", "проверить позиции, SEO, ставки manual и релевантность ключей")
                demand_gap = r.get("gap_to_target_pct__Спрос / частотность", np.nan)
                if pd.notna(demand_gap) and demand_gap < -10:
                    add_factor("Общий спрос на WB ниже", demand_gap, f"частотность ниже цели на {abs(demand_gap):.1f}%", "сравнить с категорией и не завышать план на период низкого спроса")
                clicks_gap = r.get("gap_to_target_pct__Открытия карточки / клики", np.nan)
                if pd.notna(clicks_gap) and clicks_gap < -10:
                    add_factor("Меньше открытий карточки", clicks_gap, f"клики ниже цели на {abs(clicks_gap):.1f}%", "проверить выдачу, рекламу, CTR и карточку")
                cart_gap = r.get("gap_to_target_pct__Конверсия в корзину, %", np.nan)
                if pd.notna(cart_gap) and cart_gap < -10:
                    add_factor("Просела конверсия в корзину", cart_gap, f"конверсия в корзину ниже цели на {abs(cart_gap):.1f}%", "проверить фото, первый экран, цену, отзывы и УТП")
                order_gap = r.get("gap_to_target_pct__Конверсия в заказ, %", np.nan)
                if pd.notna(order_gap) and order_gap < -10:
                    add_factor("Просела конверсия в заказ", order_gap, f"конверсия в заказ ниже цели на {abs(order_gap):.1f}%", "проверить конечную цену, доставку, остатки и рейтинг")
                rating_gap = r.get("gap_to_target_pct__Рейтинг отзывов", np.nan)
                if pd.notna(rating_gap) and rating_gap < -2:
                    add_factor("Ухудшился рейтинг отзывов", rating_gap, f"рейтинг отзывов ниже нормы на {abs(rating_gap):.1f}%", "проверить свежие отзывы и причины снижения доверия")
            else:
                if status.startswith("Опережаем"):
                    add_factor("Сумма заказов выше цели", sales_gap if pd.notna(sales_gap) else 0, f"сумма заказов выше цели на {sales_gap:.1f}%" if pd.notna(sales_gap) else "сумма заказов выше цели", "зафиксировать условия лучших дней: цену, СПП, каналы, локализацию")
                else:
                    add_factor("Факт близок к цели", 1, "ключевые показатели около целевого уровня", "держать показатели не ниже средней планки")
            candidates = sorted(candidates, reverse=True)
            if candidates:
                main = candidates[0]
                secondary = candidates[1] if len(candidates) > 1 else None
                main_reason = f"{main[1]}: {main[2]}"
                second_reason = f"{secondary[1]}: {secondary[2]}" if secondary else "нет явной вторичной причины"
                recommendation = main[3]
                if secondary and secondary[3] != main[3]:
                    recommendation = recommendation + "; " + secondary[3]
            else:
                main_reason = "отклонение без явного единственного фактора"
                second_reason = "нужно смотреть блок факторов лучших дней"
                recommendation = "сравнить лучшие дни с обычными: цена, трафик, реклама, локализация"
            rec = r.to_dict()
            rec.update({
                "status": status,
                "main_reason": main_reason,
                "secondary_reason": second_reason,
                "recommendation": recommendation,
            })
            rows.append(rec)
        out = pd.DataFrame(rows)
        first = ["subject", "product", "supplier_article", "nm_id", "status", "main_reason", "secondary_reason", "recommendation", "localization_with_replacements_pct", "localization_status", "uncovered_warehouses"]
        rest = [c for c in out.columns if c not in first]
        return out[first + rest]

    def build_all(self) -> Dict[str, pd.DataFrame]:
        daily = self.article_day_fact()
        metrics = self.metrics_summary(daily)
        best = self.best_days(daily)
        best_factors = self.best_day_factors(daily)
        price = self.price_ranges(daily)
        channel = self.channel_summary(daily)
        search_summary, core_queries = self.search_daily_summary()
        entry = self.entry_points_summary()
        loc_detail, loc_summary = self.localization()
        gp_weekly, gp_potential = self.gross_profit_potential()
        buyout = self.buyout_rates()
        conclusions = self.conclusions(metrics, daily, loc_summary)
        return {
            "article_day_fact": daily,
            "metrics_summary_90d": metrics,
            "best_days": best,
            "best_day_factors": best_factors,
            "price_ranges": price,
            "channel_summary": channel,
            "search_daily_summary": search_summary,
            "core_queries_80": core_queries,
            "entry_points_summary": entry,
            "localization_detail": loc_detail,
            "localization_summary": loc_summary,
            "gp_potential_weekly_90d": gp_weekly,
            "gp_potential_90d": gp_potential,
            "buyout_validation": buyout,
            "conclusions": conclusions,
            "dictionary": self.dictionary,
            "diagnostics": self.diag.frame(),
        }




COLUMN_RU = {
    "subject": "Категория", "product": "Товар", "supplier_article": "Артикул продавца", "nm_id": "Артикул WB", "day": "Дата",
    "metric": "Показатель", "avg_90d_all_days": "Среднее за 90 дней", "avg_90d_nonzero_days": "Среднее по активным дням",
    "target_above_mean_90d": "Целевое значение", "best_days_avg": "Среднее в лучшие дни", "last_full_week_avg": "Среднее за последнюю полную неделю",
    "last_full_week_sum": "Сумма за последнюю полную неделю", "gap_to_target_pct": "Отклонение от цели, %", "days_count": "Дней в анализе", "best_days_count": "Лучших дней",
    "orders": "Заказы", "orders_rows": "Строк заказов", "order_sum": "Сумма заказов", "gross_profit_model": "Валовая прибыль модель",
    "open_cards": "Открытия карточки / клики", "add_to_cart": "Добавления в корзину", "cart_conv_pct": "Конверсия в корзину, %", "order_conv_pct": "Конверсия в заказ, %",
    "finished_price": "finishedPrice", "price_with_disc": "priceWithDisc", "spp": "СПП, %",
    "manual_impressions": "Показы manual", "manual_clicks": "Клики manual", "manual_orders": "Заказы manual", "manual_order_sum": "Сумма заказов manual", "manual_spend": "Расход manual", "manual_ctr_pct": "CTR manual, %", "manual_cpc": "CPC manual, ₽", "manual_cr_pct": "CR manual, %", "manual_drr_pct": "ДРР manual, %",
    "unified_impressions": "Показы unified", "unified_clicks": "Клики unified", "unified_orders": "Заказы unified", "unified_order_sum": "Сумма заказов unified", "unified_spend": "Расход unified", "unified_ctr_pct": "CTR unified, %", "unified_cpc": "CPC unified, ₽", "unified_cr_pct": "CR unified, %", "unified_drr_pct": "ДРР unified, %",
    "unknown_impressions": "Показы без типа", "unknown_clicks": "Клики без типа", "unknown_orders": "Заказы без типа", "unknown_order_sum": "Сумма заказов без типа", "unknown_spend": "Расход без типа", "unknown_ctr_pct": "CTR без типа, %", "unknown_cpc": "CPC без типа, ₽", "unknown_cr_pct": "CR без типа, %", "unknown_drr_pct": "ДРР без типа, %",
    "search_frequency": "Спрос / частотность", "search_transitions": "Переходы из поиска", "search_add_to_cart": "Добавления из поиска", "search_orders": "Заказы из поиска", "search_traffic_capture_pct": "% поискового трафика", "total_traffic_capture_pct": "% общего захвата спроса",
    "search_avg_position": "Средняя позиция", "search_median_position": "Медианная позиция", "visibility_pct": "Видимость, %", "rating_card": "Рейтинг карточки", "rating_reviews": "Рейтинг отзывов",
    "direct_localization_pct": "Прямая локализация, %", "localization_with_replacements_pct": "Локализация с заменами, %", "localization_status": "Статус локализации", "stock_qty_total": "Остаток всего", "uncovered_warehouses": "Непокрытые склады", "key_warehouses": "Ключевых складов", "stock_day": "Дата остатков",
    "price_range": "Диапазон finishedPrice", "days": "Дней", "avg_finished_price": "Средний finishedPrice", "avg_gross_profit": "Средняя валовая прибыль", "avg_drr_manual": "Средний ДРР manual, %", "avg_drr_unified": "Средний ДРР unified, %", "avg_cart_conv_pct": "Средняя конверсия в корзину, %", "avg_order_conv_pct": "Средняя конверсия в заказ, %", "is_recommended": "Рекомендуемый диапазон",
    "channel": "Канал", "channel_group": "Группа канала", "impressions": "Показы", "clicks": "Клики / переходы", "ctr_pct": "CTR, %", "spend": "Расход", "cpc": "CPC, ₽", "cr_pct": "CR, %", "drr_pct": "ДРР, %", "orders_share_pct": "Доля заказов, %", "estimated_order_sum": "Оценочная сумма заказов", "comment": "Комментарий",
    "entry_section": "Раздел", "entry_point": "Точка входа", "transitions": "Переходы", "frequency": "Частотность", "search_query": "Поисковый запрос", "traffic_capture_pct": "% трафика", "orders_share_pct": "Доля заказов, %", "cum_orders_share_pct": "Накопленная доля заказов, %", "avg_position": "Средняя позиция", "median_position": "Медианная позиция",
    "warehouse": "Склад", "orders_90": "Заказы за 90 дней", "orders_total": "Заказы всего", "warehouse_weight_pct": "Вес склада, %", "cum_weight_pct": "Накопленный вес, %", "avg_daily_orders_wh": "Средние заказы склада в день", "needed_stock_2d": "Нужно остатка на 2 дня", "warehouse_pool": "Региональный пул", "stock_qty": "Остаток", "is_direct_covered": "Покрыт напрямую", "is_covered_with_replacement": "Покрыт с заменой", "pool_stock_qty": "Остаток пула", "pool_need_qty": "Потребность пула", "direct_coverage_weight_pct": "Вклад прямого покрытия, %", "replacement_coverage_weight_pct": "Вклад покрытия с заменой, %",
    "week_code": "Неделя", "week_label": "Период недели", "period_start": "Начало периода", "period_end": "Конец периода", "gross_profit": "Валовая прибыль", "gross_revenue": "Валовая выручка", "gp_per_day": "ВП в день", "weeks_count": "Недель в анализе", "gross_profit_90d": "ВП за 90 дней", "avg_gp_per_day": "Средняя ВП/день", "target_gp_per_day": "Целевая ВП/день", "best_week_gp_per_day": "Лучшая неделя ВП/день", "prev_month_gross_profit": "ВП прошлого месяца", "plan_month": "План месяца", "current_month_gross_profit": "ВП текущего месяца", "plan_completion_pct": "Выполнение плана, %", "plan_to_date": "План на дату", "plan_to_date_completion_pct": "Выполнение плана на дату, %",
    "orders_90": "Заказали за 90 дней", "buyouts_90": "Выкупили за 90 дней", "cancels_90": "Отменили за 90 дней", "resolved_90": "Завершённые заказы", "buyout_pct_90": "% выкупа правильный", "buyout_pct_wrong_orders": "% выкупа старый ошибочный", "product_buyout_pct_90": "% выкупа товара", "category_buyout_pct_90": "% выкупа категории", "used_buyout_pct_90": "Использованный % выкупа",
    "factor": "Фактор", "normal_days_avg": "Обычные дни", "best_days_avg": "Лучшие дни", "difference_pct": "Разница, %", "conclusion": "Вывод",
    "status": "Статус", "main_reason": "Главная причина", "secondary_reason": "Вторичная причина", "recommendation": "Рекомендация",
    "source": "Источник", "source_file": "Файл-источник", "timestamp": "Время", "level": "Уровень", "message": "Сообщение", "details": "Детали",
}


def translate_col_name(col: Any) -> str:
    c = str(col)
    if c in COLUMN_RU:
        return COLUMN_RU[c]
    if "__" in c:
        left, right = c.split("__", 1)
        left_ru = COLUMN_RU.get(left, left)
        return f"{left_ru}: {right}"
    # Остаточные служебные имена переводим по частям.
    out = c
    replacements = {
        "pct": "%", "avg": "среднее", "target": "цель", "last_full_week": "последняя полная неделя",
        "order_sum": "сумма заказов", "gross_profit": "валовая прибыль", "localization": "локализация",
    }
    for a, b in replacements.items():
        out = out.replace(a, b)
    return out


def translate_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    x = df.copy()
    x = x.rename(columns={c: translate_col_name(c) for c in x.columns})
    return x

# ------------------------- export -------------------------
def autofit(ws) -> None:
    for col_idx in range(1, ws.max_column + 1):
        letter = get_column_letter(col_idx)
        max_len = 8
        for cell in ws[letter]:
            max_len = max(max_len, min(len(str(cell.value)) if cell.value is not None else 0, 60))
        ws.column_dimensions[letter].width = min(max_len + 2, 42)


def style_sheet(ws) -> None:
    for cell in ws[1]:
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = Alignment(horizontal="center", vertical="center")
    for row in ws.iter_rows():
        for cell in row:
            cell.border = BORDER
            if isinstance(cell.value, (int, float, np.number)) and not isinstance(cell.value, bool):
                header = str(ws.cell(1, cell.column).value or "")
                if "%" in header or "pct" in header.lower() or "ДРР" in header or "CTR" in header:
                    cell.number_format = '0.0'
                elif "цена" in header.lower() or "сум" in header.lower() or "ВП" in header or "profit" in header.lower() or "spend" in header.lower() or "выруч" in header.lower() or "order_sum" in header.lower():
                    cell.number_format = money_format()
                else:
                    cell.number_format = '# ##0.00'
    ws.freeze_panes = "A2"
    autofit(ws)


def write_df_sheet(wb: Workbook, name: str, df: pd.DataFrame) -> None:
    ws = wb.create_sheet(name[:31])
    if df is None or df.empty:
        ws.cell(1, 1, "Нет данных")
        return
    x = translate_df(df.copy())
    for c in x.columns:
        if pd.api.types.is_datetime64_any_dtype(x[c]):
            x[c] = x[c].dt.strftime("%Y-%m-%d")
    ws.append(list(x.columns))
    for row in x.itertuples(index=False, name=None):
        ws.append(list(row))
    style_sheet(ws)


def write_product_blocks(path: Path, title: str, outputs: Dict[str, pd.DataFrame], sections: List[Tuple[str, str, Optional[List[str]]]]) -> None:
    wb = Workbook()
    wb.remove(wb.active)
    # Determine products from metrics/conclusions/daily.
    base = None
    for key in ["metrics_summary_90d", "conclusions", "article_day_fact", "channel_summary"]:
        if key in outputs and outputs[key] is not None and not outputs[key].empty:
            base = outputs[key]
            break
    if base is None or base.empty:
        ws = wb.create_sheet("Нет данных")
        ws.cell(1, 1, "Нет данных")
        wb.save(path)
        return
    used = set()
    products = base[["subject", "product"]].drop_duplicates().sort_values(["subject", "product"]).itertuples(index=False, name=None)
    for subject, product in products:
        ws = wb.create_sheet(safe_sheet_name(str(product), used))
        row = 1
        ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=12)
        c = ws.cell(row, 1, f"{title}: товар {product} / {subject}")
        c.fill = TITLE_FILL
        c.font = Font(color="FFFFFF", bold=True, size=14)
        c.alignment = Alignment(horizontal="center")
        row += 2
        articles = base[(base["subject"] == subject) & (base["product"].astype(str) == str(product))]["supplier_article"].dropna().drop_duplicates().astype(str).sort_values().tolist()
        for art in articles:
            ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=12)
            ac = ws.cell(row, 1, f"Артикул {art}")
            ac.fill = SECTION_FILL
            ac.font = Font(bold=True, size=12)
            row += 1
            for section_title, key, cols in sections:
                df = outputs.get(key, pd.DataFrame())
                if df is None or df.empty or "supplier_article" not in df.columns:
                    continue
                part = df[(df["subject"] == subject) & (df["product"].astype(str) == str(product)) & (df["supplier_article"].astype(str) == art)].copy()
                if part.empty:
                    continue
                if cols:
                    keep = [c for c in cols if c in part.columns]
                    part = part[keep]
                if len(part) > 60:
                    part = part.head(60)
                part = translate_df(part)
                ws.cell(row, 1, section_title).fill = SUBSECTION_FILL
                ws.cell(row, 1).font = Font(bold=True)
                row += 1
                for col_idx, col in enumerate(part.columns, start=1):
                    cell = ws.cell(row, col_idx, col)
                    cell.fill = HEADER_FILL
                    cell.font = HEADER_FONT
                    cell.alignment = Alignment(horizontal="center")
                    cell.border = BORDER
                row += 1
                for rec in part.to_dict("records"):
                    for col_idx, col in enumerate(part.columns, start=1):
                        val = rec.get(col)
                        if isinstance(val, pd.Timestamp):
                            val = val.strftime("%Y-%m-%d")
                        cell = ws.cell(row, col_idx, val)
                        cell.border = BORDER
                        if isinstance(val, (int, float, np.number)) and not isinstance(val, bool):
                            if "%" in col or "pct" in col.lower() or "ДРР" in col or "CTR" in col:
                                cell.number_format = '0.0'
                            elif "цена" in col.lower() or "сум" in col.lower() or "profit" in col.lower() or "spend" in col.lower() or "ВП" in col:
                                cell.number_format = money_format()
                            else:
                                cell.number_format = '# ##0.00'
                    row += 1
                row += 2
            row += 1
        ws.freeze_panes = "A3"
        autofit(ws)
    wb.save(path)


def export_outputs(outputs: Dict[str, pd.DataFrame], local_dir: Path) -> List[Path]:
    local_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    # Main concise report
    wb = Workbook()
    ws = wb.active
    ws.title = "Сводка"
    ws.cell(1, 1, "Сводка по причинам и целям")
    ws.cell(1, 1).fill = TITLE_FILL
    ws.cell(1, 1).font = Font(color="FFFFFF", bold=True, size=14)
    cons = outputs.get("conclusions", pd.DataFrame())
    if not cons.empty:
        small_cols = [c for c in ["subject", "product", "supplier_article", "status", "main_reason", "recommendation", "localization_with_replacements_pct"] if c in cons.columns]
        x = translate_df(cons[small_cols].copy())
        ws.append(list(x.columns))
        for row in x.itertuples(index=False, name=None):
            ws.append(list(row))
        style_sheet(ws)
    p = local_dir / MAIN_REPORT_NAME
    wb.save(p)
    paths.append(p)
    # Technical report
    wb = Workbook()
    wb.remove(wb.active)
    for name in ["article_day_fact", "metrics_summary_90d", "best_days", "best_day_factors", "price_ranges", "channel_summary", "core_queries_80", "entry_points_summary", "localization_summary", "localization_detail", "gp_potential_90d", "buyout_validation", "dictionary", "diagnostics"]:
        write_df_sheet(wb, name[:31], outputs.get(name, pd.DataFrame()))
    p = local_dir / TECH_REPORT_NAME
    wb.save(p)
    paths.append(p)
    # Example 901
    wb = Workbook()
    wb.remove(wb.active)
    for name in ["article_day_fact", "metrics_summary_90d", "best_days", "price_ranges", "core_queries_80"]:
        df = outputs.get(name, pd.DataFrame())
        if df is not None and not df.empty and "supplier_article" in df.columns:
            df = df[df["supplier_article"].isin(EXAMPLE_ARTICLES)].copy()
        write_df_sheet(wb, name[:31], df)
    p = local_dir / EXAMPLE_REPORT_NAME
    wb.save(p)
    paths.append(p)
    # Product block reports
    potential_sections = [
        ("Средние и целевые значения", "metrics_summary_90d", ["metric", "avg_90d_all_days", "avg_90d_nonzero_days", "target_above_mean_90d", "best_days_avg", "last_full_week_avg", "gap_to_target_pct"]),
        ("Лучшие дни по сумме заказов", "best_days", None),
        ("Факторы лучших дней", "best_day_factors", ["factor", "normal_days_avg", "best_days_avg", "difference_pct", "best_days_count", "conclusion"]),
        ("Рекомендуемый ценовой диапазон", "price_ranges", ["price_range", "days", "order_sum", "orders", "avg_finished_price", "avg_gross_profit", "avg_drr_manual", "avg_drr_unified", "avg_cart_conv_pct", "avg_order_conv_pct", "is_recommended"]),
        ("Потенциал валовой прибыли по ABC", "gp_potential_90d", ["gross_profit_90d", "avg_gp_per_day", "target_gp_per_day", "best_week_gp_per_day", "prev_month_gross_profit", "plan_month", "current_month_gross_profit", "plan_to_date_completion_pct"]),
    ]
    p = local_dir / POTENTIAL_REPORT_NAME
    write_product_blocks(p, "Средние и целевые значения", outputs, potential_sections)
    paths.append(p)
    channel_sections = [
        ("Каналы продаж и реклама", "channel_summary", None),
        ("Точки входа", "entry_points_summary", None),
    ]
    p = local_dir / CHANNEL_REPORT_NAME
    write_product_blocks(p, "Каналы продаж и реклама", outputs, channel_sections)
    paths.append(p)
    search_sections = [
        ("Ключи, которые дают 80%+ заказов", "core_queries_80", None),
        ("Поиск по дням", "search_daily_summary", None),
    ]
    p = local_dir / SEARCH_REPORT_NAME
    write_product_blocks(p, "Поисковые запросы и позиции", outputs, search_sections)
    paths.append(p)
    loc_sections = [
        ("Сводка локализации", "localization_summary", None),
        ("Детализация складов", "localization_detail", None),
    ]
    p = local_dir / LOCALIZATION_REPORT_NAME
    write_product_blocks(p, "Локализация", outputs, loc_sections)
    paths.append(p)
    cons_sections = [
        ("Выводы", "conclusions", None),
        ("Факторы лучших дней", "best_day_factors", ["factor", "normal_days_avg", "best_days_avg", "difference_pct", "conclusion"]),
        ("Средние и целевые значения", "metrics_summary_90d", ["metric", "last_full_week_avg", "target_above_mean_90d", "gap_to_target_pct"]),
    ]
    p = local_dir / CONCLUSIONS_REPORT_NAME
    write_product_blocks(p, "Выводы по причинам", outputs, cons_sections)
    paths.append(p)
    return paths


def upload_to_storage(storage: Storage, local_paths: List[Path], root: str) -> None:
    for path in local_paths:
        rel = str(path.relative_to(Path(root))).replace("\\", "/") if Path(root) in path.parents or Path(root) == path.parent else f"{OUT_DIR}/{path.name}"
        try:
            storage.write_bytes(rel, path.read_bytes())
            log(f"Saved: {rel}")
        except Exception as exc:
            log(f"WARN: failed to save {rel}: {exc}")



# ------------------------- factor money bridge + PDF + Telegram -------------------------
FACTOR_REPORT_NAME = "Факторный_мост_ВП_TOPFACE.xlsx"
PDF_REPORT_NAME = "Управленческий_отчет_TOPFACE.pdf"


def _period_bounds_from_daily(daily: pd.DataFrame) -> Tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp, pd.Timestamp]:
    if daily is None or daily.empty or "day" not in daily.columns:
        today = pd.Timestamp(datetime.today().date())
        last_monday = today - pd.Timedelta(days=int(today.weekday()))
        if today.weekday() == 6:
            week_start, week_end = last_monday, today
        else:
            week_start, week_end = last_monday - pd.Timedelta(days=7), last_monday - pd.Timedelta(days=1)
        return week_start, week_end, week_start - pd.Timedelta(days=7), week_start - pd.Timedelta(days=1)
    mx = pd.to_datetime(daily["day"], errors="coerce").max()
    if pd.isna(mx):
        mx = pd.Timestamp(datetime.today().date())
    mx = pd.Timestamp(mx).normalize()
    last_monday = mx - pd.Timedelta(days=int(mx.weekday()))
    if mx.weekday() == 6:
        week_start, week_end = last_monday, mx
    else:
        week_start, week_end = last_monday - pd.Timedelta(days=7), last_monday - pd.Timedelta(days=1)
    return week_start, week_end, week_start - pd.Timedelta(days=7), week_start - pd.Timedelta(days=1)


def _agg_daily_for_bridge(daily: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp, group_cols: List[str]) -> pd.DataFrame:
    base_sum_cols = [
        "orders", "order_sum", "open_cards", "add_to_cart", "buyouts_count", "cancels_count",
        "manual_impressions", "manual_clicks", "manual_spend", "manual_orders", "manual_order_sum",
        "unified_impressions", "unified_clicks", "unified_spend", "unified_orders", "unified_order_sum",
        "unknown_impressions", "unknown_clicks", "unknown_spend", "unknown_orders", "unknown_order_sum",
        "search_frequency", "search_transitions", "search_add_to_cart", "search_orders",
        "ad_spend_model", "gross_profit_model", "buyout_qty_model", "revenue_model",
        "commission_model", "acquiring_model", "logistics_direct_model", "logistics_return_model",
        "storage_model", "other_costs_model", "cost_model"
    ]
    base_mean_cols = ["finished_price", "price_with_disc", "spp", "finished_price_funnel", "spp_funnel", "rating_reviews", "localization_with_replacements_pct"]
    derived_cols = ["ad_spend_total", "ad_clicks_total", "ad_impressions_total", "drr_pct", "cpc", "ctr_pct", "cart_conv_pct", "order_conv_pct", "card_to_order_pct", "search_traffic_capture_pct", "avg_order_price"]

    if daily is None or daily.empty:
        return pd.DataFrame(columns=group_cols + base_sum_cols + base_mean_cols + derived_cols)
    x = daily.copy()
    for c in group_cols:
        if c not in x.columns:
            x[c] = ""
    sum_cols = [c for c in base_sum_cols if c in x.columns]
    mean_cols = [c for c in base_mean_cols if c in x.columns]
    empty_cols = group_cols + sum_cols + mean_cols + derived_cols

    x["day"] = pd.to_datetime(x["day"], errors="coerce").dt.normalize()
    x = x[(x["day"] >= start) & (x["day"] <= end)].copy()
    if x.empty:
        return pd.DataFrame(columns=empty_cols)

    agg = {c: (c, "sum") for c in sum_cols}
    for c in mean_cols:
        agg[c] = (c, "mean")
    g = x.groupby(group_cols, dropna=False, as_index=False).agg(**agg)
    g["ad_spend_total"] = sum((g[c] if c in g.columns else 0) for c in ["manual_spend", "unified_spend", "unknown_spend", "ad_spend_model"])
    # Avoid double-count if ad_spend_model already includes manual+unified and channels exist.
    channel_spend = sum((g[c] if c in g.columns else 0) for c in ["manual_spend", "unified_spend", "unknown_spend"])
    if "ad_spend_model" in g.columns and float(pd.to_numeric(channel_spend, errors="coerce").fillna(0).sum()) > 0:
        g["ad_spend_total"] = channel_spend
    g["ad_clicks_total"] = sum((g[c] if c in g.columns else 0) for c in ["manual_clicks", "unified_clicks", "unknown_clicks"])
    g["ad_impressions_total"] = sum((g[c] if c in g.columns else 0) for c in ["manual_impressions", "unified_impressions", "unknown_impressions"])

    zero = pd.Series(0.0, index=g.index)
    def _num_col(col: str) -> pd.Series:
        return pd.to_numeric(g[col], errors="coerce").fillna(0) if col in g.columns else zero

    order_sum = _num_col("order_sum")
    orders = _num_col("orders")
    open_cards = _num_col("open_cards")
    add_to_cart = _num_col("add_to_cart")
    search_frequency = _num_col("search_frequency")
    search_transitions = _num_col("search_transitions")

    g["drr_pct"] = np.where(order_sum > 0, g["ad_spend_total"] / order_sum * 100, np.nan)
    g["cpc"] = np.where(g["ad_clicks_total"] > 0, g["ad_spend_total"] / g["ad_clicks_total"], np.nan)
    g["ctr_pct"] = np.where(g["ad_impressions_total"] > 0, g["ad_clicks_total"] / g["ad_impressions_total"] * 100, np.nan)
    g["cart_conv_pct"] = np.where(open_cards > 0, add_to_cart / open_cards * 100, np.nan)
    g["order_conv_pct"] = np.where(add_to_cart > 0, orders / add_to_cart * 100, np.nan)
    g["card_to_order_pct"] = np.where(open_cards > 0, orders / open_cards * 100, np.nan)
    g["search_traffic_capture_pct"] = np.where(search_frequency > 0, search_transitions / search_frequency * 100, np.nan)
    g["avg_order_price"] = np.where(orders > 0, order_sum / orders, np.nan)
    return g


def _abc_gp_for_period(builder: AnalyticsBuilder, start: pd.Timestamp, end: pd.Timestamp, group_cols: List[str]) -> pd.DataFrame:
    abc = builder.enrich(builder.pack.abc_weekly, "abc_weekly")
    if abc is None or abc.empty:
        return pd.DataFrame(columns=group_cols + ["gp_fact", "gross_revenue_fact", "sales_qty_fact"])
    x = abc.copy()
    x["period_start"] = pd.to_datetime(x["period_start"], errors="coerce").dt.normalize()
    x["period_end"] = pd.to_datetime(x["period_end"], errors="coerce").dt.normalize()
    # Exact weekly file if available; otherwise overlap fallback.
    exact = x[(x["period_start"] == start) & (x["period_end"] == end)].copy()
    if exact.empty:
        exact = x[(x["period_start"] <= end) & (x["period_end"] >= start)].copy()
    if exact.empty:
        return pd.DataFrame(columns=group_cols + ["gp_fact", "gross_revenue_fact", "sales_qty_fact"])
    for c in group_cols:
        if c not in exact.columns:
            exact[c] = ""
    return exact.groupby(group_cols, dropna=False, as_index=False).agg(
        gp_fact=("gross_profit", "sum"),
        gross_revenue_fact=("gross_revenue", "sum"),
        sales_qty_fact=("orders", "sum"),
    )


def _merge_cur_prev(cur: pd.DataFrame, prev: pd.DataFrame, keys: List[str]) -> pd.DataFrame:
    cur = cur.copy() if cur is not None else pd.DataFrame(columns=keys)
    prev = prev.copy() if prev is not None else pd.DataFrame(columns=keys)
    for k in keys:
        if k not in cur.columns:
            cur[k] = ""
        if k not in prev.columns:
            prev[k] = ""
    value_cols = sorted(set([c for c in cur.columns if c not in keys]) | set([c for c in prev.columns if c not in keys]))
    for c in value_cols:
        if c not in cur.columns:
            cur[c] = np.nan
        if c not in prev.columns:
            prev[c] = np.nan
    out = cur.merge(prev, on=keys, how="outer", suffixes=("", "_prev"))
    for c in value_cols:
        if c not in out.columns:
            out[c] = np.nan
        pc = f"{c}_prev"
        if pc not in out.columns:
            out[pc] = np.nan
    return out.fillna(0)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return default
        return float(value)
    except Exception:
        return default


def _factor_row(level: str, keys: Dict[str, Any], factor: str, was: Any, now: Any, change: Any, effect: float, zone: str, comment: str) -> Dict[str, Any]:
    rec = {"level": level, **keys}
    rec.update({
        "factor": factor, "was": was, "now": now, "change": change,
        "effect_gp_rub": float(effect) if pd.notna(effect) else 0.0,
        "effect_type": "плюс" if effect > 0 else ("минус" if effect < 0 else "нейтрально"),
        "zone": zone, "comment": comment,
    })
    return rec


def _entity_factor_rows(level: str, g: pd.DataFrame, keys: List[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for _, r in g.iterrows():
        keyvals = {k: r.get(k, "") for k in keys}
        cur_sum = _safe_float(r.get("order_sum"))
        prev_sum = _safe_float(r.get("order_sum_prev"))
        cur_gp = _safe_float(r.get("gp_fact"), _safe_float(r.get("gross_profit_model")))
        prev_gp = _safe_float(r.get("gp_fact_prev"), _safe_float(r.get("gross_profit_model_prev")))
        cur_margin = cur_gp / cur_sum if cur_sum else 0.0
        prev_margin = prev_gp / prev_sum if prev_sum else cur_margin
        cur_orders = _safe_float(r.get("orders"))
        avg_price = cur_sum / cur_orders if cur_orders else _safe_float(r.get("avg_order_price"), 0)
        # 1) Volume/order sum effect.
        volume_effect = (cur_sum - prev_sum) * prev_margin
        rows.append(_factor_row(level, keyvals, "Объём / сумма заказов", prev_sum, cur_sum, cur_sum - prev_sum, volume_effect, "рынок + управление", "Сколько ВП изменилось из-за изменения суммы заказов при прежней маржинальности."))
        # 2) Margin total effect.
        margin_effect = cur_sum * (cur_margin - prev_margin)
        rows.append(_factor_row(level, keyvals, "Маржинальность", prev_margin * 100, cur_margin * 100, (cur_margin - prev_margin) * 100, margin_effect, "экономика", "Изменение ВП из-за изменения маржинальности после расходов и ABC-факта."))
        # 3) Advertising/Drr effect.
        cur_drr = _safe_float(r.get("drr_pct"), np.nan)
        prev_drr = _safe_float(r.get("drr_pct_prev"), np.nan)
        if pd.notna(cur_drr) and pd.notna(prev_drr):
            drr_effect = -cur_sum * ((cur_drr - prev_drr) / 100.0)
            rows.append(_factor_row(level, keyvals, "ДРР / рекламная нагрузка", prev_drr, cur_drr, cur_drr - prev_drr, drr_effect, "управляемый", "Сколько ВП забрал или добавил сдвиг ДРР относительно прошлой недели."))
        # 4) Price effect.
        cur_price = _safe_float(r.get("avg_order_price"), _safe_float(r.get("finished_price")))
        prev_price = _safe_float(r.get("avg_order_price_prev"), _safe_float(r.get("finished_price_prev")))
        if cur_orders and prev_price:
            price_effect = cur_orders * (cur_price - prev_price) * prev_margin
            rows.append(_factor_row(level, keyvals, "Цена продажи", prev_price, cur_price, cur_price - prev_price, price_effect, "управляемый", "Эффект изменения продажной цены при текущем объёме заказов."))
        # 5) WB buyer price / SPP.
        cur_spp = _safe_float(r.get("spp"), _safe_float(r.get("spp_funnel")))
        prev_spp = _safe_float(r.get("spp_prev"), _safe_float(r.get("spp_funnel_prev")))
        if cur_spp or prev_spp:
            # Higher SPP usually makes buyer price better; this is an explanatory external/partly external factor.
            spp_effect = cur_sum * ((cur_spp - prev_spp) / 100.0) * 0.30 * prev_margin
            rows.append(_factor_row(level, keyvals, "СПП / цена покупателя", prev_spp, cur_spp, cur_spp - prev_spp, spp_effect, "WB / внешний", "Оценка влияния изменения СПП на привлекательность цены и ВП."))
        # 6) Unit expenses.
        qty = _safe_float(r.get("buyout_qty_model"), _safe_float(r.get("sales_qty_fact"), cur_orders))
        for title, col, zone in [
            ("Комиссия WB/шт", "commission_model", "WB / экономика"),
            ("Эквайринг/шт", "acquiring_model", "WB / экономика"),
            ("Логистика/шт", "logistics_direct_model", "WB / логистика"),
            ("Обратная логистика/шт", "logistics_return_model", "WB / логистика"),
            ("Хранение/шт", "storage_model", "WB / логистика"),
            ("Себестоимость/шт", "cost_model", "управляемый"),
            ("Прочие расходы/шт", "other_costs_model", "экономика"),
        ]:
            cur_total = _safe_float(r.get(col))
            prev_total = _safe_float(r.get(f"{col}_prev"))
            cur_unit = cur_total / qty if qty else 0.0
            prev_qty = _safe_float(r.get("buyout_qty_model_prev"), _safe_float(r.get("sales_qty_fact_prev"), _safe_float(r.get("orders_prev"))))
            prev_unit = prev_total / prev_qty if prev_qty else 0.0
            if cur_unit or prev_unit:
                effect = -qty * (cur_unit - prev_unit)
                rows.append(_factor_row(level, keyvals, title, prev_unit, cur_unit, cur_unit - prev_unit, effect, zone, "Эффект изменения расхода на единицу."))
        # 7) Demand, traffic share and conversions.
        demand_cur = _safe_float(r.get("search_frequency"))
        demand_prev = _safe_float(r.get("search_frequency_prev"))
        capture_cur = _safe_float(r.get("search_traffic_capture_pct")) / 100.0
        capture_prev = _safe_float(r.get("search_traffic_capture_pct_prev")) / 100.0
        cart_cur = _safe_float(r.get("cart_conv_pct")) / 100.0
        cart_prev = _safe_float(r.get("cart_conv_pct_prev")) / 100.0
        order_cur = _safe_float(r.get("order_conv_pct")) / 100.0
        order_prev = _safe_float(r.get("order_conv_pct_prev")) / 100.0
        if demand_cur or demand_prev:
            demand_effect = (demand_cur - demand_prev) * capture_prev * cart_prev * order_prev * avg_price * prev_margin
            rows.append(_factor_row(level, keyvals, "Спрос WB", demand_prev, demand_cur, demand_cur - demand_prev, demand_effect, "внешний", "Эффект изменения общего поискового спроса WB."))
        if demand_cur and (capture_cur or capture_prev):
            traffic_effect = demand_cur * (capture_cur - capture_prev) * cart_prev * order_prev * avg_price * prev_margin
            rows.append(_factor_row(level, keyvals, "% поискового трафика", capture_prev * 100, capture_cur * 100, (capture_cur - capture_prev) * 100, traffic_effect, "управляемый", "Эффект изменения доли спроса, которую забрала карточка."))
        opens = _safe_float(r.get("open_cards"))
        if opens and (cart_cur or cart_prev):
            cart_effect = opens * (cart_cur - cart_prev) * order_prev * avg_price * prev_margin
            rows.append(_factor_row(level, keyvals, "Конверсия в корзину", cart_prev * 100, cart_cur * 100, (cart_cur - cart_prev) * 100, cart_effect, "карточка", "Сколько ВП изменилось из-за входной конверсии карточки."))
        if opens and (order_cur or order_prev):
            order_effect = opens * cart_cur * (order_cur - order_prev) * avg_price * prev_margin
            rows.append(_factor_row(level, keyvals, "Корзина -> заказ", order_prev * 100, order_cur * 100, (order_cur - order_prev) * 100, order_effect, "карточка / цена / доставка", "Сколько ВП изменилось из-за дожима из корзины в заказ."))
    return rows


def compute_optimal_benchmarks(outputs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    daily = outputs.get("article_day_fact", pd.DataFrame())
    if daily is None or daily.empty:
        return pd.DataFrame()
    x = daily.copy()
    x["day"] = pd.to_datetime(x["day"], errors="coerce").dt.normalize()
    rows = []
    group_cols = ["subject", "product", "supplier_article", "nm_id"]
    for keys, part in x.groupby(group_cols, dropna=False):
        p = part.sort_values("day").copy()
        if p.empty:
            continue
        positive = p[p["order_sum"].fillna(0) > 0].copy()
        if positive.empty:
            positive = p.copy()
        threshold = positive["order_sum"].quantile(0.80) if len(positive) >= 5 else positive["order_sum"].mean()
        best = positive[positive["order_sum"] >= threshold].copy()
        if best.empty:
            best = positive.nlargest(min(3, len(positive)), "order_sum")
        def ratio(num, den, mul=1.0):
            den_sum = pd.to_numeric(best.get(den, 0), errors="coerce").sum()
            if not den_sum:
                return np.nan
            return pd.to_numeric(best.get(num, 0), errors="coerce").sum() / den_sum * mul
        ad_spend = best[[c for c in ["manual_spend", "unified_spend", "unknown_spend", "ad_spend_model"] if c in best.columns]].sum(axis=1)
        clicks = best[[c for c in ["manual_clicks", "unified_clicks", "unknown_clicks"] if c in best.columns]].sum(axis=1)
        impressions = best[[c for c in ["manual_impressions", "unified_impressions", "unknown_impressions"] if c in best.columns]].sum(axis=1)
        order_sum = pd.to_numeric(best.get("order_sum", 0), errors="coerce").sum()
        rec = dict(zip(group_cols, keys))
        rec.update({
            "best_days_count": len(best),
            "optimal_order_sum_day": pd.to_numeric(best.get("order_sum", 0), errors="coerce").mean(),
            "optimal_orders_day": pd.to_numeric(best.get("orders", 0), errors="coerce").mean(),
            "optimal_drr_pct": ad_spend.sum() / order_sum * 100 if order_sum else np.nan,
            "optimal_cpc": ad_spend.sum() / clicks.sum() if clicks.sum() else np.nan,
            "optimal_ctr_pct": clicks.sum() / impressions.sum() * 100 if impressions.sum() else np.nan,
            "optimal_cart_conv_pct": ratio("add_to_cart", "open_cards", 100),
            "optimal_order_conv_pct": ratio("orders", "add_to_cart", 100),
            "optimal_search_capture_pct": ratio("search_transitions", "search_frequency", 100),
            "optimal_price_sale": ratio("order_sum", "orders", 1),
            "optimal_spp": pd.to_numeric(best.get("spp", best.get("spp_funnel", np.nan)), errors="coerce").mean(),
        })
        rows.append(rec)
    return pd.DataFrame(rows)


def compute_entry_points_bridge(builder: AnalyticsBuilder, week_start: pd.Timestamp, week_end: pd.Timestamp, prev_start: pd.Timestamp, prev_end: pd.Timestamp) -> pd.DataFrame:
    e = builder.enrich(builder.pack.entry_points, "entry_points")
    if e is None or e.empty:
        return pd.DataFrame()
    e = e.copy()
    e["day"] = pd.to_datetime(e["day"], errors="coerce").dt.normalize()
    group_cols = ["subject", "product", "supplier_article", "nm_id", "entry_section", "entry_point"]
    metric_cols = ["impressions", "transitions", "add_to_cart", "orders", "ctr_pct", "cart_conv_pct", "order_conv_pct"]
    def agg(start, end):
        x = e[(e["day"] >= start) & (e["day"] <= end)].copy()
        if x.empty:
            return pd.DataFrame(columns=group_cols + metric_cols)
        g = x.groupby(group_cols, dropna=False, as_index=False).agg(
            impressions=("impressions", "sum"), transitions=("transitions", "sum"),
            add_to_cart=("add_to_cart", "sum"), orders=("orders", "sum"),
        )
        g["ctr_pct"] = np.where(g["impressions"] > 0, g["transitions"] / g["impressions"] * 100, np.nan)
        g["cart_conv_pct"] = np.where(g["transitions"] > 0, g["add_to_cart"] / g["transitions"] * 100, np.nan)
        g["order_conv_pct"] = np.where(g["add_to_cart"] > 0, g["orders"] / g["add_to_cart"] * 100, np.nan)
        return g
    cur = agg(week_start, week_end)
    prev = agg(prev_start, prev_end)
    out = _merge_cur_prev(cur, prev, group_cols)
    for c in metric_cols:
        if c not in out.columns:
            out[c] = 0.0
        if f"{c}_prev" not in out.columns:
            out[f"{c}_prev"] = 0.0
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0)
        out[f"{c}_prev"] = pd.to_numeric(out[f"{c}_prev"], errors="coerce").fillna(0)
    # Join article margin and avg price from daily data for ₽ effect.
    daily = outputs_global_for_bridge.get("article_day_fact", pd.DataFrame()) if "outputs_global_for_bridge" in globals() else pd.DataFrame()
    if daily is not None and not daily.empty:
        ag = _agg_daily_for_bridge(daily, week_start, week_end, ["subject", "product", "supplier_article", "nm_id"])
        gp_cur = _abc_gp_for_period(builder, week_start, week_end, ["subject", "product", "supplier_article", "nm_id"])
        ag = ag.merge(gp_cur, on=["subject", "product", "supplier_article", "nm_id"], how="left")
        ag["gp_use"] = ag["gp_fact"].fillna(ag.get("gross_profit_model", 0))
        ag["margin_use"] = np.where(ag["order_sum"] > 0, ag["gp_use"] / ag["order_sum"], 0)
        ag["avg_price_use"] = np.where(ag["orders"] > 0, ag["order_sum"] / ag["orders"], 0)
        out = out.merge(ag[["subject", "product", "supplier_article", "nm_id", "margin_use", "avg_price_use"]], on=["subject", "product", "supplier_article", "nm_id"], how="left")
    else:
        out["margin_use"] = 0.0
        out["avg_price_use"] = 0.0
    out["delta_transitions"] = out["transitions"] - out["transitions_prev"]
    out["delta_orders"] = out["orders"] - out["orders_prev"]
    out["delta_cart_conv_pp"] = out["cart_conv_pct"] - out["cart_conv_pct_prev"]
    out["delta_order_conv_pp"] = out["order_conv_pct"] - out["order_conv_pct_prev"]
    out["effect_gp_rub"] = out["delta_orders"] * out["avg_price_use"].fillna(0) * out["margin_use"].fillna(0)
    totals = out.groupby(["subject", "product", "supplier_article", "nm_id"], as_index=False)["orders"].sum().rename(columns={"orders": "orders_total"})
    out = out.merge(totals, on=["subject", "product", "supplier_article", "nm_id"], how="left")
    out["orders_share_pct"] = np.where(out["orders_total"] > 0, out["orders"] / out["orders_total"] * 100, np.nan)
    return out.sort_values(["subject", "product", "supplier_article", "effect_gp_rub"], ascending=[True, True, True, False])


def build_factor_outputs(builder: AnalyticsBuilder, outputs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    global outputs_global_for_bridge
    outputs_global_for_bridge = outputs
    daily = outputs.get("article_day_fact", pd.DataFrame())
    week_start, week_end, prev_start, prev_end = _period_bounds_from_daily(daily)
    optimal = compute_optimal_benchmarks(outputs)
    factor_rows: List[Dict[str, Any]] = []
    for level, keys in [
        ("category", ["subject"]),
        ("product", ["subject", "product"]),
        ("article", ["subject", "product", "supplier_article", "nm_id"]),
    ]:
        cur = _agg_daily_for_bridge(daily, week_start, week_end, keys)
        prev = _agg_daily_for_bridge(daily, prev_start, prev_end, keys)
        cur_gp = _abc_gp_for_period(builder, week_start, week_end, keys)
        prev_gp = _abc_gp_for_period(builder, prev_start, prev_end, keys).rename(columns={"gp_fact": "gp_fact_prev", "gross_revenue_fact": "gross_revenue_fact_prev", "sales_qty_fact": "sales_qty_fact_prev"})
        g = _merge_cur_prev(cur, prev, keys)
        if not cur_gp.empty:
            g = g.merge(cur_gp, on=keys, how="left")
        if not prev_gp.empty:
            g = g.merge(prev_gp, on=keys, how="left")
        for c in ["gp_fact", "gp_fact_prev", "gross_revenue_fact", "gross_revenue_fact_prev", "sales_qty_fact", "sales_qty_fact_prev"]:
            if c not in g.columns:
                g[c] = np.nan
        factor_rows.extend(_entity_factor_rows(level, g, keys))
    factor_bridge = pd.DataFrame(factor_rows)
    if not factor_bridge.empty:
        factor_bridge["abs_effect_gp_rub"] = factor_bridge["effect_gp_rub"].abs()
        totals = factor_bridge.groupby(["level", "subject", "product", "supplier_article", "nm_id"], dropna=False)["abs_effect_gp_rub"].sum().rename("total_abs_effect").reset_index()
        factor_bridge = factor_bridge.merge(totals, on=["level", "subject", "product", "supplier_article", "nm_id"], how="left")
        factor_bridge["effect_weight_pct"] = np.where(factor_bridge["total_abs_effect"] > 0, factor_bridge["abs_effect_gp_rub"] / factor_bridge["total_abs_effect"] * 100, 0)
        factor_bridge = factor_bridge.sort_values(["level", "subject", "product", "supplier_article", "abs_effect_gp_rub"], ascending=[True, True, True, True, False])
    entry_bridge = compute_entry_points_bridge(builder, week_start, week_end, prev_start, prev_end)
    # Human-readable summary for PDF: top-4 money factors per entity.
    summary_rows = []
    if not factor_bridge.empty:
        for keys, part in factor_bridge.groupby(["level", "subject", "product", "supplier_article", "nm_id"], dropna=False):
            lvl, subject, product, art, nm_id = keys
            p = part[part["abs_effect_gp_rub"] > 50].sort_values("abs_effect_gp_rub", ascending=False).head(4)
            if p.empty:
                text = "Критичных денежных факторов не выделено: изменение ВП находится в рабочем диапазоне."
            else:
                phrases = []
                for _, r in p.iterrows():
                    val = float(r["effect_gp_rub"])
                    sign = "добавил" if val > 0 else "забрал"
                    phrases.append(f"{r['factor']}: {sign} около {abs(val):,.0f} ₽ ВП".replace(",", " "))
                text = "; ".join(phrases) + "."
            summary_rows.append({
                "level": lvl, "subject": subject, "product": product, "supplier_article": art, "nm_id": nm_id,
                "period": f"{week_start.strftime('%d.%m')}-{week_end.strftime('%d.%m.%Y')}",
                "compare_period": f"{prev_start.strftime('%d.%m')}-{prev_end.strftime('%d.%m.%Y')}",
                "summary_text": text,
            })
    factor_summary = pd.DataFrame(summary_rows)
    return {
        "optimal_benchmarks": optimal,
        "factor_bridge": factor_bridge,
        "entry_points_bridge": entry_bridge,
        "factor_summary_for_pdf": factor_summary,
    }


def write_factor_report(path: Path, factor_outputs: Dict[str, pd.DataFrame]) -> None:
    wb = Workbook()
    wb.remove(wb.active)
    for sheet_name, df in factor_outputs.items():
        write_df_sheet(wb, sheet_name[:31], df if df is not None else pd.DataFrame())
    wb.save(path)


def _fmt_rub(x: Any, short: bool = False) -> str:
    try:
        val = float(x)
    except Exception:
        return "-"
    if pd.isna(val):
        return "-"
    if short and abs(val) >= 1000:
        return f"{val/1000:.0f}к ₽".replace(".", ",")
    return f"{val:,.0f} ₽".replace(",", " ")


def _fmt_num_pdf(x: Any) -> str:
    try:
        val = float(x)
    except Exception:
        return "-"
    if pd.isna(val):
        return "-"
    return f"{val:,.0f}".replace(",", " ")


def _fmt_pct_pdf(x: Any, digits: int = 1) -> str:
    try:
        val = float(x)
    except Exception:
        return "-"
    if pd.isna(val):
        return "-"
    return f"{val:.{digits}f}%".replace(".", ",")


def _fmt_cpc_pdf(x: Any) -> str:
    try:
        val = float(x)
    except Exception:
        return "-"
    if pd.isna(val):
        return "-"
    return f"{val:.1f} ₽".replace(".", ",")


def _delta_pct(cur: Any, prev: Any) -> Optional[float]:
    try:
        cur = float(cur); prev = float(prev)
        if pd.isna(cur) or pd.isna(prev) or abs(prev) < 1e-9:
            return None
        return (cur / prev - 1) * 100
    except Exception:
        return None


def _register_topface_fonts():
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    candidates = [
        (os.getenv("TOPFACE_FONT_REGULAR"), os.getenv("TOPFACE_FONT_BOLD"), os.getenv("TOPFACE_FONT_BLACK")),
        ("/usr/share/fonts/truetype/noto/NotoSans-Regular.ttf", "/usr/share/fonts/truetype/noto/NotoSans-Bold.ttf", "/usr/share/fonts/truetype/noto/NotoSans-Black.ttf"),
        ("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"),
    ]
    for reg, bold, black in candidates:
        if reg and bold and black and Path(reg).exists() and Path(bold).exists() and Path(black).exists():
            pdfmetrics.registerFont(TTFont("TFReg", reg))
            pdfmetrics.registerFont(TTFont("TFBold", bold))
            pdfmetrics.registerFont(TTFont("TFBlack", black))
            return "TFReg", "TFBold", "TFBlack"
    return "Helvetica", "Helvetica-Bold", "Helvetica-Bold"


def generate_management_pdf(outputs: Dict[str, pd.DataFrame], path: Path) -> Optional[Path]:
    try:
        from reportlab.pdfgen import canvas
        from reportlab.lib import colors
        from reportlab.lib.colors import HexColor
        from reportlab.pdfbase.pdfmetrics import stringWidth
    except Exception as exc:
        log(f"WARN: reportlab недоступен, PDF не создан: {exc}")
        return None
    F_REG, F_BOLD, F_BLACK = _register_topface_fonts()
    W, H = 1600, 900
    RED = HexColor("#c90022")
    RED_DARK = HexColor("#a50019")
    WHITE = colors.white
    SOFT = HexColor("#fff4f5")
    BLACK = HexColor("#111111")
    GRAY = HexColor("#555555")
    GREEN = HexColor("#087a38")
    BAD = HexColor("#b00020")
    daily = outputs.get("article_day_fact", pd.DataFrame())
    week_start, week_end, prev_start, prev_end = _period_bounds_from_daily(daily)
    # Current incomplete week: from Monday to latest available day in daily.
    latest = pd.to_datetime(daily["day"], errors="coerce").max() if daily is not None and not daily.empty else week_end
    cur_monday = latest - pd.Timedelta(days=int(latest.weekday()))
    cur_week_end = cur_monday + pd.Timedelta(days=6)
    c = canvas.Canvas(str(path), pagesize=(W, H))
    bookmarks: Dict[str, str] = {}
    page_num = 0

    def bg(title: str, subtitle: str = "", section: str = ""):
        nonlocal page_num
        page_num += 1
        c.setFillColor(RED); c.rect(0, 0, W, H, fill=1, stroke=0)
        c.setFillColor(WHITE); c.setFont(F_REG, 34); c.drawString(70, 835, "topface")
        c.setFont(F_BLACK, 46); c.drawString(70, 765, title)
        if subtitle:
            c.setFont(F_BOLD, 20); c.drawString(70, 725, subtitle)
        if section:
            c.setFont(F_BOLD, 14); c.drawRightString(W-75, 710, section)
        c.setFont(F_BOLD, 13); c.drawRightString(W-75, 38, f"Страница {page_num}")

    def button(x, y, w, label, target=None):
        c.setFillColor(WHITE); c.roundRect(x, y, w, 44, 14, fill=1, stroke=0)
        c.setFillColor(RED_DARK); c.setFont(F_BOLD, 13); c.drawCentredString(x+w/2, y+17, label)
        if target and target in bookmarks:
            c.linkRect("", bookmarks[target], (x, y, x+w, y+44), relative=0)

    def top_nav(active=""):
        labels = [("cur", "Текущая"), ("prev", "Прошлая"), ("month", "Месяц"), ("closed", "Закр. месяц"), ("summary", "Сводка")]
        x = 880
        for key, lab in labels:
            button(x, 800, 128, lab, key)
            x += 142

    def card(x, y, w, h, value, label, sub1="", sub2="", metric="good"):
        c.setFillColor(WHITE); c.roundRect(x, y, w, h, 14, fill=1, stroke=0)
        c.setFillColor(BLACK); c.setFont(F_BLACK, 28); c.drawCentredString(x+w/2, y+h-42, str(value))
        c.setFillColor(GRAY); c.setFont(F_REG, 14); c.drawCentredString(x+w/2, y+h-72, label)
        if sub1:
            color = GREEN if "↑" in sub1 else BAD if "↓" in sub1 else GRAY
            c.setFillColor(color); c.setFont(F_BOLD, 11); c.drawCentredString(x+w/2, y+34, sub1)
        if sub2:
            color = GREEN if "↑" in sub2 else BAD if "↓" in sub2 else GRAY
            c.setFillColor(color); c.setFont(F_BOLD, 11); c.drawCentredString(x+w/2, y+18, sub2)

    def table_box(x, y, w, h, headers, rows, col_widths=None, font_size=13, row_h=48, first_col_red=True):
        if col_widths is None:
            col_widths = [w/len(headers)]*len(headers)
        c.setFillColor(WHITE); c.roundRect(x, y, w, h, 16, fill=1, stroke=0)
        c.setFillColor(RED_DARK); c.roundRect(x, y+h-54, w, 54, 12, fill=1, stroke=0)
        xx=x
        c.setFillColor(WHITE); c.setFont(F_BOLD, font_size)
        for i, head in enumerate(headers):
            c.drawCentredString(xx+col_widths[i]/2, y+h-34, str(head))
            xx += col_widths[i]
        yy=y+h-54-row_h
        for ridx, row in enumerate(rows):
            c.setFillColor(WHITE if ridx%2==0 else SOFT); c.rect(x, yy, w, row_h, fill=1, stroke=0)
            xx=x
            for i, val in enumerate(row):
                if i == 0 and first_col_red:
                    c.setFillColor(SOFT); c.roundRect(xx+8, yy+6, col_widths[i]-16, row_h-12, 8, fill=1, stroke=0)
                    c.setFillColor(RED_DARK); c.setFont(F_BOLD, font_size)
                else:
                    c.setFillColor(BLACK); c.setFont(F_BOLD, font_size)
                # Manual multi-line support.
                text = str(val)
                lines = text.split("\n")
                line_y = yy + row_h/2 + (len(lines)-1)*8
                for line in lines:
                    c.drawCentredString(xx+col_widths[i]/2, line_y, line[:44])
                    line_y -= 16
                xx += col_widths[i]
            yy -= row_h

    def delta_text(cur, prev, metric_lower_better=False):
        d = _delta_pct(cur, prev)
        if d is None:
            return ""
        arrow = "↑" if d > 0 else "↓" if d < 0 else "→"
        # visual sign only; color is handled by arrow in card; compact.
        return f"{arrow} {abs(d):.1f}%".replace(".", ",")

    # Page 1 current week by days.
    bookmarks["cur"] = "cur"; c.bookmarkPage("cur")
    bg("Текущая неделя", f"{cur_monday.strftime('%d.%m')}-{cur_week_end.strftime('%d.%m.%Y')} / оперативно: дни и план", "Текущая неделя")
    top_nav("cur")
    cur_period = _agg_daily_for_bridge(daily, cur_monday, latest, ["subject"])
    prev_same = _agg_daily_for_bridge(daily, cur_monday-pd.Timedelta(days=7), latest-pd.Timedelta(days=7), ["subject"])
    cur_total = cur_period.sum(numeric_only=True)
    prev_total = prev_same.sum(numeric_only=True) if not prev_same.empty else pd.Series(dtype=float)
    card(70, 610, 260, 120, _fmt_rub(cur_total.get("order_sum", 0)), "Сумма заказов", delta_text(cur_total.get("order_sum",0), prev_total.get("order_sum",0)))
    card(360, 610, 260, 120, _fmt_rub(cur_total.get("gross_profit_model", 0)), "ВП расч.", delta_text(cur_total.get("gross_profit_model",0), prev_total.get("gross_profit_model",0)))
    card(650, 610, 260, 120, _fmt_pct_pdf(cur_total.get("ad_spend_total",0)/cur_total.get("order_sum",1)*100 if cur_total.get("order_sum",0) else 0), "ДРР", delta_text(cur_total.get("ad_spend_total",0)/cur_total.get("order_sum",1), prev_total.get("ad_spend_total",0)/prev_total.get("order_sum",1) if prev_total.get("order_sum",0) else None))
    card(940, 610, 260, 120, _fmt_rub(cur_total.get("ad_spend_total", 0)), "Расход РК", delta_text(cur_total.get("ad_spend_total",0), prev_total.get("ad_spend_total",0)))
    plan_day = max(0, float(cur_total.get("order_sum",0)) / max(1, (latest-cur_monday).days+1) * 1.1)
    card(1230, 610, 260, 120, _fmt_rub(plan_day), "План/день", "по сумме")
    # Day table.
    headers = ["Категория", "Пн\n"+cur_monday.strftime("%d.%m"), "Вт\n"+(cur_monday+pd.Timedelta(days=1)).strftime("%d.%m"), "Ср\n"+(cur_monday+pd.Timedelta(days=2)).strftime("%d.%m"), "Чт\n"+(cur_monday+pd.Timedelta(days=3)).strftime("%d.%m"), "Пт\n"+(cur_monday+pd.Timedelta(days=4)).strftime("%d.%m"), "Сб\n"+(cur_monday+pd.Timedelta(days=5)).strftime("%d.%m"), "Вс\n"+(cur_monday+pd.Timedelta(days=6)).strftime("%d.%m"), "План/день"]
    rows=[]
    cats = ["Кисти косметические", "Косметические карандаши", "Помады", "Блески"]
    cat_short = {"Кисти косметические":"Кисти", "Косметические карандаши":"Карандаши", "Помады":"Помады", "Блески":"Блески"}
    day_agg = _agg_daily_for_bridge(daily, cur_monday, cur_week_end, ["day", "subject"])
    for cat in cats:
        left = f"{cat_short.get(cat,cat)}\nСумма\nВП\nРасх. РК\nДРР"
        vals=[left]
        for i in range(7):
            day = cur_monday + pd.Timedelta(days=i)
            p = day_agg[(day_agg["day"] == day) & (day_agg["subject"] == cat)] if not day_agg.empty and "day" in day_agg.columns else pd.DataFrame()
            if p.empty or day > latest:
                vals.append("-\n-\n-\n-")
            else:
                rr=p.iloc[0]
                vals.append(f"{_fmt_rub(rr.get('order_sum',0), True)}\n{_fmt_rub(rr.get('gross_profit_model',0), True)}\n{_fmt_rub(rr.get('ad_spend_total',0), True)}\n{_fmt_pct_pdf(rr.get('drr_pct',0))}")
        vals.append(f"{_fmt_rub(plan_day/4, True)}\n-\n-\n-")
        rows.append(vals)
    table_box(70, 80, 1460, 470, headers, rows, col_widths=[190]+[145]*7+[255], font_size=12, row_h=92)
    c.showPage()

    # Page 2 current categories.
    bookmarks["cur_cat"] = "cur_cat"; c.bookmarkPage("cur_cat")
    bg("Текущая неделя: категории", f"{cur_monday.strftime('%d.%m')}-{latest.strftime('%d.%m.%Y')} / переход по категории", "Текущая неделя")
    top_nav("cur")
    cat_cur = _merge_cur_prev(cur_period, prev_same, ["subject"])
    rows=[]
    for _, r in cat_cur.sort_values("order_sum", ascending=False).iterrows():
        rows.append([cat_short.get(r.get("subject"), r.get("subject")), _fmt_rub(r.get("order_sum")), delta_text(r.get("order_sum"), r.get("order_sum_prev")), _fmt_rub(r.get("gross_profit_model")), _fmt_pct_pdf(r.get("drr_pct")), _fmt_rub(r.get("ad_spend_total")), _fmt_cpc_pdf(r.get("cpc"))])
    table_box(80, 300, 1360, 330, ["Категория", "Сумма", "Δ", "ВП расч.", "ДРР", "Расход РК", "CPC"], rows, col_widths=[230,210,120,210,160,220,160], font_size=14, row_h=66)
    c.showPage()

    # Page 3 previous week summary.
    bookmarks["prev"] = "prev"; c.bookmarkPage("prev")
    bg("Прошлая неделя", f"{week_start.strftime('%d.%m')}-{week_end.strftime('%d.%m.%Y')} / сравнение с {prev_start.strftime('%d.%m')}-{prev_end.strftime('%d.%m.%Y')}", "Прошлая неделя")
    top_nav("prev")
    cat = _agg_daily_for_bridge(daily, week_start, week_end, ["subject"])
    cat_prev = _agg_daily_for_bridge(daily, prev_start, prev_end, ["subject"])
    cat_gp = _abc_gp_for_period(builder_global_for_pdf, week_start, week_end, ["subject"]) if "builder_global_for_pdf" in globals() else pd.DataFrame()
    cat_gp_prev = _abc_gp_for_period(builder_global_for_pdf, prev_start, prev_end, ["subject"]).rename(columns={"gp_fact":"gp_fact_prev"}) if "builder_global_for_pdf" in globals() else pd.DataFrame()
    cat_sum = _merge_cur_prev(cat, cat_prev, ["subject"])
    if not cat_gp.empty: cat_sum = cat_sum.merge(cat_gp, on="subject", how="left")
    if not cat_gp_prev.empty: cat_sum = cat_sum.merge(cat_gp_prev[["subject","gp_fact_prev"]], on="subject", how="left")
    total = cat_sum.sum(numeric_only=True)
    card(70, 610, 260, 120, _fmt_rub(total.get("order_sum",0)), "Сумма заказов", delta_text(total.get("order_sum",0), total.get("order_sum_prev",0)))
    card(360, 610, 260, 120, _fmt_rub(total.get("gp_fact", total.get("gross_profit_model",0))), "ВП факт ABC", delta_text(total.get("gp_fact",0), total.get("gp_fact_prev",0)))
    card(650, 610, 260, 120, _fmt_pct_pdf(total.get("ad_spend_total",0)/total.get("order_sum",1)*100 if total.get("order_sum",0) else 0), "ДРР", delta_text(total.get("ad_spend_total",0)/total.get("order_sum",1), total.get("ad_spend_total_prev",0)/total.get("order_sum_prev",1) if total.get("order_sum_prev",0) else None))
    card(940, 610, 260, 120, _fmt_rub(total.get("ad_spend_total",0)), "Расход РК", delta_text(total.get("ad_spend_total",0), total.get("ad_spend_total_prev",0)))
    card(1230, 610, 260, 120, _fmt_cpc_pdf(total.get("ad_spend_total",0)/total.get("ad_clicks_total",1) if total.get("ad_clicks_total",0) else 0), "CPC")
    rows=[]
    for _, r in cat_sum.sort_values("order_sum", ascending=False).iterrows():
        gp = r.get("gp_fact", r.get("gross_profit_model", 0))
        mar = gp / r.get("order_sum",1)*100 if r.get("order_sum",0) else 0
        rows.append([cat_short.get(r.get("subject"), r.get("subject")), _fmt_rub(r.get("order_sum")), delta_text(r.get("order_sum"), r.get("order_sum_prev")), _fmt_rub(gp), delta_text(gp, r.get("gp_fact_prev", r.get("gross_profit_model_prev",0))), _fmt_pct_pdf(mar), _fmt_pct_pdf(r.get("drr_pct")), _fmt_cpc_pdf(r.get("cpc"))])
    table_box(70, 90, 1460, 420, ["Категория", "Сумма", "Δ", "ВП", "Δ", "Маржа", "ДРР", "CPC"], rows, col_widths=[220,200,100,200,100,150,150,150], font_size=13, row_h=66)
    c.showPage()

    # Month placeholders simplified.
    for key, title, sub in [("month", "Текущий месяц", "месяц неполный / темп к плану"), ("closed", "Последний закрытый месяц", "факт по доступным данным"), ("summary", "Сводка по месяцам", "категории / без лишней детализации")]:
        bookmarks[key] = key; c.bookmarkPage(key)
        bg(title, sub, title)
        top_nav(key)
        c.setFillColor(WHITE); c.roundRect(100, 300, 1400, 180, 20, fill=1, stroke=0)
        c.setFillColor(BLACK); c.setFont(F_BOLD, 24); c.drawCentredString(800, 390, "Данные раздела формируются в Excel-расчёте; PDF использует этот блок как навигационный уровень.")
        c.showPage()

    # Category pages for previous week.
    factor_summary = outputs.get("factor_summary_for_pdf", pd.DataFrame())
    factor_bridge = outputs.get("factor_bridge", pd.DataFrame())
    opt = outputs.get("optimal_benchmarks", pd.DataFrame())
    detail_articles = []
    cat_rows = cat_sum.sort_values("order_sum", ascending=False)
    for _, catr in cat_rows.iterrows():
        subject = catr.get("subject")
        cat_name = cat_short.get(subject, subject)
        cat_book = f"cat_{cat_name}"
        bookmarks[cat_book] = cat_book; c.bookmarkPage(cat_book)
        # Article/product rows from bridge/daily.
        a_cur = _agg_daily_for_bridge(daily, week_start, week_end, ["subject", "product", "supplier_article", "nm_id"])
        a_prev = _agg_daily_for_bridge(daily, prev_start, prev_end, ["subject", "product", "supplier_article", "nm_id"])
        a = _merge_cur_prev(a_cur, a_prev, ["subject", "product", "supplier_article", "nm_id"])
        agp = _abc_gp_for_period(builder_global_for_pdf, week_start, week_end, ["subject", "product", "supplier_article", "nm_id"]) if "builder_global_for_pdf" in globals() else pd.DataFrame()
        if not agp.empty: a = a.merge(agp, on=["subject", "product", "supplier_article", "nm_id"], how="left")
        a = a[a["subject"] == subject].copy()
        if a.empty:
            continue
        a["gp_use"] = a["gp_fact"].fillna(a.get("gross_profit_model", 0)) if "gp_fact" in a.columns else a.get("gross_profit_model", 0)
        # ABC-80: only details contributing 80% positive GP; category table still shows visible rows.
        apos = a[a["gp_use"] > 0].sort_values("gp_use", ascending=False).copy()
        total_gp = apos["gp_use"].sum()
        if total_gp > 0:
            apos["cum_share"] = apos["gp_use"].cumsum() / total_gp
            detail_articles += apos[apos["cum_share"] <= 0.80][["subject","product","supplier_article","nm_id"]].to_dict("records")
        pages = [a.sort_values("order_sum", ascending=False).iloc[i:i+7] for i in range(0, len(a), 7)]
        for pi, part in enumerate(pages, start=1):
            bg(f"Категория: {cat_name}", f"{week_start.strftime('%d.%m')}-{week_end.strftime('%d.%m.%Y')} / динамика к прошлой неделе / лист {pi} из {len(pages)}", "Категория")
            button(1280, 800, 180, "← прошлая", "prev")
            rows=[]
            for _, r in part.iterrows():
                gp = r.get("gp_use", 0); mar = gp / r.get("order_sum",1)*100 if r.get("order_sum",0) else 0
                rows.append([r.get("supplier_article"), _fmt_rub(r.get("order_sum")), delta_text(r.get("order_sum"), r.get("order_sum_prev")), _fmt_rub(gp), _fmt_pct_pdf(mar), _fmt_pct_pdf(r.get("drr_pct")), _fmt_cpc_pdf(r.get("cpc")), _fmt_pct_pdf(r.get("search_traffic_capture_pct")), _fmt_pct_pdf(r.get("localization_with_replacements_pct"))])
            table_box(80, 170, 1400, 520, ["Артикул", "Сумма", "Δ", "ВП", "Маржа", "ДРР", "CPC", "% поиска", "Локал."], rows, col_widths=[190,180,90,180,140,130,120,140,140], font_size=13, row_h=62)
            c.showPage()

    # Article detail pages from ABC-80 only.
    seen = set()
    for rec in detail_articles:
        key_tuple = (rec.get("subject"), str(rec.get("product")), str(rec.get("supplier_article")), rec.get("nm_id"))
        if key_tuple in seen:
            continue
        seen.add(key_tuple)
        subject, product, art, nm_id = key_tuple
        a_cur = _agg_daily_for_bridge(daily, week_start, week_end, ["subject", "product", "supplier_article", "nm_id"])
        a_prev = _agg_daily_for_bridge(daily, prev_start, prev_end, ["subject", "product", "supplier_article", "nm_id"])
        a = _merge_cur_prev(a_cur, a_prev, ["subject", "product", "supplier_article", "nm_id"])
        agp = _abc_gp_for_period(builder_global_for_pdf, week_start, week_end, ["subject", "product", "supplier_article", "nm_id"]) if "builder_global_for_pdf" in globals() else pd.DataFrame()
        if not agp.empty: a = a.merge(agp, on=["subject", "product", "supplier_article", "nm_id"], how="left")
        rr = a[(a["subject"] == subject) & (a["supplier_article"].astype(str) == str(art))]
        if rr.empty:
            continue
        r = rr.iloc[0]
        page1 = f"article_{art}_1"; page2=f"article_{art}_2"
        bookmarks[page1]=page1; c.bookmarkPage(page1)
        bg(f"Артикул: {art}", f"{cat_short.get(subject,subject)} / товар {product} / {week_start.strftime('%d.%m')}-{week_end.strftime('%d.%m.%Y')}", "Артикул 1/2")
        button(1240, 800, 170, "← категория", f"cat_{cat_short.get(subject,subject)}")
        button(1430, 800, 100, "стр.2", page2)
        c.setFillColor(RED_DARK); c.roundRect(80, 690, 1440, 44, 10, fill=1, stroke=0); c.setFillColor(WHITE); c.setFont(F_BLACK, 20); c.drawString(105, 705, "Блок 1. Продажи и экономика")
        gp = _safe_float(r.get("gp_fact"), _safe_float(r.get("gross_profit_model")))
        mar = gp / _safe_float(r.get("order_sum"),1)*100 if _safe_float(r.get("order_sum")) else 0
        cards1 = [
            (_fmt_rub(r.get("order_sum")), "Сумма заказов", delta_text(r.get("order_sum"), r.get("order_sum_prev"))),
            (_fmt_rub(gp), "ВП факт ABC", ""),
            (_fmt_pct_pdf(mar), "Маржинальность", ""),
            (_fmt_rub(r.get("avg_order_price")), "Цена продажи", delta_text(r.get("avg_order_price"), r.get("avg_order_price_prev"))),
            (_fmt_pct_pdf(r.get("spp", r.get("spp_funnel",0))), "СПП", delta_text(r.get("spp",0), r.get("spp_prev",0))),
            (_fmt_rub(r.get("commission_model",0)/max(_safe_float(r.get("buyout_qty_model"),1),1)), "Комиссия/шт", ""),
            (_fmt_rub((r.get("logistics_direct_model",0)+r.get("logistics_return_model",0))/max(_safe_float(r.get("buyout_qty_model"),1),1)), "Логистика/шт", ""),
            (_fmt_rub(r.get("storage_model",0)/max(_safe_float(r.get("buyout_qty_model"),1),1)), "Хранение/шт", ""),
            (_fmt_rub(r.get("acquiring_model",0)/max(_safe_float(r.get("buyout_qty_model"),1),1)), "Эквайринг/шт", ""),
            (_fmt_rub(r.get("cost_model",0)/max(_safe_float(r.get("buyout_qty_model"),1),1)), "Себест./шт", ""),
            (_fmt_rub(r.get("other_costs_model",0)/max(_safe_float(r.get("buyout_qty_model"),1),1)), "Прочие/шт", ""),
        ]
        for idx, it in enumerate(cards1[:6]): card(80+idx*240, 570, 220, 96, *it)
        for idx, it in enumerate(cards1[6:]): card(80+idx*240, 455, 220, 96, *it)
        c.setFillColor(RED_DARK); c.roundRect(80, 385, 1440, 44, 10, fill=1, stroke=0); c.setFillColor(WHITE); c.setFont(F_BLACK, 20); c.drawString(105, 400, "Блок 2. Реклама, спрос и конверсии")
        cards2 = [
            (_fmt_rub(r.get("ad_spend_total")), "Расход РК", delta_text(r.get("ad_spend_total"), r.get("ad_spend_total_prev"))),
            (_fmt_pct_pdf(r.get("drr_pct")), "ДРР", delta_text(r.get("drr_pct"), r.get("drr_pct_prev"))),
            (_fmt_cpc_pdf(r.get("cpc")), "CPC", delta_text(r.get("cpc"), r.get("cpc_prev"))),
            (_fmt_num_pdf(r.get("ad_impressions_total")), "Показы РК", delta_text(r.get("ad_impressions_total"), r.get("ad_impressions_total_prev"))),
            (_fmt_num_pdf(r.get("ad_clicks_total")), "Клики РК", delta_text(r.get("ad_clicks_total"), r.get("ad_clicks_total_prev"))),
            (_fmt_num_pdf(r.get("open_cards")), "Открытия", delta_text(r.get("open_cards"), r.get("open_cards_prev"))),
            (_fmt_pct_pdf(r.get("cart_conv_pct")), "Конв. в корзину", delta_text(r.get("cart_conv_pct"), r.get("cart_conv_pct_prev"))),
            (_fmt_pct_pdf(r.get("order_conv_pct")), "Корзина -> заказ", delta_text(r.get("order_conv_pct"), r.get("order_conv_pct_prev"))),
            (_fmt_num_pdf(r.get("search_frequency")), "Спрос WB", delta_text(r.get("search_frequency"), r.get("search_frequency_prev"))),
            (_fmt_pct_pdf(r.get("search_traffic_capture_pct")), "% поиска", delta_text(r.get("search_traffic_capture_pct"), r.get("search_traffic_capture_pct_prev"))),
            (_fmt_pct_pdf(r.get("localization_with_replacements_pct")), "Локализация", ""),
        ]
        for idx, it in enumerate(cards2[:6]): card(80+idx*240, 260, 220, 96, *it)
        for idx, it in enumerate(cards2[6:]): card(80+idx*240, 145, 220, 96, *it)
        c.showPage()
        bookmarks[page2]=page2; c.bookmarkPage(page2)
        bg(f"Артикул: {art}", f"{cat_short.get(subject,subject)} / товар {product} / точки входа и выводы", "Артикул 2/2")
        button(1240, 800, 170, "← категория", f"cat_{cat_short.get(subject,subject)}")
        button(1430, 800, 100, "стр.1", page1)
        eb = outputs.get("entry_points_bridge", pd.DataFrame())
        ep_rows=[]
        if eb is not None and not eb.empty:
            part = eb[(eb["subject"]==subject) & (eb["supplier_article"].astype(str)==str(art))].sort_values("orders", ascending=False).head(7)
            for _, ebr in part.iterrows():
                ep_rows.append([f"{ebr.get('entry_section','')} / {ebr.get('entry_point','')}", _fmt_num_pdf(ebr.get("transitions")), delta_text(ebr.get("transitions"), ebr.get("transitions_prev")), _fmt_num_pdf(ebr.get("orders")), delta_text(ebr.get("orders"), ebr.get("orders_prev")), _fmt_rub(ebr.get("effect_gp_rub"))])
        if not ep_rows:
            ep_rows=[['-', '-', '-', '-', '-', '-']]
        table_box(90, 380, 1420, 340, ["Точка входа", "Переходы", "Δ", "Заказы", "Δ", "Вклад ВП"], ep_rows, col_widths=[570,160,100,140,100,180], font_size=13, row_h=42, first_col_red=False)
        # Factor summary money.
        fs = factor_summary[(factor_summary["level"]=="article") & (factor_summary["supplier_article"].astype(str)==str(art))] if factor_summary is not None and not factor_summary.empty else pd.DataFrame()
        txt = fs.iloc[0]["summary_text"] if not fs.empty else "Факторный мост не выделил значимых денежных причин."
        c.setFillColor(WHITE); c.roundRect(90, 130, 1420, 190, 18, fill=1, stroke=0)
        c.setFillColor(RED_DARK); c.setFont(F_BLACK, 22); c.drawString(120, 280, "Факторный вывод в деньгах")
        c.setFillColor(BLACK); c.setFont(F_BOLD, 18)
        # simple wrap
        words = str(txt).split()
        lines=[]; line=""
        for w0 in words:
            cand = (line + " " + w0).strip()
            if stringWidth(cand, F_BOLD, 18) > 1320:
                lines.append(line); line=w0
            else:
                line=cand
        if line: lines.append(line)
        yy=245
        for line in lines[:6]:
            c.drawString(120, yy, line); yy -= 26
        c.showPage()
    c.save()
    return path


def send_telegram_document(file_path: Path, caption: str = "") -> bool:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        log("Telegram: TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID не заданы, отправка пропущена")
        return False
    import urllib.request
    import uuid
    boundary = "----WebKitFormBoundary" + uuid.uuid4().hex
    url = f"https://api.telegram.org/bot{token}/sendDocument"
    fields = {"chat_id": chat_id, "caption": caption[:1000]}
    thread_id = os.getenv("TELEGRAM_MESSAGE_THREAD_ID", "").strip()
    if thread_id:
        fields["message_thread_id"] = thread_id
    body = bytearray()
    for name, value in fields.items():
        body.extend(f"--{boundary}\r\n".encode())
        body.extend(f'Content-Disposition: form-data; name="{name}"\r\n\r\n{value}\r\n'.encode())
    data = file_path.read_bytes()
    body.extend(f"--{boundary}\r\n".encode())
    body.extend(f'Content-Disposition: form-data; name="document"; filename="{file_path.name}"\r\n'.encode())
    body.extend(b"Content-Type: application/pdf\r\n\r\n")
    body.extend(data)
    body.extend(f"\r\n--{boundary}--\r\n".encode())
    req = urllib.request.Request(url, data=bytes(body), headers={"Content-Type": f"multipart/form-data; boundary={boundary}"})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            ok = 200 <= resp.status < 300
            log(f"Telegram: {'sent' if ok else 'failed'} status={resp.status}")
            return ok
    except Exception as exc:
        log(f"Telegram: ошибка отправки PDF: {exc}")
        return False


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".", help="Local root used for local copies and local mode")
    parser.add_argument("--reports-root", default="Отчёты")
    parser.add_argument("--store", default="TOPFACE")
    parser.add_argument("--no-pdf", action="store_true", help="Не формировать PDF")
    parser.add_argument("--send-telegram", action="store_true", help="Отправить PDF в Telegram через TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID")
    args = parser.parse_args()
    diagnostics = Diagnostics()
    storage = make_storage(args.root)
    loader = Loader(storage, args.reports_root, args.store, diagnostics)
    pack = loader.load_all()
    builder = AnalyticsBuilder(pack)
    outputs = builder.build_all()
    factor_outputs = build_factor_outputs(builder, outputs)
    outputs.update(factor_outputs)
    local_dir = Path(args.root) / OUT_DIR
    paths = export_outputs(outputs, local_dir)
    # Отдельный техфайл по денежному факторному мосту, чтобы не ломать основные Excel-структуры.
    factor_path = local_dir / FACTOR_REPORT_NAME
    write_factor_report(factor_path, factor_outputs)
    paths.append(factor_path)
    pdf_path = local_dir / PDF_REPORT_NAME
    if not args.no_pdf:
        global builder_global_for_pdf
        builder_global_for_pdf = builder
        pdf_created = generate_management_pdf(outputs, pdf_path)
        if pdf_created:
            paths.append(pdf_path)
    log(f"Saved local copies: {local_dir}")
    # Always save to S3 too when S3 is active. In local mode this overwrites same local files safely.
    if storage.is_s3:
        for p in paths:
            storage.write_bytes(f"{OUT_DIR}/{p.name}", p.read_bytes())
            log(f"Saved: {OUT_DIR}/{p.name}")
    if args.send_telegram and pdf_path.exists():
        caption = f"TOPFACE WB: управленческий отчёт {datetime.now().strftime('%d.%m.%Y %H:%M')}"
        send_telegram_document(pdf_path, caption)
    log("Done")


if __name__ == "__main__":
    main()
