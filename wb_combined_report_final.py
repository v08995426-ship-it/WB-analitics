
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import calendar
import io
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


TARGET_SUBJECTS = [
    "Кисти косметические",
    "Помады",
    "Блески",
    "Косметические карандаши",
]
TARGET_SUBJECTS_LOWER = {x.lower(): x for x in TARGET_SUBJECTS}
EXCLUDE_ARTICLES = {"CZ420","CZ420БРОВИ","CZ420ГЛАЗА","DE49","DE49ГЛАЗА","PT901","cz420","cz420глаза","cz420брови".upper()}
EXAMPLE_ARTICLES = ["901/5","901/8","901/14","901/18"]

THIN = Side(style="thin", color="D9D9D9")
BORDER = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)
FILL_HEADER = PatternFill("solid", fgColor="DDEBF7")
FILL_SECTION = PatternFill("solid", fgColor="E2F0D9")
FILL_CATEGORY = PatternFill("solid", fgColor="EAF4FF")
FILL_PRODUCT = PatternFill("solid", fgColor="F7FBFF")
FILL_TOTAL = PatternFill("solid", fgColor="FFF2CC")


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("\xa0", " ").strip()
    return re.sub(r"\s+", " ", text)


def clean_article(value: Any) -> str:
    s = normalize_text(value)
    if s.lower() in {"nan","none",""}:
        return ""
    return s


def upper_article(value: Any) -> str:
    return clean_article(value).upper()


def extract_code(value: Any) -> str:
    s = upper_article(value)
    if not s or s in EXCLUDE_ARTICLES:
        return ""
    m = re.match(r"^PT(\d+)", s)
    if m:
        return m.group(1)
    m = re.match(r"^(\d+)", s)
    if m:
        return m.group(1)
    return ""


def canonical_subject(value: Any) -> str:
    s = normalize_text(value).lower()
    return TARGET_SUBJECTS_LOWER.get(s, normalize_text(value))


def to_numeric(x: Any) -> pd.Series:
    return pd.to_numeric(x, errors="coerce")


def to_dt(x: Any) -> pd.Series:
    return pd.to_datetime(x, errors="coerce").dt.normalize()


def safe_div(a: Any, b: Any) -> float:
    try:
        a = float(a); b = float(b)
    except Exception:
        return np.nan
    if pd.isna(a) or pd.isna(b) or b == 0:
        return np.nan
    return a / b


def weighted_mean(values, weights) -> float:
    v = pd.to_numeric(values, errors="coerce")
    w = pd.to_numeric(weights, errors="coerce")
    mask = v.notna() & w.notna()
    if not mask.any():
        return np.nan
    v = v[mask]; w = w[mask]
    if w.sum() == 0:
        return np.nan
    return float(np.average(v, weights=w))


def week_code_from_date(v: Any) -> Optional[str]:
    if pd.isna(v):
        return None
    ts = pd.Timestamp(v)
    iso = ts.isocalendar()
    return f"{int(iso.year)}-W{int(iso.week):02d}"


def week_bounds_from_code(week_code: str) -> Tuple[Optional[date], Optional[date]]:
    m = re.match(r"^(\d{4})-W(\d{2})$", str(week_code))
    if not m:
        return None, None
    y = int(m.group(1)); w = int(m.group(2))
    return date.fromisocalendar(y, w, 1), date.fromisocalendar(y, w, 7)


def parse_abc_period_from_name(name: str) -> Tuple[Optional[date], Optional[date]]:
    m = re.search(r"__(\d{2})\.(\d{2})\.(\d{4})-(\d{2})\.(\d{2})\.(\d{4})__", name)
    if not m:
        return None, None
    return date(int(m.group(3)), int(m.group(2)), int(m.group(1))), date(int(m.group(6)), int(m.group(5)), int(m.group(4)))


def russian_month_name(month_num: int) -> str:
    names = {1:"Январь",2:"Февраль",3:"Март",4:"Апрель",5:"Май",6:"Июнь",7:"Июль",8:"Август",9:"Сентябрь",10:"Октябрь",11:"Ноябрь",12:"Декабрь"}
    return names[month_num]


ALIASES = {
    "day": ["Дата","date","dt","Дата заказа"],
    "week": ["Неделя","week"],
    "nm_id": ["Артикул WB","Артикул ВБ","nmID","nmId"],
    "supplier_article": ["Артикул продавца","supplierArticle","Артикул WB продавца"],
    "subject": ["Предмет","subject","Название предмета","Название предмета", "Категория"],
    "brand": ["Бренд","brand"],
    "title": ["Название","Название товара","Товар"],
    "warehouse": ["Склад","warehouseName"],
    "orders": ["Заказы","orders","ordersCount","Кол-во продаж"],
    "buyouts_count": ["buyoutsCount"],
    "finished_price": ["finishedPrice","Средняя цена покупателя","Ср. цена продажи","Цена с учетом всех скидок, кроме суммы по WB Кошельку"],
    "price_with_disc": ["priceWithDisc","Средняя цена продажи","Цена со скидкой продавца, в том числе со скидкой WB Клуба"],
    "spp": ["SPP","СПП","СПП, %","Скидка WB, %","spp"],
    "spend": ["Расход","spend","Продвижение"],
    "gross_profit": ["Валовая прибыль"],
    "gross_revenue": ["Валовая выручка"],
    "campaign_name": ["Название"],
}


def rename_using_aliases(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    norm_cols = {normalize_text(c).lower().replace("ё","е"): c for c in out.columns}
    for target, variants in ALIASES.items():
        if target in out.columns:
            continue
        found = None
        for v in variants:
            key = normalize_text(v).lower().replace("ё","е")
            if key in norm_cols:
                found = norm_cols[key]
                break
        if found is not None:
            out[target] = out[found]
    return out


def read_excel_flexible(data: bytes, preferred_sheets: Optional[Iterable[str]]=None, header_candidates=(0,1,2)) -> Tuple[pd.DataFrame, str]:
    bio = io.BytesIO(data)
    xl = pd.ExcelFile(bio)
    chosen = None
    if preferred_sheets:
        lower_map = {normalize_text(s).lower().replace("ё","е"): s for s in xl.sheet_names}
        for s in preferred_sheets:
            k = normalize_text(s).lower().replace("ё","е")
            if k in lower_map:
                chosen = lower_map[k]
                break
    if chosen is None:
        chosen = xl.sheet_names[0]
    best = None; best_score = -10**9
    for h in header_candidates:
        try:
            df = xl.parse(chosen, header=h, dtype=object)
        except Exception:
            continue
        df = df.dropna(axis=0, how="all").dropna(axis=1, how="all")
        df.columns = [normalize_text(c) or f"col_{i}" for i, c in enumerate(df.columns)]
        score = len(df.columns)
        if score > best_score:
            best_score = score; best = df
    if best is None:
        raise ValueError(f"cannot read {chosen}")
    return rename_using_aliases(best), chosen


class BaseStorage:
    def list_files(self, prefix: str) -> List[str]: raise NotImplementedError
    def read_bytes(self, path: str) -> bytes: raise NotImplementedError
    def write_bytes(self, path: str, data: bytes) -> None: raise NotImplementedError
    def exists(self, path: str) -> bool: raise NotImplementedError


class LocalStorage(BaseStorage):
    def __init__(self, root: str): self.root = Path(root)
    def _abs(self, p: str) -> Path: return self.root / p
    def list_files(self, prefix: str) -> List[str]:
        prefix = prefix.replace("\\","/").rstrip("/")
        start = self._abs(prefix)
        base = start if start.exists() else start.parent
        if not base.exists(): return []
        out = []
        for p in base.rglob("*"):
            if p.is_file():
                rel = str(p.relative_to(self.root)).replace("\\","/")
                if rel.startswith(prefix): out.append(rel)
        return sorted(out)
    def glob_root(self, pattern: str) -> List[str]:
        return sorted(str(p.relative_to(self.root)).replace("\\","/") for p in self.root.glob(pattern) if p.is_file())
    def read_bytes(self, path: str) -> bytes: return self._abs(path).read_bytes()
    def write_bytes(self, path: str, data: bytes) -> None:
        out = self._abs(path); out.parent.mkdir(parents=True, exist_ok=True); out.write_bytes(data)
    def exists(self, path: str) -> bool: return self._abs(path).exists()


class S3Storage(BaseStorage):
    def __init__(self, bucket: str, access_key: str, secret_key: str):
        self.bucket = bucket
        self.s3 = boto3.client("s3", endpoint_url="https://storage.yandexcloud.net", aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    def list_files(self, prefix: str) -> List[str]:
        files=[]; token=None
        while True:
            kwargs={"Bucket": self.bucket, "Prefix": prefix}
            if token: kwargs["ContinuationToken"]=token
            resp=self.s3.list_objects_v2(**kwargs)
            for item in resp.get("Contents",[]):
                k=item["Key"]
                if not k.endswith("/"): files.append(k)
            if not resp.get("IsTruncated"): break
            token=resp.get("NextContinuationToken")
        return sorted(files)
    def read_bytes(self, path: str) -> bytes: return self.s3.get_object(Bucket=self.bucket, Key=path)["Body"].read()
    def write_bytes(self, path: str, data: bytes) -> None: self.s3.put_object(Bucket=self.bucket, Key=path, Body=data)
    def exists(self, path: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=path); return True
        except Exception:
            return False


def make_storage(root: str) -> BaseStorage:
    if os.getenv("YC_BUCKET_NAME") and os.getenv("YC_ACCESS_KEY_ID") and os.getenv("YC_SECRET_ACCESS_KEY"):
        log("Using Yandex Object Storage (S3)")
        return S3Storage(os.getenv("YC_BUCKET_NAME"), os.getenv("YC_ACCESS_KEY_ID"), os.getenv("YC_SECRET_ACCESS_KEY"))
    log("Using local filesystem")
    return LocalStorage(root)


@dataclass
class LoadedData:
    orders: pd.DataFrame
    funnel: pd.DataFrame
    ads_daily: pd.DataFrame
    ads_campaigns: pd.DataFrame
    economics: pd.DataFrame
    abc_weekly: pd.DataFrame
    abc_monthly: pd.DataFrame
    plan: pd.DataFrame
    latest_day: pd.Timestamp
    source_paths: pd.DataFrame
    warnings: List[str]


class Loader:
    def __init__(self, storage: BaseStorage, reports_root: str, store: str):
        self.storage=storage; self.reports_root=reports_root.rstrip("/"); self.store=store
        self.warnings=[]; self.paths=[]
    def _prefix(self,*parts): return "/".join([self.reports_root,*parts]).replace("//","/")
    def _list(self, prefixes):
        out=[]
        for p in prefixes: out.extend(self.storage.list_files(p))
        return sorted(set(f for f in out if f.lower().endswith(".xlsx")))
    def _glob(self, patterns):
        if hasattr(self.storage,"glob_root"):
            out=[]
            for p in patterns: out.extend(self.storage.glob_root(p))
            return sorted(set(out))
        return []
    def _record_path(self, dataset, path, sheet): self.paths.append({"dataset":dataset,"path":path,"sheet":sheet})

    def load_orders(self):
        files=self._list([self._prefix("Заказы", self.store, "Недельные"), self._prefix("Заказы", self.store)])
        if not files: files=self._glob(["Заказы_*.xlsx"])
        dfs=[]
        for p in files:
            try:
                df,sheet=read_excel_flexible(self.storage.read_bytes(p), ["Заказы"], (0,))
                self._record_path("orders", p, sheet)
                df["day"]=to_dt(df.get("day", pd.Series(dtype=object)))
                df["nm_id"]=to_numeric(df.get("nm_id", np.nan))
                df["supplier_article"]=df.get("supplier_article", pd.Series(dtype=object)).map(clean_article)
                df["subject"]=df.get("subject", pd.Series(dtype=object)).map(canonical_subject)
                df["brand"]=df.get("brand", pd.Series(dtype=object)).map(normalize_text)
                df["title"]=df.get("title", pd.Series(dtype=object)).map(normalize_text)
                df["orders"]=to_numeric(df.get("orders", np.nan))
                if df["orders"].isna().all(): df["orders"]=1.0
                df["finished_price"]=to_numeric(df.get("finished_price", np.nan))
                df["price_with_disc"]=to_numeric(df.get("price_with_disc", np.nan))
                df["spp"]=to_numeric(df.get("spp", np.nan))
                dfs.append(df[["day","nm_id","supplier_article","subject","brand","title","orders","finished_price","price_with_disc","spp"]])
            except Exception as e:
                self.warnings.append(f"Orders read error {p}: {e}")
        out=pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame(columns=["day"])
        out=out[out["day"].notna()].copy()
        if not out.empty: log(f"Orders rows loaded: {len(out):,}; date range {out['day'].min().date()} .. {out['day'].max().date()}")
        return out

    def load_funnel(self):
        candidates=[self._prefix("Воронка продаж", self.store, "Воронка продаж.xlsx"), self._prefix("Воронка продаж", "Воронка продаж.xlsx"), "Воронка продаж.xlsx", "Воронка продаж (1).xlsx"]
        p=next((x for x in candidates if self.storage.exists(x)), None)
        if p is None:
            files=self._glob(["Воронка продаж*.xlsx"]); p=files[0] if files else None
        if p is None: return pd.DataFrame(columns=["day"])
        try:
            df,sheet=read_excel_flexible(self.storage.read_bytes(p), None, (0,))
            self._record_path("funnel", p, sheet)
            df["day"]=to_dt(df.get("day", pd.Series(dtype=object)))
            df["nm_id"]=to_numeric(df.get("nm_id", np.nan))
            df["orders"]=to_numeric(df.get("orders", np.nan))
            df["buyouts_count"]=to_numeric(df.get("buyouts_count", np.nan))
            out=df[df["day"].notna()].copy()
            log(f"Funnel rows loaded: {len(out):,}; date range {out['day'].min().date()} .. {out['day'].max().date()}")
            return out
        except Exception as e:
            self.warnings.append(f"Funnel read error {p}: {e}")
            return pd.DataFrame(columns=["day"])

    def load_ads(self):
        files=self._list([self._prefix("Реклама", self.store, "Недельные"), self._prefix("Реклама", self.store)])
        if not files: files=self._glob(["Реклама_*.xlsx","Анализ рекламы.xlsx"])
        daily=[]; campaigns=[]
        for p in files:
            try:
                df,sheet=read_excel_flexible(self.storage.read_bytes(p), ["Статистика_Ежедневно"], (0,))
                self._record_path("ads_daily", p, sheet)
                df["day"]=to_dt(df.get("day", df.get("Дата", pd.Series(dtype=object))))
                df["nm_id"]=to_numeric(df.get("nm_id", np.nan))
                df["subject"]=df.get("subject", pd.Series(dtype=object)).map(canonical_subject)
                df["supplier_article"]=df.get("supplier_article", pd.Series(dtype=object)).map(clean_article)
                df["spend"]=to_numeric(df.get("spend", np.nan)).fillna(0)
                daily.append(df[[c for c in ["day","nm_id","supplier_article","subject","campaign_name","spend"] if c in df.columns]].copy())
            except Exception as e:
                self.warnings.append(f"Ads daily read error {p}: {e}")
            try:
                cdf,sheet=read_excel_flexible(self.storage.read_bytes(p), ["Список_кампаний"], (0,))
                self._record_path("ads_campaigns", p, sheet)
                cdf["nm_id"]=to_numeric(cdf.get("nm_id", np.nan))
                if "nm_id" not in cdf.columns and "Артикул WB" in cdf.columns: cdf["nm_id"]=to_numeric(cdf["Артикул WB"])
                cdf["campaign_name"]=cdf.get("campaign_name", cdf.get("Название", pd.Series(dtype=object))).map(normalize_text)
                # extract supplier article from campaign name like 901/14
                extracted=cdf["campaign_name"].str.extract(r"((?:PT)?\d+\/\d+|(?:PT)?\d+\.\w+\d+|(?:PT)?\d+)", expand=False)
                cdf["supplier_article"]=cdf.get("supplier_article", extracted).fillna(extracted).map(clean_article)
                cdf["subject"]=cdf.get("subject", cdf.get("Название предмета", pd.Series(dtype=object))).map(canonical_subject)
                campaigns.append(cdf[[c for c in ["nm_id","supplier_article","subject","campaign_name"] if c in cdf.columns]].copy())
            except Exception:
                pass
        d=pd.concat(daily, ignore_index=True) if daily else pd.DataFrame(columns=["day"])
        c=pd.concat(campaigns, ignore_index=True) if campaigns else pd.DataFrame(columns=["nm_id"])
        if not d.empty:
            d=d[d["day"].notna()].copy()
            log(f"Ads rows loaded: {len(d):,}; date range {d['day'].min().date()} .. {d['day'].max().date()}; spend sum {d['spend'].sum():,.0f}")
        return d,c

    def load_economics(self):
        candidates=[self._prefix("Финансовые показатели", self.store, "Экономика.xlsx"), self._prefix("Финансовые показатели", self.store, "Недельные", "Экономика.xlsx"), "Экономика (4).xlsx","Экономика.xlsx"]
        p=next((x for x in candidates if self.storage.exists(x)), None)
        if p is None:
            files=self._glob(["Экономика*.xlsx"]); p=files[0] if files else None
        if p is None: return pd.DataFrame()
        try:
            df,sheet=read_excel_flexible(self.storage.read_bytes(p), ["Юнит экономика"], (0,1,2))
            self._record_path("economics", p, sheet)
            df["week"]=df.get("week", df.get("Неделя", pd.Series(dtype=object))).astype(str).str.strip()
            df["nm_id"]=to_numeric(df.get("nm_id", np.nan))
            df["supplier_article"]=df.get("supplier_article", pd.Series(dtype=object)).map(clean_article)
            df["subject"]=df.get("subject", pd.Series(dtype=object)).map(canonical_subject)
            df["brand"]=df.get("brand", pd.Series(dtype=object)).map(normalize_text)
            df["title"]=df.get("title", pd.Series(dtype=object)).map(normalize_text)
            mapping={
                "Процент выкупа":"buyout_pct",
                "Комиссия WB, %":"commission_pct",
                "Эквайринг, %":"acquiring_pct",
                "Логистика прямая, руб/ед":"logistics_direct_unit",
                "Логистика обратная, руб/ед":"logistics_return_unit",
                "Хранение, руб/ед":"storage_unit",
                "Прочие расходы, руб/ед":"other_unit",
                "Себестоимость, руб":"cost_unit",
                "НДС, руб/ед":"vat_unit",
                "Валовая прибыль, руб/ед":"gp_unit",
                "Средняя цена продажи":"econ_price_with_disc",
                "Средняя цена покупателя":"econ_finished_price",
            }
            for src,dst in mapping.items():
                if src in df.columns and dst not in df.columns: df[dst]=df[src]
                if dst not in df.columns: df[dst]=np.nan
                df[dst]=to_numeric(df[dst])
            out=df[["week","nm_id","supplier_article","subject","brand","title",*mapping.values()]].copy()
            log(f"Economics rows loaded: {len(out):,}; weeks {', '.join(sorted(out['week'].dropna().astype(str).unique())[-10:])}")
            return out
        except Exception as e:
            self.warnings.append(f"Economics read error {p}: {e}")
            return pd.DataFrame()

    def load_abc(self):
        files=self._list([self._prefix("ABC")]); files=[f for f in files if "wb_abc_report_goods__" in Path(f).name]
        if not files: files=self._glob(["wb_abc_report_goods__*.xlsx"])
        weekly=[]; monthly=[]
        for p in files:
            try:
                df,sheet=read_excel_flexible(self.storage.read_bytes(p), None, (0,))
                self._record_path("abc", p, sheet)
                start,end=parse_abc_period_from_name(Path(p).name)
                if not start or not end: continue
                df["supplier_article"]=df.get("supplier_article", pd.Series(dtype=object)).map(clean_article)
                df["nm_id"]=to_numeric(df.get("nm_id", np.nan))
                df["subject"]=df.get("subject", pd.Series(dtype=object)).map(canonical_subject)
                df["brand"]=df.get("brand", pd.Series(dtype=object)).map(normalize_text)
                df["title"]=df.get("title", pd.Series(dtype=object)).map(normalize_text)
                df["code"]=df["supplier_article"].map(extract_code)
                df["gross_profit"]=to_numeric(df.get("gross_profit", np.nan))
                df["gross_revenue"]=to_numeric(df.get("gross_revenue", np.nan))
                df["orders"]=to_numeric(df.get("orders", df.get("Кол-во продаж", np.nan)))
                df["vat"]=df["gross_revenue"]*7.0/107.0
                df["gp_minus_nds"]=df["gross_profit"]-df["vat"]
                month_end=(pd.Timestamp(start).to_period("M").end_time.normalize()).date()
                if start.day==1 and end==month_end:
                    df["month_key"]=pd.Timestamp(start).strftime("%Y-%m")
                    monthly.append(df[["month_key","supplier_article","nm_id","subject","brand","title","code","gross_profit","gross_revenue","vat","gp_minus_nds","orders"]].copy())
                else:
                    df["week_code"]=week_code_from_date(start)
                    df["week_label"]=pd.Timestamp(start).strftime("%d.%m")
                    df["week_start"]=pd.Timestamp(start); df["week_end"]=pd.Timestamp(end)
                    weekly.append(df[["week_code","week_label","week_start","week_end","supplier_article","nm_id","subject","brand","title","code","gross_profit","gross_revenue","vat","gp_minus_nds","orders"]].copy())
            except Exception as e:
                self.warnings.append(f"ABC read error {p}: {e}")
        w=pd.concat(weekly, ignore_index=True) if weekly else pd.DataFrame()
        m=pd.concat(monthly, ignore_index=True) if monthly else pd.DataFrame()
        if not w.empty: log(f"ABC weekly rows loaded: {len(w):,}; weeks {', '.join(sorted(w['week_code'].astype(str).unique()))}")
        if not m.empty: log(f"ABC monthly rows loaded: {len(m):,}; months {', '.join(sorted(m['month_key'].astype(str).unique()))}")
        return w,m

    def load_plan(self, current_month_key: str):
        candidates=[self._prefix("Объединенный отчет", self.store, "План.xlsx"), "План.xlsx"]
        p=next((x for x in candidates if self.storage.exists(x)), None)
        if p is None: return pd.DataFrame()
        try:
            df=pd.read_excel(io.BytesIO(self.storage.read_bytes(p)), sheet_name="Итог_все_категории", header=2)
            self._record_path("plan", p, "Итог_все_категории")
            df.columns=[normalize_text(c) for c in df.columns]
            df=rename_using_aliases(df)
            df["supplier_article"]=df.get("supplier_article", pd.Series(dtype=object)).map(clean_article)
            df["subject"]=df.get("subject", pd.Series(dtype=object)).map(canonical_subject)
            target=f"ВП-НДС {russian_month_name(pd.Period(current_month_key,freq='M').month)} {pd.Period(current_month_key,freq='M').year}"
            chosen=None
            for c in df.columns:
                if normalize_text(c).lower()==normalize_text(target).lower() or normalize_text(target).lower() in normalize_text(c).lower():
                    chosen=c; break
            if chosen is None: return pd.DataFrame()
            out=df[["supplier_article","subject",chosen]].copy()
            out["plan_gp_minus_nds_month"]=to_numeric(out[chosen]); out["code"]=out["supplier_article"].map(extract_code)
            out=out.drop(columns=[chosen]); log(f"Plan rows loaded: {len(out):,}; non-null plan {out['plan_gp_minus_nds_month'].notna().sum():,}")
            return out
        except Exception as e:
            self.warnings.append(f"Plan read error {p}: {e}")
            return pd.DataFrame()

    def load_all(self) -> LoadedData:
        log("Loading data")
        log("Loading orders"); orders=self.load_orders()
        log("Loading funnel"); funnel=self.load_funnel()
        log("Loading ads"); ads_daily, ads_campaigns=self.load_ads()
        log("Loading economics"); economics=self.load_economics()
        log("Loading ABC"); abc_weekly, abc_monthly=self.load_abc()
        latest_candidates=[]
        for df,col in [(orders,"day"),(funnel,"day"),(ads_daily,"day")]:
            if not df.empty: latest_candidates.append(pd.to_datetime(df[col]).max())
        latest_day=max([x for x in latest_candidates if pd.notna(x)], default=pd.Timestamp.today().normalize())
        log("Loading plan"); plan=self.load_plan(latest_day.to_period("M").strftime("%Y-%m"))
        return LoadedData(orders,funnel,ads_daily,ads_campaigns,economics,abc_weekly,abc_monthly,plan,pd.Timestamp(latest_day).normalize(),pd.DataFrame(self.paths),self.warnings)


class Builder:
    def __init__(self, data: LoadedData):
        self.data=data
        self.latest_day=data.latest_day
        self.current_week_start=self.latest_day - pd.Timedelta(days=self.latest_day.weekday())
        self.current_week_days=[self.current_week_start + pd.Timedelta(days=i) for i in range((self.latest_day-self.current_week_start).days+1)]
        self.current_month_key=self.latest_day.to_period("M").strftime("%Y-%m")
        self.current_month_start=self.latest_day.replace(day=1)
        self.days_in_month=calendar.monthrange(self.latest_day.year, self.latest_day.month)[1]
        self.master=self.build_master()
        self.buyout90=self.build_buyout90()
        self.econ=self.prepare_economics()
        self.subject_week_commission_pct, self.subject_latest_commission_pct = self.build_commission_fallback_maps()
        self.ads=self.prepare_ads()

    def filter_targets(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty: return df.copy()
        out=df.copy()
        if "subject" in out.columns:
            out["subject"]=out["subject"].map(canonical_subject)
            out=out[out["subject"].isin(TARGET_SUBJECTS)].copy()
        if "supplier_article" in out.columns:
            out["supplier_article"]=out["supplier_article"].map(clean_article)
            out=out[~out["supplier_article"].map(lambda x: upper_article(x) in EXCLUDE_ARTICLES)].copy()
        if "code" not in out.columns: out["code"]=out.get("supplier_article", pd.Series(dtype=object)).map(extract_code)
        out=out[out["code"]!=""].copy()
        return out

    def build_master(self) -> pd.DataFrame:
        frames=[]
        for df in [self.data.orders,self.data.economics,self.data.abc_weekly,self.data.abc_monthly]:
            if df.empty: continue
            x=df.copy()
            for c in ["supplier_article","nm_id","subject","brand","title"]:
                if c not in x.columns: x[c]=""
            x=x[["supplier_article","nm_id","subject","brand","title"]]
            frames.append(x)
        # campaigns are important to map ads
        if not self.data.ads_campaigns.empty:
            c=self.data.ads_campaigns.copy()
            for col in ["supplier_article","nm_id","subject"]:
                if col not in c.columns: c[col]=""
            c["brand"]=""; c["title"]=c.get("campaign_name","")
            frames.append(c[["supplier_article","nm_id","subject","brand","title"]])
        m=pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["supplier_article","nm_id","subject","brand","title"])
        m["supplier_article"]=m["supplier_article"].map(clean_article)
        m["nm_id"]=to_numeric(m["nm_id"])
        m["subject"]=m["subject"].map(canonical_subject)
        m["brand"]=m["brand"].map(normalize_text)
        m["title"]=m["title"].map(normalize_text)
        m["code"]=m["supplier_article"].map(extract_code)
        m=self.filter_targets(m)
        m["quality"]=m["supplier_article"].ne("").astype(int)*4 + m["subject"].ne("").astype(int)*3 + m["title"].ne("").astype(int)
        m=m.sort_values("quality", ascending=False)
        art_map=m.drop_duplicates(subset=["supplier_article"], keep="first")
        nm_map=m.dropna(subset=["nm_id"]).drop_duplicates(subset=["nm_id"], keep="first")
        out=pd.concat([art_map,nm_map], ignore_index=True).drop_duplicates(subset=["supplier_article","nm_id"], keep="first")
        return out[["supplier_article","nm_id","subject","brand","title","code"]]

    def attach_master(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or self.master.empty: return df.copy()
        out=df.copy()
        if "supplier_article" in out.columns:
            out=out.merge(self.master[["supplier_article","nm_id","subject","brand","title","code"]].drop_duplicates("supplier_article"), on="supplier_article", how="left", suffixes=("","_m"))
            for c in ["nm_id","subject","brand","title","code"]:
                if f"{c}_m" in out.columns:
                    if c not in out.columns: out[c]=out[f"{c}_m"]
                    else:
                        mask=out[c].isna() | (out[c]=="")
                        out.loc[mask,c]=out.loc[mask,f"{c}_m"]
                    out.drop(columns=[f"{c}_m"], inplace=True)
        if "nm_id" in out.columns:
            out=out.merge(self.master[["nm_id","supplier_article","subject","brand","title","code"]].dropna(subset=["nm_id"]).drop_duplicates("nm_id"), on="nm_id", how="left", suffixes=("","_n"))
            for c in ["supplier_article","subject","brand","title","code"]:
                if f"{c}_n" in out.columns:
                    if c not in out.columns: out[c]=out[f"{c}_n"]
                    else:
                        mask=out[c].isna() | (out[c]=="")
                        out.loc[mask,c]=out.loc[mask,f"{c}_n"]
                    out.drop(columns=[f"{c}_n"], inplace=True)
        return self.filter_targets(out)

    def build_sku_dictionary(self) -> pd.DataFrame:
        d=self.master.copy()
        d=d.sort_values(["subject","code","supplier_article"])
        return d[["subject","code","supplier_article","nm_id","brand","title"]]

    def build_buyout90(self) -> pd.DataFrame:
        f=self.data.funnel.copy()
        if f.empty: return pd.DataFrame(columns=["nm_id","buyout_pct_90"])
        f=f[(f["day"]>=self.latest_day-pd.Timedelta(days=89)) & (f["day"]<=self.latest_day)].copy()
        g=f.groupby("nm_id", dropna=False).agg(orders_90=("orders","sum"), buyouts_90=("buyouts_count","sum")).reset_index()
        g["buyout_pct_90"]=g.apply(lambda r: safe_div(r["buyouts_90"], r["orders_90"]), axis=1)
        log(f"Buyout90 rows: {len(g):,}; non-null ratios {g['buyout_pct_90'].notna().sum():,}")
        return g[["nm_id","buyout_pct_90"]]

    def prepare_economics(self) -> pd.DataFrame:
        econ=self.attach_master(self.filter_targets(self.data.economics))
        if econ.empty: return econ
        econ["week_start"]=econ["week"].map(lambda x: pd.Timestamp(week_bounds_from_code(str(x))[0]) if week_bounds_from_code(str(x))[0] else pd.NaT)
        econ=econ.sort_values(["supplier_article","week_start"], ascending=[True,False])
        log(f"Economics usable rows: {len(econ):,}; articles {econ['supplier_article'].nunique():,}")
        return econ

    def build_commission_fallback_maps(self) -> Tuple[Dict[Tuple[str,str], float], Dict[str,float]]:
        econ = self.econ.copy()
        if econ.empty:
            return {}, {}
        econ["subject"] = econ["subject"].map(canonical_subject)
        econ["commission_pct"] = pd.to_numeric(econ.get("commission_pct", np.nan), errors="coerce")
        valid = econ[econ["commission_pct"].fillna(0) > 0].copy()
        if valid.empty:
            return {}, {}
        # subject + week fallback: median of non-zero commission for that subject/week
        sw = valid.groupby(["subject", "week"], dropna=False)["commission_pct"].median().reset_index()
        subject_week = {(normalize_text(r.subject), str(r.week)): float(r.commission_pct) for r in sw.itertuples(index=False)}
        # latest non-zero by subject
        if "week_start" in valid.columns:
            valid = valid.sort_values(["subject", "week_start"], ascending=[True, False])
        else:
            valid = valid.sort_values(["subject"], ascending=[True])
        sl = valid.drop_duplicates(subset=["subject"], keep="first")[["subject", "commission_pct"]].copy()
        subject_latest = {normalize_text(r.subject): float(r.commission_pct) for r in sl.itertuples(index=False)}
        log(f"Commission fallback maps: subject_week={len(subject_week):,}, subject_latest={len(subject_latest):,}")
        return subject_week, subject_latest

    def prepare_ads(self) -> pd.DataFrame:
        ads=self.data.ads_daily.copy()
        if ads.empty: return ads
        # attach campaigns first, then master
        if not self.data.ads_campaigns.empty:
            camp=self.data.ads_campaigns.copy()
            camp=camp[["nm_id","supplier_article","subject","campaign_name"]].drop_duplicates()
            ads=ads.merge(camp, on=["nm_id","campaign_name"], how="left", suffixes=("","_c"))
            for c in ["supplier_article","subject"]:
                if f"{c}_c" in ads.columns:
                    if c not in ads.columns: ads[c]=ads[f"{c}_c"]
                    else:
                        mask=ads[c].isna() | (ads[c]=="")
                        ads.loc[mask,c]=ads.loc[mask,f"{c}_c"]
                    ads.drop(columns=[f"{c}_c"], inplace=True)
        ads=self.attach_master(ads)
        # only now filter targets
        ads=self.filter_targets(ads)
        return ads

    def match_ads_daily(self) -> Tuple[pd.DataFrame,pd.DataFrame,pd.DataFrame]:
        ads=self.ads.copy()
        if ads.empty:
            diag=pd.DataFrame([{"ads_rows_source":0,"ads_rows_after_mapping":0,"ads_spend_source":0.0,"ads_spend_after_mapping":0.0}])
            return pd.DataFrame(columns=["day","supplier_article","nm_id","ad_spend"]), pd.DataFrame(columns=["day","nm_id","ad_spend_nm"]), diag
        by_both=ads.groupby(["day","supplier_article","nm_id"], dropna=False)["spend"].sum().reset_index().rename(columns={"spend":"ad_spend"})
        by_nm=ads.groupby(["day","nm_id"], dropna=False)["spend"].sum().reset_index().rename(columns={"spend":"ad_spend_nm"})
        diag=pd.DataFrame([{
            "ads_rows_source": len(self.data.ads_daily),
            "ads_rows_after_mapping": len(ads),
            "ads_spend_source": float(self.data.ads_daily["spend"].fillna(0).sum()) if not self.data.ads_daily.empty else 0.0,
            "ads_spend_after_mapping": float(ads["spend"].sum()),
            "ads_unique_nm_after_mapping": int(ads["nm_id"].nunique()),
            "ads_unique_articles_after_mapping": int(ads["supplier_article"].replace("", np.nan).nunique()),
        }])
        return by_both, by_nm, diag

    def pick_econ_for_day(self, daily_keys: pd.DataFrame) -> Tuple[pd.DataFrame,pd.DataFrame]:
        rows=[]; diag=[]
        exact=fallback=missing=0
        commission_replaced=0
        for rec in daily_keys.itertuples(index=False):
            day=rec.day; art=rec.supplier_article; nm=rec.nm_id
            target_week=week_code_from_date(day)
            g=self.econ[self.econ["supplier_article"]==art].copy()
            if g.empty and pd.notna(nm):
                g=self.econ[self.econ["nm_id"]==nm].copy()
            if g.empty:
                missing += 1
                diag.append({"day":day,"supplier_article":art,"nm_id":nm,"target_week":target_week,"picked_week":None,"match_type":"missing","commission_source":"missing"})
                continue
            eg=g[g["week"].astype(str)==str(target_week)]
            if not eg.empty:
                chosen=eg.iloc[0].copy(); exact += 1; mt="exact"
            else:
                g2=g.sort_values(["week_start"], ascending=[False])
                chosen=g2.iloc[0].copy(); fallback += 1; mt="fallback_latest"

            subject = normalize_text(chosen.get("subject", "")) or normalize_text(getattr(rec, "subject", ""))
            commission_source = "row"
            commission_pct = pd.to_numeric(chosen.get("commission_pct", np.nan), errors="coerce")
            if pd.isna(commission_pct) or float(commission_pct) == 0:
                commission_pct = self.subject_week_commission_pct.get((subject, str(target_week)), np.nan)
                if pd.notna(commission_pct) and float(commission_pct) > 0:
                    commission_source = "subject_week"
                    commission_replaced += 1
                else:
                    commission_pct = self.subject_latest_commission_pct.get(subject, np.nan)
                    if pd.notna(commission_pct) and float(commission_pct) > 0:
                        commission_source = "subject_latest"
                        commission_replaced += 1
                    else:
                        commission_source = "zero_or_missing"

            row={"day":day,"supplier_article":art,"nm_id":nm}
            for c in ["week","buyout_pct","acquiring_pct","logistics_direct_unit","logistics_return_unit","storage_unit","other_unit","cost_unit","vat_unit","gp_unit","econ_price_with_disc","econ_finished_price"]:
                row[c]=chosen.get(c,np.nan)
            row["commission_pct"] = commission_pct
            rows.append(row)
            diag.append({
                "day":day,"supplier_article":art,"nm_id":nm,"target_week":target_week,"picked_week":chosen.get("week"),
                "match_type":mt,"commission_source":commission_source,"commission_pct_used":commission_pct,"subject":subject
            })
        log(f"Economics matching: exact week = {exact:,}, fallback latest = {fallback:,}, missing = {missing:,}, commission replaced = {commission_replaced:,}")
        return pd.DataFrame(rows), pd.DataFrame(diag)

    def build_daily_calc(self) -> Tuple[pd.DataFrame,Dict[str,pd.DataFrame]]:
        orders=self.attach_master(self.filter_targets(self.data.orders))
        orders=orders[(orders["day"]>=self.current_week_start) & (orders["day"]<=self.latest_day)].copy()
        if orders.empty:
            return pd.DataFrame(), {"orders_daily": orders}
        log(f"Current week order rows: {len(orders):,}; day range {orders['day'].min().date()} .. {orders['day'].max().date()}")
        daily=orders.groupby(["day","subject","code","supplier_article","nm_id"], dropna=False).agg(
            orders_day=("orders","sum"),
            finished_price_avg=("finished_price", lambda s: weighted_mean(s, orders.loc[s.index,"orders"])),
            price_with_disc_avg=("price_with_disc", lambda s: weighted_mean(s, orders.loc[s.index,"orders"])),
            spp_avg=("spp", lambda s: weighted_mean(s, orders.loc[s.index,"orders"])),
        ).reset_index()
        daily=daily.merge(self.buyout90, on="nm_id", how="left")
        econ_pick,econ_diag=self.pick_econ_for_day(daily[["day","supplier_article","nm_id"]].drop_duplicates())
        daily=daily.merge(econ_pick, on=["day","supplier_article","nm_id"], how="left")
        ads_by_both, ads_by_nm, ads_diag=self.match_ads_daily()
        daily=daily.merge(ads_by_both, on=["day","supplier_article","nm_id"], how="left")
        daily=daily.merge(ads_by_nm, on=["day","nm_id"], how="left")
        daily["ad_spend"]=daily["ad_spend"].fillna(daily["ad_spend_nm"]).fillna(0.0)
        log(f"Ads matching to daily rows: matched rows = {(daily['ad_spend']>0).sum():,} из {len(daily):,}; spend matched = {daily['ad_spend'].sum():,.0f}")
        daily["buyout_factor"]=daily["buyout_pct_90"].fillna(daily["buyout_pct"]/100.0).fillna(1.0)
        daily["buyout_qty"]=daily["orders_day"]*daily["buyout_factor"]
        daily["used_price_with_disc"]=daily["price_with_disc_avg"].fillna(daily["econ_price_with_disc"]).fillna(0)
        daily["used_finished_price"]=daily["finished_price_avg"].fillna(daily["econ_finished_price"]).fillna(0)
        daily["revenue_pwd"]=daily["buyout_qty"]*daily["used_price_with_disc"]
        daily["commission_rub"]=daily["revenue_pwd"]*daily["commission_pct"].fillna(0)/100.0
        daily["acquiring_rub"]=daily["revenue_pwd"]*daily["acquiring_pct"].fillna(0)/100.0
        daily["logistics_direct_rub"]=daily["buyout_qty"]*daily["logistics_direct_unit"].fillna(0)
        daily["logistics_return_rub"]=daily["buyout_qty"]*daily["logistics_return_unit"].fillna(0)
        daily["storage_rub"]=daily["buyout_qty"]*daily["storage_unit"].fillna(0)
        daily["other_rub"]=daily["buyout_qty"]*daily["other_unit"].fillna(0)
        daily["cost_rub"]=daily["buyout_qty"]*daily["cost_unit"].fillna(0)
        daily["vat_rub"]=daily["buyout_qty"]*daily["used_finished_price"]*7.0/107.0
        daily["gross_profit_rub"]=daily["revenue_pwd"]-daily["commission_rub"]-daily["acquiring_rub"]-daily["logistics_direct_rub"]-daily["logistics_return_rub"]-daily["storage_rub"]-daily["other_rub"]-daily["cost_rub"]-daily["ad_spend"]
        daily["gp_minus_nds_rub"]=daily["gross_profit_rub"]-daily["vat_rub"]
        daily["day_label"]=daily["day"].dt.strftime("%d.%m")
        log(f"Commission diagnostics: zero/empty commission_pct rows after subject fallback = {(daily['commission_pct'].fillna(0)==0).sum():,} из {len(daily):,}")
        tech={
            "orders_daily":daily.copy(),
            "ads_diag":ads_diag,
            "econ_match_diag":econ_diag,
            "sku_dictionary":self.build_sku_dictionary(),
            "source_paths":self.data.source_paths.copy(),
        }
        return daily, tech

    def build_weekly_fact(self):
        abc=self.attach_master(self.filter_targets(self.data.abc_weekly))
        if abc.empty: return abc
        abc=abc[(abc["week_end"]>=self.current_month_start) & (abc["week_start"]<=self.latest_day)].copy()
        log(f"ABC weeks used in current month block: {', '.join(sorted(abc['week_code'].astype(str).unique()))}")
        return abc

    def build_monthly_fact(self):
        am=self.attach_master(self.filter_targets(self.data.abc_monthly))
        aw=self.attach_master(self.filter_targets(self.data.abc_weekly))
        periods=[self.latest_day.to_period("M")-2,self.latest_day.to_period("M")-1,self.latest_day.to_period("M")]
        keys=[p.strftime("%Y-%m") for p in periods]
        frames=[]
        if not am.empty: frames.append(am[am["month_key"].isin(keys)].copy())
        if self.current_month_key not in set(am.get("month_key", pd.Series(dtype=str)).astype(str)):
            wk=aw.copy()
            if not wk.empty:
                wk["month_key"]=wk["week_start"].dt.to_period("M").astype(str)
                wk=wk[wk["month_key"]==self.current_month_key].copy()
                if not wk.empty:
                    cur=wk.groupby(["month_key","subject","code","supplier_article","nm_id"], dropna=False).agg(gross_profit=("gross_profit","sum"),gross_revenue=("gross_revenue","sum"),vat=("vat","sum"),gp_minus_nds=("gp_minus_nds","sum"),orders=("orders","sum")).reset_index()
                    frames=[f[f["month_key"]!=self.current_month_key] for f in frames]
                    frames.append(cur)
        out=pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        if not out.empty: log(f"ABC months used in 3-month block: {', '.join(sorted(out['month_key'].astype(str).unique()))}")
        return out

    def build_plan(self):
        return self.attach_master(self.filter_targets(self.data.plan))

    def month_maps(self, monthly):
        cur=monthly[monthly["month_key"]==self.current_month_key].copy() if not monthly.empty else pd.DataFrame()
        return (
            cur.groupby("supplier_article",dropna=False)["gp_minus_nds"].sum().to_dict() if not cur.empty else {},
            cur.groupby(["subject","code"],dropna=False)["gp_minus_nds"].sum().to_dict() if not cur.empty else {},
            cur.groupby("subject",dropna=False)["gp_minus_nds"].sum().to_dict() if not cur.empty else {},
        )

    def aggregate(self, base, value_col, label_col, labels, daily_mode, monthly, plan):
        if base.empty: return pd.DataFrame()
        art_fact, prod_fact, cat_fact=self.month_maps(monthly)
        art_plan=plan.set_index("supplier_article")["plan_gp_minus_nds_month"].to_dict() if not plan.empty else {}
        prod_plan=plan.groupby(["subject","code"],dropna=False)["plan_gp_minus_nds_month"].sum().to_dict() if not plan.empty else {}
        cat_plan=plan.groupby("subject",dropna=False)["plan_gp_minus_nds_month"].sum().to_dict() if not plan.empty else {}
        rows=[]
        def calc_plan(level, subject, code, art, facts):
            if daily_mode:
                val = art_plan.get(art,np.nan) if level=="article" else prod_plan.get((subject,code),np.nan) if level=="product" else cat_plan.get(subject,np.nan)
                if pd.isna(val): return float(np.nanmean(facts)) if facts else 0.0
                return float(val)/self.days_in_month
            else:
                val = art_plan.get(art,np.nan) if level=="article" else prod_plan.get((subject,code),np.nan) if level=="product" else cat_plan.get(subject,np.nan)
                if pd.isna(val):
                    return float(art_fact.get(art,0.0)) if level=="article" else float(prod_fact.get((subject,code),0.0)) if level=="product" else float(cat_fact.get(subject,0.0))
                return float(val)
        for subject in TARGET_SUBJECTS:
            sg=base[base["subject"]==subject].copy()
            if sg.empty: continue
            facts=[float(sg.loc[sg[label_col]==lbl, value_col].sum()) for lbl in labels]
            rows.append({"Наименование":subject,"_kind":"category", **{lbl:v for lbl,v in zip(labels,facts)}, "План":calc_plan("category",subject,"","",facts)})
            for code in sg.groupby("code")[value_col].sum().sort_values(ascending=False).index.tolist():
                pg=sg[sg["code"]==code]
                facts=[float(pg.loc[pg[label_col]==lbl, value_col].sum()) for lbl in labels]
                rows.append({"Наименование":str(code),"_kind":"product","_subject":subject,"_code":code, **{lbl:v for lbl,v in zip(labels,facts)}, "План":calc_plan("product",subject,code,"",facts)})
                for art in pg.groupby("supplier_article")[value_col].sum().sort_values(ascending=False).index.tolist():
                    ag=pg[pg["supplier_article"]==art]
                    facts=[float(ag.loc[ag[label_col]==lbl, value_col].sum()) for lbl in labels]
                    rows.append({"Наименование":art,"_kind":"article","_subject":subject,"_code":code,"_article":art, **{lbl:v for lbl,v in zip(labels,facts)}, "План":calc_plan("article",subject,code,art,facts)})
            totals={lbl: float(sg.loc[sg[label_col]==lbl, value_col].sum()) for lbl in labels}
            rows.append({"Наименование":f"Итого {subject}","_kind":"subject_total","_subject":subject, **totals, "План":calc_plan("category",subject,"","",list(totals.values()))})
        grand={lbl: float(base.loc[base[label_col]==lbl, value_col].sum()) for lbl in labels}
        rows.append({"Наименование":"Итого по всем 4 категориям","_kind":"grand_total", **grand, "План": (float(sum(v for v in cat_plan.values() if pd.notna(v)))/self.days_in_month if daily_mode else float(sum(v for v in cat_plan.values() if pd.notna(v)))) if cat_plan else float(np.nanmean(list(grand.values())))})
        return pd.DataFrame(rows)

    def build_examples(self, articles, daily, weekly):
        orders=self.attach_master(self.filter_targets(self.data.orders)); orders["week_code"]=orders["day"].map(week_code_from_date)
        ads=self.ads.copy(); abc=self.attach_master(self.filter_targets(self.data.abc_weekly))
        rows=[]
        for art in articles:
            ow_all=orders[orders["supplier_article"]==art].copy()
            if ow_all.empty: continue
            nm=ow_all["nm_id"].dropna().iloc[0] if ow_all["nm_id"].notna().any() else np.nan
            subject=ow_all["subject"].dropna().iloc[0] if ow_all["subject"].notna().any() else ""
            weeks=sorted(ow_all["week_code"].dropna().unique())[-4:]
            for wk in weeks:
                ws,we=week_bounds_from_code(wk); ws=pd.Timestamp(ws) if ws else pd.NaT; we=pd.Timestamp(we) if we else pd.NaT
                ow=ow_all[ow_all["week_code"]==wk].copy()
                ords=ow["orders"].sum()
                f=self.data.funnel.copy()
                if pd.notna(nm):
                    f=f[(f["nm_id"]==nm) & (f["day"]>=we-pd.Timedelta(days=89)) & (f["day"]<=we)]
                    buyout=safe_div(f["buyouts_count"].sum(), f["orders"].sum())
                else:
                    buyout=np.nan
                eg=self.econ[(self.econ["supplier_article"]==art) & (self.econ["week"].astype(str)==str(wk))]
                if eg.empty: eg=self.econ[self.econ["supplier_article"]==art].head(1)
                if eg.empty: continue
                e=eg.iloc[0]
                buyout=buyout if pd.notna(buyout) else safe_div(e.get("buyout_pct"),100)
                if pd.isna(buyout): buyout=1.0
                buy_qty=ords*buyout
                pwd=weighted_mean(ow["price_with_disc"], ow["orders"]); fp=weighted_mean(ow["finished_price"], ow["orders"])
                revenue=buy_qty*pwd
                comm_pct = pd.to_numeric(e.get("commission_pct", np.nan), errors="coerce")
                if pd.isna(comm_pct) or float(comm_pct) == 0:
                    comm_pct = self.subject_week_commission_pct.get((normalize_text(subject), str(wk)), np.nan)
                    if pd.isna(comm_pct) or float(comm_pct) == 0:
                        comm_pct = self.subject_latest_commission_pct.get(normalize_text(subject), 0.0)
                commission=revenue*float(comm_pct or 0)/100.0
                acquiring=revenue*float(e.get("acquiring_pct",0) or 0)/100.0
                logistics_direct=buy_qty*float(e.get("logistics_direct_unit",0) or 0)
                logistics_return=buy_qty*float(e.get("logistics_return_unit",0) or 0)
                storage=buy_qty*float(e.get("storage_unit",0) or 0)
                other=buy_qty*float(e.get("other_unit",0) or 0)
                cost=buy_qty*float(e.get("cost_unit",0) or 0)
                ad_spend=ads[(ads["supplier_article"]==art) & (ads["day"]>=ws) & (ads["day"]<=we)]["spend"].sum() if not ads.empty else 0.0
                vat=buy_qty*fp*7.0/107.0
                gp=revenue-commission-acquiring-logistics_direct-logistics_return-storage-other-cost-ad_spend
                gp_minus_nds=gp-vat
                ab=abc[(abc["supplier_article"]==art) & (abc["week_code"].astype(str)==str(wk))]
                rows.append({
                    "Артикул":art,"Категория":subject,"Неделя":wk,"Заказы":ords,"% выкупа 90д":buyout,
                    "Выкупленные продажи":buy_qty,"Средний priceWithDisc":pwd,"Средний finishedPrice":fp,
                    "Выручка по priceWithDisc":revenue,"Комиссия WB":commission,"Эквайринг":acquiring,
                    "Логистика прямая":logistics_direct,"Логистика обратная":logistics_return,
                    "Хранение":storage,"Прочие расходы":other,"Себестоимость":cost,"Реклама":ad_spend,"НДС":vat,
                    "Валовая прибыль прогноз":gp,"Валовая прибыль - НДС прогноз":gp_minus_nds,
                    "ABC Валовая прибыль":ab["gross_profit"].sum() if not ab.empty else np.nan,
                    "ABC НДС":ab["vat"].sum() if not ab.empty else np.nan,
                    "ABC Валовая прибыль - НДС":ab["gp_minus_nds"].sum() if not ab.empty else np.nan,
                })
        return pd.DataFrame(rows)

    def build(self):
        log("Building stage 1")
        daily, tech=self.build_daily_calc()
        weekly=self.build_weekly_fact()
        monthly=self.build_monthly_fact()
        plan=self.build_plan()
        day_labels=[d.strftime("%d.%m") for d in self.current_week_days]
        week_labels=sorted(weekly["week_label"].dropna().unique().tolist()) if not weekly.empty else []
        month_keys=[(self.latest_day.to_period("M")-2).strftime("%Y-%m"),(self.latest_day.to_period("M")-1).strftime("%Y-%m"),self.current_month_key]
        blocks={
            "daily_main": self.aggregate(daily,"gp_minus_nds_rub","day_label",day_labels,True,monthly,plan) if not daily.empty else pd.DataFrame(),
            "daily_gp": self.aggregate(daily,"gross_profit_rub","day_label",day_labels,True,monthly,plan) if not daily.empty else pd.DataFrame(),
            "daily_vat": self.aggregate(daily,"vat_rub","day_label",day_labels,True,monthly,plan) if not daily.empty else pd.DataFrame(),
            "weekly_main": self.aggregate(weekly,"gp_minus_nds","week_label",week_labels,False,monthly,plan) if not weekly.empty else pd.DataFrame(),
            "monthly_main": self.aggregate(monthly,"gp_minus_nds","month_key",month_keys,False,monthly,plan) if not monthly.empty else pd.DataFrame(),
            "tech_daily": daily,
            "tech_weekly": weekly,
            "tech_monthly": monthly,
            "tech_buyout90": self.buyout90,
            "tech_plan": plan,
            "tech_dictionary": self.build_sku_dictionary(),
            "tech_paths": self.data.source_paths,
            "tech_ads_diag": tech["ads_diag"],
            "tech_econ_match_diag": tech["econ_match_diag"],
            "example": self.build_examples(EXAMPLE_ARTICLES, daily, weekly),
        }
        return blocks


def set_header(cell):
    cell.fill=FILL_HEADER; cell.font=Font(bold=True); cell.border=BORDER; cell.alignment=Alignment(horizontal="center", vertical="center", wrap_text=True)
def fmt_money(c): c.number_format='# ##0 "₽"'
def autofit(ws):
    widths={}
    for row in ws.iter_rows():
        for c in row:
            if c.value is None: continue
            widths[c.column]=max(widths.get(c.column,0), len(str(c.value))+2)
    for i,w in widths.items():
        ws.column_dimensions[get_column_letter(i)].width = 28 if i==1 else min(max(w,12),18)
def style_title(ws,row,start,end,title):
    ws.merge_cells(start_row=row,start_column=start,end_row=row,end_column=end)
    c=ws.cell(row,start,title); c.fill=FILL_SECTION; c.font=Font(bold=True,size=12); c.alignment=Alignment(horizontal="center", vertical="center")

def write_block(ws, start_row, title, df):
    if df is None or df.empty:
        ws.cell(start_row,1,title).font=Font(bold=True); ws.cell(start_row+1,1,"Нет данных"); return start_row+3
    cols=[c for c in df.columns if not c.startswith("_")]
    style_title(ws,start_row,1,len(cols),title)
    hdr=start_row+1
    for j,col in enumerate(cols,1): set_header(ws.cell(hdr,j,col if col!="Наименование" else ""))
    row=hdr+1
    current_category=None; current_product=None
    for _, rec in df.iterrows():
        kind=rec.get("_kind","")
        if kind=="category":
            current_category=row
            current_product=None
        elif kind=="product":
            current_product=row
            ws.row_dimensions[row].outlineLevel=1
        elif kind=="article":
            ws.row_dimensions[row].outlineLevel=2
            ws.row_dimensions[row].hidden=True
        for j,col in enumerate(cols,1):
            c=ws.cell(row,j,rec[col]); c.border=BORDER; c.alignment=Alignment(horizontal="center", vertical="center")
            if j>=2 and isinstance(rec[col], (int,float,np.integer,np.floating)) and not pd.isna(rec[col]): fmt_money(c)
        if kind=="category":
            for j in range(1,len(cols)+1): ws.cell(row,j).font=Font(bold=True); ws.cell(row,j).fill=FILL_CATEGORY
        elif kind=="product":
            for j in range(1,len(cols)+1): ws.cell(row,j).font=Font(bold=True, italic=True); ws.cell(row,j).fill=FILL_PRODUCT
        elif kind in {"subject_total","grand_total"}:
            for j in range(1,len(cols)+1): ws.cell(row,j).font=Font(bold=True); ws.cell(row,j).fill=FILL_TOTAL
        row += 1
    ws.sheet_properties.outlinePr.summaryBelow=False
    return row+2

def write_df_sheet(wb, name, df):
    ws=wb.create_sheet(name[:31])
    if df is None or df.empty:
        ws.cell(1,1,"Нет данных"); return
    for j,col in enumerate(df.columns,1): set_header(ws.cell(1,j,col))
    for i,row in enumerate(df.itertuples(index=False),2):
        for j,val in enumerate(row,1):
            c=ws.cell(i,j,val); c.border=BORDER; c.alignment=Alignment(horizontal="center", vertical="center")
            if isinstance(val,(int,float,np.integer,np.floating)) and not pd.isna(val):
                n=df.columns[j-1].lower()
                if "%" in df.columns[j-1] or "процент" in n: c.number_format='0.00%'
                elif any(k in n for k in ["руб","прибыль","ндс","расход","выруч","цена","себестоим","план"]): fmt_money(c)
                else: c.number_format='# ##0.00'
    autofit(ws); ws.freeze_panes="A2"

def export(blocks, report_path, tech_path, example_path):
    wb=Workbook(); ws=wb.active; ws.title="Сводка"
    row=1
    row=write_block(ws,row,"Текущая неделя — Валовая прибыль - НДС", blocks["daily_main"])
    row=write_block(ws,row,"Текущая неделя — Валовая прибыль", blocks["daily_gp"])
    row=write_block(ws,row,"Текущая неделя — НДС", blocks["daily_vat"])
    row=write_block(ws,row,"Текущий месяц — Валовая прибыль - НДС по неделям", blocks["weekly_main"])
    row=write_block(ws,row,"Последние 3 месяца — Валовая прибыль - НДС", blocks["monthly_main"])
    autofit(ws); ws.freeze_panes="B3"; wb.save(report_path)

    twb=Workbook(); twb.remove(twb.active)
    for key in ["tech_daily","tech_weekly","tech_monthly","tech_buyout90","tech_plan","tech_dictionary","tech_paths","tech_ads_diag","tech_econ_match_diag"]:
        write_df_sheet(twb,key.replace("tech_",""),blocks.get(key,pd.DataFrame()))
    twb.save(tech_path)

    ewb=Workbook(); ewb.remove(ewb.active)
    ex=blocks.get("example", pd.DataFrame())
    if ex.empty:
        ws=ewb.create_sheet("Пример"); ws.cell(1,1,"Нет данных")
    else:
        for art in EXAMPLE_ARTICLES:
            write_df_sheet(ewb, art.replace("/","_"), ex[ex["Артикул"]==art].copy())
    ewb.save(example_path)

def parse_args():
    p=argparse.ArgumentParser()
    p.add_argument("--root", default=".")
    p.add_argument("--reports-root", default="Отчёты")
    p.add_argument("--store", default="TOPFACE")
    p.add_argument("--out-subdir", default="Отчёты/Объединенный отчет/TOPFACE")
    return p.parse_args()

def main():
    args=parse_args()
    storage=make_storage(args.root)
    loader=Loader(storage,args.reports_root,args.store)
    data=loader.load_all()
    for w in data.warnings: log(f"WARN: {w}")
    builder=Builder(data)
    blocks=builder.build()
    stamp=datetime.now().strftime("%Y-%m-%d")
    report=f"{args.out_subdir}/Объединенный_отчет_{args.store}_{stamp}.xlsx"
    tech=f"{args.out_subdir}/Технические_расчеты_{args.store}_{stamp}.xlsx"
    example=f"{args.out_subdir}/Пример_расчета_901_{args.store}_{stamp}.xlsx"
    local_report=Path("/tmp")/f"report_{stamp}.xlsx"
    local_tech=Path("/tmp")/f"tech_{stamp}.xlsx"
    local_example=Path("/tmp")/f"example_{stamp}.xlsx"
    export(blocks,str(local_report),str(local_tech),str(local_example))
    storage.write_bytes(report, local_report.read_bytes())
    storage.write_bytes(tech, local_tech.read_bytes())
    storage.write_bytes(example, local_example.read_bytes())
    log(f"Saved report: {report}")
    log(f"Saved technical workbook: {tech}")
    log(f"Saved example workbook: {example}")
    return 0

if __name__=="__main__":
    raise SystemExit(main())
