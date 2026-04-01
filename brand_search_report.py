
from __future__ import annotations

import io
import json
import os
import re
import zipfile
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional

import boto3
import pandas as pd
import requests
from openpyxl import load_workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side


# =========================
# Базовые настройки
# =========================

DEFAULT_BRAND_SEEDS = [
    "topface",
    "top face",
    "top-face",
    "top face ",
    "топфейс",
    "топ фейс",
    "топ-фейс",
    "топфе",
    "topfac",
    "topfase",
    "top fase",
]

# Эти префиксы можно переопределить через переменную окружения WB_ARCHIVE_PREFIXES
DEFAULT_ARCHIVE_PREFIXES = [
    "Отчёты/Поисковые запросы/TOPFACE/Архив/",
    "Отчёты/Поисковые запросы/TOPFACE/Архивы/",
]

FONT_NAME = "Calibri"
FONT_SIZE = 11
FILL_HEADER = PatternFill("solid", fgColor="D9EAF7")
BORDER_THIN = Border(
    left=Side(style="thin", color="D0D7DE"),
    right=Side(style="thin", color="D0D7DE"),
    top=Side(style="thin", color="D0D7DE"),
    bottom=Side(style="thin", color="D0D7DE"),
)


@dataclass
class Config:
    bucket: str
    access_key: str
    secret_key: str
    endpoint_url: str
    region_name: str

    telegram_bot_token: str
    telegram_chat_id: str
    force_send: bool
    send_on_manual: bool
    run_date: date

    store_name: str
    wb_keywords_prefix: str
    output_prefix: str
    archive_prefixes: list[str]

    yandex_api_key: str
    yandex_folder_id: str
    yandex_region_id: Optional[str]
    yandex_top_url: str

    brand_seeds: list[str]
    yandex_max_phrases: int
    wb_weeks_to_compare: int
    wb_use_only_orders_filter: bool


class S3Storage:
    def __init__(self, cfg: Config) -> None:
        self.bucket = cfg.bucket
        self.client = boto3.client(
            "s3",
            endpoint_url=cfg.endpoint_url,
            aws_access_key_id=cfg.access_key,
            aws_secret_access_key=cfg.secret_key,
            region_name=cfg.region_name,
        )

    def list_keys(self, prefix: str) -> list[str]:
        keys: list[str] = []
        token = None
        while True:
            kwargs = {"Bucket": self.bucket, "Prefix": prefix, "MaxKeys": 1000}
            if token:
                kwargs["ContinuationToken"] = token
            resp = self.client.list_objects_v2(**kwargs)
            for item in resp.get("Contents", []):
                key = item["Key"]
                if not key.endswith("/"):
                    keys.append(key)
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        return keys

    def read_bytes(self, key: str) -> bytes:
        obj = self.client.get_object(Bucket=self.bucket, Key=key)
        return obj["Body"].read()

    def write_bytes(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> None:
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=data,
            ContentType=content_type,
        )


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


def getenv(name: str, default: Optional[str] = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Не задана переменная окружения {name}")
    return value


def parse_bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "да"}


def parse_list_env(name: str, default: list[str]) -> list[str]:
    value = os.getenv(name)
    if not value:
        return list(default)
    parts = [x.strip() for x in re.split(r"[;\n,]+", value) if x.strip()]
    return parts or list(default)


def normalize_text(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def normalize_query(value: object) -> str:
    text = normalize_text(value).lower().replace("ё", "е")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def safe_float(value: object) -> float:
    if value is None:
        return 0.0
    try:
        if pd.isna(value):
            return 0.0
    except Exception:
        pass
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(" ", "").replace(",", ".").replace("%", "")
    if not text:
        return 0.0
    try:
        return float(text)
    except Exception:
        return 0.0


def safe_int(value: object) -> int:
    return int(round(safe_float(value)))


def build_config() -> Config:
    bucket = getenv("YC_BUCKET_NAME")
    access_key = getenv("YC_ACCESS_KEY_ID")
    secret_key = getenv("YC_SECRET_ACCESS_KEY")
    endpoint_url = os.getenv("YC_ENDPOINT_URL", "https://storage.yandexcloud.net")
    region_name = os.getenv("YC_REGION_NAME", "ru-central1")

    telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    force_send = parse_bool_env("FORCE_SEND", False)
    send_on_manual = parse_bool_env("SEND_ON_MANUAL", True)
    run_date = datetime.now().date()

    store_name = os.getenv("STORE_NAME", "TOPFACE")
    wb_keywords_prefix = os.getenv("WB_KEYWORDS_PREFIX", f"Отчёты/Поисковые запросы/{store_name}/Недельные/")
    output_prefix = os.getenv("BRAND_REPORT_OUTPUT_PREFIX", f"Отчёты/Поисковые запросы/Брендовый отчет/{store_name}/")
    archive_prefixes = parse_list_env("WB_ARCHIVE_PREFIXES", DEFAULT_ARCHIVE_PREFIXES)

    yandex_api_key = os.getenv("YANDEX_API_KEY", "")
    yandex_folder_id = os.getenv("YANDEX_FOLDER_ID", "")
    yandex_region_id = os.getenv("YANDEX_REGION_ID", None)
    yandex_top_url = os.getenv("YANDEX_TOP_URL", "https://searchapi.api.cloud.yandex.net/v2/wordstat/topRequests")

    brand_seeds = parse_list_env("BRAND_SEEDS", DEFAULT_BRAND_SEEDS)
    yandex_max_phrases = int(os.getenv("YANDEX_MAX_PHRASES", "30"))
    wb_weeks_to_compare = int(os.getenv("WB_WEEKS_TO_COMPARE", "8"))
    wb_use_only_orders_filter = parse_bool_env("WB_USE_ONLY_ORDERS_FILTER", True)

    return Config(
        bucket=bucket,
        access_key=access_key,
        secret_key=secret_key,
        endpoint_url=endpoint_url,
        region_name=region_name,
        telegram_bot_token=telegram_bot_token,
        telegram_chat_id=telegram_chat_id,
        force_send=force_send,
        send_on_manual=send_on_manual,
        run_date=run_date,
        store_name=store_name,
        wb_keywords_prefix=wb_keywords_prefix,
        output_prefix=output_prefix,
        archive_prefixes=archive_prefixes,
        yandex_api_key=yandex_api_key,
        yandex_folder_id=yandex_folder_id,
        yandex_region_id=yandex_region_id,
        yandex_top_url=yandex_top_url,
        brand_seeds=brand_seeds,
        yandex_max_phrases=yandex_max_phrases,
        wb_weeks_to_compare=wb_weeks_to_compare,
        wb_use_only_orders_filter=wb_use_only_orders_filter,
    )


def contains_brand(query: str, seeds: list[str]) -> bool:
    q = normalize_query(query)
    if not q:
        return False
    return any(seed in q for seed in seeds)


def extract_week_label_from_key(key: str) -> str:
    m = re.search(r"(20\d{2}-W\d{2})", key)
    return m.group(1) if m else ""


def parse_weekly_wb_xlsx(content: bytes, week_label: str, source_key: str, cfg: Config) -> pd.DataFrame:
    df = pd.read_excel(io.BytesIO(content))
    if df.empty:
        return pd.DataFrame()

    if "Поисковый запрос" not in df.columns:
        return pd.DataFrame()

    if cfg.wb_use_only_orders_filter and "Фильтр" in df.columns:
        df = df[df["Фильтр"].astype(str).str.lower().eq("orders")].copy()

    df["query_norm"] = df["Поисковый запрос"].apply(normalize_query)
    df = df[df["query_norm"].apply(lambda x: contains_brand(x, cfg.brand_seeds))].copy()
    if df.empty:
        return pd.DataFrame()

    num_cols = [
        "Частота запросов",
        "Частота за неделю",
        "Переходы в карточку",
        "Добавления в корзину",
        "Заказы",
        "Видимость %",
    ]
    for col in num_cols:
        if col in df.columns:
            df[col] = df[col].apply(safe_float)

    agg_map: dict[str, Any] = {
        "Частота запросов": "max",
        "Частота за неделю": "max",
        "Переходы в карточку": "sum",
        "Добавления в корзину": "sum",
        "Заказы": "sum",
        "Видимость %": "max",
    }
    existing_agg = {k: v for k, v in agg_map.items() if k in df.columns}
    grouped = df.groupby("query_norm", as_index=False).agg(existing_agg)

    # Сохраним исходное написание с максимальной частотой
    display_map = (
        df.sort_values(
            by=["Частота за неделю" if "Частота за неделю" in df.columns else "Частота запросов"],
            ascending=False,
        )
        .drop_duplicates("query_norm")
        [["query_norm", "Поисковый запрос"]]
        .rename(columns={"Поисковый запрос": "Поисковый запрос (WB)"})
    )
    grouped = grouped.merge(display_map, on="query_norm", how="left")
    grouped["Неделя"] = week_label
    grouped["Источник"] = "WB weekly"
    grouped["Файл"] = source_key

    desired_cols = [
        "Неделя",
        "Поисковый запрос (WB)",
        "query_norm",
        "Частота запросов",
        "Частота за неделю",
        "Переходы в карточку",
        "Добавления в корзину",
        "Заказы",
        "Видимость %",
        "Источник",
        "Файл",
    ]
    return grouped[[c for c in desired_cols if c in grouped.columns]].copy()


def parse_archive_query_xlsx(content: bytes, source_key: str, cfg: Config) -> pd.DataFrame:
    # Ищем лист "Детальная информация" и заголовок на второй строке
    xls = pd.ExcelFile(io.BytesIO(content))
    target_sheet = None
    for sheet in xls.sheet_names:
        if "детал" in sheet.lower():
            target_sheet = sheet
            break
    if target_sheet is None:
        return pd.DataFrame()

    df = pd.read_excel(io.BytesIO(content), sheet_name=target_sheet, header=1)
    if "Поисковый запрос" not in df.columns:
        return pd.DataFrame()

    df["query_norm"] = df["Поисковый запрос"].apply(normalize_query)
    df = df[df["query_norm"].apply(lambda x: contains_brand(x, cfg.brand_seeds))].copy()
    if df.empty:
        return pd.DataFrame()

    if "Количество запросов" in df.columns:
        df["Количество запросов"] = df["Количество запросов"].apply(safe_float)
    if "Запросов в среднем за день" in df.columns:
        df["Запросов в среднем за день"] = df["Запросов в среднем за день"].apply(safe_float)
    if "Перешли в карточку товара" in df.columns:
        df["Перешли в карточку товара"] = df["Перешли в карточку товара"].apply(safe_float)
    if "Добавили в корзину" in df.columns:
        df["Добавили в корзину"] = df["Добавили в корзину"].apply(safe_float)
    if "Заказали товаров" in df.columns:
        df["Заказали товаров"] = df["Заказали товаров"].apply(safe_float)

    # Период заберём из имени файла, если есть
    period_match = re.search(r"с (\d{2}[-.]\d{2}[-.]\d{4}) по (\d{2}[-.]\d{2}[-.]\d{4})", source_key)
    period_label = ""
    if period_match:
        period_label = f"{period_match.group(1)} - {period_match.group(2)}"

    out = pd.DataFrame({
        "Период архива": period_label,
        "Поисковый запрос (WB)": df["Поисковый запрос"],
        "query_norm": df["query_norm"],
        "Количество запросов": df["Количество запросов"] if "Количество запросов" in df.columns else 0,
        "Запросов в среднем за день": df["Запросов в среднем за день"] if "Запросов в среднем за день" in df.columns else 0,
        "Перешли в карточку товара": df["Перешли в карточку товара"] if "Перешли в карточку товара" in df.columns else 0,
        "Добавили в корзину": df["Добавили в корзину"] if "Добавили в корзину" in df.columns else 0,
        "Заказали товаров": df["Заказали товаров"] if "Заказали товаров" in df.columns else 0,
        "Источник": "WB archive",
        "Файл": source_key,
    })
    return out


def load_wb_weekly_brand_queries(storage: S3Storage, cfg: Config) -> pd.DataFrame:
    keys = sorted([k for k in storage.list_keys(cfg.wb_keywords_prefix) if k.lower().endswith(".xlsx")])
    if not keys:
        log(f"WB: по префиксу {cfg.wb_keywords_prefix} xlsx-файлы не найдены")
        return pd.DataFrame()

    keys = keys[-cfg.wb_weeks_to_compare:]
    frames: list[pd.DataFrame] = []
    for key in keys:
        log(f"WB: читаю {key}")
        content = storage.read_bytes(key)
        week_label = extract_week_label_from_key(key)
        frame = parse_weekly_wb_xlsx(content, week_label=week_label, source_key=key, cfg=cfg)
        if not frame.empty:
            frames.append(frame)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def load_wb_archive_brand_queries(storage: S3Storage, cfg: Config) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for prefix in cfg.archive_prefixes:
        keys = sorted(storage.list_keys(prefix))
        if not keys:
            continue
        for key in keys:
            low = key.lower()
            try:
                if low.endswith(".xlsx"):
                    log(f"WB archive: читаю {key}")
                    content = storage.read_bytes(key)
                    frame = parse_archive_query_xlsx(content, source_key=key, cfg=cfg)
                    if not frame.empty:
                        frames.append(frame)
                elif low.endswith(".zip"):
                    log(f"WB archive: читаю {key}")
                    raw = storage.read_bytes(key)
                    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                        for name in zf.namelist():
                            if name.lower().endswith(".xlsx"):
                                frame = parse_archive_query_xlsx(zf.read(name), source_key=f"{key}::{name}", cfg=cfg)
                                if not frame.empty:
                                    frames.append(frame)
            except Exception as e:
                log(f"WB archive: ошибка чтения {key}: {e}")

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def build_phrase_pool_from_wb(wb_weekly_df: pd.DataFrame, wb_archive_df: pd.DataFrame, cfg: Config) -> tuple[pd.DataFrame, list[str], list[str]]:
    candidates: list[pd.DataFrame] = []

    if not wb_weekly_df.empty:
        tmp = wb_weekly_df.copy()
        tmp["weight"] = tmp.get("Частота за неделю", 0).apply(safe_float)
        tmp["source"] = "weekly"
        candidates.append(tmp[["Поисковый запрос (WB)", "query_norm", "weight", "source"]])

    if not wb_archive_df.empty:
        tmp = wb_archive_df.copy()
        tmp["weight"] = tmp.get("Количество запросов", 0).apply(safe_float)
        tmp["source"] = "archive"
        candidates.append(tmp[["Поисковый запрос (WB)", "query_norm", "weight", "source"]])

    if not candidates:
        return pd.DataFrame(), [], list(cfg.brand_seeds)

    pool = pd.concat(candidates, ignore_index=True)
    pool["query_norm"] = pool["query_norm"].apply(normalize_query)
    pool = pool[pool["query_norm"].astype(bool)].copy()

    agg = (
        pool.groupby("query_norm", as_index=False)
        .agg(
            weight=("weight", "sum"),
            sources=("source", lambda s: ", ".join(sorted(set(map(str, s))))),
            examples=("Поисковый запрос (WB)", lambda s: next((x for x in s if normalize_text(x)), "")),
        )
        .sort_values(["weight", "query_norm"], ascending=[False, True])
        .reset_index(drop=True)
    )
    agg["rank"] = range(1, len(agg) + 1)

    exact_phrases = agg["examples"].head(cfg.yandex_max_phrases).tolist()

    expanded_seeds = list(cfg.brand_seeds)
    # Добавим короткие brand-токены из реальных запросов
    token_hits = set()
    for q in agg["query_norm"].tolist():
        if "topface" in q:
            token_hits.add("topface")
        if "top face" in q:
            token_hits.add("top face")
        if "топфейс" in q:
            token_hits.add("топфейс")
        if "топ фейс" in q:
            token_hits.add("топ фейс")
    expanded_seeds.extend(sorted(token_hits))
    expanded_seeds = sorted(set([x.strip().lower() for x in expanded_seeds if x.strip()]))

    return agg.rename(columns={"examples": "Поисковый запрос (WB)"}), exact_phrases, expanded_seeds


def yandex_post(url: str, payload: dict[str, Any], cfg: Config) -> dict[str, Any]:
    headers = {
        "Authorization": f"Api-Key {cfg.yandex_api_key}",
        "Content-Type": "application/json",
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=90)
    try:
        data = resp.json()
    except Exception:
        data = {"raw_text": resp.text}
    if resp.status_code >= 400:
        raise RuntimeError(f"Yandex API {resp.status_code}: {json.dumps(data, ensure_ascii=False)[:1500]}")
    return data


def flatten_top_items(data: Any, phrase: str, source_phrase_rank: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    def walk(node: Any, path: str = "") -> None:
        if isinstance(node, dict):
            # Кандидаты на запись
            text_candidates = [
                node.get("text"),
                node.get("phrase"),
                node.get("query"),
                node.get("request"),
                node.get("keyword"),
                node.get("searchRequest"),
            ]
            text_value = next((normalize_text(x) for x in text_candidates if normalize_text(x)), "")
            count_candidates = [
                node.get("shows"),
                node.get("count"),
                node.get("value"),
                node.get("freq"),
                node.get("frequency"),
                node.get("requests"),
            ]
            count_value = next((safe_float(x) for x in count_candidates if safe_float(x) != 0), 0.0)

            if text_value:
                rows.append(
                    {
                        "Исходная фраза WB": phrase,
                        "Ранг исходной фразы": source_phrase_rank,
                        "Yandex запрос": text_value,
                        "Yandex запрос norm": normalize_query(text_value),
                        "Показатель": count_value,
                        "Путь": path,
                    }
                )

            for k, v in node.items():
                walk(v, f"{path}/{k}" if path else str(k))
        elif isinstance(node, list):
            for i, item in enumerate(node):
                walk(item, f"{path}[{i}]")

    walk(data)
    dedup = pd.DataFrame(rows)
    if dedup.empty:
        return rows

    dedup = (
        dedup.sort_values(["Показатель", "Yandex запрос"], ascending=[False, True])
        .drop_duplicates(subset=["Исходная фраза WB", "Yandex запрос norm", "Путь"], keep="first")
    )
    return dedup.to_dict("records")


def load_yandex_top_by_wb_phrases(phrases: list[str], cfg: Config) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not cfg.yandex_api_key or not cfg.yandex_folder_id:
        log("Yandex: пропущено, потому что не заданы YANDEX_API_KEY / YANDEX_FOLDER_ID")
        return pd.DataFrame(), pd.DataFrame()

    rows: list[dict[str, Any]] = []
    raw_rows: list[dict[str, Any]] = []

    for idx, phrase in enumerate(phrases, start=1):
        payload: dict[str, Any] = {
            "folderId": cfg.yandex_folder_id,
            "phrase": phrase,
            "numPhrases": 30,
        }
        if cfg.yandex_region_id:
            payload["regions"] = [str(cfg.yandex_region_id)]

        try:
            log(f"Yandex Wordstat: GetTop по фразе '{phrase}'")
            data = yandex_post(cfg.yandex_top_url, payload, cfg)
            raw_rows.append({"Исходная фраза WB": phrase, "JSON": json.dumps(data, ensure_ascii=False)})
            rows.extend(flatten_top_items(data, phrase=phrase, source_phrase_rank=idx))
        except Exception as e:
            raw_rows.append({"Исходная фраза WB": phrase, "JSON": f"ERROR: {e}"})
            log(f"Yandex Wordstat: ошибка GetTop по фразе '{phrase}': {e}")

    top_df = pd.DataFrame(rows)
    raw_df = pd.DataFrame(raw_rows)

    if not top_df.empty:
        # убираем слишком технические / пустые дубль-строки
        top_df = top_df[top_df["Yandex запрос"].astype(str).str.len() > 0].copy()
        top_df = (
            top_df.sort_values(["Исходная фраза WB", "Показатель", "Yandex запрос"], ascending=[True, False, True])
            .drop_duplicates(subset=["Исходная фраза WB", "Yandex запрос norm"], keep="first")
            .reset_index(drop=True)
        )

    return top_df, raw_df


def make_wb_weekly_summary(wb_weekly_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if wb_weekly_df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    summary = (
        wb_weekly_df.groupby(["Неделя"], as_index=False)
        .agg(
            Брендовых_запросов=("query_norm", "nunique"),
            Частота_за_неделю=("Частота за неделю", "sum"),
            Переходы=("Переходы в карточку", "sum"),
            Корзины=("Добавления в корзину", "sum"),
            Заказы=("Заказы", "sum"),
        )
        .sort_values("Неделя")
        .reset_index(drop=True)
    )

    latest_week = summary["Неделя"].dropna().iloc[-1] if not summary.empty else ""
    prev_week = summary["Неделя"].dropna().iloc[-2] if len(summary) > 1 else ""

    latest_queries = pd.DataFrame()
    wow = pd.DataFrame()

    if latest_week:
        latest_queries = (
            wb_weekly_df[wb_weekly_df["Неделя"] == latest_week]
            .sort_values(["Частота за неделю", "Заказы"], ascending=[False, False])
            .reset_index(drop=True)
        )

    if latest_week and prev_week:
        cur = wb_weekly_df[wb_weekly_df["Неделя"] == latest_week].copy()
        prv = wb_weekly_df[wb_weekly_df["Неделя"] == prev_week].copy()

        cur = cur.rename(
            columns={
                "Частота за неделю": "Частота текущая",
                "Переходы в карточку": "Переходы текущие",
                "Добавления в корзину": "Корзины текущие",
                "Заказы": "Заказы текущие",
            }
        )
        prv = prv.rename(
            columns={
                "Частота за неделю": "Частота предыдущая",
                "Переходы в карточку": "Переходы предыдущие",
                "Добавления в корзину": "Корзины предыдущие",
                "Заказы": "Заказы предыдущие",
            }
        )

        cols_cur = ["query_norm", "Поисковый запрос (WB)", "Частота текущая", "Переходы текущие", "Корзины текущие", "Заказы текущие"]
        cols_prv = ["query_norm", "Частота предыдущая", "Переходы предыдущие", "Корзины предыдущие", "Заказы предыдущие"]

        wow = cur[cols_cur].merge(prv[cols_prv], on="query_norm", how="outer")
        wow["Поисковый запрос (WB)"] = wow["Поисковый запрос (WB)"].fillna(wow["query_norm"])
        for col in [
            "Частота текущая",
            "Переходы текущие",
            "Корзины текущие",
            "Заказы текущие",
            "Частота предыдущая",
            "Переходы предыдущие",
            "Корзины предыдущие",
            "Заказы предыдущие",
        ]:
            wow[col] = wow[col].fillna(0)

        wow["Δ Частота"] = wow["Частота текущая"] - wow["Частота предыдущая"]
        wow["Δ Переходы"] = wow["Переходы текущие"] - wow["Переходы предыдущие"]
        wow["Δ Заказы"] = wow["Заказы текущие"] - wow["Заказы предыдущие"]
        wow["Новая фраза"] = ((wow["Частота предыдущая"] == 0) & (wow["Частота текущая"] > 0)).map({True: "Да", False: ""})
        wow["Исчезла"] = ((wow["Частота предыдущая"] > 0) & (wow["Частота текущая"] == 0)).map({True: "Да", False: ""})

        wow = wow.sort_values(["Δ Частота", "Частота текущая"], ascending=[False, False]).reset_index(drop=True)

    return summary, latest_queries, wow


def make_yandex_summary(yandex_top_df: pd.DataFrame, seeds: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    if yandex_top_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    top_by_source = (
        yandex_top_df.groupby(["Исходная фраза WB"], as_index=False)
        .agg(
            Уникальных_Yandex_запросов=("Yandex запрос norm", "nunique"),
            Сумма_показателя=("Показатель", "sum"),
        )
        .sort_values(["Сумма_показателя", "Уникальных_Yandex_запросов"], ascending=[False, False])
        .reset_index(drop=True)
    )

    branded = yandex_top_df[yandex_top_df["Yandex запрос norm"].apply(lambda x: contains_brand(x, seeds))].copy()
    branded = (
        branded.groupby(["Yandex запрос norm"], as_index=False)
        .agg(
            Yandex_запрос=("Yandex запрос", "first"),
            Сумма_показателя=("Показатель", "sum"),
            Источники_WB=("Исходная фраза WB", lambda s: ", ".join(sorted(set(map(str, s))))),
        )
        .sort_values(["Сумма_показателя", "Yandex_запрос"], ascending=[False, True])
        .reset_index(drop=True)
    )
    return top_by_source, branded


def send_file_to_telegram(path: str, caption: str, cfg: Config) -> None:
    if not cfg.telegram_bot_token or not cfg.telegram_chat_id:
        log("Telegram: пропущено, потому что не заданы TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID")
        return

    url = f"https://api.telegram.org/bot{cfg.telegram_bot_token}/sendDocument"
    with open(path, "rb") as f:
        resp = requests.post(
            url,
            data={"chat_id": cfg.telegram_chat_id, "caption": caption},
            files={"document": f},
            timeout=180,
        )
    if resp.status_code >= 400:
        raise RuntimeError(f"Telegram API {resp.status_code}: {resp.text[:1000]}")


def should_send_report(cfg: Config) -> bool:
    if cfg.force_send:
        return True
    event_name = os.getenv("GITHUB_EVENT_NAME", "").strip().lower()
    if cfg.send_on_manual and event_name == "workflow_dispatch":
        return True
    return cfg.run_date.weekday() == 0  # понедельник


def format_excel(path: str) -> None:
    wb = load_workbook(path)
    for ws in wb.worksheets:
        for row in ws.iter_rows():
            for cell in row:
                cell.font = Font(name=FONT_NAME, size=FONT_SIZE)
                cell.alignment = Alignment(vertical="top", wrap_text=True)
                cell.border = BORDER_THIN

        if ws.max_row >= 1:
            for cell in ws[1]:
                cell.fill = FILL_HEADER
                cell.font = Font(name=FONT_NAME, size=FONT_SIZE, bold=True)

        # Автоширина колонок
        for col_cells in ws.columns:
            max_len = 0
            col_letter = col_cells[0].column_letter
            for cell in col_cells[:300]:
                value = "" if cell.value is None else str(cell.value)
                max_len = max(max_len, len(value))
            ws.column_dimensions[col_letter].width = min(max(max_len + 2, 12), 45)

        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions

    wb.save(path)


def save_report(
    wb_weekly_df: pd.DataFrame,
    wb_archive_df: pd.DataFrame,
    wb_phrase_pool_df: pd.DataFrame,
    wb_weekly_summary_df: pd.DataFrame,
    wb_latest_week_df: pd.DataFrame,
    wb_wow_df: pd.DataFrame,
    yandex_top_df: pd.DataFrame,
    yandex_top_summary_df: pd.DataFrame,
    yandex_brand_df: pd.DataFrame,
    yandex_raw_df: pd.DataFrame,
    output_path: str,
) -> None:
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        def write(df: pd.DataFrame, sheet_name: str) -> None:
            if df is None or df.empty:
                pd.DataFrame({"Пусто": []}).to_excel(writer, sheet_name=sheet_name, index=False)
            else:
                df.to_excel(writer, sheet_name=sheet_name, index=False)

        write(wb_weekly_summary_df, "WB_Summary_Weekly")
        write(wb_latest_week_df, "WB_Last_Week")
        write(wb_wow_df, "WB_Week_over_Week")
        write(wb_phrase_pool_df, "WB_Phrases_for_Yandex")
        write(wb_weekly_df, "WB_Brand_Weekly_All")
        write(wb_archive_df, "WB_Brand_Archive")
        write(yandex_top_summary_df, "Yandex_Top_30d_Summary")
        write(yandex_brand_df, "Yandex_Brand_30d")
        write(yandex_top_df, "Yandex_Top_30d")
        write(yandex_raw_df, "Yandex_Raw")

    format_excel(output_path)


def run() -> None:
    cfg = build_config()
    storage = S3Storage(cfg)

    wb_weekly_df = load_wb_weekly_brand_queries(storage, cfg)
    wb_archive_df = load_wb_archive_brand_queries(storage, cfg)

    wb_phrase_pool_df, yandex_phrases, expanded_seeds = build_phrase_pool_from_wb(wb_weekly_df, wb_archive_df, cfg)
    wb_weekly_summary_df, wb_latest_week_df, wb_wow_df = make_wb_weekly_summary(wb_weekly_df)

    yandex_top_df, yandex_raw_df = load_yandex_top_by_wb_phrases(yandex_phrases, cfg)
    yandex_top_summary_df, yandex_brand_df = make_yandex_summary(yandex_top_df, expanded_seeds)

    output_dir = Path("output")
    output_dir.mkdir(parents=True, exist_ok=True)
    filename = f"Брендовые_запросы_{cfg.store_name}_{cfg.run_date.strftime('%Y%m%d')}.xlsx"
    output_path = output_dir / filename

    save_report(
        wb_weekly_df=wb_weekly_df,
        wb_archive_df=wb_archive_df,
        wb_phrase_pool_df=wb_phrase_pool_df,
        wb_weekly_summary_df=wb_weekly_summary_df,
        wb_latest_week_df=wb_latest_week_df,
        wb_wow_df=wb_wow_df,
        yandex_top_df=yandex_top_df,
        yandex_top_summary_df=yandex_top_summary_df,
        yandex_brand_df=yandex_brand_df,
        yandex_raw_df=yandex_raw_df,
        output_path=str(output_path),
    )
    log(f"Отчёт сохранён: {output_path}")

    out_key = f"{cfg.output_prefix.rstrip('/')}/{filename}"
    with open(output_path, "rb") as f:
        storage.write_bytes(
            out_key,
            f.read(),
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    log(f"Файл загружен в Object Storage: {out_key}")

    if should_send_report(cfg):
        caption = f"Брендовый отчёт по поисковым запросам {cfg.store_name} за {cfg.run_date.strftime('%d.%m.%Y')}"
        send_file_to_telegram(str(output_path), caption, cfg)
        log("Отчёт отправлен в Telegram")
    else:
        log("Сегодня не понедельник и не ручной запуск — отправка в Telegram пропущена")


if __name__ == "__main__":
    run()
