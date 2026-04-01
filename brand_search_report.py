
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import io
import json
import os
import re
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import boto3
import pandas as pd
import requests


BRAND_REGEX = re.compile(r"(top\s*face|topface|топ\s*фейс|топфейс|topfase)", re.IGNORECASE)
WEEKLY_FILE_RE = re.compile(r"Неделя\s+(\d{4})-W(\d{2})\.xlsx$", re.IGNORECASE)


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def getenv_str(name: str, default: str = "") -> str:
    value = os.getenv(name)
    if value is None:
        return default
    value = value.strip()
    return value if value else default


@dataclass
class Config:
    yc_access_key_id: str
    yc_secret_access_key: str
    yc_bucket_name: str

    telegram_bot_token: str
    telegram_chat_id: str

    yandex_api_key: str
    yandex_folder_id: str
    yandex_region_ids: List[str]

    store_name: str
    wb_keywords_prefix: str
    output_prefix: str
    force_send: bool

    @staticmethod
    def from_env() -> "Config":
        store_name = getenv_str("STORE_NAME", "TOPFACE")
        wb_prefix = getenv_str(
            "WB_KEYWORDS_PREFIX",
            f"Отчёты/Поисковые запросы/{store_name}/Недельные/"
        )
        output_prefix = getenv_str(
            "BRAND_REPORT_OUTPUT_PREFIX",
            f"Отчёты/Поисковые запросы/Брендовый отчет/{store_name}/"
        )
        regions_raw = getenv_str("YANDEX_REGION_IDS", "")
        region_ids = [x.strip() for x in regions_raw.split(",") if x.strip()]
        return Config(
            yc_access_key_id=os.environ["YC_ACCESS_KEY_ID"],
            yc_secret_access_key=os.environ["YC_SECRET_ACCESS_KEY"],
            yc_bucket_name=os.environ["YC_BUCKET_NAME"],
            telegram_bot_token=os.environ["TELEGRAM_BOT_TOKEN"],
            telegram_chat_id=os.environ["TELEGRAM_CHAT_ID"],
            yandex_api_key=os.environ["YANDEX_API_KEY"],
            yandex_folder_id=os.environ["YANDEX_FOLDER_ID"],
            yandex_region_ids=region_ids,
            store_name=store_name,
            wb_keywords_prefix=wb_prefix,
            output_prefix=output_prefix,
            force_send=getenv_str("FORCE_SEND", "false").lower() in {"1", "true", "yes", "y"},
        )


def make_s3_client(cfg: Config):
    return boto3.client(
        "s3",
        aws_access_key_id=cfg.yc_access_key_id,
        aws_secret_access_key=cfg.yc_secret_access_key,
        endpoint_url="https://storage.yandexcloud.net",
    )


def list_all_keys(s3, bucket: str, prefix: str) -> List[str]:
    keys: List[str] = []
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            keys.append(obj["Key"])
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    return keys


def read_excel_from_s3(s3, bucket: str, key: str) -> pd.DataFrame:
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj["Body"].read()
    return pd.read_excel(io.BytesIO(data))


def normalize_query(text: str) -> str:
    text = str(text or "")
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text.strip().lower())
    return text


def contains_brand(text: str) -> bool:
    return bool(BRAND_REGEX.search(str(text or "")))


def get_last_full_iso_week(today: Optional[date] = None) -> Tuple[int, int]:
    today = today or date.today()
    last_sunday = today - timedelta(days=today.weekday() + 1)
    iso = last_sunday.isocalendar()
    return iso.year, iso.week


def pick_target_weekly_file(keys: List[str], today: Optional[date] = None) -> Tuple[str, int, int]:
    candidates: List[Tuple[int, int, str]] = []
    for key in keys:
        m = WEEKLY_FILE_RE.search(key)
        if not m:
            continue
        y, w = int(m.group(1)), int(m.group(2))
        candidates.append((y, w, key))
    if not candidates:
        raise RuntimeError("Не найдены weekly-файлы вида '.../Неделя YYYY-WNN.xlsx'")

    target_y, target_w = get_last_full_iso_week(today)
    exact = [x for x in candidates if x[0] == target_y and x[1] == target_w]
    if exact:
        y, w, key = sorted(exact)[-1]
        return key, y, w

    past = [x for x in candidates if (x[0], x[1]) <= (target_y, target_w)]
    if past:
        y, w, key = sorted(past)[-1]
        return key, y, w

    y, w, key = sorted(candidates)[-1]
    return key, y, w


def prepare_wb_week(df: pd.DataFrame) -> pd.DataFrame:
    required = {"Дата", "Поисковый запрос", "Частота запросов"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"В weekly-файле нет колонок: {', '.join(sorted(missing))}")

    tmp = df.copy()
    tmp["Дата"] = pd.to_datetime(tmp["Дата"], errors="coerce").dt.date
    tmp["Поисковый запрос"] = tmp["Поисковый запрос"].astype(str).fillna("")
    tmp["query_norm"] = tmp["Поисковый запрос"].map(normalize_query)
    tmp = tmp[tmp["Дата"].notna()]
    tmp = tmp[tmp["query_norm"] != ""]
    tmp = tmp[tmp["query_norm"].map(contains_brand)]
    tmp["Частота запросов"] = pd.to_numeric(tmp["Частота запросов"], errors="coerce").fillna(0)

    # Главное исправление:
    # 1) уникальный запрос внутри конкретной даты -> одно значение частоты (берём max)
    day_level = (
        tmp.groupby(["Дата", "query_norm"], as_index=False)
        .agg(
            wb_day_freq=("Частота запросов", "max"),
            query_sample=("Поисковый запрос", lambda s: s.astype(str).mode().iloc[0] if not s.mode().empty else s.iloc[0]),
        )
    )

    # 2) сумма дневных частот за выбранную неделю
    week_level = (
        day_level.groupby("query_norm", as_index=False)
        .agg(
            wb_week_sum=("wb_day_freq", "sum"),
            active_days=("Дата", "nunique"),
            query=("query_sample", lambda s: s.astype(str).mode().iloc[0] if not s.mode().empty else s.iloc[0]),
        )
        .sort_values(["wb_week_sum", "query_norm"], ascending=[False, True])
        .reset_index(drop=True)
    )

    return day_level, week_level


def yandex_post(phrase: str, cfg: Config) -> dict:
    url = "https://searchapi.api.cloud.yandex.net/v2/wordstat/topRequests"
    headers = {
        "Authorization": f"Api-Key {cfg.yandex_api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "folderId": cfg.yandex_folder_id,
        "phrase": phrase,
        "numPhrases": 40,
    }
    if cfg.yandex_region_ids:
        payload["regions"] = cfg.yandex_region_ids

    resp = requests.post(url, headers=headers, json=payload, timeout=120)
    try:
        data = resp.json()
    except Exception:
        data = {"raw_text": resp.text[:2000]}
    if not resp.ok:
        raise RuntimeError(f"Yandex API {resp.status_code}: {json.dumps(data, ensure_ascii=False)[:1000]}")
    return data


def extract_yandex_value_for_phrase(raw: dict, phrase: str) -> int:
    """
    Берём частотность именно целевой фразы, если она есть в ответе.
    Иначе 0. Никаких сумм по похожим запросам.
    """
    target = normalize_query(phrase)

    candidates: List[Tuple[str, int]] = []

    def walk(obj):
        if isinstance(obj, dict):
            lowered = {str(k).lower(): v for k, v in obj.items()}
            text_val = None
            freq_val = None
            for key in ("phrase", "query", "text", "request", "searchquery", "search_query"):
                if key in lowered and isinstance(lowered[key], (str, int, float)):
                    text_val = str(lowered[key])
                    break
            for key in ("shows", "count", "freq", "frequency", "requests", "value"):
                if key in lowered and isinstance(lowered[key], (int, float, str)):
                    try:
                        freq_val = int(float(str(lowered[key]).replace(" ", "")))
                        break
                    except Exception:
                        pass
            if text_val is not None and freq_val is not None:
                candidates.append((normalize_query(text_val), freq_val))
            for v in obj.values():
                walk(v)
        elif isinstance(obj, list):
            for item in obj:
                walk(item)

    walk(raw)

    exact_values = [freq for text, freq in candidates if text == target]
    if exact_values:
        return max(exact_values)
    return 0


def query_yandex_for_wb_queries(phrases: List[str], cfg: Config) -> pd.DataFrame:
    rows = []
    for phrase in phrases:
        clean = phrase.strip()
        if not clean:
            continue
        log(f"Yandex Wordstat: GetTop по фразе '{clean}'")
        try:
            raw = yandex_post(clean, cfg)
            val = extract_yandex_value_for_phrase(raw, clean)
            rows.append({"query_norm": normalize_query(clean), "yandex_30d": val, "yandex_ok": True, "yandex_error": ""})
        except Exception as e:
            rows.append({"query_norm": normalize_query(clean), "yandex_30d": 0, "yandex_ok": False, "yandex_error": str(e)})
            log(f"Yandex Wordstat: ошибка по фразе '{clean}': {e}")
    return pd.DataFrame(rows)


def send_telegram_document(cfg: Config, file_path: str, caption: str) -> None:
    url = f"https://api.telegram.org/bot{cfg.telegram_bot_token}/sendDocument"
    with open(file_path, "rb") as f:
        files = {"document": f}
        data = {"chat_id": cfg.telegram_chat_id, "caption": caption}
        resp = requests.post(url, data=data, files=files, timeout=120)
        resp.raise_for_status()


def should_send_today(cfg: Config) -> bool:
    event_name = getenv_str("GITHUB_EVENT_NAME", "")
    if cfg.force_send:
        return True
    if event_name == "workflow_dispatch":
        return True
    return date.today().weekday() == 0


def run() -> None:
    cfg = Config.from_env()
    s3 = make_s3_client(cfg)

    keys = list_all_keys(s3, cfg.yc_bucket_name, cfg.wb_keywords_prefix)
    weekly_keys = [k for k in keys if WEEKLY_FILE_RE.search(k)]
    if not weekly_keys:
        raise RuntimeError(
            f"Не найдены weekly-файлы по prefix={cfg.wb_keywords_prefix}. "
            "Ожидались ключи вида '.../Неделя YYYY-WNN.xlsx'"
        )

    target_key, iso_year, iso_week = pick_target_weekly_file(weekly_keys)
    log(f"WB: читаю {target_key}")
    wb_raw = read_excel_from_s3(s3, cfg.yc_bucket_name, target_key)

    day_level, week_level = prepare_wb_week(wb_raw)

    # Для Яндекса отправляем именно те запросы, которые получились в WB на недельном уровне
    yandex_df = query_yandex_for_wb_queries(week_level["query"].astype(str).tolist(), cfg)

    final_df = week_level.merge(yandex_df[["query_norm", "yandex_30d"]], on="query_norm", how="left")
    final_df["yandex_30d"] = final_df["yandex_30d"].fillna(0).astype(int)
    final_df = final_df.rename(
        columns={
            "query": "Запрос",
            "wb_week_sum": f"WB {iso_year}-W{iso_week:02d} (сумма max('Частота запросов') по каждой дате)",
            "active_days": "Дней в неделе с запросом",
            "yandex_30d": "Яндекс 30 дней",
        }
    )
    final_df = final_df[
        ["Запрос", f"WB {iso_year}-W{iso_week:02d} (сумма max('Частота запросов') по каждой дате)", "Яндекс 30 дней", "Дней в неделе с запросом"]
    ].sort_values(
        by=[f"WB {iso_year}-W{iso_week:02d} (сумма max('Частота запросов') по каждой дате)", "Запрос"],
        ascending=[False, True]
    ).reset_index(drop=True)

    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    out_name = f"Брендовые_запросы_{cfg.store_name}_{iso_year}W{iso_week:02d}.xlsx"
    out_path = os.path.join(output_dir, out_name)

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        final_df.to_excel(writer, sheet_name="Сводка", index=False)
        day_level.to_excel(writer, sheet_name="WB_дни", index=False)

    log(f"Отчёт сохранён: {out_path}")

    output_key = f"{cfg.output_prefix.rstrip('/')}/{out_name}"
    s3.upload_file(out_path, cfg.yc_bucket_name, output_key)
    log(f"Файл загружен в Object Storage: {output_key}")

    if should_send_today(cfg):
        send_telegram_document(
            cfg,
            out_path,
            f"{cfg.store_name}: брендовые запросы за {iso_year}-W{iso_week:02d}",
        )
        log("Отчёт отправлен в Telegram")
    else:
        log("Сегодня не понедельник — отправка в Telegram пропущена")


if __name__ == "__main__":
    run()
