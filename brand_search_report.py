
import io
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime

import boto3
import pandas as pd
import requests

DEFAULT_STORE = "TOPFACE"
DEFAULT_WEEKLY_PREFIX = os.getenv("WB_WEEKLY_PREFIX", f"Отчёты/Поисковые запросы/{DEFAULT_STORE}/Недельные/")
DEFAULT_OUTPUT_PREFIX = os.getenv("BRAND_REPORT_OUTPUT_PREFIX", f"Отчёты/Поисковые запросы/Брендовый отчет/{DEFAULT_STORE}/")
DEFAULT_BRAND_PATTERN = os.getenv(
    "BRAND_REGEX",
    r"(?:\btop\s*-?\s*face\b|\btopface\b|\btopfase\b|топ\s*-?\s*фейс|топфейс|топфеис)"
)

YANDEX_TOP_URL = "https://searchapi.api.cloud.yandex.net/v2/wordstat/topRequests"
WEEKLY_FILE_RE = re.compile(r"(?:^|/)(Неделя\s+\d{4}-W\d{2}\.xlsx)$", re.IGNORECASE)


@dataclass
class Config:
    bucket_name: str
    access_key_id: str
    secret_access_key: str
    telegram_bot_token: str | None
    telegram_chat_id: str | None
    yandex_api_key: str | None
    yandex_folder_id: str | None
    yandex_region_id: str | None
    store_name: str
    wb_weekly_prefix: str
    output_prefix: str
    brand_regex: str
    force_send: bool


def log(message: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {message}", flush=True)


def must_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Не задан обязательный env: {name}")
    return value


def build_config() -> Config:
    store_name = (os.getenv("STORE_NAME") or DEFAULT_STORE).strip() or DEFAULT_STORE
    return Config(
        bucket_name=must_env("YC_BUCKET_NAME"),
        access_key_id=must_env("YC_ACCESS_KEY_ID"),
        secret_access_key=must_env("YC_SECRET_ACCESS_KEY"),
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip() or None,
        telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", "").strip() or None,
        yandex_api_key=os.getenv("YANDEX_API_KEY", "").strip() or None,
        yandex_folder_id=os.getenv("YANDEX_FOLDER_ID", "").strip() or None,
        yandex_region_id=os.getenv("YANDEX_REGION_ID", "").strip() or None,
        store_name=store_name,
        wb_weekly_prefix=(os.getenv("WB_WEEKLY_PREFIX") or f"Отчёты/Поисковые запросы/{store_name}/Недельные/").strip(),
        output_prefix=(os.getenv("BRAND_REPORT_OUTPUT_PREFIX") or f"Отчёты/Поисковые запросы/Брендовый отчет/{store_name}/").strip(),
        brand_regex=(os.getenv("BRAND_REGEX") or DEFAULT_BRAND_PATTERN).strip(),
        force_send=os.getenv("FORCE_SEND", "").strip().lower() in {"1", "true", "yes", "y"},
    )


def s3_client(cfg: Config):
    session = boto3.session.Session()
    return session.client(
        "s3",
        endpoint_url="https://storage.yandexcloud.net",
        aws_access_key_id=cfg.access_key_id,
        aws_secret_access_key=cfg.secret_access_key,
    )


def list_s3_keys(client, bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            kwargs["ContinuationToken"] = token
        resp = client.list_objects_v2(**kwargs)
        for item in resp.get("Contents", []):
            key = item["Key"]
            if not key.endswith("/"):
                keys.append(key)
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    return keys


def read_s3_bytes(client, bucket: str, key: str) -> bytes:
    return client.get_object(Bucket=bucket, Key=key)["Body"].read()


def upload_s3_bytes(client, bucket: str, key: str, data: bytes) -> None:
    client.put_object(Bucket=bucket, Key=key, Body=data)


def normalize_text(value) -> str:
    if value is None:
        return ""
    text = str(value).strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def coerce_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0)


def extract_week_code_from_key(key: str) -> str:
    m = re.search(r"(\d{4}-W\d{2})", key)
    return m.group(1) if m else ""


def parse_weekly_wb_file(data: bytes, key: str, brand_re: re.Pattern) -> pd.DataFrame:
    df = pd.read_excel(io.BytesIO(data))
    required = {"Дата", "Поисковый запрос", "Частота запросов"}
    if not required.issubset(set(df.columns)):
        return pd.DataFrame(columns=["Неделя", "Поисковый запрос", "WB_количество"])

    work = df.copy()
    work["Поисковый запрос"] = work["Поисковый запрос"].map(normalize_text)
    work["Дата"] = pd.to_datetime(work["Дата"], errors="coerce").dt.date
    work["Частота запросов"] = coerce_numeric(work["Частота запросов"])
    work = work[(work["Поисковый запрос"] != "") & work["Дата"].notna()].copy()
    work = work[work["Поисковый запрос"].str.contains(brand_re, na=False)].copy()
    if work.empty:
        return pd.DataFrame(columns=["Неделя", "Поисковый запрос", "WB_количество"])

    week = extract_week_code_from_key(key)

    # Правильная логика:
    # в weekly-файле один и тот же запрос за одну и ту же дату повторяется по разным артикулам,
    # а "Частота запросов" у этих дублей одинаковая.
    # Поэтому сначала берём 1 значение на уровне Дата + Запрос, затем суммируем по неделе.
    daily_unique = (
        work.groupby(["Дата", "Поисковый запрос"], as_index=False)["Частота запросов"]
        .max()
    )
    weekly = (
        daily_unique.groupby(["Поисковый запрос"], as_index=False)["Частота запросов"]
        .sum()
        .rename(columns={"Частота запросов": "WB_количество"})
        .sort_values(["WB_количество", "Поисковый запрос"], ascending=[False, True])
        .reset_index(drop=True)
    )
    weekly.insert(0, "Неделя", week)
    return weekly


def load_wb_latest(cfg: Config) -> tuple[pd.DataFrame, str]:
    client = s3_client(cfg)
    brand_re = re.compile(cfg.brand_regex, flags=re.IGNORECASE)
    weekly_keys = sorted([k for k in list_s3_keys(client, cfg.bucket_name, cfg.wb_weekly_prefix) if WEEKLY_FILE_RE.search(k)])
    if not weekly_keys:
        raise RuntimeError(
            f"Не найдены weekly-файлы по prefix={cfg.wb_weekly_prefix}. "
            f"Ожидались ключи вида '.../Неделя YYYY-WNN.xlsx'"
        )

    latest_key = weekly_keys[-1]
    log(f"WB: читаю {latest_key}")
    wb_latest = parse_weekly_wb_file(read_s3_bytes(client, cfg.bucket_name, latest_key), latest_key, brand_re)
    week_code = extract_week_code_from_key(latest_key)
    return wb_latest, week_code


def yandex_post(cfg: Config, payload: dict) -> dict:
    if not cfg.yandex_api_key or not cfg.yandex_folder_id:
        raise RuntimeError("Не заданы YANDEX_API_KEY или YANDEX_FOLDER_ID")
    headers = {
        "Authorization": f"Api-Key {cfg.yandex_api_key}",
        "Content-Type": "application/json",
    }
    resp = requests.post(YANDEX_TOP_URL, headers=headers, json=payload, timeout=120)
    try:
        data = resp.json()
    except Exception:
        data = {"raw_text": resp.text}
    if resp.status_code >= 400:
        raise RuntimeError(f"Yandex API {resp.status_code}: {json.dumps(data, ensure_ascii=False)[:1000]}")
    return data


def extract_yandex_count(resp_json: dict, target_phrase: str) -> int:
    target = normalize_text(target_phrase)
    best = 0

    def maybe_num(v):
        try:
            return int(float(str(v).replace(" ", "").replace(",", ".")))
        except Exception:
            return 0

    def walk(node):
        nonlocal best
        if isinstance(node, dict):
            lowered = {str(k).lower(): v for k, v in node.items()}

            phrase = None
            for k in ["phrase", "text", "query", "request"]:
                if k in lowered and isinstance(lowered[k], str):
                    phrase = normalize_text(lowered[k])
                    break

            count = 0
            for k in ["shows", "showcount", "count", "value", "requests", "freq", "frequency"]:
                if k in lowered:
                    count = maybe_num(lowered[k])
                    break

            if phrase == target and count > best:
                best = count

            for v in node.values():
                walk(v)

        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(resp_json)
    return best


def load_yandex_counts(cfg: Config, wb_latest: pd.DataFrame) -> pd.DataFrame:
    if wb_latest.empty:
        return pd.DataFrame(columns=["Поисковый запрос", "Яндекс_количество"])

    phrases = wb_latest["Поисковый запрос"].dropna().astype(str).tolist()
    rows: list[dict] = []

    for phrase in phrases:
        log(f"Yandex Wordstat: GetTop по фразе '{phrase}'")
        payload: dict[str, object] = {
            "folderId": cfg.yandex_folder_id,
            "phrase": phrase,
            "numPhrases": 100,
        }
        if cfg.yandex_region_id:
            payload["regions"] = [cfg.yandex_region_id]

        try:
            resp_json = yandex_post(cfg, payload)
            rows.append({
                "Поисковый запрос": phrase,
                "Яндекс_количество": extract_yandex_count(resp_json, phrase),
            })
        except Exception as e:
            log(f"Yandex Wordstat: ошибка по фразе '{phrase}': {e}")
            rows.append({"Поисковый запрос": phrase, "Яндекс_количество": 0})

    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=["Поисковый запрос", "Яндекс_количество"])
    out["Яндекс_количество"] = coerce_numeric(out["Яндекс_количество"]).astype(int)
    return out


def build_final_table(wb_latest: pd.DataFrame, yandex_counts: pd.DataFrame, week_code: str) -> pd.DataFrame:
    final = wb_latest.merge(yandex_counts, on="Поисковый запрос", how="left")
    final["WB_количество"] = coerce_numeric(final["WB_количество"]).astype(int)
    final["Яндекс_количество"] = coerce_numeric(final.get("Яндекс_количество", pd.Series(dtype=float))).astype(int)

    final = final.rename(columns={
        "Поисковый запрос": "Запрос",
        "WB_количество": f"WB {week_code} (сумма 'Частота запросов' по датам недели)",
        "Яндекс_количество": "Яндекс (GetTop / 30 дней)"
    }).sort_values([f"WB {week_code} (сумма 'Частота запросов' по датам недели)", "Запрос"], ascending=[False, True]).reset_index(drop=True)
    return final


def autosize_worksheet(ws, df: pd.DataFrame) -> None:
    from openpyxl.styles import Font, PatternFill
    from openpyxl.utils import get_column_letter

    header_fill = PatternFill(fill_type="solid", fgColor="D9EAF7")
    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.fill = header_fill

    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions

    for idx, col in enumerate(df.columns, start=1):
        max_len = max([len(str(col))] + [len(str(v)) for v in df[col].head(2000).tolist()]) + 2
        ws.column_dimensions[get_column_letter(idx)].width = min(max_len, 60)


def save_report(cfg: Config, final_df: pd.DataFrame) -> tuple[str, bytes]:
    today = datetime.now().strftime("%Y%m%d")
    filename = f"Брендовые_запросы_{cfg.store_name}_{today}.xlsx"
    os.makedirs("output", exist_ok=True)
    local_path = os.path.join("output", filename)

    with pd.ExcelWriter(local_path, engine="openpyxl") as writer:
        final_df.to_excel(writer, sheet_name="Сводка", index=False)
        wb = writer.book
        autosize_worksheet(wb["Сводка"], final_df)

    with open(local_path, "rb") as f:
        return local_path, f.read()


def upload_report(cfg: Config, file_name: str, file_bytes: bytes) -> str:
    client = s3_client(cfg)
    key = f"{cfg.output_prefix.rstrip('/')}/{file_name}"
    upload_s3_bytes(client, cfg.bucket_name, key, file_bytes)
    return key


def should_send_telegram(cfg: Config) -> bool:
    if cfg.force_send:
        return True
    event_name = (os.getenv("GITHUB_EVENT_NAME") or "").strip().lower()
    if event_name == "workflow_dispatch":
        return True
    return datetime.now().weekday() == 0


def send_to_telegram(cfg: Config, filename: str, file_bytes: bytes) -> None:
    if not cfg.telegram_bot_token or not cfg.telegram_chat_id:
        log("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID не заданы — отправка пропущена")
        return
    url = f"https://api.telegram.org/bot{cfg.telegram_bot_token}/sendDocument"
    files = {"document": (filename, file_bytes, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
    data = {"chat_id": cfg.telegram_chat_id, "caption": filename}
    resp = requests.post(url, data=data, files=files, timeout=120)
    if resp.status_code >= 400:
        raise RuntimeError(f"Telegram API {resp.status_code}: {resp.text[:1000]}")


def run() -> None:
    cfg = build_config()
    wb_latest, week_code = load_wb_latest(cfg)
    yandex_counts = load_yandex_counts(cfg, wb_latest)
    final_df = build_final_table(wb_latest, yandex_counts, week_code)

    local_path, file_bytes = save_report(cfg, final_df)
    log(f"Отчёт сохранён: {local_path}")

    uploaded_key = upload_report(cfg, os.path.basename(local_path), file_bytes)
    log(f"Файл загружен в Object Storage: {uploaded_key}")

    if should_send_telegram(cfg):
        send_to_telegram(cfg, os.path.basename(local_path), file_bytes)
        log("Отчёт отправлен в Telegram")
    else:
        log("Сегодня не понедельник — отправка в Telegram пропущена")


if __name__ == "__main__":
    run()
