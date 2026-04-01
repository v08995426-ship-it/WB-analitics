
import io
import json
import os
import re
import sys
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Dict, List, Optional, Tuple

import boto3
import pandas as pd
import requests


# =========================
# CONFIG
# =========================

DEFAULT_STORE = "TOPFACE"
DEFAULT_WEEKLY_PREFIX = os.getenv(
    "WB_WEEKLY_PREFIX",
    f"Отчёты/Поисковые запросы/{DEFAULT_STORE}/Недельные/"
)
DEFAULT_ARCHIVE_PREFIXES = [
    p.strip() for p in os.getenv(
        "WB_ARCHIVE_PREFIXES",
        f"Отчёты/Поисковые запросы/{DEFAULT_STORE}/Архив/;"
        f"Отчёты/Поисковые запросы/{DEFAULT_STORE}/Архивы/"
    ).split(";") if p.strip()
]
DEFAULT_OUTPUT_PREFIX = os.getenv(
    "BRAND_REPORT_OUTPUT_PREFIX",
    f"Отчёты/Поисковые запросы/Брендовый отчет/{DEFAULT_STORE}/"
)
DEFAULT_BRAND_PATTERN = os.getenv(
    "BRAND_REGEX",
    r"(?:\btop\s*-?\s*face\b|\btopface\b|\btopfase\b|топ\s*-?\s*фейс|топфейс|топфеис)"
)

YANDEX_TOP_URL = "https://searchapi.api.cloud.yandex.net/v2/wordstat/topRequests"
WEEKLY_FILE_RE = re.compile(r"(?:^|/)(Неделя\s+\d{4}-W\d{2}\.xlsx)$", re.IGNORECASE)
ARCHIVE_FILE_RE = re.compile(r"\.(xlsx|zip)$", re.IGNORECASE)


@dataclass
class Config:
    bucket_name: str
    access_key_id: str
    secret_access_key: str
    telegram_bot_token: Optional[str]
    telegram_chat_id: Optional[str]
    yandex_api_key: Optional[str]
    yandex_folder_id: Optional[str]
    yandex_region_id: Optional[str]
    store_name: str
    wb_weekly_prefix: str
    wb_archive_prefixes: List[str]
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
    return Config(
        bucket_name=must_env("YC_BUCKET_NAME"),
        access_key_id=must_env("YC_ACCESS_KEY_ID"),
        secret_access_key=must_env("YC_SECRET_ACCESS_KEY"),
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip() or None,
        telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", "").strip() or None,
        yandex_api_key=os.getenv("YANDEX_API_KEY", "").strip() or None,
        yandex_folder_id=os.getenv("YANDEX_FOLDER_ID", "").strip() or None,
        yandex_region_id=os.getenv("YANDEX_REGION_ID", "").strip() or None,
        store_name=(os.getenv("STORE_NAME") or DEFAULT_STORE).strip() or DEFAULT_STORE,
        wb_weekly_prefix=os.getenv("WB_WEEKLY_PREFIX", DEFAULT_WEEKLY_PREFIX).strip(),
        wb_archive_prefixes=[p.strip() for p in os.getenv(
            "WB_ARCHIVE_PREFIXES",
            ";".join(DEFAULT_ARCHIVE_PREFIXES)
        ).split(";") if p.strip()],
        output_prefix=os.getenv("BRAND_REPORT_OUTPUT_PREFIX", DEFAULT_OUTPUT_PREFIX).strip(),
        brand_regex=os.getenv("BRAND_REGEX", DEFAULT_BRAND_PATTERN).strip(),
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


def list_s3_keys(client, bucket: str, prefix: str) -> List[str]:
    keys: List[str] = []
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


def normalize_text(value) -> str:
    if value is None:
        return ""
    text = str(value).strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def prepare_brand_pattern(cfg: Config):
    return re.compile(cfg.brand_regex, flags=re.IGNORECASE)


def extract_week_code_from_key(key: str) -> str:
    m = re.search(r"(\d{4}-W\d{2})", key)
    return m.group(1) if m else ""


def read_excel_any(data: bytes, sheet_name=0, header=0) -> pd.DataFrame:
    bio = io.BytesIO(data)
    return pd.read_excel(bio, sheet_name=sheet_name, header=header)


def coerce_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0)


def parse_weekly_wb_file(data: bytes, key: str, brand_re: re.Pattern) -> pd.DataFrame:
    df = read_excel_any(data)
    if "Поисковый запрос" not in df.columns or "Частота запросов" not in df.columns:
        return pd.DataFrame()

    work = df.copy()
    work["Поисковый запрос"] = work["Поисковый запрос"].map(normalize_text)
    work = work[work["Поисковый запрос"] != ""].copy()
    work["is_brand_query"] = work["Поисковый запрос"].str.contains(brand_re, na=False)
    work = work[work["is_brand_query"]].copy()
    if work.empty:
        return pd.DataFrame()

    # Берём именно ежедневную "Частота запросов" и суммируем её за неделю.
    # Но внутри одного дня один и тот же запрос может повторяться по нескольким карточкам/артикулам,
    # поэтому сначала схлопываем до одного значения на пару (Дата, Поисковый запрос) через max,
    # а уже потом суммируем по неделе.
    work["Дата"] = pd.to_datetime(work.get("Дата"), errors="coerce").dt.date
    work = work[work["Дата"].notna()].copy()
    if work.empty:
        return pd.DataFrame()

    work["WB_частота_день"] = coerce_numeric(work["Частота запросов"])
    work["Неделя"] = extract_week_code_from_key(key)

    daily_unique = (
        work.groupby(["Неделя", "Дата", "Поисковый запрос"], as_index=False)["WB_частота_день"]
        .max()
    )

    out = (
        daily_unique.groupby(["Неделя", "Поисковый запрос"], as_index=False)["WB_частота_день"]
        .sum()
        .rename(columns={"WB_частота_день": "WB_количество"})
        .sort_values(["Неделя", "WB_количество"], ascending=[True, False])
    )
    return out


def parse_archive_detail_df(df_raw: pd.DataFrame, brand_re: re.Pattern, source_name: str) -> pd.DataFrame:
    if df_raw.empty:
        return pd.DataFrame()

    # Ищем строку заголовков внутри тела
    header_row_idx = None
    first_col = df_raw.iloc[:, 0].astype(str)
    for idx, val in enumerate(first_col):
        if normalize_text(val) == "поисковый запрос":
            header_row_idx = idx
            break
    if header_row_idx is None:
        return pd.DataFrame()

    header = [str(x).strip() for x in df_raw.iloc[header_row_idx].tolist()]
    data = df_raw.iloc[header_row_idx + 1 :].copy()
    data.columns = header
    data = data.dropna(how="all")

    if "Поисковый запрос" not in data.columns:
        return pd.DataFrame()

    qty_col = None
    for cand in ["Количество запросов", "Запросов в среднем за день"]:
        if cand in data.columns:
            qty_col = cand
            break
    if qty_col is None:
        return pd.DataFrame()

    data["Поисковый запрос"] = data["Поисковый запрос"].map(normalize_text)
    data = data[data["Поисковый запрос"] != ""].copy()
    data = data[data["Поисковый запрос"].str.contains(brand_re, na=False)].copy()
    if data.empty:
        return pd.DataFrame()

    data["WB_архив"] = coerce_numeric(data[qty_col])
    data["Источник"] = source_name

    out = (
        data.groupby(["Поисковый запрос"], as_index=False)["WB_архив"]
        .max()
        .sort_values("WB_архив", ascending=False)
    )
    return out


def parse_archive_xlsx_bytes(data: bytes, source_name: str, brand_re: re.Pattern) -> pd.DataFrame:
    try:
        xls = pd.ExcelFile(io.BytesIO(data))
    except Exception:
        return pd.DataFrame()

    for sheet in xls.sheet_names:
        if normalize_text(sheet) == "детальная информация":
            df_raw = pd.read_excel(io.BytesIO(data), sheet_name=sheet, header=None)
            return parse_archive_detail_df(df_raw, brand_re, source_name)
    return pd.DataFrame()


def load_wb_data(cfg: Config) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    client = s3_client(cfg)
    brand_re = prepare_brand_pattern(cfg)

    weekly_keys_all = list_s3_keys(client, cfg.bucket_name, cfg.wb_weekly_prefix)
    weekly_keys = [k for k in weekly_keys_all if WEEKLY_FILE_RE.search(k)]
    weekly_keys = sorted(weekly_keys)

    if not weekly_keys:
        raise RuntimeError(
            f"Не найдены weekly-файлы по prefix={cfg.wb_weekly_prefix}. "
            f"Ожидались ключи вида '.../Неделя YYYY-WNN.xlsx'"
        )

    weekly_frames = []
    for key in weekly_keys:
        log(f"WB: читаю {key}")
        data = read_s3_bytes(client, cfg.bucket_name, key)
        df = parse_weekly_wb_file(data, key, brand_re)
        if not df.empty:
            weekly_frames.append(df)

    wb_all = pd.concat(weekly_frames, ignore_index=True) if weekly_frames else pd.DataFrame(
        columns=["Неделя", "Поисковый запрос", "WB_количество"]
    )

    archive_frames = []
    for prefix in cfg.wb_archive_prefixes:
        for key in sorted(list_s3_keys(client, cfg.bucket_name, prefix)):
            if not ARCHIVE_FILE_RE.search(key):
                continue
            log(f"WB archive: читаю {key}")
            blob = read_s3_bytes(client, cfg.bucket_name, key)

            if key.lower().endswith(".xlsx"):
                df_arch = parse_archive_xlsx_bytes(blob, key, brand_re)
                if not df_arch.empty:
                    archive_frames.append(df_arch)

            elif key.lower().endswith(".zip"):
                try:
                    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
                        for name in zf.namelist():
                            if name.lower().endswith(".xlsx"):
                                df_arch = parse_archive_xlsx_bytes(zf.read(name), f"{key}::{name}", brand_re)
                                if not df_arch.empty:
                                    archive_frames.append(df_arch)
                except Exception:
                    continue

    wb_archive = pd.concat(archive_frames, ignore_index=True) if archive_frames else pd.DataFrame(
        columns=["Поисковый запрос", "WB_архив"]
    )

    latest_week = ""
    wb_latest = pd.DataFrame(columns=["Поисковый запрос", "WB_количество"])
    if not wb_all.empty:
        latest_week = sorted([w for w in wb_all["Неделя"].dropna().astype(str).unique() if w])[-1]
        wb_latest = (
            wb_all[wb_all["Неделя"] == latest_week]
            .groupby("Поисковый запрос", as_index=False)["WB_количество"]
            .max()
            .sort_values("WB_количество", ascending=False)
        )

    return wb_all, wb_latest, wb_archive


def build_phrase_pool(wb_latest: pd.DataFrame, wb_archive: pd.DataFrame) -> pd.DataFrame:
    latest = wb_latest.copy()
    arch = wb_archive.copy()

    if latest.empty and arch.empty:
        return pd.DataFrame(columns=["Поисковый запрос", "WB_количество", "WB_архив", "Вес"])

    if latest.empty:
        latest = pd.DataFrame(columns=["Поисковый запрос", "WB_количество"])
    if arch.empty:
        arch = pd.DataFrame(columns=["Поисковый запрос", "WB_архив"])

    merged = latest.merge(arch, on="Поисковый запрос", how="outer")
    merged["WB_количество"] = coerce_numeric(merged.get("WB_количество", pd.Series(dtype=float)))
    merged["WB_архив"] = coerce_numeric(merged.get("WB_архив", pd.Series(dtype=float)))
    merged["Вес"] = merged["WB_количество"] * 1000 + merged["WB_архив"]
    merged = merged.sort_values(["WB_количество", "WB_архив", "Поисковый запрос"], ascending=[False, False, True])

    # ограничим число запросов в Яндекс, но в таблицу вернем все
    return merged


def yandex_post(cfg: Config, payload: Dict) -> Dict:
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
        raise RuntimeError(f"Yandex API {resp.status_code}: {json.dumps(data, ensure_ascii=False)[:1500]}")
    return data


def _extract_yandex_rows_from_response(resp_json: Dict, source_phrase: str) -> List[Dict]:
    rows: List[Dict] = []

    def walk(node):
        if isinstance(node, dict):
            lowered = {str(k).lower(): v for k, v in node.items()}

            phrase = None
            for k in ["text", "phrase", "query", "request"]:
                if k in lowered and isinstance(lowered[k], str) and lowered[k].strip():
                    phrase = lowered[k].strip()
                    break

            count = None
            for k in ["shows", "showcount", "count", "value", "requests", "freq", "frequency"]:
                if k in lowered:
                    val = lowered[k]
                    if isinstance(val, (int, float, str)):
                        try:
                            count = float(str(val).replace(" ", "").replace(",", "."))
                            break
                        except Exception:
                            pass

            if phrase is not None:
                rows.append({
                    "Поисковый запрос": normalize_text(phrase),
                    "Яндекс_количество": count if count is not None else 0,
                    "Фраза_источник": source_phrase,
                })

            for v in node.values():
                walk(v)

        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(resp_json)
    return rows


def load_yandex_counts(cfg: Config, phrase_pool: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if phrase_pool.empty:
        return (
            pd.DataFrame(columns=["Поисковый запрос", "Яндекс_количество"]),
            pd.DataFrame(columns=["Фраза_источник", "raw_json"]),
        )

    phrases = phrase_pool["Поисковый запрос"].dropna().astype(str).tolist()
    # Не стреляем слишком большим числом запросов
    phrases = phrases[:50]

    rows = []
    raw_rows = []

    for phrase in phrases:
        log(f"Yandex Wordstat: GetTop по фразе '{phrase}'")
        payload: Dict[str, object] = {
            "folderId": cfg.yandex_folder_id,
            "phrase": phrase,
            "numPhrases": 100,
        }
        if cfg.yandex_region_id:
            payload["regions"] = [cfg.yandex_region_id]

        try:
            resp_json = yandex_post(cfg, payload)
            raw_rows.append({"Фраза_источник": phrase, "raw_json": json.dumps(resp_json, ensure_ascii=False)})
            part_rows = _extract_yandex_rows_from_response(resp_json, phrase)
            rows.extend(part_rows)
        except Exception as e:
            raw_rows.append({"Фраза_источник": phrase, "raw_json": json.dumps({"error": str(e)}, ensure_ascii=False)})
            log(f"Yandex Wordstat: ошибка по фразе '{phrase}': {e}")

    raw_df = pd.DataFrame(raw_rows)

    if not rows:
        return pd.DataFrame(columns=["Поисковый запрос", "Яндекс_количество"]), raw_df

    yandex_all = pd.DataFrame(rows)
    yandex_all["Поисковый запрос"] = yandex_all["Поисковый запрос"].map(normalize_text)
    yandex_all["Яндекс_количество"] = coerce_numeric(yandex_all["Яндекс_количество"])

    # Оставляем максимальное найденное значение по каждой фразе
    yandex_counts = (
        yandex_all.groupby("Поисковый запрос", as_index=False)["Яндекс_количество"]
        .max()
        .sort_values("Яндекс_количество", ascending=False)
    )

    return yandex_counts, raw_df


def build_final_table(wb_latest: pd.DataFrame, wb_archive: pd.DataFrame, yandex_counts: pd.DataFrame) -> pd.DataFrame:
    latest = wb_latest.copy()
    arch = wb_archive.copy()
    yan = yandex_counts.copy()

    if latest.empty:
        latest = pd.DataFrame(columns=["Поисковый запрос", "WB_количество"])
    if arch.empty:
        arch = pd.DataFrame(columns=["Поисковый запрос", "WB_архив"])
    if yan.empty:
        yan = pd.DataFrame(columns=["Поисковый запрос", "Яндекс_количество"])

    final = latest.merge(yan, on="Поисковый запрос", how="left").merge(arch, on="Поисковый запрос", how="left")
    final["WB_количество"] = coerce_numeric(final.get("WB_количество", pd.Series(dtype=float)))
    final["Яндекс_количество"] = coerce_numeric(final.get("Яндекс_количество", pd.Series(dtype=float)))
    final["WB_архив"] = coerce_numeric(final.get("WB_архив", pd.Series(dtype=float)))

    # Если в latest неделе запроса нет, но он есть в архиве, все равно покажем
    missing = arch[~arch["Поисковый запрос"].isin(final["Поисковый запрос"])] if not arch.empty else pd.DataFrame()
    if not missing.empty:
        extra = missing.merge(yan, on="Поисковый запрос", how="left")
        extra["WB_количество"] = 0
        extra["Яндекс_количество"] = coerce_numeric(extra.get("Яндекс_количество", pd.Series(dtype=float)))
        final = pd.concat([final, extra[["Поисковый запрос", "WB_количество", "Яндекс_количество", "WB_архив"]]], ignore_index=True)

    final = final.fillna(0)
    final["WB_архив"] = final["WB_архив"].astype(int)
    final["WB_количество"] = final["WB_количество"].astype(int)
    final["Яндекс_количество"] = final["Яндекс_количество"].astype(int)

    # Только один понятный лист
    final = final.sort_values(["WB_количество", "Яндекс_количество", "WB_архив", "Поисковый запрос"],
                              ascending=[False, False, False, True]).reset_index(drop=True)
    final = final.rename(columns={
        "Поисковый запрос": "Запрос",
        "WB_количество": "Сколько на WB (последняя неделя)",
        "Яндекс_количество": "Сколько на Яндексе (GetTop / 30 дней)",
        "WB_архив": "WB архив"
    })
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
        max_len = min(max_len, 45)
        ws.column_dimensions[get_column_letter(idx)].width = max_len


def save_report(cfg: Config, final_df: pd.DataFrame, wb_latest: pd.DataFrame, yandex_counts: pd.DataFrame) -> Tuple[str, bytes]:
    today = datetime.now().strftime("%Y%m%d")
    filename = f"Брендовые_запросы_{cfg.store_name}_{today}.xlsx"
    os.makedirs("output", exist_ok=True)
    local_path = os.path.join("output", filename)

    with pd.ExcelWriter(local_path, engine="openpyxl") as writer:
        final_df.to_excel(writer, sheet_name="Сводка", index=False)
        wb_latest.to_excel(writer, sheet_name="WB_сырье", index=False)
        yandex_counts.to_excel(writer, sheet_name="Yandex_сырье", index=False)

        wb = writer.book
        autosize_worksheet(wb["Сводка"], final_df)
        autosize_worksheet(wb["WB_сырье"], wb_latest if not wb_latest.empty else pd.DataFrame(columns=["Пусто"]))
        autosize_worksheet(wb["Yandex_сырье"], yandex_counts if not yandex_counts.empty else pd.DataFrame(columns=["Пусто"]))

    with open(local_path, "rb") as f:
        payload = f.read()

    return local_path, payload


def upload_report(cfg: Config, payload: bytes, filename: str) -> str:
    client = s3_client(cfg)
    key = f"{cfg.output_prefix.rstrip('/')}/{filename}"
    client.put_object(
        Bucket=cfg.bucket_name,
        Key=key,
        Body=payload,
        ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    return key


def should_send(cfg: Config) -> bool:
    event_name = os.getenv("GITHUB_EVENT_NAME", "").strip()
    weekday = datetime.now(timezone.utc).weekday()  # 0 Monday
    return cfg.force_send or event_name == "workflow_dispatch" or weekday == 0


def send_telegram_document(cfg: Config, payload: bytes, filename: str, caption: str) -> None:
    if not cfg.telegram_bot_token or not cfg.telegram_chat_id:
        log("Telegram: пропуск, не заданы TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID")
        return

    url = f"https://api.telegram.org/bot{cfg.telegram_bot_token}/sendDocument"
    files = {"document": (filename, payload, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
    data = {"chat_id": cfg.telegram_chat_id, "caption": caption}
    resp = requests.post(url, data=data, files=files, timeout=120)
    if resp.status_code >= 400:
        raise RuntimeError(f"Telegram API {resp.status_code}: {resp.text[:1000]}")


def run() -> None:
    cfg = build_config()

    wb_all, wb_latest, wb_archive = load_wb_data(cfg)
    phrase_pool = build_phrase_pool(wb_latest, wb_archive)
    yandex_counts, _yandex_raw = load_yandex_counts(cfg, phrase_pool)
    final_df = build_final_table(wb_latest, wb_archive, yandex_counts)

    local_path, payload = save_report(cfg, final_df, wb_latest, yandex_counts)
    log(f"Отчёт сохранён: {local_path}")

    filename = os.path.basename(local_path)
    key = upload_report(cfg, payload, filename)
    log(f"Файл загружен в Object Storage: {key}")

    if should_send(cfg):
        caption = f"Брендовые запросы {cfg.store_name}: WB vs Yandex"
        send_telegram_document(cfg, payload, filename, caption)
        log("Отчёт отправлен в Telegram")
    else:
        log("Сегодня не понедельник и не ручной запуск — отправка в Telegram пропущена")


if __name__ == "__main__":
    run()
