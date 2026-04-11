#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ALOR BR 5m intraday robot (test-ready, self-hosted, local storage)

Назначение:
- интрадей-робот для тестового контура ALOR
- работает по активному BR-контракту
- анализирует только 5m свечи
- ищет breakout по тренду
- умеет открывать market-вход и сопровождать позицию:
  stop-loss, take-profit, перевод стопа в безубыток
- все решения логируются: и действия, и бездействие

Важно:
- стратегия учебно-тестовая, не обещает прибыль
- рассчитана на self-hosted runner или локальный запуск
- рассчитана на периодический запуск (рекомендуется каждые 5 минут)
"""

from __future__ import annotations

import json
import logging
import math
import os
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import requests

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None


def env_str(name: str, default: str = "") -> str:
    return str(os.getenv(name, default)).strip()


def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    return int(str(v).strip())


def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    return float(str(v).strip().replace(",", "."))


def env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass
class Config:
    env_name: str
    refresh_token: str
    portfolio: str
    exchange: str
    instrument_group: str
    base_symbol: str
    data_dir: Path
    timezone_name: str

    enable_trading: bool
    qty: int

    candle_lookback_days: int
    breakout_lookback: int
    ema_fast: int
    ema_mid: int
    ema_slow: int
    rsi_len: int
    atr_len: int

    min_rsi_long: float
    max_rsi_long: float
    min_rsi_short: float
    max_rsi_short: float

    min_vol_ratio: float
    min_atr_ticks: float
    max_atr_ticks: float
    max_spread_ticks: float
    max_signal_bar_ticks: float
    min_ob_imbalance_long: float
    max_ob_imbalance_short: float

    take_ticks: float
    stop_ticks: float
    breakeven_ticks: float

    no_entry_last_minutes: int
    force_exit_last_minutes: int
    session_warmup_minutes: int

    day_session_start: str
    day_session_end: str
    evening_session_start: str
    evening_session_end: str

    order_estimate_required: bool
    allow_margin: bool
    request_sleep_sec: float
    read_timeout_sec: int

    @property
    def oauth_base(self) -> str:
        return "https://oauthdev.alor.ru" if self.env_name == "test" else "https://oauth.alor.ru"

    @property
    def api_base(self) -> str:
        return "https://apidev.alor.ru" if self.env_name == "test" else "https://api.alor.ru"


def load_config() -> Config:
    return Config(
        env_name=env_str("ALOR_TR_ENV", "test").lower(),
        refresh_token=env_str("ALOR_TR_REFRESH_TOKEN"),
        portfolio=env_str("ALOR_TR_PORTFOLIO"),
        exchange=env_str("ALOR_TR_EXCHANGE", "MOEX"),
        instrument_group=env_str("ALOR_TR_INSTRUMENT_GROUP", "RFUD"),
        base_symbol=env_str("ALOR_TR_BASE_SYMBOL", "BR"),
        data_dir=Path(env_str("ALOR_TR_DATA_DIR", r"C:\Users\Владислав\Documents\ТОРГОВЫЕ РОБОТЫ\alor_tr_robot_data")),
        timezone_name=env_str("ALOR_TR_TIMEZONE", "Europe/Moscow"),
        enable_trading=env_bool("ALOR_TR_ENABLE_TRADING", False),
        qty=env_int("ALOR_TR_ORDER_QTY", 1),
        candle_lookback_days=env_int("ALOR_TR_CANDLE_LOOKBACK_DAYS", 5),
        breakout_lookback=env_int("ALOR_TR_BREAKOUT_LOOKBACK", 20),
        ema_fast=env_int("ALOR_TR_EMA_FAST", 20),
        ema_mid=env_int("ALOR_TR_EMA_MID", 50),
        ema_slow=env_int("ALOR_TR_EMA_SLOW", 100),
        rsi_len=env_int("ALOR_TR_RSI_LEN", 14),
        atr_len=env_int("ALOR_TR_ATR_LEN", 14),
        min_rsi_long=env_float("ALOR_TR_MIN_RSI_LONG", 55.0),
        max_rsi_long=env_float("ALOR_TR_MAX_RSI_LONG", 70.0),
        min_rsi_short=env_float("ALOR_TR_MIN_RSI_SHORT", 30.0),
        max_rsi_short=env_float("ALOR_TR_MAX_RSI_SHORT", 45.0),
        min_vol_ratio=env_float("ALOR_TR_MIN_VOL_RATIO", 1.0),
        min_atr_ticks=env_float("ALOR_TR_MIN_ATR_TICKS", 4.0),
        max_atr_ticks=env_float("ALOR_TR_MAX_ATR_TICKS", 40.0),
        max_spread_ticks=env_float("ALOR_TR_MAX_SPREAD_TICKS", 3.0),
        max_signal_bar_ticks=env_float("ALOR_TR_MAX_SIGNAL_BAR_TICKS", 12.0),
        min_ob_imbalance_long=env_float("ALOR_TR_MIN_OB_IMBALANCE_LONG", 0.55),
        max_ob_imbalance_short=env_float("ALOR_TR_MAX_OB_IMBALANCE_SHORT", 0.45),
        take_ticks=env_float("ALOR_TR_TAKE_TICKS", 10.0),
        stop_ticks=env_float("ALOR_TR_STOP_TICKS", 6.0),
        breakeven_ticks=env_float("ALOR_TR_BREAKEVEN_TICKS", 6.0),
        no_entry_last_minutes=env_int("ALOR_TR_NO_ENTRY_LAST_MINUTES", 60),
        force_exit_last_minutes=env_int("ALOR_TR_FORCE_EXIT_LAST_MINUTES", 15),
        session_warmup_minutes=env_int("ALOR_TR_SESSION_WARMUP_MINUTES", 15),
        day_session_start=env_str("ALOR_TR_DAY_SESSION_START", "10:00"),
        day_session_end=env_str("ALOR_TR_DAY_SESSION_END", "18:45"),
        evening_session_start=env_str("ALOR_TR_EVENING_SESSION_START", "19:05"),
        evening_session_end=env_str("ALOR_TR_EVENING_SESSION_END", "23:45"),
        order_estimate_required=env_bool("ALOR_TR_ORDER_ESTIMATE_REQUIRED", True),
        allow_margin=env_bool("ALOR_TR_ALLOW_MARGIN", False),
        request_sleep_sec=env_float("ALOR_TR_REQUEST_SLEEP_SEC", 0.25),
        read_timeout_sec=env_int("ALOR_TR_READ_TIMEOUT_SEC", 30),
    )


CFG = load_config()


def validate_config(cfg: Config):
    missing = []
    if not cfg.refresh_token:
        missing.append("ALOR_TR_REFRESH_TOKEN")
    if not cfg.portfolio:
        missing.append("ALOR_TR_PORTFOLIO")
    if missing:
        raise RuntimeError(f"Не заданы обязательные переменные: {', '.join(missing)}")


RUN_TS_UTC = datetime.now(timezone.utc)
RUN_ID = RUN_TS_UTC.strftime("%Y%m%d_%H%M%S")
ROBOT_PREFIX = "TRBOT"

LOG_DIR = CFG.data_dir / "logs"
STATE_DIR = CFG.data_dir / "state"
CACHE_DIR = CFG.data_dir / "cache"
REPORTS_DIR = CFG.data_dir / "reports"
JOURNAL_DIR = CFG.data_dir / "journal"

STATE_FILE = STATE_DIR / "robot_state.json"
DAILY_LOG_FILE = LOG_DIR / f"robot_{RUN_TS_UTC.strftime('%Y%m%d')}.log"
LATEST_LOG_FILE = LOG_DIR / "robot_latest.log"
DECISION_JOURNAL = JOURNAL_DIR / "decision_journal.jsonl"
ORDER_JOURNAL = JOURNAL_DIR / "orders_journal.jsonl"
RUN_REPORT_FILE = REPORTS_DIR / f"run_report_{RUN_ID}.json"
LATEST_REPORT_FILE = REPORTS_DIR / "run_report_latest.json"
LATEST_CANDLES_FILE = CACHE_DIR / "latest_candles_5m.parquet"

for p in (CFG.data_dir, LOG_DIR, STATE_DIR, CACHE_DIR, REPORTS_DIR, JOURNAL_DIR):
    p.mkdir(parents=True, exist_ok=True)


def setup_logger() -> logging.Logger:
    logger = logging.getLogger("alor_tr_robot")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    for fp in (DAILY_LOG_FILE, LATEST_LOG_FILE):
        fh = logging.FileHandler(fp, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    return logger


logger = setup_logger()


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, obj: Any):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, default=str)
    tmp.replace(path)


def append_jsonl(path: Path, obj: Any):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False, default=str) + "\n")


def default_state() -> dict[str, Any]:
    return {
        "managed_symbol": None,
        "managed_position": False,
        "managed_side": None,
        "last_signal_bar_time": None,
        "breakeven_applied_for_position_key": None,
        "last_run_started_at": None,
        "last_run_finished_at": None,
    }


STATE = load_json(STATE_FILE, default_state())


def get_tz(name: str):
    if ZoneInfo is not None:
        try:
            return ZoneInfo(name)
        except Exception:
            pass
    if name == "Europe/Moscow":
        return timezone(timedelta(hours=3))
    return timezone.utc


LOCAL_TZ = get_tz(CFG.timezone_name)


def now_local() -> datetime:
    return datetime.now(LOCAL_TZ)


def parse_hhmm(hhmm: str) -> tuple[int, int]:
    hh, mm = hhmm.split(":")
    return int(hh), int(mm)


def local_dt_for_today(hhmm: str, base: Optional[datetime] = None) -> datetime:
    base = base or now_local()
    hh, mm = parse_hhmm(hhmm)
    return base.replace(hour=hh, minute=mm, second=0, microsecond=0)


@dataclass
class SessionContext:
    in_session: bool
    session_name: Optional[str]
    session_start: Optional[datetime]
    session_end: Optional[datetime]
    minutes_to_end: Optional[int]
    minutes_from_start: Optional[int]
    entry_allowed: bool
    force_exit_zone: bool
    reason: str


def get_session_context(dt_local: datetime) -> SessionContext:
    windows = [
        ("day", local_dt_for_today(CFG.day_session_start, dt_local), local_dt_for_today(CFG.day_session_end, dt_local)),
        ("evening", local_dt_for_today(CFG.evening_session_start, dt_local), local_dt_for_today(CFG.evening_session_end, dt_local)),
    ]
    for name, start, end in windows:
        if start <= dt_local <= end:
            minutes_to_end = max(0, int((end - dt_local).total_seconds() // 60))
            minutes_from_start = max(0, int((dt_local - start).total_seconds() // 60))
            entry_allowed = minutes_to_end > CFG.no_entry_last_minutes and minutes_from_start >= CFG.session_warmup_minutes
            force_exit_zone = minutes_to_end <= CFG.force_exit_last_minutes
            reason_bits = []
            if minutes_from_start < CFG.session_warmup_minutes:
                reason_bits.append("session_warmup")
            if minutes_to_end <= CFG.no_entry_last_minutes:
                reason_bits.append("last_hour_no_entry")
            if force_exit_zone:
                reason_bits.append("force_exit_zone")
            return SessionContext(True, name, start, end, minutes_to_end, minutes_from_start, entry_allowed, force_exit_zone, ";".join(reason_bits) if reason_bits else "normal_session")
    return SessionContext(False, None, None, None, None, None, False, False, "outside_session")


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


def round_to_step(price: float, step: float) -> float:
    if step <= 0:
        return price
    return round(round(price / step) * step, 10)


class AlorClient:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.session = requests.Session()
        self.access_token: Optional[str] = None

    def refresh_access_token(self):
        url = f"{self.cfg.oauth_base}/refresh"
        logger.info(f"[AUTH] POST {url}")
        resp = self.session.post(url, params={"token": self.cfg.refresh_token}, timeout=30)
        logger.info(f"[AUTH] status={resp.status_code}")
        resp.raise_for_status()
        data = resp.json()
        token = data.get("AccessToken")
        if not token:
            raise RuntimeError(f"AccessToken не получен. Ответ: {data}")
        self.access_token = token
        logger.info("[AUTH] AccessToken получен")

    def auth_headers(self) -> dict[str, str]:
        if not self.access_token:
            self.refresh_access_token()
        return {"Authorization": f"Bearer {self.access_token}", "Accept": "application/json"}

    def md_get(self, path: str, params: Optional[dict[str, Any]] = None, private: bool = True, step: str = ""):
        url = f"{self.cfg.api_base}{path}"
        headers = self.auth_headers() if private else {"Accept": "application/json"}
        logger.info(f"[{step}] GET {url}")
        logger.info(f"[{step}] params={params}")
        resp = self.session.get(url, params=params, headers=headers, timeout=self.cfg.read_timeout_sec)
        logger.info(f"[{step}] status={resp.status_code}")
        if resp.status_code == 401 and private:
            self.refresh_access_token()
            headers = self.auth_headers()
            resp = self.session.get(url, params=params, headers=headers, timeout=self.cfg.read_timeout_sec)
            logger.info(f"[{step}] retry status={resp.status_code}")
        if resp.status_code in (400, 404):
            return None
        resp.raise_for_status()
        if not resp.text.strip():
            return None
        data = resp.json()
        if isinstance(data, dict):
            logger.info(f"[{step}] JSON object keys={list(data.keys())[:20]}")
        elif isinstance(data, list):
            logger.info(f"[{step}] JSON list len={len(data)}")
        return data

    def cmd_request(self, method: str, path: str, body: Optional[dict[str, Any]] = None, step: str = ""):
        if not self.access_token:
            self.refresh_access_token()
        url = f"{self.cfg.api_base}{path}"
        params = {"token": self.access_token}
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        logger.info(f"[{step}] {method.upper()} {url}")
        logger.info(f"[{step}] body={body}")
        resp = self.session.request(method=method.upper(), url=url, params=params, headers=headers, json=body, timeout=self.cfg.read_timeout_sec)
        logger.info(f"[{step}] status={resp.status_code}")
        if resp.status_code == 401:
            self.refresh_access_token()
            params = {"token": self.access_token}
            headers["Authorization"] = f"Bearer {self.access_token}"
            resp = self.session.request(method=method.upper(), url=url, params=params, headers=headers, json=body, timeout=self.cfg.read_timeout_sec)
            logger.info(f"[{step}] retry status={resp.status_code}")
        if resp.status_code in (400, 403, 404):
            logger.warning(f"[{step}] failed response={resp.text[:1000]}")
        resp.raise_for_status()
        if not resp.text.strip():
            return None
        data = resp.json()
        logger.info(f"[{step}] response={data}")
        return data


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    close = out["close"]
    high = out["high"]
    low = out["low"]
    volume = out["volume"]
    out["ema_fast"] = close.ewm(span=CFG.ema_fast, adjust=False).mean()
    out["ema_mid"] = close.ewm(span=CFG.ema_mid, adjust=False).mean()
    out["ema_slow"] = close.ewm(span=CFG.ema_slow, adjust=False).mean()
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / CFG.rsi_len, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / CFG.rsi_len, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
    out["rsi"] = 100 - (100 / (1 + rs))
    out["rsi"] = out["rsi"].fillna(50.0)
    prev_close = close.shift(1)
    tr = pd.concat([(high - low).abs(), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    out["atr"] = tr.ewm(alpha=1 / CFG.atr_len, adjust=False).mean()
    out["avg_volume"] = volume.rolling(CFG.breakout_lookback).mean()
    out["range_high_prev"] = high.shift(1).rolling(CFG.breakout_lookback).max()
    out["range_low_prev"] = low.shift(1).rolling(CFG.breakout_lookback).min()
    out["bar_size"] = high - low
    return out


def get_active_futures_quote(client: AlorClient) -> dict[str, Any]:
    data = client.md_get(f"/md/v2/Securities/{CFG.exchange}/{CFG.base_symbol}/actualFuturesQuote", params={"format": "Simple"}, private=True, step="ACTIVE_FUTURES")
    if not data or not data.get("symbol"):
        raise RuntimeError("Не удалось получить активный BR-контракт")
    return data


def get_security_detail(client: AlorClient, symbol: str) -> dict[str, Any]:
    data = client.md_get(f"/md/v2/Securities/{CFG.exchange}/{symbol}", params={"instrumentGroup": CFG.instrument_group, "format": "Simple"}, private=True, step="SECURITY_DETAIL")
    if not data:
        raise RuntimeError(f"Не удалось получить детали инструмента {symbol}")
    return data


def get_recent_candles(client: AlorClient, symbol: str) -> pd.DataFrame:
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=CFG.candle_lookback_days)
    data = client.md_get("/md/v2/history", params={"symbol": symbol, "exchange": CFG.exchange, "instrumentGroup": CFG.instrument_group, "tf": "300", "from": int(start_dt.timestamp()), "to": int(end_dt.timestamp()), "untraded": "true", "splitAdjust": "false", "format": "Simple"}, private=True, step="HISTORY_5M")
    if not data or "history" not in data:
        return pd.DataFrame()
    rows = data.get("history") or []
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["dt"] = pd.to_datetime(df["time"], unit="s", utc=True).dt.tz_convert(LOCAL_TZ)
    for c in ("open", "high", "low", "close", "volume"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["dt", "open", "high", "low", "close", "volume"]).sort_values("dt").reset_index(drop=True)
    return df


def get_quote(client: AlorClient, symbol: str) -> Optional[dict[str, Any]]:
    data = client.md_get(
        f"/md/v2/Securities/{symbol}/quotes",
        params={"exchange": CFG.exchange, "format": "Simple"},
        private=True,
        step="QUOTE",
    )
    if data is None:
        return None
    if isinstance(data, list):
        return data[0] if data else None
    if isinstance(data, dict):
        if "last_price" in data or "lastPrice" in data:
            return data
        values = list(data.values())
        return values[0] if values and isinstance(values[0], dict) else data
    return None


def get_orderbook(client: AlorClient, symbol: str) -> Optional[dict[str, Any]]:
    return client.md_get(f"/md/v2/orderbooks/{CFG.exchange}/{symbol}", params={"depth": 20}, private=True, step="ORDERBOOK")


def get_summary(client: AlorClient) -> Optional[dict[str, Any]]:
    return client.md_get(f"/md/v2/Clients/{CFG.exchange}/{CFG.portfolio}/summary", private=True, step="SUMMARY")


def get_positions(client: AlorClient) -> list[dict]:
    data = client.md_get(f"/md/v2/Clients/{CFG.exchange}/{CFG.portfolio}/positions", private=True, step="POSITIONS")
    return data if isinstance(data, list) else []


def get_orders(client: AlorClient) -> list[dict]:
    data = client.md_get(f"/md/v2/Clients/{CFG.exchange}/{CFG.portfolio}/orders", private=True, step="ORDERS")
    return data if isinstance(data, list) else []


def get_stoporders(client: AlorClient) -> list[dict]:
    data = client.md_get(f"/md/v2/Clients/{CFG.exchange}/{CFG.portfolio}/stoporders", private=True, step="STOPORDERS")
    return data if isinstance(data, list) else []


def get_trades_current_session(client: AlorClient, symbol: str) -> list[dict]:
    data = client.md_get(f"/md/v2/Clients/{CFG.exchange}/{CFG.portfolio}/{symbol}/trades", private=True, step="TRADES_SESSION")
    return data if isinstance(data, list) else []


def position_qty_from_item(item: dict[str, Any]) -> float:
    for key in ("qty", "quantity", "balance", "volume", "lots"):
        if key in item:
            return safe_float(item.get(key), 0.0)
    return 0.0


def position_avg_price(item: dict[str, Any]) -> float:
    for key in ("avgPrice", "avgprice", "priceAvg", "averagePrice", "waprice", "price"):
        if key in item:
            return safe_float(item.get(key), 0.0)
    return 0.0


def extract_position_for_symbol(positions: list[dict], symbol: str) -> Optional[dict]:
    for item in positions:
        if str(item.get("symbol")) == symbol:
            qty = position_qty_from_item(item)
            if abs(qty) > 0:
                return {"raw": item, "symbol": symbol, "qty": qty, "side": "long" if qty > 0 else "short", "abs_qty": int(abs(qty)), "avg_price": position_avg_price(item)}
    return None


def is_order_active(item: dict[str, Any]) -> bool:
    status = str(item.get("status") or item.get("state") or "").lower()
    inactive_markers = {"filled", "matched", "cancelled", "canceled", "rejected", "expired", "done"}
    return status not in inactive_markers


def is_robot_order(item: dict[str, Any]) -> bool:
    comment = str(item.get("comment") or item.get("message") or "")
    return comment.startswith(ROBOT_PREFIX)


def filter_symbol_orders(items: list[dict], symbol: str) -> list[dict]:
    result = []
    for item in items:
        if str(item.get("symbol") or item.get("code") or "") != symbol:
            continue
        if not is_order_active(item):
            continue
        result.append(item)
    return result


def get_order_id(item: dict[str, Any]) -> Optional[str]:
    for key in ("id", "orderId", "orderNumber", "orderno"):
        if key in item and str(item.get(key)).strip():
            return str(item.get(key))
    return None


def classify_robot_orders(exchange_orders: list[dict], stop_orders: list[dict]) -> dict[str, Any]:
    robot_exchange = [o for o in exchange_orders if is_robot_order(o)]
    robot_stop = [o for o in stop_orders if is_robot_order(o)]
    tp_orders, entry_orders, other_orders = [], [], []
    for o in robot_exchange:
        comment = str(o.get("comment") or "")
        if " TP " in comment or comment.endswith(" TP"):
            tp_orders.append(o)
        elif " ENTRY " in comment or comment.endswith(" ENTRY"):
            entry_orders.append(o)
        else:
            other_orders.append(o)
    return {"tp_orders": tp_orders, "entry_orders": entry_orders, "other_exchange_orders": other_orders, "stop_orders": robot_stop}


def orderbook_imbalance(orderbook: Optional[dict[str, Any]], levels: int = 5) -> float:
    if not orderbook:
        return 0.5
    bids = orderbook.get("bids") or []
    asks = orderbook.get("asks") or []
    bid_vol, ask_vol = 0.0, 0.0
    for row in bids[:levels]:
        if isinstance(row, dict):
            bid_vol += safe_float(row.get("volume") or row.get("qty") or row.get("quantity") or row.get("lots"))
        elif isinstance(row, (list, tuple)) and len(row) >= 2:
            bid_vol += safe_float(row[1])
    for row in asks[:levels]:
        if isinstance(row, dict):
            ask_vol += safe_float(row.get("volume") or row.get("qty") or row.get("quantity") or row.get("lots"))
        elif isinstance(row, (list, tuple)) and len(row) >= 2:
            ask_vol += safe_float(row[1])
    total = bid_vol + ask_vol
    return 0.5 if total <= 0 else bid_vol / total


@dataclass
class SignalDecision:
    symbol: str
    signal_bar_time: Optional[str]
    signal_side: str
    trend_up: bool
    trend_down: bool
    ema_stack_long: bool
    ema_stack_short: bool
    ema_slope_long: bool
    ema_slope_short: bool
    breakout_long: bool
    breakout_short: bool
    rsi_value: float
    rsi_long_ok: bool
    rsi_short_ok: bool
    atr_value: float
    atr_ticks: float
    atr_ok: bool
    vol_ratio: float
    vol_ok: bool
    spread_ticks: float
    spread_ok: bool
    signal_bar_ticks: float
    signal_bar_ok: bool
    orderbook_imbalance: float
    ob_long_ok: bool
    ob_short_ok: bool
    final_long: bool
    final_short: bool
    reason: str


def compute_signal(symbol: str, candles: pd.DataFrame, quote: Optional[dict], security: dict, orderbook: Optional[dict]) -> SignalDecision:
    df = add_indicators(candles)
    if len(df) < max(CFG.ema_slow + 3, CFG.breakout_lookback + 3):
        return SignalDecision(symbol, None, "none", False, False, False, False, False, False, False, False, 0.0, False, False, 0.0, 0.0, False, 0.0, False, 999.0, False, 0.0, False, 0.5, False, False, False, False, "not_enough_bars")
    signal_bar = df.iloc[-2]
    prev_bar = df.iloc[-3]
    step = safe_float(security.get("minstep"), 0.01) or 0.01
    bid = safe_float((quote or {}).get("bid"))
    ask = safe_float((quote or {}).get("ask"))
    spread_ticks = ((ask - bid) / step) if bid > 0 and ask > 0 else 999.0
    atr_value = safe_float(signal_bar["atr"])
    atr_ticks = atr_value / step if step > 0 else 0.0
    avg_vol = safe_float(signal_bar["avg_volume"], 0.0)
    vol_ratio = safe_float(signal_bar["volume"], 0.0) / avg_vol if avg_vol > 0 else 0.0
    signal_bar_ticks = safe_float(signal_bar["bar_size"], 0.0) / step if step > 0 else 0.0
    ob_imb = orderbook_imbalance(orderbook)
    ema_stack_long = signal_bar["ema_fast"] > signal_bar["ema_mid"] > signal_bar["ema_slow"]
    ema_stack_short = signal_bar["ema_fast"] < signal_bar["ema_mid"] < signal_bar["ema_slow"]
    ema_slope_long = signal_bar["ema_fast"] > prev_bar["ema_fast"] and signal_bar["ema_mid"] > prev_bar["ema_mid"]
    ema_slope_short = signal_bar["ema_fast"] < prev_bar["ema_fast"] and signal_bar["ema_mid"] < prev_bar["ema_mid"]
    trend_up = bool(ema_stack_long and ema_slope_long and signal_bar["close"] > signal_bar["ema_fast"])
    trend_down = bool(ema_stack_short and ema_slope_short and signal_bar["close"] < signal_bar["ema_fast"])
    breakout_long = bool(signal_bar["close"] > signal_bar["range_high_prev"])
    breakout_short = bool(signal_bar["close"] < signal_bar["range_low_prev"])
    rsi_value = safe_float(signal_bar["rsi"])
    rsi_long_ok = CFG.min_rsi_long <= rsi_value <= CFG.max_rsi_long
    rsi_short_ok = CFG.min_rsi_short <= rsi_value <= CFG.max_rsi_short
    atr_ok = CFG.min_atr_ticks <= atr_ticks <= CFG.max_atr_ticks
    vol_ok = vol_ratio >= CFG.min_vol_ratio
    spread_ok = spread_ticks <= CFG.max_spread_ticks
    signal_bar_ok = signal_bar_ticks <= CFG.max_signal_bar_ticks
    ob_long_ok = ob_imb >= CFG.min_ob_imbalance_long
    ob_short_ok = ob_imb <= CFG.max_ob_imbalance_short
    final_long = all([trend_up, breakout_long, rsi_long_ok, atr_ok, vol_ok, spread_ok, signal_bar_ok, ob_long_ok])
    final_short = all([trend_down, breakout_short, rsi_short_ok, atr_ok, vol_ok, spread_ok, signal_bar_ok, ob_short_ok])
    side, reason = "none", "no_signal"
    if final_long and not final_short:
        side, reason = "buy", "trend_breakout_long_confirmed"
    elif final_short and not final_long:
        side, reason = "sell", "trend_breakout_short_confirmed"
    elif final_long and final_short:
        reason = "conflict_signal"
    return SignalDecision(symbol, str(signal_bar["dt"]), side, bool(trend_up), bool(trend_down), bool(ema_stack_long), bool(ema_stack_short), bool(ema_slope_long), bool(ema_slope_short), bool(breakout_long), bool(breakout_short), float(rsi_value), bool(rsi_long_ok), bool(rsi_short_ok), float(atr_value), float(atr_ticks), bool(atr_ok), float(vol_ratio), bool(vol_ok), float(spread_ticks), bool(spread_ok), float(signal_bar_ticks), bool(signal_bar_ok), float(ob_imb), bool(ob_long_ok), bool(ob_short_ok), bool(final_long), bool(final_short), reason)


def base_instrument(symbol: str) -> dict[str, Any]:
    return {"symbol": symbol, "exchange": CFG.exchange, "instrumentGroup": CFG.instrument_group}


def base_user() -> dict[str, Any]:
    return {"portfolio": CFG.portfolio}


def build_market_order_body(symbol: str, side: str, qty: int, comment: str) -> dict[str, Any]:
    return {"side": side, "quantity": int(qty), "instrument": base_instrument(symbol), "user": base_user(), "timeInForce": "OneDay", "allowMargin": CFG.allow_margin, "checkDuplicates": True, "comment": comment}


def build_limit_order_body(symbol: str, side: str, qty: int, price: float, comment: str) -> dict[str, Any]:
    return {"side": side, "quantity": int(qty), "price": float(price), "instrument": base_instrument(symbol), "user": base_user(), "timeInForce": "OneDay", "allowMargin": CFG.allow_margin, "checkDuplicates": True, "comment": comment}


def build_stop_order_body(symbol: str, side: str, qty: int, trigger_price: float, condition: str, stop_end_utc: datetime, comment: str) -> dict[str, Any]:
    return {"side": side, "quantity": int(qty), "condition": condition, "triggerPrice": float(trigger_price), "stopEndUnixTime": int(stop_end_utc.timestamp()), "instrument": base_instrument(symbol), "user": base_user(), "allowMargin": CFG.allow_margin, "checkDuplicates": True, "protectingSeconds": 15, "comment": comment, "activate": True}


def estimate_order(client: AlorClient, body: dict[str, Any]) -> dict[str, Any]:
    return client.cmd_request("POST", "/commandapi/warptrans/TRADE/v2/client/orders/estimate", body=body, step="ESTIMATE") or {}


def create_market_order(client: AlorClient, body: dict[str, Any]) -> dict[str, Any]:
    return client.cmd_request("POST", "/commandapi/warptrans/TRADE/v2/client/orders/actions/market", body=body, step="CREATE_MARKET") or {}


def create_limit_order(client: AlorClient, body: dict[str, Any]) -> dict[str, Any]:
    return client.cmd_request("POST", "/commandapi/warptrans/TRADE/v2/client/orders/actions/limit", body=body, step="CREATE_LIMIT") or {}


def create_stop_order(client: AlorClient, body: dict[str, Any]) -> dict[str, Any]:
    return client.cmd_request("POST", "/commandapi/warptrans/TRADE/v2/client/orders/actions/stop", body=body, step="CREATE_STOP") or {}


def update_stop_order(client: AlorClient, stop_order_id: str, body: dict[str, Any]) -> dict[str, Any]:
    return client.cmd_request("PUT", f"/commandapi/warptrans/TRADE/v2/client/orders/actions/stop/{stop_order_id}", body=body, step="UPDATE_STOP") or {}


def cancel_order_any(client: AlorClient, order_id: str) -> dict[str, Any]:
    return client.cmd_request("DELETE", f"/commandapi/warptrans/TRADE/v2/client/orders/{order_id}", body={"portfolio": CFG.portfolio}, step="CANCEL_ORDER") or {}


def journal_order(event_type: str, payload: dict[str, Any]):
    append_jsonl(ORDER_JOURNAL, {"ts_utc": datetime.now(timezone.utc).isoformat(), "run_id": RUN_ID, "event_type": event_type, **payload})


def journal_decision(entry: dict[str, Any]):
    append_jsonl(DECISION_JOURNAL, entry)


def cleanup_stale_robot_orders(client: AlorClient, symbol: str, exchange_orders: list[dict], stop_orders: list[dict]) -> list[str]:
    cancelled = []
    for item in exchange_orders + stop_orders:
        if not is_robot_order(item):
            continue
        order_id = get_order_id(item)
        if not order_id:
            continue
        try:
            if CFG.enable_trading:
                cancel_order_any(client, order_id)
                cancelled.append(order_id)
                journal_order("cancel_stale_order", {"symbol": symbol, "order_id": order_id, "comment": item.get("comment")})
                time.sleep(CFG.request_sleep_sec)
            else:
                journal_order("cancel_stale_order_dry_run", {"symbol": symbol, "order_id": order_id, "comment": item.get("comment")})
        except Exception as e:
            logger.warning(f"Не удалось снять заявку {order_id}: {e}")
    return cancelled


def protective_prices(position_side: str, entry_price: float, step: float) -> tuple[float, float, str]:
    if position_side == "long":
        stop_price = round_to_step(entry_price - CFG.stop_ticks * step, step)
        tp_price = round_to_step(entry_price + CFG.take_ticks * step, step)
        exit_side = "sell"
    else:
        stop_price = round_to_step(entry_price + CFG.stop_ticks * step, step)
        tp_price = round_to_step(entry_price - CFG.take_ticks * step, step)
        exit_side = "buy"
    return stop_price, tp_price, exit_side


def breakeven_stop_price(position_side: str, entry_price: float, step: float) -> float:
    return round_to_step(entry_price + step if position_side == "long" else entry_price - step, step)


def stop_condition_for_exit(position_side: str) -> str:
    return "LessOrEqual" if position_side == "long" else "MoreOrEqual"


def is_breakeven_reached(position: dict[str, Any], last_price: float, step: float) -> bool:
    if last_price <= 0 or step <= 0:
        return False
    entry = safe_float(position.get("avg_price"), 0.0)
    if entry <= 0:
        return False
    if position["side"] == "long":
        return (last_price - entry) / step >= CFG.breakeven_ticks
    return (entry - last_price) / step >= CFG.breakeven_ticks


def ensure_exit_orders(client: AlorClient, symbol: str, position: dict[str, Any], security: dict, session_ctx: SessionContext, robot_orders: dict[str, Any]):
    step = safe_float(security.get("minstep"), 0.01) or 0.01
    entry_price = safe_float(position["avg_price"], 0.0)
    qty = int(position["abs_qty"])
    if entry_price <= 0 or qty <= 0:
        logger.info("Нет корректной средней цены позиции, защитные заявки не выставляю")
        return
    stop_price, tp_price, exit_side = protective_prices(position["side"], entry_price, step)
    stop_condition = stop_condition_for_exit(position["side"])
    stop_end_utc = session_ctx.session_end.astimezone(timezone.utc) if session_ctx.session_end else datetime.now(timezone.utc) + timedelta(hours=4)
    if not robot_orders["tp_orders"]:
        body = build_limit_order_body(symbol, exit_side, qty, tp_price, f"{ROBOT_PREFIX} TP {symbol} {position['side'].upper()}")
        if CFG.enable_trading:
            resp = create_limit_order(client, body)
            journal_order("create_tp", {"symbol": symbol, "body": body, "response": resp})
            time.sleep(CFG.request_sleep_sec)
        else:
            journal_order("create_tp_dry_run", {"symbol": symbol, "body": body})
    if not robot_orders["stop_orders"]:
        body = build_stop_order_body(symbol, exit_side, qty, stop_price, stop_condition, stop_end_utc, f"{ROBOT_PREFIX} STOP {symbol} {position['side'].upper()}")
        if CFG.enable_trading:
            resp = create_stop_order(client, body)
            journal_order("create_stop", {"symbol": symbol, "body": body, "response": resp})
            time.sleep(CFG.request_sleep_sec)
        else:
            journal_order("create_stop_dry_run", {"symbol": symbol, "body": body})


def maybe_move_stop_to_breakeven(client: AlorClient, symbol: str, position: dict[str, Any], security: dict, quote: Optional[dict], session_ctx: SessionContext, robot_orders: dict[str, Any]):
    step = safe_float(security.get("minstep"), 0.01) or 0.01
    last_price = safe_float((quote or {}).get("last_price") or (quote or {}).get("lastPrice"))
    if not is_breakeven_reached(position, last_price, step) or not robot_orders["stop_orders"]:
        return
    position_key = f"{symbol}:{position['side']}:{position['abs_qty']}:{position['avg_price']}"
    if STATE.get("breakeven_applied_for_position_key") == position_key:
        return
    stop_order = robot_orders["stop_orders"][0]
    stop_id = get_order_id(stop_order)
    if not stop_id:
        return
    exit_side = "sell" if position["side"] == "long" else "buy"
    be_price = breakeven_stop_price(position["side"], safe_float(position["avg_price"]), step)
    stop_end_utc = session_ctx.session_end.astimezone(timezone.utc) if session_ctx.session_end else datetime.now(timezone.utc) + timedelta(hours=4)
    body = build_stop_order_body(symbol, exit_side, int(position["abs_qty"]), be_price, stop_condition_for_exit(position["side"]), stop_end_utc, str(stop_order.get("comment") or f"{ROBOT_PREFIX} STOP {symbol} {position['side'].upper()}"))
    if CFG.enable_trading:
        resp = update_stop_order(client, stop_id, body)
        journal_order("move_stop_to_breakeven", {"symbol": symbol, "stop_order_id": stop_id, "body": body, "response": resp})
    else:
        journal_order("move_stop_to_breakeven_dry_run", {"symbol": symbol, "stop_order_id": stop_id, "body": body})
    STATE["breakeven_applied_for_position_key"] = position_key


def force_close_position(client: AlorClient, symbol: str, position: dict[str, Any], exchange_orders: list[dict], stop_orders: list[dict]):
    cleanup_stale_robot_orders(client, symbol, exchange_orders, stop_orders)
    exit_side = "sell" if position["side"] == "long" else "buy"
    body = build_market_order_body(symbol, exit_side, int(position["abs_qty"]), f"{ROBOT_PREFIX} FORCE_EXIT {symbol} {position['side'].upper()}")
    if CFG.order_estimate_required:
        est = estimate_order(client, body)
        journal_order("estimate_force_exit", {"symbol": symbol, "body": body, "estimate": est})
    if CFG.enable_trading:
        resp = create_market_order(client, body)
        journal_order("force_exit", {"symbol": symbol, "body": body, "response": resp})
    else:
        journal_order("force_exit_dry_run", {"symbol": symbol, "body": body})


def run_robot():
    validate_config(CFG)
    STATE["last_run_started_at"] = datetime.now(timezone.utc).isoformat()
    save_json(STATE_FILE, STATE)
    client = AlorClient(CFG)
    run_summary: dict[str, Any] = {"run_id": RUN_ID, "ts_utc": datetime.now(timezone.utc).isoformat(), "env": CFG.env_name, "portfolio": CFG.portfolio, "enable_trading": CFG.enable_trading}
    local_ts = now_local()
    session_ctx = get_session_context(local_ts)
    run_summary["session"] = asdict(session_ctx)
    active_quote = get_active_futures_quote(client)
    symbol = str(active_quote["symbol"])
    run_summary["symbol"] = symbol
    security = get_security_detail(client, symbol)
    run_summary["security"] = {"symbol": symbol, "shortname": security.get("shortname"), "minstep": security.get("minstep"), "lotsize": security.get("lotsize"), "facevalue": security.get("facevalue"), "cancellation": security.get("cancellation")}
    candles = get_recent_candles(client, symbol)
    if candles.empty:
        decision = {"run_id": RUN_ID, "ts_local": str(local_ts), "symbol": symbol, "action": "no_action", "reason": "no_candles", "session_reason": session_ctx.reason}
        journal_decision(decision)
        run_summary["decision"] = decision
        save_json(RUN_REPORT_FILE, run_summary)
        save_json(LATEST_REPORT_FILE, run_summary)
        STATE["last_run_finished_at"] = datetime.now(timezone.utc).isoformat()
        save_json(STATE_FILE, STATE)
        return
    candles.to_parquet(LATEST_CANDLES_FILE, index=False)
    quote = get_quote(client, symbol) or active_quote
    orderbook = get_orderbook(client, symbol)
    summary = get_summary(client)
    positions = get_positions(client)
    orders = filter_symbol_orders(get_orders(client), symbol)
    stoporders = filter_symbol_orders(get_stoporders(client), symbol)
    trades_session = get_trades_current_session(client, symbol)
    run_summary["summary"] = summary or {}
    run_summary["position_count"] = len(positions)
    run_summary["orders_count"] = len(orders)
    run_summary["stoporders_count"] = len(stoporders)
    run_summary["trades_session_count"] = len(trades_session)
    position = extract_position_for_symbol(positions, symbol)
    robot_orders = classify_robot_orders(orders, stoporders)
    signal = compute_signal(symbol, candles, quote, security, orderbook)
    decision_payload = asdict(signal)
    decision_payload.update({"run_id": RUN_ID, "ts_local": str(local_ts), "session": asdict(session_ctx), "position_exists": bool(position), "enable_trading": CFG.enable_trading})
    last_signal_bar_time = STATE.get("last_signal_bar_time")
    if position:
        STATE["managed_symbol"] = symbol
        STATE["managed_position"] = True
        STATE["managed_side"] = position["side"]
        run_summary["position"] = position
        if session_ctx.force_exit_zone:
            logger.info("Позиция есть и мы в force-exit зоне. Закрываю позицию по рынку.")
            force_close_position(client, symbol, position, orders, stoporders)
            decision_payload["action"] = "force_exit_position"
            decision_payload["action_reason"] = "force_exit_zone"
        else:
            ensure_exit_orders(client, symbol, position, security, session_ctx, robot_orders)
            maybe_move_stop_to_breakeven(client, symbol, position, security, quote, session_ctx, robot_orders)
            decision_payload["action"] = "hold_and_manage_position"
            decision_payload["action_reason"] = "position_exists_manage_exits"
        journal_decision(decision_payload)
        run_summary["decision"] = decision_payload
    else:
        STATE["managed_position"] = False
        STATE["managed_side"] = None
        STATE["breakeven_applied_for_position_key"] = None
        if robot_orders["tp_orders"] or robot_orders["stop_orders"] or robot_orders["other_exchange_orders"]:
            cancelled = cleanup_stale_robot_orders(client, symbol, orders, stoporders)
            decision_payload["stale_orders_cancelled"] = cancelled
        if not session_ctx.in_session:
            decision_payload["action"] = "no_action"
            decision_payload["action_reason"] = "outside_session"
        elif not session_ctx.entry_allowed:
            decision_payload["action"] = "no_action"
            decision_payload["action_reason"] = f"entry_blocked:{session_ctx.reason}"
        elif signal.signal_side == "none":
            decision_payload["action"] = "no_action"
            decision_payload["action_reason"] = signal.reason
        elif last_signal_bar_time == signal.signal_bar_time:
            decision_payload["action"] = "no_action"
            decision_payload["action_reason"] = "signal_bar_already_processed"
        else:
            entry_side = signal.signal_side
            body = build_market_order_body(symbol, entry_side, CFG.qty, f"{ROBOT_PREFIX} ENTRY {symbol} {'LONG' if entry_side == 'buy' else 'SHORT'}")
            estimate_result = None
            if CFG.order_estimate_required:
                estimate_result = estimate_order(client, body)
                journal_order("estimate_entry", {"symbol": symbol, "side": entry_side, "body": body, "estimate": estimate_result})
            if CFG.enable_trading:
                resp = create_market_order(client, body)
                journal_order("create_entry_market", {"symbol": symbol, "side": entry_side, "body": body, "response": resp})
                time.sleep(max(CFG.request_sleep_sec, 2.0))
                positions_after = get_positions(client)
                orders_after = filter_symbol_orders(get_orders(client), symbol)
                stop_after = filter_symbol_orders(get_stoporders(client), symbol)
                position_after = extract_position_for_symbol(positions_after, symbol)
                if position_after:
                    robot_orders_after = classify_robot_orders(orders_after, stop_after)
                    ensure_exit_orders(client, symbol, position_after, security, session_ctx, robot_orders_after)
                    STATE["managed_symbol"] = symbol
                    STATE["managed_position"] = True
                    STATE["managed_side"] = position_after["side"]
                decision_payload["action"] = "enter_market"
                decision_payload["action_reason"] = signal.reason
                decision_payload["entry_body"] = body
                decision_payload["estimate"] = estimate_result
                decision_payload["response"] = resp
            else:
                decision_payload["action"] = "dry_run_entry"
                decision_payload["action_reason"] = signal.reason
                decision_payload["entry_body"] = body
                decision_payload["estimate"] = estimate_result
            STATE["last_signal_bar_time"] = signal.signal_bar_time
        journal_decision(decision_payload)
        run_summary["decision"] = decision_payload
    STATE["last_run_finished_at"] = datetime.now(timezone.utc).isoformat()
    save_json(STATE_FILE, STATE)
    save_json(RUN_REPORT_FILE, run_summary)
    save_json(LATEST_REPORT_FILE, run_summary)


if __name__ == "__main__":
    try:
        run_robot()
    except Exception as exc:
        logger.exception("Фатальная ошибка робота")
        error_report = {"run_id": RUN_ID, "ts_utc": datetime.now(timezone.utc).isoformat(), "status": "error", "error": repr(exc)}
        save_json(RUN_REPORT_FILE, error_report)
        save_json(LATEST_REPORT_FILE, error_report)
        raise
