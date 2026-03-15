#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Ежедневный сбор данных Wildberries с сохранением в Yandex Cloud Object Storage.
Данные хранятся только в недельных файлах (кроме воронки продаж и 1С).
Автоматическое получение артикулов из заказов для отчёта по ключам.
Формат для keywords: Неделя ГГГГ-WНН.xlsx
Финансовые показатели: проверяется только последняя неделя.
Всегда читается первый лист в файле.
Поисковые запросы: загружается ТОЛЬКО предыдущий день (вчера).
Реклама: получает кампании из API, статистика за последние 30 дней, формирует отчёты по категориям.
Добавлено формирование единого объединённого отчёта (воронка + заказы + реклама).
Добавлен расчёт экономики (валовая прибыль и юнит-экономика) для последней полной недели.
Отчёт 1c_stocks временно исключён из списка (можно вернуть позже).
"""

import os
import io
import json
import time
import uuid
import zipfile
import tempfile
import traceback
import re
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple, Any, Set
import warnings
from collections import defaultdict

import pandas as pd
import requests
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
import pytz

warnings.simplefilter(action='ignore', category=FutureWarning)

# ========================== КЛАСС ДЛЯ РАБОТЫ С YANDEX CLOUD ==========================

class S3Storage:
    """Клиент для работы с S3-совместимым хранилищем Yandex Cloud."""

    def __init__(self, access_key: str, secret_key: str, bucket_name: str):
        self.bucket = bucket_name
        self.s3 = boto3.client(
            's3',
            endpoint_url='https://storage.yandexcloud.net',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name='ru-central1',
            config=Config(
                signature_version='s3v4',
                read_timeout=300,
                connect_timeout=60,
                retries={'max_attempts': 5}
            )
        )
        print(f"🔑 DEBUG: подключение к Yandex Cloud, Access Key (первые 5 символов): {access_key[:5]}...")

    def read_excel(self, key: str, sheet_name: Optional[str] = None) -> pd.DataFrame:
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=key)
            data = obj['Body'].read()
            df = pd.read_excel(io.BytesIO(data), sheet_name=sheet_name)
            return df
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return pd.DataFrame()
            else:
                raise e
        except Exception as e:
            print(f"Ошибка чтения {key}: {e}")
            return pd.DataFrame()

    def write_excel(self, key: str, df: pd.DataFrame, sheet_name: str = 'Data'):
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
            tmp_path = tmp.name
        try:
            with pd.ExcelWriter(tmp_path, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name=sheet_name)
            self.upload_file(tmp_path, key)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def read_excel_all_sheets(self, key: str) -> Dict[str, pd.DataFrame]:
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=key)
            data = obj['Body'].read()
            return pd.read_excel(io.BytesIO(data), sheet_name=None)
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return {}
            else:
                raise e
        except Exception as e:
            print(f"Ошибка чтения всех листов {key}: {e}")
            return {}

    def write_excel_sheets(self, key: str, sheets: Dict[str, pd.DataFrame]):
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
            tmp_path = tmp.name
        try:
            with pd.ExcelWriter(tmp_path, engine='openpyxl') as writer:
                for sheet_name, df in sheets.items():
                    safe_sheet_name = str(sheet_name)[:31] if sheet_name else 'Sheet1'
                    if df is None:
                        df = pd.DataFrame()
                    df.to_excel(writer, index=False, sheet_name=safe_sheet_name)
            self.upload_file(tmp_path, key)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def upload_file(self, local_path: str, key: str):
        self.s3.upload_file(local_path, self.bucket, key)

    def file_exists(self, key: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False

    def list_files(self, prefix: str) -> List[str]:
        try:
            response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents']]
            else:
                return []
        except Exception as e:
            print(f"Ошибка при list_files: {e}")
            return []


# ====================== ОСНОВНОЙ КЛАСС СБОРЩИКА ДАННЫХ ======================

class WildberriesDailyUpdater:
    def __init__(self, api_keys: Dict[str, Dict[str, str]], s3: S3Storage):
        self.api_keys = api_keys
        self.s3 = s3
        self.start_time = datetime.now(pytz.timezone('Europe/Moscow'))
        self.data_period_days = 90
        self.keyword_errors = []  # для сбора ошибок поисковых запросов

        self.reports_config = {
            'orders': {
                'name': 'Заказы',
                'folder': 'Заказы',
                'date_column': 'date',
                'id_columns': ['date', 'gNumber', 'srid'],
                'api_url': 'https://statistics-api.wildberries.ru/api/v1/supplier/orders',
                'api_method': 'GET',
                'key_type': 'stats',
            },
            'stocks': {
                'name': 'Остатки',
                'folder': 'Остатки',
                'date_column': 'Дата запроса',
                'id_columns': ['Дата запроса', 'Артикул WB', 'Склад'],
                'api_url': 'https://statistics-api.wildberries.ru/api/v1/supplier/stocks',
                'api_method': 'GET',
                'key_type': 'stats',
            },
            'finance': {
                'name': 'Финансовые показатели',
                'folder': 'Финансовые показатели',
                'date_column': 'rr_dt',
                'id_columns': ['rr_dt', 'rrd_id', 'nm_id'],
                'api_url': 'https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod',
                'api_method': 'GET',
                'key_type': 'stats',
            },
            'keywords': {
                'name': 'Позиции по Ключам',
                'folder': 'Поисковые запросы',
                'date_column': 'Дата',
                'id_columns': ['Дата', 'Поисковый запрос', 'Артикул WB', 'Фильтр'],
                'api_url': 'https://seller-analytics-api.wildberries.ru/api/v2/search-report/product/search-texts',
                'api_method': 'POST',
                'key_type': 'promo',
            },
            'funnel': {
                'name': 'Воронка продаж',
                'folder': 'Воронка продаж',
                'filename': 'Воронка продаж.xlsx',
                'date_column': 'dt',
                'id_columns': ['dt', 'nmID'],
                'api_url': 'https://seller-analytics-api.wildberries.ru/api/v2/nm-report/downloads',
                'api_method': 'POST',
                'key_type': 'promo',
                'retention_days': 90,
            },
            'adverts': {
                'name': 'Реклама',
                'folder': 'Реклама',
                'date_column': 'Дата',
                'id_columns': ['ID кампании', 'Дата'],
                'api_url': 'https://advert-api.wildberries.ru/api/advert/v2/adverts',
                'api_method': 'GET',
                'key_type': 'promo',
                'retention_days': 30,
            },
            '1c_stocks': {
                'name': 'Остатки 1С',
                'folder': 'Остатки',
                'filename': 'Остатки_1С.xlsx',
                'date_column': None,
                'id_columns': [],
                'api_url': None,
                'key_type': None,
            }
        }

        self.delays = {
            'orders': 65,
            'stocks': 65,
            'finance': 65,
            'keywords': 90,
            'funnel': 30,
            'adverts': 30,
            '1c_stocks': 0,
        }

        self.target_subjects = ['Помады', 'Косметические карандаши', 'Кисти косметические', 'Блески']
        self.log(f"🚀 Запуск обновления данных. Время: {self.start_time}")

    # ====================== ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ======================
    def log(self, message: str, level: str = "INFO", end: str = "\n"):
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [{level}] {message}", end=end, flush=True)

    def _get_week_start(self, date: datetime) -> datetime:
        return date - timedelta(days=date.weekday())

    def _get_weekly_key(self, store_name: str, report_type: str, date: datetime) -> str:
        year, week, _ = date.isocalendar()
        config = self.reports_config[report_type]
        if report_type == 'keywords':
            filename = f"Неделя {year}-W{week:02d}.xlsx"
        else:
            filename = f"{config['name']}_{year}-W{week:02d}.xlsx"
        return f"Отчёты/{config['folder']}/{store_name}/Недельные/{filename}"

    def _load_weekly_data(self, store_name: str, report_type: str, week_date: datetime) -> pd.DataFrame:
        key = self._get_weekly_key(store_name, report_type, week_date)
        self.log(f"📥 Загрузка недельного файла: {key}")
        try:
            df = self.s3.read_excel(key, sheet_name=0)
            if df.empty:
                self.log(f"ℹ️ Файл пуст")
                return df
            self.log(f"📋 Колонки в файле: {list(df.columns)}")
            date_col = self.reports_config[report_type]['date_column']
            if date_col in df.columns:
                df[date_col] = pd.to_datetime(df[date_col]).dt.strftime('%Y-%m-%d')
                unique_dates = sorted(df[date_col].unique())
                self.log(f"📊 В файле {len(df)} записей, даты: {unique_dates}")
                if report_type == 'keywords' and 'Фильтр' in df.columns and 'Артикул WB' in df.columns:
                    filters_present = df['Фильтр'].unique()
                    articles_count = df['Артикул WB'].nunique()
                    self.log(f"🔍 Фильтры в файле: {list(filters_present)}, уникальных артикулов: {articles_count}")
            else:
                self.log(f"⚠️ Колонка даты '{date_col}' не найдена")
            return df
        except Exception as e:
            self.log(f"⚠️ Ошибка загрузки {key}: {e}")
            return pd.DataFrame()

    def _save_weekly_data(self, df: pd.DataFrame, store_name: str, report_type: str, week_date: datetime) -> bool:
        if df.empty:
            return True
        key = self._get_weekly_key(store_name, report_type, week_date)
        config = self.reports_config[report_type]

        before = len(df)
        if config['id_columns']:
            existing_cols = [c for c in config['id_columns'] if c in df.columns]
            if existing_cols:
                df = df.drop_duplicates(subset=existing_cols, keep='last')
                after = len(df)
                if before > after:
                    self.log(f"🔍 Удалено дубликатов в недельном файле: {before - after}")

        try:
            self.s3.write_excel(key, df, sheet_name=config['name'])
            self.log(f"✅ Недельный файл сохранён: {key}, записей: {len(df)}")
            return True
        except Exception as e:
            self.log(f"❌ Ошибка сохранения {key}: {e}")
            traceback.print_exc()
            return False

    def _get_date_range_90_days(self) -> Tuple[datetime.date, datetime.date]:
        today = datetime.now(pytz.timezone('Europe/Moscow')).date()
        end_date = today - timedelta(days=1)
        start_date = end_date - timedelta(days=self.data_period_days - 1)
        return start_date, end_date

    def _get_date_range_last_n_days(self, n: int) -> Tuple[datetime.date, datetime.date]:
        today = datetime.now(pytz.timezone('Europe/Moscow')).date()
        end_date = today - timedelta(days=1)
        start_date = end_date - timedelta(days=n - 1)
        return start_date, end_date

    def _get_articles_by_subjects(self, store_name: str, subjects: List[str]) -> List[int]:
        self.log(f"🔍 Сбор артикулов из заказов по категориям: {subjects}")
        prefix = f"Отчёты/Заказы/{store_name}/Недельные/"
        all_files = self.s3.list_files(prefix)
        if not all_files:
            self.log("⚠️ Не найдено недельных файлов заказов")
            return []

        articles_set = set()
        possible_nm_cols = ['nmId', 'nmID', 'Артикул WB', 'Артикул']
        possible_subj_cols = ['subject', 'Предмет', 'subjectName', 'Название предмета']

        for file_key in all_files:
            self.log(f"📄 Обработка файла: {file_key}")
            try:
                df = self.s3.read_excel(file_key, sheet_name=0)
                if df.empty:
                    continue

                nm_col = None
                for col in possible_nm_cols:
                    if col in df.columns:
                        nm_col = col
                        break
                subj_col = None
                for col in possible_subj_cols:
                    if col in df.columns:
                        subj_col = col
                        break

                if nm_col is None or subj_col is None:
                    self.log(f"⚠️ В файле {file_key} не найдены колонки с артикулом или предметом")
                    continue

                df[subj_col] = df[subj_col].astype(str).str.lower().str.strip()
                target_lower = [s.lower() for s in subjects]

                mask = df[subj_col].isin(target_lower)
                filtered = df.loc[mask, nm_col].dropna().unique()
                for val in filtered:
                    try:
                        articles_set.add(int(val))
                    except (ValueError, TypeError):
                        continue

            except Exception as e:
                self.log(f"❌ Ошибка при обработке файла {file_key}: {e}")
                continue

        articles = list(articles_set)
        self.log(f"✅ Собрано {len(articles)} уникальных артикулов из заказов")
        return articles

    # ====================== МЕТОДЫ ДЛЯ КАЖДОГО ОТЧЁТА ======================
    def _make_request(self, config: dict, headers: dict, date_str: str, **kwargs) -> Optional[Any]:
        url = config['api_url']
        method = config['api_method']
        params = {}
        payload = None

        if config['name'] == 'Заказы':
            params = {"dateFrom": date_str, "flag": 1}
        elif config['name'] == 'Остатки':
            params = {"dateFrom": date_str}
        elif config['name'] == 'Финансовые показатели':
            return self._fetch_finance_day(config, headers, date_str)

        max_attempts = 5
        for attempt in range(max_attempts):
            try:
                if method == 'GET':
                    resp = requests.get(url, headers=headers, params=params, timeout=120)
                else:
                    resp = requests.post(url, headers=headers, json=payload, timeout=120)

                if resp.status_code == 200:
                    return resp.json()
                elif resp.status_code == 429:
                    wait = 60 * (attempt + 1)
                    self.log(f"    ⚠ Лимит запросов (429), попытка {attempt+1}/{max_attempts}, ждём {wait} сек...")
                    time.sleep(wait)
                elif resp.status_code == 204:
                    return []
                elif resp.status_code in (502, 503, 504):
                    wait = 30 * (attempt + 1)
                    self.log(f"    ⚠ Ошибка шлюза {resp.status_code}, попытка {attempt+1}/{max_attempts}, ждём {wait} сек...")
                    time.sleep(wait)
                else:
                    self.log(f"    ❌ Ошибка {resp.status_code}: {resp.text[:200]}")
                    if attempt < max_attempts - 1:
                        time.sleep(10)
                    else:
                        return None
            except Exception as e:
                self.log(f"    ❌ Исключение при запросе: {e}")
                if attempt < max_attempts - 1:
                    time.sleep(10)
                else:
                    return None
        return None

    def _fetch_finance_day(self, config: dict, headers: dict, date_str: str) -> List[dict]:
        url = config['api_url']
        all_items = []
        rrdid = 0
        limit = 100000
        max_attempts = 3
        while True:
            params = {
                "dateFrom": date_str,
                "dateTo": date_str,
                "limit": limit,
                "rrdid": rrdid,
                "period": "daily"
            }
            for attempt in range(max_attempts):
                try:
                    resp = requests.get(url, headers=headers, params=params, timeout=120)
                    if resp.status_code == 200:
                        data = resp.json()
                        if not data:
                            return all_items
                        all_items.extend(data)
                        last_rrdid = data[-1].get("rrd_id", 0)
                        if len(data) < limit or last_rrdid <= rrdid:
                            return all_items
                        rrdid = last_rrdid
                        break
                    elif resp.status_code == 204:
                        return all_items
                    elif resp.status_code == 429:
                        wait = 60 * (attempt + 1)
                        self.log(f"    ⚠ Лимит, попытка {attempt+1}/{max_attempts}, ждём {wait} сек...")
                        time.sleep(wait)
                    else:
                        self.log(f"    ❌ Ошибка {resp.status_code}: {resp.text[:200]}")
                        if attempt == max_attempts - 1:
                            return all_items
                        time.sleep(10)
                except Exception as e:
                    self.log(f"    ❌ Исключение: {e}")
                    if attempt == max_attempts - 1:
                        return all_items
                    time.sleep(10)
        return all_items

    # ---------- Заказы ----------
    def update_orders(self, store_name: str) -> bool:
        self.log(f"\n📌 ОБНОВЛЕНИЕ: Заказы для магазина {store_name}")
        config = self.reports_config['orders']
        start_date, end_date = self._get_date_range_90_days()
        all_dates = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]

        weeks = defaultdict(list)
        for d in all_dates:
            week_start = self._get_week_start(datetime.combine(d, datetime.min.time()))
            weeks[week_start].append(d)

        api_key = self.api_keys[store_name][config['key_type']]
        headers = {"Authorization": api_key.strip()}

        for week_start, dates in weeks.items():
            self.log(f"📅 Обработка недели, начинающейся {week_start.strftime('%Y-%m-%d')}")
            weekly_df = self._load_weekly_data(store_name, 'orders', week_start)
            if not weekly_df.empty:
                existing_dates = set(pd.to_datetime(weekly_df['date']).dt.date.unique()) if 'date' in weekly_df.columns else set()
            else:
                existing_dates = set()

            dates_to_load = [d for d in dates if d not in existing_dates]
            if not dates_to_load:
                self.log(f"✅ Все дни недели уже загружены")
                continue

            self.log(f"📅 Недостающие дни: {[d.strftime('%Y-%m-%d') for d in dates_to_load]}")
            new_data = []
            for date in dates_to_load:
                date_str = date.strftime('%Y-%m-%d')
                self.log(f"📅 Загрузка дня: {date_str}")
                data = self._make_request(config, headers, date_str)
                if data and isinstance(data, list):
                    day_df = pd.DataFrame(data)
                    if not day_df.empty:
                        day_df['store'] = store_name
                        if 'date' in day_df.columns:
                            day_df['date'] = pd.to_datetime(day_df['date']).dt.strftime('%Y-%m-%d')
                        new_data.append(day_df)
                        self.log(f"✅ Получено {len(day_df)} записей")
                    else:
                        self.log(f"ℹ️ Нет данных за {date_str}")
                else:
                    self.log(f"⚠️ Не удалось получить данные за {date_str}")

                if date != dates_to_load[-1]:
                    time.sleep(self.delays['orders'])

            if new_data:
                new_df = pd.concat(new_data, ignore_index=True)
                if weekly_df.empty:
                    weekly_df = new_df
                else:
                    weekly_df = pd.concat([weekly_df, new_df], ignore_index=True)
                self._save_weekly_data(weekly_df, store_name, 'orders', week_start)
            else:
                self.log(f"ℹ️ Нет новых данных за неделю")
        return True

    # ---------- Остатки ----------
    def update_stocks(self, store_name: str) -> bool:
        self.log(f"\n📌 ОБНОВЛЕНИЕ: Остатки для магазина {store_name}")
        config = self.reports_config['stocks']
        target_date = (datetime.now() - timedelta(days=1)).date()
        week_start = self._get_week_start(datetime.combine(target_date, datetime.min.time()))
        weekly_df = self._load_weekly_data(store_name, 'stocks', week_start)
        if not weekly_df.empty:
            existing_dates = set(pd.to_datetime(weekly_df['Дата запроса']).dt.date.unique()) if 'Дата запроса' in weekly_df.columns else set()
        else:
            existing_dates = set()

        if target_date in existing_dates:
            self.log(f"✅ Данные за {target_date} уже есть в недельном файле, пропускаем")
            return True

        self.log(f"📅 Загрузка остатков за {target_date}...")
        api_key = self.api_keys[store_name][config['key_type']]
        headers = {"Authorization": api_key.strip()}
        try:
            params = {"dateFrom": target_date.strftime('%Y-%m-%d')}
            resp = requests.get(config['api_url'], headers=headers, params=params, timeout=60)
            if resp.status_code == 200:
                data = resp.json()
                if data:
                    df_day = pd.DataFrame(data)
                    df_day['Дата запроса'] = target_date.strftime('%Y-%m-%d')
                    df_day['Магазин'] = store_name
                    df_day['Дата сбора'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    rename_map = {
                        'lastChangeDate': 'Дата последнего изменения',
                        'warehouseName': 'Склад',
                        'supplierArticle': 'Артикул продавца',
                        'nmId': 'Артикул WB',
                        'barcode': 'Баркод',
                        'quantity': 'Доступно для продажи',
                        'inWayToClient': 'В пути к клиенту',
                        'inWayFromClient': 'В пути от клиента',
                        'quantityFull': 'Полное количество',
                        'category': 'Категория',
                        'subject': 'Предмет',
                        'brand': 'Бренд',
                        'techSize': 'Размер',
                        'Price': 'Цена',
                        'Discount': 'Скидка',
                        'isSupply': 'Договор поставки',
                        'isRealization': 'Договор реализации',
                        'SCCode': 'Код контракта'
                    }
                    df_day.rename(columns={k: v for k, v in rename_map.items() if k in df_day.columns}, inplace=True)

                    if weekly_df.empty:
                        weekly_df = df_day
                    else:
                        weekly_df = pd.concat([weekly_df, df_day], ignore_index=True)

                    self._save_weekly_data(weekly_df, store_name, 'stocks', week_start)
                    self.log(f"✅ Данные за {target_date} добавлены в недельный файл")
                else:
                    self.log(f"ℹ️ Нет данных за {target_date}")
            elif resp.status_code == 429:
                self.log(f"⚠️ Лимит запросов, ждём 65 сек...")
                time.sleep(65)
                return False
            else:
                self.log(f"❌ Ошибка {resp.status_code}: {resp.text[:200]}")
                return False
        except Exception as e:
            self.log(f"❌ Исключение при запросе: {e}")
            return False
        return True

    # ---------- Финансовые показатели ----------
    def update_finance(self, store_name: str) -> bool:
        self.log(f"\n📌 ОБНОВЛЕНИЕ: Финансовые показатели для магазина {store_name} (оптимизировано)")
        config = self.reports_config['finance']
        today = datetime.now(pytz.timezone('Europe/Moscow')).date()
        last_date = today - timedelta(days=1)
        last_week_start = self._get_week_start(datetime.combine(last_date, datetime.min.time()))

        self.log(f"📅 Обработка последней недели, начинающейся {last_week_start.strftime('%Y-%m-%d')}")
        weekly_df = self._load_weekly_data(store_name, 'finance', last_week_start)

        if not weekly_df.empty:
            existing_dates = set(pd.to_datetime(weekly_df['rr_dt']).dt.date.unique()) if 'rr_dt' in weekly_df.columns else set()
        else:
            existing_dates = set()

        required_dates = []
        current = last_week_start.date()
        while current <= last_date:
            required_dates.append(current)
            current += timedelta(days=1)

        dates_to_load = [d for d in required_dates if d not in existing_dates]
        if not dates_to_load:
            self.log(f"✅ Все дни последней недели уже загружены")
        else:
            self.log(f"📅 Недостающие дни последней недели: {[d.strftime('%Y-%m-%d') for d in dates_to_load]}")
            api_key = self.api_keys[store_name][config['key_type']]
            headers = {"Authorization": f"Bearer {api_key.strip()}"}
            new_data = []

            for date in dates_to_load:
                date_str = date.strftime('%Y-%m-%d')
                self.log(f"📅 Загрузка дня: {date_str}")
                day_data = self._fetch_finance_day(config, headers, date_str)
                if day_data:
                    day_df = pd.DataFrame(day_data)
                    day_df['store'] = store_name
                    if 'rr_dt' in day_df.columns:
                        day_df['rr_dt'] = pd.to_datetime(day_df['rr_dt']).dt.strftime('%Y-%m-%d')
                    new_data.append(day_df)
                    self.log(f"✅ Получено {len(day_df)} записей")
                else:
                    self.log(f"ℹ️ Нет данных за {date_str}")

                if date != dates_to_load[-1]:
                    time.sleep(self.delays['finance'])

            if new_data:
                new_df = pd.concat(new_data, ignore_index=True)
                if weekly_df.empty:
                    weekly_df = new_df
                else:
                    weekly_df = pd.concat([weekly_df, new_df], ignore_index=True)
                self._save_weekly_data(weekly_df, store_name, 'finance', last_week_start)
            else:
                self.log(f"ℹ️ Нет новых данных за последнюю неделю")

        # Проверка наличия файлов для остальных недель
        start_date, end_date = self._get_date_range_90_days()
        all_dates = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
        weeks = set()
        for d in all_dates:
            week_start = self._get_week_start(datetime.combine(d, datetime.min.time()))
            weeks.add(week_start)
        weeks.discard(last_week_start)

        for week_start in weeks:
            key = self._get_weekly_key(store_name, 'finance', week_start)
            if not self.s3.file_exists(key):
                self.log(f"⚠️ Отсутствует файл за неделю {week_start.strftime('%Y-%m-%d')}. Возможно, потребуется историческая загрузка.")

        self.log("✅ Финансовые показатели успешно обновлены")
        return True

    # ---------- Повторные попытки для поисковых запросов ----------
    def _retry_keyword_errors(self, store_name: str):
        if not self.keyword_errors:
            return

        self.log(f"\n🔄 Повторная загрузка для {len(self.keyword_errors)} ошибочных комбинаций...")
        api_key = self.api_keys[store_name]['promo']
        headers = {"Authorization": f"Bearer {api_key.strip()}", "Content-Type": "application/json"}
        url = self.reports_config['keywords']['api_url']
        filters = ["orders", "openCard", "addToCart"]

        # Группируем по дате и фильтру
        by_date_filter = defaultdict(list)
        for date_str, nm_id, filter_field in self.keyword_errors:
            by_date_filter[(date_str, filter_field)].append(nm_id)

        new_errors = []
        for (date_str, filter_field), nm_ids in by_date_filter.items():
            nm_ids = list(set(nm_ids))
            self.log(f"📅 {date_str} | Фильтр {filter_field} | артикулов: {len(nm_ids)}")

            batches = [nm_ids[i:i+50] for i in range(0, len(nm_ids), 50)]
            for batch in batches:
                payload = {
                    "currentPeriod": {"start": date_str, "end": date_str},
                    "nmIds": batch,
                    "topOrderBy": filter_field,
                    "includeSubstitutedSKUs": False,
                    "includeSearchTexts": True,
                    "orderBy": {"field": filter_field, "mode": "desc"},
                    "limit": 100
                }
                max_retries = 5
                for attempt in range(max_retries):
                    try:
                        resp = requests.post(url, headers=headers, json=payload, timeout=120)
                        if resp.status_code == 200:
                            data = resp.json()
                            items = data.get('data', {}).get('items', [])
                            if items:
                                batch_data = []
                                for item in items:
                                    text = item.get('text', '').strip()
                                    if not text:
                                        continue
                                    row = {
                                        "Дата": date_str,
                                        "Магазин": store_name,
                                        "Поисковый запрос": text,
                                        "Фильтр": filter_field,
                                        "Артикул WB": item.get("nmId", ""),
                                        "Предмет": item.get("subjectName", ""),
                                        "Бренд": item.get("brandName", ""),
                                        "Артикул продавца": item.get("vendorCode", ""),
                                        "Название товара": item.get("name", ""),
                                        "Рейтинг карточки": item.get("rating", 0),
                                        "Рейтинг отзывов": item.get("feedbackRating", 0),
                                        "Частота запросов": item.get("frequency", {}).get("current", 0),
                                        "Частота динамика %": item.get("frequency", {}).get("dynamics", 0),
                                        "Частота за неделю": item.get("weekFrequency", 0),
                                        "Медианная позиция": item.get("medianPosition", {}).get("current", 0),
                                        "Медианная позиция динамика %": item.get("medianPosition", {}).get("dynamics", 0),
                                        "Средняя позиция": item.get("avgPosition", {}).get("current", 0),
                                        "Средняя позиция динамика %": item.get("avgPosition", {}).get("dynamics", 0),
                                        "Переходы в карточку": item.get("openCard", {}).get("current", 0),
                                        "Переходы динамика %": item.get("openCard", {}).get("dynamics", 0),
                                        "% выше конкурентов (переходы)": item.get("openCard", {}).get("percentile", 0),
                                        "Добавления в корзину": item.get("addToCart", {}).get("current", 0),
                                        "Добавления динамика %": item.get("addToCart", {}).get("dynamics", 0),
                                        "% выше конкурентов (добавления)": item.get("addToCart", {}).get("percentile", 0),
                                        "Заказы": item.get("orders", {}).get("current", 0),
                                        "Заказы динамика %": item.get("orders", {}).get("dynamics", 0),
                                        "% выше конкурентов (заказы)": item.get("orders", {}).get("percentile", 0),
                                        "Конверсия в заказ %": item.get("cartToOrder", {}).get("current", 0),
                                        "Конверсия в заказ динамика %": item.get("cartToOrder", {}).get("dynamics", 0),
                                        "% выше конкурентов (конв. в заказ)": item.get("cartToOrder", {}).get("percentile", 0),
                                        "Конверсия в корзину %": item.get("openToCart", {}).get("current", 0),
                                        "Конверсия в корзину динамика %": item.get("openToCart", {}).get("dynamics", 0),
                                        "% выше конкурентов (конв. в корзину)": item.get("openToCart", {}).get("percentile", 0),
                                        "Видимость %": item.get("visibility", {}).get("current", 0),
                                        "Видимость динамика %": item.get("visibility", {}).get("dynamics", 0),
                                        "Есть рейтинг карточки": item.get("isCardRated", False),
                                        "Минимальная цена": item.get("price", {}).get("minPrice", 0),
                                        "Максимальная цена": item.get("price", {}).get("maxPrice", 0),
                                    }
                                    batch_data.append(row)
                                if batch_data:
                                    # Сохраняем в соответствующий недельный файл
                                    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                                    week_start = self._get_week_start(date_obj)
                                    weekly_df = self._load_weekly_data(store_name, 'keywords', week_start)
                                    new_df = pd.DataFrame(batch_data)
                                    if weekly_df.empty:
                                        weekly_df = new_df
                                    else:
                                        weekly_df = pd.concat([weekly_df, new_df], ignore_index=True)
                                    self._save_weekly_data(weekly_df, store_name, 'keywords', week_start)
                            break
                        elif resp.status_code in (429, 502, 503, 504):
                            wait = 60 * (attempt + 1)
                            self.log(f"    ⚠ Ошибка {resp.status_code}, повтор через {wait} сек...")
                            time.sleep(wait)
                        else:
                            self.log(f"    ❌ Ошибка {resp.status_code}, пропускаем")
                            for nm_id in batch:
                                new_errors.append((date_str, nm_id, filter_field))
                            break
                    except Exception as e:
                        self.log(f"    ❌ Исключение: {e}")
                        if attempt < max_retries - 1:
                            time.sleep(10)
                        else:
                            for nm_id in batch:
                                new_errors.append((date_str, nm_id, filter_field))
                        break
                time.sleep(30)

        self.keyword_errors = new_errors
        if self.keyword_errors:
            self.log(f"⚠️ После повторов осталось {len(self.keyword_errors)} ошибок")
        else:
            self.log("✅ Все ошибки устранены")

    # ---------- Позиции по ключам (загрузка только за предыдущий день) ----------
    def update_keywords(self, store_name: str) -> bool:
        self.log(f"\n📌 ОБНОВЛЕНИЕ: Позиции по ключам для магазина {store_name} (только за вчера)")

        # 1. Получаем актуальные артикулы из заказов
        articles = self._get_articles_by_subjects(store_name, self.target_subjects)
        if not articles:
            self.log("⚠️ Не найдено артикулов из заказов. Отчёт будет пропущен.")
            return False

        self.log(f"📦 Актуальных артикулов: {len(articles)}")

        # 2. Определяем целевую дату – вчера
        target_date = (datetime.now(pytz.timezone('Europe/Moscow')) - timedelta(days=1)).date()
        target_date_str = target_date.strftime('%Y-%m-%d')
        self.log(f"📅 Целевая дата: {target_date_str}")

        # 3. Определяем неделю, к которой относится целевая дата
        week_start = self._get_week_start(datetime.combine(target_date, datetime.min.time()))
        self.log(f"📅 Неделя начинается: {week_start.strftime('%Y-%m-%d')}")

        # 4. Загружаем существующий недельный файл (если есть)
        weekly_df = self._load_weekly_data(store_name, 'keywords', week_start)

        # 5. Формируем множество существующих комбинаций (дата, артикул, фильтр) для целевой даты
        existing_keys = set()
        if not weekly_df.empty:
            # Фильтруем только строки за целевую дату
            day_df = weekly_df[weekly_df['Дата'] == target_date_str].copy()
            if not day_df.empty:
                day_df['Артикул WB'] = day_df['Артикул WB'].astype(int)
                for _, row in day_df.iterrows():
                    nm = row['Артикул WB']
                    f = row['Фильтр']
                    existing_keys.add((target_date_str, nm, f))
                self.log(f"🔍 В недельном файле найдено {len(existing_keys)} записей за {target_date_str}")
            else:
                self.log(f"ℹ️ За {target_date_str} в недельном файле записей нет")

        filters = ["orders", "openCard", "addToCart"]

        # 6. Определяем, каких фильтров не хватает для каждого артикула
        missing_articles = []
        for nm_id in articles:
            missing_filters = []
            for f in filters:
                if (target_date_str, nm_id, f) not in existing_keys:
                    missing_filters.append(f)
            if missing_filters:
                missing_articles.append(nm_id)
                if len(missing_articles) <= 3:
                    self.log(f"❌ Для артикула {nm_id} пропущены фильтры: {missing_filters}")

        if not missing_articles:
            self.log(f"✅ Все данные за {target_date_str} уже загружены полностью.")
            return True

        self.log(f"📅 Необходимо загрузить данные для {len(missing_articles)} артикулов")

        # 7. Загружаем недостающие данные
        api_key = self.api_keys[store_name]['promo']
        headers = {"Authorization": f"Bearer {api_key.strip()}", "Content-Type": "application/json"}
        url = self.reports_config['keywords']['api_url']

        # Сброс списка ошибок перед началом
        self.keyword_errors = []

        new_data = []
        batches = [missing_articles[i:i+50] for i in range(0, len(missing_articles), 50)]
        for batch_idx, batch in enumerate(batches, 1):
            self.log(f"  📦 Батч {batch_idx}/{len(batches)}: {len(batch)} артикулов")
            batch_data = []
            for filter_field in filters:
                self.log(f"    🔍 Фильтр {filter_field}", end="")
                payload = {
                    "currentPeriod": {"start": target_date_str, "end": target_date_str},
                    "nmIds": batch,
                    "topOrderBy": filter_field,
                    "includeSubstitutedSKUs": False,
                    "includeSearchTexts": True,
                    "orderBy": {"field": filter_field, "mode": "desc"},
                    "limit": 100
                }
                max_retries = 5
                success = False
                for attempt in range(max_retries):
                    try:
                        resp = requests.post(url, headers=headers, json=payload, timeout=120)
                        if resp.status_code == 200:
                            data = resp.json()
                            items = data.get('data', {}).get('items', [])
                            for item in items:
                                text = item.get('text', '').strip()
                                if not text:
                                    continue
                                row = {
                                    "Дата": target_date_str,
                                    "Магазин": store_name,
                                    "Поисковый запрос": text,
                                    "Фильтр": filter_field,
                                    "Артикул WB": item.get("nmId", ""),
                                    "Предмет": item.get("subjectName", ""),
                                    "Бренд": item.get("brandName", ""),
                                    "Артикул продавца": item.get("vendorCode", ""),
                                    "Название товара": item.get("name", ""),
                                    "Рейтинг карточки": item.get("rating", 0),
                                    "Рейтинг отзывов": item.get("feedbackRating", 0),
                                    "Частота запросов": item.get("frequency", {}).get("current", 0),
                                    "Частота динамика %": item.get("frequency", {}).get("dynamics", 0),
                                    "Частота за неделю": item.get("weekFrequency", 0),
                                    "Медианная позиция": item.get("medianPosition", {}).get("current", 0),
                                    "Медианная позиция динамика %": item.get("medianPosition", {}).get("dynamics", 0),
                                    "Средняя позиция": item.get("avgPosition", {}).get("current", 0),
                                    "Средняя позиция динамика %": item.get("avgPosition", {}).get("dynamics", 0),
                                    "Переходы в карточку": item.get("openCard", {}).get("current", 0),
                                    "Переходы динамика %": item.get("openCard", {}).get("dynamics", 0),
                                    "% выше конкурентов (переходы)": item.get("openCard", {}).get("percentile", 0),
                                    "Добавления в корзину": item.get("addToCart", {}).get("current", 0),
                                    "Добавления динамика %": item.get("addToCart", {}).get("dynamics", 0),
                                    "% выше конкурентов (добавления)": item.get("addToCart", {}).get("percentile", 0),
                                    "Заказы": item.get("orders", {}).get("current", 0),
                                    "Заказы динамика %": item.get("orders", {}).get("dynamics", 0),
                                    "% выше конкурентов (заказы)": item.get("orders", {}).get("percentile", 0),
                                    "Конверсия в заказ %": item.get("cartToOrder", {}).get("current", 0),
                                    "Конверсия в заказ динамика %": item.get("cartToOrder", {}).get("dynamics", 0),
                                    "% выше конкурентов (конв. в заказ)": item.get("cartToOrder", {}).get("percentile", 0),
                                    "Конверсия в корзину %": item.get("openToCart", {}).get("current", 0),
                                    "Конверсия в корзину динамика %": item.get("openToCart", {}).get("dynamics", 0),
                                    "% выше конкурентов (конв. в корзину)": item.get("openToCart", {}).get("percentile", 0),
                                    "Видимость %": item.get("visibility", {}).get("current", 0),
                                    "Видимость динамика %": item.get("visibility", {}).get("dynamics", 0),
                                    "Есть рейтинг карточки": item.get("isCardRated", False),
                                    "Минимальная цена": item.get("price", {}).get("minPrice", 0),
                                    "Максимальная цена": item.get("price", {}).get("maxPrice", 0),
                                }
                                batch_data.append(row)
                            self.log(f" -> ✓ {len(items)} записей")
                            success = True
                            break
                        elif resp.status_code == 429:
                            wait = 60 * (attempt + 1)
                            self.log(f" -> ⚠ Лимит, попытка {attempt+1}/{max_retries}, ждём {wait} сек...")
                            time.sleep(wait)
                        elif resp.status_code in (502, 503, 504):
                            wait = 30 * (attempt + 1)
                            self.log(f" -> ⚠ Ошибка шлюза {resp.status_code}, попытка {attempt+1}/{max_retries}, ждём {wait} сек...")
                            time.sleep(wait)
                        else:
                            self.log(f" -> ❌ Ошибка {resp.status_code}")
                            break
                    except Exception as e:
                        self.log(f"    ❌ Исключение: {e}")
                        if attempt < max_retries - 1:
                            time.sleep(10)
                        else:
                            break
                if not success:
                    for nm_id in batch:
                        self.keyword_errors.append((target_date_str, nm_id, filter_field))
                if filter_field != filters[-1]:
                    time.sleep(30)

            if batch_data:
                batch_df = pd.DataFrame(batch_data)
                new_data.append(batch_df)

            if batch_idx < len(batches):
                self.log("    ⏳ Пауза 30 сек между батчами...")
                time.sleep(30)

        if new_data:
            new_df = pd.concat(new_data, ignore_index=True)
            if weekly_df.empty:
                weekly_df = new_df
            else:
                weekly_df = pd.concat([weekly_df, new_df], ignore_index=True)

            self._save_weekly_data(weekly_df, store_name, 'keywords', week_start)
            self.log(f"✅ Данные за {target_date_str} успешно добавлены в недельный файл")
        else:
            self.log(f"ℹ️ Нет новых данных для {target_date_str}")

        if self.keyword_errors:
            self._retry_keyword_errors(store_name)

        return True

    # ---------- Воронка продаж ----------
    def update_funnel(self, store_name: str) -> bool:
        self.log(f"\n📌 ОБНОВЛЕНИЕ: Воронка продаж для магазина {store_name}")
        config = self.reports_config['funnel']
        key = f"Отчёты/{config['folder']}/{store_name}/{config['filename']}"
        if self.s3.file_exists(key):
            df_existing = self.s3.read_excel(key, sheet_name=0)
            if not df_existing.empty:
                start_date, _ = self._get_date_range_90_days()
                date_col = config['date_column']
                if date_col in df_existing.columns:
                    df_existing[date_col] = pd.to_datetime(df_existing[date_col])
                    max_date = df_existing[date_col].max()
                    if max_date and max_date.date() >= start_date:
                        self.log("✅ Данные воронки уже актуальны")
                        return True

        self.log("🔄 Запуск формирования отчёта воронки...")
        api_key = self.api_keys[store_name][config['key_type']]
        headers = {"Authorization": f"Bearer {api_key.strip()}", "Content-Type": "application/json"}

        start_date, end_date = self._get_date_range_90_days()
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        report_id = str(uuid.uuid4())

        create_payload = {
            "id": report_id,
            "reportType": "DETAIL_HISTORY_REPORT",
            "userReportName": "Воронка продаж",
            "params": {
                "nmIDs": [],
                "subjectIds": [],
                "brandNames": [],
                "tagIds": [],
                "startDate": start_str,
                "endDate": end_str,
                "timezone": "Europe/Moscow",
                "aggregationLevel": "day",
                "skipDeletedNm": False
            }
        }

        try:
            resp = requests.post(config['api_url'], headers=headers, json=create_payload, timeout=60)
            if resp.status_code != 200:
                self.log(f"❌ Ошибка создания отчёта: {resp.status_code}")
                return False
        except Exception as e:
            self.log(f"❌ Ошибка соединения: {e}")
            return False

        self.log("⏳ Ожидание готовности отчёта (до 30 попыток)...")
        download_url = f"https://seller-analytics-api.wildberries.ru/api/v2/nm-report/downloads/file/{report_id}"
        for attempt in range(1, 31):
            time.sleep(30)
            try:
                resp = requests.get(download_url, headers=headers, stream=True, timeout=120)
                if resp.status_code == 200:
                    self.log("✅ Отчёт готов, скачиваю...")
                    zip_data = io.BytesIO(resp.content)
                    with zipfile.ZipFile(zip_data, 'r') as zf:
                        for name in zf.namelist():
                            with zf.open(name) as f:
                                content = f.read()
                                for enc in ['utf-8', 'utf-8-sig', 'cp1251', 'windows-1251']:
                                    try:
                                        text = content.decode(enc)
                                        break
                                    except:
                                        continue
                                else:
                                    self.log("⚠️ Не удалось декодировать файл")
                                    continue
                                for sep in [',', ';', '\t']:
                                    try:
                                        df = pd.read_csv(io.StringIO(text), delimiter=sep)
                                        if len(df.columns) > 1:
                                            break
                                    except:
                                        continue
                                else:
                                    self.log("⚠️ Не удалось прочитать CSV")
                                    continue
                                df['store'] = store_name
                                if 'dt' in df.columns:
                                    df['dt'] = pd.to_datetime(df['dt']).dt.strftime('%Y-%m-%d')
                                self._last_save_start = time.time()
                                self.s3.write_excel(key, df, sheet_name=config['name'])
                                self.log(f"✅ Воронка продаж сохранена: {key}")
                                return True
                elif resp.status_code == 202:
                    self.log(f"⏳ Отчёт ещё не готов, попытка {attempt}/30")
                else:
                    self.log(f"⚠️ Статус {resp.status_code}")
            except Exception as e:
                self.log(f"⚠️ Ошибка при скачивании: {e}")

        self.log("❌ Не удалось получить отчёт воронки")
        return False

    def _safe_div(self, numerator: float, denominator: float, multiplier: float = 1.0, digits: int = 2) -> float:
        try:
            denominator = float(denominator)
            if denominator == 0:
                return 0.0
            return round((float(numerator) / denominator) * multiplier, digits)
        except Exception:
            return 0.0

    def _normalize_cost_dataframe(self, cost_df: pd.DataFrame) -> pd.DataFrame:
        if cost_df.empty:
            return pd.DataFrame(columns=['nm_id', 'cost_price'])
        df = cost_df.copy()
        original_columns = list(df.columns)
        normalized = {col: str(col).strip().lower().replace('ё', 'е') for col in df.columns}
        nm_col = None
        cost_col = None
        for col, norm in normalized.items():
            if norm in ['nm_id', 'nmid', 'артикул wb', 'артикул', 'id товара'] or 'артикул wb' in norm:
                nm_col = col
                break
        for col, norm in normalized.items():
            if norm in ['cost_price', 'себестоимость', 'cost', 'cost price', 'закупочная цена'] or 'себестоим' in norm:
                cost_col = col
                break
        if nm_col is None and len(original_columns) >= 3:
            nm_col = original_columns[2]
        if cost_col is None:
            for col, norm in normalized.items():
                if 'себестоим' in norm or norm.startswith('cost'):
                    cost_col = col
                    break
        if nm_col is None or cost_col is None:
            self.log(f"⚠️ Не удалось определить колонки себестоимости. Найдены колонки: {original_columns}")
            return pd.DataFrame(columns=['nm_id', 'cost_price'])
        df = df.rename(columns={nm_col: 'nm_id', cost_col: 'cost_price'})
        df['nm_id'] = pd.to_numeric(df['nm_id'], errors='coerce')
        df['cost_price'] = pd.to_numeric(df['cost_price'], errors='coerce')
        df = df[['nm_id', 'cost_price']].dropna(subset=['nm_id'])
        df['nm_id'] = df['nm_id'].astype('int64')
        df['cost_price'] = df['cost_price'].fillna(0.0)
        return df.drop_duplicates(subset='nm_id', keep='last')

    def _build_campaigns_dataframe(self, adverts: List[Dict[str, Any]]) -> pd.DataFrame:
        rows = []
        current_date = datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d')
        for advert in adverts:
            settings = advert.get('settings', {}) or {}
            timestamps = advert.get('timestamps', {}) or {}
            placements = settings.get('placements', {}) or {}
            nm_settings = advert.get('nm_settings', []) or []
            first_nm = nm_settings[0] if nm_settings else {}
            bids_kopecks = first_nm.get('bids_kopecks', {}) or {}
            subject = first_nm.get('subject', {}) or {}
            rows.append({
                'ID': advert.get('id'),
                'Название': settings.get('name', ''),
                'Статус': 'Активна' if advert.get('status') == 9 else 'На паузе' if advert.get('status') == 11 else advert.get('status', ''),
                'Тип оплаты': settings.get('payment_type', ''),
                'Тип ставки': advert.get('bid_type', ''),
                'Создана': timestamps.get('created', ''),
                'Обновлена': timestamps.get('updated', ''),
                'Запущена': timestamps.get('started', ''),
                'Размещение в поиске': 'Да' if placements.get('search') is True else 'Нет',
                'Размещение в рекомендациях': 'Да' if placements.get('recommendations') is True else 'Нет',
                'Ставка в поиске (руб)': round((bids_kopecks.get('search', 0) or 0) / 100, 2),
                'Ставка в рекомендациях (руб)': round((bids_kopecks.get('recommendations', 0) or 0) / 100, 2),
                'ID предмета': subject.get('id', ''),
                'Название предмета': subject.get('name', ''),
                'Артикул WB': first_nm.get('nm_id', ''),
                'Количество товаров': len(nm_settings),
                'Первый артикул товара': first_nm.get('nm_id', ''),
                'Дата_сбора': current_date
            })
        return pd.DataFrame(rows)

    def _build_adverts_summary_dataframe(self, all_stats: List[Dict[str, Any]], campaign_info: Dict[int, Dict[str, Any]]) -> pd.DataFrame:
        rows = []
        for campaign in all_stats:
            campaign_id = campaign.get('advertId')
            info = campaign_info.get(campaign_id, {})
            sum_price = campaign.get('sum_price', 0) or 0
            spent = campaign.get('sum', 0) or 0
            rows.append({
                'ID кампании': campaign_id,
                'Артикул WB': info.get('article', ''),
                'Название предмета': info.get('subject', ''),
                'Название': info.get('name', ''),
                'Показы': campaign.get('views', 0),
                'Клики': campaign.get('clicks', 0),
                'CTR': campaign.get('ctr', 0),
                'CPC': campaign.get('cpc', 0),
                'Заказы': campaign.get('orders', 0),
                'CR': campaign.get('cr', 0),
                'Расход': spent,
                'ATBS': campaign.get('atbs', 0),
                'SHKS': campaign.get('shks', 0),
                'Сумма заказов': sum_price,
                'Отменено': campaign.get('canceled', 0),
                'ДРР': self._safe_div(spent, sum_price * 0.88 if sum_price else 0, 100, 2)
            })
        return pd.DataFrame(rows)

    def _build_category_reports(self, daily_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        if daily_df.empty or 'Название предмета' not in daily_df.columns:
            return pd.DataFrame(), pd.DataFrame()
        daily_category = daily_df.groupby(['Дата', 'Название предмета'], dropna=False).agg({'Показы':'sum','Клики':'sum','Заказы':'sum','Расход':'sum','Сумма заказов':'sum'}).reset_index()
        daily_category['CTR'] = daily_category.apply(lambda x: self._safe_div(x['Клики'], x['Показы'], 100, 2), axis=1)
        daily_category['CPC'] = daily_category.apply(lambda x: self._safe_div(x['Расход'], x['Клики'], 1, 2), axis=1)
        daily_category['CR'] = daily_category.apply(lambda x: self._safe_div(x['Заказы'], x['Клики'], 100, 2), axis=1)
        daily_category['ROI'] = daily_category.apply(lambda x: self._safe_div(x['Сумма заказов'] - x['Расход'], x['Расход'], 100, 2), axis=1)
        daily_category['ДРР'] = daily_category.apply(lambda x: self._safe_div(x['Расход'], x['Сумма заказов'] * 0.88 if x['Сумма заказов'] else 0, 100, 2), axis=1)
        daily_category = daily_category[['Дата','Название предмета','Показы','Клики','CTR','CPC','Заказы','CR','Расход','Сумма заказов','ROI','ДРР']].sort_values(['Дата','Расход'], ascending=[True,False])
        summary_category = daily_df.groupby('Название предмета', dropna=False).agg({'Показы':'sum','Клики':'sum','Заказы':'sum','Расход':'sum','Сумма заказов':'sum'}).reset_index()
        summary_category['CTR'] = summary_category.apply(lambda x: self._safe_div(x['Клики'], x['Показы'], 100, 2), axis=1)
        summary_category['CPC'] = summary_category.apply(lambda x: self._safe_div(x['Расход'], x['Клики'], 1, 2), axis=1)
        summary_category['CR'] = summary_category.apply(lambda x: self._safe_div(x['Заказы'], x['Клики'], 100, 2), axis=1)
        summary_category['ROI'] = summary_category.apply(lambda x: self._safe_div(x['Сумма заказов'] - x['Расход'], x['Расход'], 100, 2), axis=1)
        summary_category['ДРР'] = summary_category.apply(lambda x: self._safe_div(x['Расход'], x['Сумма заказов'] * 0.88 if x['Сумма заказов'] else 0, 100, 2), axis=1)
        summary_category = summary_category[['Название предмета','Показы','Клики','CTR','CPC','Заказы','CR','Расход','Сумма заказов','ROI','ДРР']].sort_values('Расход', ascending=False)
        return daily_category, summary_category

    def _update_adverts_history_14_days(self, store_name: str, daily_df: pd.DataFrame):
        if daily_df.empty:
            return
        history_key = f"Отчёты/Реклама/{store_name}/Реклама_14_дней_история.xlsx"
        history_df = self.s3.read_excel(history_key, sheet_name=0)
        request_date = datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d')
        start_14, end_14 = self._get_date_range_last_n_days(14)
        part = daily_df.copy()
        part['Дата'] = pd.to_datetime(part['Дата'], errors='coerce').dt.date
        part = part[(part['Дата'] >= start_14) & (part['Дата'] <= end_14)].copy()
        part['Дата'] = part['Дата'].astype(str)
        part['Дата запроса'] = request_date
        if history_df.empty:
            combined = part
        else:
            if 'Дата' in history_df.columns:
                history_df['Дата'] = pd.to_datetime(history_df['Дата'], errors='coerce').dt.date.astype(str)
            combined = pd.concat([history_df, part], ignore_index=True)
            dedupe_cols = [col for col in ['Дата запроса','ID кампании','Артикул WB','Дата'] if col in combined.columns]
            if dedupe_cols:
                combined = combined.drop_duplicates(subset=dedupe_cols, keep='last')
        self.s3.write_excel(history_key, combined, sheet_name='История_14_дней')
        self.log(f"📊 История рекламы за 14 дней сохранена: {history_key}, записей: {len(combined)}")

    # ---------- Реклама (получение данных напрямую из API) ----------
    def update_adverts(self, store_name: str) -> bool:
        """
        Обновление данных по рекламным кампаниям.
        Формирует недельные файлы с листами:
        Статистика_Ежедневно, Список_кампаний, Статистика_Итого,
        Отчет_по_Категории, Отчет_по_Категории_Итог.
        Также ведёт отдельную историю за последние 14 дней с датой запроса.
        """
        self.log(f"\n📌 ОБНОВЛЕНИЕ: Реклама для магазина {store_name}")
        config = self.reports_config['adverts']
        api_key = self.api_keys[store_name][config['key_type']]
        headers = {"Authorization": f"Bearer {api_key.strip()}"}

        self.log("📋 Запрос списка рекламных кампаний...")
        all_adverts = []
        for payment_type in ['cpm', 'cpc']:
            url = f"{config['api_url']}?statuses=9,11&payment_type={payment_type}"
            try:
                resp = requests.get(url, headers=headers, timeout=30)
                if resp.status_code == 200:
                    adverts = (resp.json() or {}).get('adverts', [])
                    all_adverts.extend(adverts)
                    self.log(f"✅ Получено кампаний для {payment_type}: {len(adverts)}")
                else:
                    self.log(f"⚠️ Не удалось получить список кампаний для {payment_type}: {resp.status_code}")
                time.sleep(0.5)
            except Exception as e:
                self.log(f"❌ Ошибка при запросе кампаний: {e}")
                return False

        if not all_adverts:
            self.log("❌ Не получено ни одной кампании. Отчёт пропущен.")
            return False

        self.log(f"✅ Всего получено кампаний: {len(all_adverts)}")
        campaigns_df = self._build_campaigns_dataframe(all_adverts)

        campaign_ids = []
        campaign_info = {}
        for adv in all_adverts:
            adv_id = adv.get('id')
            if not adv_id:
                continue
            settings = adv.get('settings', {}) or {}
            nm_settings = adv.get('nm_settings', []) or []
            first_nm = nm_settings[0] if nm_settings else {}
            subject_obj = first_nm.get('subject', {}) or {}
            campaign_info[adv_id] = {'name': settings.get('name', ''), 'subject': subject_obj.get('name', ''), 'article': first_nm.get('nm_id', '')}
            campaign_ids.append(adv_id)

        self.log(f"📊 Получено {len(campaign_ids)} кампаний с информацией")

        end_date = (datetime.now(pytz.timezone('Europe/Moscow')) - timedelta(days=1)).date()
        start_date = end_date - timedelta(days=29)
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')
        self.log(f"📅 Запрашиваем статистику за период: {start_str} - {end_str}")

        weeks = defaultdict(list)
        for d in [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]:
            week_start = self._get_week_start(datetime.combine(d, datetime.min.time()))
            weeks[week_start].append(d)

        all_stats = []
        stats_url = "https://advert-api.wildberries.ru/adv/v3/fullstats"
        for i in range(0, len(campaign_ids), 30):
            chunk = campaign_ids[i:i+30]
            params = {'ids': ','.join(map(str, chunk)), 'beginDate': start_str, 'endDate': end_str}
            retries = 0
            while retries < 5:
                try:
                    self.log(f"⏳ Запрос статистики для кампаний {i+1}-{min(i+30, len(campaign_ids))}...")
                    resp = requests.get(stats_url, headers=headers, params=params, timeout=60)
                    if resp.status_code == 200:
                        data = resp.json() or []
                        all_stats.extend(data)
                        self.log(f"✅ Получены данные для {len(data)} кампаний")
                        break
                    elif resp.status_code == 429:
                        retries += 1
                        wait = 60 * retries
                        self.log(f"⚠️ Лимит API, ожидание {wait} сек...")
                        time.sleep(wait)
                    else:
                        self.log(f"❌ Ошибка {resp.status_code}: {resp.text[:200]}")
                        break
                except Exception as e:
                    retries += 1
                    self.log(f"❌ Ошибка запроса статистики: {e}")
                    time.sleep(15)
            time.sleep(30)

        if not all_stats:
            self.log("⚠️ Не получено статистических данных.")
            return False

        daily_rows = []
        for camp in all_stats:
            camp_id = camp.get('advertId')
            if not camp_id:
                continue
            info = campaign_info.get(camp_id, {})
            for day in camp.get('days', []) or []:
                day_date = str(day.get('date', '')).split('T')[0]
                if not day_date or day_date < start_str or day_date > end_str:
                    continue
                spent = day.get('sum', 0) or 0
                sum_price = day.get('sum_price', 0) or 0
                daily_rows.append({
                    'ID кампании': camp_id,
                    'Артикул WB': info.get('article', ''),
                    'Название': info.get('name', ''),
                    'Название предмета': info.get('subject', ''),
                    'Дата': day_date,
                    'Показы': day.get('views', 0),
                    'Клики': day.get('clicks', 0),
                    'CTR': day.get('ctr', 0),
                    'CPC': day.get('cpc', 0),
                    'Заказы': day.get('orders', 0),
                    'CR': day.get('cr', 0),
                    'Расход': spent,
                    'ATBS': day.get('atbs', 0),
                    'SHKS': day.get('shks', 0),
                    'Сумма заказов': sum_price,
                    'Отменено': day.get('canceled', 0),
                    'ДРР': self._safe_div(spent, sum_price * 0.88 if sum_price else 0, 100, 2)
                })

        if not daily_rows:
            self.log("⚠️ Нет ежедневных данных для сохранения.")
            return False

        daily_df = pd.DataFrame(daily_rows)
        daily_df['Дата'] = pd.to_datetime(daily_df['Дата'], errors='coerce').dt.date.astype(str)
        self.log(f"📊 Сформировано {len(daily_df)} ежедневных записей")

        summary_df = self._build_adverts_summary_dataframe(all_stats, campaign_info)
        category_daily_df, category_total_df = self._build_category_reports(daily_df)

        for week_start, dates in weeks.items():
            week_dates = [d.strftime('%Y-%m-%d') for d in dates]
            week_daily_df = daily_df[daily_df['Дата'].isin(week_dates)].copy()
            if week_daily_df.empty:
                continue
            existing_key = self._get_weekly_key(store_name, 'adverts', week_start)
            existing_sheets = self.s3.read_excel_all_sheets(existing_key) if self.s3.file_exists(existing_key) else {}
            existing_daily_df = existing_sheets.get('Статистика_Ежедневно', pd.DataFrame())
            if existing_daily_df.empty and existing_sheets:
                existing_daily_df = existing_sheets.get(next(iter(existing_sheets.keys())), pd.DataFrame())
            if not existing_daily_df.empty:
                existing_daily_df['Дата'] = pd.to_datetime(existing_daily_df['Дата'], errors='coerce').dt.date.astype(str)
                week_daily_df = pd.concat([existing_daily_df, week_daily_df], ignore_index=True)
                week_daily_df = week_daily_df.drop_duplicates(subset=['ID кампании', 'Дата', 'Артикул WB'], keep='last')
            week_summary_df = self._build_adverts_summary_dataframe([camp for camp in all_stats if camp.get('advertId') in set(week_daily_df['ID кампании'].unique())], campaign_info)
            week_category_daily_df, week_category_total_df = self._build_category_reports(week_daily_df)
            week_campaigns_df = campaigns_df[campaigns_df['ID'].isin(week_daily_df['ID кампании'].unique())].copy() if not campaigns_df.empty else pd.DataFrame()
            sheets_to_write = {
                'Статистика_Ежедневно': week_daily_df.sort_values(['Дата','ID кампании']).reset_index(drop=True),
                'Список_кампаний': week_campaigns_df.reset_index(drop=True),
                'Статистика_Итого': week_summary_df.reset_index(drop=True),
                'Отчет_по_Категории': week_category_daily_df.reset_index(drop=True),
                'Отчет_по_Категории_Итог': week_category_total_df.reset_index(drop=True)
            }
            self.s3.write_excel_sheets(existing_key, sheets_to_write)
            self.log(f"✅ Недельный файл сохранён: {existing_key}, листов: {list(sheets_to_write.keys())}")

        analytics_key = f"Отчёты/{config['folder']}/{store_name}/Анализ рекламы.xlsx"
        self.s3.write_excel_sheets(analytics_key, {
            'Статистика_Ежедневно': daily_df,
            'Список_кампаний': campaigns_df,
            'Статистика_Итого': summary_df,
            'Отчет_по_Категории': category_daily_df,
            'Отчет_по_Категории_Итог': category_total_df
        })
        self.log(f"📊 Аналитический отчёт сохранён: {analytics_key}")
        self._update_adverts_history_14_days(store_name, daily_df)
        self.log("✅ Реклама успешно обновлена")
        return True

    # ---------- Остатки из 1С (отключено, метод оставлен для возможности возврата) ----------
    def update_1c_stocks(self, store_name: str = '1С') -> bool:
        self.log(f"\n📌 ОБНОВЛЕНИЕ: Остатки из 1С для магазина {store_name}")
        config = self.reports_config['1c_stocks']

        url_1c = os.environ.get('URL_1C_STOCKS')
        username = os.environ.get('_1C_USER')
        password = os.environ.get('_1C_PASSWORD')

        if not url_1c:
            self.log("❌ Переменная окружения URL_1C_STOCKS не задана. Пропускаем.")
            return False

        auth = None
        if username and password:
            auth = (username, password)
            self.log(f"🔐 Используется базовая аутентификация для пользователя {username}")

        google_match = re.search(r'docs\.google\.com/spreadsheets/d/([a-zA-Z0-9_-]+)(?:/.*?gid=(\d+))?', url_1c)
        if google_match:
            spreadsheet_id = google_match.group(1)
            gid = google_match.group(2)
            if not gid:
                self.log("❌ В ссылке на Google Sheets не найден параметр gid. Укажите ссылку на конкретный лист.")
                return False
            download_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=xlsx&gid={gid}"
            self.log(f"📄 Обнаружена Google Sheets, gid={gid}. Будет скачан лист с этим gid.")
        else:
            download_url = url_1c
            self.log("📄 Используется прямая ссылка на файл.")

        tmp_path = None
        try:
            self.log(f"📥 Скачивание файла из: {download_url}")
            resp = requests.get(download_url, auth=auth, timeout=120, stream=True, allow_redirects=True)
            if resp.status_code != 200:
                self.log(f"❌ Ошибка при скачивании: HTTP {resp.status_code}")
                return False

            with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
                tmp_path = tmp.name
                for chunk in resp.iter_content(chunk_size=8192):
                    tmp.write(chunk)
            self.log(f"📦 Файл временно сохранён: {tmp_path}")

            key = f"Отчёты/{config['folder']}/{store_name}/{config['filename']}"
            self.log(f"☁️ Загрузка в бакет: {key}")
            self.s3.upload_file(tmp_path, key)
            self.log(f"✅ Файл успешно сохранён в бакет: {key}")

            return True

        except Exception as e:
            self.log(f"❌ Исключение при обработке: {e}")
            traceback.print_exc()
            return False
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)
                self.log("🧹 Временный файл удалён")

    # ---------- Единый объединённый отчёт ----------
    def create_unified_report(self, store_name: str) -> bool:
        """
        Формирует единый отчёт из данных воронки продаж, заказов и рекламы за последние 90 дней.
        Использует union дат/товаров по всем источникам, чтобы не терять дни, если один источник отстаёт.
        """
        self.log(f"\n📌 ФОРМИРОВАНИЕ ЕДИНОГО ОТЧЁТА для магазина {store_name}")
        start_date, end_date = self._get_date_range_90_days()
        self.log(f"📅 Период данных: {start_date} - {end_date}")
        funnel_key = f"Отчёты/{self.reports_config['funnel']['folder']}/{store_name}/{self.reports_config['funnel']['filename']}"
        self.log(f"📥 Загрузка воронки продаж: {funnel_key}")
        funnel_df = self.s3.read_excel(funnel_key, sheet_name=0)
        if funnel_df.empty:
            self.log("⚠️ Воронка продаж не загружена. Продолжаем без неё.")
            funnel_df = pd.DataFrame(columns=['nmID','dt'])
        else:
            funnel_df['dt'] = pd.to_datetime(funnel_df['dt'], errors='coerce').dt.date
            funnel_df = funnel_df[(funnel_df['dt'] >= start_date) & (funnel_df['dt'] <= end_date)].copy()
            self.log(f"📊 Воронка продаж: {len(funnel_df)} записей")

        orders_files = self.s3.list_files(f"Отчёты/{self.reports_config['orders']['folder']}/{store_name}/Недельные/")
        orders_list = []
        for file_key in orders_files:
            self.log(f"📥 Загрузка заказов: {file_key}")
            df = self.s3.read_excel(file_key, sheet_name=0)
            if df.empty:
                continue
            df = df.rename(columns={'nmId':'nmID','date':'dt'})
            if 'nmID' not in df.columns or 'dt' not in df.columns:
                continue
            df['dt'] = pd.to_datetime(df['dt'], errors='coerce').dt.date
            df = df[(df['dt'] >= start_date) & (df['dt'] <= end_date)].copy()
            orders_list.append(df)
        orders_df = pd.concat(orders_list, ignore_index=True) if orders_list else pd.DataFrame()
        if not orders_df.empty:
            self.log(f"📊 Заказы: {len(orders_df)} записей")

        adverts_files = self.s3.list_files(f"Отчёты/{self.reports_config['adverts']['folder']}/{store_name}/Недельные/")
        adverts_list = []
        for file_key in adverts_files:
            self.log(f"📥 Загрузка рекламы: {file_key}")
            sheets = self.s3.read_excel_all_sheets(file_key)
            df = sheets.get('Статистика_Ежедневно', pd.DataFrame()) if sheets else pd.DataFrame()
            if df.empty:
                df = self.s3.read_excel(file_key, sheet_name=0)
            if df.empty:
                continue
            df = df.rename(columns={'Артикул WB':'nmID','Дата':'dt'})
            if 'nmID' not in df.columns or 'dt' not in df.columns:
                continue
            df['dt'] = pd.to_datetime(df['dt'], errors='coerce').dt.date
            df = df[(df['dt'] >= start_date) & (df['dt'] <= end_date)].copy()
            adverts_list.append(df)
        adverts_df = pd.concat(adverts_list, ignore_index=True) if adverts_list else pd.DataFrame()
        if not adverts_df.empty:
            self.log(f"📊 Реклама: {len(adverts_df)} записей")

        if funnel_df.empty and orders_df.empty and adverts_df.empty:
            self.log("❌ Нет данных для формирования отчёта")
            return False

        orders_grouped = pd.DataFrame(columns=['nmID','dt'])
        if not orders_df.empty:
            agg = {}
            if 'priceWithDisc' in orders_df.columns: agg['priceWithDisc'] = 'sum'
            if 'spp' in orders_df.columns: agg['spp'] = 'mean'
            if 'finishedPrice' in orders_df.columns: agg['finishedPrice'] = 'mean'
            if 'subject' in orders_df.columns: agg['subject'] = lambda x: x.mode().iloc[0] if not x.mode().empty else None
            if 'supplierArticle' in orders_df.columns: agg['supplierArticle'] = lambda x: x.mode().iloc[0] if not x.mode().empty else None
            if agg:
                orders_grouped = orders_df.groupby(['nmID','dt']).agg(agg).reset_index().rename(columns={'priceWithDisc':'total_priceWithDisc','spp':'avg_spp','finishedPrice':'avg_finishedPrice'})
                self.log(f"📊 Заказы сгруппированы: {len(orders_grouped)} записей")

        adverts_grouped = pd.DataFrame(columns=['nmID','dt'])
        if not adverts_df.empty:
            cols = {k:v for k,v in {'Расход':'sum','Показы':'sum','Клики':'sum','Заказы':'sum','Сумма заказов':'sum'}.items() if k in adverts_df.columns}
            if cols:
                adverts_grouped = adverts_df.groupby(['nmID','dt']).agg(cols).reset_index()
                adverts_grouped['CTR РК'] = adverts_grouped.apply(lambda x: self._safe_div(x.get('Клики',0), x.get('Показы',0), 100, 2), axis=1)
                adverts_grouped['CPC (цена клика)'] = adverts_grouped.apply(lambda x: self._safe_div(x.get('Расход',0), x.get('Клики',0), 1, 2), axis=1)
                self.log(f"📊 Реклама сгруппирована: {len(adverts_grouped)} записей")

        bases = []
        for df in [funnel_df, orders_grouped, adverts_grouped]:
            if not df.empty and 'nmID' in df.columns and 'dt' in df.columns:
                bases.append(df[['nmID','dt']].drop_duplicates())
        merged = pd.concat(bases, ignore_index=True).drop_duplicates() if bases else pd.DataFrame(columns=['nmID','dt'])
        if not funnel_df.empty:
            merged = merged.merge(funnel_df, on=['nmID','dt'], how='left')
        if not orders_grouped.empty:
            merged = merged.merge(orders_grouped, on=['nmID','dt'], how='left')
        if not adverts_grouped.empty:
            merged = merged.merge(adverts_grouped, on=['nmID','dt'], how='left')
        for col in merged.columns:
            if col not in ['nmID','dt','subject','supplierArticle','brand']:
                merged[col] = merged[col].fillna(0)
        merged['dt'] = pd.to_datetime(merged['dt'], errors='coerce').dt.strftime('%Y-%m-%d')
        merged = merged.sort_values(['dt','nmID']).reset_index(drop=True)
        self.log(f"📊 Объединено записей: {len(merged)}")
        merged = merged.rename(columns={
            'nmID':'ID товара','dt':'Дата','subject':'Категория','supplierArticle':'Артикул поставщика','brand':'Бренд',
            'openCardCount':'Открытия карточки','addToCartCount':'Добавления в корзину','ordersCount':'Количество заказов',
            'cartToOrderCount':'Конверсия корзина-заказ','total_priceWithDisc':'Сумма заказов (со скидкой)',
            'avg_spp':'Средняя СПП','avg_finishedPrice':'Средняя конечная цена','viewCount':'Просмотры','clickCount':'Клики',
            'ctr':'CTR','conversion':'Конверсия','Расход':'Расход на рекламу','Показы':'Показы РК','Клики':'Клики РК',
            'Заказы':'Заказы из рекламы','Сумма заказов':'Сумма заказов из рекламы'
        })
        output_key = f"Отчёты/Объединенный отчет/{store_name}/Объединенный_отчет.xlsx"
        self.s3.write_excel(output_key, merged, sheet_name='Объединенный отчет')
        self.log(f"✅ Единый отчёт сохранён: {output_key}")
        return True

    # ====================== РАСЧЁТ ЭКОНОМИКИ ======================
    def calculate_economics(self, store_name: str):
        """
        Расчёт экономических показателей для последней полной недели.
        Устойчив к плавающей структуре файла себестоимости.
        """
        self.log_section("РАСЧЁТ ЭКОНОМИКИ")
        today = datetime.now(pytz.timezone('Europe/Moscow')).date()
        yesterday = today - timedelta(days=1)
        days_since_sunday = (yesterday.weekday() + 1) % 7
        end_of_last_week = yesterday - timedelta(days=days_since_sunday)
        start_of_last_week = end_of_last_week - timedelta(days=6)
        week_number = f"{start_of_last_week.isocalendar()[0]}-W{start_of_last_week.isocalendar()[1]:02d}"
        self.log(f"📅 Неделя расчёта: {week_number} ({start_of_last_week} - {end_of_last_week})")

        finance_key = f"Отчёты/Финансовые показатели/{store_name}/Недельные/Финансовые показатели_{week_number}.xlsx"
        finance_df = self.s3.read_excel(finance_key, sheet_name=0)
        if finance_df.empty:
            self.log(f"❌ Финансовый отчёт за неделю {week_number} не найден. Расчёт отменён.")
            return
        finance_df['nm_id'] = pd.to_numeric(finance_df.get('nm_id'), errors='coerce')
        finance_df = finance_df.dropna(subset=['nm_id']).copy()
        finance_df['nm_id'] = finance_df['nm_id'].astype('int64')

        stocks_key = f"Отчёты/Остатки/{store_name}/Недельные/Остатки_{week_number}.xlsx"
        stocks_df = self.s3.read_excel(stocks_key, sheet_name=0)
        if stocks_df.empty:
            self.log(f"⚠️ Остатки за неделю {week_number} не найдены. Хранение не будет распределено.")
            stocks_start = pd.DataFrame()
        else:
            stocks_df['Дата запроса'] = pd.to_datetime(stocks_df['Дата запроса'], errors='coerce').dt.date
            stocks_start = stocks_df[stocks_df['Дата запроса'] == start_of_last_week].copy()
            if stocks_start.empty:
                self.log(f"⚠️ Нет остатков на дату {start_of_last_week}. Хранение не распределено.")

        raw_cost_df = self.s3.read_excel("Отчёты/Себестоимость/Себестоимость.xlsx", sheet_name=0)
        cost_df = self._normalize_cost_dataframe(raw_cost_df) if not raw_cost_df.empty else pd.DataFrame(columns=['nm_id','cost_price'])
        if cost_df.empty:
            self.log("⚠️ Себестоимость не найдена или не распознана. Продолжаем с нулевой себестоимостью.")

        advert_key = f"Отчёты/Реклама/{store_name}/Недельные/Реклама_{week_number}.xlsx"
        advert_sheets = self.s3.read_excel_all_sheets(advert_key)
        advert_df = advert_sheets.get('Статистика_Ежедневно', pd.DataFrame()) if advert_sheets else pd.DataFrame()
        if advert_df.empty:
            advert_df = self.s3.read_excel(advert_key, sheet_name=0)
        if not advert_df.empty and 'Артикул WB' in advert_df.columns and 'Расход' in advert_df.columns:
            advert_df['Артикул WB'] = pd.to_numeric(advert_df['Артикул WB'], errors='coerce')
            advert_agg = advert_df.dropna(subset=['Артикул WB']).groupby('Артикул WB')['Расход'].sum().reset_index().rename(columns={'Артикул WB':'nm_id','Расход':'advert_cost'})
            advert_agg['nm_id'] = advert_agg['nm_id'].astype('int64')
        else:
            self.log(f"ℹ️ Нет данных по рекламе за неделю {week_number}.")
            advert_agg = pd.DataFrame(columns=['nm_id','advert_cost'])

        expense_types = ['Логистика','Возмещение издержек по перевозке/по складским операциям с товаром','Штрафы','Платная приёмка','Услуги WB Продвижения']
        results = []
        for nm_id, group in finance_df.groupby('nm_id'):
            sales = group[group['doc_type_name'] == 'Продажа']
            returns = group[group['doc_type_name'] == 'Возврат']
            compensations = group[group['doc_type_name'] == 'Компенсация скидки по программе лояльности']
            expenses = group[group['doc_type_name'].isin(expense_types)]
            qty_sold = sales['quantity'].sum() - returns['quantity'].sum()
            if qty_sold <= 0:
                continue
            revenue = sales['retail_amount'].sum() - returns['retail_amount'].sum()
            cash_flow = sales['ppvz_for_pay'].sum() - returns['ppvz_for_pay'].sum() + compensations['ppvz_for_pay'].sum() + expenses['ppvz_for_pay'].sum()
            cost_row = cost_df[cost_df['nm_id'] == nm_id]
            cost_price = float(cost_row.iloc[0]['cost_price']) if not cost_row.empty else 0.0
            total_cost = qty_sold * cost_price
            if cost_row.empty:
                self.log(f"⚠️ Для артикула {nm_id} нет себестоимости. Использую 0.")
            adv_row = advert_agg[advert_agg['nm_id'] == nm_id]
            advert_cost = float(adv_row.iloc[0]['advert_cost']) if not adv_row.empty else 0.0
            if not stocks_start.empty and 'Артикул WB' in stocks_start.columns and 'Доступно для продажи' in stocks_start.columns:
                stocks_start['Артикул WB'] = pd.to_numeric(stocks_start['Артикул WB'], errors='coerce')
                total_stock_all = pd.to_numeric(stocks_start['Доступно для продажи'], errors='coerce').fillna(0).sum()
                sku_stock = pd.to_numeric(stocks_start[stocks_start['Артикул WB'] == nm_id]['Доступно для продажи'], errors='coerce').fillna(0).sum()
                storage_fee_rows = group[group['doc_type_name'].astype(str).str.contains('Хранение', na=False)]
                total_storage_cost = abs(storage_fee_rows['ppvz_for_pay'].sum()) if not storage_fee_rows.empty else 0
                storage_cost = (sku_stock / total_stock_all) * total_storage_cost if total_stock_all > 0 and total_storage_cost > 0 else 0
            else:
                storage_cost = 0
            nds = revenue * 0.07
            profit_base = cash_flow - total_cost - storage_cost - advert_cost
            profit_tax = max(0, profit_base) * 0.15
            gross_profit = profit_base - nds - profit_tax
            if not sales.empty:
                sales_sorted = sales.sort_values('sale_dt', ascending=False)
                last_commission = sales_sorted.iloc[0].get('commission_percent', 0)
                last_acquiring = sales_sorted.iloc[0].get('acquiring_percent', 0)
                avg_price = revenue / qty_sold
                avg_logistics = abs(expenses[expenses['doc_type_name'].isin(['Логистика','Возмещение издержек по перевозке/по складским операциям с товаром'])]['ppvz_for_pay'].sum()) / qty_sold
                avg_storage = storage_cost / qty_sold
                avg_advert = advert_cost / qty_sold
                avg_tax = (nds + profit_tax) / qty_sold
                avg_profit = gross_profit / qty_sold
            else:
                last_commission = last_acquiring = avg_price = avg_logistics = avg_storage = avg_advert = avg_tax = avg_profit = 0
            results.append({'nm_id':nm_id,'week':week_number,'revenue':round(revenue,2),'cash_flow':round(cash_flow,2),'total_cost':round(total_cost,2),'storage_cost':round(storage_cost,2),'advert_cost':round(advert_cost,2),'nds':round(nds,2),'profit_tax':round(profit_tax,2),'gross_profit':round(gross_profit,2),'qty_sold':qty_sold,'avg_price':round(avg_price,2),'last_commission_percent':last_commission,'last_acquiring_percent':last_acquiring,'avg_logistics_per_unit':round(avg_logistics,2),'avg_storage_per_unit':round(avg_storage,2),'avg_advert_per_unit':round(avg_advert,2),'avg_tax_per_unit':round(avg_tax,2),'avg_profit_per_unit':round(avg_profit,2)})
        if not results:
            self.log("❌ Нет данных для формирования отчёта.")
            return
        gross_df = pd.DataFrame(results)[['nm_id','week','revenue','cash_flow','total_cost','storage_cost','advert_cost','nds','profit_tax','gross_profit']]
        unit_df = pd.DataFrame(results)[['nm_id','week','avg_price','last_commission_percent','last_acquiring_percent','avg_logistics_per_unit','avg_storage_per_unit','avg_advert_per_unit','avg_tax_per_unit','avg_profit_per_unit']]
        output_key = f"Отчёты/Финансовые показатели/{store_name}/Экономика.xlsx"
        self.s3.write_excel_sheets(output_key, {'Валовая прибыль': gross_df, 'Юнит экономика': unit_df})
        self.log(f"✅ Экономика сохранена: {output_key}")
        self.log("✅ Расчёт экономики завершён.")

    # ====================== ОСНОВНОЙ ЗАПУСК ======================
    def run_daily_update(self, store_name: str, reports: List[str] = None):
        # Исключаем 1c_stocks из списка по умолчанию (можно вернуть позже, добавив в список)
        all_reports = ['orders', 'stocks', 'finance', 'funnel', 'adverts', 'keywords']
        if reports is None:
            reports = all_reports

        self.log(f"🚀 Начало обновления для магазина {store_name}. Запрошенные отчёты: {reports}")
        for report in reports:
            self.log(f"➡️ Переход к отчёту: {report}")
            method_name = f"update_{report}"
            if hasattr(self, method_name):
                method = getattr(self, method_name)
                try:
                    success = method(store_name)
                    self.log(f"📊 Отчёт {report}: {'✅' if success else '❌'}")
                except Exception as e:
                    self.log(f"❌ Критическая ошибка в {report}: {e}")
                    traceback.print_exc()
                    self.log(f"📊 Отчёт {report}: ❌ (исключение)")
            else:
                self.log(f"⚠️ Неизвестный тип отчёта: {report}")
            if report != reports[-1]:
                self.log(f"⏳ Пауза 30 секунд перед следующим отчётом...")
                time.sleep(30)

        self.log_section("ФОРМИРОВАНИЕ ЕДИНОГО ОТЧЁТА")
        self.create_unified_report(store_name)

        self.log_section("РАСЧЁТ ЭКОНОМИКИ")
        self.calculate_economics(store_name)

        self.log("✅ Обновление завершено")

    def log_section(self, title: str):
        self.log("")
        self.log("=" * 80)
        self.log(f"📌 {title}")
        self.log("=" * 80)


# ========================== ТОЧКА ВХОДА ==========================

if __name__ == "__main__":
    required_env = [
        'YC_ACCESS_KEY_ID',
        'YC_SECRET_ACCESS_KEY',
        'YC_BUCKET_NAME',
        'WB_STATS_KEY_TOPFACE',
        'WB_PROMO_KEY_TOPFACE'
    ]
    missing = [var for var in required_env if not os.environ.get(var)]
    if missing:
        print(f"❌ Отсутствуют переменные окружения: {missing}")
        exit(1)

    s3 = S3Storage(
        access_key=os.environ['YC_ACCESS_KEY_ID'],
        secret_key=os.environ['YC_SECRET_ACCESS_KEY'],
        bucket_name=os.environ['YC_BUCKET_NAME']
    )

    api_keys = {
        'TOPFACE': {
            'stats': os.environ['WB_STATS_KEY_TOPFACE'],
            'promo': os.environ['WB_PROMO_KEY_TOPFACE']
        }
    }

    updater = WildberriesDailyUpdater(api_keys, s3)
    store = 'TOPFACE'
    updater.run_daily_update(store)
