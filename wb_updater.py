#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Ежедневный сбор данных Wildberries с сохранением в Yandex Cloud Object Storage.
Данные хранятся только в недельных файлах (кроме воронки продаж и 1С).
Автоматическое получение артикулов из заказов для отчёта по ключам.
Формат для keywords: Неделя ГГГГ-WНН.xlsx
Финансовые показатели: проверяется только последняя неделя.
Всегда читается первый лист в файле.
Добавлен механизм повторных попыток для неудачных комбинаций (дата, артикул, фильтр).
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
from typing import List, Dict, Optional, Tuple, Any, Set, Union
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
        """
        :param access_key: Access Key ID
        :param secret_key: Secret Access Key
        :param bucket_name: имя бакета
        """
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

    def read_excel(self, key: str, sheet_name: Optional[Union[int, str]] = None) -> pd.DataFrame:
        """Скачивает Excel-файл из бакета и читает его."""
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
        """Сохраняет DataFrame во временный файл и загружает в бакет."""
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
            tmp_path = tmp.name
        try:
            df.to_excel(tmp_path, index=False, sheet_name=sheet_name)
            self.upload_file(tmp_path, key)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def upload_file(self, local_path: str, key: str):
        """Загружает локальный файл в бакет."""
        self.s3.upload_file(local_path, self.bucket, key)

    def file_exists(self, key: str) -> bool:
        """Проверяет существование файла в бакете."""
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False

    def list_files(self, prefix: str) -> List[str]:
        """Возвращает список ключей (имён файлов) с заданным префиксом."""
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
    """
    Ежедневный сборщик данных Wildberries с хранением в S3 (Yandex Cloud).
    Данные хранятся только в недельных файлах (кроме воронки продаж и 1С).
    """

    def __init__(self, api_keys: Dict[str, Dict[str, str]], s3: S3Storage):
        """
        :param api_keys: словарь вида {'TOPFACE': {'stats': '...', 'promo': '...'}}
        :param s3: экземпляр S3Storage для работы с бакетом
        """
        self.api_keys = api_keys
        self.s3 = s3
        self.start_time = datetime.now(pytz.timezone('Europe/Moscow'))
        self.data_period_days = 90

        # Конфигурация отчётов (только метаданные, без общего файла)
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
        """
        Загружает данные из недельного файла за указанную неделю.
        Всегда читает первый лист. Выводит статистику по файлу.
        """
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

    # ---------- Финансовые показатели (оптимизировано) ----------
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

        # Проверка наличия файлов для остальных недель (без чтения)
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

    # ---------- Позиции по ключам (с расширенным логированием и повторными попытками) ----------
    def update_keywords(self, store_name: str) -> bool:
        self.log(f"\n📌 ОБНОВЛЕНИЕ: Позиции по ключам для магазина {store_name}")

        articles = self._get_articles_by_subjects(store_name, self.target_subjects)
        if not articles:
            self.log("⚠️ Не найдено артикулов из заказов. Отчёт будет пропущен.")
            return False

        self.log(f"📦 Будет обработано артикулов: {len(articles)}")

        start_date, end_date = self._get_date_range_90_days()
        all_dates = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]

        weeks = defaultdict(list)
        for d in all_dates:
            week_start = self._get_week_start(datetime.combine(d, datetime.min.time()))
            weeks[week_start].append(d)

        api_key = self.api_keys[store_name]['promo']
        headers = {"Authorization": f"Bearer {api_key.strip()}", "Content-Type": "application/json"}
        url = self.reports_config['keywords']['api_url']

        filters = ["orders", "openCard", "addToCart"]

        failed_combinations = []  # список (date_str, nm_id, filter_field)

        for week_start, dates in weeks.items():
            self.log(f"📅 Обработка недели, начинающейся {week_start.strftime('%Y-%m-%d')}")
            weekly_df = self._load_weekly_data(store_name, 'keywords', week_start)

            existing_keys = set()
            if not weekly_df.empty:
                # Приводим типы для единообразия
                weekly_df['Дата'] = weekly_df['Дата'].astype(str)
                weekly_df['Артикул WB'] = weekly_df['Артикул WB'].astype(int)
                for _, row in weekly_df.iterrows():
                    d = row['Дата']
                    nm = row['Артикул WB']
                    f = row['Фильтр']
                    existing_keys.add((d, nm, f))

                self.log(f"🔍 В недельном файле найдено {len(existing_keys)} уникальных комбинаций (дата, артикул, фильтр)")
                sample_keys = list(existing_keys)[:5]
                self.log(f"📋 Примеры существующих ключей: {sample_keys}")

            else:
                self.log(f"ℹ️ Недельный файл пуст или не существует")

            missing_by_date = defaultdict(list)
            for date in dates:
                date_str = date.strftime('%Y-%m-%d')
                for nm_id in articles:
                    missing_filters = []
                    for f in filters:
                        if (date_str, nm_id, f) not in existing_keys:
                            missing_filters.append(f)
                    if missing_filters:
                        missing_by_date[date_str].append(nm_id)
                        if len(missing_by_date[date_str]) <= 3:
                            self.log(f"❌ Для даты {date_str} артикул {nm_id} пропущены фильтры: {missing_filters}")

            if not missing_by_date:
                self.log(f"✅ Все данные за неделю уже загружены")
                continue

            self.log(f"📅 Недостающие дни: {list(missing_by_date.keys())}")
            total_missing = sum(len(v) for v in missing_by_date.values())
            self.log(f"📊 Всего пропущено комбинаций (дата, артикул): {total_missing}")

            new_data = []
            for date_str, nm_ids in missing_by_date.items():
                self.log(f"📅 Загрузка дня {date_str}, артикулов: {len(nm_ids)}")
                batches = [nm_ids[i:i+50] for i in range(0, len(nm_ids), 50)]
                for batch_idx, batch in enumerate(batches, 1):
                    self.log(f"  📦 Батч {batch_idx}/{len(batches)}: {len(batch)} артикулов")
                    batch_data = []
                    for filter_field in filters:
                        self.log(f"    🔍 Фильтр {filter_field}", end="")
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
                            # Запоминаем все артикулы в этом батче для этого фильтра как неудачные
                            for nm_id in batch:
                                failed_combinations.append((date_str, nm_id, filter_field))
                            self.log(f"    ❌ Не удалось загрузить фильтр {filter_field} для батча, добавлено {len(batch)} комбинаций в список ошибок")
                        if filter_field != filters[-1]:
                            time.sleep(30)

                    if batch_data:
                        batch_df = pd.DataFrame(batch_data)
                        new_data.append(batch_df)

                    if batch_idx < len(batches):
                        self.log("    ⏳ Пауза 30 сек между батчами...")
                        time.sleep(30)

                if date_str != list(missing_by_date.keys())[-1]:
                    self.log("⏳ Пауза 90 сек между днями...")
                    time.sleep(90)

            if new_data:
                new_df = pd.concat(new_data, ignore_index=True)
                if weekly_df.empty:
                    weekly_df = new_df
                else:
                    weekly_df = pd.concat([weekly_df, new_df], ignore_index=True)

                self._save_weekly_data(weekly_df, store_name, 'keywords', week_start)
            else:
                self.log(f"ℹ️ Нет новых данных за неделю")

        # После обработки всех недель пробуем повторно загрузить неудачные комбинации
        if failed_combinations:
            self.log(f"\n🔄 Начинаем повторные попытки для {len(failed_combinations)} неудачных комбинаций")
            # Группируем по дате и фильтру для более эффективной загрузки
            retry_by_date_filter = defaultdict(list)
            for date_str, nm_id, filter_field in failed_combinations:
                retry_by_date_filter[(date_str, filter_field)].append(nm_id)

            total_retry_success = 0
            for (date_str, filter_field), nm_ids in retry_by_date_filter.items():
                # Убираем дубликаты артикулов
                nm_ids = list(set(nm_ids))
                self.log(f"📅 Повторная загрузка для {date_str}, фильтр {filter_field}, артикулов: {len(nm_ids)}")

                # Используем меньшие батчи (по 10) для повышения вероятности успеха
                batches = [nm_ids[i:i+10] for i in range(0, len(nm_ids), 10)]
                for batch_idx, batch in enumerate(batches, 1):
                    self.log(f"  📦 Батч {batch_idx}/{len(batches)}: {len(batch)} артикулов")
                    payload = {
                        "currentPeriod": {"start": date_str, "end": date_str},
                        "nmIds": batch,
                        "topOrderBy": filter_field,
                        "includeSubstitutedSKUs": False,
                        "includeSearchTexts": True,
                        "orderBy": {"field": filter_field, "mode": "desc"},
                        "limit": 100
                    }
                    max_retries = 7  # больше попыток
                    success = False
                    for attempt in range(max_retries):
                        try:
                            resp = requests.post(url, headers=headers, json=payload, timeout=150)  # увеличен таймаут
                            if resp.status_code == 200:
                                data = resp.json()
                                items = data.get('data', {}).get('items', [])
                                if items:
                                    # Сохраняем полученные данные
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
                                    # Сохраняем в соответствующий недельный файл
                                    week_date = datetime.strptime(date_str, '%Y-%m-%d')
                                    week_start = self._get_week_start(week_date)
                                    # Загружаем текущие данные недели
                                    weekly_retry_df = self._load_weekly_data(store_name, 'keywords', week_start)
                                    if weekly_retry_df.empty:
                                        weekly_retry_df = pd.DataFrame(batch_data)
                                    else:
                                        weekly_retry_df = pd.concat([weekly_retry_df, pd.DataFrame(batch_data)], ignore_index=True)
                                    self._save_weekly_data(weekly_retry_df, store_name, 'keywords', week_start)
                                    total_retry_success += len(batch_data)
                                    self.log(f"      ✅ Успешно загружено {len(batch_data)} записей")
                                else:
                                    self.log(f"      ℹ️ Нет данных для этого батча")
                                success = True
                                break
                            elif resp.status_code == 429:
                                wait = 60 * (attempt + 1)
                                self.log(f"      ⚠ Лимит, попытка {attempt+1}/{max_retries}, ждём {wait} сек...")
                                time.sleep(wait)
                            elif resp.status_code in (502, 503, 504):
                                wait = 30 * (attempt + 1)
                                self.log(f"      ⚠ Ошибка шлюза {resp.status_code}, попытка {attempt+1}/{max_retries}, ждём {wait} сек...")
                                time.sleep(wait)
                            else:
                                self.log(f"      ❌ Ошибка {resp.status_code}")
                                break
                        except Exception as e:
                            self.log(f"      ❌ Исключение: {e}")
                            if attempt < max_retries - 1:
                                time.sleep(10)
                            else:
                                break
                    if not success:
                        self.log(f"      ❌ Не удалось загрузить батч после {max_retries} попыток")
                    time.sleep(30)  # пауза между батчами

            self.log(f"🔄 Повторные попытки завершены. Успешно загружено {total_retry_success} записей.")

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

    # ---------- Реклама ----------
    def update_adverts(self, store_name: str) -> bool:
        self.log(f"\n📌 ОБНОВЛЕНИЕ: Реклама для магазина {store_name}")
        config = self.reports_config['adverts']

        base_key = f"Отчёты/{config['folder']}/{store_name}/База данных.xlsx"
        try:
            df_base = self.s3.read_excel(base_key, sheet_name=0)
            if df_base.empty:
                self.log("❌ Файл База данных.xlsx пуст или не найден")
                return False
            campaign_ids = df_base.iloc[:, 0].dropna().astype(int).tolist()
            id_to_article = {}
            if len(df_base.columns) >= 2:
                id_to_article = dict(zip(df_base.iloc[:, 0].astype(int), df_base.iloc[:, 1].astype(str)))
            self.log(f"✅ Загружено {len(campaign_ids)} ID кампаний из базы")
        except Exception as e:
            self.log(f"❌ Ошибка чтения База данных.xlsx: {e}")
            return False

        start_date, end_date = self._get_date_range_last_n_days(30)
        all_dates = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]

        weeks = defaultdict(list)
        for d in all_dates:
            week_start = self._get_week_start(datetime.combine(d, datetime.min.time()))
            weeks[week_start].append(d)

        api_key = self.api_keys[store_name][config['key_type']]
        headers = {"Authorization": f"Bearer {api_key.strip()}"}

        campaigns_info = {}
        for payment_type in ['cpm', 'cpc']:
            url = f"{config['api_url']}?statuses=9,11&payment_type={payment_type}"
            try:
                resp = requests.get(url, headers=headers, timeout=30)
                if resp.status_code == 200:
                    data = resp.json()
                    adverts = data.get('adverts', [])
                    for adv in adverts:
                        adv_id = adv.get('id')
                        settings = adv.get('settings', {})
                        name = settings.get('name', '')
                        nm_settings = adv.get('nm_settings', [])
                        subject = ''
                        if nm_settings:
                            subject = nm_settings[0].get('subject', {}).get('name', '')
                        campaigns_info[adv_id] = {'name': name, 'subject': subject}
                else:
                    self.log(f"⚠️ Не удалось получить список кампаний для {payment_type}: {resp.status_code}")
                time.sleep(0.5)
            except Exception as e:
                self.log(f"❌ Ошибка при запросе кампаний: {e}")

        self.log(f"✅ Получена информация о {len(campaigns_info)} кампаниях")

        for week_start, dates in weeks.items():
            self.log(f"📅 Обработка недели, начинающейся {week_start.strftime('%Y-%m-%d')}")
            weekly_df = self._load_weekly_data(store_name, 'adverts', week_start)

            existing_keys = set()
            if not weekly_df.empty:
                for _, row in weekly_df.iterrows():
                    cid = row.get('ID кампании')
                    d = row.get('Дата')
                    if cid and d:
                        existing_keys.add((cid, d))

            missing = []
            for d in dates:
                date_str = d.strftime('%Y-%m-%d')
                for cid in campaign_ids:
                    if (cid, date_str) not in existing_keys:
                        missing.append((cid, date_str))

            if not missing:
                self.log(f"✅ Все данные за неделю уже загружены")
                continue

            self.log(f"📅 Недостающих записей: {len(missing)}")

            missing_by_date = defaultdict(list)
            for cid, date_str in missing:
                missing_by_date[date_str].append(cid)

            new_data = []
            stats_url = "https://advert-api.wildberries.ru/adv/v3/fullstats"

            for date_str, cids in missing_by_date.items():
                self.log(f"📅 Загрузка статистики за {date_str} для {len(cids)} кампаний...")
                chunks = [cids[i:i+30] for i in range(0, len(cids), 30)]
                day_data = []
                for chunk in chunks:
                    ids_param = ','.join(map(str, chunk))
                    params = {
                        'ids': ids_param,
                        'beginDate': date_str,
                        'endDate': date_str
                    }
                    retries = 0
                    success = False
                    while retries < 3 and not success:
                        try:
                            resp = requests.get(stats_url, headers=headers, params=params, timeout=60)
                            if resp.status_code == 200:
                                data = resp.json()
                                if data:
                                    for camp in data:
                                        camp_id = camp.get('advertId')
                                        days = camp.get('days', [])
                                        for day in days:
                                            day_date = day.get('date', '').split('T')[0]
                                            if day_date != date_str:
                                                continue
                                            row = {
                                                'ID кампании': camp_id,
                                                'Артикул WB': id_to_article.get(camp_id, ''),
                                                'Название': campaigns_info.get(camp_id, {}).get('name', ''),
                                                'Название предмета': campaigns_info.get(camp_id, {}).get('subject', ''),
                                                'Дата': day_date,
                                                'Показы': day.get('views', 0),
                                                'Клики': day.get('clicks', 0),
                                                'CTR': day.get('ctr', 0),
                                                'CPC': day.get('cpc', 0),
                                                'Заказы': day.get('orders', 0),
                                                'CR': day.get('cr', 0),
                                                'Расход': day.get('sum', 0),
                                                'ATBS': day.get('atbs', 0),
                                                'SHKS': day.get('shks', 0),
                                                'Сумма заказов': day.get('sum_price', 0),
                                                'Отменено': day.get('canceled', 0),
                                            }
                                            if row['Сумма заказов'] > 0:
                                                row['ДРР'] = round(row['Расход'] / (row['Сумма заказов'] * 0.88) * 100, 2)
                                            else:
                                                row['ДРР'] = 0
                                            day_data.append(row)
                                success = True
                            elif resp.status_code == 429:
                                retries += 1
                                wait = 60 * retries
                                self.log(f"    ⚠️ Лимит, ждём {wait} сек...")
                                time.sleep(wait)
                            else:
                                self.log(f"    ❌ Ошибка {resp.status_code}: {resp.text[:200]}")
                                break
                        except Exception as e:
                            self.log(f"    ❌ Исключение: {e}")
                            break
                    time.sleep(30)

                if day_data:
                    day_df = pd.DataFrame(day_data)
                    if 'Название предмета' in day_df.columns:
                        day_df['Название предмета'] = day_df['Название предмета'].str.strip().str.lower().str.capitalize()
                    new_data.append(day_df)

                time.sleep(self.delays['adverts'])

            if new_data:
                new_df = pd.concat(new_data, ignore_index=True)
                if weekly_df.empty:
                    weekly_df = new_df
                else:
                    weekly_df = pd.concat([weekly_df, new_df], ignore_index=True)

                self._save_weekly_data(weekly_df, store_name, 'adverts', week_start)
            else:
                self.log(f"ℹ️ Нет новых данных за неделю")

        return True

    # ---------- Остатки из 1С ----------
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
            resp = requests.get(download_url, auth=auth, timeout=120, stream=True)
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

    # ====================== ОСНОВНОЙ ЗАПУСК ======================

    def run_daily_update(self, store_name: str, reports: List[str] = None):
        all_reports = ['orders', 'stocks', 'finance', 'keywords', 'funnel', 'adverts', '1c_stocks']
        if reports is None:
            reports = all_reports

        self.log(f"🚀 Начало обновления для магазина {store_name}. Запрошенные отчёты: {reports}")
        for report in reports:
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
                time.sleep(30)

        self.log("✅ Обновление завершено")


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

    if not os.environ.get('URL_1C_STOCKS'):
        print("⚠️ Переменная URL_1C_STOCKS не задана. Отчёт '1c_stocks' будет пропущен.")

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
