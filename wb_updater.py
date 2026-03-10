#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Ежедневный сбор данных Wildberries с сохранением в Yandex Cloud Object Storage.
Поддерживает все основные отчёты, включая выгрузку остатков из 1С.
"""

import os
import io
import json
import time
import uuid
import zipfile
import tempfile
import traceback
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

    def read_excel(self, key: str, sheet_name: Optional[str] = None) -> pd.DataFrame:
        """Скачивает Excel-файл из бакета и читает его."""
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=key)
            data = obj['Body'].read()
            if sheet_name:
                df = pd.read_excel(io.BytesIO(data), sheet_name=sheet_name)
            else:
                df = pd.read_excel(io.BytesIO(data))
            return df
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return pd.DataFrame()  # файл не найден
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

        # Конфигурация отчётов
        self.reports_config = {
            'orders': {
                'name': 'Заказы',
                'folder': 'Заказы',
                'filename': 'Заказы.xlsx',
                'date_column': 'date',
                'id_columns': ['date', 'gNumber', 'srid'],
                'api_url': 'https://statistics-api.wildberries.ru/api/v1/supplier/orders',
                'api_method': 'GET',
                'key_type': 'stats',
                'weekly': True,
            },
            'stocks': {
                'name': 'Остатки',
                'folder': 'Остатки',
                'filename': 'Остатки.xlsx',
                'date_column': 'Дата запроса',
                'id_columns': ['Дата запроса', 'Артикул WB', 'Склад'],
                'api_url': 'https://statistics-api.wildberries.ru/api/v1/supplier/stocks',
                'api_method': 'GET',
                'key_type': 'stats',
                'weekly': True,
            },
            'finance': {
                'name': 'Финансовые показатели',
                'folder': 'Финансовые показатели',
                'filename': 'Финансовые показатели.xlsx',
                'date_column': 'rr_dt',
                'id_columns': ['rr_dt', 'rrd_id', 'nm_id'],
                'api_url': 'https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod',
                'api_method': 'GET',
                'key_type': 'stats',
                'weekly': True,
            },
            'keywords': {
                'name': 'Позиции по Ключам',
                'folder': 'Поисковые запросы',
                'filename': 'Позиции по Ключам.xlsx',
                'date_column': 'Дата',
                'id_columns': ['Дата', 'Поисковый запрос', 'Артикул WB', 'Фильтр'],
                'api_url': 'https://seller-analytics-api.wildberries.ru/api/v2/search-report/product/search-texts',
                'api_method': 'POST',
                'key_type': 'promo',
                'weekly': True,
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
                'weekly': False,
            },
            'adverts': {
                'name': 'Реклама',
                'folder': 'Реклама',
                'filename': 'Реклама.xlsx',
                'date_column': 'Дата',
                'id_columns': ['ID кампании', 'Дата'],
                'api_url': 'https://advert-api.wildberries.ru/api/advert/v2/adverts',
                'api_method': 'GET',
                'key_type': 'promo',
                'weekly': True,
                'retention_days': 30,
            },
            # НОВЫЙ ОТЧЁТ: Остатки из 1С
            '1c_stocks': {
                'name': 'Остатки 1С',
                'folder': 'Остатки',
                'filename': 'Остатки_1С.xlsx',          # имя файла в бакете
                'date_column': 'Дата',                   # для возможной очистки (если потребуется)
                'id_columns': [],                         # дедупликация не нужна, т.к. файл перезаписывается
                'api_url': None,                           # не используется
                'key_type': None,
                'weekly': False,
                'retention_days': 90,
            }
        }

        # Задержки между запросами (секунды) – соответствуют лимитам API
        self.delays = {
            'orders': 65,
            'stocks': 65,
            'finance': 65,
            'keywords': 70,
            'funnel': 30,
            'adverts': 30,
            '1c_stocks': 0,   # для 1С задержка не нужна
        }

        # Список категорий для фильтрации артикулов в keywords
        self.target_subjects = ['Помады', 'Косметические карандаши', 'Кисти косметические', 'Блески']

        self.log(f"🚀 Запуск обновления данных. Время: {self.start_time}")

    # ====================== ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ======================

    def log(self, message: str, level: str = "INFO", end: str = "\n"):
        """Логирование в stdout с принудительным сбросом буфера."""
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [{level}] {message}", end=end, flush=True)

    def _get_s3_key(self, store_name: str, report_type: str, filename: Optional[str] = None) -> str:
        """Формирует ключ (путь) в бакете для указанного отчёта."""
        config = self.reports_config[report_type]
        folder = config['folder']
        if filename is None:
            filename = config['filename']
        return f"Отчёты/{folder}/{store_name}/{filename}"

    def _get_weekly_key(self, store_name: str, report_type: str, date: datetime) -> str:
        """Генерирует ключ для недельного файла (например, Заказы_2025-W10.xlsx)."""
        year, week, _ = date.isocalendar()
        config = self.reports_config[report_type]
        filename = f"{config['name']}_{year}-W{week:02d}.xlsx"
        return f"Отчёты/{config['folder']}/{store_name}/Недельные/{filename}"

    def _load_existing_report(self, store_name: str, report_type: str) -> pd.DataFrame:
        """Загружает существующий основной отчёт из бакета."""
        key = self._get_s3_key(store_name, report_type)
        self.log(f"📥 Загрузка отчёта {report_type} для {store_name} из {key}")
        try:
            df = self.s3.read_excel(key, sheet_name=self.reports_config[report_type]['name'])
            if df.empty:
                self.log("ℹ️ Файл не найден или пуст, будет создан новый")
                return pd.DataFrame()
            # Приводим колонку с датой к строковому формату для единообразия
            date_col = self.reports_config[report_type]['date_column']
            if date_col in df.columns:
                df[date_col] = pd.to_datetime(df[date_col]).dt.strftime('%Y-%m-%d')
            return df
        except Exception as e:
            self.log(f"⚠️ Ошибка загрузки {key}: {e}")
            return pd.DataFrame()

    def _save_report(self, df: pd.DataFrame, store_name: str, report_type: str, extra_sheets: Optional[Dict[str, pd.DataFrame]] = None) -> bool:
        """
        Сохраняет отчёт в бакет (перезапись). Если передан extra_sheets, добавляет их как дополнительные листы.
        """
        key = self._get_s3_key(store_name, report_type)
        config = self.reports_config[report_type]

        # Дедупликация основного df
        before = len(df)
        if config['id_columns'] and not df.empty:
            existing_cols = [c for c in config['id_columns'] if c in df.columns]
            if existing_cols:
                df = df.drop_duplicates(subset=existing_cols, keep='last')
                after = len(df)
                if before > after:
                    self.log(f"🔍 Удалено дубликатов: {before - after}")

        # Удаление устаревших данных
        retention = config.get('retention_days', self.data_period_days)
        df = self._remove_outdated_data(df, config, retention)

        try:
            # Сохраняем во временный файл
            with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
                tmp_path = tmp.name
            with pd.ExcelWriter(tmp_path, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name=config['name'], index=False)
                if extra_sheets:
                    for sheet_name, sheet_df in extra_sheets.items():
                        if not sheet_df.empty:
                            sheet_df.to_excel(writer, sheet_name=sheet_name, index=False)
            # Загружаем файл в S3
            self.s3.upload_file(tmp_path, key)
            self.log(f"✅ Отчёт сохранён: {key}, записей: {len(df)} (за {time.time()-self._last_save_start:.1f} сек)")
            if extra_sheets:
                self.log(f"   + дополнительные листы: {', '.join(extra_sheets.keys())}")
            return True
        except Exception as e:
            self.log(f"❌ Ошибка сохранения {key}: {e}")
            return False
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def _save_weekly(self, df: pd.DataFrame, store_name: str, report_type: str, date: datetime):
        """Сохраняет данные в недельный файл."""
        if not self.reports_config[report_type]['weekly']:
            return
        week_key = self._get_weekly_key(store_name, report_type, date)
        start = time.time()
        try:
            existing = self.s3.read_excel(week_key)
        except:
            existing = pd.DataFrame()
        if not existing.empty:
            combined = pd.concat([existing, df], ignore_index=True)
            # дедупликация по ключевым колонкам
            id_cols = self.reports_config[report_type]['id_columns']
            id_cols_present = [c for c in id_cols if c in combined.columns]
            if id_cols_present:
                combined = combined.drop_duplicates(subset=id_cols_present, keep='last')
        else:
            combined = df
        self.s3.write_excel(week_key, combined)
        self.log(f"💾 Недельные данные сохранены: {week_key} (за {time.time()-start:.1f} сек)")

    def _remove_outdated_data(self, df: pd.DataFrame, config: dict, retention_days: int) -> pd.DataFrame:
        """Удаляет строки с датой старше retention_days дней."""
        if df.empty:
            return df
        date_col = config['date_column']
        if date_col not in df.columns:
            return df
        today = datetime.now(pytz.timezone('Europe/Moscow')).date()
        end_date = today - timedelta(days=1)
        start_date = end_date - timedelta(days=retention_days - 1)
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        df = df[df[date_col].notna()]
        df = df[(df[date_col] >= pd.Timestamp(start_date)) & (df[date_col] <= pd.Timestamp(end_date))]
        return df

    def _get_date_range_90_days(self) -> Tuple[datetime, datetime]:
        """Возвращает диапазон дат за последние 90 дней (без сегодня)."""
        today = datetime.now(pytz.timezone('Europe/Moscow')).date()
        end_date = today - timedelta(days=1)
        start_date = end_date - timedelta(days=self.data_period_days - 1)
        return start_date, end_date

    def _get_date_range_last_n_days(self, n: int) -> Tuple[datetime, datetime]:
        """Возвращает диапазон дат за последние n дней (без сегодня)."""
        today = datetime.now(pytz.timezone('Europe/Moscow')).date()
        end_date = today - timedelta(days=1)
        start_date = end_date - timedelta(days=n - 1)
        return start_date, end_date

    def _get_articles_by_subjects(self, store_name: str, subjects: List[str]) -> List[int]:
        """
        Извлекает уникальные nmId из файла заказов магазина,
        отфильтрованные по заданным subject.
        """
        orders_key = self._get_s3_key(store_name, 'orders')
        try:
            df = self.s3.read_excel(orders_key, sheet_name='Заказы')
            if df.empty:
                self.log("⚠️ Файл заказов пуст или не найден")
                return []
            # Ищем колонки с артикулом и предметом
            nm_col = None
            subj_col = None
            possible_nm = ['nmId', 'nmID', 'Артикул WB', 'Артикул']
            possible_subj = ['subject', 'Предмет', 'subjectName', 'Название предмета']
            for c in possible_nm:
                if c in df.columns:
                    nm_col = c
                    break
            for c in possible_subj:
                if c in df.columns:
                    subj_col = c
                    break
            if nm_col is None or subj_col is None:
                self.log("⚠️ В заказах не найдены нужные колонки")
                return []
            # Приводим subject к нижнему регистру для сравнения
            df[subj_col] = df[subj_col].astype(str).str.lower().str.strip()
            target_lower = [s.lower() for s in subjects]
            mask = df[subj_col].isin(target_lower)
            filtered = df.loc[mask, nm_col].dropna().unique()
            articles = [int(a) for a in filtered if pd.notna(a)]
            self.log(f"✅ Получено {len(articles)} артикулов из заказов по заданным категориям")
            return articles
        except Exception as e:
            self.log(f"❌ Ошибка чтения заказов: {e}")
            return []

    # ====================== МЕТОДЫ ДЛЯ КАЖДОГО ОТЧЁТА ======================

    def _make_request(self, config: dict, headers: dict, date_str: str, **kwargs) -> Optional[Any]:
        """Универсальный метод для выполнения API-запроса (упрощённый)."""
        url = config['api_url']
        method = config['api_method']
        params = {}
        payload = None

        if config['name'] == 'Заказы':
            params = {"dateFrom": date_str, "flag": 1}
        elif config['name'] == 'Остатки':
            params = {"dateFrom": date_str}
        elif config['name'] == 'Финансовые показатели':
            # Для финансов требуется постраничная загрузка, обработаем отдельно
            return self._fetch_finance_day(config, headers, date_str)
        elif config['name'] == 'Позиции по Ключам':
            # Обрабатывается отдельно с батчами
            pass
        elif config['name'] == 'Воронка продаж':
            pass
        elif config['name'] == 'Реклама':
            # для рекламы не используется этот метод
            pass

        try:
            if method == 'GET':
                resp = requests.get(url, headers=headers, params=params, timeout=60)
            else:
                resp = requests.post(url, headers=headers, json=payload, timeout=60)

            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 429:
                self.log(f"    ⚠ Лимит запросов (429), ждём 60 сек...")
                time.sleep(60)
                return None
            elif resp.status_code == 204:
                return []  # нет данных
            else:
                self.log(f"    ❌ Ошибка {resp.status_code}: {resp.text[:200]}")
                return None
        except Exception as e:
            self.log(f"    ❌ Исключение при запросе: {e}")
            return None

    def _fetch_finance_day(self, config: dict, headers: dict, date_str: str) -> List[dict]:
        """Загружает финансовые показатели за день с учётом постраничности."""
        url = config['api_url']
        all_items = []
        rrdid = 0
        limit = 100000
        while True:
            params = {
                "dateFrom": date_str,
                "dateTo": date_str,
                "limit": limit,
                "rrdid": rrdid,
                "period": "daily"
            }
            try:
                resp = requests.get(url, headers=headers, params=params, timeout=60)
                if resp.status_code == 200:
                    data = resp.json()
                    if not data:
                        break
                    all_items.extend(data)
                    last_rrdid = data[-1].get("rrd_id", 0)
                    if len(data) < limit or last_rrdid <= rrdid:
                        break
                    rrdid = last_rrdid
                elif resp.status_code == 204:
                    break
                elif resp.status_code == 429:
                    self.log("    ⚠ Лимит, ждём 65 сек...")
                    time.sleep(65)
                else:
                    self.log(f"    ❌ Ошибка {resp.status_code}")
                    break
            except Exception as e:
                self.log(f"    ❌ Исключение: {e}")
                break
        return all_items

    # ---------- Заказы ----------
    def update_orders(self, store_name: str) -> bool:
        """Обновление данных по заказам. Сохраняем после каждого дня."""
        self.log(f"\n📌 ОБНОВЛЕНИЕ: Заказы для магазина {store_name}")
        config = self.reports_config['orders']
        existing_df = self._load_existing_report(store_name, 'orders')
        start_date, end_date = self._get_date_range_90_days()
        all_dates = [(start_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in range((end_date - start_date).days + 1)]

        if not existing_df.empty:
            existing_df['date'] = pd.to_datetime(existing_df['date']).dt.strftime('%Y-%m-%d')
            existing_dates = set(existing_df['date'].unique())
        else:
            existing_dates = set()

        dates_to_load = [d for d in all_dates if d not in existing_dates]
        if not dates_to_load:
            self.log("✅ Все данные по заказам уже загружены")
            return True

        self.log(f"📅 Будет загружено дней: {len(dates_to_load)}")
        api_key = self.api_keys[store_name][config['key_type']]
        headers = {"Authorization": api_key.strip()}
        current_df = existing_df.copy()

        for i, date_str in enumerate(dates_to_load, 1):
            self.log(f"📅 Загрузка дня {i}/{len(dates_to_load)}: {date_str}")
            data = self._make_request(config, headers, date_str)
            if data and isinstance(data, list):
                day_df = pd.DataFrame(data)
                if not day_df.empty:
                    day_df['store'] = store_name
                    if 'date' in day_df.columns:
                        day_df['date'] = pd.to_datetime(day_df['date']).dt.strftime('%Y-%m-%d')
                    # Объединяем с текущим датафреймом
                    current_df = pd.concat([current_df, day_df], ignore_index=True) if not current_df.empty else day_df
                    self.log(f"✅ Получено {len(day_df)} записей")
                else:
                    self.log("ℹ️ Нет данных за этот день")
                    continue
            else:
                self.log("⚠️ Не удалось получить данные, пропускаем день")
                continue

            # Сохраняем после каждого дня
            self._last_save_start = time.time()
            if self._save_report(current_df, store_name, 'orders'):
                self._save_weekly(day_df, store_name, 'orders', datetime.strptime(date_str, '%Y-%m-%d'))
            else:
                self.log(f"❌ Ошибка сохранения после дня {date_str}, но продолжаем")

            # Пауза между днями
            if i < len(dates_to_load):
                time.sleep(self.delays['orders'])

        return True

    # ---------- Остатки ----------
    def update_stocks(self, store_name: str) -> bool:
        """
        Обновление остатков. Загружаем только за вчерашний день.
        """
        self.log(f"\n📌 ОБНОВЛЕНИЕ: Остатки для магазина {store_name}")
        config = self.reports_config['stocks']

        existing_df = self._load_existing_report(store_name, 'stocks')
        if not existing_df.empty and 'Дата запроса' in existing_df.columns:
            existing_dates = set(pd.to_datetime(existing_df['Дата запроса']).dt.strftime('%Y-%m-%d').unique())
        else:
            existing_dates = set()

        target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

        if target_date in existing_dates:
            self.log(f"✅ Данные за {target_date} уже есть, пропускаем загрузку")
        else:
            self.log(f"📅 Загрузка остатков за {target_date}...")
            api_key = self.api_keys[store_name][config['key_type']]
            headers = {"Authorization": api_key.strip()}
            try:
                params = {"dateFrom": target_date}
                resp = requests.get(config['api_url'], headers=headers, params=params, timeout=60)
                if resp.status_code == 200:
                    data = resp.json()
                    if data:
                        df_day = pd.DataFrame(data)
                        df_day['Дата запроса'] = target_date
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
                        existing_df = pd.concat([existing_df, df_day], ignore_index=True) if not existing_df.empty else df_day
                        self.log(f"✅ Получено {len(df_day)} записей")
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

        # Удаляем устаревшие данные
        if not existing_df.empty:
            cutoff_date = (datetime.now() - timedelta(days=self.data_period_days)).strftime('%Y-%m-%d')
            existing_df['Дата запроса_dt'] = pd.to_datetime(existing_df['Дата запроса'])
            filtered = existing_df[existing_df['Дата запроса_dt'] >= cutoff_date].copy()
            filtered.drop(columns=['Дата запроса_dt'], inplace=True)
            self.log(f"🗑️ Удалено записей старше {cutoff_date}: {len(existing_df) - len(filtered)}")
            existing_df = filtered

        # Сохраняем основной файл
        self._last_save_start = time.time()
        if self._save_report(existing_df, store_name, 'stocks'):
            # Сохраняем недельные данные, если был новый день
            if target_date not in existing_dates and 'df_day' in locals():
                self._save_weekly(df_day, store_name, 'stocks', datetime.strptime(target_date, '%Y-%m-%d'))
            return True
        return True

    # ---------- Финансовые показатели (оптимизировано: основной файл сохраняется один раз в конце) ----------
    def update_finance(self, store_name: str) -> bool:
        """
        Обновление финансовых показателей.
        Загружаем все недостающие дни, но основной файл сохраняем только один раз в конце,
        чтобы избежать огромных задержек при перезаписи растущего файла после каждого дня.
        Недельные файлы сохраняем после каждого дня (они маленькие).
        """
        self.log(f"\n📌 ОБНОВЛЕНИЕ: Финансовые показатели для магазина {store_name} (оптимизированное сохранение)")
        config = self.reports_config['finance']
        existing_df = self._load_existing_report(store_name, 'finance')
        start_date, end_date = self._get_date_range_90_days()
        all_dates = [(start_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in range((end_date - start_date).days + 1)]

        if not existing_df.empty:
            existing_df['rr_dt'] = pd.to_datetime(existing_df['rr_dt']).dt.strftime('%Y-%m-%d')
            existing_dates = set(existing_df['rr_dt'].unique())
        else:
            existing_dates = set()

        dates_to_load = [d for d in all_dates if d not in existing_dates]
        if not dates_to_load:
            self.log("✅ Все финансовые данные уже загружены")
            return True

        self.log(f"📅 Будет загружено дней: {len(dates_to_load)}")
        api_key = self.api_keys[store_name][config['key_type']]
        headers = {"Authorization": f"Bearer {api_key.strip()}"}
        current_df = existing_df.copy()
        loaded_days = 0

        for i, date_str in enumerate(dates_to_load, 1):
            self.log(f"📅 Загрузка дня {i}/{len(dates_to_load)}: {date_str}")
            day_data = self._fetch_finance_day(config, headers, date_str)
            if day_data:
                day_df = pd.DataFrame(day_data)
                day_df['store'] = store_name
                if 'rr_dt' in day_df.columns:
                    day_df['rr_dt'] = pd.to_datetime(day_df['rr_dt']).dt.strftime('%Y-%m-%d')
                current_df = pd.concat([current_df, day_df], ignore_index=True) if not current_df.empty else day_df
                loaded_days += 1
                self.log(f"✅ Получено {len(day_df)} записей")
            else:
                self.log("ℹ️ Нет данных за этот день")
                continue

            # Сохраняем недельные файлы после каждого дня (они небольшие)
            self._save_weekly(day_df, store_name, 'finance', datetime.strptime(date_str, '%Y-%m-%d'))

            # Пауза между днями (обязательно для соблюдения лимита API)
            if i < len(dates_to_load):
                time.sleep(self.delays['finance'])

        # После загрузки всех дней сохраняем основной файл один раз
        if loaded_days > 0:
            self.log(f"💾 Сохраняем итоговый файл с {len(current_df)} записями...")
            self._last_save_start = time.time()
            if self._save_report(current_df, store_name, 'finance'):
                self.log(f"✅ Финансовые данные успешно обновлены, добавлено {loaded_days} дней")
            else:
                self.log("❌ Ошибка сохранения итогового файла")
                return False
        else:
            self.log("⚠️ Ни одного нового дня не загружено")

        return True

    # ---------- Позиции по ключам ----------
    def update_keywords(self, store_name: str) -> bool:
        """Инкрементальное обновление данных по поисковым запросам (только по заданным категориям)."""
        self.log(f"\n📌 ОБНОВЛЕНИЕ: Позиции по ключам для магазина {store_name} (фильтр по категориям)")

        articles = self._get_articles_by_subjects(store_name, self.target_subjects)
        if not articles:
            self.log("⚠️ Нет артикулов из заказов по заданным категориям, используем резервный список")
            articles = [87705142, 110254021, 118217701, 111944302, 110561153]

        existing_df = self._load_existing_report(store_name, 'keywords')
        if existing_df.empty:
            self.log("📊 Существующий файл пуст, будет создан новый")
        else:
            self.log(f"📊 Загружено существующих записей: {len(existing_df)}")

        start_date, end_date = self._get_date_range_90_days()
        all_dates = [(start_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in range((end_date - start_date).days + 1)]
        filters = ["orders", "openCard", "addToCart"]

        missing_by_date = {}
        if not existing_df.empty:
            existing_df['Дата'] = existing_df['Дата'].astype(str)

        for date_str in all_dates:
            missing_articles = []
            for nm_id in articles:
                if existing_df.empty:
                    missing_articles.append(nm_id)
                else:
                    mask = (existing_df['Дата'] == date_str) & (existing_df['Артикул WB'] == nm_id)
                    day_data = existing_df[mask]
                    present_filters = set(day_data['Фильтр'].unique()) if 'Фильтр' in day_data.columns else set()
                    if not set(filters).issubset(present_filters):
                        missing_articles.append(nm_id)
            if missing_articles:
                missing_by_date[date_str] = missing_articles

        total_missing = sum(len(v) for v in missing_by_date.values())
        if total_missing == 0:
            self.log("✅ Все данные по ключам уже загружены")
            return True

        self.log(f"📊 Обнаружено пропусков: {total_missing} комбинаций (дата, артикул)")
        sorted_dates = sorted(missing_by_date.keys(), reverse=True)
        self.log(f"📅 Будет загружено дней: {len(sorted_dates)}")

        api_key = self.api_keys[store_name]['promo']
        headers = {"Authorization": f"Bearer {api_key.strip()}", "Content-Type": "application/json"}
        url = self.reports_config['keywords']['api_url']

        errors = []
        for date_idx, date_str in enumerate(sorted_dates, 1):
            articles_for_date = missing_by_date[date_str]
            self.log(f"📅 День {date_idx}/{len(sorted_dates)}: {date_str}, артикулов: {len(articles_for_date)}")

            batches = [articles_for_date[i:i+50] for i in range(0, len(articles_for_date), 50)]
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
                    max_retries = 3
                    for attempt in range(max_retries):
                        try:
                            resp = requests.post(url, headers=headers, json=payload, timeout=60)
                            if resp.status_code == 200:
                                data = resp.json()
                                items = data.get('data', {}).get('items', [])
                                if items:
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
                                else:
                                    self.log(f" -> ℹ️ нет данных")
                                break
                            elif resp.status_code == 429:
                                wait = 60 * (attempt + 1)
                                self.log(f"    ⚠ Лимит, ждём {wait} сек...", end="")
                                time.sleep(wait)
                            else:
                                self.log(f" -> ❌ ошибка {resp.status_code}")
                                break
                        except Exception as e:
                            self.log(f"    ❌ Исключение: {e}")
                            if attempt < max_retries - 1:
                                time.sleep(10)
                            else:
                                for nm_id in batch:
                                    errors.append((date_str, nm_id, filter_field))
                            break

                    if filter_field != filters[-1]:
                        time.sleep(20)

                if batch_data:
                    new_df = pd.DataFrame(batch_data)
                    existing_df = pd.concat([existing_df, new_df], ignore_index=True) if not existing_df.empty else new_df
                    self._last_save_start = time.time()
                    if self._save_report(existing_df, store_name, 'keywords'):
                        self._save_weekly(new_df, store_name, 'keywords', datetime.strptime(date_str, '%Y-%m-%d'))

                if batch_idx < len(batches):
                    self.log("    ⏳ Пауза 20 сек между батчами...")
                    time.sleep(20)

            if date_idx < len(sorted_dates):
                self.log("⏳ Пауза 20 сек между днями...")
                time.sleep(20)

        if errors:
            self.log(f"\n⚠ Зафиксировано {len(errors)} ошибок.")
        else:
            self.log("✅ Все данные успешно загружены")
        return True

    # ---------- Воронка продаж ----------
    def update_funnel(self, store_name: str) -> bool:
        """Обновление воронки продаж (сложный отчёт через генерацию)."""
        self.log(f"\n📌 ОБНОВЛЕНИЕ: Воронка продаж для магазина {store_name}")
        config = self.reports_config['funnel']

        key = self._get_s3_key(store_name, 'funnel')
        if self.s3.file_exists(key):
            df_existing = self._load_existing_report(store_name, 'funnel')
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
                                self._save_report(df, store_name, 'funnel')
                                return True
                elif resp.status_code == 202:
                    self.log(f"⏳ Отчёт ещё не готов, попытка {attempt}/30")
                else:
                    self.log(f"⚠️ Статус {resp.status_code}")
            except Exception as e:
                self.log(f"⚠️ Ошибка при скачивании: {e}")

        self.log("❌ Не удалось получить отчёт воронки")
        return False

    # ---------- Реклама (оптимизировано: основной файл сохраняется один раз в конце) ----------
    def update_adverts(self, store_name: str) -> bool:
        """
        Обновление данных по рекламным кампаниям.
        Загружает статистику за последние 30 дней.
        Сохраняем основной файл один раз в конце, недельные – после каждого дня.
        """
        self.log(f"\n📌 ОБНОВЛЕНИЕ: Реклама для магазина {store_name}")
        config = self.reports_config['adverts']

        # Загружаем базу данных с ID кампаний и артикулами
        base_key = f"Отчёты/{config['folder']}/{store_name}/База данных.xlsx"
        try:
            df_base = self.s3.read_excel(base_key, sheet_name='Кампании')
            if df_base.empty:
                self.log("❌ Файл База данных.xlsx пуст или не найден")
                return False
            # Первая колонка – ID кампании, вторая – артикул WB (если есть)
            campaign_ids = df_base.iloc[:, 0].dropna().astype(int).tolist()
            id_to_article = {}
            if len(df_base.columns) >= 2:
                id_to_article = dict(zip(df_base.iloc[:, 0].astype(int), df_base.iloc[:, 1].astype(str)))
            self.log(f"✅ Загружено {len(campaign_ids)} ID кампаний из базы")
        except Exception as e:
            self.log(f"❌ Ошибка чтения База данных.xlsx: {e}")
            return False

        # Загружаем существующую статистику
        existing_df = self._load_existing_report(store_name, 'adverts')
        if not existing_df.empty:
            existing_df['Дата'] = pd.to_datetime(existing_df['Дата']).dt.strftime('%Y-%m-%d')
            existing_keys = set(zip(existing_df['ID кампании'], existing_df['Дата']))
        else:
            existing_keys = set()

        # Определяем диапазон дат за последние 30 дней (без сегодня)
        start_date, end_date = self._get_date_range_last_n_days(30)
        required_dates = [(start_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(30)]

        # Собираем недостающие комбинации (кампания, дата)
        missing = []
        for cid in campaign_ids:
            for d in required_dates:
                if (cid, d) not in existing_keys:
                    missing.append((cid, d))

        if not missing:
            self.log("✅ Все рекламные данные за последние 30 дней уже загружены")
            return True

        self.log(f"📊 Необходимо загрузить {len(missing)} записей (кампания, дата)")

        # Получаем актуальную информацию о кампаниях (названия, предметы) через advert-api
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

        # Группируем недостающие записи по датам
        missing_by_date = defaultdict(list)
        for cid, d in missing:
            missing_by_date[d].append(cid)

        # Будем накапливать данные и сохранять основной файл в конце
        current_df = existing_df.copy()
        loaded_days = 0
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
                            else:
                                self.log(f"    ℹ️ Нет данных за {date_str} для этой группы")
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
                current_df = pd.concat([current_df, day_df], ignore_index=True) if not current_df.empty else day_df
                loaded_days += 1
                # Сохраняем недельные файлы после каждого дня
                self._save_weekly(day_df, store_name, 'adverts', datetime.strptime(date_str, '%Y-%m-%d'))
            else:
                self.log(f"ℹ️ Нет новых данных за {date_str}")

            time.sleep(self.delays['adverts'])

        # После загрузки всех дней сохраняем основной файл с дополнительными отчётами
        if loaded_days > 0:
            self.log(f"💾 Сохраняем итоговый файл рекламы с {len(current_df)} записями...")
            extra_sheets = {}
            if not current_df.empty:
                daily_cat = current_df.groupby(['Дата', 'Название предмета']).agg({
                    'Показы': 'sum',
                    'Клики': 'sum',
                    'Заказы': 'sum',
                    'Расход': 'sum',
                    'Сумма заказов': 'sum'
                }).reset_index()
                daily_cat['CTR'] = (daily_cat['Клики'] / daily_cat['Показы'] * 100).round(2)
                daily_cat['CPC'] = (daily_cat['Расход'] / daily_cat['Клики']).round(2)
                daily_cat['CR'] = (daily_cat['Заказы'] / daily_cat['Клики'] * 100).round(2)
                daily_cat['ROI'] = ((daily_cat['Сумма заказов'] - daily_cat['Расход']) / daily_cat['Расход'] * 100).round(2)
                daily_cat['ДРР'] = (daily_cat['Расход'] / (daily_cat['Сумма заказов'] * 0.88) * 100).round(2)
                daily_cat = daily_cat.sort_values(['Дата', 'Расход'], ascending=[True, False])
                extra_sheets['Отчет_по_Категории'] = daily_cat

                summary_cat = current_df.groupby('Название предмета').agg({
                    'Показы': 'sum',
                    'Клики': 'sum',
                    'Заказы': 'sum',
                    'Расход': 'sum',
                    'Сумма заказов': 'sum'
                }).reset_index()
                summary_cat['CTR'] = (summary_cat['Клики'] / summary_cat['Показы'] * 100).round(2)
                summary_cat['CPC'] = (summary_cat['Расход'] / summary_cat['Клики']).round(2)
                summary_cat['CR'] = (summary_cat['Заказы'] / summary_cat['Клики'] * 100).round(2)
                summary_cat['ROI'] = ((summary_cat['Сумма заказов'] - summary_cat['Расход']) / summary_cat['Расход'] * 100).round(2)
                summary_cat['ДРР'] = (summary_cat['Расход'] / (summary_cat['Сумма заказов'] * 0.88) * 100).round(2)
                summary_cat = summary_cat.sort_values('Расход', ascending=False)
                extra_sheets['Отчет_по_Категории_Итог'] = summary_cat

            self._last_save_start = time.time()
            if self._save_report(current_df, store_name, 'adverts', extra_sheets):
                self.log(f"✅ Рекламные данные успешно обновлены, добавлено {loaded_days} дней")
            else:
                self.log("❌ Ошибка сохранения итогового файла")
                return False
        else:
            self.log("⚠️ Ни одного нового дня не загружено")

        return True

    # ---------- НОВЫЙ МЕТОД: Остатки из 1С ----------
    def update_1c_stocks(self, store_name: str = '1С') -> bool:
        """
        Загружает файл с остатками из 1С по внешнему URL (например, опубликованный HTTP-сервис)
        и сохраняет его в бакет Yandex Cloud в папку Отчёты/Остатки/1С/Остатки_1С.xlsx.
        Файл перезаписывается ежедневно.
        Для аутентификации используются переменные окружения:
        - URL_1C_STOCKS : ссылка на файл (Excel или CSV)
        - _1C_USER      : имя пользователя (если требуется базовая аутентификация)
        - _1C_PASSWORD  : пароль
        """
        self.log(f"\n📌 ОБНОВЛЕНИЕ: Остатки из 1С для магазина {store_name}")
        config = self.reports_config['1c_stocks']

        # Получаем настройки из переменных окружения
        url_1c = os.environ.get('URL_1C_STOCKS')
        username = os.environ.get('_1C_USER')
        password = os.environ.get('_1C_PASSWORD')

        if not url_1c:
            self.log("❌ Переменная окружения URL_1C_STOCKS не задана. Невозможно загрузить данные из 1С.")
            return False

        # Подготовка аутентификации (Basic Auth)
        auth = None
        if username and password:
            auth = (username, password)
            self.log(f"🔐 Используется базовая аутентификация для пользователя {username}")

        # Скачиваем файл
        try:
            self.log(f"📥 Скачивание файла из 1С: {url_1c}")
            resp = requests.get(url_1c, auth=auth, timeout=120, stream=True)
            if resp.status_code != 200:
                self.log(f"❌ Ошибка при скачивании: HTTP {resp.status_code}")
                return False

            # Сохраняем во временный файл
            with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
                tmp_path = tmp.name
                for chunk in resp.iter_content(chunk_size=8192):
                    tmp.write(chunk)
            self.log(f"📦 Файл временно сохранён: {tmp_path}")

            # Формируем ключ в бакете: Отчёты/Остатки/1С/Остатки_1С.xlsx
            key = self._get_s3_key(store_name, '1c_stocks')
            self.log(f"☁️ Загрузка в бакет: {key}")

            # Загружаем файл в S3
            self.s3.upload_file(tmp_path, key)
            self.log(f"✅ Файл успешно сохранён в бакет: {key}")

            # Можно также сохранить копию с датой, если нужно хранить историю
            # Но по условию задачи достаточно ежедневной перезаписи

            return True

        except Exception as e:
            self.log(f"❌ Исключение при обработке: {e}")
            traceback.print_exc()
            return False
        finally:
            # Удаляем временный файл
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
                self.log("🧹 Временный файл удалён")

    # ====================== ОСНОВНОЙ ЗАПУСК ======================

    def run_daily_update(self, store_name: str, reports: List[str] = None):
        """
        Запускает обновление для указанного магазина.
        Если reports = None, обновляются все отчёты, включая '1c_stocks'.
        """
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
            else:
                self.log(f"⚠️ Неизвестный тип отчёта: {report}")
            # Пауза между отчётами (кроме последнего)
            if report != reports[-1]:
                time.sleep(30)

        self.log("✅ Обновление завершено")


# ========================== ТОЧКА ВХОДА ==========================

if __name__ == "__main__":
    # Читаем переменные окружения
    required_env = [
        'YC_ACCESS_KEY_ID',
        'YC_SECRET_ACCESS_KEY',
        'YC_BUCKET_NAME',
        'WB_STATS_KEY_TOPFACE',
        'WB_PROMO_KEY_TOPFACE'
    ]
    # Для 1С обязательна только URL_1C_STOCKS, остальные опциональны
    # Проверяем только обязательные для Wildberries
    missing = [var for var in required_env if not os.environ.get(var)]
    if missing:
        print(f"❌ Отсутствуют переменные окружения: {missing}")
        exit(1)

    # Проверяем наличие URL для 1С (необязательно, но предупредим)
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
