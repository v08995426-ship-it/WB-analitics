#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Ежедневный сбор данных Wildberries с сохранением в Cloud.ru Object Storage.
Основной упор на отчёт "Позиции по ключам" (keywords).
"""

import os
import io
import json
import time
import uuid
import zipfile
import random
import tempfile
import traceback
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set, Tuple, Any
from pathlib import Path
import warnings

import pandas as pd
import requests
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
import pytz

warnings.simplefilter(action='ignore', category=FutureWarning)

# ========================== КЛАСС ДЛЯ РАБОТЫ С CLOUD.RU ==========================

class S3Storage:
    """Клиент для работы с S3-совместимым хранилищем Cloud.ru."""

    def __init__(self, tenant_id: str, access_key: str, secret_key: str, bucket_name: str):
        """
        :param tenant_id: ID тенанта (например, "3bd21226-c7be-4960-8403-9f8d48a5eaa2")
        :param access_key: Access Key (Key ID)
        :param secret_key: Secret Key
        :param bucket_name: имя бакета
        """
        self.bucket = bucket_name
        # Полный ключ доступа = tenant_id:access_key
        full_access_key = f"{tenant_id}:{access_key}"
        self.s3 = boto3.client(
            's3',
            endpoint_url='https://s3.cloud.ru',
            aws_access_key_id=full_access_key,
            aws_secret_access_key=secret_key,
            region_name='ru-central-1',
            config=Config(signature_version='s3v4')  # обязательная подпись для Cloud.ru
        )

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
            self.s3.upload_file(tmp_path, self.bucket, key)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

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
    Ежедневный сборщик данных Wildberries с хранением в S3 (Cloud.ru).
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

        # Конфигурация отчётов (имена файлов и папок)
        self.reports_config = {
            'orders': {
                'name': 'Заказы',
                'folder': 'Заказы',
                'filename': 'Заказы.xlsx',
                'date_column': 'date',
                'id_columns': ['date', 'gNumber', 'srid']
            },
            'stocks': {
                'name': 'История остатков',
                'folder': 'Остатки',
                'filename': 'Остатки.xlsx',
                'date_column': 'Дата',
                'id_columns': ['Дата', 'nmID', 'Артикул продавца']
            },
            'finance': {
                'name': 'Финансовые показатели',
                'folder': 'Финансовые показатели',
                'filename': 'Финансовые показатели.xlsx',
                'date_column': 'rr_dt',
                'id_columns': ['rr_dt', 'rrd_id', 'nm_id']
            },
            'keywords': {
                'name': 'Позиции по Ключам',
                'folder': 'Поисковые запросы',  # обратите внимание – папка для этого отчёта
                'filename': 'Позиции по Ключам.xlsx',
                'date_column': 'Дата',
                'id_columns': ['Дата', 'Поисковый запрос', 'Артикул WB', 'Фильтр']
            },
            'funnel': {
                'name': 'Воронка продаж',
                'folder': 'Воронка продаж',
                'filename': 'Воронка продаж.xlsx',
                'date_column': 'dt',
                'id_columns': ['dt', 'nmID']
            }
        }

        # Задержки между запросами (секунды)
        self.delays = {
            'orders': 65,
            'stocks': 20,
            'finance': 65,
            'keywords': 70,
            'funnel': 30
        }

        self.log(f"🚀 Запуск обновления данных. Время: {self.start_time}")

    # ====================== ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ======================

    def log(self, message: str, level: str = "INFO"):
        """Простое логирование в stdout."""
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [{level}] {message}")

    def _get_s3_key(self, store_name: str, report_type: str, filename: Optional[str] = None) -> str:
        """Формирует ключ (путь) в бакете для указанного отчёта."""
        config = self.reports_config[report_type]
        folder = config['folder']
        if filename is None:
            filename = config['filename']
        return f"Отчёты/{folder}/{store_name}/{filename}"

    def _get_weekly_folder_prefix(self, store_name: str) -> str:
        """Возвращает префикс для папки с понедельными файлами (для keywords)."""
        config = self.reports_config['keywords']
        return f"Отчёты/{config['folder']}/{store_name}/Поисковые запросы понедельно/"

    def _get_weekly_key(self, date_str: str, store_name: str) -> str:
        """Генерирует ключ для недельного файла (например, Неделя 2025-W10.xlsx)."""
        date = datetime.strptime(date_str, '%Y-%m-%d')
        year, week, _ = date.isocalendar()
        filename = f"Неделя {year}-W{week:02d}.xlsx"
        return self._get_weekly_folder_prefix(store_name) + filename

    def _load_existing_report(self, store_name: str, report_type: str) -> pd.DataFrame:
        """Загружает существующий отчёт из бакета."""
        key = self._get_s3_key(store_name, report_type)
        self.log(f"📥 Загрузка отчёта {report_type} для {store_name} из {key}")
        try:
            df = self.s3.read_excel(key, sheet_name=self.reports_config[report_type]['name'])
            if df.empty:
                self.log("ℹ️ Файл не найден или пуст, будет создан новый")
                return pd.DataFrame()
            # Приводим колонку с датой к строковому формату
            date_col = self.reports_config[report_type]['date_column']
            if date_col in df.columns:
                df[date_col] = pd.to_datetime(df[date_col]).dt.strftime('%Y-%m-%d')
            return df
        except Exception as e:
            self.log(f"⚠️ Ошибка загрузки {key}: {e}")
            return pd.DataFrame()

    def _save_report(self, df: pd.DataFrame, store_name: str, report_type: str) -> bool:
        """Сохраняет отчёт в бакет (перезапись)."""
        key = self._get_s3_key(store_name, report_type)
        config = self.reports_config[report_type]

        # Дедупликация
        before = len(df)
        if config['id_columns'] and not df.empty:
            existing_cols = [c for c in config['id_columns'] if c in df.columns]
            if existing_cols:
                df = df.drop_duplicates(subset=existing_cols, keep='last')
                after = len(df)
                if before > after:
                    self.log(f"🔍 Удалено дубликатов: {before - after}")

        # Удаление устаревших данных (старше 90 дней)
        df = self._remove_outdated_data(df, config)

        try:
            self.s3.write_excel(key, df, sheet_name=config['name'])
            self.log(f"✅ Отчёт сохранён: {key}, записей: {len(df)}")
            return True
        except Exception as e:
            self.log(f"❌ Ошибка сохранения {key}: {e}")
            return False

    def _remove_outdated_data(self, df: pd.DataFrame, config: dict) -> pd.DataFrame:
        """Удаляет строки с датой старше 90 дней."""
        if df.empty:
            return df
        date_col = config['date_column']
        if date_col not in df.columns:
            return df
        start_date, end_date = self._get_date_range_90_days()
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

    def _get_articles_from_orders(self, store_name: str) -> List[int]:
        """Извлекает уникальные nmId из файла заказов магазина."""
        orders_key = self._get_s3_key(store_name, 'orders')
        try:
            df = self.s3.read_excel(orders_key, sheet_name='Заказы')
            if df.empty:
                self.log("⚠️ Файл заказов пуст или не найден")
                return []
            # Ищем колонку с артикулом
            possible_cols = ['nmId', 'nmID', 'Артикул WB', 'Артикул']
            col = None
            for c in possible_cols:
                if c in df.columns:
                    col = c
                    break
            if col is None:
                self.log("⚠️ В заказах не найдена колонка с артикулом")
                return []
            articles = df[col].dropna().unique()
            articles = [int(a) for a in articles if pd.notna(a)]
            self.log(f"✅ Получено {len(articles)} артикулов из заказов")
            return articles
        except Exception as e:
            self.log(f"❌ Ошибка чтения заказов: {e}")
            return []

    # ====================== МЕТОДЫ ДЛЯ КЛЮЧЕЙ (KEYWORDS) ======================

    def _make_keywords_request(self, headers: dict, date_str: str, nm_ids: List[int], filter_field: str) -> Optional[Dict]:
        """Выполняет запрос к API поисковых запросов."""
        url = "https://seller-analytics-api.wildberries.ru/api/v2/search-report/product/search-texts"
        payload = {
            "currentPeriod": {"start": date_str, "end": date_str},
            "nmIds": nm_ids,
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
                    if "data" in data and "items" in data["data"]:
                        return data
                    else:
                        self.log("    ⚠ Неожиданная структура ответа")
                        return None
                elif resp.status_code == 429:
                    wait = 60 * (attempt + 1)
                    self.log(f"    ⚠ Лимит, ждём {wait} сек...")
                    time.sleep(wait)
                else:
                    self.log(f"    ❌ Ошибка {resp.status_code}")
                    return None
            except Exception as e:
                self.log(f"    ❌ Исключение: {e}")
                if attempt < max_retries - 1:
                    time.sleep(10)
                else:
                    return None
        return None

    def _extract_keywords_data(self, response: Dict, date_str: str, store_name: str, filter_field: str) -> List[Dict]:
        """Извлекает данные из ответа API."""
        extracted = []
        items = response.get("data", {}).get("items", [])
        for item in items:
            text = item.get("text", "").strip()
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
            extracted.append(row)
        return extracted

    def _save_to_weekly(self, data: List[Dict], store_name: str):
        """Сохраняет данные в недельные файлы (для keywords)."""
        if not data:
            return
        df = pd.DataFrame(data)
        # Группируем по неделям
        by_week = {}
        for _, row in df.iterrows():
            date_str = row['Дата']
            week_key = self._get_weekly_key(date_str, store_name)
            if week_key not in by_week:
                by_week[week_key] = []
            by_week[week_key].append(row.to_dict())

        for week_key, week_data in by_week.items():
            week_df = pd.DataFrame(week_data)
            # Загружаем существующий недельный файл
            try:
                existing = self.s3.read_excel(week_key)
            except:
                existing = pd.DataFrame()
            # Объединяем и дедуплицируем
            if not existing.empty:
                combined = pd.concat([existing, week_df], ignore_index=True)
                id_cols = self.reports_config['keywords']['id_columns']
                id_cols_present = [c for c in id_cols if c in combined.columns]
                if id_cols_present:
                    combined = combined.drop_duplicates(subset=id_cols_present, keep='last')
            else:
                combined = week_df
            # Сохраняем
            self.s3.write_excel(week_key, combined)
            self.log(f"💾 Недельные данные сохранены: {week_key}")

    def update_keywords(self, store_name: str) -> bool:
        """Инкрементальное обновление данных по поисковым запросам."""
        self.log(f"\n📌 ОБНОВЛЕНИЕ: Позиции по ключам для магазина {store_name}")

        # 1. Получаем артикулы из заказов
        articles = self._get_articles_from_orders(store_name)
        if not articles:
            self.log("⚠️ Нет артикулов из заказов, используем резервный список")
            articles = [87705142, 110254021, 118217701, 111944302, 110561153]

        # 2. Загружаем существующие данные
        existing_df = self._load_existing_report(store_name, 'keywords')
        if existing_df.empty:
            self.log("📊 Существующий файл пуст, будет создан новый")
        else:
            self.log(f"📊 Загружено существующих записей: {len(existing_df)}")

        # 3. Определяем даты для загрузки (последние 90 дней, кроме сегодня)
        start_date, end_date = self._get_date_range_90_days()
        all_dates = []
        current = start_date
        while current <= end_date:
            all_dates.append(current.strftime('%Y-%m-%d'))
            current += timedelta(days=1)

        filters = ["orders", "openCard", "addToCart"]

        # 4. Проверяем, какие комбинации (дата, артикул, фильтр) отсутствуют
        missing_by_date = {}  # {дата: [артикулы]}
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

        # 5. Получаем API-ключ
        api_key = self.api_keys.get(store_name, {}).get('promo')
        if not api_key:
            self.log("❌ Нет API-ключа для отчёта 'Позиции по Ключам'")
            return False
        headers = {"Authorization": f"Bearer {api_key.strip()}", "Content-Type": "application/json"}

        # 6. Основной цикл загрузки
        errors = []  # (дата, артикул, фильтр)
        successful_days = 0

        for date_idx, date_str in enumerate(sorted_dates, 1):
            articles_for_date = missing_by_date[date_str]
            self.log(f"📅 День {date_idx}/{len(sorted_dates)}: {date_str}, артикулов: {len(articles_for_date)}")

            # Разбиваем на батчи по 50
            batches = [articles_for_date[i:i+50] for i in range(0, len(articles_for_date), 50)]
            for batch_idx, batch in enumerate(batches, 1):
                self.log(f"  📦 Батч {batch_idx}/{len(batches)}: {len(batch)} артикулов")
                batch_data = []

                for filter_field in filters:
                    self.log(f"    🔍 Фильтр {filter_field}", end="")
                    resp = self._make_keywords_request(headers, date_str, batch, filter_field)
                    if resp:
                        data = self._extract_keywords_data(resp, date_str, store_name, filter_field)
                        batch_data.extend(data)
                        self.log(f" -> ✓ {len(data)} записей")
                    else:
                        for nm_id in batch:
                            errors.append((date_str, nm_id, filter_field))
                        self.log(" -> ❌ ошибка")

                    # Пауза между фильтрами
                    if filter_field != filters[-1]:
                        time.sleep(20)

                # Сохраняем данные батча
                if batch_data:
                    new_df = pd.DataFrame(batch_data)
                    if not existing_df.empty:
                        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                    else:
                        combined_df = new_df
                    if self._save_report(combined_df, store_name, 'keywords'):
                        existing_df = combined_df
                    # Сохраняем также в недельные файлы
                    self._save_to_weekly(batch_data, store_name)

                # Пауза между батчами
                if batch_idx < len(batches):
                    self.log("    ⏳ Пауза 20 сек между батчами...")
                    time.sleep(20)

            successful_days += 1
            # Пауза между днями
            if date_idx < len(sorted_dates):
                self.log("⏳ Пауза 20 сек между днями...")
                time.sleep(20)

        if errors:
            self.log(f"\n⚠ Зафиксировано {len(errors)} ошибок. Повторные попытки не реализованы, но можно добавить позже.")
        else:
            self.log("✅ Все данные успешно загружены")
        return True

    # ====================== ЗАГОТОВКИ ДЛЯ ДРУГИХ ОТЧЁТОВ ======================
    # (пока просто заглушки, чтобы можно было запустить)

    def update_orders(self, store_name: str) -> bool:
        self.log(f"⚠️ Метод update_orders пока не реализован, пропускаем")
        return True

    def update_stocks(self, store_name: str) -> bool:
        self.log(f"⚠️ Метод update_stocks пока не реализован, пропускаем")
        return True

    def update_finance(self, store_name: str) -> bool:
        self.log(f"⚠️ Метод update_finance пока не реализован, пропускаем")
        return True

    def update_funnel(self, store_name: str) -> bool:
        self.log(f"⚠️ Метод update_funnel пока не реализован, пропускаем")
        return True

    # ====================== ОСНОВНОЙ ЗАПУСК ======================

    def run_daily_update(self, store_name: str, reports: List[str] = None):
        """
        Запускает обновление для указанного магазина.
        Если reports = None, обновляются все отчёты.
        """
        all_reports = ['orders', 'stocks', 'finance', 'keywords', 'funnel']
        if reports is None:
            reports = all_reports

        self.log(f"🚀 Начало обновления для магазина {store_name}")
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
            time.sleep(30)  # пауза между разными отчётами

        self.log("✅ Обновление завершено")


# ========================== ТОЧКА ВХОДА ==========================

if __name__ == "__main__":
    # Читаем все необходимые переменные окружения
    required_env = [
    'CLOUD_RU_TENANT_ID',
    'CLOUD_RU_ACCESS_KEY',
    'CLOUD_RU_SECRET_KEY',
    'CLOUD_RU_BUCKET',
    'WB_STATS_KEY_TOPFACE',
    'WB_PROMO_KEY_TOPFACE'
]
    missing = [var for var in required_env if not os.environ.get(var)]
    if missing:
        print(f"❌ Отсутствуют переменные окружения: {missing}")
        print("Убедитесь, что они заданы (в GitHub Secrets или локально в .env)")
        exit(1)

    # Создаём экземпляр S3Storage
    s3 = S3Storage(
        tenant_id=os.environ['CLOUD_RU_TENANT_ID'],
        access_key=os.environ['CLOUD_RU_ACCESS_KEY'],
        secret_key=os.environ['CLOUD_RU_SECRET_KEY'],
        bucket_name=os.environ['CLOUD_RU_BUCKET']
    )

    # Формируем словарь с ключами Wildberries
    api_keys = {
    'TOPFACE': {
        'stats': os.environ['WB_STATS_KEY_TOPFACE'],
        'promo': os.environ['WB_PROMO_KEY_TOPFACE']
    }
}
        # При необходимости добавьте другие магазины
    }

    # Создаём обновлятор и запускаем
    updater = WildberriesDailyUpdater(api_keys, s3)
    # Можно передать имя магазина из аргументов командной строки
    import sys
    store = sys.argv[1] if len(sys.argv) > 1 else 'TOPFACE'
    updater.run_daily_update(store)
