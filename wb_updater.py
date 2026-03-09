import os
import pandas as pd
import glob
import re
import time
import threading
import tkinter as tk
from tkinter import ttk, messagebox
from pathlib import Path
import openai
import json
import traceback
from collections import defaultdict

# ================== НАСТРОЙКИ ==================
PATH_B = r"C:\Users\Владислав\Documents\ЭКСПЕРТ\ХАЙЛЕР\Аналитика\Запросы с ВБ"
OUTPUT_DIR = r"C:\Users\Владислав\Documents\ЭКСПЕРТ\ХАЙЛЕР\Аналитика\Отчёты\Рекомендации по СЕО"
PATH_MARKETING = os.path.join(OUTPUT_DIR, "Маркетинговое описание")

# Ключи YandexGPT
FOLDER_ID = "b1g949oa48c83q9ms028"
API_KEY = "AQVN2Vg7gxC9NDjSrmjYYq_I3MmZqaqpsQ3Pq4U1"

# Идентификаторы промптов
PROMPT_ID_FILTER = "fvttpnqdi9b8k8va4hph"
PROMPT_ID_GENERATE = "fvttpnqdi9b8k8va4hph"

# Увеличиваем размер батча до 200 для ускорения
MAX_QUERIES_PER_BATCH = 200
TEST_MODE = False

Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
Path(PATH_MARKETING).mkdir(parents=True, exist_ok=True)

# ================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==================
def sanitize_sheet_name(name):
    return re.sub(r'[\\/\?*\[\]]', '_', str(name))[:31]

def extract_json(text):
    text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
    text = re.sub(r'^```(?:json)?\n?', '', text)
    text = re.sub(r'\n?```$', '', text)
    start = text.find('{')
    end = text.rfind('}') + 1
    if start == -1 or end <= start:
        start = text.find('[')
        end = text.rfind(']') + 1
        if start == -1 or end <= start:
            return None
    json_str = text[start:end]
    try:
        return json.loads(json_str)
    except json.JSONDecodeError as e:
        error_pos = e.pos
        print(f"JSON error at position {error_pos}: {e}")
        print("Fragment around error:", json_str[max(0, error_pos-100):error_pos+100])
        return None

def extract_text_from_response(response):
    if hasattr(response, 'output_text') and response.output_text:
        return response.output_text
    if hasattr(response, 'output') and response.output:
        collected_texts = []
        for item in response.output:
            if hasattr(item, 'type') and item.type == 'reasoning' and hasattr(item, 'summary') and item.summary:
                for summary_item in item.summary:
                    if hasattr(summary_item, 'text') and summary_item.text:
                        collected_texts.append(summary_item.text)
            if hasattr(item, 'content') and item.content:
                for content_item in item.content:
                    if hasattr(content_item, 'text') and content_item.text:
                        collected_texts.append(content_item.text)
        if collected_texts:
            return "\n".join(collected_texts)
    if hasattr(response, 'choices') and response.choices:
        try:
            return response.choices[0].message.content
        except (AttributeError, IndexError):
            pass
    return None

def call_ai_agent(prompt_id, user_input, timeout=120):
    client = openai.OpenAI(
        api_key=API_KEY,
        base_url="https://ai.api.cloud.yandex.net/v1",
        project=FOLDER_ID,
        timeout=timeout
    )
    try:
        print(f"Отправка запроса к ИИ с prompt_id={prompt_id}, длина ввода={len(user_input)} символов")
        response = client.responses.create(
            prompt={"id": prompt_id},
            input=user_input,
            max_output_tokens=7000,
            temperature=0.1,
            reasoning={"effort": "none"}
        )
        if hasattr(response, 'incomplete_details') and response.incomplete_details:
            print(f"ПРЕДУПРЕЖДЕНИЕ: неполный ответ - {response.incomplete_details}")
        text = extract_text_from_response(response)
        if text:
            print(f"Длина полученного ответа: {len(text)} символов")
            return text
        else:
            print("Не удалось извлечь текст из ответа.")
            return None
    except Exception as e:
        print(f"Ошибка при вызове ИИ-агента: {e}")
        traceback.print_exc()
        return None

# ================== РАБОТА С ФАЙЛАМИ ==================
def get_category_from_excel(file_path):
    try:
        df_info = pd.read_excel(file_path, sheet_name="Общая информация", header=None)
        mask = df_info[0].astype(str).str.contains("Предмет", na=False)
        if mask.any():
            predmet = str(df_info[mask].iloc[0, 1]).strip()
            if predmet:
                return predmet
    except:
        pass
    base = os.path.basename(file_path)
    match = re.search(r'\d{1,2}-\d{1,2}-\d{4}\s+(.+?)\s+с\s+', base)
    if match:
        return match.group(1).strip()
    return os.path.splitext(base)[0]

def get_categories_from_marketing():
    excel_files = glob.glob(os.path.join(PATH_MARKETING, "*.xlsx")) + glob.glob(os.path.join(PATH_MARKETING, "*.xls"))
    categories = [os.path.splitext(os.path.basename(f))[0] for f in excel_files]
    return categories

def get_marketing_descriptions(category):
    marketing_file = os.path.join(PATH_MARKETING, f"{category}.xlsx")
    if not os.path.exists(marketing_file):
        marketing_file = os.path.join(PATH_MARKETING, f"{category}.xls")
        if not os.path.exists(marketing_file):
            return None
    try:
        xl = pd.ExcelFile(marketing_file)
        sheets = xl.sheet_names
        descriptions = {}
        for sheet in sheets:
            df = pd.read_excel(marketing_file, sheet_name=sheet, header=None)
            if not df.empty and pd.notna(df.iloc[0, 0]):
                desc = str(df.iloc[0, 0]).strip()
                if desc:
                    descriptions[sheet] = desc
        return descriptions
    except Exception as e:
        print(f"Ошибка чтения маркетинговых описаний: {e}")
        return None

def read_detailed_info_from_excel(file_path):
    try:
        df_detail = pd.read_excel(file_path, sheet_name="Детальная информация", header=1)
        df_detail = df_detail.rename(columns={
            'Поисковый запрос': 'query',
            'Количество запросов': 'freq',
            'Конверсия в заказ': 'conv'
        })
        df_detail = df_detail[['query', 'freq', 'conv']].dropna(subset=['query'])
        df_detail['query'] = df_detail['query'].astype(str).str.strip()
        df_detail['freq'] = pd.to_numeric(df_detail['freq'], errors='coerce')
        df_detail['conv'] = pd.to_numeric(df_detail['conv'], errors='coerce')
        return df_detail
    except Exception as e:
        print(f"Ошибка чтения {file_path}: {e}")
        return pd.DataFrame()

def filter_by_strategy(df, strategy, thresholds=None):
    if strategy == 1:
        mask = (df['freq'] >= 500) & (df['conv'] >= 15)
    elif strategy == 2:
        if thresholds is None:
            return df
        min_freq, min_conv = thresholds
        mask = (df['freq'] >= min_freq) & (df['conv'] >= min_conv)
    else:
        mask = pd.Series([True] * len(df), index=df.index)
    return df[mask].copy()

# ================== ЭТАП 1: ФИЛЬТРАЦИЯ (ИИ) ==================
def filter_queries_with_ai(queries, category):
    if not queries:
        return None, None

    if TEST_MODE:
        queries = queries[:5]
        print(f"ТЕСТОВЫЙ РЕЖИМ: обрабатываем только {len(queries)} запросов")

    if len(queries) > MAX_QUERIES_PER_BATCH:
        print(f"Слишком много запросов ({len(queries)}). Разбиваем на батчи по {MAX_QUERIES_PER_BATCH}.")
        all_results = []
        all_unique = defaultdict(list)
        total_batches = (len(queries) + MAX_QUERIES_PER_BATCH - 1) // MAX_QUERIES_PER_BATCH
        processed_queries = set()
        any_success = False

        for i in range(0, len(queries), MAX_QUERIES_PER_BATCH):
            batch = queries[i:i+MAX_QUERIES_PER_BATCH]
            batch = [q for q in batch if q not in processed_queries]
            if not batch:
                print(f"Батч {i//MAX_QUERIES_PER_BATCH + 1} не содержит новых запросов, пропускаем")
                continue
            print(f"Батч {i//MAX_QUERIES_PER_BATCH + 1}/{total_batches} ({len(batch)} запросов)")
            results, unique = filter_queries_with_ai_single_batch(batch, category, batch_num=i//MAX_QUERIES_PER_BATCH + 1)
            if results is None:
                print(f"Ошибка при обработке батча {i//MAX_QUERIES_PER_BATCH + 1}, пропускаем")
                continue
            any_success = True
            for r in results:
                if 'query' in r:
                    processed_queries.add(r['query'])
            all_results.extend(results)
            if unique:
                for subject, props in unique.items():
                    all_unique[subject].extend(props)

        if not any_success:
            return None, None

        for subject in all_unique:
            all_unique[subject] = list(set(all_unique[subject]))
        return all_results, dict(all_unique)
    else:
        return filter_queries_with_ai_single_batch(queries, category, batch_num=1)

def extract_from_standard_array(parsed):
    """
    Ищет в parsed массив объектов, содержащих запрос и метку.
    Возвращает (results, unique) или (None, None).
    """
    result_keys = ['filtered_queries', 'search_queries', 'Поисковые запросы', 'results', 'queries', 'filters']
    query_keys = ['query', 'запрос', 'Запрос']
    label_keys = ['label', 'метка', 'tag', 'Метка', 'status']

    results = None
    for key in result_keys:
        if key in parsed and isinstance(parsed[key], list):
            if len(parsed[key]) > 0 and isinstance(parsed[key][0], dict):
                sample = parsed[key][0]
                has_query = any(qk in sample for qk in query_keys)
                has_label = any(lk in sample for lk in label_keys)
                if has_query and has_label:
                    results = []
                    for item in parsed[key]:
                        new_item = {}
                        for qk in query_keys:
                            if qk in item:
                                new_item['query'] = item[qk]
                                break
                        for lk in label_keys:
                            if lk in item:
                                new_item['label'] = item[lk]
                                break
                        results.append(new_item)
                    print(f"Найден массив результатов по ключу '{key}'")
                    break

    unique = None
    unique_keys = ['unique_words_and_phrases', 'Уникальные слова и фразы', 'unique_words_phrases']
    for key in unique_keys:
        if key in parsed and isinstance(parsed[key], dict):
            unique = parsed[key]
            print(f"Найден блок unique по ключу '{key}'")
            break

    return results, unique

def extract_from_dict_map(parsed):
    """
    Обрабатывает случай, когда в ответе есть словарь вида {запрос: метка, ...}
    Возвращает results или None.
    """
    if not isinstance(parsed, dict):
        return None
    results = []
    for key, value in parsed.items():
        if isinstance(key, str) and isinstance(value, str) and value in ['Бренд', 'Дубль', 'Рабочий', 'Цвет', 'Набор', 'География']:
            results.append({'query': key, 'label': value})
        else:
            # Если хотя бы один элемент не подходит, прерываем (это не тот формат)
            return None
    if results:
        print(f"Извлечено {len(results)} записей из словаря запрос:метка")
        return results
    return None

def filter_queries_with_ai_single_batch(queries, category, batch_num):
    max_retries = 3
    for attempt in range(max_retries):
        if attempt > 0:
            print(f"Повторная попытка {attempt+1} для батча {batch_num}")
            time.sleep(2)

        input_text = "Список поисковых запросов:\n" + "\n".join(queries)
        input_text += "\n\nВерни результат строго в формате JSON, как описано в инструкции."
        answer = call_ai_agent(PROMPT_ID_FILTER, input_text, timeout=600)
        if not answer:
            if attempt == max_retries - 1:
                print(f"Не удалось получить ответ от ИИ для батча {batch_num} после {max_retries} попыток")
                return None, None
            continue

        debug_file = os.path.join(OUTPUT_DIR, f"debug_response_batch_{batch_num}.json")
        with open(debug_file, "w", encoding="utf-8") as f:
            f.write(answer)
        parsed = extract_json(answer)
        if not parsed:
            if attempt == max_retries - 1:
                print(f"Не удалось распарсить JSON для батча {batch_num} после {max_retries} попыток")
                return None, None
            continue

        print("Ключи верхнего уровня JSON:", list(parsed.keys()))

        # 1. Пытаемся извлечь из стандартного массива
        results, unique = extract_from_standard_array(parsed)
        if results is not None:
            return results, unique

        # 2. Пытаемся извлечь из словаря {запрос: метка}
        results_dict = extract_from_dict_map(parsed)
        if results_dict is not None:
            unique = None
            for key in ['unique_words_and_phrases', 'Уникальные слова и фразы', 'unique_words_phrases']:
                if key in parsed and isinstance(parsed[key], dict):
                    unique = parsed[key]
                    break
            return results_dict, unique

        # 3. Проверяем формат с ключом "Этап 1"
        if 'Этап 1' in parsed and isinstance(parsed['Этап 1'], dict):
            stage1 = parsed['Этап 1']
            results, unique = extract_from_standard_array(stage1)
            if results is not None:
                return results, unique
            results_dict = extract_from_dict_map(stage1)
            if results_dict is not None:
                return results_dict, find_unique_dict(stage1)

        # 4. Рекурсивный поиск
        results = find_query_list(parsed)
        if results is not None:
            unique = find_unique_dict(parsed)
            return results, unique

        # Проверяем, не является ли ответ слишком коротким (возможно, агент вернул пустой массив)
        if len(answer) < 100:
            print(f"Ответ для батча {batch_num} слишком короткий, возможно, пустой результат")
            return [], None  # возвращаем пустой список, чтобы не прерывать обработку

        if attempt == max_retries - 1:
            print(f"Не удалось найти список результатов в ответе для батча {batch_num}")
            return None, None

    return None, None

def find_query_list(obj, path=""):
    if isinstance(obj, dict):
        for key, value in obj.items():
            new_path = f"{path}.{key}" if path else key
            if isinstance(value, list) and len(value) > 0:
                first = value[0]
                if isinstance(first, dict) and ('query' in first or 'запрос' in first):
                    print(f"Найден список по пути {new_path}")
                    new_list = []
                    for item in value:
                        if 'запрос' in item:
                            item['query'] = item.pop('запрос')
                        if 'метка' in item:
                            item['label'] = item.pop('метка')
                        new_list.append(item)
                    return new_list
            res = find_query_list(value, new_path)
            if res is not None:
                return res
    elif isinstance(obj, list):
        if len(obj) > 0 and isinstance(obj[0], dict) and ('query' in obj[0] or 'запрос' in obj[0]):
            new_list = []
            for item in obj:
                if 'запрос' in item:
                    item['query'] = item.pop('запрос')
                if 'метка' in item:
                    item['label'] = item.pop('метка')
                new_list.append(item)
            return new_list
        for idx, item in enumerate(obj):
            res = find_query_list(item, path + f"[{idx}]")
            if res is not None:
                return res
    return None

def find_unique_dict(obj):
    if isinstance(obj, dict):
        if obj and all(isinstance(v, list) for v in obj.values()):
            return obj
        for key, value in obj.items():
            res = find_unique_dict(value)
            if res is not None:
                return res
    elif isinstance(obj, list):
        for item in obj:
            res = find_unique_dict(item)
            if res is not None:
                return res
    return None

def run_stage1(category, strategy, thresholds, progress_callback):
    categories_dict = {}
    excel_files = glob.glob(os.path.join(PATH_B, "*.xlsx")) + glob.glob(os.path.join(PATH_B, "*.xls"))
    for file_path in excel_files:
        cat = get_category_from_excel(file_path)
        if cat:
            categories_dict.setdefault(cat, []).append(file_path)

    if category not in categories_dict:
        return "Категория не найдена в папке с запросами."

    file_paths = categories_dict[category]
    all_dfs = []
    for file_path in file_paths:
        df = read_detailed_info_from_excel(file_path)
        if not df.empty:
            all_dfs.append(df)
    if not all_dfs:
        return "Нет данных по запросам для этой категории."

    df_combined = pd.concat(all_dfs, ignore_index=True)
    df_filtered = filter_by_strategy(df_combined, strategy, thresholds)
    unique_queries = df_filtered['query'].unique().tolist()
    progress_callback(f"Найдено {len(unique_queries)} уникальных запросов после фильтрации. Отправка в ИИ...")

    results, unique_dict = filter_queries_with_ai(unique_queries, category)
    if results is None:
        return "Не удалось получить ответ от ИИ."

    df_results = pd.DataFrame(results)
    print("Столбцы df_results:", df_results.columns.tolist())
    if 'query' not in df_results.columns:
        return "В ответе ИИ отсутствует колонка 'query'."

    label_col = None
    possible_label_names = ['label', 'метка', 'tag', 'status']
    for col in df_results.columns:
        if any(name in col for name in possible_label_names):
            label_col = col
            break

    if label_col is not None:
        if label_col != 'label':
            df_results.rename(columns={label_col: 'label'}, inplace=True)
            print(f"Переименовали колонку {label_col} в 'label'")
    else:
        print("ВНИМАНИЕ: не найдена колонка с метками. Все запросы будут помечены как 'Рабочий'.")
        df_results['label'] = 'Рабочий'

    # Теперь у нас могут быть метки: Бренд, Дубль, Рабочий, Цвет, Набор, География
    # Создаём отдельные датафреймы для каждой метки (для удобства, но можно объединить все в рабочие)
    df_brand = df_results[df_results['label'] == 'Бренд'].copy()
    df_duplicate = df_results[df_results['label'] == 'Дубль'].copy()
    df_working = df_results[df_results['label'] == 'Рабочий'].copy()
    df_color = df_results[df_results['label'] == 'Цвет'].copy()
    df_set = df_results[df_results['label'] == 'Набор'].copy()
    df_geo = df_results[df_results['label'] == 'География'].copy()

    # Объединяем все метки, кроме Бренд и Дубль, в рабочие запросы (если нужно, можно оставить отдельно)
    # Здесь для простоты добавим все в рабочие, чтобы они были доступны для ручного отбора
    df_working = pd.concat([df_working, df_color, df_set, df_geo], ignore_index=True)

    df_merged = df_results.merge(df_filtered, on='query', how='left')
    df_merged.drop_duplicates(subset=['query'], inplace=True)

    unique_rows = []
    if unique_dict and isinstance(unique_dict, dict):
        for subject, props in unique_dict.items():
            if isinstance(props, list):
                for prop in props:
                    unique_rows.append({'Предмет': subject, 'Свойство': prop})
    df_unique = pd.DataFrame(unique_rows)

    output_filename = os.path.join(OUTPUT_DIR, f"Обработка_{sanitize_sheet_name(category)}.xlsx")
    try:
        with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
            df_merged.to_excel(writer, sheet_name='Все запросы', index=False)
            if not df_working.empty:
                df_working.to_excel(writer, sheet_name='Рабочие запросы', index=False)
            if not df_brand.empty:
                df_brand.to_excel(writer, sheet_name='Брендовые', index=False)
            if not df_duplicate.empty:
                df_duplicate.to_excel(writer, sheet_name='Дубли', index=False)
            if not df_unique.empty:
                df_unique.to_excel(writer, sheet_name='Уникальные слова и фразы', index=False)
        return f"✅ Фильтрация завершена. Файл сохранён:\n{output_filename}"
    except Exception as e:
        return f"Ошибка сохранения: {e}"

# ================== ЭТАП 2: РУЧНОЙ ОТБОР ==================
def assign_subject(query, df_unique):
    query_lower = query.lower()
    subjects = df_unique['Предмет'].unique()
    for subj in subjects:
        props = df_unique[df_unique['Предмет'] == subj]['Свойство'].tolist()
        if subj.lower() in query_lower:
            return subj
        for prop in props:
            if prop.lower() in query_lower:
                return subj
    return "Прочее"

class QueryReviewWindow:
    def __init__(self, master, category, sku, df_queries, df_unique, output_file, on_close):
        self.master = master
        self.category = category
        self.sku = sku
        self.df_queries = df_queries.copy()
        self.df_unique = df_unique
        self.output_file = output_file
        self.on_close = on_close
        self.checkboxes = []
        self.subject_frames = {}

        self.df_queries['subject'] = self.df_queries['query'].apply(lambda q: assign_subject(q, df_unique))
        self.df_queries.sort_values(['subject', 'freq'], ascending=[True, False], inplace=True)

        self.setup_ui()

    def setup_ui(self):
        self.master.title(f"Отбор запросов - {self.category} - {self.sku}")
        self.master.geometry("1200x800")

        main_canvas = tk.Canvas(self.master)
        scrollbar = ttk.Scrollbar(self.master, orient="vertical", command=main_canvas.yview)
        scrollable_frame = ttk.Frame(main_canvas)

        scrollable_frame.bind("<Configure>", lambda e: main_canvas.configure(scrollregion=main_canvas.bbox("all")))
        main_canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        main_canvas.configure(yscrollcommand=scrollbar.set)

        btn_frame = ttk.Frame(self.master)
        btn_frame.pack(side=tk.BOTTOM, fill=tk.X, padx=10, pady=10)
        ttk.Button(btn_frame, text="Сохранить и закрыть", command=self.save_and_close).pack(side=tk.RIGHT, padx=5)
        ttk.Button(btn_frame, text="Отмена", command=self.master.destroy).pack(side=tk.RIGHT, padx=5)

        header = ttk.Frame(scrollable_frame)
        header.pack(fill=tk.X, padx=5, pady=5)
        tk.Label(header, text="✓", width=3, font=("Arial", 12, "bold")).grid(row=0, column=0)
        tk.Label(header, text="Поисковый запрос", width=50, font=("Arial", 12, "bold")).grid(row=0, column=1)
        tk.Label(header, text="Частота", width=15, font=("Arial", 12, "bold")).grid(row=0, column=2)
        tk.Label(header, text="Конверсия", width=15, font=("Arial", 12, "bold")).grid(row=0, column=3)

        current_subject = None
        for idx, row in self.df_queries.iterrows():
            subject = row['subject']
            if subject != current_subject:
                frame = tk.LabelFrame(scrollable_frame, text=f"Предмет: {subject}", font=("Arial", 11, "bold"))
                frame.pack(fill=tk.X, padx=5, pady=(10,0))
                self.subject_frames[subject] = frame
                current_subject = subject
            else:
                frame = self.subject_frames[subject]

            row_frame = ttk.Frame(frame)
            row_frame.pack(fill=tk.X, padx=10, pady=2)

            var = tk.BooleanVar(value=False)
            chk = tk.Checkbutton(row_frame, variable=var, width=3)
            chk.grid(row=0, column=0)
            self.checkboxes.append((var, idx))

            tk.Label(row_frame, text=row['query'], font=("Arial", 11), anchor="w", width=80).grid(row=0, column=1, sticky="w")
            tk.Label(row_frame, text=str(row['freq']), font=("Arial", 11), width=15).grid(row=0, column=2)
            conv_val = f"{row['conv']:.2f}" if pd.notna(row['conv']) else ""
            tk.Label(row_frame, text=conv_val, font=("Arial", 11), width=15).grid(row=0, column=3)

        main_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

    def save_and_close(self):
        selected_indices = [idx for var, idx in self.checkboxes if var.get()]
        self.df_queries['selected'] = False
        self.df_queries.loc[selected_indices, 'selected'] = True
        selected_df = self.df_queries[self.df_queries['selected']][['query', 'freq', 'conv', 'subject']].copy()

        try:
            with pd.ExcelWriter(self.output_file, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
                selected_df.to_excel(writer, sheet_name=sanitize_sheet_name(self.sku), index=False)
            messagebox.showinfo("Успех", f"Сохранено {len(selected_df)} запросов.")
            self.master.destroy()
            if self.on_close:
                self.on_close()
        except PermissionError:
            messagebox.showerror("Ошибка", "Не удалось записать файл. Закройте Excel и повторите.")

def run_stage2(category, selected_skus, progress_callback):
    output_file = os.path.join(OUTPUT_DIR, f"Обработка_{category}.xlsx")
    if not os.path.exists(output_file):
        return "Файл обработки не найден. Сначала выполните фильтрацию."

    try:
        df_working = pd.read_excel(output_file, sheet_name='Рабочие запросы')
        if 'query' not in df_working.columns and 'Поисковый запрос' in df_working.columns:
            df_working.rename(columns={'Поисковый запрос': 'query'}, inplace=True)
        df_unique = pd.read_excel(output_file, sheet_name='Уникальные слова и фразы')
    except Exception as e:
        return f"Ошибка загрузки данных: {e}"

    if 'Количество запросов' in df_working.columns:
        df_working.rename(columns={'Количество запросов': 'freq'}, inplace=True)
    if 'Конверсия в заказ' in df_working.columns:
        df_working.rename(columns={'Конверсия в заказ': 'conv'}, inplace=True)

    progress_callback(f"Начинаем отбор для {len(selected_skus)} артикулов.")

    root = tk.Toplevel()
    root.withdraw()

    for i, sku in enumerate(selected_skus):
        if sku in pd.ExcelFile(output_file).sheet_names:
            ans = messagebox.askyesno("Внимание", f"Для артикула {sku} уже есть отобранные запросы. Заменить?")
            if not ans:
                if i < len(selected_skus) - 1:
                    cont = messagebox.askyesno("Продолжить", "Перейти к следующему артикулу?")
                    if not cont:
                        break
                continue

        win = tk.Toplevel(root)
        app = QueryReviewWindow(win, category, sku, df_working, df_unique, output_file, on_close=lambda: None)
        win.wait_window()

        if i < len(selected_skus) - 1:
            cont = messagebox.askyesno("Продолжить", "Перейти к следующему артикулу?")
            if not cont:
                break

    root.destroy()
    return "Ручной отбор завершён."

# ================== ЭТАП 3: ГЕНЕРАЦИЯ ОПИСАНИЯ ==================
def call_ai_for_description(original_description, keywords_list, unique_phrases_dict):
    if not keywords_list:
        return None
    keywords_text = "\n".join(keywords_list)
    unique_text = ""
    if unique_phrases_dict:
        for subject, props in unique_phrases_dict.items():
            unique_text += f"{subject}: {', '.join(props)}\n"
    input_text = (
        f"Исходное маркетинговое описание:\n{original_description}\n\n"
        f"Отобранные поисковые запросы:\n{keywords_text}\n\n"
        f"Уникальные слова и фразы (предметы и свойства):\n{unique_text}\n\n"
        "Составь новое SEO-описание товара, следуя инструкции."
    )
    answer = call_ai_agent(PROMPT_ID_GENERATE, input_text, timeout=600)
    return answer

def run_stage3(category, selected_skus, progress_callback):
    output_file = os.path.join(OUTPUT_DIR, f"Обработка_{category}.xlsx")
    if not os.path.exists(output_file):
        return "Файл обработки не найден. Сначала выполните фильтрацию и отбор."

    descriptions = get_marketing_descriptions(category)
    if not descriptions:
        return "Не удалось загрузить маркетинговые описания."

    try:
        df_unique = pd.read_excel(output_file, sheet_name='Уникальные слова и фразы')
        unique_dict = df_unique.groupby('Предмет')['Свойство'].apply(list).to_dict()
    except:
        unique_dict = {}

    results = {}
    total = len(selected_skus)
    for i, sku in enumerate(selected_skus):
        progress_callback(f"Генерация {i+1}/{total}: {sku}")
        try:
            df = pd.read_excel(output_file, sheet_name=sku)
            if 'query' not in df.columns:
                continue
            selected_queries = df['query'].tolist()
            if not selected_queries:
                continue
        except:
            continue

        original = descriptions.get(sku)
        if not original:
            continue

        new_desc = call_ai_for_description(original, selected_queries, unique_dict)
        if new_desc:
            results[sku] = new_desc
        time.sleep(1)

    if not results:
        return "Не удалось сгенерировать ни одного описания."

    output_desc_file = os.path.join(OUTPUT_DIR, f"Новое_описание_{category}.xlsx")
    try:
        with pd.ExcelWriter(output_desc_file, engine='openpyxl') as writer:
            for sku, desc in results.items():
                df_desc = pd.DataFrame([{"Артикул": sku, "Новое описание": desc}])
                df_desc.to_excel(writer, sheet_name=sanitize_sheet_name(sku), index=False)
        return f"✅ Описания сохранены в {output_desc_file}"
    except Exception as e:
        return f"Ошибка сохранения: {e}"

# ================== НОВАЯ ФУНКЦИЯ ДЛЯ ЭТАПА 2+3 ==================
def run_manual_and_generate(category, skus, progress_callback):
    res2 = run_stage2(category, skus, progress_callback)
    progress_callback(res2)
    ans = messagebox.askyesno("Генерация", "Запустить генерацию описаний для выбранных артикулов?")
    if ans:
        res3 = run_stage3(category, skus, progress_callback)
        progress_callback(res3)
    else:
        progress_callback("Генерация отменена пользователем.")

# ================== GUI ==================
class MainApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Ассистент WB - SEO оптимизация")
        self.root.geometry("600x500")
        self.root.resizable(False, False)

        tk.Label(root, text="Выберите действие:", font=("Arial", 14)).pack(pady=20)

        tk.Button(root, text="1. Фильтрация запросов (ИИ + ручной отбор)",
                  width=60, height=2, command=self.run_filter_selection).pack(pady=5)
        tk.Button(root, text="2. Составление SEO-описания",
                  width=60, height=2, command=self.run_generation).pack(pady=5)
        tk.Button(root, text="3. Ручной отбор + генерация",
                  width=60, height=2, bg="lightblue", command=self.run_manual_then_generate).pack(pady=5)
        tk.Button(root, text="4. Полный цикл (1→2→3→4)",
                  width=60, height=2, bg="lightgreen", command=self.run_full_cycle).pack(pady=20)
        tk.Button(root, text="Выход", width=60, height=2, command=root.quit).pack(pady=5)

        self.log_text = tk.Text(root, height=8, width=80, state='disabled')
        self.log_text.pack(pady=10)

    def log(self, message):
        self.log_text.config(state='normal')
        self.log_text.insert(tk.END, message + "\n")
        self.log_text.see(tk.END)
        self.log_text.config(state='disabled')
        self.root.update()

    def progress_callback(self, msg):
        self.log(msg)

    def get_category_choice(self):
        categories = get_categories_from_marketing()
        if not categories:
            messagebox.showerror("Ошибка", "В папке с маркетинговыми описаниями нет файлов.")
            return None
        dialog = tk.Toplevel(self.root)
        dialog.title("Выбор категории")
        dialog.geometry("400x300")
        dialog.transient(self.root)
        dialog.grab_set()
        tk.Label(dialog, text="Выберите категорию (предмет):").pack(pady=10)
        listbox = tk.Listbox(dialog)
        for cat in categories:
            listbox.insert(tk.END, cat)
        listbox.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        result = [None]
        def on_ok():
            selection = listbox.curselection()
            if selection:
                result[0] = categories[selection[0]]
                dialog.destroy()
            else:
                messagebox.showwarning("Внимание", "Выберите категорию.")
        tk.Button(dialog, text="OK", command=on_ok).pack(pady=5)
        tk.Button(dialog, text="Отмена", command=dialog.destroy).pack(pady=5)
        self.root.wait_window(dialog)
        return result[0]

    def get_strategy_and_thresholds(self):
        dialog = tk.Toplevel(self.root)
        dialog.title("Настройки фильтрации")
        dialog.geometry("400x250")
        dialog.transient(self.root)
        dialog.grab_set()
        strategy_var = tk.IntVar(value=1)
        min_freq_var = tk.StringVar(value="500")
        min_conv_var = tk.StringVar(value="15")
        tk.Label(dialog, text="Выберите стратегию фильтрации:").pack(pady=10)
        ttk.Radiobutton(dialog, text="По умолчанию (частота >=500, конверсия >=15)", variable=strategy_var, value=1).pack(anchor='w', padx=20)
        ttk.Radiobutton(dialog, text="Ручной режим", variable=strategy_var, value=2).pack(anchor='w', padx=20)
        ttk.Radiobutton(dialog, text="Без фильтрации (все запросы)", variable=strategy_var, value=3).pack(anchor='w', padx=20)
        frame = ttk.Frame(dialog)
        frame.pack(pady=10)
        ttk.Label(frame, text="Мин. частота:").grid(row=0, column=0, padx=5)
        ttk.Entry(frame, textvariable=min_freq_var, width=10).grid(row=0, column=1, padx=5)
        ttk.Label(frame, text="Мин. конверсия (%):").grid(row=1, column=0, padx=5)
        ttk.Entry(frame, textvariable=min_conv_var, width=10).grid(row=1, column=1, padx=5)
        result = [None]
        def on_ok():
            strat = strategy_var.get()
            if strat == 2:
                try:
                    freq = float(min_freq_var.get())
                    conv = float(min_conv_var.get())
                    result[0] = (strat, (freq, conv))
                except:
                    messagebox.showerror("Ошибка", "Введите корректные числа.")
                    return
            else:
                result[0] = (strat, None)
            dialog.destroy()
        ttk.Button(dialog, text="OK", command=on_ok).pack(pady=10)
        ttk.Button(dialog, text="Отмена", command=dialog.destroy).pack(pady=5)
        self.root.wait_window(dialog)
        return result[0]

    def get_skus_choice(self, category):
        descriptions = get_marketing_descriptions(category)
        if not descriptions:
            messagebox.showerror("Ошибка", "Не удалось загрузить маркетинговые описания.")
            return None
        skus = list(descriptions.keys())
        dialog = tk.Toplevel(self.root)
        dialog.title("Выбор артикулов")
        dialog.geometry("500x400")
        dialog.transient(self.root)
        dialog.grab_set()
        tk.Label(dialog, text="Выберите артикулы (можно несколько):").pack(pady=10)
        frame = ttk.Frame(dialog)
        frame.pack(fill=tk.BOTH, expand=True, padx=10)
        listbox = tk.Listbox(frame, selectmode=tk.MULTIPLE)
        scrollbar = ttk.Scrollbar(frame, orient="vertical", command=listbox.yview)
        listbox.configure(yscrollcommand=scrollbar.set)
        listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        for sku in skus:
            listbox.insert(tk.END, sku)
        def select_all():
            listbox.selection_set(0, tk.END)
        ttk.Button(dialog, text="Выбрать все", command=select_all).pack(pady=5)
        result = [None]
        def on_ok():
            indices = listbox.curselection()
            if not indices:
                messagebox.showwarning("Внимание", "Выберите хотя бы один артикул.")
                return
            result[0] = [skus[i] for i in indices]
            dialog.destroy()
        ttk.Button(dialog, text="OK", command=on_ok).pack(side=tk.LEFT, padx=20, pady=10)
        ttk.Button(dialog, text="Отмена", command=dialog.destroy).pack(side=tk.RIGHT, padx=20, pady=10)
        self.root.wait_window(dialog)
        return result[0]

    def run_filter_selection(self):
        category = self.get_category_choice()
        if not category:
            return
        strat_info = self.get_strategy_and_thresholds()
        if not strat_info:
            return
        strategy, thresholds = strat_info
        def task():
            self.log(f"Начинаем фильтрацию для категории '{category}'...")
            result = run_stage1(category, strategy, thresholds, self.progress_callback)
            self.log(result)
            if "✅" in result:
                ans = messagebox.askyesno("Вопрос", "Перейти к ручному отбору запросов?")
                if ans:
                    self.run_selection_for_category(category)
        threading.Thread(target=task).start()

    def run_selection_for_category(self, category):
        skus = self.get_skus_choice(category)
        if not skus:
            return
        def task():
            self.log(f"Начинаем ручной отбор для категории '{category}'...")
            result = run_stage2(category, skus, self.progress_callback)
            self.log(result)
        threading.Thread(target=task).start()

    def run_generation(self):
        category = self.get_category_choice()
        if not category:
            return
        skus = self.get_skus_choice(category)
        if not skus:
            return
        def task():
            self.log(f"Начинаем генерацию описаний для категории '{category}'...")
            result = run_stage3(category, skus, self.progress_callback)
            self.log(result)
        threading.Thread(target=task).start()

    def run_manual_then_generate(self):
        category = self.get_category_choice()
        if not category:
            return
        skus = self.get_skus_choice(category)
        if not skus:
            return
        def task():
            run_manual_and_generate(category, skus, self.progress_callback)
        threading.Thread(target=task).start()

    def run_full_cycle(self):
        category = self.get_category_choice()
        if not category:
            return
        strat_info = self.get_strategy_and_thresholds()
        if not strat_info:
            return
        strategy, thresholds = strat_info
        def task():
            self.log(f"=== ПОЛНЫЙ ЦИКЛ для категории '{category}' ===")
            self.log("Этап 1: фильтрация...")
            res1 = run_stage1(category, strategy, thresholds, self.progress_callback)
            self.log(res1)
            if "✅" not in res1:
                return
            descriptions = get_marketing_descriptions(category)
            if not descriptions:
                self.log("Не удалось загрузить маркетинговые описания.")
                return
            skus = list(descriptions.keys())
            self.log(f"Этап 2: ручной отбор для всех артикулов...")
            run_manual_and_generate(category, skus, self.progress_callback)
        threading.Thread(target=task).start()

if __name__ == "__main__":
    root = tk.Tk()
    app = MainApp(root)
    root.mainloop()
