import os
import io
import json
import time
import boto3
import pandas as pd
import requests
from datetime import datetime, timedelta
from botocore.client import Config

# ==============================
# НАСТРОЙКИ
# ==============================

STORE = "TOPFACE"

ANALYTICS_PATH = f"Отчёты/Реклама/{STORE}/Анализ рекламы.xlsx"
WEEKLY_PATH = f"Отчёты/Реклама/{STORE}/Недельные/"

CONFIG_PATH = f"Служебные файлы/Ассистент WB/{STORE}/strategy_config.json"

DRR_THRESHOLD_CPC = 12
DRR_THRESHOLD_CPM = 15

STEP_CPC = 100
STEP_CPM = 1000

MIN_BID_CPC = 400
MIN_BID_CPM = 8000

ALLOWED_SUBJECTS = [
    "кисти косметические",
    "помады",
    "косметические карандаши",
    "блески"
]

WB_BIDS_URL = "https://advert-api.wildberries.ru/api/advert/v1/bids"

# ==============================
# S3 клиент
# ==============================

class S3Storage:

    def __init__(self):

        self.bucket = os.environ["YC_BUCKET_NAME"]

        self.s3 = boto3.client(
            "s3",
            endpoint_url="https://storage.yandexcloud.net",
            aws_access_key_id=os.environ["YC_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["YC_SECRET_ACCESS_KEY"],
            config=Config(signature_version="s3v4"),
        )

    def read_excel(self, key):

        obj = self.s3.get_object(Bucket=self.bucket, Key=key)

        return pd.read_excel(io.BytesIO(obj["Body"].read()))

    def write_json(self, key, data):

        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(data).encode("utf-8"),
        )

    def read_json(self, key):

        obj = self.s3.get_object(Bucket=self.bucket, Key=key)

        return json.loads(obj["Body"].read())

    def list_files(self, prefix):

        resp = self.s3.list_objects_v2(
            Bucket=self.bucket,
            Prefix=prefix
        )

        if "Contents" not in resp:
            return []

        return [x["Key"] for x in resp["Contents"]]

# ==============================
# ЗАГРУЗКА ДАННЫХ
# ==============================

def load_advertising_data(s3, days):

    try:

        df = s3.read_excel(ANALYTICS_PATH)

        df["Дата"] = pd.to_datetime(df["Дата"])

        return df

    except:

        print("Основной файл повреждён, используем недельные")

        end_date = datetime.now().date() - timedelta(days=1)
        start_date = end_date - timedelta(days=days)

        weeks = set()

        for i in range(days + 2):
            d = end_date - timedelta(days=i)
            year, week, _ = d.isocalendar()
            weeks.add((year, week))

        frames = []

        for year, week in weeks:

            key = f"{WEEKLY_PATH}Реклама_{year}-W{week:02d}.xlsx"

            try:

                df = s3.read_excel(key)

                df = df[df["Дата"] >= str(start_date)]

                frames.append(df)

            except:
                pass

        if not frames:
            raise Exception("Нет данных рекламы")

        return pd.concat(frames)

# ==============================
# СТРАТЕГИИ
# ==============================

def strategy_1(stats):

    grouped = stats.groupby(
        ["ID кампании", "Артикул WB"]
    ).agg(
        {"Расход": "sum", "Сумма заказов": "sum"}
    ).reset_index()

    decisions = []

    for _, row in grouped.iterrows():

        spent = row["Расход"]
        revenue = row["Сумма заказов"]

        if revenue == 0:
            drr = 100
        else:
            drr = spent / revenue * 100

        if drr > DRR_THRESHOLD_CPC:
            direction = "down"
        else:
            direction = "up"

        decisions.append(
            {
                "advert_id": int(row["ID кампании"]),
                "nm_id": int(row["Артикул WB"]),
                "direction": direction,
            }
        )

    return decisions


def strategy_2(stats):

    grouped = stats.groupby(
        "ID кампании"
    ).agg(
        {"Расход": "sum", "Сумма заказов": "sum"}
    ).reset_index()

    decisions = []

    for _, row in grouped.iterrows():

        revenue = row["Сумма заказов"]

        target_spend = revenue * 0.15

        if row["Расход"] > target_spend:
            direction = "down"
        else:
            direction = "up"

        decisions.append(
            {
                "advert_id": int(row["ID кампании"]),
                "direction": direction,
            }
        )

    return decisions


def strategy_3(stats):

    decisions = []

    grouped = stats.groupby(
        ["ID кампании", "Артикул WB"]
    ).agg(
        {"Показы": "sum"}
    ).reset_index()

    for _, row in grouped.iterrows():

        decisions.append(
            {
                "advert_id": int(row["ID кампании"]),
                "nm_id": int(row["Артикул WB"]),
                "direction": "up",
            }
        )

    return decisions


# ==============================
# ОТПРАВКА СТАВОК
# ==============================

def send_bids(decisions):

    headers = {
        "Authorization": os.environ["WB_PROMO_KEY_TOPFACE"],
        "Content-Type": "application/json"
    }

    bids = []

    for d in decisions:

        bids.append(
            {
                "advert_id": d["advert_id"],
                "nm_bids": [
                    {
                        "nm_id": d["nm_id"],
                        "bid_kopecks": 1000,
                        "placement": "search"
                    }
                ]
            }
        )

    payload = {"bids": bids}

    r = requests.patch(WB_BIDS_URL, json=payload, headers=headers)

    print("WB response:", r.status_code)


# ==============================
# ПАНЕЛЬ УПРАВЛЕНИЯ
# ==============================

def select_strategy():

    print("Выберите стратегию:")

    print("1 — контроль ДРР")
    print("2 — контроль расходов")
    print("3 — агрессивный рост")

    choice = input("> ")

    return int(choice)


def save_strategy(s3, strategy):

    s3.write_json(CONFIG_PATH, {"strategy": strategy})


def load_strategy(s3):

    try:

        data = s3.read_json(CONFIG_PATH)

        return data["strategy"]

    except:

        return 1


# ==============================
# ОСНОВНОЙ ЗАПУСК
# ==============================

def run():

    s3 = S3Storage()

    strategy = load_strategy(s3)

    print("Активная стратегия:", strategy)

    days = 7 if strategy == 2 else 3

    stats = load_advertising_data(s3, days)

    stats = stats[
        stats["Название предмета"].str.lower().isin(ALLOWED_SUBJECTS)
    ]

    if strategy == 1:
        decisions = strategy_1(stats)

    elif strategy == 2:
        decisions = strategy_2(stats)

    else:
        decisions = strategy_3(stats)

    print("Решений:", len(decisions))

    send_bids(decisions)


# ==============================

if __name__ == "__main__":

    if len(os.sys.argv) > 1 and os.sys.argv[1] == "set":

        s3 = S3Storage()

        strategy = select_strategy()

        save_strategy(s3, strategy)

        print("Стратегия сохранена")

    else:

        run()
