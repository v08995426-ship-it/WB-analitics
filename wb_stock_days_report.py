import os
import math
import io
import pandas as pd
import requests
from datetime import datetime, timedelta
import boto3

# ================= CONFIG =================

class Config:
    def __init__(self):
        # --- S3 / Object Storage ---
        self.bucket = (
            os.getenv("WB_S3_BUCKET")
            or os.getenv("YC_BUCKET_NAME")
            or os.getenv("CLOUD_RU_BUCKET")
        )

        self.access_key = (
            os.getenv("WB_S3_ACCESS_KEY")
            or os.getenv("YC_ACCESS_KEY_ID")
            or os.getenv("CLOUD_RU_ACCESS_KEY")
        )

        self.secret_key = (
            os.getenv("WB_S3_SECRET_KEY")
            or os.getenv("YC_SECRET_ACCESS_KEY")
            or os.getenv("CLOUD_RU_SECRET_KEY")
        )

        self.endpoint = (
            os.getenv("WB_S3_ENDPOINT")
            or os.getenv("YC_ENDPOINT")
            or "https://storage.yandexcloud.net"
        )

        self.region = os.getenv("WB_S3_REGION", "ru-central1")

        # --- Telegram ---
        self.tg_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.tg_chat = os.getenv("TELEGRAM_CHAT_ID")

        # --- Paths ---
        self.wb_stock_prefix = "Отчёты/Остатки/TOPFACE/Недельные/"
        self.orders_prefix = "Отчёты/Заказы/TOPFACE/Недельные/"
        self.map_file = "Отчёты/Остатки/1С/Артикулы 1с.xlsx"
        self.stock_1c_file = "Отчёты/Остатки/1С/Остатки 1С.xlsx"
        self.stop_file = os.getenv("WB_STOP_LIST_KEY", "Отчёты/Остатки/1С/СТОП к заказам.xlsx")


# ================= S3 =================

class S3Storage:
    def __init__(self, cfg: Config):
        if not cfg.bucket or not cfg.access_key or not cfg.secret_key:
            raise ValueError("Не заданы параметры Object Storage")

        self.client = boto3.client(
            "s3",
            endpoint_url=cfg.endpoint,
            aws_access_key_id=cfg.access_key,
            aws_secret_access_key=cfg.secret_key,
            region_name=cfg.region,
        )
        self.bucket = cfg.bucket

    def list_files(self, prefix):
        resp = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        return [obj["Key"] for obj in resp.get("Contents", [])]

    def read_excel(self, key):
        obj = self.client.get_object(Bucket=self.bucket, Key=key)
        return pd.read_excel(io.BytesIO(obj["Body"].read()))


# ================= HELPERS =================

def get_latest_file(files):
    return sorted(files)[-1]


def ceil_safe(x):
    if pd.isna(x):
        return 0
    return math.ceil(x)


# ================= LOAD DATA =================

def load_wb_stock(storage, cfg):
    files = storage.list_files(cfg.wb_stock_prefix)
    latest = get_latest_file(files)
    df = storage.read_excel(latest)
    return df


def load_orders(storage, cfg):
    files = storage.list_files(cfg.orders_prefix)
    latest = get_latest_file(files)
    df = storage.read_excel(latest)
    return df


def load_map(storage, cfg):
    df = storage.read_excel(cfg.map_file)
    return dict(zip(df.iloc[:, 0], df.iloc[:, 2]))


def load_1c_stock(storage, cfg):
    df = storage.read_excel(cfg.stock_1c_file)
    df["Остатки МП"] = df["Остатки МП"].apply(ceil_safe)
    return df


def load_stop(storage, cfg):
    df = storage.read_excel(cfg.stop_file)
    df.columns = ["Артикул 1С", "col2", "Статус"]
    return set(df[df["Статус"] == "Delist"]["Артикул 1С"])


# ================= CALC =================

def calculate(cfg):
    storage = S3Storage(cfg)

    wb_stock = load_wb_stock(storage, cfg)
    orders = load_orders(storage, cfg)
    article_map = load_map(storage, cfg)
    stock_1c = load_1c_stock(storage, cfg)
    stop_set = load_stop(storage, cfg)

    # --- ключ ---
    key = "nmId" if "nmId" in wb_stock.columns else "Артикул WB"

    wb_stock = wb_stock[[key, "Количество"]].rename(columns={"Количество": "stock_wb"})

    # продажи
    sales = orders.groupby(key).size().reset_index(name="sales_7d")
    sales["avg_sales"] = sales["sales_7d"] / 7

    df = wb_stock.merge(sales, on=key, how="left").fillna(0)

    # маппинг
    df["Артикул 1С"] = df[key].map(article_map)

    df = df.merge(stock_1c, left_on="Артикул 1С", right_on="Артикул", how="left")

    df["days_wb"] = df["stock_wb"] / df["avg_sales"].replace(0, 1)
    df["days_lipetsk"] = df["Остатки МП"] / df["avg_sales"].replace(0, 1)

    df["Delist"] = df["Артикул 1С"].apply(lambda x: "Delist" if x in stop_set else "")

    return df


# ================= EXCEL =================

def build_excel(df):
    output = io.BytesIO()

    critical = df[df["days_wb"] < 14]

    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        critical[["Артикул 1С", "days_wb", "days_lipetsk", "Delist"]].to_excel(writer, sheet_name="Критично", index=False)
        df.to_excel(writer, sheet_name="Расчёт", index=False)

    output.seek(0)
    return output


# ================= TELEGRAM =================

def send_telegram(cfg, file_bytes):
    url = f"https://api.telegram.org/bot{cfg.tg_token}/sendDocument"

    files = {"document": ("report.xlsx", file_bytes)}

    data = {
        "chat_id": cfg.tg_chat,
        "caption": "Отчёт по остаткам WB"
    }

    requests.post(url, files=files, data=data)


# ================= RUN =================

def run():
    cfg = Config()
    df = calculate(cfg)
    file = build_excel(df)
    send_telegram(cfg, file)


if __name__ == "__main__":
    run()
