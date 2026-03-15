import pandas as pd
import numpy as np
import os
from datetime import datetime

# ---------------------------------------------------
# НАСТРОЙКИ
# ---------------------------------------------------

VAT_RATE = 0.07
PROFIT_TAX = 0.15

FIN_REPORT = "Отчёты/Финансовые показатели"
COST_FILE = "Отчёты/Себестоимость/Себестоимость.xlsx"
AD_FILE = "Отчёты/Реклама/TOPFACE/Анализ рекламы.xlsx"

OUTPUT_FILE = "Отчёты/Финансовые показатели/TOPFACE/Экономика.xlsx"


# ---------------------------------------------------
# ЛОГ
# ---------------------------------------------------

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


# ---------------------------------------------------
# НДС
# ---------------------------------------------------

def vat_from_price(price):
    return price * VAT_RATE / (1 + VAT_RATE)


# ---------------------------------------------------
# СЕБЕСТОИМОСТЬ
# ---------------------------------------------------

def load_cost_price():

    df = pd.read_excel(COST_FILE)

    df.columns = df.columns.str.strip()

    if "Артикул ВБ" not in df.columns:
        raise Exception("Нет колонки Артикул ВБ")

    if "Стоимость" not in df.columns:
        raise Exception("Нет колонки Стоимость")

    df["Артикул ВБ"] = df["Артикул ВБ"].astype(str)

    return dict(zip(df["Артикул ВБ"], df["Стоимость"]))


# ---------------------------------------------------
# РЕКЛАМА
# ---------------------------------------------------

def load_ads():

    try:

        xls = pd.ExcelFile(AD_FILE)

        for sheet in xls.sheet_names:

            df = pd.read_excel(AD_FILE, sheet)

            cols = df.columns

            if "Артикул WB" in cols and "Расход" in cols:

                df["Артикул WB"] = df["Артикул WB"].astype(str)

                ads = df.groupby("Артикул WB")["Расход"].sum()

                return ads.to_dict()

        log("⚠️ Лист рекламы не найден")

        return {}

    except:

        log("⚠️ Ошибка чтения рекламы")

        return {}


# ---------------------------------------------------
# ФИНАНСОВЫЙ ОТЧЕТ
# ---------------------------------------------------

def load_financial():

    files = os.listdir(FIN_REPORT)

    files = [f for f in files if f.endswith(".xlsx")]

    latest = sorted(files)[-1]

    path = os.path.join(FIN_REPORT, latest)

    df = pd.read_excel(path)

    return df


# ---------------------------------------------------
# РАСЧЕТ ЭКОНОМИКИ
# ---------------------------------------------------

def build_economics():

    log("Загрузка данных")

    cost = load_cost_price()

    ads = load_ads()

    fin = load_financial()

    fin["nm_id"] = fin["nm_id"].astype(str)

    sales = fin[fin["supplier_oper_name"] == "Продажа"]

    grouped = sales.groupby("nm_id").agg({
        "retail_amount": "sum",
        "quantity": "sum",
        "ppvz_vw": "sum",
        "acquiring_fee": "sum",
        "delivery_rub": "sum"
    }).reset_index()

    rows = []

    for _, r in grouped.iterrows():

        sku = r["nm_id"]

        qty = r["quantity"]

        revenue = r["retail_amount"]

        price = revenue / qty if qty else 0

        cost_price = cost.get(sku.split("/")[0], 0)

        commission = r["ppvz_vw"] / revenue if revenue else 0

        acquiring = r["acquiring_fee"] / revenue if revenue else 0

        logistics = r["delivery_rub"] / qty if qty else 0

        advert = ads.get(sku, 0) / qty if qty else 0

        vat = vat_from_price(price)

        gross_profit = price - cost_price - price * commission - price * acquiring - logistics - advert - vat

        profit_tax = max(gross_profit, 0) * PROFIT_TAX

        net_profit = gross_profit - profit_tax

        rows.append({

            "Артикул WB": sku,
            "Цена продажи": round(price,2),
            "Себестоимость": cost_price,
            "Комиссия %": round(commission*100,2),
            "Эквайринг %": round(acquiring*100,2),
            "Логистика на ед": round(logistics,2),
            "Реклама на ед": round(advert,2),
            "НДС на ед": round(vat,2),
            "Валовая прибыль на ед": round(gross_profit,2),
            "Чистая прибыль на ед": round(net_profit,2)

        })

    df = pd.DataFrame(rows)

    return df


# ---------------------------------------------------
# СОХРАНЕНИЕ
# ---------------------------------------------------

def save(df):

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    df.to_excel(OUTPUT_FILE, index=False)

    log(f"Экономика сохранена: {OUTPUT_FILE}")


# ---------------------------------------------------
# MAIN
# ---------------------------------------------------

def main():

    log("======================================================")
    log("📌 Расчет экономики")
    log("======================================================")

    df = build_economics()

    save(df)

    log(f"Артикулов рассчитано: {len(df)}")


if __name__ == "__main__":
    main()
