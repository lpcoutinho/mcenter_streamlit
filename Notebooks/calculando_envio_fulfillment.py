import json
import math
import os
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import psycopg2
import requests
from dotenv import load_dotenv
from pandas import json_normalize
from psycopg2 import sql


def send_fulfillment(date_from,date_to):
    load_dotenv()

    ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
    HOST = os.getenv("HOST")
    POSTGRES_DB = os.getenv("POSTGRES_DB")
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

    # Informações de conexão com o banco de dados PostgreSQL
    db_config = {
        "host": HOST,
        "database": POSTGRES_DB,
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
    }

    ### Período a consultar ###

    # Defina as datas de início e fim desejadas
    # date_from = datetime(2023, 11, 1).date()
    # date_to = datetime(2023, 12, 8).date()
    # date_to = date_to + timedelta(days=1)  # + 1 dia para pegar a data atual no DB
    # date_to = date_to - timedelta(days=1)  # - 1 dia para pegar a data atual no DB
    print("date_from: ", date_from)
    print("date_to: ", date_to)

    ### Historico de estoque fulfillment ###
    # Buscando histórico de estoque na tabela
    try:
        conn = psycopg2.connect(**db_config)

        sql_query = f"SELECT * FROM fulfillment_stock WHERE created_at BETWEEN '{date_from}' AND '{date_to};'"
        # sql_query = f"SELECT * FROM fulfillment_stock WHERE created_at BETWEEN '2023-12-04' AND '2023-12-05';"
        print(sql_query)
        df_stock = pd.read_sql(sql_query, conn)

    except psycopg2.Error as e:
        print(f"Erro do psycopg2 ao consultar fulfillment_stock: {e}")

    except Exception as e:
        print(f"Erro ao consultar fulfillment_stock: {e}")

    finally:
        if conn is not None:
            conn.close()

    # Ordenando stock por data
    df_stock = df_stock.sort_values(by="created_at", ascending=False)
    df_stock["data"] = df_stock["created_at"].dt.date
    df_stock = df_stock.drop(["created_at"], axis=1)

    ## Cria coluna has_stock, se available_quantity <= 0, has_stock= False ##
    df_stock = df_stock.assign(has_stock=lambda x: x["available_quantity"] > 0)
    df_stock = df_stock.sort_values(by="data", ascending=False).reset_index(drop=True)

    ## Contando dias em que produto esteve disponível
    days_available = (
        df_stock.groupby(["ml_inventory_id"])["has_stock"].sum().reset_index()
    )
    days_available = days_available.rename(columns={"has_stock": "days_available"})

    # Unindo DFs
    df_stock_days = df_stock.merge(days_available, on=["ml_inventory_id"], how="inner")
    # df_stock_days_left = df_stock.merge(days_available, on=["ml_inventory_id"], how="left")

    # data de hoje
    data_de_hoje = datetime.now().date()
    data_de_hoje = data_de_hoje - timedelta(days=1)
    print(data_de_hoje)

    df_stock_days["data"] = pd.to_datetime(df_stock_days["data"])

    # Filtra apenas as linhas onde 'data' é igual à data de hoje
    df_stock_today = df_stock_days[df_stock_days["data"].dt.date == data_de_hoje]
    df_stock_today = df_stock_today.rename(
        columns={"available_quantity": "available_quantity_today"}
    )

    # Se detail_status = transfer: available_quantity_today = available_quantity_today + detail_quantity
    df_stock_today["total_available_quantity"] = df_stock_today.apply(
        lambda row: row["detail_quantity"] + row["available_quantity_today"]
        if row["detail_status"] == "transfer"
        else row["available_quantity_today"],
        axis=1,
    )

    df_stock_today["total_available_quantity"] = df_stock_today[
        "total_available_quantity"
    ].astype("int64")

    ### Buscando hitorico de orders no BD ###

    # Buscando histórico de vendas na tabela ml_orders_hist para o período definido
    try:
        conn = psycopg2.connect(**db_config)

        sql_query = f"SELECT * FROM ml_orders WHERE date_closed BETWEEN '{date_from}' AND '{date_to}'"
        print(sql_query)
        df_orders = pd.read_sql(sql_query, conn)

    except psycopg2.Error as e:
        print(f"Erro do psycopg2 ao consultar ml_orders_hist: {e}")
        # logger.error(f"Erro do psycopg2 ao consultar ml_orders_hist: {e}")

    except Exception as e:
        print(f"Erro ao consultar ml_orders_hist: {e}")
        # logger.error(f"Erro ao consultar ml_orders_hist: {e}")

    finally:
        if conn is not None:
            conn.close()

    # filtros
    df_orders = df_orders[df_orders["logistic_type"] == "fulfillment"]
    # df_orders = df_orders.drop(columns=['category_id','pack_id','variation_attributes_id','variation_name','variation_value_id', 'data'])
    df_orders = df_orders.drop(
        columns=[
            "category_id",
            "pack_id",
            "variation_attributes_id",
            "variation_name",
            "variation_value_id",
        ]
    )

    # change column
    df_orders["variation_id"] = df_orders["variation_id"].replace(
        "nan", "0", regex=True
    )

    df_orders.rename(columns={"quantity": "sold_quantity"}, inplace=True)

    print(df_orders.shape)
    df_orders = df_orders.drop_duplicates()
    print(df_orders.shape)

    # Ordenando orders por data
    df_orders = df_orders.sort_values(by="date_approved", ascending=False)
    df_orders["data"] = df_orders["date_approved"].dt.date
    df_orders = df_orders.drop(["date_closed", "date_approved"], axis=1)

    # Total de vendas por ml_code e id de variação
    df_orders_quantity = (
        df_orders.groupby(["ml_code", "variation_id"])["sold_quantity"]
        .sum()
        .reset_index()
    )

    print(f"Número de ml_code únicos: {len(df_orders_quantity['ml_code'].unique())}")
    print(
        f"Número de variation_id únicos: {len(df_orders_quantity['variation_id'].unique())}"
    )

    # Acrescentando total de vendas ao DF
    df_total_sales = pd.merge(
        # df_orders, resultado, on=["ml_code", "variation_id"], how="inner"
        df_orders,
        df_orders_quantity,
        on=["ml_code", "variation_id"],
        how="inner",
    )
    df_total_sales = df_total_sales.rename(
        columns={"sold_quantity_y": "total_sold_quantity"}
    )
    df_total_sales = df_total_sales.drop(
        columns=["sold_quantity_x", "order_status", "payment_status"]
    )

    print(f"Total de vendas = {df_total_sales.shape[0]}")

    # Buscando dados de produtos na tabela tiny_fulfillment
    try:
        conn = psycopg2.connect(**db_config)
        sql_query = "SELECT * FROM items"
        df_codes = pd.read_sql(sql_query, conn)
    except psycopg2.Error as e:
        # logger.error(f"Erro do psycopg2 ao consultar fulfillment_stock: {e}")
        print(f"Erro do psycopg2 ao consultar tiny_fulfillment: {e}")

    except Exception as e:
        # logger.error(f"Erro ao consultar tabela tiny_fulfillment: {e}")
        print(f"Erro ao consultar tabela tiny_fulfillment: {e}")

    finally:
        if conn is not None:
            conn.close()

    # df_codes["ml_code"] = df_codes["ml_code"].apply(lambda x: "MLB" + str(x))
    df_codes.rename(columns={"inventory_id": "ml_inventory_id"}, inplace=True)
    df_codes = df_codes.drop(["created_at", "updated_at"], axis=1)

    df_not_catalogo = df_codes[df_codes["catalog_listing"] == False]
    df_catalogo = df_codes[df_codes["catalog_listing"] == True]

    df_total_sales_cat = pd.merge(
        df_catalogo,
        df_total_sales,
        left_on=["ml_code"],
        right_on=["ml_code"],
        how="left",
    )
    df_total_sales_cat = df_total_sales_cat.drop_duplicates()

    df_total_sales_not_cat = pd.merge(
        df_not_catalogo,
        df_total_sales,
        left_on=["ml_code", "variation_id"],
        right_on=["ml_code", "variation_id"],
        how="left",
    )
    df_total_sales_not_cat = df_total_sales_not_cat.drop_duplicates()

    df_total_sales_not_cat["total_sold_quantity"] = df_total_sales_not_cat[
        "total_sold_quantity"
    ].fillna(0)
    df_total_sales_cat["total_sold_quantity"] = df_total_sales_cat[
        "total_sold_quantity"
    ].fillna(0)

    df_total_sales_not_cat["total_sold_quantity"] = df_total_sales_not_cat[
        "total_sold_quantity"
    ].astype("int64")
    df_total_sales_cat["total_sold_quantity"] = df_total_sales_cat[
        "total_sold_quantity"
    ].astype("int64")

    print(df_total_sales_cat.shape)
    print(df_total_sales_not_cat.shape)

    df_total_sales_cat_x = df_total_sales_cat.drop(
        columns=["data", "shipping_id", "variation_id_x", "order_id"]
    )
    df_total_sales_cat_x = df_total_sales_cat_x.drop_duplicates()
    df_total_sales_cat_x.shape

    print(df_total_sales_not_cat.shape)

    print(df_total_sales_not_cat.shape)
    df_total_sales_not_cat_x = df_total_sales_not_cat.drop(
        columns=["data", "shipping_id", "order_id"]
    )
    df_total_sales_not_cat_x = df_total_sales_not_cat_x.drop_duplicates()
    print(df_total_sales_not_cat_x.shape)

    print(df_total_sales_cat_x.shape)
    print(df_total_sales_not_cat_x.shape)

    df_total_sales_cat_x = df_total_sales_cat.drop_duplicates(
        subset=["ml_code", "ml_inventory_id"]
    )
    df_total_sales_not_cat_x = df_total_sales_not_cat.drop_duplicates(
        subset=["ml_code", "ml_inventory_id"]
    )

    df_total_cat = df_total_sales_cat_x.copy()
    df_total_not_cat = df_total_sales_not_cat_x.copy()

    # Somando total de vendas por inventory_id
    df_sum_qt_sold_cat = (
        df_total_cat.groupby("ml_inventory_id")["total_sold_quantity"]
        .sum()
        .reset_index()
    )
    df_sum_qt_sold_cat = df_sum_qt_sold_cat.rename(
        columns={"total_sold_quantity": "total_sold_catalog"}
    )

    df_total_cat = pd.merge(
        df_total_cat,
        df_sum_qt_sold_cat[["ml_inventory_id", "total_sold_catalog"]],
        on="ml_inventory_id",
        how="left",
    )

    df_total_cat.rename(columns={"variation_id_y": "variation_id_"})

    df_sum_qt_sold_not_cat = (
        df_total_not_cat.groupby("ml_inventory_id")["total_sold_quantity"]
        .sum()
        .reset_index()
    )
    df_sum_qt_sold_not_cat = df_sum_qt_sold_not_cat.rename(
        columns={"total_sold_quantity": "total_sold_not_catalog"}
    )
    df_total_not_cat = pd.merge(
        df_total_not_cat,
        df_sum_qt_sold_not_cat[["ml_inventory_id", "total_sold_not_catalog"]],
        on="ml_inventory_id",
        how="left",
    )

    print(df_total_cat.shape)
    print(df_total_not_cat.shape)

    df_total_cat.rename(columns={"variation_id_y": "variation_id"}, inplace=True)
    df_total_cat.shape

    df_total_cat = df_total_cat.drop_duplicates(subset=["ml_inventory_id"])

    df_combined = pd.merge(
        df_total_not_cat,
        df_total_cat[["ml_inventory_id", "total_sold_catalog"]],
        on="ml_inventory_id",
        how="left",
    )
    print(df_combined.shape)

    df = pd.merge(df_combined, df_stock_today, on="ml_inventory_id", how="left")
    df["total_sold_catalog"] = df["total_sold_catalog"].fillna(0).astype("int64")

    df.shape

    days = 30

    df["total_sold"] = df["total_sold_catalog"] + df["total_sold_not_catalog"]
    # qtd de produtos a enviar no período, caso seja valor negativo produto está acima do esperado para envio(sobrando)
    df["period_send_fulfillment"] = np.ceil(
        (df["total_sold"] / df["days_available"]) * days
        - df["total_available_quantity"]
    )

    df["period_send_fulfillment"] = df["period_send_fulfillment"].fillna(0)

    df.shape

    df_have_itens = df[df["days_available"] > 0]
    df_have_itens.shape

    # produtos sem estoque no período
    df_no_itens = df[df["days_available"] <= 0]
    df_no_itens = df_no_itens.drop(columns=["period_send_fulfillment"])
    df_no_itens.shape

    df_sold_zero = df_have_itens[df_have_itens["total_sold"] == 0]
    df_sold = df_have_itens[df_have_itens["total_sold"] > 0]

    dfx = df_have_itens.copy()

    cols = [
        "ml_code",
        "seller_sku",
        "ml_inventory_id",
        "value_name",
        "status",
        "title",
        "available_quantity_today",
        "detail_status",
        "detail_quantity",
        "total_available_quantity",
        "days_available",
        "total_sold_not_catalog",
        "total_sold_catalog",
        "total_sold",
        "period_send_fulfillment",
    ]

    dfx = dfx[cols]

    dfx = dfx.rename(
        columns={
            "detail_status": "transfer_status",
            "detail_quantity": "transfer_quantity",
        }
    )

    dfx.shape

    return df_no_itens, df_sold_zero, df_sold
