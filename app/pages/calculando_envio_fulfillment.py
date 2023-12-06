import json
import math
import os
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import psycopg2
import requests
import streamlit as st
from dotenv import load_dotenv
from pandas import json_normalize
from psycopg2 import sql

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

# ### Período a consultar

# Defina as datas de início e fim desejadas
data_inicio = datetime(2023, 11, 4).date()
data_fim = datetime(2023, 12, 5).date()
data_fim = data_fim + timedelta(days=1)  # + 1 dia para pegar a data atual no DB
print(data_fim)


# ### Historico de estoque
# Buscando histórico de estoque na tabela
try:
    conn = psycopg2.connect(**db_config)

    sql_query = f"SELECT * FROM fulfillment_stock_hist WHERE created_at BETWEEN '{data_inicio}' AND '{data_fim};'"
    # sql_query = f"SELECT * FROM fulfillment_stock_hist WHERE created_at BETWEEN '2023-12-04' AND '2023-12-05';"
    print(sql_query)
    df_stock = pd.read_sql(sql_query, conn)

except psycopg2.Error as e:
    print(f"Erro do psycopg2 ao consultar fulfillment_stock: {e}")

except Exception as e:
    print(f"Erro ao consultar fulfillment_stock: {e}")

finally:
    if conn is not None:
        conn.close()

# ### Adicionando dados de estoque

# conn = psycopg2.connect(**db_config)
# cursor = conn.cursor()

# for index, row in df_stock.iterrows():
#     data_ontem = datetime.now() - timedelta(days=1)
#     insert_query = sql.SQL(
#         """
#         INSERT INTO fulfillment_stock_hist (ml_inventory_id, available_quantity,created_at)
#         VALUES (%s, %s, %s)
#     """
#     )
#     cursor.execute(insert_query, (row["ml_inventory_id"], row["available_quantity"],data_ontem))

# conn.commit()

# cursor.close()
# conn.close()

# print("Dados inseridos com sucesso!")


# datas consultadas, dias em que um produto pode ou não estar disponível
df_stock["created_at"].value_counts().index.to_list()

# Ordenando stock por data
df_stock = df_stock.sort_values(by="created_at", ascending=False)
df_stock["data"] = df_stock["created_at"].dt.date
df_stock = df_stock.drop(["created_at"], axis=1)

# Se detail_status = transfer: available_quantity_today = available_quantity_today + detail_quantity

condicao = df_stock["detail_status"] == "transfer"

df_stock.loc[condicao, "available_quantity"] += df_stock.loc[
    condicao, "detail_quantity"
]

## Cria coluna has_stock, se available_quantity <= 0, has_stock= False ##
df_stock = df_stock.assign(has_stock=lambda x: x["available_quantity"] > 0)
df_stock = df_stock.sort_values(by="data", ascending=False).reset_index(drop=True)
df_stock = df_stock.drop_duplicates()

# #### Dias em que produto esteve disponível

## Contando dias em que produto esteve disponível
days_available = (
    df_stock.groupby(["ml_inventory_id", "references_variation_id"])["has_stock"]
    .sum()
    .reset_index()
)
days_available = days_available.rename(columns={"has_stock": "days_available"})


# Unindo DFs
df_stock = df_stock.merge(
    days_available, on=["ml_inventory_id", "references_variation_id"], how="inner"
)
# df_stock.rename(columns={'days_available_x': 'days_available'}, inplace=True)  # Corrigindo a sintaxe
# df_stock = df_stock.drop(columns='days_available_y', axis=1)  # Corrigindo a sintaxe

# data de hoje
# data_de_hoje = datetime.now().date() - timedelta(days=1)
# print(data_de_hoje)
data_de_hoje = datetime.now().date()
print(data_de_hoje)
df_stock["data"] = pd.to_datetime(df_stock["data"])

# Filtra apenas as linhas onde 'data' é igual à data de hoje
df_stock_today = df_stock[df_stock["data"].dt.date == data_de_hoje]
df_stock_today = df_stock_today.rename(
    columns={"available_quantity": "available_quantity_today"}
)
# df_stock_today = df_stock.drop(['has_stock'], axis=1)

# ### Buscando hitorico de orders no BD

# Buscando histórico de vendas na tabela ml_orders_hist para o período definido
try:
    conn = psycopg2.connect(**db_config)

    # sql_query = f"SELECT * FROM ml_orders_hist WHERE date_closed BETWEEN '{data_inicio}' AND '{data_fim}'"
    sql_query = f"SELECT * FROM ml_orders WHERE date_closed BETWEEN '{data_inicio}' AND '{data_fim}'"
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
df_orders = df_orders[df_orders["fulfilled"] == True]
df_orders = df_orders[df_orders["order_status"] == "paid"]
df_orders = df_orders[df_orders["payment_status"] == "approved"]
df_orders = df_orders.drop(
    ["pack_id", "date_approved", "fulfilled", "order_status", "payment_status"], axis=1
)
df_orders.rename(columns={"quantity": "sold_quantity"}, inplace=True)

# Ordenando orders por data
df_orders = df_orders.sort_values(by="date_closed", ascending=False)
df_orders["data"] = df_orders["date_closed"].dt.date
df_orders = df_orders.drop(["date_closed"], axis=1)
df_orders = df_orders.drop_duplicates()

# #### Total de vendas por ml_code e variation_id
# Verificar se a coluna variation_id existe
condicao_variation = df_orders["variation_id"].notna()

# Agrupar por variation_id ou ml_code, dependendo da condição
# grupo_coluna = 'variation_id' if condicao_variation.any() else 'ml_code'

# Somar sold_quantity com base na regra
resultado = (
    df_orders.groupby(["ml_code", "variation_id"])["sold_quantity"].sum().reset_index()
)

# Exibir o resultado
# resultado['variation_id	'].value_counts()

# Acrescentando total de vendas ao DF
df_total_sales = pd.merge(
    # df_orders, resultado, on=["ml_code", "variation_id"], how="inner"
    df_orders,
    resultado,
    on=["ml_code", "variation_id"],
    how="inner",
)
df_total_sales = df_total_sales.rename(
    columns={"sold_quantity_y": "total_sold_quantity"}
)
df_total_sales.shape

df_total_sales = df_total_sales.drop(["sold_quantity_x", "shipping_id", "data"], axis=1)
df_total_sales = df_total_sales.drop_duplicates()


# Neste ponto temos o total de itens vendidos de um anúncio por período e a quantidade de dias em que um produto esteve disponível.
# precisamos juntar esses dados para calcular, para isso trarei as informações de produtos


# #### Buscando Produtos

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
df_not_catalogo.sample()


df_vendas_cat = pd.merge(
    df_catalogo, df_orders, left_on=["ml_code"], right_on=["ml_code"], how="inner"
)
df_vendas_not_cat = pd.merge(
    df_not_catalogo,
    df_orders,
    left_on=["ml_code", "variation_id"],
    right_on=["ml_code", "variation_id"],
    how="inner",
)

resultado_cat = (
    df_vendas_cat.groupby("ml_inventory_id")["sold_quantity"].sum().reset_index()
)
resultado_not_cat = (
    df_vendas_not_cat.groupby("ml_inventory_id")["sold_quantity"].sum().reset_index()
)


resultado = pd.merge(
    resultado_cat, resultado_not_cat, on=["ml_inventory_id"], how="outer"
)
resultado = resultado.fillna(0)

resultado["sold_quantity_sum"] = resultado["sold_quantity_x"].fillna(0) + resultado[
    "sold_quantity_y"
].fillna(0)
df_total_sales = resultado.copy()


# ### Produtos + Dias disponíveis

prod_day = pd.merge(df_codes, df_stock_today, on="ml_inventory_id", how="inner")
# prod_day = pd.merge(df_codes, df_stock_today, on="ml_inventory_id", how="outer")

# ### Prod_Day + Total_sales
df_total_sales = df_total_sales.drop(["sold_quantity_x", "sold_quantity_y"], axis=1)

# df_sales = pd.merge(
#     df_total_sales,
#     prod_day,
#     left_on=["ml_code", "variation_id"],
#     right_on=["ml_code", "variation_id"],
#     how="inner",
# )

df_sales = pd.merge(
    df_total_sales,
    prod_day,
    left_on=["ml_inventory_id"],
    right_on=["ml_inventory_id"],
    how="inner",
)

cols = [
    "ml_code",
    "ml_sku",
    "ml_inventory_id",
    "tiny_id",
    "tiny_sku",
    "var_code",
    "variation_id",
    "title",
    "total_sales_quantity",
    "qtd_item",
    "days_available",
    "available_quantity_today",
    "data",
]

# df_sales = df_sales[cols]
df_sales = df_sales
print(df_total_sales.shape)
print(prod_day.shape)
# print(x.shape)
print(df_sales.shape)


# ### Calculando métricas

# media de produtos disponiveis no período
df_sales["media_prod_days_available"] = (
    df_sales["sold_quantity_sum"] / df_sales["days_available"]
)
df_sales["media_prod_days_available"] = df_sales["media_prod_days_available"].fillna(0)

days = 30

# qtd de produtos a enviar no período, caso seja valor negativo produto está acima do esperado para envio(sobrando)
df_sales["period_send_fulfillment"] = np.ceil(
    (df_sales["sold_quantity_sum"] / df_sales["days_available"]) * days
    - df_sales["available_quantity_today"]
)
df_sales["period_send_fulfillment"] = df_sales["period_send_fulfillment"].fillna(0)

# qtd de produtos a enviar hoje, caso seja valor negativo produto está acima do esperado para envio(sobrando)
df_sales["today_send_fulfillment"] = np.ceil(
    (df_sales["sold_quantity_sum"] / df_sales["days_available"])
    - df_sales["available_quantity_today"]
)
df_sales["today_send_fulfillment"] = df_sales["today_send_fulfillment"].fillna(0)

# st.write(df_sales.columns)
# print(df_sales.columns)

cols = [
    "ml_inventory_id",
    "sold_quantity_sum",
    "ml_code",
    "value_name",
    "variation_id",
    "status",
    "available_quantity_today",
    "detail_status",
    "detail_quantity",
    "data",
    "has_stock",
    "days_available",
    "media_prod_days_available",
    "period_send_fulfillment",
    "today_send_fulfillment",
]

df_sales = df_sales[cols]

st.write(df_sales)
