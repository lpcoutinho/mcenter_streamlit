#!/usr/bin/env python
# coding: utf-8

# Depositos para enviar, orderm de preferencia:
# musical_matriz
# musical_filal
# em seguida onde houver mais estoque
#
#
# onde days_available = 0 criar observação onde informa que há muito tempo sem estoque

# In[1]:


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

# Registra o tempo antes da execução
start_prog = time.time()


# In[2]:


# pd.set_option('display.max_rows', None)
pd.set_option("display.max_colwidth", None)

pd.reset_option("display.max_columns")


# In[3]:


def condf(df, coluna, valor):
    """
    Consulta um DataFrame com base em uma coluna e valor específicos.

    Parâmetros:
    - df: DataFrame a ser consultado.
    - coluna: Nome da coluna para a condição de consulta.
    - valor: Valor desejado na coluna.

    Retorna:
    Um DataFrame contendo apenas as linhas que atendem à condição.
    """
    resultado = df[df[coluna] == valor]
    return resultado


def condf_date(df, coluna_data, data_pesquisada):
    """
    Consulta um DataFrame com base em uma coluna de datas.

    Parâmetros:
    - df: DataFrame a ser consultado.
    - coluna_data: Nome da coluna de datas.
    - data_pesquisada: Data desejada para a consulta.

    Retorna:
    Um DataFrame contendo apenas as linhas que correspondem à data pesquisada.
    """
    resultado = df[pd.to_datetime(df[coluna_data]).dt.date == data_pesquisada]
    return resultado


# ### Período a consultar

# In[4]:


# Defina as datas de início e fim desejadas
data_inicio = datetime(2023, 11, 15).date()
data_fim = datetime(2023, 12, 23).date()


# ### Historico de estoque

# In[20]:


# Buscando histórico de estoque na tabela
try:
    conn = psycopg2.connect(**db_config)

    sql_query = f"SELECT * FROM fulfillment_stock_hist WHERE created_at BETWEEN '{data_inicio}' AND '{data_fim}'"
    df_stock = pd.read_sql(sql_query, conn)

except psycopg2.Error as e:
    print(f"Erro do psycopg2 ao consultar fulfillment_stock: {e}")

except Exception as e:
    print(f"Erro ao consultar fulfillment_stock: {e}")

finally:
    if conn is not None:
        conn.close()


# In[21]:


df_stock


# In[22]:


# datas consultadas, dias em que um produto pode ou não estar disponível
df_stock["created_at"].value_counts().index.to_list()


# In[23]:


# Ordenando stock por data
df_stock = df_stock.sort_values(by="created_at", ascending=False)
df_stock["data"] = df_stock["created_at"].dt.date
df_stock = df_stock.drop(["created_at"], axis=1)

df_stock


# In[24]:


## Cria coluna has_stock, se available_quantity <= 0, has_stock= False ##
df_stock = df_stock.assign(has_stock=lambda x: x["available_quantity"] > 0)
df_stock = df_stock.sort_values(by="data", ascending=False).reset_index(drop=True)
df_stock


# In[25]:


df_stock = df_stock.drop_duplicates()


# In[26]:


condf(df_stock, "ml_inventory_id", "DSGP06967")


# #### Dias em que produto esteve disponível

# In[27]:


## Contando dias em que produto esteve disponível
days_available = df_stock.groupby("ml_inventory_id")["has_stock"].sum().reset_index()
days_available = days_available.rename(columns={"has_stock": "days_available"})


# In[28]:


condf(days_available, "ml_inventory_id", "DSGP06967")


# In[33]:


# Unindo DFs
df_stock = df_stock.merge(days_available, on="ml_inventory_id", how="inner")

df_stock.shape


# In[34]:


# data de hoje
# data_de_hoje = datetime.now().date() - timedelta(days=1)
# print(data_de_hoje)
data_de_hoje = datetime.now().date()
df_stock["data"] = pd.to_datetime(df_stock["data"])

# Filtra apenas as linhas onde 'data' é igual à data de hoje
df_stock_today = df_stock[df_stock["data"].dt.date == data_de_hoje]
df_stock_today = df_stock_today.rename(
    columns={"available_quantity": "available_quantity_today"}
)
# df_stock_today = df_stock.drop(['has_stock'], axis=1)


# In[35]:


df_stock_today


# In[36]:


df_stock_today["days_available"].value_counts()


# ### Buscando hitorico de orders no BD

# In[37]:


# Buscando histórico de vendas na tabela ml_orders_hist para o período definido
try:
    conn = psycopg2.connect(**db_config)

    # Construa a consulta SQL com a condição de data
    sql_query = f"SELECT * FROM ml_orders_hist WHERE date_closed BETWEEN '{data_inicio}' AND '{data_fim}'"
    print(sql_query)
    # Execute a consulta e leia os dados em um DataFrame
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
df_orders.rename(columns={"quantity": "sales_quantity"}, inplace=True)


# In[38]:


df_orders.sample()


# In[39]:


# Ordenando orders por data
df_orders = df_orders.sort_values(by="date_closed", ascending=False)
df_orders["data"] = df_orders["date_closed"].dt.date
df_orders = df_orders.drop(["date_closed"], axis=1)

print(df_orders.shape)
df_orders.head(3)


# In[40]:


df_orders = df_orders.drop_duplicates()
df_orders.shape


# In[41]:


condf(df_orders, "ml_code", "MLB1992541482")


# #### Total de vendas por ml_code e seller_sku

# In[42]:


# calcular total de vendas por ml_code e seller_sku no periodo
total_sales_by_filter = (
    df_orders.groupby(["ml_code", "seller_sku"])["sales_quantity"].sum().reset_index()
)
total_sales_by_filter.rename(
    columns={"sales_quantity": "total_sales_quantity"}, inplace=True
)


# In[43]:


condf(total_sales_by_filter, "ml_code", "MLB1992541482")


# In[50]:


# Acrescentando total de vendas ao DF
df_total_sales = pd.merge(
    df_orders, total_sales_by_filter, on=["ml_code", "seller_sku"], how="inner"
)
df_total_sales.shape


# In[51]:


df_total_sales.head(3)


# In[52]:


condf(df_total_sales, "ml_code", "MLB1992541482")


# In[53]:


df_total_sales = df_total_sales.drop(["sales_quantity", "shipping_id", "data"], axis=1)
df_total_sales = df_total_sales.drop_duplicates()


# In[54]:


condf(df_total_sales, "ml_code", "MLB1992541482")


# Neste ponto temos o total de vendas de um anúncio por período e a quantidade de dias em que um produto esteve disponível.
# precisamos juntar esses dados para calcular, para isso trarei as informações de produtos

# In[55]:


print(df_total_sales.shape)
df_total_sales.sample()


# In[56]:


print(df_stock_today.shape)
df_stock_today.sample()


# #### Buscando Produtos

# In[57]:


# Buscando dados de produtos na tabela tiny_fulfillment
try:
    conn = psycopg2.connect(**db_config)

    sql_query = "SELECT * FROM tiny_fulfillment"
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

df_codes["ml_code"] = df_codes["ml_code"].apply(lambda x: "MLB" + str(x))
df_codes.rename(columns={"quantity": "total_sales_quantity"}, inplace=True)
df_codes = df_codes.drop(["mcenter_id", "created_at", "updated_at"], axis=1)


# In[58]:


df_codes.sample()


# ### Produtos + Dias disponíveis

# In[64]:


print(df_codes.shape)
print(df_stock_today.shape)

print(df_codes.columns)
print(df_stock_today.columns)


# In[78]:


prod_day = pd.merge(df_codes, df_stock_today, on="ml_inventory_id", how="inner")

prod_day.shape


# In[79]:


prod_day["ml_inventory_id"].value_counts()
condf(prod_day, "ml_inventory_id", "FSNB76403")


# ### Prod_Day + Total_sales

# In[80]:


print(df_total_sales.shape)
df_total_sales.sample()


# In[121]:


df_sales = pd.merge(
    df_total_sales,
    prod_day,
    left_on=["ml_code", "seller_sku"],
    right_on=["ml_code", "ml_sku"],
    how="inner",
)
# x  = pd.merge(df_total_sales, prod_day, left_on=['ml_code','seller_sku'], right_on=['ml_code', 'ml_sku'], how='left')

# df_sales = df_sales.drop([], axis=1)

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

df_sales = df_sales[cols]
print(df_total_sales.shape)
print(prod_day.shape)
# print(x.shape)
print(df_sales.shape)


# In[123]:


df_sales.sample()


# In[124]:


df_sales.columns


# In[125]:


# x = pd.merge(x, df_sales, how='outer', indicator=True)
# x = x[x['_merge'] == 'right_only']

# x


# In[126]:


condf(df_total_sales, "ml_code", "MLB1992541482")


# In[127]:


condf(df_sales, "ml_code", "MLB1992541482")


# ### Calculando métricas

# In[128]:


df_sales.sample()


# In[129]:


# media de produtos disponiveis no período
df_sales["media_prod_days_available"] = (
    df_sales["total_sales_quantity"] / df_sales["days_available"]
)
df_sales["media_prod_days_available"] = df_sales["media_prod_days_available"].fillna(0)

days = 30

# qtd de produtos a enviar no período, caso seja valor negativo produto está acima do esperado para envio(sobrando)
df_sales["period_send_fulfillment"] = np.ceil(
    (df_sales["total_sales_quantity"] / df_sales["days_available"]) * days
    - df_sales["available_quantity_today"]
)
df_sales["period_send_fulfillment"] = df_sales["period_send_fulfillment"].fillna(0)

# qtd de produtos a enviar hoje, caso seja valor negativo produto está acima do esperado para envio(sobrando)
df_sales["today_send_fulfillment"] = np.ceil(
    (df_sales["total_sales_quantity"] / df_sales["days_available"])
    - df_sales["available_quantity_today"]
)
df_sales["today_send_fulfillment"] = df_sales["today_send_fulfillment"].fillna(0)


# In[131]:


print(df_sales)

# In[132]:


condf(df_sales, "ml_code", "MLB1992541482")


# Pergunta:
#
# Caso em seja necessário enviar produtos de um kit e apenas um dos produtos estiver em falta, o que fazer?

#
