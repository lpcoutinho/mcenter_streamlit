# from pages.ml_consume.ml_consume import MeLiLoader
# from pages.tiny_consume.tiny_consume import TinyLoader
import os
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import psycopg2
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

# Interface do Streamlit
st.title("Produtos a enviar ao Fulfillment")

# Selecionar data da pesquisa
st.write("Defina o período da consulta")
date_from = st.date_input(label="Data inicial")
date_t = st.date_input(label="Data final")
date_to = date_t + timedelta(days=1)  # + 1 dia para pegar a data atual no DB

st.write(f"Perído a consultar vai de {date_from} até {date_t}")

# date_from = st.text_input(label='Data inicial, digite apenas números',placeholder='01112023',max_chars=8)
# date_to = st.text_input(label='Data final, digite apenas números',placeholder='30112023')

st.write("Defina a quantidade de dias para o cálculo de envio")
input_days = st.number_input(
    label="Enviar produtos para os próximos x dias", step=1, value=30
)

# Botão para iniciar a consulta
if st.button("Iniciar Consulta"):
    # Exibe uma mensagem enquanto a consulta está em andamento
    mensagem_aguarde = st.warning("Aguarde, a consulta está em andamento...")

    # # Remove a mensagem de aviso e exibe os resultados
    mensagem_aguarde.empty()
    st.success("Consulta concluída com sucesso!")

    # ### Historico de estoque
    # Buscando histórico de estoque na tabela
    try:
        conn = psycopg2.connect(**db_config)

        # sql_query = f"SELECT * FROM fulfillment_stock_hist WHERE created_at BETWEEN '{ano_from}-{mes_from}-{dia_from}' AND '{ano_to}-{mes_to}-{dia_to}'"
        sql_query = f"SELECT * FROM fulfillment_stock_hist WHERE created_at BETWEEN '{date_from}' AND '{date_to}'"
        df_stock = pd.read_sql(sql_query, conn)

    except psycopg2.Error as e:
        print(f"Erro do psycopg2 ao consultar fulfillment_stock: {e}")

    except Exception as e:
        print(f"Erro ao consultar fulfillment_stock: {e}")

    finally:
        if conn is not None:
            conn.close()

    # datas consultadas, dias em que um produto pode ou não estar disponível
    df_stock["created_at"].value_counts().index.to_list()

    # Ordenando stock por data
    # df_stock["created_at"] = pd.to_datetime(df_stock["created_at"])
    df_stock = df_stock.sort_values(by="created_at", ascending=False)
    df_stock["data"] = df_stock["created_at"].dt.date
    df_stock = df_stock.drop(["created_at"], axis=1)

    ## Cria coluna has_stock, se available_quantity <= 0, has_stock= False ##
    df_stock = df_stock.assign(has_stock=lambda x: x["available_quantity"] > 0)
    df_stock = df_stock.sort_values(by="data", ascending=False).reset_index(drop=True)

    df_stock = df_stock.drop_duplicates()

    # Dias em que produto esteve disponível
    #  Contando dias em que produto esteve disponível
    days_available = (
        df_stock.groupby("ml_inventory_id")["has_stock"].sum().reset_index()
    )
    days_available = days_available.rename(columns={"has_stock": "days_available"})

    # Unindo DFs
    df_stock = df_stock.merge(days_available, on="ml_inventory_id", how="inner")

    # datas
    data_de_hoje = datetime.now().date()
    data_de_ontem = datetime.now().date() - timedelta(days=1)

    df_stock["data"] = pd.to_datetime(df_stock["data"])

    # Filtra apenas as linhas onde 'data' é igual à data de hoje
    df_stock_today = df_stock[df_stock["data"].dt.date == data_de_hoje]
    df_stock_today = df_stock_today.rename(
        columns={"available_quantity": "available_quantity_today"}
    )
    # df_stock_today = df_stock.drop(['has_stock'], axis=1)

    st.write("Estoque de produtos do fulfillment")
    st.dataframe(df_stock, use_container_width=True)

    st.write("Disponiblidade dos produtos hoje e dias disponíveis")
    # df_available = df_stock_today.drop(['available_quantity_today'], axis=1)
    # st.dataframe(df_available, use_container_width=True)
    st.dataframe(df_stock_today, use_container_width=True)

    # SIZW84848

    #     # ### Buscando hitorico de orders no BD
    #     # Buscando histórico de vendas na tabela ml_orders_hist para o período definido
    #     try:
    #         conn = psycopg2.connect(**db_config)

    #         # Construa a consulta SQL com a condição de data
    #         sql_query = f"SELECT * FROM ml_orders_hist WHERE date_closed BETWEEN '{ano_from}-{mes_from}-{dia_from}' AND '{ano_to}-{mes_to}-{dia_to}'"
    #         # sql_query = f"SELECT * FROM ml_orders_hist WHERE date_closed BETWEEN '{date_from}' AND '{date_to}'"
    #         # Execute a consulta e leia os dados em um DataFrame
    #         df_orders = pd.read_sql(sql_query, conn)

    #     except psycopg2.Error as e:
    #         print(f"Erro do psycopg2 ao consultar ml_orders_hist: {e}")
    #         # logger.error(f"Erro do psycopg2 ao consultar ml_orders_hist: {e}")

    #     except Exception as e:
    #         print(f"Erro ao consultar ml_orders_hist: {e}")
    #         # logger.error(f"Erro ao consultar ml_orders_hist: {e}")

    #     finally:
    #         if conn is not None:
    #             conn.close()

    #     # filtros
    #     df_orders = df_orders[df_orders["fulfilled"] == True]
    #     df_orders = df_orders[df_orders["order_status"] == "paid"]
    #     df_orders = df_orders[df_orders["payment_status"] == "approved"]
    #     df_orders = df_orders.drop(
    #         ["pack_id", "date_approved", "fulfilled", "order_status", "payment_status"], axis=1
    #     )
    #     df_orders.rename(columns={"quantity": "sales_quantity"}, inplace=True)

    #     # Ordenando orders por data
    #     df_orders = df_orders.sort_values(by="date_closed", ascending=False)
    #     df_orders["data"] = df_orders["date_closed"].dt.date
    #     df_orders = df_orders.drop(["date_closed"], axis=1)
    #     df_orders = df_orders.drop_duplicates()

    #     # st.write('Vendas')
    #     # st.dataframe(df_orders, use_container_width=True)

    #     # #### Total de vendas por ml_code e seller_sku

    #     # calcular total de vendas por ml_code e seller_sku no periodo
    #     total_sales_by_filter = (
    #         df_orders.groupby(["ml_code", "seller_sku"])["sales_quantity"].sum().reset_index()
    #     )
    #     total_sales_by_filter.rename(
    #         columns={"sales_quantity": "total_sales_quantity"}, inplace=True
    #     )

    #     # Acrescentando total de vendas ao DF
    #     df_total_sales = pd.merge(
    #         df_orders, total_sales_by_filter, on=["ml_code", "seller_sku"], how="inner"
    #     )

    #     df_total_sales = df_total_sales.drop(["sales_quantity", "shipping_id", "data"], axis=1)
    #     df_total_sales = df_total_sales.drop_duplicates()

    #     # st.write('Total de vendas')
    #     # st.dataframe(df_total_sales, use_container_width=True)

    #     # #### Buscando Produtos
    #     # Buscando dados de produtos na tabela tiny_fulfillment
    #     try:
    #         conn = psycopg2.connect(**db_config)

    #         sql_query = "SELECT * FROM tiny_fulfillment"
    #         df_codes = pd.read_sql(sql_query, conn)
    #     except psycopg2.Error as e:
    #         # logger.error(f"Erro do psycopg2 ao consultar fulfillment_stock: {e}")
    #         print(f"Erro do psycopg2 ao consultar tiny_fulfillment: {e}")

    #     except Exception as e:
    #         # logger.error(f"Erro ao consultar tabela tiny_fulfillment: {e}")
    #         print(f"Erro ao consultar tabela tiny_fulfillment: {e}")

    #     finally:
    #         if conn is not None:
    #             conn.close()

    #     df_codes["ml_code"] = df_codes["ml_code"].apply(lambda x: "MLB" + str(x))
    #     df_codes.rename(columns={"quantity": "total_sales_quantity"}, inplace=True)
    #     df_codes = df_codes.drop(["mcenter_id", "created_at", "updated_at"], axis=1)

    #     # st.write('FulxTiny')
    #     # st.dataframe(df_codes, use_container_width=True)

    #     # ### Produtos + Dias disponíveis
    #     prod_day = pd.merge(df_codes, df_stock_today, on="ml_inventory_id", how="inner")

    #     # st.write('Produtos + Dias disponíveis')
    #     # st.dataframe(prod_day, use_container_width=True)

    #     # ### Prod_Day + Total_sales
    #     df_sales = pd.merge(
    #         df_total_sales,
    #         prod_day,
    #         left_on=["ml_code", "seller_sku"],
    #         right_on=["ml_code", "ml_sku"],
    #         how="inner",
    #     )

    #     cols = [
    #         "ml_code",
    #         "ml_sku",
    #         "ml_inventory_id",
    #         "tiny_id",
    #         "tiny_sku",
    #         "var_code",
    #         "variation_id",
    #         "title",
    #         "total_sales_quantity",
    #         "qtd_item",
    #         "days_available",
    #         "available_quantity_today",
    #         "data",
    #     ]

    #     df_sales = df_sales[cols]

    #     # st.write('df_sales')
    #     # st.dataframe(df_sales, use_container_width=True)

    #     # ### Calculando métricas
    #     # media de produtos disponiveis no período
    #     df_sales["media_prod_days_available"] = (
    #         df_sales["total_sales_quantity"] / df_sales["days_available"]
    #     )
    #     df_sales["media_prod_days_available"] = df_sales["media_prod_days_available"].fillna(0)

    #     days = input_days

    #     # qtd de produtos a enviar no período, caso seja valor negativo produto está acima do esperado para envio(sobrando)
    #     df_sales["period_send_fulfillment"] = np.ceil(
    #         (df_sales["total_sales_quantity"] / df_sales["days_available"]) * days
    #         - df_sales["available_quantity_today"]
    #     )
    #     df_sales["period_send_fulfillment"] = df_sales["period_send_fulfillment"].fillna(0)

    #     # qtd de produtos a enviar hoje, caso seja valor negativo produto está acima do esperado para envio(sobrando)
    #     df_sales["today_send_fulfillment"] = np.ceil(
    #         (df_sales["total_sales_quantity"] / df_sales["days_available"])
    #         - df_sales["available_quantity_today"]
    #     )
    #     df_sales["today_send_fulfillment"] = df_sales["today_send_fulfillment"].fillna(0)

    #     st.write('Metricas')
    #     st.dataframe(df_sales, use_container_width=True)

    # Defina as datas de início e fim desejadas
    # date_from = datetime(2023, 11, 15).date()
    # date_to = datetime(2023, 12, 23).date()

    # Historico de estoque
    # Buscando histórico de estoque na tabela
    # try:
    #     conn = psycopg2.connect(**db_config)

    #     sql_query = f"SELECT * FROM fulfillment_stock_hist WHERE created_at BETWEEN '{date_from}' AND '{date_to}'"
    #     df_stock = pd.read_sql(sql_query, conn)
    #     print(df_stock)
    # except psycopg2.Error as e:
    #     print(f"Erro do psycopg2 ao consultar fulfillment_stock: {e}")

    # except Exception as e:
    #     print(f"Erro ao consultar fulfillment_stock: {e}")

    # finally:
    #     if conn is not None:
    #         conn.close()

    # # datas consultadas, dias em que um produto pode ou não estar disponível
    # df_stock["created_at"].value_counts().index.to_list()

    # # Ordenando stock por data
    # df_stock = df_stock.sort_values(by="created_at", ascending=False)
    # df_stock["data"] = df_stock["created_at"].dt.date
    # df_stock = df_stock.drop(["created_at"], axis=1)

    # # Cria coluna has_stock, se available_quantity <= 0, has_stock= False ##
    # df_stock = df_stock.assign(has_stock=lambda x: x["available_quantity"] > 0)
    # df_stock = df_stock.sort_values(by="data", ascending=False).reset_index(drop=True)

    # df_stock = df_stock.drop_duplicates()

    # # Dias em que produto esteve disponível

    # ## Contando dias em que produto esteve disponível
    # days_available = (
    #     df_stock.groupby("ml_inventory_id")["has_stock"].sum().reset_index()
    # )
    # days_available = days_available.rename(columns={"has_stock": "days_available"})

    # # Unindo DFs
    # df_stock = df_stock.merge(days_available, on="ml_inventory_id", how="inner")

    # # data de hoje
    # # data_de_hoje = datetime.now().date() - timedelta(days=1)
    # # print(data_de_hoje)
    # data_de_hoje = datetime.now().date()
    # df_stock["data"] = pd.to_datetime(df_stock["data"])

    # # Filtra apenas as linhas onde 'data' é igual à data de hoje
    # df_stock_today = df_stock[df_stock["data"].dt.date == data_de_hoje]
    # df_stock_today = df_stock_today.rename(
    #     columns={"available_quantity": "available_quantity_today"}
    # )

    # df_stock_today["days_available"].value_counts()

    # # Buscando hitorico de orders no BD
    # # Buscando histórico de vendas na tabela ml_orders_hist para o período definido
    # try:
    #     conn = psycopg2.connect(**db_config)

    #     # Construa a consulta SQL com a condição de data
    #     sql_query = f"SELECT * FROM ml_orders_hist WHERE date_closed BETWEEN '{date_from}' AND '{date_to}'"
    #     print(sql_query)
    #     # Execute a consulta e leia os dados em um DataFrame
    #     df_orders = pd.read_sql(sql_query, conn)

    # except psycopg2.Error as e:
    #     print(f"Erro do psycopg2 ao consultar ml_orders_hist: {e}")
    #     # logger.error(f"Erro do psycopg2 ao consultar ml_orders_hist: {e}")

    # except Exception as e:
    #     print(f"Erro ao consultar ml_orders_hist: {e}")
    #     # logger.error(f"Erro ao consultar ml_orders_hist: {e}")

    # finally:
    #     if conn is not None:
    #         conn.close()

    # # filtros
    # df_orders = df_orders[df_orders["fulfilled"] == True]
    # df_orders = df_orders[df_orders["order_status"] == "paid"]
    # df_orders = df_orders[df_orders["payment_status"] == "approved"]
    # df_orders = df_orders.drop(
    #     ["pack_id", "date_approved", "fulfilled", "order_status", "payment_status"],
    #     axis=1,
    # )
    # df_orders.rename(columns={"quantity": "sales_quantity"}, inplace=True)

    # # Ordenando orders por data
    # df_orders = df_orders.sort_values(by="date_closed", ascending=False)
    # df_orders["data"] = df_orders["date_closed"].dt.date
    # df_orders = df_orders.drop(["date_closed"], axis=1)

    # df_orders = df_orders.drop_duplicates()

    # # Total de vendas por ml_code e seller_sku

    # # calcular total de vendas por ml_code e seller_sku no periodo
    # total_sales_by_filter = (
    #     df_orders.groupby(["ml_code", "seller_sku"])["sales_quantity"]
    #     .sum()
    #     .reset_index()
    # )
    # total_sales_by_filter.rename(
    #     columns={"sales_quantity": "total_sales_quantity"}, inplace=True
    # )

    # # Acrescentando total de vendas ao DF
    # df_total_sales = pd.merge(
    #     df_orders, total_sales_by_filter, on=["ml_code", "seller_sku"], how="inner"
    # )

    # df_total_sales = df_total_sales.drop(
    #     ["sales_quantity", "shipping_id", "data"], axis=1
    # )
    # df_total_sales = df_total_sales.drop_duplicates()

    # # #### Buscando Produtos
    # # Buscando dados de produtos na tabela tiny_fulfillment
    # try:
    #     conn = psycopg2.connect(**db_config)

    #     sql_query = "SELECT * FROM tiny_fulfillment"
    #     df_codes = pd.read_sql(sql_query, conn)
    # except psycopg2.Error as e:
    #     # logger.error(f"Erro do psycopg2 ao consultar fulfillment_stock: {e}")
    #     print(f"Erro do psycopg2 ao consultar tiny_fulfillment: {e}")

    # except Exception as e:
    #     # logger.error(f"Erro ao consultar tabela tiny_fulfillment: {e}")
    #     print(f"Erro ao consultar tabela tiny_fulfillment: {e}")

    # finally:
    #     if conn is not None:
    #         conn.close()

    # df_codes["ml_code"] = df_codes["ml_code"].apply(lambda x: "MLB" + str(x))
    # df_codes.rename(columns={"quantity": "total_sales_quantity"}, inplace=True)
    # df_codes = df_codes.drop(["mcenter_id", "created_at", "updated_at"], axis=1)

    # # ### Produtos + Dias disponíveis
    # prod_day = pd.merge(df_codes, df_stock_today, on="ml_inventory_id", how="inner")

    # # ### Prod_Day + Total_sales
    # df_sales = pd.merge(
    #     df_total_sales,
    #     prod_day,
    #     left_on=["ml_code", "seller_sku"],
    #     right_on=["ml_code", "ml_sku"],
    #     how="inner",
    # )

    # cols = [
    #     "ml_code",
    #     "ml_sku",
    #     "ml_inventory_id",
    #     "tiny_id",
    #     "tiny_sku",
    #     "var_code",
    #     "variation_id",
    #     "title",
    #     "total_sales_quantity",
    #     "qtd_item",
    #     "days_available",
    #     "available_quantity_today",
    #     "data",
    # ]

    # df_sales = df_sales[cols]

    # # media de produtos disponiveis no período
    # df_sales["media_prod_days_available"] = (
    #     df_sales["total_sales_quantity"] / df_sales["days_available"]
    # )
    # df_sales["media_prod_days_available"] = df_sales[
    #     "media_prod_days_available"
    # ].fillna(0)

    # days = input_days

    # # qtd de produtos a enviar no período, caso seja valor negativo produto está acima do esperado para envio(sobrando)
    # df_sales["period_send_fulfillment"] = np.ceil(
    #     (df_sales["total_sales_quantity"] / df_sales["days_available"]) * days
    #     - df_sales["available_quantity_today"]
    # )
    # df_sales["period_send_fulfillment"] = df_sales["period_send_fulfillment"].fillna(0)

    # # qtd de produtos a enviar hoje, caso seja valor negativo produto está acima do esperado para envio(sobrando)
    # df_sales["today_send_fulfillment"] = np.ceil(
    #     (df_sales["total_sales_quantity"] / df_sales["days_available"])
    #     - df_sales["available_quantity_today"]
    # )
    # df_sales["today_send_fulfillment"] = df_sales["today_send_fulfillment"].fillna(0)

    # st.write("Métricas")
    # st.dataframe(df_sales, use_container_width=True)
