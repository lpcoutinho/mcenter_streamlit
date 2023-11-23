# from pages.ml_consume.ml_consume import MeLiLoader
# from pages.tiny_consume.tiny_consume import TinyLoader
from dotenv import load_dotenv
import os
import streamlit as st
import time

from pandas import json_normalize
import psycopg2
from psycopg2 import sql
import pandas as pd
from datetime import datetime
import numpy as np

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

# Simula uma consulta longa
def consulta_longa():
    time.sleep(5)  # Simula um processo demorado
    return "Resultados da consulta longa"

# # Interface do Streamlit
st.title("Produtos a enviar ao Fulfillment")

# Selecionar data da pesquisa
st.write('Defina o período da consulta')
date_from = st.date_input(label='Data inicial')
date_to = st.date_input(label='Data final')

st.write('Defina a quantidade de dias para o cálculo de envio')
input_days = st.number_input(label='Enviar produtos para os próximos x dias',step=1,value=30)

# Converta as datas para strings no formato desejado
date_from_str = date_from.strftime('%Y-%m-%d')
date_to_str = date_to.strftime('%Y-%m-%d')


# Botão para iniciar a consulta
if st.button("Iniciar Consulta"):
    # Exibe uma mensagem enquanto a consulta está em andamento
    mensagem_aguarde = st.warning("Aguarde, a consulta está em andamento...")
    # meli = MeLiLoader(db_config, ACCESS_TOKEN)
    
    # json_list = meli.get_orders_data(date_from_str, date_to_str)
    # # Realiza a consulta longa
    # resultados = consulta_longa()

    # json_list = meli.get_orders_data(date_from_str, date_to_str)
    # # st.json(json_list)
    # df_payments = meli.process_payment_data(json_list)
    # # df_payments
    
    # df_orders = meli.process_order_data(json_list)
    # # df_orders
    
    # df_res_fulfillment = meli.process_shipments_data(df_orders,df_payments)
    # # df_res_fulfillment

    # df_tiny_fulfillment = meli.load_tiny_fulfillment()
    # # df_tiny_fulfillment
    
    # df_fulfillment_stock = meli.load_fulfillment_stock_hist()
    # # df_fulfillment_stock

    # df_stock, df_orders = meli.process_data(df_res_fulfillment, df_tiny_fulfillment, df_fulfillment_stock, date_to, date_from)
    # # df_stock
    # # df_orders
    
    # df_unido = meli.process_sales_and_inventory_data(df_orders, df_stock, df_tiny_fulfillment)
    # # df_unido

    # df_unido, df_fulfillment_greater_zero, df_fulfillment_less_one = meli.calculate_fulfillment_metrics(df_unido, date_from, date_to)

    # df_tiny_stock = tiny.get_tiny_stock_hist(db_config)
    
    # df_resultado = meli.filter_today_fulfillment(df_tiny_stock,df_fulfillment_greater_zero)
    # df_resultado
    
    # # Remove a mensagem de aviso e exibe os resultados
    mensagem_aguarde.empty()
    st.success("Consulta concluída com sucesso!")
    

    # Defina as datas de início e fim desejadas
    data_inicio = datetime(2023, 11, 20).date()
    data_fim = datetime(2023, 12, 23).date()

    print(type(data_inicio))
    print(type(date_to_str))
    print(type(date_from))
    print(data_inicio)
    print(date_to)
    
    # Registra o tempo antes da execução
    start_prog = time.time()

    # Abra a conexão com o banco de dados
    try:
        conn = psycopg2.connect(**db_config)
        
        # Construa a consulta SQL com a condição de data
        sql_query = f"SELECT * FROM ml_orders_hist WHERE date_closed BETWEEN '{date_from}' AND '{date_to}'"
        
        # sql_query = f"SELECT * FROM ml_orders_hist WHERE date_closed BETWEEN '{data_inicio}' AND '{data_fim}'"
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

    # filtrando apenas orders do fulfillment
    df_orders = df_orders[df_orders['fulfilled'] == True]

    # Buscando dados de produtos no BD
    try:
        conn = psycopg2.connect(**db_config)

        sql_query = "SELECT * FROM tiny_fulfillment"
        df_codes = pd.read_sql(sql_query, conn)
    except psycopg2.Error as e:
        # logger.error(f"Erro do psycopg2 ao consultar fulfillment_stock: {e}")
        print(f"Erro do psycopg2 ao consultar fulfillment_stock: {e}")
        
    except Exception as e:
        # logger.error(f"Erro ao consultar tabela tiny_fulfillment: {e}")
        print(f"Erro ao consultar tabela tiny_fulfillment: {e}")

    finally:
        if conn is not None:
            conn.close()

    df_codes['ml_code'] = df_codes['ml_code'].apply(lambda x: 'MLB' + str(x))

    # unindo DFs
    df_filtered = pd.merge(df_orders, df_codes, left_on=['ml_code', 'seller_sku'], right_on=['ml_code', 'ml_sku'], how='left')
    df_filtered['ml_inventory_id'] = df_filtered['ml_inventory_id'].replace('NaN', pd.NA)
    df_filtered = df_filtered.dropna(subset=['ml_inventory_id'])

    # filtrando apenas aprovados
    df_filtered = df_filtered[df_filtered['payment_status'] == 'approved']

    # Soma de produtos vendidos
    soma_por_ml_inventory_id = df_filtered.groupby('ml_inventory_id')['quantity'].sum().reset_index()

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
            
    # Ordenando stock por data
    df_stock = df_stock.sort_values(by='created_at', ascending=False)
    df_stock['data'] = df_stock['created_at'].dt.date
    df_stock = df_stock.drop(['created_at'], axis=1)

    # Filtrando dados de orders por datas
    df_orders = df_filtered.copy()
    df_orders['date_approved'] = pd.to_datetime(df_orders['date_approved'])
    df_orders['data'] = df_orders['date_approved'].dt.date
    df_orders = df_orders.drop(['date_closed', 'created_at', 'updated_at'], axis=1)

    ## Cria coluna has_stock, se available_quantity <= 0, has_stock= False ##
    df_stock = df_stock.assign(has_stock=lambda x: x["available_quantity"] > 0)
    df_stock = df_stock.sort_values(by='data', ascending=False).reset_index(drop=True)

    ## Contando dias em que produto esteve disponível 
    days_available = df_stock.groupby('ml_inventory_id')['has_stock'].sum().reset_index()
    days_available = days_available.rename(columns={'has_stock': 'days_available'})

    # Unindo DFs
    df_stock = df_stock.merge(days_available, on='ml_inventory_id', how='left')

    # Total de vendas por ml_inventory_id
    total_sales_by_id = df_orders.groupby('ml_inventory_id')['quantity'].sum().reset_index()

    # Acrescentando total de vendas ao DF
    df_total_sales = pd.merge(df_orders, total_sales_by_id, on='ml_inventory_id', how='left')

    df_total_sales.rename(columns={'quantity_x': 'sales_quantity'}, inplace=True)
    df_total_sales.rename(columns={'quantity_y': 'total_sales_quantity'}, inplace=True)
    df_total_sales['sales_quantity'] = df_total_sales['sales_quantity'].fillna(0)
    df_total_sales['total_sales_quantity'] = df_total_sales['total_sales_quantity'].fillna(0)

    df_stock_days_available = df_stock.copy()

    indices_recentes = df_stock_days_available.groupby('ml_inventory_id')['data'].idxmax()

    df_stock_days_available = df_stock_days_available.loc[indices_recentes]
    df_stock_days_available = df_stock_days_available.drop(['has_stock'], axis=1)

    df_total_sales_filter = df_total_sales[['ml_inventory_id', 'ml_code','ml_sku', 'tiny_id', 'date_approved', 'sales_quantity', 'total_sales_quantity']]
    df_total_sales_filter = df_total_sales_filter.drop_duplicates()

    indices_recentes = df_total_sales.groupby('ml_inventory_id')['date_approved'].idxmax()
    df_total_sales = df_total_sales.loc[indices_recentes]
    df_total_sales = df_total_sales.drop(['date_approved','sales_quantity'], axis=1)

    ## Juntando dias disponiveis e total de vendas
    df_to_calc = pd.merge(df_stock_days_available, df_total_sales, on='ml_inventory_id', how='left')
    df_to_calc['total_sales_quantity'] = df_to_calc['total_sales_quantity'].fillna(0)

    df_to_calc = df_to_calc.dropna(subset=['ml_code', 'ml_sku'])
    df_to_calc['total_sales_quantity'] = df_to_calc['total_sales_quantity'].fillna(0)
    df_to_calc['days_available'] = df_to_calc['days_available'].fillna(0)

    # Calculando Métricas
    df_to_calc['media_prod_days_available'] = (df_to_calc['total_sales_quantity'] / df_to_calc['days_available'])
    df_to_calc['media_prod_days_available'] = df_to_calc['media_prod_days_available'].fillna(0)

    df_to_calc['media_prod_days_available'] = (df_to_calc['total_sales_quantity'] / df_to_calc['days_available'])
    df_to_calc['media_prod_days_available'] = df_to_calc['media_prod_days_available'].fillna(0)

    days = input_days
    days = 30
    print('dias', days)

    df_to_calc['period_send_fulfillment'] = np.ceil((df_to_calc['total_sales_quantity'] / df_to_calc['days_available'])* days - df_to_calc['available_quantity'])
    df_to_calc['period_send_fulfillment'] = df_to_calc['period_send_fulfillment'].fillna(0)

    df_to_calc['today_send_fulfillment'] = np.ceil((df_to_calc['total_sales_quantity'] / df_to_calc['days_available']) - df_to_calc['available_quantity'])
    df_to_calc['today_send_fulfillment'] = df_to_calc['today_send_fulfillment'].fillna(0)

    # Selecionando a exibição
    df_to_calc = df_to_calc.drop(['category_id','pack_id','title','order_status', 'payment_status', 'seller_sku', 'shipping_id', 'fulfilled', 'mcenter_id', 'variation_id', 'var_code', 'ad_title', 'data_y', 'tiny_sku'], axis=1)
    df_to_calc = df_to_calc.rename(columns={'data_x':'data'})


    ## Filtrando produtos a serem envidos ou não, period_send_fulfillment > 0 produto deve ser enviado 
    df_fulfillment_greater_zero = df_to_calc[df_to_calc['period_send_fulfillment'] > 0]
    df_fulfillment_less_one = df_to_calc[df_to_calc['period_send_fulfillment'] <= 0]


    ## Comparando com stock Tiny

    # Pegando dados de stock da tiny
    # Buscando histórico de estoque na tabela
    try:
        conn = psycopg2.connect(**db_config)

        sql_query = "SELECT * FROM tiny_stock_hist"
        df_tiny_stock = pd.read_sql(sql_query, conn)

    except psycopg2.Error as e:
        print(f"Erro do psycopg2 ao consultar fulfillment_stock: {e}")

    except Exception as e:
        print(f"Erro ao consultar fulfillment_stock: {e}")

    finally:
        if conn is not None:
            conn.close()

    # Obtém a data de hoje
    data_de_hoje = pd.to_datetime('today').date()

    # Filtra apenas as linhas onde 'created_at' é igual à data de hoje
    df_today = df_tiny_stock[df_tiny_stock['created_at'].dt.date == data_de_hoje]

    df_res = df_today[df_today['tiny_id'].isin(df_fulfillment_greater_zero['tiny_id'])]

    df_res_ = pd.merge(df_fulfillment_greater_zero, df_res, on='tiny_id', how='left')

    df_send = df_res_[df_res_['deposito_saldo'] > 0]

    cols = ['ml_inventory_id', 'ml_code','ml_sku', 'tiny_id',  'days_available', 'total_sales_quantity', 'media_prod_days_available', 'period_send_fulfillment', 'today_send_fulfillment', 'saldo_reservado', 'deposito_nome', 'deposito_saldo', 'deposito_empresa', 'data']
    df_send = df_send[cols]

    st.dataframe(df_send, use_container_width=True)
