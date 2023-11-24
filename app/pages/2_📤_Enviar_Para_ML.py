# from dotenv import load_dotenv
# import os
# import streamlit as st
# import time

# from pandas import json_normalize
# import psycopg2
# from psycopg2 import sql
# import pandas as pd
# from datetime import datetime
# import numpy as np

# load_dotenv()

# ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
# HOST = os.getenv("HOST")
# POSTGRES_DB = os.getenv("POSTGRES_DB")
# POSTGRES_USER = os.getenv("POSTGRES_USER")
# POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# # Informações de conexão com o banco de dados PostgreSQL
# db_config = {
#     "host": HOST,
#     "database": POSTGRES_DB,
#     "user": POSTGRES_USER,
#     "password": POSTGRES_PASSWORD,
# }

# # # Interface do Streamlit
# st.title("Produtos a enviar ao Fulfillment")

# # Selecionar data da pesquisa
# st.write('Defina o período da consulta')
# date_from = st.date_input(label='Data inicial')
# date_to = st.date_input(label='Data final')

# # Selecionar período de cálculo
# st.write('Defina a quantidade de dias para o cálculo de envio')
# input_days = st.number_input(label='Enviar produtos para os próximos x dias',step=1,value=30)

# # Converta as datas para strings no formato desejado
# date_from_str = date_from.strftime('%Y-%m-%d')
# date_to_str = date_to.strftime('%Y-%m-%d')


# # Botão para iniciar a consulta
# if st.button("Iniciar Consulta"):
#     # Exibe uma mensagem enquanto a consulta está em andamento
#     mensagem_aguarde = st.warning("Aguarde, a consulta está em andamento...")
    
#     mensagem_aguarde.empty()
#     st.success("Consulta concluída com sucesso!")
    

#     # Abra a conexão com o banco de dados
#     try:
#         conn = psycopg2.connect(**db_config)
        
#         # Construa a consulta SQL com a condição de data
#         sql_query = f"SELECT * FROM ml_orders_hist WHERE date_closed BETWEEN '{date_from}' AND '{date_to}'"
        
#         # sql_query = f"SELECT * FROM ml_orders_hist WHERE date_closed BETWEEN '{data_inicio}' AND '{data_fim}'"
#         print(sql_query)
#         # Execute a consulta e leia os dados em um DataFrame
#         df_orders = pd.read_sql(sql_query, conn)

#     except psycopg2.Error as e:
#         print(f"Erro do psycopg2 ao consultar ml_orders_hist: {e}")
#         # logger.error(f"Erro do psycopg2 ao consultar ml_orders_hist: {e}")

#     except Exception as e:
#         print(f"Erro ao consultar ml_orders_hist: {e}")
#         # logger.error(f"Erro ao consultar ml_orders_hist: {e}")

# #     finally:
# #         if conn is not None:
# #             conn.close()

#     # filtrando apenas orders do fulfillment
#     df_orders = df_orders[df_orders['fulfilled'] == True]

#     # Buscando dados de produtos no BD
#     try:
#         conn = psycopg2.connect(**db_config)

#         sql_query = "SELECT * FROM tiny_fulfillment"
#         df_codes = pd.read_sql(sql_query, conn)
#     except psycopg2.Error as e:
#         # logger.error(f"Erro do psycopg2 ao consultar fulfillment_stock: {e}")
#         print(f"Erro do psycopg2 ao consultar fulfillment_stock: {e}")
        
#     except Exception as e:
#         # logger.error(f"Erro ao consultar tabela tiny_fulfillment: {e}")
#         print(f"Erro ao consultar tabela tiny_fulfillment: {e}")

#     finally:
#         if conn is not None:
#             conn.close()

#     df_codes['ml_code'] = df_codes['ml_code'].apply(lambda x: 'MLB' + str(x))

#     # unindo DFs
#     df_filtered = pd.merge(df_orders, df_codes, left_on=['ml_code', 'seller_sku'], right_on=['ml_code', 'ml_sku'], how='left')
#     df_filtered['ml_inventory_id'] = df_filtered['ml_inventory_id'].replace('NaN', pd.NA)
#     df_filtered = df_filtered.dropna(subset=['ml_inventory_id'])

#     # filtrando apenas aprovados
#     df_filtered = df_filtered[df_filtered['payment_status'] == 'approved']

#     # Soma de produtos vendidos
#     soma_por_ml_inventory_id = df_filtered.groupby('ml_inventory_id')['quantity'].sum().reset_index()

#     # Buscando histórico de estoque na tabela
#     try:
#         conn = psycopg2.connect(**db_config)

#         sql_query = f"SELECT * FROM fulfillment_stock_hist WHERE created_at BETWEEN '{data_inicio}' AND '{data_fim}'"
#         df_stock = pd.read_sql(sql_query, conn)

#     except psycopg2.Error as e:
#         print(f"Erro do psycopg2 ao consultar fulfillment_stock: {e}")

#     except Exception as e:
#         print(f"Erro ao consultar fulfillment_stock: {e}")

#     finally:
#         if conn is not None:
#             conn.close()
            
#     # Ordenando stock por data
#     df_stock = df_stock.sort_values(by='created_at', ascending=False)
#     df_stock['data'] = df_stock['created_at'].dt.date
#     df_stock = df_stock.drop(['created_at'], axis=1)

#     # Filtrando dados de orders por datas
#     df_orders = df_filtered.copy()
#     df_orders['date_approved'] = pd.to_datetime(df_orders['date_approved'])
#     df_orders['data'] = df_orders['date_approved'].dt.date
#     df_orders = df_orders.drop(['date_closed', 'created_at', 'updated_at'], axis=1)

#     ## Cria coluna has_stock, se available_quantity <= 0, has_stock= False ##
#     df_stock = df_stock.assign(has_stock=lambda x: x["available_quantity"] > 0)
#     df_stock = df_stock.sort_values(by='data', ascending=False).reset_index(drop=True)

#     ## Contando dias em que produto esteve disponível 
#     days_available = df_stock.groupby('ml_inventory_id')['has_stock'].sum().reset_index()
#     days_available = days_available.rename(columns={'has_stock': 'days_available'})

#     # Unindo DFs
#     df_stock = df_stock.merge(days_available, on='ml_inventory_id', how='left')

#     # Total de vendas por ml_inventory_id
#     total_sales_by_id = df_orders.groupby('ml_inventory_id')['quantity'].sum().reset_index()

#     # Acrescentando total de vendas ao DF
#     df_total_sales = pd.merge(df_orders, total_sales_by_id, on='ml_inventory_id', how='left')

#     df_total_sales.rename(columns={'quantity_x': 'sales_quantity'}, inplace=True)
#     df_total_sales.rename(columns={'quantity_y': 'total_sales_quantity'}, inplace=True)
#     df_total_sales['sales_quantity'] = df_total_sales['sales_quantity'].fillna(0)
#     df_total_sales['total_sales_quantity'] = df_total_sales['total_sales_quantity'].fillna(0)

#     df_stock_days_available = df_stock.copy()

#     indices_recentes = df_stock_days_available.groupby('ml_inventory_id')['data'].idxmax()

#     df_stock_days_available = df_stock_days_available.loc[indices_recentes]
#     df_stock_days_available = df_stock_days_available.drop(['has_stock'], axis=1)

#     df_total_sales_filter = df_total_sales[['ml_inventory_id', 'ml_code','ml_sku', 'tiny_id', 'date_approved', 'sales_quantity', 'total_sales_quantity']]
#     df_total_sales_filter = df_total_sales_filter.drop_duplicates()

#     indices_recentes = df_total_sales.groupby('ml_inventory_id')['date_approved'].idxmax()
#     df_total_sales = df_total_sales.loc[indices_recentes]
#     df_total_sales = df_total_sales.drop(['date_approved','sales_quantity'], axis=1)

#     ## Juntando dias disponiveis e total de vendas
#     df_to_calc = pd.merge(df_stock_days_available, df_total_sales, on='ml_inventory_id', how='left')
#     df_to_calc['total_sales_quantity'] = df_to_calc['total_sales_quantity'].fillna(0)

#     df_to_calc = df_to_calc.dropna(subset=['ml_code', 'ml_sku'])
#     df_to_calc['total_sales_quantity'] = df_to_calc['total_sales_quantity'].fillna(0)
#     df_to_calc['days_available'] = df_to_calc['days_available'].fillna(0)

#     # Calculando Métricas
#     df_to_calc['media_prod_days_available'] = (df_to_calc['total_sales_quantity'] / df_to_calc['days_available'])
#     df_to_calc['media_prod_days_available'] = df_to_calc['media_prod_days_available'].fillna(0)

#     df_to_calc['media_prod_days_available'] = (df_to_calc['total_sales_quantity'] / df_to_calc['days_available'])
#     df_to_calc['media_prod_days_available'] = df_to_calc['media_prod_days_available'].fillna(0)

#     days = input_days
#     days = 30
#     print('dias', days)

#     df_to_calc['period_send_fulfillment'] = np.ceil((df_to_calc['total_sales_quantity'] / df_to_calc['days_available'])* days - df_to_calc['available_quantity'])
#     df_to_calc['period_send_fulfillment'] = df_to_calc['period_send_fulfillment'].fillna(0)

#     df_to_calc['today_send_fulfillment'] = np.ceil((df_to_calc['total_sales_quantity'] / df_to_calc['days_available']) - df_to_calc['available_quantity'])
#     df_to_calc['today_send_fulfillment'] = df_to_calc['today_send_fulfillment'].fillna(0)

#     # Selecionando a exibição
#     df_to_calc = df_to_calc.drop(['category_id','pack_id','title','order_status', 'payment_status', 'seller_sku', 'shipping_id', 'fulfilled', 'mcenter_id', 'variation_id', 'var_code', 'ad_title', 'data_y', 'tiny_sku'], axis=1)
#     df_to_calc = df_to_calc.rename(columns={'data_x':'data'})


#     ## Filtrando produtos a serem envidos ou não, period_send_fulfillment > 0 produto deve ser enviado 
#     df_fulfillment_greater_zero = df_to_calc[df_to_calc['period_send_fulfillment'] > 0]
#     df_fulfillment_less_one = df_to_calc[df_to_calc['period_send_fulfillment'] <= 0]


#     ## Comparando com stock Tiny

#     # Pegando dados de stock da tiny
#     # Buscando histórico de estoque na tabela
#     try:
#         conn = psycopg2.connect(**db_config)

#         sql_query = "SELECT * FROM tiny_stock_hist"
#         df_tiny_stock = pd.read_sql(sql_query, conn)

#     except psycopg2.Error as e:
#         print(f"Erro do psycopg2 ao consultar fulfillment_stock: {e}")

#     except Exception as e:
#         print(f"Erro ao consultar fulfillment_stock: {e}")

#     finally:
#         if conn is not None:
#             conn.close()

#     # Obtém a data de hoje
#     data_de_hoje = pd.to_datetime('today').date()

#     # Filtra apenas as linhas onde 'created_at' é igual à data de hoje
#     df_today = df_tiny_stock[df_tiny_stock['created_at'].dt.date == data_de_hoje]

#     df_res = df_today[df_today['tiny_id'].isin(df_fulfillment_greater_zero['tiny_id'])]

#     df_res_ = pd.merge(df_fulfillment_greater_zero, df_res, on='tiny_id', how='left')

#     df_send = df_res_[df_res_['deposito_saldo'] > 0]

#     cols = ['ml_inventory_id', 'ml_code','ml_sku', 'tiny_id',  'days_available', 'total_sales_quantity', 'media_prod_days_available', 'period_send_fulfillment', 'today_send_fulfillment', 'saldo_reservado', 'deposito_nome', 'deposito_saldo', 'deposito_empresa', 'data']
#     df_send = df_send[cols]

    # st.dataframe(df_send, use_container_width=True)
from dotenv import load_dotenv
import os
import streamlit as st
import time

from pandas import json_normalize
import psycopg2
from psycopg2 import sql
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
from loguru import logger

logger.add(
    "Data/Output/Log/enviar_para_ml.log",
    rotation="10 MB",
    format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
)

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

# # Interface do Streamlit
st.title("Produtos a enviar ao Fulfillment")

# Selecionar data da pesquisa
st.write('Defina o período da consulta')
date_from = st.date_input(label='Data inicial')
date_to = st.date_input(label='Data final')

# Selecionar período de cálculo
st.write('Defina a quantidade de dias para o cálculo de envio')
input_days = st.number_input(label='Enviar produtos para os próximos x dias',step=1,value=30)

# Converta as datas para strings no formato desejado
date_from_str = date_from.strftime('%Y-%m-%d')
date_to_str = date_to.strftime('%Y-%m-%d')


# Botão para iniciar a consulta
if st.button("Iniciar Consulta"):
    # Exibe uma mensagem enquanto a consulta está em andamento
    mensagem_aguarde = st.warning("Aguarde, a consulta está em andamento...")
    
    mensagem_aguarde.empty()
    st.success("Consulta concluída com sucesso!")
    
    # Buscando histórico de estoque na tabela fulfillment_stock_hist
    logger.info(f'Buscando histórico de estoque na tabela fulfillment_stock_hist para o perído entre {date_from} e {date_to}')
    try:
        conn = psycopg2.connect(**db_config)

        sql_query = f"SELECT * FROM fulfillment_stock_hist WHERE created_at BETWEEN '{date_from}' AND '{date_to}'"
        df_stock = pd.read_sql(sql_query, conn)

    except psycopg2.Error as e:
        logger.error(f"Erro do psycopg2 ao consultar fulfillment_stock: {e}")

    except Exception as e:
        logger.error(f"Erro ao consultar fulfillment_stock: {e}")

    finally:
        if conn is not None:
            conn.close()
    
    # Ordenando stock por data
    logger.info('Ordenando stock por data')
    df_stock = df_stock.sort_values(by='created_at', ascending=False)
    df_stock['data'] = df_stock['created_at'].dt.date
    df_stock = df_stock.drop(['created_at'], axis=1)
    
    # Cria coluna has_stock, se available_quantity <= 0, has_stock= False 
    logger.info('Cria coluna has_stock, se available_quantity <= 0, has_stock= False')
    df_stock = df_stock.assign(has_stock=lambda x: x["available_quantity"] > 0)
    df_stock = df_stock.sort_values(by='data', ascending=False).reset_index(drop=True)
    df_stock = df_stock.drop_duplicates()
    
    # Contando dias em que produto esteve disponível
    logger.info('Contando dias em que produto esteve disponível')
    days_available = df_stock.groupby('ml_inventory_id')['has_stock'].sum().reset_index()
    days_available = days_available.rename(columns={'has_stock': 'days_available'})
    
    # Unindo DFs
    logger.info('Unindo dados de estoque')
    df_stock = df_stock.merge(days_available, on='ml_inventory_id', how='inner')
    
    # data de hoje
    # data_de_hoje = datetime.now().date() - timedelta(days=1)
    data_de_hoje = datetime.now().date()
    df_stock['data'] = pd.to_datetime(df_stock['data'])

    # Filtra apenas as linhas onde 'data' é igual à data de hoje
    logger.info('Buscando dados de hoje')
    df_stock_today = df_stock[df_stock['data'].dt.date == data_de_hoje]
    df_stock_today = df_stock_today.rename(columns={'available_quantity':'available_quantity_today'})
    # df_stock_today = df_stock.drop(['has_stock'], axis=1)
    
    
    # Buscando histórico de vendas na tabela ml_orders_hist para o período definido
    logger.info(f'Buscando histórico de vendas na tabela ml_orders_hist para o perído entre {date_from} e {date_to}')

    try:
        conn = psycopg2.connect(**db_config)
        
        # Construa a consulta SQL com a condição de data
        sql_query = f"SELECT * FROM ml_orders_hist WHERE date_closed BETWEEN '{date_from}' AND '{date_to}'"
        print(sql_query)
        # Execute a consulta e leia os dados em um DataFrame
        df_orders = pd.read_sql(sql_query, conn)

    except psycopg2.Error as e:
        logger.error(f"Erro do psycopg2 ao consultar ml_orders_hist: {e}")

    except Exception as e:
        logger.error(f"Erro ao consultar ml_orders_hist: {e}")

    finally:
        if conn is not None:
            conn.close()

    # filtros
    logger.info('Filtrando tabela de orders')
    df_orders = df_orders[df_orders['fulfilled'] == True]
    df_orders = df_orders[df_orders['order_status'] == 'paid']
    df_orders = df_orders[df_orders['payment_status'] == 'approved']
    df_orders = df_orders.drop(['pack_id','date_approved','fulfilled','order_status','payment_status'], axis=1)
    df_orders.rename(columns={'quantity': 'sales_quantity'}, inplace=True)

    # Ordenando orders por data
    logger.info('Ordenando orders por data')
    df_orders = df_orders.sort_values(by='date_closed', ascending=False)
    df_orders['data'] = df_orders['date_closed'].dt.date
    df_orders = df_orders.drop(['date_closed'], axis=1)
    df_orders = df_orders.drop_duplicates()

    # calcular total de vendas por ml_code e seller_sku no periodo
    logger.info(f'Calculando total de vendas no período entre {date_from} e {date_to}')
    total_sales_by_filter = df_orders.groupby(['ml_code','seller_sku'])['sales_quantity'].sum().reset_index()
    total_sales_by_filter.rename(columns={'sales_quantity': 'total_sales_quantity'}, inplace=True)
    
    # Acrescentando total de vendas ao DF
    logger.info('Acrescentando total de vendas ao dataframe')
    df_total_sales = pd.merge(df_orders, total_sales_by_filter, on=['ml_code','seller_sku'], how='inner')
    df_total_sales = df_total_sales.drop(['sales_quantity','shipping_id','data'], axis=1)
    df_total_sales = df_total_sales.drop_duplicates()

    # Buscando dados de produtos na tabela tiny_fulfillment
    logger.info('Buscando dados de produtos na tabela tiny_fulfillment')
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

    logger.info('Aplicando filtros no dataFrame de códigos')
    df_codes['ml_code'] = df_codes['ml_code'].apply(lambda x: 'MLB' + str(x))
    df_codes.rename(columns={'quantity': 'total_sales_quantity'}, inplace=True)
    df_codes = df_codes.drop(['mcenter_id', 'created_at', 'updated_at'],axis=1)

    logger.info('Unindo produtos ao estoque diário')
    prod_day = pd.merge(df_codes,df_stock_today,on='ml_inventory_id', how='inner')

    logger.info('Unindo total de vendas aos produtos com estoque do dia')
    df_sales = pd.merge(df_total_sales, prod_day, left_on=['ml_code','seller_sku'], right_on=['ml_code', 'ml_sku'], how='inner')
    
    cols = ['ml_code', 'ml_sku', 'ml_inventory_id', 'tiny_id', 'tiny_sku', 'var_code', 'variation_id', 'title', 'total_sales_quantity', 'qtd_item','days_available', 'available_quantity_today', 'data']
    df_sales = df_sales[cols]
    
    logger.info('Calculando métricas')
    
    # media de produtos disponiveis no período
    logger.info('Calculando média de produtos disponiveis no período')
    df_sales['media_prod_days_available'] = (df_sales['total_sales_quantity'] / df_sales['days_available'])
    df_sales['media_prod_days_available'] = df_sales['media_prod_days_available'].fillna(0)

    days = input_days

    # qtd de produtos a enviar no período, caso seja valor negativo produto está acima do esperado para envio(sobrando)
    logger.info(f'Calculando quantidade de produtos a serem enviados para suprir os próximos {input_days} dias')
    df_sales['period_send_fulfillment'] = np.ceil((df_sales['total_sales_quantity'] / df_sales['days_available'])* days - df_sales['available_quantity_today'])
    df_sales['period_send_fulfillment'] = df_sales['period_send_fulfillment'].fillna(0)

    # qtd de produtos a enviar hoje, caso seja valor negativo produto está acima do esperado para envio(sobrando)
    # logger.info('Calculando quantidade de produtos a serem enviados caso a necessidade seja hoje')
    # df_sales['today_send_fulfillment'] = np.ceil((df_sales['total_sales_quantity'] / df_sales['days_available']) - df_sales['available_quantity_today'])
    # df_sales['today_send_fulfillment'] = df_sales['today_send_fulfillment'].fillna(0)
    
    logger.info('Consulta finalizada. Dataframe pronto para exibição')
    st.dataframe(df_sales, use_container_width=True)