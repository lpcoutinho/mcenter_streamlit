import json
import os
import time
from pandas import json_normalize
import psycopg2
from psycopg2 import sql
import math
import pandas as pd
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta
import numpy as np

load_dotenv()

ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
HOST = os.getenv("HOST")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")


# Informações de conexão com o banco de dados PostgreSQL
db_config = {
    "host":HOST,
    "database": POSTGRES_DB,
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
}

# Registra o tempo antes da execução
start_prog = time.time()


url = "https://api.mercadolibre.com/users/me"

payload = {}
headers = {
  'Authorization': f'Bearer {ACCESS_TOKEN}'
}

response = requests.request("GET", url, headers=headers, data=payload)
response = response.text

print(response)




# Selecionar data da pesquisa
date_from = '2023-11-21'
date_to = '2023-11-22'

# URL base da API
base_url = "https://api.mercadolibre.com/orders/search"

# Parâmetros iniciais
params = {
    'seller': '233632476',
    'order.date_created.from': f'{date_from}T00:00:00.000-00:00',
    'order.date_created.to': f'{date_to}T00:00:00.000-00:00',
    'limit': 50,
    'offset': 0
}

headers = {
    'Authorization': f'Bearer {ACCESS_TOKEN}'
}

json_list = []

counter = 0

# Paginando e coletando dados de orders
try:
    while True:
        response = requests.get(base_url, params=params, headers=headers)
        response.raise_for_status()  # Lança uma exceção se a resposta não for bem-sucedida
        data = response.json()

        if 'results' in data:
            json_list.extend(data['results'])
        else:
            break

        # Verifique se há mais páginas
        if 'paging' in data:
            total_paging = data['paging'].get('total')
            if total_paging is None:
                break

            total_pages = math.ceil(total_paging / params['limit'])
            print(f'Total esperado de páginas: {counter}/{total_pages}')
            print(f'Offset atual: {params["offset"]}')

            counter += 1
            if params['offset'] >= total_paging:
                break

            params['offset'] += params['limit']
        else:
            break

except requests.exceptions.RequestException as req_err:
    print(f"Erro ao fazer a requisição para {base_url}: {req_err}")

except Exception as e:
    print(f"Erro não esperado: {e}")

print(f'Total de dados coletados: {len(json_list)}')


# In[ ]:


print(len(json_list))
json_list[0]


# ### Coletando dados de pagamento
# 

# In[ ]:


meta_cols = ['date_closed', 'pack_id', 'shipping', 'order_items']

df_payments = json_normalize(json_list, record_path=['payments'], meta=meta_cols)


# In[ ]:


df_payments.columns


# In[ ]:


cols = ['date_approved','status','shipping']
df_payments = df_payments[cols]

col = {'status':'payment_status'}
df_payments.rename(columns=col, inplace= True)


# In[ ]:


df_payments.head(1)


# In[ ]:


# Removendo valores nulos
print(df_payments.shape)
df_payments = df_payments.dropna(subset=['date_approved'])
print(df_payments.shape)


# In[ ]:


# Extraindo shipping_id
df_payments['shipping_id'] = df_payments['shipping'].apply(lambda x: x['id'])
df_payments['shipping_id'] = df_payments['shipping_id'].astype(str).apply(lambda x: x.split('.')[0] if '.' in x else x)
df_payments = df_payments.drop('shipping', axis=1)
df_payments.head(1)


# In[ ]:


df_payments['shipping_id'].value_counts()


# In[ ]:


print(df_payments.shape)


# In[ ]:


df_payments = df_payments.drop_duplicates()
df_payments.sample()


# In[ ]:


# Encontrando os índices das linhas com a data mais recente para cada shipping_id
indices_recentes = df_payments.groupby('shipping_id')['date_approved'].idxmax()
indices_recentes


# In[ ]:


# Verificando se existe mais de um envio
df_payments['shipping_id'].value_counts()[df_payments['shipping_id'].value_counts() > 1]


# In[ ]:


# Criando um novo DataFrame com base nos índices de envio encontrados
df_payments = df_payments.loc[indices_recentes]
df_payments['shipping_id'].value_counts()[df_payments['shipping_id'].value_counts() > 1]


# ### Coletando dados de orders

# In[ ]:


df_orders = json_normalize(json_list, record_path=['order_items'], meta=['date_closed', 'pack_id', 'status', 'shipping'])

## pd.set_option('display.max_columns', None)
df_orders.columns


# In[ ]:


cols = ['quantity', 'item.id', 'item.title', 'item.category_id', 'item.variation_id',
       'item.seller_sku', 'date_closed', 'pack_id', 'status',
       'shipping']

df_orders = df_orders[cols]
df_orders.sample()


# In[ ]:


# Extraindo shipping_id
df_orders['shipping_id'] = df_orders['shipping'].apply(lambda x: x['id'])
df_orders['shipping_id'] = df_orders['shipping_id'].astype(str).apply(lambda x: x.split('.')[0] if '.' in x else x)
df_orders = df_orders.drop('shipping', axis=1)
df_orders.sample()


# In[ ]:


valores_unicos = df_orders['shipping_id'].value_counts()
valores_unicos


# ### Unindo DFs de pagamentos e vendas

# In[ ]:


df_resultado = pd.merge(df_orders, df_payments, on='shipping_id', how='left')
df_resultado.sample()


# In[ ]:


df_resultado.shape


# In[ ]:


# Pegando valores unicos para consula futura
uniq_shipping_id = df_resultado['shipping_id'].unique()
print(len(uniq_shipping_id))
print(df_orders.shape)
print(type(uniq_shipping_id))


# ### Consultando envios

# In[ ]:


json_shipments_list = []
success_count = 0
error_count = 0
counter = 0

for shipping_id in uniq_shipping_id:
    url = f"https://api.mercadolibre.com/shipments/{shipping_id}"

    payload = {}
    headers = {
      'Authorization': f'Bearer {ACCESS_TOKEN}'
    }

    try:
        print(f'Loop nº {counter}/{len(uniq_shipping_id)}: shipping_id = {shipping_id}')
        response = requests.request("GET", url, headers=headers, data=payload)
        response.raise_for_status()  # Verifica se a resposta tem um status de sucesso (código 2xx)

        json_shipments_list.append(response.json())
        success_count += 1
    except requests.exceptions.RequestException as e:
        # Lidar com erros de solicitação (por exemplo, conexão perdida, timeout)
        error_count += 1
        logger.error(f"Erro na solicitação para shipping_id {shipping_id}: {str(e)}")
    counter +=1

logger.info(f"Solicitações bem-sucedidas: {success_count}")
logger.info(f"Erros de solicitação: {error_count}")

print(len(json_shipments_list))
print(type(json_shipments_list))


# In[ ]:


df = pd.DataFrame(json_shipments_list)
df.sample()


# In[ ]:


df_orders = df_resultado.copy()
df_orders.sample()


# In[ ]:


df_fulfillment_ship = df[['id', 'logistic_type']]
df_fulfillment_ship = df_fulfillment_ship[df_fulfillment_ship['logistic_type'] == 'fulfillment']
df_fulfillment_ship.sample()


# In[ ]:


print(df_orders.shape)
print(df_fulfillment_ship.shape)
print(df_fulfillment_ship['logistic_type'].value_counts())


# In[ ]:


# Extraindo shipping_id
df_fulfillment_ship['shipping_id'] = df_fulfillment_ship['id'].astype(str)
df_fulfillment_ship = df_fulfillment_ship.drop(['id'], axis=1)
df_fulfillment_ship.sample()


# In[ ]:


# Unindo os DFs. Retorna apenas as linhas em df_orders onde shipping_id == id
df_res_fulfillment = pd.merge(df_orders, df_fulfillment_ship, left_on='shipping_id', right_on='shipping_id', how='inner')

df_res_fulfillment.shape


# In[ ]:


cols = ['quantity','item.id','item.seller_sku','date_closed', 'date_approved', 'pack_id', 'status', 'shipping_id', 'logistic_type']
df_res_fulfillment = df_res_fulfillment[cols]
df_res_fulfillment


# ### Buscando dados de produtos no BD

# In[ ]:


# Buscando dados de produtos no BD
try:
    conn = psycopg2.connect(**db_config)

    sql_query = "SELECT * FROM tiny_fulfillment"
    df_codes = pd.read_sql(sql_query, conn)
except psycopg2.Error as e:
    logger.error(f"Erro do psycopg2 ao consultar fulfillment_stock: {e}")
    
except Exception as e:
    logger.error(f"Erro ao consultar tabela tiny_fulfillment: {e}")

finally:
    if conn is not None:
        conn.close()


# In[ ]:


df_codes.sample()


# In[ ]:


df_codes['ml_code'] = df_codes['ml_code'].apply(lambda x: 'MLB' + str(x))
df_codes.sample()


# In[ ]:


df_res_fulfillment.sample()


# ### DF de produtos e orders

# In[ ]:


df_filtered = pd.merge(df_res_fulfillment, df_codes, left_on=['item.id', 'item.seller_sku'], right_on=['ml_code', 'ml_sku'], how='left')
df_filtered.sample()


# ### Calculando quantidade de produtos vendidos

# In[ ]:


# Soma de produtos vendidos
soma_por_ml_inventory_id = df_filtered.groupby('ml_inventory_id')['quantity'].sum().reset_index()
soma_por_ml_inventory_id.head(3)


# In[ ]:


# Buscando histórico de estoque na tabela
try:
    conn = psycopg2.connect(**db_config)

    sql_query = "SELECT * FROM fulfillment_stock_hist"
    df_stock = pd.read_sql(sql_query, conn)

except psycopg2.Error as e:
    print(f"Erro do psycopg2 ao consultar fulfillment_stock: {e}")

except Exception as e:
    print(f"Erro ao consultar fulfillment_stock: {e}")

finally:
    if conn is not None:
        conn.close()


# In[ ]:


# Ordenando por data
df_stock = df_stock.sort_values(by='created_at', ascending=True)
df_stock['data'] = df_stock['created_at'].dt.date
df_stock.sample()


# In[ ]:


# Filtrando dados por datas
df_orders_f = df_filtered.copy()
df_orders_f['date_approved'] = pd.to_datetime(df_orders_f['date_approved'])
df_orders_f['data'] = df_orders_f['date_approved'].dt.date
df_orders_f = df_orders_f.drop(['date_closed', 'created_at', 'updated_at'], axis=1)
df_orders_f.sample()


# In[ ]:


# Filtrando DFs por Período
def filtrar_por_periodo(df, data_inicio, data_fim):
    return df[(df['data'] >= data_inicio) & (df['data'] <= data_fim)]

data_inicio = datetime(2023, 11, 10).date()  
data_fim = datetime(2023, 11, 22).date()     

# Filtrar DataFrames com base nas datas definidas
df_stock_filtrado = filtrar_por_periodo(df_stock, data_inicio, data_fim)
df_orders_filtrado = filtrar_por_periodo(df_orders_f, data_inicio, data_fim)


# In[ ]:


df_orders_filtrado.sample()


# In[ ]:


df_stock_filtrado.sample()


# In[ ]:


## Cria coluna has_stock, se available_quantity <= 0, has_stock= False ##
df_stock_filtrado = df_stock_filtrado.assign(has_stock=lambda x: x["available_quantity"] > 0)
df_stock_filtrado = df_stock_filtrado.sort_values(by='data', ascending=False).reset_index(drop=True)

df_stock_filtrado.head()


# In[ ]:


## Contando dias em que produto esteve disponível 
days_available = df_stock_filtrado.groupby('ml_inventory_id')['has_stock'].sum().reset_index()
days_available = days_available.rename(columns={'has_stock': 'days_available'})

days_available.sample(5)


# In[ ]:


# Unindo DFs
df_stock_filtrado = df_stock_filtrado.merge(days_available, on='ml_inventory_id', how='left')
df_stock_filtrado


# In[ ]:


# Total de vendas por ml_inventory_id
total_sales_by_id = df_orders_filtrado.groupby('ml_inventory_id')['quantity'].sum().reset_index()
total_sales_by_id.sample(5)


# In[ ]:


# Acrescentando total de vendas ao DF
df_orders_filtrado = pd.merge(df_orders_filtrado, total_sales_by_id, on='ml_inventory_id', how='left')

df_orders_filtrado.rename(columns={'quantity_x': 'sales_quantity'}, inplace=True)
df_orders_filtrado.rename(columns={'quantity_y': 'total_sales_quantity'}, inplace=True)
df_orders_filtrado['sales_quantity'] = df_orders_filtrado['sales_quantity'].fillna(0)
df_orders_filtrado['total_sales_quantity'] = df_orders_filtrado['total_sales_quantity'].fillna(0)

df_orders_filtrado.sample(5)


# In[ ]:


df_stock_filtrado_ = df_stock_filtrado.copy()

indices_recentes = df_stock_filtrado_.groupby('ml_inventory_id')['data'].idxmax()
indices_recentes


# In[ ]:


df_stock_filtrado_ = df_stock_filtrado_.loc[indices_recentes]
df_stock_filtrado_ = df_stock_filtrado_.drop(['created_at', 'has_stock'], axis=1)

df_stock_filtrado_.sample(3)


# In[ ]:


df_orders_filtrado_ = df_orders_filtrado[['ml_inventory_id','item.id', 'ml_code', 'item.seller_sku' ,'ml_sku', 'date_approved', 'sales_quantity', 'total_sales_quantity']]
print(df_orders_filtrado_.shape)
df_orders_filtrado_ = df_orders_filtrado_.drop_duplicates()
print(df_orders_filtrado_.shape)
df_orders_filtrado_.sample(3)


# In[ ]:


_df_orders_filtrado_ = df_orders_filtrado_.copy()

indices_recentes = _df_orders_filtrado_.groupby('ml_inventory_id')['date_approved'].idxmax()
indices_recentes

_df_orders_filtrado_ = _df_orders_filtrado_.loc[indices_recentes]
_df_orders_filtrado_ = _df_orders_filtrado_.drop(['item.id','item.seller_sku','date_approved','sales_quantity'], axis=1)

_df_orders_filtrado_.sample()


# In[ ]:


print(df_orders_filtrado_.shape)
print(_df_orders_filtrado_.shape)


# In[ ]:


df_stock_filtrado_.sample()


# In[ ]:


df_unido = pd.merge(df_stock_filtrado_, _df_orders_filtrado_, on='ml_inventory_id', how='left')
df_unido['total_sales_quantity'] = df_unido['total_sales_quantity'].fillna(0)
df_unido.shape


# In[ ]:


# Criando um novo DataFrame onde 'ml_code' é NaN
df_nan_values = df_unido[pd.isnull(df_unido['ml_code'])]

df_ful = pd.merge(df_nan_values, df_codes, on='ml_inventory_id', how='left')
df_ful = df_ful.drop(['available_quantity', 'data', 'days_available', 'ml_code_x', 'ml_sku_x', 'mcenter_id', 'var_code', 'ad_title', 'created_at', 'updated_at', 'tiny_id', 'tiny_sku'], axis=1)
df_ful.rename(columns={'ml_code_y': 'ml_code', 'ml_sku_y':'ml_sku'}, inplace=True)

df_ful.sample()


# In[ ]:


df_ful = pd.merge(df_unido, df_ful, on='ml_inventory_id', how='left')
df_ful = df_ful.drop(['data', 'ml_code_x', 'ml_sku_x', 'total_sales_quantity_y'], axis=1)
df_ful.rename(columns={'ml_code_y': 'ml_code', 'ml_sku_y':'ml_sku'}, inplace=True)

df_ful = df_ful.drop_duplicates()
df_ful.shape


# In[ ]:


df_ful = pd.merge(df_codes, df_ful, on='ml_inventory_id', how='left')
df_ful = df_ful.drop(['mcenter_id', 'var_code','created_at', 'updated_at', 'ad_title', 'ml_code_y', 'ml_sku_y', 'tiny_sku'], axis=1)
df_ful = df_ful.drop_duplicates()
df_ful.rename(columns={'ml_code_x': 'ml_code', 'ml_sku_x':'ml_sku', 'total_sales_quantity_x':'total_sales_quantity'}, inplace=True)

df_ful.shape


# In[ ]:


df_ful.sample(5)


# In[ ]:


df_unido = df_ful.copy()
df_unido.sample()


# In[ ]:


df_unido['total_sales_quantity'] = df_unido['total_sales_quantity'].fillna(0)
df_unido['days_available'] = df_unido['days_available'].fillna(0)
df_unido.shape


# In[ ]:


df_unido['media_prod_days_available'] = (df_unido['total_sales_quantity'] / df_unido['days_available'])
df_unido['media_prod_days_available'] = df_unido['media_prod_days_available'].fillna(0)


# In[ ]:


df_unido['media_prod_days_available'] = (df_unido['total_sales_quantity'] / df_unido['days_available'])
df_unido['media_prod_days_available'] = df_unido['media_prod_days_available'].fillna(0)

days = 30


df_unido['period_send_fulfillment'] = np.ceil((df_unido['total_sales_quantity'] / df_unido['days_available'])* days - df_unido['available_quantity'])
df_unido['period_send_fulfillment'] = df_unido['30_send_fulfillment'].fillna(0)


# In[ ]:


df_unido['today_send_fulfillment'] = np.ceil((df_unido['total_sales_quantity'] / df_unido['days_available']) - df_unido['available_quantity'])
df_unido['today_send_fulfillment'] = df_unido['total_send_fulfillment'].fillna(0)


# In[ ]:


df_fulfillment_maior_que_zero = df_unido[df_unido['30_send_fulfillment'] > 0]

df_fulfillment_maior_que_zero


# In[ ]:


df_fulfillment_less_one = df_unido[df_unido['30_send_fulfillment'] <= 0]
df_fulfillment_less_one.sample(5)


# ### Comparando com a Tiny

# In[ ]:


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


# In[ ]:


# Obtém a data de hoje
data_de_hoje = pd.to_datetime('today').date()

# Filtra apenas as linhas onde 'created_at' é igual à data de hoje
df_hoje = df_tiny_stock[df_tiny_stock['created_at'].dt.date == data_de_hoje]
df_hoje.shape


# In[ ]:


df_resultado = df_hoje[df_hoje['tiny_id'].isin(df_fulfillment_maior_que_zero['tiny_id'])]
print(df_resultado.shape)
print(df_fulfillment_maior_que_zero.shape)


# In[ ]:


valores_unicos = df_fulfillment_maior_que_zero['tiny_id'].unique()

# Filtra as linhas em df_hoje onde 'tiny_id' está presente na lista de valores únicos
df_resultado = df_hoje[df_hoje['tiny_id'].isin(valores_unicos)]
print(df_resultado.shape)
print(df_fulfillment_maior_que_zero.shape)


# In[ ]:


df_resultado


# In[ ]:


id = '759523162' 
x = df_resultado[df_resultado['tiny_id'] == id]
x


# In[ ]:


df_fulfillment_maior_que_zero['tiny_id'].value_counts()


# In[ ]:


df_resultado = pd.merge(df_fulfillment_maior_que_zero, df_resultado, on='tiny_id', how='left')
df_resultado.shape


# In[ ]:


# Obtém a data de hoje
data_de_hoje = pd.to_datetime('today').date()

# Filtra apenas as linhas onde 'created_at' é igual à data de hoje
df_hoje = df_tiny_stock[df_tiny_stock['created_at'].dt.date == data_de_hoje]

df_resultado = df_hoje[df_hoje['tiny_id'].isin(df_fulfillment_maior_que_zero['tiny_id'])]

df_resultado = pd.merge(df_fulfillment_greater_zero, df_resultado, on='tiny_id', how='left')


# In[1]:


# import json
# import os
# import time
# from pandas import json_normalize
# import psycopg2
# from psycopg2 import sql
# import math
# import pandas as pd
# import requests
# from dotenv import load_dotenv
# from datetime import datetime, timedelta
# import numpy as np
# from loguru import logger


# logger.add(
#     "Data/Output/Log/send_fulfillment.log",
#     rotation="10 MB",
#     format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
# )

# load_dotenv()

# ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
# HOST = os.getenv("HOST")
# POSTGRES_DB = os.getenv("POSTGRES_DB")
# POSTGRES_USER = os.getenv("POSTGRES_USER")
# POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")


# # Informações de conexão com o banco de dados PostgreSQL
# db_config = {
#     "host":HOST,
#     "database": POSTGRES_DB,
#     "user": POSTGRES_USER,
#     "password": POSTGRES_PASSWORD,
# }

# # Registra o tempo antes da execução
# start_prog = time.time()

# Selecionar data da pesquisa
date_from = '2023-11-21'
date_to = '2023-11-22'

# URL base da API
base_url = "https://api.mercadolibre.com/orders/search"

# Parâmetros iniciais
params = {
    'seller': '233632476',
    # 'order.status': 'paid',
    'order.date_created.from': f'{date_from}T00:00:00.000-00:00',
    'order.date_created.to': f'{date_to}T00:00:00.000-00:00',
    'limit': 50,
    'offset': 0
}

headers = {
    'Authorization': f'Bearer {ACCESS_TOKEN}'
}

json_list = []

counter = 0

# Pagianndo e coletando dados de orders
while True:
    response = requests.get(base_url, params=params, headers=headers)
    data = response.json()

    
    if 'results' in data:
        json_list.extend(data['results'])
    else:
        break
    
    # Verifique se há mais páginas
    if 'paging' in data:
        total_paging = data['paging'].get('total')
        if total_paging is None:
            break

        total_pages = math.ceil(total_paging / params['limit'])
        logger.info(f'Total esperado de páginas: {counter}/{total_pages}')
        logger.info(f'Offset atual: {params["offset"]}')

        counter += 1
        if params['offset'] >= total_paging:
            break

        params['offset'] += params['limit']
    else:
        break

logger.info(f'Total de dados coletados: {len(json_list)}')

# Coletando dados de pagamentos
meta_cols = ['date_closed', 'pack_id', 'shipping', 'order_items']
df_payments = json_normalize(json_list, record_path=['payments'], meta=meta_cols)

cols = ['date_approved','status','shipping']
df_payments = df_payments[cols]

col = {'status':'payment_status'}
df_payments.rename(columns=col, inplace= True)

# Removendo valores nulos
df_payments = df_payments.dropna(subset=['date_approved'])

# Extraindo shipping_id
df_payments['shipping_id'] = df_payments['shipping'].apply(lambda x: x['id'])
df_payments['shipping_id'] = df_payments['shipping_id'].astype(str).apply(lambda x: x.split('.')[0] if '.' in x else x)
df_payments = df_payments.drop('shipping', axis=1)

# Remove duplicatas
df_payments = df_payments.drop_duplicates()

# Encontrando os índices de envio mais recentes
indices_recentes = df_payments.groupby('shipping_id')['date_approved'].idxmax()

# Criando um novo DataFrame com base nos índices de envio encontrados
df_payments = df_payments.loc[indices_recentes]

# Coletando dados de orders
df_orders = json_normalize(json_list, record_path=['order_items'], meta=['date_closed', 'pack_id', 'status', 'shipping'])

cols = ['quantity', 'item.id', 'item.title', 'item.category_id', 'item.variation_id',
       'item.seller_sku', 'date_closed', 'pack_id', 'status',
       'shipping']
df_orders = df_orders[cols]

# Extraindo shipping_id
df_orders['shipping_id'] = df_orders['shipping'].apply(lambda x: x['id'])
df_orders['shipping_id'] = df_orders['shipping_id'].astype(str).apply(lambda x: x.split('.')[0] if '.' in x else x)
df_orders = df_orders.drop('shipping', axis=1)

# Unindo DFs de orders e payments
df_resultado = pd.merge(df_orders, df_payments, on='shipping_id', how='left')

# Pegando valores unicos para consula futura
uniq_shipping_id = df_resultado['shipping_id'].unique()

# Coletando dados de envio, para pegar apenas o que for fulfillment
json_shipments_list = []
success_count = 0
error_count = 0
counter = 0

for shipping_id in uniq_shipping_id:
    url = f"https://api.mercadolibre.com/shipments/{shipping_id}"

    payload = {}
    headers = {
      'Authorization': f'Bearer {ACCESS_TOKEN}'
    }

    try:
        print(f'Loop nº {counter}/{len(uniq_shipping_id)}: shipping_id = {shipping_id}')
        response = requests.request("GET", url, headers=headers, data=payload)
        response.raise_for_status() 

        json_shipments_list.append(response.json())
        success_count += 1
    except requests.exceptions.RequestException as e:
        error_count += 1
        logger.error(f"Erro na solicitação para shipping_id {shipping_id}: {str(e)}")
    counter +=1

logger.info(f"Solicitações bem-sucedidas: {success_count}")
logger.info(f"Erros de solicitação: {error_count}")

df = pd.DataFrame(json_shipments_list)

df_orders = df_resultado.copy()

df_fulfillment_ship = df[['id', 'logistic_type']]
df_fulfillment_ship = df_fulfillment_ship[df_fulfillment_ship['logistic_type'] == 'fulfillment']

# Extraindo shipping_id
df_fulfillment_ship['shipping_id'] = df_fulfillment_ship['id'].astype(str)
df_fulfillment_ship = df_fulfillment_ship.drop(['id'], axis=1)


# Unindo os DFs. Retorna apenas as linhas em df_orders onde shipping_id == id
df_res_fulfillment = pd.merge(df_orders, df_fulfillment_ship, left_on='shipping_id', right_on='shipping_id', how='inner')

cols = ['quantity','item.id','item.seller_sku','date_closed', 'date_approved', 'pack_id', 'status', 'shipping_id', 'logistic_type']
df_res_fulfillment = df_res_fulfillment[cols]

# Buscando dados de produtos na tabela tiny_fulfillment
try:
    conn = psycopg2.connect(**db_config)

    sql_query = "SELECT * FROM tiny_fulfillment"
    df_codes = pd.read_sql(sql_query, conn)
except psycopg2.Error as e:
    logger.error(f"Erro do psycopg2 ao consultar fulfillment_stock: {e}")
    
except Exception as e:
    logger.error(f"Erro ao consultar tabela tiny_fulfillment: {e}")

finally:
    if conn is not None:
        conn.close()


# Buscando histórico de estoque na tabela
try:
    conn = psycopg2.connect(**db_config)

    sql_query = "SELECT * FROM fulfillment_stock_hist"
    df_stock = pd.read_sql(sql_query, conn)

except psycopg2.Error as e:
    print(f"Erro do psycopg2 ao consultar fulfillment_stock: {e}")

except Exception as e:
    print(f"Erro ao consultar fulfillment_stock: {e}")

finally:
    if conn is not None:
        conn.close()

# Acrescentando prefixo MLB em ml_code
df_codes['ml_code'] = df_codes['ml_code'].apply(lambda x: 'MLB' + str(x))

# Dfs de orders e produtos unidos por ml_code e ml_sku
df_filtered = pd.merge(df_res_fulfillment, df_codes, left_on=['item.id', 'item.seller_sku'], right_on=['ml_code', 'ml_sku'], how='left')

# Soma de produtos vendidos
soma_por_ml_inventory_id = df_filtered.groupby('ml_inventory_id')['quantity'].sum().reset_index()

# Ordenando por data
df_stock = df_stock.sort_values(by='created_at', ascending=True)
df_stock['data'] = df_stock['created_at'].dt.date

# Criando campo de datas
df_orders_f = df_filtered.copy()
df_orders_f['date_approved'] = pd.to_datetime(df_orders_f['date_approved'])
df_orders_f['data'] = df_orders_f['date_approved'].dt.date
df_orders_f = df_orders_f.drop(['date_closed', 'created_at', 'updated_at'], axis=1)

# Filtrando DFs por Período
def filtrar_por_periodo(df, data_inicio, data_fim):
    return df[(df['data'] >= data_inicio) & (df['data'] <= data_fim)]

data_inicio = datetime(2023, 11, 10).date()  
data_fim = datetime(2023, 11, 22).date()     

# Filtrar DataFrames com base nas datas definidas
df_stock_filtrado = filtrar_por_periodo(df_stock, data_inicio, data_fim)
df_orders_filtrado = filtrar_por_periodo(df_orders_f, data_inicio, data_fim)

# Cria coluna has_stock, se available_quantity <= 0, has_stock= False
df_stock_filtrado = df_stock_filtrado.assign(has_stock=lambda x: x["available_quantity"] > 0)
df_stock_filtrado = df_stock_filtrado.sort_values(by='data', ascending=False).reset_index(drop=True)

# Contando dias em que produto esteve disponível 
days_available = df_stock_filtrado.groupby('ml_inventory_id')['has_stock'].sum().reset_index()
days_available = days_available.rename(columns={'has_stock': 'days_available'})

# Unindo DFs
df_stock_filtrado = df_stock_filtrado.merge(days_available, on='ml_inventory_id', how='left')

# Total de vendas por ml_inventory_id
total_sales_by_id = df_orders_filtrado.groupby('ml_inventory_id')['quantity'].sum().reset_index()

# Acrescentando total de vendas ao DF
df_orders_filtrado = pd.merge(df_orders_filtrado, total_sales_by_id, on='ml_inventory_id', how='left')

df_orders_filtrado.rename(columns={'quantity_x': 'sales_quantity'}, inplace=True)
df_orders_filtrado.rename(columns={'quantity_y': 'total_sales_quantity'}, inplace=True)
df_orders_filtrado['sales_quantity'] = df_orders_filtrado['sales_quantity'].fillna(0)
df_orders_filtrado['total_sales_quantity'] = df_orders_filtrado['total_sales_quantity'].fillna(0)

# Novos filtros
df_stock_filtrado_ = df_stock_filtrado.copy()

indices_recentes = df_stock_filtrado_.groupby('ml_inventory_id')['data'].idxmax()
df_stock_filtrado_ = df_stock_filtrado_.loc[indices_recentes]
df_stock_filtrado_ = df_stock_filtrado_.drop(['created_at', 'has_stock'], axis=1)

df_orders_filtrado_ = df_orders_filtrado[['ml_inventory_id','item.id', 'ml_code', 'item.seller_sku' ,'ml_sku', 'date_approved', 'sales_quantity', 'total_sales_quantity']]
df_orders_filtrado_ = df_orders_filtrado_.drop_duplicates()

_df_orders_filtrado_ = df_orders_filtrado_.copy()

indices_recentes = _df_orders_filtrado_.groupby('ml_inventory_id')['date_approved'].idxmax()

_df_orders_filtrado_ = _df_orders_filtrado_.loc[indices_recentes]
_df_orders_filtrado_ = _df_orders_filtrado_.drop(['item.id','item.seller_sku','date_approved','sales_quantity'], axis=1)

df_unido = pd.merge(df_stock_filtrado_, _df_orders_filtrado_, on='ml_inventory_id', how='left')
df_unido['total_sales_quantity'] = df_unido['total_sales_quantity'].fillna(0)

# Criando um novo DataFrame onde 'ml_code' é NaN
df_nan_values = df_unido[pd.isnull(df_unido['ml_code'])]

# Unindo e filtrando DFs
df_ful = pd.merge(df_nan_values, df_codes, on='ml_inventory_id', how='left')
df_ful = df_ful.drop(['available_quantity', 'data', 'days_available', 'ml_code_x', 'ml_sku_x', 'mcenter_id', 'var_code', 'ad_title', 'created_at', 'updated_at', 'tiny_id', 'tiny_sku'], axis=1)
df_ful.rename(columns={'ml_code_y': 'ml_code', 'ml_sku_y':'ml_sku'}, inplace=True)

df_ful = pd.merge(df_unido, df_ful, on='ml_inventory_id', how='left')
df_ful = df_ful.drop(['data', 'ml_code_x', 'ml_sku_x', 'total_sales_quantity_y'], axis=1)
df_ful.rename(columns={'ml_code_y': 'ml_code', 'ml_sku_y':'ml_sku'}, inplace=True)

df_ful = df_ful.drop_duplicates()

df_ful = pd.merge(df_codes, df_ful, on='ml_inventory_id', how='left')
df_ful = df_ful.drop(['mcenter_id', 'var_code','created_at', 'updated_at', 'ad_title', 'ml_code_y', 'ml_sku_y', 'tiny_sku'], axis=1)
df_ful = df_ful.drop_duplicates()
df_ful.rename(columns={'ml_code_x': 'ml_code', 'ml_sku_x':'ml_sku', 'total_sales_quantity_x':'total_sales_quantity'}, inplace=True)

df_unido = df_ful.copy()

# Calculando total de produtos
df_unido['total_sales_quantity'] = df_unido['total_sales_quantity'].fillna(0)
df_unido['days_available'] = df_unido['days_available'].fillna(0)

# Calculando media de produtos por dias disponiveis
df_unido['media_prod_days_available'] = (df_unido['total_sales_quantity'] / df_unido['days_available'])
df_unido['media_prod_days_available'] = df_unido['media_prod_days_available'].fillna(0)

# Calculando produtos a enviar hoje
df_unido['today_send_fulfillment'] = np.ceil((df_unido['total_sales_quantity'] / df_unido['days_available']) - df_unido['available_quantity'])
df_unido['today_send_fulfillment'] = df_unido['total_send_fulfillment'].fillna(0)

days = 30
# Calculando produtos a enviar no período de x dias
df_unido['period_send_fulfillment'] = np.ceil((df_unido['total_sales_quantity'] / df_unido['days_available'])* days - df_unido['available_quantity'])
df_unido['period_send_fulfillment'] = df_unido['30_send_fulfillment'].fillna(0)


# Filtrando DFs onde o produto a ser enviado é maior ou menor ou igual a 0
df_fulfillment_greater_zero = df_unido[df_unido['30_send_fulfillment'] > 0]
df_fulfillment_less_one = df_unido[df_unido['30_send_fulfillment'] <= 0]


# In[ ]:




