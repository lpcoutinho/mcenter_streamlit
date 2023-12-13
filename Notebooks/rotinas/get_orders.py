#!/usr/bin/env python
# coding: utf-8

# In[10]:


import datetime
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
from loguru import logger
from pandas import json_normalize
from psycopg2 import sql

load_dotenv()

ACCESS_TOKEN_MUSICALCRIS = os.getenv("ACCESS_TOKEN_MUSICALCRIS")
SELLER_ID_MUSICALCRIS = os.getenv("SELLER_ID_MUSICALCRIS")

ACCESS_TOKEN_BUENOSHOPS = os.getenv("ACCESS_TOKEN_BUENOSHOPS")
SELLER_ID_BUENOSHOPS = os.getenv("SELLER_ID_BUENOSHOPS")

ACCESS_TOKEN_MCENTER = os.getenv("ACCESS_TOKEN_MCENTER")
SELLER_ID_MCENTER = os.getenv("SELLER_ID_MCENTER")

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


# In[11]:


def write_file(json_data, nome_arquivo):
    """
    Escreve dados em um arquivo JSON, adicionando ao arquivo existente se ele já existir.

    Parâmetros:
    - json_data (list): Lista de dados em formato JSON a serem escritos no arquivo.
    - nome_arquivo (str): Nome do arquivo onde os dados serão escritos ou adicionados.

    Exemplo de uso:
    ```python
    json_list = [{'order_id': 1, 'product': 'Item 1'}, {'order_id': 2, 'product': 'Item 2'}]
    write_file(json_list, 'orders.json')
    ```

    Se o arquivo já existir, os dados fornecidos serão adicionados aos dados existentes.
    Se o arquivo não existir, um novo arquivo será criado e os dados serão escritos nele.
    """
    if os.path.exists(nome_arquivo):
        with open(nome_arquivo, "r") as arquivo_existente:
            dados_existente = json.load(arquivo_existente)

        dados_existente.extend(json_data)

        with open(nome_arquivo, "w") as arquivo:
            json.dump(dados_existente, arquivo)
    else:
        with open(nome_arquivo, "w") as arquivo:
            json.dump(json_data, arquivo)


# In[21]:


# Função para obter dados para um intervalo de datas específico
def get_orders_for_date_range(
    access_token, seller_id, date_from, date_to, table_orders, offset=0, limit=50
):
    logger.add(
        f"Data/Output/Log/{table_orders}.log",
        rotation="10 MB",
        format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
    )

    base_url = "https://api.mercadolibre.com/orders/search"

    params = {
        "seller": seller_id,
        "order.date_closed.from": f"{date_from}T00:00:00.000-03:00",
        "order.date_closed.to": f"{date_to}T00:00:00.000-03:00",
        "limit": limit,
        "offset": offset,
    }
    logger.info(params)

    headers = {"Authorization": f"Bearer {access_token}"}
    logger.info(headers)

    json_list = []
    counter = 0

    try:
        while True:
            response = requests.get(base_url, params=params, headers=headers)
            response.raise_for_status()
            data = response.json()

            if "results" in data:
                json_list.extend(data["results"])
            else:
                break

            if "paging" in data:
                total_paging = data["paging"].get("total")
                if total_paging is None:
                    break

                total_pages = math.ceil(total_paging / params["limit"])
                logger.info(f"Total esperado de páginas: {counter + 1}/{total_pages}")
                logger.info(f"Total de dados esperados: {total_paging}")
                logger.info(f'Offset atual: {params["offset"]}')
                logger.info(f"Intervalo de datas atual: {date_from} - {date_to}")

                if params["offset"] > total_paging:
                    break

                params["offset"] += params["limit"]
                counter += 1
            else:
                break

    except requests.exceptions.RequestException as req_err:
        logger.info(f"Erro ao fazer a requisição para {base_url}: {req_err}")

    except Exception as e:
        logger.info(f"Erro não esperado: {e}")

    logger.info(
        f"Total de dados coletados para {date_from} - {date_to}: {len(json_list)}"
    )
    # return json_list

    write_file(json_list, f"../../Data/Output/{table_orders}.json")

    resultados = []

    # coletando dados de orders
    for item in json_list:
        # Extrair os valores desejados
        payments = item.get("payments", [])
        status = item["status"]
        date_closed = item["date_closed"]
        pack_id = item["pack_id"]
        shipping = item["shipping"]
        order_items = item.get("order_items", [])
        fulfilled = item["fulfilled"]

        for payment in payments:
            order_id = payment["order_id"]
            reason = payment["reason"]
            payment_status = payment["status"]
            date_approved = payment["date_approved"]

        # Inicializa variável para armazenar shipping_id
        shipping_id = None
        shipping_id = shipping["id"]

        # # Inicializa listas para armazenar informações específicas de order_items

        # Itera sobre os dicionários em order_items
        for order_item in order_items:
            item_info = order_item.get("item", {})
            # item_id = item_info.get("id")
            ml_code = item_info["id"]
            title = item_info["title"]
            variation_id = item_info["variation_id"]
            seller_sku = item_info["seller_sku"]
            quantity = order_item["quantity"]
            category_id = item_info["category_id"]

            variation_attributes = item_info.get("variation_attributes", [])

            name = None
            value_id = None
            value_name = None
            id = None
            for attribute in variation_attributes:
                name = attribute["name"]
                id = attribute["id"]
                value_id = attribute["value_id"]
                value_name = attribute["value_name"]

        # Adicionar os resultados à lista
        resultados.append(
            {
                # "payments": payments,
                "ml_code": ml_code,
                "payment_status": payment_status,
                "order_status": status,
                "order_id": order_id,
                "shipping_id": shipping_id,
                "pack_id": pack_id,
                "title": title,
                "variation_id": variation_id,
                "category_id": category_id,
                "seller_sku": seller_sku,
                "quantity": quantity,
                "variation_name": name,
                "variation_attributes_id": id,
                "variation_value_id": value_id,
                "variation_value_name": value_name,
                "date_approved": date_approved,
                "date_closed": date_closed,
            }
        )

    # Exibir os resultados
    pd.set_option("display.max_colwidth", None)

    df = pd.DataFrame(resultados)

    # Tratando dados numéricos
    pd.set_option("display.float_format", "{:.0f}".format)
    df["shipping_id"] = df["shipping_id"].fillna(0)
    df["pack_id"] = df["pack_id"].fillna(0)
    df["variation_id"] = df["variation_id"].fillna(0)
    df["shipping_id"] = df["shipping_id"].astype("int64")
    df["pack_id"] = df["pack_id"].astype("int64")
    df["variation_id"] = df["variation_id"].astype("int64")

    # Adiciona 1h a mais para chegar ao horário do Brasil
    df["date_approved"] = pd.to_datetime(df["date_approved"])
    df["date_closed"] = pd.to_datetime(df["date_closed"])
    df["date_approved"] = df["date_approved"] + pd.to_timedelta("1 hour")
    df["date_approved"] = df["date_approved"].dt.tz_localize(None)
    df["date_closed"] = df["date_closed"] + pd.to_timedelta("1 hour")
    df["date_closed"] = df["date_closed"].dt.tz_localize(None)

    logger.info(df.shape)
    df = df.drop_duplicates()
    logger.info(df.shape)

    # coletando logistic type
    json_logistic_list = []
    success_count = 0

    total_iterations = len(df["shipping_id"])
    for index, shipping_id in enumerate(df["shipping_id"], start=1):
        url = f"https://api.mercadolibre.com/shipments/{shipping_id}"

        headers = {"Authorization": f"Bearer {access_token}"}

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()

            json_data = response.json()
            json_logistic_list.append(json_data)
            success_count += 1
            logger.info(
                f"Obtido com sucesso para shipping_id {shipping_id}: {json_data}"
            )
        except requests.exceptions.RequestException as e:
            logger.info(f"Erro ao obter dados para shipping_id {shipping_id}: {e}")

        # time.sleep(5)

        # Exibir o progresso
        progress_percentage = (index / total_iterations) * 100
        logger.info(
            f"Progresso: {index}/{total_iterations} ({progress_percentage:.2f}%)"
        )

    # Exibir os resultados
    logger.info(json_logistic_list)

    # Exibir estatísticas de conclusão
    logger.info(f"Número total de iterações: {total_iterations}")
    logger.info(f"Número de iterações bem-sucedidas: {success_count}")
    logger.info(f"Número de iterações falhadas: {total_iterations - success_count}")

    write_file(json_logistic_list, f"../../Data/Output/{table_orders}shipping.json")

    dfx = pd.DataFrame(json_logistic_list)
    # cols = ['id', 'order_id', 'logistic_type']
    cols = ["id", "logistic_type"]
    dfx = dfx[cols]
    # dfx['id'].value_counts()

    logger.info(dfx.shape)
    # dfx.head(3)

    # Criar um dicionário a partir de dfx para mapear 'order_id' para 'logistic_type'
    order_id_to_logistic_type = dfx.set_index("id")["logistic_type"].to_dict()

    # Adicionar a coluna 'logistic_type' a df usando o mapeamento
    df_result = df.copy()  # Criar uma cópia de df para manter o original intacto
    df_result["logistic_type"] = df_result["shipping_id"].map(order_id_to_logistic_type)

    logger.info(df_result.shape)
    df_result.head(3)

    df_result[["order_id", "shipping_id", "pack_id", "variation_id"]] = df_result[
        ["order_id", "shipping_id", "pack_id", "variation_id"]
    ].astype(str)
    df_result[["order_id", "shipping_id", "pack_id", "variation_id"]]

    df_result["variation_attributes_id"] = df_result["variation_attributes_id"].replace(
        "<built-in function id>", None, inplace=True
    )

    conn = psycopg2.connect(**db_config)

    cursor = conn.cursor()

    for index, row in df_result.iterrows():
        query = f"INSERT INTO {table_orders} (ml_code,category_id,variation_id,seller_sku,order_id,pack_id,quantity,title,order_status,payment_status,variation_name,variation_attributes_id,variation_value_id,variation_value_name,shipping_id,date_approved,date_closed,logistic_type) VALUES (%s, %s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s)"

        insert_query = sql.SQL(query)

        logger.info(f"Inserindo dados: {[value for value in row]}")

        print("Inserindo dados:", [value for value in row])

        cursor.execute(
            insert_query,
            (
                row["ml_code"],
                row["category_id"],
                row["variation_id"],
                row["seller_sku"],
                row["order_id"],
                row["pack_id"],
                row["quantity"],
                row["title"],
                row["order_status"],
                row["payment_status"],
                row["variation_name"],
                row["variation_attributes_id"],
                row["variation_value_id"],
                row["variation_value_name"],
                row["shipping_id"],
                row["date_approved"],
                row["date_closed"],
                row["logistic_type"],
            ),
        )

    conn.commit()

    # Feche o cursor e a conexão
    cursor.close()
    conn.close()
    logger.info("Dados inseridos com sucesso!")


# In[22]:


today = datetime.date.today()
today = today - datetime.timedelta(days=1)
tomorrow = today + datetime.timedelta(days=2)

today_str = today.strftime("%Y-%m-%d")
tomorrow_str = tomorrow.strftime("%Y-%m-%d")


# In[23]:


get_orders_for_date_range(
    ACCESS_TOKEN_MUSICALCRIS, SELLER_ID_MUSICALCRIS, today_str, tomorrow_str, "cris_ot"
)


# In[ ]:


# get_orders_for_date_range(ACCESS_TOKEN_BUENOSHOPS, SELLER_ID_BUENOSHOPS, today_str, tomorrow_str, 'cris_ot')


# In[9]:


get_orders_for_date_range(
    ACCESS_TOKEN_MCENTER, SELLER_ID_MCENTER, today_str, tomorrow_str, "mcenter_ot"
)
