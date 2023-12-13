import json
import os
import time
from datetime import datetime

import numpy as np
import pandas as pd
import psycopg2
import requests
from dotenv import load_dotenv
from loguru import logger
from pandas import json_normalize
from psycopg2 import sql

load_dotenv()

ACCESS_TOKEN_BUENOSHOPS = os.getenv("ACCESS_TOKEN_BUENOSHOPS")
ACCESS_TOKEN_MUSICALCRIS = os.getenv("ACCESS_TOKEN_MUSICALCRIS")
ACCESS_TOKEN_MCENTER = os.getenv("ACCESS_TOKEN_MCENTER")

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


def get_fulfillment_stock(access_token, db_config, table_item, table_fulfillment):
    logger.add(
        f"Data/Output/Log/{table_fulfillment}.log",
        rotation="10 MB",
        format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
    )

    # Informações de conexão com o banco de dados PostgreSQL
    conn = psycopg2.connect(**db_config)

    sql_query = f"SELECT * FROM {table_item}"

    logger.info(sql_query)

    df_codes = pd.read_sql(sql_query, conn)

    # df_codes = df_codes.sample(10)

    # Verifica se há duplicatas
    duplicates = df_codes.duplicated()
    n_dup = duplicates.sum()

    logger.info(f"Número de duplicatas: {n_dup}")

    # Encontre as duplicatas no DataFrame
    duplicates = df_codes[df_codes.duplicated(keep=False)]

    # Remove as duplicatas e atualiza o DataFrame
    df_codes = df_codes.drop_duplicates()

    # ### Pegando Estoque de FulFillment

    # removendo valores nulos
    df_codes = df_codes.dropna(subset=["inventory_id"])
    # df_codes["inventory_id"].value_counts()

    # removendo valores nulos
    df_variations = df_codes.dropna(subset=["variation_id"])
    # df_variations["variation_id"].value_counts()

    var_codes = df_codes["variation_id"].unique()
    codes = df_codes["inventory_id"].unique()

    codes = np.delete(codes, np.where(codes == "NaN"))
    var_codes = np.delete(var_codes, np.where(var_codes == "NaN"))

    # unique_values = set(codes)
    # num_unique_values = len(unique_values)

    unique_values = set(codes)
    num_unique_values = len(unique_values)

    logger.info(f"Quantidade de valores únicos: {num_unique_values}")

    counter = 0
    json_list = []

    for item in codes:
        url = f"https://api.mercadolibre.com/inventories/{item}/stock/fulfillment"

        payload = {}
        headers = {"Authorization": f"Bearer {access_token}"}
        logger.info(f"Buscando dados {counter}/{len(codes)}: {item}")

        try:
            response = requests.get(url, headers=headers, data=payload)
            response.raise_for_status()  # Lança uma exceção HTTPError para códigos de status de erro
            response_data = response.json()
            json_list.append(response_data)
            counter += 1

            if counter % 50 == 0:
                logger.warning(f"Fazendo uma pausa de 1 minuto...")
                time.sleep(60)

        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao fazer a requisição para {url}: {e}")

        except Exception as e:
            logger.error(f"Erro não esperado: {e}")

    file_path = f"Data/Output/list_{table_fulfillment}.json"
    with open(file_path, "w") as file:
        json.dump(json_list, file)

    print(json_list)

    resultados = []

    for item in json_list:
        # Extrair os valores desejados
        inventory_id = item["inventory_id"]
        total = item["total"]
        available_quantity = item["available_quantity"]
        not_available_quantity = item["not_available_quantity"]
        not_available_detail = item.get(
            "not_available_detail", []
        )  # Evita KeyError se "not_available_detail" estiver ausente
        external_references = item.get("external_references", [])

        # Inicializa variáveis para armazenar detalhes
        not_available_detail_status = None
        not_available_detail_quantity = None
        external_references_type = None
        external_references_id = None
        external_references_variation_id = None

        # Verifica se há pelo menos um item em "not_available_detail"
        if not_available_detail:
            not_available_detail_status = not_available_detail[0].get("status")
            not_available_detail_quantity = not_available_detail[0].get("quantity")

        if external_references:
            external_references_type = external_references[0].get("type")
            external_references_id = external_references[0].get("id")
            external_references_variation_id = external_references[0].get(
                "variation_id"
            )

        # Adicionar os resultados à lista
        resultados.append(
            {
                "inventory_id": inventory_id,
                "total": total,
                "available_quantity": available_quantity,
                "not_available_quantity": not_available_quantity,
                # "not_available_detail": not_available_detail,
                # "external_references": external_references,
                "not_available_detail_status": not_available_detail_status,
                "not_available_detail_quantity": not_available_detail_quantity,
                "external_references_type": external_references_type,
                "external_references_id": external_references_id,
                "external_references_variation_id": external_references_variation_id,
            }
        )

    # Exibir os resultados
    df = pd.DataFrame(resultados)

    # df['external_references_variation_id'].astype(str).apply(lambda x:x.rstrip('.0'))
    df["external_references_variation_id"] = (
        df["external_references_variation_id"]
        .astype(str)
        .apply(lambda x: x.rstrip(".0"))
    )

    df["not_available_detail_quantity"] = pd.to_numeric(
        df["not_available_detail_quantity"], errors="coerce"
    ).astype("Int64")
    df["not_available_detail_status"] = df["not_available_detail_status"].astype(str)
    df["external_references_id"] = df["external_references_id"].astype(str)

    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    for index, row in df.iterrows():
        # Substituir valores NaN por None
        row = row.apply(lambda x: None if pd.isna(x) else x)

        current_time = datetime.now()
        query = f"""
                INSERT INTO {table_fulfillment} (ml_inventory_id, available_quantity, detail_status, detail_quantity, references_id, references_variation_id, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """

        insert_query = sql.SQL(query)

        logger.info(f"Inserindo dados em {table_fulfillment}: {[value for value in row]}")

        cursor.execute(
            insert_query,
            (
                row["inventory_id"],
                row["available_quantity"],
                row["not_available_detail_status"],
                row["not_available_detail_quantity"],
                row["external_references_id"],
                row["external_references_variation_id"],
                current_time,
            ),
        )

    conn.commit()

    cursor.close()
    conn.close()

    logger.info(f"Dados inseridos em {table_fulfillment} com sucesso!")


get_fulfillment_stock(ACCESS_TOKEN_BUENOSHOPS, db_config, "bueno_items", "bueno_ft")


get_fulfillment_stock(ACCESS_TOKEN_MUSICALCRIS, db_config, "cris_items", "cris_ft")


get_fulfillment_stock(ACCESS_TOKEN_MCENTER, db_config, "mcenter_items", "mcenter_ft")
