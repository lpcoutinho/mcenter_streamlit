import json
import math
import os
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import psycopg2
import requests
import streamlit
from dotenv import load_dotenv
from loguru import logger
from psycopg2 import sql


def import_data_to_db():
    logger.add(
        "Data/Output/Log/ml_log.log",
        rotation="10 MB",
        format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
    )

    start_prog = time.time()  # Registra o inicio da aplicação

    load_dotenv()

    ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
    HOST = os.getenv("HOST")
    POSTGRES_DB = os.getenv("POSTGRES_DB")
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

    db_config = {
        "host": HOST,
        "database": POSTGRES_DB,
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
    }

    # Consulta aos itens com logistic_type=fulfillment
    base_url = "https://api.mercadolibre.com/users/233632476/items/search?logistic_type=fulfillment"

    params = {
        "limit": 100,
        "offset": 0,
    }

    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}

    # buscando lista de códigos
    json_list = []
    try:
        while True:
            response = requests.get(base_url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            logger.info(data)
            if "results" in data:
                json_list.extend(data["results"])
                logger.info(data["results"])
            else:
                break

            # Verifique se há mais páginas
            if "paging" in data:
                total_data = data["paging"].get("total")

                total_pages = math.ceil(total_data / params["limit"])
                logger.info(f"Total de páginas a serem processadas: {total_pages}")
                logger.info(f'Offset atual: {params["offset"]}')

                if params["offset"] >= total_pages * params["limit"]:
                    break

                params["offset"] += params["limit"]
            else:
                break

    except requests.exceptions.RequestException as req_err:
        logger.error(f"Erro ao fazer a requisição para {base_url}: {req_err}")
    except Exception as e:
        logger.error(f"Erro não esperado: {e}")

    logger.info(f"Total esperado de dados: {total_data}")
    logger.info(f"Total de dados coletados: {len(json_list)}")

    # buscando de itens em json
    json_list_item = []
    c = 1
    for item in json_list:
        base_url = f"https://api.mercadolibre.com/items/{item}"
        headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
        t = len(json_list)
        logger.info(item)
        logger.info(f"{c}/{t}")
        c += 1

        try:
            response = requests.get(base_url, headers=headers)
            response.raise_for_status()
            data = response.json()
            json_list_item.append(data)
            logger.info(f"Tamanho da nova lista: {len(json_list_item)}/{t}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao obter dados para o item {item}: {e}")

        # Se c for um múltiplo de 50, aguarde 1 minuto
        if c % 50 == 0:
            logger.error("Esperando 1 minuto...")
            time.sleep(60)

    logger.info(f"Tamanho da lista de itens: {len(json_list_item)}")

    # Salvando a lista de itens
    caminho_arquivo = "../../Data/Output/lista_itens.json"

    with open(caminho_arquivo, "w") as arquivo:
        json.dump(json_list_item, arquivo)

    with open(caminho_arquivo, "r") as arquivo:
        json_list_item = json.load(arquivo)

    df = pd.DataFrame(json_list_item)

    logger.info(f"Tamanho do dataframe de itens: {df.shape}")
    df.sample()

    # pegando dados em attributes
    # attributes: SELLER_SKU
    resultados_attributes = []

    for item in json_list_item:
        # Extrair os valores desejados
        first_id = item["id"]
        inventory_id = item["inventory_id"]
        variations = item["variations"]
        status = item["status"]
        catalog_product_id = item["catalog_product_id"]
        seller_custom_field = item["seller_custom_field"]
        catalog_listing = item["catalog_listing"]
        logistic_type = item["shipping"]["logistic_type"]
        item_relations = item["item_relations"]

        # Procurar em "attributes" onde "id" é "SELLER_SKU"
        seller_sku_entry = next(
            (attr for attr in item["attributes"] if attr["id"] == "SELLER_SKU"), None
        )

        # Pegar "value_name" e "value_id" se a entrada existir, caso contrário, definir como None
        attribute_value_name = (
            seller_sku_entry["value_name"] if seller_sku_entry else None
        )
        attribute_value_id = seller_sku_entry["value_id"] if seller_sku_entry else None

        # attribute_value_name = item["attributes"][0]["value_name"]
        # attribute_value_id = item["attributes"][0]["value_id"]

        # Adicionar os resultados_attributes à lista
        resultados_attributes.append(
            {
                "ml_code": first_id,
                "inventory_id": inventory_id,
                # "logistic_type": logistic_type,
                # "sku": attribute_value_name,
                "status": status,
                "variations": variations,
                # "attribute_value_id": attribute_value_id,
                # "catalog_product_id": catalog_product_id,
                # "seller_custom_field": seller_custom_field,
                "catalog_listing": catalog_listing,
                # "item_relations": item_relations
            }
        )

    df_sku = pd.DataFrame(resultados_attributes)

    # Exibir os resultados
    # logger.info(resultados_attributes)
    # logger.info(df_sku.shape)
    # df_sku.sample()

    # pegando dados em variations
    # variations: variation_id,  attribute_combination: value_id, value_name, seller_sku ,inventory_id
    resultados_variations = []

    for item in json_list_item:
        # Extrair os valores comuns para cada item
        first_id = item.get("id")
        inventory_id = item.get("inventory_id")
        logistic_type = item.get("shipping", {}).get("logistic_type")

        # Extrair os valores específicos para cada variação
        for variacao in item.get("variations", []):
            variation_id = variacao.get("id")
            variation_seller_sku = variacao.get("seller_custom_field")
            variation_inventory_id = variacao.get("inventory_id")
            attribute_combination = variacao.get("attribute_combinations", [{}])[0]
            value_id = attribute_combination.get("value_id")
            value_name = attribute_combination.get("value_name")
            item_relations = attribute_combination.get("item_relations", [{}])[0]

            # Adicionar os resultados_variations à lista
            resultados_variations.append(
                {
                    "ml_code": first_id,
                    "inventory_id": inventory_id,
                    # "logistic_type": logistic_type,
                    "variation_id": variation_id,
                    # "value_id": value_id,
                    "value_name": value_name,
                    # "var_seller_sku": variation_seller_sku,
                    "variation_inventory_id": variation_inventory_id,
                    # "item_relations":item_relations,
                }
            )

    df_variations = pd.DataFrame(resultados_variations)

    # Unindo as duas tabelas
    df_sku_var = pd.merge(
        df_sku,
        df_variations,
        left_on=["ml_code", "inventory_id"],
        right_on=["ml_code", "inventory_id"],
        how="left",
    )
    df_sku_var = df_sku_var.drop(["variations", "variation_id"], axis=1)
    df_sku_var

    # #### *se variation_inventory_id = None -> variation_inventory_id == inventory_id && remove inventory_id && variation_inventory_id rename to inventory_id*
    df_sku_var["variation_inventory_id"].fillna(
        df_sku_var["inventory_id"], inplace=True
    )

    # Editando tabela
    cols = [
        "ml_code",
        "variation_inventory_id",
        "value_name",
        "status",
        "catalog_listing",
    ]
    df_sku_var = df_sku_var[cols]
    df_sku_var = df_sku_var.rename(columns={"variation_inventory_id": "inventory_id"})

    logger.info(f"Tamanho do dataframe final: {df_sku_var.shape}")

    # ## Populando banco de dados
    conn = psycopg2.connect(**db_config)

    cursor = conn.cursor()

    for index, row in df_sku_var.iterrows():
        insert_query = sql.SQL(
            "INSERT INTO items (ml_code, inventory_id, value_name, status, catalog_listing) VALUES (%s, %s, %s, %s, %s)"
        )
        cursor.execute(
            insert_query,
            (
                row["ml_code"],
                row["inventory_id"],
                row["value_name"],
                row["status"],
                row["catalog_listing"],
            ),
        )

    conn.commit()

    # Feche o cursor e a conexão
    cursor.close()
    conn.close()
    logger.info("Dados inseridos com sucesso!")

    end_prog = time.time()  # Registra o tempo depois de toda aplicação
    elapsed_time = end_prog - start_prog  # Calcula o tempo decorrido
    logger.info(f"Tempo Total do processo: {elapsed_time / 60} minutos")


import_data_to_db()
