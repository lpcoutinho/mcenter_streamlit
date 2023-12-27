import json
import math
import os
import time
from datetime import datetime

import numpy as np
import pandas as pd
import psycopg2
import requests
from dotenv import load_dotenv
from loguru import logger
from psycopg2 import sql

load_dotenv()
ACCESS_TOKEN_BUENOSHOPS = os.getenv("ACCESS_TOKEN_BUENOSHOPS")
SELLER_ID_BUENOSHOPS = os.getenv("SELLER_ID_BUENOSHOPS")

ACCESS_TOKEN_MUSICALCRIS = os.getenv("ACCESS_TOKEN_MUSICALCRIS")
SELLER_ID_MUSICALCRIS = os.getenv("SELLER_ID_MUSICALCRIS")

ACCESS_TOKEN_MCENTER = os.getenv("ACCESS_TOKEN_MCENTER")
SELLER_ID_MCENTER = os.getenv("SELLER_ID_MCENTER")

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


def get_update_items(access_token, seller_id, db_config, table_item):
    start_prog = time.time()  # Registra o inicio da aplicação
    # Consulta aos itens com logistic_type=fulfillment
    base_url = f"https://api.mercadolibre.com/users/{seller_id}/items/search?logistic_type=fulfillment"

    params = {
        "limit": 100,
        "offset": 0,
    }

    headers = {"Authorization": f"Bearer {access_token}"}

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

    df_json_list = pd.DataFrame(json_list)

    # buscando de itens em json
    json_list_item = []
    c = 1
    for item in json_list:
        base_url = f"https://api.mercadolibre.com/items/{item}"
        headers = {"Authorization": f"Bearer {access_token}"}
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
            logger.warning("Esperando 1 minuto...")
            time.sleep(60)

    logger.info(f"Tamanho da lista de itens: {len(json_list_item)}")

    # Salvando a lista de itens
    caminho_arquivo = f"Data/Output/list_{table_item}.json"

    with open(caminho_arquivo, "w") as arquivo:
        json.dump(json_list_item, arquivo)

    with open(caminho_arquivo, "r") as arquivo:
        json_list_item = json.load(arquivo)

    df_list_item = pd.DataFrame(json_list_item)

    logger.info(f"Tamanho do dataframe de itens: {df_list_item.shape}")
    df_list_item.sample()

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

        # Adicionar os resultados_attributes à lista
        resultados_attributes.append(
            {
                "ml_code": first_id,
                "inventory_id": inventory_id,
                "status": status,
                "variations": variations,
                "catalog_listing": catalog_listing,
                "logistic_type": logistic_type,
            }
        )

    df_sku = pd.DataFrame(resultados_attributes)

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

    # *se variation_inventory_id = None -> variation_inventory_id == inventory_id && remove inventory_id && variation_inventory_id rename to inventory_id*
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
        "logistic_type",
    ]
    df_sku_var = df_sku_var[cols]
    df_sku_var = df_sku_var.rename(columns={"variation_inventory_id": "inventory_id"})

    logger.info(f"Tamanho do dataframe final: {df_sku_var.shape}")

    ### Populando banco de dados ###
    try:
        conn = psycopg2.connect(**db_config)

        # Use a tabela fornecida como parâmetro
        # query = f"SELECT * FROM {table_item};"
        query = f"SELECT * FROM {table_item};"
        logger.info(query)
        df_items = pd.read_sql(query, conn)
    except psycopg2.Error as e:
        logger.error(f"Erro do psycopg2 em 'items': {e}")
    except Exception as e:
        logger.error(f"Erro ao consultar 'items': {e}")

    dx = df_items.copy()
    dy = df_sku_var.copy()

    # Editando DFs
    dx = dx.drop(columns=["created_at", "updated_at"])  # remove linhas de data
    dx.replace("NaN", np.nan, inplace=True)  # altera de strin para NaN
    dx = dx.astype(str)  # altera para tipo string
    dy = dy.astype(str)

    # verificando itens que existiam na tabela e não são retornados pelo endpoint
    dx_not_in_dy = dx[~dx["inventory_id"].isin(dy["inventory_id"])]
    # dx_not_in_dy

    # buscando de itens em json
    json_no_ful = []
    c = 1
    for item in dx_not_in_dy["ml_code"]:
        base_url = f"https://api.mercadolibre.com/items/{item}"
        headers = {"Authorization": f"Bearer {access_token}"}
        t = dx_not_in_dy.shape[0]
        logger.info(item)
        logger.info(f"{c}/{t}")
        c += 1

        try:
            response = requests.get(base_url, headers=headers)
            response.raise_for_status()
            data = response.json()
            json_no_ful.append(data)
            logger.info(f"Tamanho da nova lista: {len(json_no_ful)}/{t}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao obter dados para o item {item}: {e}")

        # Se c for um múltiplo de 50, aguarde 1 minuto
        if c % 50 == 0:
            logger.warning("Esperando 1 minuto...")
            time.sleep(60)

    logger.info(f"Tamanho da lista de itens: {len(json_no_ful)}")

    # Salvando a lista de itens
    caminho_arquivo = f"Data/Output/list_{table_item}_no_ful.json"

    with open(caminho_arquivo, "w") as arquivo:
        json.dump(json_no_ful, arquivo)

    with open(caminho_arquivo, "r") as arquivo:
        json_no_ful = json.load(arquivo)

    df_no_ful = pd.DataFrame(json_no_ful)

    logger.info(f"Tamanho do dataframe de itens: {df_no_ful.shape}")
    df_no_ful.sample()

    # pegando dados em attributes
    no_ful_attributes = []

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

        # Adicionar os no_ful_attributes à lista
        no_ful_attributes.append(
            {
                "ml_code": first_id,
                "inventory_id": inventory_id,
                "status": status,
                "variations": variations,
                "catalog_listing": catalog_listing,
                "logistic_type": logistic_type,
            }
        )

    df_sku_no_ful = pd.DataFrame(no_ful_attributes)

    # pegando dados em variations
    # variations: variation_id,  attribute_combination: value_id, value_name, seller_sku ,inventory_id
    no_ful_variations = []

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

            # Adicionar os no_ful_variations à lista
            no_ful_variations.append(
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

    df_no_fulvariations = pd.DataFrame(no_ful_variations)

    # Unindo as duas tabelas
    df_no_ful = pd.merge(
        df_sku_no_ful,
        df_no_fulvariations,
        left_on=["ml_code", "inventory_id"],
        right_on=["ml_code", "inventory_id"],
        how="left",
    )
    df_no_ful = df_no_ful.drop(["variations", "variation_id"], axis=1)
    df_no_ful

    # *se variation_inventory_id = None -> variation_inventory_id == inventory_id && remove inventory_id && variation_inventory_id rename to inventory_id*
    df_no_ful["variation_inventory_id"].fillna(df_no_ful["inventory_id"], inplace=True)

    # Editando tabela
    cols = [
        "ml_code",
        "variation_inventory_id",
        "value_name",
        "status",
        "catalog_listing",
        "logistic_type",
    ]
    df_no_ful = df_no_ful[cols]
    df_no_ful = df_no_ful.rename(columns={"variation_inventory_id": "inventory_id"})

    logger.info(f"Tamanho do dataframe final: {df_no_ful.shape}")

    dy = pd.concat([dy, df_no_ful], ignore_index=True)

    # Merge com base nas colunas ml_code e inventory_id a tabela do banco de dados com a busca de hoje + itens fora do full
    merged_df = pd.merge(
        dy,
        dx,
        on=["ml_code", "inventory_id"],
        how="inner",
        suffixes=("_sku_var", "_items"),
    )

    # Linhas com valores diferentes
    different_rows = merged_df[
        (merged_df["value_name_sku_var"] != merged_df["value_name_items"])
        | (merged_df["status_sku_var"] != merged_df["status_items"])
        | (merged_df["catalog_listing_sku_var"] != merged_df["catalog_listing_items"])
    ]

    # Compare os DataFrames
    identicos = dx.equals(dy)
    # Exiba o resultado
    logger.info(f"Os DataFrames são idênticos? {identicos}")

    # Encontrar diferenças usando merge
    diferencas = (
        # pd.merge(dx, dy, how="outer", indicator=True)
        pd.merge(dy, dx, how="outer", indicator=True)
        .query('_merge == "left_only"')
        .drop("_merge", axis=1)
    )

    # Criar um novo DataFrame apenas com as colunas modificadas
    df_atualizado = dx.copy()
    df_atualizado[diferencas.columns] = diferencas

    # Remover linhas onde todos os valores em TODAS as colunas são NaN
    df_atualizado_sem_nan = df_atualizado.dropna(
        how="all", subset=df_atualizado.columns
    )

    conn = psycopg2.connect(**db_config)

    cursor = conn.cursor()

    # Iterar sobre as linhas do DataFrame e executar as atualizações no banco de dados
    for index, row in df_atualizado_sem_nan.iterrows():
        ml_code = row["ml_code"]
        inventory_id = row["inventory_id"]
        value_name = row["value_name"]
        status = row["status"]
        catalog_listing = row["catalog_listing"]
        logistic_type = row["logistic_type"]
        updated_at = datetime.now()  # Use a data/hora atual

        # Construir a instrução SQL de atualização
        query = f"UPDATE {table_item} SET value_name = %s, status = %s, catalog_listing = %s, updated_at = %s, logistic_type = %s  WHERE ml_code = %s AND inventory_id = %s"
        update_query = sql.SQL(query)
        logger.info(f"Inserindo dados: {[value for value in row]}")
        # Executar a instrução SQL
        cursor.execute(
            update_query,
            (
                value_name,
                status,
                catalog_listing,
                updated_at,
                logistic_type,
                ml_code,
                inventory_id,
            ),
        )

    conn.commit()

    cursor.close()
    conn.close()
    logger.info("Dados inseridos com sucesso!")

    # Encontrar linhas onde os pares ml_code e inventory_id em df_ficticio são diferentes de dx
    diferenca = pd.merge(
        dx, dy, on=["ml_code", "inventory_id"], how="right", indicator=True
    )

    # Filtrar apenas as linhas em que df_ficticio tem valores diferentes de dx
    diferenca = diferenca.query('_merge == "right_only"').drop(columns="_merge")

    # Selecionar colunas específicas e renomear
    diferenca = diferenca[
        ["ml_code", "inventory_id", "value_name_y", "status_y", "catalog_listing_y"]
    ]
    diferenca = diferenca.rename(
        columns={
            "value_name_y": "value_name",
            "status_y": "status",
            "catalog_listing_y": "catalog_listing",
        }
    )

    # Inserir novos dados no banco de dados
    conn = psycopg2.connect(**db_config)

    cursor = conn.cursor()

    # Use a tabela fornecida como parâmetro
    for index, row in diferenca.iterrows():
        query = f"INSERT INTO {table_item} (ml_code, inventory_id, value_name, status, catalog_listing) VALUES (%s, %s, %s, %s, %s)"
        insert_query = sql.SQL(query)
        logger.info(f"Inserindo dados: {[value for value in row]}")
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
    logger.info("Atualização de itens finalizada!")

    end_prog = time.time()  # Registra o tempo depois de toda aplicação
    elapsed_time = end_prog - start_prog  # Calcula o tempo decorrido
    logger.info(f"Tempo Total do processo: {elapsed_time / 60} minutos")


get_update_items(
    ACCESS_TOKEN_BUENOSHOPS, SELLER_ID_BUENOSHOPS, db_config, "bueno_items"
)


get_update_items(
    ACCESS_TOKEN_MUSICALCRIS, SELLER_ID_MUSICALCRIS, db_config, "cris_items"
)


get_update_items(ACCESS_TOKEN_MCENTER, SELLER_ID_MCENTER, db_config, "mcenter_items")
