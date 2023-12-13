import io
import json
import math
import os
import time
from datetime import datetime, timedelta

import altair as alt
import numpy as np
import pandas as pd
import psycopg2
import requests
import streamlit as st
from dotenv import load_dotenv
from loguru import logger
from psycopg2 import sql

# from ml_consume import get_Itens
# from ...ml_consume import get_Itens

logger.add(
    "Data/Output/Log/ml_log.log",
    rotation="10 MB",
    format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
)


load_dotenv(override=True)
ACCESS_TOKEN_BUENOSHOPS = os.getenv("ACCESS_TOKEN_BUENOSHOPS")
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

st.set_page_config(page_title="BUENOSHOPS ITENS", page_icon="📦", layout="wide")

# Initialize connection.
# conn = st.connection("postgresql", type="sql")

# # @st.cache_data
# df = conn.query("SELECT * FROM tiny_products;", ttl="1m")


def fetch_data(query):
    conn = psycopg2.connect(**db_config)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


# Consulta SQL
query = "SELECT * FROM bueno_items;"

# Recupera os dados usando a função fetch_data
df = fetch_data(query)

st.header("BUENOSHOPS Itens")
st.caption("Tabela de itens")
st.dataframe(df, use_container_width=True)


@st.cache_data
def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    excel_buffer = io.BytesIO()
    df.to_excel(excel_writer=excel_buffer, index=False)
    excel_buffer.seek(0)  # Move the buffer position to the beginning
    return excel_buffer


excel_buffer = convert_df(df)

col1, col2 = st.columns(2)
with col1:
    st.download_button(
        label="Download dos Dados",
        data=excel_buffer.read(),
        file_name="bueno_items.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="primary",
    )

with col2:
    if st.button("Atualizar banco de dados", type="primary"):
        start_prog = time.time()  # Registra o inicio da aplicação

        load_dotenv(override=True)

        ACCESS_TOKEN_BUENOSHOPS = os.getenv("ACCESS_TOKEN_BUENOSHOPS")
        SELLER_ID_BUENOSHOPS = os.getenv("SELLER_ID_BUENOSHOPS")
        
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
        base_url = F"https://api.mercadolibre.com/users/{SELLER_ID_BUENOSHOPS}/items/search?logistic_type=fulfillment"

        params = {
            "limit": 100,
            "offset": 0,
        }

        headers = {"Authorization": f"Bearer {ACCESS_TOKEN_BUENOSHOPS}"}

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
            headers = {"Authorization": f"Bearer {ACCESS_TOKEN_BUENOSHOPS}"}
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
                logger.info("Esperando 1 minuto...")
                time.sleep(60)

        logger.info(f"Tamanho da lista de itens: {len(json_list_item)}")

        # Salvando a lista de itens
        caminho_arquivo = "Data/Output/lista_itens.json"

        # Abre o arquivo em modo de escrita ('w' para escrever, 'a' para anexar)
        with open(caminho_arquivo, "w") as arquivo:
            # Escreve no arquivo
            arquivo.write("Olá, este é um exemplo de conteúdo para o arquivo.")

        # Mensagem indicando que a escrita foi concluída
        print("Conteúdo foi escrito no arquivo com sucesso.")

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
                (attr for attr in item["attributes"] if attr["id"] == "SELLER_SKU"),
                None,
            )

            # Pegar "value_name" e "value_id" se a entrada existir, caso contrário, definir como None
            attribute_value_name = (
                seller_sku_entry["value_name"] if seller_sku_entry else None
            )
            attribute_value_id = (
                seller_sku_entry["value_id"] if seller_sku_entry else None
            )

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
        df_sku_var = df_sku_var.rename(
            columns={"variation_inventory_id": "inventory_id"}
        )

        logger.info(f"Tamanho do dataframe final: {df_sku_var.shape}")

        # # ## Populando banco de dados
        # conn = psycopg2.connect(**db_config)

        # cursor = conn.cursor()

        # for index, row in df_sku_var.iterrows():
        #     insert_query = sql.SQL(
        #         "INSERT INTO items (ml_code, inventory_id, value_name, status, catalog_listing) VALUES (%s, %s, %s, %s, %s)"
        #     )
        #     cursor.execute(
        #         insert_query,
        #         (
        #             row["ml_code"],
        #             row["inventory_id"],
        #             row["value_name"],
        #             row["status"],
        #             row["catalog_listing"],
        #         ),
        #     )

        # conn.commit()

        # # Feche o cursor e a conexão
        # cursor.close()
        # conn.close()
        # logger.info("Dados inseridos com sucesso!")

        # Consulta tabela bueno_items do db
        try:
            conn = psycopg2.connect(**db_config)

            query = "SELECT * FROM bueno_items;"
            df_items = pd.read_sql(query, conn)
        except psycopg2.Error as e:
            logger.error(f"Erro do psycopg2 em 'bueno_items': {e}")
        except Exception as e:
            logger.error(f"Erro ao consultar 'bueno_items': {e}")

        dx = df_items.copy()
        dy = df_sku_var.copy()

        # Editando DFs
        dx = dx.drop(columns=["created_at", "updated_at"])  # remove linhas de data
        dx.replace("NaN", np.nan, inplace=True)  # altera de strin para NaN
        dx = dx.astype(str)  # altera para tipo string
        dy = dy.astype(str)

        # Merge com base nas colunas ml_code e inventory_id
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
            | (
                merged_df["catalog_listing_sku_var"]
                != merged_df["catalog_listing_items"]
            )
        ]

        # Compare os DataFrames
        identicos = dx.equals(dy)
        # Exiba o resultado
        logger.info("Os DataFrames são idênticos:", identicos)

        # Encontrar diferenças usando merge
        diferencas = (
            pd.merge(dx, dy, how="outer", indicator=True)
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
            updated_at = datetime.now()  # Use a data/hora atual

            # Construir a instrução SQL de atualização
            update_query = sql.SQL(
                "UPDATE bueno_items SET value_name = %s, status = %s, catalog_listing = %s, updated_at = %s WHERE ml_code = %s AND inventory_id = %s"
            )

            # Executar a instrução SQL
            cursor.execute(
                update_query,
                (
                    value_name,
                    status,
                    catalog_listing,
                    updated_at,
                    ml_code,
                    inventory_id,
                ),
            )

        conn.commit()

        cursor.close()
        conn.close()
        logger.info("Dados atualizados com sucesso em bueno_items!")

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

        for index, row in diferenca.iterrows():
            insert_query = sql.SQL(
                "INSERT INTO bueno_items (ml_code, inventory_id, value_name, status, catalog_listing) VALUES (%s, %s, %s, %s, %s)"
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
        print("Novos dados inseridos com sucesso em bueno_items!")

        end_prog = time.time()  # Registra o tempo depois de toda aplicação
        elapsed_time = end_prog - start_prog  # Calcula o tempo decorrido
        logger.info(f"Tempo Total do processo: {elapsed_time / 60} minutos")
    else:
        pass
