import json
import os
from datetime import datetime, timedelta
from math import floor

import numpy as np
import pandas as pd
import psycopg2
import requests
import streamlit as st
from dotenv import load_dotenv
from pandas import json_normalize

# from ml_consume.calculando_envio_fulfillment import send_fulfillment
# TODO ModuleNotFoundError: No module named 'ml_consume'

load_dotenv()

ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
HOST = os.getenv("HOST")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

SMARTGO_TOKEN = os.getenv("SMARTGO_TOKEN")

# Informações de conexão com o banco de dados PostgreSQL
db_config = {
    "host": HOST,
    "database": POSTGRES_DB,
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
}

# variaveis de projeto
empresa = 'BUENOSHOPS'
table_ful_stock = 'bueno_fulfillment_stock'
table_orders = 'bueno_ml_orders'
table_items = 'bueno_items'
table_tf = 'tiny_fulfillment_bueno'
table_types = 'bueno_types'

# Interface do Streamlit
st.set_page_config(page_title=f"{empresa} FULFILLMENT", layout="wide")

st.title(f"{empresa} para enviar ao Fulfillment")

# Selecionar data da pesquisa
st.header("Defina o período da consulta", divider="grey")
date_from = st.date_input(label="Data inicial")
date_t = st.date_input(label="Data final")
date_to = date_t + timedelta(days=1)  # + 1 dia para pegar a data atual no DB

st.caption(f"Perído a consultar vai de {date_from} até {date_t}")

# date_from = st.text_input(label='Data inicial, digite apenas números',placeholder='01112023',max_chars=8)
# date_to = st.text_input(label='Data final, digite apenas números',placeholder='30112023')

st.header("Dias para o cálculo de envio", divider="grey")
input_days = st.number_input(
    label="Enviar produtos para os próximos x dias", step=1, value=30
)



def get_wms_data(SMARTGO_TOKEN):
    try:
        url = "https://apigateway.smartgo.com.br/estoque/saldo"

        payload = {}
        headers = {"api_key": SMARTGO_TOKEN}

        response = requests.request("GET", url, headers=headers, data=payload)

        if response.status_code == 200:
            data = json.loads(response.text)

            items = data.get("model", {}).get("items", [])

            result_list = []

            for item in items:
                result_dict = {
                    "id_depositante": item.get("idDepositante"),
                    "depositante": item.get("depositante"),
                    "area": item.get("area"),
                    "areaComputaSaldo": item.get("areaComputaSaldo"),
                    "idProduto": item.get("idProduto"),
                    "produto_nome": item.get("produtoNome"),
                    "produtoCodigoInterno": item.get("produtoCodigoInterno"),
                    "produtoCodigoExterno": item.get("produtoCodigoExterno"),
                    "quantidade": item.get("quantidade"),
                    "quantidadeProduto": item.get("quantidadeProduto"),
                    "quantidadeDeMovimentacao": item.get("quantidadeDeMovimentacao"),
                    "quantidadeProdutosEmbalagem": item.get(
                        "quantidadeProdutosEmbalagem"
                    ),
                    "tipoUnidadeEmbalagem": item.get("tipoUnidadeEmbalagem"),
                    "tipoUnidadeMovimentacao": item.get("tipoUnidadeMovimentacao"),
                    "tipoUnidadeProduto": item.get("tipoUnidadeProduto"),
                    "quantidadeEnderecos": item.get("quantidadeEnderecos"),
                    "quantidade_disponivel": item.get("quantidadeDisponivel"),
                    "quantidadeEmExpedicao": item.get("quantidadeEmExpedicao"),
                    "pedidosCodigosExternos": item.get("pedidosCodigosExternos"),
                    "codigosDeIdentificacao": item.get("codigosDeIdentificacao"),
                    "notasFiscais": item.get("notasFiscais"),
                    "depositos": item.get("depositos"),
                }
                result_list.append(result_dict)

            df = pd.DataFrame(result_list)
            cols = [
                "id_depositante",
                "depositante",
                "idProduto",
                "produto_nome",
                "produtoCodigoInterno",
                "produtoCodigoExterno",
                "quantidade_disponivel",
            ]
            df = df[cols]
            return df

    except requests.exceptions.RequestException as e:
        print(f"Request Exception: {e}")
        return None

    except json.JSONDecodeError as e:
        print(f"JSON Decode Error: {e}")
        return None

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None


# Botão para iniciar a consulta
if st.button("Iniciar Consulta"):
    # Exibe uma mensagem enquanto a consulta está em andamento
    mensagem_aguarde = st.warning("Aguarde, a consulta está em andamento...")

    # TODO ModuleNotFoundError: No module named 'ml_consume'
    # dfx, df_sold_zero, df_sold = send_fulfillment()

    ### Historico de estoque fulfillment ###
    # Buscando histórico de estoque na tabela
    try:
        conn = psycopg2.connect(**db_config)

        sql_query = f"SELECT * FROM {table_ful_stock} WHERE created_at BETWEEN '{date_from}' AND '{date_to};'"
        # sql_query = f"SELECT * FROM {table_ful_stock} WHERE created_at BETWEEN '2023-12-04' AND '2023-12-05';"
        # print(sql_query)
        df_stock = pd.read_sql(sql_query, conn)

    except psycopg2.Error as e:
        print(f"Erro do psycopg2 ao consultar {table_ful_stock}: {e}")

    except Exception as e:
        print(f"Erro ao consultar {table_ful_stock}: {e}")

    finally:
        if conn is not None:
            conn.close()

    # Ordenando stock por data
    df_stock = df_stock.sort_values(by="created_at", ascending=False)
    df_stock["data"] = df_stock["created_at"].dt.date
    df_stock = df_stock.drop(["created_at"], axis=1)

    ## Cria coluna has_stock, se available_quantity <= 0, has_stock= False ##
    df_stock = df_stock.assign(has_stock=lambda x: x["available_quantity"] > 0)
    df_stock = df_stock.sort_values(by="data", ascending=False).reset_index(drop=True)

    ## Contando dias em que produto esteve disponível
    days_available = (
        df_stock.groupby(["ml_inventory_id"])["has_stock"].sum().reset_index()
    )
    days_available = days_available.rename(columns={"has_stock": "days_available"})

    # Unindo DFs
    df_stock_days = df_stock.merge(days_available, on=["ml_inventory_id"], how="inner")
    # df_stock_days_left = df_stock.merge(days_available, on=["ml_inventory_id"], how="left")

    # data de hoje
    data_de_hoje = datetime.now().date()
    data_de_hoje = data_de_hoje - timedelta(days=1)
    # print(data_de_hoje)

    df_stock_days["data"] = pd.to_datetime(df_stock_days["data"])

    # Filtra apenas as linhas onde 'data' é igual à data de hoje
    df_stock_today = df_stock_days[df_stock_days["data"].dt.date == data_de_hoje]
    df_stock_today = df_stock_today.rename(
        columns={"available_quantity": "available_quantity_today"}
    )

    # Se detail_status = transfer: available_quantity_today = available_quantity_today + detail_quantity
    df_stock_today["total_available_quantity"] = df_stock_today.apply(
        lambda row: row["detail_quantity"] + row["available_quantity_today"]
        if row["detail_status"] == "transfer"
        else row["available_quantity_today"],
        axis=1,
    )

    df_stock_today["total_available_quantity"] = df_stock_today[
        "total_available_quantity"
    ].astype("int64")

    ### Buscando hitorico de orders no BD ###

    # Buscando histórico de vendas na tabela {table_orders} para o período definido
    try:
        conn = psycopg2.connect(**db_config)

        sql_query = f"SELECT * FROM {table_orders} WHERE date_closed BETWEEN '{date_from}' AND '{date_to}'"
        # print(sql_query)
        df_orders = pd.read_sql(sql_query, conn)

    except psycopg2.Error as e:
        print(f"Erro do psycopg2 ao consultar {table_orders}: {e}")
        # logger.error(f"Erro do psycopg2 ao consultar {table_orders}: {e}")

    except Exception as e:
        print(f"Erro ao consultar {table_orders}: {e}")
        # logger.error(f"Erro ao consultar {table_orders}: {e}")

    finally:
        if conn is not None:
            conn.close()

    # filtros
    df_orders = df_orders[df_orders["logistic_type"] == "fulfillment"]
    # df_orders = df_orders.drop(columns=['category_id','pack_id','variation_attributes_id','variation_name','variation_value_id', 'data'])
    df_orders = df_orders.drop(
        columns=[
            "category_id",
            "pack_id",
            "variation_attributes_id",
            "variation_name",
            "variation_value_id",
        ]
    )

    # change column
    df_orders["variation_id"] = df_orders["variation_id"].replace(
        "nan", "0", regex=True
    )

    df_orders.rename(columns={"quantity": "sold_quantity"}, inplace=True)

    # print(df_orders.shape)
    df_orders = df_orders.drop_duplicates()
    # print(df_orders.shape)

    # Ordenando orders por data
    df_orders = df_orders.sort_values(by="date_approved", ascending=False)
    df_orders["data"] = df_orders["date_approved"].dt.date
    df_orders = df_orders.drop(["date_closed", "date_approved"], axis=1)

    # Total de vendas por ml_code e id de variação
    df_orders_quantity = (
        df_orders.groupby(["ml_code", "variation_id"])["sold_quantity"]
        .sum()
        .reset_index()
    )

    print(f"Número de ml_code únicos: {len(df_orders_quantity['ml_code'].unique())}")
    print(
        f"Número de variation_id únicos: {len(df_orders_quantity['variation_id'].unique())}"
    )

    # Acrescentando total de vendas ao DF
    df_total_sales = pd.merge(
        # df_orders, resultado, on=["ml_code", "variation_id"], how="inner"
        df_orders,
        df_orders_quantity,
        on=["ml_code", "variation_id"],
        how="inner",
    )
    df_total_sales = df_total_sales.rename(
        columns={"sold_quantity_y": "total_sold_quantity"}
    )
    df_total_sales = df_total_sales.drop(
        columns=["sold_quantity_x", "order_status", "payment_status"]
    )

    # print(f"Total de vendas = {df_total_sales.shape[0]}")

    # Buscando dados de produtos na tabela {table_items}
    try:
        conn = psycopg2.connect(**db_config)
        sql_query = f"SELECT * FROM {table_items}"
        df_codes = pd.read_sql(sql_query, conn)
    except psycopg2.Error as e:
        # logger.error(f"Erro do psycopg2 ao consultar mcenter_fulfillment_stock: {e}")
        print(f"Erro do psycopg2 ao consultar {table_items}: {e}")

    except Exception as e:
        # logger.error(f"Erro ao consultar tabela {table_items}: {e}")
        print(f"Erro ao consultar tabela {table_items}: {e}")

    finally:
        if conn is not None:
            conn.close()

    # df_codes["ml_code"] = df_codes["ml_code"].apply(lambda x: "MLB" + str(x))
    df_codes.rename(columns={"inventory_id": "ml_inventory_id"}, inplace=True)
    df_codes = df_codes.drop(["created_at", "updated_at"], axis=1)

    # separando itens que são catálogos dos que não são
    df_not_catalogo = df_codes[df_codes["catalog_listing"] == False]
    df_catalogo = df_codes[df_codes["catalog_listing"] == True]

    df_total_sales_cat = pd.merge(
        df_catalogo,
        df_total_sales,
        left_on=["ml_code"],
        right_on=["ml_code"],
        how="left",
    )
    df_total_sales_cat = df_total_sales_cat.drop_duplicates()

    df_total_sales_not_cat = pd.merge(
        df_not_catalogo,
        df_total_sales,
        left_on=["ml_code", "variation_id"],
        right_on=["ml_code", "variation_id"],
        how="left",
    )
    df_total_sales_not_cat = df_total_sales_not_cat.drop_duplicates()

    df_total_sales_not_cat["total_sold_quantity"] = df_total_sales_not_cat[
        "total_sold_quantity"
    ].fillna(0)
    df_total_sales_cat["total_sold_quantity"] = df_total_sales_cat[
        "total_sold_quantity"
    ].fillna(0)

    df_total_sales_not_cat["total_sold_quantity"] = df_total_sales_not_cat[
        "total_sold_quantity"
    ].astype("int64")
    df_total_sales_cat["total_sold_quantity"] = df_total_sales_cat[
        "total_sold_quantity"
    ].astype("int64")

    # print(df_total_sales_cat.shape)
    # print(df_total_sales_not_cat.shape)

    df_total_sales_cat_x = df_total_sales_cat.drop(
        columns=["data", "shipping_id", "variation_id_x", "order_id"]
    )
    df_total_sales_cat_x = df_total_sales_cat_x.drop_duplicates()

    # df_total_sales_cat_x.shape

    # print(df_total_sales_not_cat.shape)
    # print(df_total_sales_not_cat.shape)

    df_total_sales_not_cat_x = df_total_sales_not_cat.drop(
        columns=["data", "shipping_id", "order_id"]
    )
    df_total_sales_not_cat_x = df_total_sales_not_cat_x.drop_duplicates()

    # print(df_total_sales_not_cat_x.shape)
    # print(df_total_sales_cat_x.shape)
    # print(df_total_sales_not_cat_x.shape)

    df_total_sales_cat_x = df_total_sales_cat.drop_duplicates(
        subset=["ml_code", "ml_inventory_id"]
    )
    df_total_sales_not_cat_x = df_total_sales_not_cat.drop_duplicates(
        subset=["ml_code", "ml_inventory_id"]
    )

    df_total_cat = df_total_sales_cat_x.copy()
    df_total_not_cat = df_total_sales_not_cat_x.copy()

    # Somando total de vendas por inventory_id
    df_sum_qt_sold_cat = (
        df_total_cat.groupby("ml_inventory_id")["total_sold_quantity"]
        .sum()
        .reset_index()
    )
    df_sum_qt_sold_cat = df_sum_qt_sold_cat.rename(
        columns={"total_sold_quantity": "total_sold_catalog"}
    )

    df_total_cat = pd.merge(
        df_total_cat,
        df_sum_qt_sold_cat[["ml_inventory_id", "total_sold_catalog"]],
        on="ml_inventory_id",
        how="left",
    )

    df_total_cat.rename(columns={"variation_id_y": "variation_id_"})

    df_sum_qt_sold_not_cat = (
        df_total_not_cat.groupby("ml_inventory_id")["total_sold_quantity"]
        .sum()
        .reset_index()
    )
    df_sum_qt_sold_not_cat = df_sum_qt_sold_not_cat.rename(
        columns={"total_sold_quantity": "total_sold_not_catalog"}
    )
    df_total_not_cat = pd.merge(
        df_total_not_cat,
        df_sum_qt_sold_not_cat[["ml_inventory_id", "total_sold_not_catalog"]],
        on="ml_inventory_id",
        how="left",
    )

    # print(df_total_cat.shape)
    # print(df_total_not_cat.shape)

    df_total_cat.rename(columns={"variation_id_y": "variation_id"}, inplace=True)

    # df_total_cat.shape

    df_total_cat = df_total_cat.drop_duplicates(subset=["ml_inventory_id"])

    df_combined = pd.merge(
        df_total_not_cat,
        df_total_cat[["ml_inventory_id", "total_sold_catalog"]],
        on="ml_inventory_id",
        how="left",
    )

    # print(df_combined.shape)

    df = pd.merge(df_combined, df_stock_today, on="ml_inventory_id", how="left")
    df["total_sold_catalog"] = df["total_sold_catalog"].fillna(0).astype("int64")

    # df.shape

    days = input_days

    df["total_sold"] = df["total_sold_catalog"] + df["total_sold_not_catalog"]
    # qtd de produtos a enviar no período, caso seja valor negativo produto está acima do esperado para envio(sobrando)
    df["period_send_fulfillment"] = np.ceil(
        (df["total_sold"] / df["days_available"]) * days
        - df["total_available_quantity"]
    )

    df["period_send_fulfillment"] = df["period_send_fulfillment"].fillna(0)

    # df.shape

    def calculate_percentual_send(row):
        if row["days_available"] != 0:
            if np.ceil(
                (row["total_sold"] / row["days_available"]) * days * 0.7
                > row["total_available_quantity"]
            ):
                # return (np.ceil(row["total_sold"] / row["days_available"]) * days - row["total_available_quantity"])
                return np.ceil(
                    (row["total_sold"] / row["days_available"]) * days
                    - row["total_available_quantity"]
                )

        return 0

    # Aplicando a função à coluna "percentual_send"
    df["stock_replenishment"] = df.apply(calculate_percentual_send, axis=1)

    df_have_itens = df[df["days_available"] > 0]

    # df_have_itens.shape

    # produtos sem estoque no período
    df_no_itens = df[df["days_available"] <= 0]
    # df_no_itens = df_no_itens.drop(columns=["period_send_fulfillment"])

    # df_no_itens.shape

    df_sold_zero = df_have_itens[df_have_itens["total_sold"] == 0]
    df_sold = df_have_itens[df_have_itens["total_sold"] > 0]

    dfx = df_have_itens.copy()

    cols = [
        "ml_code",
        "seller_sku",
        "ml_inventory_id",
        "value_name",
        "status",
        "title",
        "available_quantity_today",
        "detail_status",
        "detail_quantity",
        "total_available_quantity",
        "days_available",
        "total_sold_not_catalog",
        "total_sold_catalog",
        "total_sold",
        "period_send_fulfillment",
        "stock_replenishment",
    ]

    df_sold_zero = df_sold_zero[cols]
    df_sold = df_sold[cols]
    df_no_itens = df_no_itens[cols]

    def rename_columns(df):
        return df.rename(
            columns={
                "detail_status": "transfer_status",
                "detail_quantity": "transfer_quantity",
            }
        )

    df_sold_zero = rename_columns(df_sold_zero)
    df_sold = rename_columns(df_sold)
    df_no_itens = rename_columns(df_no_itens)

    ### Agrupando por categoria de produto para enviar
    df_itens_to_send = df_sold[df_sold["stock_replenishment"] > 0]
    df_itens_to_send = df_itens_to_send[["ml_inventory_id", "stock_replenishment"]]
    df_itens_to_send = df_itens_to_send.rename(
        columns={"ml_inventory_id": "inventory_id"}
    )

    # Buscando relação fulfillment X tiny
    try:
        conn = psycopg2.connect(**db_config)

        sql_query = f"SELECT * FROM {table_tf}"
        print(sql_query)
        df_tiny_fulfillment = pd.read_sql(sql_query, conn)

    except psycopg2.Error as e:
        print(f"Erro do psycopg2 ao consultar {table_tf}: {e}")
        # logger.error(f"Erro do psycopg2 ao consultar {table_tf}: {e}")

    except Exception as e:
        print(f"Erro ao consultar {table_tf}: {e}")
        # logger.error(f"Erro ao consultar {table_tf}: {e}")

    finally:
        if conn is not None:
            conn.close()

    # Buscando categorias dos produtos
    try:
        conn = psycopg2.connect(**db_config)

        sql_query = f"SELECT * FROM {table_types}"
        print(sql_query)
        df_types = pd.read_sql(sql_query, conn)

    except psycopg2.Error as e:
        print(f"Erro do psycopg2 ao consultar {table_types}: {e}")
        # logger.error(f"Erro do psycopg2 ao consultar {table_types}: {e}")

    except Exception as e:
        print(f"Erro ao consultar {table_types}: {e}")
        # logger.error(f"Erro ao consultar {table_types}: {e}")

    finally:
        if conn is not None:
            conn.close()

    # Agrupando itens a enviar com tipo de itens
    df_send_types = pd.merge(df_itens_to_send, df_types, on="inventory_id", how="inner")

    # Identificar todos os tipos únicos
    unique_types = df_send_types["type"].unique()

    # Criar grupos onde cada grupo contém todas as instâncias associadas a um tipo
    result_dfs = []

    for unique_type in unique_types:
        type_group = df_send_types[df_send_types["type"] == unique_type]
        result_dfs.append(type_group)

    ## Exibir os DataFrames resultantes
    # for i, result_df in enumerate(result_dfs):
    #     print(f"Grupo {i + 1}:\n{result_df}\n")
    #     result_df

    # Lista para armazenar os DataFrames resultantes de cada agrupamento
    result_dfs_list = []

    while any(result_df.shape[0] > 0 for result_df in result_dfs):
        first_rows_dfs = []
        remaining_rows_dfs = []

        for result_df in result_dfs:
            if not result_df.empty:
                # Pega a primeira linha do DataFrame
                first_row_df = result_df.head(1)
                first_rows_dfs.append(first_row_df)

                # Pega as linhas restantes do DataFrame
                remaining_rows_df = result_df.iloc[1:]
                if not remaining_rows_df.empty:
                    remaining_rows_dfs.append(remaining_rows_df)
                else:
                    print(
                        f"DataFrame vazio encontrado após extrair a primeira linha:\n{result_df}"
                    )

        # Adiciona o DataFrame resultante de cada agrupamento à lista
        result_dfs_list.append(pd.concat(first_rows_dfs, ignore_index=True))

        # Atualiza a lista result_dfs com os DataFrames restantes
        result_dfs = remaining_rows_dfs.copy()

    ### Contando estoque da WMS em Produtos sem estoque no período

    # buscando dados da SmartGo
    df_wms = get_wms_data(SMARTGO_TOKEN)

    # renomeando colunas
    dic_column_name = {"ml_inventory_id": "inventory_id"}
    df_no_itens = df_no_itens.rename(columns=dic_column_name)
    df_sold_zero = df_sold_zero.rename(columns=dic_column_name)

    # unindo df de 'sem itens' com a relação tiny x fulfillment
    df_tiny_fulfillment_no_itens = pd.merge(
        df_no_itens, df_tiny_fulfillment, on="inventory_id", how="inner"
    )

    # organizando dados
    df_tiny_fulfillment_no_itens = df_tiny_fulfillment_no_itens.drop(
        columns=["ml_code_y", "seller_sku", "title"]
    )

    dic_new_names = {
        "ml_code_x": "ml_code",
        "SKU": "seller_sku",
        "Título do anúncio": "title",
        "Quantidade do item": "qtd_item",
        "ID Tiny": "tiny_id",
        "SKU Tiny": "tiny_sku",
        "Tipo de produto": "type",
    }
    df_tiny_fulfillment_no_itens = df_tiny_fulfillment_no_itens.rename(
        columns=dic_new_names
    )

    cols = [
        "inventory_id",
        "ml_code",
        "seller_sku",
        "title",
        "tiny_id",
        "tiny_sku",
        "qtd_item",
        "type",
        "stock_replenishment",
        "status",
    ]

    df_tiny_fulfillment_no_itens = df_tiny_fulfillment_no_itens[cols]

    # Unindo dfs por tiny_sku e códigos externos e internos
    df_wms_tf_no_itens_ci = pd.merge(
        df_tiny_fulfillment_no_itens,
        df_wms,
        left_on="tiny_sku",
        right_on="produtoCodigoInterno",
        how="left",
    )
    df_wms_tf_no_itens_ce = pd.merge(
        df_tiny_fulfillment_no_itens,
        df_wms,
        left_on="tiny_sku",
        right_on="produtoCodigoExterno",
        how="left",
    )

    # concatenando os dfs
    df_wms_tf_no_itens = pd.concat(
        [df_wms_tf_no_itens_ci, df_wms_tf_no_itens_ce], ignore_index=True
    )
    df_wms_tf_no_itens = df_wms_tf_no_itens.drop_duplicates()

    cols = [
        "inventory_id",
        "ml_code",
        "seller_sku",
        "title",
        "tiny_id",
        "tiny_sku",
        "qtd_item",
        "stock_replenishment",
        "status",
        "produtoCodigoInterno",
        "produtoCodigoExterno",
        "quantidade_disponivel",
    ]

    df_wms_tf_no_itens = df_wms_tf_no_itens[cols]

    df_wms_tf_no_itens["quantidade_disponivel"] = (
        df_wms_tf_no_itens["quantidade_disponivel"].fillna(0).astype("int64")
    )

    # Dados onde quantidade_disponivel = 0
    df_wms_tf_no_itens_less_zero = df_wms_tf_no_itens[
        df_wms_tf_no_itens["quantidade_disponivel"] < 1
    ]
    df_wms_tf_no_itens_less_zero = df_wms_tf_no_itens_less_zero.drop_duplicates()

    ### Contando estoque da WMS em Produtos sem vendas no período

    # unindo df de 'sem vendar' com a relação tiny x fulfillment
    df_tiny_fulfillment_sold_zero = pd.merge(
        df_sold_zero, df_tiny_fulfillment, on="inventory_id", how="inner"
    )

    # organizando dados
    df_tiny_fulfillment_sold_zero = df_tiny_fulfillment_sold_zero.drop(
        columns=["ml_code_y", "seller_sku", "title"]
    )

    dic_new_names = {
        "ml_code_x": "ml_code",
        "SKU": "seller_sku",
        "Título do anúncio": "title",
        "Quantidade do item": "qtd_item",
        "ID Tiny": "tiny_id",
        "SKU Tiny": "tiny_sku",
        "Tipo de produto": "type",
    }
    df_tiny_fulfillment_sold_zero = df_tiny_fulfillment_sold_zero.rename(
        columns=dic_new_names
    )

    cols = [
        "inventory_id",
        "ml_code",
        "seller_sku",
        "title",
        "tiny_id",
        "tiny_sku",
        "qtd_item",
        "type",
        "stock_replenishment",
        "status",
    ]

    df_tiny_fulfillment_sold_zero = df_tiny_fulfillment_sold_zero[cols]

    # Unindo dfs por tiny_sku e códigos externos e internos
    df_wms_tf_sold_zero_ci = pd.merge(
        df_tiny_fulfillment_sold_zero,
        df_wms,
        left_on="tiny_sku",
        right_on="produtoCodigoInterno",
        how="left",
    )
    df_wms_tf_sold_zero_ce = pd.merge(
        df_tiny_fulfillment_sold_zero,
        df_wms,
        left_on="tiny_sku",
        right_on="produtoCodigoExterno",
        how="left",
    )

    # concatenando os dfs
    df_wms_tf_sold_zero = pd.concat(
        [df_wms_tf_sold_zero_ci, df_wms_tf_sold_zero_ce], ignore_index=True
    )
    df_wms_tf_sold_zero = df_wms_tf_sold_zero.drop_duplicates()

    cols = [
        "inventory_id",
        "ml_code",
        "seller_sku",
        "title",
        "tiny_id",
        "tiny_sku",
        "qtd_item",
        "stock_replenishment",
        "status",
        "produtoCodigoInterno",
        "produtoCodigoExterno",
        "quantidade_disponivel",
    ]

    df_wms_tf_sold_zero = df_wms_tf_sold_zero[cols]

    df_wms_tf_sold_zero["quantidade_disponivel"] = (
        df_wms_tf_sold_zero["quantidade_disponivel"].fillna(0).astype("int64")
    )

    # Dados onde quantidade_disponivel = 0
    df_wms_tf_sold_zero_less_zero = df_wms_tf_sold_zero[
        df_wms_tf_sold_zero["quantidade_disponivel"] < 1
    ]
    df_wms_tf_sold_zero_less_zero = df_wms_tf_sold_zero_less_zero.drop_duplicates()

    ## Removendo e somando duplicatas
    # Lista das colunas que devem ser usadas para identificar linhas repetidas
    cols_to_check_duplicates = [
        "inventory_id",
        "ml_code",
        "seller_sku",
        "title",
        "tiny_id",
        "tiny_sku",
        "qtd_item",
        "stock_replenishment",
        "status",
        "produtoCodigoInterno",
        "produtoCodigoExterno",
    ]

    # Agrupar por linhas repetidas e somar a coluna quantidade_disponivel
    df_wms_tf_no_itens_sum = (
        df_wms_tf_no_itens.groupby(cols_to_check_duplicates)["quantidade_disponivel"]
        .sum()
        .reset_index()
    )
    df_wms_tf_sold_zero_sum = (
        df_wms_tf_sold_zero.groupby(cols_to_check_duplicates)["quantidade_disponivel"]
        .sum()
        .reset_index()
    )

    # Concatene os dois DataFrames verticalmente
    df_wms_tf_no_itens = pd.concat(
        [df_wms_tf_no_itens_sum, df_wms_tf_no_itens_less_zero], ignore_index=True
    )
    df_wms_tf_no_itens = df_wms_tf_no_itens.drop_duplicates()

    df_wms_tf_sold_zero = pd.concat(
        [df_wms_tf_sold_zero_sum, df_wms_tf_sold_zero_less_zero], ignore_index=True
    )
    df_wms_tf_sold_zero = df_wms_tf_sold_zero.drop_duplicates()

    # organizando
    df_wms_tf_no_itens = df_wms_tf_no_itens[cols]
    df_wms_tf_sold_zero = df_wms_tf_sold_zero[cols]

    ### Streamlit exibição
    # # Remove a mensagem de aviso e exibe os resultados
    mensagem_aguarde.empty()
    st.success("Consulta concluída com sucesso!")

    st.header("Produtos com vendas no período", divider="grey")
    st.dataframe(df_sold, use_container_width=True)
    # st.write(len(dfx), len(df_sold_zero), len(df_sold))
    st.header("Produtos sem vendas no período", divider="grey")
    # st.dataframe(df_sold_zero, use_container_width=True)
    st.dataframe(df_wms_tf_sold_zero, use_container_width=True)
    st.header("Produtos sem estoque no período", divider="grey")
    # st.dataframe(df_no_itens, use_container_width=True)
    st.dataframe(df_wms_tf_no_itens, use_container_width=True)

    # Exibir os DataFrames resultantes de cada agrupamento
    cols = [
        "inventory_id",
        "ml_code",
        "seller_sku",
        "title",
        "stock_replenishment",
        "tiny_id",
        "tiny_sku",
        "qtd_item",
        "qtd_to_send",
        "type",
    ]

    st.header("Agrupamento de produtos a enviar", divider="grey")
    # for i, result_df in enumerate(result_dfs_list):
    #     st.subheader(f"Grupo de envio {i + 1}")
    #     result_df = result_df[cols]
    #     st.dataframe(result_df, use_container_width=True)

    # Exibir os DataFrames resultantes de cada agrupamento
    for i, result_df in enumerate(result_dfs_list):
        result_df = pd.merge(
            result_df, df_tiny_fulfillment, on="inventory_id", how="inner"
        )
        result_df = result_df.drop(["Tipo de produto"], axis=1)
        result_df = result_df.rename(
            columns={
                "Quantidade do item": "qtd_item",
                "SKU": "seller_sku",
                "Título do anúncio": "title",
                "ID Tiny": "tiny_id",
                "SKU Tiny": "tiny_sku",
            }
        )

        result_df["qtd_to_send"] = (
            result_df["stock_replenishment"] * result_df["qtd_item"]
        )
        result_df["qtd_item"] = result_df["qtd_item"].astype("int64")
        result_df["qtd_to_send"] = result_df["qtd_to_send"].astype("int64")

        result_df = result_df[cols]
        
        ##############################################################################################
        resultado = pd.merge(result_df, df_wms, left_on='tiny_sku', right_on='produtoCodigoInterno', how='inner')
        resultado['produtoCodigoInterno'].fillna(resultado['tiny_sku'], inplace=True)

        df_envio = resultado.copy()
        # Função para aplicar nas linhas do DataFrame
        def ajustar_envio(row):
            if (row['quantidade_disponivel'] < row['qtd_to_send']) and (row['quantidade_disponivel'] > 0):
                return row['quantidade_disponivel']
            elif row['quantidade_disponivel'] > row['qtd_to_send']:
                return row['qtd_to_send']
            else:
                return 0

        # Criar a coluna 'envio_ajust' aplicando a função nas linhas
        df_envio['envio_ajust'] = df_envio.apply(ajustar_envio, axis=1)

        # Produtos sem estoque 
        df_no_stock = df_envio[(df_envio['quantidade_disponivel'] <  0)]

        # Produtos com estoque menor que o solicitado e quantidade disponível maior que 0
        df_less_stock = df_envio[(df_envio['quantidade_disponivel'] < df_envio['qtd_to_send']) & (df_envio['quantidade_disponivel'] > 0)]

        # Encontrar as linhas comuns entre df_envio e df_no_stock
        common_rows = pd.merge(df_envio, df_no_stock, how='inner')

        # Remover as linhas comuns de df_envio
        df_envio = df_envio[~df_envio.isin(common_rows.to_dict(orient='list')).all(axis=1)]
        
        # Encontrar as linhas duplicadas com base na coluna 'inventory_id'
        duplicated_rows = df_envio[df_envio.duplicated(subset='inventory_id', keep=False)]

        # Verificar se duplicated_rows não é vazio
        if not duplicated_rows.empty:
            # Criar um novo DataFrame com as linhas duplicadas
            df_duplicatas = duplicated_rows.copy()

            # Agrupar por 'inventory_id'
            grouped_df = df_duplicatas.groupby('inventory_id')

            # Iterar sobre os grupos
            for group_name, group_df in grouped_df:
                # Verificar a condição em cada linha do grupo
                condition_check = group_df['envio_ajust'] > group_df['quantidade_disponivel']

                # Exibir as linhas que satisfazem a condição
                df_satisfaz_condicao = group_df[condition_check]

                # Calcular a razão e adicionar uma nova coluna
                df_satisfaz_condicao['razao'] = df_satisfaz_condicao['quantidade_disponivel'] / df_satisfaz_condicao['qtd_item']
                df_satisfaz_condicao['razao'] = df_satisfaz_condicao['razao'].apply(floor)

                # Aplicar o valor da razão em df_envio com base na condição
                df_envio.loc[df_envio['inventory_id'].isin(df_satisfaz_condicao['inventory_id']), 'razao'] = df_satisfaz_condicao['razao'].values

                # Atualizar a coluna 'envio_ajust' com base na nova lógica
                df_envio['envio_ajust'] = df_envio.apply(lambda row: row['qtd_item'] * row['razao'] if row['razao'] > 1 else row['envio_ajust'], axis=1)

                # Preencher valores nulos na coluna 'razao' em df_envio com 1
                df_envio['razao'] = df_envio['razao'].fillna(1).astype('int64')

        
        ##############################################################################################
        
        # print(f"Novo DataFrame do Agrupamento {i + 1}:\n", result_df)
        st.subheader(f"Grupo de envio {i + 1}")
        st.dataframe(df_envio, use_container_width=True)
