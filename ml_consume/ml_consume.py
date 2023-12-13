# Importando bibliotecas
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
from utils import save_json_list_to_txt, sendREST

logger.add(
    "Data/Output/Log/ml_log.log",
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


class MeLiLoader:
    def __init__(self, db_config, ACCESS_TOKEN):
        self.db_config = db_config
        self.ACCESS_TOKEN = ACCESS_TOKEN

    def load_tiny_fulfillment(self):
        logger.info(
            f"Buscando Inventory IDs de Produtos FulFillment na API do Mercado Livre"
        )
        try:
            conn = psycopg2.connect(
                **self.db_config
            )  # Conecta ao banco de dados usando db_config
            # query = "SELECT * FROM tiny_ml_codes;"
            query = "SELECT * FROM tiny_fulfillment;"
            df_tiny_fulfillment = pd.read_sql_query(query, conn)
            conn.close()
            logger.info(f"Criando DataFrame com a tabela 'tiny_fulfillment'")
            return df_tiny_fulfillment
        except Exception as e:
            logger.info(f"Ocorreu um erro: {str(e)}")
            return None

    def get_fulfillment_json_list(self):
        load_dotenv(override=True)
        logger.info("Buscando inventory_ids para consultar no Mercado Livre")

        df_codes = self.load_tiny_fulfillment()

        df_codes = df_codes.head(10)

        counter = 0
        json_list = []

        for item in df_codes["ml_inventory_id"]:
            url = f"https://api.mercadolibre.com/inventories/{item}/stock/fulfillment"

            payload = {}
            headers = {"Authorization": f"Bearer {self.ACCESS_TOKEN}"}
            logger.info(f"Buscando dados de: {item}")

            response = requests.get(url, headers=headers, data=payload)
            response_data = response.json()

            json_list.append(response_data)

            counter += 1

            if counter % 50 == 0:
                logger.info(f"Fazendo uma pausa de 1 minuto...")
                time.sleep(60)

        return json_list

    def get_fulfillment(self):
        logger.info("Buscando inventory_ids para consultar no Mercado Livre")

        json_list = self.get_fulfillment_json_list()

        output_file = "ml_fulfillment.txt"

        save_json_list_to_txt(json_list, output_file)

        df_er = json_normalize(
            json_list,
            record_path="external_references",
            meta=[
                "inventory_id",
                "total",
                "available_quantity",
                "not_available_quantity",
                "not_available_detail",
            ],
        )
        df_nad = json_normalize(
            json_list,
            record_path="not_available_detail",
            meta=[
                "inventory_id",
                "total",
                "available_quantity",
                "not_available_quantity",
                "external_references",
            ],
        )

        df_nad = df_nad.drop(columns="external_references")
        df_er = df_er.drop(columns="not_available_detail")

        common_cols = [
            "inventory_id",
            "total",
            "available_quantity",
            "not_available_quantity",
        ]

        df_fulfillment = df_er.merge(df_nad, on=common_cols, how="left")

        map_cols = {
            "inventory_id": "ml_inventory_id",
            "id": "ml_item_id",
            "status": "nad_status",
            "quantity": "nad_quantity",
        }
        df_fulfillment = df_fulfillment.rename(columns=map_cols)
        order_col = [
            "ml_inventory_id",
            "ml_item_id",
            "variation_id",
            "nad_status",
            "nad_quantity",
            "total",
            "available_quantity",
            "not_available_quantity",
            "type",
        ]
        df_fulfillment = df_fulfillment[order_col]

        df_fulfillment["variation_id"] = df_fulfillment["variation_id"].astype(str)
        df_fulfillment["nad_quantity"] = (
            df_fulfillment["nad_quantity"].fillna(0).astype(int)
        )

        return df_fulfillment

    def get_and_insert_fulfillment_stock(self):
        load_dotenv(override=True)
        logger.info("Obtendo e inserindo dados de estoque de fulfillment")

        conn = psycopg2.connect(**self.db_config)
        cursor = conn.cursor()

        try:
            # Coletando inventory_id
            sql_query = "SELECT * FROM tiny_fulfillment"
            df_codes = pd.read_sql(sql_query, conn)
            df_codes = df_codes.drop_duplicates()
            df_codes = df_codes.dropna(subset=["ml_inventory_id"])

            codes = df_codes["ml_inventory_id"].unique()
            codes = np.delete(codes, np.where(codes == "NaN"))

            counter = 0
            json_list = []

            # Solicitando dados de estoque do fulfillment
            for item in codes:
                url = (
                    f"https://api.mercadolibre.com/inventories/{item}/stock/fulfillment"
                )

                payload = {}
                headers = {"Authorization": f"Bearer {self.ACCESS_TOKEN}"}
                logger.info(f"Buscando dados {counter}/{len(codes)}: {item}")

                try:
                    response = requests.get(url, headers=headers, data=payload)
                    response.raise_for_status()
                    response_data = response.json()
                    json_list.append(response_data)
                    counter += 1

                    if counter % 50 == 0:
                        logger.info(f"Fazendo uma pausa de 1 minuto...")
                        time.sleep(60)

                except requests.exceptions.RequestException as e:
                    logger.error(f"Erro ao fazer a requisição para {url}: {e}")

                except Exception as e:
                    logger.error(f"Erro não esperado: {e}")

            df = pd.DataFrame(json_list)
            df_full = df[["inventory_id", "available_quantity"]]

            # Inserindo quantidade de produtos disponíveis no BD
            for index, row in df_full.iterrows():
                insert_query = sql.SQL(
                    """
                    INSERT INTO fulfillment_stock_hist (ml_inventory_id, available_quantity)
                    VALUES (%s, %s)
                """
                )
                cursor.execute(
                    insert_query, (row["inventory_id"], row["available_quantity"])
                )

            conn.commit()

            logger.info("Dados de estoque de fulfillment inseridos com sucesso!")

        finally:
            cursor.close()
            conn.close()

    def load_fulfillment_stock_hist(self):
        logger.info(f"Carregando histórico de produtos disponíveis no fulfillment")
        try:
            conn = psycopg2.connect(
                **self.db_config
            )  # Conecta ao banco de dados usando db_config
            # query = "SELECT * FROM tiny_ml_codes;"
            query = "SELECT * FROM fulfillment_stock_hist;"
            df_fulfillment_stock_hist = pd.read_sql_query(query, conn)
            conn.close()
            logger.info(f"Criando DataFrame com a tabela 'fulfillment_stock_hist'")
            return df_fulfillment_stock_hist
        except Exception as e:
            logger.info(f"Ocorreu um erro: {str(e)}")
            return None

    def get_orders_data(self):
        load_dotenv(override=True)

        # Selecionar data da pesquisa
        data_hoje = datetime.now().date()
        date_from = data_hoje - timedelta(days=1)

        date_to = data_hoje

        # URL base da API
        base_url = "https://api.mercadolibre.com/orders/search"

        # Parâmetros iniciais
        params = {
            "seller": "233632476",
            "order.date_created.from": f"{date_from}T00:00:00.000-03:00",
            "order.date_created.to": f"{date_to}T00:00:00.000-03:00",
            "limit": 50,
            "offset": 0,
        }

        headers = {"Authorization": f"Bearer {self.ACCESS_TOKEN}"}

        json_list = []

        counter = 0

        # Paginando e coletando dados de orders
        try:
            while True:
                response = requests.get(base_url, params=params, headers=headers)
                response.raise_for_status()  # Lança uma exceção se a resposta não for bem-sucedida
                data = response.json()

                if "results" in data:
                    json_list.extend(data["results"])
                else:
                    break

                # Verifique se há mais páginas
                if "paging" in data:
                    total_paging = data["paging"].get("total")
                    if total_paging is None:
                        break

                    total_pages = math.ceil(total_paging / params["limit"])
                    print(f"Total esperado de páginas: {counter}/{total_pages}")
                    print(f'Offset atual: {params["offset"]}')

                    counter += 1
                    if params["offset"] >= total_paging:
                        break

                    params["offset"] += params["limit"]
                else:
                    break

        except requests.exceptions.RequestException as req_err:
            print(f"Erro ao fazer a requisição para {base_url}: {req_err}")

        except Exception as e:
            print(f"Erro não esperado: {e}")

        print(f"Total de dados coletados: {len(json_list)}")

        meta_cols = ["date_closed", "pack_id", "shipping", "order_items"]

        df_payments = json_normalize(
            json_list, record_path=["payments"], meta=meta_cols
        )

        cols = ["date_approved", "status", "shipping"]
        df_payments = df_payments[cols]

        col = {"status": "payment_status"}
        df_payments.rename(columns=col, inplace=True)

        # Removendo valores nulos
        df_payments = df_payments.dropna(subset=["date_approved"])

        # Extraindo shipping_id
        df_payments["shipping_id"] = df_payments["shipping"].apply(lambda x: x["id"])
        df_payments["shipping_id"] = (
            df_payments["shipping_id"]
            .astype(str)
            .apply(lambda x: x.split(".")[0] if "." in x else x)
        )
        df_payments = df_payments.drop("shipping", axis=1)

        df_payments = df_payments.drop_duplicates()

        # Encontrando os índices das linhas com a data mais recente para cada shipping_id
        indices_recentes = df_payments.groupby("shipping_id")["date_approved"].idxmax()

        # Criando um novo DataFrame com base nos índices de envio encontrados
        df_payments = df_payments.loc[indices_recentes]
        df_payments = df_payments.loc[df_payments["shipping_id"] != "nan"]

        df_payments["shipping_id"].value_counts()[
            df_payments["shipping_id"].value_counts() > 1
        ]

        df_orders = json_normalize(
            json_list,
            record_path=["order_items"],
            meta=["date_closed", "pack_id", "status", "shipping"],
        )

        cols = [
            "quantity",
            "item.id",
            "item.title",
            "item.category_id",
            "item.variation_id",
            "item.seller_sku",
            "date_closed",
            "pack_id",
            "status",
            "shipping",
        ]

        df_orders = df_orders[cols]
        col = {"status": "order_status"}
        df_orders.rename(columns=col, inplace=True)

        df_orders["item.variation_id"] = (
            df_orders["item.variation_id"]
            .astype(str)
            .apply(lambda x: x.split(".")[0] if "." in x else x)
        )

        # Extraindo shipping_id
        df_orders["shipping_id"] = df_orders["shipping"].apply(lambda x: x["id"])
        df_orders["shipping_id"] = (
            df_orders["shipping_id"]
            .astype(str)
            .apply(lambda x: x.split(".")[0] if "." in x else x)
        )
        df_orders = df_orders.drop("shipping", axis=1)

        df_resultado = pd.merge(df_orders, df_payments, on="shipping_id", how="left")

        cols = {
            "item.id": "ml_code",
            "item.category_id": "category_id",
            "item.variation_id": "variation_id",
            "item.seller_sku": "seller_sku",
            "item.title": "title",
        }
        df_resultado.rename(columns=cols, inplace=True)

        return df_resultado

        # def process_payment_data(self, json_list):
        # Coletando dados de pagamentos
        meta_cols = ["date_closed", "pack_id", "shipping", "order_items"]
        df_payments = json_normalize(
            json_list, record_path=["payments"], meta=meta_cols
        )

        cols = ["date_approved", "status", "shipping"]
        df_payments = df_payments[cols]

        col = {"status": "payment_status"}
        df_payments.rename(columns=col, inplace=True)

        # Removendo valores nulos
        df_payments = df_payments.dropna(subset=["date_approved"])

        # Extraindo shipping_id
        df_payments["shipping_id"] = df_payments["shipping"].apply(lambda x: x["id"])
        df_payments["shipping_id"] = (
            df_payments["shipping_id"]
            .astype(str)
            .apply(lambda x: x.split(".")[0] if "." in x else x)
        )
        df_payments = df_payments.drop("shipping", axis=1)

        # Remove duplicatas
        df_payments = df_payments.drop_duplicates()

        # Encontrando os índices de envio mais recentes
        indices_recentes = df_payments.groupby("shipping_id")["date_approved"].idxmax()

        # Criando um novo DataFrame com base nos índices de envio encontrados
        df_payments = df_payments.loc[indices_recentes]

        return df_payments

        # def process_order_data(self, json_list):
        # Coletando dados de orders
        df_orders = json_normalize(
            json_list,
            record_path=["order_items"],
            meta=["date_closed", "pack_id", "status", "shipping"],
        )

        cols = [
            "quantity",
            "item.id",
            "item.title",
            "item.category_id",
            "item.variation_id",
            "item.seller_sku",
            "date_closed",
            "pack_id",
            "status",
            "shipping",
        ]
        df_orders = df_orders[cols]

        # Extraindo shipping_id
        df_orders["shipping_id"] = df_orders["shipping"].apply(lambda x: x["id"])
        df_orders["shipping_id"] = (
            df_orders["shipping_id"]
            .astype(str)
            .apply(lambda x: x.split(".")[0] if "." in x else x)
        )
        df_orders = df_orders.drop("shipping", axis=1)

        return df_orders

        # def process_shipments_data(self, df_orders, df_payments):
        load_dotenv(override=True)
        # Unindo DFs de orders e payments
        df_resultado = pd.merge(df_orders, df_payments, on="shipping_id", how="left")

        # Pegando valores unicos para consula futura
        uniq_shipping_id = df_resultado["shipping_id"].unique()

        json_shipments_list = []
        success_count = 0
        error_count = 0
        counter = 0

        for shipping_id in uniq_shipping_id:
            url = f"https://api.mercadolibre.com/shipments/{shipping_id}"

            payload = {}
            headers = {"Authorization": f"Bearer {self.ACCESS_TOKEN}"}

            try:
                print(
                    f"Loop nº {counter}/{len(uniq_shipping_id)}: shipping_id = {shipping_id}"
                )
                response = requests.request("GET", url, headers=headers, data=payload)
                response.raise_for_status()

                json_shipments_list.append(response.json())
                success_count += 1
            except requests.exceptions.RequestException as e:
                error_count += 1
                logger.error(
                    f"Erro na solicitação para shipping_id {shipping_id}: {str(e)}"
                )
            counter += 1

        logger.info(f"Solicitações bem-sucedidas: {success_count}")
        logger.info(f"Erros de solicitação: {error_count}")

        df = pd.DataFrame(json_shipments_list)

        df_orders = df_resultado.copy()

        df_fulfillment_ship = df[["id", "logistic_type"]]
        df_fulfillment_ship = df_fulfillment_ship[
            df_fulfillment_ship["logistic_type"] == "fulfillment"
        ]

        # Extraindo shipping_id
        df_fulfillment_ship["shipping_id"] = df_fulfillment_ship["id"].astype(str)
        df_fulfillment_ship = df_fulfillment_ship.drop(["id"], axis=1)

        # Unindo os DFs. Retorna apenas as linhas em df_orders onde shipping_id == id
        df_res_fulfillment = pd.merge(
            df_orders,
            df_fulfillment_ship,
            left_on="shipping_id",
            right_on="shipping_id",
            how="inner",
        )

        cols = [
            "quantity",
            "item.id",
            "item.seller_sku",
            "date_closed",
            "date_approved",
            "pack_id",
            "status",
            "shipping_id",
            "logistic_type",
        ]
        df_res_fulfillment = df_res_fulfillment[cols]

        return df_res_fulfillment

        # def process_data(self, df_res_fulfillment, df_codes, df_stock, date_from, date_to):
        # Acrescentando prefixo MLB em ml_code
        df_codes["ml_code"] = df_codes["ml_code"].apply(lambda x: "MLB" + str(x))

        # Dfs de orders e produtos unidos por ml_code e ml_sku
        df_filtered = pd.merge(
            df_res_fulfillment,
            df_codes,
            left_on=["item.id", "item.seller_sku"],
            right_on=["ml_code", "ml_sku"],
            how="left",
        )

        # Soma de produtos vendidos
        soma_por_ml_inventory_id = (
            df_filtered.groupby("ml_inventory_id")["quantity"].sum().reset_index()
        )

        # Ordenando por data
        df_stock = df_stock.sort_values(by="created_at", ascending=True)
        df_stock["data"] = df_stock["created_at"].dt.date

        # Criando campo de datas
        df_orders_f = df_filtered.copy()
        df_orders_f["date_approved"] = pd.to_datetime(df_orders_f["date_approved"])
        df_orders_f["data"] = df_orders_f["date_approved"].dt.date
        df_orders_f = df_orders_f.drop(
            ["date_closed", "created_at", "updated_at"], axis=1
        )

        data_inicio = date_from
        data_fim = date_to

        # Filtrando DFs por Período
        def filtrar_por_periodo(df, data_inicio, data_fim):
            return df[(df["data"] >= data_inicio) & (df["data"] <= data_fim)]

        # Filtrar DataFrames com base nas datas definidas
        df_stock_filtrado = filtrar_por_periodo(df_stock, data_inicio, data_fim)
        df_orders_filtrado = filtrar_por_periodo(df_orders_f, data_inicio, data_fim)

        # Cria coluna has_stock, se available_quantity <= 0, has_stock= False
        df_stock_filtrado = df_stock_filtrado.assign(
            has_stock=lambda x: x["available_quantity"] > 0
        )
        df_stock_filtrado = df_stock_filtrado.sort_values(
            by="data", ascending=False
        ).reset_index(drop=True)

        # Contando dias em que produto esteve disponível
        days_available = (
            df_stock_filtrado.groupby("ml_inventory_id")["has_stock"]
            .sum()
            .reset_index()
        )
        days_available = days_available.rename(columns={"has_stock": "days_available"})

        # Unindo DFs
        df_stock_filtrado = df_stock_filtrado.merge(
            days_available, on="ml_inventory_id", how="left"
        )

        return df_stock_filtrado, df_orders_filtrado

        # def process_sales_and_inventory_data(self, df_orders_filtrado, df_stock_filtrado, df_codes):
        # Total de vendas por ml_inventory_id
        total_sales_by_id = (
            df_orders_filtrado.groupby("ml_inventory_id")["quantity"]
            .sum()
            .reset_index()
        )

        # Acrescentando total de vendas ao DF
        df_orders_filtrado = pd.merge(
            df_orders_filtrado, total_sales_by_id, on="ml_inventory_id", how="left"
        )

        df_orders_filtrado.rename(
            columns={"quantity_x": "sales_quantity"}, inplace=True
        )
        df_orders_filtrado.rename(
            columns={"quantity_y": "total_sales_quantity"}, inplace=True
        )
        df_orders_filtrado["sales_quantity"] = df_orders_filtrado[
            "sales_quantity"
        ].fillna(0)
        df_orders_filtrado["total_sales_quantity"] = df_orders_filtrado[
            "total_sales_quantity"
        ].fillna(0)

        # Novos filtros
        df_stock_filtrado_ = df_stock_filtrado.copy()

        indices_recentes = df_stock_filtrado_.groupby("ml_inventory_id")[
            "data"
        ].idxmax()
        df_stock_filtrado_ = df_stock_filtrado_.loc[indices_recentes]
        df_stock_filtrado_ = df_stock_filtrado_.drop(
            ["created_at", "has_stock"], axis=1
        )

        df_orders_filtrado_ = df_orders_filtrado[
            [
                "ml_inventory_id",
                "item.id",
                "ml_code",
                "item.seller_sku",
                "ml_sku",
                "date_approved",
                "sales_quantity",
                "total_sales_quantity",
            ]
        ]
        df_orders_filtrado_ = df_orders_filtrado_.drop_duplicates()

        _df_orders_filtrado_ = df_orders_filtrado_.copy()

        indices_recentes = _df_orders_filtrado_.groupby("ml_inventory_id")[
            "date_approved"
        ].idxmax()

        _df_orders_filtrado_ = _df_orders_filtrado_.loc[indices_recentes]
        _df_orders_filtrado_ = _df_orders_filtrado_.drop(
            ["item.id", "item.seller_sku", "date_approved", "sales_quantity"], axis=1
        )

        df_unido = pd.merge(
            df_stock_filtrado_, _df_orders_filtrado_, on="ml_inventory_id", how="left"
        )
        df_unido["total_sales_quantity"] = df_unido["total_sales_quantity"].fillna(0)

        # Criando um novo DataFrame onde 'ml_code' é NaN
        df_nan_values = df_unido[pd.isnull(df_unido["ml_code"])]

        # Unindo e filtrando DFs
        df_ful = pd.merge(df_nan_values, df_codes, on="ml_inventory_id", how="left")
        df_ful = df_ful.drop(
            [
                "available_quantity",
                "data",
                "days_available",
                "ml_code_x",
                "ml_sku_x",
                "mcenter_id",
                "var_code",
                "ad_title",
                "created_at",
                "updated_at",
                "tiny_id",
                "tiny_sku",
            ],
            axis=1,
        )
        df_ful.rename(
            columns={"ml_code_y": "ml_code", "ml_sku_y": "ml_sku"}, inplace=True
        )

        df_ful = pd.merge(df_unido, df_ful, on="ml_inventory_id", how="left")
        df_ful = df_ful.drop(
            ["data", "ml_code_x", "ml_sku_x", "total_sales_quantity_y"], axis=1
        )
        df_ful.rename(
            columns={"ml_code_y": "ml_code", "ml_sku_y": "ml_sku"}, inplace=True
        )

        df_ful = df_ful.drop_duplicates()

        df_ful = pd.merge(df_codes, df_ful, on="ml_inventory_id", how="left")
        df_ful = df_ful.drop(
            [
                "mcenter_id",
                "var_code",
                "created_at",
                "updated_at",
                "ad_title",
                "ml_code_y",
                "ml_sku_y",
                "tiny_sku",
            ],
            axis=1,
        )
        df_ful = df_ful.drop_duplicates()
        df_ful.rename(
            columns={
                "ml_code_x": "ml_code",
                "ml_sku_x": "ml_sku",
                "total_sales_quantity_x": "total_sales_quantity",
            },
            inplace=True,
        )

        df_unido = df_ful.copy()

        return df_unido

        # def calculate_fulfillment_metrics(self, df_unido, date_from, date_to):
        days = (date_to - date_from).days

        # Calculando total de produtos
        df_unido["total_sales_quantity"] = df_unido["total_sales_quantity"].fillna(0)
        df_unido["days_available"] = df_unido["days_available"].fillna(0)

        # Calculando média de produtos por dias disponíveis
        df_unido["media_prod_days_available"] = (
            df_unido["total_sales_quantity"] / df_unido["days_available"]
        )
        df_unido["media_prod_days_available"] = df_unido[
            "media_prod_days_available"
        ].fillna(0)

        # Calculando produtos a enviar hoje
        df_unido["today_send_fulfillment"] = np.ceil(
            (df_unido["total_sales_quantity"] / df_unido["days_available"])
            - df_unido["available_quantity"]
        )
        df_unido["today_send_fulfillment"] = df_unido["today_send_fulfillment"].fillna(
            0
        )

        # Calculando produtos a enviar no período de x dias
        df_unido["period_send_fulfillment"] = np.ceil(
            (df_unido["total_sales_quantity"] / df_unido["days_available"]) * days
            - df_unido["available_quantity"]
        )
        df_unido["period_send_fulfillment"] = df_unido[
            "period_send_fulfillment"
        ].fillna(0)

        # Filtrando DFs onde o produto a ser enviado é maior ou menor ou igual a 0
        df_fulfillment_greater_zero = df_unido[df_unido["period_send_fulfillment"] > 0]
        df_fulfillment_less_one = df_unido[df_unido["period_send_fulfillment"] <= 0]

        return df_unido, df_fulfillment_greater_zero, df_fulfillment_less_one

        # def filter_today_fulfillment(self, df_tiny_stock, df_fulfillment_greater_zero):
        # Obtém a data de hoje
        data_de_hoje = pd.to_datetime("today").date()

        # Filtra apenas as linhas onde 'created_at' é igual à data de hoje
        df_hoje = df_tiny_stock[df_tiny_stock["created_at"].dt.date == data_de_hoje]

        # Filtra as linhas que estão em df_fulfillment_greater_zero
        df_resultado = df_hoje[
            df_hoje["tiny_id"].isin(df_fulfillment_greater_zero["tiny_id"])
        ]

        # Faz um left join entre df_fulfillment_greater_zero e df_resultado
        df_resultado = pd.merge(
            df_fulfillment_greater_zero, df_resultado, on="tiny_id", how="left"
        )

        return df_resultado

    def insert_orders_data(self, df_orders):
        logger.info("Salvando informações na base de dados")

        try:
            conn = psycopg2.connect(**db_config)
            cursor = conn.cursor()

            for index, row in df_orders.iterrows():
                insert_query = sql.SQL(
                    "INSERT INTO ml_orders_hist (ml_code,category_id,variation_id,seller_sku,pack_id,quantity,title,order_status,payment_status,shipping_id,date_approved,date_closed) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                )
                cursor.execute(
                    insert_query,
                    (
                        row["ml_code"],
                        row["category_id"],
                        row["variation_id"],
                        row["seller_sku"],
                        row["pack_id"],
                        row["quantity"],
                        row["title"],
                        row["order_status"],
                        row["payment_status"],
                        row["shipping_id"],
                        row["date_approved"],
                        row["date_closed"],
                    ),
                )

            conn.commit()
            logger.info("Dados inseridos com sucesso!")

        except psycopg2.Error as e:
            logger.error(f"Erro do psycopg2 ao inserir dados: {e}")

        except Exception as e:
            logger.error(f"Erro ao inserir dados: {e}")

        finally:
            # Feche o cursor e a conexão mesmo em caso de erro
            if cursor:
                cursor.close()
            if conn:
                conn.close()


# if __name__ == "__main__":
#     start_prog = time.time()  # Registra o inicio da aplicação

#     loader = MeLiLoader(db_config, ACCESS_TOKEN)
#     df_fulfillment = loader.get_fulfillment()
#     loader.insert_fulfillment_db(df_fulfillment)

#     print(df_fulfillment)

#     end_prog = time.time()  # Registra o tempo depois de toda aplicação
#     elapsed_time = end_prog - start_prog  # Calcula o tempo decorrido
#     logger.info(f"Tempo Total do processo: {elapsed_time / 60} minutos")
