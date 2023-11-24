# Importando bibliotecas
import json
import os
import time
from datetime import datetime

import pandas as pd
import psycopg2
import requests
from dotenv import load_dotenv
from loguru import logger
from utils import sendREST

logger.add(
    "Data/Output/Log/tiny_log.log",
    rotation="10 MB",
    format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
)

load_dotenv()

TINY_TOKEN = os.getenv("TINY_TOKEN")
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


# Configurações da API Tiny
token = TINY_TOKEN
tiny_format = "JSON"


class TinyLoader:
    def __init__(self, db_config, token, tiny_format):
        self.db_config = db_config
        self.token = token
        self.tiny_format = tiny_format

    def load_tiny_ids(self):
        logger.info(f"Buscando IDs de Produtos na API Tiny")
        try:
            conn = psycopg2.connect(**self.db_config)
            query = "select tiny_id from tiny_fulfillment;"
            df_tiny_id = pd.read_sql_query(query, conn)
            conn.close()
            logger.info(
                f"Criando DataFrame com IDs de produtos na tabela 'tiny_fulfillment'"
            )
            return df_tiny_id
        except psycopg2.Error as e:
            logger.error(
                f"Ocorreu um erro no psycopg2 ao consultar a tabela tiny_fulfillment: {str(e)}"
            )
            return None

        except Exception as e:
            logger.error(
                f"Ocorreu um erro ao consultar a tabela tiny_fulfillment: {str(e)}"
            )
            return None

    def process_json_list(self, json_list):
        logger.info(f"Processando {json_list}.")
        tiny_stock_df = pd.DataFrame()  # Inicializa DataFrame vazio

        try:
            # Processar cada JSON na lista
            for json_str in json_list:
                logger.info("Carregando lista Json")
                json_data = json.loads(
                    json_str
                )  # Transforma string JSON em objeto Python

                # Extrair a parte "produto" do JSON
                logger.info("Extraindo dados dos produtos da lista Json")
                produto = json_data["retorno"]["produto"]
                depositos = produto["depositos"]

                # Verifica se lista de depósitos está presente
                logger.info("Extraindo dados de depósitos")
                if depositos:
                    # Cria DataFrame temporário a partir da lista de depósitos
                    temp_df = pd.json_normalize(depositos)

                    # Adiciona colunas do nível "produto" ao DataFrame temporário
                    for key, value in produto.items():
                        temp_df[key] = value

                    # Concatena DataFrame temporário ao DataFrame principal
                    logger.info("Construindo DataFrame de Estoque")
                    tiny_stock_df = pd.concat(
                        [tiny_stock_df, temp_df], ignore_index=True
                    )

        except (json.JSONDecodeError, KeyError, TypeError) as e:
            # Lidar com exceções específicas que podem ocorrer durante o processamento do JSON
            logger.error(f"Erro durante o processamento do JSON: {e}")

        except Exception as e:
            # Lidar com outras exceções não previstas
            logger.error(f"Erro inesperado: {e}")

        tiny_stock_df = tiny_stock_df[tiny_stock_df["deposito.empresa"] != "cpmusical"]

        # Reordena colunas
        col_order = [
            "id",
            "nome",
            "codigo",
            "unidade",
            "saldo",
            "saldoReservado",
            "deposito.nome",
            "deposito.desconsiderar",
            "deposito.saldo",
            "deposito.empresa",
        ]

        tiny_stock_df = tiny_stock_df[col_order]

        logger.info("Dataframe construido!")
        return tiny_stock_df

    def save_responses_txt(reponses, output_txt_path):
        try:
            # Escreve cada elemento da lista em uma linha do arquivo
            with open(output_txt_path, "w") as file:
                for response in reponses:
                    file.write(response + "\n")
            logger.info(f"Os dados foram gravados no arquivo: {output_txt_path}")

        except Exception as e:
            logger.error(
                f"Erro ao armazenar resposta da requisição em {output_txt_path}: {e}"
            )

    def verify_json_list(self, json_list):
        lines_search1 = []
        lines_search2 = []

        text_search1 = '"status_processamento":"1"'
        text_search2 = '"status_processamento":"2"'

        for n_line, json_str in enumerate(json_list, start=1):
            json_data = json.loads(json_str)

            if text_search1 in json_str:
                lines_search1.append(n_line)
            if text_search2 in json_str:
                lines_search2.append(n_line)

        if lines_search1 and lines_search2:
            logger.info(f'"{text_search1}" foi encontrado nas linhas: {lines_search1}')
            logger.info(f'"{text_search2}" foi encontrado nas linhas: {lines_search2}')
        elif lines_search1:
            logger.info(f'"{text_search1}" foi encontrado nas linhas: {lines_search1}')
            logger.info(f'"{text_search2}" não foi encontrado em nenhuma linha.')
        elif lines_search2:
            logger.info(f'"{text_search2}" foi encontrado nas linhas: {lines_search2}')
            logger.info(f'"{text_search1}" não foi encontrado em nenhuma linha.')
        else:
            logger.info("Não foi verificado erro de processamento.")

    def save_tiny_stock(self, reponses, output_csv_path):
        try:
            df_tiny_stock = self.process_json_list(
                reponses
            )  # Cria DataFrame de Estoque da Tiny

            # Salva o DataFrame em um arquivo CSV
            logger.info(f"Criando arquivo {output_csv_path}")
            df_tiny_stock.to_csv(output_csv_path, index=False)
            logger.info(f"{output_csv_path} criado com sucesso!")

        except Exception as e:
            logger.error(f"Erro ao construir df_tiny_stock: {e}")

    def insert_tiny_stock_db(self, df, db_config):
        try:
            conn = psycopg2.connect(**db_config)
            cursor = conn.cursor()
            n = 1

            for index, row in df.iterrows():
                logger.info(f"Loop nº: {n}/{df.shape[0]}")
                query = """
                INSERT INTO tiny_stock_hist (tiny_id,nome,sku_tiny,unidade,saldo_reservado,deposito_nome,deposito_desconsiderar,
                deposito_saldo,deposito_empresa)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);
                """

                values = (
                    row["id"],
                    row["nome"],
                    row["codigo"],
                    row["unidade"],
                    row["saldoReservado"],
                    row["deposito.nome"],
                    row["deposito.desconsiderar"],
                    row["deposito.saldo"],
                    row["deposito.empresa"],
                )
                n += 1
                cursor.execute(query, values)

            conn.commit()

            cursor.close()
            conn.close()
            logger.info("Produtos adicionados ao Banco de dados.")
        except Exception as e:
            logger.info(f"Ocorreu um erro: {str(e)}")

    def get_tiny_stock_hist(self, db_config):
        try:
            conn = psycopg2.connect(**db_config)
            sql_query = "SELECT * FROM tiny_stock_hist"
            df_tiny_stock = pd.read_sql(sql_query, conn)
        except psycopg2.Error as e:
            print(f"Erro do psycopg2 ao consultar tiny_stock_hist: {e}")
            df_tiny_stock = pd.DataFrame()  # Retorna um DataFrame vazio em caso de erro
        except Exception as e:
            print(f"Erro ao consultar tiny_stock_hist: {e}")
            df_tiny_stock = pd.DataFrame()  # Retorna um DataFrame vazio em caso de erro
        finally:
            if conn is not None:
                conn.close()

        return df_tiny_stock

    def get_tiny_stock(self):
        df_tiny_id = self.load_tiny_ids()

        df_tiny_id_no_nan = df_tiny_id.dropna(
            subset=["tiny_id"]
        )  # Remove valores não numéricos
        df_tiny_id_no_dup = df_tiny_id_no_nan.drop_duplicates()  # Remove duplicatas

        df_tiny_id = df_tiny_id_no_dup

        # df_tiny_id = df_tiny_id.head(1)

        url = "https://api.tiny.com.br/api2/produto.obter.estoque.php"

        responses = []  # Lista para armazenar os resultados
        num = 0

        for id in df_tiny_id["tiny_id"]:
            logger.info(f"Buscando dados de {id}")
            num += 1
            logger.info(f"Loop: {num}/{df_tiny_id.shape[0]}")
            data = {"token": self.token, "id": id, "formato": self.tiny_format}
            response = sendREST(url, data)
            responses.append(response)

            # Verifica se é múltiplo de 50 para aguardar 1 minuto a cada 50 requisições
            if num % 50 == 0:
                logger.info("Esperando 1 minuto...")
                time.sleep(60)  # Pausa por 1 minuto

        self.verify_json_list(responses)

        df_tiny_id = self.process_json_list(responses)

        self.insert_tiny_stock_db(df_tiny_id, self.db_config)


# if __name__ == "__main__":
#     start_prog = time.time()  # Registra o inicio da aplicação

#     loader = TinyLoader(db_config, token, tiny_format)
#     loader.get_tiny_stock()
#     # main()

#     end_prog = time.time()  # Registra o tempo depois de toda aplicação
#     elapsed_time = end_prog - start_prog  # Calcula o tempo decorrido
#     logger.info(f"Tempo Total do processo: {elapsed_time / 60} minutos")
