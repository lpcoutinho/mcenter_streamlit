import json
import os
import requests
from loguru import logger


def sendREST(url, data, optional_headers=None):
    # - url: a URL para a qual a solicitação será enviada.
    # - data: os dados que serão enviados na solicitação.
    # - optional_headers: cabeçalhos opcionais que podem ser fornecidos. Se não fornecidos, assume-se um dicionário vazio.

    # Verifica se optional_headers não é None e atribua um dicionário vazio, se necessário.
    headers = optional_headers if optional_headers is not None else {}
    logger.info("Verificando headers")

    # Envia uma solicitação POST para a URL especificada com os dados e cabeçalhos fornecidos.
    response = requests.post(url, data=data, headers=headers)
    logger.info("Enviando requisição POST")

    # Se o status da resposta não é 200 (OK).
    if response.status_code != 200:
        # Lança uma exceção com mensagem de erro.
        msg = f"Problema com {url}, Status Code: {response.status_code}"
        raise Exception(logger.error(msg))

    logger.info("Requisição realizada com sucesso!")
    # Retorna o conteúdo da resposta como texto.
    return response.text


def save_csv_df(df, path):
    logger.info(f"Criando arquivo {path}")
    df.to_csv(path, index=False)
    logger.info(f"DataFrame salvos como CSV em {path}")


def save_json_list_to_txt(json_list, output_file):
    with open(output_file, "w") as file:
        for json_data in json_list:
            json.dump(json_data, file)
            file.write("\n")


def condf(df, coluna, valor):
    """
    Consulta um DataFrame com base em uma coluna e valor específicos.

    Parâmetros:
    - df: DataFrame a ser consultado.
    - coluna: Nome da coluna para a condição de consulta.
    - valor: Valor desejado na coluna.

    Retorna:
    Um DataFrame contendo apenas as linhas que atendem à condição.
    """
    resultado = df[df[coluna] == valor]
    return resultado


def condf_date(df, coluna_data, data_pesquisada):
    """
    Consulta um DataFrame com base em uma coluna de datas.

    Parâmetros:
    - df: DataFrame a ser consultado.
    - coluna_data: Nome da coluna de datas.
    - data_pesquisada: Data desejada para a consulta.

    Retorna:
    Um DataFrame contendo apenas as linhas que correspondem à data pesquisada.
    """
    resultado = df[pd.to_datetime(df[coluna_data]).dt.date == data_pesquisada]
    return resultado

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