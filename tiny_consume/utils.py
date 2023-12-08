import json

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
