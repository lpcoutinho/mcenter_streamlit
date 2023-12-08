import json
import os

import requests
from dotenv import find_dotenv, load_dotenv
from loguru import logger

def update_tokens(env_path, access_token, refresh_token):
    logger.info("Atualizando ACCESS_TOKEN e REFRESH_TOKEN")

    if not os.path.exists(env_path):
        logger.error(f"Arquivo de configurações não encontrado.")
        return

    with open(env_path, "r") as file:
        lines = file.readlines()

    # Atualiza ACCESS_TOKEN e REFRESH_TOKEN
    for i in range(len(lines)):
        if lines[i].startswith("ACCESS_TOKEN="):
            lines[i] = f"ACCESS_TOKEN='{access_token}'\n"
        elif lines[i].startswith("REFRESH_TOKEN="):
            lines[i] = f"REFRESH_TOKEN='{refresh_token}'\n"

    with open(env_path, "w") as file:
        file.writelines(lines)

    logger.info("Tokens atualizados com sucesso.")


def refresh_tokens():
    try:
        logger.add(
            "Data/Output/Log/auth.log",
            rotation="10 MB",
            format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
        )

        load_dotenv()

        CLIENT_ID = os.getenv("CLIENT_ID")
        SCRET_KEY = os.getenv("SCRET_KEY")
        REDIRECT_URI = os.getenv("REDIRECT_URI")
        REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")

        logger.info("Gerando novo access_token e refresh_token")

        url = "https://api.mercadolibre.com/oauth/token"
        payload = f"grant_type=refresh_token&client_id={CLIENT_ID}&client_secret={SCRET_KEY}&refresh_token={REFRESH_TOKEN}"
        headers = {
            "accept": "application/json",
            "content-type": "application/x-www-form-urlencoded",
        }

        response = requests.post(url, headers=headers, data=payload)
        response.raise_for_status()

        logger.info(response.text)

        json_data = response.text
        data = json.loads(json_data)

        access_token = data.get("access_token")
        refresh_token = data.get("refresh_token")

        logger.info("Procurando arquivo de configurações")

        env_path = find_dotenv()
        
        update_tokens(env_path, access_token, refresh_token)

    except Exception as e:
        logger.error(f"Error: {str(e)}")

    logger.info(f"Novo access_token: {access_token}")
    logger.info(f"Novo refresh_token: {refresh_token}")

refresh_tokens()
