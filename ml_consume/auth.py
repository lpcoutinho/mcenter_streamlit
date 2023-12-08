import json
import os

import requests
from dotenv import find_dotenv, load_dotenv
from loguru import logger


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
        
        logger.info("env_path: ",env_path)
        
        with open(env_path, "r") as file:
            lines = file.readlines()

        logger.info("Atualizando ACCESS_TOKEN")

        with open(env_path, "w") as file:
            for line in lines:
                if line.startswith("ACCESS_TOKEN="):
                    line = f"ACCESS_TOKEN='{access_token}'\n"
                file.write(line)

        logger.info("Atualizando REFRESH_TOKEN")

        with open(env_path, "w") as file:
            for line in lines:
                if line.startswith("REFRESH_TOKEN="):
                    line = f"REFRESH_TOKEN='{refresh_token}'\n"
                file.write(line)

    except Exception as e:
        logger.error(f"Error: {str(e)}")

    logger.info(f"Novo access_token: {access_token}")
    logger.info(f"Novo refresh_token: {refresh_token}")
refresh_tokens()
