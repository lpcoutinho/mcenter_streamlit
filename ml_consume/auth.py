import json
import os

import requests
from dotenv import find_dotenv, load_dotenv
from loguru import logger

load_dotenv()

REFRESH_TOKEN_MCENTER = os.getenv("REFRESH_TOKEN_MCENTER")
CLIENT_ID_MCENTER = os.getenv("CLIENT_ID_MCENTER")
SECRET_KEY_MCENTER = os.getenv("SECRET_KEY_MCENTER")
REFRESH_TOKEN_BUENOSHOPS = os.getenv("REFRESH_TOKEN_BUENOSHOPS")
CLIENT_ID_BUENOSHOPS = os.getenv("CLIENT_ID_BUENOSHOPS")
SECRET_KEY_BUENOSHOPS = os.getenv("SECRET_KEY_BUENOSHOPS")
REFRESH_TOKEN_MUSICALCRIS = os.getenv("REFRESH_TOKEN_MUSICALCRIS")
CLIENT_ID_MUSICALCRIS = os.getenv("CLIENT_ID_MUSICALCRIS")
SECRET_KEY_MUSICALCRIS = os.getenv("SECRET_KEY_MUSICALCRIS")


def update_tokens(env_path, access_token, refresh_token):
    logger.info("Atualizando ACCESS_TOKEN e REFRESH_TOKEN")

    if not os.path.exists(env_path):
        logger.error(f"Arquivo de configurações não encontrado.")
        return

    with open(env_path, "r") as file:
        lines = file.readlines()

    # Atualiza ACCESS_TOKEN e REFRESH_TOKEN
    for i in range(len(lines)):
        if lines[i].startswith("ACCESS_TOKEN_MCENTER="):
            lines[i] = f"ACCESS_TOKEN_MCENTER='{access_token}'\n"
        elif lines[i].startswith("REFRESH_TOKEN_MCENTER="):
            lines[i] = f"REFRESH_TOKEN_MCENTER='{refresh_token}'\n"
        if lines[i].startswith("ACCESS_TOKEN_BUENOSHOPS="):
            lines[i] = f"ACCESS_TOKEN_BUENOSHOPS='{access_token}'\n"
        elif lines[i].startswith("REFRESH_TOKEN_BUENOSHOPS="):
            lines[i] = f"REFRESH_TOKEN_BUENOSHOPS='{refresh_token}'\n"
        if lines[i].startswith("ACCESS_TOKEN_MUSICALCRIS="):
            lines[i] = f"ACCESS_TOKEN_MUSICALCRIS='{access_token}'\n"
        elif lines[i].startswith("REFRESH_TOKEN_MUSICALCRIS="):
            lines[i] = f"REFRESH_TOKEN_MUSICALCRIS='{refresh_token}'\n"
                
    with open(env_path, "w") as file:
        file.writelines(lines)

    logger.info("Tokens atualizados com sucesso.")


def refresh_tokens(CLIENT_ID,SECRET_KEY,REFRESH_TOKEN):
    try:
        logger.add(
            "Data/Output/Log/auth.log",
            rotation="10 MB",
            format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
        )

        # load_dotenv()

        # CLIENT_ID = os.getenv("CLIENT_ID")
        # SCRET_KEY = os.getenv("SCRET_KEY")
        # REDIRECT_URI = os.getenv("REDIRECT_URI")
        # REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")

        logger.info("Gerando novo access_token e refresh_token")

        url = "https://api.mercadolibre.com/oauth/token"
        payload = f"grant_type=refresh_token&client_id={CLIENT_ID}&client_secret={SECRET_KEY}&refresh_token={REFRESH_TOKEN}"
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
        
        logger.info("Atualizando ACCESS_TOKEN e REFRESH_TOKEN")

        if not os.path.exists(env_path):
            logger.error(f"Arquivo de configurações não encontrado.")
            return

        with open(env_path, "r") as file:
            lines = file.readlines()

        # Atualiza ACCESS_TOKEN e REFRESH_TOKEN
        for i in range(len(lines)):
            if lines[i].startswith("ACCESS_TOKEN_MCENTER="):
                lines[i] = f"ACCESS_TOKEN_MCENTER='{access_token}'\n"
            elif lines[i].startswith("REFRESH_TOKEN_MCENTER="):
                lines[i] = f"REFRESH_TOKEN_MCENTER='{refresh_token}'\n"
            if lines[i].startswith("ACCESS_TOKEN_BUENOSHOPS="):
                lines[i] = f"ACCESS_TOKEN_BUENOSHOPS='{access_token}'\n"
            elif lines[i].startswith("REFRESH_TOKEN_BUENOSHOPS="):
                lines[i] = f"REFRESH_TOKEN_BUENOSHOPS='{refresh_token}'\n"
            if lines[i].startswith("ACCESS_TOKEN_MUSICALCRIS="):
                lines[i] = f"ACCESS_TOKEN_MUSICALCRIS='{access_token}'\n"
            elif lines[i].startswith("REFRESH_TOKEN_MUSICALCRIS="):
                lines[i] = f"REFRESH_TOKEN_MUSICALCRIS='{refresh_token}'\n"
                    
        with open(env_path, "w") as file:
            file.writelines(lines)

        logger.info("Tokens atualizados com sucesso.")
            
    except Exception as e:
        logger.error(f"Error: {str(e)}")

    logger.info(f"Novo access_token: {access_token}")
    logger.info(f"Novo refresh_token: {refresh_token}")

refresh_tokens(CLIENT_ID_MCENTER,SECRET_KEY_MCENTER,REFRESH_TOKEN_MCENTER)
refresh_tokens(CLIENT_ID_BUENOSHOPS,SECRET_KEY_BUENOSHOPS,REFRESH_TOKEN_BUENOSHOPS)
refresh_tokens(CLIENT_ID_MUSICALCRIS,SECRET_KEY_MUSICALCRIS,REFRESH_TOKEN_MUSICALCRIS)
