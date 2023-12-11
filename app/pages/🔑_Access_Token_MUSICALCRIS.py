# Importando bibliotecas
import json
import os
import re
import time

import requests
import streamlit as st
from dotenv import find_dotenv, load_dotenv

load_dotenv()
# load_dotenv(find_dotenv())

REFRESH_TOKEN_MUSICALCRIS = os.getenv("REFRESH_TOKEN_MUSICALCRIS")
CLIENT_ID_MUSICALCRIS = os.getenv("CLIENT_ID_MUSICALCRIS")
SECRET_KEY_MUSICALCRIS = os.getenv("SECRET_KEY_MUSICALCRIS")
REDIRECT_URI_MUSICALCRIS = os.getenv("REDIRECT_URI_MUSICALCRIS")

st.header("Capturando Access Token do ML")

auth_link_musicalcris = f"https://auth.mercadolivre.com.br/authorization?response_type=code&client_id={CLIENT_ID_MUSICALCRIS}&redirect_uri={REDIRECT_URI_MUSICALCRIS}"

f"""
Clique no link: 

{auth_link_musicalcris}
"""

user_input_musicalcris = st.text_input("Digite a url gerado a partir de Musical Cris:")


# Exibe o texto inserido pelo usuário
st.write("Você digitou:", user_input_musicalcris)

url_musicalcris = user_input_musicalcris

match_musicalcris = re.search(r"code=(.*)", url_musicalcris)

if match_musicalcris:
    code_musicalcris = match_musicalcris.group(1)
    print(code_musicalcris)
else:
    print("Nenhum valor encontrado entre 'code=' e '&state=' na string da URL.")

### Access Token ###
url = "https://api.mercadolibre.com/oauth/token"

payload_musicalcris = f"grant_type=authorization_code&client_id={CLIENT_ID_MUSICALCRIS}&client_secret={SECRET_KEY_MUSICALCRIS}&code={code_musicalcris}&redirect_uri={REDIRECT_URI_MUSICALCRIS}"

headers = {
    "accept": "application/json",
    "content-type": "application/x-www-form-urlencoded",
}

response_musicalcris = requests.request("POST", url, headers=headers, data=payload_musicalcris)

print(response_musicalcris.text)

json_data_musicalcris = response_musicalcris.text
data_musicalcris = json.loads(json_data_musicalcris)

access_token_musicalcris = data_musicalcris.get("access_token")
refresh_token_musicalcris = data_musicalcris.get("refresh_token")


if access_token_musicalcris:
    print("access_token_musicalcris: ", access_token_musicalcris)
else:
    print("O campo 'access_token' não foi encontrado em nenhum JSON.")

if refresh_token_musicalcris:
    print("refresh_token_musicalcris: ", refresh_token_musicalcris)
else:
    print("O campo 'refresh_token' não foi encontrado em nenhum no JSON.")


# ### Refresh token ###
# url = "https://api.mercadolibre.com/oauth/token"
# payload = f"grant_type=refresh_token&client_id={clientId}&client_secret={secretKey}&refresh_token={refresh_token}"
# headers = {
#     "accept": "application/json",
#     "content-type": "application/x-www-form-urlencoded",
# }

# response = requests.request("POST", url, headers=headers, data=payload)

# print(response.text)

# json_data = response.text
# data = json.loads(json_data)

# access_token_ = data.get("access_token")
# refresh_token_ = data.get("refresh_token")

env_path = find_dotenv()

with open(env_path, "r") as file:
            lines = file.readlines()

# Atualiza ACCESS_TOKEN e REFRESH_TOKEN
for i in range(len(lines)):
    if lines[i].startswith("ACCESS_TOKEN_MUSICALCRIS="):
        lines[i] = f"ACCESS_TOKEN_MUSICALCRIS='{access_token_musicalcris}'\n"
    elif lines[i].startswith("REFRESH_TOKEN_MUSICALCRIS="):
        lines[i] = f"REFRESH_TOKEN_MUSICALCRIS='{refresh_token_musicalcris}'\n"

with open(env_path, "w") as file:
    file.writelines(lines)

st.write("access_token_musicalcris", access_token_musicalcris)
st.write("refresh_token_musicalcris:", refresh_token_musicalcris)