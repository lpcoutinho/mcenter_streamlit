# Importando bibliotecas
import json
import os
import re
import time

import requests
from dotenv import find_dotenv, load_dotenv

import streamlit as st

load_dotenv()
# load_dotenv(find_dotenv())

CLIENT_ID = os.getenv("CLIENT_ID")
SCRET_KEY = os.getenv("SCRET_KEY")
REDIRECT_URI = os.getenv("REDIRECT_URI")

start_prog = time.time()

clientId = CLIENT_ID
secretKey = SCRET_KEY
redirectURI = REDIRECT_URI

st.header("Capturando Access Token do ML")
auth_link = f"https://auth.mercadolivre.com.br/authorization?response_type=code&client_id={clientId}&redirect_uri={redirectURI}"

f"""
Clique no link {auth_link}
"""

# Adiciona um campo de entrada de texto no Streamlit
user_input = st.text_input("Digite a url gerado a partir do link:")

# Exibe o texto inserido pelo usuário
st.write("Você digitou:", user_input)

# Cole aqui o url gerado após a autorização
url = user_input

match = re.search(r"code=(.*)", url)

if match:
    code = match.group(1)
    print(code)
else:
    print("Nenhum valor encontrado entre 'code=' e '&state=' na string da URL.")

### Refresh Token
url = "https://api.mercadolibre.com/oauth/token"

payload = f"grant_type=authorization_code&client_id={clientId}&client_secret={secretKey}&code={code}&redirect_uri={redirectURI}"
headers = {
    "accept": "application/json",
    "content-type": "application/x-www-form-urlencoded",
}

response = requests.request("POST", url, headers=headers, data=payload)

json_data = response.text
data = json.loads(json_data)

refresh_token = data.get("refresh_token")

if refresh_token:
    print("refresh_token: ", refresh_token)
else:
    print("O campo 'refresh_token' não foi encontrado no JSON.")

### Access token
url = "https://api.mercadolibre.com/oauth/token"

payload = f"grant_type=refresh_token&client_id={clientId}&client_secret={secretKey}&refresh_token={refresh_token}"
headers = {
    "accept": "application/json",
    "content-type": "application/x-www-form-urlencoded",
}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)

json_data = response.text
data = json.loads(json_data)

access_token = data.get("access_token")

# Atualiza o arquivo .env com o novo ACCESS_TOKEN
env_path = find_dotenv()
with open(env_path, "r") as file:
    lines = file.readlines()

with open(env_path, "w") as file:
    for line in lines:
        if line.startswith("ACCESS_TOKEN="):
            line = f"ACCESS_TOKEN='{access_token}'\n"
        file.write(line)

st.write("access_token:", access_token)
