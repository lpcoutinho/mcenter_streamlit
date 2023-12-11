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

REFRESH_TOKEN_BUENOSHOPS = os.getenv("REFRESH_TOKEN_BUENOSHOPS")
CLIENT_ID_BUENOSHOPS = os.getenv("CLIENT_ID_BUENOSHOPS")
SECRET_KEY_BUENOSHOPS = os.getenv("SECRET_KEY_BUENOSHOPS")
REDIRECT_URI_BUENOSHOPS = os.getenv("REDIRECT_URI_BUENOSHOPS")

st.header("Capturando Access Token do ML")
auth_link_mcenter = f"https://auth.mercadolivre.com.br/authorization?response_type=code&client_id={CLIENT_ID_BUENOSHOPS}&redirect_uri={REDIRECT_URI_BUENOSHOPS}"

f"""
Clique no link: 

{auth_link_mcenter}
"""

# Adiciona um campo de entrada de texto no Streamlit
user_input_mcenter = st.text_input("Digite a url gerado a partir do link:")

# Exibe o texto inserido pelo usuário
st.write("Você digitou:", user_input_mcenter)

# Cole aqui o url gerado após a autorização
url_mcenter = user_input_mcenter

match_mcenter = re.search(r"code=(.*)", url_mcenter)

if match_mcenter:
    code_mcenter = match_mcenter.group(1)
    print(code_mcenter)
else:
    print("Nenhum valor encontrado entre 'code=' e '&state=' na string da URL.")

### Access Token ###
url = "https://api.mercadolibre.com/oauth/token"

payload_mcenter = f"grant_type=authorization_code&client_id={CLIENT_ID_BUENOSHOPS}&client_secret={SECRET_KEY_BUENOSHOPS}&code={code_mcenter}&redirect_uri={REDIRECT_URI_BUENOSHOPS}"

headers = {
    "accept": "application/json",
    "content-type": "application/x-www-form-urlencoded",
}

response_mcenter = requests.request("POST", url, headers=headers, data=payload_mcenter)
print(response_mcenter.text)

json_data_mcenter = response_mcenter.text
data_mcenter = json.loads(json_data_mcenter)


access_token_mcenter = data_mcenter.get("access_token")
refresh_token_buenoshops = data_mcenter.get("refresh_token")

if access_token_mcenter:
    print("access_token_mcenter: ", access_token_mcenter)
else:
    print("O campo 'access_token' não foi encontrado em nenhum JSON.")

if refresh_token_buenoshops:
    print("refresh_token_buenoshops: ", refresh_token_buenoshops)
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
    if lines[i].startswith("ACCESS_TOKEN_MCENTER="):
        lines[i] = f"ACCESS_TOKEN_MCENTER='{access_token_mcenter}'\n"
    elif lines[i].startswith("REFRESH_TOKEN_BUENOSHOPS="):
        lines[i] = f"REFRESH_TOKEN_BUENOSHOPS='{refresh_token_buenoshops}'\n"
    
with open(env_path, "w") as file:
    file.writelines(lines)

st.write("access_token_mcenter:", access_token_mcenter)
st.write("REFRESH_TOKEN_BUENOSHOPS:", refresh_token_buenoshops)