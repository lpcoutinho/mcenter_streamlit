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

match_buenoshops = re.search(r"code=(.*)", url_mcenter)

if match_buenoshops:
    code_buenoshops = match_buenoshops.group(1)
    print(code_buenoshops)
else:
    print("Nenhum valor encontrado entre 'code=' e '&state=' na string da URL.")

### Access Token ###
url = "https://api.mercadolibre.com/oauth/token"

payload_mcenter = f"grant_type=authorization_code&client_id={CLIENT_ID_BUENOSHOPS}&client_secret={SECRET_KEY_BUENOSHOPS}&code={code_buenoshops}&redirect_uri={REDIRECT_URI_BUENOSHOPS}"

headers = {
    "accept": "application/json",
    "content-type": "application/x-www-form-urlencoded",
}

response_buenoshops = requests.request("POST", url, headers=headers, data=payload_mcenter)
print(response_buenoshops.text)

json_data_buenoshops = response_buenoshops.text
data_buenoshops = json.loads(json_data_buenoshops)


access_token_buenoshops = data_buenoshops.get("access_token")
refresh_token_buenoshops = data_buenoshops.get("refresh_token")

if access_token_buenoshops:
    print("access_token_buenoshops: ", access_token_buenoshops)
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

try:
    with open(env_path, "r") as file:
        lines = file.readlines()

    # Atualiza ACCESS_TOKEN e REFRESH_TOKEN
    for i in range(len(lines)):
        if lines[i].startswith("ACCESS_TOKEN_BUENOSHOPS="):
            lines[i] = f"ACCESS_TOKEN_BUENOSHOPS='{access_token_buenoshops}'\n"
        elif lines[i].startswith("REFRESH_TOKEN_BUENOSHOPS="):
            lines[i] = f"REFRESH_TOKEN_BUENOSHOPS='{refresh_token_buenoshops}'\n"

    with open(env_path, "w") as file:
        file.writelines(lines)

    print("Tokens atualizados com sucesso.")
except Exception as e:
    print(f"Error updating tokens: {str(e)}")


st.write("ACCESS_TOKEN_BUENOSHOPS:", access_token_buenoshops)
st.write("REFRESH_TOKEN_BUENOSHOPS:", refresh_token_buenoshops)