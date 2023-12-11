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

REFRESH_TOKEN_MCENTER = os.getenv("REFRESH_TOKEN_MCENTER")
CLIENT_ID_MCENTER = os.getenv("CLIENT_ID_MCENTER")
SECRET_KEY_MCENTER = os.getenv("SECRET_KEY_MCENTER")
REDIRECT_URI_MCENTER = os.getenv("REDIRECT_URI_MCENTER")
REFRESH_TOKEN_BUENOSHOPS = os.getenv("REFRESH_TOKEN_BUENOSHOPS")
CLIENT_ID_BUENOSHOPS = os.getenv("CLIENT_ID_BUENOSHOPS")
SECRET_KEY_BUENOSHOPS = os.getenv("SECRET_KEY_BUENOSHOPS")
REDIRECT_URI_BUENOSHOPS = os.getenv("REDIRECT_URI_BUENOSHOPS")
REFRESH_TOKEN_MUSICALCRIS = os.getenv("REFRESH_TOKEN_MUSICALCRIS")
CLIENT_ID_MUSICALCRIS = os.getenv("CLIENT_ID_MUSICALCRIS")
SECRET_KEY_MUSICALCRIS = os.getenv("SECRET_KEY_MUSICALCRIS")
REDIRECT_URI_MUSICALCRIS = os.getenv("REDIRECT_URI_MUSICALCRIS")

st.header("Capturando Access Token do ML")
auth_link_mcenter = f"https://auth.mercadolivre.com.br/authorization?response_type=code&client_id={CLIENT_ID_MCENTER}&redirect_uri={REDIRECT_URI_MCENTER}"

f"""
Clique no link: 

{auth_link_mcenter}
"""

# Adiciona um campo de entrada de texto no Streamlit
user_input_mcenter = st.text_input("Digite a url gerado a partir do link:")

auth_link_buenoshops = f"https://auth.mercadolivre.com.br/authorization?response_type=code&client_id={CLIENT_ID_BUENOSHOPS}&redirect_uri={REDIRECT_URI_BUENOSHOPS}"

f"""
Clique no link: 

{auth_link_buenoshops}
"""

user_input_buenoshops = st.text_input("Digite a url gerado a partir do link de Buenos Shop:")

auth_link_musicalcris = f"https://auth.mercadolivre.com.br/authorization?response_type=code&client_id={CLIENT_ID_MUSICALCRIS}&redirect_uri={REDIRECT_URI_MUSICALCRIS}"

f"""
Clique no link: 

{auth_link_musicalcris}
"""

user_input_musicalcris = st.text_input("Digite a url gerado a partir de Musical Cris:")


# Exibe o texto inserido pelo usuário
st.write("Você digitou:", user_input_mcenter)

# Cole aqui o url gerado após a autorização
url_mcenter = user_input_mcenter
url_buenoshops = user_input_buenoshops
url_musicalcris = user_input_musicalcris

match_mcenter = re.search(r"code=(.*)", url_mcenter)
match_buenoshops = re.search(r"code=(.*)", url_buenoshops)
match_musicalcris = re.search(r"code=(.*)", url_musicalcris)

if match_mcenter:
    code_mcenter = match_mcenter.group(1)
    print(code_mcenter)
elif match_buenoshops:
    code_buenoshops = match_buenoshops.group(1)
    print(code_buenoshops)
elif match_musicalcris:
    code_musicalcris = match_musicalcris.group(1)
    print(code_musicalcris)
else:
    print("Nenhum valor encontrado entre 'code=' e '&state=' na string da URL.")

### Access Token ###
url = "https://api.mercadolibre.com/oauth/token"

payload_mcenter = f"grant_type=authorization_code&client_id={CLIENT_ID_MCENTER}&client_secret={SECRET_KEY_MCENTER}&code={code_mcenter}&redirect_uri={REDIRECT_URI_MCENTER}"
payload_buenoshops = f"grant_type=authorization_code&client_id={CLIENT_ID_BUENOSHOPS}&client_secret={SECRET_KEY_BUENOSHOPS}&code={code_buenoshops}&redirect_uri={REDIRECT_URI_BUENOSHOPS}"
payload_musicalcris = f"grant_type=authorization_code&client_id={CLIENT_ID_MUSICALCRIS}&client_secret={SECRET_KEY_MUSICALCRIS}&code={code_musicalcris}&redirect_uri={REDIRECT_URI_MUSICALCRIS}"

headers = {
    "accept": "application/json",
    "content-type": "application/x-www-form-urlencoded",
}

response_mcenter = requests.request("POST", url, headers=headers, data=payload_mcenter)
response_buenoshops = requests.request("POST", url, headers=headers, data=payload_buenoshops)
response_musicalcris = requests.request("POST", url, headers=headers, data=payload_musicalcris)

print(response_mcenter.text)
print(response_buenoshops.text)
print(response_musicalcris.text)

json_data_mcenter = response_mcenter.text
data_mcenter = json.loads(json_data_mcenter)

json_data_buenoshops = response_buenoshops.text
data_buenoshops = json.loads(json_data_buenoshops)

json_data_musicalcris = response_musicalcris.text
data_musicalcris = json.loads(json_data_musicalcris)

access_token_mcenter = data_mcenter.get("access_token")
refresh_token_mcenter = data_mcenter.get("refresh_token")

access_token_buenoshops = data_buenoshops.get("access_token")
refresh_token_buenoshops = data_buenoshops.get("refresh_token")

access_token_musicalcris = data_musicalcris.get("access_token")
refresh_token_musicalcris = data_musicalcris.get("refresh_token")


if access_token_mcenter:
    print("access_token_mcenter: ", access_token_mcenter)
if access_token_buenoshops:
    print("access_token_buenoshops: ", access_token_buenoshops)
if access_token_musicalcris:
    print("access_token_musicalcris: ", access_token_musicalcris)
else:
    print("O campo 'access_token' não foi encontrado em nenhum JSON.")

if refresh_token_mcenter:
    print("refresh_token_mcenter: ", refresh_token_mcenter)
if refresh_token_buenoshops:
    print("refresh_token_buenoshops: ", refresh_token_buenoshops)
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
    if lines[i].startswith("ACCESS_TOKEN_MCENTER="):
        lines[i] = f"ACCESS_TOKEN_MCENTER='{access_token_mcenter}'\n"
    elif lines[i].startswith("REFRESH_TOKEN_MCENTER="):
        lines[i] = f"REFRESH_TOKEN_MCENTER='{refresh_token_mcenter}'\n"
    if lines[i].startswith("ACCESS_TOKEN_BUENOSHOPS="):
        lines[i] = f"ACCESS_TOKEN_BUENOSHOPS='{access_token_buenoshops}'\n"
    elif lines[i].startswith("REFRESH_TOKEN_BUENOSHOPS="):
        lines[i] = f"REFRESH_TOKEN_BUENOSHOPS='{refresh_token_buenoshops}'\n"
    if lines[i].startswith("ACCESS_TOKEN_MUSICALCRIS="):
        lines[i] = f"ACCESS_TOKEN_MUSICALCRIS='{access_token_musicalcris}'\n"
    elif lines[i].startswith("REFRESH_TOKEN_MUSICALCRIS="):
        lines[i] = f"REFRESH_TOKEN_MUSICALCRIS='{refresh_token_musicalcris}'\n"

with open(env_path, "w") as file:
    file.writelines(lines)

st.write("access_token_mcenter:", access_token_mcenter)
st.write("refresh_token_mcenter:", refresh_token_mcenter)

st.write("access_token_buenoshops", access_token_buenoshops)
st.write("refresh_token_buenoshops:", refresh_token_buenoshops)

st.write("access_token_musicalcris", access_token_musicalcris)
st.write("refresh_token_musicalcris:", refresh_token_musicalcris)