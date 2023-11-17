# Importando bibliotecas
import requests
import json
import time
import re
import os
import streamlit as st

from dotenv import load_dotenv, find_dotenv

load_dotenv()
# load_dotenv(find_dotenv())

CLIENT_ID = os.getenv("CLIENT_ID")
SCRET_KEY = os.getenv("SCRET_KEY")
REDIRECT_URI = os.getenv("REDIRECT_URI")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

### Verificando dados da conta
url = "https://api.mercadolibre.com/users/me"

payload = {}
headers = {
  'Authorization': f'Bearer {ACCESS_TOKEN}'
}

response = requests.request("GET", url, headers=headers, data=payload)
response = response.text

st.json(response)

# # Encontre a posição do início e fim do campo "id"
# start_pos = response.find('"id"')  # Encontra onde começa "id"
# end_pos = response.find(',', start_pos)  # Encontra onde termina "id"

# # Extraia o valor de "id"
# seller_id = response[start_pos:end_pos].split(":")[1]
# seller_id = seller_id.strip().strip('"')