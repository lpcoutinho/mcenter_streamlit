# Importando bibliotecas
import json
import os
import re
import time

import requests
import streamlit as st
from dotenv import find_dotenv, load_dotenv


# @st.cache_data(ttl=3600)
def get_account_data():
    load_dotenv(override=True)
    ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

    url = "https://api.mercadolibre.com/users/me"

    payload = {}
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}

    response = requests.get(url, headers=headers, data=payload)
    return response.text


# account_data = get_account_data(ACCESS_TOKEN)
account_data = get_account_data()


st.json(account_data)

# # Encontre a posição do início e fim do campo "id"
# start_pos = response.find('"id"')  # Encontra onde começa "id"
# end_pos = response.find(',', start_pos)  # Encontra onde termina "id"

# # Extraia o valor de "id"
# seller_id = response[start_pos:end_pos].split(":")[1]
# seller_id = seller_id.strip().strip('"')
