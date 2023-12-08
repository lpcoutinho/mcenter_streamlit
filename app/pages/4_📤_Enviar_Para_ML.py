import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import psycopg2
import streamlit as st
from dotenv import load_dotenv
from pandas import json_normalize
from ml_consume.calculando_envio_fulfillment import send_fulfillment

load_dotenv()

ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
# HOST = os.getenv("HOST")
# POSTGRES_DB = os.getenv("POSTGRES_DB")
# POSTGRES_USER = os.getenv("POSTGRES_USER")
# POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Interface do Streamlit
st.title("Produtos a enviar ao Fulfillment")

# Selecionar data da pesquisa
st.header("Defina o período da consulta", divider="grey")
date_from = st.date_input(label="Data inicial")
date_t = st.date_input(label="Data final")
date_to = date_t + timedelta(days=1)  # + 1 dia para pegar a data atual no DB

st.caption(f"Perído a consultar vai de {date_from} até {date_t}")

# date_from = st.text_input(label='Data inicial, digite apenas números',placeholder='01112023',max_chars=8)
# date_to = st.text_input(label='Data final, digite apenas números',placeholder='30112023')

st.header("Dias para o cálculo de envio", divider="grey")
input_days = st.number_input(
    label="Enviar produtos para os próximos x dias", step=1, value=30
)


# ### Período a consultar

# Defina as datas de início e fim desejadas
data_inicio = datetime(2023, 11, 1).date()
data_fim = datetime(2023, 12, 8).date()
data_fim = data_fim + timedelta(days=1)  # + 1 dia para pegar a data atual no DB
print(data_fim)

# Botão para iniciar a consulta
if st.button("Iniciar Consulta"):
    # Exibe uma mensagem enquanto a consulta está em andamento
    mensagem_aguarde = st.warning("Aguarde, a consulta está em andamento...")

    # # Remove a mensagem de aviso e exibe os resultados
    mensagem_aguarde.empty()
    st.success("Consulta concluída com sucesso!")

    dfx, df_sold_zero, df_sold = send_fulfillment()

    st.dataframe(dfx, use_container_width=True)
    st.write(len(dfx), len(df_sold_zero), len(df_sold))
    st.header("Produtos sem estoque no período", divider="grey")
    st.dataframe(df_sold_zero, use_container_width=True)
    st.header("Produtos sold", divider="grey")
    st.dataframe(df_sold, use_container_width=True)


