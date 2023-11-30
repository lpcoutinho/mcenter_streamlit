import io
import os

import altair as alt
import pandas as pd
import psycopg2
import streamlit as st
from dotenv import load_dotenv
from psycopg2 import sql
from streamlit_js_eval import streamlit_js_eval

load_dotenv()

HOST = os.getenv("HOST")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Informações de conexão com o banco de dados PostgreSQL
db_config = {
    "host": HOST,
    "database": POSTGRES_DB,
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
}


@st.cache_data
def convert_df(df):
    excel_buffer = io.BytesIO()
    df.to_excel(excel_writer=excel_buffer, index=False)
    excel_buffer.seek(0)
    return excel_buffer


st.set_page_config(page_title="Histórico de Vendas", page_icon="", layout="wide")


# # @st.cache_data()
def fetch_data(query):
    conn = psycopg2.connect(**db_config)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


# Consulta SQL
query = "SELECT * FROM ml_orders_hist;"

# Recupera os dados usando a função fetch_data
df = fetch_data(query)
d_maior = df['date_closed'].max().date()
d_menor = df['date_closed'].min().date()

st.header('Histórico de Vendas')
df = df.drop_duplicates()
st.caption(f"Tamanho do dataframe: {df.shape}")
st.caption(f"Total de vendas: {df.shape[0]} entre {d_menor} e {d_maior}")
st.dataframe(df, use_container_width=True)