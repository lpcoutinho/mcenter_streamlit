import io
import os

import altair as alt
import pandas as pd
import psycopg2
import streamlit as st
from dotenv import load_dotenv

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

st.set_page_config(page_title="Produtos - Tiny", page_icon="", layout="wide")

# Initialize connection.
# conn = st.connection("postgresql", type="sql")

# # @st.cache_data
# df = conn.query("SELECT * FROM tiny_products;", ttl="1m")


def fetch_data(query):
    conn = psycopg2.connect(**db_config)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


# Consulta SQL
query = "SELECT * FROM tiny_products;"

# Recupera os dados usando a função fetch_data
df = fetch_data(query)

st.header("Produtos")
st.caption("Tabela de produtos")
st.dataframe(df, use_container_width=True)


@st.cache_data
def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    excel_buffer = io.BytesIO()
    df.to_excel(excel_writer=excel_buffer, index=False)
    excel_buffer.seek(0)  # Move the buffer position to the beginning
    return excel_buffer


excel_buffer = convert_df(df)

st.download_button(
    label="Download dos Dados",
    data=excel_buffer.read(),
    file_name="produtos.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)
