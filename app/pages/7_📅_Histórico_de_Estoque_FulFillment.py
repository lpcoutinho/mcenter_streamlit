import io
import os

import altair as alt
import pandas as pd
import psycopg2
import streamlit as st
from dotenv import load_dotenv
from psycopg2 import sql
from streamlit_js_eval import streamlit_js_eval
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

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


st.set_page_config(page_title="Histórico de Estoque Fulfillment", page_icon="", layout="wide")


@st.cache_data()
def fetch_data(query):
    conn = psycopg2.connect(**db_config)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


# Consulta SQL
query = "SELECT * FROM fulfillment_stock_hist;"
# Recupera os dados usando a função fetch_data
df = fetch_data(query)
d_maior = df['created_at'].max().date()
d_menor = df['created_at'].min().date()

st.header('Histórico de Estoque Fulfillment')
df = df.drop_duplicates()
st.caption(f"Tamanho do dataframe: {df.shape}")
st.caption(f"Total de vendas: {df.shape[0]} entre {d_menor} e {d_maior}")
# st.dataframe(df, use_container_width=True)

# Definir intervalo permitido
data_inicio_min = df['created_at'].min()
data_fim_max = df['created_at'].max()

# Calcular um valor padrão dentro do intervalo permitido
# data_padrao = data_inicio_min + (data_fim_max - data_inicio_min) // 2

# Título da página
st.write('Filtre por Data')

# Widget de seleção de data com valor padrão ajustado
data_inicio = st.date_input("Selecione a data de início", min_value=data_inicio_min.date(), max_value=data_fim_max.date(), value=data_inicio_min.date())
data_fim = st.date_input("Selecione a data de fim", min_value=data_inicio_min.date(), max_value=data_fim_max.date(), value=data_fim_max.date())
data_fim = data_fim + timedelta(days=1)

# Converter a coluna created_at para datetime64[ns]
df['created_at'] = pd.to_datetime(df['created_at'])

# Filtrar o DataFrame com base nas datas selecionadas
df_filtrado = df[(df['created_at'] >= pd.to_datetime(data_inicio)) & (df['created_at'] <= pd.to_datetime(data_fim))]

# Exibir DataFrame filtrado
st.write('DataFrame Filtrado:', df_filtrado)

# # Título da página
# st.title('Gráfico de Disponibilidade por ID de Inventário')

# # Criar gráfico de barras
# fig, ax = plt.subplots()
# ax.bar(df['available_quantity'],df['ml_inventory_id'])
# ax.set_xlabel('ID de Inventário')
# ax.set_ylabel('Quantidade Disponível')
# ax.set_title('Disponibilidade por ID de Inventário')

# # Exibir o gráfico no Streamlit
# st.pyplot(fig)

# Reduzir a quantidade de IDs ou pegar os 10 maiores disponíveis
df_top_10 = df.sort_values(by='available_quantity', ascending=False).drop_duplicates('ml_inventory_id').head(10)

# Título da página
st.title('')
st.write('Top 10:', df_top_10)

# Criar gráfico de barras
fig, ax = plt.subplots()
ax.bar(df_top_10['ml_inventory_id'], df_top_10['available_quantity'])
ax.set_xlabel('Inventory_ID')
ax.set_ylabel('Maior Quantidade Já Disponível')
ax.set_title('10 Produtos com Maior Disponibilidade')

# Girar os rótulos do eixo x na vertical
ax.set_xticklabels(df_top_10['ml_inventory_id'], rotation=90)

# Exibir o gráfico no Streamlit
st.pyplot(fig)