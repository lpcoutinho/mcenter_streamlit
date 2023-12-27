import io
import os

import pandas as pd
import psycopg2
import streamlit as st
from dotenv import load_dotenv
from loguru import logger
from sqlalchemy import create_engine

logger.add(
    "Data/Output/Log/mcenter_ful_x_tiny.log",
    rotation="10 MB",
    format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
)

load_dotenv(override=True)

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

conn_db = psycopg2.connect(
    host=HOST, database=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD
)

cursor = conn_db.cursor()

st.set_page_config(
    page_title="MCENTER Fullfillment x Tiny", page_icon="📦", layout="wide"
)

conn_db_sqlalchemy = create_engine(
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{HOST}/{POSTGRES_DB}"
)

table = "tiny_fulfillment_mcenter"

query = f"SELECT * FROM {table};"
df = pd.read_sql_query(query, conn_db_sqlalchemy)
df["Quantidade do item"] = df["Quantidade do item"].fillna(0)
df["Quantidade do item"] = df["Quantidade do item"].astype(int)


@st.cache_data
def convert_df(df):
    excel_buffer = io.BytesIO()
    df.to_excel(excel_writer=excel_buffer, index=False)
    excel_buffer.seek(0)
    return excel_buffer


excel_buffer = convert_df(df)

st.sidebar.header("Formulário de Busca")
parametro_busca = st.sidebar.selectbox(
    "Selecione o parâmetro de busca:",
    ["inventory_id", "ml_code", "SKU", "ID Tiny", "SKU Tiny"],
)
valor_busca = st.sidebar.text_input(f"Digite o valor de {parametro_busca}:")

# Usando st.session_state para manter o estado das variáveis
if "resultado_busca_mcenter" not in st.session_state:
    st.session_state.resultado_busca_mcenter = pd.DataFrame()

# Filtrar o DataFrame com base na busca
if st.sidebar.button("Buscar Dados"):
    if valor_busca:
        try:
            valor_busca = valor_busca  # Converter para inteiro se possível
        except ValueError:
            pass  # Ignorar se não for um número inteiro

        st.session_state.resultado_busca_mcenter = df[
            df[parametro_busca] == valor_busca
        ]
        st.header("Resultados da Busca")
        st.dataframe(st.session_state.resultado_busca_mcenter)
    else:
        st.warning("Digite um valor para realizar a busca.")

# Display Options for CRUD Operations
option = st.sidebar.selectbox(
    "Select an Operation",
    ("Ler Tabela", "Atualizar Dados", "Inserir Novos Dados", "Deletar Dados"),
)

if option == "Atualizar Dados":
    st.subheader("Atualizar Dados")
    for index, row in st.session_state.resultado_busca_mcenter.iterrows():
        st.write(
            f"Valor para inventory_id: {row['inventory_id']}", value=row["inventory_id"]
        )
        st.write(f"Valor para ml_code: {row['ml_code']}", value=row["ml_code"])
        st.write(f"Valor para SKU: {row['SKU']}", value=row["SKU"])
        st.write(
            f"Valor para Título do anúncio: {row['Título do anúncio']}",
            key=f"title_{index}",
            value=row["Título do anúncio"],
        )
        st.write(
            f"Valor para tipo do produto: {row['Tipo de produto']}",
            key=f"tipo_{index}",
            value=row["Tipo de produto"],
        )
        tiny_id = st.text_input(
            f"Valor para tiny_id:", key=f"tiny_id_{index}", value=row["ID Tiny"]
        )
        tiny_sku = st.text_input(
            f"Valor para tiny_sku:", key=f"tiny_sku_{index}", value=row["SKU Tiny"]
        )
        qtd_item = st.text_input(
            f"Valor para qtd_item:",
            key=f"qtd_item_{index}",
            value=row["Quantidade do item"],
        )

        if st.button("Atualizar", key=f"update_{index}"):
            update_query = f'UPDATE tiny_fulfillment_mcenter SET "Título do anúncio" = %s, "ID Tiny" = %s, "SKU Tiny" = %s, "Quantidade do item" = %s WHERE inventory_id = %s AND "ID Tiny"= %s'
            cursor.execute(
                update_query,
                (
                    row["Título do anúncio"],
                    tiny_id,
                    tiny_sku,
                    qtd_item,
                    row["inventory_id"],
                    row["ID Tiny"],
                ),
            )
            conn_db.commit()
            st.success("Dados atualizados com sucesso!!!")

elif option == "Deletar Dados":
    st.subheader("Deletar Dados")

    for index, row in st.session_state.resultado_busca_mcenter.iterrows():
        st.write(
            f"Valor para inventory_id: {row['inventory_id']}", value=row["inventory_id"]
        )
        st.write(f"Valor para ml_code: {row['ml_code']}", value=row["ml_code"])
        st.write(f"Valor para SKU: {row['SKU']}", value=row["SKU"])
        st.write(
            f"Valor para Título do anúncio: {row['Título do anúncio']}",
            key=f"title_{index}",
            value=row["Título do anúncio"],
        )
        st.write(
            f"Valor para tiny_id: {row['ID Tiny']}",
            key=f"tiny_id_{index}",
            value=row["ID Tiny"],
        )
        st.write(
            f"Valor para tiny_sku: {row['SKU Tiny']}",
            key=f"tiny_sku_{index}",
            value=row["SKU Tiny"],
        )
        st.write(
            f"Valor para qtd_item: {row['Quantidade do item']}",
            key=f"qtd_item_{index}",
            value=row["Quantidade do item"],
        )

        if st.button("Deletar", key=f"delete_{index}"):
            delete_query = f'DELETE FROM tiny_fulfillment_mcenter WHERE inventory_id = %s AND "ID Tiny"= %s;'
            cursor.execute(delete_query, (row["inventory_id"], row["ID Tiny"]))
            conn_db.commit()
            st.success("Dados deletados com sucesso!!!")

elif option == "Inserir Novos Dados":
    st.subheader("Inserir Novos Dados")

    inventory_id = st.text_input("Valor para inventory_id:")
    ml_code = st.text_input("Valor para ml_code:")
    sku = st.text_input("Valor para SKU:")
    title = st.text_input("Valor para Título do anúncio:")
    tiny_id = st.text_input("Valor para tiny_id:")
    tiny_sku = st.text_input("Valor para tiny_sku:")

    qtd_item_input = st.text_input("Valor para quantidade do item:")
    try:
        qtd_item = int(qtd_item_input)
    except ValueError:
        st.error(
            "A quantidade do item deve ser um número. Por favor, digite novamente."
        )

    tipo = st.text_input("Valor para Tipo de produto:")

    if st.button("Inserir novos dados"):
        insert_query = f'INSERT INTO {table}(inventory_id, ml_code, "SKU", "Título do anúncio", "ID Tiny", "SKU Tiny", "Quantidade do item", "Tipo de produto") VALUES (%s, %s, %s, %s, %s, %s, %s, %s);'
        cursor.execute(
            insert_query,
            (inventory_id, ml_code, sku, title, tiny_id, tiny_sku, qtd_item, tipo),
        )
        conn_db.commit()

        st.success("Dados inseridos com sucesso!!!")

elif option == "Ler Tabela":
    st.header("MCENTER: Relação Fulfillment x Tiny")
    st.caption("Tabela de itens")
    st.dataframe(df, use_container_width=True)

# st.header("MCENTER: Relação Fulfillment x Tiny")
# st.caption("Tabela de itens")
# st.dataframe(df, use_container_width=True)
