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


st.set_page_config(page_title="Relação Fulfillment X Tiny", page_icon="", layout="wide")


# # @st.cache_data()
def fetch_data(query):
    conn = psycopg2.connect(**db_config)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


# Consulta SQL
query = "SELECT * FROM tiny_fulfillment;"

# Recupera os dados usando a função fetch_data
df = fetch_data(query)

# # # Initialize connection.
# conn = st.connection("postgresql", type="sql")

# # # @st.cache_data
# # df = conn.query("SELECT * FROM tiny_fulfillment;", ttl="10m")
# df = conn.query("SELECT * FROM tiny_fulfillment;")
# df = pd.DataFrame(df)

cols = ["created_at", "updated_at"]
df.set_index("mcenter_id", inplace=True)

df = df.drop(cols, axis=1)
df = df.sort_values(by="mcenter_id")
st.header("Relação Fulfillment X Tiny")
st.caption("Tabela de produtos relacionados entre Fulfillment e Tiny")
st.dataframe(df, use_container_width=True)

excel_buffer = convert_df(df)

st.download_button(
    label="Download dos Dados",
    data=excel_buffer.read(),
    file_name="tiny_fulfillment.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)


# Função para atualizar o banco de dados e o DataFrame
def update_data(row_id, new_data):
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    row_id = row_id - 1
    # Obter o valor da coluna "mcenter_id" na linha especificada
    mcenter_id_value = df.index[row_id]
    st.write("mcenter_id_value: ", mcenter_id_value)
    # Construir a parte SET da consulta SQL
    set_clause = ", ".join(
        [
            f"{column} = '{new_data[column]}'"
            for column in new_data
            if new_data[column] != ""
        ]
    )

    # Se a set_clause for vazia, não há nada para atualizar
    if set_clause:
        # Atualizar a linha no banco de dados usando o valor da coluna "mcenter_id"
        update_query = f"UPDATE tiny_fulfillment SET {set_clause}, updated_at = (now() at time zone 'utc') WHERE mcenter_id = {mcenter_id_value}"
        cursor.execute(update_query)
        conn.commit()

        # Atualizar o DataFrame com os novos dados
        for column, value in new_data.items():
            if value != "":
                df.loc[df.index == mcenter_id_value, column] = value

        st.success("Dados Atualizados com Sucesso!")
    else:
        st.warning("Nenhum campo preenchido. Nenhum dado foi atualizado.")

    # Fechar a conexão
    conn.close()
    st.write(update_query)


with st.expander("Edição de dados"):
    st.write("Edite segundo o mcenter_id")

    # Selecionar linha para edição
    selected_row_id = st.number_input(
        "McenterID da Linha para Edição:", min_value=1, max_value=df.shape[0]
    )

    # Mostrar dados da linha selecionada
    selected_row_data = df.loc[[selected_row_id]]
    st.write("Dados da Linha Selecionada:")
    st.write(selected_row_data)

    # Obter novos dados para a linha
    new_ml_inventory_id = st.text_input("Novo ml_inventory_id:")
    new_ml_code = st.text_input("Nova ml_code:")
    new_ml_sku = st.text_input("Nova ml_sku:")
    new_var_code = st.text_input("Nova var_code:")
    new_tiny_id = st.text_input("Nova tiny_id:")
    new_tiny_sku = st.text_input("Nova tiny_sku:")
    new_ad_title = st.text_input("Nova ad_title:")

    new_data = {
        "ml_inventory_id": new_ml_inventory_id,
        "ml_code": new_ml_code,
        "ml_sku": new_ml_sku,
        "var_code": new_var_code,
        "tiny_id": new_tiny_id,
        "tiny_sku": new_tiny_sku,
        "ad_title": new_ad_title,
    }

    # Botão para atualizar os dados
    if st.button("Atualizar Dados"):
        update_data(selected_row_id, new_data)
        streamlit_js_eval(js_expressions="parent.window.location.reload()")


def insert_data(db_config, new_data):
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Insere a nova linha no banco de dados
        insert_query = sql.SQL(
            "INSERT INTO tiny_fulfillment (ml_inventory_id, ml_code, ml_sku, var_code, tiny_id, tiny_sku, ad_title) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        )
        cursor.execute(
            insert_query,
            (
                new_data["ml_inventory_id"],
                new_data["ml_code"],
                new_data["ml_sku"],
                new_data["var_code"],
                new_data["tiny_id"],
                new_data["tiny_sku"],
                new_data["ad_title"],
            ),
        )

        # Commit para efetivar as alterações
        conn.commit()

        st.success("Dados inseridos com sucesso!")
    except Exception as e:
        st.error(f"Erro ao inserir dados: {e}")
    finally:
        # Fecha o cursor e a conexão
        cursor.close()
        conn.close()


with st.expander("Adicionar Dados"):
    st.write("Preencha os campos abaixo:")

    ml_inventory_id = st.text_input("ml_inventory_id:")
    ml_code = st.text_input("ml_code:")
    ml_sku = st.text_input("ml_sku:")
    var_code = st.text_input("var_code:")
    tiny_id = st.text_input("tiny_id:")
    tiny_sku = st.text_input("tiny_sku:")
    ad_title = st.text_input("ad_title:")

    new_data = {
        "ml_inventory_id": ml_inventory_id,
        "ml_code": ml_code,
        "ml_sku": ml_sku,
        "var_code": var_code,
        "tiny_id": tiny_id,
        "tiny_sku": tiny_sku,
        "ad_title": ad_title,
    }

    if st.button("Adicionar Dados"):
        insert_data(db_config, new_data)
        streamlit_js_eval(js_expressions="parent.window.location.reload()")
