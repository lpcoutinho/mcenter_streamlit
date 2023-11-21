import io

import altair as alt
import pandas as pd

import streamlit as st

st.set_page_config(page_title="Produtos", page_icon="", layout="wide")

# Initialize connection.
conn = st.connection("postgresql", type="sql")

# @st.cache_data
df = conn.query("SELECT * FROM tiny_products;", ttl="1m")

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
