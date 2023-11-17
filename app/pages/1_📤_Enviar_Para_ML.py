import altair as alt
import streamlit as st
import pandas as pd
import io

# Initialize connection.
# conn = st.connection("postgresql", type="sql")

# @st.cache_data
# df = conn.query('SELECT * FROM tiny_products;', ttl="60m")

st.title('Produtos A Enviar ao FulFilment')
st.caption('Tabela de produtos a enviar')