import altair as alt
import streamlit as st
import pandas as pd
import io

# Initialize connection.
conn = st.connection("postgresql", type="sql")

# @st.cache_data
df = conn.query('SELECT * FROM tiny_products;', ttl="60m")

st.title('Produtos')
st.caption('Tabela de produtos')
df

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
    file_name='produtos.xlsx',
    mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
)

chart = (
    alt.Chart(
        df,
        title="Static site generators popularity",
    )
    .mark_bar()
    # .encode(
    #     x=alt.X("stars", title="'000 stars on Github"),
    #     y=alt.Y(
    #         "name",
    #         sort=alt.EncodingSortField(field="stars", order="descending"),
    #         title="",
    #     ),
    #     color=alt.Color(
    #         "lang",
    #         legend=alt.Legend(title="Language"),
    #         # scale=get_github_scale(),
    #     ),
    #     tooltip=["name", "stars", "lang"],
    # )
)


st.altair_chart(chart, use_container_width=True)