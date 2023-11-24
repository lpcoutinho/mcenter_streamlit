import os
import time
from dotenv import load_dotenv
from loguru import logger
from ml_consume import MeLiLoader

logger.add(
    "Data/Output/Log/routines.log",
    rotation="10 MB",
    format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
)

load_dotenv()

ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
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

start_prog = time.time()  # Registra o inicio da aplicação

loader = MeLiLoader(db_config, ACCESS_TOKEN)
# df_fulfillment = loader.get_and_insert_fulfillment_stock()
# df_orders = loader.get_orders_data()

loader.get_and_insert_fulfillment_stock()
df_orders = loader.get_orders_data()
loader.insert_orders_data(df_orders)


end_prog = time.time()  # Registra o tempo depois de toda aplicação
elapsed_time = end_prog - start_prog  # Calcula o tempo decorrido
logger.info(f"Tempo Total do processo: {elapsed_time / 60} minutos")

python3 mcenter_streamlit/app/pages/ml_consume/routines.py