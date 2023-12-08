import os
import time

from dotenv import load_dotenv
from loguru import logger

logger.add(
    "Data/Output/Log/routines.log",
    rotation="10 MB",
    format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
)

load_dotenv()

TINY_TOKEN = os.getenv("TINY_TOKEN")
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


# Configurações da API Tiny
token = TINY_TOKEN
tiny_format = "JSON"

from tiny_consume import TinyLoader

start_prog = time.time()  # Registra o inicio da aplicação

loader = TinyLoader(db_config, token, tiny_format)
# loader.get_tiny_stock_hist()
loader.get_tiny_stock()

end_prog = time.time()  # Registra o tempo depois de toda aplicação
elapsed_time = end_prog - start_prog  # Calcula o tempo decorrido
logger.info(f"Tempo Total do processo: {elapsed_time / 60} minutos")
