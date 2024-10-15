from dotenv import load_dotenv
import os
from sqlalchemy import create_engine

# Função para carregar as variáveis de ambiente
def load_env_variables():
    load_dotenv()

    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_name = os.getenv('DB_NAME')

    if not all([db_user, db_password, db_host, db_port, db_name]):
        raise ValueError("Alguma variável de ambiente não está definida. Verifique o arquivo .env.")
   
    return db_user, db_password, db_host, db_port, db_name

def create_db_engine(db_user, db_password, db_host, db_port, db_name):
    return create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')