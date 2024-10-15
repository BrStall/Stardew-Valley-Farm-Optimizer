import os
import pandas as pd
import json
from sqlalchemy import create_engine
from utils.scripts import load_env_variables, create_db_engine

def read_json_file(file_path):
    """Lê um arquivo JSON e retorna o conteúdo da chave 'content'."""
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    return data.get('content', {})  # Retorna o conteúdo da chave 'content'

def create_dataframe(content_data):
    """Cria um DataFrame a partir dos dados de conteúdo, transformando as colunas."""
    rows = []

    # Verifica se content_data é um dicionário
    if isinstance(content_data, dict):
        for key, value in content_data.items():
            # Cria um dicionário para cada entrada
            row = {
                'id': key,  # O número se torna a coluna 'id'
                'description': value,  # O valor completo se torna 'description'
            }
            rows.append(row)
    else:
        print("Estrutura de dados não suportada.")
        return pd.DataFrame()  # Retorna um DataFrame vazio em caso de erro

    # Cria o DataFrame a partir das linhas coletadas
    df = pd.DataFrame(rows)
    return df

def save_to_database(df, table_name, engine):
    """Salva o DataFrame em uma tabela do banco de dados."""
    try:
        df.to_sql(table_name, engine, if_exists='replace', index=False, schema='data')
        print(f"Dados enviados para a tabela '{table_name}' com sucesso.")
    except Exception as e:
        print(f"Erro ao enviar dados para a tabela '{table_name}': {str(e)}")

def process_json_files(engine):
    """Processa todos os arquivos JSON na pasta e salva os dados no banco de dados."""
    json_folder_path = os.path.join(os.getcwd(), 'json_files/data3')
    
    # Verifica se a pasta existe
    if not os.path.exists(json_folder_path):
        print(f"A pasta '{json_folder_path}' não existe.")
        return
    
    json_files = [f for f in os.listdir(json_folder_path) if f.endswith('.json')]
    if not json_files:
        print(f"Nenhum arquivo JSON encontrado na pasta '{json_folder_path}'.")
        return
    
    for filename in json_files:
        table_name = os.path.splitext(filename)[0]
        file_path = os.path.join(json_folder_path, filename)

        try:
            content_data = read_json_file(file_path)
            if not content_data:
                print(f"A chave 'content' não foi encontrada no arquivo {filename}.")
                continue
            
            df = create_dataframe(content_data)
            print(f"Dados normalizados do arquivo {filename}:")
            print(df)  # Exibe os dados normalizados para verificação
            save_to_database(df, table_name, engine)

        except Exception as e:
            print(f"Erro ao processar o arquivo {filename}: {str(e)}")

# Função principal
def main():
    # Carrega as variáveis de ambiente
    db_user, db_password, db_host, db_port, db_name = load_env_variables()
    
    # Cria a conexão com o banco de dados
    engine = create_db_engine(db_user, db_password, db_host, db_port, db_name)
    
    process_json_files(engine)

if __name__ == "__main__":
    main()
