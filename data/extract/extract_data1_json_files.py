import os
import pandas as pd
import json
from sqlalchemy import create_engine
from utils.scripts import load_env_variables, create_db_engine

def flatten_json(y):
    """Achata um JSON aninhado em um dicionário plano."""
    out = {}
    
    def flatten(x, name=''):
        if isinstance(x, dict):
            for a in x:
                flatten(x[a], name + a + '_')
        elif isinstance(x, list):
            for i, a in enumerate(x):
                flatten(a, name + str(i) + '_')
        else:
            out[name[:-1]] = x  # Remove o último underscore
    
    flatten(y)
    return out

def read_json_file(file_path):
    """Lê um arquivo JSON e retorna o conteúdo da chave 'content'."""
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    return data.get('content', {})

def create_dataframe(content_data):
    """Cria um DataFrame a partir dos dados de conteúdo, incluindo a chave como 'id'."""
    rows = []
    for key, value in content_data.items():
        flattened = flatten_json(value)
        flattened['id'] = key  # Adiciona a chave como 'id'
        rows.append(flattened)

        df = pd.DataFrame(rows)
    
        # Reorganiza as colunas para que 'id' seja a primeira
        column_order = ['id'] + [col for col in df.columns if col != 'id']
        df = df[column_order]

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
    json_folder_path = os.path.join(os.getcwd(), 'json_files/data1')
    
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