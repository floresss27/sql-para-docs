import os
from typing import Any
from decimal import Decimal
import psycopg2
from dotenv import load_dotenv
from pymongo import MongoClient
import pandas as pd
load_dotenv()

from queries import buscar_historico_aluno, disciplinas_professor, alunos_formados, chefes_departamento, grupo_de_tcc

postgres = psycopg2.connect(os.getenv('SQL_URL'))
mongo = MongoClient(os.getenv('MONGODB_URL'))
banco_mongo = mongo.sqlparadocs

def listar_tabelas() -> list[str]:
    print("Listando tabelas disponíveis no PostgreSQL...")
    with postgres.cursor() as db_context:
        db_context.execute("SHOW TABLES;")
        resultado = db_context.fetchall()
        postgres.commit()
        return [tabela[1] for tabela in resultado]

def buscar_todos_registros(nome_tabela: str):
    print(f"Buscando todos os registros de '{nome_tabela}'...")
    with postgres.cursor() as db_context:
        db_context.execute(f'SELECT * FROM "{nome_tabela}";')
        registros = db_context.fetchall()
        postgres.commit()
        return registros

def obter_colunas(nome_tabela: str):
    print(f"Obtendo colunas da tabela '{nome_tabela}'...")
    with postgres.cursor() as db_context:
        db_context.execute(f"SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{nome_tabela}';")
        resposta = db_context.fetchall()
        postgres.commit()
        return [table[3] for table in resposta]

def migrar_dados():
    tabelas = listar_tabelas()

    for tabela in tabelas:
        colunas = obter_colunas(tabela)
        registros = buscar_todos_registros(tabela)

        df = pd.DataFrame(registros, columns=colunas)

        for coluna in df.select_dtypes(include=['object']):
            df[coluna] = df[coluna].map(lambda x: float(x) if isinstance(x, Decimal) else x)

        banco_mongo[tabela].insert_many(df.to_dict('records'))

    print("Migração de dados concluída com sucesso!")

def limpar_colecoes():
    colecoes = banco_mongo.list_collection_names()
    if colecoes:
        print("Removendo todas as coleções do MongoDB...")
        for colecao in colecoes:
            banco_mongo[colecao].drop()
            print(f"Coleção '{colecao}' removida.")
    else:
        print("Nenhuma coleção encontrada para remover.")

if __name__ == '__main__':
    limpar_colecoes()
    migrar_dados()

    print("Saídas estarão disponíveis na pasta './resultados_docs'.")

    if not os.path.exists('./resultados_docs'):
        os.makedirs('./resultados_docs')

    buscar_historico_aluno()
    disciplinas_professor()
    alunos_formados()
    chefes_departamento()
    grupo_de_tcc()
