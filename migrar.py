# migrar.py
# Este script é de uso único para migrar o CSV para o SQLite.

import pandas as pd
from sqlalchemy import create_engine
import data_source  # Reutiliza as nossas funções de leitura
import config  # Reutiliza as nossas configurações
import os
import sys

print("--- INICIANDO MIGRAÇÃO MANUAL (CSV -> SQLite) ---")

# 1. Configurar os caminhos (para encontrar o CSV e o DB)
try:
    data_source.setup_disk_paths()
except Exception as e:
    print(f"Erro ao configurar caminhos: {e}")
    sys.exit()

# 2. Ler os dados do CSV
print(f"Lendo dados de: {data_source.HISTORICO_FILE_CSV}")
df_csv = data_source.read_historico_from_csv()

if df_csv.empty:
    print("ERRO: 'historico_temp.csv' está vazio ou não foi encontrado. Nada para migrar.")
else:
    print(f"Sucesso! {len(df_csv)} linhas lidas do CSV.")

    try:
        # 3. Obter o engine do SQLite
        engine = data_source.get_engine()

        # 4. Salvar no SQLite (usando 'replace' para apagar a tabela antiga)
        print(f"Conectando ao SQLite em: {config.DB_CONNECTION_STRING}")
        print(f"Escrevendo {len(df_csv)} linhas na tabela: '{config.DB_TABLE_NAME}'...")

        # if_exists='replace' apaga a tabela 'historico_monitoramento' se ela existir
        # e cria uma nova com os dados frescos do CSV.
        df_csv.to_sql(
            config.DB_TABLE_NAME,
            engine,
            if_exists='replace',
            index=False
        )

        print("\n--- MIGRAÇÃO CONCLUÍDA COM SUCESSO! ---")
        print(f"Os seus {len(df_csv)} registos do CSV estão agora salvos em 'temp_local_db.db'.")

    except Exception as e:
        print(f"\n--- ERRO NA MIGRAÇÃO ---")
        print(f"Ocorreu um erro: {e}")

print("-------------------------------------------------")