import data_source
import sys
import pandas as pd
import traceback
import os
from sqlalchemy import create_engine, text
import config

print("--- SCRIPT DE CORREÇÃO DE UMIDADE (v3) ---")
print("RECONSTRÓI O SQLITE (para PDF/Excel) com a trava UNIQUE.")
print("NENHUM DADO DE CHUVA SERÁ APAGADO.")

try:
    # 1. Configurar caminhos
    data_source.setup_disk_paths()
    print(f"Lendo de: {data_source.HISTORICO_FILE_CSV}")

    # 2. Ler o histórico ATUAL (com os dados de chuva)
    df_historico_existente = data_source.read_historico_from_csv()
    if df_historico_existente.empty:
        print("ERRO CRÍTICO: historico_temp.csv está vazio. Não fazer nada para evitar perda de dados.")
        sys.exit()

    print(f"Histórico (CSV) lido: {len(df_historico_existente)} linhas.")

    # --- INÍCIO DA CORREÇÃO SQLITE ---
    print("\n--- Reconstruindo o Banco de Dados (SQLite) para PDF/Excel ---")

    # 3. Apagar o banco de dados antigo (que está dessincronizado)
    db_path = config.DB_CONNECTION_STRING.replace("sqlite:///", "")
    if os.environ.get('RENDER'):
        db_path = f"{data_source.DATA_DIR}/{os.path.basename(db_path)}"

    db_shm = db_path + "-shm"
    db_wal = db_path + "-wal"

    print(f"Apagando banco de dados antigo em {db_path}...")
    if os.path.exists(db_path): os.remove(db_path)
    if os.path.exists(db_shm): os.remove(db_shm)
    if os.path.exists(db_wal): os.remove(db_wal)
    print("Banco de dados antigo apagado.")

    # 4. Criar um NOVO engine (após o arquivo ser apagado)
    engine_novo = data_source.get_engine()

    # 5. Criar a nova tabela com a trava UNIQUE
    create_table_sql = f"""
    CREATE TABLE {config.DB_TABLE_NAME} (
        "timestamp" TIMESTAMP,
        id_ponto TEXT,
        chuva_mm REAL,
        precipitacao_acumulada_mm REAL,
        umidade_1m_perc REAL,
        umidade_2m_perc REAL,
        umidade_3m_perc REAL,
        base_1m REAL,
        base_2m REAL,
        base_3m REAL,
        UNIQUE(id_ponto, "timestamp")
    );
    """
    print("Criando nova tabela no SQLite com a restrição UNIQUE(id_ponto, timestamp)...")
    with engine_novo.connect() as connection:
        connection.execute(text(create_table_sql))
    print("Tabela criada com sucesso.")

    # 6. Salvar o DF completo no SQLite (usando 'append')
    # (Usamos o df_historico_existente, que foi lido do CSV, que já é limpo pelo groupby)
    print(f"Escrevendo {len(df_historico_existente)} linhas completas no novo SQLite...")

    df_para_salvar_sql = df_historico_existente[data_source.COLUNAS_HISTORICO].copy()

    df_para_salvar_sql.to_sql(
        config.DB_TABLE_NAME,
        engine_novo,
        if_exists='append',
        index=False
    )
    print("Banco de dados (SQLite) reconstruído com sucesso.")
    # --- FIM DA CORREÇÃO SQLITE ---

    print("\n--- CORREÇÃO COMPLETA (CSV e SQLITE) CONCLUÍDA ---")
    print("Execute 'Manual Restart' no Web Service do Render.")

except Exception as e:
    print(f"\n--- ERRO NA CORREÇÃO DE UMIDADE (v3) ---")
    print(f"Ocorreu um erro: {e}")
    traceback.print_exc()