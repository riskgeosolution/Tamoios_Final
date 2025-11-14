import data_source
import sys
import pandas as pd
import traceback
import os
from sqlalchemy import create_engine
import config

print("--- SCRIPT DE CORREÇÃO DE UMIDADE (v2) ---")
print("Preenche dados Zentra E RECONSTRÓI O SQLITE PARA PDF/EXCEL.")
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

    print(f"Histórico de chuva (CSV) lido: {len(df_historico_existente)} linhas.")

    # 3. Chamar a função de backfill (que mescla e SALVA O CSV CORRIGIDO)
    print("Iniciando a chamada ao data_source.backfill_zentra_km72_data...")
    data_source.backfill_zentra_km72_data(df_historico_existente)
    print("CSV mesclado e salvo com sucesso.")

    # --- INÍCIO DA CORREÇÃO SQLITE ---
    print("\n--- Reconstruindo o Banco de Dados (SQLite) para PDF/Excel ---")

    # 4. Apagar o banco de dados antigo (que está dessincronizado)
    # Precisamos pegar o caminho exato do DB
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

    # 5. Ler o CSV *AGORA CORRIGIDO* (que foi salvo no passo 3)
    print("Lendo o CSV recém-corrigido (com chuva e umidade)...")
    df_corrigido_completo = data_source.read_historico_from_csv()

    if df_corrigido_completo.empty:
        print("ERRO CRÍTICO: O CSV corrigido está vazio após a leitura. Abortando.")
        sys.exit()

    print(f"{len(df_corrigido_completo)} linhas lidas do CSV corrigido.")

    # 6. Salvar o DF completo no SQLite (usando 'replace')
    engine = data_source.get_engine()
    print(f"Escrevendo dados completos no novo SQLite (tabela: {config.DB_TABLE_NAME})...")

    # Assegura que apenas colunas válidas sejam enviadas (evita erros de colunas extras do merge)
    df_para_salvar_sql = df_corrigido_completo[data_source.COLUNAS_HISTORICO].copy()

    df_para_salvar_sql.to_sql(
        config.DB_TABLE_NAME,
        engine,
        if_exists='replace',  # Força a recriação da tabela
        index=False
    )
    print("Banco de dados (SQLite) reconstruído com sucesso.")
    # --- FIM DA CORREÇÃO SQLITE ---

    print("\n--- CORREÇÃO COMPLETA (CSV e SQLITE) CONCLUÍDA ---")
    print("Execute 'Manual Restart' no Web Service do Render.")

except Exception as e:
    print(f"\n--- ERRO NA CORREÇÃO DE UMIDADE (v2) ---")
    print(f"Ocorreu um erro: {e}")
    traceback.print_exc()