# corrigir_db.py (CORRIGIDO v2 - Força a desconexão do engine)

import pandas as pd
from sqlalchemy import create_engine, text
import data_source  # Reutiliza nossas funções
import config  # Reutiliza nossas configurações
import os
import sys
import shutil  # Para fazer backup

print("--- INICIANDO CORREÇÃO DE DUPLICATAS (v2) ---")
print("Este script preserva TODOS os dados históricos.")

try:
    data_source.setup_disk_paths()
except Exception as e:
    print(f"Erro ao configurar caminhos: {e}")
    sys.exit()

# Define os caminhos dos arquivos
db_path = config.DB_CONNECTION_STRING.replace("sqlite:///", "")
if os.environ.get('RENDER'):
    db_path = f"{data_source.DATA_DIR}/{os.path.basename(db_path)}"

db_backup_path = db_path + ".backup"
db_shm = db_path + "-shm"
db_wal = db_path + "-wal"

# --- 1. Verificar se o DB existe ---
if not os.path.exists(db_path):
    print(f"ERRO: Banco de dados '{db_path}' não encontrado. Nada para corrigir.")
    print("Se você já rodou e falhou, restaure o .backup primeiro.")
    print("Ex: mv /var/data/temp_local_db.db.backup /var/data/temp_local_db.db")
    sys.exit()

print(f"Encontrado banco de dados: {db_path}")

try:
    engine = data_source.get_engine()

    # --- 2. Ler TODOS os dados do banco atual ---
    print("Lendo TODO o histórico do banco de dados atual para a memória...")
    query = f"SELECT * FROM {config.DB_TABLE_NAME}"
    df_completo = pd.read_sql_query(query, engine, parse_dates=["timestamp"])

    if df_completo.empty:
        print("Aviso: O banco de dados está vazio. Saindo.")
        sys.exit()

    print(f"Total de {len(df_completo)} linhas lidas.")

    # --- 3. Limpar duplicatas na memória ---
    df_limpo = df_completo.drop_duplicates(subset=['id_ponto', 'timestamp'], keep='last')

    num_removidas = len(df_completo) - len(df_limpo)
    if num_removidas > 0:
        print(f"LIMPEZA: {num_removidas} linhas duplicadas foram removidas em memória.")
    else:
        print("LIMPEZA: Nenhuma duplicata encontrada.")

    print(f"Total de {len(df_limpo)} linhas únicas restantes.")

    # --- INÍCIO DA CORREÇÃO ---
    # 3.5. Força a desconexão do banco de dados antigo
    print("Desconectando do banco de dados antigo...")
    engine.dispose()
    # --- FIM DA CORREÇÃO ---

    # --- 4. Fazer Backup (Segurança) ---
    print(f"Criando backup do banco antigo em: {db_backup_path}")
    shutil.copy2(db_path, db_backup_path)

    # --- 5. Apagar o banco antigo (com duplicatas) ---
    print("Apagando o banco de dados antigo (sem a trava UNIQUE)...")
    if os.path.exists(db_path): os.remove(db_path)
    if os.path.exists(db_shm): os.remove(db_shm)
    if os.path.exists(db_wal): os.remove(db_wal)

    # --- INÍCIO DA CORREÇÃO ---
    # 6. Criar um NOVO engine (após o arquivo ser apagado)
    print("Conectando ao novo arquivo de banco de dados...")
    engine_novo = data_source.get_engine()
    # --- FIM DA CORREÇÃO ---

    # 6.5 Criar a nova tabela com a trava UNIQUE
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
    # --- INÍCIO DA CORREÇÃO ---
    with engine_novo.connect() as connection:  # Usa o NOVO engine
        # --- FIM DA CORREÇÃO ---
        connection.execute(text(create_table_sql))

    # --- 7. Salvar os dados limpos no novo banco ---
    print(f"Salvando as {len(df_limpo)} linhas limpas no novo banco de dados...")

    df_para_salvar = df_limpo[data_source.COLUNAS_HISTORICO]

    df_para_salvar.to_sql(
        config.DB_TABLE_NAME,
        # --- INÍCIO DA CORREÇÃO ---
        engine_novo,  # Usa o NOVO engine
        # --- FIM DA CORREÇÃO ---
        if_exists='append',
        index=False
    )

    print("\n--- CORREÇÃO CONCLUÍDA COM SUCESSO! ---")
    print("Seu histórico completo foi preservado (sem duplicatas) e protegido contra futuras duplicatas.")

except Exception as e:
    print(f"\n--- ERRO NA CORREÇÃO ---")
    print(f"Ocorreu um erro: {e}")
    print("Seus dados originais estão salvos no arquivo .backup")

print("-------------------------------------------------")