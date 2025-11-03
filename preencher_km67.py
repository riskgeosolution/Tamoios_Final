# preencher_km67.py
#
# Este script de uso manual força o download do histórico de 72h
# do KM 67 (Plano Pro) e o mescla com os dados existentes.
#
# Use-o para preencher "buracos" (gaps) de dados
# após uma falha prolongada da API da WeatherLink.

import data_source
import pandas as pd
import time
import sys

print("--- INICIANDO PREENCHIMENTO MANUAL DO HISTÓRICO KM 67 (72h) ---")

try:
    # 1. Configurar os caminhos
    print("Configurando caminhos...")
    data_source.setup_disk_paths()

    # 2. Ler o histórico existente do CSV
    print("Lendo histórico atual do CSV (para mesclagem)...")
    # Nós lemos o CSV, pois ele é o cache de 72h que o backfill usa
    df_existente = data_source.read_historico_from_csv()
    if not df_existente.empty:
        print(f"Histórico existente encontrado: {len(df_existente)} linhas.")
    else:
        print("Nenhum histórico existente. Um novo será criado.")

    # 3. Chamar a função de backfill
    print("\nChamando a API /historic da WeatherLink para o KM 67...")
    print("(Isso pode levar de 10 a 30 segundos)...")

    # Esta função (data_source.py) foi corrigida (v6/v7) para:
    # 1. Baixar os 288 registros (72h) do KM 67.
    # 2. Mesclar com o 'df_existente'.
    # 3. Salvar no SQLite (novos dados).
    # 4. Salvar no CSV (removendo duplicatas e truncando).
    data_source.backfill_km67_pro_data(df_existente)

    print("\n--- DOWNLOAD DE HISTÓRICO CONCLUÍDO ---")
    print("O arquivo 'historico_temp.csv' foi atualizado e limpo.")

except Exception as e:
    print(f"\n--- ERRO DURANTE O PREENCHIMENTO ---")
    print(f"Ocorreu um erro: {e}")
    sys.exit()

# 4. PASSO CRÍTICO: Rodar a migração
print("\n--- INICIANDO SINCRONIZAÇÃO DO BANCO DE DADOS ---")
print("Limpando o banco de dados 'temp_local_db.db' e recarregando-o...")
try:
    # Agora, lemos o CSV (que está 100% completo)
    df_completo = data_source.read_historico_from_csv()

    # E o usamos para SUBSTITUIR (replace) o banco de dados SQLite
    engine = data_source.get_engine()
    df_completo.to_sql(
        data_source.DB_TABLE_NAME,
        engine,
        if_exists='replace',
        index=False
    )
    print("SUCESSO! O banco de dados SQLite foi sincronizado.")
    print(f"Total de {len(df_completo)} registros salvos.")

except Exception as e:
    print(f"\n--- ERRO DURANTE A SINCRONIZAÇÃO DO SQLITE ---")
    print(f"Ocorreu um erro: {e}")

print("-------------------------------------------------")
print("Processo de preenchimento manual concluído.")