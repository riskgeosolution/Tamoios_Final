# preencher_km67.py (VERSÃO SEGURA: Apenas preenche o "buraco" de dados, não apaga)

import data_source
import pandas as pd
import time
import sys

print("--- INICIANDO PREENCHIMENTO MANUAL DO HISTÓRICO KM 67 (72h) ---")
print("MODO SEGURO: Esta versão apenas adiciona (append) os dados faltantes ao SQLite.")

try:
    # 1. Configurar os caminhos
    print("Configurando caminhos...")
    data_source.setup_disk_paths()

    # 2. Ler o histórico existente do CSV (necessário para a função)
    print("Lendo histórico atual do CSV (para mesclagem)...")
    df_existente = data_source.read_historico_from_csv()
    if not df_existente.empty:
        print(f"Histórico CSV existente encontrado: {len(df_existente)} linhas.")
    else:
        print("Nenhum histórico CSV existente. Um novo será criado.")

    # 3. Chamar a função de backfill (que é SEGURA)
    print("\nChamando a API /historic da WeatherLink para o KM 67...")
    print("(Isso pode levar de 10 a 30 segundos)...")

    # Esta função (data_source.py) é SEGURA:
    # 1. Baixa 72h de dados (incluindo o "buraco").
    # 2. Chama save_to_sqlite(df_backfill), que usa 'append' (ADICIONAR) ao seu banco de dados de 4 dias.
    # 3. Atualiza o cache 'historico_temp.csv' (que só tem 72h, o que está correto).
    data_source.backfill_km67_pro_data(df_existente)

    print("\n--- DOWNLOAD DE HISTÓRICO E PREENCHIMENTO DO BANCO CONCLUÍDOS ---")
    print("O 'buraco' de dados no seu SQLite (temp_local_db.db) foi preenchido.")
    print("O arquivo 'historico_temp.csv' (cache de 72h) foi atualizado.")

except Exception as e:
    print(f"\n--- ERRO DURANTE O PREENCHIMENTO ---")
    print(f"Ocorreu um erro: {e}")
    sys.exit()

print("-------------------------------------------------")
print("Processo de preenchimento manual concluído.")