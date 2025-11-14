import data_source
import sys
import pandas as pd
import traceback

print("--- SCRIPT DE CORREÇÃO DE UMIDADE (KM 72) ---")
print("Este script irá ler o histórico existente e preencher os dados de umidade faltantes do Zentra Cloud.")
print("NENHUM DADO DE CHUVA SERÁ APAGADO.")

try:
    # 1. Configurar caminhos (Render: /var/data)
    data_source.setup_disk_paths()
    print(f"Caminhos configurados. Lendo de: {data_source.HISTORICO_FILE_CSV}")

    # 2. Ler o histórico ATUAL (com os dados de chuva)
    df_historico_existente = data_source.read_historico_from_csv()
    if df_historico_existente.empty:
        print("AVISO: historico_temp.csv está vazio. O backfill normal do worker (index.py) deve assumir.")
        print("Execute 'Manual Restart' no Render.")
        sys.exit()

    print(f"Histórico de chuva (CSV) lido com sucesso: {len(df_historico_existente)} linhas.")

    # 3. Chamar a função de backfill (que agora tem a lógica de 08/11/2025)
    # Esta função fará o merge e salvará o CSV e o SQLite
    print("Iniciando a chamada ao data_source.backfill_zentra_km72_data...")
    data_source.backfill_zentra_km72_data(df_historico_existente)

    print("\n--- CORREÇÃO DE UMIDADE CONCLUÍDA ---")
    print("O histórico foi atualizado com os dados de umidade.")
    print("Execute 'Manual Restart' no Web Service do Render para que o dashboard leia os novos dados.")

except Exception as e:
    print(f"\n--- ERRO NA CORREÇÃO DE UMIDADE ---")
    print(f"Ocorreu um erro: {e}")
    traceback.print_exc()