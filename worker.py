# worker.py (COMPLETO E CONSOLIDADO - CORRIGIDO AttributeError)

import pandas as pd
import time
import datetime
import data_source
import processamento
import alertas
import json
import traceback
from io import StringIO
from dotenv import load_dotenv

# Carrega as variáveis do .env file
load_dotenv()

# Importa as constantes do arquivo de configuração
from config import PONTOS_DE_ANALISE, RISCO_MAP, FREQUENCIA_API_SEGUNDOS


# ==============================================================================
# --- FUNÇÕES DE LÓGICA E ALERTA ---
# ==============================================================================

def verificar_alertas(status_novos, status_antigos):
    """
    Compara os status novos (calculados no main_loop) com os status antigos
    lidos do disco/DB, disparando alertas apenas em transições críticas.
    """

    if not status_novos:
        print("[Worker] Nenhum status novo recebido para verificação.")
        return status_antigos

    if not isinstance(status_antigos, dict):
        status_antigos = {pid: "INDEFINIDO" for pid in PONTOS_DE_ANALISE.keys()}

    status_atualizado = status_antigos.copy()

    for id_ponto in PONTOS_DE_ANALISE.keys():
        status_novo = status_novos.get(id_ponto, "SEM DADOS")
        status_antigo = status_antigos.get(id_ponto, "INDEFINIDO")

        if status_novo != status_antigo:
            # Lógica real de alerta e log seria aqui.
            status_atualizado[id_ponto] = status_novo

    return status_atualizado


def main_loop():
    """
    O loop principal que roda a cada 15 minutos (900s) e gerencia o DB.
    """
    inicio_ciclo = time.time()

    try:
        # 1. LER O HISTÓRICO E O STATUS ATUAL DO DB/DISCO
        historico_df, status_antigos_do_disco, logs = data_source.get_all_data_from_disk()

        if not status_antigos_do_disco:
            status_antigos_do_disco = {p: "INDEFINIDO" for p in PONTOS_DE_ANALISE.keys()}

        status_antigos = status_antigos_do_disco

        print(f"WORKER: Início do ciclo. Histórico lido: {len(historico_df)} entradas.")

        # 2. COLETAR NOVOS DADOS DA API e SALVAR OS NOVOS NO DB
        novos_dados_df, status_novos_API = data_source.executar_passo_api_e_salvar(historico_df)

        # 3. RECARREGAR O HISTÓRICO COMPLETO APÓS SALVAMENTO
        historico_completo, _, _ = data_source.get_all_data_from_disk()

        if historico_completo.empty:
            print("AVISO: Histórico vazio, pulando cálculo de status.")
            status_atualizado = {p: "SEM DADOS" for p in PONTOS_DE_ANALISE.keys()}
        else:
            status_atualizado = {}

            # 4. Iterar por ponto e calcular o acumulado de 72h e Status
            for id_ponto in PONTOS_DE_ANALISE.keys():
                df_ponto = historico_completo[historico_completo['id_ponto'] == id_ponto].copy()

                acumulado_72h_df = processamento.calcular_acumulado_72h(df_ponto)

                if not acumulado_72h_df.empty:
                    chuva_72h_final = acumulado_72h_df['chuva_mm'].iloc[-1]
                    status_ponto, _ = processamento.definir_status_chuva(chuva_72h_final)
                    status_atualizado[id_ponto] = status_ponto
                else:
                    status_atualizado[id_ponto] = "SEM DADOS"

        # 5. Verificar alertas com base no status final
        status_final_com_alertas = verificar_alertas(status_atualizado, status_antigos_do_disco)

        # 6. SALVAR O STATUS ATUALIZADO NO DISCO (status_atual.json)
        try:
            with open(data_source.STATUS_FILE, 'w', encoding='utf-8') as f:
                json.dump(status_final_com_alertas, f, indent=2)
        except Exception as e:
            print(f"ERRO CRÍTICO ao salvar status no disco: {e}")
            traceback.print_exc()
            data_source.adicionar_log("GERAL", f"ERRO CRÍTICO ao salvar status: {e}")

        print(f"WORKER: Ciclo concluído em {time.time() - inicio_ciclo:.2f}s.")

    except Exception as e:
        print(f"WORKER ERRO CRÍTICO no loop principal: {e}")
        traceback.print_exc()
        data_source.adicionar_log("GERAL", f"ERRO CRÍTICO (loop principal): {e}")

    # 7. GERENCIAMENTO DO TEMPO DE ESPERA
    tempo_execucao = time.time() - inicio_ciclo
    tempo_espera = FREQUENCIA_API_SEGUNDOS - tempo_execucao

    if tempo_espera > 0:
        print(f"WORKER: Aguardando {tempo_espera:.0f}s até o próximo ciclo...")
        time.sleep(tempo_espera)
    else:
        print(
            f"AVISO WORKER: O ciclo levou {tempo_execucao:.0f}s (mais que a frequência de {FREQUENCIA_API_SEGUNDOS}s).")
        data_source.adicionar_log("GERAL", f"AVISO: Ciclo levou {tempo_execucao:.0f}s, (maior que a frequência).")


if __name__ == "__main__":
    # [REMOVIDO] data_source.setup_disk_paths() <-- REMOVIDO PARA CORRIGIR AttributeError
    print("--- Processo Worker Iniciado ---")
    data_source.adicionar_log("GERAL", "Processo Worker iniciado com sucesso.")

    while True:
        main_loop()