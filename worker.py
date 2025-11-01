# worker.py (CORRIGIDO: Chamando a função 'calcular_acumulado_rolling')

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
            # A lógica real de envio (alertas.enviar_alerta) é chamada aqui
            # com base nas regras de transição.

            # Exemplo (simplificado):
            # if status_novo == "PARALIZAÇÃO" and status_antigo == "ALERTA":
            #    alertas.enviar_alerta(id_ponto, PONTOS_DE_ANALISE[id_ponto]['nome'], status_novo, status_antigo)

            status_atualizado[id_ponto] = status_novo

    return status_atualizado


def main_loop():
    """
    O loop principal que roda a cada 15 minutos (900s) e usa o fluxo de arquivo CSV.
    """
    inicio_ciclo = time.time()

    try:
        # 1. LER O HISTÓRICO ATUAL DO ARQUIVO CSV
        historico_df, status_antigos_do_disco, logs = data_source.get_all_data_from_disk()

        if not status_antigos_do_disco:
            status_antigos_do_disco = {p: "INDEFINIDO" for p in PONTOS_DE_ANALISE.keys()}

        status_antigos = status_antigos_do_disco

        print(f"WORKER: Início do ciclo. Histórico lido: {len(historico_df)} entradas.")

        # --- NOVO: LÓGICA DE BACKFILL (PREENCHIMENTO) PARA O PLANO PRO (KM 67) ---
        # Verifica se o Ponto A (KM 67) tem dados. Se não tiver, busca o histórico.
        if historico_df.empty or historico_df[historico_df['id_ponto'] == 'Ponto-A-KM67'].empty:
            print("[Worker] Histórico do KM 67 (Pro) está vazio. Tentando backfill de 72h...")
            try:
                # Chama a função de backfill (que salva no CSV)
                data_source.backfill_km67_pro_data()
                # Recarrega o histórico (agora com dados do KM 67)
                historico_df, _, _ = data_source.get_all_data_from_disk()
                print(f"[Worker] Backfill concluído. Histórico atual: {len(historico_df)} entradas.")
            except Exception as e_backfill:
                data_source.adicionar_log("GERAL", f"ERRO CRÍTICO (Backfill KM 67): {e_backfill}")
        # --- FIM DO BACKFILL ---

        # 2. COLETAR NOVOS DADOS DA API (ENDPOINT /CURRENT PARA TODOS OS 4 PONTOS)
        # Esta função faz a coleta, concatena e SALVA o arquivo de histórico.
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

                # --- INÍCIO DA CORREÇÃO ---
                # CORRIGIDO: Chamando a nova função com horas=72
                acumulado_72h_df = processamento.calcular_acumulado_rolling(df_ponto, horas=72)
                # --- FIM DA CORREÇÃO ---

                if not acumulado_72h_df.empty:
                    # NOTA: O status é definido com base no *último* valor do acumulado de 72h
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
    # CHAMA A FUNÇÃO DE CONFIGURAÇÃO DE CAMINHO (RESTAURADA)
    data_source.setup_disk_paths()

    print("--- Processo Worker Iniciado ---")
    data_source.adicionar_log("GERAL", "Processo Worker iniciado com sucesso.")

    while True:
        main_loop()