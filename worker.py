# worker.py (CORRIGIDO v6: Corrigindo o bug "ValueError: second must be in 0..59")

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

load_dotenv()

from config import PONTOS_DE_ANALISE, RISCO_MAP, FREQUENCIA_API_SEGUNDOS


# ==============================================================================
# --- FUNÇÕES DE LÓGICA E ALERTA ---
# ==============================================================================

def verificar_alertas(status_novos, status_antigos):
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

            try:
                nome_ponto = PONTOS_DE_ANALISE[id_ponto]['nome']
                mensagem_log = f"MUDANÇA DE STATUS: {nome_ponto} mudou de {status_antigo} para {status_novo}."
                data_source.adicionar_log(id_ponto, mensagem_log)
                print(f"| {id_ponto} | {mensagem_log}")
            except Exception as e:
                print(f"Erro ao gerar log de mudança de status: {e}")

            # (A lógica de envio de alertas.enviar_alerta() vai aqui)
            # alertas.enviar_alerta(id_ponto, PONTOS_DE_ANALISE[id_ponto]['nome'], status_novo, status_antigo)

            status_atualizado[id_ponto] = status_novo

    return status_atualizado


def main_loop():
    """
    Executa UM ciclo de coleta e processamento.
    Retorna True se foi bem-sucedido, False se falhou.
    """
    inicio_ciclo = time.time()

    try:
        # 1. LER O HISTÓRICO ATUAL DO ARQUIVO CSV
        historico_df, status_antigos_do_disco, logs = data_source.get_all_data_from_disk()

        if not status_antigos_do_disco:
            status_antigos_do_disco = {p: "INDEFINIDO" for p in PONTOS_DE_ANALISE.keys()}

        status_antigos = status_antigos_do_disco

        print(f"WORKER: Início do ciclo. Histórico lido: {len(historico_df)} entradas.")

        # --- LÓGICA DE BACKFILL (só roda se os arquivos estiverem vazios) ---
        if historico_df.empty or historico_df[historico_df['id_ponto'] == 'Ponto-A-KM67'].empty:
            print("[Worker] Histórico do KM 67 (Pro) está vazio. Tentando backfill de 72h...")
            try:
                data_source.backfill_km67_pro_data(historico_df)
                historico_df, _, _ = data_source.get_all_data_from_disk()
                print(f"[Worker] Backfill concluído. Histórico atual: {len(historico_df)} entradas.")
            except Exception as e_backfill:
                data_source.adicionar_log("GERAL", f"ERRO CRÍTICO (Backfill KM 67): {e_backfill}")
        # --- FIM DO BACKFILL ---

        # 2. COLETAR NOVOS DADOS DA API
        novos_dados_df, status_novos_API = data_source.executar_passo_api_e_salvar(historico_df)

        # 3. RECARREGAR O HISTÓRICO COMPLETO APÓS SALVAMENTO
        historico_completo, _, _ = data_source.get_all_data_from_disk()

        if historico_completo.empty:
            print("AVISO: Histórico vazio, pulando cálculo de status.")
            status_atualizado = {p: "SEM DADOS" for p in PONTOS_DE_ANALISE.keys()}
        else:
            status_atualizado = {}
            for id_ponto in PONTOS_DE_ANALISE.keys():
                df_ponto = historico_completo[historico_completo['id_ponto'] == id_ponto].copy()

                acumulado_72h_df = processamento.calcular_acumulado_rolling(df_ponto, horas=72)

                if not acumulado_72h_df.empty:
                    chuva_72h_final = acumulado_72h_df['chuva_mm'].iloc[-1]
                    status_ponto, _ = processamento.definir_status_chuva(chuva_72h_final)
                    status_atualizado[id_ponto] = status_ponto
                else:
                    status_atualizado[id_ponto] = "SEM DADOS"

        # 5. Verificar alertas com base no status final
        status_final_com_alertas = verificar_alertas(status_atualizado, status_antigos_do_disco)

        # 6. SALVAR O STATUS ATUALIZADO NO DISCO
        try:
            with open(data_source.STATUS_FILE, 'w', encoding='utf-8') as f:
                json.dump(status_final_com_alertas, f, indent=2)
        except Exception as e:
            print(f"ERRO CRÍTICO ao salvar status no disco: {e}")
            traceback.print_exc()
            data_source.adicionar_log("GERAL", f"ERRO CRÍTICO ao salvar status: {e}")

        print(f"WORKER: Ciclo concluído em {time.time() - inicio_ciclo:.2f}s.")
        return True

    except Exception as e:
        print(f"WORKER ERRO CRÍTICO no loop principal: {e}")
        traceback.print_exc()
        data_source.adicionar_log("GERAL", f"ERRO CRÍTICO (loop principal): {e}")
        return False


# ==============================================================================
# --- NOVO: LÓGICA DE EXECUÇÃO "INTELIGENTE" (CORRIGIDA) ---
# ==============================================================================
if __name__ == "__main__":
    data_source.setup_disk_paths()
    print("--- Processo Worker Iniciado (Modo Sincronizado) ---")
    data_source.adicionar_log("GERAL", "Processo Worker iniciado com sucesso.")

    INTERVALO_EM_MINUTOS = 15
    CARENCIA_EM_SEGUNDOS = 60  # 1 minuto de "folga"

    while True:
        inicio_total = time.time()

        # 1. Roda o ciclo de coleta
        main_loop()

        tempo_execucao = time.time() - inicio_total

        # 2. Pega a hora atual (UTC)
        agora_utc = datetime.datetime.now(datetime.timezone.utc)

        # 3. Calcula quando o *próximo* ciclo de 15 min começa
        proximo_minuto_base = (agora_utc.minute // INTERVALO_EM_MINUTOS + 1) * INTERVALO_EM_MINUTOS

        proxima_hora_utc = agora_utc

        # 4. Lida com a virada da hora (ex: 10:47 -> 11:00)
        if proximo_minuto_base >= 60:
            proxima_hora_utc = agora_utc + datetime.timedelta(hours=1)
            proximo_minuto_base = 0

        # --- INÍCIO DA CORREÇÃO ---
        # 5. Define a hora exata da próxima execução (a hora "base")
        proxima_execucao_base_utc = proxima_hora_utc.replace(
            minute=proximo_minuto_base,
            second=0,
            microsecond=0
        )

        # 6. Adiciona o período de carência (a "folga")
        proxima_execucao_com_carencia_utc = proxima_execucao_base_utc + datetime.timedelta(seconds=CARENCIA_EM_SEGUNDOS)

        # 7. Calcula quantos segundos dormir até lá
        tempo_para_dormir_seg = (proxima_execucao_com_carencia_utc - agora_utc).total_seconds()
        # --- FIM DA CORREÇÃO ---

        # 8. Garante que não dormimos um tempo negativo
        if tempo_para_dormir_seg < 0:
            print(f"AVISO: O ciclo demorou {tempo_execucao:.1f}s e perdeu a janela. Rodando novamente...")
            tempo_para_dormir_seg = 1

        print(f"WORKER: Ciclo levou {tempo_execucao:.1f}s.")
        print(
            f"WORKER: Próxima execução às {proxima_execucao_com_carencia_utc.isoformat()}. Dormindo por {tempo_para_dormir_seg:.0f}s...")
        time.sleep(tempo_para_dormir_seg)