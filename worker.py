# worker.py (CORRIGIDO v7: Separação dos Status de Chuva e Umidade)

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
# --- FUNÇÕES DE LÓGICA E ALERTA (MODIFICADA) ---
# ==============================================================================

def verificar_alertas(status_novos, status_antigos):
    """
    Verifica mudanças nos status de CHUVA e UMIDADE de forma independente.
    """
    if not status_novos:
        print("[Worker] Nenhum status novo recebido para verificação.")
        return status_antigos

    # Garantir que o status antigo tenha a estrutura de dicionário correta para comparação
    if not isinstance(status_antigos, dict) or not all(isinstance(v, dict) for v in status_antigos.values()):
        status_antigos = {pid: {"chuva": "INDEFINIDO", "umidade": "INDEFINIDO"} for pid in PONTOS_DE_ANALISE.keys()}

    status_atualizado = status_antigos.copy()

    for id_ponto in PONTOS_DE_ANALISE.keys():
        status_novo_ponto = status_novos.get(id_ponto, {"chuva": "SEM DADOS", "umidade": "SEM DADOS"})
        status_antigo_ponto = status_antigos.get(id_ponto, {"chuva": "INDEFINIDO", "umidade": "INDEFINIDO"})

        # Assegurar que status_antigo_ponto seja um dicionário para evitar erros
        if not isinstance(status_antigo_ponto, dict):
            status_antigo_ponto = {"chuva": "INDEFINIDO", "umidade": "INDEFINIDO"}

        # --- Verificar mudança no status da CHUVA ---
        status_novo_chuva = status_novo_ponto.get("chuva", "SEM DADOS")
        status_antigo_chuva = status_antigo_ponto.get("chuva", "INDEFINIDO")
        if status_novo_chuva != status_antigo_chuva:
            try:
                nome_ponto = PONTOS_DE_ANALISE[id_ponto]['nome']
                mensagem_log = f"MUDANÇA DE STATUS (Chuva): {nome_ponto} mudou de {status_antigo_chuva} para {status_novo_chuva}."
                data_source.adicionar_log(id_ponto, mensagem_log)
                print(f"| {id_ponto} | {mensagem_log}")
                # No futuro: alertas.enviar_alerta(id_ponto, nome_ponto, status_novo_chuva, status_antigo_chuva, tipo="Chuva")
            except Exception as e:
                print(f"Erro ao gerar log de mudança de status (Chuva): {e}")

        # --- Verificar mudança no status da UMIDADE ---
        status_novo_umidade = status_novo_ponto.get("umidade", "SEM DADOS")
        status_antigo_umidade = status_antigo_ponto.get("umidade", "INDEFINIDO")
        if status_novo_umidade != status_antigo_umidade:
            try:
                nome_ponto = PONTOS_DE_ANALISE[id_ponto]['nome']
                mensagem_log = f"MUDANÇA DE STATUS (Umidade): {nome_ponto} mudou de {status_antigo_umidade} para {status_novo_umidade}."
                data_source.adicionar_log(id_ponto, mensagem_log)
                print(f"| {id_ponto} | {mensagem_log}")
                # No futuro: alertas.enviar_alerta(id_ponto, nome_ponto, status_novo_umidade, status_antigo_umidade, tipo="Umidade")
            except Exception as e:
                print(f"Erro ao gerar log de mudança de status (Umidade): {e}")

        # Atualiza o status do ponto com a nova estrutura completa
        status_atualizado[id_ponto] = status_novo_ponto

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

        # Define um padrão inicial com a nova estrutura de dicionário
        if not status_antigos_do_disco or not isinstance(list(status_antigos_do_disco.values())[0], dict):
            status_antigos_do_disco = {p: {"chuva": "INDEFINIDO", "umidade": "INDEFINIDO"} for p in PONTOS_DE_ANALISE.keys()}

        print(f"WORKER: Início do ciclo. Histórico lido: {len(historico_df)} entradas.")

        # --- LÓGICA DE BACKFILL ---
        if historico_df.empty or historico_df[historico_df['id_ponto'] == 'Ponto-A-KM67'].empty:
            print("[Worker] Histórico do KM 67 (Pro) está vazio. Tentando backfill de 72h...")
            try:
                data_source.backfill_km67_pro_data(historico_df)
                historico_df, _, _ = data_source.get_all_data_from_disk()
                print(f"[Worker] Backfill concluído. Histórico atual: {len(historico_df)} entradas.")
            except Exception as e_backfill:
                data_source.adicionar_log("GERAL", f"ERRO CRÍTICO (Backfill KM 67): {e_backfill}")

        # 2. COLETAR NOVOS DADOS DA API
        novos_dados_df, status_novos_API = data_source.executar_passo_api_e_salvar(historico_df)

        # 3. RECARREGAR O HISTÓRICO COMPLETO APÓS SALVAMENTO
        historico_completo, _, _ = data_source.get_all_data_from_disk()

        if historico_completo.empty:
            print("AVISO: Histórico vazio, pulando cálculo de status.")
            status_atualizado = {p: {"chuva": "SEM DADOS", "umidade": "SEM DADOS"} for p in PONTOS_DE_ANALISE.keys()}
        else:
            status_atualizado = {}
            for id_ponto in PONTOS_DE_ANALISE.keys():
                df_ponto = historico_completo[historico_completo['id_ponto'] == id_ponto].copy()
                
                # --- Status da Chuva ---
                acumulado_72h_df = processamento.calcular_acumulado_rolling(df_ponto, horas=72)
                chuva_72h_final = 0.0
                if not acumulado_72h_df.empty:
                    chuva_72h_final = acumulado_72h_df['chuva_mm'].iloc[-1]
                    status_chuva, _ = processamento.definir_status_chuva(chuva_72h_final)
                else:
                    status_chuva = "SEM DADOS"

                # --- Status da Umidade ---
                ultima_leitura = df_ponto.sort_values('timestamp').iloc[-1] if not df_ponto.empty else None
                if ultima_leitura is not None:
                    status_umidade, _ = processamento.definir_status_umidade_hierarquico(
                        umidade_1m=ultima_leitura.get('umidade_1m'),
                        umidade_2m=ultima_leitura.get('umidade_2m'),
                        umidade_3m=ultima_leitura.get('umidade_3m'),
                        base_1m=ultima_leitura.get('base_1m'),
                        base_2m=ultima_leitura.get('base_2m'),
                        base_3m=ultima_leitura.get('base_3m'),
                        chuva_acumulada_72h=chuva_72h_final
                    )
                else:
                    status_umidade = "SEM DADOS"

                # --- Armazena os status individuais ---
                status_atualizado[id_ponto] = {
                    "chuva": status_chuva,
                    "umidade": status_umidade
                }

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
# --- LÓGICA DE EXECUÇÃO "INTELIGENTE" ---
# ==============================================================================
if __name__ == "__main__":
    data_source.setup_disk_paths()
    print("--- Processo Worker Iniciado (Modo Sincronizado) ---")
    data_source.adicionar_log("GERAL", "Processo Worker iniciado com sucesso.")

    INTERVALO_EM_MINUTOS = 15
    CARENCIA_EM_SEGUNDOS = 60

    while True:
        inicio_total = time.time()
        main_loop()
        tempo_execucao = time.time() - inicio_total

        agora_utc = datetime.datetime.now(datetime.timezone.utc)
        proximo_minuto_base = (agora_utc.minute // INTERVALO_EM_MINUTOS + 1) * INTERVALO_EM_MINUTOS
        proxima_hora_utc = agora_utc

        if proximo_minuto_base >= 60:
            proxima_hora_utc = agora_utc + datetime.timedelta(hours=1)
            proximo_minuto_base = 0

        proxima_execucao_base_utc = proxima_hora_utc.replace(
            minute=proximo_minuto_base,
            second=0,
            microsecond=0
        )
        proxima_execucao_com_carencia_utc = proxima_execucao_base_utc + datetime.timedelta(seconds=CARENCIA_EM_SEGUNDOS)
        tempo_para_dormir_seg = (proxima_execucao_com_carencia_utc - agora_utc).total_seconds()

        if tempo_para_dormir_seg < 0:
            print(f"AVISO: O ciclo demorou {tempo_execucao:.1f}s e perdeu a janela. Rodando novamente...")
            tempo_para_dormir_seg = 1

        print(f"WORKER: Ciclo levou {tempo_execucao:.1f}s.")
        print(
            f"WORKER: Próxima execução às {proxima_execucao_com_carencia_utc.isoformat()}. Dormindo por {tempo_para_dormir_seg:.0f}s...")
        time.sleep(tempo_para_dormir_seg)
