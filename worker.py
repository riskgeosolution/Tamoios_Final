import pandas as pd
import time
import datetime
import data_source
import processamento
import alertas
import json
import traceback
from io import StringIO

# Importa as constantes do arquivo de configuração
from config import PONTOS_DE_ANALISE, RISCO_MAP, FREQUENCIA_API_SEGUNDOS


def verificar_alertas(status_novos, status_antigos):
    """
    Compara os status novos (da API) com os antigos (do disco).
    Dispara alertas (e-mail/SMS) apenas nas transições críticas.
    """

    if not status_novos:
        print("[Worker] Nenhum status novo recebido da API.")
        return status_antigos

    if not isinstance(status_antigos, dict):
        print(f"[Worker] status_antigos não era um dicionário (era {type(status_antigos)}). Resetando.")
        status_antigos = {pid: "INDEFINIDO" for pid in PONTOS_DE_ANALISE.keys()}

    status_atualizado = status_antigos.copy()

    for id_ponto, config in PONTOS_DE_ANALISE.items():
        try:
            status_novo = status_novos.get(id_ponto, "SEM DADOS")
            status_antigo = status_antigos.get(id_ponto, "INDEFINIDO")

            if status_novo == status_antigo:
                continue

            # --- INÍCIO DA LÓGICA DE ALERTA ---
            print(f"[{id_ponto}] MUDANÇA DE STATUS DETECTADA: {status_antigo} -> {status_novo}")

            deve_enviar = False
            tipo_log = "MUDANÇA DE STATUS"

            # REGRA 1: ALERTA -> PARALIZAÇÃO (Crítico)
            if status_novo == "PARALIZAÇÃO" and status_antigo == "ALERTA":
                deve_enviar = True
                tipo_log = "MUDANÇA DE STATUS CRÍTICA"
                print(
                    f">>> Transição CRÍTICA {id_ponto} ({status_antigo}->{status_novo}) detectada. Disparando alarme.")

            # REGRA 2: ATENÇÃO -> LIVRE (Retorno à Normalidade)
            elif status_novo == "LIVRE" and status_antigo == "ATENÇÃO":
                deve_enviar = True
                tipo_log = "MUDANÇA DE STATUS NORMALIZADA"
                print(
                    f">>> Transição de NORMALIZAÇÃO {id_ponto} ({status_antigo}->{status_novo}) detectada. Disparando alarme.")

            if deve_enviar:
                try:
                    alertas.enviar_alerta(
                        id_ponto,
                        config.get('nome', id_ponto),
                        status_novo,
                        status_antigo
                    )
                    data_source.adicionar_log(id_ponto,
                                              f"{tipo_log}: {status_antigo} -> {status_novo} (Notificação ENVIADA)")
                except Exception as e:
                    print(f"AVISO: Falha na notificação para {id_ponto}. Erro: {e}")
                    data_source.adicionar_log(id_ponto,
                                              f"{tipo_log}: {status_antigo} -> {status_novo} (Falha ao enviar notificação: {e})")
            else:
                data_source.adicionar_log(id_ponto,
                                          f"{tipo_log}: {status_antigo} -> {status_novo} (Notificação NÃO enviada)")

            # --- FIM DA LÓGICA DE ALERTA ---

            # Atualiza o status
            status_atualizado[id_ponto] = status_novo

            # --- Lógica de verificação de mudança de base (CORRIGIDA) ---
            # REMOVIDA A CHAMADA PROBLEMÁTICA À API AQUI. O novo dado (novos_df) JÁ foi lido
            # no main_loop, e a lógica de base de umidade não se aplica, pois os dados são NULOS.
            # Se a lógica de base for implementada, ela deve ser feita com base no 'novos_df'
            # retornado por executar_passo_api_e_salvar, não chamando a API novamente.
            # Mantendo a lógica de log de mudança de base como existia:
            try:
                # O Worker não deve chamar a API aqui. A lógica de log de base está
                # incompleta porque os dados de umidade são NA, mas mantemos
                # a estrutura para evitar NameError desnecessário.
                pass

            except Exception as e_base:
                print(f"[Worker] Erro ao verificar mudança de base para {id_ponto}: {e_base}")


        except Exception as e:
            print(f"ERRO CRÍTICO no loop de verificação de alertas para {id_ponto}: {e}")
            traceback.print_exc()
            data_source.adicionar_log("GERAL", f"ERRO CRÍTICO (loop de verificação): {e}")

    return status_atualizado


def main_loop():
    """
    Loop principal do Worker.
    """
    print(
        f"\nWORKER: {datetime.datetime.now(datetime.timezone.utc).isoformat()} - Iniciando ciclo de {FREQUENCIA_API_SEGUNDOS} segundos...")

    try:
        with open(data_source.HISTORICO_FILE, 'r', encoding='utf-8') as f:
            historico_json = f.read()
        historico_df = pd.read_json(StringIO(historico_json), orient='split')

    except FileNotFoundError:
        print("[Worker] Arquivo de histórico não encontrado. Iniciando com DataFrame vazio.")
        historico_df = pd.DataFrame(columns=data_source.COLUNAS_HISTORICO)
    except Exception as e:
        print(f"ERRO ao ler histórico: {e}. Iniciando com DataFrame vazio.")
        historico_df = pd.DataFrame(columns=data_source.COLUNAS_HISTORICO)

    try:
        with open(data_source.STATUS_FILE, 'r', encoding='utf-8') as f:
            status_antigos = json.load(f)
    except FileNotFoundError:
        status_antigos = {pid: "INDEFINIDO" for pid in PONTOS_DE_ANALISE.keys()}
    except Exception as e:
        print(f"ERRO ao ler status: {e}. Criando estado inicial.")
        status_antigos = {pid: "INDEFINIDO" for pid in PONTOS_DE_ANALISE.keys()}

    inicio_ciclo = time.time()  # Deve ser definido aqui para o cálculo de tempo de execução

    # CHAMA A FUNÇÃO CORRIGIDA EM data_source
    novos_dados_df, status_novos = data_source.executar_passo_api_e_salvar(historico_df)

    status_atualizado = verificar_alertas(status_novos, status_antigos)

    try:
        with open(data_source.STATUS_FILE, 'w', encoding='utf-8') as f:
            json.dump(status_atualizado, f, indent=2)
    except Exception as e:
        print(f"ERRO CRÍTICO ao salvar status no disco: {e}")
        traceback.print_exc()
        data_source.adicionar_log("GERAL", f"ERRO CRÍTICO ao salvar status: {e}")

    print(f"WORKER: Ciclo concluído em {time.time() - inicio_ciclo:.2f}s.")


if __name__ == "__main__":
    data_source.setup_disk_paths()
    print("--- Processo Worker Iniciado ---")
    data_source.adicionar_log("GERAL", "Processo Worker iniciado com sucesso.")

    while True:
        inicio_ciclo = time.time()
        try:
            main_loop()

        except Exception as e:
            print(f"WORKER ERRO CRÍTICO no loop principal: {e}")
            traceback.print_exc()
            data_source.adicionar_log("GERAL", f"ERRO CRÍTICO (loop principal): {e}")

        tempo_execucao = time.time() - inicio_ciclo
        tempo_espera = FREQUENCIA_API_SEGUNDOS - tempo_execucao

        if tempo_espera > 0:
            print(f"WORKER: Aguardando {tempo_espera:.0f}s até o próximo ciclo...")
            time.sleep(tempo_espera)
        else:
            print(
                f"AVISO WORKER: O ciclo levou {tempo_execucao:.0f}s (mais que a frequência de {FREQUENCIA_API_SEGUNDOS}s).")
            data_source.adicionar_log("GERAL", f"AVISO: Ciclo levou {tempo_execucao:.0f}s, (maior que a frequência).")