import pandas as pd
import time
import datetime
import data_source
import processamento
import alertas
import json
import traceback

# Importa as constantes do arquivo de configuração
from config import PONTOS_DE_ANALISE, RISCO_MAP, FREQUENCIA_API_SEGUNDOS


def verificar_alertas(status_novos, status_antigos):
    """
    Compara os status novos (da API) com os antigos (do disco).
    Dispara alertas (e-mail/SMS) apenas nas transições críticas.
    Preserva a lógica de negócios original.
    """
    if not status_novos:
        print("[Worker] Nenhum status novo recebido da API.")
        return status_antigos  # Retorna o status antigo

    # Garante que o status_antigos seja um dicionário
    if not isinstance(status_antigos, dict):
        print(f"[Worker] status_antigos não era um dicionário (era {type(status_antigos)}). Resetando.")
        status_antigos = {pid: "INDEFINIDO" for pid in PONTOS_DE_ANALISE.keys()}

    status_atualizado = status_antigos.copy()

    for id_ponto, config in PONTOS_DE_ANALISE.items():
        try:
            status_novo = status_novos.get(id_ponto, "SEM DADOS")
            status_antigo = status_antigos.get(id_ponto, "INDEFINIDO")

            # Se o status não mudou, não faz nada
            if status_novo == status_antigo:
                # Opcional: Logar que o status foi verificado (bom para debug)
                # print(f"[{id_ponto}] Status verificado: {status_novo} (Sem mudança)")
                continue

            # ==========================================================
            # --- INÍCIO DA LÓGICA DE ALERTA (PRESERVADA 100%) ---
            # ==========================================================

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

            # --- CORREÇÃO: Removido o 3º argumento ("ERRO", "INFO") ---
            if deve_enviar:
                try:
                    alertas.enviar_alerta(
                        id_ponto,
                        config.get('nome', id_ponto),
                        status_novo,  # Novo Status
                        status_antigo  # Status Anterior
                    )
                    # Loga o SUCESSO do envio
                    data_source.adicionar_log(id_ponto,
                                              f"{tipo_log}: {status_antigo} -> {status_novo} (Notificação ENVIADA)")
                except Exception as e:
                    print(f"AVISO: Falha na notificação para {id_ponto}. Erro: {e}")
                    # Loga a FALHA do envio
                    data_source.adicionar_log(id_ponto,
                                              f"{tipo_log}: {status_antigo} -> {status_novo} (Falha ao enviar notificação: {e})")
            else:
                # Loga a mudança de status que NÃO gera notificação
                data_source.adicionar_log(id_ponto,
                                          f"{tipo_log}: {status_antigo} -> {status_novo} (Notificação NÃO enviada)")

            # ==========================================================
            # --- FIM DA LÓGICA DE ALERTA ---
            # ==========================================================

            # Atualiza o status (para garantir envio único)
            status_atualizado[id_ponto] = status_novo

            # --- Lógica de verificação de mudança de base (Preservada) ---
            try:
                # (Esta lógica assume que _parse_api_data retorna as bases)
                dados_api, _, _ = data_source.fetch_data_from_zentra_api()
                if dados_api:
                    dado_ponto = next((d for d in dados_api if d['id_ponto'] == id_ponto), None)
                    if dado_ponto:
                        antiga_base_1m = status_antigos.get(f"base_1m_{id_ponto}", 0)
                        nova_base_1m = dado_ponto.get('base_1m', antiga_base_1m)
                        if nova_base_1m != antiga_base_1m:
                            # --- CORREÇÃO: Removido o 3º argumento ---
                            data_source.adicionar_log(id_ponto,
                                                      f"MUDANÇA DE BASE (1m): {antiga_base_1m} -> {nova_base_1m}")
                            status_atualizado[f"base_1m_{id_ponto}"] = nova_base_1m
            except Exception as e_base:
                print(f"[Worker] Erro ao verificar mudança de base para {id_ponto}: {e_base}")


        except Exception as e:
            print(f"ERRO CRÍTICO no loop de verificação de alertas para {id_ponto}: {e}")
            traceback.print_exc()
            # --- CORREÇÃO: Removido o 3º argumento ---
            data_source.adicionar_log(id_ponto, f"ERRO CRÍTICO no loop de verificação: {e}")

    return status_atualizado


def main_loop():
    """
    Loop principal do Worker.
    """
    print(
        f"\nWORKER: {datetime.datetime.now(datetime.timezone.utc).isoformat()} - Iniciando ciclo de {FREQUENCIA_API_SEGUNDOS} segundos...")

    # 1. Ler o histórico e o status ANTES de chamar a API
    try:
        # Lê o histórico (JSON String)
        with open(data_source.HISTORICO_FILE, 'r', encoding='utf-8') as f:
            historico_json = f.read()
        # Converte para DataFrame
        historico_df = pd.read_json(StringIO(historico_json), orient='split')

    except FileNotFoundError:
        print("[Worker] Arquivo de histórico não encontrado. Iniciando com DataFrame vazio.")
        historico_df = pd.DataFrame(columns=data_source.COLUNAS_HISTORICO)
    except Exception as e:
        print(f"ERRO ao ler histórico: {e}. Iniciando com DataFrame vazio.")
        historico_df = pd.DataFrame(columns=data_source.COLUNAS_HISTORICO)

    try:
        # Lê o status
        with open(data_source.STATUS_FILE, 'r', encoding='utf-8') as f:
            status_antigos = json.load(f)
    except FileNotFoundError:
        print("[Worker] Arquivo de status não encontrado. Criando estado inicial.")
        status_antigos = {pid: "INDEFINIDO" for pid in PONTOS_DE_ANALISE.keys()}
    except Exception as e:
        print(f"ERRO ao ler status: {e}. Criando estado inicial.")
        status_antigos = {pid: "INDEFINIDO" for pid in PONTOS_DE_ANALISE.keys()}

    # 2. Executar o passo da API (Chamar API, Processar, Salvar no Disco)
    # (Passamos o histórico_df para evitar reler o disco)
    novos_dados_df, status_novos = data_source.executar_passo_api_e_salvar(historico_df)

    # 3. Verificar Alertas (Comparar novos e antigos)
    status_atualizado = verificar_alertas(status_novos, status_antigos)

    # 4. Salvar o novo status no disco
    try:
        with open(data_source.STATUS_FILE, 'w', encoding='utf-8') as f:
            json.dump(status_atualizado, f, indent=2)
    except Exception as e:
        print(f"ERRO CRÍTICO ao salvar status no disco: {e}")
        traceback.print_exc()
        # --- CORREÇÃO: Removido o 3º argumento ---
        data_source.adicionar_log("GERAL", f"ERRO CRÍTICO ao salvar status: {e}")

    print(f"WORKER: Ciclo concluído em {time.time() - inicio_ciclo:.2f}s.")


if __name__ == "__main__":
    # Garante que os caminhos são definidos antes de tudo
    data_source.setup_disk_paths()
    print("--- Processo Worker Iniciado ---")

    # --- CORREÇÃO: Removido o 3º argumento ("INFO") ---
    # Log de inicialização
    data_source.adicionar_log("GERAL", "Processo Worker iniciado com sucesso.")

    # Loop infinito do Worker
    while True:
        inicio_ciclo = time.time()
        try:
            main_loop()  # Executa o ciclo principal

        except Exception as e:
            print(f"WORKER ERRO CRÍTICO no loop principal: {e}")
            traceback.print_exc()
            # --- CORREÇÃO: Removido o 3º argumento ---
            data_source.adicionar_log("GERAL", f"ERRO CRÍTICO (loop principal): {e}")

        # Calcula o tempo restante e dorme
        tempo_execucao = time.time() - inicio_ciclo
        tempo_espera = FREQUENCIA_API_SEGUNDOS - tempo_execucao

        if tempo_espera > 0:
            print(f"WORKER: Aguardando {tempo_espera:.0f}s até o próximo ciclo...")
            time.sleep(tempo_espera)
        else:
            print(
                f"AVISO WORKER: O ciclo levou {tempo_execucao:.0f}s (mais que a frequência de {FREQUENCIA_API_SEGUNDOS}s).")
            # --- CORREÇÃO: Removido o 3º argumento ---
            data_source.adicionar_log("GERAL", f"AVISO: Ciclo levou {tempo_execucao:.0f}s, (maior que a frequência).")

