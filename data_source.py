import pandas as pd
import json
import os
import datetime
import httpx
import traceback
from io import StringIO
import hashlib
import hmac  # Necessário para a assinatura HMAC

# Importa as constantes do config.py
from config import (
    PONTOS_DE_ANALISE, CONSTANTES_PADRAO,
    FREQUENCIA_API_SEGUNDOS,
    MAX_HISTORICO_PONTOS,
    WEATHERLINK_CONFIG  # <-- NOVO DICIONÁRIO DE CONFIGURAÇÃO
)

# --- Configurações de Disco (Caminhos) ---
DATA_DIR = "."
HISTORICO_FILE = os.path.join(DATA_DIR, "historico_72h.json")
STATUS_FILE = os.path.join(DATA_DIR, "status_atual.json")
LOG_FILE = os.path.join(DATA_DIR, "eventos.log")

# Define as colunas esperadas (para consistência)
COLUNAS_HISTORICO = [
    'timestamp', 'id_ponto', 'chuva_mm', 'precipitacao_acumulada_mm',
    'umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc',
    'base_1m', 'base_2m', 'base_3m'
]


def setup_disk_paths():
    """ Apenas imprime os caminhos dos arquivos para debug. """
    print("--- data_source.py ---")
    global DATA_DIR, HISTORICO_FILE, STATUS_FILE, LOG_FILE

    if os.environ.get('RENDER'):
        DATA_DIR = "/var/data"
        HISTORICO_FILE = os.path.join(DATA_DIR, "historico_72h.json")
        STATUS_FILE = os.path.join(DATA_DIR, "status_atual.json")
        LOG_FILE = os.path.join(DATA_DIR, "eventos.log")

    print(f"Caminho do Disco de Dados: {DATA_DIR}")
    print(f"Arquivo de Histórico: {HISTORICO_FILE}")
    print(f"Arquivo de Status: {STATUS_FILE}")
    print(f"Arquivo de Log: {LOG_FILE}")


def get_all_data_from_disk():
    """ Lê TODOS os dados (histórico, status, logs) do disco. """

    # 1. Ler Histórico (historico_72h.json)
    try:
        with open(HISTORICO_FILE, 'r', encoding='utf-8') as f:
            historico_json_str = f.read()

        historico_df = pd.read_json(StringIO(historico_json_str), orient='split')

        if 'timestamp' in historico_df.columns:
            historico_df['timestamp'] = pd.to_datetime(historico_df['timestamp'])

    except FileNotFoundError:
        print(f"[Web] Aviso: Não foi possível ler {HISTORICO_FILE} (File does not exist). Retornando dados padrão.")
        historico_df = pd.DataFrame(columns=COLUNAS_HISTORICO)
    except Exception as e:
        print(f"ERRO ao decodificar {HISTORICO_FILE}: {e}. Retornando vazio.")
        traceback.print_exc()
        historico_df = pd.DataFrame(columns=COLUNAS_HISTORICO)

    # 2. Ler Status (status_atual.json)
    try:
        with open(STATUS_FILE, 'r', encoding='utf-8') as f:
            status_atual = json.load(f)
    except FileNotFoundError:
        status_atual = {p: "INDEFINIDO" for p in PONTOS_DE_ANALISE.keys()}
    except Exception as e:
        print(f"ERRO ao ler {STATUS_FILE}: {e}. Retornando dados padrão.")
        traceback.print_exc()
        status_atual = {p: "INDEFINIDO" for p in PONTOS_DE_ANALISE.keys()}

    # 3. Ler Logs (eventos.log)
    try:
        with open(LOG_FILE, 'r', encoding='utf-8') as f:
            logs = f.read()
    except FileNotFoundError:
        logs = "Nenhum evento registrado ainda."
    except Exception:
        logs = "Erro ao ler arquivo de log."

    return historico_df, status_atual, logs


# ==========================================================
# FUNÇÕES USADAS PELO WORKER.PY
# ==========================================================

def executar_passo_api_e_salvar(historico_df):
    """
    Função principal chamada pelo worker.py.
    1. Chama a API
    2. Processa os dados
    3. Salva o histórico atualizado no disco
    4. Retorna os novos dados e status para o worker
    """

    try:
        # CHAMA A FUNÇÃO DE COLETA (COM O NOVO NOME)
        dados_api, status_novos, logs_api = fetch_data_from_weatherlink_api()

        for log in logs_api:
            adicionar_log(log['id_ponto'], log['mensagem'])

    except Exception as e:
        # CORREÇÃO: A mensagem de erro reflete o nome da nova função
        print(f"ERRO CRÍTICO em fetch_data_from_weatherlink_api: {e}")
        traceback.print_exc()
        adicionar_log("GERAL", f"ERRO CRÍTICO (fetch_data): {e}")
        return pd.DataFrame(), None

    if not dados_api:
        print("[Worker] API não retornou novos dados neste ciclo.")
        return pd.DataFrame(), status_novos

    try:
        novos_df = pd.DataFrame(dados_api)

        for col in COLUNAS_HISTORICO:
            if col not in novos_df:
                novos_df[col] = pd.NA
        novos_df = novos_df[COLUNAS_HISTORICO]

        # CORREÇÃO PANDAS WARNING: Evitando a alteração do DF in-place
        historico_atualizado_df = pd.concat([historico_df, novos_df], ignore_index=True)

        historico_atualizado_df['timestamp'] = pd.to_datetime(historico_atualizado_df['timestamp'])

        historico_atualizado_df = historico_atualizado_df.sort_values(by='timestamp').drop_duplicates(
            subset=['id_ponto', 'timestamp'], keep='last')

        historico_atualizado_df = historico_atualizado_df.tail(MAX_HISTORICO_PONTOS * len(PONTOS_DE_ANALISE))

        historico_json_str = historico_atualizado_df.to_json(orient='split', index=False, date_format='iso')
        with open(HISTORICO_FILE, 'w', encoding='utf-8') as f:
            f.write(historico_json_str)

        print("[Worker] Histórico de 72h salvo no disco.")

        return novos_df, status_novos

    except Exception as e:
        print(f"ERRO CRÍTICO ao salvar dados no disco: {e}")
        traceback.print_exc()
        adicionar_log("GERAL", f"ERRO CRÍTICO (salvar disco): {e}")
        return pd.DataFrame(), status_novos


def fetch_data_from_weatherlink_api():
    """
    IMPLEMENTAÇÃO DA API REAL (WeatherLink) - FOCADA APENAS EM CHUVA.
    Dados de Umidade são NULOS.
    """
    WEATHERLINK_API_URL = "https://api.weatherlink.com/v2/current/{}"

    logs_api = []
    dados_processados = []
    status_calculados = {}
    timestamp_atual = datetime.datetime.now(datetime.timezone.utc).isoformat()

    from processamento import definir_status_chuva

    # Placeholder de CHUVA (Simula dados que a API REAL retornaria)
    placeholder_chuva_real = {
        "Ponto-A-KM67": 82.0,
        "Ponto-B-KM72": 10.0,
        "Ponto-C-KM74": 0.0,
        "Ponto-D-KM81": 82.0,
    }

    print("[API] COLETANDO DADOS DE CHUVA DA WEATHERLINK (Umidade = NA)")

    for id_ponto, config_ponto in PONTOS_DE_ANALISE.items():

        station_config = WEATHERLINK_CONFIG[id_ponto]
        api_key = station_config['API_KEY']
        api_secret = station_config['API_SECRET']

        if "SUA_CHAVE_API" in api_key or "SEU_SEGREDO_API" in api_secret:
            logs_api.append({"id_ponto": id_ponto,
                             "mensagem": f"AVISO API: Credenciais para {id_ponto} não preenchidas. Usando Placeholder."})
            chuva_72h = placeholder_chuva_real.get(id_ponto, 0.0)
        else:
            # --- ESPAÇO PARA A LÓGICA DE CHAMADA DA API REAL ---
            # Implementação real de HMAC e httpx.get() virá aqui.
            # Por enquanto, mantemos a simulação.
            chuva_72h = placeholder_chuva_real.get(id_ponto, 0.0)
            logs_api.append({"id_ponto": id_ponto, "mensagem": f"INFO API: Usando dados de chuva simulados."})

        # 1. Calcular Status (baseado apenas na chuva)
        if chuva_72h is None:
            status_ponto = "SEM DADOS"
        else:
            status_ponto, _ = definir_status_chuva(chuva_72h)

        status_calculados[id_ponto] = status_ponto

        # 2. Criar os dados brutos (para o histórico)
        dados_processados.append({
            "timestamp": timestamp_atual,
            "id_ponto": id_ponto,
            "chuva_mm": pd.NA,
            "precipitacao_acumulada_mm": chuva_72h,

            # --- DADOS DE UMIDADE NULOS ---
            "umidade_1m_perc": pd.NA,
            "umidade_2m_perc": pd.NA,
            "umidade_3m_perc": pd.NA,
            "base_1m": pd.NA,
            "base_2m": pd.NA,
            "base_3m": pd.NA,
            # --- FIM DADOS DE UMIDADE NULOS ---
        })

    return dados_processados, status_calculados, logs_api


def adicionar_log(id_ponto, mensagem):
    """
    Adiciona uma entrada de log ao arquivo de log no disco.
    """
    try:
        log_entry = f"{datetime.datetime.now(datetime.timezone.utc).isoformat()} | {id_ponto} | {mensagem}\n"

        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(log_entry)

    except Exception as e:
        print(f"ERRO CRÍTICO ao escrever no log: {e}")
        traceback.print_exc()