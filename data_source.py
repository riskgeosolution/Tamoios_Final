# data_source.py (CORRIGIDO: Arredondando timestamps para 15 minutos)

import pandas as pd
import json
import os
import datetime
import httpx
import traceback
import hashlib
import hmac
from io import StringIO
import warnings
import time

# Suprime FutureWarnings (como o de concatenação)
warnings.simplefilter(action='ignore', category=FutureWarning)

# Importa as constantes do config.py
from config import (
    PONTOS_DE_ANALISE, CONSTANTES_PADRAO,
    FREQUENCIA_API_SEGUNDOS,
    MAX_HISTORICO_PONTOS,
    WEATHERLINK_CONFIG,
    DB_CONNECTION_STRING
)

# --- Configurações de Disco (Caminhos) ---
DATA_DIR = "."
HISTORICO_FILE_CSV = os.path.join(DATA_DIR, "historico_temp.csv")
STATUS_FILE = os.path.join(DATA_DIR, "status_atual.json")
LOG_FILE = os.path.join(DATA_DIR, "eventos.log")

# Define as colunas esperadas (para consistência)
COLUNAS_HISTORICO = [
    'timestamp', 'id_ponto', 'chuva_mm', 'precipitacao_acumulada_mm',
    'umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc',
    'base_1m', 'base_2m', 'base_3m'
]


# ==========================================================
# --- FUNÇÕES DE LOG E CONFIGURAÇÃO DE CAMINHO ---
# ==========================================================

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


def setup_disk_paths():
    """
    Define o caminho do disco (local ou Render) e imprime para debug.
    Esta função é chamada pelo index.py e worker.py na inicialização.
    """
    print("--- data_source.py ---")
    global DATA_DIR, STATUS_FILE, LOG_FILE, HISTORICO_FILE_CSV

    # Define caminhos baseados no ambiente (Render ou Local)
    if os.environ.get('RENDER'):
        DATA_DIR = "/var/data"
    else:
        DATA_DIR = "."  # Garante que está no diretório local

    # Atualiza os caminhos globais
    STATUS_FILE = os.path.join(DATA_DIR, "status_atual.json")
    LOG_FILE = os.path.join(DATA_DIR, "eventos.log")
    HISTORICO_FILE_CSV = os.path.join(DATA_DIR, "historico_temp.csv")

    print(f"Caminho do Disco de Dados: {DATA_DIR}")
    print(f"Arquivo de Status: {STATUS_FILE}")
    print(f"Arquivo de Log: {LOG_FILE}")
    print(f"Fonte de Histórico: ARQUIVO LOCAL ({HISTORICO_FILE_CSV})")


# ==========================================================
# --- FUNÇÕES DE LEITURA E ESCRITA CSV ---
# ==========================================================

def read_historico_from_csv():
    """ Lê todo o histórico do arquivo CSV local. """
    try:
        historico_df = pd.read_csv(HISTORICO_FILE_CSV, sep=',')

        if 'timestamp' in historico_df.columns:
            historico_df['timestamp'] = pd.to_datetime(historico_df['timestamp'])

        # Garante que apenas colunas conhecidas sejam lidas
        colunas_validas = [col for col in COLUNAS_HISTORICO if col in historico_df.columns]
        historico_df = historico_df[colunas_validas]

        print(f"[CSV] Histórico lido: {len(historico_df)} entradas.")
        return historico_df

    except FileNotFoundError:
        print(f"[CSV] Arquivo '{HISTORICO_FILE_CSV}' não encontrado. Criando novo.")
        return pd.DataFrame(columns=COLUNAS_HISTORICO)
    except Exception as e:
        adicionar_log("CSV_READ", f"ERRO ao ler {HISTORICO_FILE_CSV}: {e}")
        print(f"ERRO ao ler {HISTORICO_FILE_CSV}: {e}")
        traceback.print_exc()
        return pd.DataFrame(columns=COLUNAS_HISTORICO)


def save_historico_to_csv(df):
    """ Salva o DataFrame completo no arquivo CSV. """
    try:
        df.to_csv(HISTORICO_FILE_CSV, index=False)
        print(f"[CSV] Histórico salvo no arquivo.")
    except Exception as e:
        adicionar_log("CSV_SAVE", f"ERRO ao salvar histórico: {e}")
        print(f"ERRO ao salvar CSV: {e}")
        traceback.print_exc()


# ==========================================================
# --- FUNÇÃO DE LEITURA PRINCIPAL PARA O DASHBOARD ---
# ==========================================================

def get_all_data_from_disk():
    """
    Lê o histórico do arquivo CSV (fonte principal) e status/logs dos arquivos.
    """
    # 1. Ler Histórico do CSV
    historico_df = read_historico_from_csv()

    # 2. Ler Status (status_atual.json)
    try:
        with open(STATUS_FILE, 'r', encoding='utf-8') as f:
            status_atual = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        status_atual = {p: "INDEFINIDO" for p in PONTOS_DE_ANALISE.keys()}
    except Exception as e:
        print(f"ERRO ao ler {STATUS_FILE}: {e}.")
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
# --- FUNÇÕES USADAS PELO WORKER.PY ---
# ==========================================================

def executar_passo_api_e_salvar(historico_df):
    """
    Função principal chamada pelo worker.py.
    1. Chama a API
    2. Salva NOVO DADO no histórico CSV
    """

    try:
        # PONTO CRÍTICO: CHAMA A FUNÇÃO DE COLETA DA API (/current)
        dados_api, status_novos, logs_api = fetch_data_from_weatherlink_api()

        for log in logs_api:
            adicionar_log(log['id_ponto'], log['mensagem'])

    except Exception as e:
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

        # 1. CONCATENAR E SALVAR (Fluxo de arquivo CSV)
        historico_atualizado_df = pd.concat([historico_df, novos_df], ignore_index=True)
        historico_atualizado_df['timestamp'] = pd.to_datetime(historico_atualizado_df['timestamp'])

        max_pontos = MAX_HISTORICO_PONTOS * len(PONTOS_DE_ANALISE)

        # --- CORREÇÃO DE DUPLICATAS ---
        # Garante que os timestamps arredondados sejam únicos, mantendo o último registro
        historico_atualizado_df = historico_atualizado_df.sort_values(by='timestamp').drop_duplicates(
            subset=['id_ponto', 'timestamp'], keep='last').tail(max_pontos)
        # --- FIM DA CORREÇÃO ---

        save_historico_to_csv(historico_atualizado_df)

        return novos_df, status_novos

    except Exception as e:
        adicionar_log("GERAL", f"ERRO CRÍTICO (processar/salvar): {e}")
        return pd.DataFrame(), status_novos


# ==========================================================
# --- FUNÇÕES DE ASSINATURA E COLETA DE API ---
# ==========================================================

def calculate_hmac_signature_current(api_key, api_secret, t, station_id):
    """
    Calcula a assinatura HMAC para o endpoint /current.
    (Esta é a versão que funciona, com station-id e t)
    """
    params_para_assinar = {
        "api-key": api_key,
        "station-id": str(station_id),
        "t": str(t)
    }
    string_para_assinar = ""
    for key in sorted(params_para_assinar.keys()):
        string_para_assinar += f"{key}{params_para_assinar[key]}"

    print(f"[HMAC /current] Assinando Mensagem: {string_para_assinar}")

    hmac_digest = hmac.new(
        api_secret.encode('utf-8'),
        string_para_assinar.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()

    return hmac_digest


def calculate_hmac_signature_historic(api_key, api_secret, t, station_id, start_t, end_t):
    """
    Calcula a assinatura HMAC para o endpoint /historic.
    (Esta é a versão que funciona, com station-id e t)
    """
    params_para_assinar = {
        "api-key": api_key,
        "end-timestamp": str(end_t),
        "start-timestamp": str(start_t),
        "station-id": str(station_id),
        "t": str(t)
    }
    string_para_assinar = ""
    for key in sorted(params_para_assinar.keys()):
        string_para_assinar += f"{key}{params_para_assinar[key]}"

    print(f"[HMAC /historic] Assinando Mensagem: {string_para_assinar}")

    hmac_digest = hmac.new(
        api_secret.encode('utf-8'),
        string_para_assinar.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()

    return hmac_digest


# --- INÍCIO DA CORREÇÃO (ARREDONDAMENTO DE TIMESTAMP) ---
def arredondar_timestamp_15min(ts_epoch):
    """ Arredonda um timestamp (em segundos) para baixo, para o intervalo de 15 minutos mais próximo. """
    dt_obj = datetime.datetime.fromtimestamp(ts_epoch, datetime.timezone.utc)
    # Zera segundos e microssegundos, e arredonda os minutos
    dt_obj = dt_obj.replace(second=0, microsecond=0, minute=(dt_obj.minute // 15) * 15)
    return dt_obj.isoformat()


# --- FIM DA CORREÇÃO ---


def fetch_data_from_weatherlink_api():
    """
    Busca dados /CURRENT (incrementais) para TODOS os 4 pontos.
    """
    WEATHERLINK_API_ENDPOINT = "https://api.weatherlink.com/v2/current/{station_id}"

    logs_api = []
    dados_processados = []
    status_calculados = {}

    try:
        from processamento import definir_status_chuva
    except ImportError:
        print("ERRO: Não foi possível importar definir_status_chuva. Verifique processamento.py")
        return [], {}, []

    t = int(datetime.datetime.now(datetime.timezone.utc).timestamp())

    # --- INÍCIO DA CORREÇÃO (ARREDONDAMENTO DE TIMESTAMP) ---
    # Arredonda o timestamp atual ANTES de salvar
    timestamp_arredondado = arredondar_timestamp_15min(t)
    # --- FIM DA CORREÇÃO ---

    print("[API] COLETANDO DADOS DE CHUVA (INCREMENTAL /current) DA WEATHERLINK")

    with httpx.Client(timeout=30.0) as client:
        for id_ponto, config_ponto in PONTOS_DE_ANALISE.items():

            station_config = WEATHERLINK_CONFIG[id_ponto]
            station_id = station_config['STATION_ID']
            api_key = station_config['API_KEY']
            api_secret = station_config['API_SECRET']

            chuva_incremental = pd.NA

            # 1. VERIFICAÇÃO DE CREDENCIAIS / FALLBACK
            if "SUA_CHAVE_API" in api_key or "SEU_SEGREDO_API" in api_secret:
                logs_api.append({"id_ponto": id_ponto,
                                 "mensagem": f"AVISO API: Credenciais para {id_ponto} não preenchidas. Usando Placeholder."})
                simulacao_incremental = {"Ponto-A-KM67": 0.5, "Ponto-B-KM72": 0.0,
                                         "Ponto-C-KM74": 0.0, "Ponto-D-KM81": 0.5}
                chuva_incremental = simulacao_incremental.get(id_ponto, 0.0)

            else:
                # 2. GERAÇÃO DE ASSINATURA E CHAMADA DA API REAL

                signature = calculate_hmac_signature_current(api_key, api_secret, t, station_id)

                params_requisicao = {
                    "api-key": api_key,
                    "t": str(t),
                    "api-signature": signature
                }

                url = WEATHERLINK_API_ENDPOINT.format(station_id=station_id)

                print(f"[API {id_ponto}] URL BASE: {url}. Params: {signature[:10]}...")

                try:
                    response = client.get(url, params=params_requisicao)
                    print(f"[API {id_ponto}] Status: {response.status_code}. Tentando extrair dados...")
                    response.raise_for_status()
                    data = response.json()

                    # Esta verificação 'code: 0' ESTÁ CORRETA para o /current
                    if data.get('code') != 0:
                        raise Exception(f"API retornou erro: {data.get('message', 'Erro desconhecido')}")

                    # 3. EXTRAÇÃO DOS DADOS DE CHUVA INCREMENTAL
                    if data.get('sensors') and data['sensors'][0].get('data'):
                        current_data = data['sensors'][0]['data'][0]

                        chuva_incremental = current_data.get('rain_in_mm')
                        if chuva_incremental is None:
                            chuva_incremental = current_data.get('rainfall_mm', 0.0)

                        if chuva_incremental is None:
                            logs_api.append({"id_ponto": id_ponto,
                                             "mensagem": "API: Métrica de chuva incremental (rain_in_mm) não encontrada. Usando 0.0"})
                            chuva_incremental = 0.0

                        logs_api.append({"id_ponto": id_ponto,
                                         "mensagem": f"API: Sucesso. Chuva incremental: {chuva_incremental:.2f}mm"})
                    else:
                        logs_api.append({"id_ponto": id_ponto, "mensagem": "API: Resposta JSON vazia ou inesperada."})
                        chuva_incremental = 0.0

                except httpx.HTTPStatusError as e:
                    logs_api.append({"id_ponto": id_ponto,
                                     "mensagem": f"ERRO HTTP ({e.response.status_code}): Falha ao coletar dados. Verifique credenciais."})
                    chuva_incremental = 0.0
                except Exception as e:
                    logs_api.append({"id_ponto": id_ponto, "mensagem": f"ERRO CRÍTICO (API /current): {e}"})
                    chuva_incremental = 0.0

            # 4. Cálculo de Status (Provisório)
            status_ponto, _ = definir_status_chuva(0.0)
            status_calculados[id_ponto] = status_ponto

            # 5. Criar os dados brutos
            dados_processados.append({
                # --- INÍCIO DA CORREÇÃO (ARREDONDAMENTO DE TIMESTAMP) ---
                "timestamp": timestamp_arredondado,  # <-- USA O VALOR ARREDONDADO
                # --- FIM DA CORREÇÃO ---
                "id_ponto": id_ponto,
                "chuva_mm": chuva_incremental,
                "precipitacao_acumulada_mm": pd.NA,
                "umidade_1m_perc": pd.NA, "umidade_2m_perc": pd.NA, "umidade_3m_perc": pd.NA,
                "base_1m": pd.NA, "base_2m": pd.NA, "base_3m": pd.NA,
            })

    return dados_processados, status_calculados, logs_api


# --- FUNÇÃO DE BACKFILL (REESCRITA PARA FAZER 3 CHAMADAS DE 24H) ---

def backfill_km67_pro_data():
    """
    Busca os últimos 3 dias (72h) de dados históricos para o KM 67 (Plano Pro)
    fazendo 3 REQUISIÇÕES SEPARADAS DE 24H para respeitar o limite da API.
    """
    id_ponto = "Ponto-A-KM67"
    station_config = WEATHERLINK_CONFIG[id_ponto]
    station_id = station_config['STATION_ID']
    api_key = station_config['API_KEY']
    api_secret = station_config['API_SECRET']

    if "SUA_CHAVE_API" in api_key or "SEU_SEGREDO_API" in api_secret:
        adicionar_log(id_ponto, "AVISO (Backfill): Credenciais não preenchidas. Pulando backfill.")
        return

    print(f"[API {id_ponto}] Iniciando Backfill de 72h (em 3 blocos de 24h)...")

    # URL Base (sem parâmetros)
    url_base = f"https://api.weatherlink.com/v2/historic/{station_id}"
    dados_processados = []  # Lista para agregar dados de todas as 3 chamadas
    now_dt = datetime.datetime.now(datetime.timezone.utc)

    try:
        with httpx.Client(timeout=60.0) as client:

            # Loop para fazer 3 chamadas de 24h
            for i in range(3):  # i = 0 (0-24h), i = 1 (24-48h), i = 2 (48-72h)

                # 1. Definir Timestamps para esta chamada de 24h
                end_dt = now_dt - datetime.timedelta(hours=24 * i)
                start_dt = end_dt - datetime.timedelta(hours=24)  # 24h antes do fim

                end_t = int(end_dt.timestamp())
                start_t = int(start_dt.timestamp())

                # 't' deve ser novo para cada requisição
                t = int(datetime.datetime.now(datetime.timezone.utc).timestamp())

                print(f"[API {id_ponto}] Buscando Bloco {i + 1}/3 ({start_dt.date()} a {end_dt.date()})...")

                # 2. Gerar Assinatura Histórica (Usando a função original, com 't' e 'station-id')
                signature = calculate_hmac_signature_historic(api_key, api_secret, t, station_id, start_t, end_t)

                # 3. Dicionário de parâmetros (o método que funciona)
                params_requisicao = {
                    "api-key": api_key,
                    "api-signature": signature,
                    "end-timestamp": str(end_t),
                    "start-timestamp": str(start_t),
                    "t": str(t)
                }

                # 4. Fazer a requisição
                response = client.get(url_base, params=params_requisicao)

                print(f"[API {id_ponto}] URL (Bloco {i + 1}): {response.url}")
                print(f"[API {id_ponto}] Status (Bloco {i + 1}): {response.status_code}.")

                response.raise_for_status()  # Vai falhar aqui se não for 200
                data = response.json()

                # Removida a verificação 'code: 0' que causava o erro anterior

                # 5. Extrair dados históricos deste bloco (LÓGICA ROBUSTA)

                # Itera por todos os sensores retornados
                for sensor in data.get('sensors', []):
                    # Itera por todos os registros de dados dentro de cada sensor
                    for registro in sensor.get('data', []):

                        # Procura ativamente por uma chave de chuva
                        chuva_incremental = registro.get('rain_in_mm')
                        if chuva_incremental is None:
                            chuva_incremental = registro.get('rainfall_mm')  # Tenta a outra chave

                        # Se (e somente se) este registro continha dados de chuva, nós o salvamos
                        if chuva_incremental is not None:
                            # --- INÍCIO DA CORREÇÃO (ARREDONDAMENTO DE TIMESTAMP) ---
                            ts_registro = arredondar_timestamp_15min(registro['ts'])
                            # --- FIM DA CORREÇÃO ---

                            dados_processados.append({
                                "timestamp": ts_registro,
                                "id_ponto": id_ponto,
                                "chuva_mm": chuva_incremental,
                                # Deixa os outros campos como Nulos (NA)
                                "precipitacao_acumulada_mm": pd.NA,
                                "umidade_1m_perc": pd.NA, "umidade_2m_perc": pd.NA, "umidade_3m_perc": pd.NA,
                                "base_1m": pd.NA, "base_2m": pd.NA, "base_3m": pd.NA,
                            })

                # Pequena pausa para não sobrecarregar a API
                if i < 2:
                    time.sleep(1)

                    # --- FIM DO LOOP ---

        if not dados_processados:
            adicionar_log(id_ponto, "AVISO (Backfill): API não retornou dados históricos (JSON vazio).")
            return

        # 6. Salvar todos os dados agregados no CSV de uma vez
        df_backfill = pd.DataFrame(dados_processados)
        df_backfill = df_backfill[COLUNAS_HISTORICO]

        df_existente = read_historico_from_csv()

        df_final = pd.concat([df_existente, df_backfill], ignore_index=True)
        df_final['timestamp'] = pd.to_datetime(df_final['timestamp'])

        # --- CORREÇÃO DE DUPLICATAS ---
        # Garante que os timestamps arredondados sejam únicos, mantendo o último registro
        df_final = df_final.sort_values(by='timestamp').drop_duplicates(
            subset=['id_ponto', 'timestamp'], keep='last'
        )
        # --- FIM DA CORREÇÃO ---

        save_historico_to_csv(df_final)
        adicionar_log(id_ponto, f"SUCESSO (Backfill): {len(df_backfill)} registros históricos de 72h (3x 24h) salvos.")

    except httpx.HTTPStatusError as e:
        adicionar_log(id_ponto, f"ERRO HTTP (Backfill) ({e.response.status_code}): Falha ao coletar histórico.")
        adicionar_log(id_ponto, f"Resposta da API: {e.response.text}")
    except Exception as e:
        adicionar_log(id_ponto, f"ERRO CRÍTICO (Backfill): {e}")
        traceback.print_exc()