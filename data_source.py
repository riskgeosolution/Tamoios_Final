# data_source.py (COMPLETO E FINALMENTE CORRIGIDO: Assinatura HMAC na Query)

import pandas as pd
import json
import os
import datetime
import httpx
import traceback
import hashlib
import hmac
from io import StringIO
from sqlalchemy import create_engine, text
import sqlite3
import warnings  # Adicionado para suprimir FutureWarnings do Pandas

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

# --- Configurações de Disco (AGORA SÓ PARA STATUS E LOG) ---
DATA_DIR = "."
STATUS_FILE = os.path.join(DATA_DIR, "status_atual.json")
LOG_FILE = os.path.join(DATA_DIR, "eventos.log")
TABELA_DADOS = "dados_monitoramento"

# Define as colunas esperadas (para consistência)
COLUNAS_HISTORICO = [
    'timestamp', 'id_ponto', 'chuva_mm', 'precipitacao_acumulada_mm',
    'umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc',
    'base_1m', 'base_2m', 'base_3m'
]


# ==========================================================
# --- FUNÇÕES DE CONEXÃO E ESTRUTURA DO BANCO DE DADOS ---
# ==========================================================

def get_db_engine():
    """ Cria e retorna o objeto Engine do SQLAlchemy. """
    try:
        engine = create_engine(DB_CONNECTION_STRING)
        with engine.connect():
            pass
        return engine
    except Exception as e:
        adicionar_log("DB_SETUP", f"ERRO ao criar Engine DB: {e}")
        print(f"ERRO ao criar Engine DB: {e}")
        traceback.print_exc()
        return None


def setup_db_table(engine):
    """ Garante que a tabela de dados exista no banco. """

    timestamp_type = "DATETIME" if DB_CONNECTION_STRING.startswith("sqlite") else "TIMESTAMP WITH TIME ZONE"

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {TABELA_DADOS} (
        timestamp {timestamp_type} NOT NULL,
        id_ponto VARCHAR(50) NOT NULL,
        chuva_mm REAL,
        precipitacao_acumulada_mm REAL,
        umidade_1m_perc REAL,
        umidade_2m_perc REAL,
        umidade_3m_perc REAL,
        base_1m REAL,
        base_2m REAL,
        base_3m REAL,
        PRIMARY KEY (timestamp, id_ponto)
    );
    """
    try:
        with engine.connect() as connection:
            connection.execute(text(create_table_sql))
            connection.commit()
        print(f"[DB] Tabela '{TABELA_DADOS}' verificada/criada com sucesso.")
    except Exception as e:
        adicionar_log("DB_SETUP", f"ERRO ao configurar tabela: {e}")
        print(f"ERRO ao configurar tabela: {e}")
        traceback.print_exc()


def save_new_data_to_db(novos_df, engine):
    """ Salva o novo DataFrame (dados incrementais) no banco de dados. """
    try:
        novos_df.to_sql(
            TABELA_DADOS,
            con=engine,
            if_exists='append',
            index=False
        )
        print(f"[DB] {len(novos_df)} novos registros salvos no banco.")
    except Exception as e:
        if "UNIQUE constraint failed" in str(e) or "duplicate key value violates" in str(e):
            adicionar_log("DB_SAVE", f"AVISO: Tentativa de salvar dado duplicado (Timestamp/ID). Ignorando.")
        else:
            adicionar_log("DB_SAVE", f"ERRO ao salvar dados: {e}")
            print(f"ERRO ao salvar dados no DB: {e}")
            traceback.print_exc()


def get_historic_data_from_db():
    """ Lê TODO o histórico dos últimos 72h + 1h do banco. """
    engine = get_db_engine()
    if engine is None:
        return pd.DataFrame(columns=COLUNAS_HISTORICO)

    time_limit = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=73)
    time_limit_str = time_limit.isoformat().replace('+00:00', 'Z')

    read_sql = f"""
    SELECT * FROM {TABELA_DADOS} 
    WHERE timestamp >= '{time_limit_str}'
    ORDER BY timestamp ASC;
    """

    try:
        historico_df = pd.read_sql(read_sql, engine)

        if 'timestamp' in historico_df.columns:
            historico_df['timestamp'] = pd.to_datetime(historico_df['timestamp'], utc=True)

        print(f"[DB] Histórico de 72h+ lido: {len(historico_df)} entradas.")
        return historico_df

    except Exception as e:
        adicionar_log("DB_READ", f"ERRO ao ler histórico: {e}")
        print(f"ERRO ao ler histórico do DB: {e}")
        traceback.print_exc()
        return pd.DataFrame(columns=COLUNAS_HISTORICO)


# ==========================================================
# --- FUNÇÃO DE LEITURA PRINCIPAL PARA O DASHBOARD ---
# ==========================================================

def get_all_data_from_disk():
    """
    Lê o histórico do DB (fonte principal) e status/logs dos arquivos.
    """
    # 1. Ler Histórico do DB
    historico_df = get_historic_data_from_db()

    # 2. Ler Status (status_atual.json) - Mantido no disco para o Dashboard
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
    """
    engine = get_db_engine()
    if engine is None:
        adicionar_log("GERAL", "Falha na conexão com o DB. Pulando ciclo.")
        return pd.DataFrame(), None

    setup_db_table(engine)

    try:
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

        save_new_data_to_db(novos_df, engine)

        return novos_df, status_novos

    except Exception as e:
        adicionar_log("GERAL", f"ERRO CRÍTICO (processar/salvar): {e}")
        return pd.DataFrame(), status_novos


def setup_disk_paths():
    """ Define o caminho do disco (local ou Render) e imprime para debug. """
    print("--- data_source.py ---")
    global DATA_DIR, STATUS_FILE, LOG_FILE

    if os.environ.get('RENDER'):
        DATA_DIR = "/var/data"
        STATUS_FILE = os.path.join(DATA_DIR, "status_atual.json")
        LOG_FILE = os.path.join(DATA_DIR, "eventos.log")

    print(f"Caminho do Disco de Dados: {DATA_DIR}")
    print(f"Arquivo de Status: {STATUS_FILE}")
    print(f"Arquivo de Log: {LOG_FILE}")
    print(f"Fonte de Histórico: SQLite/PostgreSQL Tabela '{TABELA_DADOS}'")


def calculate_hmac_signature(api_key, api_secret, t, station_id):
    """
    Calcula a assinatura HMAC-SHA256 para autenticação na WeatherLink API v2.
    CORREÇÃO: Inclui api-key, station-id e t no hash (Ordem Alfabética).
    """

    params_para_assinar = {
        "api-key": api_key,
        "station-id": str(station_id),
        "t": str(t)
    }

    string_para_assinar = ""
    for key in sorted(params_para_assinar.keys()):
        string_para_assinar += f"{key}{params_para_assinar[key]}"

    print(f"[HMAC DEBUG] Assinando Mensagem: {string_para_assinar}")

    hmac_digest = hmac.new(
        api_secret.encode('utf-8'),
        string_para_assinar.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()

    return hmac_digest


def fetch_data_from_weatherlink_api():
    """
    IMPLEMENTAÇÃO DA API REAL (WeatherLINK) - USANDO REQUISIÇÃO E PARÂMETROS DO TESTE DE SUCESSO.
    """
    # A URL BASE NÃO DEVE CONTER PARAMETROS DE QUERY, POIS SERÃO ENVIADOS VIA PARAMS ABAIXO.
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
    timestamp_atual = datetime.datetime.fromtimestamp(t, datetime.timezone.utc).isoformat()

    print("[API] COLETANDO DADOS DE CHUVA (INCREMENTAL) DA WEATHERLINK")

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

                # Placeholder de CHUVA Incremental (Simula 0.5mm de chuva)
                simulacao_incremental = {"Ponto-A-KM67": 0.5, "Ponto-B-KM72": 0.0,
                                         "Ponto-C-KM74": 0.0, "Ponto-D-KM81": 0.5}
                chuva_incremental = simulacao_incremental.get(id_ponto, 0.0)

            else:
                # 2. GERAÇÃO DE ASSINATURA E CHAMADA DA API REAL

                signature = calculate_hmac_signature(api_key, api_secret, t, station_id)

                # 2.1 CONSTRUÇÃO DOS PARÂMETROS (Igual ao script de sucesso)
                params_requisicao = {
                    "api-key": api_key,
                    "t": str(t),
                    "api-signature": signature
                }

                # A URL DEVE INCLUIR APENAS O STATION_ID NO PATH
                url = WEATHERLINK_API_ENDPOINT.format(station_id=station_id)

                print(f"[API {id_ponto}] URL BASE: {url}. Params: {signature[:10]}...")

                try:
                    # REQUISIÇÃO PASSANDO PARÂMETROS VIA 'params='
                    response = client.get(url, params=params_requisicao)

                    # LOG DE RESPOSTA HTTP (CRÍTICO)
                    print(f"[API {id_ponto}] Status: {response.status_code}. Tentando extrair dados...")

                    response.raise_for_status()
                    data = response.json()

                    if data.get('code') != 0:
                        raise Exception(f"API retornou erro: {data.get('message', 'Erro desconhecido')}")

                    # 3. EXTRAÇÃO DOS DADOS DE CHUVA INCREMENTAL
                    if data.get('sensors') and data['sensors'][0].get('data'):
                        current_data = data['sensors'][0]['data'][0]

                        chuva_incremental = current_data.get('rain_in_mm')

                        if chuva_incremental is None:
                            chuva_incremental = current_data.get('rainfall_mm')

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
                    logs_api.append({"id_ponto": id_ponto, "mensagem": f"ERRO CRÍTICO (API): {e}"})
                    chuva_incremental = 0.0

            # 4. Cálculo de Status (Provisório)
            status_ponto, _ = definir_status_chuva(0.0)
            status_calculados[id_ponto] = status_ponto

            # 5. Criar os dados brutos (para o DB)
            dados_processados.append({
                "timestamp": timestamp_atual,
                "id_ponto": id_ponto,
                "chuva_mm": chuva_incremental,
                "precipitacao_acumulada_mm": pd.NA,

                # --- DADOS DE UMIDADE NULOS ---
                "umidade_1m_perc": pd.NA, "umidade_2m_perc": pd.NA, "umidade_3m_perc": pd.NA,
                "base_1m": pd.NA, "base_2m": pd.NA, "base_3m": pd.NA,
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