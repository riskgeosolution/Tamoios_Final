# data_source.py (CORRIGIDO v8: Usando o Timestamp 'ts' da API)

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
from sqlalchemy import create_engine, inspect, text

warnings.simplefilter(action='ignore', category=FutureWarning)

from config import (
    PONTOS_DE_ANALISE, CONSTANTES_PADRAO,
    FREQUENCIA_API_SEGUNDOS,
    MAX_HISTORICO_PONTOS,
    WEATHERLINK_CONFIG,
    DB_CONNECTION_STRING,
    DB_TABLE_NAME
)

# --- Configurações de Disco (Caminhos) ---
DATA_DIR = "."
HISTORICO_FILE_CSV = os.path.join(DATA_DIR, "historico_temp.csv")
STATUS_FILE = os.path.join(DATA_DIR, "status_atual.json")
LOG_FILE = os.path.join(DATA_DIR, "eventos.log")

COLUNAS_HISTORICO = [
    'timestamp', 'id_ponto', 'chuva_mm', 'precipitacao_acumulada_mm',
    'umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc',
    'base_1m', 'base_2m', 'base_3m'
]


# ==========================================================
# --- FUNÇÕES DE LOG E CONFIGURAÇÃO DE CAMINHO ---
# ==========================================================

def adicionar_log(id_ponto, mensagem):
    try:
        log_entry = f"{datetime.datetime.now(datetime.timezone.utc).isoformat()} | {id_ponto} | {mensagem}\n"
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(log_entry)
    except Exception as e:
        print(f"ERRO CRÍTICO ao escrever no log: {e}")
        traceback.print_exc()


def setup_disk_paths():
    print("--- data_source.py ---")
    global DATA_DIR, STATUS_FILE, LOG_FILE, HISTORICO_FILE_CSV, DB_CONNECTION_STRING

    if os.environ.get('RENDER'):
        DATA_DIR = "/var/data"
        DB_CONNECTION_STRING = f'sqlite:///{DATA_DIR}/temp_local_db.db'
    else:
        DATA_DIR = "."

    STATUS_FILE = os.path.join(DATA_DIR, "status_atual.json")
    LOG_FILE = os.path.join(DATA_DIR, "eventos.log")
    HISTORICO_FILE_CSV = os.path.join(DATA_DIR, "historico_temp.csv")

    print(f"Caminho do Disco de Dados: {DATA_DIR}")
    print(f"Arquivo de Status: {STATUS_FILE}")
    print(f"Arquivo de Log: {LOG_FILE}")
    print(f"Fonte de Histórico (Leitura): ARQUIVO LOCAL ({HISTORICO_FILE_CSV})")
    print(f"Banco de Dados (Escrita): {DB_CONNECTION_STRING}")


# ==========================================================
# --- FUNÇÕES DE BANCO de DADOS (SQLITE) ---
# ==========================================================

def get_engine():
    return create_engine(DB_CONNECTION_STRING)


def save_to_sqlite(df_novos_dados):
    if df_novos_dados.empty:
        return
    try:
        engine = get_engine()
        df_novos_dados.to_sql(DB_TABLE_NAME, engine, if_exists='append', index=False)
        print(f"[SQLite] {len(df_novos_dados)} novos pontos salvos no DB.")
    except Exception as e:
        # Erro comum é 'UNIQUE constraint failed' se o dado já existe.
        if "UNIQUE constraint failed" in str(e):
            print(f"[SQLite] Aviso: Dados duplicados para este timestamp. Ignorando.")
        else:
            adicionar_log("GERAL", f"ERRO CRÍTICO ao salvar no SQLite: {e}")
            print(f"ERRO CRÍTICO ao salvar no SQLite: {e}")


def migrate_csv_to_sqlite_initial():
    engine = get_engine()
    inspector = inspect(engine)
    try:
        if inspector.has_table(DB_TABLE_NAME):
            with engine.connect() as connection:
                query = text(f"SELECT COUNT(1) FROM {DB_TABLE_NAME}")
                result = connection.execute(query)
                count = result.scalar()
                if count > 0:
                    print(f"[MIGRAÇÃO] Tabela SQLite '{DB_TABLE_NAME}' já contém {count} linhas. Migração ignorada.")
                    return True
    except Exception as e:
        print(f"[MIGRAÇÃO] Erro ao verificar tabela SQLite ({e}). Tentando migrar...")
    df_csv = read_historico_from_csv()
    if df_csv.empty:
        print("[MIGRAÇÃO] CSV histórico vazio. Migração concluída (sem dados).")
        return True
    try:
        df_csv.to_sql(DB_TABLE_NAME, engine, if_exists='replace', index=False)
        print(f"[MIGRAÇÃO] SUCESSO! {len(df_csv)} linhas transferidas do CSV para o SQLite.")
        return True
    except Exception as e:
        adicionar_log("GERAL", f"ERRO CRÍTICO na migração CSV->SQLite: {e}")
        print(f"ERRO CRÍTICO na migração CSV->SQLite: {e}")
        return False


def read_data_from_sqlite(id_ponto, start_dt, end_dt):
    print(f"[SQLite] Consultando dados para {id_ponto} de {start_dt} a {end_dt}")
    engine = get_engine()
    start_str = start_dt.isoformat()
    end_str = end_dt.isoformat()
    query = f"""
        SELECT * FROM {DB_TABLE_NAME}
        WHERE id_ponto = :ponto
        AND timestamp >= :start
        AND timestamp <= :end
        ORDER BY timestamp ASC
    """
    try:
        df = pd.read_sql_query(
            query,
            engine,
            params={"ponto": id_ponto, "start": start_str, "end": end_str},
            parse_dates=["timestamp"]
        )
        return df
    except Exception as e:
        print(f"ERRO CRÍTICO ao ler do SQLite: {e}")
        adicionar_log("GERAL", f"ERRO CRÍTICO ao ler do SQLite: {e}")
        return pd.DataFrame()


# ==========================================================
# --- FUNÇÕES DE LEITURA E ESCRITA CSV (PARA TESTES) ---
# ==========================================================

def read_historico_from_csv():
    try:
        historico_df = pd.read_csv(HISTORICO_FILE_CSV, sep=',')
        if 'timestamp' in historico_df.columns:
            historico_df['timestamp'] = pd.to_datetime(historico_df['timestamp'], utc=True)
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
    try:
        # --- CORREÇÃO (Salvar duplicados) ---
        # Garante que timestamps duplicados sejam removidos ANTES de salvar
        df_sem_duplicatas = df.sort_values(by='timestamp').drop_duplicates(
            subset=['id_ponto', 'timestamp'], keep='last')

        # Aplica o truncamento de 72h
        max_pontos = MAX_HISTORICO_PONTOS * len(PONTOS_DE_ANALISE)
        df_truncado = df_sem_duplicatas.tail(max_pontos)

        df_truncado.to_csv(HISTORICO_FILE_CSV, index=False)
        print(f"[CSV] Histórico salvo no arquivo (Mantidas {len(df_truncado)} entradas).")
    except Exception as e:
        adicionar_log("CSV_SAVE", f"ERRO ao salvar histórico: {e}")
        print(f"ERRO ao salvar CSV: {e}")
        traceback.print_exc()


# ==========================================================
# --- FUNÇÃO DE LEITURA PRINCIPAL PARA O DASHBOARD ---
# ==========================================================

def get_all_data_from_disk():
    historico_df = read_historico_from_csv()
    try:
        with open(STATUS_FILE, 'r', encoding='utf-8') as f:
            status_atual = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        status_atual = {p: "INDEFINIDO" for p in PONTOS_DE_ANALISE.keys()}
    except Exception as e:
        print(f"ERRO ao ler {STATUS_FILE}: {e}.")
        status_atual = {p: "INDEFINIDO" for p in PONTOS_DE_ANALISE.keys()}
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

def executar_passo_api_e_salvar(historico_df_csv):
    try:
        dados_api_df, status_novos, logs_api = fetch_data_from_weatherlink_api(historico_df_csv)
        for log in logs_api:
            mensagem_log_completa = f"| {log['id_ponto']} | {log['mensagem']}"
            print(mensagem_log_completa)
            adicionar_log(log['id_ponto'], log['mensagem'])
    except Exception as e:
        adicionar_log("GERAL", f"ERRO CRÍTICO (fetch_data): {e}")
        traceback.print_exc()
        return pd.DataFrame(), None

    if dados_api_df.empty:
        print("[Worker] API não retornou novos dados neste ciclo.")
        return pd.DataFrame(), status_novos

    try:
        for col in COLUNAS_HISTORICO:
            if col not in dados_api_df:
                dados_api_df[col] = pd.NA
        dados_api_df = dados_api_df[COLUNAS_HISTORICO]

        if 'timestamp' in dados_api_df.columns:
            dados_api_df['timestamp'] = pd.to_datetime(dados_api_df['timestamp'], utc=True)

        dados_api_df['chuva_mm'] = pd.to_numeric(dados_api_df['chuva_mm'], errors='coerce').fillna(0.0)
        dados_api_df['precipitacao_acumulada_mm'] = pd.to_numeric(dados_api_df['precipitacao_acumulada_mm'],
                                                                  errors='coerce')

        save_to_sqlite(dados_api_df)

        historico_atualizado_df_csv = pd.concat([historico_df_csv, dados_api_df], ignore_index=True)
        # (A conversão de timestamp já foi feita)
        save_historico_to_csv(historico_atualizado_df_csv)

        return dados_api_df, status_novos

    except Exception as e:
        adicionar_log("GERAL", f"ERRO CRÍTICO (processar/salvar): {e}")
        traceback.print_exc()
        return pd.DataFrame(), status_novos


# ==========================================================
# --- FUNÇÕES DE ASSINATURA E COLETA de API ---
# ==========================================================

def calculate_hmac_signature_current(api_key, api_secret, t, station_id):
    params_para_assinar = {"api-key": api_key, "station-id": str(station_id), "t": str(t)}
    string_para_assinar = ""
    for key in sorted(params_para_assinar.keys()):
        string_para_assinar += f"{key}{params_para_assinar[key]}"
    hmac_digest = hmac.new(api_secret.encode('utf-8'), string_para_assinar.encode('utf-8'), hashlib.sha256).hexdigest()
    return hmac_digest


def calculate_hmac_signature_historic(api_key, api_secret, t, station_id, start_t, end_t):
    params_para_assinar = {"api-key": api_key, "end-timestamp": str(end_t), "start-timestamp": str(start_t),
                           "station-id": str(station_id), "t": str(t)}
    string_para_assinar = ""
    for key in sorted(params_para_assinar.keys()):
        string_para_assinar += f"{key}{params_para_assinar[key]}"
    print(f"[HMAC /historic] Assinando Mensagem: {string_para_assinar}")
    hmac_digest = hmac.new(api_secret.encode('utf-8'), string_para_assinar.encode('utf-8'), hashlib.sha256).hexdigest()
    return hmac_digest


def arredondar_timestamp_15min(ts_epoch):
    dt_obj = datetime.datetime.fromtimestamp(ts_epoch, datetime.timezone.utc)
    dt_obj = dt_obj.replace(second=0, microsecond=0, minute=(dt_obj.minute // 15) * 15)
    return dt_obj.isoformat()


# --- INÍCIO DA ALTERAÇÃO (Lógica Robusta de Timestamp) ---
def fetch_data_from_weatherlink_api(df_historico):
    WEATHERLINK_API_ENDPOINT = "https://api.weatherlink.com/v2/current/{station_id}"
    logs_api = []
    dados_processados = []
    status_calculados = {}

    t = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
    # REMOVIDO: timestamp_arredondado = arredondar_timestamp_15min(t)

    print("[API] COLETANDO DADOS (INCREMENTAL /current) DA WEATHERLINK")

    with httpx.Client(timeout=30.0) as client:
        for id_ponto, config_ponto in PONTOS_DE_ANALISE.items():
            station_config = WEATHERLINK_CONFIG[id_ponto]
            station_id = station_config['STATION_ID']
            api_key = station_config['API_KEY']
            api_secret = station_config['API_SECRET']

            chuva_incremental = 0.0
            precip_acumulada_dia = pd.NA
            umidade_1m = pd.NA
            umidade_2m = pd.NA
            umidade_3m = pd.NA
            mensagem_log = ""

            # --- CORREÇÃO: Variável para o carimbo de data/hora ---
            timestamp_arredondado = None

            if "SUA_CHAVE_API" in api_key or "SEU_SEGREDO_API" in api_secret:
                mensagem_log = f"AVISO API: Credenciais para {id_ponto} não preenchidas. Pulando."
                logs_api.append({"id_ponto": id_ponto, "mensagem": mensagem_log})
                continue

            signature = calculate_hmac_signature_current(api_key, api_secret, t, station_id)
            params_requisicao = {"api-key": api_key, "t": str(t), "api-signature": signature}
            url = WEATHERLINK_API_ENDPOINT.format(station_id=station_id)
            print(f"[API {id_ponto}] Chamando URL: {url}")

            try:
                response = client.get(url, params=params_requisicao)
                response.raise_for_status()
                data = response.json()

                if not (data.get('sensors') and data['sensors'][0].get('data')):
                    mensagem_log = "API: Resposta JSON vazia ou inesperada."
                    logs_api.append({"id_ponto": id_ponto, "mensagem": mensagem_log})
                    continue

                sensor_principal = None
                for sensor in data.get('sensors', []):
                    if sensor.get('sensor_type') == 48 and sensor.get('data'):
                        sensor_principal = sensor['data'][0]
                        break

                if not sensor_principal:
                    mensagem_log = "API: ERRO. Não encontrou o Sensor Principal (type 48) na resposta."
                    logs_api.append({"id_ponto": id_ponto, "mensagem": mensagem_log})
                    continue

                # --- INÍCIO DA CORREÇÃO DE TIMESTAMP ---
                # 1. Pega o carimbo de data/hora DO SENSOR
                ts_do_sensor = sensor_principal.get('ts')

                if ts_do_sensor is None:
                    mensagem_log = "API: ERRO. Sensor não retornou um carimbo 'ts'. Usando hora local."
                    logs_api.append({"id_ponto": id_ponto, "mensagem": mensagem_log})
                    # Fallback (comportamento antigo e ingênuo)
                    ts_do_sensor = int(datetime.datetime.now(datetime.timezone.utc).timestamp())

                # 2. Arredonda o carimbo DO SENSOR
                timestamp_arredondado = arredondar_timestamp_15min(ts_do_sensor)
                # --- FIM DA CORREÇÃO DE TIMESTAMP ---

                chuva_incremental = sensor_principal.get('rainfall_last_15_min_mm')
                precip_acumulada_dia = sensor_principal.get('rainfall_daily_mm')

                if chuva_incremental is None:
                    mensagem_log = "API: ERRO. Campo 'rainfall_last_15_min_mm' não encontrado. Salvando 0."
                    chuva_incremental = 0.0
                else:
                    mensagem_log = f"API: Sucesso. Chuva 15min: {chuva_incremental:.2f}mm. Total Dia: {precip_acumulada_dia:.2f}mm"

                logs_api.append({"id_ponto": id_ponto, "mensagem": mensagem_log})

            except httpx.HTTPStatusError as e:
                mensagem_log = f"ERRO HTTP ({e.response.status_code}): Falha ao coletar dados."
                logs_api.append({"id_ponto": id_ponto, "mensagem": mensagem_log})
                continue
            except Exception as e:
                mensagem_log = f"ERRO CRÍTICO (API /current): {e}"
                logs_api.append({"id_ponto": id_ponto, "mensagem": mensagem_log})
                traceback.print_exc()
                continue

            # Se o timestamp não pôde ser definido (ex: erro antes da extração), pulamos
            if timestamp_arredondado is None:
                logs_api.append(
                    {"id_ponto": id_ponto, "mensagem": "ERRO: Timestamp não pôde ser definido. Ponto ignorado."})
                continue

            dados_processados.append({
                "timestamp": timestamp_arredondado,  # <-- AGORA USA O CARIMBO CORRETO
                "id_ponto": id_ponto,
                "chuva_mm": chuva_incremental,
                "precipitacao_acumulada_mm": precip_acumulada_dia,
                "umidade_1m_perc": umidade_1m,
                "umidade_2m_perc": umidade_2m,
                "umidade_3m_perc": umidade_3m,
                "base_1m": pd.NA,
                "base_2m": pd.NA,
                "base_3m": pd.NA,
            })

    return pd.DataFrame(dados_processados), status_calculados, logs_api


# --- FIM DA ALTERAÇÃO ---


def backfill_km67_pro_data(df_historico_existente):
    id_ponto = "Ponto-A-KM67"
    station_config = WEATHERLINK_CONFIG[id_ponto]
    station_id = station_config['STATION_ID']
    api_key = station_config['API_KEY']
    api_secret = station_config['API_SECRET']

    if "SUA_CHAVE_API" in api_key or "SEU_SEGREDO_API" in api_secret:
        adicionar_log(id_ponto, "AVISO (Backfill): Credenciais não preenchidas. Pulando backfill.")
        return

    print(f"[API {id_ponto}] Iniciando Backfill de 72h (em 3 blocos de 24h)...")

    url_base = f"https://api.weatherlink.com/v2/historic/{station_id}"
    dados_processados = []
    now_dt = datetime.datetime.now(datetime.timezone.utc)

    try:
        with httpx.Client(timeout=60.0) as client:
            for i in range(3):
                end_dt = now_dt - datetime.timedelta(hours=24 * i)
                start_dt = end_dt - datetime.timedelta(hours=24)
                end_t = int(end_dt.timestamp())
                start_t = int(start_dt.timestamp())
                t = int(datetime.datetime.now(datetime.timezone.utc).timestamp())

                print(f"[API {id_ponto}] Buscando Bloco {i + 1}/3 ({start_dt.date()} a {end_dt.date()})...")

                signature = calculate_hmac_signature_historic(api_key, api_secret, t, station_id, start_t, end_t)
                params_requisicao = {
                    "api-key": api_key, "api-signature": signature,
                    "end-timestamp": str(end_t), "start-timestamp": str(start_t), "t": str(t)
                }

                response = client.get(url_base, params=params_requisicao)
                response.raise_for_status()
                data = response.json()

                for sensor in data.get('sensors', []):
                    if sensor.get('sensor_type') != 48:
                        continue

                    for registro in sensor.get('data', []):

                        # --- LÓGICA CORRIGIDA (Backfill v2) ---
                        chuva_incremental_hist = registro.get('rainfall_mm')

                        if chuva_incremental_hist is not None:
                            precip_acumulada_dia = registro.get('rainfall_daily_mm', pd.NA)
                            ts_registro = arredondar_timestamp_15min(registro['ts'])
                            dados_processados.append({
                                "timestamp": ts_registro,
                                "id_ponto": id_ponto,
                                "chuva_mm": chuva_incremental_hist,
                                "precipitacao_acumulada_mm": precip_acumulada_dia,
                                "umidade_1m_perc": pd.NA, "umidade_2m_perc": pd.NA, "umidade_3m_perc": pd.NA,
                                "base_1m": pd.NA, "base_2m": pd.NA, "base_3m": pd.NA,
                            })
                        # --- FIM DA LÓGICA CORRIGIDA ---

                if i < 2: time.sleep(1)

        if not dados_processados:
            adicionar_log(id_ponto, "AVISO (Backfill): API não retornou dados históricos (JSON vazio).")
            return

        df_backfill = pd.DataFrame(dados_processados)
        df_backfill = df_backfill[COLUNAS_HISTORICO]

        if 'timestamp' in df_backfill.columns:
            df_backfill['timestamp'] = pd.to_datetime(df_backfill['timestamp'], utc=True)

        save_to_sqlite(df_backfill)

        df_final_csv = pd.concat([df_historico_existente, df_backfill], ignore_index=True)
        df_final_csv['timestamp'] = pd.to_datetime(df_final_csv['timestamp'], utc=True)
        save_historico_to_csv(df_final_csv)

        adicionar_log(id_ponto,
                      f"SUCESSO (Backfill): {len(df_backfill)} registros históricos de 72h (3x 24h) salvos (CSV e SQLite).")

    except httpx.HTTPStatusError as e:
        adicionar_log(id_ponto, f"ERRO HTTP (Backfill) ({e.response.status_code}): Falha ao coletar histórico.")
        adicionar_log(id_ponto, f"Resposta da API: {e.response.text}")
    except Exception as e:
        adicionar_log(id_ponto, f"ERRO CRÍTICO (Backfill): {e}")
        traceback.print_exc()
# --- FIM DA ALTERAÇÃO ---