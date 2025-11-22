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
from sqlalchemy import create_engine, inspect, text, bindparam, delete, table, column
from sqlalchemy.pool import NullPool
from httpx import HTTPStatusError
import threading
from sqlalchemy import event

warnings.simplefilter(action='ignore', category=FutureWarning)

from config import (
    PONTOS_DE_ANALISE, CONSTANTES_PADRAO,
    FREQUENCIA_API_SEGUNDOS, BACKFILL_RUN_TIME_SEC,
    MAX_HISTORICO_PONTOS,
    WEATHERLINK_CONFIG,
    DB_TABLE_NAME,
    ZENTRA_API_TOKEN, ZENTRA_STATION_SERIAL, ZENTRA_BASE_URL,
    MAPA_ZENTRA_KM72, ID_PONTO_ZENTRA_KM72,
    RENDER_SLEEP_TIME_SEC
)

# -----------------------------------------------------------------------------
# -- VARIÁVEIS GLOBAIS DE CONEXÃO --
# -----------------------------------------------------------------------------
DATA_DIR = "."
DB_CONNECTION_STRING = ""
STATUS_FILE = os.path.join(DATA_DIR, "status_atual.json")
LOG_FILE = os.path.join(DATA_DIR, "eventos.log")
DB_ENGINE = None

COLUNAS_HISTORICO = [
    'timestamp', 'id_ponto', 'chuva_mm', 'precipitacao_acumulada_mm',
    'umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc',
    'base_1m', 'base_2m', 'base_3m'
]


# --- FUNÇÃO DE ESCRITA SEGURA ---
def write_with_timeout(file_path, content, mode='w', timeout=15):
    def target():
        try:
            with open(file_path, mode, encoding='utf-8') as f:
                if isinstance(content, str):
                    f.write(content)
                else:
                    json.dump(content, f, indent=2)
            target.success = True
        except Exception as e:
            target.error = e
            target.success = False

    target.success = False
    thread = threading.Thread(target=target)
    thread.start()
    thread.join(timeout)

    if thread.is_alive():
        raise TimeoutError(f"Timeout escrita: {os.path.basename(file_path)}")
    if not target.success:
        raise target.error


def adicionar_log(id_ponto, mensagem, level="INFO"):
    try:
        log_entry = f"{datetime.datetime.now(datetime.timezone.utc).isoformat()} | {level:<5} | {id_ponto} | {mensagem}\n"
        print(log_entry.strip())
        write_with_timeout(LOG_FILE, log_entry, mode='a', timeout=10)
    except Exception as e:
        print(f"ERRO CRÍTICO AO LOGAR: {e}")


def ler_logs_eventos(id_ponto):
    try:
        with open(LOG_FILE, 'r', encoding='utf-8') as f:
            logs_str = f.read()
        logs_list = logs_str.split('\n')
        logs_filtrados = [log for log in logs_list if f"| {id_ponto} |" in log or "| GERAL |" in log]
        return '\n'.join(logs_filtrados)
    except FileNotFoundError:
        return "Nenhum evento registrado ainda."
    except Exception as e:
        return f"ERRO ao ler logs: {e}"


def setup_disk_paths():
    """ Define caminhos e inicializa o DB_ENGINE global com WAL ativado. """
    global DATA_DIR, STATUS_FILE, LOG_FILE, DB_CONNECTION_STRING, DB_ENGINE

    DB_FILENAME = "temp_local_db.db"

    if os.environ.get('RENDER'):
        DATA_DIR = "/var/data"
        DB_CONNECTION_STRING = f'sqlite:///{os.path.join(DATA_DIR, DB_FILENAME)}'
    else:
        DATA_DIR = "."
        DB_CONNECTION_STRING = f'sqlite:///{DB_FILENAME}'

    print(f"DEBUG: setup_disk_paths - DB_CONNECTION_STRING: {DB_CONNECTION_STRING}")

    if DB_ENGINE is None:
        # Cria o engine
        DB_ENGINE = create_engine(
            DB_CONNECTION_STRING,
            connect_args={"timeout": 30, "check_same_thread": False},
            poolclass=NullPool
        )

        # Ativa o modo WAL e NORMAL sync para balancear segurança e performance
        @event.listens_for(DB_ENGINE, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.close()

    STATUS_FILE = os.path.join(DATA_DIR, "status_atual.json")
    LOG_FILE = os.path.join(DATA_DIR, "eventos.log")

    adicionar_log("SISTEMA", f"Caminho do Disco de Dados: {DATA_DIR}")
    adicionar_log("SISTEMA", f"Banco de Dados: {DB_CONNECTION_STRING}")


def initialize_database():
    global DB_ENGINE
    if DB_ENGINE is None: setup_disk_paths()
    try:
        with DB_ENGINE.connect() as connection:
            pass
        inspector = inspect(DB_ENGINE)
        if not inspector.has_table(DB_TABLE_NAME):
            df_vazio = pd.DataFrame(columns=COLUNAS_HISTORICO)
            df_vazio.to_sql(DB_TABLE_NAME, DB_ENGINE, index=False)
            adicionar_log("DB", f"Tabela '{DB_TABLE_NAME}' criada.")

        existing_indexes = []
        try:
            indexes = inspector.get_indexes(DB_TABLE_NAME)
            existing_indexes = [idx['name'] for idx in indexes]
        except Exception:
            pass

        with DB_ENGINE.connect() as connection:
            if 'idx_timestamp' not in existing_indexes:
                try:
                    connection.execute(
                        text(f'CREATE INDEX idx_timestamp ON {DB_TABLE_NAME} (timestamp)')); adicionar_log("DB",
                                                                                                           "Índice 'idx_timestamp' criado.")
                except Exception:
                    pass
            if 'idx_id_ponto' not in existing_indexes:
                try:
                    connection.execute(text(f'CREATE INDEX idx_id_ponto ON {DB_TABLE_NAME} (id_ponto)')); adicionar_log(
                        "DB", "Índice 'idx_id_ponto' criado.")
                except Exception:
                    pass
            connection.commit()
        adicionar_log("DB", "Banco de dados verificado e pronto.")
    except Exception as e:
        adicionar_log("SISTEMA", f"ERRO CRÍTICO DB Init: {e}", level="ERROR")
        traceback.print_exc()


def save_to_sqlite(df_novos_dados):
    """ Salva dados sem forçar o checkpoint TRUNCATE, evitando locks. """
    global DB_ENGINE
    if df_novos_dados.empty: return
    try:
        df_para_salvar = df_novos_dados.copy()
        if 'timestamp' in df_para_salvar.columns:
            df_para_salvar['timestamp'] = pd.to_datetime(df_para_salvar['timestamp'], utc=True)
        colunas_para_salvar = [col for col in COLUNAS_HISTORICO if col in df_para_salvar.columns]
        df_para_salvar = df_para_salvar[colunas_para_salvar]

        adicionar_log("DB", f"Salvando {len(df_para_salvar)} linhas no banco de dados.")

        with DB_ENGINE.connect() as connection:
            df_para_salvar.to_sql(DB_TABLE_NAME, connection, if_exists='append', index=False)
            connection.commit()

    except Exception as e:
        if "UNIQUE constraint failed" in str(e):
            adicionar_log("DB", "Aviso: Dados duplicados ignorados.", level="WARN")
        else:
            adicionar_log("DB", f"ERRO CRÍTICO Salvar SQLite: {e}", level="ERROR")


def delete_from_sqlite(timestamps):
    global DB_ENGINE
    if not timestamps: return
    try:
        ts_strings = [pd.to_datetime(ts).strftime('%Y-%m-%d %H:%M:%S') for ts in timestamps]
        with DB_ENGINE.connect() as connection:
            t_historico = table(DB_TABLE_NAME, column('timestamp'))
            stmt = delete(t_historico).where(t_historico.c.timestamp.in_(ts_strings))
            adicionar_log("DB", f"Deletando {len(ts_strings)} registros antigos.")
            connection.execute(stmt)
            connection.commit()
    except Exception as e:
        adicionar_log("DB", f"ERRO CRÍTICO Deletar SQLite: {e}", level="ERROR")
        traceback.print_exc()


def read_data_from_sqlite(id_ponto=None, start_dt=None, end_dt=None, last_hours=None):
    global DB_ENGINE
    query_base = f"SELECT * FROM {DB_TABLE_NAME}"
    conditions, params = [], {}
    if last_hours: start_dt = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=last_hours)
    if id_ponto: conditions.append("id_ponto = :ponto"); params["ponto"] = id_ponto
    if start_dt: conditions.append("timestamp >= :start"); params["start"] = start_dt.strftime('%Y-%m-%d %H:%M:%S')
    if end_dt: conditions.append("timestamp < :end"); params["end"] = end_dt.strftime('%Y-%m-%d %H:%M:%S')
    if conditions: query_base += " WHERE " + " AND ".join(conditions)
    query_base += " ORDER BY timestamp ASC"
    try:
        df = pd.read_sql_query(query_base, DB_ENGINE, params=params, parse_dates=["timestamp"])
        if 'timestamp' in df.columns and not df.empty:
            if df['timestamp'].dt.tz is None: df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')

            # LOG DIAGNÓSTICO
            ultimo = df['timestamp'].max()
            ponto_log = id_ponto if id_ponto else 'GLOBAL'
            print(f"🔍 [DEBUG LEITURA] Leitura: {ponto_log} | Linhas: {len(df)} | Último: {ultimo}")

        return df
    except Exception as e:
        adicionar_log("DB", f"ERRO Leitura SQLite: {e}", level="ERROR")
        return pd.DataFrame()


def get_recent_data_for_worker(hours=73): return read_data_from_sqlite(last_hours=hours)


def get_all_data_for_dashboard():
    df_completo = read_data_from_sqlite(last_hours=100)
    status_atual = get_status_from_disk()
    try:
        with open(LOG_FILE, 'r', encoding='utf-8') as f:
            logs = f.read()
    except FileNotFoundError:
        logs = "Nenhum evento registrado ainda."
    return df_completo, status_atual, logs


def get_status_from_disk():
    try:
        with open(STATUS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {p: {"status": "SEM DADOS"} for p in PONTOS_DE_ANALISE.keys()}


# --- FUNÇÕES DE INTEGRAÇÃO API ---

def calculate_hmac_signature(params, api_secret):
    string_para_assinar = "".join(f"{key}{params[key]}" for key in sorted(params.keys()))
    # CORREÇÃO DE TYPO AQUI: string_para_assinar (correto)
    return hmac.new(api_secret.encode('utf-8'), string_para_assinar.encode('utf-8'), hashlib.sha256).hexdigest()


def arredondar_timestamp_10min(ts_epoch):
    dt_obj = datetime.datetime.fromtimestamp(ts_epoch, datetime.timezone.utc)
    return (dt_obj.replace(second=0, microsecond=0, minute=(dt_obj.minute // 10) * 10)).isoformat()


def fetch_data_from_weatherlink_api():
    ENDPOINT = "https://api.weatherlink.com/v2/current/{station_id}"
    logs, dados = [], []
    t = str(int(datetime.datetime.now(datetime.timezone.utc).timestamp()))
    with httpx.Client(timeout=30.0) as client:
        for id_ponto, config in WEATHERLINK_CONFIG.items():
            if "SUA_CHAVE" in config.get('API_KEY', ''): continue
            adicionar_log(id_ponto, f"Requisitando API WeatherLink (Station ID: {config['STATION_ID']}).")
            params_to_sign = {"api-key": config['API_KEY'], "station-id": str(config['STATION_ID']), "t": t}
            signature = calculate_hmac_signature(params_to_sign, config['API_SECRET'])
            params_to_send = {"api-key": config['API_KEY'], "t": t, "api-signature": signature}
            try:
                r = client.get(ENDPOINT.format(station_id=config['STATION_ID']), params=params_to_send)
                r.raise_for_status();
                response_json = r.json()
                s = next((s['data'][0] for s in response_json.get('sensors', []) if
                          (s.get('data_structure_type') == 10 or s.get('sensor_type') == 48) and s.get('data')), None)
                if not s or 'ts' not in s or (int(t) - s['ts']) > 3600: adicionar_log(id_ponto,
                                                                                      "Dados atrasados/offline.",
                                                                                      level="WARN"); continue
                dados.append({"timestamp": arredondar_timestamp_10min(s['ts']), "id_ponto": id_ponto,
                              "chuva_mm": s.get('rainfall_last_15_min_mm', 0.0) or 0.0,
                              "precipitacao_acumulada_mm": s.get('rainfall_daily_mm')})
            except Exception as e:
                adicionar_log(id_ponto, f"Erro API WL: {e}", level="ERROR")
    df = pd.DataFrame(dados)
    if not df.empty and 'timestamp' in df.columns: df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    return df, None, logs


def _get_readings_zentra(client, station_serial, start_date, end_date):
    url = f"{ZENTRA_BASE_URL}/get_readings/";
    params = {"device_sn": station_serial, "start": start_date.strftime("%Y-%m-%d"),
              "end": end_date.strftime("%Y-%m-%d")};
    headers = {"Authorization": f"Token {ZENTRA_API_TOKEN}"}
    try:
        return client.get(url, headers=headers, params=params, timeout=30.0)
    except httpx.TimeoutException:
        return None


def fetch_data_from_zentra_cloud():
    end_date = datetime.datetime.now(datetime.timezone.utc);
    start_date = end_date - datetime.timedelta(days=2)
    adicionar_log(ID_PONTO_ZENTRA_KM72, f"Buscando dados da Zentra Cloud.")
    with httpx.Client() as client:
        for attempt in range(3):
            r = _get_readings_zentra(client, ZENTRA_STATION_SERIAL, start_date, end_date)
            if r and r.status_code == 200: break
            if r and r.status_code == 429 and attempt < 2: time.sleep(60)
    if not r or r.status_code != 200: adicionar_log(ID_PONTO_ZENTRA_KM72,
                                                    f"Erro Zentra: {r.status_code if r else 'N/A'}",
                                                    level="ERROR"); return pd.DataFrame()
    try:
        wc_data = next((d for n, d in r.json().get('data', {}).items() if 'water content' in n.lower()), None)
        if not wc_data: return pd.DataFrame()
        dados_por_timestamp = {}
        for sensor_block in wc_data:
            port_num = sensor_block.get('metadata', {}).get('port_number')
            if port_num in MAPA_ZENTRA_KM72:
                coluna = MAPA_ZENTRA_KM72[port_num]
                for reading in sensor_block.get('readings', []):
                    ts_iso, value = reading.get('datetime'), reading.get('value')
                    if ts_iso and value is not None:
                        ts_arr = arredondar_timestamp_10min(datetime.datetime.fromisoformat(ts_iso).timestamp())
                        if ts_arr not in dados_por_timestamp: dados_por_timestamp[ts_arr] = {}
                        dados_por_timestamp[ts_arr][coluna] = float(value) * 100.0
        if not dados_por_timestamp: return pd.DataFrame()
        df_bloco = pd.DataFrame(
            [{'timestamp': ts, 'id_ponto': ID_PONTO_ZENTRA_KM72, **v} for ts, v in dados_por_timestamp.items()])
        df_bloco['timestamp'] = pd.to_datetime(df_bloco['timestamp'], utc=True)
        return df_bloco
    except Exception as e:
        adicionar_log(ID_PONTO_ZENTRA_KM72, f"Erro JSON Zentra: {e}", level="ERROR"); return pd.DataFrame()


def backfill_zentra_km72_data():
    id_ponto = ID_PONTO_ZENTRA_KM72;
    adicionar_log(id_ponto, "Iniciando backfill Zentra.")
    df_existente = read_data_from_sqlite(id_ponto=id_ponto)
    start_date = df_existente['timestamp'].max() if not df_existente.empty else datetime.datetime(2023, 1, 1,
                                                                                                  tzinfo=datetime.timezone.utc)
    end_date = datetime.datetime.now(datetime.timezone.utc);
    current = start_date;
    start_time = time.time()
    while current < end_date:
        if (time.time() - start_time) > BACKFILL_RUN_TIME_SEC: break
        period_end = min(current + datetime.timedelta(days=7), end_date)
        with httpx.Client(timeout=60.0) as client:
            response = _get_readings_zentra(client, ZENTRA_STATION_SERIAL, current, period_end)
        if response and response.status_code == 200:
            try:
                wc_data = next((d for n, d in response.json().get('data', {}).items() if 'water content' in n.lower()),
                               None)
                if wc_data:
                    dados_por_timestamp = {}
                    for sb in wc_data:
                        port = sb.get('metadata', {}).get('port_number');
                        if port in MAPA_ZENTRA_KM72:
                            col = MAPA_ZENTRA_KM72[port]
                            for r in sb.get('readings', []):
                                ts, val = r.get('datetime'), r.get('value')
                                if ts and val:
                                    ts_arr = arredondar_timestamp_10min(datetime.datetime.fromisoformat(ts).timestamp())
                                    if ts_arr not in dados_por_timestamp: dados_por_timestamp[ts_arr] = {}
                                    dados_por_timestamp[ts_arr][col] = float(val) * 100.0
                    if dados_por_timestamp: df = pd.DataFrame(
                        [{'timestamp': k, 'id_ponto': id_ponto, **v} for k, v in dados_por_timestamp.items()]); df[
                        'timestamp'] = pd.to_datetime(df['timestamp'], utc=True); save_to_sqlite(df)
            except Exception:
                pass
        current += datetime.timedelta(days=7);
        time.sleep(1)
    return True


def backfill_weatherlink_data(id_ponto):
    config = WEATHERLINK_CONFIG.get(id_ponto)
    if not config or "SUA_CHAVE" in config.get('API_KEY', ''): return
    adicionar_log(id_ponto, "Iniciando Backfill WL.")
    now_dt = datetime.datetime.now(datetime.timezone.utc);
    url_base = f"https://api.weatherlink.com/v2/historic/{config['STATION_ID']}";
    dados = [];
    start_time = time.time()
    with httpx.Client(timeout=60.0) as client:
        for i in range(4):
            if (time.time() - start_time) > BACKFILL_RUN_TIME_SEC: break
            end_dt = now_dt - datetime.timedelta(hours=24 * i);
            start_dt = end_dt - datetime.timedelta(hours=24);
            t = str(int(now_dt.timestamp()))
            params = {"api-key": config['API_KEY'], "end-timestamp": str(int(end_dt.timestamp())),
                      "start-timestamp": str(int(start_dt.timestamp())), "station-id": str(config['STATION_ID']),
                      "t": t}
            sig = calculate_hmac_signature(params, config['API_SECRET']);
            params["api-signature"] = sig
            try:
                resp = client.get(url_base, params=params)
                if resp.status_code == 200:
                    data = resp.json()
                    for s in data.get('sensors', []):
                        if s.get('sensor_type') == 48 or s.get('data_structure_type') == 10:
                            for r in s.get('data', []):
                                if 'rainfall_mm' in r: dados.append(
                                    {"timestamp": arredondar_timestamp_10min(r['ts']), "id_ponto": id_ponto,
                                     "chuva_mm": r['rainfall_mm']})
            except Exception:
                pass
            time.sleep(1)
    if dados: df = pd.DataFrame(dados); df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True); save_to_sqlite(
        df); return True
    return False