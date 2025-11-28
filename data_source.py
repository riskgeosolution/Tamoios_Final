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
from httpx import HTTPStatusError
import threading

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
DB_CONNECTION_STRING = ""
STATUS_FILE = "status_atual.json"
LOG_FILE = "eventos.log"
DB_ENGINE = None

COLUNAS_HISTORICO = [
    'timestamp', 'id_ponto', 'chuva_mm', 'precipitacao_acumulada_mm',
    'umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc',
    'base_1m', 'base_2m', 'base_3m'
]


# --- FUNÇÃO DE CAMINHO SEGURO (Persistência no Render) ---
def get_base_path():
    """
    Retorna o caminho base correto.
    Se estiver no Render, usa /var/data (Disco Persistente).
    Se estiver local, usa o diretório atual.
    """
    if os.environ.get('RENDER'):
        if os.path.exists("/var/data"):
            return "/var/data"
        return os.getcwd()
    return os.getcwd()


# --- FUNÇÃO DE ESCRITA SEGURA ---
def write_with_timeout(file_path, content, mode='w', timeout=15):
    def target():
        try:
            base = get_base_path()
            full_file_path = os.path.join(base, file_path)

            with open(full_file_path, mode, encoding='utf-8') as f:
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


def adicionar_log(id_ponto, mensagem, level="INFO", salvar_arquivo=True):
    try:
        log_entry = f"{datetime.datetime.now(datetime.timezone.utc).isoformat()} | {level:<5} | {id_ponto} | {mensagem}\n"
        print(log_entry.strip())

        if salvar_arquivo:
            write_with_timeout(LOG_FILE, log_entry, mode='a', timeout=10)
    except Exception as e:
        print(f"ERRO CRÍTICO AO LOGAR: {e}")


# --- FUNÇÃO DE LEITURA DE LOGS (Retorna Lista Limpa) ---
def ler_logs_eventos(id_ponto):
    try:
        base = get_base_path()
        full_log_file_path = os.path.join(base, LOG_FILE)

        if not os.path.exists(full_log_file_path):
            return []

        with open(full_log_file_path, 'r', encoding='utf-8') as f:
            logs_str = f.read()

        if not logs_str:
            return []

        logs_list = logs_str.strip().split('\n')

        if id_ponto == "GERAL":
            return logs_list

        logs_filtrados = [log for log in logs_list if f"| {id_ponto} |" in log or "| GERAL |" in log]
        return logs_filtrados
    except Exception as e:
        return [f"ERRO ao ler logs: {e}"]


def setup_disk_paths():
    """ Define conexão DB. """
    global DB_CONNECTION_STRING, DB_ENGINE

    DB_CONNECTION_STRING = os.getenv("DATABASE_URL", "sqlite:///temp_local_db.db")
    is_sqlite = "sqlite" in DB_CONNECTION_STRING

    if DB_ENGINE is None:
        if is_sqlite:
            connect_args = {"check_same_thread": False, "timeout": 30}
        else:
            connect_args = {"connect_timeout": 30}

        DB_ENGINE = create_engine(
            DB_CONNECTION_STRING,
            connect_args=connect_args,
            pool_pre_ping=True
        )

    adicionar_log("SISTEMA", f"Banco de Dados Configurado.", salvar_arquivo=False)


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
            adicionar_log("DB", f"Tabela '{DB_TABLE_NAME}' criada.", salvar_arquivo=False)

        existing_indexes = []
        try:
            indexes = inspector.get_indexes(DB_TABLE_NAME);
            existing_indexes = [idx['name'] for idx in indexes]
        except Exception:
            pass

        with DB_ENGINE.connect() as connection:
            if 'idx_timestamp' not in existing_indexes:
                try:
                    connection.execute(text(f'CREATE INDEX idx_timestamp ON {DB_TABLE_NAME} (timestamp)'))
                except Exception:
                    pass
            if 'idx_id_ponto' not in existing_indexes:
                try:
                    connection.execute(text(f'CREATE INDEX idx_id_ponto ON {DB_TABLE_NAME} (id_ponto)'))
                except Exception:
                    pass
            connection.commit()
        adicionar_log("DB", "Banco de dados verificado e pronto.", salvar_arquivo=False)
    except Exception as e:
        adicionar_log("SISTEMA", f"ERRO CRÍTICO DB Init: {e}", level="ERROR", salvar_arquivo=True)
        traceback.print_exc()


def upsert_data(df_novos_dados):
    if df_novos_dados.empty: return
    timestamps = df_novos_dados['timestamp'].unique()
    delete_from_sqlite(timestamps)
    save_to_sqlite(df_novos_dados)


def save_to_sqlite(df_novos_dados):
    global DB_ENGINE
    if df_novos_dados.empty: return
    try:
        df_para_salvar = df_novos_dados.copy()
        if 'timestamp' in df_para_salvar.columns:
            df_para_salvar['timestamp'] = pd.to_datetime(df_para_salvar['timestamp'], utc=True)
        colunas_para_salvar = [col for col in COLUNAS_HISTORICO if col in df_para_salvar.columns]
        df_para_salvar = df_para_salvar[colunas_para_salvar]

        adicionar_log("DB", f"Salvando {len(df_para_salvar)} linhas no banco de dados.", level="INFO",
                      salvar_arquivo=False)

        with DB_ENGINE.connect() as connection:
            df_para_salvar.to_sql(DB_TABLE_NAME, connection, if_exists='append', index=False)
            connection.commit()

    except Exception as e:
        if "UNIQUE constraint failed" in str(e):
            adicionar_log("DB", "Aviso: Dados duplicados ignorados.", level="WARN", salvar_arquivo=False)
        else:
            adicionar_log("DB", f"ERRO CRÍTICO Salvar DB: {e}", level="ERROR", salvar_arquivo=True)


def delete_from_sqlite(timestamps):
    global DB_ENGINE
    if not timestamps: return
    try:
        ts_strings = [pd.to_datetime(ts).strftime('%Y-%m-%d %H:%M:%S') for ts in timestamps]
        with DB_ENGINE.connect() as connection:
            t_historico = table(DB_TABLE_NAME, column('timestamp'))
            stmt = delete(t_historico).where(t_historico.c.timestamp.in_(ts_strings))
            adicionar_log("DB", f"Deletando {len(ts_strings)} registros antigos.", level="INFO", salvar_arquivo=False)
            connection.execute(stmt)
            connection.commit()
    except Exception as e:
        adicionar_log("DB", f"ERRO CRÍTICO Deletar DB: {e}", level="ERROR", salvar_arquivo=True)
        traceback.print_exc()


# --- OTIMIZAÇÃO DE LEITURA (SELECT COLUNAS) ---
def read_data_from_sqlite(id_ponto=None, start_dt=None, end_dt=None, last_hours=None, colunas=None):
    global DB_ENGINE

    # Se colunas forem passadas, seleciona apenas elas. Caso contrário, SELECT *
    cols_str = "*"
    if colunas and isinstance(colunas, list):
        cols_str = ", ".join(colunas)

    query_base = f"SELECT {cols_str} FROM {DB_TABLE_NAME}"
    conditions = []
    params = {}

    if last_hours: start_dt = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=last_hours)
    if id_ponto: conditions.append("id_ponto = :ponto"); params["ponto"] = id_ponto
    if start_dt: conditions.append("timestamp >= :start"); params["start"] = start_dt.strftime('%Y-%m-%d %H:%M:%S')
    if end_dt: conditions.append("timestamp < :end"); params["end"] = end_dt.strftime('%Y-%m-%d %H:%M:%S')

    if conditions: query_base += " WHERE " + " AND ".join(conditions)
    query_base += " ORDER BY timestamp ASC"

    df = pd.DataFrame()
    try:
        with DB_ENGINE.connect() as connection:
            df = pd.read_sql_query(text(query_base), connection, params=params, parse_dates=["timestamp"])
            connection.commit()

        if 'timestamp' in df.columns and not df.empty:
            if df['timestamp'].dt.tz is None: df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')

            # Log reduzido para não poluir em leituras frequentes
            if not colunas:
                ultimo = df['timestamp'].max()
                ponto_log = id_ponto if id_ponto else 'GLOBAL'
                adicionar_log("DB_READ", f"Leitura: {ponto_log} | Linhas: {len(df)} | Último: {ultimo}", level="INFO",
                              salvar_arquivo=False)

        return df
    except Exception as e:
        adicionar_log("DB", f"ERRO Leitura DB: {e}", level="ERROR", salvar_arquivo=True)
        return pd.DataFrame()


def get_recent_data_for_worker(hours=73): return read_data_from_sqlite(last_hours=hours)


def get_all_data_for_dashboard():
    df_completo = read_data_from_sqlite(last_hours=100)
    status_atual = get_status_from_disk()
    logs = ler_logs_eventos("GERAL")
    logs_str = "\n".join(logs) if isinstance(logs, list) else str(logs)
    return df_completo, status_atual, logs_str


def get_status_from_disk():
    try:
        base = get_base_path()
        full_status_file_path = os.path.join(base, STATUS_FILE)

        with open(full_status_file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {p: {"chuva": "SEM DADOS", "umidade": "SEM DADOS"} for p in PONTOS_DE_ANALISE.keys()}


# --- FUNÇÕES DE INTEGRAÇÃO API ---

def calculate_hmac_signature(params, api_secret):
    string_para_assinar = "".join(f"{key}{params[key]}" for key in sorted(params.keys()))
    return hmac.new(api_secret.encode('utf-8'), string_para_assinar.encode('utf-8'), hashlib.sha256).hexdigest()


def arredondar_timestamp_10min(ts_epoch):
    dt_obj = datetime.datetime.fromtimestamp(ts_epoch, datetime.timezone.utc)
    return (dt_obj.replace(second=0, microsecond=0, minute=(dt_obj.minute // 10) * 10)).isoformat()


def fetch_data_from_weatherlink_api(ultimo_acumulado_chuva=None):
    ENDPOINT = "https://api.weatherlink.com/v2/current/{station_id}"
    logs, dados = [], []
    novos_acumulados = {}

    t = str(int(datetime.datetime.now(datetime.timezone.utc).timestamp()))
    with httpx.Client(timeout=30.0) as client:
        for id_ponto, config in WEATHERLINK_CONFIG.items():
            if "SUA_CHAVE" in config.get('API_KEY', ''): continue
            adicionar_log(id_ponto, f"Requisitando API WeatherLink (Station ID: {config['STATION_ID']}).", level="INFO",
                          salvar_arquivo=False)
            params_to_sign = {"api-key": config['API_KEY'], "station-id": str(config['STATION_ID']), "t": t}
            signature = calculate_hmac_signature(params_to_sign, config['API_SECRET']);
            params_to_send = {"api-key": config['API_KEY'], "t": t, "api-signature": signature}
            try:
                r = client.get(ENDPOINT.format(station_id=config['STATION_ID']), params=params_to_send)
                r.raise_for_status();
                response_json = r.json()
                s = next((s['data'][0] for s in response_json.get('sensors', []) if
                          (s.get('data_structure_type') == 10 or s.get('sensor_type') == 48) and s.get('data')), None)

                if not s or 'ts' not in s: continue

                acumulado_dia = float(s.get('rainfall_daily_mm', 0.0) or 0.0)
                novos_acumulados[id_ponto] = acumulado_dia

                if (int(t) - s['ts']) > 3600:
                    adicionar_log(id_ponto, "Dados atrasados/offline.", level="WARN", salvar_arquivo=True)
                    continue

                dados.append({
                    "timestamp": arredondar_timestamp_10min(s['ts']),
                    "id_ponto": id_ponto,
                    "chuva_mm": 0.0,
                    "precipitacao_acumulada_mm": acumulado_dia
                })
            except Exception as e:
                adicionar_log(id_ponto, f"Erro API WL: {e}", level="ERROR", salvar_arquivo=True)

    df = pd.DataFrame(dados);
    if not df.empty and 'timestamp' in df.columns: df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)

    return df, novos_acumulados


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
    adicionar_log(ID_PONTO_ZENTRA_KM72, f"Buscando dados da Zentra Cloud.", level="INFO", salvar_arquivo=False)
    with httpx.Client() as client:
        for attempt in range(3):
            r = _get_readings_zentra(client, ZENTRA_STATION_SERIAL, start_date, end_date)
            if r and r.status_code == 200: break
            if r and r.status_code == 429 and attempt < 2: time.sleep(60)
    if not r or r.status_code != 200:
        adicionar_log(ID_PONTO_ZENTRA_KM72, f"Erro Zentra: {r.status_code if r else 'N/A'}", level="ERROR",
                      salvar_arquivo=True);
        return pd.DataFrame()
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
        adicionar_log(ID_PONTO_ZENTRA_KM72, f"Erro JSON Zentra: {e}", level="ERROR", salvar_arquivo=True);
        return pd.DataFrame()


def backfill_zentra_km72_data(): pass


def backfill_weatherlink_data(id_ponto): pass