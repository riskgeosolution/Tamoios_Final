# data_source.py (v15.21: Reversão da lógica Zentra e manutenção WeatherLink)

import pandas as pd
import json
import os
import datetime
import httpx
import requests
import traceback
import hashlib
import hmac
from io import StringIO
import warnings
import time
from sqlalchemy import create_engine, inspect, text, types
from urllib.parse import urlencode
from threading import Lock

warnings.simplefilter(action='ignore', category=FutureWarning)

from config import (
    PONTOS_DE_ANALISE,
    WEATHERLINK_CONFIG, DB_TABLE_NAME,
    ZENTRA_API_TOKEN, ZENTRA_STATION_SERIAL, ZENTRA_BASE_URL,
    MAPA_ZENTRA_KM72, ID_PONTO_ZENTRA_KM72
)

DB_LOCK = Lock()

DATA_DIR = "."
DB_CONNECTION_STRING = "" 
STATUS_FILE = os.path.join(DATA_DIR, "status_atual.json")
LOG_FILE = os.path.join(DATA_DIR, "eventos.log")

_db_engine = None

def get_engine():
    global _db_engine
    if _db_engine is None:
        if not DB_CONNECTION_STRING:
            setup_disk_paths()
        _db_engine = create_engine(DB_CONNECTION_STRING)
    return _db_engine

def adicionar_log(id_ponto, mensagem):
    try:
        log_entry = f"{datetime.datetime.now(datetime.timezone.utc).isoformat()} | {id_ponto} | {mensagem}\n"
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(log_entry)
    except Exception as e:
        print(f"ERRO CRÍTICO ao escrever no log: {e}")

def setup_disk_paths():
    print("--- data_source.py (v15.21) ---")
    global DATA_DIR, STATUS_FILE, LOG_FILE, DB_CONNECTION_STRING
    DB_FILENAME = "temp_local_db.db" 
    if os.environ.get('RENDER'):
        DATA_DIR = "/var/data"
        db_path = os.path.join(DATA_DIR, DB_FILENAME)
        DB_CONNECTION_STRING = f'sqlite:///{db_path}?timeout=30&journal_mode=WAL'
    else:
        DATA_DIR = "."
        db_path = DB_FILENAME
        DB_CONNECTION_STRING = f'sqlite:///{db_path}?timeout=30&journal_mode=WAL'
    STATUS_FILE = os.path.join(DATA_DIR, "status_atual.json")
    LOG_FILE = os.path.join(DATA_DIR, "eventos.log")

def initialize_database():
    with DB_LOCK:
        try:
            engine = get_engine()
            inspector = inspect(engine)
            if not inspector.has_table(DB_TABLE_NAME):
                colunas_com_tipos = {
                    'timestamp': types.TIMESTAMP(timezone=True),
                    'id_ponto': types.String,
                    'chuva_mm': types.Float,
                    'precipitacao_acumulada_mm': types.Float,
                    'umidade_1m_perc': types.Float,
                    'umidade_2m_perc': types.Float,
                    'umidade_3m_perc': types.Float,
                    'base_1m': types.Float,
                    'base_2m': types.Float,
                    'base_3m': types.Float
                }
                df_vazio = pd.DataFrame({col: pd.Series(dtype=typ) for col, typ in {
                    'timestamp': 'datetime64[ns, UTC]', 'id_ponto': 'object', 'chuva_mm': 'float64', 
                    'precipitacao_acumulada_mm': 'float64', 'umidade_1m_perc': 'float64', 
                    'umidade_2m_perc': 'float64', 'umidade_3m_perc': 'float64', 'base_1m': 'float64', 
                    'base_2m': 'float64', 'base_3m': 'float64'}.items()})
                df_vazio.to_sql(DB_TABLE_NAME, engine, index=False, dtype=colunas_com_tipos)
                print(f"Tabela '{DB_TABLE_NAME}' criada com tipos de dados corretos.")

            with engine.connect() as connection:
                inspector = inspect(connection)
                if 'idx_timestamp' not in [index['name'] for index in inspector.get_indexes(DB_TABLE_NAME)]:
                    connection.execute(text(f'CREATE INDEX idx_timestamp ON {DB_TABLE_NAME} (timestamp)'))
                if 'idx_id_ponto' not in [index['name'] for index in inspector.get_indexes(DB_TABLE_NAME)]:
                    connection.execute(text(f'CREATE INDEX idx_id_ponto ON {DB_TABLE_NAME} (id_ponto)'))
            print("Banco de dados verificado e pronto.")
        except Exception as e:
            adicionar_log("GERAL", f"ERRO CRÍTICO ao inicializar o banco de dados: {e}")

def save_to_sqlite(df_novos_dados):
    if df_novos_dados.empty: return
    with DB_LOCK:
        try:
            engine = get_engine()
            df_para_salvar = df_novos_dados.copy()
            if 'timestamp' in df_para_salvar.columns:
                df_para_salvar['timestamp'] = pd.to_datetime(df_para_salvar['timestamp']).dt.to_pydatetime()
            
            inspector = inspect(engine)
            colunas_db = [col['name'] for col in inspector.get_columns(DB_TABLE_NAME)]
            colunas_para_salvar = [col for col in df_para_salvar.columns if col in colunas_db]
            df_para_salvar = df_para_salvar[colunas_para_salvar]

            df_para_salvar.to_sql(DB_TABLE_NAME, engine, if_exists='append', index=False)
        except Exception as e:
            if "UNIQUE constraint failed" in str(e):
                print(f"[SQLite] Aviso: Dados duplicados ignorados.")
            else:
                adicionar_log("GERAL", f"ERRO CRÍTICO ao salvar no SQLite: {e}")

def read_data_from_sqlite(id_ponto=None, start_dt=None, end_dt=None, last_hours=None):
    with DB_LOCK:
        engine = get_engine()
        query_base = f"SELECT * FROM {DB_TABLE_NAME}"
        conditions, params = [], {}
        if id_ponto:
            conditions.append("id_ponto = :ponto"); params["ponto"] = id_ponto
        if last_hours:
            start_dt = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=last_hours)
        if start_dt:
            conditions.append("timestamp >= :start"); params["start"] = start_dt.strftime('%Y-%m-%d %H:%M:%S')
        if end_dt:
            conditions.append("timestamp < :end"); params["end"] = end_dt.strftime('%Y-%m-%d %H:%M:%S')
        if conditions:
            query_base += " WHERE " + " AND ".join(conditions)
        query_base += " ORDER BY timestamp ASC"
        try:
            df = pd.read_sql_query(query_base, engine, params=params, parse_dates=["timestamp"])
            if 'timestamp' in df.columns and not df.empty and df['timestamp'].dt.tz is None:
                df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
            return df
        except Exception as e:
            adicionar_log("GERAL", f"ERRO CRÍTICO ao ler do SQLite: {e}")
            return pd.DataFrame()

def get_recent_data_for_worker(hours=73):
    return read_data_from_sqlite(last_hours=hours)

def get_all_data_for_dashboard():
    df_completo = read_data_from_sqlite(last_hours=7*24)
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
        return {p: "INDEFINIDO" for p in PONTOS_DE_ANALISE.keys()}

def arredondar_timestamp_15min(ts_epoch):
    dt_obj = datetime.datetime.fromtimestamp(ts_epoch, datetime.timezone.utc)
    return (dt_obj.replace(second=0, microsecond=0, minute=(dt_obj.minute // 15) * 15)).isoformat()

def fetch_data_from_weatherlink_api():
    BASE_URL = "https://api.weatherlink.com"
    dados = []
    logs = []
    for id_ponto, config in WEATHERLINK_CONFIG.items():
        api_key, api_secret, station_id = config.get('API_KEY'), config.get('API_SECRET'), str(config.get('STATION_ID'))
        if not all([api_key, api_secret, station_id]) or "SUA_CHAVE" in api_key: continue
        agora_ts = int(time.time())
        params_para_assinar = {"api-key": api_key, "t": str(agora_ts), "station-id": station_id}
        string_para_assinar = "".join(f"{key}{params_para_assinar[key]}" for key in sorted(params_para_assinar.keys()))
        api_signature = hmac.new(api_secret.encode('utf-8'), string_para_assinar.encode('utf-8'), hashlib.sha256).hexdigest()
        url_params = {"api-key": api_key, "t": str(agora_ts), "api-signature": api_signature}
        endpoint = f"/v2/current/{station_id}"
        full_url = f"{BASE_URL}{endpoint}?{urlencode(url_params)}"
        try:
            response = requests.get(full_url)
            response.raise_for_status()
            s = next((s['data'][0] for s in response.json().get('sensors', []) if s.get('data_structure_type') == 10 and s.get('data')), None)
            if not s or not (ts_sensor := s.get('ts')): continue
            if (agora_ts - ts_sensor) > 1800:
                adicionar_log(id_ponto, f"AVISO API: Dados de chuva desatualizados.")
                continue
            dados.append({"timestamp": arredondar_timestamp_15min(ts_sensor), "id_ponto": id_ponto, "chuva_mm": s.get('rainfall_last_15_min_mm', 0.0)})
            logs.append({'id_ponto': id_ponto, 'mensagem': f"API: Sucesso. Chuva 15min: {s.get('rainfall_last_15_min_mm', 0.0):.2f}mm"})
        except Exception as e:
            adicionar_log(id_ponto, f"ERRO API WeatherLink: {e}")
            logs.append({'id_ponto': id_ponto, 'mensagem': f"ERRO API: {e}"})
    return pd.DataFrame(dados), None, logs

def _get_readings_zentra(client, station_serial, start_date, end_date):
    url = f"{ZENTRA_BASE_URL}/get_readings/"
    params = {"device_sn": station_serial, "start": start_date.strftime("%Y-%m-%d"), "end": end_date.strftime("%Y-%m-%d")}
    headers = {"Authorization": f"Token {ZENTRA_API_TOKEN}"}
    try:
        return client.get(url, headers=headers, params=params, timeout=20.0)
    except httpx.TimeoutException:
        adicionar_log(ID_PONTO_ZENTRA_KM72, "AVISO API Zentra: Timeout ao buscar dados.")
        return None

def fetch_data_from_zentra_cloud():
    end_date = datetime.datetime.now(datetime.timezone.utc)
    start_date = end_date - datetime.timedelta(days=1)
    with httpx.Client() as client:
        r = None
        for attempt in range(3):
            r = _get_readings_zentra(client, ZENTRA_STATION_SERIAL, start_date, end_date)
            if r and r.status_code == 200: break
            if r and r.status_code == 429 and attempt < 2: time.sleep(60)
            time.sleep(5)
    if not r or r.status_code != 200:
        adicionar_log(ID_PONTO_ZENTRA_KM72, f"ERRO API Zentra (Incremental): Status {r.status_code if r else 'N/A'}")
        return None
    try:
        json_data = r.json()
        wc_data = next((d for n, d in json_data.get('data', {}).items() if 'water content' in n.lower()), None)
        if not wc_data: return None
        
        latest_readings = {}
        for s_block in wc_data:
            port = s_block.get('metadata', {}).get('port_number')
            if port in MAPA_ZENTRA_KM72 and (readings := s_block.get('readings')):
                latest_reading = readings[0]
                if (val := latest_reading.get('value')) is not None:
                    latest_readings[MAPA_ZENTRA_KM72[port]] = float(val) * 100.0
        
        return latest_readings if latest_readings else None
        
    except Exception as e:
        adicionar_log(ID_PONTO_ZENTRA_KM72, f"ERRO Zentra (Processamento JSON Incremental): {e}")
        return None

def backfill_zentra_km72_data():
    id_ponto = ID_PONTO_ZENTRA_KM72
    adicionar_log(id_ponto, "Iniciando backfill para Zentra.")
    df_ponto_existente = read_data_from_sqlite(id_ponto=id_ponto)
    start_date = df_ponto_existente['timestamp'].max() if not df_ponto_existente.empty else datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc)
    end_date = datetime.datetime.now(datetime.timezone.utc)
    total_registros_salvos = 0
    current_start = start_date
    while current_start < end_date:
        period_end = min(current_start + datetime.timedelta(days=7), end_date)
        print(f"[API Zentra Backfill] Buscando bloco: {current_start.date()} a {period_end.date()}")
        with httpx.Client(timeout=60.0) as client:
            response = _get_readings_zentra(client, ZENTRA_STATION_SERIAL, current_start, period_end)
        if not response or response.status_code != 200:
            current_start += datetime.timedelta(days=7); continue
        try:
            wc_data = next((d for n, d in response.json().get('data', {}).items() if 'water content' in n.lower()), None)
            if not wc_data: current_start += datetime.timedelta(days=7); continue
            dados_por_timestamp = {}
            for sensor_block in wc_data:
                port_num = sensor_block.get('metadata', {}).get('port_number')
                if port_num in MAPA_ZENTRA_KM72:
                    coluna = MAPA_ZENTRA_KM72[port_num]
                    for reading in sensor_block.get('readings', []):
                        ts_iso, value = reading.get('datetime'), reading.get('value')
                        if ts_iso and value is not None:
                            ts_arredondado = arredondar_timestamp_15min(datetime.datetime.fromisoformat(ts_iso).timestamp())
                            if ts_arredondado not in dados_por_timestamp: dados_por_timestamp[ts_arredondado] = {}
                            dados_por_timestamp[ts_arredondado][coluna] = float(value) * 100.0
            if dados_por_timestamp:
                df_bloco = pd.DataFrame([{'timestamp': ts, 'id_ponto': id_ponto, **v} for ts, v in dados_por_timestamp.items()])
                df_bloco['timestamp'] = pd.to_datetime(df_bloco['timestamp'], utc=True)
                save_to_sqlite(df_bloco)
                total_registros_salvos += len(df_bloco)
        except Exception as e:
            adicionar_log(id_ponto, f"ERRO Zentra (Backfill Processamento): {e}")
        current_start += datetime.timedelta(days=7)
        time.sleep(2)
    if total_registros_salvos > 0:
        adicionar_log(id_ponto, f"SUCESSO (Backfill Zentra): {total_registros_salvos} registros salvos.")

def backfill_weatherlink_data(id_ponto):
    station_config = WEATHERLINK_CONFIG.get(id_ponto)
    if not station_config or "SUA_CHAVE" in station_config.get('API_KEY', ''): return
    station_id, api_key, api_secret = station_config['STATION_ID'], station_config['API_KEY'], station_config['API_SECRET']
    print(f"[API {id_ponto}] Iniciando Backfill de 72h...")
    url_base = f"https://api.weatherlink.com/v2/historic/{station_id}"
    dados_processados = []
    now_dt = datetime.datetime.now(datetime.timezone.utc)
    try:
        with httpx.Client(timeout=60.0) as client:
            for i in range(3):
                end_dt = now_dt - datetime.timedelta(hours=24 * i)
                start_dt = end_dt - datetime.timedelta(hours=24)
                t = str(int(now_dt.timestamp()))
                params = {"api-key": api_key, "end-timestamp": str(int(end_dt.timestamp())), "start-timestamp": str(int(start_dt.timestamp())), "station-id": str(station_id), "t": t}
                params["api-signature"] = calculate_hmac_signature(params, api_secret)
                response = client.get(url_base, params=params)
                response.raise_for_status()
                data = response.json()
                for sensor in data.get('sensors', []):
                    if sensor.get('sensor_type') == 48:
                        for reg in sensor.get('data', []):
                            if 'rainfall_mm' in reg:
                                dados_processados.append({"timestamp": arredondar_timestamp_15min(reg['ts']), "id_ponto": id_ponto, "chuva_mm": reg['rainfall_mm'], "precipitacao_acumulada_mm": reg.get('rainfall_daily_mm')})
                if i < 2: time.sleep(1)
        if not dados_processados: return
        df_backfill = pd.DataFrame(dados_processados)
        df_backfill['timestamp'] = pd.to_datetime(df_backfill['timestamp'], utc=True)
        save_to_sqlite(df_backfill)
        adicionar_log(id_ponto, f"SUCESSO (Backfill WeatherLink): {len(df_backfill)} registros salvos.")
    except Exception as e:
        adicionar_log(id_ponto, f"ERRO CRÍTICO (Backfill WeatherLink): {e}")
        traceback.print_exc()
