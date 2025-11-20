# data_source.py (CORREÇÃO FINAL E DEFINITIVA)

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
DATA_DIR = "."
DB_CONNECTION_STRING = ""
STATUS_FILE = os.path.join(DATA_DIR, "status_atual.json")
LOG_FILE = os.path.join(DATA_DIR, "eventos.log")
DB_ENGINE = None  # Engine global para otimização de concorrência

COLUNAS_HISTORICO = [
    'timestamp', 'id_ponto', 'chuva_mm', 'precipitacao_acumulada_mm',
    'umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc',
    'base_1m', 'base_2m', 'base_3m'
]

# --- INÍCIO DA NOVA FUNÇÃO DE ESCRITA SEGURA ---
def write_with_timeout(file_path, content, mode='w', timeout=15):
    """
    Escreve conteúdo em um arquivo com um timeout para prevenir bloqueios de I/O.
    Levanta um TimeoutError se a escrita demorar mais que o timeout especificado.
    """
    def target():
        try:
            with open(file_path, mode, encoding='utf-8') as f:
                if isinstance(content, str):
                    f.write(content)
                else: # Assume que é um objeto JSON para dar dump
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
        # A thread ainda está rodando, o que significa que o timeout foi atingido.
        raise TimeoutError(f"A escrita no arquivo '{os.path.basename(file_path)}' excedeu o timeout de {timeout}s.")

    if not target.success:
        # A thread terminou, mas falhou.
        raise target.error
# --- FIM DA NOVA FUNÇÃO DE ESCRITA SEGURA ---


def adicionar_log(id_ponto, mensagem):
    """ Adiciona uma entrada de log usando o wrapper de escrita segura. """
    try:
        log_entry = f"{datetime.datetime.now(datetime.timezone.utc).isoformat()} | {id_ponto} | {mensagem}\n"
        # Usa o novo wrapper para a escrita, com timeout mais curto para logs.
        write_with_timeout(LOG_FILE, log_entry, mode='a', timeout=10)
    except Exception as e:
        # Se até a escrita segura falhar, imprime no console como último recurso.
        print(f"ERRO CRÍTICO ao escrever no log (após timeout ou falha): {e}")


def ler_logs_eventos(id_ponto):
    """
    Lê o log do arquivo eventos.log e retorna o conteúdo filtrado como string.
    """
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
    """
    Define os caminhos de disco e, o mais importante, inicializa o DB_ENGINE global.
    """
    print("--- data_source.py (v12.18 - Engine Global Otimizado) ---")
    global DATA_DIR, STATUS_FILE, LOG_FILE, DB_CONNECTION_STRING, DB_ENGINE

    DB_FILENAME = "temp_local_db.db"

    if os.environ.get('RENDER'):
        DATA_DIR = "/var/data"
        DB_CONNECTION_STRING = f'sqlite:///{os.path.join(DATA_DIR, DB_FILENAME)}'
    else:
        DATA_DIR = "."
        DB_CONNECTION_STRING = f'sqlite:///{DB_FILENAME}'

    # --- FIX CRÍTICO: Cria o Engine GLOBAL uma única vez. ---
    # Adiciona connect_args={"timeout": 30} para dar 30s para o leitor adquirir o lock do DB.
    DB_ENGINE = create_engine(DB_CONNECTION_STRING, connect_args={"timeout": 30})

    STATUS_FILE = os.path.join(DATA_DIR, "status_atual.json")
    LOG_FILE = os.path.join(DATA_DIR, "eventos.log")

    print(f"Caminho do Disco de Dados: {DATA_DIR}")
    print(f"Banco de Dados (Fonte da Verdade): {DB_CONNECTION_STRING}")


def initialize_database():
    """Verifica e cria a tabela e índices no SQLite, usando o DB_ENGINE global."""
    global DB_ENGINE
    if DB_ENGINE is None:
        raise Exception("DB_ENGINE não foi inicializado. Chame setup_disk_paths() primeiro.")

    try:
        inspector = inspect(DB_ENGINE)
        if not inspector.has_table(DB_TABLE_NAME):
            df_vazio = pd.DataFrame(columns=COLUNAS_HISTORICO)
            df_vazio.to_sql(DB_TABLE_NAME, DB_ENGINE, index=False)
            print(f"Tabela '{DB_TABLE_NAME}' criada.")

        existing_indexes = [index['name'] for index in inspector.get_indexes(DB_ENGINE, DB_TABLE_NAME)]
        with DB_ENGINE.connect() as connection:
            if 'idx_timestamp' not in existing_indexes:
                connection.execute(text(f'CREATE INDEX idx_timestamp ON {DB_TABLE_NAME} (timestamp)'))
                print("Índice 'idx_timestamp' criado com sucesso.")
            if 'idx_id_ponto' not in existing_indexes:
                connection.execute(text(f'CREATE INDEX idx_id_ponto ON {DB_TABLE_NAME} (id_ponto)'))
                print("Índice 'idx_id_ponto' criado com sucesso.")
            connection.commit()

        print("Banco de dados verificado e pronto.")
    except Exception as e:
        adicionar_log("GERAL", f"ERRO CRÍTICO ao inicializar o banco de dados: {e}")


def save_to_sqlite(df_novos_dados):
    """Salva o DataFrame no SQLite, usando o DB_ENGINE global."""
    global DB_ENGINE
    if df_novos_dados.empty: return
    try:
        df_para_salvar = df_novos_dados.copy()

        if 'timestamp' in df_para_salvar.columns:
            df_para_salvar['timestamp'] = pd.to_datetime(df_para_salvar['timestamp'], utc=True)

        colunas_para_salvar = [col for col in COLUNAS_HISTORICO if col in df_para_salvar.columns]
        df_para_salvar = df_para_salvar[colunas_para_salvar]
        
        # --- INÍCIO DA ALTERAÇÃO: Adiciona log ---
        print(f"[WORKER LOG] ...salvando {len(df_para_salvar)} linhas no banco de dados.")
        # --- FIM DA ALTERAÇÃO ---
        
        df_para_salvar.to_sql(DB_TABLE_NAME, DB_ENGINE, if_exists='append', index=False)
    except Exception as e:
        if "UNIQUE constraint failed" in str(e):
            print(f"[SQLite] Aviso: Dados duplicados ignorados.")
        else:
            adicionar_log("GERAL", f"ERRO CRÍTICO ao salvar no SQLite: {e}")


def read_data_from_sqlite(id_ponto=None, start_dt=None, end_dt=None, last_hours=None):
    """Lê dados do SQLite, usando o DB_ENGINE global."""
    global DB_ENGINE
    query_base = f"SELECT * FROM {DB_TABLE_NAME}"
    conditions, params = [], {}
    if id_ponto:
        conditions.append("id_ponto = :ponto");
        params["ponto"] = id_ponto
    if last_hours:
        start_dt = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=last_hours)

    if start_dt:
        conditions.append("timestamp >= :start")
        params["start"] = start_dt.strftime('%Y-%m-%d %H:%M:%S')
    if end_dt:
        conditions.append("timestamp < :end")
        params["end"] = end_dt.strftime('%Y-%m-%d %H:%M:%S')

    if conditions:
        query_base += " WHERE " + " AND ".join(conditions)
    query_base += " ORDER BY timestamp ASC"
    try:
        df = pd.read_sql_query(query_base, DB_ENGINE, params=params, parse_dates=["timestamp"])
        if 'timestamp' in df.columns and df['timestamp'].dt.tz is None:
            df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
        return df
    except Exception as e:
        adicionar_log("GERAL", f"ERRO CRÍTICO ao ler do SQLite: {e}")
        return pd.DataFrame()


def get_recent_data_for_worker(hours=73):
    return read_data_from_sqlite(last_hours=hours)


def get_all_data_for_dashboard():
    # --- INÍCIO DA CORREÇÃO DEFINITIVA: Aumentar janela de dados para o dashboard ---
    # Aumenta a janela para 100 horas para garantir que o cálculo de 72h (e outros) sempre tenha dados suficientes.
    df_completo = read_data_from_sqlite(last_hours=100)
    # --- FIM DA CORREÇÃO DEFINITIVA ---

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


def get_last_n_entries(n=4):
    """
    Retorna o registro mais recente para CADA PONTO (o que garante que
    o print mostre o status de todos os KMs).
    """
    global DB_ENGINE
    # Busca um número suficiente de registros (50) ordenados pelo mais recente.
    query = f"SELECT * FROM {DB_TABLE_NAME} ORDER BY timestamp DESC LIMIT 50"

    try:
        df = pd.read_sql_query(query, DB_ENGINE, parse_dates=["timestamp"])
        if 'timestamp' in df.columns and df['timestamp'].dt.tz is None:
            df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')

        if df.empty:
            return df

        # Agrupa por 'id_ponto' e mantém o primeiro (mais recente) de cada grupo
        df_ultimos_por_ponto = df.drop_duplicates(subset=['id_ponto'], keep='first')

        # Ordena o resultado final apenas pelo timestamp (para melhor visualização)
        return df_ultimos_por_ponto.sort_values(by='timestamp', ascending=False)

    except Exception as e:
        adicionar_log("GERAL", f"ERRO ao buscar últimos registros por ponto: {e}")
        return pd.DataFrame()


def calculate_hmac_signature(params, api_secret):
    string_para_assinar = "".join(f"{key}{params[key]}" for key in sorted(params.keys()))
    return hmac.new(api_secret.encode('utf-8'), string_para_assinar.encode('utf-8'), hashlib.sha256).hexdigest()


def arredondar_timestamp_15min(ts_epoch):
    dt_obj = datetime.datetime.fromtimestamp(ts_epoch, datetime.timezone.utc)
    return (dt_obj.replace(second=0, microsecond=0, minute=(dt_obj.minute // 15) * 15)).isoformat()


def fetch_data_from_weatherlink_api():
    ENDPOINT = "https://api.weatherlink.com/v2/current/{station_id}"
    logs, dados = [], []
    t = str(int(datetime.datetime.now(datetime.timezone.utc).timestamp()))

    with httpx.Client(timeout=30.0) as client:
        for id_ponto, config in WEATHERLINK_CONFIG.items():
            if "SUA_CHAVE" in config.get('API_KEY', ''):
                print(f"[API] PULANDO {id_ponto}: Chave não configurada/contém placeholder.")
                continue

            print(f"[API] REQUISITANDO {id_ponto} (Station ID: {config['STATION_ID']})")

            params_to_sign = {"api-key": config['API_KEY'], "station-id": str(config['STATION_ID']), "t": t}
            signature = calculate_hmac_signature(params_to_sign, config['API_SECRET'])
            params_to_send = {"api-key": config['API_KEY'], "t": t, "api-signature": signature}

            # --- INÍCIO DA CORREÇÃO DE ROBUSTEZ DE REDE ---
            def do_request():
                try:
                    do_request.response = client.get(ENDPOINT.format(station_id=config['STATION_ID']), params=params_to_send, timeout=25.0)
                except Exception as e:
                    do_request.error = e
            
            do_request.response = None
            do_request.error = None
            
            request_thread = threading.Thread(target=do_request)
            request_thread.start()
            request_thread.join(timeout=30.0) # Timeout geral de 30s

            if request_thread.is_alive():
                logs.append({'id_ponto': id_ponto, 'mensagem': f"ERRO API: Timeout de 30s excedido para a estação."})
                continue # Pula para a próxima estação

            if do_request.error:
                logs.append({'id_ponto': id_ponto, 'mensagem': f"ERRO API: {do_request.error}"})
                continue

            r = do_request.response
            # --- FIM DA CORREÇÃO DE ROBUSTEZ DE REDE ---

            try:
                r.raise_for_status()
                response_json = r.json()
                print(f"WeatherLink API Response for {id_ponto}: Status 200, JSON: {response_json}")

                s = next((s['data'][0] for s in response_json.get('sensors', []) if (s.get('data_structure_type') == 10 or s.get('sensor_type') == 48) and s.get('data')), None)

                if not s or 'ts' not in s or (int(t) - s['ts']) > 3600:
                    logs.append({'id_ponto': id_ponto, 'mensagem': 'AVISO API: Estação offline ou dados atrasados.'})
                    continue

                chuva_15 = s.get('rainfall_last_15_min_mm', 0.0)
                if chuva_15 is None: chuva_15 = 0.0

                dados.append({
                    "timestamp": arredondar_timestamp_15min(s['ts']),
                    "id_ponto": id_ponto,
                    "chuva_mm": chuva_15,
                    "precipitacao_acumulada_mm": s.get('rainfall_daily_mm')
                })
                logs.append({'id_ponto': id_ponto, 'mensagem': f"API: Sucesso. Chuva: {chuva_15:.2f}mm"})
            except Exception as e:
                logs.append({'id_ponto': id_ponto, 'mensagem': f"ERRO API (Processamento): {e}"})

    df = pd.DataFrame(dados)
    if not df.empty and 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)

    print("\n--- DEBUG: DataFrame WeatherLink FINAL antes de salvar ---")
    print(df.to_string())
    print("----------------------------------------------------------\n")

    return df, None, logs


def _get_readings_zentra(client, station_serial, start_date, end_date):
    url = f"{ZENTRA_BASE_URL}/get_readings/"
    params = {"device_sn": station_serial, "start": start_date.strftime("%Y-%m-%d"),
              "end": end_date.strftime("%Y-%m-%d")}
    headers = {"Authorization": f"Token {ZENTRA_API_TOKEN}"}
    try:
        # Adicionado timeout explícito por requisição
        return client.get(url, headers=headers, params=params, timeout=30.0)
    except httpx.TimeoutException:
        return None


def fetch_data_from_zentra_cloud():
    end_date = datetime.datetime.now(datetime.timezone.utc)
    start_date = end_date - datetime.timedelta(days=1)

    print(f"[API] BUSCANDO DADOS ZENTRA (Incremental) para {ID_PONTO_ZENTRA_KM72}")

    with httpx.Client() as client:
        for attempt in range(3):
            r = _get_readings_zentra(client, ZENTRA_STATION_SERIAL, start_date, end_date)
            if r and r.status_code == 200: break
            if r and r.status_code == 429 and attempt < 2: time.sleep(60)
    if not r or r.status_code != 200:
        adicionar_log(ID_PONTO_ZENTRA_KM72, f"ERRO API Zentra (Incremental): Status {r.status_code if r else 'N/A'}")
        return None, None
    try:
        wc_data = next((d for n, d in r.json().get('data', {}).items() if 'water content' in n.lower()), None)
        if not wc_data:
            print(f"[API] Zentra: 'water content' não encontrado na resposta.")
            return None, None
        
        latest_timestamp = None
        latest_readings = {}
        
        for s_block in wc_data:
            port = s_block.get('metadata', {}).get('port_number')
            if port in MAPA_ZENTRA_KM72 and s_block.get('readings'):
                # Pega a leitura mais recente (primeira da lista)
                reading = s_block['readings'][0]
                val = reading.get('value')
                ts_iso = reading.get('datetime')

                if val is not None:
                    latest_readings[MAPA_ZENTRA_KM72[port]] = float(val) * 100.0
                
                # Atualiza o timestamp mais recente encontrado em todos os sensores
                if ts_iso:
                    current_ts = datetime.datetime.fromisoformat(ts_iso).timestamp()
                    if latest_timestamp is None or current_ts > latest_timestamp:
                        latest_timestamp = current_ts

        print(f"[API] Zentra: Dados de umidade lidos: {latest_readings} com timestamp {latest_timestamp}")
        return latest_timestamp, latest_readings if latest_readings else None
    except Exception as e:
        adicionar_log(ID_PONTO_ZENTRA_KM72, f"ERRO Zentra (Processamento JSON): {e}")
        return None, None


def backfill_zentra_km72_data():
    id_ponto = ID_PONTO_ZENTRA_KM72
    adicionar_log(id_ponto, "Iniciando backfill para Zentra.")
    df_ponto_existente = read_data_from_sqlite(id_ponto=id_ponto)
    start_date = df_ponto_existente['timestamp'].max() if not df_ponto_existente.empty else datetime.datetime(2023, 1,
                                                                                                              1,
                                                                                                              tzinfo=datetime.timezone.utc)
    end_date = datetime.datetime.now(datetime.timezone.utc)
    total_registros_salvos = 0
    current_start = start_date

    start_time = time.time()
    time_limit = BACKFILL_RUN_TIME_SEC

    while current_start < end_date:
        if (time.time() - start_time) > time_limit:
            adicionar_log(id_ponto,
                          f"PAUSA (Backfill Zentra): Limite de {time_limit}s atingido. {total_registros_salvos} salvos nesta sessao.")
            return False

        period_end = min(current_start + datetime.timedelta(days=7), end_date)
        print(f"[API Zentra Backfill] Buscando bloco: {current_start.date()} a {period_end.date()}")
        with httpx.Client(timeout=60.0) as client:
            response = _get_readings_zentra(client, ZENTRA_STATION_SERIAL, current_start, period_end)
        if not response or response.status_code != 200:
            current_start += datetime.timedelta(days=7);
            continue
        try:
            wc_data = next((d for n, d in response.json().get('data', {}).items() if 'water content' in n.lower()),
                           None)
            if not wc_data: current_start += datetime.timedelta(days=7); continue
            dados_por_timestamp = {}
            for sensor_block in wc_data:
                port_num = sensor_block.get('metadata', {}).get('port_number')
                if port_num in MAPA_ZENTRA_KM72:
                    coluna = MAPA_ZENTRA_KM72[port_num]
                    for reading in sensor_block.get('readings', []):
                        ts_iso, value = reading.get('datetime'), reading.get('value')
                        if ts_iso and value is not None:
                            ts_arredondado = arredondar_timestamp_15min(
                                datetime.datetime.fromisoformat(ts_iso).timestamp())
                            if ts_arredondado not in dados_por_timestamp: dados_por_timestamp[ts_arredondado] = {}
                            dados_por_timestamp[ts_arredondado][coluna] = float(value) * 100.0
            if dados_por_timestamp:
                df_bloco = pd.DataFrame(
                    [{'timestamp': ts, 'id_ponto': id_ponto, **v} for ts, v in dados_por_timestamp.items()])
                df_bloco['timestamp'] = pd.to_datetime(df_bloco['timestamp'], utc=True)
                save_to_sqlite(df_bloco)
                total_registros_salvos += len(df_bloco)
        except Exception as e:
            adicionar_log(id_ponto, f"ERRO Zentra (Backfill Processamento): {e}")
        current_start += datetime.timedelta(days=7)
        time.sleep(2)
    if total_registros_salvos > 0:
        adicionar_log(id_ponto, f"SUCESSO (Backfill Zentra): {total_registros_salvos} registros salvos.")

    return True


def backfill_weatherlink_data(id_ponto):
    station_config = WEATHERLINK_CONFIG.get(id_ponto)
    if not station_config or "SUA_CHAVE" in station_config.get('API_KEY', ''): return
    station_id, api_key, api_secret = station_config['STATION_ID'], station_config['API_KEY'], station_config[
        'API_SECRET']
    print(f"[API {id_ponto}] Iniciando Backfill de 96h...")
    url_base = f"https://api.weatherlink.com/v2/historic/{station_id}"
    dados_processados = []
    now_dt = datetime.datetime.now(datetime.timezone.utc)

    start_time = time.time()
    time_limit = BACKFILL_RUN_TIME_SEC

    try:
        with httpx.Client(timeout=60.0) as client:
            for i in range(4):
                if (time.time() - start_time) > time_limit:
                    adicionar_log(id_ponto, f"PAUSA (Backfill WL): Limite de {time_limit}s atingido.")
                    return False

                end_dt = now_dt - datetime.timedelta(hours=24 * i)
                start_dt = end_dt - datetime.timedelta(hours=24)
                t = str(int(now_dt.timestamp()))

                params_to_sign = {
                    "api-key": api_key,
                    "end-timestamp": str(int(end_dt.timestamp())),
                    "start-timestamp": str(int(start_dt.timestamp())),
                    "station-id": str(station_id),
                    "t": t
                }
                signature = calculate_hmac_signature(params_to_sign, api_secret)

                params_to_send = {
                    "api-key": api_key,
                    "end-timestamp": str(int(end_dt.timestamp())),
                    "start-timestamp": str(int(start_dt.timestamp())),
                    "t": t,
                    "api-signature": signature
                }

                response = client.get(url_base, params=params_to_send, timeout=30.0)
                response.raise_for_status()

                data = response.json()
                for sensor in data.get('sensors', []):
                    if sensor.get('sensor_type') == 48 or sensor.get('data_structure_type') == 10:
                        for reg in sensor.get('data', []):
                            if 'rainfall_mm' in reg:
                                dados_processados.append({
                                    "timestamp": arredondar_timestamp_15min(reg['ts']),
                                    "id_ponto": id_ponto,
                                    "chuva_mm": reg['rainfall_mm'],
                                    "precipitacao_acumulada_mm": reg.get('rainfall_daily_mm')
                                })
                if i < 3: time.sleep(1)
        if not dados_processados: return True
        df_backfill = pd.DataFrame(dados_processados)
        df_backfill['timestamp'] = pd.to_datetime(df_backfill['timestamp'], utc=True)
        save_to_sqlite(df_backfill)
        adicionar_log(id_ponto, f"SUCESSO (Backfill WeatherLink): {len(df_backfill)} registros salvos.")
        return True

    except HTTPStatusError as e:
        if e.response.status_code == 401:
            adicionar_log(id_ponto, f"AVISO: Falha na autenticação (401) para {id_ponto}. Backfill pulado.")
            print(f"[API {id_ponto}] AVISO: Falha na autenticação (401). Backfill pulado.")
            return True
        else:
            adicionar_log(id_ponto, f"ERRO API WeatherLink (Backfill): {e}")
            print(f"[API {id_ponto}] ERRO API WeatherLink (Backfill): {e}")
            traceback.print_exc()
            return True
    except Exception as e:
        adicionar_log(id_ponto, f"ERRO CRÍTICO (Backfill WeatherLink): {e}")
        print(f"[API {id_ponto}] ERRO CRÍTICO (Backfill WeatherLink): {e}")
        traceback.print_exc()
        return True