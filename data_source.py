# data_source.py (CORREÇÃO FINAL v15 - Lógica Híbrida: Sync, Backfill e Force)

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

# --- CONFIGURAÇÕES GLOBAIS ---
DB_CONNECTION_STRING = ""
STATUS_FILE = "status_atual.json"
LOG_FILE = "eventos.log"
DB_ENGINE = None

COLUNAS_HISTORICO = [
    'timestamp', 'id_ponto', 'chuva_mm', 'precipitacao_acumulada_mm',
    'umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc',
    'base_1m', 'base_2m', 'base_3m'
]


# --- FUNÇÕES UTILITÁRIAS ---

def write_with_timeout(file_path, content, mode='w', timeout=15):
    """Escreve em arquivo com timeout para evitar travamento no Render (Sistema de arquivos lento)."""

    def target():
        try:
            full_file_path = os.path.join("/var/data", file_path) if os.environ.get('RENDER') else os.path.join(
                os.getcwd(), file_path)
            with open(full_file_path, mode, encoding='utf-8') as f:
                if isinstance(content, str):
                    f.write(content)
                else:
                    json.dump(content, f, indent=2)
            target.success = True
        except Exception as e:
            target.error = e;
            target.success = False

    target.success = False
    thread = threading.Thread(target=target)
    thread.start()
    thread.join(timeout)
    if thread.is_alive(): raise TimeoutError(f"Timeout escrita: {os.path.basename(file_path)}")
    if not target.success: raise target.error


def adicionar_log(id_ponto, mensagem, level="INFO"):
    try:
        log_entry = f"{datetime.datetime.now(datetime.timezone.utc).isoformat()} | {level:<5} | {id_ponto} | {mensagem}\n"
        print(log_entry.strip())
        # Tenta escrever no arquivo, mas não falha se der erro (Render limpa arquivos)
        try:
            write_with_timeout(LOG_FILE, log_entry, mode='a', timeout=5)
        except:
            pass
    except Exception as e:
        print(f"ERRO CRÍTICO AO LOGAR: {e}")


def ler_logs_eventos(id_ponto):
    try:
        full_log_file_path = os.path.join("/var/data", LOG_FILE) if os.environ.get('RENDER') else os.path.join(
            os.getcwd(), LOG_FILE)
        if not os.path.exists(full_log_file_path): return "Nenhum evento registrado ainda."
        with open(full_log_file_path, 'r', encoding='utf-8') as f:
            logs_str = f.read()
        logs_list = logs_str.split('\n')
        logs_filtrados = [log for log in logs_list if f"| {id_ponto} |" in log or "| GERAL |" in log]
        return '\n'.join(logs_filtrados)
    except Exception as e:
        return f"ERRO ao ler logs: {e}"


# --- FUNÇÕES DE BANCO DE DADOS (SQLAlchemy) ---

def setup_disk_paths():
    global DB_CONNECTION_STRING, DB_ENGINE

    # Obtém URL do ambiente ou usa SQLite local
    raw_url = os.getenv("DATABASE_URL", "sqlite:///temp_local_db.db")

    # --- CORREÇÃO PARA RENDER/SQLALCHEMY ---
    # O Render fornece 'postgres://', mas o SQLAlchemy exige 'postgresql://'
    if raw_url.startswith("postgres://"):
        DB_CONNECTION_STRING = raw_url.replace("postgres://", "postgresql://", 1)
    else:
        DB_CONNECTION_STRING = raw_url

    is_sqlite = "sqlite" in DB_CONNECTION_STRING

    if DB_ENGINE is None:
        # Configuração de pool e timeout
        connect_args = {"check_same_thread": False, "timeout": 30} if is_sqlite else {"connect_timeout": 30}
        DB_ENGINE = create_engine(DB_CONNECTION_STRING, connect_args=connect_args, pool_pre_ping=True)

    adicionar_log("SISTEMA", f"Banco de Dados: {DB_CONNECTION_STRING.split('@')[-1]}")


def initialize_database():
    global DB_ENGINE
    if DB_ENGINE is None: setup_disk_paths()
    try:
        with DB_ENGINE.connect() as connection:
            inspector = inspect(DB_ENGINE)
            if not inspector.has_table(DB_TABLE_NAME):
                pd.DataFrame(columns=COLUNAS_HISTORICO).to_sql(DB_TABLE_NAME, DB_ENGINE, index=False)
                adicionar_log("DB", f"Tabela '{DB_TABLE_NAME}' criada.")

            # Cria índices para performance
            existing_indexes = [idx['name'] for idx in inspector.get_indexes(DB_TABLE_NAME)]
            if 'idx_timestamp' not in existing_indexes:
                connection.execute(text(f'CREATE INDEX idx_timestamp ON {DB_TABLE_NAME} (timestamp)'))
            if 'idx_id_ponto' not in existing_indexes:
                connection.execute(text(f'CREATE INDEX idx_id_ponto ON {DB_TABLE_NAME} (id_ponto)'))
            connection.commit()
        adicionar_log("DB", "Banco de dados verificado e pronto.")
    except Exception as e:
        adicionar_log("SISTEMA", f"ERRO CRÍTICO DB Init: {e}", level="ERROR");
        traceback.print_exc()


def save_to_sqlite(df_novos_dados):
    global DB_ENGINE
    if df_novos_dados.empty: return
    try:
        df_para_salvar = df_novos_dados.copy()
        if 'timestamp' in df_para_salvar.columns: df_para_salvar['timestamp'] = pd.to_datetime(
            df_para_salvar['timestamp'], utc=True)
        colunas_para_salvar = [col for col in COLUNAS_HISTORICO if col in df_para_salvar.columns]
        df_para_salvar = df_para_salvar[colunas_para_salvar]
        with DB_ENGINE.connect() as connection:
            df_para_salvar.to_sql(DB_TABLE_NAME, connection, if_exists='append', index=False)
            connection.commit()
    except Exception as e:
        adicionar_log("DB", f"ERRO CRÍTICO Salvar DB: {e}", level="ERROR")


def delete_from_sqlite(timestamps):
    global DB_ENGINE
    if not timestamps: return
    try:
        # Formata timestamps para string compatível com SQL (ISO format)
        ts_strings = [pd.to_datetime(ts).strftime('%Y-%m-%d %H:%M:%S') for ts in timestamps]
        if not ts_strings: return

        with DB_ENGINE.connect() as connection:
            # SQLAlchemy Core Delete
            t_historico = table(DB_TABLE_NAME, column('timestamp'))
            # Dependendo do dialeto, a formatação de data pode variar, mas string ISO geralmente funciona
            stmt = delete(t_historico).where(t_historico.c.timestamp.in_(ts_strings))
            connection.execute(stmt)
            connection.commit()
    except Exception as e:
        adicionar_log("DB", f"ERRO CRÍTICO Deletar DB: {e}", level="ERROR");
        traceback.print_exc()


def upsert_data(df_novos_dados):
    """ Atualiza dados existentes (Delete + Insert) """
    if df_novos_dados.empty: return

    # Normaliza timestamp
    df_novos_dados['timestamp'] = pd.to_datetime(df_novos_dados['timestamp'], utc=True)

    # 1. Identifica timestamps conflitantes
    timestamps_unicos = df_novos_dados['timestamp'].unique()

    # 2. Remove antigos
    delete_from_sqlite(timestamps=timestamps_unicos)

    # 3. Insere novos
    save_to_sqlite(df_novos_dados)
    adicionar_log("DB", f"Upsert concluído para {len(df_novos_dados)} registros.")


def read_data_from_sqlite(id_ponto=None, start_dt=None, end_dt=None, last_hours=None):
    global DB_ENGINE
    query_base = f"SELECT * FROM {DB_TABLE_NAME}"
    conditions, params = [], {}

    if last_hours:
        start_dt = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=last_hours)

    if id_ponto:
        conditions.append("id_ponto = :ponto")
        params["ponto"] = id_ponto
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
        with DB_ENGINE.connect() as connection:
            df = pd.read_sql_query(text(query_base), connection, params=params, parse_dates=["timestamp"])
            if 'timestamp' in df.columns and not df.empty and df['timestamp'].dt.tz is None:
                df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
        return df
    except Exception as e:
        adicionar_log("DB", f"ERRO Leitura DB: {e}", level="ERROR");
        return pd.DataFrame()


def get_recent_data_for_worker(hours=75): return read_data_from_sqlite(last_hours=hours)


def get_status_from_disk():
    try:
        fpath = os.path.join("/var/data" if os.environ.get('RENDER') else os.getcwd(), STATUS_FILE)
        if not os.path.exists(fpath): return {}
        with open(fpath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}


# --- INTEGRAÇÃO COM APIS ---

def calculate_hmac_signature(params, api_secret):
    string_para_assinar = "".join(f"{key}{params[key]}" for key in sorted(params.keys()))
    return hmac.new(api_secret.encode('utf-8'), string_para_assinar.encode('utf-8'), hashlib.sha256).hexdigest()


def arredondar_timestamp_10min(ts_epoch):
    dt_obj = datetime.datetime.fromtimestamp(ts_epoch, datetime.timezone.utc)
    return (dt_obj.replace(second=0, microsecond=0, minute=(dt_obj.minute // 10) * 10)).isoformat()


def backfill_weatherlink_data_manually(id_ponto, dias_para_tras):
    """ Recupera dados históricos da WeatherLink (API Historic). """
    config = WEATHERLINK_CONFIG.get(id_ponto)
    if not config or "SUA_CHAVE" in config.get('API_KEY', ''): return False

    adicionar_log(id_ponto, f"Iniciando backfill manual: últimos {dias_para_tras} dias.")
    now_dt = datetime.datetime.now(datetime.timezone.utc)
    url_base = f"https://api.weatherlink.com/v2/historic/{config['STATION_ID']}"
    dados_historicos = []

    with httpx.Client(timeout=60.0) as client:
        for i in range(dias_para_tras):
            end_dt = now_dt - datetime.timedelta(days=i)
            start_dt = end_dt - datetime.timedelta(days=1)
            t = str(int(datetime.datetime.now(datetime.timezone.utc).timestamp()))

            params = {"api-key": config['API_KEY'], "end-timestamp": str(int(end_dt.timestamp())),
                      "start-timestamp": str(int(start_dt.timestamp())), "station-id": str(config['STATION_ID']),
                      "t": t}
            sig = calculate_hmac_signature(params, config['API_SECRET'])
            params["api-signature"] = sig

            try:
                resp = client.get(url_base, params=params)
                if resp.status_code == 200:
                    data = resp.json()
                    for s in data.get('sensors', []):
                        if s.get('sensor_type') == 48 or s.get('data_structure_type') == 10:
                            for r in s.get('data', []):
                                if 'rainfall_mm' in r:  # Ou rainfall_daily_mm
                                    daily = r.get('rainfall_daily_mm', 0.0)
                                    if daily is None: daily = 0.0
                                    dados_historicos.append({
                                        "timestamp_epoch": r['ts'],
                                        "precipitacao_acumulada_mm": float(daily)
                                    })
                time.sleep(1)
            except Exception as e:
                adicionar_log(id_ponto, f"Erro Backfill dia {i}: {e}", level="ERROR")

    if not dados_historicos: return False

    df_historico = pd.DataFrame(dados_historicos).sort_values('timestamp_epoch').drop_duplicates()
    df_historico['chuva_mm'] = df_historico['precipitacao_acumulada_mm'].diff().fillna(
        df_historico['precipitacao_acumulada_mm'])
    df_historico.loc[df_historico['chuva_mm'] < 0, 'chuva_mm'] = df_historico[
        'precipitacao_acumulada_mm']  # Virada do dia

    df_historico['timestamp'] = pd.to_datetime(df_historico['timestamp_epoch'], unit='s', utc=True).dt.floor('10min')
    df_historico['id_ponto'] = id_ponto

    df_final = df_historico.groupby(['timestamp', 'id_ponto']).agg({
        'chuva_mm': 'sum',
        'precipitacao_acumulada_mm': 'last'
    }).reset_index()

    upsert_data(df_final)
    return True


# --- FUNÇÃO PRINCIPAL DA WEATHERLINK (MODIFICADA) ---
def fetch_data_from_weatherlink_api(ultimo_acumulado_chuva):
    """
    Busca dados atuais da WeatherLink.
    LÓGICA HÍBRIDA:
    1. Sincroniza memória com DB.
    2. Se DB vazio: Tenta Backfill (KM67) ou Força Valor (Outros) para recuperar dados de hoje.
    """
    ENDPOINT = "https://api.weatherlink.com/v2/current/{station_id}"
    dados = []
    novos_acumulados = ultimo_acumulado_chuva.copy()

    t = str(int(datetime.datetime.now(datetime.timezone.utc).timestamp()))

    with httpx.Client(timeout=60.0) as client:
        for id_ponto, config in WEATHERLINK_CONFIG.items():
            if "SUA_CHAVE" in config.get('API_KEY', ''): continue

            # --- TENTATIVA DE SINCRONIZAÇÃO ---
            if id_ponto not in novos_acumulados:
                try:
                    df_last = read_data_from_sqlite(id_ponto=id_ponto, last_hours=1)
                    if not df_last.empty:
                        # Recupera estado do banco
                        base = float(df_last.iloc[-1]['precipitacao_acumulada_mm'])
                        novos_acumulados[id_ponto] = base
                        adicionar_log(id_ponto, f"Sincronizado com DB. Base: {base}mm", level="INFO")
                    else:
                        # Banco vazio. Tenta backfill se for KM67
                        if id_ponto == "Ponto-A-KM67":
                            adicionar_log(id_ponto, "Sem dados. Tentando Backfill...", level="WARN")
                            if backfill_weatherlink_data_manually(id_ponto, 1):
                                df_pos = read_data_from_sqlite(id_ponto=id_ponto, last_hours=1)
                                if not df_pos.empty:
                                    novos_acumulados[id_ponto] = float(df_pos.iloc[-1]['precipitacao_acumulada_mm'])
                except Exception as e:
                    adicionar_log(id_ponto, f"Erro sync inicial: {e}", level="ERROR")

            # --- REQUISIÇÃO API ---
            params_to_sign = {"api-key": config['API_KEY'], "station-id": str(config['STATION_ID']), "t": t}
            signature = calculate_hmac_signature(params_to_sign, config['API_SECRET'])
            params_to_send = {"api-key": config['API_KEY'], "t": t, "api-signature": signature}

            try:
                r = client.get(ENDPOINT.format(station_id=config['STATION_ID']), params=params_to_send)
                r.raise_for_status()
                response_json = r.json()

                s = next((s['data'][0] for s in response_json.get('sensors', []) if
                          (s.get('data_structure_type') == 10 or s.get('sensor_type') == 48) and s.get('data')), None)

                if not s or 'ts' not in s: continue

                novo_acumulado_dia = float(s.get('rainfall_daily_mm', 0.0) or 0.0)

                # --- CÁLCULO INCREMENTAL ---
                if id_ponto in novos_acumulados:
                    # Cálculo normal (diferença)
                    ultimo = float(novos_acumulados.get(id_ponto, 0.0))
                    chuva_incremental = novo_acumulado_dia - ultimo
                    if chuva_incremental < 0: chuva_incremental = novo_acumulado_dia  # Virada dia
                else:
                    # FORÇA BRUTA (Sem histórico): Aceita o valor total para mostrar algo no gráfico
                    adicionar_log(id_ponto, f"Sem histórico. Aceitando acumulado de {novo_acumulado_dia}mm.",
                                  level="WARN")
                    chuva_incremental = novo_acumulado_dia

                novos_acumulados[id_ponto] = novo_acumulado_dia

                dados.append({
                    "timestamp": arredondar_timestamp_10min(s['ts']),
                    "id_ponto": id_ponto,
                    "chuva_mm": chuva_incremental,
                    "precipitacao_acumulada_mm": novo_acumulado_dia
                })

            except Exception as e:
                adicionar_log("DB", f"ERRO API WL ({id_ponto}): {e}", level="ERROR")

    df = pd.DataFrame(dados)
    if not df.empty and 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    return df, novos_acumulados


def fetch_data_from_zentra_cloud():
    """ Busca dados da Zentra (Umidade KM 72). """
    end_date = datetime.datetime.now(datetime.timezone.utc)
    start_date = end_date - datetime.timedelta(days=2)  # Busca 2 dias para garantir

    adicionar_log(ID_PONTO_ZENTRA_KM72, "Buscando dados da Zentra Cloud.")

    with httpx.Client() as client:
        r = None
        for attempt in range(3):
            url = f"{ZENTRA_BASE_URL}/get_readings/"
            params = {"device_sn": ZENTRA_STATION_SERIAL, "start": start_date.strftime("%Y-%m-%d"),
                      "end": end_date.strftime("%Y-%m-%d")}
            headers = {"Authorization": f"Token {ZENTRA_API_TOKEN}"}
            try:
                r = client.get(url, headers=headers, params=params, timeout=30.0)
                if r.status_code == 200: break
                if r.status_code == 429: time.sleep(60)
            except:
                pass

    if not r or r.status_code != 200:
        return pd.DataFrame()

    try:
        data_json = r.json()
        wc_data = next((d for n, d in data_json.get('data', {}).items() if 'water content' in n.lower()), None)
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
        adicionar_log(ID_PONTO_ZENTRA_KM72, f"Erro Zentra: {e}", level="ERROR");
        return pd.DataFrame()