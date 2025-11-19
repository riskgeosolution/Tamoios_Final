# config.py (COMPLETO, v12.1: Adicionando Disparo Inteligente)

import os
import datetime
from dotenv import load_dotenv

# Carrega as variáveis do .env file
load_dotenv()

# --- Configurações da API Real (WeatherLink) ---
MAPEAMENTO_API_IDS = {
    "ID_DA_SUA_API_KM67": "Ponto-A-KM67",
    "ID_DA_SUA_API_KM72": "Ponto-B-KM72",
    "ID_DA_SUA_API_KM74": "Ponto-C-KM74",
    "ID_DA_SUA_API_KM81": "Ponto-D-KM81",
}

WEATHERLINK_CONFIG = {
    "Ponto-A-KM67": {
        "STATION_ID": 179221,  # KM67
        "API_KEY": os.getenv('WL_API_KEY_KM67', "SUA_CHAVE_API_KM67"),
        "API_SECRET": os.getenv('WL_API_SECRET_KM67', "SEU_SEGREDO_API_SEGREDO_KM67")
    },
    "Ponto-B-KM72": {
        "STATION_ID": 197768,  # KM72
        "API_KEY": os.getenv('WL_API_KEY_KM72', "SUA_CHAVE_API_KM72"),
        "API_SECRET": os.getenv('WL_API_SECRET_KM72', "SEU_SEGREDO_API_KM72")
    },
    "Ponto-C-KM74": {
        "STATION_ID": 197774,  # KM74
        "API_KEY": os.getenv('WL_API_KEY_KM74', "SUA_CHAVE_API_KM74"),
        "API_SECRET": os.getenv('WL_API_SECRET_KM74', "SEU_SEGREDO_API_KM74")
    },
    "Ponto-D-KM81": {
        "STATION_ID": 197778,  # KM81
        "API_KEY": os.getenv('WL_API_KEY_KM81', "SUA_CHAVE_API_KM81"),
        "API_SECRET": os.getenv('WL_API_SECRET_KM81', "SEU_SEGREDO_API_KM81")
    }
}
# --- FIM DA CONFIGURAÇÃO MULTI-ESTAÇÃO ---


# --- INÍCIO DA SEÇÃO (ZENTRA CLOUD) ---
ZENTRA_API_TOKEN = os.getenv("ZENTRA_API_TOKEN", "a6483331a7e3fd1920483a61eb9524a51298be9b")
ZENTRA_STATION_SERIAL = os.getenv("ZENTRA_STATION_SERIAL", "z6-32707")
ZENTRA_BASE_URL = "https://zentracloud.com/api/v4"
MAPA_ZENTRA_KM72 = {
    1: "umidade_1m_perc",  # Porta 1 -> 1 metro
    2: "umidade_2m_perc",  # Porta 2 -> 2 metros
    3: "umidade_3m_perc",  # Porta 3 -> 3 metros
}
ID_PONTO_ZENTRA_KM72 = "Ponto-B-KM72"
# --- FIM DA SEÇÃO (ZENTRA CLOUD) ---


# --- CONFIGURAÇÕES DE DISPARO INTELIGENTE (RENDER KEEPALIVE) ---
BACKFILL_RUN_TIME_SEC = 20 # Tempo máximo de processamento contínuo para backfill
RENDER_SLEEP_TIME_SEC = 10 # Tempo de pausa para evitar timeout no Render
# --- FIM DAS CONFIGURAÇÕES DE DISPARO INTELIGENTE ---


# --- CONFIGURAÇÕES DO BANCO DE DADOS ---
DB_CONNECTION_STRING = os.getenv("DATABASE_URL", "sqlite:///temp_local_db.db")
DB_TABLE_NAME = "historico_monitoramento"
# --- FIM DA CONFIGURAÇÃO DB ---


# --- Configurações do Worker ---
FREQUENCIA_API_SEGUNDOS = 60 * 15
MAX_HISTORICO_PONTOS = (72 * 60 * 60) // FREQUENCIA_API_SEGUNDOS

# --- Configurações dos Pontos de Análise ---
CONSTANTES_PADRAO = {
    "UMIDADE_BASE_1M": 39.0,
    "UMIDADE_BASE_2M": 43.0,
    "UMIDADE_BASE_3M": 10.0,
    "UMIDADE_SATURACAO_1M": 47.0,
    "UMIDADE_SATURACAO_2M": 46.0,
    "UMIDADE_SATURACAO_3M": 49.0,
}

PONTOS_DE_ANALISE = {
    "Ponto-A-KM67": {"nome": "KM 67", "constantes": CONSTANTES_PADRAO.copy(), "lat_lon": [-23.585137, -45.456733]},
    "Ponto-B-KM72": {"nome": "KM 72", "constantes": CONSTANTES_PADRAO.copy(), "lat_lon": [-23.592805, -45.447181]},
    "Ponto-C-KM74": {"nome": "KM 74", "constantes": CONSTANTES_PADRAO.copy(), "lat_lon": [-23.589068, -45.440229]},
    "Ponto-D-KM81": {"nome": "KM 81", "constantes": CONSTANTES_PADRAO.copy(), "lat_lon": [-23.613498, -45.431119]},
}

# --- Regras de Negócio (Alertas) ---
CHUVA_LIMITE_VERDE = 60.0
CHUVA_LIMITE_AMARELO = 79.0
CHUVA_LIMITE_LARANJA = 100.0

DELTA_TRIGGER_UMIDADE = 3.0

RISCO_MAP = {
    "LIVRE": 0,
    "ATENÇÃO": 1,
    "ALERTA": 2,
    "PARALIZAÇÃO": 3,
    "SEM DADOS": -1,
    "INDEFINIDO": -1,
    "ERRO": -1
}

STATUS_MAP_HIERARQUICO = {
    3: ("PARALIZAÇÃO", "danger", "bg-danger"),
    2: ("ALERTA", "orange", "bg-orange"),
    1: ("ATENÇÃO", "warning", "bg-warning"),
    0: ("LIVRE", "success", "bg-success"),
    -1: ("SEM DADOS", "secondary", "bg-secondary")
}

# --- CONSTANTES DE CORES (CENTRALIZADAS) ---
CORES_ALERTAS_CSS = {
    "verde": "green",
    "amarelo": "#FFD700",
    "laranja": "#fd7e14",
    "vermelho": "#dc3545",
    "cinza": "grey"
}

CORES_UMIDADE = {
    '1m': CORES_ALERTAS_CSS["verde"],
    '2m': CORES_ALERTAS_CSS["amarelo"],
    '3m': CORES_ALERTAS_CSS["vermelho"]
}