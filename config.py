# config.py (CORRIGIDO: Limites de chuva atualizados e pronto para SQLite)

import os
import datetime
from dotenv import load_dotenv

# Carrega as variáveis do .env file
load_dotenv()

# --- Configurações da API Real (WeatherLink) ---

# Mapeamento dos IDs da sua API para os IDs do nosso sistema
MAPEAMENTO_API_IDS = {
    "ID_DA_SUA_API_KM67": "Ponto-A-KM67",
    "ID_DA_SUA_API_KM72": "Ponto-B-KM72",
    "ID_DA_SUA_API_KM74": "Ponto-C-KM74",
    "ID_DA_SUA_API_KM81": "Ponto-D-KM81",
}

# --- CONFIGURAÇÃO MULTI-ESTAÇÃO ---
# Credenciais lidas do ambiente (Render ou .env local)
WEATHERLINK_CONFIG = {
    "Ponto-A-KM67": {
        "STATION_ID": 179221, # KM67
        "API_KEY": os.getenv('WL_API_KEY_KM67', "SUA_CHAVE_API_KM67"),
        "API_SECRET": os.getenv('WL_API_SECRET_KM67', "SEU_SEGREDO_API_KM67")
    },
    "Ponto-B-KM72": {
        "STATION_ID": 197768, # KM72
        "API_KEY": os.getenv('WL_API_KEY_KM72', "SUA_CHAVE_API_KM72"),
        "API_SECRET": os.getenv('WL_API_SECRET_KM72', "SEU_SEGREDO_API_KM72")
    },
    "Ponto-C-KM74": {
        "STATION_ID": 197774, # KM74
        "API_KEY": os.getenv('WL_API_KEY_KM74', "SUA_CHAVE_API_KM74"),
        "API_SECRET": os.getenv('WL_API_SECRET_KM74', "SEU_SEGREDO_API_KM74")
    },
    "Ponto-D-KM81": {
        "STATION_ID": 197778, # KM81
        "API_KEY": os.getenv('WL_API_KEY_KM81', "SUA_CHAVE_API_KM81"),
        "API_SECRET": os.getenv('WL_API_SECRET_KM81', "SEU_SEGREDO_API_KM81")
    }
}
# --- FIM DA CONFIGURAÇÃO MULTI-ESTAÇÃO ---

# --- CONFIGURAÇÕES DO BANCO DE DADOS ---
# Usa DATABASE_URL do ambiente (Render/Postgres) ou o ficheiro SQLite local
# O seu ficheiro local chama-se 'temp_local_db.db'
DB_CONNECTION_STRING = os.getenv("DATABASE_URL", "sqlite:///temp_local_db.db")
DB_TABLE_NAME = "historico_monitoramento" # Nome da tabela para guardar os dados
# --- FIM DA CONFIGURAÇÃO DB ---


# --- Configurações do Worker ---
FREQUENCIA_API_SEGUNDOS = 60 * 15
# Esta constante continua a ser usada para o truncamento do CSV
MAX_HISTORICO_PONTOS = (72 * 60 * 60) // FREQUENCIA_API_SEGUNDOS # Manter 72h de dados (no CSV)


# --- Configurações dos Pontos de Análise ---
CONSTANTES_PADRAO = {
    "UMIDADE_BASE_1M": 30.0, "UMIDADE_BASE_2M": 36.0, "UMIDADE_BASE_3M": 39.0,
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

# --- INÍCIO DA ALTERAÇÃO (NOVOS LIMITES DE CHUVA) ---
CHUVA_LIMITE_VERDE = 60.0   # Limite para LIVRE (Era 50.0)
CHUVA_LIMITE_AMARELO = 79.0 # Limite para ATENÇÃO (Era 69.0)
CHUVA_LIMITE_LARANJA = 100.0 # Limite para ALERTA (Era 89.0)
# --- FIM DA ALTERAÇÃO ---

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