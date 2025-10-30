import os
import datetime

# --- Configurações da API Real (WeatherLink) ---
# (Você precisará preencher estas)
WEATHERLINK_API_KEY = os.environ.get('WEATHERLINK_API_KEY', "SUA_CHAVE_API_AQUI")
WEATHERLINK_API_SECRET = os.environ.get('WEATHERLINK_API_SECRET', "SEU_SEGREDO_API_AQUI")

# Mapeamento dos IDs da sua API para os IDs do nosso sistema
# (Exemplo: "ID_DA_SUA_API_KM67" deve ser o ID que a WeatherLink usa)
MAPEAMENTO_API_IDS = {
    "ID_DA_SUA_API_KM67": "Ponto-A-KM67",
    "ID_DA_SUA_API_KM72": "Ponto-B-KM72",
    "ID_DA_SUA_API_KM74": "Ponto-C-KM74",
    "ID_DA_SUA_API_KM81": "Ponto-D-KM81",
}

# --- Configurações do Worker ---
# A API atualiza a cada 15 minutos (15 * 60 = 900 segundos)
FREQUENCIA_API_SEGUNDOS = 60 * 15
MAX_HISTORICO_PONTOS = (72 * 60 * 60) // FREQUENCIA_API_SEGUNDOS # Manter 72h de dados


# --- Configurações dos Pontos de Análise ---
# (Usado como fallback e para os nomes/localizações no mapa)
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
# (Trazido do processamento.py para centralizar)

# 1. Limites da Chuva
CHUVA_LIMITE_VERDE = 50.0
CHUVA_LIMITE_AMARELO = 69.0
CHUVA_LIMITE_LARANJA = 89.0

# 2. Limites da Umidade
DELTA_TRIGGER_UMIDADE = 3.0

# 3. Mapeamento de Risco (Texto -> Nível)
RISCO_MAP = {
    "LIVRE": 0,
    "ATENÇÃO": 1,
    "ALERTA": 2,
    "PARALIZAÇÃO": 3,
    "SEM DADOS": -1,
    "INDEFINIDO": -1,
    "ERRO": -1
}

# 4. Mapeamento de Status (Nível -> Info de UI)
# (Usado pelo processamento.py e map_view.py)
STATUS_MAP_HIERARQUICO = {
    3: ("PARALIZAÇÃO", "danger", "bg-danger"),  # Risco 3 (Vermelho)
    2: ("ALERTA", "orange", "bg-orange"),  # Risco 2 (Laranja)
    1: ("ATENÇÃO", "warning", "bg-warning"),  # Risco 1 (Amarelo)
    0: ("LIVRE", "success", "bg-success"),  # Risco 0 (Verde)
    -1: ("SEM DADOS", "secondary", "bg-secondary")  # Risco -1
}

