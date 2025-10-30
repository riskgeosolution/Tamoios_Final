import pandas as pd
import json
import os
import datetime
import httpx
import traceback
from io import StringIO  # Importa o StringIO

# Importa as constantes do config.py
from config import (
    PONTOS_DE_ANALISE, CONSTANTES_PADRAO,
    FREQUENCIA_API_SEGUNDOS,  # <-- CORREÇÃO: Importa o nome correto
    MAX_HISTORICO_PONTOS,
    WEATHERLINK_API_KEY, WEATHERLINK_API_SECRET,
    MAPEAMENTO_API_IDS
)

# --- Configurações de Disco (Caminhos) ---
# (Definido globalmente para que o worker e o web usem os mesmos caminhos)
DATA_DIR = "."
HISTORICO_FILE = os.path.join(DATA_DIR, "historico_72h.json")
STATUS_FILE = os.path.join(DATA_DIR, "status_atual.json")
LOG_FILE = os.path.join(DATA_DIR, "eventos.log")

# Define as colunas esperadas (para consistência)
COLUNAS_HISTORICO = [
    'timestamp', 'id_ponto', 'chuva_mm', 'precipitacao_acumulada_mm',
    'umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc',
    'base_1m', 'base_2m', 'base_3m'
]


def setup_disk_paths():
    """ Apenas imprime os caminhos dos arquivos para debug. """
    print("--- data_source.py ---")
    global DATA_DIR, HISTORICO_FILE, STATUS_FILE, LOG_FILE

    # Se estiver no Render, usa o caminho /var/data
    # Se não, usa o diretório local ('.')
    if os.environ.get('RENDER'):
        DATA_DIR = "/var/data"
        HISTORICO_FILE = os.path.join(DATA_DIR, "historico_72h.json")
        STATUS_FILE = os.path.join(DATA_DIR, "status_atual.json")
        LOG_FILE = os.path.join(DATA_DIR, "eventos.log")

    print(f"Caminho do Disco de Dados: {DATA_DIR}")
    print(f"Arquivo de Histórico: {HISTORICO_FILE}")
    print(f"Arquivo de Status: {STATUS_FILE}")
    print(f"Arquivo de Log: {LOG_FILE}")


def get_all_data_from_disk():
    """
    Lê TODOS os dados (histórico, status, logs) do disco.
    Usado pelo index.py (dashboard) para atualizar a interface.
    """

    # 1. Ler Histórico (historico_72h.json)
    try:
        # Lê o JSON string do arquivo
        with open(HISTORICO_FILE, 'r', encoding='utf-8') as f:
            historico_json_str = f.read()

        # Converte a string JSON de volta para um DataFrame
        # (CORREÇÃO: Usa StringIO para ler a string)
        historico_df = pd.read_json(StringIO(historico_json_str), orient='split')

        # Garante que 'timestamp' seja datetime (pode se perder no JSON)
        if 'timestamp' in historico_df.columns:
            historico_df['timestamp'] = pd.to_datetime(historico_df['timestamp'])

    except FileNotFoundError:
        print(f"[Web] Aviso: Não foi possível ler {HISTORICO_FILE} (File does not exist). Retornando dados padrão.")
        historico_df = pd.DataFrame(columns=COLUNAS_HISTORICO)  # Retorna DF vazio com colunas
    except Exception as e:
        print(f"ERRO ao decodificar {HISTORICO_FILE}: {e}. Retornando vazio.")
        historico_df = pd.DataFrame(columns=COLUNAS_HISTORICO)  # Retorna DF vazio com colunas

    # 2. Ler Status (status_atual.json)
    try:
        with open(STATUS_FILE, 'r', encoding='utf-8') as f:
            status_atual = json.load(f)
    except FileNotFoundError:
        print(f"[Web] Aviso: Não foi possível ler {STATUS_FILE} (File does not exist). Retornando dados padrão.")
        status_atual = {p: "INDEFINIDO" for p in PONTOS_DE_ANALISE.keys()}  # Retorna status padrão
    except Exception as e:
        print(f"ERRO ao ler {STATUS_FILE}: {e}. Retornando dados padrão.")
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
# FUNÇÕES USADAS PELO WORKER.PY
# ==========================================================

def executar_passo_api_e_salvar(historico_df):
    """
    Função principal chamada pelo worker.py.
    1. Chama a API
    2. Processa os dados
    3. Salva o histórico atualizado no disco
    4. Retorna os novos dados e status para o worker
    """

    # 1. Chamar a API (WeatherLink)
    try:
        dados_api, status_novos, logs_api = fetch_data_from_zentra_api()

        # Salva os logs gerados pela chamada da API
        for log in logs_api:
            adicionar_log(log['id_ponto'], log['mensagem'])

    except Exception as e:
        print(f"ERRO CRÍTICO em fetch_data_from_zentra_api: {e}")
        traceback.print_exc()
        adicionar_log("GERAL", f"ERRO CRÍTICO (fetch_data): {e}")
        return pd.DataFrame(), None  # Retorna DF vazio e Sem Status

    if not dados_api:
        print("[Worker] API não retornou novos dados neste ciclo.")
        return pd.DataFrame(), status_novos  # Retorna DF vazio

    # 2. Processar e Salvar
    try:
        # Converte a lista de dicts da API para um DataFrame
        novos_df = pd.DataFrame(dados_api)

        # Garante que as colunas do novo DF batem com as do histórico
        for col in COLUNAS_HISTORICO:
            if col not in novos_df:
                novos_df[col] = pd.NA
        novos_df = novos_df[COLUNAS_HISTORICO]  # Garante a ordem

        # Concatena o histórico antigo com os novos dados
        # (Usa o historico_df que o worker passou como argumento)
        historico_atualizado_df = pd.concat([historico_df, novos_df], ignore_index=True)

        # Converte 'timestamp' para datetime (se não for)
        historico_atualizado_df['timestamp'] = pd.to_datetime(historico_atualizado_df['timestamp'])

        # Ordena e remove duplicatas (segurança)
        historico_atualizado_df = historico_atualizado_df.sort_values(by='timestamp').drop_duplicates(
            subset=['id_ponto', 'timestamp'], keep='last')

        # Trunca o histórico para 72h (MAX_HISTORICO_PONTOS)
        historico_atualizado_df = historico_atualizado_df.tail(MAX_HISTORICO_PONTOS * len(PONTOS_DE_ANALISE))

        # 3. Salvar o novo histórico no disco
        # (Converte para JSON String antes de salvar)
        historico_json_str = historico_atualizado_df.to_json(orient='split', index=False, date_format='iso')
        with open(HISTORICO_FILE, 'w', encoding='utf-8') as f:
            f.write(historico_json_str)

        print("[Worker] Histórico de 72h salvo no disco.")

        return novos_df, status_novos

    except Exception as e:
        print(f"ERRO CRÍTICO ao salvar dados no disco: {e}")
        traceback.print_exc()
        adicionar_log("GERAL", f"ERRO CRÍTICO (salvar disco): {e}")
        return pd.DataFrame(), status_novos


def fetch_data_from_zentra_api():
    """
    (NOME ANTIGO)
    Simula a chamada à API WeatherLink.
    Retorna os dados brutos e os status calculados (pela API ou pelo placeholder).
    """

    # Lista de logs gerados nesta chamada
    logs_api = []

    # Lista de dados processados
    dados_processados = []

    # Dicionário de status (calculado aqui ou vindo da API)
    status_calculados = {}

    # --- INÍCIO DA LÓGICA DA API REAL (WeatherLink) ---
    # (Este bloco está comentado pois não temos a chave)

    # 1. Gerar Assinatura (Exemplo, pode variar)
    # timestamp = str(int(time.time()))
    # hmac_signature = hmac.new(WEATHERLINK_API_SECRET.encode(), (WEATHERLINK_API_KEY + timestamp).encode(), hashlib.sha256).hexdigest()

    # headers = {
    #     'X-Api-Key': WEATHERLINK_API_KEY,
    #     'X-Api-Signature': hmac_signature,
    #     'X-Timestamp': timestamp
    # }

    # try:
    #     # (URL de exemplo, você precisa da URL correta)
    #     url = "https://api.weatherlink.com/v2/current"
    #     with httpx.Client(timeout=10.0) as client:
    #         response = client.get(url, headers=headers)
    #         response.raise_for_status() # Lança erro se for 4xx ou 5xx

    #     api_data = response.json()
    #     # (Aqui você faria o loop nos 'sensors' ou 'stations' da resposta)

    # except httpx.RequestError as e:
    #     logs_api.append({"id_ponto": "GERAL", "mensagem": f"ERRO API: Falha de comunicação: {e}"})
    #     return None, None, logs_api
    # except httpx.HTTPStatusError as e:
    #     logs_api.append({"id_ponto": "GERAL", "mensagem": f"ERRO API: Servidor retornou status {e.response.status_code}"})
    #     return None, None, logs_api
    # except Exception as e:
    #     logs_api.append({"id_ponto": "GERAL", "mensagem": f"ERRO API: Erro inesperado ao chamar API: {e}"})
    #     return None, None, logs_api

    # --- FIM DA LÓGICA DA API REAL ---

    # --- INÍCIO DO PLACEHOLDER (Simulação de API) ---
    # (Usado para testes enquanto a API não está conectada)
    # (Este é o "placebo" que você notou)

    print("[API] USANDO DADOS DE PLACEHOLDER (SIMULAÇÃO)")

    # Placeholders para chuva (simulando a resposta da API)
    placeholder_chuva = {
        "Ponto-A-KM67": 82.0,  # (Simula um ALERTA)
        "Ponto-B-KM72": 10.0,  # (Simula LIVRE)
        "Ponto-C-KM74": None,  # (Simula um sensor offline - SEM DADOS)
        "Ponto-D-KM81": 82.0,  # (Simula um ALERTA)
    }

    # Placeholders para Umidade (simulando a API)
    placeholder_umidade = {
        "Ponto-A-KM67": (30.5, 36.5, 39.5, 30.0, 36.0, 39.0),  # (umid1, umid2, umid3, base1, base2, base3)
        "Ponto-B-KM72": (31.0, 37.0, 40.0, 30.0, 36.0, 39.0),
        "Ponto-C-KM74": (None, None, None, 30.0, 36.0, 39.0),
        "Ponto-D-KM81": (30.5, 36.5, 39.5, 30.0, 36.0, 39.0),
    }

    timestamp_atual = datetime.datetime.now(datetime.timezone.utc).isoformat()

    # O worker.py espera que a API já calcule o status (seja a API real ou este placeholder)
    from processamento import definir_status_chuva  # Import local

    for id_ponto in PONTOS_DE_ANALISE.keys():

        chuva_72h = placeholder_chuva.get(id_ponto, 0.0)

        # 1. Calcular o Status (o worker espera isso)
        if chuva_72h is None:
            status_ponto = "SEM DADOS"
        else:
            status_ponto, _ = definir_status_chuva(chuva_72h)

        status_calculados[id_ponto] = status_ponto

        # 2. Criar os dados brutos (para o histórico)
        if chuva_72h is None:
            # Se a API retornar Nulo, guardamos Nulo (ou NA)
            dados_processados.append({
                "timestamp": timestamp_atual,
                "id_ponto": id_ponto,
                "chuva_mm": pd.NA,  # Salva como Nulo
                "precipitacao_acumulada_mm": pd.NA,
                "umidade_1m_perc": pd.NA,
                "umidade_2m_perc": pd.NA,
                "umidade_3m_perc": pd.NA,
                "base_1m": pd.NA,
                "base_2m": pd.NA,
                "base_3m": pd.NA,
            })
            logs_api.append({"id_ponto": id_ponto, "mensagem": "AVISO API: API não retornou dados para o ponto."})

        else:
            # Se a API retornar dados, processa
            umidade = placeholder_umidade.get(id_ponto, (0, 0, 0, 0, 0, 0))

            dados_processados.append({
                "timestamp": timestamp_atual,
                "id_ponto": id_ponto,
                "chuva_mm": 0.0,  # Chuva instantânea (API não retorna)
                "precipitacao_acumulada_mm": chuva_72h,  # Guarda o acumulado
                "umidade_1m_perc": umidade[0],
                "umidade_2m_perc": umidade[1],
                "umidade_3m_perc": umidade[2],
                "base_1m": umidade[3],
                "base_2m": umidade[4],
                "base_3m": umidade[5],
            })

    # --- FIM DO PLACEHOLDER ---

    # (Quando a API real for usada, o placeholder acima será removido/comentado
    # e esta seção de 'parse' será preenchida)

    # ...
    # (Lógica de Parse da API Real)
    # ...
    # (Fim da Lógica de Parse)

    return dados_processados, status_calculados, logs_api


def adicionar_log(id_ponto, mensagem):
    """
    Adiciona uma entrada de log ao arquivo de log no disco.
    (Função com 2 argumentos)
    """
    try:
        # Formato: [Timestamp] | [ID_Ponto/GERAL] | [Mensagem]
        log_entry = f"{datetime.datetime.now(datetime.timezone.utc).isoformat()} | {id_ponto} | {mensagem}\n"

        # 'a' (append) cria o arquivo se não existir, ou anexa ao final
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(log_entry)

    except Exception as e:
        print(f"ERRO CRÍTICO ao escrever no log: {e}")
        traceback.print_exc()

