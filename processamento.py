# processamento_saas.py
# Versão Corrigida: Otimização de Memória + Deduplicação de Dados (Corrige soma excessiva de chuva)

import pandas as pd
import traceback


# --- FUNÇÃO calcular_acumulado_rolling (Corrigida) ---
def calcular_acumulado_rolling(df_ponto, frequencia_segundos=900, horas=72):
    """
    Calcula o acumulado 'rolling' removendo duplicatas antes de somar.
    """
    if df_ponto.empty or 'timestamp' not in df_ponto.columns:
        return pd.DataFrame(columns=['id_ponto', 'timestamp', 'chuva_mm'])

    # 1. Seleciona colunas essenciais
    df_process = df_ponto[['timestamp', 'chuva_mm', 'id_ponto']].copy()

    # 2. Garante ID e Tipos
    if 'id_ponto' not in df_process.columns:
        df_process['id_ponto'] = 1

    try:
        df_process['timestamp'] = pd.to_datetime(df_process['timestamp'])
        df_process['chuva_mm'] = df_process['chuva_mm'].fillna(0.0).astype(float)
    except Exception as e:
        print(f"Erro de tipo/indexação: {e}")
        return pd.DataFrame(columns=['id_ponto', 'timestamp', 'chuva_mm'])

    # 3. DEDUPLICAÇÃO (O Passo que faltava!)
    # Remove registros com o mesmo timestamp para o mesmo ponto, mantendo o maior valor (ou o primeiro)
    # Isso evita que o resample some o mesmo dado duas vezes.
    df_process = df_process.sort_values('chuva_mm', ascending=False).drop_duplicates(subset=['id_ponto', 'timestamp'],
                                                                                     keep='first')

    # 4. Indexação
    df_process = df_process.set_index('timestamp').sort_index()

    lista_dfs_acumulados = []

    pontos_por_hora = 3600 // frequencia_segundos
    window_size = int(horas * pontos_por_hora)

    for ponto_id in df_process['id_ponto'].unique():
        # Filtra série
        series_chuva = df_process[df_process['id_ponto'] == ponto_id]['chuva_mm']

        if series_chuva.empty: continue

        # Resample para garantir frequência (Preenche buracos com 0)
        freq_str = f"{int(frequencia_segundos / 60)}min"
        df_resampled = series_chuva.resample(freq_str).sum()
        df_resampled = df_resampled.fillna(0)

        # Cálculo Rolling
        acumulado = df_resampled.rolling(window=window_size, min_periods=1).sum()

        # Reconstrói DataFrame
        acumulado_df = acumulado.to_frame(name='chuva_mm')
        acumulado_df['id_ponto'] = ponto_id

        lista_dfs_acumulados.append(acumulado_df)

    if not lista_dfs_acumulados:
        return pd.DataFrame(columns=['id_ponto', 'timestamp', 'chuva_mm'])

    df_final = pd.concat(lista_dfs_acumulados)
    return df_final.reset_index()


# --- FUNÇÃO definir_status_chuva ---
def definir_status_chuva(chuva_mm, regras_chuva=None):
    if regras_chuva is None:
        regras_chuva = {}

    STATUS_MAP_CHUVA = {
        "LIVRE": "success",
        "ATENÇÃO": "warning",
        "ALERTA": "orange",
        "PARALIZAÇÃO": "danger",
        "SEM DADOS": "secondary",
        "INDEFINIDO": "secondary"
    }

    try:
        if pd.isna(chuva_mm):
            status_texto = "SEM DADOS"

        elif chuva_mm >= regras_chuva.get('limite_laranja', 100):
            status_texto = "PARALIZAÇÃO"
        elif chuva_mm > regras_chuva.get('limite_amarelo', 80):
            status_texto = "ALERTA"
        elif chuva_mm > regras_chuva.get('limite_verde', 60):
            status_texto = "ATENÇÃO"
        else:
            status_texto = "LIVRE"

        return status_texto, STATUS_MAP_CHUVA.get(status_texto, "secondary")

    except Exception as e:
        print(f"Erro status chuva: {e}")
        return "INDEFINIDO", "secondary"


# --- FUNÇÃO definir_status_umidade_hierarquico ---
def definir_status_umidade_hierarquico(umidade_1m, umidade_2m, umidade_3m,
                                       base_1m, base_2m, base_3m,
                                       config_umidade=None):
    if config_umidade is None:
        config_umidade = {}

    delta_trigger = config_umidade.get('delta_trigger', 2.0)
    status_map = config_umidade.get('mapa_risco', {
        -1: "secondary", 0: "success", 1: "warning", 2: "orange", 3: "danger"
    })

    try:
        if pd.isna(umidade_1m) or pd.isna(umidade_2m) or pd.isna(umidade_3m) or \
                pd.isna(base_1m) or pd.isna(base_2m) or pd.isna(base_3m):
            return status_map.get(-1, "secondary")

        s1_sim = (umidade_1m - base_1m) >= delta_trigger
        s2_sim = (umidade_2m - base_2m) >= delta_trigger
        s3_sim = (umidade_3m - base_3m) >= delta_trigger

        risco_final = 0

        if s1_sim and s2_sim and s3_sim:
            risco_final = 3
        elif (s1_sim and s2_sim and not s3_sim) or \
                (not s1_sim and s2_sim and s3_sim):
            risco_final = 2
        elif (s1_sim and not s2_sim and not s3_sim) or \
                (not s1_sim and not s2_sim and s3_sim):
            risco_final = 1

        return status_map.get(risco_final, "secondary")

    except Exception as e:
        print(f"Erro ao definir status de umidade solo: {e}")
        return status_map.get(-1, "secondary")


# --- FUNÇÃO verificar_trava_base ---
def verificar_trava_base(df_historico_ponto, coluna_umidade, nova_leitura, base_antiga, horas=6):
    try:
        if nova_leitura >= base_antiga:
            return base_antiga

        if df_historico_ponto.empty or 'timestamp' not in df_historico_ponto.columns:
            return nova_leitura

        ultimo_timestamp_no_df = df_historico_ponto['timestamp'].max()
        limite_tempo = ultimo_timestamp_no_df - pd.Timedelta(hours=horas)
        df_ultimas_horas = df_historico_ponto[df_historico_ponto['timestamp'] >= limite_tempo]
        serie_umidade_historica = df_ultimas_horas[coluna_umidade].dropna()

        if not serie_umidade_historica.empty:
            if not (serie_umidade_historica < base_antiga).all():
                return base_antiga

        return nova_leitura

    except Exception as e:
        print(f"ERRO CRÍTICO (verificar_trava_base): {e}")
        traceback.print_exc()
        return base_antiga