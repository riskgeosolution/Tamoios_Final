# processamento.py (CORRIGIDO v5: Centralização de Constantes)

import pandas as pd
import datetime
import traceback
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

# --- Importações Corrigidas ---
from config import (
    CHUVA_LIMITE_VERDE, CHUVA_LIMITE_AMARELO, CHUVA_LIMITE_LARANJA,
    DELTA_TRIGGER_UMIDADE, RISCO_MAP, STATUS_MAP_HIERARQUICO,
    FREQUENCIA_API_SEGUNDOS, STATUS_MAP_CHUVA  # Importa o mapa de status de chuva
)


# --- FUNÇÃO calcular_acumulado_rolling (MODIFICADA E MAIS ROBUSTA) ---
def calcular_acumulado_rolling(df_ponto, horas=72):
    """
    Calcula o acumulado 'rolling' somando os valores de chuva incremental (chuva_mm)
    para um número dinâmico de horas, corrigindo a duplicação em sensores de 15 minutos.
    """
    if 'chuva_mm' not in df_ponto.columns or df_ponto.empty or 'timestamp' not in df_ponto.columns:
        return pd.DataFrame(columns=['id_ponto', 'timestamp', 'chuva_mm'])

    # --- Pré-condição: Garantir que os dados estejam ordenados por tempo ---
    df_ponto = df_ponto.sort_values('timestamp')

    df_original = df_ponto.copy()

    try:
        df_original['timestamp'] = pd.to_datetime(df_original['timestamp'])
        df_original = df_original.set_index('timestamp')
        df_original['chuva_mm'] = pd.to_numeric(df_original['chuva_mm'], errors='coerce').fillna(0)

        lista_dfs_acumulados = []

        # O Worker roda a cada 10 minutos (FREQUENCIA_API_SEGUNDOS=600)
        # O Sensor da WeatherLink atualiza a cada 15 minutos.

        # Define a janela de rolling em termos de intervalos de 15 minutos
        window_size_15min = int(horas * (60 / 15))

        # Itera por cada ponto único no DataFrame
        for ponto_id in df_original['id_ponto'].unique():
            series_chuva = df_original[df_original['id_ponto'] == ponto_id]['chuva_mm']

            # --- CORREÇÃO CRÍTICA PARA DUPLICAÇÃO ---
            # 1. Resample para 15 minutos, pegando o valor máximo da chuva incremental (max)
            # Isso garante que a leitura incremental de 15 minutos só seja contada uma vez,
            # eliminando o double-counting que ocorreria na amostragem de 10 minutos.
            df_15min = series_chuva.resample('15T').max().fillna(0)

            # 2. Calcula o acumulado com a nova frequência de 15 minutos
            acumulado_15min = df_15min.rolling(window=window_size_15min, min_periods=1).sum()

            # 3. Volta para a frequência de 10 minutos (10T) para alinhar com o Dashboard
            # O .ffill() preenche os buracos de 10 minutos com o dado de 15 minutos mais recente.
            acumulado_10min = acumulado_15min.resample('10T').ffill()
            # ------------------------------------------

            acumulado_df = acumulado_10min.to_frame(name='chuva_mm')
            acumulado_df['id_ponto'] = ponto_id

            lista_dfs_acumulados.append(acumulado_df)

        df_final = pd.concat(lista_dfs_acumulados)

        return df_final.reset_index()

    except Exception as e:
        print(f"Erro CRÍTICO ao calcular acumulado rolling: {e}")
        traceback.print_exc()
        return pd.DataFrame(columns=['id_ponto', 'timestamp', 'chuva_mm'])


# --- FUNÇÃO definir_status_chuva (ALTERADA LÓGICA >=) ---
def definir_status_chuva(chuva_mm):
    """
    Define o status da chuva (LIVRE, ATENÇÃO, etc.)
    """
    try:
        if pd.isna(chuva_mm):
            status_texto = "SEM DADOS"

        elif chuva_mm >= CHUVA_LIMITE_LARANJA:  # >= 100.0
            status_texto = "PARALIZAÇÃO"
        elif chuva_mm > CHUVA_LIMITE_AMARELO:  # > 79.0
            status_texto = "ALERTA"
        elif chuva_mm > CHUVA_LIMITE_VERDE:  # > 60.0
            status_texto = "ATENÇÃO"

        else:
            status_texto = "LIVRE"
        return status_texto, STATUS_MAP_CHUVA.get(status_texto, "secondary")
    except Exception as e:
        print(f"Erro status chuva: {e}");
        return "INDEFINIDO", "secondary"


# --- FUNÇÃO definir_status_umidade_hierarquico (Mantida) ---
def definir_status_umidade_hierarquico(umidade_1m, umidade_2m, umidade_3m,
                                       base_1m, base_2m, base_3m,
                                       chuva_acumulada_72h=0.0):
    """ Define o status/cor de alerta com base nas combinações do fluxograma. """
    try:
        if pd.isna(umidade_1m) or pd.isna(umidade_2m) or pd.isna(umidade_3m) or \
                pd.isna(base_1m) or pd.isna(base_2m) or pd.isna(base_3m):
            return STATUS_MAP_HIERARQUICO[-1]

        s1_sim = (umidade_1m - base_1m) >= DELTA_TRIGGER_UMIDADE
        s2_sim = (umidade_2m - base_2m) >= DELTA_TRIGGER_UMIDADE
        s3_sim = (umidade_3m - base_3m) >= DELTA_TRIGGER_UMIDADE

        risco_final = 0

        if s1_sim and s2_sim and s3_sim:
            risco_final = 3
        elif (s1_sim and s2_sim and not s3_sim) or \
                (not s1_sim and s2_sim and s3_sim):
            risco_final = 2
        elif (s1_sim and not s2_sim and not s3_sim) or \
                (not s1_sim and not s2_sim and s3_sim):
            risco_final = 1

        return STATUS_MAP_HIERARQUICO[risco_final]

    except Exception as e:
        print(f"Erro ao definir status de umidade solo (fluxograma): {e}")
        return STATUS_MAP_HIERARQUICO[-1]


# --- FUNÇÃO definir_status_umidade_individual (Mantida) ---
def definir_status_umidade_individual(umidade_atual, umidade_base, risco_nivel):
    """ Define a cor CSS para um ÚNICO sensor. """
    try:
        if pd.isna(umidade_atual) or pd.isna(umidade_base):
            return "grey"

        if (umidade_atual - umidade_base) >= DELTA_TRIGGER_UMIDADE:
            if risco_nivel == 1:
                return "#FFD700"
            elif risco_nivel == 2:
                return "#fd7e14"
            elif risco_nivel == 3:
                return "#dc3545"
            else:
                return "#FFD700"
        else:
            return "green"

    except Exception:
        return "grey"


# --- FUNÇÃO verificar_trava_base (Mantida) ---
def verificar_trava_base(df_historico_ponto, coluna_umidade, nova_leitura, base_antiga, horas=6):
    """
    Verifica se uma nova leitura baixa deve se tornar a nova base,
    exigindo que todas as leituras nas últimas X horas também estejam abaixo da base antiga.
    """
    try:
        if nova_leitura >= base_antiga:
            return base_antiga

        if df_historico_ponto.empty or 'timestamp' not in df_historico_ponto.columns:
            print(f"[{coluna_umidade}] Primeiro registro é menor que a base. Definindo nova base: {nova_leitura}")
            return nova_leitura

        ultimo_timestamp_no_df = df_historico_ponto['timestamp'].max()
        limite_tempo = ultimo_timestamp_no_df - pd.Timedelta(hours=horas)

        df_ultimas_horas = df_historico_ponto[df_historico_ponto['timestamp'] >= limite_tempo]
        serie_umidade_historica = df_ultimas_horas[coluna_umidade].dropna()

        if not serie_umidade_historica.empty:
            if not (serie_umidade_historica < base_antiga).all():
                print(
                    f"[{coluna_umidade}] GATILHO FALHOU: Nova leitura {nova_leitura} é baixa, mas dados nas últimas {horas}h não estavam. Mantendo base {base_antiga}.")
                return base_antiga

        print(
            f"[{coluna_umidade}] GATILHO SUCESSO: Trava de {horas}h satisfeita. Nova base definida: {nova_leitura} (era {base_antiga})")
        return nova_leitura

    except Exception as e:
        print(f"ERRO CRÍTICO (verificar_trava_base) para {coluna_umidade}: {e}")
        traceback.print_exc()
        return base_antiga