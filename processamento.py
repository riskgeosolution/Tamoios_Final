# processamento.py (CORREÇÃO FINAL v6 - Lógica de Acumulado Simplificada)

import pandas as pd
import datetime
import traceback
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

from config import (
    CHUVA_LIMITE_VERDE, CHUVA_LIMITE_AMARELO, CHUVA_LIMITE_LARANJA,
    DELTA_TRIGGER_UMIDADE, RISCO_MAP, STATUS_MAP_HIERARQUICO,
    FREQUENCIA_API_SEGUNDOS, STATUS_MAP_CHUVA
)


# --- FUNÇÃO calcular_acumulado_rolling (SIMPLIFICADA E CORRIGIDA) ---
def calcular_acumulado_rolling(df_ponto, horas=72):
    """
    Calcula o acumulado 'rolling' somando os valores de chuva incremental (chuva_mm)
    que já foram previamente tratados na coleta.
    
    Esta versão é mais simples e direta, pois a lógica de tratamento de duplicatas
    foi movida para a etapa de aquisição de dados.
    """
    if 'chuva_mm' not in df_ponto.columns or df_ponto.empty or 'timestamp' not in df_ponto.columns:
        return pd.DataFrame(columns=['id_ponto', 'timestamp', 'chuva_mm'])

    df_ponto = df_ponto.sort_values('timestamp').set_index('timestamp')

    try:
        # Garante que a coluna de chuva é numérica
        series_chuva = pd.to_numeric(df_ponto['chuva_mm'], errors='coerce').fillna(0)

        # A frequência dos dados é de 10 minutos. A janela de rolling é em número de períodos.
        # Ex: 72 horas = 72 * 6 períodos de 10 minutos
        window_size = int(horas * (60 / 10))

        # Calcula o acumulado diretamente sobre os dados incrementais
        acumulado = series_chuva.rolling(window=window_size, min_periods=1).sum()
        
        df_final = acumulado.to_frame(name='chuva_mm')
        df_final['id_ponto'] = df_ponto['id_ponto']

        return df_final.reset_index()

    except Exception as e:
        print(f"Erro CRÍTICO ao calcular acumulado rolling: {e}")
        traceback.print_exc()
        return pd.DataFrame(columns=['id_ponto', 'timestamp', 'chuva_mm'])


# --- FUNÇÃO definir_status_chuva (Mantida) ---
def definir_status_chuva(chuva_mm):
    """
    Define o status da chuva (LIVRE, ATENÇÃO, etc.)
    """
    try:
        if pd.isna(chuva_mm):
            status_texto = "SEM DADOS"
        elif chuva_mm >= CHUVA_LIMITE_LARANJA:
            status_texto = "PARALIZAÇÃO"
        elif chuva_mm > CHUVA_LIMITE_AMARELO:
            status_texto = "ALERTA"
        elif chuva_mm > CHUVA_LIMITE_VERDE:
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