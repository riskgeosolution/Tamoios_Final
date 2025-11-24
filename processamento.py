# processamento.py (VERSÃO FINAL: LÓGICA DE ODÔMETRO/ACUMULADO)

import pandas as pd
import numpy as np
import traceback
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

from config import (
    CHUVA_LIMITE_VERDE, CHUVA_LIMITE_AMARELO, CHUVA_LIMITE_LARANJA,
    DELTA_TRIGGER_UMIDADE, RISCO_MAP, STATUS_MAP_HIERARQUICO
)


def calcular_acumulado_rolling(df_ponto, horas=72):
    """
    Calcula a chuva real usando a diferença do ACUMULADO DIÁRIO (Odômetro),
    que é à prova de falhas de coleta e sincronia.
    """
    # Validação básica
    required_cols = ['timestamp', 'id_ponto']
    if df_ponto.empty or not all(col in df_ponto.columns for col in required_cols):
        return pd.DataFrame(columns=['id_ponto', 'timestamp', 'chuva_mm'])

    df_ponto = df_ponto.sort_values('timestamp')
    df_original = df_ponto.copy()

    try:
        df_original['timestamp'] = pd.to_datetime(df_original['timestamp'])
        df_original = df_original.set_index('timestamp')

        # Verifica se temos a coluna vital: precipitacao_acumulada_mm
        if 'precipitacao_acumulada_mm' not in df_original.columns:
            # Fallback: Se não tiver acumulado, usa a soma simples (legado)
            df_original['chuva_mm'] = pd.to_numeric(df_original['chuva_mm'], errors='coerce').fillna(0)
            window_size = int(horas * 6)  # 6 dados por hora (10 min)
            return df_original['chuva_mm'].rolling(window=window_size, min_periods=1).sum().reset_index()

        # Garante numérico e preenche vazios com o último valor válido (o odômetro não volta)
        df_original['precipitacao_acumulada_mm'] = pd.to_numeric(df_original['precipitacao_acumulada_mm'],
                                                                 errors='coerce')

        lista_dfs = []

        for ponto_id in df_original['id_ponto'].unique():
            df_pt = df_original[df_original['id_ponto'] == ponto_id].copy()

            # 1. Preenche buracos no acumulado (se a net cair, o acumulado do dia se mantém)
            df_pt['precipitacao_acumulada_mm'] = df_pt['precipitacao_acumulada_mm'].ffill().fillna(0)

            # 2. A MÁGICA: Calcula a chuva real subtraindo o valor atual do anterior (Diferencial)
            df_pt['chuva_real_incremental'] = df_pt['precipitacao_acumulada_mm'].diff().fillna(0)

            # 3. Correção da Meia-Noite:
            # Quando vira o dia, o acumulado vai de ex: 20mm para 0mm. O diff dá -20.
            # Se o diff for negativo, significa que o dia virou. A chuva real é apenas o novo valor.
            mask_virada_dia = df_pt['chuva_real_incremental'] < 0
            df_pt.loc[mask_virada_dia, 'chuva_real_incremental'] = df_pt.loc[
                mask_virada_dia, 'precipitacao_acumulada_mm']

            # 4. Agora temos a chuva exata minuto a minuto. Fazemos a soma Rolling de 72h.
            # Resample para 10T para garantir a grade temporal correta
            df_resampled = df_pt['chuva_real_incremental'].resample('10T').sum().fillna(0)

            # Janela de 72h (72 * 6 blocos de 10min = 432)
            acumulado_rolling = df_resampled.rolling(window=432, min_periods=1).sum()

            temp_df = acumulado_rolling.to_frame(name='chuva_mm')
            temp_df['id_ponto'] = ponto_id
            lista_dfs.append(temp_df)

        if not lista_dfs:
            return pd.DataFrame(columns=['id_ponto', 'timestamp', 'chuva_mm'])

        return pd.concat(lista_dfs).reset_index()

    except Exception as e:
        print(f"Erro CRÍTICO (Cálculo Acumulado): {e}")
        traceback.print_exc()
        return pd.DataFrame(columns=['id_ponto', 'timestamp', 'chuva_mm'])


def definir_status_chuva(chuva_mm):
    STATUS_MAP_CHUVA = {"LIVRE": "success", "ATENÇÃO": "warning", "ALERTA": "orange", "PARALIZAÇÃO": "danger",
                        "SEM DADOS": "secondary", "INDEFINIDO": "secondary"}
    try:
        if pd.isna(chuva_mm): return "SEM DADOS", "secondary"
        if chuva_mm >= CHUVA_LIMITE_LARANJA: return "PARALIZAÇÃO", STATUS_MAP_CHUVA["PARALIZAÇÃO"]
        if chuva_mm > CHUVA_LIMITE_AMARELO: return "ALERTA", STATUS_MAP_CHUVA["ALERTA"]
        if chuva_mm > CHUVA_LIMITE_VERDE: return "ATENÇÃO", STATUS_MAP_CHUVA["ATENÇÃO"]
        return "LIVRE", STATUS_MAP_CHUVA["LIVRE"]
    except:
        return "INDEFINIDO", "secondary"


def definir_status_umidade_hierarquico(umidade_1m, umidade_2m, umidade_3m, base_1m, base_2m, base_3m):
    try:
        if pd.isna(umidade_1m) or pd.isna(umidade_2m) or pd.isna(umidade_3m) or pd.isna(base_1m) or pd.isna(
                base_2m) or pd.isna(base_3m):
            return STATUS_MAP_HIERARQUICO[-1]  # SEM DADOS

        s1 = (umidade_1m - base_1m) >= DELTA_TRIGGER_UMIDADE
        s2 = (umidade_2m - base_2m) >= DELTA_TRIGGER_UMIDADE
        s3 = (umidade_3m - base_3m) >= DELTA_TRIGGER_UMIDADE

        risco = 0
        if s1 and s2 and s3:
            risco = 3
        elif (s1 and s2 and not s3) or (not s1 and s2 and s3):
            risco = 2
        elif (s1 and not s2 and not s3) or (not s1 and not s2 and s3):
            risco = 1

        return STATUS_MAP_HIERARQUICO[risco]
    except:
        return STATUS_MAP_HIERARQUICO[-1]