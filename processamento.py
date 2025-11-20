# processamento.py (CORRIGIDO v3: Adicionada função de trava de 6h)

import pandas as pd
import datetime
import traceback  # Importar traceback

# --- Importações Corrigidas ---
from config import (
    CHUVA_LIMITE_VERDE, CHUVA_LIMITE_AMARELO, CHUVA_LIMITE_LARANJA,
    DELTA_TRIGGER_UMIDADE, RISCO_MAP, STATUS_MAP_HIERARQUICO,
    FREQUENCIA_API_SEGUNDOS
)


# --- FUNÇÃO calcular_acumulado_rolling (MODIFICADA E MAIS ROBUSTA) ---
def calcular_acumulado_rolling(df_ponto, horas=72):
    """
    Calcula o acumulado 'rolling' somando os valores de chuva incremental (chuva_mm)
    para um número dinâmico de horas.
    Esta versão é robusta a buracos nos dados E a múltiplos pontos.
    """
    if 'chuva_mm' not in df_ponto.columns or df_ponto.empty or 'timestamp' not in df_ponto.columns:
        return pd.DataFrame(columns=['id_ponto', 'timestamp', 'chuva_mm'])

    # --- CORREÇÃO DEFINITIVA: Garantir que os dados estejam ordenados por tempo ---
    # Esta é a pré-condição mais importante para qualquer cálculo de janela (rolling).
    df_ponto = df_ponto.sort_values('timestamp')
    
    df_original = df_ponto.copy()

    try:
        # 1. Garantir que o timestamp é datetime e é o índice
        df_original['timestamp'] = pd.to_datetime(df_original['timestamp'])
        df_original = df_original.set_index('timestamp')
        df_original['chuva_mm'] = pd.to_numeric(df_original['chuva_mm'], errors='coerce')

        # --- INÍCIO DA ALTERAÇÃO (LÓGICA MAIS ROBUSTA) ---

        lista_dfs_acumulados = []
        PONTOS_POR_HORA = 3600 // FREQUENCIA_API_SEGUNDOS
        window_size = int(horas * PONTOS_POR_HORA)

        # 2. Define uma função interna para processar uma Série
        def calcular_rolling_para_serie(serie_chuva):
            # Resample para 15T para preencher buracos
            df_resampled = serie_chuva.resample('15T').sum()
            df_resampled = df_resampled.fillna(0)
            # Calcula o rolling
            acumulado = df_resampled.rolling(window=window_size, min_periods=1).sum()
            return acumulado

        # 3. Itera por cada ponto único no DataFrame
        for ponto_id in df_original['id_ponto'].unique():
            # Pega a Série de chuva apenas para este ponto
            series_ponto = df_original[df_original['id_ponto'] == ponto_id]['chuva_mm']

            # Calcula o acumulado para esta Série
            acumulado_series = calcular_rolling_para_serie(series_ponto)

            # Converte a Série de volta para um DataFrame
            acumulado_df = acumulado_series.to_frame(name='chuva_mm')

            # Adiciona a coluna 'id_ponto' de volta
            acumulado_df['id_ponto'] = ponto_id

            # Adiciona à lista
            lista_dfs_acumulados.append(acumulado_df)

        # 4. Concatena todos os resultados
        df_final = pd.concat(lista_dfs_acumulados)

        # 5. Retorna o DataFrame com as colunas 'timestamp', 'chuva_mm', 'id_ponto'
        return df_final.reset_index()
        # --- FIM DA ALTERAÇÃO ---

    except Exception as e:
        print(f"Erro CRÍTICO ao calcular acumulado rolling: {e}")
        traceback.print_exc()
        return pd.DataFrame(columns=['id_ponto', 'timestamp', 'chuva_mm'])


# --- FIM DA ALTERAÇÃO ---


# --- FUNÇÃO definir_status_chuva (ALTERADA LÓGICA >=) ---
def definir_status_chuva(chuva_mm):
    """
    Define o status da chuva (LIVRE, ATENÇÃO, etc.)
    """
    STATUS_MAP_CHUVA = {"LIVRE": "success", "ATENÇÃO": "warning", "ALERTA": "orange", "PARALIZAÇÃO": "danger",
                        "SEM DADOS": "secondary", "INDEFINIDO": "secondary"}
    try:
        if pd.isna(chuva_mm):
            status_texto = "SEM DADOS"

        # --- INÍCIO DA ALTERAÇÃO ---
        # A lógica agora é >= para PARALIZAÇÃO
        elif chuva_mm >= CHUVA_LIMITE_LARANJA:  # >= 100.0
            status_texto = "PARALIZAÇÃO"
        elif chuva_mm > CHUVA_LIMITE_AMARELO:  # > 79.0
            status_texto = "ALERTA"
        elif chuva_mm > CHUVA_LIMITE_VERDE:  # > 60.0
            status_texto = "ATENÇÃO"
        # --- FIM DA ALTERAÇÃO ---

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


# --- INÍCIO DA NOVA FUNÇÃO (TRAVA DE SEGURANÇA DA BASE) ---
def verificar_trava_base(df_historico_ponto, coluna_umidade, nova_leitura, base_antiga, horas=6):
    """
    Verifica se uma nova leitura baixa deve se tornar a nova base,
    exigindo que todas as leituras nas últimas X horas também estejam abaixo da base antiga.
    """
    try:
        # 1. Se a nova leitura não for um gatilho (não for menor), a base antiga é mantida.
        if nova_leitura >= base_antiga:
            return base_antiga  # Retorna a base antiga (sem mudança)

        # 2. GATILHO: A nova leitura é menor. Agora, verifique as últimas 6 horas.

        # Pega o timestamp mais recente do histórico
        if df_historico_ponto.empty or 'timestamp' not in df_historico_ponto.columns:
            # Não há histórico (primeiro run), mas a nova leitura é menor que a base padrão.
            # Neste caso, devemos assumir que é a nova base (pois não há histórico para verificar).
            print(f"[{coluna_umidade}] Primeiro registro é menor que a base. Definindo nova base: {nova_leitura}")
            return nova_leitura

            # Pega o timestamp mais recente do DF
        ultimo_timestamp_no_df = df_historico_ponto['timestamp'].max()
        # Define o limite de 6 horas atrás
        limite_tempo = ultimo_timestamp_no_df - pd.Timedelta(hours=horas)

        # Filtra o histórico para as últimas 6 horas (excluindo a nova leitura)
        df_ultimas_horas = df_historico_ponto[df_historico_ponto['timestamp'] >= limite_tempo]

        # Pega a série de dados de umidade, removendo valores nulos (NaN)
        serie_umidade_historica = df_ultimas_horas[coluna_umidade].dropna()

        # 3. Verifica se TODO o histórico recente está abaixo da base antiga

        if not serie_umidade_historica.empty:
            # .all() verifica se TODOS os valores são True
            if not (serie_umidade_historica < base_antiga).all():
                # Se *qualquer* valor histórico nas últimas 6h for >= à base antiga, a trava falha.
                # O solo "se recuperou" nesse período, então foi um spike ou evento curto.
                print(
                    f"[{coluna_umidade}] GATILHO FALHOU: Nova leitura {nova_leitura} é baixa, mas dados nas últimas {horas}h não estavam. Mantendo base {base_antiga}.")
                return base_antiga  # Retorna a base antiga (sem mudança)

        # 4. Se chegou aqui:
        # (A) A nova leitura é < base_antiga (verificado no Passo 1)
        # (B) Todo o histórico das últimas 6h também é < base_antiga (verificado no Passo 3)
        # A trava de 6 horas foi satisfeita.
        print(
            f"[{coluna_umidade}] GATILHO SUCESSO: Trava de {horas}h satisfeita. Nova base definida: {nova_leitura} (era {base_antiga})")
        return nova_leitura  # Retorna a nova base!

    except Exception as e:
        print(f"ERRO CRÍTICO (verificar_trava_base) para {coluna_umidade}: {e}")
        traceback.print_exc()
        # FAIL-SAFE: Em caso de erro, NUNCA mude a base.
        return base_antiga
# --- FIM DA NOVA FUNÇÃO ---