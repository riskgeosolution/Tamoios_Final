# processamento.py (FINAL CONSOLIDADO - ADICIONANDO LOGS)

import pandas as pd
import datetime

# --- IMPORTS NECESSÁRIAS ---
import data_source  # Necessário para ler o eventos.log
# --- FIM IMPORTS NECESSÁRIAS ---

# --- Importações Corrigidas ---
# Importa TODAS as regras de negócio do config.py
from config import (
    CHUVA_LIMITE_VERDE, CHUVA_LIMITE_AMARELO, CHUVA_LIMITE_LARANJA,
    DELTA_TRIGGER_UMIDADE, RISCO_MAP, STATUS_MAP_HIERARQUICO
)


# --- FUNÇÃO calcular_acumulado_72h (Mantida) ---
def calcular_acumulado_72h(df_ponto):
    """
    Calcula o acumulado de 72h (rolling) somando os valores de chuva incremental (chuva_mm).
    """
    if 'chuva_mm' not in df_ponto.columns or df_ponto.empty or 'timestamp' not in df_ponto.columns:
        return pd.DataFrame(columns=['timestamp', 'chuva_mm'])
    df = df_ponto.sort_values('timestamp').copy()

    try:
        # Garante que o timestamp seja o índice
        if not pd.api.types.is_datetime64_any_dtype(df.index):
            df = df.set_index('timestamp').copy()

            # Garante que não haja duplicatas no índice (pode causar erro no rolling)
        df = df[~df.index.duplicated(keep='last')]

        # Garante que 'chuva_mm' é numérico para somar
        df['chuva_mm'] = pd.to_numeric(df['chuva_mm'], errors='coerce').fillna(0)

        # **LÓGICA CRÍTICA DE ACUMULAÇÃO:** Soma a chuva_mm (incremental) na janela de 72h.
        acumulado_72h = df['chuva_mm'].rolling(window='72h', min_periods=1).sum()
        acumulado_72h = acumulado_72h.rename('chuva_mm')
        return acumulado_72h.reset_index()

    except Exception as e:
        print(f"Erro ao calcular acumulado 72h com rolling window: {e}")
        try:
            df = df.reset_index(drop=True)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.set_index('timestamp')
            df = df[~df.index.duplicated(keep='last')]
            df['chuva_mm'] = pd.to_numeric(df['chuva_mm'], errors='coerce').fillna(0)
            acumulado_72h = df['chuva_mm'].rolling(window='72h', min_periods=1).sum()
            acumulado_72h = acumulado_72h.rename('chuva_mm')
            return acumulado_72h.reset_index()
        except Exception as e_inner:
            print(f"Erro interno (e_inner) ao calcular acumulado 72h: {e_inner}")
            return pd.DataFrame(columns=['timestamp', 'chuva_mm'])


# --- FUNÇÃO definir_status_chuva (Mantida, usa constantes importadas) ---
def definir_status_chuva(chuva_mm):
    """
    Define o status da chuva (LIVRE, ATENÇÃO, etc.)
    """
    STATUS_MAP_CHUVA = {"LIVRE": "success", "ATENÇÃO": "warning", "ALERTA": "orange", "PARALIZAÇÃO": "danger",
                        "SEM DADOS": "secondary", "INDEFINIDO": "secondary"}
    try:
        if pd.isna(chuva_mm):
            status_texto = "SEM DADOS"
        elif chuva_mm > CHUVA_LIMITE_LARANJA:
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


# --- NOVO: FUNÇÃO PARA LER LOGS DO DISCO ---
def ler_logs_eventos(id_ponto):
    """
    Lê o log do arquivo eventos.log e retorna o conteúdo filtrado como string.
    """
    try:
        # A função get_all_data_from_disk retorna logs_str como uma string grande
        # Precisamos importá-la do data_source
        logs_str, status_antigos_do_disco, _ = data_source.get_all_data_from_disk()

        # O data_source.get_all_data_from_disk() retorna (historico_df, status, logs_str)
        # O log string é o terceiro elemento retornado. Vamos ajusta a chamada para o log

        # Ajustando a importação:
        from data_source import get_all_data_from_disk

        _, _, logs_str = get_all_data_from_disk()

        # Filtra a string de logs
        logs_list = logs_str.split('\n')
        # Filtra por ponto específico ou logs gerais
        logs_filtrados = [log for log in logs_list if f"| {id_ponto} |" in log or "| GERAL |" in log]

        # Retorna logs como uma única string (com nova linha)
        return '\n'.join(logs_filtrados)

    except Exception as e:
        return f"ERRO ao ler logs: {e}"