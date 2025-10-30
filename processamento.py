import pandas as pd
import datetime

# --- Importações Corrigidas ---
# Importa TODAS as regras de negócio do config.py
from config import (
    CHUVA_LIMITE_VERDE, CHUVA_LIMITE_AMARELO, CHUVA_LIMITE_LARANJA,
    DELTA_TRIGGER_UMIDADE, RISCO_MAP, STATUS_MAP_HIERARQUICO
)


# --- Fim da Correção ---


# --- FUNÇÃO calcular_acumulado_72h (Mantida) ---
def calcular_acumulado_72h(df_ponto):
    """
    Calcula o acumulado de 72h (rolling).
    Esta função é usada pelo DASHBOARD para gerar os gráficos.
    O WORKER usa um cálculo mais simples (soma total)
    para a verificação de alertas.
    """
    if 'chuva_mm' not in df_ponto.columns or df_ponto.empty or 'timestamp' not in df_ponto.columns:
        return pd.DataFrame(columns=['timestamp', 'chuva_mm'])
    df = df_ponto.sort_values('timestamp').copy()

    try:
        # Garante que o timestamp seja o índice
        if not pd.api.types.is_datetime64_any_dtype(df.index):
            df = df.set_index('timestamp')

        # Garante que não haja duplicatas no índice (pode causar erro no rolling)
        df = df[~df.index.duplicated(keep='last')]

        acumulado_72h = df['chuva_mm'].rolling(window='72h', min_periods=1).sum()
        acumulado_72h = acumulado_72h.rename('chuva_mm')
        return acumulado_72h.reset_index()

    except Exception as e:
        print(f"Erro ao calcular acumulado 72h com rolling window: {e}")
        # Tenta uma abordagem mais simples se o índice de tempo falhar
        try:
            df = df.reset_index(drop=True)  # Reseta se o índice for o problema
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.set_index('timestamp')
            df = df[~df.index.duplicated(keep='last')]
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
    baseado nos limites definidos em config.py
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


# --- FUNÇÃO definir_status_umidade_hierarquico (Mantida, usa constantes importadas) ---
def definir_status_umidade_hierarquico(umidade_1m, umidade_2m, umidade_3m,
                                       base_1m, base_2m, base_3m,
                                       chuva_acumulada_72h=0.0):
    """
    Define o status/cor de alerta com base nas combinações EXATAS do fluxograma.
    Usa DELTA_TRIGGER_UMIDADE de config.py
    Retorna (texto_status_padrão, cor_badge_bootstrap, cor_barra_css).
    """
    try:
        if pd.isna(umidade_1m) or pd.isna(umidade_2m) or pd.isna(umidade_3m) or \
                pd.isna(base_1m) or pd.isna(base_2m) or pd.isna(base_3m):  # Checa NaN nas bases também
            return STATUS_MAP_HIERARQUICO[-1]  # Sem dados

        # Verificar "Atingimento dos limiares" (SIM = True, NÃO = False)
        # Usa o DELTA_TRIGGER_UMIDADE importado (3.0)
        s1_sim = (umidade_1m - base_1m) >= DELTA_TRIGGER_UMIDADE
        s2_sim = (umidade_2m - base_2m) >= DELTA_TRIGGER_UMIDADE
        s3_sim = (umidade_3m - base_3m) >= DELTA_TRIGGER_UMIDADE

        risco_final = 0  # Default é LIVRE (Verde)

        # Condição PARALIZAÇÃO (Vermelho)
        if s1_sim and s2_sim and s3_sim:
            risco_final = 3
        # Condições ALERTA (Laranja)
        elif (s1_sim and s2_sim and not s3_sim) or \
                (not s1_sim and s2_sim and s3_sim):
            risco_final = 2
        # Condições ATENÇÃO (Amarelo)
        elif (s1_sim and not s2_sim and not s3_sim) or \
                (not s1_sim and not s2_sim and s3_sim):
            risco_final = 1
        # Outras combinações (ex: 2m sozinho) são LIVRE

        return STATUS_MAP_HIERARQUICO[risco_final]

    except Exception as e:
        print(f"Erro ao definir status de umidade solo (fluxograma): {e}")
        return STATUS_MAP_HIERARQUICO[-1]


# --- FUNÇÃO definir_status_umidade_individual (Mantida, usa constantes importadas) ---
def definir_status_umidade_individual(umidade_atual, umidade_base, risco_nivel):
    """
    Define a cor CSS para um ÚNICO sensor (usado nos gauges do map_view).
    Usa o nível de risco (1, 2, 3) para determinar a cor (Amarelo, Laranja, Vermelho)
    se o sensor estiver ativo (delta >= 3%).
    """
    try:
        if pd.isna(umidade_atual) or pd.isna(umidade_base):
            return "grey"  # Sem Dados

        # Usa o DELTA_TRIGGER_UMIDADE importado (3.0)
        if (umidade_atual - umidade_base) >= DELTA_TRIGGER_UMIDADE:
            # Se o delta é >= 3%, o sensor está "ativo".
            if risco_nivel == 1:
                return "#FFD700"  # Amarelo/Ouro (Atenção)
            elif risco_nivel == 2:
                return "#fd7e14"  # Laranja (Alerta)
            elif risco_nivel == 3:
                return "#dc3545"  # Vermelho (Paralisação)
            else:
                # Se o risco for 0 (LIVRE) mas o delta for > 3% (inconsistente)
                # ou se o risco for desconhecido, trata como Amarelo.
                return "#FFD700"
        else:
            # Se está abaixo do gatilho, está "Livre"
            return "green"  # Verde/Livre

    except Exception:
        return "grey"

