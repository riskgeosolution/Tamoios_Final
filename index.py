# index.py (CORREÇÃO FINAL - Migração de Status)

import dash
from dash import html, dcc, callback, Input, Output, State
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
import pandas as pd
from io import StringIO
import os
import json
from dotenv import load_dotenv
import time
from threading import Thread
import datetime
from httpx import HTTPStatusError
import traceback

load_dotenv()

from app import app, server
from pages import login as login_page, main_app as main_app_page, map_view, general_dash, specific_dash
import data_source
import config
import processamento
import alertas
from config import PONTOS_DE_ANALISE, RISCO_MAP, FREQUENCIA_API_SEGUNDOS, ID_PONTO_ZENTRA_KM72, CONSTANTES_PADRAO
from config import RENDER_SLEEP_TIME_SEC

SENHA_CLIENTE = '@Tamoiosv1'
SENHA_ADMIN = 'admin456'

# --- INÍCIO DA CORREÇÃO: Inicialização centralizada do banco de dados ---
data_source.setup_disk_paths()
data_source.initialize_database()
# --- FIM DA CORREÇÃO ---

# ==============================================================================
# --- LÓGICA DO WORKER (FONTE ÚNICA DA VERDADE) ---
# ==============================================================================

def worker_verificar_alertas(status_novos, status_antigos):
    """ Compara o status novo com o antigo e loga as mudanças, lidando com a migração de formato. """
    if not status_novos: return status_antigos
    if not isinstance(status_antigos, dict):
        status_antigos = {}

    status_atualizado = status_antigos.copy()
    for id_ponto in PONTOS_DE_ANALISE.keys():
        status_novo_info = status_novos.get(id_ponto, {"status": "SEM DADOS"})
        status_antigo_info = status_antigos.get(id_ponto)

        status_antigo_str = "INDEFINIDO"
        if isinstance(status_antigo_info, dict):
            status_antigo_str = status_antigo_info.get('status', "INDEFINIDO")
        elif isinstance(status_antigo_info, str):
            status_antigo_str = status_antigo_info

        status_novo_str = status_novo_info.get('status', "SEM DADOS")

        if status_novo_str != status_antigo_str:
            try:
                nome_ponto = PONTOS_DE_ANALISE[id_ponto]['nome']
                msg = f"MUDANÇA DE STATUS: {nome_ponto} mudou de {status_antigo_str} para {status_novo_str}."
                data_source.adicionar_log(id_ponto, msg, level="WARN") # Mudanças de status são importantes
            except Exception as e:
                data_source.adicionar_log("GERAL", f"Erro ao processar mudança de status: {e}", level="ERROR")
        
        status_atualizado[id_ponto] = status_novo_info
        
    return status_atualizado


def worker_main_loop():
    inicio_ciclo = time.time()
    
    try:
        data_source.adicionar_log("WORKER", "Início do ciclo de processamento.")

        # 1. BUSCAR DADOS
        historico_recente_df = data_source.get_recent_data_for_worker(hours=100)
        status_antigos_do_disco = data_source.get_status_from_disk()
        data_source.adicionar_log("WORKER", f"Encontrados {len(historico_recente_df)} registros no histórico recente.")

        numeric_cols_history = ['chuva_mm', 'umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc']
        for col in numeric_cols_history:
            if col in historico_recente_df.columns:
                historico_recente_df.loc[:, col] = pd.to_numeric(historico_recente_df[col], errors='coerce')

        # 2. COLETAR DADOS DAS APIS
        novos_dados_chuva_df, _, _ = data_source.fetch_data_from_weatherlink_api()
        data_source.adicionar_log("API", f"WeatherLink retornou {len(novos_dados_chuva_df)} novos registros.")
        
        df_umidade_incremental = data_source.fetch_data_from_zentra_cloud()
        if not df_umidade_incremental.empty:
            data_source.adicionar_log("API", f"Zentra Cloud retornou {len(df_umidade_incremental)} novos registros.")
        else:
            data_source.adicionar_log("API", "Zentra Cloud não retornou dados novos.")

        # 3. NOVA LÓGICA DE MERGE E ATUALIZAÇÃO
        df_combinado = pd.concat([historico_recente_df, novos_dados_chuva_df, df_umidade_incremental], ignore_index=True)
        df_combinado.drop_duplicates(inplace=True)

        key_cols = ['timestamp', 'id_ponto']
        df_mesclado = df_combinado.groupby(key_cols).first().reset_index()
        
        if not historico_recente_df.empty:
            df_merged_indexed = df_mesclado.set_index(key_cols)
            df_historico_indexed = historico_recente_df.set_index(key_cols)
            df_merged_aligned, df_historico_aligned = df_merged_indexed.align(df_historico_indexed, fill_value=-999)
            diferencas = (df_merged_aligned != df_historico_aligned).any(axis=1)
            df_para_salvar = df_mesclado[df_mesclado.set_index(key_cols).index.isin(diferencas[diferencas].index)]
        else:
            df_para_salvar = df_mesclado

        # 4. SALVAR E PREPARAR DADOS PARA CÁLCULO
        if not df_para_salvar.empty:
            data_source.adicionar_log("DB", f"Iniciando operação de UPSERT para {len(df_para_salvar)} registros.")
            timestamps_para_deletar = df_para_salvar['timestamp'].unique()
            data_source.delete_from_sqlite(timestamps=timestamps_para_deletar)
            data_source.save_to_sqlite(df_para_salvar)
        else:
            data_source.adicionar_log("DB", "Nenhum dado novo ou atualizado para salvar.")

        historico_para_calculo = df_mesclado.sort_values('timestamp').reset_index(drop=True)

        # 5. LÓGICA DE BACKFILL (Simplificada)
        for id_ponto_analise in PONTOS_DE_ANALISE.keys():
            agora_utc = datetime.datetime.now(datetime.timezone.utc)
            limite_lacuna = agora_utc - datetime.timedelta(minutes=30) # Se o último dado for mais velho que 30 min

            df_ponto_historico = historico_para_calculo[historico_para_calculo['id_ponto'] == id_ponto_analise]
            ultimo_timestamp_db = df_ponto_historico['timestamp'].max() if not df_ponto_historico.empty else None

            if ultimo_timestamp_db is None or ultimo_timestamp_db < limite_lacuna:
                data_source.adicionar_log(id_ponto_analise, f"Lacuna detectada. Último dado: {ultimo_timestamp_db}. Disparando backfill.", level="WARN")
                if id_ponto_analise == ID_PONTO_ZENTRA_KM72:
                    data_source.backfill_zentra_km72_data()
                data_source.backfill_weatherlink_data(id_ponto_analise)
                historico_para_calculo = data_source.get_recent_data_for_worker(hours=100) # Recarrega

        # 6. CALCULAR STATUS
        status_atualizado = {}
        if not historico_para_calculo.empty:
            cols_numericas_calculo = ['chuva_mm', 'umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc']
            for col in cols_numericas_calculo:
                if col in historico_para_calculo.columns:
                    historico_para_calculo[col] = pd.to_numeric(historico_para_calculo[col], errors='coerce')

            for id_ponto in PONTOS_DE_ANALISE.keys():
                df_ponto = historico_para_calculo[historico_para_calculo['id_ponto'] == id_ponto].copy()
                ponto_info = {"status": "SEM DADOS", "chuva_72h": 0.0, "umidade_1m": None, "umidade_2m": None, "umidade_3m": None, "timestamp_local": None}

                if df_ponto.empty:
                    status_atualizado[id_ponto] = ponto_info
                    continue

                acumulado_72h = processamento.calcular_acumulado_rolling(df_ponto, horas=72)
                chuva_72h_final = acumulado_72h['chuva_mm'].iloc[-1] if not acumulado_72h.empty else 0.0
                ponto_info['chuva_72h'] = round(chuva_72h_final, 1) if not pd.isna(chuva_72h_final) else 0.0
                status_chuva, _ = processamento.definir_status_chuva(ponto_info['chuva_72h'])

                ultimo_dado = df_ponto.sort_values('timestamp').iloc[-1]
                ponto_info['umidade_1m'] = round(ultimo_dado.get('umidade_1m_perc'), 1) if pd.notna(ultimo_dado.get('umidade_1m_perc')) else None
                ponto_info['umidade_2m'] = round(ultimo_dado.get('umidade_2m_perc'), 1) if pd.notna(ultimo_dado.get('umidade_2m_perc')) else None
                ponto_info['umidade_3m'] = round(ultimo_dado.get('umidade_3m_perc'), 1) if pd.notna(ultimo_dado.get('umidade_3m_perc')) else None
                
                if pd.notna(ultimo_dado.get('timestamp')):
                    ponto_info['timestamp_local'] = pd.to_datetime(ultimo_dado.get('timestamp')).tz_convert('America/Sao_Paulo').isoformat()

                df_umidade = df_ponto.dropna(subset=['umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc'])
                bases = {f'base_{d}m': df_umidade[f'umidade_{d}m_perc'].min() if not df_umidade.empty else CONSTANTES_PADRAO[f'UMIDADE_BASE_{d}M'] for d in [1, 2, 3]}
                status_umidade_tuple = processamento.definir_status_umidade_hierarquico(ponto_info['umidade_1m'], ponto_info['umidade_2m'], ponto_info['umidade_3m'], **bases)
                status_umidade = status_umidade_tuple[0] if status_umidade_tuple[0] != "SEM DADOS" else "LIVRE"

                status_final = status_umidade if RISCO_MAP.get(status_umidade, 0) > RISCO_MAP.get(status_chuva, 0) else status_chuva
                ponto_info['status'] = status_final
                
                status_atualizado[id_ponto] = ponto_info
        
        # 7. VERIFICAR ALERTAS E SALVAR STATUS
        status_final_completo = worker_verificar_alertas(status_atualizado, status_antigos_do_disco)
        data_source.write_with_timeout(data_source.STATUS_FILE, status_final_completo, timeout=20)
        data_source.adicionar_log("WORKER", "Arquivo de status salvo com sucesso.")

        tempo_ciclo = time.time() - inicio_ciclo
        data_source.adicionar_log("WORKER", f"Ciclo concluído em {tempo_ciclo:.2f}s.")
        return True
    except Exception as e:
        data_source.adicionar_log("WORKER", f"ERRO CRÍTICO NO CICLO: {e}", level="ERROR")
        traceback.print_exc()
        return False


def background_task_wrapper():
    # data_source.setup_disk_paths() # REMOVIDO: Inicializado globalmente
    # data_source.initialize_database() # REMOVIDO: Inicializado globalmente
    data_source.adicionar_log("SISTEMA", "Processo Worker (Thread) iniciado.")
    
    time.sleep(RENDER_SLEEP_TIME_SEC)

    INTERVALO_EM_MINUTOS = 10
    CARENCIA_EM_SEGUNDOS = 60
    while True:
        if not worker_main_loop():
            data_source.adicionar_log("SISTEMA", f"Ciclo do worker falhou. Reiniciando em {RENDER_SLEEP_TIME_SEC}s.", level="ERROR")
            time.sleep(RENDER_SLEEP_TIME_SEC)
            continue

        agora = datetime.datetime.now(datetime.timezone.utc)
        proximo_minuto_slot = (agora.minute // INTERVALO_EM_MINUTOS + 1) * INTERVALO_EM_MINUTOS
        if proximo_minuto_slot >= 60:
            proxima_hora = agora + datetime.timedelta(hours=1)
            proxima_exec_base = proxima_hora.replace(minute=0, second=0, microsecond=0)
        else:
            proxima_exec_base = agora.replace(minute=proximo_minuto_slot, second=0, microsecond=0)
        proxima_exec_com_carencia = proxima_exec_base + datetime.timedelta(seconds=CARENCIA_EM_SEGUNDOS)
        sleep_time = (proxima_exec_com_carencia - agora).total_seconds()
        if sleep_time < 0: sleep_time = CARENCIA_EM_SEGUNDOS
        data_source.adicionar_log("WORKER", f"Próxima execução em ~{sleep_time:.0f}s...")
        time.sleep(sleep_time)


# ==============================================================================
# --- LAYOUT E CALLBACKS DA APLICAÇÃO ---
# ==============================================================================

app.layout = html.Div([
    dcc.Store(id='session-store', data={'logged_in': False, 'user_type': 'guest'}, storage_type='session'),
    dcc.Store(id='store-dados-sessao', storage_type='session'),
    dcc.Store(id='store-ultimo-status', storage_type='session'),
    dcc.Store(id='store-logs-sessao', storage_type='session'),
    dcc.Location(id='url-raiz', refresh=False),
    dcc.Interval(id='intervalo-atualizacao-dados', interval=60 * 1000, n_intervals=0, disabled=True),
    html.Div(id='page-container-root')
])


@app.callback(Output('page-container-root', 'children'), [Input('session-store', 'data')])
def display_page_root(session_data):
    if session_data and session_data.get('logged_in'):
        return main_app_page.get_layout()
    return login_page.get_layout()


@app.callback(Output('page-content', 'children'), [Input('url-raiz', 'pathname'), Input('session-store', 'data')])
def display_page_content(pathname, session_data):
    if not (session_data and session_data.get('logged_in')):
        raise PreventUpdate
    if pathname.startswith('/ponto/'): return specific_dash.get_layout()
    if pathname == '/dashboard-geral': return general_dash.get_layout()
    return map_view.get_layout()


@app.callback(
    [Output('session-store', 'data'), Output('login-error-output', 'children'),
     Output('login-error-output', 'className'), Output('input-password', 'value')],
    [Input('btn-login', 'n_clicks'), Input('input-password', 'n_submit')],
    [State('input-password', 'value')], prevent_initial_call=True
)
def login_callback(n_clicks, n_submit, password):
    if not (n_clicks or n_submit): raise PreventUpdate
    if not password: return dash.no_update, "Por favor, digite a senha.", "text-danger mb-3 text-center", ""
    password = password.strip()
    if password == SENHA_ADMIN: return {'logged_in': True, 'user_type': 'admin'}, "", "", ""
    if password == SENHA_CLIENTE: return {'logged_in': True, 'user_type': 'client'}, "", "", ""
    return dash.no_update, "Senha incorreta.", "text-danger mb-3 text-center", ""


@app.callback(
    [Output('session-store', 'data', allow_duplicate=True), Output('url-raiz', 'pathname')],
    [Input('logout-button', 'n_clicks')], prevent_initial_call=True
)
def logout_callback(n_clicks):
    if not n_clicks: raise PreventUpdate
    return {'logged_in': False, 'user_type': 'guest'}, '/'


@app.callback(Output('intervalo-atualizacao-dados', 'disabled'), [Input('session-store', 'data')])
def toggle_interval_update(session_data):
    return not (session_data and session_data.get('logged_in'))


@app.callback(
    [Output('store-dados-sessao', 'data'), Output('store-ultimo-status', 'data'), Output('store-logs-sessao', 'data')],
    [Input('intervalo-atualizacao-dados', 'n_intervals')]
)
def update_data_and_logs_from_disk(n_intervals):
    df, status, logs = data_source.get_all_data_for_dashboard()
    return df.to_json(date_format='iso', orient='split'), status, logs


# ==============================================================================
# --- EXECUÇÃO ---
# ==============================================================================

if not os.environ.get("WERKZEUG_MAIN"):
    data_source.adicionar_log("SISTEMA", "Iniciando o worker (coletor de dados) em um thread separado.")
    Thread(target=background_task_wrapper, daemon=True).start()
else:
    data_source.adicionar_log("SISTEMA", "O reloader do Dash está ativo. O worker não será iniciado neste processo.", level="WARN")

if __name__ == '__main__':
    # data_source.setup_disk_paths() # REMOVIDO: Inicializado globalmente
    data_source.adicionar_log("SISTEMA", f"Iniciando o servidor Dash em http://127.0.0.1:8050/")
    app.run(debug=True, host='127.0.0.1', port=8050, use_reloader=False)