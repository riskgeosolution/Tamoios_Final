# index.py (CORREÇÃO FINAL v8 - Persistência de Memória do Worker)

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

# --- Inicialização centralizada do banco de dados ---
data_source.setup_disk_paths()
data_source.initialize_database()

# ==============================================================================
# --- LÓGICA DO WORKER (MODIFICADA PARA STATUS SEPARADOS) ---
# ==============================================================================

def worker_verificar_alertas(status_novos, status_antigos):
    """ Compara os status de chuva e umidade de forma independente. """
    if not status_novos: return status_antigos
    if not isinstance(status_antigos, dict):
        status_antigos = {}

    status_atualizado = status_antigos.copy()
    for id_ponto in PONTOS_DE_ANALISE.keys():
        status_novo_ponto = status_novos.get(id_ponto, {"chuva": "SEM DADOS", "umidade": "SEM DADOS"})
        status_antigo_ponto = status_antigos.get(id_ponto, {"chuva": "INDEFINIDO", "umidade": "INDEFINIDO"})

        if not isinstance(status_antigo_ponto, dict):
            status_antigo_ponto = {"chuva": "INDEFINIDO", "umidade": "INDEFINIDO"}

        if status_novo_ponto.get("chuva") != status_antigo_ponto.get("chuva"):
            nome_ponto = PONTOS_DE_ANALISE[id_ponto]['nome']
            msg = f"MUDANÇA DE STATUS (Chuva): {nome_ponto} de {status_antigo_ponto.get('chuva')} para {status_novo_ponto.get('chuva')}."
            data_source.adicionar_log(id_ponto, msg, level="WARN")

        if status_novo_ponto.get("umidade") != status_antigo_ponto.get("umidade"):
            nome_ponto = PONTOS_DE_ANALISE[id_ponto]['nome']
            msg = f"MUDANÇA DE STATUS (Umidade): {nome_ponto} de {status_antigo_ponto.get('umidade')} para {status_novo_ponto.get('umidade')}."
            data_source.adicionar_log(id_ponto, msg, level="WARN")

        status_atualizado[id_ponto] = status_novo_ponto
        
    return status_atualizado


def worker_main_loop(memoria_worker):
    inicio_ciclo = time.time()
    
    try:
        data_source.adicionar_log("WORKER", "Início do ciclo de processamento.")
        
        historico_recente_df = data_source.get_recent_data_for_worker(hours=75)
        status_antigos_do_disco = data_source.get_status_from_disk()
        
        novos_dados_chuva_df, novos_acumulados_chuva = data_source.fetch_data_from_weatherlink_api(
            memoria_worker.get('ultimo_acumulado_chuva', {})
        )
        # Atualiza a memória do worker com os valores mais recentes
        memoria_worker['ultimo_acumulado_chuva'] = novos_acumulados_chuva

        df_umidade_incremental = data_source.fetch_data_from_zentra_cloud()

        df_combinado = pd.concat([novos_dados_chuva_df, df_umidade_incremental, historico_recente_df], ignore_index=True)

        numeric_cols = ['chuva_mm', 'precipitacao_acumulada_mm', 'umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc']
        for col in numeric_cols:
            if col in df_combinado.columns:
                df_combinado[col] = pd.to_numeric(df_combinado[col], errors='coerce')

        agg_funcs = {col: 'first' for col in df_combinado.columns if col not in ['timestamp', 'id_ponto']}
        df_mesclado = df_combinado.groupby(['timestamp', 'id_ponto'], as_index=False).agg(agg_funcs)
        
        data_source.upsert_data(df_mesclado)
        historico_para_calculo = df_mesclado.sort_values('timestamp').reset_index(drop=True)

        status_atualizado = {}
        if not historico_para_calculo.empty:
            for id_ponto in PONTOS_DE_ANALISE.keys():
                df_ponto = historico_para_calculo[historico_para_calculo['id_ponto'] == id_ponto].copy()
                
                ponto_info = {
                    "chuva": "SEM DADOS", "umidade": "SEM DADOS", "chuva_72h": 0.0,
                    "umidade_1m": None, "umidade_2m": None, "umidade_3m": None, "timestamp_local": None
                }

                if df_ponto.empty:
                    status_atualizado[id_ponto] = ponto_info
                    continue

                acumulado_72h = processamento.calcular_acumulado_rolling(df_ponto, horas=72)
                chuva_72h_final = acumulado_72h['chuva_mm'].iloc[-1] if not acumulado_72h.empty else 0.0
                ponto_info['chuva_72h'] = round(chuva_72h_final, 1) if pd.notna(chuva_72h_final) else 0.0
                status_chuva, _ = processamento.definir_status_chuva(ponto_info['chuva_72h'])
                ponto_info['chuva'] = status_chuva

                ultimo_dado = df_ponto.sort_values('timestamp').iloc[-1]
                ponto_info['umidade_1m'] = round(ultimo_dado.get('umidade_1m_perc'), 1) if pd.notna(ultimo_dado.get('umidade_1m_perc')) else None
                ponto_info['umidade_2m'] = round(ultimo_dado.get('umidade_2m_perc'), 1) if pd.notna(ultimo_dado.get('umidade_2m_perc')) else None
                ponto_info['umidade_3m'] = round(ultimo_dado.get('umidade_3m_perc'), 1) if pd.notna(ultimo_dado.get('umidade_3m_perc')) else None
                
                if pd.notna(ultimo_dado.get('timestamp')):
                    ponto_info['timestamp_local'] = pd.to_datetime(ultimo_dado.get('timestamp')).tz_convert('America/Sao_Paulo').isoformat()

                df_umidade_hist = df_ponto.dropna(subset=['umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc'])
                bases = {f'base_{d}m': df_umidade_hist[f'umidade_{d}m_perc'].min() if not df_umidade_hist.empty else CONSTANTES_PADRAO[f'UMIDADE_BASE_{d}M'] for d in [1, 2, 3]}
                
                status_umidade_tuple = processamento.definir_status_umidade_hierarquico(ponto_info['umidade_1m'], ponto_info['umidade_2m'], ponto_info['umidade_3m'], **bases)
                status_umidade = status_umidade_tuple[0] if status_umidade_tuple[0] != "SEM DADOS" else "LIVRE"
                ponto_info['umidade'] = status_umidade
                
                status_atualizado[id_ponto] = ponto_info
        
        status_final_completo = worker_verificar_alertas(status_atualizado, status_antigos_do_disco)
        data_source.write_with_timeout(data_source.STATUS_FILE, status_final_completo, timeout=20)
        
        data_source.adicionar_log("WORKER", f"Ciclo concluído em {time.time() - inicio_ciclo:.2f}s.")
        return True, memoria_worker
    except Exception as e:
        data_source.adicionar_log("WORKER", f"ERRO CRÍTICO NO CICLO: {e}", level="ERROR")
        traceback.print_exc()
        return False, memoria_worker


def background_task_wrapper():
    data_source.adicionar_log("SISTEMA", "Processo Worker (Thread) iniciado.")
    time.sleep(RENDER_SLEEP_TIME_SEC)
    
    # --- LÓGICA DE "PRÉ-AQUECIMENTO" DA MEMÓRIA ---
    memoria_worker = {}
    try:
        data_source.adicionar_log("WORKER", "Pré-aquecendo memória de acumulados de chuva...")
        df_historico_completo = data_source.read_data_from_sqlite(last_hours=75)
        if not df_historico_completo.empty:
            ultimo_acumulado = {}
            for id_ponto in PONTOS_DE_ANALISE.keys():
                df_ponto = df_historico_completo[df_historico_completo['id_ponto'] == id_ponto]
                if not df_ponto.empty:
                    ultimo_registro = df_ponto.sort_values('timestamp').iloc[-1]
                    if pd.notna(ultimo_registro.get('precipitacao_acumulada_mm')):
                        ultimo_acumulado[id_ponto] = ultimo_registro['precipitacao_acumulada_mm']
            memoria_worker['ultimo_acumulado_chuva'] = ultimo_acumulado
            data_source.adicionar_log("WORKER", f"Memória pré-aquecida: {ultimo_acumulado}")
    except Exception as e:
        data_source.adicionar_log("WORKER", f"Falha ao pré-aquecer memória: {e}", level="ERROR")
    # --- FIM DA LÓGICA DE "PRÉ-AQUECIMENTO" ---

    INTERVALO_EM_MINUTOS = 10
    CARENCIA_EM_SEGUNDOS = 60
    while True:
        sucesso, memoria_worker = worker_main_loop(memoria_worker)
        if not sucesso:
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
    [Output('store-ultimo-status', 'data'), Output('store-logs-sessao', 'data')],
    [Input('intervalo-atualizacao-dados', 'n_intervals')]
)
def update_status_and_logs_from_disk(n_intervals):
    status = data_source.get_status_from_disk()
    logs = data_source.ler_logs_eventos("GERAL")
    return status, logs

# ==============================================================================
# --- EXECUÇÃO ---
# ==============================================================================

if not os.environ.get("WERKZEUG_MAIN"):
    data_source.adicionar_log("SISTEMA", "Iniciando o worker (coletor de dados) em um thread separado.")
    Thread(target=background_task_wrapper, daemon=True).start()
else:
    data_source.adicionar_log("SISTEMA", "O reloader do Dash está ativo. O worker não será iniciado neste processo.", level="WARN")

if __name__ == '__main__':
    data_source.adicionar_log("SISTEMA", f"Iniciando o servidor Dash em http://127.0.0.1:8050/")
    app.run(debug=True, host='127.0.0.1', port=8050, use_reloader=False)
