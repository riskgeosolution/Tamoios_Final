# index.py (CORREÇÃO FINAL v19 - Proteção contra overwrite no KM 72)

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
import sqlite3
import sys

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

data_source.setup_disk_paths()
data_source.initialize_database()


# --- NOVA FUNÇÃO DE SEGURANÇA ---
def get_first_valid(series):
    """
    Retorna o primeiro valor VÁLIDO (não nulo).
    Isso impede que um NaN novo apague um número antigo no banco.
    """
    # Remove valores nulos da série e pega o primeiro que sobrar
    valid_values = series.dropna()
    if not valid_values.empty:
        return valid_values.iloc[0]
    return None


# --------------------------------


# --- FUNÇÕES DO WORKER ---

def worker_verificar_alertas(status_novos, status_antigos):
    if not status_novos: return status_antigos
    if not isinstance(status_antigos, dict): status_antigos = {}
    status_atualizado = status_antigos.copy()
    for id_ponto in PONTOS_DE_ANALISE.keys():
        status_novo_ponto = status_novos.get(id_ponto, {"chuva": "SEM DADOS", "umidade": "SEM DADOS"})
        status_antigo_ponto = status_antigos.get(id_ponto, {"chuva": "INDEFINIDO", "umidade": "INDEFINIDO"})
        if not isinstance(status_antigo_ponto, dict): status_antigo_ponto = {"chuva": "INDEFINIDO",
                                                                             "umidade": "INDEFINIDO"}

        if status_novo_ponto.get("chuva") != status_antigo_ponto.get("chuva"):
            nome_ponto = PONTOS_DE_ANALISE[id_ponto]['nome']
            msg = f"MUDANÇA DE STATUS (Chuva): {nome_ponto} de {status_antigo_ponto.get('chuva')} para {status_novo_ponto.get('chuva')}."
            data_source.adicionar_log(id_ponto, msg, level="WARN", salvar_arquivo=True)

        if id_ponto == ID_PONTO_ZENTRA_KM72:
            if status_novo_ponto.get("umidade") != status_antigo_ponto.get("umidade"):
                nome_ponto = PONTOS_DE_ANALISE[id_ponto]['nome']
                msg = f"MUDANÇA DE STATUS (Umidade): {nome_ponto} de {status_antigo_ponto.get('umidade')} para {status_novo_ponto.get('umidade')}."
                data_source.adicionar_log(id_ponto, msg, level="WARN", salvar_arquivo=True)

        status_atualizado[id_ponto] = status_novo_ponto
    return status_atualizado


def worker_main_loop(memoria_worker):
    inicio_ciclo = time.time()
    try:
        data_source.adicionar_log("WORKER", "Início do ciclo de processamento.", salvar_arquivo=False)

        # 1. Busca histórico para garantir base de dados
        historico_recente_df = data_source.get_recent_data_for_worker(hours=75)
        status_antigos_do_disco = data_source.get_status_from_disk()

        # 2. Coleta Novos Dados
        novos_dados_chuva_df, novos_acumulados_chuva = data_source.fetch_data_from_weatherlink_api()  # Removido argumento extra se n houver suporte
        # Se sua função fetch aceita memoria, mantenha. Se não, use a linha acima.
        # Assumindo padrão do data_source.py atual:
        # novos_dados_chuva_df, _, _ = data_source.fetch_data_from_weatherlink_api()

        df_umidade_incremental = data_source.fetch_data_from_zentra_cloud()

        # 3. Merge com Proteção
        df_combinado = pd.concat([novos_dados_chuva_df, df_umidade_incremental, historico_recente_df],
                                 ignore_index=True)
        df_combinado['timestamp'] = pd.to_datetime(df_combinado['timestamp'], errors='coerce')
        df_combinado.dropna(subset=['timestamp'], inplace=True)

        numeric_cols = ['chuva_mm', 'precipitacao_acumulada_mm', 'umidade_1m_perc', 'umidade_2m_perc',
                        'umidade_3m_perc']
        for col in numeric_cols:
            if col in df_combinado.columns:
                df_combinado[col] = pd.to_numeric(df_combinado[col], errors='coerce')

        # AQUI APLICA A PROTEÇÃO: Usa get_first_valid em vez de 'first'
        agg_funcs = {col: get_first_valid for col in df_combinado.columns if col not in ['timestamp', 'id_ponto']}
        df_final = df_combinado.groupby(['timestamp', 'id_ponto'], as_index=False).agg(agg_funcs)

        data_source.save_to_sqlite(df_final)  # Usando save_to_sqlite (compativel com seu data_source atual)

        historico_para_calculo = df_final.sort_values('timestamp').reset_index(drop=True)
        status_atualizado = {}

        if not historico_para_calculo.empty:
            for id_ponto in PONTOS_DE_ANALISE.keys():
                df_ponto = historico_para_calculo[historico_para_calculo['id_ponto'] == id_ponto].copy()
                ponto_info = {"chuva": "SEM DADOS", "umidade": "SEM DADOS", "chuva_72h": 0.0, "umidade_1m": None,
                              "umidade_2m": None, "umidade_3m": None, "timestamp_local": None}

                if df_ponto.empty:
                    status_atualizado[id_ponto] = ponto_info
                    continue

                # Chuva
                acumulado_72h = processamento.calcular_acumulado_rolling(df_ponto, horas=72)
                chuva_72h_final = acumulado_72h['chuva_mm'].iloc[-1] if not acumulado_72h.empty else 0.0
                ponto_info['chuva_72h'] = round(chuva_72h_final, 1) if pd.notna(chuva_72h_final) else 0.0
                status_chuva, _ = processamento.definir_status_chuva(ponto_info['chuva_72h'])
                ponto_info['chuva'] = status_chuva

                # Umidade (Apenas KM 72)
                if id_ponto == ID_PONTO_ZENTRA_KM72:
                    cols_umidade = ['umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc']
                    df_com_umidade = df_ponto.dropna(subset=cols_umidade, how='all')

                    if not df_com_umidade.empty:
                        ultima_leitura_valida = df_com_umidade.sort_values('timestamp').iloc[-1]
                        ts_leitura = ultima_leitura_valida['timestamp']
                        agora = pd.Timestamp.now(tz='UTC')
                        if (agora - ts_leitura) < pd.Timedelta(hours=3):
                            ponto_info['umidade_1m'] = round(ultima_leitura_valida.get('umidade_1m_perc'),
                                                             1) if pd.notna(
                                ultima_leitura_valida.get('umidade_1m_perc')) else None
                            ponto_info['umidade_2m'] = round(ultima_leitura_valida.get('umidade_2m_perc'),
                                                             1) if pd.notna(
                                ultima_leitura_valida.get('umidade_2m_perc')) else None
                            ponto_info['umidade_3m'] = round(ultima_leitura_valida.get('umidade_3m_perc'),
                                                             1) if pd.notna(
                                ultima_leitura_valida.get('umidade_3m_perc')) else None
                            ponto_info['timestamp_local'] = ts_leitura.tz_convert('America/Sao_Paulo').isoformat()

                            bases = {f'base_{d}m': df_com_umidade[f'umidade_{d}m_perc'].min() for d in [1, 2, 3]}
                            for d in [1, 2, 3]:
                                if pd.isna(bases[f'base_{d}m']): bases[f'base_{d}m'] = CONSTANTES_PADRAO[
                                    f'UMIDADE_BASE_{d}M']

                            status_umidade_tuple = processamento.definir_status_umidade_hierarquico(
                                ponto_info['umidade_1m'], ponto_info['umidade_2m'], ponto_info['umidade_3m'], **bases)
                            ponto_info['umidade'] = status_umidade_tuple[0] if status_umidade_tuple[
                                                                                   0] != "SEM DADOS" else "LIVRE"
                        else:
                            ponto_info['umidade'] = "SEM DADOS"
                    else:
                        ponto_info['umidade'] = "SEM DADOS"
                else:
                    ponto_info['umidade'] = "SEM DADOS"

                status_atualizado[id_ponto] = ponto_info

        status_final_completo = worker_verificar_alertas(status_atualizado, status_antigos_do_disco)
        data_source.write_with_timeout(data_source.STATUS_FILE, status_final_completo, timeout=20)
        data_source.adicionar_log("WORKER", f"Ciclo concluído em {time.time() - inicio_ciclo:.2f}s.",
                                  salvar_arquivo=False)
        return True, memoria_worker
    except Exception as e:
        data_source.adicionar_log("WORKER", f"ERRO CRÍTICO NO CICLO: {e}", level="ERROR")
        traceback.print_exc()
        return False, memoria_worker


def background_task_wrapper():
    data_source.adicionar_log("SISTEMA", "Processo Worker (Thread) iniciado.", salvar_arquivo=False)
    time.sleep(RENDER_SLEEP_TIME_SEC)
    memoria_worker = {}
    INTERVALO_EM_SEGUNDOS = config.FREQUENCIA_API_SEGUNDOS
    while True:
        sucesso, memoria_worker = worker_main_loop(memoria_worker)
        if not sucesso:
            data_source.adicionar_log("SISTEMA", f"Ciclo do worker falhou. Reiniciando em {RENDER_SLEEP_TIME_SEC}s.",
                                      level="ERROR")
            time.sleep(RENDER_SLEEP_TIME_SEC)
            continue
        data_source.adicionar_log("WORKER", f"Dormindo por {INTERVALO_EM_SEGUNDOS}s...", salvar_arquivo=False)
        time.sleep(INTERVALO_EM_SEGUNDOS)


# --- APP E CALLBACKS ---

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
    if session_data and session_data.get('logged_in'): return main_app_page.get_layout()
    return login_page.get_layout()


@app.callback(Output('page-content', 'children'), [Input('url-raiz', 'pathname'), Input('session-store', 'data')])
def display_page_content(pathname, session_data):
    if not (session_data and session_data.get('logged_in')): raise PreventUpdate
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


@app.callback([Output('session-store', 'data', allow_duplicate=True), Output('url-raiz', 'pathname')],
              [Input('logout-button', 'n_clicks')], prevent_initial_call=True)
def logout_callback(n_clicks):
    if not n_clicks: raise PreventUpdate
    return {'logged_in': False, 'user_type': 'guest'}, '/'


@app.callback(Output('intervalo-atualizacao-dados', 'disabled'), [Input('session-store', 'data')])
def toggle_interval_update(session_data):
    return not (session_data and session_data.get('logged_in'))


@app.callback([Output('store-ultimo-status', 'data'), Output('store-logs-sessao', 'data')],
              [Input('intervalo-atualizacao-dados', 'n_intervals')])
def update_status_and_logs_from_disk(n_intervals):
    status = data_source.get_status_from_disk()
    logs = data_source.ler_logs_eventos("GERAL")
    return status, logs


def iniciar_worker_automatico():
    if not os.environ.get("WERKZEUG_MAIN"):
        data_source.adicionar_log("SISTEMA", "Inicializando Worker em Thread (Global Scope).", salvar_arquivo=False)
        t = Thread(target=background_task_wrapper, daemon=True)
        t.start()


iniciar_worker_automatico()

if __name__ == '__main__':
    args = sys.argv
    if len(args) > 1 and args[1].lower() == 'backfill':
        if len(args) != 4:
            print("Uso: python index.py backfill <ID> <DIAS>")
        else:
            id_ponto = args[2]
            try:
                dias = int(args[3])
                # data_source.backfill_weatherlink_data_manually(id_ponto, dias)
                print("Backfill manual")
            except Exception as e:
                print(f"Erro backfill: {e}")
    else:
        data_source.adicionar_log("SISTEMA", "Iniciando servidor Dash Localmente...", salvar_arquivo=False)
        app.run(debug=True, host='127.0.0.1', port=8050, use_reloader=False)