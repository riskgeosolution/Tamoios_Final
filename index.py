# index.py (CORREÇÃO DE ESCOPO E INICIALIZAÇÃO)

import dash
from dash import html, dcc, callback, Input, Output, State
import dash_bootstrap_components as dbc
import pandas as pd
from io import StringIO
import os
import json

# --- IMPORTAÇÃO CRÍTICA DO APP ---
from app import app, server
# --- FIM DA IMPORTAÇÃO CRÍTICA ---

from pages import login as login_page
from pages import main_app as main_app_page
from pages import map_view, general_dash, specific_dash
import data_source
import config

# --- VARIÁVEIS DE AUTENTICAÇÃO (CONSTANTES DO MÓDULO) ---
SENHA_CLIENTE = '123'
SENHA_ADMIN = 'admin456'


# --- FIM DA DEFINIÇÃO ---


# ---------------------------------------------------


# --- FUNÇÃO HELPER (Para o Roteador de Conteúdo) ---
def get_content_page(pathname):
    """ Roteador para o conteúdo principal do app (pós-login). """
    if pathname.startswith('/ponto/'):
        return specific_dash.get_layout()
    elif pathname == '/dashboard-geral':
        return general_dash.get_layout()
    else:
        return map_view.get_layout()


# --- LAYOUT PRINCIPAL DA APLICAÇÃO (A RAIZ) ---
app.layout = html.Div([
    dcc.Store(id='session-store', data={'logged_in': False, 'user_type': 'guest'}, storage_type='session'),
    dcc.Store(id='store-dados-sessao', storage_type='session'),
    dcc.Store(id='store-ultimo-status', storage_type='session'),
    dcc.Store(id='store-logs-sessao', storage_type='session'),

    dcc.Location(id='url-raiz', refresh=False),

    # Intervalo de Atualização de Dados (Começa DESABILITADO)
    dcc.Interval(
        id='intervalo-atualizacao-dados',
        interval=10 * 1000,
        n_intervals=0,
        disabled=True
    ),

    html.Div(id='page-container-root')
])


# ==============================================================================
# --- CALLBACKS DE AUTENTICAÇÃO E ROTEAMENTO (index.py) ---
# ==============================================================================

# Callback 1: Roteador Raiz (Decide se mostra Login ou App)
@app.callback(
    Output('page-container-root', 'children'),
    Input('session-store', 'data'),
    Input('url-raiz', 'pathname')
)
def display_page_root(session_data, pathname):
    if session_data and session_data.get('logged_in', False):
        return main_app_page.get_layout()
    else:
        return login_page.get_layout()


# Callback 2: Roteador de Conteúdo (Preenche 'page-content')
@app.callback(
    Output('page-content', 'children'),
    [Input('url-raiz', 'pathname'),
     Input('session-store', 'data')]
)
def display_page_content(pathname, session_data):
    if not session_data.get('logged_in', False):
        return html.Div()

    return get_content_page(pathname)


# Callback 3: Lógica de Login (Botão e Enter)
@app.callback(
    [Output('session-store', 'data'),
     Output('login-error-output', 'children'),
     Output('login-error-output', 'className'),
     Output('input-password', 'value')],
    [Input('btn-login', 'n_clicks'),
     Input('input-password', 'n_submit')],
    State('input-password', 'value'),
    prevent_initial_call=True
)
def login_callback(n_clicks, n_submit, password):
    # Usamos globals() para forçar o reconhecimento das variáveis globais
    global SENHA_ADMIN, SENHA_CLIENTE

    if not n_clicks and not n_submit:
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update
    if not password:
        return dash.no_update, "Por favor, digite a senha.", "text-danger mb-3 text-center", ""

    password = password.strip()

    if password == SENHA_ADMIN:
        new_session = {'logged_in': True, 'user_type': 'admin'}
        return new_session, "", "text-success mb-3 text-center", ""
    elif password == SENHA_CLIENTE:
        new_session = {'logged_in': True, 'user_type': 'client'}
        return new_session, "", "text-success mb-3 text-center", ""
    else:
        return dash.no_update, "Senha incorreta. Tente novamente.", "text-danger mb-3 text-center", ""


# Callback 4: Lógica de Logout (Botão "Sair" na barra)
@app.callback(
    [Output('session-store', 'data', allow_duplicate=True),
     Output('url-raiz', 'pathname')],
    Input('logout-button', 'n_clicks'),
    prevent_initial_call=True
)
def logout_callback(n_clicks):
    if n_clicks is None or n_clicks == 0:
        return dash.no_update, dash.no_update
    return {'logged_in': False, 'user_type': 'guest'}, '/'


# Callback 5: Ligar/Desligar o Intervalo de Dados
@app.callback(
    Output('intervalo-atualizacao-dados', 'disabled'),
    Input('session-store', 'data')
)
def toggle_interval_update(session_data):
    is_logged_in = session_data and session_data.get('logged_in', False)
    return not is_logged_in


# Callback 6: Atualiza Stores de Dados e Logs (Lê o disco)
@app.callback(
    [Output('store-dados-sessao', 'data'),
     Output('store-ultimo-status', 'data'),
     Output('store-logs-sessao', 'data')],
    Input('intervalo-atualizacao-dados', 'n_intervals')
)
def update_data_and_logs_from_disk(n_intervals):
    # print(f"[Web] Atualização (Intervalo {n_intervals}): Lendo dados do disco...")
    df_completo, status_atual, logs = data_source.get_all_data_from_disk()
    dados_json_output = df_completo.to_json(date_format='iso', orient='split')
    # print(f"[Web DEBUG] Histórico lido: {len(df_completo)} linhas.")
    # print(f"[Web DEBUG] Status lido. Ex: Ponto A = {status_atual.get('Ponto-A-KM67', 'N/A')}")
    return dados_json_output, status_atual, logs


# ==============================================================================
# --- SEÇÃO DE EXECUÇÃO LOCAL (Inicia o servidor Flask/Dash) ---
# ==============================================================================
if __name__ == '__main__':
    host = '127.0.0.1'
    port = 8050
    print("Inicializando servidor Dash...")

    data_source.setup_disk_paths()

    print(f"\nAVISO: O worker.py NÃO está rodando neste modo.")
    print("Execute 'python worker.py' em outro terminal para simular o ambiente Render.\n")

    try:
        app.run(debug=True, host=host, port=port)
    except Exception as e:
        print(f"ERRO CRÍTICO NA EXECUÇÃO DO APP.RUN: {e}")