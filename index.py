# index.py (CORREÇÃO FINAL DE LOGIN E PINOS)
# Arquivo principal que gerencia o roteamento e a sessão de login.

import dash
from dash import html, dcc, callback, Input, Output, State
import dash_bootstrap_components as dbc
import pandas as pd
from io import StringIO
import os
import json

# --- IMPORTAÇÕES ESSENCIAIS ---
from app import app, server  # app.py deve ter suppress_callback_exceptions=True
from pages import login as login_page  # Página de login
from pages import main_app as main_app_page  # Layout principal pós-login
from pages import map_view, general_dash, specific_dash  # Páginas de conteúdo
import data_source
import config  # Contém as constantes e o mapa de riscos

# --- VARIÁVEIS DE AUTENTICAÇÃO (MUDE ESTAS SENHAS) ---
SENHA_CLIENTE = 'cliente123'
SENHA_ADMIN = 'admin456'


# ---------------------------------------------------


# --- FUNÇÃO HELPER (Para o Roteador de Conteúdo) ---
def get_content_page(pathname):
    """ Roteador para o conteúdo principal do app (pós-login). """
    if pathname.startswith('/ponto/'):
        return specific_dash.get_layout()
    elif pathname == '/dashboard-geral':
        return general_dash.get_layout()
    else:
        # Default para a página do mapa
        return map_view.get_layout()


# --- LAYOUT PRINCIPAL DA APLICAÇÃO (A RAIZ) ---
# Contém os Stores de dados e a URL/Sessão (que devem existir SEMPRE)
app.layout = html.Div([
    # Store de Sessão (Guarda o estado logado e o tipo de usuário)
    dcc.Store(id='session-store', data={'logged_in': False, 'user_type': 'guest'}, storage_type='session'),

    # Stores de Dados (Atualizados pelo dcc.Interval, lidos pelo worker.py)
    dcc.Store(id='store-dados-sessao', storage_type='session'),
    dcc.Store(id='store-ultimo-status', storage_type='session'),
    dcc.Store(id='store-logs-sessao', storage_type='session'),

    # Componente de URL (Necessário para roteamento Dash)
    dcc.Location(id='url-raiz', refresh=False),

    # Intervalo de Atualização de Dados (Começa DESABILITADO)
    dcc.Interval(
        id='intervalo-atualizacao-dados',
        interval=10 * 1000,  # 10 segundos para o painel (o worker usa 15 minutos)
        n_intervals=0,
        disabled=True  # Começa desabilitado para evitar o bug de login
    ),

    # Container da Página (Login ou App)
    html.Div(id='page-container-root')
])


# ==============================================================================
# --- CALLBACKS DE AUTENTICAÇÃO E ROTEAMENTO (index.py) ---
# ==============================================================================

# Callback 1: Roteador Raiz (Decide se mostra Login ou App)
@app.callback(
    Output('page-container-root', 'children'),
    Input('session-store', 'data'),
    Input('url-raiz', 'pathname')  # Input é necessário, mas usamos o store
)
def display_page_root(session_data, pathname):
    """ Exibe a tela de Login ou o Layout Principal do App. """
    if session_data and session_data.get('logged_in', False):
        # Usuário logado: Carrega o layout principal (barra de navegação)
        return main_app_page.get_layout(session_data.get('user_type', 'client'))
    else:
        # Usuário não logado: Força a página de login
        return login_page.get_layout()


# Callback 2: Lógica de Login (Botão e Enter)
@app.callback(
    [Output('session-store', 'data'),
     Output('login-alert', 'children'),
     Output('login-alert', 'is_open'),
     Output('login-password-input', 'value')],
    [Input('btn-login', 'n_clicks'),
     Input('login-password-input', 'n_submit')],
    State('login-password-input', 'value'),
    prevent_initial_call=True
)
def login_callback(n_clicks, n_submit, password):
    """ Verifica a senha, salva a sessão e exibe mensagens de erro/sucesso. """

    # 1. Trava de segurança: Se a página recarregar e n_clicks=None, sai
    if not n_clicks and not n_submit:
        return dash.no_update, "", False, ""

    # 2. Trava de segurança: Se a senha é None ou vazia (e não é inicialização)
    if not password:
        return dash.no_update, "Por favor, digite a senha.", True, ""

    # 3. Trava de segurança: Se a senha está correta
    password = password.strip()

    # print(f"[Login Attempt] Senha recebida: '{password}' (Tipo: {type(password)})")

    if password == SENHA_ADMIN:
        new_session = {'logged_in': True, 'user_type': 'admin'}
        return new_session, "Login de Administrador bem-sucedido!", False, ""
    elif password == SENHA_CLIENTE:
        new_session = {'logged_in': True, 'user_type': 'client'}
        return new_session, "Login bem-sucedido!", False, ""
    else:
        # Senha incorreta
        return dash.no_update, "Senha incorreta. Tente novamente.", True, ""


# Callback 3: Lógica de Logout (Botão "Sair" na barra)
@app.callback(
    [Output('session-store', 'data', allow_duplicate=True),
     Output('url-raiz', 'pathname')],  # Redireciona para '/' após logout
    Input('logout-button', 'n_clicks'),
    prevent_initial_call=True
)
def logout_callback(n_clicks):
    """ Limpa a sessão e redireciona para a tela de login. """
    # Trava para garantir que o callback só rode se houver clique (n_clicks > 0)
    if n_clicks is None or n_clicks == 0:
        return dash.no_update, dash.no_update

    # Limpa a sessão
    return {'logged_in': False, 'user_type': 'guest'}, '/'


# ==============================================================================
# --- CALLBACKS DE DADOS (Chamados a cada 10s APÓS o login) ---
# ==============================================================================

# Callback 4: Ligar/Desligar o Intervalo de Dados (Chave da Aplicação)
@app.callback(
    Output('intervalo-atualizacao-dados', 'disabled'),
    Input('session-store', 'data')
)
def toggle_interval_update(session_data):
    """ Liga o dcc.Interval (worker) quando o usuário faz login. """
    is_logged_in = session_data and session_data.get('logged_in', False)

    # Log para depuração
    auth_status = "logado" if is_logged_in else "deslogado"
    action = "Habilitando" if is_logged_in else "Desabilitando"

    # print(f"[Auth] Usuário {auth_status}. {action} atualização de dados.")

    # O Intervalo é 'disabled=True' quando está desligado.
    # Queremos que ele seja 'disabled=False' quando logado.
    return not is_logged_in


# Callback 5: Atualiza Stores de Dados e Logs (Lê o disco)
@app.callback(
    [Output('store-dados-sessao', 'data'),
     Output('store-ultimo-status', 'data'),
     Output('store-logs-sessao', 'data')],
    Input('intervalo-atualizacao-dados', 'n_intervals')
)
def update_data_and_logs_from_disk(n_intervals):
    """ Busca os arquivos JSON/Logs salvos pelo worker.py no disco persistente. """

    # print(f"[Web] Atualização (Intervalo {n_intervals}): Lendo dados do disco...")

    # A função retorna df_completo, status_atual, logs
    df_completo, status_atual, logs = data_source.get_all_data_from_disk()

    # Transforma o DataFrame em JSON para ser salvo no Store
    # A exceção é tratada no data_source.py, então não deve ser vazia.
    dados_json_output = df_completo.to_json(date_format='iso', orient='split')

    # Log: print("[Web] Dados e logs lidos. Atualizando stores.")

    return dados_json_output, status_atual, logs


# ==============================================================================
# --- SEÇÃO DE EXECUÇÃO LOCAL (Inicia o servidor Flask/Dash) ---
# ==============================================================================
if __name__ == '__main__':
    host = '127.0.0.1'
    port = 8050
    print("Inicializando servidor Dash...")

    # Garante que os caminhos do disco sejam definidos e impressos
    data_source.setup_disk_paths()

    # Aviso ao usuário para rodar o worker em outro terminal
    print(f"\nAVISO: O worker.py NÃO está rodando neste modo.")
    print("Execute 'python worker.py' em outro terminal para simular o ambiente Render.\n")

    # A ATENÇÃO: debug=True é essencial para o Dash, e use_reloader=False
    # Não podemos usar use_reloader=False aqui porque precisamos do hot-reloading do Dash,
    # mas o ambiente de produção (Render) usa Gunicorn/worker.
    app.run(debug=True, host=host, port=port)
