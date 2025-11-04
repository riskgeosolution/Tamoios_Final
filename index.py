# index.py (CORRIGIDO v4: Importando 'datetime')

import dash
from dash import html, dcc, callback, Input, Output, State
import dash_bootstrap_components as dbc
import pandas as pd
from io import StringIO
import os
import json
from dotenv import load_dotenv
import time
from threading import Thread
import datetime  # <-- ESTA É A LINHA QUE FALTAVA

# Carrega as variáveis do .env file
load_dotenv()

# --- IMPORTAÇÃO CRÍTICA DO APP ---
# Agora importamos o 'app' e o 'server' do app.py
from app import app, server
# --- FIM DA IMPORTAÇÃO CRÍTICA ---

from pages import login as login_page
from pages import main_app as main_app_page
from pages import map_view, general_dash, specific_dash
import data_source
import config

# --- IMPORTAÇÕES DO WORKER ---
import processamento
import alertas
import traceback
from config import PONTOS_DE_ANALISE, RISCO_MAP, FREQUENCIA_API_SEGUNDOS

# --- FIM DAS IMPORTAÇÕES DO WORKER ---


# --- VARIÁVEIS DE AUTENTICAÇÃO ---
SENHA_CLIENTE = '123'
SENHA_ADMIN = 'admin456'


# ==============================================================================
# --- LÓGICA DO WORKER (O COLETOR DE DADOS EM SEGUNDO PLANO) ---
# (As funções worker_verificar_alertas, worker_main_loop, e
# background_task_wrapper são mantidas exatamente como antes)
# ==============================================================================

def worker_verificar_alertas(status_novos, status_antigos):
    """ (Copiado do worker.py) Compara status e loga mudanças. """
    if not status_novos:
        print("[Worker Thread] Nenhum status novo recebido para verificação.")
        return status_antigos
    if not isinstance(status_antigos, dict):
        status_antigos = {pid: "INDEFINIDO" for pid in PONTOS_DE_ANALISE.keys()}

    status_atualizado = status_antigos.copy()

    for id_ponto in PONTOS_DE_ANALISE.keys():
        status_novo = status_novos.get(id_ponto, "SEM DADOS")
        status_antigo = status_antigos.get(id_ponto, "INDEFINIDO")

        if status_novo != status_antigo:
            try:
                nome_ponto = PONTOS_DE_ANALISE[id_ponto]['nome']
                mensagem_log = f"MUDANÇA DE STATUS: {nome_ponto} mudou de {status_antigo} para {status_novo}."
                data_source.adicionar_log(id_ponto, mensagem_log)
                print(f"| {id_ponto} | {mensagem_log}")
            except Exception as e:
                print(f"Erro ao gerar log de mudança de status: {e}")

            # (alertas.enviar_alerta() iria aqui)
            status_atualizado[id_ponto] = status_novo

    return status_atualizado


def worker_main_loop():
    """ (Copiado do worker.py) O loop principal de coleta. """
    inicio_ciclo = time.time()
    try:
        historico_df, status_antigos_do_disco, logs = data_source.get_all_data_from_disk()
        if not status_antigos_do_disco:
            status_antigos_do_disco = {p: "INDEFINIDO" for p in PONTOS_DE_ANALISE.keys()}

        print(f"WORKER (Thread): Início do ciclo. Histórico lido: {len(historico_df)} entradas.")

        if historico_df.empty or historico_df[historico_df['id_ponto'] == 'Ponto-A-KM67'].empty:
            print("[Worker Thread] Histórico do KM 67 (Pro) está vazio. Tentando backfill de 72h...")
            try:
                data_source.backfill_km67_pro_data(historico_df)
                historico_df, _, _ = data_source.get_all_data_from_disk()
                print(f"[Worker Thread] Backfill concluído. Histórico atual: {len(historico_df)} entradas.")
            except Exception as e_backfill:
                data_source.adicionar_log("GERAL", f"ERRO CRÍTICO (Backfill KM 67): {e_backfill}")

        novos_dados_df, status_novos_API = data_source.executar_passo_api_e_salvar(historico_df)
        historico_completo, _, _ = data_source.get_all_data_from_disk()

        if historico_completo.empty:
            print("AVISO (Thread): Histórico vazio, pulando cálculo de status.")
            status_atualizado = {p: "SEM DADOS" for p in PONTOS_DE_ANALISE.keys()}
        else:
            status_atualizado = {}
            for id_ponto in PONTOS_DE_ANALISE.keys():
                df_ponto = historico_completo[historico_completo['id_ponto'] == id_ponto].copy()
                acumulado_72h_df = processamento.calcular_acumulado_rolling(df_ponto, horas=72)
                if not acumulado_72h_df.empty:
                    chuva_72h_final = acumulado_72h_df['chuva_mm'].iloc[-1]
                    status_ponto, _ = processamento.definir_status_chuva(chuva_72h_final)
                    status_atualizado[id_ponto] = status_ponto
                else:
                    status_atualizado[id_ponto] = "SEM DADOS"

        status_final_com_alertas = worker_verificar_alertas(status_atualizado, status_antigos_do_disco)

        try:
            with open(data_source.STATUS_FILE, 'w', encoding='utf-8') as f:
                json.dump(status_final_com_alertas, f, indent=2)
        except Exception as e:
            print(f"ERRO CRÍTICO (Thread) ao salvar status: {e}")
            traceback.print_exc()
            data_source.adicionar_log("GERAL", f"ERRO CRÍTICO (Thread) ao salvar status: {e}")

        print(f"WORKER (Thread): Ciclo concluído em {time.time() - inicio_ciclo:.2f}s.")
        return True

    except Exception as e:
        print(f"WORKER ERRO CRÍTICO (Thread) no loop principal: {e}")
        traceback.print_exc()
        data_source.adicionar_log("GERAL", f"ERRO CRÍTICO (Thread loop): {e}")
        return False


def background_task_wrapper():
    """
    (Copiado do worker.py) O loop "inteligente" que sincroniza
    com o relógio UTC e roda o main_loop.
    """
    data_source.setup_disk_paths()
    print("--- Processo Worker (Thread) Iniciado (Modo Sincronizado) ---")
    data_source.adicionar_log("GERAL", "Processo Worker (Thread) iniciado com sucesso.")

    INTERVALO_EM_MINUTOS = 15
    CARENCIA_EM_SEGUNDOS = 60  # 1 minuto de "folga"

    while True:
        inicio_total = time.time()

        worker_main_loop()

        tempo_execucao = time.time() - inicio_total
        agora_utc = datetime.datetime.now(datetime.timezone.utc)

        proximo_minuto_base = (agora_utc.minute // INTERVALO_EM_MINUTOS + 1) * INTERVALO_EM_MINUTOS
        proxima_hora_utc = agora_utc

        if proximo_minuto_base >= 60:
            proxima_hora_utc = agora_utc + datetime.timedelta(hours=1)
            proxima_minuto_base = 0

        proxima_execucao_base_utc = proxima_hora_utc.replace(
            minute=proximo_minuto_base,
            second=0,
            microsecond=0
        )
        proxima_execucao_com_carencia_utc = proxima_execucao_base_utc + datetime.timedelta(seconds=CARENCIA_EM_SEGUNDOS)
        tempo_para_dormir_seg = (proxima_execucao_com_carencia_utc - agora_utc).total_seconds()

        if tempo_para_dormir_seg < 0:
            print(f"AVISO (Thread): O ciclo demorou {tempo_execucao:.1f}s e perdeu a janela. Rodando novamente...")
            tempo_para_dormir_seg = 1

        print(f"WORKER (Thread): Ciclo levou {tempo_execucao:.1f}s.")
        print(
            f"WORKER (Thread): Próxima execução às {proxima_execucao_com_carencia_utc.isoformat()}. Dormindo por {tempo_para_dormir_seg:.0f}s...")
        time.sleep(tempo_para_dormir_seg)


# ==============================================================================
# --- LAYOUT PRINCIPAL DA APLICAÇÃO (A RAIZ) ---
# ==============================================================================

# ESTA É A LINHA QUE CORRIGE O ERRO 'NoLayoutException'
app.layout = html.Div([
    dcc.Store(id='session-store', data={'logged_in': False, 'user_type': 'guest'}, storage_type='session'),
    dcc.Store(id='store-dados-sessao', storage_type='session'),
    dcc.Store(id='store-ultimo-status', storage_type='session'),
    dcc.Store(id='store-logs-sessao', storage_type='session'),
    dcc.Location(id='url-raiz', refresh=False),
    dcc.Interval(
        id='intervalo-atualizacao-dados',
        interval=10 * 1000,  # Continua 10s (o painel)
        n_intervals=0,
        disabled=True
    ),
    html.Div(id='page-container-root')
])


# ==============================================================================
# --- CALLBACKS DE AUTENTICAÇÃO E ROTEAMENTO ---
# ==============================================================================

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


@app.callback(
    Output('page-content', 'children'),
    [Input('url-raiz', 'pathname'),
     Input('session-store', 'data')]
)
def display_page_content(pathname, session_data):
    if not session_data.get('logged_in', False):
        return html.Div()

    if pathname.startswith('/ponto/'):
        return specific_dash.get_layout()
    elif pathname == '/dashboard-geral':
        return general_dash.get_layout()
    else:
        return map_view.get_layout()


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


# ==============================================================================
# --- CALLBACKS DE DADOS (Lê o DB/Disco) ---
# ==============================================================================

@app.callback(
    Output('intervalo-atualizacao-dados', 'disabled'),
    Input('session-store', 'data')
)
def toggle_interval_update(session_data):
    is_logged_in = session_data and session_data.get('logged_in', False)
    return not is_logged_in


@app.callback(
    [Output('store-dados-sessao', 'data'),
     Output('store-ultimo-status', 'data'),
     Output('store-logs-sessao', 'data')],
    Input('intervalo-atualizacao-dados', 'n_intervals')
)
def update_data_and_logs_from_disk(n_intervals):
    df_completo, status_atual, logs = data_source.get_all_data_from_disk()
    dados_json_output = df_completo.to_json(date_format='iso', orient='split')
    return dados_json_output, status_atual, logs


# ==============================================================================
# --- INICIA O WORKER THREAD ---
# ==============================================================================

# 1. Configura os caminhos ANTES de iniciar a thread
data_source.setup_disk_paths()

# 2. Inicia o coletor de dados (worker) em um processo de fundo
# Esta linha será executada pelo Gunicorn assim que o app carregar
print("Iniciando o worker (coletor de dados) em um thread separado...")
worker_thread = Thread(target=background_task_wrapper, daemon=True)
worker_thread.start()

# 3. O 'if __name__ == "__main__":' foi removido
#    pois o Gunicorn não o executa. O app é iniciado pelo 'server'
#    no arquivo app.py