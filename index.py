# index.py (COMPLETO, COM LÓGICA DE BASE DINÂMICA CORRIGIDA)

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
import datetime

# Carrega as variáveis do .env file
load_dotenv()

# --- IMPORTAÇÃO CRÍTICA DO APP ---
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
from config import PONTOS_DE_ANALISE, RISCO_MAP, FREQUENCIA_API_SEGUNDOS, ID_PONTO_ZENTRA_KM72, CONSTANTES_PADRAO

# --- FIM DAS IMPORTAÇÕES DO WORKER ---


# --- VARIÁVEIS DE AUTENTICAÇÃO ---
SENHA_CLIENTE = '@Tamoiosv1'
SENHA_ADMIN = 'admin456'


# ==============================================================================
# --- LÓGICA DO WORKER (O COLETOR DE DADOS EM SEGUNDO PLANO) ---
# ==============================================================================

def worker_verificar_alertas(status_novos, status_antigos):
    # ... (Esta função permanece EXATAMENTE IGUAL) ...
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

            status_atualizado[id_ponto] = status_novo
    return status_atualizado


def worker_main_loop():
    inicio_ciclo = time.time()
    try:
        # 1. Lê o histórico (cache CSV de 72h) e os status
        historico_df, status_antigos_do_disco, logs = data_source.get_all_data_from_disk()
        if not status_antigos_do_disco:
            status_antigos_do_disco = {p: "INDEFINIDO" for p in PONTOS_DE_ANALISE.keys()}

        print(f"WORKER (Thread): Início do ciclo. Histórico lido: {len(historico_df)} entradas.")

        # --- LÓGICA DE BACKFILL (GAP CHECK) ---

        # A. Backfill Chuva KM 67 (Como antes)
        # ... (Esta secção permanece EXATAMENTE IGUAL) ...
        df_km67 = historico_df[historico_df['id_ponto'] == 'Ponto-A-KM67']
        run_backfill_km67 = False

        if df_km67.empty:
            print("[Worker Thread] Histórico do KM 67 (Pro) está vazio. Tentando backfill de 72h...")
            run_backfill_km67 = True
        else:
            try:
                if not pd.api.types.is_datetime64_any_dtype(df_km67['timestamp']):
                    df_km67.loc[:, 'timestamp'] = pd.to_datetime(df_km67['timestamp'])
                latest_timestamp = df_km67['timestamp'].max()
                agora_utc = datetime.datetime.now(datetime.timezone.utc)
                gap_segundos = (agora_utc - latest_timestamp).total_seconds()
                if gap_segundos > (20 * 60):
                    print(
                        f"[Worker Thread] Detectado 'gap' de {gap_segundos / 60:.0f} min para o KM 67. Tentando backfill...")
                    run_backfill_km67 = True
            except Exception as e_gap:
                print(f"[Worker Thread] Erro ao checar 'gap' de dados (KM 67): {e_gap}. Pulando backfill.")

        if run_backfill_km67:
            try:
                data_source.backfill_km67_pro_data(historico_df)
                historico_df, _, _ = data_source.get_all_data_from_disk()  # Recarrega o DF
                print(f"[Worker Thread] Backfill KM 67 concluído.")
            except Exception as e_backfill:
                data_source.adicionar_log("GERAL", f"ERRO CRÍTICO (Backfill KM 67): {e_backfill}")

        # B. Backfill Umidade KM 72 (Como antes)
        # ... (Esta secção permanece EXATAMENTE IGUAL) ...
        df_km72 = historico_df[historico_df['id_ponto'] == ID_PONTO_ZENTRA_KM72]
        run_backfill_km72 = False

        if df_km72.empty or df_km72['umidade_1m_perc'].isnull().all():
            print("[Worker Thread] Histórico de Umidade do KM 72 está vazio. Tentando backfill de 72h...")
            run_backfill_km72 = True
        else:
            try:
                if not pd.api.types.is_datetime64_any_dtype(df_km72['timestamp']):
                    df_km72.loc[:, 'timestamp'] = pd.to_datetime(df_km72['timestamp'])

                latest_timestamp_umidade = df_km72.dropna(subset=['umidade_1m_perc'])['timestamp'].max()

                if pd.isna(latest_timestamp_umidade):
                    print(
                        "[Worker Thread] Histórico de Umidade do KM 72 (coluna existe) está vazio. Tentando backfill...")
                    run_backfill_km72 = True
                else:
                    agora_utc = datetime.datetime.now(datetime.timezone.utc)
                    gap_segundos = (agora_utc - latest_timestamp_umidade).total_seconds()
                    if gap_segundos > (20 * 60):
                        print(
                            f"[Worker Thread] Detectado 'gap' de {gap_segundos / 60:.0f} min para a Umidade KM 72. Tentando backfill...")
                        run_backfill_km72 = True
            except Exception as e_gap_km72:
                print(f"[Worker Thread] Erro ao checar 'gap' de dados (KM 72): {e_gap_km72}. Pulando backfill.")

        if run_backfill_km72:
            try:
                data_source.backfill_zentra_km72_data(historico_df)
                historico_df, _, _ = data_source.get_all_data_from_disk()  # Recarrega o DF
                print(f"[Worker Thread] Backfill Umidade KM 72 concluído.")
            except Exception as e_backfill_km72:
                data_source.adicionar_log(ID_PONTO_ZENTRA_KM72, f"ERRO CRÍTICO (Backfill Zentra): {e_backfill_km72}")
        # --- FIM DO BACKFILL ---

        # 2. COLETAR NOVOS DADOS DA API (passo /current)
        # Primeiro a WeatherLink (chuva)
        novos_dados_df, status_novos_API = data_source.executar_passo_api_e_salvar(historico_df)

        # SEGUNDO: Coleta incremental da Zentra (Umidade)
        dados_umidade_km72_inc = data_source.fetch_data_from_zentra_cloud()

        # --- INÍCIO DA ALTERAÇÃO (LÓGICA DE BASE DINÂMICA com TRAVA DE 6H) ---

        if dados_umidade_km72_inc:
            # Recarrega o histórico (que agora inclui a linha da WeatherLink)
            historico_df, _, _ = data_source.get_all_data_from_disk()
            df_km72_hist = historico_df[historico_df['id_ponto'] == ID_PONTO_ZENTRA_KM72].copy()

            # Garante que o timestamp está em formato datetime para o processamento
            if not df_km72_hist.empty:
                df_km72_hist['timestamp'] = pd.to_datetime(df_km72_hist['timestamp'])

            if not df_km72_hist.empty:

                # 1. Encontrar a base anterior
                df_bases_validas = df_km72_hist.dropna(subset=['base_1m', 'base_2m', 'base_3m'])

                if df_bases_validas.empty:
                    print("[Worker] Primeiro run ou bases nulas. Usando valores estáticos do config.")
                    old_base_1m = CONSTANTES_PADRAO['UMIDADE_BASE_1M']
                    old_base_2m = CONSTANTES_PADRAO['UMIDADE_BASE_2M']
                    old_base_3m = CONSTANTES_PADRAO['UMIDADE_BASE_3M']
                else:
                    ultimo_dado_base = df_bases_validas.sort_values('timestamp').iloc[-1]
                    old_base_1m = ultimo_dado_base['base_1m']
                    old_base_2m = ultimo_dado_base['base_2m']
                    old_base_3m = ultimo_dado_base['base_3m']

                # 2. Preparar os novos dados
                novos_dados_para_salvar = {}

                # Sensor 1m
                new_sensor_1m = dados_umidade_km72_inc.get('umidade_1m_perc')
                if new_sensor_1m is not None:
                    novos_dados_para_salvar['umidade_1m_perc'] = new_sensor_1m
                    # CHAMA A NOVA FUNÇÃO DE TRAVA (6 horas)
                    new_base_1m = processamento.verificar_trava_base(
                        df_km72_hist, 'umidade_1m_perc', new_sensor_1m, old_base_1m, horas=6
                    )
                    novos_dados_para_salvar['base_1m'] = new_base_1m

                # Sensor 2m
                new_sensor_2m = dados_umidade_km72_inc.get('umidade_2m_perc')
                if new_sensor_2m is not None:
                    novos_dados_para_salvar['umidade_2m_perc'] = new_sensor_2m
                    # CHAMA A NOVA FUNÇÃO DE TRAVA (6 horas)
                    new_base_2m = processamento.verificar_trava_base(
                        df_km72_hist, 'umidade_2m_perc', new_sensor_2m, old_base_2m, horas=6
                    )
                    novos_dados_para_salvar['base_2m'] = new_base_2m

                # Sensor 3m
                new_sensor_3m = dados_umidade_km72_inc.get('umidade_3m_perc')
                if new_sensor_3m is not None:
                    novos_dados_para_salvar['umidade_3m_perc'] = new_sensor_3m
                    # CHAMA A NOVA FUNÇÃO DE TRAVA (6 horas)
                    new_base_3m = processamento.verificar_trava_base(
                        df_km72_hist, 'umidade_3m_perc', new_sensor_3m, old_base_3m, horas=6
                    )
                    novos_dados_para_salvar['base_3m'] = new_base_3m

                # 3. Encontrar a linha para atualizar
                ts_recente = df_km72_hist['timestamp'].max()
                idx_list = historico_df.index[
                    (historico_df['id_ponto'] == ID_PONTO_ZENTRA_KM72) &
                    (historico_df['timestamp'] == ts_recente)
                    ].tolist()

                if idx_list:
                    idx = idx_list[0]
                    # Anexa os dados (sensor E base) no DF em memória
                    for coluna, valor in novos_dados_para_salvar.items():
                        historico_df.at[idx, coluna] = valor

                    print(
                        f"[Worker] Dados Zentra (Incremental) e Bases Dinâmicas (Trava 6h) anexadas ao Ponto {ID_PONTO_ZENTRA_KM72}.")
                    # (O log de mudança de base agora está dentro da função 'verificar_trava_base')

                    # Salva o DF atualizado
                    data_source.save_historico_to_csv(historico_df)
                    # (O SQLite será atualizado no próximo ciclo de backfill se houver gap)
                else:
                    print(
                        f"[Worker] Aviso: Zentra retornou dados, mas não encontrou a linha de timestamp mais recente para {ID_PONTO_ZENTRA_KM72}.")

        # --- FIM DA ALTERAÇÃO (LÓGICA DE BASE DINÂMICA) ---

        # 3. RECARREGAR O HISTÓRICO COMPLETO (FINAL)
        # (O historico_df em memória já foi atualizado, então podemos usá-lo)
        historico_completo = historico_df.copy()

        if historico_completo.empty:
            print("AVISO (Thread): Histórico vazio, pulando cálculo de status.")
            status_atualizado = {p: "SEM DADOS" for p in PONTOS_DE_ANALISE.keys()}
        else:

            # 4. Calcular Status (Chuva + Umidade)
            status_atualizado = {}
            for id_ponto in PONTOS_DE_ANALISE.keys():
                df_ponto = historico_completo[historico_completo['id_ponto'] == id_ponto].copy()

                # A. Status da CHUVA
                status_chuva_txt = "SEM DADOS"
                acumulado_72h_df = processamento.calcular_acumulado_rolling(df_ponto, horas=72)

                if not acumulado_72h_df.empty and not pd.isna(acumulado_72h_df['chuva_mm'].iloc[-1]):
                    chuva_72h_final = acumulado_72h_df['chuva_mm'].iloc[-1]
                    status_chuva_txt, _ = processamento.definir_status_chuva(chuva_72h_final)

                # B. Status da UMIDADE
                status_umidade_txt = "LIVRE"

                if not df_ponto.empty:
                    try:
                        ultimo_dado = df_ponto.sort_values('timestamp').iloc[-1]

                        # --- INÍCIO DA ALTERAÇÃO (USAR BASE DINÂMICA) ---

                        # A base agora vem do 'ultimo_dado', não do 'config'
                        base_1m = ultimo_dado.get('base_1m')
                        base_2m = ultimo_dado.get('base_2m')
                        base_3m = ultimo_dado.get('base_3m')

                        # Fallback (essencial para o primeiro run e para os KMs sem Zentra)
                        if pd.isna(base_1m):
                            base_1m = CONSTANTES_PADRAO['UMIDADE_BASE_1M']
                        if pd.isna(base_2m):
                            base_2m = CONSTANTES_PADRAO['UMIDADE_BASE_2M']
                        if pd.isna(base_3m):
                            base_3m = CONSTANTES_PADRAO['UMIDADE_BASE_3M']

                        # --- FIM DA ALTERAÇÃO ---

                        status_umidade_tuple = processamento.definir_status_umidade_hierarquico(
                            ultimo_dado.get('umidade_1m_perc'),
                            ultimo_dado.get('umidade_2m_perc'),
                            ultimo_dado.get('umidade_3m_perc'),
                            base_1m, base_2m, base_3m
                        )

                        texto_status_umidade = status_umidade_tuple[0]

                        if texto_status_umidade not in ["SEM DADOS", "INDEFINIDO", "ERRO"]:
                            status_umidade_txt = texto_status_umidade

                    except IndexError:
                        pass
                    except Exception as e_umid:
                        print(f"[Worker] Erro ao calcular status de umidade para {id_ponto}: {e_umid}")
                        status_umidade_txt = "LIVRE"

                # C. Define o Status Final (O mais grave vence)
                risco_chuva = RISCO_MAP.get(status_chuva_txt, -1)
                risco_umidade = RISCO_MAP.get(status_umidade_txt, -1)

                status_final_txt = status_chuva_txt
                if risco_umidade > risco_chuva:
                    status_final_txt = status_umidade_txt

                status_atualizado[id_ponto] = status_final_txt

            # --- FIM DO CÁLCULO DE STATUS ---

        # 5. Verificar alertas
        status_final_com_alertas = worker_verificar_alertas(status_atualizado, status_antigos_do_disco)

        # 6. SALVAR O STATUS ATUALIZADO NO DISCO
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
    # ... (Esta função permanece EXATAMENTE IGUAL) ...
    data_source.setup_disk_paths()
    print("--- Processo Worker (Thread) Iniciado (Modo Sincronizado) ---")
    data_source.adicionar_log("GERAL", "Processo Worker (Thread) iniciado com sucesso.")
    INTERVALO_EM_MINUTOS = 15
    CARENCIA_EM_SEGUNDOS = 60
    while True:
        inicio_total = time.time()
        worker_main_loop()
        tempo_execucao = time.time() - inicio_total
        agora_utc = datetime.datetime.now(datetime.timezone.utc)
        minutos_restantes = INTERVALO_EM_MINUTOS - (agora_utc.minute % INTERVALO_EM_MINUTOS)
        proxima_execucao_base_utc = agora_utc + datetime.timedelta(minutes=minutos_restantes)
        proxima_execucao_base_utc = proxima_execucao_base_utc.replace(
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

app.layout = html.Div([
    # ... (Esta secção permanece EXATAMENTE IGUAL) ...
    dcc.Store(id='session-store', data={'logged_in': False, 'user_type': 'guest'}, storage_type='session'),
    dcc.Store(id='store-dados-sessao', storage_type='session'),
    dcc.Store(id='store-ultimo-status', storage_type='session'),
    dcc.Store(id='store-logs-sessao', storage_type='session'),
    dcc.Location(id='url-raiz', refresh=False),
    dcc.Interval(
        id='intervalo-atualizacao-dados',
        interval=10 * 1000,
        n_intervals=0,
        disabled=True
    ),
    html.Div(id='page-container-root')
])


# ==============================================================================
# --- CALLBACKS ---
# ==============================================================================

# ... (Todos os callbacks permanecem EXATAMENTE IGUAIS) ...
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
# --- SEÇÃO DE EXECUÇÃO LOCAL (COM CORREÇÃO PARA DEBUG MODE) ---
# ==============================================================================

# ... (Esta secção permanece EXATAMENTE IGUAL) ...
data_source.setup_disk_paths()
if not os.environ.get("WERKZEUG_MAIN"):
    print("Iniciando o worker (coletor de dados) em um thread separado...")
    worker_thread = Thread(target=background_task_wrapper, daemon=True)
    worker_thread.start()
else:
    print("O reloader do Dash está ativo. O worker não será iniciado neste processo.")

if __name__ == '__main__':
    host = '127.0.0.1'
    port = 8050
    print(f"Iniciando o servidor Dash (site) em http://{host}:{port}/")
    try:
        app.run(debug=True, host=host, port=port)
    except Exception as e:
        print(f"ERRO CRÍTICO NA EXECUÇÃO DO APP.RUN: {e}")