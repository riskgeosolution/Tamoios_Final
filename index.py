# index.py (COMPLETO, v6: Reverte para o modo "somente incremental" v4)

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
    """
    Função que roda a CADA 15 MINUTOS (Modo Leve)
    """
    inicio_ciclo = time.time()
    try:
        # 1. Lê o histórico (cache CSV de 72h) e os status
        historico_df, status_antigos_do_disco, logs = data_source.get_all_data_from_disk()
        if not status_antigos_do_disco:
            status_antigos_do_disco = {p: "INDEFINIDO" for p in PONTOS_DE_ANALISE.keys()}

        print(f"WORKER (Thread): Início do ciclo. Histórico lido: {len(historico_df)} entradas.")

        # --- LÓGICA DE BACKFILL DE CHUVA (KM 67) ---
        # (Mantido: Isso só roda se houver uma falha, é seguro)
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
        # --- FIM DO BACKFILL DE CHUVA ---

        # --- LÓGICA DE COLETA LEVE (CHUVA + UMIDADE) ---

        # 1. COLETAR CHUVA (WeatherLink)
        novos_dados_df, status_novos_API = data_source.executar_passo_api_e_salvar(historico_df)
        historico_df, _, _ = data_source.get_all_data_from_disk()  # Recarrega

        # 2. COLETAR UMIDADE (Zentra - MODO LEVE)
        try:
            print("[Worker Thread] Executando coleta INCREMENTAL de Umidade Zentra...")
            dados_umidade_dict = data_source.fetch_data_from_zentra_cloud()

            if dados_umidade_dict:
                agora_epoch = datetime.datetime.now(datetime.timezone.utc).timestamp()
                timestamp_arredondado = data_source.arredondar_timestamp_15min(agora_epoch)

                linha_umidade = {
                    "timestamp": timestamp_arredondado,
                    "id_ponto": ID_PONTO_ZENTRA_KM72,
                    "chuva_mm": pd.NA,
                    "precipitacao_acumulada_mm": pd.NA,
                    "umidade_1m_perc": dados_umidade_dict.get('umidade_1m_perc'),
                    "umidade_2m_perc": dados_umidade_dict.get('umidade_2m_perc'),
                    "umidade_3m_perc": dados_umidade_dict.get('umidade_3m_perc'),
                    "base_1m": pd.NA, "base_2m": pd.NA, "base_3m": pd.NA,
                }

                df_umidade_incremental = pd.DataFrame([linha_umidade])
                df_umidade_incremental['timestamp'] = pd.to_datetime(df_umidade_incremental['timestamp'], utc=True)

                # Salva em AMBOS os locais (importante para PDF/Excel)
                data_source.save_to_sqlite(df_umidade_incremental)
                historico_combinado = pd.concat([historico_df, df_umidade_incremental], ignore_index=True)
                data_source.save_historico_to_csv(historico_combinado)

                # Recarrega o histórico final mesclado
                historico_df, _, _ = data_source.get_all_data_from_disk()
                print("[Worker Thread] Coleta incremental de Umidade Zentra mesclada.")
            else:
                print("[Worker Thread] Zentra (Incremental) não retornou novos dados.")
        except Exception as e_incremental_km72:
            data_source.adicionar_log(ID_PONTO_ZENTRA_KM72, f"ERRO CRÍTICO (Incremental Zentra): {e_incremental_km72}")
            traceback.print_exc()
        # --- FIM DA COLETA LEVE ---

        # 3. CÁLCULO DE STATUS (Com os dados mesclados)
        historico_completo = historico_df.copy()
        if historico_completo.empty:
            print("AVISO (Thread): Histórico vazio, pulando cálculo de status.")
            status_atualizado = {p: "SEM DADOS" for p in PONTOS_DE_ANALISE.keys()}
        else:
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
                        df_ponto_umidade = df_ponto.dropna(
                            subset=['umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc'])

                        if df_ponto_umidade.empty:
                            base_1m = CONSTANTES_PADRAO['UMIDADE_BASE_1M']
                            base_2m = CONSTANTES_PADRAO['UMIDADE_BASE_2M']
                            base_3m = CONSTANTES_PADRAO['UMIDADE_BASE_3M']
                        else:
                            base_1m = df_ponto_umidade['umidade_1m_perc'].min()
                            base_2m = df_ponto_umidade['umidade_2m_perc'].min()
                            base_3m = df_ponto_umidade['umidade_3m_perc'].min()

                        ultimo_dado = df_ponto.sort_values('timestamp').iloc[-1]
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
    """
    Função que inicia a thread.
    NÃO faz backfill pesado no início.
    Entra DIRETAMENTE no loop leve de 15 minutos.
    """
    data_source.setup_disk_paths()
    print("--- Processo Worker (Thread) Iniciado (Modo Sincronizado) ---")
    data_source.adicionar_log("GERAL", "Processo Worker (Thread) iniciado com sucesso.")

    # --- INÍCIO DA CORREÇÃO (REMOVIDO BACKFILL INICIAL) ---
    # A lógica de backfill pesado foi removida daqui
    # para evitar o travamento (deadlock) no deploy.
    # --- FIM DA CORREÇÃO ---

    INTERVALO_EM_MINUTOS = 15
    CARENCIA_EM_SEGUNDOS = 60
    while True:
        inicio_total = time.time()

        # Roda o loop LEVE
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
    # --- MODIFICADO: A thread agora chama a nova função wrapper ---
    worker_thread = Thread(target=background_task_wrapper, daemon=True)
    worker_thread.start()
else:
    print("O reloader do Dash está ativo. O worker não será iniciado neste processo.")

if __name__ == '__main__':
    host = '127.0.0.1'
    port = 8050
    print(f"Iniciando o servidor Dash (site) em http://{host}:{port}/")
    try:
        # ATENÇÃO: Adicione use_reloader=False para testar localmente
        app.run(debug=True, host=host, port=port, use_reloader=False)
    except Exception as e:
        print(f"ERRO CRÍTICO NA EXECUÇÃO DO APP.RUN: {e}")