# index.py (COMPLETO, v12.11: Final com Fix de Tipagem e Importação)

import dash
from dash import html, dcc, callback, Input, Output, State
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
import pandas as pd
from io import StringIO
import os
import json
from dotenv import load_dotenv  # <<< FIX: IMPORTAÇÃO ADICIONADA
import time
from threading import Thread
import datetime
from httpx import HTTPStatusError  # Importar explicitamente

load_dotenv()

from app import app, server
from pages import login as login_page, main_app as main_app_page, map_view, general_dash, specific_dash
import data_source
import config
import processamento
import alertas
import traceback
from config import PONTOS_DE_ANALISE, RISCO_MAP, FREQUENCIA_API_SEGUNDOS, ID_PONTO_ZENTRA_KM72, CONSTANTES_PADRAO

SENHA_CLIENTE = '@Tamoiosv1'
SENHA_ADMIN = 'admin456'


# ==============================================================================
# --- LÓGICA DO WORKER (ESTÁVEL) ---
# ==============================================================================

def worker_verificar_alertas(status_novos, status_antigos):
    if not status_novos: return status_antigos
    if not isinstance(status_antigos, dict):
        status_antigos = {pid: "INDEFINIDO" for pid in PONTOS_DE_ANALISE.keys()}
    status_atualizado = status_antigos.copy()
    for id_ponto in PONTOS_DE_ANALISE.keys():
        status_novo = status_novos.get(id_ponto, "SEM DADOS")
        status_antigo = status_antigos.get(id_ponto, "INDEFINIDO")
        if status_novo != status_antigo:
            try:
                nome_ponto = PONTOS_DE_ANALISE[id_ponto]['nome']
                msg = f"MUDANÇA DE STATUS: {nome_ponto} mudou de {status_antigo} para {status_novo}."
                data_source.adicionar_log(id_ponto, msg)
                print(f"| {id_ponto} | {msg}")
                # if novo_status == "PARALIZAÇÃO" or status_anterior == "PARALIZAÇÃO":
                #      alertas.enviar_alerta(id_ponto, nome_ponto, novo_status, status_antigo)
            except Exception as e:
                print(f"Erro ao processar mudança de status: {e}")
            status_atualizado[id_ponto] = status_novo
    return status_atualizado


def worker_main_loop():
    inicio_ciclo = time.time()
    trigger_backfill = False  # Flag para controlar se precisamos reiniciar o loop

    try:
        # --- PRINT DE DEBUG DOS ÚLTIMOS DADOS DO DB ---
        print("\n--- 💾 Últimos 3 Registros no DB (Antes da API) ---")
        print(data_source.get_last_n_entries(n=3).to_string())
        print("--------------------------------------------------\n")
        # --- FIM: PRINT DE DEBUG ---

        print(f"WORKER (Thread): Início do ciclo.")
        historico_recente_df = data_source.get_recent_data_for_worker(hours=73)
        status_antigos_do_disco = data_source.get_status_from_disk()

        # --- BLOCO DE CORREÇÃO CRÍTICA DE TIPAGEM (Linha ~68) ---
        # Garantir que as colunas numéricas no histórico são floats antes de qualquer cálculo (min())
        numeric_cols_history = ['chuva_mm', 'umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc']
        for col in numeric_cols_history:
            if col in historico_recente_df.columns:
                # Usando o .loc para evitar o SettingWithCopyWarning e garantindo a coerção para float
                historico_recente_df.loc[:, col] = pd.to_numeric(historico_recente_df[col], errors='coerce')
        # --- FIM DO BLOCO DE CORREÇÃO ---

        novos_dados_chuva_df, _, _ = data_source.fetch_data_from_weatherlink_api()

        df_umidade_incremental = pd.DataFrame()
        try:
            dados_umidade_dict = data_source.fetch_data_from_zentra_cloud()
            if dados_umidade_dict:
                agora_epoch = datetime.datetime.now(datetime.timezone.utc).timestamp()
                ts_arredondado = data_source.arredondar_timestamp_15min(agora_epoch)
                linha_umidade = {"timestamp": ts_arredondado, "id_ponto": ID_PONTO_ZENTRA_KM72, **dados_umidade_dict}
                df_umidade_incremental = pd.DataFrame([linha_umidade])
                df_umidade_incremental['timestamp'] = pd.to_datetime(df_umidade_incremental['timestamp'], utc=True)
        except Exception as e:
            data_source.adicionar_log(ID_PONTO_ZENTRA_KM72, f"ERRO (Incremental Zentra): {e}")

        # --- CORREÇÃO CRÍTICA: SALVAMENTO ANTES DE FAZER O BACKFILL ---
        df_novos = pd.concat([novos_dados_chuva_df, df_umidade_incremental], ignore_index=True)

        if not df_novos.empty:
            data_source.save_to_sqlite(df_novos)
            historico_para_calculo = pd.concat([historico_recente_df, df_novos], ignore_index=True)
        else:
            historico_para_calculo = historico_recente_df
        # --- FIM DA CORREÇÃO ---

        # --- INÍCIO DA DETECÇÃO DE BACKFILL ---

        if not novos_dados_chuva_df.empty:
            for id_ponto in novos_dados_chuva_df['id_ponto'].unique():
                df_ponto_recente = historico_recente_df[historico_recente_df['id_ponto'] == id_ponto]
                if not df_ponto_recente.empty and (novos_dados_chuva_df['timestamp'].min() - df_ponto_recente[
                    'timestamp'].max()).total_seconds() > (FREQUENCIA_API_SEGUNDOS * 2):
                    if id_ponto == "Ponto-A-KM67":
                        print(f"[{id_ponto}] LACUNA WeatherLink DETECTADA. Acionando backfill.")
                        data_source.backfill_weatherlink_data(id_ponto)
                        trigger_backfill = True
                    else:
                        print(
                            f"[{id_ponto}] LACUNA WeatherLink DETECTADA. Backfill automático não permitido. Apenas registrando.")
                        data_source.adicionar_log(id_ponto,
                                                  "AVISO: Lacuna de dados detectada. Backfill automático não acionado para este ponto.")

        if not df_umidade_incremental.empty:
            df_ponto_zentra = historico_recente_df[historico_recente_df['id_ponto'] == ID_PONTO_ZENTRA_KM72]
            if not df_ponto_zentra.empty and (df_umidade_incremental['timestamp'].min() - df_ponto_zentra[
                'timestamp'].max()).total_seconds() > 3600 * 2:
                print(f"[{ID_PONTO_ZENTRA_KM72}] LACUNA Zentra DETECTADA. Acionando backfill.")
                data_source.backfill_zentra_km72_data()
                trigger_backfill = True

        if trigger_backfill:
            return False
        # --- FIM DA DETECÇÃO DE BACKFILL ---

        status_atualizado = {}
        if not historico_para_calculo.empty:
            for id_ponto in PONTOS_DE_ANALISE.keys():
                df_ponto = historico_para_calculo[historico_para_calculo['id_ponto'] == id_ponto].copy()
                if df_ponto.empty:
                    status_atualizado[id_ponto] = "SEM DADOS";
                    continue

                acumulado_72h = processamento.calcular_acumulado_rolling(df_ponto, horas=72)
                chuva_72h_final = acumulado_72h['chuva_mm'].iloc[-1] if not acumulado_72h.empty else 0.0
                status_chuva, _ = processamento.definir_status_chuva(chuva_72h_final)

                df_umidade = df_ponto.dropna(subset=['umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc'])
                bases = {
                    f'base_{d}m': df_umidade[f'umidade_{d}m_perc'].min() if not df_umidade.empty else CONSTANTES_PADRAO[
                        f'UMIDADE_BASE_{d}M'] for d in [1, 2, 3]}

                ultimo_dado = df_ponto.sort_values('timestamp').iloc[-1]
                status_umidade_tuple = processamento.definir_status_umidade_hierarquico(
                    ultimo_dado.get('umidade_1m_perc'), ultimo_dado.get('umidade_2m_perc'),
                    ultimo_dado.get('umidade_3m_perc'),
                    **bases
                )
                status_umidade = status_umidade_tuple[0] if status_umidade_tuple[0] != "SEM DADOS" else "LIVRE"
                status_atualizado[id_ponto] = status_umidade if RISCO_MAP.get(status_umidade, 0) > RISCO_MAP.get(
                    status_chuva, 0) else status_chuva
        else:
            status_atualizado = {p: "SEM DADOS" for p in PONTOS_DE_ANALISE.keys()}

        status_final = worker_verificar_alertas(status_atualizado, status_antigos_do_disco)
        with open(data_source.STATUS_FILE, 'w', encoding='utf-8') as f:
            json.dump(status_final, f, indent=2)

        print(f"WORKER (Thread): Ciclo concluído em {time.time() - inicio_ciclo:.2f}s.")
        return True
    except Exception as e:
        print(f"WORKER ERRO CRÍTICO (Thread): {e}");
        traceback.print_exc()
        data_source.adicionar_log("GERAL", f"ERRO CRÍTICO (Thread loop): {e}")
        return False


def background_task_wrapper():
    # CORREÇÃO: A inicialização dos caminhos DEVE ser a primeira coisa que a thread faz.
    data_source.setup_disk_paths()
    data_source.initialize_database()

    print("--- Processo Worker (Thread) Iniciado (v12.5 - Estável) ---")
    data_source.adicionar_log("GERAL", "Processo Worker (Thread) iniciado.")
    if data_source.read_data_from_sqlite().empty:
        print("[Worker Inicial] DB vazio. Executando backfill inicial para pontos autorizados.")
        # Executa o backfill apenas para o ponto Pro autorizado para evitar falhas de API.
        try:
            data_source.backfill_weatherlink_data("Ponto-A-KM67")
        except HTTPStatusError as e:
            if e.response.status_code == 401:
                data_source.adicionar_log("Ponto-A-KM67",
                                          "AVISO: Falha na autenticação (401) no backfill inicial. Verifique as credenciais da API. Backfill pulado.")
                print(f"[API Ponto-A-KM67] AVISO: Falha na autenticação (401) no backfill inicial. Backfill pulado.")
            else:
                data_source.adicionar_log("Ponto-A-KM67", f"ERRO API WeatherLink (Backfill Inicial): {e}")
                print(f"[API Ponto-A-KM67] ERRO API WeatherLink (Backfill Inicial): {e}")
                traceback.print_exc()
        except Exception as e:
            data_source.adicionar_log("Ponto-A-KM67", f"ERRO CRÍTICO (Backfill Inicial WeatherLink): {e}")
            print(f"[API Ponto-A-KM67] ERRO CRÍTICO (Backfill Inicial WeatherLink): {e}")
            traceback.print_exc()

        data_source.backfill_zentra_km72_data()
    else:
        print("[Worker Inicial] DB já contém dados. Pulando backfill inicial.")

    INTERVALO_EM_MINUTOS = 15
    CARENCIA_EM_SEGUNDOS = 60
    while True:
        if not worker_main_loop():
            print("WORKER (Thread): Reiniciando ciclo após backfill.")
            time.sleep(5)
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

        print(f"WORKER (Thread): Próxima execução em ~{sleep_time:.0f}s...")
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

# CORREÇÃO: A inicialização dos caminhos também é necessária para o processo principal do Dash.
data_source.setup_disk_paths()

if not os.environ.get("WERKZEUG_MAIN"):
    print("Iniciando o worker (coletor de dados) em um thread separado...")
    Thread(target=background_task_wrapper, daemon=True).start()
else:
    print("O reloader do Dash está ativo. O worker não será iniciado neste processo.")

if __name__ == '__main__':
    print(f"Iniciando o servidor Dash em http://127.0.0.1:8050/")
    app.run(debug=True, host='127.0.0.1', port=8050, use_reloader=False)