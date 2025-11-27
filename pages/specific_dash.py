# pages/specific_dash.py (CORRIGIDO v3 - Correção do Gráfico)

import dash
from dash import html, dcc, callback, Input, Output, State
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
from io import StringIO
import io
import traceback
import matplotlib.pyplot as plt
from matplotlib.figure import Figure
import numpy as np
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import uuid
from threading import Thread
import json

plt.switch_backend('Agg')

from app import app, TEMPLATE_GRAFICO_MODERNO
from config import (
    PONTOS_DE_ANALISE, CONSTANTES_PADRAO,
    RISCO_MAP, STATUS_MAP_HIERARQUICO,
    CORES_UMIDADE, DELTA_TRIGGER_UMIDADE
)
import processamento
import gerador_pdf
import data_source

from gerador_pdf import PDF_CACHE_LOCK, EXCEL_CACHE_LOCK, PDF_CACHE, EXCEL_CACHE


def get_layout():
    """ Retorna o layout da página de dashboard específico. """
    opcoes_tempo_lista = [1, 3, 6, 12, 18, 24, 48, 72, 84, 96]
    opcoes_tempo = [{'label': f'Últimas {h} horas', 'value': h} for h in opcoes_tempo_lista] + [
        {'label': 'Todo o Histórico (Máx 7 dias)', 'value': 7 * 24}]

    layout = dbc.Container([
        dcc.Store(id='store-id-ponto-ativo'),
        dcc.Store(id='store-logs-filtrados'),
        dcc.Store(id='pdf-task-id-store'),
        dcc.Store(id='excel-task-id-store'),
        dcc.Interval(id='pdf-check-interval', interval=2 * 1000, n_intervals=0, disabled=True),
        dcc.Interval(id='excel-check-interval', interval=2 * 1000, n_intervals=0, disabled=True),

        html.Div(id='specific-dash-title', className="my-3 text-center"),
        dbc.Row(id='specific-dash-cards', children=[dbc.Spinner(size="lg")]),

        dbc.Row([
            dbc.Col(dbc.Label("Período (Gráficos):"), width="auto"),
            dbc.Col(dcc.Dropdown(id='graph-time-selector', options=opcoes_tempo, value=72, clearable=False,
                                 searchable=False), width=12, lg=4),
            dbc.Col(html.Div(id='dynamic-accumulated-output'), width=12, lg=4, className="d-flex align-items-center")
        ], align="center", className="my-3"),

        dbc.Row(id='specific-dash-graphs', children=[dbc.Spinner(size="lg")], className="my-4"),

        dbc.Row([
            dbc.Col([
                html.H5("Relatórios e Eventos", className="mb-3"),
                dbc.Card(dbc.CardBody([
                    html.H6("Gerar Relatórios (PDF/Excel)", className="card-title"),
                    dcc.DatePickerRange(id='pdf-date-picker',
                                        start_date=(pd.Timestamp.now() - pd.Timedelta(days=7)).date(),
                                        end_date=pd.Timestamp.now().date(), display_format='DD/MM/YYYY',
                                        className="mb-3 w-100"),
                    html.Br(),
                    html.Div([
                        dbc.Button("Gerar PDF", id='btn-pdf-especifico', color="primary", size="sm", className="me-2"),
                        dcc.Download(id='download-pdf-especifico'),
                        dbc.Button("Gerar Excel", id='btn-excel-especifico', color="success", size="sm"),
                        dcc.Download(id='download-excel-especifico')
                    ], className="d-flex justify-content-center"),
                    html.Div(id='report-status-indicator', children=None,
                             className="text-center mt-3 small text-muted"),
                    dbc.Alert("Não há dados neste período para gerar o relatório.", id="alert-pdf-error",
                              color="danger", is_open=False, dismissable=True, className="mt-3"),
                    dbc.Alert("Erro desconhecido ao gerar relatório.", id="alert-pdf-generic-error", color="danger",
                              is_open=False, dismissable=True, className="mt-3"),
                ]), className="shadow-sm mb-4"),
                dbc.Card(dbc.CardBody([
                    html.H6("Logs de Eventos", className="card-title"),
                    dbc.Button("Ver Histórico de Eventos do Ponto", id='btn-ver-logs', color="secondary", size="sm")
                ], className="text-center"), className="shadow-sm"),
            ]),
        ], justify="center", className="mb-5"),

        dbc.Modal([
            dbc.ModalHeader("Histórico de Eventos do Ponto"),
            dbc.ModalBody(dcc.Loading(children=[
                html.Div(id='modal-logs-content', style={'whiteSpace': 'pre-wrap', 'wordWrap': 'break-word'})])),
            dbc.ModalFooter([
                dcc.Loading(id="loading-pdf-logs", type="default", children=[
                    dbc.Button("Gerar PDF dos Logs", id='btn-pdf-logs', color="success", className="me-2"),
                    dcc.Download(id='download-pdf-logs')
                ]),
                dbc.Button("Fechar", id='btn-fechar-logs', color="secondary")
            ]),
        ], id='modal-logs', is_open=False, size="lg"),
    ], fluid=True)
    return layout


@app.callback(
    Output('specific-dash-title', 'children'),
    Input('url-raiz', 'pathname')
)
def update_specific_title(pathname):
    if not pathname.startswith('/ponto/'):
        return dash.no_update
    try:
        id_ponto = pathname.split('/')[-1]
        config = PONTOS_DE_ANALISE.get(id_ponto, {"nome": "Ponto Desconhecido"})
        return html.H3(f"Estação {config['nome']}", style={'color': '#000000', 'font-weight': 'bold'})
    except Exception:
        return html.H3("Detalhes da Estação", style={'color': '#000000', 'font-weight': 'bold'})


@app.callback(
    Output('specific-dash-cards', 'children'),
    [Input('url-raiz', 'pathname'),
     Input('store-ultimo-status', 'data')]
)
def update_specific_cards(pathname, status_json):
    if not status_json or not pathname.startswith('/ponto/'):
        return dbc.Spinner(size="lg")

    id_ponto = pathname.split('/')[-1]
    if id_ponto not in PONTOS_DE_ANALISE:
        return dbc.Alert("Ponto não encontrado.", color="danger")

    status_info = status_json.get(id_ponto, {})
    if not isinstance(status_info, dict):
        status_info = {}

    # --- LÓGICA DE STATUS SEPARADA ---
    status_chuva_txt = status_info.get('chuva', 'INDEFINIDO')
    status_umidade_txt = status_info.get('umidade', 'INDEFINIDO')
    
    # Determina o status geral com base na hierarquia de risco
    risco_chuva = RISCO_MAP.get(status_chuva_txt, -1)
    risco_umidade = RISCO_MAP.get(status_umidade_txt, -1)
    risco_geral = max(risco_chuva, risco_umidade)
    
    status_geral_texto, _, card_class_color = STATUS_MAP_HIERARQUICO.get(risco_geral, ("INDEFINIDO", "secondary", "bg-secondary"))

    ultima_chuva_72h = status_info.get('chuva_72h', 0.0)
    umidade_1m = status_info.get('umidade_1m')
    umidade_2m = status_info.get('umidade_2m')
    umidade_3m = status_info.get('umidade_3m')

    css_color_s1 = CORES_UMIDADE['1m'] if umidade_1m and (umidade_1m - CONSTANTES_PADRAO.get('UMIDADE_BASE_1M', 39.0)) >= DELTA_TRIGGER_UMIDADE else 'green'
    css_color_s2 = CORES_UMIDADE['2m'] if umidade_2m and (umidade_2m - CONSTANTES_PADRAO.get('UMIDADE_BASE_2M', 43.0)) >= DELTA_TRIGGER_UMIDADE else 'green'
    css_color_s3 = CORES_UMIDADE['3m'] if umidade_3m and (umidade_3m - CONSTANTES_PADRAO.get('UMIDADE_BASE_3M', 10.0)) >= DELTA_TRIGGER_UMIDADE else 'green'

    layout_cards = [
        dbc.Col(dbc.Card(dbc.CardBody([html.H5("Status Geral"), html.P(status_geral_texto, className="fs-3 fw-bold")]),
                         className=f"shadow h-100 {card_class_color}"), xs=12, md=4, className="mb-4"),
        dbc.Col(dbc.Card(
            dbc.CardBody([html.H5("Chuva 72h"), html.P(f"{ultima_chuva_72h:.1f} mm", className="fs-3 fw-bold")]),
            className="shadow h-100 bg-white"), xs=12, md=4, className="mb-4"),
        dbc.Col(dbc.Card(dbc.CardBody([html.H5("Umidade do Solo (%)"), dbc.Row([
            dbc.Col(html.P(
                [html.Span(f"{umidade_1m or 0.0:.1f}", className="fs-3 fw-bold", style={'color': css_color_s1}),
                 html.Span(" (1m)", className="small")], className="mb-0 text-center"), width=4),
            dbc.Col(html.P(
                [html.Span(f"{umidade_2m or 0.0:.1f}", className="fs-3 fw-bold", style={'color': css_color_s2}),
                 html.Span(" (2m)", className="small")], className="mb-0 text-center"), width=4),
            dbc.Col(html.P(
                [html.Span(f"{umidade_3m or 0.0:.1f}", className="fs-3 fw-bold", style={'color': css_color_s3}),
                 html.Span(" (3m)", className="small")], className="mb-0 text-center"), width=4),
        ])]), className="shadow h-100 bg-white"), xs=12, md=4, className="mb-4"),
    ]
    return layout_cards


@app.callback(
    [Output('specific-dash-graphs', 'children'),
     Output('store-id-ponto-ativo', 'data')],
    [Input('intervalo-atualizacao-dados', 'n_intervals'),
     Input('url-raiz', 'pathname'),
     Input('graph-time-selector', 'value')]
)
def update_specific_graphs(n_intervals, pathname, selected_hours):
    if not pathname.startswith('/ponto/') or selected_hours is None:
        return dash.no_update, dash.no_update

    id_ponto = pathname.split('/')[-1]
    config = PONTOS_DE_ANALISE.get(id_ponto)
    if not config:
        return dbc.Alert("Ponto não encontrado.", color="danger"), id_ponto

    # --- OTIMIZAÇÃO DE MEMÓRIA ---
    horas_para_buscar = max(selected_hours, 73)
    df_completo = data_source.read_data_from_sqlite(id_ponto=id_ponto, last_hours=horas_para_buscar)

    try:
        if df_completo.empty or 'timestamp' not in df_completo.columns:
            return dbc.Alert("Dados históricos indisponíveis no momento.", color="warning"), id_ponto

        df_completo['timestamp'] = pd.to_datetime(df_completo['timestamp'])
        if df_completo['timestamp'].dt.tz is None:
            df_completo['timestamp'] = df_completo['timestamp'].dt.tz_localize('UTC')
        df_completo['timestamp_local'] = df_completo['timestamp'].dt.tz_convert('America/Sao_Paulo')

        numeric_cols = ['chuva_mm', 'umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc', 'precipitacao_acumulada_mm']
        for col in numeric_cols:
            if col in df_completo.columns:
                df_completo[col] = pd.to_numeric(df_completo[col], errors='coerce')

    except Exception as e:
        return dbc.Alert(f"Erro ao processar dados: {e}", color="danger"), id_ponto

    df_ponto = df_completo.copy()
    if df_ponto.empty:
        return dbc.Alert("Sem dados históricos para este ponto.", color="warning"), id_ponto

    df_ponto = df_ponto.sort_values('timestamp').drop_duplicates(subset=['timestamp'], keep='last')
    
    # --- CÁLCULO DA CHUVA INCREMENTAL PARA O GRÁFICO DE BARRAS ---
    if 'precipitacao_acumulada_mm' in df_ponto.columns:
        # Garante ordenação e preenche valores nulos para o cálculo do diff
        df_ponto = df_ponto.sort_values('timestamp').reset_index(drop=True)
        df_ponto['precipitacao_acumulada_mm'] = df_ponto['precipitacao_acumulada_mm'].ffill().fillna(0)
        
        # Calcula a chuva incremental (diferença entre leituras)
        df_ponto['chuva_incremental'] = df_ponto['precipitacao_acumulada_mm'].diff().fillna(0)
        
        # Corrige o reset da meia-noite (quando o acumulado zera e o diff se torna negativo)
        mask_virada_dia = df_ponto['chuva_incremental'] < 0
        df_ponto.loc[mask_virada_dia, 'chuva_incremental'] = df_ponto.loc[mask_virada_dia, 'precipitacao_acumulada_mm']
    else:
        # Fallback: se não houver 'precipitacao_acumulada_mm', usa 'chuva_mm' que pode já ser incremental
        df_ponto['chuva_incremental'] = df_ponto.get('chuva_mm', 0)

    umidade_cols = ['umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc']
    if all(c in df_ponto.columns for c in umidade_cols):
        df_ponto[umidade_cols] = df_ponto[umidade_cols].ffill()

    # 1. Calcular o acumulado para a linha do gráfico
    df_chuva_acumulada = processamento.calcular_acumulado_rolling(df_ponto, horas=selected_hours)

    # Filtra os dados para o período de tempo selecionado para plotagem
    ultimo_timestamp_no_df = df_ponto['timestamp_local'].max()
    limite_tempo = ultimo_timestamp_no_df - pd.Timedelta(hours=selected_hours)
    df_ponto_plot = df_ponto[df_ponto['timestamp_local'] >= limite_tempo].copy()

    # Agrega os dados em intervalos de 10 minutos para o gráfico
    agg_dict = {'chuva_incremental': 'sum'}
    if 'umidade_1m_perc' in df_ponto_plot.columns: agg_dict['umidade_1m_perc'] = 'mean'
    if 'umidade_2m_perc' in df_ponto_plot.columns: agg_dict['umidade_2m_perc'] = 'mean'
    if 'umidade_3m_perc' in df_ponto_plot.columns: agg_dict['umidade_3m_perc'] = 'mean'

    if not df_ponto_plot.empty:
        df_plot_10min = df_ponto_plot.set_index('timestamp_local').resample('10T').agg(agg_dict).reset_index()
        if all(c in df_plot_10min.columns for c in umidade_cols):
            df_plot_10min[umidade_cols] = df_plot_10min[umidade_cols].interpolate(method='linear', limit_direction='forward')
    else:
        df_plot_10min = pd.DataFrame(columns=['timestamp_local', 'chuva_incremental'] + umidade_cols)

    # 2. Filtrar o DataFrame de chuva acumulada para o período de plotagem
    df_chuva_acumulada_plot = df_chuva_acumulada[
        df_chuva_acumulada['timestamp'] >= df_ponto_plot['timestamp'].min()].copy()

    if 'timestamp' in df_chuva_acumulada_plot.columns:
        df_chuva_acumulada_plot.loc[:, 'timestamp_local'] = df_chuva_acumulada_plot['timestamp'].dt.tz_convert('America/Sao_Paulo')

    axis_style = dict(title="Data e Hora", dtick=10800000, tickformat="%H:%M\n%d/%b", tickangle=-45)

    fig_chuva = make_subplots(specs=[[{"secondary_y": True}]])
    # 3. Usar a nova coluna 'chuva_incremental' para as barras
    fig_chuva.add_trace(go.Bar(x=df_plot_10min['timestamp_local'], y=df_plot_10min['chuva_incremental'], name='Pluv. 10 min (mm)', marker_color='#2C3E50', opacity=0.8), secondary_y=False)
    
    # 4. Usar o DataFrame de acumulado para a linha
    fig_chuva.add_trace(go.Scatter(x=df_chuva_acumulada_plot['timestamp_local'], y=df_chuva_acumulada_plot['chuva_mm'], name=f'Acumulada ({selected_hours}h)', mode='lines', line=dict(color='#007BFF', width=2.5)), secondary_y=True)
    
    fig_chuva.update_layout(title_text=f"Pluviometria - Estação {config['nome']}", template=TEMPLATE_GRAFICO_MODERNO, margin=dict(l=40, r=20, t=50, b=80), legend=dict(orientation="h", yanchor="bottom", y=-0.5, xanchor='center', x=0.5), xaxis=axis_style, yaxis_title="Pluviometria (mm/10min)", yaxis2_title=f"Acumulada ({selected_hours}h)", hovermode="x unified")

    fig_umidade = go.Figure()
    if 'umidade_1m_perc' in df_plot_10min.columns:
        fig_umidade.add_trace(go.Scatter(x=df_plot_10min['timestamp_local'], y=df_plot_10min['umidade_1m_perc'], name='Umidade 1m', mode='lines', line=dict(color=CORES_UMIDADE['1m'], width=3)))
    if 'umidade_2m_perc' in df_plot_10min.columns:
        fig_umidade.add_trace(go.Scatter(x=df_plot_10min['timestamp_local'], y=df_plot_10min['umidade_2m_perc'], name='Umidade 2m', mode='lines', line=dict(color=CORES_UMIDADE['2m'], width=3)))
    if 'umidade_3m_perc' in df_plot_10min.columns:
        fig_umidade.add_trace(go.Scatter(x=df_plot_10min['timestamp_local'], y=df_plot_10min['umidade_3m_perc'], name='Umidade 3m', mode='lines', line=dict(color=CORES_UMIDADE['3m'], width=3)))

    umidade_cols_existentes = [c for c in ['umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc'] if c in df_plot_10min.columns]
    max_val_umidade = 0
    if umidade_cols_existentes:
        max_val_umidade = df_plot_10min[umidade_cols_existentes].max().max()
    if pd.isna(max_val_umidade): max_val_umidade = 0
    range_max = max(50, max_val_umidade * 1.1)

    fig_umidade.update_layout(title_text=f"Variação da Umidade do Solo - Estação {config['nome']}", template=TEMPLATE_GRAFICO_MODERNO, margin=dict(l=40, r=20, t=40, b=80), legend=dict(orientation="h", yanchor="bottom", y=-0.5, xanchor="center", x=0.5), xaxis=axis_style, yaxis_title="Umidade do Solo (%)", yaxis=dict(range=[0, range_max]), hovermode="x unified")

    layout_graficos = [
        dbc.Col(dbc.Card(dbc.CardBody(dcc.Graph(figure=fig_chuva)), className="shadow-sm"), width=12, className="mb-4"),
        dbc.Col(dbc.Card(dbc.CardBody(dcc.Graph(figure=fig_umidade)), className="shadow-sm"), width=12, className="mb-4"),
    ]
    return layout_graficos, id_ponto


# (O resto das callbacks de logs e relatórios permanece o mesmo)
@app.callback(Output('modal-logs', 'is_open'),
              [Input('btn-ver-logs', 'n_clicks'), Input('btn-fechar-logs', 'n_clicks')],
              [State('modal-logs', 'is_open')], prevent_initial_call=True)
def toggle_logs_modal(n_open, n_close, is_open):
    ctx = dash.callback_context
    if not ctx.triggered: return is_open
    button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    if button_id == 'btn-ver-logs':
        return not is_open
    elif button_id == 'btn-fechar-logs':
        return False
    return is_open


@app.callback([Output('modal-logs-content', 'children'), Output('store-logs-filtrados', 'data')],
              Input('modal-logs', 'is_open'),
              [State('store-id-ponto-ativo', 'data'), State('store-logs-sessao', 'data')])
def load_logs_content(is_open, id_ponto, logs_json):
    if not is_open or not id_ponto or not logs_json: return dash.no_update, dash.no_update
    try:
        logs_list = json.loads(logs_json) if isinstance(logs_json, str) else logs_json
        logs_ponto = [log for log in logs_list if f"| {id_ponto} |" in log or "| GERAL |" in log]
        return html.Pre('\n'.join(reversed(logs_ponto))), logs_ponto
    except Exception:
        return "Erro ao carregar logs.", []


@app.callback(Output('download-pdf-logs', 'data'), Input('btn-pdf-logs', 'n_clicks'),
              [State('store-id-ponto-ativo', 'data'), State('store-logs-filtrados', 'data')], prevent_initial_call=True)
def generate_logs_pdf(n_clicks, id_ponto, logs_filtrados):
    if not n_clicks or not id_ponto or not logs_filtrados: return dash.no_update
    config = PONTOS_DE_ANALISE.get(id_ponto, {"nome": "Ponto"})
    pdf_buffer = gerador_pdf.criar_relatorio_logs_em_memoria(config['nome'], logs_filtrados)
    return dcc.send_bytes(lambda f: f.write(pdf_buffer.getvalue()), f"Logs_{config['nome']}_{pd.Timestamp.now().strftime('%Y%m%d')}.pdf")


@app.callback([Output('pdf-task-id-store', 'data'), Output('pdf-check-interval', 'disabled'),
               Output('report-status-indicator', 'children'), Output('btn-pdf-especifico', 'disabled'),
               Output('btn-excel-especifico', 'disabled')], Input('btn-pdf-especifico', 'n_clicks'),
              [State('pdf-date-picker', 'start_date'), State('pdf-date-picker', 'end_date'),
               State('store-id-ponto-ativo', 'data')], prevent_initial_call=True)
def trigger_pdf_generation(n_clicks, start_date, end_date, id_ponto):
    if not n_clicks: return dash.no_update, True, dash.no_update, dash.no_update, dash.no_update
    task_id = str(uuid.uuid4())
    thread = Thread(target=gerador_pdf.thread_gerar_pdf, args=(task_id, start_date, end_date, id_ponto))
    thread.start()
    return task_id, False, html.Div([dbc.Spinner(size="sm"), " Gerando PDF..."]), True, True


@app.callback(
    [Output('download-pdf-especifico', 'data'), Output('pdf-check-interval', 'disabled', allow_duplicate=True),
     Output('report-status-indicator', 'children', allow_duplicate=True), Output('alert-pdf-error', 'is_open'),
     Output('alert-pdf-generic-error', 'is_open'), Output('btn-pdf-especifico', 'disabled', allow_duplicate=True),
     Output('btn-excel-especifico', 'disabled', allow_duplicate=True)], Input('pdf-check-interval', 'n_intervals'),
    State('pdf-task-id-store', 'data'), prevent_initial_call=True)
def check_pdf_status(n, task_id):
    if not task_id: return dash.no_update, True, dash.no_update, False, False, dash.no_update, dash.no_update
    with PDF_CACHE_LOCK:
        task = PDF_CACHE.get(task_id)
    if task:
        with PDF_CACHE_LOCK:
            del PDF_CACHE[task_id]
        if task["status"] == "concluido":
            return dcc.send_bytes(lambda f: f.write(task["data"]), task["filename"]), True, None, False, False, False, False
        else:
            is_no_data = "Sem dados" in task["message"]
            return dash.no_update, True, None, is_no_data, not is_no_data, False, False
    return dash.no_update, False, dash.no_update, False, False, dash.no_update, dash.no_update


@app.callback([Output('excel-task-id-store', 'data'), Output('excel-check-interval', 'disabled'),
               Output('report-status-indicator', 'children', allow_duplicate=True),
               Output('btn-pdf-especifico', 'disabled', allow_duplicate=True),
               Output('btn-excel-especifico', 'disabled', allow_duplicate=True)],
              Input('btn-excel-especifico', 'n_clicks'),
              [State('pdf-date-picker', 'start_date'), State('pdf-date-picker', 'end_date'),
               State('store-id-ponto-ativo', 'data')], prevent_initial_call=True)
def trigger_excel_generation(n_clicks, start_date, end_date, id_ponto):
    if not n_clicks: return dash.no_update, True, dash.no_update, dash.no_update, dash.no_update
    task_id = str(uuid.uuid4())
    thread = Thread(target=gerador_pdf.thread_gerar_excel, args=(task_id, start_date, end_date, id_ponto))
    thread.start()
    return task_id, False, html.Div([dbc.Spinner(size="sm"), " Gerando Excel..."]), True, True


@app.callback(
    [Output('download-excel-especifico', 'data'), Output('excel-check-interval', 'disabled', allow_duplicate=True),
     Output('report-status-indicator', 'children', allow_duplicate=True),
     Output('alert-pdf-error', 'is_open', allow_duplicate=True),
     Output('alert-pdf-generic-error', 'is_open', allow_duplicate=True),
     Output('btn-pdf-especifico', 'disabled', allow_duplicate=True),
     Output('btn-excel-especifico', 'disabled', allow_duplicate=True)], Input('excel-check-interval', 'n_intervals'),
    State('excel-task-id-store', 'data'), prevent_initial_call=True)
def check_excel_status(n, task_id):
    if not task_id: return dash.no_update, True, dash.no_update, False, False, dash.no_update, dash.no_update
    with EXCEL_CACHE_LOCK:
        task = EXCEL_CACHE.get(task_id)
    if task:
        with EXCEL_CACHE_LOCK:
            del EXCEL_CACHE[task_id]
        if task["status"] == "concluido":
            return dcc.send_bytes(lambda f: f.write(task["data"]), task["filename"]), True, None, False, False, False, False
        else:
            is_no_data = "Sem dados" in task["message"]
            return dash.no_update, True, None, is_no_data, not is_no_data, False, False
    return dash.no_update, False, dash.no_update, False, False, dash.no_update, dash.no_update


@app.callback(Output('dynamic-accumulated-output', 'children'),
              [Input('graph-time-selector', 'value'),
               Input('url-raiz', 'pathname')])
def update_dynamic_accumulated_text(selected_hours, pathname):
    if selected_hours == 72 or not pathname.startswith('/ponto/'): return None
    try:
        id_ponto = pathname.split('/')[-1]
        # Busca apenas os dados necessários para o cálculo
        df_ponto = data_source.read_data_from_sqlite(id_ponto=id_ponto, last_hours=selected_hours)
        if df_ponto.empty: return None
        
        df_ponto.loc[:, 'timestamp'] = pd.to_datetime(df_ponto['timestamp'])
        df_ponto.loc[:, 'chuva_mm'] = pd.to_numeric(df_ponto['chuva_mm'], errors='coerce').fillna(0)
        
        df_acumulado = processamento.calcular_acumulado_rolling(df_ponto, horas=selected_hours)
        if df_acumulado.empty: return None
        
        valor_acumulado = df_acumulado.iloc[-1]['chuva_mm']
        if pd.isna(valor_acumulado): return None
        
        return html.P(f"Acumulado ({selected_hours}h): {valor_acumulado:.1f} mm", className="mb-0 ms-3",
                      style={'fontSize': '0.85rem', 'fontWeight': 'bold', 'color': '#555'})
    except Exception:
        return None