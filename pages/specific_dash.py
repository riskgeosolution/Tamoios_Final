import dash
from dash import html, dcc, callback, Input, Output, State
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from datetime import datetime
from io import StringIO
import base64
import plotly.express as px
import json
import traceback
import io  # Importação necessária para o io.BytesIO

# --- CORREÇÃO CRÍTICA: IMPORTAÇÃO DO APP NO TOPO ---
from app import app, TEMPLATE_GRAFICO_MODERNO
# --- FIM DA CORREÇÃO ---

from config import (
    PONTOS_DE_ANALISE, CONSTANTES_PADRAO, FREQUENCIA_API_SEGUNDOS,
    RISCO_MAP, STATUS_MAP_HIERARQUICO
)
import processamento
import gerador_pdf
import data_source  # Necessário para COLUNAS_HISTORICO

# --- Mapas de Cores ---
CORES_ALERTAS_CSS = {
    "verde": "green",
    "amarelo": "#FFD700",
    "laranja": "#fd7e14",
    "vermelho": "#dc3545",
    "cinza": "grey"
}
CORES_UMIDADE = {
    '1m': CORES_ALERTAS_CSS["verde"],
    '2m': CORES_ALERTAS_CSS["amarelo"],
    '3m': CORES_ALERTAS_CSS["vermelho"]
}


# --- Layout da Página Específica ---
def get_layout():
    """ Retorna o layout da página de dashboard específico. """
    opcoes_tempo = [{'label': f'Últimas {h} horas', 'value': h} for h in [1, 3, 6, 12, 18, 24, 72, 84, 96]] + [
        {'label': 'Todo o Histórico (Máx 7 dias)', 'value': 7 * 24}]

    return dbc.Container([
        dcc.Store(id='store-id-ponto-ativo'),
        dcc.Store(id='store-logs-filtrados'),  # Store para logs PDF/Excel

        # TÍTULO DA ESTAÇÃO (Centralizado)
        html.Div(id='specific-dash-title', className="my-3 text-center"),

        dbc.Row(id='specific-dash-cards', children=[dbc.Spinner(size="lg")]),

        dbc.Row([
            dbc.Col(dbc.Label("Período (Gráficos):"), width="auto"),
            dbc.Col(
                dcc.Dropdown(
                    id='graph-time-selector',
                    options=opcoes_tempo,
                    value=72,
                    clearable=False,
                    searchable=False
                ),
                width=12, lg=4
            )
        ], align="center", className="my-3"),

        dbc.Row(id='specific-dash-graphs', children=[dbc.Spinner(size="lg")], className="my-4"),

        # --- Seção de Relatórios e Eventos ---
        dbc.Row([
            dbc.Col([
                html.H5("Relatórios e Eventos", className="mb-3"),

                # --- Card de Data Picker e PDF ---
                dbc.Card(dbc.CardBody([
                    html.H6("Gerar Relatórios (PDF/Excel)", className="card-title"),  # Título atualizado
                    dcc.DatePickerRange(
                        id='pdf-date-picker',
                        start_date=(pd.Timestamp.now() - pd.Timedelta(days=7)).date(),
                        end_date=pd.Timestamp.now().date(),
                        display_format='DD/MM/YYYY',
                        className="mb-3 w-100"
                    ),
                    html.Br(),

                    # --- INÍCIO DA ALTERAÇÃO (BOTÕES PDF E EXCEL LADO A LADO) ---
                    html.Div([
                        # Botão PDF
                        dcc.Loading(id="loading-pdf", type="default", children=[
                            dbc.Button("Gerar PDF", id='btn-pdf-especifico', color="primary",
                                       size="sm", className="me-2"),  # me-2 = margin end 2
                            dcc.Download(id='download-pdf-especifico')
                        ]),

                        # NOVO Botão Excel
                        dcc.Loading(id="loading-excel", type="default", children=[
                            dbc.Button("Gerar Excel", id='btn-excel-especifico', color="success",
                                       size="sm"),  # success = green
                            dcc.Download(id='download-excel-especifico')
                        ])
                        # d-flex (torna-o flexível) e justify-content-center (centraliza)
                    ], className="d-flex justify-content-center"),
                    # --- FIM DA ALTERAÇÃO ---

                    # Alerta de erro (reutilizado para PDF e Excel)
                    dbc.Alert(
                        "Não há dados neste período para gerar o relatório.",
                        id="alert-pdf-error",
                        color="danger",
                        is_open=False,
                        dismissable=True,
                        className="mt-3",
                    ),
                ]), className="shadow-sm mb-4"),

                # --- Botão de Logs ---
                dbc.Card(dbc.CardBody([
                    html.H6("Logs de Eventos", className="card-title"),
                    dbc.Button("Ver Histórico de Eventos do Ponto", id='btn-ver-logs', color="secondary",
                               size="sm")
                ], className="text-center"), className="shadow-sm"),

            ]),
        ], justify="center", className="mb-5"),

        # --- Modal de Logs (Inicialmente oculto) ---
        dbc.Modal([
            dbc.ModalHeader("Histórico de Eventos do Ponto"),
            dbc.ModalBody(dcc.Loading(children=[
                html.Div(id='modal-logs-content', style={'whiteSpace': 'pre-wrap', 'wordWrap': 'break-word'})
            ])),
            dbc.ModalFooter([
                # Botão para Gerar PDF dos Logs
                dcc.Loading(id="loading-pdf-logs", type="default", children=[
                    dbc.Button("Gerar PDF dos Logs", id='btn-pdf-logs', color="success", className="me-2"),
                    dcc.Download(id='download-pdf-logs')
                ]),
                dbc.Button("Fechar", id='btn-fechar-logs', color="secondary")
            ]),
        ], id='modal-logs', is_open=False, size="lg"),

    ], fluid=True)


# --- Callbacks da Página Específica ---

# Callback para definir o TÍTULO (Estação KM)
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
        nome_km = config['nome']
        nome_estacao_formatado = f"Estação {nome_km}"
        return html.H3(nome_estacao_formatado, style={'color': '#000000', 'font-weight': 'bold'})
    except Exception:
        return html.H3("Detalhes da Estação", style={'color': '#000000', 'font-weight': 'bold'})


# Callback 1: Atualiza os cards e gráficos
@app.callback(
    [
        Output('specific-dash-cards', 'children'),
        Output('specific-dash-graphs', 'children'),
        Output('store-id-ponto-ativo', 'data')
    ],
    [
        Input('url-raiz', 'pathname'),
        Input('store-dados-sessao', 'data'),
        Input('store-ultimo-status', 'data'),
        Input('graph-time-selector', 'value')
    ]
)
def update_specific_dashboard(pathname, dados_json, status_json, selected_hours):
    # Esta função continua a ler do CSV (store-dados-sessao) para velocidade, como solicitado.

    if not dados_json or not status_json or not pathname.startswith('/ponto/') or selected_hours is None:
        return dash.no_update, dash.no_update, dash.no_update

    id_ponto = ""
    config = {}
    try:
        id_ponto = pathname.split('/')[-1]
        config = PONTOS_DE_ANALISE[id_ponto]
    except KeyError:
        return "Ponto não encontrado", "Erro: Ponto inválido.", None

    constantes_ponto = config.get('constantes', CONSTANTES_PADRAO)
    base_1m = constantes_ponto.get('UMIDADE_BASE_1M', CONSTANTES_PADRAO['UMIDADE_BASE_1M'])
    base_2m = constantes_ponto.get('UMIDADE_BASE_2M', CONSTANTES_PADRAO['UMIDADE_BASE_2M'])
    base_3m = constantes_ponto.get('UMIDADE_BASE_3M', CONSTANTES_PADRAO['UMIDADE_BASE_3M'])

    try:
        df_completo = pd.read_json(StringIO(dados_json), orient='split')

        # status_json JÁ É UM DICIONÁRIO
        status_atual_dict = status_json

    except Exception as e:
        print(f"Erro ao ler JSON (specific_dash): {e}")
        return "Erro ao carregar dados.", "", id_ponto

    df_ponto = df_completo[df_completo['id_ponto'] == id_ponto].copy()

    if df_ponto.empty:
        return dbc.Alert("Sem dados históricos para este ponto.", color="warning", className="m-3"), "", id_ponto

    # --- TIPAGEM E CONVERSÃO DE FUSO HORÁRIO (ROBUSTO) ---
    df_ponto.loc[:, 'timestamp'] = pd.to_datetime(df_ponto['timestamp'])

    # 1. Trata fuso horário
    if df_ponto['timestamp'].dt.tz is None:
        df_ponto.loc[:, 'timestamp'] = df_ponto['timestamp'].dt.tz_localize('UTC')
    else:
        df_ponto.loc[:, 'timestamp'] = df_ponto['timestamp'].dt.tz_convert('UTC')

    # 2. Adiciona coluna local para visualização
    df_ponto.loc[:, 'timestamp_local'] = df_ponto['timestamp'].dt.tz_convert('America/Sao_Paulo')

    # 3. Tipagem numérica
    df_ponto.loc[:, 'chuva_mm'] = pd.to_numeric(df_ponto['chuva_mm'], errors='coerce')
    df_ponto.loc[:, 'precipitacao_acumulada_mm'] = pd.to_numeric(df_ponto['precipitacao_acumulada_mm'], errors='coerce')
    df_ponto.loc[:, 'umidade_1m_perc'] = pd.to_numeric(df_ponto['umidade_1m_perc'], errors='coerce')
    df_ponto.loc[:, 'umidade_2m_perc'] = pd.to_numeric(df_ponto['umidade_2m_perc'], errors='coerce')
    df_ponto.loc[:, 'umidade_3m_perc'] = pd.to_numeric(df_ponto['umidade_3m_perc'], errors='coerce')
    # --- FIM DA CONVERSÃO DE FUSO HORÁRIO E TIPAGEM ---

    # Pega o status geral (calculado pelo worker)
    status_geral_ponto_txt = status_atual_dict.get(id_ponto, "INDEFINIDO")
    risco_geral = RISCO_MAP.get(status_geral_ponto_txt, -1)
    status_geral_texto, status_geral_cor_bootstrap = STATUS_MAP_HIERARQUICO.get(risco_geral, ("INDEFINIDO", "secondary",
                                                                                              "bg-secondary"))[:2]

    # Aplica a classe CSS para a cor de fundo do card
    if status_geral_cor_bootstrap == "warning":
        card_class_color = "bg-warning"
    elif status_geral_cor_bootstrap == "orange":
        card_class_color = "bg-orange"
    elif status_geral_cor_bootstrap == "danger":
        card_class_color = "bg-danger"
    else:
        card_class_color = "bg-" + status_geral_cor_bootstrap

    # --- INÍCIO DA CORREÇÃO (FILTRO DE TEMPO) ---
    # Em vez de usar .tail(), filtramos pelo período de tempo real
    ultimo_timestamp_no_df = df_ponto['timestamp_local'].max()
    limite_tempo = ultimo_timestamp_no_df - pd.Timedelta(hours=selected_hours)
    df_ponto_plot = df_ponto[df_ponto['timestamp_local'] >= limite_tempo].copy()
    n_horas_titulo = selected_hours
    # --- FIM DA CORREÇÃO ---

    # Recalcula o acumulado de 72h (rolling) para o gráfico
    df_chuva_72h_completo = processamento.calcular_acumulado_rolling(df_ponto, horas=72)

    # 3. Calcula o acumulado DINÂMICO para o Gráfico (baseado no selected_hours)
    df_chuva_FILTRO_completo = processamento.calcular_acumulado_rolling(df_ponto, horas=selected_hours)

    # 4. Filtra o DataFrame do gráfico para o período de tempo
    df_chuva_FILTRO_plot = df_chuva_FILTRO_completo[
        df_chuva_FILTRO_completo['timestamp'] >= df_ponto_plot['timestamp'].min()
        ].copy()

    # Pega o último dado para os cards
    try:
        ultimo_dado = df_ponto.sort_values('timestamp').iloc[-1]

        # O Card de "Chuva 72h" usa o cálculo fixo de 72h
        if not df_chuva_72h_completo.empty:
            ultima_chuva_72h = df_chuva_72h_completo.iloc[-1]['chuva_mm']
        else:
            ultima_chuva_72h = 0.0

        umidade_1m_atual = ultimo_dado.get('umidade_1m_perc', base_1m)
        umidade_2m_atual = ultimo_dado.get('umidade_2m_perc', base_2m)
        umidade_3m_atual = ultimo_dado.get('umidade_3m_perc', base_3m)

        # Pega as bases dinâmicas (se existirem)
        base_1m = ultimo_dado.get('base_1m', base_1m)
        base_2m = ultimo_dado.get('base_2m', base_2m)
        base_3m = ultimo_dado.get('base_3m', base_3m)

        if pd.isna(ultima_chuva_72h): ultima_chuva_72h = 0.0
        if pd.isna(umidade_1m_atual): umidade_1m_atual = base_1m
        if pd.isna(umidade_2m_atual): umidade_2m_atual = base_2m
        if pd.isna(umidade_3m_atual): umidade_3m_atual = base_3m
    except IndexError:
        return "Dados insuficientes.", "", id_ponto

    # Cores Individuais para Card de Umidade (baseado no delta)
    from config import DELTA_TRIGGER_UMIDADE
    css_color_s1 = CORES_ALERTAS_CSS["amarelo"] if (umidade_1m_atual - base_1m) >= DELTA_TRIGGER_UMIDADE else \
        CORES_ALERTAS_CSS["verde"]
    css_color_s2 = CORES_ALERTAS_CSS["laranja"] if (umidade_2m_atual - base_2m) >= DELTA_TRIGGER_UMIDADE else \
        CORES_ALERTAS_CSS["verde"]
    css_color_s3 = CORES_ALERTAS_CSS["vermelho"] if (umidade_3m_atual - base_3m) >= DELTA_TRIGGER_UMIDADE else \
        CORES_ALERTAS_CSS["verde"]

    # Layout dos Cards
    layout_cards = [
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H5("Status Atual", style={'color': '#000000', 'font-weight': 'bold'}),
            html.P(status_geral_texto, className="fs-3", style={'color': '#000000', 'font-weight': 'bold'})
        ]),
            className=f"shadow h-100 {card_class_color}",
        ), xs=12, md=4, className="mb-4"),

        dbc.Col(dbc.Card(
            dbc.CardBody([html.H5("Chuva 72h"), html.P(f"{ultima_chuva_72h:.1f} mm", className="fs-3 fw-bold")]),
            className="shadow h-100 bg-white"), xs=12, md=4, className="mb-4"),

        dbc.Col(dbc.Card(
            dbc.CardBody([
                html.H5("Umidade do Solo (%)", className="mb-3"),
                dbc.Row([
                    dbc.Col(
                        html.P([
                            html.Span(f"{umidade_1m_atual:.1f}", className="fs-3 fw-bold",
                                      style={'color': css_color_s1}),
                            html.Span(" (1m)", className="small", style={'color': css_color_s1})
                        ], className="mb-0 text-center"),
                        width=4
                    ),
                    dbc.Col(
                        html.P([
                            html.Span(f"{umidade_2m_atual:.1f}", className="fs-3 fw-bold",
                                      style={'color': css_color_s2}),
                            html.Span(" (2m)", className="small", style={'color': css_color_s2})
                        ], className="mb-0 text-center"),
                        width=4
                    ),
                    dbc.Col(
                        html.P([
                            html.Span(f"{umidade_3m_atual:.1f}", className="fs-3 fw-bold",
                                      style={'color': css_color_s3}),
                            html.Span(" (3m)", className="small", style={'color': css_color_s3})
                        ], className="mb-0 text-center"),
                        width=4
                    ),
                ], justify="around")
            ]),
            className="shadow h-100 bg-white"), xs=12, md=4, className="mb-4"),
    ]

    # Corrigindo com .loc
    if 'timestamp' in df_chuva_FILTRO_plot.columns:
        if df_chuva_FILTRO_plot['timestamp'].dt.tz is None:
            df_chuva_FILTRO_plot.loc[:, 'timestamp'] = df_chuva_FILTRO_plot['timestamp'].dt.tz_localize('UTC')

        df_chuva_FILTRO_plot.loc[:, 'timestamp_local'] = df_chuva_FILTRO_plot['timestamp'].dt.tz_convert(
            'America/Sao_Paulo')
    else:
        df_chuva_FILTRO_plot.loc[:, 'timestamp_local'] = df_chuva_FILTRO_plot['timestamp']

    # Gráfico de Chuva
    fig_chuva = make_subplots(specs=[[{"secondary_y": True}]])

    fig_chuva.add_trace(
        go.Bar(x=df_ponto_plot['timestamp_local'], y=df_ponto_plot['chuva_mm'], name='Pluviometria Horária (mm)',
               marker_color='#2C3E50', opacity=0.8), secondary_y=False)

    fig_chuva.add_trace(go.Scatter(x=df_chuva_FILTRO_plot['timestamp_local'], y=df_chuva_FILTRO_plot['chuva_mm'],
                                   name=f'Acumulada ({n_horas_titulo}h)',  # Nome dinâmico
                                   mode='lines', line=dict(color='#007BFF', width=2.5)), secondary_y=True)

    fig_chuva.update_layout(
        title_text=f"Pluviometria - Estação {config['nome']} ({n_horas_titulo}h)",
        template=TEMPLATE_GRAFICO_MODERNO,
        margin=dict(l=40, r=20, t=50, b=80),
        legend=dict(orientation="h", yanchor="bottom", y=-0.5, xanchor='center', x=0.5),
        xaxis_title="Data e Hora", yaxis_title="Pluviometria Horária (mm)",
        yaxis2_title=f"Acumulada ({n_horas_titulo}h)",
        hovermode="x unified", bargap=0.1)

    fig_chuva.update_xaxes(
        dtick=3 * 60 * 60 * 1000,
        tickformat="%d/%m %Hh",
        tickangle=-45
    )

    fig_chuva.update_yaxes(title_text="Pluviometria Horária (mm)", secondary_y=False);
    fig_chuva.update_yaxes(title_text=f"Acumulada ({n_horas_titulo}h)", secondary_y=True)

    # Gráfico de Umidade
    df_umidade = df_ponto_plot.melt(id_vars=['timestamp_local'],
                                    value_vars=['umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc'],
                                    var_name='Sensor', value_name='Umidade (%)')

    df_umidade['Sensor'] = df_umidade['Sensor'].replace({
        'umidade_1m_perc': '1m',
        'umidade_2m_perc': '2m',
        'umidade_3m_perc': '3m'
    })

    fig_umidade = px.line(df_umidade, x='timestamp_local', y='Umidade (%)', color='Sensor',
                          title=f"Variação da Umidade do Solo - Estação {config['nome']} ({n_horas_titulo}h)",
                          color_discrete_map=CORES_UMIDADE)
    fig_umidade.update_traces(line=dict(width=3));

    fig_umidade.update_layout(template=TEMPLATE_GRAFICO_MODERNO,
                              margin=dict(l=40, r=20, t=40, b=80),
                              legend=dict(orientation="h", yanchor="bottom", y=-0.5, xanchor="center", x=0.5),
                              xaxis_title="Data e Hora",
                              yaxis_title="Umidade do Solo (%)")

    fig_umidade.update_xaxes(
        dtick=3 * 60 * 60 * 1000,
        tickformat="%d/%m %Hh",
        tickangle=-45
    )

    # Layout Lado a Lado
    layout_graficos = [
        dbc.Col(dbc.Card(dbc.CardBody(dcc.Graph(figure=fig_chuva)), className="shadow-sm"), width=12, className="mb-4"),
        dbc.Col(dbc.Card(dbc.CardBody(dcc.Graph(figure=fig_umidade)), className="shadow-sm"), width=12,
                className="mb-4"), ]

    return layout_cards, layout_graficos, id_ponto


# --- Callbacks de PDF/Logs ---

# Callback para Abrir/Fechar o Modal de Logs
@app.callback(
    Output('modal-logs', 'is_open'),
    [Input('btn-ver-logs', 'n_clicks'), Input('btn-fechar-logs', 'n_clicks')],
    [State('modal-logs', 'is_open')],
    prevent_initial_call=True
)
def toggle_logs_modal(n_open, n_close, is_open):
    ctx = dash.callback_context
    if not ctx.triggered:
        return is_open

    button_id = ctx.triggered[0]['prop_id'].split('.')[0]

    if button_id == 'btn-ver-logs':
        return not is_open  # Alterna o estado se for o botão 'ver'
    elif button_id == 'btn-fechar-logs':
        return False  # Fecha o modal

    return is_open


# --- INÍCIO DA ALTERAÇÃO (Filtro de Logs + Formato de Data/Hora) ---
# Callback para Carregar o Conteúdo do Log no Modal
@app.callback(
    [Output('modal-logs-content', 'children'),
     Output('store-logs-filtrados', 'data')],  # Guarda os logs para o PDF
    Input('modal-logs', 'is_open'),  # Disparado quando o modal abre/fecha
    State('store-id-ponto-ativo', 'data'),
    State('store-logs-sessao', 'data')
)
def load_logs_content(is_open, id_ponto, logs_json):
    if not is_open or not id_ponto or not logs_json:
        return dash.no_update if not is_open else "Nenhum evento registrado.", dash.no_update

    try:
        if isinstance(logs_json, str):
            logs_list = logs_json.split('\n')
        elif isinstance(logs_json, list):
            logs_list = logs_json
        else:
            logs_list = json.loads(logs_json)

        logs_list = [log.strip() for log in logs_list if log.strip()]

        if not logs_list:
            return "Nenhum evento registrado.", []

        # 1. Filtra logs por ponto ou GERAL
        logs_ponto_ou_geral = [log for log in logs_list if f"| {id_ponto} |" in log or "| GERAL |" in log]

        # 2. NOVO FILTRO: Esconde mensagens de "API: Sucesso"
        logs_filtrados_raw = [
            log for log in logs_ponto_ou_geral
            if "API: Sucesso" not in log
        ]

        if not logs_filtrados_raw:
            return f"Nenhum evento crítico ou mudança de status registrada para o ponto {id_ponto}.", []

        logs_formatados_display = []
        for log_str in reversed(logs_filtrados_raw):  # Mostra os mais recentes primeiro
            parts = log_str.split('|')
            if len(parts) < 3:
                logs_formatados_display.append(html.P(log_str))
                continue

            # --- INÍCIO DA FORMATAÇÃO DE DATA/HORA ---
            timestamp_str_utc_iso = parts[0].strip()
            ponto_str = parts[1].strip()
            msg_str = "|".join(parts[2:]).strip()

            try:
                # Converte a string ISO (que já é UTC) para um datetime object
                dt_utc = pd.to_datetime(timestamp_str_utc_iso).tz_localize('UTC')
                # Converte para o fuso horário de São Paulo
                dt_local = dt_utc.tz_convert('America/Sao_Paulo')
                # Formata no padrão brasileiro
                timestamp_formatado = dt_local.strftime('%d/%m/%Y %H:%M:%S')
            except Exception as e:
                # Se falhar, apenas usa a data original (sem o +00:00 e 'T')
                timestamp_formatado = timestamp_str_utc_iso.split('+')[0].replace('T', ' ')
            # --- FIM DA FORMATAÇÃO DE DATA/HORA ---

            cor = "black"
            if "ERRO" in msg_str:
                cor = "red"
            elif "AVISO" in msg_str:
                cor = "#E69B00"
            elif "MUDANÇA" in msg_str:
                cor = "blue"

            logs_formatados_display.append(html.P([
                html.Strong(f"{timestamp_formatado} [{ponto_str}]: ", style={'color': cor}),
                html.Span(msg_str, style={'color': cor})
            ], className="mb-1"))

        logs_div = html.Div(logs_formatados_display, style={'maxHeight': '400px', 'overflowY': 'auto'})

        # Retorna o componente Div e os logs brutos filtrados para o store
        return logs_div, logs_filtrados_raw

    except Exception as e:
        print(f"ERRO CRÍTICO no Carregamento de Logs:")
        traceback.print_exc()
        return f"Erro ao formatar logs: {e}", []


# --- FIM DA CORREÇÃO (Logs) ---


# --- Callback para Gerar e Baixar PDF dos Dados Históricos ---
@app.callback(
    [Output('download-pdf-especifico', 'data'),
     Output('alert-pdf-error', 'is_open', allow_duplicate=True)],
    Input('btn-pdf-especifico', 'n_clicks'),
    [
        State('pdf-date-picker', 'start_date'),
        State('pdf-date-picker', 'end_date'),
        State('store-id-ponto-ativo', 'data'),
        State('store-ultimo-status', 'data')
    ],
    prevent_initial_call=True
)
def generate_data_pdf(n_clicks, start_date, end_date, id_ponto, status_json):
    if n_clicks is None or not id_ponto:
        return dash.no_update, False

    try:
        # 1. Preparar datas
        start_dt = pd.to_datetime(start_date).tz_localize('UTC')

        # --- INÍCIO DA CORREÇÃO (FUSO LOCAL) ---
        end_dt_naive = pd.to_datetime(end_date)
        end_dt_local = end_dt_naive.tz_localize('America/Sao_Paulo')
        end_dt_local_final = end_dt_local + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        end_dt = end_dt_local_final.tz_convert('UTC')
        # --- FIM DA CORREÇÃO ---

        # 2. Ler dados DIRETAMENTE DO SQLITE
        df_filtrado = data_source.read_data_from_sqlite(id_ponto, start_dt, end_dt)

        # 2b. Limpar dados nulos
        df_filtrado = df_filtrado.dropna(subset=['timestamp'])

        if df_filtrado.empty:
            print("LOG PDF: Sem dados no período selecionado (SQLite).")
            return dash.no_update, True

        # --- CORREÇÃO (Fuso Horário Robusto para PDF) ---
        if df_filtrado['timestamp'].dt.tz is None:
            print("LOG PDF: Detectados timestamps 'naive'. Assumindo UTC.")
            try:
                df_filtrado['timestamp'] = pd.to_datetime(df_filtrado['timestamp']).dt.tz_localize('UTC')
            except Exception as e_tz:
                print(f"LOG PDF: Falha ao localizar timestamps 'naive' (pode ser misto): {e_tz}")
                df_filtrado['timestamp'] = pd.to_datetime(df_filtrado['timestamp']).dt.tz_localize('UTC',
                                                                                                   ambiguous='infer',
                                                                                                   nonexistent='shift_forward')

        df_filtrado.loc[:, 'timestamp_local'] = df_filtrado['timestamp'].dt.tz_convert('America/Sao_Paulo')
        # --- FIM DA CORREÇÃO ---

        # 3. Configurações e Status
        config = PONTOS_DE_ANALISE.get(id_ponto, {"nome": "Ponto"})
        status_atual_dict = status_json

        # --- CORREÇÃO: status_geral_ponto_txt deve ser definido para ser usado ---
        status_geral_ponto_txt = status_atual_dict.get(id_ponto, "INDEFINIDO")
        risco_geral = RISCO_MAP.get(status_geral_ponto_txt, -1)
        status_texto, status_cor = STATUS_MAP_HIERARQUICO.get(risco_geral, ("INDEFINIDO", "secondary"))[:2]
        # --- FIM DA CORREÇÃO ---

        # 5. Calcular Acumulado
        df_chuva_72h_pdf = processamento.calcular_acumulado_rolling(df_filtrado, horas=72)
        if 'timestamp' in df_chuva_72h_pdf.columns:
            if df_chuva_72h_pdf['timestamp'].dt.tz is None:
                df_chuva_72h_pdf.loc[:, 'timestamp'] = df_chuva_72h_pdf['timestamp'].dt.tz_localize('UTC')
            df_chuva_72h_pdf.loc[:, 'timestamp_local'] = df_chuva_72h_pdf['timestamp'].dt.tz_convert(
                'America/Sao_Paulo')
        else:
            df_chuva_72h_pdf = df_chuva_72h_pdf.copy()
            df_chuva_72h_pdf.loc[:, 'timestamp_local'] = df_chuva_72h_pdf['timestamp']

        # 6. Gerar Gráfico de Chuva
        fig_chuva_pdf = make_subplots(specs=[[{"secondary_y": True}]])
        fig_chuva_pdf.add_trace(
            go.Bar(x=df_filtrado['timestamp_local'], y=df_filtrado['chuva_mm'], name='Pluv. Horária (mm)',
                   marker_color='#2C3E50', opacity=0.8), secondary_y=False)

        fig_chuva_pdf.add_trace(go.Scatter(x=df_chuva_72h_pdf['timestamp_local'], y=df_chuva_72h_pdf['chuva_mm'],
                                           name='Acumulada (72h)', mode='lines',
                                           line=dict(color='#007BFF', width=2.5)), secondary_y=True)

        titulo_chuva = f"Pluviometria - Estação {config['nome']}"
        fig_chuva_pdf.update_layout(
            title_text=titulo_chuva, template=TEMPLATE_GRAFICO_MODERNO,
            margin=dict(l=40, r=20, t=50, b=80),
            legend=dict(orientation="h", yanchor="bottom", y=-0.5, xanchor='center', x=0.5)
        )

        # 7. Gerar Gráfico de Umidade
        df_umidade_pdf = df_filtrado.melt(id_vars=['timestamp_local'],
                                          value_vars=['umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc'],
                                          var_name='Sensor', value_name='Umidade (%)')
        df_umidade_pdf['Sensor'] = df_umidade_pdf['Sensor'].replace({
            'umidade_1m_perc': '1m', 'umidade_2m_perc': '2m', 'umidade_3m_perc': '3m'
        })
        fig_umidade_pdf = px.line(df_umidade_pdf, x='timestamp_local', y='Umidade (%)', color='Sensor',
                                  title=f"Variação da Umidade do Solo - Estação {config['nome']}",
                                  color_discrete_map=CORES_UMIDADE)
        fig_umidade_pdf.update_layout(
            template=TEMPLATE_GRAFICO_MODERNO, margin=dict(l=40, r=20, t=40, b=80),
            legend=dict(orientation="h", yanchor="bottom", y=-0.5, xanchor="center", x=0.5)
        )

        # 8. Chamar a função de gerar PDF
        pdf_buffer = gerador_pdf.criar_relatorio_em_memoria(
            df_filtrado, fig_chuva_pdf, fig_umidade_pdf, status_texto, status_cor
        )
        nome_arquivo = f"Relatorio_{config['nome']}_{datetime.now().strftime('%Y%m%d')}.pdf"
        print(f"LOG PDF: PDF gerado com sucesso (COM GRÁFICOS, via SQLite). Arquivo: {nome_arquivo}")

        # 9. Download
        pdf_output = io.BytesIO(pdf_buffer)
        # O retorno do dcc.send_bytes deve ser o primeiro item da tupla de retorno
        return dcc.send_bytes(pdf_output.read(), nome_arquivo, type="application/pdf"), False

    except Exception as e:
        print(f"ERRO CRÍTICO no Callback PDF:")
        traceback.print_exc()
        # Retorna dash.no_update e abre o alerta de erro
        return dash.no_update, True


# --- Callback para Gerar e Baixar PDF dos Logs ---
@app.callback(
    Output('download-pdf-logs', 'data'),
    Input('btn-pdf-logs', 'n_clicks'),
    [
        State('store-id-ponto-ativo', 'data'),
        State('store-logs-filtrados', 'data')
    ],
    prevent_initial_call=True
)
def generate_logs_pdf(n_clicks, id_ponto, logs_filtrados):
    if n_clicks is None or not id_ponto or not logs_filtrados:
        return dash.no_update

    try:
        config = PONTOS_DE_ANALISE.get(id_ponto, {"nome": "Ponto"})

        # Gera o PDF dos logs usando o gerador_pdf
        pdf_buffer = gerador_pdf.criar_relatorio_logs_em_memoria(config['nome'], logs_filtrados)

        nome_arquivo = f"Logs_{config['nome']}_{datetime.now().strftime('%Y%m%d')}.pdf"
        print(f"LOG PDF: PDF de logs gerado com sucesso. Arquivo: {nome_arquivo}")

        pdf_output = io.BytesIO(pdf_buffer)
        return dcc.send_bytes(pdf_output.read(), nome_arquivo, type="application/pdf")

    except Exception as e:
        print(f"ERRO CRÍTICO no Callback PDF de Logs:")
        traceback.print_exc()
        return dash.no_update


# --- Callback para Gerar e Baixar Excel dos Dados Históricos ---
@app.callback(
    [Output('download-excel-especifico', 'data'),
     Output('alert-pdf-error', 'is_open', allow_duplicate=True)],  # Reutiliza o alerta
    Input('btn-excel-especifico', 'n_clicks'),
    [
        State('pdf-date-picker', 'start_date'),
        State('pdf-date-picker', 'end_date'),
        State('store-id-ponto-ativo', 'data')
    ],
    prevent_initial_call=True
)
def generate_data_excel(n_clicks, start_date, end_date, id_ponto):
    if n_clicks is None or not id_ponto:
        return dash.no_update, False

    try:
        # 1. Preparar datas
        start_dt = pd.to_datetime(start_date).tz_localize('UTC')

        # --- CORREÇÃO (FUSO LOCAL) ---
        end_dt_naive = pd.to_datetime(end_date)
        end_dt_local = end_dt_naive.tz_localize('America/Sao_Paulo')
        end_dt_local_final = end_dt_local + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        end_dt = end_dt_local_final.tz_convert('UTC')
        # --- FIM DA CORREÇÃO ---

        # 2. Ler dados DIRETAMENTE DO SQLITE
        df_filtrado = data_source.read_data_from_sqlite(id_ponto, start_dt, end_dt)
        df_filtrado = df_filtrado.dropna(subset=['timestamp'])

        if df_filtrado.empty:
            print("LOG EXCEL: Sem dados no período selecionado (SQLite).")
            return dash.no_update, True

        # --- Fuso Horário para Excel ---
        if df_filtrado['timestamp'].dt.tz is None:
            df_filtrado.loc[:, 'timestamp'] = pd.to_datetime(df_filtrado['timestamp']).dt.tz_localize('UTC')

        # Converte a coluna para o fuso horário local e formata
        df_filtrado.loc[:, 'Data/Hora (Local)'] = df_filtrado['timestamp'].dt.tz_convert(
            'America/Sao_Paulo').dt.strftime('%d/%m/%Y %H:%M:%S')

        # Remove a coluna original 'timestamp' (UTC)
        df_filtrado = df_filtrado.drop(columns=['timestamp'])

        # Renomeia as colunas restantes para o relatório (se necessário)
        colunas_renomeadas = {
            'id_ponto': 'ID Ponto',
            'chuva_mm': 'Chuva (mm/h)',
            'precipitacao_acumulada_mm': 'Precipitação Acumulada (mm)',
            'umidade_1m_perc': 'Umidade 1m (%)',
            'umidade_2m_perc': 'Umidade 2m (%)',
            'umidade_3m_perc': 'Umidade 3m (%)',
            'base_1m': 'Base Umidade 1m',
            'base_2m': 'Base Umidade 2m',
            'base_3m': 'Base Umidade 3m',
        }
        df_filtrado = df_filtrado.rename(columns=colunas_renomeadas)

        # Reordenar colunas
        colunas_ordenadas = ['ID Ponto', 'Data/Hora (Local)'] + [col for col in df_filtrado.columns if
                                                                 col not in ['ID Ponto', 'Data/Hora (Local)']]
        df_filtrado = df_filtrado[colunas_ordenadas]

        # 3. Gerar Excel em memória
        output = io.BytesIO()
        writer = pd.ExcelWriter(output, engine='xlsxwriter')
        df_filtrado.to_excel(writer, sheet_name='Dados Históricos', index=False)
        writer.close()  # Usa .close() em vez de .save() para compatibilidade pandas 2.x
        output.seek(0)

        # 4. Download
        config = PONTOS_DE_ANALISE.get(id_ponto, {"nome": "Ponto"})
        nome_arquivo = f"Dados_Historicos_{config['nome']}_{datetime.now().strftime('%Y%m%d')}.xlsx"
        print(f"LOG EXCEL: Excel gerado com sucesso. Arquivo: {nome_arquivo}")

        # Retorna o Excel para download
        return dcc.send_bytes(output.read(), nome_arquivo,
                              type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"), False

    except Exception as e:
        print(f"ERRO CRÍTICO no Callback Excel:")
        traceback.print_exc()
        return dash.no_update, True