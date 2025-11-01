# pages/specific_dash.py (CORRIGIDO: Título do gráfico e eixo Y da Umidade)

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

        # --- INÍCIO DA ALTERAÇÃO 1 (TÍTULO PRINCIPAL) ---
        # TÍTULO DA ESTAÇÃO (Centralizado)
        html.Div(id='specific-dash-title', className="my-3 text-center"),
        # --- FIM DA ALTERAÇÃO 1 ---

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
                    html.H6("Gerar PDF (Dados)", className="card-title"),
                    dcc.DatePickerRange(
                        id='pdf-date-picker',
                        start_date=(pd.Timestamp.now() - pd.Timedelta(days=7)).date(),
                        end_date=pd.Timestamp.now().date(),
                        display_format='DD/MM/YYYY',
                        className="mb-3 w-100"
                    ),
                    html.Br(),

                    # --- INÍCIO DA ALTERAÇÃO (BOTÃO PDF) ---
                    dcc.Loading(id="loading-pdf", type="default", children=[
                        html.Div([
                            dbc.Button("Gerar e Baixar PDF", id='btn-pdf-especifico', color="primary",
                                       size="sm"),
                            dcc.Download(id='download-pdf-especifico')
                        ], className="text-center")
                    ]),
                    # --- FIM DA ALTERAÇÃO ---

                    # Alerta de erro do PDF
                    dbc.Alert(
                        "Não há dados neste período para gerar o PDF.",
                        id="alert-pdf-error",
                        color="danger",
                        is_open=False,
                        dismissable=True,
                        className="mt-3",
                    ),
                ]), className="shadow-sm mb-4"),

                # --- Botão de Logs ---
                # --- INÍCIO DA ALTERAÇÃO (BOTÃO LOGS) ---
                dbc.Card(dbc.CardBody([
                    html.H6("Logs de Eventos", className="card-title"),
                    dbc.Button("Ver Histórico de Eventos do Ponto", id='btn-ver-logs', color="secondary",
                               size="sm")
                ], className="text-center"), className="shadow-sm"),
                # --- FIM DA ALTERAÇÃO ---

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
        # --- ALTERAÇÃO 2: Nome do Ponto ---
        nome_estacao_formatado = f"Estação {nome_km}"
        return html.H3(nome_estacao_formatado, style={'color': '#000000', 'font-weight': 'bold'})
        # --- FIM DA ALTERAÇÃO 2 ---
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

    # --- INÍCIO DA ALTERAÇÃO (CÁLCULO DINÂMICO) ---

    # 1. Filtra o DataFrame para o período selecionado (ex: 1h, 24h, 72h)
    ultimo_timestamp_no_df = df_ponto['timestamp_local'].max()
    limite_tempo = ultimo_timestamp_no_df - pd.Timedelta(hours=selected_hours)
    df_ponto_plot = df_ponto[df_ponto['timestamp_local'] >= limite_tempo].copy()
    n_horas_titulo = selected_hours

    # 2. Calcula o acumulado de 72h (FIXO) para o Card
    df_chuva_72h_completo = processamento.calcular_acumulado_rolling(df_ponto, horas=72)

    # 3. Calcula o acumulado DINÂMICO para o Gráfico (baseado no selected_hours)
    df_chuva_FILTRO_completo = processamento.calcular_acumulado_rolling(df_ponto, horas=selected_hours)

    # 4. Filtra o DataFrame do gráfico para o período de tempo
    df_chuva_FILTRO_plot = df_chuva_FILTRO_completo[
        df_chuva_FILTRO_completo['timestamp'] >= df_ponto_plot['timestamp'].min()
        ].copy()

    # --- FIM DA ALTERAÇÃO ---

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
                # --- ALTERAÇÃO 3: Título da Umidade ---
                html.H5("Umidade do Solo (%)", className="mb-3"),
                # --- FIM DA ALTERAÇÃO 3 ---
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

    # --- INÍCIO DA CORREÇÃO (SettingWithCopyWarning) ---
    # Corrigindo com .loc
    if 'timestamp' in df_chuva_FILTRO_plot.columns:
        if df_chuva_FILTRO_plot['timestamp'].dt.tz is None:
            df_chuva_FILTRO_plot.loc[:, 'timestamp'] = df_chuva_FILTRO_plot['timestamp'].dt.tz_localize('UTC')

        df_chuva_FILTRO_plot.loc[:, 'timestamp_local'] = df_chuva_FILTRO_plot['timestamp'].dt.tz_convert(
            'America/Sao_Paulo')
    else:
        # Este else é seguro, pois 'timestamp_local' já existe
        df_chuva_FILTRO_plot.loc[:, 'timestamp_local'] = df_chuva_FILTRO_plot['timestamp']
    # --- FIM DA CORREÇÃO ---

    # Gráfico de Chuva
    fig_chuva = make_subplots(specs=[[{"secondary_y": True}]])

    # --- USANDO TIMESTAMP LOCAL PARA O EIXO X ---
    fig_chuva.add_trace(
        go.Bar(x=df_ponto_plot['timestamp_local'], y=df_ponto_plot['chuva_mm'], name='Pluviometria Horária (mm)',
               marker_color='#2C3E50', opacity=0.8), secondary_y=False)

    # --- INÍCIO DA ALTERAÇÃO (GRÁFICO DINÂMICO) ---
    fig_chuva.add_trace(go.Scatter(x=df_chuva_FILTRO_plot['timestamp_local'], y=df_chuva_FILTRO_plot['chuva_mm'],
                                   name=f'Acumulada ({n_horas_titulo}h)',  # Nome dinâmico
                                   mode='lines', line=dict(color='#007BFF', width=2.5)), secondary_y=True)

    fig_chuva.update_layout(
        # --- ALTERAÇÃO 1: TÍTULO DO GRÁFICO ---
        title_text=f"Pluviometria - Estação {config['nome']} ({n_horas_titulo}h)",
        # --- FIM DA ALTERAÇÃO 1 ---
        template=TEMPLATE_GRAFICO_MODERNO,
        # Aumentada a margem inferior (b=80)
        margin=dict(l=40, r=20, t=50, b=80),
        # Ajustada a posição Y da legenda para -0.5 (mais para baixo)
        legend=dict(orientation="h", yanchor="bottom", y=-0.5, xanchor='center', x=0.5),
        xaxis_title="Data e Hora", yaxis_title="Pluviometria Horária (mm)",
        yaxis2_title=f"Acumulada ({n_horas_titulo}h)",  # Título do eixo dinâmico
        hovermode="x unified", bargap=0.1)
    # --- FIM DA ALTERAÇÃO ---

    # --- EIXO X (3 EM 3 HORAS, DIAGONAL) ---
    fig_chuva.update_xaxes(
        dtick=3 * 60 * 60 * 1000,  # 3 horas em milissegundos
        tickformat="%d/%m %Hh",  # Formato Dia/Mês Hora (ex: 31/10 14h)
        tickangle=-45  # Rotaciona os labels
    )
    # --- FIM DA ALTERAÇÃO ---

    fig_chuva.update_yaxes(title_text="Pluviometria Horária (mm)", secondary_y=False);
    fig_chuva.update_yaxes(title_text=f"Acumulada ({n_horas_titulo}h)", secondary_y=True)  # Título do eixo dinâmico

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
                          # --- ALTERAÇÃO 2: TÍTULO DO GRÁFICO ---
                          title=f"Variação da Umidade do Solo - Estação {config['nome']} ({n_horas_titulo}h)",
                          # --- FIM DA ALTERAÇÃO 2 ---
                          color_discrete_map=CORES_UMIDADE)
    fig_umidade.update_traces(line=dict(width=3));

    # --- INÍCIO DA ALTERAÇÃO (LEGENDA, MARGEM E TÍTULO DO EIXO) ---
    fig_umidade.update_layout(template=TEMPLATE_GRAFICO_MODERNO,
                              # Aumentada a margem inferior (b=80)
                              margin=dict(l=40, r=20, t=40, b=80),
                              # Ajustada a posição Y da legenda
                              legend=dict(orientation="h", yanchor="bottom", y=-0.5, xanchor="center", x=0.5),
                              # Adicionado título do eixo X
                              xaxis_title="Data e Hora",
                              # --- ALTERAÇÃO 4: TÍTULO DO EIXO Y ---
                              yaxis_title="Umidade do Solo (%)")
    # --- FIM DA ALTERAÇÃO 4 ---
    # --- FIM DA ALTERAÇÃO ---

    # --- EIXO X (3 EM 3 HORAS, DIAGONAL) ---
    fig_umidade.update_xaxes(
        dtick=3 * 60 * 60 * 1000,  # 3 horas em milissegundos
        tickformat="%d/%m %Hh",  # Formato Dia/Mês Hora (ex: 31/10 14h)
        tickangle=-45  # Rotaciona os labels
    )
    # --- FIM DA ALTERAÇÃO ---

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


# Callback para Carregar o Conteúdo do Log no Modal
@app.callback(
    Output('modal-logs-content', 'children'),
    Input('modal-logs', 'is_open'),  # Disparado quando o modal abre/fecha
    State('store-id-ponto-ativo', 'data'),
    State('store-logs-sessao', 'data')
)
def load_logs_content(is_open, id_ponto, logs_json):
    if not is_open or not id_ponto or not logs_json:
        return dash.no_update if not is_open else "Nenhum evento registrado."

    try:
        # logs_json é a STRING BRUTA do eventos.log. Apenas dividimos por linha.
        if isinstance(logs_json, str):
            logs_list = logs_json.split('\n')
        elif isinstance(logs_json, list):
            logs_list = logs_json
        else:
            logs_list = json.loads(logs_json)

        logs_list = [log.strip() for log in logs_list if log.strip()]

        if not logs_list:
            return "Nenhum evento registrado."

        # Filtra logs para este ponto específico ou logs 'GERAL'
        logs_filtrados = [log for log in logs_list if f"| {id_ponto} |" in log or "| GERAL |" in log]

        if not logs_filtrados:
            return f"Nenhum evento específico registrado para o ponto {id_ponto}."

        # Formata os logs para exibição
        logs_formatados = []
        for log_str in reversed(logs_filtrados):  # Mostra os mais recentes primeiro
            parts = log_str.split('|')
            if len(parts) < 3:
                logs_formatados.append(html.P(log_str))
                continue

            timestamp_str = parts[0].strip()
            ponto_str = parts[1].strip()
            msg_str = "|".join(parts[2:]).strip()

            cor = "black"
            if "ERRO" in msg_str:
                cor = "red"
            elif "AVISO" in msg_str:
                cor = "#E69B00"
            elif "MUDANÇA" in msg_str:
                cor = "blue"

            logs_formatados.append(html.P([
                html.Strong(f"{timestamp_str} [{ponto_str}]: ", style={'color': cor}),
                html.Span(msg_str, style={'color': cor})
            ], className="mb-1"))

        return html.Div(logs_formatados, style={'maxHeight': '400px', 'overflowY': 'auto'})

    except Exception as e:
        print(f"ERRO CRÍTICO no Carregamento de Logs:")
        traceback.print_exc()
        return f"Erro ao formatar logs: {e}"


# --- INÍCIO DA ALTERAÇÃO (PDF COM GRÁFICOS) ---

# Callback para Gerar e Baixar PDF dos Dados Históricos
@app.callback(
    [Output('download-pdf-especifico', 'data'),
     Output('alert-pdf-error', 'is_open')],
    Input('btn-pdf-especifico', 'n_clicks'),
    [
        State('pdf-date-picker', 'start_date'),
        State('pdf-date-picker', 'end_date'),
        State('store-id-ponto-ativo', 'data'),
        State('store-dados-sessao', 'data'),
        State('store-ultimo-status', 'data')  # <-- Adicionado State para Status
    ],
    prevent_initial_call=True
)
def generate_data_pdf(n_clicks, start_date, end_date, id_ponto, dados_json, status_json):
    if n_clicks is None or not id_ponto or not dados_json:
        return dash.no_update, False

    try:
        # 1. Preparar dados
        df_completo = pd.read_json(StringIO(dados_json), orient='split')
        df_ponto = df_completo[df_completo['id_ponto'] == id_ponto].copy()

        # Conversão de datas para filtro
        start_dt = pd.to_datetime(start_date).tz_localize('UTC')
        end_dt = pd.to_datetime(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        end_dt = end_dt.tz_localize('UTC')

        # Filtrar o DataFrame
        df_ponto.loc[:, 'timestamp'] = pd.to_datetime(df_ponto['timestamp'])

        if df_ponto['timestamp'].dt.tz is None:
            df_ponto.loc[:, 'timestamp'] = df_ponto['timestamp'].dt.tz_localize('UTC')
        else:
            df_ponto.loc[:, 'timestamp'] = df_ponto['timestamp'].dt.tz_convert('UTC')

        df_filtrado = df_ponto[
            (df_ponto['timestamp'] >= start_dt) &
            (df_ponto['timestamp'] <= end_dt)
            ].copy()

        if df_filtrado.empty:
            print("LOG PDF: Sem dados no período selecionado.")
            return dash.no_update, True

        # 2. Configurações e Status
        config = PONTOS_DE_ANALISE.get(id_ponto, {"nome": "Ponto"})

        # Pega o Status Atual para o PDF
        status_atual_dict = status_json
        status_geral_ponto_txt = status_atual_dict.get(id_ponto, "INDEFINIDO")
        risco_geral = RISCO_MAP.get(status_geral_ponto_txt, -1)
        status_texto, status_cor = STATUS_MAP_HIERARQUICO.get(risco_geral, ("INDEFINIDO", "secondary"))[:2]

        # --- INÍCIO DA GERAÇÃO DE GRÁFICOS PARA PDF ---

        # 3. Adicionar timestamp_local ao df_filtrado (necessário para os eixos X)
        # --- INÍCIO DA CORREÇÃO (SettingWithCopyWarning) ---
        df_filtrado.loc[:, 'timestamp_local'] = df_filtrado['timestamp'].dt.tz_convert('America/Sao_Paulo')
        # --- FIM DA CORREÇÃO ---

        # 4. Calcular Acumulado (usando o df_filtrado)
        # --- ALTERAÇÃO: Usar 72h fixo para o PDF ---
        df_chuva_72h_pdf = processamento.calcular_acumulado_rolling(df_filtrado, horas=72)

        # --- INÍCIO DA CORREÇÃO (SettingWithCopyWarning) ---
        if 'timestamp' in df_chuva_72h_pdf.columns:
            if df_chuva_72h_pdf['timestamp'].dt.tz is None:
                df_chuva_72h_pdf.loc[:, 'timestamp'] = df_chuva_72h_pdf['timestamp'].dt.tz_localize('UTC')
            df_chuva_72h_pdf.loc[:, 'timestamp_local'] = df_chuva_72h_pdf['timestamp'].dt.tz_convert(
                'America/Sao_Paulo')
        else:
            df_chuva_72h_pdf = df_chuva_72h_pdf.copy()
            df_chuva_72h_pdf.loc[:, 'timestamp_local'] = df_chuva_72h_pdf['timestamp']  # fallback
        # --- FIM DA CORREÇÃO ---

        # 5. Gerar Gráfico de Chuva
        fig_chuva_pdf = make_subplots(specs=[[{"secondary_y": True}]])
        fig_chuva_pdf.add_trace(
            go.Bar(x=df_filtrado['timestamp_local'], y=df_filtrado['chuva_mm'], name='Pluv. Horária (mm)',
                   marker_color='#2C3E50', opacity=0.8), secondary_y=False)
        fig_chuva_pdf.add_trace(go.Scatter(x=df_chuva_72h_pdf['timestamp_local'], y=df_chuva_72h_pdf['chuva_mm'],
                                           name='Acumulada (72h)', mode='lines',  # Nome fixo para PDF
                                           line=dict(color='#007BFF', width=2.5)), secondary_y=True)

        titulo_chuva = f"Pluviometria - Estação {config['nome']}"
        fig_chuva_pdf.update_layout(
            title_text=titulo_chuva,
            template=TEMPLATE_GRAFICO_MODERNO,
            margin=dict(l=40, r=20, t=50, b=80),
            legend=dict(orientation="h", yanchor="bottom", y=-0.5, xanchor='center', x=0.5)
        )

        # 6. Gerar Gráfico de Umidade
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
            template=TEMPLATE_GRAFICO_MODERNO,
            margin=dict(l=40, r=20, t=40, b=80),
            legend=dict(orientation="h", yanchor="bottom", y=-0.5, xanchor="center", x=0.5)
        )

        # --- FIM DA GERAÇÃO DE GRÁFICOS PARA PDF ---

        # 7. Chamar a função de gerar PDF (passando os gráficos)
        pdf_buffer = gerador_pdf.criar_relatorio_em_memoria(
            df_filtrado,
            fig_chuva_pdf,
            fig_umidade_pdf,
            status_texto,
            status_cor
        )

        nome_arquivo = f"Relatorio_{config['nome']}_{datetime.now().strftime('%Y%m%d')}.pdf"

        print(f"LOG PDF: PDF gerado com sucesso (COM GRÁFICOS). Arquivo: {nome_arquivo}")

        # 8. Download
        pdf_output = io.BytesIO(pdf_buffer)
        return dcc.send_bytes(pdf_output.read(), nome_arquivo, type="application/pdf"), False

    except Exception as e:
        print(f"ERRO CRÍTICO no Callback PDF:")
        traceback.print_exc()
        return dash.no_update, True


# --- FIM DA ALTERAÇÃO (PDF COM GRÁFICOS) ---


# --- INÍCIO DA ALTERAÇÃO (Callback PDF de Logs) ---
@app.callback(
    Output('download-pdf-logs', 'data'),
    Input('btn-pdf-logs', 'n_clicks'),
    [
        State('store-logs-sessao', 'data'),  # <-- CORRIGIDO: Ler do store de dados brutos
        State('store-id-ponto-ativo', 'data')
    ],
    prevent_initial_call=True
)
def generate_logs_pdf(n_clicks, logs_json, id_ponto):  # <-- Variável renomeada
    if n_clicks is None or not id_ponto or not logs_json:
        return dash.no_update

    try:
        config = PONTOS_DE_ANALISE.get(id_ponto, {"nome": "Ponto"})

        # Converte o conteúdo do store (logs_json) para uma lista de strings
        if isinstance(logs_json, str):
            logs_list = logs_json.split('\n')
        else:
            logs_list = json.loads(logs_json)  # Fallback

        logs_list = [log.strip() for log in logs_list if log.strip()]

        # Filtra logs (mesma lógica do modal)
        logs_to_pdf = [log for log in logs_list if f"| {id_ponto} |" in log or "| GERAL |" in log]

        if not logs_to_pdf:
            return dash.no_update  # Não gera PDF vazio

        # Usamos a função de gerar logs do gerador_pdf
        pdf_buffer = gerador_pdf.criar_relatorio_logs_em_memoria(id_ponto, logs_to_pdf)

        nome_arquivo = f"Logs_{config['nome']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"

        # CORREÇÃO DE DOWNLOAD: ENVELOPAR O BUFFER BINÁRIO EM io.BytesIO
        pdf_output = io.BytesIO(pdf_buffer)
        return dcc.send_bytes(pdf_output.read(), nome_arquivo, type="application/pdf")

    except Exception as e:
        print(f"ERRO CRÍTICO no Callback PDF de Logs:")
        traceback.print_exc()
        return dash.no_update