import dash
from dash import html, dcc, callback, Input, Output, State
import dash_bootstrap_components as dbc
import plotly.express as px  # Mantido para os gráficos do Dashboard
import pandas as pd
from io import StringIO
import io
import traceback
import matplotlib.pyplot as plt  # <-- NOVO: Para Matplotlib
from matplotlib.figure import Figure  # <-- NOVO
import numpy as np  # Necessário para cálculos

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

# Configura o Matplotlib para rodar em ambientes sem tela (headless)
plt.switch_backend('Agg')

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


# Callback 1: Atualiza os cards e gráficos (USA PLOTLY)
# NOTE: Plotly é usado aqui para a interatividade do Dashboard
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
    # --- Lógica central do Plotly mantida ---

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
        status_atual_dict = status_json
    except Exception as e:
        return "Erro ao carregar dados.", "", id_ponto

    df_ponto = df_completo[df_completo['id_ponto'] == id_ponto].copy()

    if df_ponto.empty:
        return dbc.Alert("Sem dados históricos para este ponto.", color="warning", className="m-3"), "", id_ponto

    df_ponto.loc[:, 'timestamp'] = pd.to_datetime(df_ponto['timestamp'])
    if df_ponto['timestamp'].dt.tz is None:
        df_ponto.loc[:, 'timestamp'] = df_ponto['timestamp'].dt.tz_localize('UTC')
    else:
        df_ponto.loc[:, 'timestamp'] = df_ponto['timestamp'].dt.tz_convert('UTC')
    df_ponto.loc[:, 'timestamp_local'] = df_ponto['timestamp'].dt.tz_convert('America/Sao_Paulo')
    df_ponto.loc[:, 'chuva_mm'] = pd.to_numeric(df_ponto['chuva_mm'], errors='coerce')
    df_ponto.loc[:, 'precipitacao_acumulada_mm'] = pd.to_numeric(df_ponto['precipitacao_acumulada_mm'], errors='coerce')
    df_ponto.loc[:, 'umidade_1m_perc'] = pd.to_numeric(df_ponto['umidade_1m_perc'], errors='coerce')
    df_ponto.loc[:, 'umidade_2m_perc'] = pd.to_numeric(df_ponto['umidade_2m_perc'], errors='coerce')
    df_ponto.loc[:, 'umidade_3m_perc'] = pd.to_numeric(df_ponto['umidade_3m_perc'], errors='coerce')

    status_geral_ponto_txt = status_atual_dict.get(id_ponto, "INDEFINIDO")
    risco_geral = RISCO_MAP.get(status_geral_ponto_txt, -1)
    status_geral_texto, status_geral_cor_bootstrap = STATUS_MAP_HIERARQUICO.get(risco_geral, ("INDEFINIDO", "secondary",
                                                                                              "bg-secondary"))[:2]
    card_class_color = "bg-" + status_geral_cor_bootstrap if status_geral_cor_bootstrap not in ["warning", "orange",
                                                                                                "danger"] else "bg-" + status_geral_cor_bootstrap

    ultimo_timestamp_no_df = df_ponto['timestamp_local'].max()
    limite_tempo = ultimo_timestamp_no_df - pd.Timedelta(hours=selected_hours)
    df_ponto_plot = df_ponto[df_ponto['timestamp_local'] >= limite_tempo].copy()
    n_horas_titulo = selected_hours

    df_chuva_72h_completo = processamento.calcular_acumulado_rolling(df_ponto, horas=72)
    df_chuva_FILTRO_completo = processamento.calcular_acumulado_rolling(df_ponto, horas=selected_hours)
    df_chuva_FILTRO_plot = df_chuva_FILTRO_completo[
        df_chuva_FILTRO_completo['timestamp'] >= df_ponto_plot['timestamp'].min()].copy()

    # Cards Logic (Mantido)
    try:
        ultimo_dado = df_ponto.sort_values('timestamp').iloc[-1]
        ultima_chuva_72h = df_chuva_72h_completo.iloc[-1]['chuva_mm'] if not df_chuva_72h_completo.empty else 0.0

        ultima_chuva_72h = ultima_chuva_72h if not pd.isna(ultima_chuva_72h) else 0.0

        from config import DELTA_TRIGGER_UMIDADE
        css_color_s1 = CORES_UMIDADE['1m'] if (ultimo_dado.get('umidade_1m_perc',
                                                               base_1m) - base_1m) >= DELTA_TRIGGER_UMIDADE else 'green'
        css_color_s2 = CORES_UMIDADE['2m'] if (ultimo_dado.get('umidade_2m_perc',
                                                               base_2m) - base_2m) >= DELTA_TRIGGER_UMIDADE else 'green'
        css_color_s3 = CORES_UMIDADE['3m'] if (ultimo_dado.get('umidade_3m_perc',
                                                               base_3m) - base_3m) >= DELTA_TRIGGER_UMIDADE else 'green'

        layout_cards = [
            dbc.Col(dbc.Card(dbc.CardBody([html.H5("Status Atual", style={'color': '#000000', 'font-weight': 'bold'}),
                                           html.P(status_geral_texto, className="fs-3",
                                                  style={'color': '#000000', 'font-weight': 'bold'})]),
                             className=f"shadow h-100 {card_class_color}", ), xs=12, md=4, className="mb-4"),
            dbc.Col(dbc.Card(
                dbc.CardBody([html.H5("Chuva 72h"), html.P(f"{ultima_chuva_72h:.1f} mm", className="fs-3 fw-bold")]),
                className="shadow h-100 bg-white"), xs=12, md=4, className="mb-4"),
            dbc.Col(dbc.Card(dbc.CardBody([html.H5("Umidade do Solo (%)", className="mb-3"), dbc.Row([
                dbc.Col(html.P(
                    [html.Span(f"{ultimo_dado.get('umidade_1m_perc', base_1m):.1f}", className="fs-3 fw-bold",
                               style={'color': css_color_s1}),
                     html.Span(" (1m)", className="small", style={'color': css_color_s1})],
                    className="mb-0 text-center"), width=4),
                dbc.Col(html.P(
                    [html.Span(f"{ultimo_dado.get('umidade_2m_perc', base_2m):.1f}", className="fs-3 fw-bold",
                               style={'color': css_color_s2}),
                     html.Span(" (2m)", className="small", style={'color': css_color_s2})],
                    className="mb-0 text-center"), width=4),
                dbc.Col(html.P(
                    [html.Span(f"{ultimo_dado.get('umidade_3m_perc', base_3m):.1f}", className="fs-3 fw-bold",
                               style={'color': css_color_s3}),
                     html.Span(" (3m)", className="small", style={'color': css_color_s3})],
                    className="mb-0 text-center"), width=4),
            ], justify="around")]), className="shadow h-100 bg-white"), xs=12, md=4, className="mb-4"),
        ]

    except IndexError:
        return "Dados insuficientes.", "", id_ponto

    # Plotly figures (mantidas)
    from plotly.subplots import make_subplots
    import plotly.graph_objects as go
    if 'timestamp' in df_chuva_FILTRO_plot.columns:
        if df_chuva_FILTRO_plot['timestamp'].dt.tz is None:
            df_chuva_FILTRO_plot.loc[:, 'timestamp'] = df_chuva_FILTRO_plot['timestamp'].dt.tz_localize('UTC')

        df_chuva_FILTRO_plot.loc[:, 'timestamp_local'] = df_chuva_FILTRO_plot['timestamp'].dt.tz_convert(
            'America/Sao_Paulo')
    else:
        df_chuva_FILTRO_plot.loc[:, 'timestamp_local'] = df_chuva_FILTRO_plot['timestamp']

    fig_chuva = make_subplots(specs=[[{"secondary_y": True}]])
    fig_chuva.add_trace(
        go.Bar(x=df_ponto_plot['timestamp_local'], y=df_ponto_plot['chuva_mm'], name='Pluv. Horária (mm)',
               marker_color='#2C3E50', opacity=0.8), secondary_y=False)
    fig_chuva.add_trace(go.Scatter(x=df_chuva_FILTRO_plot['timestamp_local'], y=df_chuva_FILTRO_plot['chuva_mm'],
                                   name=f'Acumulada ({n_horas_titulo}h)', mode='lines',
                                   line=dict(color='#007BFF', width=2.5)), secondary_y=True)
    fig_chuva.update_layout(title_text=f"Pluviometria - Estação {config['nome']} ({n_horas_titulo}h)",
                            template=TEMPLATE_GRAFICO_MODERNO, margin=dict(l=40, r=20, t=50, b=80),
                            legend=dict(orientation="h", yanchor="bottom", y=-0.5, xanchor='center', x=0.5),
                            xaxis_title="Data e Hora", yaxis_title="Pluviometria Horária (mm)",
                            yaxis2_title=f"Acumulada ({n_horas_titulo}h)", hovermode="x unified", bargap=0.1)
    fig_chuva.update_xaxes(dtick=3 * 60 * 60 * 1000, tickformat="%d/%m %Hh", tickangle=-45)
    fig_chuva.update_yaxes(title_text="Pluviometria Horária (mm)", secondary_y=False);
    fig_chuva.update_yaxes(title_text=f"Acumulada ({n_horas_titulo}h)", secondary_y=True)

    df_umidade = df_ponto_plot.melt(id_vars=['timestamp_local'],
                                    value_vars=['umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc'],
                                    var_name='Sensor', value_name='Umidade (%)')
    df_umidade['Sensor'] = df_umidade['Sensor'].replace(
        {'umidade_1m_perc': '1m', 'umidade_2m_perc': '2m', 'umidade_3m_perc': '3m'})
    fig_umidade = px.line(df_umidade, x='timestamp_local', y='Umidade (%)', color='Sensor',
                          title=f"Variação da Umidade do Solo - Estação {config['nome']} ({n_horas_titulo}h)",
                          color_discrete_map=CORES_UMIDADE)
    fig_umidade.update_traces(line=dict(width=3));
    fig_umidade.update_layout(template=TEMPLATE_GRAFICO_MODERNO, margin=dict(l=40, r=20, t=40, b=80),
                              legend=dict(orientation="h", yanchor="bottom", y=-0.5, xanchor="center", x=0.5),
                              xaxis_title="Data e Hora", yaxis_title="Umidade do Solo (%)")
    fig_umidade.update_xaxes(dtick=3 * 60 * 60 * 1000, tickformat="%d/%m %Hh", tickangle=-45)

    layout_graficos = [
        dbc.Col(dbc.Card(dbc.CardBody(dcc.Graph(figure=fig_chuva)), className="shadow-sm"), width=12, className="mb-4"),
        dbc.Col(dbc.Card(dbc.CardBody(dcc.Graph(figure=fig_umidade)), className="shadow-sm"), width=12,
                className="mb-4"), ]

    return layout_cards, layout_graficos, id_ponto
    # --- Fim da Lógica Central ---


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


# --- Callback para Carregar o Conteúdo do Log no Modal (Mantido) ---
@app.callback(
    [Output('modal-logs-content', 'children'),
     Output('store-logs-filtrados', 'data')],
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

        logs_ponto_ou_geral = [log for log in logs_list if f"| {id_ponto} |" in log or "| GERAL |" in log]

        logs_filtrados_raw = [log for log in logs_ponto_ou_geral if "API: Sucesso" not in log]

        if not logs_filtrados_raw:
            return f"Nenhum evento crítico ou mudança de status registrada para o ponto {id_ponto}.", []

        logs_formatados_display = []
        for log_str in reversed(logs_filtrados_raw):
            parts = log_str.split('|')
            if len(parts) < 3:
                logs_formatados_display.append(html.P(log_str))
                continue

            timestamp_str_utc_iso = parts[0].strip()
            ponto_str = parts[1].strip()
            msg_str = "|".join(parts[2:]).strip()

            try:
                dt_utc = pd.to_datetime(timestamp_str_utc_iso).tz_localize('UTC')
                dt_local = dt_utc.tz_convert('America/Sao_Paulo')
                timestamp_formatado = dt_local.strftime('%d/%m/%Y %H:%M:%S')
            except Exception as e:
                timestamp_formatado = timestamp_str_utc_iso.split('+')[0].replace('T', ' ')

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

        return logs_div, logs_filtrados_raw

    except Exception as e:
        print(f"ERRO CRÍTICO no Carregamento de Logs:")
        traceback.print_exc()
        return f"Erro ao formatar logs: {e}", []


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

        end_dt_naive = pd.to_datetime(end_date)
        end_dt_local = end_dt_naive.tz_localize('America/Sao_Paulo')
        end_dt_local_final = end_dt_local + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        end_dt = end_dt_local_final.tz_convert('UTC')

        # 2. Ler dados DIRETAMENTE DO SQLITE
        df_filtrado = data_source.read_data_from_sqlite(id_ponto, start_dt, end_dt)
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

        status_geral_ponto_txt = status_atual_dict.get(id_ponto, "INDEFINIDO")
        risco_geral = RISCO_MAP.get(status_geral_ponto_txt, -1)
        status_texto, status_cor = STATUS_MAP_HIERARQUICO.get(risco_geral, ("INDEFINIDO", "secondary"))[:2]

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

        # 6. Gerar Gráfico de Chuva (Plotly para o Matplotlib)
        import matplotlib.pyplot as plt
        import plotly.graph_objects as go  # Necessário para o fig_chuva_pdf original
        from plotly.subplots import make_subplots

        # Gera o objeto Plotly (para fins de Plotly)
        fig_chuva_plotly = make_subplots(specs=[[{"secondary_y": True}]])
        fig_chuva_plotly.add_trace(
            go.Bar(x=df_filtrado['timestamp_local'], y=df_filtrado['chuva_mm'], name='Pluv. Horária (mm)',
                   marker_color='#2C3E50', opacity=0.8), secondary_y=False)
        fig_chuva_plotly.add_trace(
            go.Scatter(x=df_chuva_72h_pdf['timestamp_local'], y=df_chuva_72h_pdf['chuva_mm'], name=f'Acumulada (72h)',
                       mode='lines', line=dict(color='#007BFF', width=2.5)), secondary_y=True)

        # Convertendo Plotly para Matplotlib para o PDF
        # NOTE: Esta etapa será desnecessária se usarmos o Matplotlib nativo.
        # Vamos gerar os gráficos Matplotlib diretamente:

        # Matplotlib Figure 1: Pluviometria
        fig_chuva_mp, ax1 = plt.subplots(figsize=(10, 5))
        ax1.bar(df_filtrado['timestamp_local'], df_filtrado['chuva_mm'], color='#2C3E50', alpha=0.8,
                label='Pluv. Horária (mm)')
        ax1.set_xlabel("Data e Hora (Local)")
        ax1.set_ylabel("Pluviometria Horária (mm)", color='#2C3E50')
        ax1.tick_params(axis='y', labelcolor='#2C3E50')
        ax1.tick_params(axis='x', rotation=45, labelsize=8)
        ax1.grid(True, linestyle='--', alpha=0.6, which='both')
        ax2 = ax1.twinx()
        ax2.plot(df_chuva_72h_pdf['timestamp_local'], df_chuva_72h_pdf['chuva_mm'], color='#007BFF', linewidth=2.5,
                 label='Acumulada (72h)')
        ax2.set_ylabel("Acumulada (72h)", color='#007BFF')
        ax2.tick_params(axis='y', labelcolor='#007BFF')
        fig_chuva_mp.suptitle(f"Pluviometria - Estação {config['nome']}", fontsize=12)
        fig_chuva_mp.legend(loc="upper left", bbox_to_anchor=(0.1, 0.95))
        plt.tight_layout(rect=[0, 0, 1, 0.95])

        # Matplotlib Figure 2: Umidade
        fig_umidade_mp, ax_umidade = plt.subplots(figsize=(10, 5))
        ax_umidade.plot(df_filtrado['timestamp_local'], df_filtrado['umidade_1m_perc'], label='1m',
                        color=CORES_UMIDADE['1m'], linewidth=2)
        ax_umidade.plot(df_filtrado['timestamp_local'], df_filtrado['umidade_2m_perc'], label='2m',
                        color=CORES_UMIDADE['2m'], linewidth=2)
        ax_umidade.plot(df_filtrado['timestamp_local'], df_filtrado['umidade_3m_perc'], label='3m',
                        color=CORES_UMIDADE['3m'], linewidth=2)
        ax_umidade.set_title(f"Variação da Umidade do Solo - Estação {config['nome']}", fontsize=12)
        ax_umidade.set_xlabel("Data e Hora (Local)")
        ax_umidade.set_ylabel("Umidade do Solo (%)")
        ax_umidade.legend(loc='lower center', ncol=3)
        ax_umidade.grid(True, linestyle='--', alpha=0.6)
        ax_umidade.tick_params(axis='x', rotation=45, labelsize=8)
        plt.tight_layout()

        # 8. Chamar a função de gerar PDF (USA gerador_pdf v4)
        pdf_buffer = gerador_pdf.criar_relatorio_em_memoria(
            df_filtrado, fig_chuva_mp, fig_umidade_mp, status_texto, status_cor
        )

        # 9. Fechar as figuras Matplotlib (libera memória)
        plt.close(fig_chuva_mp)
        plt.close(fig_umidade_mp)

        nome_arquivo = f"Relatorio_{config['nome']}_{datetime.now().strftime('%Y%m%d')}.pdf"
        print(f"LOG PDF: PDF gerado com sucesso (COM GRÁFICOS MATPLOTLIB). Arquivo: {nome_arquivo}")

        # 10. Download
        pdf_output = io.BytesIO(pdf_buffer)
        return dcc.send_bytes(pdf_output.read(), nome_arquivo, type="application/pdf"), False

    except Exception as e:
        print(f"ERRO CRÍTICO no Callback PDF:")
        traceback.print_exc()
        return dash.no_update, True

# ... (O restante do arquivo permanece o mesmo)