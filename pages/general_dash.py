# pages/general_dash.py (CORRIGIDO)

import dash
from dash import html, dcc, callback, Input, Output
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
from io import StringIO
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Importa o app central e helpers
from app import app, TEMPLATE_GRAFICO_MODERNO
from config import PONTOS_DE_ANALISE, FREQUENCIA_API_SEGUNDOS
import processamento

CORES_UMIDADE = {
    '1m': 'green',
    '2m': '#FFD700',
    '3m': 'red'
}


# --- Layout da Página Geral ---
def get_layout():
    """Retorna o layout do dashboard geral."""
    opcoes_tempo = [{'label': f'Últimas {h} horas', 'value': h} for h in [1, 3, 6, 12, 18, 24, 72, 84, 96]] + [
        {'label': 'Todo o Histórico', 'value': 14 * 24}]

    return dbc.Container([
        # Seletor de Período
        dbc.Row([
            dbc.Col(dbc.Label("Período (Gráficos):"), width="auto"),
            dbc.Col(
                dcc.Dropdown(
                    id='general-graph-time-selector',
                    options=opcoes_tempo,
                    value=72,
                    clearable=False,
                    searchable=False
                ),
                width=12, lg=4
            )
        ], align="center", className="my-3"),

        # Conteúdo dos gráficos
        html.Div(id='general-dash-content', children=[dbc.Spinner(size="lg", children="Carregando...")])
    ], fluid=True)


# --- Callback da Página Geral ---
@app.callback(
    Output('general-dash-content', 'children'),
    Input('store-dados-sessao', 'data'),
    Input('general-graph-time-selector', 'value')
)
def update_general_dashboard(dados_json, selected_hours):
    if not dados_json or selected_hours is None:
        return dbc.Spinner(size="lg", children="Carregando dados...")
    try:
        df_completo = pd.read_json(StringIO(dados_json), orient='split')

        # --- CORREÇÃO DE TIPAGEM: Garante que colunas chave são do tipo correto ---
        df_completo['timestamp'] = pd.to_datetime(df_completo['timestamp'])
        df_completo['chuva_mm'] = pd.to_numeric(df_completo['chuva_mm'], errors='coerce')
        df_completo['umidade_1m_perc'] = pd.to_numeric(df_completo['umidade_1m_perc'], errors='coerce')
        df_completo['umidade_2m_perc'] = pd.to_numeric(df_completo['umidade_2m_perc'], errors='coerce')
        df_completo['umidade_3m_perc'] = pd.to_numeric(df_completo['umidade_3m_perc'], errors='coerce')
        # --- FIM DA CORREÇÃO DE TIPAGEM ---

    except Exception as e:
        return dbc.Alert(f"Erro ao ler dados: {e}", color="danger")

    layout_geral = []
    for id_ponto, config in PONTOS_DE_ANALISE.items():
        df_ponto = df_completo[df_completo['id_ponto'] == id_ponto].copy()
        if df_ponto.empty: continue

        # Lógica de cálculo de pontos (baseada no selected_hours)
        PONTOS_POR_HORA = int(60 / (FREQUENCIA_API_SEGUNDOS / 60))
        n_pontos_desejados = selected_hours * PONTOS_POR_HORA
        n_pontos_plot = min(n_pontos_desejados, len(df_ponto))
        df_ponto_plot = df_ponto.tail(n_pontos_plot)
        df_chuva_72h_completo = processamento.calcular_acumulado_72h(df_ponto)
        df_chuva_72h_plot = df_chuva_72h_completo.tail(n_pontos_plot)
        n_horas_titulo = selected_hours

        # Gráfico de Chuva
        fig_chuva = make_subplots(specs=[[{"secondary_y": True}]])
        fig_chuva.add_trace(go.Bar(x=df_ponto_plot['timestamp'], y=df_ponto_plot['chuva_mm'], name='Pluv. Horária',
                                   marker_color='#2C3E50', opacity=0.8), secondary_y=False)
        fig_chuva.add_trace(
            go.Scatter(x=df_chuva_72h_plot['timestamp'], y=df_chuva_72h_plot['chuva_mm'], name='Acumulada (72h)',
                       mode='lines', line=dict(color='#007BFF', width=2.5)), secondary_y=True)
        fig_chuva.update_layout(title_text=f"Pluviometria - {config['nome']} ({n_horas_titulo}h)",
                                template=TEMPLATE_GRAFICO_MODERNO,
                                margin=dict(l=40, r=20, t=50, b=40),
                                legend=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor='center', x=0.5),
                                yaxis_title="Pluv. Horária (mm)",
                                yaxis2_title="Acumulada (mm)",
                                hovermode="x unified", bargap=0.1)
        fig_chuva.update_yaxes(title_text="Pluv. Horária (mm)", secondary_y=False);
        fig_chuva.update_yaxes(title_text="Acumulada (mm)", secondary_y=True)

        # Gráfico de Umidade
        df_umidade = df_ponto_plot.melt(id_vars=['timestamp'],
                                        value_vars=['umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc'],
                                        var_name='Sensor', value_name='Umidade Solo (%)')

        df_umidade['Sensor'] = df_umidade['Sensor'].replace({
            'umidade_1m_perc': '1m',
            'umidade_2m_perc': '2m',
            'umidade_3m_perc': '3m'
        })

        fig_umidade = px.line(df_umidade, x='timestamp', y='Umidade Solo (%)', color='Sensor',
                              title=f"Umidade Solo - {config['nome']} ({n_horas_titulo}h)",
                              color_discrete_map=CORES_UMIDADE)

        fig_umidade.update_traces(line=dict(width=3))
        fig_umidade.update_layout(template=TEMPLATE_GRAFICO_MODERNO, margin=dict(l=40, r=20, t=40, b=50),
                                  legend=dict(orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5))

        # Layout Lado a Lado
        col_chuva = dbc.Col(dbc.Card(dbc.CardBody(dcc.Graph(figure=fig_chuva)), className="shadow-sm"), width=12, lg=6,
                            className="mb-4")
        col_umidade = dbc.Col(dbc.Card(dbc.CardBody(dcc.Graph(figure=fig_umidade)), className="shadow-sm"), width=12,
                              lg=6, className="mb-4")
        linha_ponto = dbc.Row([col_chuva, col_umidade], className="mb-4")
        layout_geral.append(linha_ponto)

    if not layout_geral: return dbc.Alert("Nenhum dado.", color="warning")
    return layout_geral