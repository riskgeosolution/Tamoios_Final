# pages/general_dash.py (v12.6: CORREÇÃO VISUAL DO EIXO X)

import dash
from dash import html, dcc, callback, Input, Output
import dash_bootstrap_components as dbc
import pandas as pd
from io import StringIO
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from app import app, TEMPLATE_GRAFICO_MODERNO
from config import PONTOS_DE_ANALISE, CORES_UMIDADE
import processamento

def get_layout():
    """Retorna o layout do dashboard geral."""
    opcoes_tempo_lista = [1, 3, 6, 12, 18, 24, 48, 72, 96]
    opcoes_tempo = [{'label': f'Últimas {h} horas', 'value': h} for h in opcoes_tempo_lista] + [
        {'label': 'Últimos 7 dias', 'value': 7 * 24}]

    return dbc.Container([
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
        html.Div(id='general-dash-content', children=[dbc.Spinner(size="lg", children="Carregando...")])
    ], fluid=True)

@app.callback(
    Output('general-dash-content', 'children'),
    [Input('store-dados-sessao', 'data'),
     Input('general-graph-time-selector', 'value')]
)
def update_general_dashboard(dados_json, selected_hours):
    if not dados_json or selected_hours is None:
        return dbc.Spinner(size="lg", children="Carregando dados...")

    try:
        df_completo = pd.read_json(StringIO(dados_json), orient='split')
        df_completo['timestamp'] = pd.to_datetime(df_completo['timestamp'])
        if df_completo['timestamp'].dt.tz is None:
            df_completo['timestamp'] = df_completo['timestamp'].dt.tz_localize('UTC')
        df_completo['timestamp_local'] = df_completo['timestamp'].dt.tz_convert('America/Sao_Paulo')
        
        numeric_cols = ['chuva_mm', 'umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc']
        for col in numeric_cols:
            df_completo[col] = pd.to_numeric(df_completo[col], errors='coerce')

    except Exception as e:
        return dbc.Alert(f"Erro ao ler dados: {e}", color="danger")

    layout_geral = []
    for id_ponto, config in PONTOS_DE_ANALISE.items():
        df_ponto = df_completo[df_completo['id_ponto'] == id_ponto].copy()
        if df_ponto.empty: continue

        ultimo_timestamp_no_df = df_ponto['timestamp_local'].max()
        limite_tempo = ultimo_timestamp_no_df - pd.Timedelta(hours=selected_hours)
        df_ponto_plot = df_ponto[df_ponto['timestamp_local'] >= limite_tempo].copy()

        if df_ponto_plot.empty: continue

        df_plot_15min = df_ponto_plot.set_index('timestamp_local').resample('15T').agg({
            'chuva_mm': 'sum',
            'umidade_1m_perc': 'mean',
            'umidade_2m_perc': 'mean',
            'umidade_3m_perc': 'mean'
        }).reset_index()

        df_chuva_72h_completo = processamento.calcular_acumulado_rolling(df_ponto, horas=72)
        df_chuva_periodo_completo = processamento.calcular_acumulado_rolling(df_ponto, horas=selected_hours)
        
        df_chuva_72h_plot = df_chuva_72h_completo[df_chuva_72h_completo['timestamp'] >= df_ponto_plot['timestamp'].min()].copy()
        df_chuva_periodo_plot = df_chuva_periodo_completo[df_chuva_periodo_completo['timestamp'] >= df_ponto_plot['timestamp'].min()].copy()

        for df in [df_chuva_72h_plot, df_chuva_periodo_plot]:
            if 'timestamp' in df.columns:
                if df['timestamp'].dt.tz is None: df.loc[:, 'timestamp'] = df['timestamp'].dt.tz_localize('UTC')
                df.loc[:, 'timestamp_local'] = df['timestamp'].dt.tz_convert('America/Sao_Paulo')

        fig_chuva = make_subplots(specs=[[{"secondary_y": True}]])
        fig_chuva.add_trace(go.Bar(x=df_plot_15min['timestamp_local'], y=df_plot_15min['chuva_mm'], name='Pluv. 15 min', marker_color='#2C3E50', opacity=0.8), secondary_y=False)
        fig_chuva.add_trace(go.Scatter(x=df_chuva_periodo_plot['timestamp_local'], y=df_chuva_periodo_plot['chuva_mm'], name=f'Acumulada ({selected_hours}h)', mode='lines', line=dict(color='#007BFF', width=2.5)), secondary_y=True)
        fig_chuva.add_trace(go.Scatter(x=df_chuva_72h_plot['timestamp_local'], y=df_chuva_72h_plot['chuva_mm'], name='Acumulada (72h)', mode='lines', line=dict(color='green', width=2, dash='dot'), visible='legendonly'), secondary_y=True)
        
        fig_chuva.update_layout(
            title_text=f"Pluviometria - {config['nome']} ({selected_hours}h)", template=TEMPLATE_GRAFICO_MODERNO,
            margin=dict(l=40, r=20, t=50, b=80), legend=dict(orientation="h", yanchor="bottom", y=-0.5, xanchor='center', x=0.5),
            xaxis_title="Data e Hora", yaxis_title="Pluviometria (mm/15min)", yaxis2_title=f"Acumulada ({selected_hours}h)",
            hovermode="x unified", bargap=0.1, hoverlabel=dict(bgcolor="white", font_size=12)
        )
        # CORREÇÃO: Usando o formato de hora e minuto.
        fig_chuva.update_xaxes(dtick=3 * 60 * 60 * 1000, tickformat="%d/%m %H:%M", tickangle=-45)

        fig_umidade = go.Figure()
        fig_umidade.add_trace(go.Bar(x=df_plot_15min['timestamp_local'], y=[df_plot_15min[['umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc']].max().max() * 1.1]*len(df_plot_15min) if not df_plot_15min.empty else [], hoverinfo='none', showlegend=False, marker_opacity=0))
        fig_umidade.add_trace(go.Scatter(x=df_plot_15min['timestamp_local'], y=df_plot_15min['umidade_1m_perc'], name='Umidade 1m', mode='lines', line=dict(color=CORES_UMIDADE['1m'], width=3)))
        fig_umidade.add_trace(go.Scatter(x=df_plot_15min['timestamp_local'], y=df_plot_15min['umidade_2m_perc'], name='Umidade 2m', mode='lines', line=dict(color=CORES_UMIDADE['2m'], width=3)))
        fig_umidade.add_trace(go.Scatter(x=df_plot_15min['timestamp_local'], y=df_plot_15min['umidade_3m_perc'], name='Umidade 3m', mode='lines', line=dict(color=CORES_UMIDADE['3m'], width=3)))

        fig_umidade.update_layout(
            title_text=f"Umidade do Solo - {config['nome']} ({selected_hours}h)", template=TEMPLATE_GRAFICO_MODERNO,
            margin=dict(l=40, r=20, t=40, b=80), legend=dict(orientation="h", yanchor="bottom", y=-0.5, xanchor="center", x=0.5),
            xaxis_title="Data e Hora", yaxis_title="Umidade do Solo (%)",
            hovermode="x unified", bargap=0, hoverlabel=dict(bgcolor="white", font_size=12)
        )
        # CORREÇÃO: Usando o formato de hora e minuto.
        fig_umidade.update_xaxes(dtick=3 * 60 * 60 * 1000, tickformat="%d/%m %H:%M", tickangle=-45)
        
        try:
            all_umidade = pd.concat([df_plot_15min['umidade_1m_perc'], df_plot_15min['umidade_2m_perc'], df_plot_15min['umidade_3m_perc']]).dropna()
            if not all_umidade.empty:
                y_min = min(0, all_umidade.min() - 5)
                y_max = max(50, all_umidade.max() + 5)
                fig_umidade.update_yaxes(range=[y_min, y_max])
        except Exception:
            pass

        col_chuva = dbc.Col(dbc.Card(dbc.CardBody(dcc.Graph(figure=fig_chuva)), className="shadow-sm mb-4"), width=12, lg=6)
        col_umidade = dbc.Col(dbc.Card(dbc.CardBody(dcc.Graph(figure=fig_umidade)), className="shadow-sm mb-4"), width=12, lg=6)
        layout_geral.append(dbc.Row([col_chuva, col_umidade], className="mb-4"))

    if not layout_geral:
        return dbc.Alert("Nenhum dado disponível para o período selecionado.", color="warning")
    return layout_geral