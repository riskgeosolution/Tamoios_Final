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
import data_source


def get_layout():
    """Retorna o layout do dashboard geral (APENAS GRÁFICOS)."""
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
    [Input('intervalo-atualizacao-dados', 'n_intervals'),
     Input('general-graph-time-selector', 'value')]
)
def update_general_dashboard(n_intervals, selected_hours):
    if selected_hours is None:
        return dbc.Spinner(size="lg", children="Carregando dados...")

    # OTIMIZAÇÃO: Solicita apenas as colunas necessárias para reduzir consumo de memória
    cols_necessarias = [
        'timestamp', 'id_ponto',
        'chuva_mm', 'precipitacao_acumulada_mm',
        'umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc'
    ]

    horas_para_buscar = max(selected_hours, 73)

    # Chama o read otimizado com a lista de colunas
    df_completo = data_source.read_data_from_sqlite(
        last_hours=horas_para_buscar,
        colunas=cols_necessarias
    )

    try:
        if df_completo.empty or 'timestamp' not in df_completo.columns:
            return dbc.Alert("Dados históricos indisponíveis no momento.", color="warning")

        df_completo['timestamp'] = pd.to_datetime(df_completo['timestamp'])
        if df_completo['timestamp'].dt.tz is None:
            df_completo['timestamp'] = df_completo['timestamp'].dt.tz_localize('UTC')
        df_completo['timestamp_local'] = df_completo['timestamp'].dt.tz_convert('America/Sao_Paulo')

        # OTIMIZAÇÃO: Downcast para float32 (economiza 50% de RAM nos números)
        numeric_cols = ['chuva_mm', 'umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc',
                        'precipitacao_acumulada_mm']
        for col in numeric_cols:
            if col in df_completo.columns:
                df_completo[col] = pd.to_numeric(df_completo[col], errors='coerce', downcast='float')

    except Exception as e:
        return dbc.Alert(f"Erro ao processar dados: {e}", color="danger")

    layout_geral = []
    for id_ponto, config in PONTOS_DE_ANALISE.items():
        df_ponto = df_completo[df_completo['id_ponto'] == id_ponto].copy()
        if df_ponto.empty: continue

        df_ponto = df_ponto.sort_values('timestamp').drop_duplicates(subset=['timestamp'], keep='last')

        # --- CÁLCULO DA CHUVA INCREMENTAL ---
        if 'precipitacao_acumulada_mm' in df_ponto.columns:
            df_ponto = df_ponto.sort_values('timestamp').reset_index(drop=True)
            df_ponto['precipitacao_acumulada_mm'] = df_ponto['precipitacao_acumulada_mm'].ffill().fillna(0)
            df_ponto['chuva_incremental'] = df_ponto['precipitacao_acumulada_mm'].diff().fillna(0)
            mask_virada_dia = df_ponto['chuva_incremental'] < 0
            df_ponto.loc[mask_virada_dia, 'chuva_incremental'] = df_ponto.loc[
                mask_virada_dia, 'precipitacao_acumulada_mm']
        else:
            df_ponto['chuva_incremental'] = df_ponto.get('chuva_mm', 0)

        umidade_cols = ['umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc']
        if all(c in df_ponto.columns for c in umidade_cols):
            df_ponto[umidade_cols] = df_ponto[umidade_cols].ffill()

        # 1. Calcular o acumulado para a linha do gráfico
        df_chuva_acumulada = processamento.calcular_acumulado_rolling(df_ponto, horas=selected_hours)

        # Filtra os dados para o período de tempo selecionado
        ultimo_timestamp_no_df = df_ponto['timestamp_local'].max()
        limite_tempo = ultimo_timestamp_no_df - pd.Timedelta(hours=selected_hours)
        df_ponto_plot = df_ponto[df_ponto['timestamp_local'] >= limite_tempo].copy()

        if df_ponto_plot.empty: continue

        # Agrega os dados em intervalos de 15 minutos
        agg_dict = {'chuva_incremental': 'sum'}
        if 'umidade_1m_perc' in df_ponto_plot.columns: agg_dict['umidade_1m_perc'] = 'mean'
        if 'umidade_2m_perc' in df_ponto_plot.columns: agg_dict['umidade_2m_perc'] = 'mean'
        if 'umidade_3m_perc' in df_ponto_plot.columns: agg_dict['umidade_3m_perc'] = 'mean'

        df_plot_15min = df_ponto_plot.set_index('timestamp_local').resample('15T').agg(agg_dict).reset_index()

        # 2. Filtrar o DataFrame de chuva acumulada para o período de plotagem
        df_chuva_acumulada_plot = df_chuva_acumulada[
            df_chuva_acumulada['timestamp'] >= df_ponto_plot['timestamp'].min()].copy()

        if 'timestamp' in df_chuva_acumulada_plot.columns:
            if df_chuva_acumulada_plot['timestamp'].dt.tz is None: df_chuva_acumulada_plot.loc[:, 'timestamp'] = \
            df_chuva_acumulada_plot['timestamp'].dt.tz_localize('UTC')
            df_chuva_acumulada_plot.loc[:, 'timestamp_local'] = df_chuva_acumulada_plot['timestamp'].dt.tz_convert(
                'America/Sao_Paulo')

        # --- GRÁFICO DE CHUVA ---
        fig_chuva = make_subplots(specs=[[{"secondary_y": True}]])
        # 3. Usar a nova coluna 'chuva_incremental' para as barras
        fig_chuva.add_trace(
            go.Bar(x=df_plot_15min['timestamp_local'], y=df_plot_15min['chuva_incremental'], name='Pluv. 15 min',
                   marker_color='#2C3E50', opacity=0.8), secondary_y=False)

        # 4. Usar o DataFrame de acumulado para a linha
        fig_chuva.add_trace(
            go.Scatter(x=df_chuva_acumulada_plot['timestamp_local'], y=df_chuva_acumulada_plot['chuva_mm'],
                       name=f'Acumulada ({selected_hours}h)', mode='lines',
                       line=dict(color='#007BFF', width=2.5)), secondary_y=True)

        fig_chuva.update_layout(
            title_text=f"Pluviometria - {config['nome']}", template=TEMPLATE_GRAFICO_MODERNO,
            margin=dict(l=40, r=20, t=50, b=80),
            legend=dict(orientation="h", yanchor="bottom", y=-0.5, xanchor='center', x=0.5),
            xaxis_title="Data e Hora", yaxis_title="Pluviometria (mm/15min)",
            yaxis2_title=f"Acumulada ({selected_hours}h)",
            hovermode="x unified", bargap=0.1, hoverlabel=dict(bgcolor="white", font_size=12)
        )
        fig_chuva.update_xaxes(dtick=3 * 60 * 60 * 1000, tickformat="%d/%m %H:%M", tickangle=-45)

        # --- GRÁFICO DE UMIDADE ---
        fig_umidade = go.Figure()
        if 'umidade_1m_perc' in df_plot_15min.columns:
            fig_umidade.add_trace(
                go.Scatter(x=df_plot_15min['timestamp_local'], y=df_plot_15min['umidade_1m_perc'], name='Umidade 1m',
                           mode='lines', line=dict(color=CORES_UMIDADE['1m'], width=3)))
        if 'umidade_2m_perc' in df_plot_15min.columns:
            fig_umidade.add_trace(
                go.Scatter(x=df_plot_15min['timestamp_local'], y=df_plot_15min['umidade_2m_perc'], name='Umidade 2m',
                           mode='lines', line=dict(color=CORES_UMIDADE['2m'], width=3)))
        if 'umidade_3m_perc' in df_plot_15min.columns:
            fig_umidade.add_trace(
                go.Scatter(x=df_plot_15min['timestamp_local'], y=df_plot_15min['umidade_3m_perc'], name='Umidade 3m',
                           mode='lines', line=dict(color=CORES_UMIDADE['3m'], width=3)))

        umidade_cols_existentes = [c for c in ['umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc'] if
                                   c in df_plot_15min.columns]
        max_val_umidade = 0
        if umidade_cols_existentes:
            max_val_umidade = df_plot_15min[umidade_cols_existentes].max().max()
        if pd.isna(max_val_umidade): max_val_umidade = 0
        range_max = max(50, max_val_umidade * 1.1)

        fig_umidade.update_layout(
            title_text=f"Umidade do Solo - {config['nome']}", template=TEMPLATE_GRAFICO_MODERNO,
            margin=dict(l=40, r=20, t=40, b=80),
            legend=dict(orientation="h", yanchor="bottom", y=-0.5, xanchor="center", x=0.5),
            xaxis_title="Data e Hora", yaxis_title="Umidade do Solo (%)",
            yaxis=dict(range=[0, range_max]),
            hovermode="x unified", bargap=0, hoverlabel=dict(bgcolor="white", font_size=12)
        )
        fig_umidade.update_xaxes(dtick=3 * 60 * 60 * 1000, tickformat="%d/%m %H:%M", tickangle=-45)

        col_chuva = dbc.Col(dbc.Card(dbc.CardBody(dcc.Graph(figure=fig_chuva)), className="shadow-sm mb-4"), width=12,
                            lg=6)
        col_umidade = dbc.Col(dbc.Card(dbc.CardBody(dcc.Graph(figure=fig_umidade)), className="shadow-sm mb-4"),
                              width=12, lg=6)
        layout_geral.append(dbc.Row([col_chuva, col_umidade], className="mb-4"))
        layout_geral.append(html.Hr())

    if not layout_geral:
        return dbc.Alert("Nenhum dado disponível para o período selecionado.", color="warning")
    return layout_geral