# pages/general_dash.py (CORRIGIDO: Adicionado '48 horas')

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
# Importa constantes
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

    # --- INÍCIO DA ALTERAÇÃO (Adicionado 48 horas) ---
    opcoes_tempo_lista = [1, 3, 6, 12, 18, 24, 48, 72, 84, 96]  # Adicionado 48
    opcoes_tempo = [{'label': f'Últimas {h} horas', 'value': h} for h in opcoes_tempo_lista] + [
        {'label': 'Todo o Histórico', 'value': 14 * 24}]
    # --- FIM DA ALTERAÇÃO ---

    return dbc.Container([
        # Seletor de Período
        dbc.Row([
            dbc.Col(dbc.Label("Período (Gráficos):"), width="auto"),
            dbc.Col(
                dcc.Dropdown(
                    id='general-graph-time-selector',
                    options=opcoes_tempo,
                    value=72,  # Mantém 72h como padrão
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

        # --- TIPAGEM E CONVERSÃO DE FUSO HORÁRIO (ROBUSTO) ---
        df_completo['timestamp'] = pd.to_datetime(df_completo['timestamp'])

        # 1. Trata fuso horário
        if df_completo['timestamp'].dt.tz is None:
            df_completo['timestamp'] = df_completo['timestamp'].dt.tz_localize('UTC')
        else:
            df_completo['timestamp'] = df_completo['timestamp'].dt.tz_convert('UTC')

        # 2. Converte para o fuso horário local do Brasil para VISUALIZAÇÃO
        df_completo['timestamp_local'] = df_completo['timestamp'].dt.tz_convert('America/Sao_Paulo')

        df_completo['chuva_mm'] = pd.to_numeric(df_completo['chuva_mm'], errors='coerce')
        df_completo['umidade_1m_perc'] = pd.to_numeric(df_completo['umidade_1m_perc'], errors='coerce')
        df_completo['umidade_2m_perc'] = pd.to_numeric(df_completo['umidade_2m_perc'], errors='coerce')
        df_completo['umidade_3m_perc'] = pd.to_numeric(df_completo['umidade_3m_perc'], errors='coerce')
        # --- FIM DA CONVERSÃO DE FUSO HORÁRIO ---

    except Exception as e:
        # Retorna o erro original, mas melhor formatado
        return dbc.Alert(f"Erro ao ler dados: {e}", color="danger")

    layout_geral = []
    for id_ponto, config in PONTOS_DE_ANALISE.items():
        df_ponto = df_completo[df_completo['id_ponto'] == id_ponto].copy()
        if df_ponto.empty: continue

        # --- INÍCIO DA CORREÇÃO (FILTRO DE TEMPO) ---
        # Em vez de usar .tail(), filtramos pelo período de tempo real
        ultimo_timestamp_no_df = df_ponto['timestamp_local'].max()
        limite_tempo = ultimo_timestamp_no_df - pd.Timedelta(hours=selected_hours)
        df_ponto_plot = df_ponto[df_ponto['timestamp_local'] >= limite_tempo].copy()
        n_horas_titulo = selected_hours
        # --- FIM DA CORREÇÃO ---

        # --- INÍCIO DA ALTERAÇÃO (CÁLCULO DINÂMICO) ---
        # 1. Calcula o acumulado DINÂMICO para o Gráfico (baseado no selected_hours)
        df_chuva_acumulada_completo = processamento.calcular_acumulado_rolling(df_ponto, horas=selected_hours)

        # 2. Filtra o DataFrame do gráfico para o período de tempo
        df_chuva_acumulada_plot = df_chuva_acumulada_completo[
            df_chuva_acumulada_completo['timestamp'] >= df_ponto_plot['timestamp'].min()
            ].copy()
        # --- FIM DA ALTERAÇÃO ---

        # --- INÍCIO DA CORREÇÃO (SettingWithCopyWarning) ---
        # Corrigindo com .loc
        if 'timestamp' in df_chuva_acumulada_plot.columns:
            if df_chuva_acumulada_plot['timestamp'].dt.tz is None:
                df_chuva_acumulada_plot.loc[:, 'timestamp'] = df_chuva_acumulada_plot['timestamp'].dt.tz_localize('UTC')

            df_chuva_acumulada_plot.loc[:, 'timestamp_local'] = df_chuva_acumulada_plot['timestamp'].dt.tz_convert(
                'America/Sao_Paulo')
        else:
            # Este else é seguro, pois 'timestamp_local' já existe
            df_chuva_acumulada_plot.loc[:, 'timestamp_local'] = df_chuva_acumulada_plot['timestamp']
        # --- FIM DA CORREÇÃO ---

        # Gráfico de Chuva
        fig_chuva = make_subplots(specs=[[{"secondary_y": True}]])

        # --- USANDO TIMESTAMP LOCAL PARA O EIXO X ---
        fig_chuva.add_trace(
            go.Bar(x=df_ponto_plot['timestamp_local'], y=df_ponto_plot['chuva_mm'], name='Pluv. Horária',
                   marker_color='#2C3E50', opacity=0.8), secondary_y=False)

        # --- INÍCIO DA ALTERAÇÃO (GRÁFICO DINÂMICO) ---
        fig_chuva.add_trace(
            go.Scatter(x=df_chuva_acumulada_plot['timestamp_local'], y=df_chuva_acumulada_plot['chuva_mm'],
                       name=f'Acumulada ({selected_hours}h)',  # Nome dinâmico
                       mode='lines', line=dict(color='#007BFF', width=2.5)), secondary_y=True)
        # --- FIM DA ALTERAÇÃO ---

        # --- INÍCIO DA ALTERAÇÃO (LEGENDA, MARGEM E TÍTULO DO EIXO) ---
        fig_chuva.update_layout(
            # --- ALTERAÇÃO 1: TÍTULO DO GRÁFICO ---
            title_text=f"Pluviometria - Estação {config['nome']} ({n_horas_titulo}h)",
            # --- FIM DA ALTERAÇÃO 1 ---
            template=TEMPLATE_GRAFICO_MODERNO,
            # Aumentada a margem inferior (b=80)
            margin=dict(l=40, r=20, t=50, b=80),
            # Ajustada a posição Y da legenda para -0.5 (mais para baixo)
            legend=dict(orientation="h", yanchor="bottom", y=-0.5, xanchor='center', x=0.5),
            # Adicionado título do eixo X
            xaxis_title="Data e Hora",
            yaxis_title="Pluv. Horária (mm)",
            yaxis2_title=f"Acumulada ({selected_hours}h)",  # Título do eixo dinâmico
            hovermode="x unified", bargap=0.1)
        # --- FIM DA ALTERAÇÃO ---

        # --- EIXO X (3 EM 3 HORAS, DIAGONAL) ---
        fig_chuva.update_xaxes(
            dtick=3 * 60 * 60 * 1000,  # 3 horas em milissegundos
            tickformat="%d/%m %Hh",  # Formato Dia/Mês Hora
            tickangle=-45  # Rotaciona os labels
        )
        # --- FIM DA ALTERAÇÃO ---

        fig_chuva.update_yaxes(title_text="Pluv. Horária (mm)", secondary_y=False);
        fig_chuva.update_yaxes(title_text=f"Acumulada ({selected_hours}h)", secondary_y=True)  # Título do eixo dinâmico

        # --- REINSERÇÃO: GRÁFICO DE UMIDADE ---
        df_umidade = df_ponto_plot.melt(id_vars=['timestamp_local'],
                                        value_vars=['umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc'],
                                        var_name='Sensor', value_name='Umidade Solo (%)')

        df_umidade['Sensor'] = df_umidade['Sensor'].replace({
            'umidade_1m_perc': '1m',
            'umidade_2m_perc': '2m',
            'umidade_3m_perc': '3m'
        })

        fig_umidade = px.line(df_umidade, x='timestamp_local', y='Umidade Solo (%)', color='Sensor',
                              # --- ALTERAÇÃO 2: TÍTULO DO GRÁFICO ---
                              title=f"Umidade Solo - Estação {config['nome']} ({n_horas_titulo}h)",
                              # --- FIM DA ALTERAÇÃO 2 ---
                              color_discrete_map=CORES_UMIDADE)
        # --- FIM DA REINSERÇÃO ---

        fig_umidade.update_traces(line=dict(width=3))

        # --- INÍCIO DA ALTERAÇÃO (LEGENDA, MARGEM E TÍTULO DO EIXO) ---
        fig_umidade.update_layout(template=TEMPLATE_GRAFICO_MODERNO,
                                  # Aumentada a margem inferior (b=80)
                                  margin=dict(l=40, r=20, t=40, b=80),
                                  # Ajustada a posição Y da legenda
                                  legend=dict(orientation="h", yanchor="bottom", y=-0.5, xanchor="center", x=0.5),
                                  # Adicionado título do eixo X
                                  xaxis_title="Data e Hora")
        # --- FIM DA ALTERAÇÃO ---

        # --- EIXO X (3 EM 3 HORAS, DIAGONAL) ---
        fig_umidade.update_xaxes(
            dtick=3 * 60 * 60 * 1000,  # 3 horas em milissegundos
            tickformat="%d/%m %Hh",  # Formato Dia/Mês Hora
            tickangle=-45  # Rotaciona os labels
        )
        # --- FIM DA ALTERAÇÃO ---

        # Layout Lado a Lado
        col_chuva = dbc.Col(dbc.Card(dbc.CardBody(dcc.Graph(figure=fig_chuva)), className="shadow-sm"), width=12, lg=6,
                            className="mb-4")
        col_umidade = dbc.Col(dbc.Card(dbc.CardBody(dcc.Graph(figure=fig_umidade)), className="shadow-sm"), width=12,
                              lg=6, className="mb-4")
        linha_ponto = dbc.Row([col_chuva, col_umidade], className="mb-4")
        layout_geral.append(linha_ponto)

    if not layout_geral: return dbc.Alert("Nenhum dado.", color="warning")
    return layout_geral