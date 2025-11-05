# pages/map_view.py (CORRIGIDO: Título do card abreviado para "Umid. Solo(72h)")

import dash
from dash import html, dcc, callback, Input, Output
import dash_bootstrap_components as dbc
import dash_leaflet as dl
import pandas as pd
from io import StringIO
import traceback
import numpy as np
import json

# IMPORTAÇÃO CRÍTICA
from app import app
# Importa constantes
from config import PONTOS_DE_ANALISE, CONSTANTES_PADRAO, RISCO_MAP, STATUS_MAP_HIERARQUICO, CHUVA_LIMITE_VERDE, \
    CHUVA_LIMITE_AMARELO, CHUVA_LIMITE_LARANJA
import processamento
import data_source


# --- Layout da Página do Mapa ---
def get_layout():
    """ Retorna o layout da página do mapa. """
    print("Executando map_view.get_layout() (Dois Cards Superiores)")
    try:
        layout = dbc.Container([
            dbc.Row([dbc.Col(
                html.Div([
                    dl.Map(
                        id='mapa-principal', center=[-23.5951, -45.4438], zoom=13,

                        # (Mantém a configuração de Modo Desktop)
                        touchZoom=False,

                        children=[
                            dl.TileLayer(),
                            dl.LayerGroup(id='map-pins-layer'),
                            dbc.Card(
                                [dbc.CardHeader("Estação KM 74 & Estação KM 81", className="text-center small py-1"),
                                 dbc.CardBody(id='map-summary-left-content', children=[dbc.Spinner(size="sm")])],
                                className="map-summary-card map-summary-left"),
                            dbc.Card(
                                [dbc.CardHeader("Estação KM 67 & Estação KM 72", className="text-center small py-1"),
                                 dbc.CardBody(id='map-summary-right-content', children=[dbc.Spinner(size="sm")])],
                                className="map-summary-card map-summary-right")
                        ],
                        style={'width': '100%', 'height': '80vh', 'min-height': '600px'}
                    ),
                ], style={'position': 'relative'}),
                width=12, className="mb-4")])
        ], fluid=True)
        print("Layout do mapa (Dois Cards) criado com sucesso.")
        return layout
    except Exception as e:
        print(f"ERRO CRÍTICO em map_view.get_layout: {e}");
        traceback.print_exc();
        return html.Div(
            [html.H1("Erro Layout Mapa"), html.Pre(traceback.format_exc())])


# --- Callback 1: Atualiza os Pinos no mapa ---
@app.callback(
    Output('map-pins-layer', 'children'),
    Input('store-dados-sessao', 'data')
)
def update_map_pins(dados_json):
    if not dados_json:
        return []

    try:
        df_completo = pd.read_json(StringIO(dados_json), orient='split').copy()
        if 'timestamp' not in df_completo.columns or df_completo.empty:
            print("[map_view] update_map_pins: Histórico vazio ou malformado.")
            return []
        df_completo.loc[:, 'timestamp'] = pd.to_datetime(df_completo['timestamp'], errors='coerce')
        df_validos = df_completo.dropna(subset=['timestamp']).copy()
        if df_validos.empty:
            print("[map_view] update_map_pins: Sem timestamps válidos para plotar.")
            return []
        df_ultimo = df_validos.nlargest(len(PONTOS_DE_ANALISE), 'timestamp')
        ultimo_timestamp = df_ultimo['timestamp'].max()
        df_ultimo = df_ultimo[df_ultimo['timestamp'] == ultimo_timestamp]
        if df_ultimo.empty:
            print("[map_view] update_map_pins: Filtro nlargest falhou, mas deveria ter dados.")
            return []
    except Exception as e:
        print(f"ERRO CRÍTICO em update_map_pins ao processar dados: {e}")
        traceback.print_exc()
        return []

    pinos_do_mapa = []
    df_acumulado_completo = processamento.calcular_acumulado_rolling(df_completo, horas=72)
    df_acumulado_ultimo = df_acumulado_completo.groupby('id_ponto').last().reset_index()

    for id_ponto, config in PONTOS_DE_ANALISE.items():
        dados_ponto_acumulado = df_acumulado_ultimo[df_acumulado_ultimo['id_ponto'] == id_ponto]
        chuva_72h_pino = 0.0
        if not dados_ponto_acumulado.empty:
            try:
                chuva_72h_pino = dados_ponto_acumulado.iloc[0].get('chuva_mm', 0.0)
                if pd.isna(chuva_72h_pino):
                    chuva_72h_pino = 0.0
            except Exception as e:
                print(f"Erro ao ler dados do pino {id_ponto}: {e}")
                chuva_72h_pino = 0.0
        pino = dl.Marker(
            position=config['lat_lon'],
            children=[
                dl.Tooltip(f"Estação {config['nome']}"),
                dl.Popup([
                    html.H5(f"Estação {config['nome']}"),
                    html.P(f"Chuva (72h): {chuva_72h_pino:.1f} mm"),
                    dbc.Button("Ver Dashboard", href=f"/ponto/{id_ponto}", size="sm", color="primary")
                ])
            ]
        )
        pinos_do_mapa.append(pino)
    print(f"[map_view] update_map_pins: Gerados {len(pinos_do_mapa)} pinos.")
    return pinos_do_mapa


# --- Funções e Constantes ---
def get_color_class_chuva(value):
    if pd.isna(value): return "bg-secondary";
    if value <= CHUVA_LIMITE_VERDE:
        return "bg-success";
    elif value <= CHUVA_LIMITE_AMARELO:
        return "bg-warning";
    elif value <= CHUVA_LIMITE_LARANJA:
        return "bg-orange";
    else:
        return "bg-danger"


# --- Função create_km_block ---
def create_km_block(id_ponto, config, df_ponto, status_ponto_txt):
    """
    Cria o bloco de resumo do KM para os cards laterais.
    """

    ultima_chuva_72h = 0.0
    status_chuva_txt = "SEM DADOS"
    status_chuva_col = "secondary"
    cor_chuva_class = "bg-secondary"

    status_chuva_txt = status_ponto_txt
    _, status_chuva_col, cor_chuva_class = STATUS_MAP_HIERARQUICO.get(
        RISCO_MAP.get(status_chuva_txt, -1),
        STATUS_MAP_HIERARQUICO[-1]
    )

    # --- (Status da Umidade travado como INDEFINIDO) ---
    status_umid_txt = "INDEFINIDO"
    status_umid_col = "secondary"
    cor_umidade_class = "bg-secondary"
    risco_umidade = -1
    # --- FIM ---

    try:
        if not df_ponto.empty:
            if 'timestamp' in df_ponto.columns:
                df_ponto.loc[:, 'timestamp'] = pd.to_datetime(df_ponto['timestamp'], errors='coerce')
                df_ponto = df_ponto.dropna(subset=['timestamp']).copy()
            if 'chuva_mm' in df_ponto.columns:
                df_ponto.loc[:, 'chuva_mm'] = pd.to_numeric(df_ponto['chuva_mm'], errors='coerce')
            df_chuva_72h = processamento.calcular_acumulado_rolling(df_ponto, horas=72)
            if not df_chuva_72h.empty:
                chuva_val = df_chuva_72h.iloc[-1]['chuva_mm']
                if not pd.isna(chuva_val):
                    ultima_chuva_72h = chuva_val
    except Exception as e:
        print(f"ERRO GERAL em create_km_block para {id_ponto}: {e}")
        traceback.print_exc()
        ultima_chuva_72h = 0.0
        status_chuva_txt = "ERRO";
        status_chuva_col = "danger";
        cor_chuva_class = "bg-danger"

    # --- Lógica do Gauge de Chuva (Visual) ---
    chuva_max_visual = 90.0
    chuva_percent = max(0, min(100, (ultima_chuva_72h / chuva_max_visual) * 100))
    if status_chuva_txt == "SEM DADOS":
        chuva_percent = 0

    # --- Lógica do Gauge de Umidade (Visual) ---
    umidade_percent_realista = 0
    if risco_umidade == 0:
        umidade_percent_realista = 25
    elif risco_umidade == 1:
        umidade_percent_realista = 50
    elif risco_umidade == 2:
        umidade_percent_realista = 75
    elif risco_umidade == 3:
        umidade_percent_realista = 100

    # --- Montagem dos Gauges ---
    chuva_gauge = html.Div(
        [
            html.Div(className=f"gauge-bar {cor_chuva_class}", style={'height': f'{chuva_percent}%'}),
            html.Div(
                [html.Span(f"{ultima_chuva_72h:.0f}"), html.Br(), html.Span("mm", style={'fontSize': '0.8em'})],
                className="gauge-label", style={'fontSize': '2.5em', 'lineHeight': '1.1'}
            )
        ], className="gauge-vertical-container"
    )
    umidade_gauge = html.Div(
        [
            html.Div(className=f"gauge-bar {cor_umidade_class}", style={'height': f'{umidade_percent_realista}%'})
        ],
        className="gauge-vertical-container"
    )
    chuva_badge = dbc.Badge(status_chuva_txt, color=status_chuva_col, className="w-100 mt-1 small badge-black-text")
    umidade_badge = dbc.Badge(status_umid_txt, color=status_umid_col, className="w-100 mt-1 small badge-black-text")

    # Envolve com Link (mantido)
    link_destino = f"/ponto/{id_ponto}"
    conteudo_bloco = html.Div([
        html.H6(f"Estação {config['nome']}", className="text-center mb-1"),
        dbc.Row([
            dbc.Col([html.Div("Chuva (72h)", className="small text-center"), chuva_gauge, chuva_badge], width=6),

            # --- INÍCIO DA ALTERAÇÃO (Título da Coluna Abreviado) ---
            dbc.Col([html.Div("Umid. Solo(72h)", className="small text-center"), umidade_gauge, umidade_badge],
                    width=6),
            # --- FIM DA ALTERAÇÃO ---

        ], className="g-0"),
    ], className="km-summary-block")

    return html.A(
        conteudo_bloco,
        href=link_destino,
        style={'textDecoration': 'none', 'color': 'inherit'}
    )


# --- Callbacks 2a e 2b ---
@app.callback(
    Output('map-summary-left-content', 'children'),
    [Input('store-dados-sessao', 'data'),
     Input('store-ultimo-status', 'data')]
)
def update_summary_left(dados_json, status_json):
    if not dados_json or not status_json:
        return dbc.Spinner(size="sm")
    try:
        df_completo = pd.read_json(StringIO(dados_json), orient='split').copy()
        if df_completo.empty or 'id_ponto' not in df_completo.columns:
            return dbc.Alert("Dados indisponíveis (L).", color="warning", className="m-2 small")
        df_completo.loc[:, 'timestamp'] = pd.to_datetime(df_completo['timestamp'], errors='coerce')
        df_completo.loc[:, 'chuva_mm'] = pd.to_numeric(df_completo['chuva_mm'], errors='coerce')
        df_completo.loc[:, 'precipitacao_acumulada_mm'] = pd.to_numeric(df_completo['precipitacao_acumulada_mm'],
                                                                        errors='coerce')
        df_completo = df_completo.dropna(subset=['timestamp']).copy()
        status_atual = status_json
        left_blocks = []
        ids_esquerda = ["Ponto-C-KM74", "Ponto-D-KM81"]
        for id_ponto in ids_esquerda:
            if id_ponto in PONTOS_DE_ANALISE:
                config = PONTOS_DE_ANALISE[id_ponto]
                df_ponto = df_completo[df_completo['id_ponto'] == id_ponto].copy()
                status_ponto_txt = status_atual.get(id_ponto, "SEM DADOS")
                km_block = create_km_block(id_ponto, config, df_ponto, status_ponto_txt)
                left_blocks.append(km_block)
        return left_blocks if left_blocks else dbc.Alert("Dados indisponíveis (L).", color="warning",
                                                         className="m-2 small")
    except Exception as e:
        print(f"ERRO GERAL em update_summary_left: {e}")
        traceback.print_exc()
        return dbc.Alert(f"Erro ao carregar dados (L): {e}", color="danger", className="m-2 small")


@app.callback(
    Output('map-summary-right-content', 'children'),
    [Input('store-dados-sessao', 'data'),
     Input('store-ultimo-status', 'data')]
)
def update_summary_right(dados_json, status_json):
    if not dados_json or not status_json:
        return dbc.Spinner(size="sm")
    try:
        df_completo = pd.read_json(StringIO(dados_json), orient='split').copy()
        if df_completo.empty or 'id_ponto' not in df_completo.columns:
            return dbc.Alert("Dados indisponíveis (R).", color="warning", className="m-2 small")
        df_completo.loc[:, 'timestamp'] = pd.to_datetime(df_completo['timestamp'], errors='coerce')
        df_completo.loc[:, 'chuva_mm'] = pd.to_numeric(df_completo['chuva_mm'], errors='coerce')
        df_completo.loc[:, 'precipitacao_acumulada_mm'] = pd.to_numeric(df_completo['precipitacao_acumulada_mm'],
                                                                        errors='coerce')
        df_completo = df_completo.dropna(subset=['timestamp']).copy()
        status_atual = status_json
        right_blocks = []
        ids_direita = ["Ponto-A-KM67", "Ponto-B-KM72"]
        for id_ponto in ids_direita:
            if id_ponto in PONTOS_DE_ANALISE:
                config = PONTOS_DE_ANALISE[id_ponto]
                df_ponto = df_completo[df_completo['id_ponto'] == id_ponto].copy()
                status_ponto_txt = status_atual.get(id_ponto, "SEM DADOS")
                km_block = create_km_block(id_ponto, config, df_ponto, status_ponto_txt)
                right_blocks.append(km_block)
        return right_blocks if right_blocks else dbc.Alert("Dados indisponíveis (R).", color="warning",
                                                           className="m-2 small")
    except Exception as e:
        print(f"ERRO GERAL em update_summary_right: {e}")
        traceback.print_exc()
        return dbc.Alert(f"Erro ao carregar dados (R): {e}", color="danger", className="m-2 small")