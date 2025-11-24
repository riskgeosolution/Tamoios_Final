# pages/map_view.py (CORRIGIDO v2 - Status Separados)

import dash
from dash import html, dcc, callback, Input, Output
import dash_bootstrap_components as dbc
import dash_leaflet as dl
import pandas as pd
from io import StringIO
import traceback
import numpy as np
import json

from app import app
from config import (
    PONTOS_DE_ANALISE, CONSTANTES_PADRAO, RISCO_MAP, STATUS_MAP_HIERARQUICO, STATUS_MAP_CHUVA
)

def get_layout():
    """ Retorna o layout da página do mapa. """
    try:
        layout = dbc.Container([
            dbc.Row([dbc.Col(
                html.Div([
                    dl.Map(
                        id='mapa-principal', center=[-23.5951, -45.4438], zoom=13,
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
        return layout
    except Exception as e:
        print(f"ERRO CRÍTICO em map_view.get_layout: {e}");
        traceback.print_exc();
        return html.Div([html.H1("Erro Layout Mapa"), html.Pre(traceback.format_exc())])

@app.callback(
    Output('map-pins-layer', 'children'),
    Input('store-ultimo-status', 'data')
)
def update_map_pins(status_json):
    if not status_json:
        return []

    pinos_do_mapa = []
    for id_ponto, config in PONTOS_DE_ANALISE.items():
        status_info = status_json.get(id_ponto, {})
        if not isinstance(status_info, dict): status_info = {}
        
        chuva_72h_pino = status_info.get('chuva_72h', 0.0)
        
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
    return pinos_do_mapa

def create_km_block(id_ponto, config, status_info):
    """
    Cria o bloco de resumo do KM com status de chuva e umidade separados.
    """
    if not isinstance(status_info, dict):
        status_info = {}
    
    # --- Status da Chuva (Independente) ---
    status_chuva_txt = status_info.get('chuva', 'SEM DADOS')
    ultima_chuva_72h = status_info.get('chuva_72h', 0.0)
    cor_chuva_badge = STATUS_MAP_CHUVA.get(status_chuva_txt, "secondary")
    
    # --- Status da Umidade (Independente) ---
    status_umidade_txt = status_info.get('umidade', 'SEM DADOS')
    risco_umidade_num = RISCO_MAP.get(status_umidade_txt, -1)
    _, cor_umidade_badge, cor_umidade_gauge_class = STATUS_MAP_HIERARQUICO.get(risco_umidade_num, STATUS_MAP_HIERARQUICO[-1])

    # --- Lógica de Visualização (Gauges) ---
    chuva_max_visual = 100.0  # Limite para o gauge de chuva (100mm)
    chuva_percent = max(0, min(100, (ultima_chuva_72h / chuva_max_visual) * 100))
    if status_chuva_txt == "SEM DADOS":
        chuva_percent = 0

    umidade_percent_realista = 0
    if risco_umidade_num == 0: umidade_percent_realista = 25
    elif risco_umidade_num == 1: umidade_percent_realista = 50
    elif risco_umidade_num == 2: umidade_percent_realista = 75
    elif risco_umidade_num == 3: umidade_percent_realista = 100

    chuva_gauge = html.Div(
        [
            html.Div(className=f"gauge-bar bg-{cor_chuva_badge}", style={'height': f'{chuva_percent}%'}),
            html.Div(
                [html.Span(f"{ultima_chuva_72h:.0f}"), html.Br(), html.Span("mm", style={'fontSize': '0.8em'})],
                className="gauge-label", style={'fontSize': '2.5em', 'lineHeight': '1.1'}
            )
        ], className="gauge-vertical-container"
    )

    umidade_gauge = html.Div(
        [html.Div(className=f"gauge-bar {cor_umidade_gauge_class}", style={'height': f'{umidade_percent_realista}%'})],
        className="gauge-vertical-container"
    )
    
    chuva_badge = dbc.Badge(status_chuva_txt, color=cor_chuva_badge, className="w-100 mt-1 small badge-black-text")
    umidade_badge = dbc.Badge(status_umidade_txt, color=cor_umidade_badge, className="w-100 mt-1 small badge-black-text")

    link_destino = f"/ponto/{id_ponto}"
    conteudo_bloco = html.Div([
        html.H6(f"Estação {config['nome']}", className="text-center mb-1"),
        dbc.Row([
            dbc.Col([html.Div("Chuva (72h)", className="small text-center"), chuva_gauge, chuva_badge], width=6),
            dbc.Col([html.Div("Umid. Solo", className="small text-center"), umidade_gauge, umidade_badge], width=6),
        ], className="g-0"),
    ], className="km-summary-block")

    return html.A(conteudo_bloco, href=link_destino, style={'textDecoration': 'none', 'color': 'inherit'})

@app.callback(
    Output('map-summary-left-content', 'children'),
    Input('store-ultimo-status', 'data')
)
def update_summary_left(status_json):
    if not status_json:
        return dbc.Spinner(size="sm")
    
    left_blocks = []
    ids_esquerda = ["Ponto-C-KM74", "Ponto-D-KM81"]
    for id_ponto in ids_esquerda:
        if id_ponto in PONTOS_DE_ANALISE:
            config = PONTOS_DE_ANALISE[id_ponto]
            status_info = status_json.get(id_ponto, {})
            if not isinstance(status_info, dict): status_info = {}
            km_block = create_km_block(id_ponto, config, status_info)
            left_blocks.append(km_block)
    return left_blocks if left_blocks else dbc.Alert("Dados indisponíveis (L).", color="warning", className="m-2 small")

@app.callback(
    Output('map-summary-right-content', 'children'),
    Input('store-ultimo-status', 'data')
)
def update_summary_right(status_json):
    if not status_json:
        return dbc.Spinner(size="sm")

    right_blocks = []
    ids_direita = ["Ponto-A-KM67", "Ponto-B-KM72"]
    for id_ponto in ids_direita:
        if id_ponto in PONTOS_DE_ANALISE:
            config = PONTOS_DE_ANALISE[id_ponto]
            status_info = status_json.get(id_ponto, {})
            if not isinstance(status_info, dict): status_info = {}
            km_block = create_km_block(id_ponto, config, status_info)
            right_blocks.append(km_block)
    return right_blocks if right_blocks else dbc.Alert("Dados indisponíveis (R).", color="warning", className="m-2 small")