import dash
from dash import html, dcc, callback, Input, Output
import dash_bootstrap_components as dbc
import dash_leaflet as dl
import pandas as pd
from io import StringIO
import traceback
import numpy as np
import json

# Importa o app central e helpers
from app import app
# Importa constantes do config.py
from config import PONTOS_DE_ANALISE, CONSTANTES_PADRAO, RISCO_MAP, STATUS_MAP_HIERARQUICO, CHUVA_LIMITE_VERDE, \
    CHUVA_LIMITE_AMARELO, CHUVA_LIMITE_LARANJA
# O processamento.py ainda é usado para o cálculo da chuva (embora o status venha do worker)
import processamento
import data_source  # Importa data_source para as colunas


# --- Layout da Página do Mapa (Mantido) ---
def get_layout():
    """ Retorna o layout da página do mapa. """
    print("Executando map_view.get_layout() (Dois Cards Superiores)")
    try:
        layout = dbc.Container([
            dbc.Row([dbc.Col(
                html.Div([
                    dl.Map(
                        id='mapa-principal', center=[-23.5951, -45.4438], zoom=13,
                        children=[
                            dl.TileLayer(),
                            dl.LayerGroup(id='map-pins-layer'),  # Camada para pinos padrão
                            dbc.Card([dbc.CardHeader("KM 74 & KM 81", className="text-center small py-1"),
                                      dbc.CardBody(id='map-summary-left-content', children=[dbc.Spinner(size="sm")])],
                                     className="map-summary-card map-summary-left"),
                            dbc.Card([dbc.CardHeader("KM 67 & KM 72", className="text-center small py-1"),
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
# (CORRIGIDO para desenhar TODOS os pinos, mesmo "SEM DADOS")
@app.callback(
    Output('map-pins-layer', 'children'),
    Input('store-dados-sessao', 'data')  # Apenas dados
)
def update_map_pins(dados_json):
    if not dados_json:
        return []

    try:
        df_completo = pd.read_json(StringIO(dados_json), orient='split')

        # --- INÍCIO DA CORREÇÃO (Bug do Pino Sumido) ---
        # Verifica se o DF está vazio ou malformado
        if 'timestamp' not in df_completo.columns or df_completo.empty:
            print("[map_view] update_map_pins: Histórico vazio ou malformado, sem pinos.")
            df_ultimo = pd.DataFrame(columns=df_completo.columns)  # Cria DF vazio com colunas
        else:
            # Pega o timestamp mais recente
            ultimo_timestamp = df_completo['timestamp'].max()
            if pd.isna(ultimo_timestamp):
                print("[map_view] update_map_pins: Timestamp máximo é NaT, sem pinos.")
                df_ultimo = pd.DataFrame(columns=df_completo.columns)  # Cria DF vazio com colunas
            else:
                # Filtra o DF para conter APENAS os dados do último timestamp
                df_ultimo = df_completo[df_completo['timestamp'] == ultimo_timestamp]
        # --- FIM DA CORREÇÃO ---

    except Exception as e:
        print(f"ERRO em update_map_pins ao processar dados: {e}")
        return []

    pinos_do_mapa = []

    # Itera sobre os PONTOS DE ANALISE (do config.py) e não sobre o DF
    for id_ponto, config in PONTOS_DE_ANALISE.items():

        # Pega os dados deste ponto no último timestamp
        dados_ponto = df_ultimo[df_ultimo['id_ponto'] == id_ponto]

        # Se não houver dados no último timestamp (o que acontece com "SEM DADOS")
        # nós ainda desenhamos o pino, mas com chuva 0.0
        if dados_ponto.empty:
            # print(f"[map_view] update_map_pins: Sem dados no último timestamp para {id_ponto}. Desenhando pino com 0mm.")
            chuva_72h_pino = 0.0
        else:
            try:
                # Se houver dados, pega a chuva acumulada
                chuva_72h_pino = dados_ponto.iloc[0].get('precipitacao_acumulada_mm', 0.0)
                if pd.isna(chuva_72h_pino):
                    chuva_72h_pino = 0.0
            except Exception as e:
                print(f"Erro ao ler dados do pino {id_ponto}: {e}")
                chuva_72h_pino = 0.0

        # Cria o pino (agora sempre cria, mesmo com 0.0mm)
        pino = dl.Marker(
            position=config['lat_lon'],
            children=[
                dl.Tooltip(config['nome']),
                dl.Popup([
                    html.H5(config['nome']),
                    # Mostra o acumulado de 72h (que será 0.0 para "SEM DADOS")
                    html.P(f"Chuva (72h): {chuva_72h_pino:.1f} mm"),
                    dbc.Button("Ver Dashboard", href=f"/ponto/{id_ponto}", size="sm", color="primary")
                ])
            ]
        )
        pinos_do_mapa.append(pino)

    return pinos_do_mapa


# --- Funções e Constantes ---
def get_color_class_chuva(value):
    """ Retorna a classe CSS de cor para o gauge de chuva. """
    if pd.isna(value): return "bg-secondary";
    if value <= CHUVA_LIMITE_VERDE:
        return "bg-success";
    elif value <= CHUVA_LIMITE_AMARELO:
        return "bg-warning";
    elif value <= CHUVA_LIMITE_LARANJA:
        return "bg-orange";
    else:
        return "bg-danger"


# --- Função create_km_block (CORRIGIDA para ler o status do dcc.Store) ---
def create_km_block(id_ponto, config, df_ponto, status_ponto_txt):
    """
    Cria o bloco de resumo do KM para os cards laterais.
    Recebe o status_ponto_txt (LIVRE, ALERTA, etc.) diretamente.
    """

    ultima_chuva_72h = 0.0

    # --- Status da Chuva (baseado no status geral) ---
    # (Isso garante que o card "Chuva" e "Status Geral" mostrem o mesmo)
    status_chuva_txt = status_ponto_txt

    # Mapeia o texto do status para a cor do Bootstrap
    # (Usa os mapas importados do config.py)
    _, status_chuva_col, cor_chuva_class = STATUS_MAP_HIERARQUICO.get(
        RISCO_MAP.get(status_chuva_txt, -1),
        STATUS_MAP_HIERARQUICO[-1]  # Default é "SEM DADOS"
    )

    # --- Status da Umidade (Placeholder) ---
    # (Na arquitetura atual, os cards do mapa mostram apenas o Status Geral,
    # que é baseado principalmente na CHUVA vinda da API)
    status_umid_txt = status_ponto_txt
    status_umid_col = status_chuva_col
    cor_umidade_class = cor_chuva_class
    risco_umidade = RISCO_MAP.get(status_umid_txt, -1)

    try:
        if not df_ponto.empty:
            # Pega o último dado de chuva acumulada (apenas para o NÚMERO no gauge)
            df_chuva_72h = processamento.calcular_acumulado_72h(df_ponto)
            if not df_chuva_72h.empty:
                chuva_val = df_chuva_72h.iloc[-1]['chuva_mm']
                if not pd.isna(chuva_val):
                    ultima_chuva_72h = chuva_val

    except Exception as e:
        print(f"ERRO GERAL em create_km_block para {id_ponto}: {e}")
        ultima_chuva_72h = 0.0
        status_chuva_txt = "ERRO";
        status_chuva_col = "danger";
        cor_chuva_class = "bg-danger"
        status_umid_txt, status_umid_col, cor_umidade_class = "ERRO", "danger", "bg-danger"

    # --- Lógica do Gauge de Chuva (Visual) ---
    chuva_max_visual = 90.0
    chuva_percent = max(0, min(100, (ultima_chuva_72h / chuva_max_visual) * 100))
    if status_chuva_txt == "SEM DADOS":
        chuva_percent = 0  # Gauge de "SEM DADOS" fica cinza e vazio

    # --- Lógica do Gauge de Umidade (Visual) ---
    # (O gauge de umidade agora reflete o Status Geral)
    umidade_percent_realista = 0
    if risco_umidade == 0:
        umidade_percent_realista = 25
    elif risco_umidade == 1:
        umidade_percent_realista = 50
    elif risco_umidade == 2:
        umidade_percent_realista = 75
    elif risco_umidade == 3:
        umidade_percent_realista = 100
    if status_umid_txt == "SEM DADOS":
        umidade_percent_realista = 0  # Gauge de "SEM DADOS" fica cinza e vazio

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
        [html.Div(className=f"gauge-bar {cor_umidade_class}", style={'height': f'{umidade_percent_realista}%'})],
        className="gauge-vertical-container"
    )
    chuva_badge = dbc.Badge(status_chuva_txt, color=status_chuva_col, className="w-100 mt-1 small badge-black-text")

    # Badge de Umidade (Agora chamado de "Status Geral" para clareza)
    umidade_badge = dbc.Badge(status_umid_txt, color=status_umid_col, className="w-100 mt-1 small badge-black-text")

    # Envolve com Link (mantido)
    link_destino = f"/ponto/{id_ponto}"
    conteudo_bloco = html.Div([
        html.H6(config['nome'], className="text-center mb-1"),
        dbc.Row([
            dbc.Col([html.Div("Chuva (72h)", className="small text-center"), chuva_gauge, chuva_badge], width=6),
            # Altera o título do gauge de "Umidade" para "Status Geral"
            dbc.Col([html.Div("Status Geral", className="small text-center"), umidade_gauge, umidade_badge], width=6),
        ], className="g-0"),
    ], className="km-summary-block")

    return html.A(
        conteudo_bloco,
        href=link_destino,
        style={'textDecoration': 'none', 'color': 'inherit'}
    )


# --- Callbacks 2a e 2b (Callbacks que USAM create_km_block) ---
# (CORRIGIDOS para ler o status do dcc.Store)
@app.callback(
    Output('map-summary-left-content', 'children'),
    [Input('store-dados-sessao', 'data'),
     Input('store-ultimo-status', 'data')]  # <-- Ouve o status
)
def update_summary_left(dados_json, status_json):
    if not dados_json or not status_json:
        return dbc.Spinner(size="sm")

    try:
        df_completo = pd.read_json(StringIO(dados_json), orient='split')
        # Garante que as colunas existem (para o df_ponto)
        if 'id_ponto' not in df_completo.columns:
            df_completo = pd.DataFrame(columns=data_source.COLUNAS_HISTORICO)

        status_atual = json.loads(status_json)

        left_blocks = []
        ids_esquerda = ["Ponto-C-KM74", "Ponto-D-KM81"]

        for id_ponto in ids_esquerda:
            if id_ponto in PONTOS_DE_ANALISE:
                config = PONTOS_DE_ANALISE[id_ponto]
                df_ponto = df_completo[df_completo['id_ponto'] == id_ponto]
                # Pega o status do Ponto (do worker)
                status_ponto_txt = status_atual.get(id_ponto, "SEM DADOS")

                # Envia os dados E o status
                km_block = create_km_block(id_ponto, config, df_ponto, status_ponto_txt)
                left_blocks.append(km_block)

        return left_blocks if left_blocks else dbc.Alert("Dados indisponíveis (L).", color="warning",
                                                         className="m-2 small")
    except Exception as e:
        print(f"ERRO GERAL em update_summary_left: {e}")
        traceback.print_exc()  # Adiciona traceback para debug
        return dbc.Alert(f"Erro ao carregar dados (L): {e}", color="danger", className="m-2 small")


@app.callback(
    Output('map-summary-right-content', 'children'),
    [Input('store-dados-sessao', 'data'),
     Input('store-ultimo-status', 'data')]  # <-- Ouve o status
)
def update_summary_right(dados_json, status_json):
    if not dados_json or not status_json:
        return dbc.Spinner(size="sm")

    try:
        df_completo = pd.read_json(StringIO(dados_json), orient='split')
        # Garante que as colunas existem (para o df_ponto)
        if 'id_ponto' not in df_completo.columns:
            df_completo = pd.DataFrame(columns=data_source.COLUNAS_HISTORICO)

        status_atual = json.loads(status_json)

        right_blocks = []
        ids_direita = ["Ponto-A-KM67", "Ponto-B-KM72"]

        for id_ponto in ids_direita:
            if id_ponto in PONTOS_DE_ANALISE:
                config = PONTOS_DE_ANALISE[id_ponto]
                df_ponto = df_completo[df_completo['id_ponto'] == id_ponto]
                # Pega o status do Ponto (do worker)
                status_ponto_txt = status_atual.get(id_ponto, "SEM DADOS")

                # Envia os dados E o status
                km_block = create_km_block(id_ponto, config, df_ponto, status_ponto_txt)
                right_blocks.append(km_block)

        return right_blocks if right_blocks else dbc.Alert("Dados indisponíveis (R).", color="warning",
                                                           className="m-2 small")
    except Exception as e:
        print(f"ERRO GERAL em update_summary_right: {e}")
        traceback.print_exc()  # Adiciona traceback para debug
        return dbc.Alert(f"Erro ao carregar dados (R): {e}", color="danger", className="m-2 small")

