# app.py (CORRIGIDO: Força zoom-out inicial com scale=0.25)

import dash
import dash_bootstrap_components as dbc

# --- IMPORTAÇÃO CRÍTICA DO LEAFLET CSS ---
LEAFLET_CSS = [
    "https://unpkg.com/leaflet@1.9.4/dist/leaflet.css",
    "https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
]
# --- FIM DA IMPORTAÇÃO CRÍTICA ---

# Use um tema Bootstrap moderno.
THEME = dbc.themes.FLATLY

# Dizemos explicitamente ao Dash para carregar o nosso style.css da pasta /assets
MEU_CSS_LOCAL = [
    "/assets/style.css"
]

# --- INÍCIO DA ALTERAÇÃO (Força "Modo Desktop" + Zoom Out 0.25) ---
# width=1200: Força o layout de desktop.
# initial-scale=0.25: Um valor mais seguro para forçar o zoom-out.
META_TAGS = [
    {"name": "viewport", "content": "width=1200, initial-scale=0.25"}
]
# --- FIM DA ALTERAÇÃO ---

app = dash.Dash(__name__,
                external_stylesheets=[THEME] + LEAFLET_CSS + MEU_CSS_LOCAL,

                # --- INÍCIO DA ALTERAÇÃO (Aplica a meta tag) ---
                meta_tags=META_TAGS,
                # --- FIM DA ALTERAÇÃO ---

                suppress_callback_exceptions=True
                )

app.title = "Monitoramento Geoambiental"
server = app.server  # <-- O Gunicorn procura esta variável 'server'

# Constantes globais que outras páginas podem precisar
TEMPLATE_GRAFICO_MODERNO = "plotly_white"