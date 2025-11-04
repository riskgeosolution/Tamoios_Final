# app.py (FINAL: Apenas define o app para ser usado pelo index.py)

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

# --- INÍCIO DA ALTERAÇÃO ---
# Dizemos explicitamente ao Dash para carregar o nosso style.css da pasta /assets
MEU_CSS_LOCAL = [
    "/assets/style.css"
]
# --- FIM DA ALTERAÇÃO ---

# Meta tags para responsividade em celular
META_TAGS = [
    {"name": "viewport", "content": "width=device-width, initial-scale=1.0"}
]

app = dash.Dash(__name__,
                # --- CORREÇÃO: COMBINAR TODOS OS CSS ---
                external_stylesheets=[THEME] + LEAFLET_CSS + MEU_CSS_LOCAL,
                # --- FIM DA CORREÇÃO ---
                meta_tags=META_TAGS,
                suppress_callback_exceptions=True
)

app.title = "Monitoramento Geotécnico"
server = app.server # <-- O Gunicorn procura esta variável 'server'

# Constantes globais que outras páginas podem precisar
TEMPLATE_GRAFICO_MODERNO = "plotly_white"

# IMPORTANTE: A seção 'if __name__ == "__main__":' foi removida.
# O servidor (server) agora é iniciado pelo Gunicorn através do index.py.