# app.py
import dash
import dash_bootstrap_components as dbc

# --- IMPORTAÇÃO CRÍTICA DO LEAFLET CSS ---
# Este CSS é necessário para que o dl.Map (Leaflet) renderize corretamente
LEAFLET_CSS = [
    "https://unpkg.com/leaflet@1.9.4/dist/leaflet.css",
    "https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" # Embora seja JS, muitas vezes é incluído aqui
]
# --- FIM DA IMPORTAÇÃO CRÍTICA ---

# Use um tema Bootstrap moderno.
THEME = dbc.themes.FLATLY

# Meta tags para responsividade em celular
META_TAGS = [
    {"name": "viewport", "content": "width=device-width, initial-scale=1.0"}
]

app = dash.Dash(__name__,
                # --- CORREÇÃO: COMBINAR O TEMA COM O LEAFLET CSS ---
                external_stylesheets=[THEME] + LEAFLET_CSS,
                # --- FIM DA CORREÇÃO ---
                meta_tags=META_TAGS,
                # ESTA LINHA É A CORREÇÃO.
                # Ela permite que o index.py controle callbacks de
                # componentes que ainda não estão na tela (como 'login-button').
                suppress_callback_exceptions=True
)

app.title = "Monitoramento Geotécnico"
server = app.server

# Constantes globais que outras páginas podem precisar
TEMPLATE_GRAFICO_MODERNO = "plotly_white"