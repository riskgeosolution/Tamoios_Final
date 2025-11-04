# app.py (CORRIGIDO: Apenas cria o app, não o executa)

import dash
import dash_bootstrap_components as dbc

LEAFLET_CSS = [
    "https://unpkg.com/leaflet@1.9.4/dist/leaflet.css",
    "https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
]
THEME = dbc.themes.FLATLY
MEU_CSS_LOCAL = ["/assets/style.css"]
META_TAGS = [
    {"name": "viewport", "content": "width=device-width, initial-scale=1.0"}
]

app = dash.Dash(__name__,
                external_stylesheets=[THEME] + LEAFLET_CSS + MEU_CSS_LOCAL,
                meta_tags=META_TAGS,
                suppress_callback_exceptions=True
)

app.title = "Monitoramento Geotécnico"
server = app.server # <-- Gunicorn procura por esta variável 'server'

TEMPLATE_GRAFICO_MODERNO = "plotly_white"

# IMPORTANTE: REMOVEMOS O 'if __name__ == "__main__":' DAQUI