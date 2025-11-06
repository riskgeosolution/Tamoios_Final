# pages/login.py (CORRIGIDO: Adicionado copyright dinâmico e "Todos os direitos reservados")

import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
from app import app
import datetime  # <--- NOVO IMPORT PARA PEGAR O ANO ATUAL


def get_layout():
    """ Retorna o layout da página de login. """

    # Pega os logos da pasta /assets
    logo_riskgeo_path = app.get_asset_url('LogoMarca RiskGeo Solutions.png')
    logo_tamoios_path = app.get_asset_url('tamoios.png')

    fundo_path = app.get_asset_url('tamoios_fundo.png')

    # Altura reduzida dos logos (Mantido)
    nova_altura_logo = "90px"

    style_fundo = {
        'backgroundImage': f"url('{fundo_path}')",
        'backgroundSize': 'cover',
        'backgroundPosition': 'center',
        'backgroundRepeat': 'no-repeat'
    }

    layout = dbc.Container([
        dbc.Row(
            dbc.Col(
                dbc.Card([
                    dbc.CardBody([
                        # --- Logos Lado a Lado (Espaçados) ---
                        dbc.Row(
                            [
                                # Coluna 1: Logo Tamoios
                                dbc.Col(
                                    html.Img(src=logo_tamoios_path, height=nova_altura_logo),
                                    width="auto"
                                ),
                                # Coluna 2: Logo RiskGeo
                                dbc.Col(
                                    html.Img(src=logo_riskgeo_path, height=nova_altura_logo),
                                    width="auto"
                                ),
                            ],
                            align="center",
                            justify="around",
                            className="mb-4 pt-3"
                        ),

                        html.H4("Sistema de Monitoramento Geoambiental", className="card-title text-center mb-4"),

                        # Mensagem de erro (inicialmente vazia)
                        html.Div(id='login-error-output', className="text-danger mb-3 text-center"),

                        # --- Campo de senha menor (Mantido) ---
                        dbc.Row(
                            dbc.Col(
                                dbc.Input(
                                    id='input-password',
                                    type='password',
                                    placeholder='Digite sua senha',
                                    n_submit=0
                                ),
                                width=10,
                                lg=6
                            ),
                            justify="center",
                            className="mb-4"
                        ),

                        # --- Botão Acessar (Mantido) ---
                        dbc.Row(
                            dbc.Col(
                                dbc.Button(
                                    "Acessar",
                                    id='btn-login',
                                    color='primary',
                                    style={'font-size': '1.1rem', 'font-weight': 'bold', 'padding': '0.5rem 2rem'},
                                    n_clicks=0
                                ),
                                width="auto"
                            ),
                            justify="center",
                            className="mb-3"
                        ),

                        # --- INÍCIO DA ALTERAÇÃO (Copyright Dinâmico) ---
                        html.P(
                            f"© {datetime.datetime.now().year} RiskGeo Solutions Engenharia e Consultoria Ltda. Todos os direitos reservados.",
                            className="text-center text-muted small mt-3 mb-0"
                        )
                        # --- FIM DA ALTERAÇÃO ---

                    ])
                ], style={'backgroundColor': 'rgba(255, 255, 255, 0.8)'}),

                width=12, md=10, lg=8
            ),
            justify="center",
            align="center",
            className="vh-100"
        )
    ], fluid=True, style=style_fundo)

    return layout