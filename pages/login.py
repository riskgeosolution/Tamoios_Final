# pages/login.py (FINAL CORRIGIDO: Fundo do Card Transparente)

import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
from app import app


def get_layout():
    """ Retorna o layout da página de login. """

    # Pega os logos da pasta /assets
    logo_riskgeo_path = app.get_asset_url('LogoMarca RiskGeo Solutions.png')
    logo_tamoios_path = app.get_asset_url('tamoios.png')

    # --- NOVO: Obtém o caminho do fundo para CSS Inline ---
    fundo_path = app.get_asset_url('tamoios_fundo.png')
    # --- FIM DA ALTERAÇÃO ---

    # Altura reduzida dos logos (Mantido)
    nova_altura_logo = "90px"

    # --- NOVO: Estilo de fundo Inline ---
    style_fundo = {
        'backgroundImage': f"url('{fundo_path}')",
        'backgroundSize': 'cover',
        'backgroundPosition': 'center',
        'backgroundRepeat': 'no-repeat'
    }
    # --- FIM DA ALTERAÇÃO ---

    layout = dbc.Container([
        dbc.Row(
            dbc.Col(
                # --- INÍCIO DA ALTERAÇÃO: Adicionado estilo de transparência ao Card ---
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
                        # --- FIM DA ALTERAÇÃO ---

                        html.H4("Sistema de Monitoramento", className="card-title text-center mb-4"),

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
                        # --- FIM DA ALTERAÇÃO ---

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
                        )
                        # --- FIM DA ALTERAÇÃO DO BOTÃO ---
                    ])
                ], style={'backgroundColor': 'rgba(255, 255, 255, 0.8)'}),  # <<<< AQUI ESTÁ A ALTERAÇÃO
                # --- FIM DA ALTERAÇÃO ---

                width=12, md=10, lg=8
            ),
            justify="center",
            align="center",
            className="vh-100"
        )
    ], fluid=True, style=style_fundo)

    return layout