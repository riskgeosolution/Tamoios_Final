import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
from app import app


def get_layout():
    """ Retorna o layout da página de login. """

    # Pega os logos da pasta /assets
    logo_riskgeo_path = app.get_asset_url('LogoMarca RiskGeo Solutions.png')
    logo_tamoios_path = app.get_asset_url('tamoios.png')

    layout = dbc.Container([
        dbc.Row(
            dbc.Col(
                dbc.Card([
                    dbc.CardBody([
                        # --- 1. Logos Lado a Lado com Espaço ---
                        dbc.Row([
                            # Usando width="auto" para que a coluna se ajuste ao tamanho do logo
                            dbc.Col(
                                html.Img(src=logo_tamoios_path, height="112px"),
                                width="auto"
                            ),
                            dbc.Col(
                                html.Img(src=logo_riskgeo_path, height="112px"),
                                width="auto"
                            ),
                        ],
                        # justify="around" coloca espaço ao redor deles
                        justify="around", # Isso vai adicionar o espaço entre eles
                        align="center",
                        className="mb-4 pt-3"),
                        # --- FIM DA ALTERAÇÃO ---

                        html.H4("Sistema de Monitoramento", className="card-title text-center mb-4"),

                        # Mensagem de erro (inicialmente vazia)
                        html.Div(id='login-error-output', className="text-danger mb-3 text-center"),

                        # --- CAMPO DE SENHA (Diminuído e Centralizado) ---
                        dbc.Row(
                            dbc.Col(
                                dbc.Input(
                                    id='input-password', # <-- ID CORRETO
                                    type='password',
                                    placeholder='Digite sua senha',
                                    n_submit=0  # Permite apertar Enter
                                ),
                                # Diminui a largura da coluna (10 de 12 no celular, 8 de 12 no desktop)
                                width=10,
                                lg=8
                            ),
                            justify="center", # Centraliza a coluna
                            className="mb-4" # Margem que estava no Input
                        ),
                        # --- FIM DA ALTERAÇÃO ---

                        # --- 2. Botão Acessar (Centralizado e Estilizado) ---
                        dbc.Row(
                            dbc.Col(
                                dbc.Button(
                                    "Acessar",
                                    id='btn-login', # <-- ID CORRETO
                                    color='primary',
                                    # Removi w-100 e adicionei estilo
                                    style={'font-size': '1.1rem', 'font-weight': 'bold', 'padding': '0.5rem 2rem'},
                                    n_clicks=0
                                ),
                                width="auto" # Ocupa apenas o tamanho do botão
                            ),
                            justify="center", # Centraliza a coluna
                            className="mb-3" # Margem inferior
                        )
                        # --- FIM DA ALTERAÇÃO DO BOTÃO ---
                    ])
                ]),
                # --- 3. Aumentando o card (a "parte branca") ---
                width=12, md=8, lg=6 # Aumentado de md=6, lg=4
            ),
            justify="center",
            align="center",
            className="vh-100"  # Centraliza verticalmente na tela
        )
    ], fluid=True, className="bg-light")  # Fundo claro para a página

    return layout

