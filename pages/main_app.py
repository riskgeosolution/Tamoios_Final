# pages/main_app.py (REVERTIDO para a versão original de Desktop)

import dash
from dash import html, dcc
import dash_bootstrap_components as dbc

# Importa o app central (para os assets)
from app import app


def get_navbar():
    """ Retorna a barra de navegação azul (Layout original do Desktop) """

    logo_riskgeo_path = app.get_asset_url('LogoMarca RiskGeo Solutions.png')
    logo_tamoios_path = app.get_asset_url('tamoios.png')
    cor_fundo_navbar = '#003366'
    nova_altura_logo = "50px"

    navbar = dbc.Navbar(
        dbc.Container(
            [
                dbc.Row(
                    [
                        dbc.Col(
                            dbc.Row(
                                [
                                    dbc.Col(html.A(html.Img(src=logo_tamoios_path, height=nova_altura_logo), href="/"),
                                            width="auto"),
                                    dbc.Col(html.Img(src=logo_riskgeo_path, height=nova_altura_logo, className="ms-3"),
                                            width="auto"),
                                ],
                                align="center",
                                className="g-0",
                            ),
                            width="auto",
                        ),
                        dbc.Col(
                            html.H4("SISTEMA DE MONITORAMENTO GEOAMBIENTAL", className="mb-0 text-center",
                                    style={'fontWeight': 'bold', 'color': 'white', 'font-size': '1.3rem'}),
                            width="auto",
                        ),
                        dbc.Col(
                            dbc.Nav(
                                [
                                    dbc.NavItem(
                                        dbc.NavLink("Mapa Geral", href="/", active="exact", className="text-light",
                                                    style={'font-size': '1.0rem', 'font-weight': '500'})),
                                    dbc.NavItem(dbc.NavLink("Dashboard Geral", href="/dashboard-geral", active="exact",
                                                            className="text-light ms-3",
                                                            style={'font-size': '1.0rem', 'font-weight': '500'})),
                                    dbc.NavItem(
                                        dbc.Button(
                                            "Sair",
                                            id='logout-button',
                                            color="danger",
                                            className="ms-5",
                                            n_clicks=0
                                        ),
                                        className="d-flex align-items-center"
                                    )
                                ],
                                navbar=True,
                                className="flex-nowrap",
                            ),
                            width="auto",
                        ),
                    ],
                    align="center",
                    className="w-100 flex-nowrap",
                    justify="between",
                ),
            ],
            fluid=True
        ),
        style={'backgroundColor': cor_fundo_navbar},
        dark=True,
        className="mb-4"
    )
    return navbar


def get_layout():
    """ Retorna o layout principal do app (depois do login). """
    layout = html.Div([
        get_navbar(),
        html.Div(id='page-content')
    ])
    return layout