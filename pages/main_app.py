import dash
from dash import html, dcc
import dash_bootstrap_components as dbc

# Importa o app central (para os assets)
from app import app


def get_navbar():
    """ Retorna a barra de navegação azul (agora com botão Sair) """

    logo_riskgeo_path = app.get_asset_url('LogoMarca RiskGeo Solutions.png')
    logo_tamoios_path = app.get_asset_url('tamoios.png')
    cor_fundo_navbar = '#003366'
    nova_altura_logo = "60px"

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
                            html.H4("SISTEMA DE MONITORAMENTO TAMOIOS", className="mb-0 text-center",
                                    style={'fontWeight': 'bold', 'color': 'white'}),
                            width="auto",
                        ),
                        dbc.Col(
                            dbc.Nav(
                                [
                                    dbc.NavItem(
                                        dbc.NavLink("Mapa Geral", href="/", active="exact", className="text-light",
                                                    style={'font-size': '1.75rem', 'font-weight': '500'})),
                                    dbc.NavItem(dbc.NavLink("Dashboard Geral", href="/dashboard-geral", active="exact",
                                                            className="text-light ms-3",
                                                            style={'font-size': '1.75rem', 'font-weight': '500'})),

                                    # --- CORREÇÃO DO LOGOUT ---
                                    # Garante que o ID 'logout-button' está correto
                                    dbc.NavItem(
                                        dbc.Button(
                                            "Sair",
                                            id='logout-button',
                                            color="danger",
                                            className="ms-5",
                                            n_clicks=0
                                        ),
                                        className="d-flex align-items-center"  # Alinha o botão
                                    )
                                    # --- FIM DA CORREÇÃO ---
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

    # --- INÍCIO DA CORREÇÃO DEFINITIVA (Removido dcc.Location) ---
    # O dcc.Location(id='url-app') foi REMOVIDO daqui.
    # O único dcc.Location(id='url-raiz') no index.py controlará tudo.
    layout = html.Div([
        get_navbar(),

        # O 'page-content' é onde o index.py (Callback 4) irá renderizar
        # as páginas (Mapa, Geral, Específica)
        html.Div(id='page-content')

        # O dcc.Interval e os dcc.Store foram movidos para o index.py
    ])
    # --- FIM DA CORREÇÃO ---

    return layout

