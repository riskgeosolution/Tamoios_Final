# pages/main_app.py (CORRIGIDO: Restaura layout desktop e mantém responsividade)

import dash
from dash import html, dcc
import dash_bootstrap_components as dbc

# Importa o app central (para os assets)
from app import app


def get_navbar():
    """ Retorna a barra de navegação azul (agora responsiva E correta no desktop) """

    logo_riskgeo_path = app.get_asset_url('LogoMarca RiskGeo Solutions.png')
    logo_tamoios_path = app.get_asset_url('tamoios.png')
    cor_fundo_navbar = '#003366'
    nova_altura_logo = "50px"

    # --- INÍCIO DA CORREÇÃO (Estrutura Responsiva Simplificada) ---

    # 1. Logos (agora como 'brand' para ficarem sempre visíveis)
    logos_brand = dbc.NavbarBrand(
        dbc.Row(
            [
                dbc.Col(html.Img(src=logo_tamoios_path, height=nova_altura_logo),
                        width="auto"),
                dbc.Col(html.Img(src=logo_riskgeo_path, height=nova_altura_logo, className="ms-3"),
                        width="auto"),
            ],
            align="center",
            className="g-0",
        ),
        href="/"  # Linka os logos para a página inicial
    )

    # 2. Título Central (Visível apenas em Desktop/Telas Grandes)
    titulo_central = html.H4(
        "SISTEMA DE MONITORAMENTO GEOAMBIENTAL",
        className="mb-0 text-center d-none d-lg-block mx-auto",
        # d-none d-lg-block (esconde no celular), mx-auto (centraliza)
        style={'fontWeight': 'bold', 'color': 'white', 'font-size': '1.3rem'}
    )

    # 3. Links de Navegação (que irão para o menu hamburger)
    links_nav = dbc.Nav(
        [
            dbc.NavItem(
                dbc.NavLink("Mapa Geral", href="/", active="exact", className="text-light",
                            style={'font-size': '1.0rem', 'font-weight': '500'})),
            dbc.NavItem(dbc.NavLink("Dashboard Geral", href="/dashboard-geral", active="exact",
                                    className="text-light ms-lg-3",  # margem 'lg'
                                    style={'font-size': '1.0rem', 'font-weight': '500'})),
            dbc.NavItem(
                dbc.Button(
                    "Sair",
                    id='logout-button',
                    color="danger",
                    className="ms-lg-5",  # margem 'lg'
                    n_clicks=0
                ),
                className="d-flex align-items-center mt-3 mt-lg-0"  # Adiciona margem no celular
            )
        ],
        navbar=True,
        className="ms-auto flex-column flex-lg-row align-items-center"  # ms-auto (empurra para a direita no desktop)
    )

    navbar = dbc.Navbar(
        dbc.Container(
            [
                logos_brand,
                titulo_central,
                dbc.NavbarToggler(id="navbar-toggler", n_clicks=0),
                dbc.Collapse(
                    links_nav,
                    id="navbar-collapse",
                    is_open=False,  # Começa fechado
                    navbar=True,
                )
            ],
            fluid=True
        ),
        style={'backgroundColor': cor_fundo_navbar},
        dark=True,
        className="mb-4"
    )
    # --- FIM DA CORREÇÃO ---
    return navbar


def get_layout():
    """ Retorna o layout principal do app (depois do login). """
    layout = html.Div([
        get_navbar(),
        html.Div(id='page-content')
    ])
    return layout