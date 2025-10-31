# gerador_pdf.py (FINAL CONSOLIDADO)

import io
from fpdf import FPDF
import plotly.io as pio
import pandas as pd
from datetime import datetime
import traceback

# Importa as constantes dos pontos para usar nos títulos
try:
    from config import PONTOS_DE_ANALISE
except ImportError:
    PONTOS_DE_ANALISE = {}


class PDF(FPDF):
    """ Classe FPDF customizada com cabeçalho e rodapé """

    def header(self):
        self.set_font("Arial", "B", 12)
        self.cell(0, 10, "Relatório de Monitoramento Geotécnico", 0, 1, "C")
        self.set_font("Arial", "", 8)
        self.cell(0, 5, f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}", 0, 1, "C")
        self.ln(10)

    def footer(self):
        self.set_y(-15)
        self.set_font("Arial", "I", 8)
        self.cell(0, 10, f"Página {self.page_no()}", 0, 0, "C")


def criar_relatorio_em_memoria(df_periodo, fig_chuva, fig_umidade, status_atual, cor_status):
    """
    Gera um relatório PDF de Dados (Gráficos e Tabela) e retorna como bytes.
    NOTE: As figuras são tratadas como stubs para garantir que o download funcione.
    """
    pdf = PDF()
    pdf.add_page()

    # Tenta obter o nome do ponto a partir do DF
    if 'id_ponto' in df_periodo.columns and not df_periodo.empty:
        id_ponto = df_periodo['id_ponto'].iloc[0]
        config = PONTOS_DE_ANALISE.get(id_ponto, {"nome": id_ponto})
        nome_ponto = config['nome']
    else:
        nome_ponto = "Dados Consolidados"

    # 1. Adiciona Status
    pdf.set_font("Arial", "B", 14)
    if cor_status == "danger":
        pdf.set_text_color(220, 53, 69)
    elif cor_status == "warning":
        pdf.set_text_color(255, 193, 7)
    elif cor_status == "success":
        pdf.set_text_color(40, 167, 69)
    else:
        pdf.set_text_color(108, 117, 125)

    pdf.cell(0, 10, f"Estação: {nome_ponto}", 0, 1, "L")
    pdf.cell(0, 10, f"Status do Período: {status_atual}", 0, 1, "L")
    pdf.set_text_color(0, 0, 0)
    pdf.ln(5)

    # 2. Adiciona Gráficos (Stub: Apenas se as figuras forem passadas)
    if fig_chuva and fig_umidade:
        try:
            # NOTE: A conversão Plotly-para-PNG via pio.to_image é a lógica real que viria aqui
            img_chuva_bytes = pio.to_image(fig_chuva, format="png", width=800, height=350)
            img_umidade_bytes = pio.to_image(fig_umidade, format="png", width=800, height=350)

            pdf.set_font("Arial", "B", 12)
            pdf.cell(0, 10, "Gráfico de Chuva Acumulada (72h)", 0, 1, "L")
            pdf.image(io.BytesIO(img_chuva_bytes), x=pdf.get_x() + 10, y=pdf.get_y(), w=180)
            pdf.ln(95)

            pdf.cell(0, 10, "Gráfico de Variação da Umidade Solo", 0, 1, "L")
            pdf.image(io.BytesIO(img_umidade_bytes), x=pdf.get_x() + 10, y=pdf.get_y(), w=180)
            pdf.ln(95)
        except Exception:
            pdf.set_font("Arial", "B", 12)
            pdf.cell(0, 10, "Aviso: Não foi possível gerar gráficos.", 0, 1, "L")
            pdf.ln(5)

    # 3. Adiciona Tabela de Dados (Tabela com dados filtrados)
    pdf.add_page()
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 10, "Dados Brutos do Período Selecionado", 0, 1, "L")
    pdf.ln(5)

    df_tabela = df_periodo.copy()
    if 'timestamp' in df_tabela.columns:
        # Converte para o fuso horário local e formata para exibição
        df_tabela['timestamp'] = pd.to_datetime(df_tabela['timestamp']).dt.tz_convert('America/Sao_Paulo').dt.strftime(
            '%d/%m %H:%M')

    # Colunas a serem exibidas
    cols_display = ['timestamp', 'chuva_mm', 'umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc']
    df_tabela = df_tabela[[col for col in cols_display if col in df_tabela.columns]].fillna('-')

    pdf.set_font("Arial", "", 8)
    col_width = pdf.w / (len(df_tabela.columns) + 1)

    # Cabeçalho da Tabela
    for col in df_tabela.columns:
        pdf.cell(col_width * 1.2, 5, col, 1)
    pdf.ln()

    # Dados da Tabela
    for index, row in df_tabela.iterrows():
        if pdf.get_y() > (pdf.h - 30):
            pdf.add_page()
            pdf.set_font("Arial", "", 8)
            for col in df_tabela.columns:
                pdf.cell(col_width * 1.2, 5, col, 1)
            pdf.ln()

        for col in df_tabela.columns:
            pdf.cell(col_width * 1.2, 5, str(row[col]), 1)
        pdf.ln()

    # 4. Retorna os bytes (bytearray ou bytes)
    return pdf.output(dest='S')


def criar_relatorio_logs_em_memoria(id_ponto, logs_do_ponto):
    """
    Gera um relatório PDF simples contendo uma lista de logs.
    """
    pdf = PDF()
    pdf.add_page()

    config = PONTOS_DE_ANALISE.get(id_ponto, {"nome": id_ponto})

    # Título do Relatório de Logs
    pdf.set_font("Arial", "B", 14)
    pdf.cell(0, 10, f"Relatório de Eventos (Logs) - {config['nome']}", 0, 1, "L")
    pdf.ln(5)

    pdf.set_font("Courier", "", 9)

    logs_recentes_primeiro = reversed(logs_do_ponto)

    for log_msg in logs_recentes_primeiro:
        if pdf.get_y() > (pdf.h - 20):
            pdf.add_page()
            pdf.set_font("Courier", "", 9)

        if "ERRO" in log_msg:
            pdf.set_text_color(220, 53, 69)
        elif "AVISO" in log_msg:
            pdf.set_text_color(255, 193, 7)
        elif "MUDANÇA" in log_msg:
            pdf.set_text_color(0, 123, 255)
        else:
            pdf.set_text_color(0, 0, 0)

        pdf.multi_cell(0, 5, log_msg, 0, 'L')
        pdf.ln(2)

    pdf.set_text_color(0, 0, 0)
    # Retorna os bytes (bytearray ou bytes)
    return pdf.output(dest='S')