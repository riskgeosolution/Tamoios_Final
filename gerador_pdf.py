# gerador_pdf.py (CORRIGIDO v3: Exporta Plotly para PNG em memória - Mais robusto)

import io
from fpdf import FPDF
import plotly.io as pio
import pandas as pd
from datetime import datetime
import traceback
import tempfile
import os  # Necessário para clean-up

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
    NOTA: Agora usa um arquivo temporário PNG para evitar problemas de stream de bytes.
    """
    pdf = PDF()
    pdf.add_page()
    temp_file_chuva = None
    temp_file_umidade = None

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

    # 2. Adiciona Gráficos (Método mais seguro com arquivos temporários)
    try:
        if fig_chuva and fig_umidade:

            # --- Exportação Segura para Arquivo Temporário ---
            # O Render é mais estável quando exportamos para um arquivo e depois lemos.

            # Exporta Gráfico de Chuva
            with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
                temp_file_chuva = tmp.name
                # Tenta exportar PNG (o modo mais compatível)
                pio.write_image(fig_chuva, temp_file_chuva, format="png", width=800, height=350, engine="auto")

            # Exporta Gráfico de Umidade
            with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
                temp_file_umidade = tmp.name
                pio.write_image(fig_umidade, temp_file_umidade, format="png", width=800, height=350, engine="auto")

            # --- Adicionar ao PDF ---
            pdf.set_font("Arial", "B", 12)

            pdf.cell(0, 10, "Gráfico de Chuva Acumulada (72h)", 0, 1, "L")
            pdf.image(temp_file_chuva, x=pdf.get_x() + 10, y=pdf.get_y(), w=180)
            pdf.ln(95)

            pdf.cell(0, 10, "Gráfico de Variação da Umidade Solo", 0, 1, "L")
            pdf.image(temp_file_umidade, x=pdf.get_x() + 10, y=pdf.get_y(), w=180)
            pdf.ln(95)

        else:
            # Se não houver figuras, apenas mostra o aviso no PDF
            pdf.set_font("Arial", "B", 12)
            pdf.cell(0, 10, f"Aviso: Não há dados para gerar gráficos.", 0, 1, "L")
            pdf.ln(5)

    except Exception as e:
        print(f"ERRO DE EXPORTAÇÃO PLOTLY-PDF: {e}")
        traceback.print_exc()
        pdf.set_font("Arial", "B", 12)
        pdf.cell(0, 10, f"Avviso: Não foi possível gerar gráficos.", 0, 1, "L")
        pdf.ln(5)

    finally:
        # 3. Limpeza dos arquivos temporários (muito importante!)
        if temp_file_chuva and os.path.exists(temp_file_chuva):
            os.remove(temp_file_chuva)
        if temp_file_umidade and os.path.exists(temp_file_umidade):
            os.remove(temp_file_umidade)

    # 4. Adiciona Tabela de Dados (o resto do PDF)
    pdf.add_page()
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 10, "Dados Brutos do Período Selecionado", 0, 1, "L")
    pdf.ln(5)
    # [Lógica da Tabela de Dados mantida]
    df_tabela = df_periodo.copy()
    if 'timestamp' in df_tabela.columns:
        df_tabela['timestamp'] = pd.to_datetime(df_tabela['timestamp']).dt.tz_convert('America/Sao_Paulo').dt.strftime(
            '%d/%m %H:%M')

    cols_display = ['timestamp', 'chuva_mm', 'umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc']
    df_tabela = df_tabela[[col for col in cols_display if col in df_tabela.columns]].fillna('-')

    pdf.set_font("Arial", "", 8)
    col_width = pdf.w / (len(df_tabela.columns) + 1)

    for col in df_tabela.columns: pdf.cell(col_width * 1.2, 5, col, 1)
    pdf.ln()

    for index, row in df_tabela.iterrows():
        if pdf.get_y() > (pdf.h - 30):
            pdf.add_page()
            pdf.set_font("Arial", "", 8)
            for col in df_tabela.columns: pdf.cell(col_width * 1.2, 5, col, 1)
            pdf.ln()
        for col in df_tabela.columns: pdf.cell(col_width * 1.2, 5, str(row[col]), 1)
        pdf.ln()

    return pdf.output(dest='S')


def criar_relatorio_logs_em_memoria(id_ponto, logs_do_ponto):
    """
    Gera um relatório PDF simples contendo uma lista de logs.
    """
    # [Lógica para logs mantida]
    pdf = PDF()
    pdf.add_page()
    config = PONTOS_DE_ANALISE.get(id_ponto, {"nome": id_ponto})
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
            cor = "red"
        elif "AVISO" in log_msg:
            cor = "#E69B00"
        elif "MUDANÇA" in log_msg:
            cor = "blue"
        else:
            cor = "black"
        pdf.set_text_color(int(cor.strip('#'), 16) if '#' in cor else 0, 0, 0)
        pdf.multi_cell(0, 5, log_msg, 0, 'L')
        pdf.ln(2)
    pdf.set_text_color(0, 0, 0)
    return pdf.output(dest='S')