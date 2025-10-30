import io
from fpdf import FPDF
import plotly.io as pio
import pandas as pd
from datetime import datetime


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
    Gera um relatório PDF completo e retorna como bytes.
    Recebe um DataFrame JÁ FILTRADO para o ponto e período.
    """

    # 1. Converte gráficos Plotly para imagens em memória
    try:
        img_chuva_bytes = pio.to_image(fig_chuva, format="png", width=800, height=350)
        img_umidade_bytes = pio.to_image(fig_umidade, format="png", width=800, height=350)
    except Exception as e:
        print(f"Erro ao converter gráficos para imagem: {e}")
        pdf = PDF();
        pdf.add_page();
        pdf.set_font("Arial", "B", 16)
        pdf.cell(0, 10, "Erro ao gerar gráficos para o PDF.", 0, 1, "C")
        pdf.cell(0, 10, "Verifique se 'kaleido' está instalado.", 0, 1, "C")
        return pdf.output()

        # 2. Inicia o PDF
    pdf = PDF()
    pdf.add_page()

    # 3. Adiciona Status
    pdf.set_font("Arial", "B", 14)
    if cor_status == "danger":
        pdf.set_text_color(220, 53, 69)  # Vermelho
    elif cor_status == "warning":
        pdf.set_text_color(255, 193, 7)  # Amarelo
    elif cor_status == "success":
        pdf.set_text_color(40, 167, 69)  # Verde
    else:
        pdf.set_text_color(108, 117, 125)  # Cinza
    pdf.cell(0, 10, f"Status do Período: {status_atual}", 0, 1, "L")
    pdf.set_text_color(0, 0, 0)  # Reseta para preto
    pdf.ln(5)

    # 4. Adiciona Gráficos
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 10, "Gráfico de Chuva Acumulada (72h)", 0, 1, "L")
    pdf.image(io.BytesIO(img_chuva_bytes), x=pdf.get_x() + 10, y=pdf.get_y(), w=180)
    pdf.ln(95)  # Pula o espaço da imagem

    # --- INÍCIO DA ALTERAÇÃO ---
    pdf.cell(0, 10, "Gráfico de Variação da Umidade Solo", 0, 1, "L")  # Alterado aqui
    # --- FIM DA ALTERAÇÃO ---

    pdf.image(io.BytesIO(img_umidade_bytes), x=pdf.get_x() + 10, y=pdf.get_y(), w=180)
    pdf.ln(95)

    # 5. Adiciona Tabela de Dados (AGORA COM O PERÍODO COMPLETO)
    pdf.add_page()
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 10, "Dados Brutos do Período Selecionado", 0, 1, "L")  # Título ajustado
    pdf.ln(5)

    pdf.set_font("Arial", "", 8)

    # --- INÍCIO DA CORREÇÃO ---
    # Removemos o .tail(24) daqui
    df_tabela = df_periodo.copy()
    # --- FIM DA CORREÇÃO ---

    # Formata colunas (como antes)
    if 'timestamp' in df_tabela.columns:
        df_tabela['timestamp'] = df_tabela['timestamp'].dt.strftime('%d/%m %H:00')
    if 'chuva_mm' in df_tabela.columns:
        df_tabela['chuva_mm'] = df_tabela['chuva_mm'].round(1).astype(str)
    if 'umidade_1m_perc' in df_tabela.columns:
        df_tabela['umidade_1m_perc'] = df_tabela['umidade_1m_perc'].round(1).astype(str) + '%'
    if 'umidade_2m_perc' in df_tabela.columns:
        df_tabela['umidade_2m_perc'] = df_tabela['umidade_2m_perc'].round(1).astype(str) + '%'
    if 'umidade_3m_perc' in df_tabela.columns:
        df_tabela['umidade_3m_perc'] = df_tabela['umidade_3m_perc'].round(1).astype(str) + '%'

    # Cabeçalho da Tabela (Mantido como "Umid." para preservar o layout)
    col_width = pdf.w / 5.5
    pdf.cell(col_width * 1.5, 5, "Timestamp", 1)
    pdf.cell(col_width, 5, "Chuva (mm)", 1)
    pdf.cell(col_width, 5, "Umid. 1m", 1)
    pdf.cell(col_width, 5, "Umid. 2m", 1)
    pdf.cell(col_width, 5, "Umid. 3m", 1)
    pdf.ln()

    # Dados da Tabela (Itera sobre todo o df_tabela)
    for index, row in df_tabela.iterrows():
        # Verifica se precisa adicionar uma nova página
        if pdf.get_y() > (pdf.h - 30):  # Deixa 30mm de margem inferior
            pdf.add_page()
            # Redesenha o cabeçalho na nova página
            pdf.set_font("Arial", "", 8)  # Reseta a fonte
            pdf.cell(col_width * 1.5, 5, "Timestamp", 1)
            pdf.cell(col_width, 5, "Chuva (mm)", 1)
            pdf.cell(col_width, 5, "Umid. 1m", 1)
            pdf.cell(col_width, 5, "Umid. 2m", 1)
            pdf.cell(col_width, 5, "Umid. 3m", 1)
            pdf.ln()

        pdf.cell(col_width * 1.5, 5, str(row.get('timestamp', '')), 1)
        pdf.cell(col_width, 5, str(row.get('chuva_mm', '')), 1)
        pdf.cell(col_width, 5, str(row.get('umidade_1m_perc', '')), 1)
        pdf.cell(col_width, 5, str(row.get('umidade_2m_perc', '')), 1)
        pdf.cell(col_width, 5, str(row.get('umidade_3m_perc', '')), 1)
        pdf.ln()

    # 6. Gera o PDF em memória e retorna os bytes
    return pdf.output()


# --- INÍCIO DA NOVA FUNÇÃO ---
def criar_relatorio_logs_em_memoria(logs_do_ponto, nome_ponto):
    """
    Gera um relatório PDF simples contendo uma lista de logs.
    """
    pdf = PDF()
    pdf.add_page()

    # Título do Relatório de Logs
    pdf.set_font("Arial", "B", 14)
    pdf.cell(0, 10, f"Relatório de Eventos (Logs) - {nome_ponto}", 0, 1, "L")
    pdf.ln(5)

    pdf.set_font("Courier", "", 9)  # Monospace é bom para logs

    # Inverte para mostrar o mais recente primeiro, igual ao modal
    logs_recentes_primeiro = reversed(logs_do_ponto)

    for log_msg in logs_recentes_primeiro:
        # Checa se precisa de nova página
        if pdf.get_y() > (pdf.h - 20):  # Margem de 20mm
            pdf.add_page()
            pdf.set_font("Courier", "", 9)  # Reseta fonte na nova página

        # Tenta definir a cor baseada no log (como no modal)
        if "ERRO" in log_msg:
            pdf.set_text_color(220, 53, 69)  # Vermelho (danger)
        elif "ALERTA EXTERNO" in log_msg:
            pdf.set_text_color(255, 193, 7)  # Amarelo (warning)
        elif "MUDANÇA DE STATUS" in log_msg:
            pdf.set_text_color(0, 123, 255)  # Azul (primary)
        elif "ALERTA BASE" in log_msg:
            pdf.set_text_color(23, 162, 184)  # Ciano (info)
        else:
            pdf.set_text_color(0, 0, 0)  # Preto (padrão)

        # multi_cell quebra a linha automaticamente
        pdf.multi_cell(0, 5, log_msg, 0, 'L')
        pdf.ln(2)  # Espaço pequeno entre logs

    pdf.set_text_color(0, 0, 0)  # Reseta para preto no final
    return pdf.output()
# --- FIM DA NOVA FUNÇÃO ---
