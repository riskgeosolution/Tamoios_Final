# gerador_pdf.py (CORRIGIDO V4: Suporte Nativo a Matplotlib)

import io
from fpdf import FPDF, Align
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.figure import Figure  # Importa o tipo Figure
import json


def criar_relatorio_em_memoria(df_dados, fig_chuva_mp, fig_umidade_mp, status_texto, status_cor):
    """
    Cria um relatório PDF em buffer de memória.
    Recebe figuras do Matplotlib (fig_chuva_mp e fig_umidade_mp).
    """

    # --- 1. Inicializa o PDF ---
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Helvetica", size=10)

    # --- 2. Cabeçalho e Metadados ---
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, f"Relatório de Monitoramento - {df_dados.iloc[0]['id_ponto']}", ln=True, align="C")

    # Datas do relatório
    data_inicio_local = df_dados['timestamp_local'].min().strftime('%d/%m/%Y %H:%M')
    data_fim_local = df_dados['timestamp_local'].max().strftime('%d/%m/%Y %H:%M')
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(0, 5, f"Período: {data_inicio_local} a {data_fim_local}", ln=True, align="C")
    pdf.cell(0, 5, f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}", ln=True, align="C")
    pdf.ln(5)

    # --- 3. Status Atual ---
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(pdf.w / 2 - pdf.l_margin, 8, "Status Geral do Período:", border=1, align="L")

    # Mapeamento de cor (usando cores básicas para FPDF)
    cor_borda = 0
    cor_fundo = 200  # Cinza claro padrão
    if status_cor == 'success':
        cor_fundo = (180, 255, 180)  # Verde
    elif status_cor == 'warning':
        cor_fundo = (255, 255, 150)  # Amarelo
    elif status_cor == 'danger':
        cor_fundo = (255, 180, 180)  # Vermelho

    pdf.set_fill_color(*cor_fundo)
    pdf.cell(0, 8, status_texto, border=1, ln=True, align="C", fill=True)
    pdf.ln(5)

    # --- 4. Gráficos (Processamento Matplotlib) ---

    def _add_matplotlib_fig(fig, title):
        pdf.set_font("Helvetica", "B", 10)
        pdf.cell(0, 5, title, ln=True, align="L")
        try:
            # Salva a figura Matplotlib em memória como PNG
            img_bytes = io.BytesIO()
            fig.savefig(img_bytes, format="png", bbox_inches="tight")
            img_bytes.seek(0)
            plt.close(fig)  # Fecha a figura Matplotlib para liberar memória

            # Adiciona a imagem ao PDF
            pdf.image(img_bytes, x=pdf.l_margin, y=None, w=pdf.w - 2 * pdf.l_margin)
            pdf.ln(5)
            return True
        except Exception as e:
            pdf.set_font("Helvetica", "I", 10)
            pdf.cell(0, 5, f"AVISO: Não foi possível gerar o gráfico de {title}. Erro: {e}", ln=True, align="C")
            pdf.ln(5)
            return False

    # Adiciona Gráfico de Chuva
    _add_matplotlib_fig(fig_chuva_mp, "Pluviometria e Acumulado (72h)")

    # Adiciona Gráfico de Umidade
    _add_matplotlib_fig(fig_umidade_mp, "Umidade do Solo")

    # --- 5. Tabela de Dados (Últimas 5 leituras) ---
    pdf.ln(5)
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 10, "Amostra dos Últimos Registros", ln=True, align="L")

    df_ultimos = df_dados[
        ['timestamp_local', 'chuva_mm', 'umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc']].tail(5)

    # Configurações da Tabela
    col_widths = [45, 35, 30, 30, 30]  # Larguras das colunas
    headers = ["Data/Hora (Local)", "Chuva (mm/h)", "Umidade 1m (%)", "Umidade 2m (%)", "Umidade 3m (%)"]

    pdf.set_font("Helvetica", "B", 9)
    # Desenha o cabeçalho
    for w, h in zip(col_widths, headers):
        pdf.cell(w, 7, h, border=1, align="C", fill=True)
    pdf.ln()

    pdf.set_font("Helvetica", "", 8)
    # Desenha as linhas
    for _, row in df_ultimos.iterrows():
        pdf.cell(col_widths[0], 6, row['timestamp_local'].strftime('%d/%m/%Y %H:%M:%S'), border=1, align="C")
        pdf.cell(col_widths[1], 6, f"{row['chuva_mm']:.1f}", border=1, align="C")
        pdf.cell(col_widths[2], 6, f"{row['umidade_1m_perc']:.1f}", border=1, align="C")
        pdf.cell(col_widths[3], 6, f"{row['umidade_2m_perc']:.1f}", border=1, align="C")
        pdf.cell(col_widths[4], 6, f"{row['umidade_3m_perc']:.1f}", border=1, align="C")
        pdf.ln()

    # --- 6. Finalização e Retorno ---

    # Retorna o buffer de bytes do PDF
    buffer_output = pdf.output(dest='S')
    return buffer_output


def criar_relatorio_logs_em_memoria(nome_ponto, logs_filtrados):
    """
    Cria um relatório PDF dos logs de eventos em buffer de memória.
    """
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=10)
    pdf.add_page()
    pdf.set_font("Helvetica", size=10)

    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, f"Histórico de Eventos - {nome_ponto}", ln=True, align="C")
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(0, 5, f"Período: Últimos 7 dias (ou total dos logs)", ln=True, align="C")
    pdf.cell(0, 5, f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}", ln=True, align="C")
    pdf.ln(5)

    pdf.set_font("Courier", size=8)  # Fonte monoespaçada para logs

    # Inverte a ordem para os mais recentes estarem no topo do PDF
    for log_str in reversed(logs_filtrados):
        try:
            parts = log_str.split('|')
            timestamp_str_utc_iso = parts[0].strip()
            ponto_str = parts[1].strip()
            msg_str = "|".join(parts[2:]).strip()

            try:
                dt_utc = pd.to_datetime(timestamp_str_utc_iso).tz_localize('UTC')
                dt_local = dt_utc.tz_convert('America/Sao_Paulo')
                timestamp_formatado = dt_local.strftime('%d/%m/%Y %H:%M:%S')
            except Exception:
                timestamp_formatado = timestamp_str_utc_iso.split('+')[0].replace('T', ' ')

            # Formatação de cor básica no texto
            cor = (0, 0, 0)  # Preto padrão
            if "ERRO" in msg_str:
                cor = (200, 0, 0)  # Vermelho
            elif "AVISO" in msg_str:
                cor = (200, 150, 0)  # Laranja
            elif "MUDANÇA" in msg_str:
                cor = (0, 0, 200)  # Azul

            pdf.set_text_color(*cor)
            linha = f"[{timestamp_formatado}] {ponto_str}: {msg_str}"
            pdf.multi_cell(0, 4, linha)

        except Exception:
            pdf.set_text_color(0, 0, 0)
            pdf.multi_cell(0, 4, log_str)

    pdf.set_text_color(0, 0, 0)  # Volta para o preto

    # Retorna o buffer de bytes do PDF
    buffer_output = pdf.output(dest='S')
    return buffer_output