# gerador_pdf.py (CORRIGIDO: Lógica de data final em PDF e Excel)

import io
from fpdf import FPDF, Align
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.figure import Figure
import json
import threading
import time
import traceback

# --- NOVOS IMPORTS (Necessários para a thread) ---
import data_source
import processamento
from config import PONTOS_DE_ANALISE, RISCO_MAP, STATUS_MAP_HIERARQUICO
import matplotlib

matplotlib.use('Agg')  # Garante o backend Agg
# --- FIM DOS NOVOS IMPORTS ---


# --- INÍCIO: NOVO CACHE PARA TAREFAS EM BACKGROUND ---
PDF_CACHE = {}
PDF_CACHE_LOCK = threading.Lock()

EXCEL_CACHE = {}
EXCEL_CACHE_LOCK = threading.Lock()


# --- FIM: NOVO CACHE ---


def criar_relatorio_em_memoria(df_dados, fig_chuva_mp, fig_umidade_mp, status_texto, status_cor, periodo_str=""):
    """
    Cria um relatório PDF em buffer de memória.
    (Esta função não foi alterada)
    """

    # --- 1. Inicializa o PDF ---
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Helvetica", size=10)

    # --- 2. Cabeçalho e Metadados ---
    pdf.set_font("Helvetica", "B", 14)
    id_ponto_rel = df_dados.iloc[0]['id_ponto'] if not df_dados.empty else "Monitoramento"
    pdf.cell(0, 10, f"Relatório de Monitoramento - {id_ponto_rel}", ln=True, align="C")

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
    cor_borda = 0
    cor_fundo = 200
    if status_cor == 'success':
        cor_fundo = (180, 255, 180)
    elif status_cor == 'warning':
        cor_fundo = (255, 255, 150)
    elif status_cor == 'danger':
        cor_fundo = (255, 180, 180)
    pdf.set_fill_color(*cor_fundo)
    pdf.cell(0, 8, status_texto, border=1, ln=True, align="C", fill=True)
    pdf.ln(5)

    # --- 4. Gráficos (Processamento Matplotlib) ---
    def _add_matplotlib_fig(fig, base_title, periodo_str):
        full_title = f"{base_title} {periodo_str}"
        pdf.set_font("Helvetica", "B", 10)
        pdf.cell(0, 5, full_title, ln=True, align="L")
        try:
            img_bytes = io.BytesIO()
            fig.savefig(img_bytes, format="png", bbox_inches="tight")
            img_bytes.seek(0)
            plt.close(fig)
            pdf.image(img_bytes, x=pdf.l_margin, y=None, w=pdf.w - 2 * pdf.l_margin)
            pdf.ln(5)
            return True
        except Exception as e:
            pdf.set_font("Helvetica", "I", 10)
            pdf.cell(0, 5, f"AVISO: Não foi possível gerar o gráfico de {base_title}. Erro: {e}", ln=True, align="C")
            pdf.ln(5)
            return False

    _add_matplotlib_fig(fig_chuva_mp, "Pluviometria", periodo_str)
    _add_matplotlib_fig(fig_umidade_mp, "Umidade do Solo", periodo_str)

    # --- 5. Tabela de Dados ---
    pdf.add_page()
    pdf.ln(5)
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 10, "Últimos 30 Registros do Período", ln=True, align="L")

    df_dados.loc[:, 'timestamp_local_str'] = df_dados['timestamp_local'].apply(
        lambda x: x.strftime('%d/%m/%Y %H:%M:%S') if pd.notna(x) else '-')
    df_ultimos = df_dados[
        ['timestamp_local_str', 'chuva_mm', 'umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc']].tail(30).copy()

    col_widths = [45, 35, 30, 30, 30]
    headers = ["Data/Hora", "Chuva (mm/h)", "Umidade 1m (%)", "Umidade 2m (%)", "Umidade 3m (%)"]

    pdf.set_font("Helvetica", "B", 9)
    for w, h in zip(col_widths, headers):
        pdf.cell(w, 7, h, border=1, align="C", fill=True)
    pdf.ln()

    pdf.set_font("Helvetica", "", 8)

    def format_cell(value):
        if pd.isna(value) or value is None:
            return '-'
        try:
            return f"{value:.1f}"
        except:
            return str(value)

    for _, row in df_ultimos.iterrows():
        pdf.cell(col_widths[0], 6, row['timestamp_local_str'], border=1, align="C")
        pdf.cell(col_widths[1], 6, format_cell(row['chuva_mm']), border=1, align="C")
        pdf.cell(col_widths[2], 6, format_cell(row['umidade_1m_perc']), border=1, align="C")
        pdf.cell(col_widths[3], 6, format_cell(row['umidade_2m_perc']), border=1, align="C")
        pdf.cell(col_widths[4], 6, format_cell(row['umidade_3m_perc']), border=1, align="C")
        pdf.ln()

    # --- 6. Finalização e Retorno ---
    buffer_output = pdf.output(dest='S')
    return buffer_output


def criar_relatorio_logs_em_memoria(nome_ponto, logs_filtrados):
    """
    (Esta função não foi alterada)
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
    pdf.set_font("Courier", size=8)
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
            cor = (0, 0, 0)
            if "ERRO" in msg_str:
                cor = (200, 0, 0)
            elif "AVISO" in msg_str:
                cor = (200, 150, 0)
            elif "MUDANÇA" in msg_str:
                cor = (0, 0, 200)
            pdf.set_text_color(*cor)
            linha = f"[{timestamp_formatado}] {ponto_str}: {msg_str}"
            pdf.multi_cell(0, 4, linha)
        except Exception:
            pdf.set_text_color(0, 0, 0)
            pdf.multi_cell(0, 4, log_str)
    pdf.set_text_color(0, 0, 0)
    buffer_output = pdf.output(dest='S')
    return buffer_output


# --- INÍCIO: NOVAS FUNÇÕES DE BACKGROUND ---

def thread_gerar_pdf(task_id, start_date, end_date, id_ponto, status_json):
    """
    Esta é a função que roda na thread.
    """
    try:
        # 1. Preparar datas
        data_inicio_str = pd.to_datetime(start_date).strftime('%d/%m/%Y')
        data_fim_str = pd.to_datetime(end_date).strftime('%d/%m/%Y')
        periodo_str = f"({data_inicio_str} a {data_fim_str})"

        start_dt_naive = pd.to_datetime(start_date)
        start_dt_local = start_dt_naive.tz_localize('America/Sao_Paulo')
        start_dt = start_dt_local.tz_convert('UTC')  # Converte para UTC para a query

        end_dt_naive = pd.to_datetime(end_date)
        end_dt_local = end_dt_naive.tz_localize('America/Sao_Paulo')  # Fim do dia em SP

        # --- INÍCIO DA CORREÇÃO (PDF Data Final) ---
        # Em vez de (dia + 1) - 1 segundo, apenas pegamos o início do dia seguinte.
        end_dt_local_final = end_dt_local + pd.Timedelta(days=1)
        # --- FIM DA CORREÇÃO ---

        end_dt = end_dt_local_final.tz_convert('UTC')  # Converte para UTC para a query

        # 2. Ler dados DIRETAMENTE DO SQLITE
        df_filtrado = data_source.read_data_from_sqlite(id_ponto, start_dt, end_dt)

        if df_filtrado.empty:
            print(f"[Thread PDF {task_id}] Sem dados no período selecionado (Query: {start_dt} a {end_dt}).")
            raise Exception("Sem dados no período selecionado.")

        # 3. Fuso Horário
        if df_filtrado['timestamp'].dt.tz is None:
            df_filtrado['timestamp'] = pd.to_datetime(df_filtrado['timestamp']).dt.tz_localize('UTC')
        df_filtrado.loc[:, 'timestamp_local'] = df_filtrado['timestamp'].dt.tz_convert('America/Sao_Paulo')

        # 4. Configurações e Status
        config = PONTOS_DE_ANALISE.get(id_ponto, {"nome": "Ponto"})
        status_atual_dict = status_json
        status_geral_ponto_txt = status_atual_dict.get(id_ponto, "INDEFINIDO")
        risco_geral = RISCO_MAP.get(status_geral_ponto_txt, -1)
        status_texto, status_cor = STATUS_MAP_HIERARQUICO.get(risco_geral, ("INDEFINIDO", "secondary"))[:2]

        # 5. Calcular Acumulados
        df_filtrado['chuva_mm'] = pd.to_numeric(df_filtrado['chuva_mm'], errors='coerce').fillna(0)
        df_filtrado['chuva_acum_periodo'] = df_filtrado['chuva_mm'].cumsum()

        df_chuva_72h_pdf = processamento.calcular_acumulado_rolling(df_filtrado, horas=72)
        if 'timestamp' in df_chuva_72h_pdf.columns:
            if df_chuva_72h_pdf['timestamp'].dt.tz is None:
                df_chuva_72h_pdf.loc[:, 'timestamp'] = df_chuva_72h_pdf['timestamp'].dt.tz_localize('UTC')
            df_chuva_72h_pdf.loc[:, 'timestamp_local'] = df_chuva_72h_pdf['timestamp'].dt.tz_convert(
                'America/Sao_Paulo')
        else:
            df_chuva_72h_pdf = df_chuva_72h_pdf.copy()
            df_chuva_72h_pdf.loc[:, 'timestamp_local'] = df_chuva_72h_pdf['timestamp']

        # 6. Gerar Gráfico de Chuva (MATPLOTLIB)
        largura_barra_dias = 1 / 144  # 10 minutos
        fig_chuva_mp, ax1 = plt.subplots(figsize=(10, 5))

        ax1.bar(df_filtrado['timestamp_local'], df_filtrado['chuva_mm'],
                color='#5F6B7C',
                alpha=0.8,
                label='Pluv. Horária (mm)',
                width=largura_barra_dias,
                align='center')

        ax1.set_xlabel("Data e Hora")
        ax1.set_ylabel("Pluviometria Horária (mm)", color='#2C3E50')
        ax1.tick_params(axis='y', labelcolor='#2C3E50')
        ax1.tick_params(axis='x', rotation=45, labelsize=8)
        ax1.grid(True, linestyle='--', alpha=0.6, which='both')
        ax2 = ax1.twinx()

        ax2.plot(df_chuva_72h_pdf['timestamp_local'], df_chuva_72h_pdf['chuva_mm'], color='#007BFF', linewidth=2.5,
                 label='Acumulada (72h)')

        ax2.plot(df_filtrado['timestamp_local'], df_filtrado['chuva_acum_periodo'],
                 color='red',
                 linewidth=2.0,
                 linestyle='--',
                 label='Acumulada (Período)')

        ax2.set_ylabel("Acumulada (72h)", color='#007BFF')
        ax2.tick_params(axis='y', labelcolor='#007BFF')

        fig_chuva_mp.suptitle(f"Pluviometria - Estação {config['nome']}", fontsize=12)

        lines, labels = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()

        fig_chuva_mp.legend(lines + lines2, labels + labels2,
                            loc='upper center',
                            ncol=3,
                            fancybox=True,
                            shadow=True,
                            bbox_to_anchor=(0.5, 0.1))

        fig_chuva_mp.subplots_adjust(bottom=0.25, top=0.9)

        # 7. Gerar Gráfico de Umidade (MATPLOTLIB)
        fig_umidade_mp, ax_umidade = plt.subplots(figsize=(10, 5))
        from pages.specific_dash import CORES_ALERTAS_CSS
        ax_umidade.plot(df_filtrado['timestamp_local'], df_filtrado['umidade_1m_perc'], label='1m',
                        color=CORES_ALERTAS_CSS['verde'], linewidth=2)
        ax_umidade.plot(df_filtrado['timestamp_local'], df_filtrado['umidade_2m_perc'], label='2m',
                        color=CORES_ALERTAS_CSS['laranja'], linewidth=2)
        ax_umidade.plot(df_filtrado['timestamp_local'], df_filtrado['umidade_3m_perc'], label='3m',
                        color=CORES_ALERTAS_CSS['vermelho'], linewidth=2)
        ax_umidade.set_title(f"Variação da Umidade do Solo - Estação {config['nome']}", fontsize=12)

        ax_umidade.set_xlabel("Data e Hora")
        ax_umidade.set_ylabel("Umidade do Solo (%)")

        lines, labels = ax_umidade.get_legend_handles_labels()
        fig_umidade_mp.legend(lines, labels,
                              loc='upper center',
                              bbox_to_anchor=(0.5, 0.1),
                              ncol=3,
                              fancybox=True,
                              shadow=True)

        plt.grid(True, linestyle='--', alpha=0.6)
        ax_umidade.tick_params(axis='x', rotation=45, labelsize=8)
        fig_umidade_mp.subplots_adjust(bottom=0.25, top=0.9)

        # 8. Chamar a função de gerar PDF
        pdf_buffer = criar_relatorio_em_memoria(
            df_filtrado, fig_chuva_mp, fig_umidade_mp, status_texto, status_cor,
            periodo_str
        )

        # 9. Fechar as figuras Matplotlib (libera memória)
        plt.close(fig_chuva_mp)
        plt.close(fig_umidade_mp)

        nome_arquivo = f"Relatorio_{config['nome']}_{datetime.now().strftime('%Y%m%d')}.pdf"
        print(f"[Thread PDF {task_id}] PDF gerado com sucesso.")

        # 10. Salva no CACHE
        with PDF_CACHE_LOCK:
            PDF_CACHE[task_id] = {
                "status": "concluido",
                "data": pdf_buffer,
                "filename": nome_arquivo
            }

    except Exception as e:
        try:
            plt.close(fig_chuva_mp)
            plt.close(fig_umidade_mp)
        except:
            pass

        print(f"ERRO CRÍTICO [Thread PDF {task_id}]:")
        traceback.print_exc()
        with PDF_CACHE_LOCK:
            PDF_CACHE[task_id] = {
                "status": "erro",
                "message": str(e)
            }


def thread_gerar_excel(task_id, start_date, end_date, id_ponto):
    """
    Esta é a função que roda na thread para o Excel.
    """
    try:
        start_dt_naive = pd.to_datetime(start_date)
        start_dt_local = start_dt_naive.tz_localize('America/Sao_Paulo')
        start_dt = start_dt_local.tz_convert('UTC')

        end_dt_naive = pd.to_datetime(end_date)
        end_dt_local = end_dt_naive.tz_localize('America/Sao_Paulo')  # Fim do dia em SP

        # --- INÍCIO DA CORREÇÃO (Excel Data Final) ---
        # Em vez de (dia + 1) - 1 segundo, apenas pegamos o início do dia seguinte.
        end_dt_local_final = end_dt_local + pd.Timedelta(days=1)
        # --- FIM DA CORREÇÃO ---

        end_dt = end_dt_local_final.tz_convert('UTC')  # Converte para UTC para a query

        # 2. Ler dados DIRETAMENTE DO SQLITE
        df_filtrado = data_source.read_data_from_sqlite(id_ponto, start_dt, end_dt)

        if df_filtrado.empty:
            print(f"[Thread Excel {task_id}] Sem dados no período selecionado (Query: {start_dt} a {end_dt}).")
            raise Exception("Sem dados no período selecionado.")

        # 3. Fuso Horário para Excel
        if df_filtrado['timestamp'].dt.tz is None:
            df_filtrado.loc[:, 'timestamp'] = pd.to_datetime(df_filtrado['timestamp']).dt.tz_localize('UTC')
        df_filtrado.loc[:, 'Data/Hora (Local)'] = df_filtrado['timestamp'].dt.tz_convert(
            'America/Sao_Paulo').dt.strftime('%d/%m/%Y %H:%M:%S')
        df_filtrado = df_filtrado.drop(columns=['timestamp'])

        # 4. Renomear e Reordenar
        colunas_renomeadas = {
            'id_ponto': 'ID Ponto',
            'chuva_mm': 'Chuva (mm/h)',
            'precipitacao_acumulada_mm': 'Precipitação Acumulada (mm)',
            'umidade_1m_perc': 'Umidade 1m (%)',
            'umidade_2m_perc': 'Umidade 2m (%)',
            'umidade_3m_perc': 'Umidade 3m (%)',
            'base_1m': 'Base Umidade 1m',
            'base_2m': 'Base Umidade 2m',
            'base_3m': 'Base Umidade 3m',
        }
        df_filtrado = df_filtrado.rename(columns=colunas_renomeadas)
        colunas_ordenadas = ['ID Ponto', 'Data/Hora (Local)'] + [col for col in df_filtrado.columns if
                                                                 col not in ['ID Ponto', 'Data/Hora (Local)']]
        df_filtrado = df_filtrado[colunas_ordenadas]

        # 5. Gerar Excel em memória
        output = io.BytesIO()
        writer = pd.ExcelWriter(output, engine='xlsxwriter')
        df_filtrado.to_excel(writer, sheet_name='Dados Históricos', index=False)
        writer.close()
        output.seek(0)

        excel_data = output.read()

        # 6. Download
        config = PONTOS_DE_ANALISE.get(id_ponto, {"nome": "Ponto"})
        nome_arquivo = f"Dados_Historicos_{config['nome']}_{datetime.now().strftime('%Y%m%d')}.xlsx"
        print(f"[Thread Excel {task_id}] Excel gerado com sucesso.")

        # 7. Salva no CACHE
        with EXCEL_CACHE_LOCK:
            EXCEL_CACHE[task_id] = {
                "status": "concluido",
                "data": excel_data,
                "filename": nome_arquivo
            }

    except Exception as e:
        print(f"ERRO CRÍTICO [Thread Excel {task_id}]:")
        traceback.print_exc()
        with EXCEL_CACHE_LOCK:
            EXCEL_CACHE[task_id] = {
                "status": "erro",
                "message": str(e)
            }
# --- FIM: NOVAS FUNÇÕES DE BACKGROUND ---