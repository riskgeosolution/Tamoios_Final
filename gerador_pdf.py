# gerador_pdf.py (v12.10: CORREÇÃO FINAL E DEFINITIVA DO GRÁFICO)

import io
from fpdf import FPDF
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import threading
import traceback

import data_source
from config import PONTOS_DE_ANALISE, RISCO_MAP, STATUS_MAP_HIERARQUICO, CORES_ALERTAS_CSS
import matplotlib

matplotlib.use('Agg')

PDF_CACHE = {}
PDF_CACHE_LOCK = threading.Lock()
EXCEL_CACHE = {}
EXCEL_CACHE_LOCK = threading.Lock()

# ==============================================================================
# --- FUNÇÕES DE GERAÇÃO DE RELATÓRIO ---
# ==============================================================================

def criar_relatorio_excel_em_memoria(df_consolidado, nome_ponto):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_relatorio = df_consolidado.rename(columns={
            'timestamp_local': 'Data e Hora (Local)', 'chuva_mm': 'Chuva (mm)',
            'umidade_1m_perc': 'Umidade 1m (%)', 'umidade_2m_perc': 'Umidade 2m (%)',
            'umidade_3m_perc': 'Umidade 3m (%)'
        })
        colunas_export = ['Data e Hora (Local)', 'Chuva (mm)', 'Umidade 1m (%)', 'Umidade 2m (%)', 'Umidade 3m (%)']
        df_relatorio = df_relatorio[colunas_export]
        df_relatorio['Data e Hora (Local)'] = df_relatorio['Data e Hora (Local)'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df_relatorio.to_excel(writer, index=False, sheet_name=f'Dados_{nome_ponto}')
        worksheet = writer.sheets[f'Dados_{nome_ponto}']
        for i, col in enumerate(df_relatorio.columns):
            column_len = max(df_relatorio[col].astype(str).map(len).max(), len(col)) + 2
            worksheet.set_column(i, i, column_len)
    return output.getvalue()

def criar_relatorio_pdf_em_memoria(df_consolidado, periodo_str, nome_ponto):
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, f"Relatório de Monitoramento - Estação {nome_ponto}", ln=True, align="C")
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(0, 5, f"Período: {periodo_str}", ln=True, align="C")
    pdf.cell(0, 5, f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}", ln=True, align="C")
    pdf.ln(5)

    fig_chuva, fig_umidade = _criar_graficos_pdf(df_consolidado, nome_ponto)
    _add_matplotlib_fig(pdf, fig_chuva, "Pluviometria", periodo_str)
    _add_matplotlib_fig(pdf, fig_umidade, "Umidade do Solo", periodo_str)

    pdf.add_page()
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 10, "Registros Consolidados (a cada 15 minutos)", ln=True, align="L")
    df_tabela = df_consolidado.tail(40).copy()
    df_tabela['timestamp_local'] = df_tabela['timestamp_local'].dt.strftime('%d/%m/%y %H:%M')
    col_widths = [35, 35, 35, 35, 35]
    headers = ["Data/Hora", "Chuva (mm)", "Umidade 1m", "Umidade 2m", "Umidade 3m"]
    pdf.set_font("Helvetica", "B", 9)
    for w, h in zip(col_widths, headers):
        pdf.cell(w, 7, h, border=1, align="C")
    pdf.ln()
    pdf.set_font("Helvetica", "", 8)
    for _, row in df_tabela.iterrows():
        pdf.cell(col_widths[0], 6, str(row['timestamp_local']), border=1, align="C")
        pdf.cell(col_widths[1], 6, f"{row['chuva_mm']:.2f}" if pd.notna(row['chuva_mm']) else '0.00', border=1, align="C")
        pdf.cell(col_widths[2], 6, f"{row['umidade_1m_perc']:.1f}%" if pd.notna(row['umidade_1m_perc']) else '-', border=1, align="C")
        pdf.cell(col_widths[3], 6, f"{row['umidade_2m_perc']:.1f}%" if pd.notna(row['umidade_2m_perc']) else '-', border=1, align="C")
        pdf.cell(col_widths[4], 6, f"{row['umidade_3m_perc']:.1f}%" if pd.notna(row['umidade_3m_perc']) else '-', border=1, align="C")
        pdf.ln()
    return pdf.output(dest='S')

def _add_matplotlib_fig(pdf, fig, base_title, periodo_str):
    full_title = f"{base_title} {periodo_str}"
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(0, 5, full_title, ln=True, align="L")
    try:
        with io.BytesIO() as img_bytes:
            fig.savefig(img_bytes, format="jpeg", dpi=100, bbox_inches="tight")
            img_bytes.seek(0)
            pdf.image(img_bytes, x=pdf.l_margin, y=None, w=pdf.w - 2 * pdf.l_margin, type='jpeg')
        plt.close(fig)
        pdf.ln(5)
    except Exception as e:
        pdf.set_font("Helvetica", "I", 10)
        pdf.cell(0, 5, f"AVISO: Não foi possível gerar o gráfico de {base_title}. Erro: {e}", ln=True, align="C")
        pdf.ln(5)

def _criar_graficos_pdf(df_consolidado, nome_ponto):
    # --- GRÁFICO DE CHUVA (LÓGICA CORRIGIDA E LOCAL) ---
    fig_chuva, ax1 = plt.subplots(figsize=(10, 5))
    ax1.bar(df_consolidado['timestamp_local'], df_consolidado['chuva_mm'], color='#5F6B7C', alpha=0.8, label='Pluv. (mm/15min)', width=1/96)
    ax1.set_xlabel("Data e Hora")
    ax1.set_ylabel("Pluviometria (mm/15min)", color='#2C3E50')
    ax1.tick_params(axis='x', rotation=45, labelsize=8)
    ax1.grid(True, linestyle='--', alpha=0.6)
    
    ax2 = ax1.twinx()
    # CORREÇÃO: Cálculo do acumulado feito localmente, de forma segura.
    # A janela é 72h * 4 (intervalos de 15min por hora) = 288
    df_consolidado['chuva_acum_72h'] = df_consolidado['chuva_mm'].rolling(window=288, min_periods=1).sum()
    ax2.plot(df_consolidado['timestamp_local'], df_consolidado['chuva_acum_72h'], color='#007BFF', linewidth=2.5, label='Acumulada (72h)')
    ax2.set_ylabel("Acumulada (72h)", color='#007BFF')

    fig_chuva.suptitle(f"Pluviometria - Estação {nome_ponto}", fontsize=12)
    lines, labels = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax2.legend(lines + lines2, labels + labels2, loc='upper center', ncol=2, fancybox=True, shadow=True, bbox_to_anchor=(0.5, -0.2))
    fig_chuva.tight_layout(rect=[0, 0.05, 1, 0.95])

    # --- GRÁFICO DE UMIDADE ---
    fig_umidade, ax_umidade = plt.subplots(figsize=(10, 5))
    ax_umidade.plot(df_consolidado['timestamp_local'], df_consolidado['umidade_1m_perc'], label='1m', color=CORES_ALERTAS_CSS['verde'], linewidth=2)
    ax_umidade.plot(df_consolidado['timestamp_local'], df_consolidado['umidade_2m_perc'], label='2m', color=CORES_ALERTAS_CSS['laranja'], linewidth=2)
    ax_umidade.plot(df_consolidado['timestamp_local'], df_consolidado['umidade_3m_perc'], label='3m', color=CORES_ALERTAS_CSS['vermelho'], linewidth=2)
    ax_umidade.set_title(f"Variação da Umidade do Solo - Estação {nome_ponto}", fontsize=12)
    ax_umidade.set_xlabel("Data e Hora")
    ax_umidade.set_ylabel("Umidade do Solo (%)")
    ax_umidade.grid(True, linestyle='--', alpha=0.6)
    ax_umidade.tick_params(axis='x', rotation=45, labelsize=8)
    fig_umidade.legend(loc='upper center', ncol=3, fancybox=True, shadow=True, bbox_to_anchor=(0.5, -0.2))
    fig_umidade.tight_layout(rect=[0, 0.05, 1, 0.95])
    
    return fig_chuva, fig_umidade

def _get_and_consolidate_data(start_date, end_date, id_ponto):
    start_dt = pd.to_datetime(start_date).tz_localize('America/Sao_Paulo').tz_convert('UTC')
    end_dt = (pd.to_datetime(end_date) + pd.Timedelta(days=1)).tz_localize('America/Sao_Paulo').tz_convert('UTC')
    df_brutos = data_source.read_data_from_sqlite(id_ponto, start_dt, end_dt)
    if df_brutos.empty: return pd.DataFrame()
    df_brutos['timestamp_local'] = df_brutos['timestamp'].dt.tz_convert('America/Sao_Paulo')
    df_consolidado = df_brutos.set_index('timestamp_local').resample('15T').agg({
        'chuva_mm': 'sum', 'umidade_1m_perc': 'mean', 
        'umidade_2m_perc': 'mean', 'umidade_3m_perc': 'mean'
    }).reset_index()
    df_consolidado['chuva_mm'] = df_consolidado['chuva_mm'].fillna(0)
    return df_consolidado

def thread_gerar_excel(task_id, start_date, end_date, id_ponto):
    try:
        df_consolidado = _get_and_consolidate_data(start_date, end_date, id_ponto)
        if df_consolidado.empty: raise Exception("Sem dados no período selecionado.")
        nome_ponto = PONTOS_DE_ANALISE.get(id_ponto, {}).get("nome", "Desconhecido")
        excel_buffer = criar_relatorio_excel_em_memoria(df_consolidado, nome_ponto)
        nome_arquivo = f"Dados_{nome_ponto}_{datetime.now().strftime('%Y%m%d')}.xlsx"
        with EXCEL_CACHE_LOCK:
            EXCEL_CACHE[task_id] = {"status": "concluido", "data": excel_buffer, "filename": nome_arquivo}
    except Exception as e:
        traceback.print_exc()
        with EXCEL_CACHE_LOCK:
            EXCEL_CACHE[task_id] = {"status": "erro", "message": str(e)}

def thread_gerar_pdf(task_id, start_date, end_date, id_ponto, status_json):
    try:
        df_consolidado = _get_and_consolidate_data(start_date, end_date, id_ponto)
        if df_consolidado.empty: raise Exception("Sem dados no período selecionado.")
        periodo_str = f"{pd.to_datetime(start_date).strftime('%d/%m/%Y')} a {pd.to_datetime(end_date).strftime('%d/%m/%Y')}"
        nome_ponto = PONTOS_DE_ANALISE.get(id_ponto, {}).get("nome", "Desconhecido")
        status_texto = status_json.get(id_ponto, "INDEFINIDO")
        risco = RISCO_MAP.get(status_texto, -1)
        _, status_cor, _ = STATUS_MAP_HIERARQUICO.get(risco, ("INDEFINIDO", "secondary", "bg-secondary"))
        pdf_buffer = criar_relatorio_pdf_em_memoria(df_consolidado, periodo_str, nome_ponto)
        nome_arquivo = f"Relatorio_{nome_ponto}_{datetime.now().strftime('%Y%m%d')}.pdf"
        with PDF_CACHE_LOCK:
            PDF_CACHE[task_id] = {"status": "concluido", "data": pdf_buffer, "filename": nome_arquivo}
    except Exception as e:
        traceback.print_exc()
        with PDF_CACHE_LOCK:
            PDF_CACHE[task_id] = {"status": "erro", "message": str(e)}

def criar_relatorio_logs_em_memoria(nome_ponto, logs_filtrados):
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=10)
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, f"Histórico de Eventos - {nome_ponto}", ln=True, align="C")
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(0, 5, f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}", ln=True, align="C")
    pdf.ln(5)
    pdf.set_font("Courier", size=8)
    for log_str in reversed(logs_filtrados):
        log_str_sanitizado = log_str.encode('latin-1', 'replace').decode('latin-1')
        try:
            parts = log_str_sanitizado.split('|')
            timestamp_str_utc_iso = parts[0].strip()
            ponto_str = parts[1].strip()
            msg_str = "|".join(parts[2:]).strip()
            dt_local = pd.to_datetime(timestamp_str_utc_iso).tz_convert('America/Sao_Paulo')
            timestamp_formatado = dt_local.strftime('%d/%m/%Y %H:%M:%S')
            cor = (0, 0, 0)
            if "ERRO" in msg_str: cor = (200, 0, 0)
            elif "AVISO" in msg_str: cor = (200, 150, 0)
            elif "MUDANÇA" in msg_str: cor = (0, 0, 200)
            pdf.set_text_color(*cor)
            pdf.write(5, f"[{timestamp_formatado}] {ponto_str}: {msg_str}\n")
        except Exception:
            pdf.set_text_color(0, 0, 0)
            pdf.write(5, log_str_sanitizado + "\n")
    return pdf.output(dest='S')