# alertas.py (v12.2 - NINJA: Alertas em Threads)

import httpx
import os
import requests
import json
import traceback
from threading import Thread

# --- Constantes da API SMTP2GO ---
SMTP2GO_API_URL = "https://api.smtp2go.com/v3/email/send"

# --- Variáveis de Ambiente ---
SMTP2GO_API_KEY = os.environ.get('SMTP2GO_API_KEY')
SMTP2GO_SENDER_EMAIL = os.environ.get('SMTP2GO_SENDER_EMAIL')
DESTINATARIOS_EMAIL_STR = os.environ.get('DESTINATARIOS_EMAIL')
COMTELE_API_KEY = os.environ.get('COMTELE_API_KEY')
SMS_DESTINATARIOS_STR = os.environ.get('SMS_DESTINATARIOS')


def _enviar_email_smtp2go(api_key, sender_email, recipients_list, subject, html_body):
    """ Envia um e-mail usando a API HTTP da SMTP2GO. (Função interna) """
    cor_css = "#dc3545" if 'PARALIZAÇÃO' in subject.upper() else "#28a745"
    payload = {
        "api_key": api_key, "sender": sender_email, "to": recipients_list,
        "subject": subject,
        "html_body": f"<html><body><h1 style='color: {cor_css};'>{subject}</h1><p>{html_body}</p></body></html>",
        "text_body": f"{subject}: {html_body}"
    }
    try:
        with httpx.Client(timeout=20.0) as client:
            response = client.post(SMTP2GO_API_URL, json=payload)
        if response.status_code == 200 and response.json().get('data', {}).get('failures', 1) == 0:
            print(f"E-mail de alerta (SMTP2GO) enviado com sucesso para: {recipients_list}")
            return True
        else:
            print(f"ERRO API SMTP2GO (E-mail): {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"ERRO DE CONEXÃO/REQUISIÇÃO (E-mail): {e}")
        return False

def _enviar_sms_comtele(api_key, recipients_list, message):
    """ Envia SMS usando a API da Comtele. (Função interna) """
    COMTELE_API_URL = "https://sms.comtele.com.br/api/v2/send"
    numeros_com_virgula = ",".join(recipients_list)
    payload = {"Content": message, "Receivers": numeros_com_virgula}
    headers = {"auth-key": api_key, "Content-Type": "application/json"}
    try:
        response = requests.post(COMTELE_API_URL, headers=headers, json=payload, timeout=20.0)
        if response.status_code == 200 and response.json().get('Success', False):
            print(f"SMS de alerta (Comtele) enviado com sucesso para: {numeros_com_virgula}")
            return True
        else:
            print(f"FALHA NO ENVIO SMS (Comtele): Status {response.status_code}, Resposta: {response.text}")
            return False
    except Exception as e:
        print(f"ERRO DE CONEXÃO/REQUISIÇÃO (Comtele SMS): {e}")
        return False

def _thread_enviar_alerta(id_ponto, nome_ponto, novo_status, status_anterior):
    """
    Função que executa em uma thread separada para não bloquear o worker.
    """
    print(f"THREAD DE ALERTA: Iniciando para {nome_ponto} (Mudança: {status_anterior} -> {novo_status})")
    
    if novo_status == "PARALIZAÇÃO":
        assunto_email = f"ALERTA DE PARALIZAÇÃO: {nome_ponto}"
        html_body_part = f"O ponto de monitoramento {nome_ponto} entrou em estado de PARALIZAÇÃO."
        sms_mensagem = f"ALERTA: {nome_ponto} em PARALIZACAO. Acao necessaria."
    elif status_anterior == "PARALIZAÇÃO" and novo_status != "PARALIZAÇÃO":
        assunto_email = f"AVISO DE NORMALIZAÇÃO: {nome_ponto}"
        html_body_part = f"O ponto de monitoramento {nome_ponto} saiu do estado de PARALIZAÇÃO e retornou para {novo_status}."
        sms_mensagem = f"AVISO: {nome_ponto} saiu de PARALIZACAO. Status atual: {novo_status}."
    else:
        # Ignora outras transições para evitar spam de alertas
        return

    # Envio de E-mail
    if SMTP2GO_API_KEY and SMTP2GO_SENDER_EMAIL and DESTINATARIOS_EMAIL_STR:
        destinatarios_email = [email.strip() for email in DESTINATARIOS_EMAIL_STR.split(',')]
        if destinatarios_email:
            _enviar_email_smtp2go(SMTP2GO_API_KEY, SMTP2GO_SENDER_EMAIL, destinatarios_email, assunto_email, html_body_part)
    
    # Envio de SMS
    if COMTELE_API_KEY and SMS_DESTINATARIOS_STR:
        destinatarios_sms = [num.strip() for num in SMS_DESTINATARIOS_STR.split(',')]
        if destinatarios_sms:
            _enviar_sms_comtele(COMTELE_API_KEY, destinatarios_sms, sms_mensagem)

def enviar_alerta(id_ponto, nome_ponto, novo_status, status_anterior):
    """
    Função principal chamada pelo worker.
    Cria e dispara uma thread para enviar os alertas, retornando imediatamente.
    """
    thread = Thread(
        target=_thread_enviar_alerta,
        args=(id_ponto, nome_ponto, novo_status, status_anterior),
        daemon=True
    )
    thread.start()
    print(f"Disparando thread de alerta para {nome_ponto} em segundo plano.")