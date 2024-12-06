from confluent_kafka import Consumer
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

# Configurazione Kafka per il consumer
consumer_config = {
    'bootstrap.servers': 'localhost:29092',  # Indirizzo del broker Kafka
    'group.id': 'group1',  # ID del consumer group
    'auto.offset.reset': 'earliest',  # Inizia a leggere dal primo messaggio
    'enable.auto.commit': True,  # Committa automaticamente gli offset
    'auto.commit.interval.ms': 5000  # Intervallo per il commit degli offset
}

# Configurazione per invio email
SMTP_SERVER = 'smtp.gmail.com'  # Servizio SMTP 
SMTP_PORT = 587  
SENDER_EMAIL = ''  # email
SENDER_PASSWORD = '' # passw
SUBJECT = "Notifica Soglia Ticker"  # Oggetto delle email

# Topic Kafka
topic_notifier = 'to-notifier'  # Topic per le notifiche

# Configurazione del consumer Kafka
consumer = Consumer(consumer_config)
consumer.subscribe([topic_notifier])

def send_email(to_email, ticker, condition):
    """
    Funzione per inviare una email di notifica.
    to_email: L'indirizzo email del destinatario
    ticker: Il ticker per il quale è stata superata la soglia
    condition: La condizione che descrive quale soglia è stata superata
    """
    # Crea il messaggio dell'email
    message = MIMEMultipart()
    message['From'] = SENDER_EMAIL
    message['To'] = to_email
    message['Subject'] = f'Notifica per {ticker}'

    # Corpo del messaggio
    body = f"Il ticker {ticker} ha superato la seguente soglia: {condition}."
    message.attach(MIMEText(body, 'plain'))
    
    try:
        #Connessione al server SMTP con gestione della sicurezza TLS

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()  # Attiva la sicurezza TLS
            server.login(SENDER_EMAIL, SENDER_PASSWORD)  # Login al server SMTP
            
            #conversione in stringa
            email_text = message.as_string()
            
            #invio
            server.sendmail(SENDER_EMAIL, to_email, email_text)  # Invia l'email
            print(f"Email inviata a {to_email} per il ticker {ticker}.")

    except Exception as e:
        print(f"Errore nell'invio dell'email: {e}")
        
while True:
    # Polling per nuovi messaggi dal topic Kafka
    msg = consumer.poll(1.0)  # Tempo di attesa per il messaggio (1 secondo)
    
    if msg is None:
        continue  # Nessun messaggio ricevuto, continua a pollling
    
    if msg.error():
        print(f"Errore consumer: {msg.error()}")
        continue

    # Decodifica il messaggio JSON
    data = json.loads(msg.value().decode('utf-8'))
    email = data['email']
    ticker = data['ticker']
    condition = data['condition']

    # Invia l'email
    send_email(email, ticker, condition) 