from confluent_kafka import Consumer
import json
import smtplib
import pytz
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime

#configurazione kafka per consumatore
consumer_config = {
    'bootstrap.servers': 'kafka:9092',  # Kafka broker address
    'group.id': 'group2',  # Consumer group ID
    'auto.offset.reset': 'earliest',  # Start reading from the earliest message
    'enable.auto.commit': True,  # Automatically commit offsets periodically
    'auto.commit.interval.ms': 5000  # Commit offsets every 5000ms (5 seconds)
}

consumer = Consumer(consumer_config)  
topic = 'to-notifier' 

consumer.subscribe([topic])

# Configurazione Server SMTP
SMTP_SERVER = 'smtp.gmail.com'      
SMTP_PORT = 587                    
EMAIL_ADDRESS = 'teamunict24@gmail.com'  
EMAIL_PASSWORD = 'nqct wwth oftr vgkk'  

def send_email(email, ticker, condition):
    """Invio dell'email di Notifica"""
    try:
        # Creazione dell'email di notifica
        msg = MIMEMultipart()
        msg['From'] = EMAIL_ADDRESS
        msg['To'] = email
        msg['Subject'] = f"{ticker}"

        # Corpo dell'email
        body = f"""\
Gentile utente,

Il ticker {ticker} ha superato la soglia impostata.

Dettagli:
Condizione superamento soglia: {condition}
            
Per ulteriori dettagli, può consultare il seguente link:
(https://finance.yahoo.com/quote/{ticker})

Cordiali saluti, 
 
Bonafede e Bontempo
"""
        msg.attach(MIMEText(body, 'plain'))

        # Connessione al server SMTP
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()  # Abilita TLS
        server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)

        # Invio dell'email
        server.send_message(msg)
        print(f"Email inviata a {email} per il ticker {ticker} con condizione '{condition}'")

        # Chiusura della connessione al server
        server.quit()

    except Exception as e:
        print(f"Errore nell'invio dell'email a {email}: {e}")


while True:
    # Consuma il messaggio dal topic to-alert-system
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(f"Consumer error: {msg.error()}")
        continue
    
    data = json.loads(msg.value().decode('utf-8'))
    email = data['email']
    ticker = data['subject']
    condition = data['condizione']
    
    send_email(email, ticker, condition)
    

    
   
        