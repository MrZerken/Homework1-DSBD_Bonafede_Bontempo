from confluent_kafka import Consumer, Producer
import json
from datetime import datetime
from database import SessionLocal, User, StockData  # Modifica se necessario per il tuo modello database

# Configurazione Kafka per il consumatore e il produttore
consumer_config = {
    'bootstrap.servers': 'kafka:9092',  # Indirizzo del broker Kafka
    'group.id': 'group_alert',  # ID del gruppo di consumatori
    'auto.offset.reset': 'earliest',  # Inizia a leggere dal messaggio più vecchio
    'enable.auto.commit': True,  # Completamento automatico degli offset periodicamente
    'auto.commit.interval.ms': 5000  # Commit degli offset ogni 5000ms (5 secondi)
}
producer_config = {'bootstrap.servers': 'kafka:9092'}  # Configurazione del produttore

# Creazione delle istanze del consumatore e del produttore Kafka
consumer = Consumer(consumer_config)
producer = Producer(producer_config)

input_topic = 'to-alert-system'  # Topic di origine per i messaggi dal DataCollector
output_topic = 'to-notifier'  # Topic di destinazione per le notifiche

consumer.subscribe([input_topic])  # Sottoscrizione al topic

def produce_sync(producer, topic, value):
    """
    Funzione del produttore sincrono che blocca fino a quando il messaggio non è stato consegnato.
    :param producer: Istanza del produttore Kafka
    :param topic: Topic Kafka a cui inviare il messaggio
    :param value: Valore del messaggio (stringa)
    """
    try:
        # Produci il messaggio in modo sincrono
        producer.produce(topic, value)
        producer.flush()  # Blocca finché tutti i messaggi pendenti non sono stati inviati
        print(f"Messaggio inviato a {topic} con contenuto: {value}")
    except Exception as e:
        print(f"Errore durante l'invio del messaggio a Kafka: {e}")

while True:
    # Poll per nuovi messaggi dal topic di ingresso
    msg = consumer.poll(1.0)
    if msg is None:
        continue  # Nessun messaggio ricevuto, continua a fare il polling
    if msg.error():
        print(f"Errore nel consumatore: {msg.error()}")  # Log degli errori del consumatore
        continue
    
    # Parsing del messaggio ricevuto
    data = json.loads(msg.value().decode('utf-8'))
    ticker = data.get('ticker')
    value = data.get('value')
    
    # Accedi al database per ottenere gli utenti e i loro parametri di soglia
    session = SessionLocal()
    users = session.query(User).all()
    
    # Scansiona ogni utente
    for user in users:
        if user.ticker == ticker:
            high_value = user.high_value
            low_value = user.low_value
            email = user.email
            
            # Verifica la soglia e imposta la condizione superamento soglia
            if (high_value is not None and value >= high_value):
                condition = 'valore del ticker maggiore della soglia alta'
            elif (low_value is not None and value <= low_value):
                condition = 'valore del ticker minore della soglia bassa'            
                # Crea il messaggio di notifica
                notification_message = {
                    'email': email,
                    'ticker': ticker,
                    'condition': condition
                }

                #"email": "gaetano_bont@gmail.it",
                #"ticker": "AAPL",
                #"condition": "superamento soglia alta"
                    
                
                # Invia il messaggio al topic di notifica
                produce_sync(producer, output_topic, json.dumps(notification_message))
                print(f"Notifica inviata a {email} per il ticker {ticker} con condizione {condition}")
    
    session.close()  # Chiudi la sessione del database
