from confluent_kafka import Consumer, Producer
import json
import time
from datetime import datetime
from database import SessionLocal, User, StockData


#configurazione kafka per produttore e consumatore
consumer_config = {
    'bootstrap.servers': 'kafka:9092',  # Kafka broker address
    'group.id': 'group1',  # Consumer group ID
    'auto.offset.reset': 'earliest',  # Start reading from the earliest message
    'enable.auto.commit': True,  # Automatically commit offsets
    'auto.commit.interval.ms': 5000  # Commit offsets every 5 seconds
}

producer_config = {'bootstrap.servers': 'kafka:9092'}

consumer = Consumer(consumer_config)
producer = Producer(producer_config)

topic1 = 'to-alert-system'
topic2 = 'to-notifier'

consumer.subscribe([topic1])

def delivery_report(err, msg):
    """Callback to report the result of message delivery."""
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
        
while True:
    
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(f"Consumer error: {msg.error()}")
        continue
    
    session = SessionLocal()
    # Query per ottenere tutti gli utenti e i relativi valori dei ticker
    users = session.query(User).all()

    for user in users:
            # Ottieniano l'ultimo valore del ticker dalla tabella Stock_Data per l'utente corrente
            stock_data = session.query(StockData).filter_by(user_id=user.id).order_by(StockData.timestamp.desc()).first()
            
            if stock_data:
                ticker_value = stock_data.value
                print(f"Ticker: {user.ticker}, Value: {ticker_value}")

                # Confronta il valore del ticker con high_value e low_value
                condition = None
                if user.high_value and ticker_value > user.high_value:
                    condition = "il valore del ticker e' superiore alla soglia alta"
                elif user.low_value and ticker_value < user.low_value:
                    condition = "il valore del ticker e' inferiore alla soglia bassa"
                
                if condition:
                    # Prepara il messaggio da inviare al topic to-notifier
                    notifier_message = { "email": user.email, "subject": user.ticker, "condizione": condition}

                    # Invia il messaggio al topic to-notifier
                    producer.produce(topic2, json.dumps(notifier_message), callback=delivery_report)
                    producer.flush() 
                    print(f"Manda la notifica al {topic2}: {notifier_message}")
                    
    session.close()
    
   
