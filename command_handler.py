import hashlib
from dsbd_pb2 import UserResponse, TickerResponse
from dsbd_pb2_grpc import DSBDServiceServicer, add_DSBDServiceServicer_to_server
from database import SessionLocal, User, StockData, RegistrationMessage, UpdateMessage
from threading import Lock

cache_lock = Lock()

class CommandHandler:
    def __init__(self):
        self.session = SessionLocal()
        self.registration_cache = {}
        self.update_cache = {}
        self._initialize_caches()

    def _initialize_caches(self):
        """Riempie le cache con i message ID delle registrazioni e degli aggiornamenti."""
        registration_ids = self.session.query(RegistrationMessage.message_id).all()
        if registration_ids:
            self.registration_cache.update({msg_id[0]: True for msg_id in registration_ids})

        update_ids = self.session.query(UpdateMessage.message_id).all()
        if update_ids:
            self.update_cache.update({msg_id[0]: True for msg_id in update_ids})

    def LoginUser(self, request):
        email = request.email
        if self.session.query(User).filter_by(email=email).first():
            return UserResponse(success=True, message="Login riuscito.")
        return UserResponse(success=False, message="Login fallito, assicurati di essere registrato prima di effettuare il login.")

    def RegisterUser(self, request):
        email = request.email
        message_id = request.message_id
        ticker = request.ticker

        with cache_lock:
            if message_id in self.registration_cache:
                return UserResponse(success=False, message="Utente già registrato, l'ID del messaggio è in cache.")

        email_hash = hashlib.sha256(email.encode()).hexdigest()

        if message_id != email_hash:
            return UserResponse(success=False, message="ID del messaggio non valido.")

        new_user = User(email=email, ticker=ticker)
        self.session.add(new_user)

        reg_message = RegistrationMessage(message_id=message_id)
        self.session.add(reg_message)

        self.session.commit()
        with cache_lock:
            self.registration_cache[message_id] = True
        return UserResponse(success=True, message="Utente registrato con successo.")

    def UpdateUser(self, request):
        email = request.email
        ticker = request.ticker
        message_id = request.message_id

        with cache_lock:
            if message_id in self.update_cache:
                return UserResponse(success=False, message="Update dell'utente effettuato di recente, ID messaggio in cache.")

        user = self.session.query(User).filter_by(email=email).first()
        if not user:
            return UserResponse(success=False, message="Utente non trovato.")

        email_ticker_hash = hashlib.sha256((email + ticker).encode()).hexdigest()
        if message_id != email_ticker_hash:
            return UserResponse(success=False, message="ID messaggio non valido.")

        old_message_id = hashlib.sha256((email + user.ticker).encode()).hexdigest()
        with cache_lock:
            if old_message_id in self.update_cache:
                del self.update_cache[old_message_id]
        self.session.query(UpdateMessage).filter_by(message_id=old_message_id).delete()

        self.session.query(StockData).filter_by(user_id=user.id).delete()

        user.ticker = ticker
        self.session.add(UpdateMessage(message_id=message_id))
        self.session.commit()
        with cache_lock:
            self.update_cache[message_id] = True
        return UserResponse(success=True, message="Update effettuato con successo.")
    
    def UpdateUserThresholds(self, request):
        email = request.email
        user = self.session.query(User).filter_by(email=email).first()
        if not user:
            return UserResponse(success=False, message="Utente non trovato.")
        
        
        # Recupera i valori esistenti dal database
        current_high_value = user.high_value
        current_low_value = user.low_value

        if request.high_value != -1:
            new_high_value = request.high_value
        elif request.high_value == -1: 
            new_high_value = current_high_value
        
        if request.low_value != -1:
            new_low_value = request.low_value
        elif request.low_value == -1: 
            new_low_value = current_low_value

        if new_low_value != None and new_high_value != None and new_low_value > new_high_value:
            return UserResponse(success=False, message="low value non può essere maggiore di high value")

        # Aggiorna le soglie se fornite
        if request.high_value != -1:
            user.high_value = request.high_value
        if request.low_value != -1:
            user.low_value = request.low_value

        self.session.commit()
        return UserResponse(success=True, message="Soglie aggiornate con successo.")
    
    def ResetUserThresholds(self, request):
        email = request.email
        user = self.session.query(User).filter_by(email=email).first()
        if not user:
            return UserResponse(success=False, message="Utente non trovato.")
        
        if request.high_value == 1:
            user.high_value = None
        if request.low_value == 1:
            user.low_value = None

        self.session.commit()
        return UserResponse(success=True, message="Soglie aggiornate con successo.")
        

    def DeleteUser(self, request):
        email = request.email
        user = self.session.query(User).filter_by(email=email).first()
        if not user:
            return UserResponse(success=False, message="Utente non trovato.")

        # Calcola gli ID delle cache
        reg_message_id = hashlib.sha256(email.encode()).hexdigest()
        update_message_id = hashlib.sha256((email + user.ticker).encode()).hexdigest()

        # Rimuovi gli ID delle registrazioni e aggiornamenti
        self.session.query(RegistrationMessage).filter_by(message_id=reg_message_id).delete()
        self.session.query(UpdateMessage).filter_by(message_id=update_message_id).delete()

        self.session.query(StockData).filter_by(user_id=user.id).delete()
        self.session.delete(user)
        self.session.commit()

        # Rimuovi dalle cache
        with cache_lock:
            if reg_message_id in self.registration_cache:
                del self.registration_cache[reg_message_id]
            if update_message_id in self.update_cache:
                del self.update_cache[update_message_id]

        return UserResponse(success=True, message="Utente eliminato con successo.")
