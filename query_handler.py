from dsbd_pb2 import UserResponse, TickerResponse
from dsbd_pb2_grpc import DSBDServiceServicer, add_DSBDServiceServicer_to_server
from database import SessionLocal, User, StockData

class QueryHandler:
    def __init__(self):
        self.session = SessionLocal()

    def GetTickerValue(self, request):
        self.session.expire_all()
        user = self.session.query(User).filter_by(email=request.email).first()
        if not user:
            return TickerResponse(success=False, message="Utente non trovato.")

        stock_data = self.session.query(StockData).filter_by(user_id=user.id).order_by(StockData.timestamp.desc()).first()
        if not stock_data:
            return TickerResponse(success=False, message="Nessun dato trovato.")
        return TickerResponse(success=True, value=stock_data.value)

    def GetTickerAverage(self, request):
        self.session.expire_all()
        user = self.session.query(User).filter_by(email=request.email).first()
        if not user:
            return TickerResponse(success=False, message="Utente non trovato.")

        stock_data = self.session.query(StockData).filter_by(user_id=user.id).order_by(StockData.timestamp.desc()).limit(request.lastXValues).all()
        if not stock_data:
            return TickerResponse(success=False, message="Nessun dato trovato.")

        avg_value = sum(data.value for data in stock_data) / len(stock_data)
        return TickerResponse(success=True, value=avg_value)
