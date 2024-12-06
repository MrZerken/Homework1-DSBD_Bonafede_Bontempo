import grpc
from concurrent import futures
import time
from dsbd_pb2_grpc import DSBDServiceServicer, add_DSBDServiceServicer_to_server
from command_handler import CommandHandler
from query_handler import QueryHandler

class DSBDServer(DSBDServiceServicer):
    def __init__(self):
        self.command_handler = CommandHandler()
        self.query_handler = QueryHandler()

    def LoginUser(self, request, context):
        return self.command_handler.LoginUser(request)

    def RegisterUser(self, request, context):
        return self.command_handler.RegisterUser(request)

    def UpdateUser(self, request, context):
        return self.command_handler.UpdateUser(request)
    
    def UpdateUserThresholds(self, request, context):
        return self.command_handler.UpdateUserThresholds(request)
    def ResetUserThresholds(self, request, context):
        return self.command_handler.ResetUserThresholds(request)

    def DeleteUser(self, request, context):
        return self.command_handler.DeleteUser(request)

    def GetTickerValue(self, request, context):
        return self.query_handler.GetTickerValue(request)

    def GetTickerAverage(self, request, context):
        return self.query_handler.GetTickerAverage(request)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    add_DSBDServiceServicer_to_server(DSBDServer(), server)
    server.add_insecure_port('[::]:18072')
    server.start()
    print("Il server è in esecuzione sulla porta 18072...")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()
