import sys
from concurrent import futures

from lib.paxos_pb2 import *
from lib.paxos_pb2_grpc import *
from paxos import PaxosImpl


class Op:
    def __init__(self, type='', key='', value='', uid=''):
        self.type = type
        self.key = key
        self.value = value
        self.uid = uid


class ServerApp(KeyValueStoreServicer):
    def __init__(self, paxos, me):
        self.paxos = paxos
        self.store = {}
        self.requestNumber = {}
        self.curSeq = 0
        self.me = me

    def __sync(self, maxSeq: int):
        pass

    def __startInstance(self, seq: int, op: Op) -> Data:
        pass

    def __reachAgreement(self, value: Op):
        pass

    def get(self, request: Request, context) -> Response:
        pass

    def put(self, request: Request, context) -> Response:
        pass

    def delete(self, request: Request, context) -> Response:
        pass


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage - python3 {} <port-number> 'server-port1,server-port2...' ".format(sys.argv[0]))
        exit(1)

    serverPorts = sys.argv[2].strip().split(',')
    myPortNumber = sys.argv[1]

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    paxos = PaxosImpl(me=myPortNumber, peers=serverPorts)
    serverApp = ServerApp(paxos, myPortNumber)

    add_KeyValueStoreServicer_to_server(serverApp, server)
    add_PaxosServicer_to_server(serverApp.paxos, server)

    server.add_insecure_port('[::]:' + str(myPortNumber))
    server.start()
    server.wait_for_termination()
