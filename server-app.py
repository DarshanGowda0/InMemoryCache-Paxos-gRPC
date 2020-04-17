import logging
import sys
import time
from concurrent import futures

from lib.paxos_pb2 import *
from lib.paxos_pb2_grpc import *
from paxos import PaxosImpl, Status


class ServerApp(KeyValueStoreServicer):
    def __init__(self, px: PaxosImpl, me):
        self.paxos = px
        self.store = {}
        self.requestNumber = {}
        self.curSeq = 0
        self.me = me

    def __sync(self, maxSeq: int):
        logger.info('sync called for seq {} with curSeq {}'.format(maxSeq, self.curSeq))
        while self.curSeq <= maxSeq:
            statusReply = self.paxos.getStatus(Number(number=self.curSeq))
            print('status' + str(statusReply.status))
            if statusReply.status == Status.EMPTY.value:
                self.__startInstance(self.curSeq, Data())
                statusReply = self.paxos.getStatus(Number(number=self.curSeq))
            to = 0.01
            while True:
                if statusReply.status == Status.DECIDED.value:
                    op = statusReply.value
                    if op.type != 'GET':
                        if op.uid not in self.requestNumber:
                            if op.type == "PUT":
                                self.store[op.key] = op.value
                            elif op.type == 'DELETE':
                                del (self.store[op.key])
                            self.requestNumber[op.uid] = 1
                    break
                time.sleep(to)
                if to < 10:
                    to *= 2
                statusReply = self.paxos.getStatus(Number(number=self.curSeq))
            self.curSeq += 1

        self.paxos.doFinish(FinishArgs(pid=self.curSeq - 1))

    def __startInstance(self, pid: int, op: Data) -> Data:
        logger.info(
            'start instance called with value type {} uid {} key {} value {}'.format(op.type, op.uid, op.key, op.value))
        args = StartArgs(pid=pid, value=op)
        self.paxos.doStart(args)
        to = 0.01
        ans = None
        while True:
            reply: StatusReply = self.paxos.getStatus(Number(number=pid))

            if reply.status == Status.DECIDED.value:
                ans = reply.value
                break
            time.sleep(to)
            if to < 10:
                to *= 2

        return ans

    def __reachAgreement(self, value: Data):
        logger.info('reach agreement called for {} with key {}'.format(value.uid, value.key))
        while True:
            maxVal: Number = self.paxos.getMax()
            pid = maxVal.number + 1
            self.__sync(pid - 1)
            v: Data = self.__startInstance(pid, value)
            if v == value:
                self.__sync(pid)
                break

    def get(self, request: Request, context) -> Response:
        logger.info('GET request called for {} with key {}'.format(request.uid, request.key))
        if request.uid not in self.requestNumber:
            value = Data(type='GET', key=request.key, value=request.value, uid=request.uid)
            self.__reachAgreement(value)

        if request.key in self.store:
            return Response(statusCode=200, message=self.store[request.key])
        else:
            return Response(statusCode=500, message='Something went wrong!')

    def put(self, request: Request, context) -> Response:
        logger.info('PUT request called for {} with key {}'.format(request.uid, request.key))
        if request.uid not in self.requestNumber:
            value = Data(type='PUT', key=request.key, value=request.value, uid=request.uid)
            self.__reachAgreement(value)

        return Response(statusCode=200, message='Successfully stored')

    def delete(self, request: Request, context) -> Response:
        logger.info('DELETE request called for {} with key {}'.format(request.uid, request.key))
        if request.uid not in self.requestNumber:
            value = Data(type='DELETE', key=request.key, value=request.value, uid=request.uid)
            self.__reachAgreement(value)

        return Response(statusCode=200, message='Successfully deleted')


def init_logger(portNumber):
    logger.setLevel(logging.DEBUG)

    fh = logging.FileHandler('server-{}.log'.format(portNumber))
    fh.setLevel(logging.DEBUG)

    consoleLogger = logging.StreamHandler()
    consoleLogger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(relativeCreated)6d %(threadName)s - %(message)s')
    fh.setFormatter(formatter)
    consoleLogger.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(consoleLogger)


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage - python3 {} <port-number> 'server-port1,server-port2...' ".format(sys.argv[0]))
        exit(1)

    serverPorts = sys.argv[2].strip().split(',')
    myPortNumber = sys.argv[1]

    logger = logging.getLogger('server-logger')
    init_logger(myPortNumber)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    paxos = PaxosImpl(me=myPortNumber, peers=serverPorts)
    serverApp = ServerApp(paxos, myPortNumber)

    add_KeyValueStoreServicer_to_server(serverApp, server)
    add_PaxosServicer_to_server(serverApp.paxos, server)

    server.add_insecure_port('[::]:' + str(myPortNumber))
    print('starting server at ' + myPortNumber)
    server.start()
    server.wait_for_termination()
