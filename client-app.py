import sys
import logging
import time

from lib.paxos_pb2 import *
from lib.paxos_pb2_grpc import *


class Client:

    def __init__(self, st: KeyValueStoreStub):
        self.stub = st

    @staticmethod
    def __getUid():
        return str(time.time())

    def put_request(self, key: str, value: str):
        request = Request()
        request.key, request.value, request.uid = key, value, self.__getUid()
        response = self.stub.put(request, timeout=5)
        logger.info('status code - {} message - {}'.format(response.statusCode, response.message))

    def get_request(self, key: str):
        request = Request()
        request.key = key
        request.uid = self.__getUid()
        response = self.stub.get(request, timeout=5)
        logger.info('status code - {} message - {}'.format(response.statusCode, response.message))

    def delete_request(self, key: str):
        request = Request()
        request.key = key
        request.uid = self.__getUid()
        response = self.stub.delete(request, timeout=5)
        logger.info('status code - {} message - {}'.format(response.statusCode, response.message))

    def pre_populate(self):
        for i in range(5):
            self.put_request('key-' + str(i), 'value-' + str(i))
        for i in range(5):
            self.get_request('key-' + str(i))
        for i in range(5):
            self.delete_request('key-' + str(i))


def init_logger(portNumber):
    logger.setLevel(logging.DEBUG)

    fh = logging.FileHandler('client-{}.log'.format(portNumber))
    fh.setLevel(logging.DEBUG)

    consoleLogger = logging.StreamHandler()
    consoleLogger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s')
    fh.setFormatter(formatter)
    consoleLogger.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(consoleLogger)


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Usage - python3 {} <host-name> <port-number>'.format(sys.argv[0]))
        exit(1)

    hostName, portNumber = sys.argv[1], sys.argv[2]

    logger = logging.getLogger('client-logger')
    init_logger(portNumber)

    channel = grpc.insecure_channel('{}:{}'.format(hostName, portNumber))
    stub = KeyValueStoreStub(channel)

    client = Client(stub)
    # client.pre_populate()

    while True:
        inputString = input("Enter the operation to be performed:\nPUT <key> <value>\nGET <key>\nDELETE <key>\n")
        commands = inputString.split(' ')
        logger.info("command => " + inputString)
        if commands[0].lower() == 'put':
            if len(commands) == 3:
                client.put_request(commands[1], commands[2])
            else:
                logger.exception('Invalid put request received! - ' + inputString)
        elif commands[0].lower() == 'get':
            if len(commands) == 2:
                client.get_request(commands[1])
            else:
                logger.exception('Invalid get request received! - ' + inputString)
        elif commands[0].lower() == 'delete':
            if len(commands) == 2:
                client.delete_request(commands[1])
            else:
                logger.exception('Invalid delete request received! - ' + inputString)
        else:
            logger.exception('Invalid request received!')
