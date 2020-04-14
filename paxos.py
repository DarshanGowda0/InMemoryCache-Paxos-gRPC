from lib.paxos_pb2 import *
from lib.paxos_pb2_grpc import *


class PaxosImpl(PaxosServicer):
    def __init__(self):
        pass

    def start(self, startArgs: StartArgs, context) -> None:
        pass

    def prepare(self, prepareArgs: PrepareArgs, context) -> PrepareReply:
        pass

    def propose(self, data: Data, context) -> None:
        pass

    def accept(self, acceptArgs: AcceptArgs, context) -> AcceptReply:
        pass

    def decided(self, decideArgs: DecideArgs, context) -> DecideReply:
        pass

    def finish(self, finishArgs: FinishArgs, context) -> None:
        pass
