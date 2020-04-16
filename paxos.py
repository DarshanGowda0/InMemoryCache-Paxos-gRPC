from enum import Enum

from lib.paxos_pb2 import *
from lib.paxos_pb2_grpc import *


class Status(Enum):
    DECIDED, PENDING, FORGOTTEN, EMPTY = range(4)


RejectSignal = -1
PromisedSignal = 1


class State:
    def __init__(self, promisedN=-1, acceptedN=-1, acceptedValue=None, status=0):
        self.promisedN = promisedN
        self.acceptedN = acceptedN
        self.acceptedValue = acceptedValue
        self.status = status


class PaxosImpl(PaxosServicer):
    def __init__(self, peers, me):
        self.maxSeq = 0
        self.minSeq = 0
        self.doneSeqs = []
        self.instances = {}

        self.peers = peers
        self.me = me
        self.finishedProposals = [-1 for _ in peers]
        self.maxProposalSeen = -1
        self.values = []
        self.acceptedState = State()

    def prepare(self, prepareArgs: PrepareArgs, context) -> PrepareReply:
        reply = PrepareReply()
        reply.promise = RejectSignal
        isPromise = False

        if prepareArgs.pid in self.instances:
            if self.instances[prepareArgs.pid].promisedN < prepareArgs.proposal:
                isPromise = True

        else:
            self.instances[prepareArgs.pid] = State()
            isPromise = True

        if isPromise:
            self.instances[prepareArgs.pid].promisedN = prepareArgs.proposal
            reply.acceptedProposal = self.instances[prepareArgs.pid].acceptedN,
            reply.acceptedValue = self.instances[prepareArgs.pid].acceptedValue

        return reply

    def accept(self, acceptArgs: AcceptArgs, context) -> AcceptReply:

        reply = AcceptReply(accepted=False)

        if acceptArgs.pid not in self.instances:
            self.instances[acceptArgs.pid] = State()

        if acceptArgs.proposal >= self.instances[acceptArgs.pid].promisedN:
            self.instances[acceptArgs.pid].promisedN = acceptArgs.proposal
            self.instances[acceptArgs.pid].acceptedN = acceptArgs.proposal
            self.instances[acceptArgs.pid].acceptedValue = acceptArgs.value
            self.instances[acceptArgs.pid].status = Status.PENDING
            reply.accepted = True

        return reply

    def decided(self, decideArgs: DecideArgs, context) -> DecideReply:
        if decideArgs.pid in self.instances:
            self.instances[decideArgs.pid].acceptedN = decideArgs.proposal
            self.instances[decideArgs.pid].acceptedValue = decideArgs.value
            self.instances[decideArgs.pid].status = Status.DECIDED
        else:
            self.instances[decideArgs.pid] = State(acceptedN=decideArgs.proposal, acceptedValue=decideArgs.value,
                                                   status=Status.DECIDED)

        self.maxSeq = max(self.maxSeq, decideArgs.pid)

        return DecideReply(done=self.doneSeqs[self.me])

    def __getNextProposalNumber(self, me):
        pass

    def __selectMajorityServers(self):
        pass

    def __sendPrepare(self, acceptors, pid, proposalNumber, value):
        pass

    def __sendAccept(self, acceptors, pid, proposalNumber, maxValue):
        pass

    def __sendDecided(self, pid, proposalNumber, maxValue):
        pass

    def start(self, startArgs: StartArgs, context) -> None:
        pass

    def finish(self, finishArgs: FinishArgs, context) -> None:
        pass

    def max(self, request, context) -> Number:
        pass

    def min(self, request, context) -> Number:
        pass

    def status(self, request: Number, context) -> StatusReply:
        pass
