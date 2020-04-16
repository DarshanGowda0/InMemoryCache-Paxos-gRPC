import random
import threading
import time
from enum import Enum

from lib.paxos_pb2 import *
from lib.paxos_pb2_grpc import *


class Status(Enum):
    DECIDED, PENDING, FORGOTTEN, EMPTY = range(4)


RejectSignal = -1
PromisedSignal = 1
InitialValue = -1


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
        self.majoritySize = len(peers) / 2 + 1
        self.me = me
        self.finishedProposals = [-1 for _ in peers]
        self.maxProposalSeen = -1
        self.values = []
        self.acceptedState = State()
        self.stubs = {}

    def __doPrepare(self, prepareArgs: PrepareArgs):
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

    def prepare(self, prepareArgs: PrepareArgs, context) -> PrepareReply:
        return self.__doPrepare(prepareArgs)

    def __doAccept(self, acceptArgs: AcceptArgs):
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

    def accept(self, acceptArgs: AcceptArgs, context) -> AcceptReply:
        return self.__doAccept(acceptArgs)

    def __doDecide(self, decideArgs: DecideArgs):
        if decideArgs.pid in self.instances:
            self.instances[decideArgs.pid].acceptedN = decideArgs.proposal
            self.instances[decideArgs.pid].acceptedValue = decideArgs.value
            self.instances[decideArgs.pid].status = Status.DECIDED
        else:
            self.instances[decideArgs.pid] = State(acceptedN=decideArgs.proposal, acceptedValue=decideArgs.value,
                                                   status=Status.DECIDED)

        self.maxSeq = max(self.maxSeq, decideArgs.pid)

        return DecideReply(done=self.doneSeqs[self.me])

    def decided(self, decideArgs: DecideArgs, context) -> DecideReply:
        return self.__doDecide(decideArgs)

    @staticmethod
    def __getNextProposalNumber(me):
        return float("{}.{}".format(int(time.time()), me))

    def __getStubFor(self, portNumber):
        if portNumber in self.stubs:
            return self.stubs[portNumber]

        channel = grpc.insecure_channel('{}:{}'.format('localhost', portNumber))
        stub = PaxosStub(channel)
        self.stubs[portNumber] = stub
        return stub

    def __selectMajorityServers(self):
        servers = []
        added = {}
        size = len(self.peers)

        i = 0
        while i < self.majoritySize:
            r = random.randint(size)
            if r not in added:
                added[r] = True
                servers += self.peers[r],
                i += 1

        return servers

    def __sendPrepare(self, acceptors, pid, proposalNumber, value):
        servers = 0
        prepareArgs = PrepareArgs(pid=pid, proposal=proposalNumber)
        maxValue = value
        maxN = -1
        for acc in acceptors:
            reply: PrepareReply = None
            if acc == self.me:
                reply = self.__doPrepare(prepareArgs)
            else:
                paxosStub = self.__getStubFor(acc)
                reply = paxosStub.prepare(prepareArgs)

            if reply and reply.promised == PromisedSignal:
                if reply.acceptedN > maxN:
                    maxN = reply.acceptedN
                    maxValue = reply.acceptedValue
                servers += 1

        return servers, maxValue

    def __sendAccept(self, acceptors, pid, proposalNumber, maxValue):
        servers = 0
        acceptArgs = AcceptArgs(pid=pid, proposal=proposalNumber, value=maxValue)
        for acc in acceptors:
            if acc == self.me:
                reply = self.__doAccept(acceptArgs)
            else:
                paxosStub = self.__getStubFor(acc)
                reply = paxosStub.accept(acceptArgs)

            if reply and reply.accepted:
                servers += 1

        return servers

    def __sendDecided(self, pid, proposalNumber, maxValue):
        decideArgs = DecideArgs(pid=pid, proposal=proposalNumber, value=maxValue)
        allDecided = False
        minDone = float('inf')
        dones = [0] * len(self.peers)
        while not allDecided:
            allDecided = True
            for i, server in enumerate(self.peers):
                if server == self.me:
                    reply = self.__doDecide(decideArgs)
                else:
                    paxosStub = self.__getStubFor(server)
                    reply = paxosStub.decided(decideArgs)

                if not reply:
                    allDecided = False
                else:
                    minDone = min(minDone, reply.done)
                    dones[i] = reply.done

            if not allDecided:
                time.sleep(0.03)

        if minDone != InitialValue:
            self.doneSeqs = dones
            for key, _ in self.instances.items():
                if key <= minDone:
                    del (self.instances[key])

            self.minSeq = minDone + 1

    def start(self, startArgs: StartArgs, context) -> None:
        def doStart(args: StartArgs):
            if args.pid < self.minSeq:
                return
            while True:
                proposalNumber = self.__getNextProposalNumber(self.me)
                acceptors = self.__selectMajorityServers()
                preparedServers, maxValue = self.__sendPrepare(acceptors=acceptors, pid=args.pid,
                                                               proposalNumber=proposalNumber, value=args.value)
                if preparedServers == self.majoritySize:
                    acceptedServers = self.__sendAccept(acceptors=acceptors, pid=args.pid,
                                                        proposalNumber=proposalNumber, maxValue=maxValue)
                    if acceptedServers == self.majoritySize:
                        self.__sendDecided(pid=args.pid, proposalNumber=proposalNumber, maxValue=maxValue)
                        break
                    else:
                        time.sleep(0.03)
                else:
                    time.sleep(0.03)

        thread = threading.Thread(target=doStart, args=(startArgs,))
        thread.start()

    def finish(self, finishArgs: FinishArgs, context) -> None:
        self.doneSeqs[self.me] = finishArgs.pid

    def max(self, request, context) -> Number:
        return Number(number=self.maxSeq)

    def __doMin(self):
        minDone = float('inf')
        for value in self.doneSeqs:
            minDone = min(minDone, value)

        if minDone >= self.minSeq:
            for key, _ in self.instances.items():
                if key <= minDone:
                    del (self.instances[key])
            self.minSeq = minDone + 1

        return self.minSeq

    def min(self, request, context) -> Number:
        return self.__doMin()

    def status(self, request: Number, context) -> StatusReply:
        minSeq = self.__doMin()
        if request.number < minSeq:
            return StatusReply(status=Status.FORGOTTEN, value=None)

        if request.number in self.instances:
            state = self.instances[request.number]
            return StatusReply(state.status, state.acceptedValue)
        else:
            return StatusReply(Status.EMPTY, None)
