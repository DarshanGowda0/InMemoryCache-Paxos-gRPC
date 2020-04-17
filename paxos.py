import logging
import random
import sys
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

logger = logging.getLogger('server-logger')


class State:
    def __init__(self, promisedN=-1, acceptedN=-1, acceptedValue=None, status=Status.EMPTY.value):
        self.promisedN = promisedN
        self.acceptedN = acceptedN
        self.acceptedValue = acceptedValue
        self.status = status


class PaxosImpl(PaxosServicer):
    def __init__(self, peers, me):
        self.maxSeq = InitialValue
        self.minSeq = 0
        self.doneSeqs = {i: -1 for i in peers}
        self.instances = {}

        self.peers = peers
        self.majoritySize = 1
        self.me = me
        self.finishedProposals = [-1 for _ in peers]
        self.maxProposalSeen = -1
        self.values = []
        self.acceptedState = State()
        self.stubs = {}

    def doPrepare(self, prepareArgs: PrepareArgs):
        logger.info('do prepare called for {}'.format(prepareArgs.pid))
        reply = PrepareReply()
        reply.promised = RejectSignal
        isPromise = False

        if prepareArgs.pid in self.instances:
            if self.instances[prepareArgs.pid].promisedN < prepareArgs.proposal:
                isPromise = True
        else:
            self.instances[prepareArgs.pid] = State()
            isPromise = True

        if isPromise:
            self.instances[prepareArgs.pid].promisedN = prepareArgs.proposal
            reply.promised = PromisedSignal
            reply.acceptedProposal = self.instances[prepareArgs.pid].acceptedN
            if self.instances[prepareArgs.pid].acceptedValue:
                reply.acceptedValue = self.instances[prepareArgs.pid].acceptedValue

        logger.info('returning reply pid {} promisedN {} acceptedN {}'.format(prepareArgs.pid, reply.promised,
                                                                              reply.acceptedProposal))
        return reply

    def prepare(self, prepareArgs: PrepareArgs, context) -> PrepareReply:
        logger.info('prepare called with pid {} proposal {}'.format(prepareArgs.pid, prepareArgs.proposal))
        return self.doPrepare(prepareArgs)

    def doAccept(self, acceptArgs: AcceptArgs):
        logger.info('do accept called with pid {} proposal {}'.format(acceptArgs.pid, acceptArgs.proposal))
        reply = AcceptReply(accepted=False)

        if acceptArgs.pid not in self.instances:
            self.instances[acceptArgs.pid] = State()

        if acceptArgs.proposal >= self.instances[acceptArgs.pid].promisedN:
            self.instances[acceptArgs.pid].promisedN = acceptArgs.proposal
            self.instances[acceptArgs.pid].acceptedN = acceptArgs.proposal
            self.instances[acceptArgs.pid].acceptedValue = acceptArgs.value
            self.instances[acceptArgs.pid].status = Status.PENDING.value
            reply.accepted = True

        return reply

    def accept(self, acceptArgs: AcceptArgs, context) -> AcceptReply:
        logger.info('accept called with pid {} proposal {}'.format(acceptArgs.pid, acceptArgs.proposal))
        return self.doAccept(acceptArgs)

    def doDecide(self, decideArgs: DecideArgs):
        logger.info('do decide called with pid {} proposal {}'.format(decideArgs.pid, decideArgs.proposal))
        if decideArgs.pid in self.instances:
            logger.info('do decide updating status to decide for {}'.format(decideArgs.pid))
            self.instances[decideArgs.pid].acceptedN = decideArgs.proposal
            self.instances[decideArgs.pid].acceptedValue = decideArgs.value
            self.instances[decideArgs.pid].status = Status.DECIDED.value
        else:
            logger.info('do decide creating status to decide for {}'.format(decideArgs.pid))
            self.instances[decideArgs.pid] = State(promisedN=decideArgs.proposal, acceptedN=decideArgs.proposal,
                                                   acceptedValue=decideArgs.value, status=Status.DECIDED.value)
            logger.info('do decide creating status done to decide for {}'.format(decideArgs.pid))

        logger.info('exec maxSeq {} pid {}'.format(self.maxSeq, decideArgs.pid))
        self.maxSeq = max(self.maxSeq, decideArgs.pid)
        logger.info('returning from do decide {}!'.format(self.doneSeqs[self.me]))
        return DecideReply(done=self.doneSeqs[self.me])

    def decided(self, decideArgs: DecideArgs, context) -> DecideReply:
        logger.info('decided called with pid {} proposal {}'.format(decideArgs.pid, decideArgs.proposal))
        return self.doDecide(decideArgs)

    @staticmethod
    def __getNextProposalNumber():
        return float("{}".format(time.time()))

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
            r = random.randint(0, size - 1)
            if r not in added:
                added[r] = True
                servers += self.peers[r],
                i += 1

        return servers

    def __sendPrepare(self, acceptors, pid, proposalNumber, value):
        logger.info('send prepare called for seq {} num {} val {}'.format(pid, proposalNumber, value))
        servers = 0
        prepareArgs = PrepareArgs(pid=pid, proposal=proposalNumber)
        maxValue = value
        maxN = -1
        for acc in acceptors:
            logger.info('acc {}'.format(acc))
            reply: PrepareReply = None
            if acc == self.me:
                reply = self.doPrepare(prepareArgs)
            else:
                paxosStub = self.__getStubFor(acc)
                reply = paxosStub.prepare(prepareArgs)

            if reply and reply.promised == PromisedSignal:
                if reply.acceptedProposal > maxN:
                    maxN = reply.acceptedProposal
                    maxValue = reply.acceptedValue
                servers += 1

        return servers, maxValue

    def __sendAccept(self, acceptors, pid, proposalNumber, maxValue):
        logger.info('send accept called for seq {} num {} val {}'.format(pid, proposalNumber, maxValue))
        servers = 0
        acceptArgs = AcceptArgs(pid=pid, proposal=proposalNumber, value=maxValue)
        for acc in acceptors:
            if acc == self.me:
                reply = self.doAccept(acceptArgs)
            else:
                paxosStub = self.__getStubFor(acc)
                reply = paxosStub.accept(acceptArgs)

            if reply and reply.accepted:
                servers += 1

        return servers

    def __sendDecided(self, pid, proposalNumber, maxValue):
        logger.info('send decided called for seq {} num {} val {}'.format(pid, proposalNumber, maxValue))
        decideArgs = DecideArgs(pid=pid, proposal=proposalNumber, value=maxValue)
        allDecided = False
        minDone = sys.maxsize
        # dones = {}
        while not allDecided:
            allDecided = True
            for i, server in enumerate(self.peers):
                logger.info('for server {}'.format(server))
                if server == self.me:
                    reply = self.doDecide(decideArgs)
                else:
                    paxosStub = self.__getStubFor(server)
                    reply = paxosStub.decided(decideArgs)
                logger.info('received {}, decided {}, min done {}'.format(reply.done, allDecided, minDone))

                if not reply:
                    allDecided = False
                else:
                    minDone = min(minDone, reply.done)
                    self.doneSeqs[server] = reply.done
                logger.info('received 2 {}, decided {}, min done {}'.format(reply.done, allDecided, minDone))

            if not allDecided:
                time.sleep(0.03)

        logger.info('all decided')

        if minDone != InitialValue:
            # self.doneSeqs = dones
            for key, _ in self.instances.items():
                if key <= minDone:
                    del (self.instances[key])

            self.minSeq = minDone + 1

    def doStart(self, startArgs: StartArgs):
        logger.info('start in paxos called for {} with val {} {} {} {}'.format(startArgs.pid, startArgs.value.type,
                                                                               startArgs.value.key,
                                                                               startArgs.value.value,
                                                                               startArgs.value.uid))

        def inner(args: StartArgs):
            if args.pid < self.minSeq:
                return
            logger.info('pid {} minSeq {}'.format(args.pid, self.minSeq))
            while True:
                proposalNumber = self.__getNextProposalNumber()
                logger.info('trying proposal number {}'.format(proposalNumber))
                acceptors = self.__selectMajorityServers()
                logger.info('acceptors selected {}'.format(acceptors))
                preparedServers, maxValue = self.__sendPrepare(acceptors=acceptors, pid=args.pid,
                                                               proposalNumber=proposalNumber, value=args.value)
                logger.info('send prepare result {} {}'.format(preparedServers, maxValue))
                logger.info('got maxValue {} from {} servers'.format(maxValue, preparedServers))
                if preparedServers == self.majoritySize:
                    acceptedServers = self.__sendAccept(acceptors=acceptors, pid=args.pid,
                                                        proposalNumber=proposalNumber, maxValue=maxValue)
                    logger.info('got accept from {} servers'.format(acceptedServers))
                    if acceptedServers == self.majoritySize:
                        self.__sendDecided(pid=args.pid, proposalNumber=proposalNumber, maxValue=maxValue)
                        logger.info('send decided done, breaking out!')
                        break
                    else:
                        time.sleep(0.03)
                else:
                    time.sleep(0.03)

        thread = threading.Thread(target=inner, args=(startArgs,))
        thread.start()

    def start(self, startArgs: StartArgs, context) -> None:
        self.doStart(startArgs)

    def doFinish(self, finishArgs: FinishArgs):
        self.doneSeqs[self.me] = finishArgs.pid

    def finish(self, finishArgs: FinishArgs, context) -> None:
        return self.doFinish(finishArgs)

    def getMax(self):
        return Number(number=self.maxSeq)

    def max(self, request, context) -> Number:
        return self.getMax()

    def doMin(self):
        minDone = sys.maxsize
        for _, value in self.doneSeqs.items():
            minDone = min(minDone, value)
        print('minDone' + str(minDone))
        if minDone != sys.maxsize and minDone >= self.minSeq:
            for key, _ in self.instances.items():
                if key <= minDone:
                    del (self.instances[key])
            self.minSeq = minDone + 1

        return Number(number=self.minSeq)

    def min(self, request, context) -> Number:
        return self.doMin()

    def getStatus(self, request: Number):
        logger.info('get status called for {}'.format(request.number))
        minSeq = self.doMin().number
        print('min seq {}'.format(minSeq))
        if request.number < minSeq:
            return StatusReply(status=Status.FORGOTTEN.value, value=None)

        if request.number in self.instances:
            state = self.instances[request.number]
            return StatusReply(status=state.status, value=state.acceptedValue)
        else:
            return StatusReply(status=Status.EMPTY.value, value=None)

    def status(self, request: Number, context) -> StatusReply:
        return self.getStatus(request)
