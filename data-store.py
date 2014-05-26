import zmq

#The state for the currect process' sockets
proc = process()
#The state for the current process' RAFT_instance
raft = RAFT_instance()
#Dictionary which maps keys to values once they have been comitted
data_store = {}

#Class with all the state necessary for an instance of RAFT
class RAFT_instance:
    def __init__(self):
        self.currentTerm = 0 #The latest term the server has seen (monotonicly increasing)
        self.votedFor = None #The id of the last candidate vvoted for
        self.log = [] #A list of log entries 4-tuple (Log#,command,key,value)

        self.commitIndex #Index into the log of the highest log entry that has been commited
        self.lastApplied #the index of the highest log entry applied

        #State used when the process is a leader
        self.nextIndex = [] #index of the next log entry to send to each server
        self.matchIndex = [] #index of the highest log enty which has been replicated on each server

#Class which contains the necessary state for the process to connect over a socket
class process:
    def __init__(self):
        return
    #Connect to the socket sock which is used to send messages
    #and update the process's context/socket
    def connectSend(self,sock):
        if self.context == None:
            self.context = zmq.Context()
        self.socketSend = self.context.socket(zmq.REQ)
        self.socketSend.connect(sock)
    def connectRecv(self,sock):
        if self.context == None:
            self.context = zmq.Context()
        self.socketRecv = self.context.socket(zmq.SUB)
        self.socketRecv.connect(sock)
    #Read the message from the (bound) socket and return the result
    def read_message(self):
        return self.socketRecv.recv()

#The class which contains the information relevent to append a new entry to the log
class append_message:
    def __init__(self,term, leader, prevLogIndex,prevLogTerm,log_entries,leaderCommit,sender,recpt):
        self.term = term #The term of the current leader
        self.leader = leader #The id of the current leader
        self.prevLogIndex = prevLogIndex #index of the log entry precieding "log_entries"
        self.prevLogTerm = prevLogTerm #term corresponding to preLogIndex
        self.entries = log_entries #entries to be appended to the log. See above for format
        self.leaderCommit = leaderCommit #The commit index of the leader
        self.sender = sender #Should be the leader.
        self.recpt = recpt #Should be the same id as the process

#Sent by a candidate to request a vote
class requestVote_message:
    def __init__(self,term,candidate,lastLogIndex,lastLogTerm,sender,recpt):
        self.sender = sender
        self.recpt = recpt
        self.term = term #The term of the candidate
        self.candidate = candidate #The id of the candidate requesting a vote
        self.lastLogIndex = lastLogIndex #The index of the candidates last log entry
        self.lastLogTerm = lastLogTerm #The term of the candidates last log entry

class replyVote_message:
    def __init__(self, term, voteGranted,sender,recpt):
        self.sender = sender
        self.recpt = recpt
        self.term = term #The term of the election request
        self.voteGranted = voteGranted #True or False if the vote was granted or not

#A message used for Byzantine generals message passing
class byzantine_message:
    def __init__(self, sender_chain, recpt, frame):
        self.sender_chain = sender_chain #The chain of previous senders
        self.recpt = recpt #The recipient. Should be the same as the process
        self.frame = frame #The value to be decided upon. Value for small values, checksum for large values

#A message, probably sent by a user
class transaction_message:
    def __init__(self,key,val,sender,recpt):
        self.key = key
        self.val = val
        self.sender = sender
        self.recpt = recpt
        return

#--The main message loop--
#Before the loop begins, bind to the given socket
#After entering the loop:
#Read the message, parse it, and then respond to it
#based upon the state of the process.
def message_loop(sock):
    proc.connect(sock)
    while True:
        msg = proc.read_message()
        res = parse_message(msg)
        if handle_message(res):
            return

#Parse the json message into a friendly python object
def parse_message(msg):
    return None

#Handle the message,
#return True if the process should terminate
#return False othermise
def handle_message(res):
    return True
    
