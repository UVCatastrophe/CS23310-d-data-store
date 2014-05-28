import zmq
import argparse
from zmq.eventloop import ioloop, zmqstream
ioloop.install()

#Class with all the state necessary for an instance of RAFT
class RAFT_instance:
    def __init__(self,name,peers,isLeader):
        self.currentTerm = 0 #The latest term the server has seen (monotonicly increasing)
        self.votedFor = None #The id of the last candidate vvoted for
        self.log = [] #A list of log entries 4-tuple (term,command,key,value)

        self.commitIndex = 0#Index into the log of the highest log entry that has been commited
        self.lastApplied = 0#the index of the highest log entry applied

        self.name = name #The name of the current node

        self.leader = 0 #Best guess at the current leader...
        #All process begin in "follower" mode in a normal execution.
        #Variable is used for testing.
        self.isLeader = isLeader 

        #--State used when the process is a leader--

        #Both are ditionaries which map peer-name to a numeric value
        self.nextIndex = {} #index of the next log entry to send to each server
        self.matchIndex = {} #index of the highest log enty which has been replicated on each server

        #Initialize for each peer
        for peer in peers:
            self.nextIndex[peer] = 0
            self.matchIndex[peer] = 0

    #Updates a LEADER's commit index to the lowest value in matchIndex
    def update_commitIndex(self):
        mn = self.matchIndex[0]
        for i in self.matchIndex:
            mn = min(mn, i)
        self.commitIndex = mn

#Class which contains the necessary state for the process to connect over a socket
class sock_state:
    def __init__(self):
        self.loop = ioloop.ZMQIOLoop.current
        self.context = zmq.Context()
        return
    #Connect to the socket sock which is used to send messages
    #and update the process's context/socket
    def connectSend(self,sock):
        self.socketSend = self.context.socket(zmq.REQ)
        self.socketSend.connect(sock)
        self.send = zmqstream.ZMQStream(self.socketSend,self.loop)
        self.send.on_recv(handle)
    #Connect to the socket (sock) which is used to recieve messages from the broker
    def connectRecv(self,sock):
        self.socketRecv = self.context.socket(zmq.SUB)
        self.socketRecv.connect(sock)
        self.socketRecv.set(zmq.SUBSCRIBE, self.name)
        self.recv = zmqstream.ZMQStream(self.socketRecv,self.loop)
        self.recv.on_recv(handle_broker_message)
    #Closes send and recv sockets
    def close_all(self):
        self.socketSend.close()
        self.socketRecv.close()
    #Read the message from the (bound) socket and return the result
    def read_message(self):
        return self.socketRecv.recv()

#The class which contains the information relevent to append a new entry to the log
class append_message:
    def __init__(self,term, leader, prevLogIndex,prevLogTerm,log_entries,leaderCommit,sender,recpt):
        self.term = term #The term of the current leader
        self.leader = leader #The id of the current leader
        self.prevLogIndex = prevLogIndex #index of the log entry precieding "log_entries"
        self.prevLogTerm = prevLogTerm #term corresponding to prevLogIndex
        self.entries = log_entries #entries to be appended to the log. See above for format
        self.leaderCommit = leaderCommit #The commit index of the leader
        self.sender = sender #Should be the leader.
        self.recpt = recpt #Should be the same id as the process

class appendReply_message:
    def __init__(self,term,prevLogIndex,log_len,status,msg_id,sender,recpt):
        self.sender = sender
        self.recpt = recpt
        self.term = term
        self.prevLogIndex = prevLogIndex #From the append message. Used to identify messages.
        self.log_len = log_len #The number of log entries which this message is accepting
        self.status = status #Either true or false for success or failure

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
    def __init__(self,key,val,action,sender,recpt):
        self.key = key
        self.val = val
        self.sender = sender
        self.recpt = recpt
        return

#Responses:
    #BAD_KEY - No such key to be read in the datastore
    #TIMEOUT - Timeout before being able to read a value
    #LEADER - The message was not sent to a current leader. Val has the name of the current leader if applicable.
    #READ - Given key was read and value is stored as 'value'
    #WRITE - Given key-val has been commited to the data-store
class transactionReply_message:
    def __init__(self,key,value,action,response,sender,recpt):
        self.key = key
        self.value = value #Possibly a returned value
        self.action = action
        self.response = response #See above
        self.sender = sender
        self.recpt = recpt

#Handles a transaction message (usually sent from a user)
def handle_transaction(msg):
    if not raft.isLeader:
        reply_msg = transactionReply_message(msg.key,raft.leader,msg.action,"LEADER",msg.sender,msg.recpt)
        #Send them the location of the leader
        return

#responds to an append message with the given status
def append_response(msg,status):
    res = appendReply(msg.term,msg.prevLogIndex,len(msg.log_entries),status,msg.recpt,msg.sender)
    #Now send the message to the broker***
    return

#Update the 'commited' index of the current raft instance
#given an append_message msg
def update_commit(msg):
    if msg.leaderCommit > raft.commitIndex:
        raft.commitIndex = min(msg.leaderCommit, msg.prevLogIndex)
    return

#Apply the given logs (in the append message)
# to the current log for this instance
def apply_log(msg):
    #Remove any uncommited entries that conflict
    for i in range(msg.prevLogIndex,raft.lastApplied+1):
        raft.log.pop(i)

    for log in msg.log_entries:
        raft.log.append(log)
    raft.lastApplied = len(raft.log)-1

#Handles an append message (supposedly) from the master
def handle_append(msg):
    #A new leader has been elected
    if msg.term > raft.currentTerm:
        raft.currentTerm = msg.term
        raft.isLeader = False
        return
    if msg.term < raft.currentTerm:
        return #Message that is necessarily out of date. Should reject.

    if raft.isMaster:
        print "Error, two masters operating with the same term number"
        return
    
    update_commit(msg) #Update commit index. Done even for hearbeat message

    #Heartbeat message
    if len(msg.log_entries) == 0:
        append_response(msg, True)
    
    lastTerm = raft.log[msg.prevLogIndex][0] #Term of last commited entry
    #Log is inconsistent with respect to master
    if not(lastTerm == msg.prevLogTerm):
        append_response(msg,False)
    else:
        apply_logs(msg)
        append_response(msg,True)

#constructs and then sends an append request to the given sender
def append_request(sender):
    prevIndex = raft.matchIndex[sender]
    prevTerm = raft.log[prevIndex][0]
    msg = append_message(raft.currentTerm,raft.name,prevIndex,prevTerm,\
                         raft.commitIndex,raft.name,sender)
    #TODO: send the message to the message broker
    return

#Handle a message recived on the send socket.
#This will be either raw messages from clients or the hello message from the broker
def handle(msg_frames):
    return

#Handles a protocol message delivered by the broker
def handle_broker_message(msg_frames):
    return

#Handles the response to a append message sent by a leader to the a follower.
#Either learns of its success and moves toward a quorum, or decrements
#its index to find a place where their logs are in sync.
def handle_appendReply(msg):
    if msg.term < raft.currentTerm:
        #message lost in the network, ignore
        return
    #This should never happen
    if msg.term > raft.currentTerm:
        print "Error: Follower " + str(msg.sender) + " is responding to the wrong master, or has the wrong term number"
        return
    if msg.status:#An accepting message
        m = max(l, msg.prevLogIndex + msg.num_logs,raft.matchIndex[msg.sender])
        raft.matchIndex[msg.sender] = m
        raft.nextIndex[msg.sender] = m+1
        raft.update_CommitIndex()
    else:
        if raft.matchIndex[msg.sender] > msg.prevLogIndex:
            #stale message
            return
        raft.nextIndex[msg.sender] = min(msg.prevLogIndex-1,raft.nextInsex[msg.sender])
        append_request(msg.sender)
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

def send_message(msg):
    return


#--------------------Initialization-------------------------------
#The state for the currect process' sockets
proc = sock_state()

parser = argparse.ArgumentParser()
parser.add_argument('--pub-endpoint',
  dest='pub_endpoint', type=str,
  default='tcp://127.0.0.1:23310')
parser.add_argument('--router-endpoint',
  dest='router_endpoint', type=str,
  default='tcp://127.0.0.1:23311')
parser.add_argument('--node-name',
  dest='node_name', type=str,
  default='test_node')
parser.add_argument('--spammer',
  dest='spammer', action='store_true')
parser.set_defaults(spammer=False)
parser.add_argument('--peer-names',
  dest='peer_names', type=str,
  default='')
#Used to skip master election for testing.
parser.add_argument('--test-isMaster',
    dest='isMaster', type=bool, default=False)
args = parser.parse_args()
args.peer_names = args.peer_names.split(',')

proc.connectSend(args.pub_endpoint)
proc.connectRecv(args.router_endpoint)

#The state for the current process' RAFT_instance
raft = RAFT_instance(args.node_name,args.peer_names,args.isMaster)
#Dictionary which maps keys to values once they have been comitted
data_store = {}

