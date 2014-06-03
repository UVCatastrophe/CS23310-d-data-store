import zmq
import argparse
import sys
import signal
import json
from zmq.eventloop import ioloop, zmqstream
ioloop.install()

LEADER_LEASE_TIME = 5.0

#Class with all the state necessary for an instance of RAFT
class RAFT_instance:
    def __init__(self,name,peers,start_leader=""):
        self.currentTerm = 0 #The latest term the server has seen (monotonicly increasing)
        self.votedFor = None #The id of the last candidate vvoted for
        self.numVotes = 0 #The number of votes recieved this term

        #A list of log entries 4-tuple (term,command,key,value)
        #Initialized with one entry for purposes of our implementation
        self.log = [(self.currentTerm,"start","startKey",0)]
        
        self.commitIndex = 0#Index into the log of the highest log entry that has been commited
        self.lastApplied = 0#the index of the highest log entry applied

        self.name = name #The name of the current node

        #Best guess at the current leader...
        self.leader = start_leader
        
        #All process begin in "follower" mode in a normal execution.
        #Variable is used for testing.
        if start_leader == self.name:
            self.isLeader = True
        else:
            self.isLeader = False

        #--State used when the process is a leader--
        self.idQueue = [] #The id of a get/set message. Attached to a get/set response
        self.senderQueue = [] #Parallel to the id queue. None if sent by the system. Otherwise contains the name of the node which forwarded the message.

        #Both are ditionaries which map peer-name to a numeric value
        self.lastHeard = {} #The last time that a (current term) message was recieved from the given peer
        self.nextIndex = {} #index of the next log entry to send to each server
        self.matchIndex = {} #index of the highest log enty which has been replicated on each server

        #Initialize for each peer
        self.numPeers = 0
        for peer in peers:
            if peer == name or peer == "":
                continue
            self.numPeers += 1
            #nextIndex is STRICTLY greater than matchIndex
            self.lastHeard[peer] = proc.loop.time()
            self.nextIndex[peer] = 1
            self.matchIndex[peer] = 0

        #Add a global timeout to send hearbeat messages
        if self.isLeader:
            proc.loop.add_timeout(proc.loop.time() + (LEADER_LEASE_TIME/2), send_heartbeats)

    #Updates a LEADER's commit index to the lowest value in matchIndex
    def update_commitIndex(self):
        counts = []
        for peer in self.matchIndex:
            i = self.matchIndex[peer]
            counts.append(i)
        counts.sort()
        oldCommit = self.commitIndex
        if len(counts) % 2 == 0:
            self.commitIndex = counts[len(counts)/2 -1]
        else:
            self.commitIndex = counts[len(counts)/2]
        return oldCommit
    #Updates the state of the process so that it is now the leader
    def makeLeader(self):
        self.isLeader = True
        for peer in self.nextIndex:
            #Start by attempting to reconicle the old logs to your current one
            self.nextIndex[peer] = self.lastApplied + 1
            self.matchIndex[peer] = 0
            self.leader = self.name
    #Sets the current term to the new term and changes appropriate state
    def new_term(self,term):
        assert term > self.currentTerm#Must be monotonicaly increasing
        self.currentTerm = term
        self.votedFor = None
        self.numVotes = 0
        if raft.isLeader:
            raft.isLeader = False
        

#Class which contains the necessary state for the process to connect over a socket
class sock_state:
    def __init__(self):
        self.logAll = True #Log all messages sent/recieved from the broker
        self.loop = ioloop.ZMQIOLoop.current()
        self.context = zmq.Context()
        self.isConnected = False #Changed after recieving a hello message
        #windows is weird and only has 2 signals...
        for sig in [signal.SIGTERM, signal.SIGINT]:
            signal.signal(sig, self.close_all)
        return
    #Connect to the socket sock which is used to send messages
    #and update the process's context/socket
    def connectSend(self,sock):
        self.socketSend = self.context.socket(zmq.REQ)
        self.socketSend.connect(sock)
        self.send = zmqstream.ZMQStream(self.socketSend,self.loop)
        self.send.on_recv(self.handle_broker_message)
    #Connect to the socket (sock) which is used to recieve messages from the broker
    def connectRecv(self,sock):
        self.socketRecv = self.context.socket(zmq.SUB)
        self.socketRecv.connect(sock)
        self.socketRecv.set(zmq.SUBSCRIBE, raft.name)
        self.recv = zmqstream.ZMQStream(self.socketRecv,self.loop)
        self.recv.on_recv(self.handle)
    #Closes send and recv sockets
    def close_all(self,b,c):
        self.loop.stop()
        self.socketSend.close()
        self.socketRecv.close()
        sys.exit(0)
        #Handle a message recived on the send socket.
    #This will be either raw messages from clients or the hello message from the broker
    def handle(self,msg_frames):

        if not msg_frames[0] == raft.name:
            return #Not for you
        
        msg_json = json.loads(msg_frames[2])

        if msg_json['type'] == 'hello':
            if self.isConnected:
                return
            else:
                self.isConnected = True
            
            proc.send.send_json({'type': 'hello', 'source': raft.name})
            if self.logAll:
                proc.send.send_json({'type': 'log', 'source' : raft.name, 'debug': 'hello recieved'})
            return
        
        elif msg_json['type'] == "debug_setLeader":
             raft.isLeader = True
             if self.logAll:
                 proc.send.send_json({'type': 'log', 'source' : raft.name, 'debug': raft.name + " made leader"})
             return

        #Asked to forward the message body back to the message broker
        elif msg_json['type'] == "transaction_forward":
            self.send.send_json(msg_json['body'])
            return
        #Parse into a message object and send that to the general handle_message

        elif msg_json['type'] == 'debug_stop':
            print "debug stop"
            return
            
        else:
            if 'destination' in msg_json and (not raft.name in msg_json['destination']):
                print "message dropped"
                return
            
            if self.logAll:
                print "recieved message"
                #proc.send.send_json({'type' : 'log', 'debug' : msg_json })
            (msg_type,msg) = parse_json(msg_json)
            handle_message(msg_type,msg)
        return

    #Handles a protocol message delivered by the broker
    def handle_broker_message(self,msg_frames):
        msg_json = json.loads(msg_frames[0])
        if 'source' in msg_json and msg_json['source'] == raft.name:
            return #from you. Not for you.



        (msg_type,msg) = parse_json(msg_json)
        handle_message(msg_type,msg)
        return

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
    def to_json(self):
        return { 'type' : 'raft_append', 'term' : self.term, 'leader' : self.leader,
                 'prevLogIndex' : self.prevLogIndex, 'prevLogTerm' : self.prevLogTerm,
                 'entries' : self.entries, 'leaderCommit' : self.leaderCommit,
                 'sender' : self.sender, 'destination' : [self.recpt]}
    

class appendReply_message:
    def __init__(self,term,prevLogIndex,log_len,status,sender,recpt):
        self.sender = sender
        self.recpt = recpt
        self.term = term
        self.prevLogIndex = prevLogIndex #From the append message. Used to identify messages.
        self.log_len = log_len #The number of log entries which this message is accepting
        self.status = status #Either true or false for success or failure
    def to_json(self):
        return { 'type' : 'raft_appendReply', 'term' : self.term, 'prevLogIndex' : self.prevLogIndex,
                 'log_len' : self.log_len, 'status' : self.status, 'destination' : [self.recpt],
                 'sender' : self.sender }

#Sent by a candidate to request a vote
class requestVote_message:
    def __init__(self,term,candidate,lastLogIndex,lastLogTerm,sender,recpts):
        self.sender = sender
        self.recpts = recpts
        self.term = term #The term of the candidate
        self.candidate = candidate #The id of the candidate requesting a vote
        self.lastLogIndex = lastLogIndex #The index of the candidates last log entry
        self.lastLogTerm = lastLogTerm #The term of the candidates last log entry
    def to_json(self):
        return { 'type' : 'raft_requestVote', 'term' : self.term, 'candidate' : self.candidate,
                 'lastLogIndex' : self.lastLogIndex, 'lastLogTerm' : self.lastLogTerm,
                 'destination' : self.recpts, 'sender' : self.sender }

class replyVote_message:
    def __init__(self, term, voteGranted,sender,recpt):
        self.sender = sender
        self.recpt = recpt
        self.term = term #The term of the election request
        self.voteGranted = voteGranted #True or False if the vote was granted or not
    def to_json(self):
        return { 'type' : 'raft_replyVote', 'term' : self.term, 'voteGranted' : self.voteGranted,
                 'sender' : self.sender, 'destination' : self.destination }

#A message used for Byzantine generals message passing
class byzantine_message:
    def __init__(self, sender_chain, recpt, frame):
        self.sender_chain = sender_chain #The chain of previous senders
        self.recpt = recpt #The recipient. Should be the same as the process
        self.frame = frame #The value to be decided upon. Value for small values, checksum for large values

#A message, probably sent by a user
class transaction_message:
    def __init__(self,key,value,action,recpt,msg_id,sender):
        self.action = action
        self.key = key
        self.value = value
        self.recpt = recpt
        self.msg_id = msg_id
        #Sender is None if it came directly from a client, has a value if it is forwarded from a client
        self.sender = sender  
        return
    def to_json(self):
        return { 'type' : self.action, 'key': self.key, 'value': self.value,
                 'destination' : [self.recpt], "id" : self.msg_id, 'sender' : self.sender }

#Responses:
    #BAD_KEY - No such key to be read in the datastore
    #TIMEOUT - Timeout before being able to read a value
    #LEADER - The message was not sent to a current leader. Val has the name of the current leader if applicable.
    #READ - Given key was read and value is stored as 'value'
    #WRITE - Given key-val has been commited to the data-store
class transactionReply_message:
    def __init__(self,key,value,action,response,sender,msg_id):
        self.key = key
        self.value = value #Possibly a returned value
        self.action = action
        self.response = response #See above
        self.sender = sender
        self.msg_id = msg_id
    def to_json(self):
        return {'type' : self.action + "Response",
                'key' : self.key,
                'value' : self.value,
                'sender' : self.sender,
                'response' : self.response,
                'id' : self.msg_id}

#Handles a transaction message (usually sent from a user)
def handle_transaction(msg):
    if not raft.isLeader:
        reply_msg = transactionReply_message(msg.key,raft.leader,msg.action,"LEADER",msg.sender,msg.recpt)
        #Send them the location of the leader
        return

#responds to an append message with the given status
def append_response(msg,status):
    res = appendReply_message(msg.term,msg.prevLogIndex,len(msg.entries),status,msg.recpt,msg.sender)
    send_message(res)
    return

# ********** BEGIN: Andrew's additions ***********
# responds to a vote message with the given status (True/False)
def vote_response(msg, status):
    res = replyVote_message(raft.currentTerm, status, msg.recpt, msg.sender)
    send_message(res)
    return
# ********* END: Andrew's additions **********

#Update the 'commited' index of the current raft instance
#given an append_message msg
def update_commit(msg):
    if msg.leaderCommit > raft.commitIndex:
        raft.commitIndex = min(msg.leaderCommit, msg.prevLogIndex)
        for log in range(msg.prevLogIndex,raft.commitIndex):
            #Update the value of the data-store to the most recent one
            if log[1] == "get":
                #data_store[key] = val
                data_store[log[2]] = log[3]
    return

#Apply the given logs (in the append message)
# to the current log for this instance
def apply_log(msg):
    #Remove any uncommited entries that conflict
    for i in range(msg.prevLogIndex+1,raft.lastApplied+1):
        raft.log.pop(i)

    for log in msg.entries:
        raft.log.append(log)
    raft.lastApplied = len(raft.log)-1


#Handles an append message (supposedly) from the master
def handle_append(msg):
    #A new leader has been elected
    if msg.term > raft.currentTerm:
        raft.new_term(msg.term)
        return
    if msg.term < raft.currentTerm:
        return #Message that is necessarily out of date. Should reject.

    if raft.isLeader:
        print "Error, two masters operating with the same term number"
        return

    raft.leader = msg.leader
    raft.lastHeard[msg.sender] = proc.loop.time()
    
    update_commit(msg) #Update commit index. Done even for hearbeat message

    print "Offending Index: " + str(msg.prevLogIndex)
    lastTerm = raft.log[msg.prevLogIndex][0] #Term of last commited entry
    #Log is inconsistent with respect to master
    if not(lastTerm == msg.prevLogTerm):
        append_response(msg,False)
    else:
        apply_log(msg)
        append_response(msg,True)


#Handles the response to a append message sent by a leader to the a follower.
#Either learns of its success and moves toward a quorum, or decrements
#its index to find a place where their logs are in sync.
def handle_appendReply(msg):
    if not raft.isLeader:
        return
    
    if msg.term < raft.currentTerm:
        #message lost in the network, ignore
        return
    #This should never happen
    if msg.term > raft.currentTerm:
        print "Error: Follower " + str(msg.sender) + " is responding to the wrong master, or has the wrong term number"
        return

    raft.lastHeard[msg.sender] = proc.loop.time()
    
    if msg.status:#An accepting message
        m = max(msg.prevLogIndex + msg.log_len,raft.matchIndex[msg.sender])
        raft.matchIndex[msg.sender] = m
        raft.nextIndex[msg.sender] = m+1
        last = raft.update_commitIndex()
        #Update the datastore, send out set/get response messages for the newly updated commit indes
        transaction_reply(last)
        return
    else:
        if raft.matchIndex[msg.sender] > msg.prevLogIndex:
            #stale message
            return
        raft.nextIndex[msg.sender] = min(msg.prevLogIndex-1,raft.nextIndex[msg.sender])
        #If this has failed, then there has been a failure of some invarient
        assert raft.nextIndex[msg.sender] > raft.matchIndex[msg.sender]
        append_request(msg.sender)
        return

# ********** BEGIN: Andrew's additions **********
def handle_vote(msg):
    # Check if the candidate has a valid term.
    if msg.term < raft.currentTerm:
        vote_response(msg, False)
        return
    # If message has a higher term, then we must update the node.
    elif msg.term > raft.currentTerm:
        raft.new_term(msg.term)
    # Two requirements needed to grant vote.
    # Req 1: votedFor is null or candidateId.
    if (raft.votedFor is None) or (raft.votedFor == msg.candidateID):
        raft.votedFor = msg.candidateId
    else:
        vote_response(msg, False)
        return
    # Req 2: The candidate's log is at least as up-to-date as receiver's log.
    # A's log is more up-to-date log than B's if A's log's last term is higher.
    # If the terms are the same, then 
    # A's log is more up-to-date than B's if A's is longer (higher max index).
    # TODO: Verify the log-entry structure.
    self_lastLogTerm = raft.log[-1][0]
    if msg.lastLogTerm > self_lastLogTerm:
        vote_response(msg, True)
    elif (msg.lastLogTerm == self_lastLogTerm):
        # TODO: Check that we're not off-by-one.
        self_lastLogIndex = len(raft.log)-1
        if msg.lastLogIndex >= self_lastLogIndex:
            vote_response(msg, True)
    else:
        vote_response(msg, False)

def handle_voteReply(msg):
    if msg.currentTerm < raft.currentTerm:
        return #Out of date messge
    
    if raft.votedFor != raft.name:
        return #You should not be recieving votes
    if msg.currentTerm > raft.currentTerm:
        #Something has gone wrong...
        print "Error, being voted for without request"
        return

    if msg.success:
        raft.numVotes += 1
    else:
        return
    
    if raft.numVotes >= (raft.numPeers + 1.0)/2:
        #leader election won.
        raft.makeLeader()
        send_heartbeats()
    
# ********** END: Andrew's additions **********

#attempt to become the leader by requesting votes from all of the process'
#peers.
def request_votes():
    raft.new_term(raft.currentTerm + 1)
    raft.votedFor = raft.name

    peers = []
    for peer in raft.nextIndex:
        peers.append(peer)

    lastIndex = len(raft.log)-1
    lastTerm = raft.log[-1][0]
    msg = requestVote_message(raft.currentTerm,raft.name,
                              lastIndex,lastTerm, raft.name,peers)
    raft.send_message(msg)


#Sends replies to the broker for each transaction starting at last and going to committedIndex
def transaction_reply(last):
    for i in range(last+1,raft.commitIndex+1):
        (term,opp,key,value) = raft.log[i]
        if opp == "set":
            msg_id = raft.idQueue.pop(0)
            fwd = raft.senderQueue.pop(0)
            data_store[key] = value
            if fwd == None:
                send_message(transactionReply_message(key,value,opp,"SUCCESS",raft.name,msg_id))
            else:
                body = transactionReply_message(key,value,opp,"SUCCESS",raft.name,msg_id).to_json()
                proc.send.send_json( { 'type' : "transaction_forward", 'destination' : [fwd], 'body' : body })
        elif opp == 'get':
            msg_id = raft.idQueue.pop(0)
            fwd = raft.senderQueue.pop(0)
            if key in data_store:
                res = "SUCCESS"
            else:
                res = "FAILURE"
            rply = transactionReply_message(key,data_store[key],opp,res,raft.name,msg_id)

            if fwd == None:
                send_message(rply)
            else:
                body = rply.to_json()
                proc.send.send_json( { 'type' : "transaction_forward", 'destination' : [fwd], 'body' : body })
                
#Parse the json message into a friendly python object
def parse_json(msg_json):
    msg = None
    if msg_json['type'] == 'get' or msg_json['type'] == 'set':
        value = None
        sender = None
        if 'value' in msg_json:
            value = msg_json['value']
        if 'sender' in msg_json:
            sender = msg_json['sender']
        msg = transaction_message(msg_json['key'],value, msg_json['type'],raft.name,msg_json['id'],sender)
    elif msg_json['type'] == 'raft_append':
        msg = append_message(msg_json['term'],msg_json['leader'],msg_json['prevLogIndex'],
                             msg_json['prevLogTerm'],msg_json['entries'],msg_json['leaderCommit'],
                             msg_json['sender'], msg_json['destination'][0])
    elif msg_json['type'] == 'raft_appendReply':
        msg = appendReply_message(msg_json['term'],msg_json['prevLogIndex'],msg_json['log_len'],
                                  msg_json['status'],msg_json['sender'],raft.name)
    elif msg_json['type'] == 'raft_requestVote':
        msg = requestVote_message(msg_json['term'],msg_json['candidate'],msg_json['lastLogIndex'],
                                  msg_json['lastLogTerm'],msg_json['sender'],msg_json['destination'][0])
    elif msg_json['type'] == 'raft_replyVote':
        msg = requestVote_message(msg_json['term'],msg_json['voteGranted'],msg_json['sender'],
                                  None)
        
    return ( msg_json['type'], msg)

#Handles a request to set to a value
def handle_get_set(msg):
    if not raft.isLeader:
        #Redirect to the leader
        msg.recpt = raft.leader
        msg.sender = raft.name
        send_message(msg)
        return

    raft.idQueue.append(msg.msg_id)
    print "MSG SENDER : " + str(msg.sender)
    raft.senderQueue.append(msg.sender)
    
    raft.log.append( (raft.currentTerm, msg.action, msg.key,msg.value) )
    raft.lastApplied += 1

    send_appends()

    #Just the leader case. Useful for testing...
    if raft.numPeers == 0:
        print "zero peer case"
        if msg.action == "set":
            data_store[msg.key] = msg.value
            
        raft.commitIndex = raft.lastApplied
        transaction_reply(raft.commitIndex-1)
        
#Sends append_message's to each
def send_appends():
    for peer in raft.nextIndex:
        nIndex = raft.nextIndex[peer]
        prevTerm = raft.log[nIndex-1][0]
        log_send = raft.log[nIndex:]
        app = append_message(raft.currentTerm,raft.name,nIndex-1,
                             prevTerm, log_send, raft.commitIndex, raft.name,peer)
        print "Sending append_message to " + peer
        send_message(app)

#Handle the message,
#return True if the process should terminate
#return False othermise
def handle_message(msg_type,msg):
    print raft.name + ": handling msg of type " + msg_type 
    if msg_type == "set" or msg_type == "get":
        handle_get_set(msg)
    elif msg_type == "raft_append":
        handle_append(msg)
    elif msg_type == "raft_appendReply":
        handle_appendReply(msg)
    elif msg_type == "okay":
        return
    elif msg_type == "raft_requestVote":
        handle_vote(msg)
    elif msg_type == "raft_replyVote:
        handle_voteReply(msg)
    return True

#given a message object, send an object to the message broker
def send_message(msg):
    if proc.logAll:
        proc.send.send_json({ 'type' : 'log', 'debug' : msg.to_json()})
    proc.send.send_json(msg.to_json())
    return

#Sends a heartbeat message to all peers
#Makes sure that they have heard from the leader.
#Also serves to make sure that all logs are up to date on all replicas
def send_heartbeats():
    send_appends()
    if raft.isLeader:
        proc.loop.add_timeout(proc.loop.time() + (LEADER_LEASE_TIME/2), send_heartbeats)

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
parser.add_argument('--start-leader',
    dest='start_leader', type=str, default="")
args = parser.parse_args()
args.peer_names = args.peer_names.split(',')

#The state for the current process' RAFT_instance
raft = RAFT_instance(args.node_name,args.peer_names,start_leader=args.start_leader)

proc.connectRecv(args.pub_endpoint)
proc.connectSend(args.router_endpoint)

#Dictionary which maps keys to values once they have been comitted

data_store = {}

proc.loop.start()
