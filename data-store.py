import zmq

#The state for the current process
proc = process()
#Dictionary which maps keys to values once they have been comitted
data_store = {}

#Class which contains the necessary state for the process to run
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

#The class which contains the relavent information to run
# a round of paxos.
class paxos_message:
    def __init__(self,instr_num, seq_num, msg_type,key,val,sender,recpt, prior=None):
        self.instr_num = instr_num #The identifier for the paxos instnace
        self.seq_num = seq_num #The number for the sequnce within a paxos instance
        self.msg_type = msg_type #The type of the paxos message. See "Types"
        self.key = key #Key to read/write to the data store?
        self.val = val #Val to read/write to the data strore?
        self.sender = sender #Id of the person who sent the message
        self.recpt = recpt #Id of the recipt. Should be the ID of the current process
        self.prior = prior #Used for certain replies from acceptors

#A message used for Byzantine generals message passing
class byzantine_message:
    def __init__(self, sender_chain, recpt, frame):
        self.sender_chain = sender_chain #The chain of previous senders
        self.recpt = recpt #The recipient. Should be the same as the process
        self.frame = frame #The value to be decided upon. Value for small values, checksum for large values

#A message, probably sent by a user?
class transaction_message:
    def __init__(self):
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
    
