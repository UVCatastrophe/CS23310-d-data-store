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

#The main message loop:
#Read the message, parse it, and then respond to it
#based upon the state of the process.
def message_loop():
    while True:
        msg = read_message()
        res = parse_message(msg)
        if handle_message(res):
            return

#Read the message from the socket and return the result
def read_message():
    return ""

#Parse the json message into a friendly python object
def parse_message(msg):
    return None

#Handle the message,
#return True if the process should terminate
#return False othermise
def handle_message(res):
    return True
    
