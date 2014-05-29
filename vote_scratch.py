def handle_requestVote(msg):
    # Check if the candidate has a valid term.
    if msg.term < self.currentTerm:
       return RequestVote(self.currentTerm, False)
    # If message has a higher term, we must update the server.
    elif msg.term > self.currentTerm:
        self.currentTerm = msg.term
	self.isLeader = False
    # Two requirements needed to grant vote.
    # Req 1: votedFor is null or candidateId.
    if (self.votedFor is None) or (self.votedFor == msg.candidateId):
        self.votedFor = msg.candidateId
    else:
        return RequestVote(self.currentTerm, False)
    # Req 2: The candidate's log is at least as up-to-date as receiver's log.
    # A's log is more up-to-date log than B's if A's log's last term is higher.
    # If the terms are the same, then 
    # A's log is more up-to-date than B's if A's is longer (higher max index).
    # TODO: Verify the log-entry structure.
    self_lastLogTerm = self.log[-1].term
    if msg.lastLogTerm > self_lastLogTerm:
        return RequestVote(self.currentTerm, True)
    elif (msg.lastLogTerm == self_lastLogTerm):
    	# TODO: Check that we're not off-by-one.
	self_lastLogIndex = len(self.log)
	if msg.lastLogIndex >= self_lastLogIndex:
	    return RequestVote(self.currentTerm, True)
    else:
        return RequestVote(self.currentTerm, False)

def handle_requestVoteReply(msg):
    # Receiving a reply from someone on a higher term means updating the term,
    # resetting votedFor, and conversion to follower status.
    if msg.term > self.currentTerm:
        self.currentTerm = msg.term
	self.votedFor = None
	self.isLeader = False
    # We should have some number of sent messages.
    #if msg.voteGranted:
       	# self.votesGathered += 1
    #if msg.voteGranted > (self.all_servers / 2):
        # self.isLeader = True;
	
# TODO: Timeouts
	
