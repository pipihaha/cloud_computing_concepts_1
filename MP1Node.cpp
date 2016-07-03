/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
	this->memberNode->inited = true;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
		memberNode->booter = true;
    }
    else {
        //size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        //msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        //msg->msgType = JOINREQ;
        //memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        //memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));
        
		MessageJoinReq * request = (MessageJoinReq *) malloc(sizeof(MessageJoinReq));
		request->messageheader.msgType = JOINREQ;
		request->nodeaddr = memberNode->addr;
		request->heartbeat = memberNode->heartbeat;
#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */
	 
	if (!memberNode->inited || !memberNode->inGroup) { // not initialized or in any group, just return
		return false;
	}
	 
	MessageHdr * msg = (MessageHdr *) data;
	 
	switch (msg->msgType) {
	    case JOINREQ: 
		    handle_message_JOINREQ(env, data, size);
			break;
		case JOINREP:
		    handle_message_JOINREP(env, data, size); 
			break;
		case LEAVENOTICE:
		    handle_message_LEAVENOTICE(env, data, size); 
			break;
		case HEARTBEAT:
		    handle_message_HEARTBEAT(env, data, size); 
			break;
		case MEMBERFAILURE:
		    handle_message_MEMBERFAILURE(env, data, size); 
			break;
		default: 
		    #ifdef DEBUGLOG
            log->LOG(&memberNode->addr, "Received undefined message, exiting.");
            #endif
            exit(1);
	}
	
	//free(msg);
	
	return true;
}

/**
 * FUNCTION NAME: handle_message_JOINREQ
 *
 * DESCRIPTION: check whether this is the introducier node, if yes, process it, send back response;
 * if not, gossiply send it out (likely this will not happen, since new node specificly send request to introducer)
 */
void MP1Node::handle_message_JOINREQ(void *env, char *data, int size ) {

	/*
	 * Your code goes here
	*/
	MessageJoinReq * req = (MessageJoinReq *) data;
	Address addr = req->nodeaddr;
	size_t pos = addr.find(":");
	int id = stoi(addr.substr(0, pos));
	short port = (short)stoi(addr.substr(pos + 1, addr.size()-pos-1));
		
	for (auto entry : memberNode->memberList) {
		if (id == entry->id && port == entry->port) { // node is already registered to this machine, simply return
			return;
		}
	}
	
	//new node to register
	//step 1: advertise it to other nodes in memberlist
	for (int index = 1, index < memberNode->memberList.size(), index ++) {
		MemberListEntry entry = memberNode->memberList[index];
		Address toaddr(to_string(entry->id) + ":" + to_string(entry->port));
		// send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, toaddr, (char *)msg, msgsize);
	}
	
	//step 2: put it in the memberlist
	MemberListEntry new_entry(id, port, req->heartbeat, memberNode->heartbeat);
	memberNode->memberList.push_back(new_entry);
	
	free(req);
    return;
}

/**
 * FUNCTION NAME: handle_message_JOINREP
 *
 * DESCRIPTION: initialize memberList

 */
void MP1Node::handle_message_JOINREP(void *env, char *data, int size ) {
    
	/*
	 * Your code goes here
	 */
	MessageJoinResp * welcome_resp = (MessageJoinResp *) data;
	 
	if (memberNode->inited && !memberNode->inGroup) { // should we also compare address of node?
		memberNode->inGroup = true;
		memberNode->nnb = 0;
		
		// index start from 1 to avoid send message to itself
		for (int index = 1; index < NUM_MEMBERLIST_ENTRIES_COPY; index ++) {
			if (data->memberList[index]->id != 0 || data->memberList[index]->port != 0) {
			    memberNode->memberList.push_back(data->memberList[index]);
				memberNode->nnb += 1;
		    } else {
				break;
			}
		}
	} 
    
	free(welcome_resp);
    return;
}

/**
 * FUNCTION NAME: handle_message_LEAVENOTICE
 *
 * DESCRIPTION: 

 */
void MP1Node::handle_message_LEAVENOTICE(void *env, char *data, int size ) {

	/*
	 * Your code goes here
	 */
    
	MessageLeaveNotice * leave_notice = (MessageLeaveNotice *) data;
	Address addr = leave_notice->nodeaddr;
	size_t pos = addr.find(":");
	int id = stoi(addr.substr(0, pos));
	short port = (short)stoi(addr.substr(pos + 1, addr.size()-pos-1));
	//step 1: delete the node from the memberlisth
	for (int index = 1, index < memberNode->memberList.size(), index ++) {
		MemberListEntry entry = memberNode->memberList[index];
		
		if (id == entry->id && port == entry->port) {
			memberNode->memberList.erase(memberNode->memberList.begin() + index);
		}
	}
	
	//step 2: check the leaving node's memberlist, add any missing member to the memberlist
	
	
	free(leave_notice);
    return;
}

/**
 * FUNCTION NAME: handle_message_HEARTBEAT
 *
 * DESCRIPTION: 

 */
void MP1Node::handle_message_HEARTBEAT(void *env, char *data, int size ) {

	/*
	 * Your code goes here
	 */

    return;
}

/**
 * FUNCTION NAME: handle_message_MEMBERFAILURE
 *
 * DESCRIPTION: 

 */
void MP1Node::handle_message_MEMBERFAILURE(void *env, char *data, int size ) {

	/*
	 * Your code goes here
	 */

    return;
}

/**
 * FUNCTION NAME: send_gossip_msg
 *
 * DESCRIPTION: Send message with gossip-like way to its neiboughs
 */
void MP1Node::send_gossip_msg(char* request, int request_size) {
    int numofnb, numofrcv, rcvoffset;
    
#ifdef DEBUGLOG
    static char s[1024];
    sprintf(s, "Gossip send msgs...");
    log->LOG(&memberNode->addr, s);
#endif

    // send message to randomly selected N members
	numofnb = memberNode.memberList.size();
	numofrcv = min(numofnb, GOSSIP_NUMBER);
	
	srand (time(NULL));
	rcvoffset = rand() % numofnb;
	
	for (int count = 0; count < numofrcv; count ++) {
		int index = (count + rcvoffset) % numofnb;
		Address toaddr(memberNode->memberList[index].id + ":" + memberNode->memberList[index].port);
        emulNet->ENsend(&memberNode->addr, &toaddr, (char *)request, request_size);
    }
}

    return;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */

    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
