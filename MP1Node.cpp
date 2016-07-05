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
	//int id = *(int*)(&memberNode->addr.addr);
	//int port = *(short*)(&memberNode->addr.addr[4]);

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
	//MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
		//this.booter = true;
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
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)request, sizeof(MessageJoinReq));

        free(request);
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
	
	memberNode->bFailed = true;
	memberNode->inited = false;
	memberNode->inGroup = false;
	
	return 0;
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
 * FUNCTION NAME: delete_node_from_memberlist
 *
 * DESCRIPTION: delete a member from memberList
 */
void MP1Node::delete_node_from_memberlist(int id, short port) {
	for (int index = 0; index < (int)memberNode->memberList.size(); index ++) {
		MemberListEntry* entry = &memberNode->memberList[index];
		
		if (id == entry->id && port == entry->port) {
			// log it
			Address removed_addr(to_string(id) + ":" + to_string(port));
			log->logNodeRemove(&memberNode->addr, &removed_addr);
			
			memberNode->memberList.erase(memberNode->memberList.begin() + index);
			memberlist_set.erase(to_string(id) + ":" + to_string(port));
			removednode_timestamp[to_string(id) + ":" + to_string(port)] = memberNode->heartbeat;
			memberNode->nnb -= 1;
		}
	}
	
	return;
}
	
/**
 * FUNCTION NAME: add_node_to_memberlist
 *
 * DESCRIPTION: add a member to memberList
 */
void MP1Node::add_node_to_memberlist(int id, short port, long heartbeat, long timestamp) {
	string str = to_string(id) + ":" + to_string(port);
	string addr = memberNode->addr.getAddress();
	size_t pos = addr.find(":");
	int owner_id = stoi(addr.substr(0, pos));
	short owner_port = (short)stoi(addr.substr(pos + 1, addr.size()-pos-1));
	
	// if the new node is this node itself, do nothing
	if (id == owner_id && port == owner_port) {
		return;
	}
	
	// if the id:port is already in the memberlist, ignore the request
	if (memberlist_set.count(str) > 0) {
		return;
	}
	
	// if the node is a previously removed one, wait for at least FAILURE_NODE_REJOIN_INTERBAL time to rejoin
	if (removednode_timestamp.count(str) > 0 && removednode_timestamp[str] + FAILURE_NODE_REJOIN_INTERBAL >= memberNode->heartbeat) {
		return;
	}
	
	// log it
	Address added_addr(str);
	log->logNodeAdd(&memberNode->addr, &added_addr);
	
	MemberListEntry new_entry(id, port, heartbeat, timestamp);
	memberNode->memberList.push_back(new_entry);
	memberlist_set.insert(str);
	memberNode->nnb += 1;

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
	 
	//if (!memberNode->inited || !memberNode->inGroup || memberNode->bFailed) { // not initialized or in any group, just return
	//	return false;
	//}
	 
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
	MessageJoinReq *req = (MessageJoinReq *) data;
	string addr = req->nodeaddr.getAddress();
	size_t pos = addr.find(":");
	int id = stoi(addr.substr(0, pos));
	short port = (short)stoi(addr.substr(pos + 1, addr.size()-pos-1));
	
	#ifdef SELFDEBUG
        log->LOG(&memberNode->addr, "received join request ...");
    #endif
		
	// if the node is alreay in the memberList, do nothing
	if (memberlist_set.count(to_string(id) + ":" + to_string(port)) != 0) {
		return;
	}
	
	//new node to register
	//step 1: advertise it to other nodes in memberlist
	for (int index = 0; index < (int)memberNode->memberList.size(); index ++) {
		MemberListEntry *entry = &memberNode->memberList[index];
		Address toaddr = GetAddress(entry->id, entry->port);
		
		#ifdef SELFDEBUG
		printAddress(&toaddr);
		
        log->LOG(&memberNode->addr, "forward join request to memberList ...");
        #endif
		
        emulNet->ENsend(&memberNode->addr, &toaddr, data, size);
	}
	
	//step 2: put it in the memberlist
	add_node_to_memberlist(id, port, req->heartbeat, memberNode->heartbeat);
	
	//step 3: send JOINREP back to the new node
	MessageJoinResp *welcome_resp = (MessageJoinResp *) malloc(sizeof(MessageJoinResp));;
	welcome_resp->messageheader.msgType = JOINREP;
	welcome_resp->nodeaddr = memberNode->addr;
	welcome_resp->heartbeat = memberNode->heartbeat;
	
	int num_memberlist_items = min((int)memberNode->memberList.size() - 1, NUM_MEMBERLIST_ENTRIES_COPY);	
	srand(time(NULL));
	int start = rand() % memberNode->memberList.size();
		
	for (int i = 0; i < num_memberlist_items; i ++) {
		welcome_resp->memberList[i] = memberNode->memberList[start];
		start = (start + 1) % memberNode->memberList.size();
	}
		
	for (int i = num_memberlist_items; i < NUM_MEMBERLIST_ENTRIES_COPY; i ++) { // clear these entries,
		welcome_resp->memberList[i].id = 0;
		welcome_resp->memberList[i].port = 0;
	}
	
	// send JOINREP message to new member
    emulNet->ENsend(&memberNode->addr, &req->nodeaddr, (char *)welcome_resp, sizeof(MessageJoinResp));
	
	free(welcome_resp);
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
	MessageJoinResp *welcome_resp = (MessageJoinResp *) data;
	string addr = welcome_resp->nodeaddr.getAddress();
	size_t pos = addr.find(":");
	int id = stoi(addr.substr(0, pos));
	short port = (short)stoi(addr.substr(pos + 1, addr.size()-pos-1));
	
	
	#ifdef SELFDEBUG
        log->LOG(&memberNode->addr, "received JOINREP response ...");
     #endif
	 
	// add introducer to memberList
	 add_node_to_memberlist(id, port, welcome_resp->heartbeat, memberNode->heartbeat);
	 
	 // add members in member list to own memberListist
	if (memberNode->inited && !memberNode->inGroup) { // should we also compare address of node?
		memberNode->inGroup = true;
		memberNode->nnb = 0;
		
		for (int index = 0; index < NUM_MEMBERLIST_ENTRIES_COPY; index ++) {
			if (welcome_resp->memberList[index].id != 0 || welcome_resp->memberList[index].port != 0) {
				add_node_to_memberlist(welcome_resp->memberList[index].id, welcome_resp->memberList[index].port, welcome_resp->memberList[index].heartbeat, memberNode->heartbeat);
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
    
	MessageLeaveNotice *leave_notice = (MessageLeaveNotice *) data;
	string addr = leave_notice->nodeaddr.getAddress();
	size_t pos = addr.find(":");
	int id = stoi(addr.substr(0, pos));
	short port = (short)stoi(addr.substr(pos + 1, addr.size()-pos-1));
	
	//step 1: delete the node from the memberlisth
	delete_node_from_memberlist(id, port);
	
	//step 2: check the leaving node's memberlist, add any missing member to the memberlist
	for (int index = 0; index < NUM_MEMBERLIST_ENTRIES_COPY; index ++) {
		string str = to_string(leave_notice->memberList[index].id) + ":" + to_string(leave_notice->memberList[index].port);
		
		if (memberlist_set.count(str) == 0) {
			add_node_to_memberlist(leave_notice->memberList[index].id, leave_notice->memberList[index].port, leave_notice->memberList[index].heartbeat, memberNode->heartbeat);
		}
	}
	
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
    
	MessageHEARTBEAT *msg = (MessageHEARTBEAT *) data;
	string addr = msg->nodeaddr.getAddress();
	size_t pos = addr.find(":");
	int id = stoi(addr.substr(0, pos));
	short port = (short)stoi(addr.substr(pos + 1, addr.size()-pos-1));
	
	if (memberlist_set.count(to_string(id) + ":" + to_string(port)) == 0) { // not exist in membershiplist, add it in
		add_node_to_memberlist(id, port, msg->heartbeat, memberNode->heartbeat);
	} else { // check whether it's in the suspected failure list, if yes, removed it from this list
		for (int index = 0; index < (int)suspected_list.size(); index ++) {
			MemberListEntry *suspected_entry = &suspected_list[index];
			
			if (id == suspected_entry->id && port == suspected_entry->port) {
				if (msg->heartbeat > suspected_entry->heartbeat) { // seems updated, remove it from suspected list 
					suspected_list.erase(suspected_list.begin() + index);
					suspected_set.erase(to_string(suspected_entry->id) + ":" + to_string(suspected_entry->port));
				}
				
				break;
			}
		}
	}
	
	// update heartbeat for the coming node
	for (auto entry : memberNode->memberList) {
		if (id == entry.id && port == entry.port) {
			
			if (msg->heartbeat > entry.heartbeat) {
				entry.heartbeat = msg->heartbeat;
				entry.timestamp = memberNode->heartbeat;
			}
	
			break;
		}
	}
	
	// update heartbeat for memberlist of coming node
	for (auto updated_entry : msg->memberList) {
		if (updated_entry.id == 0 && updated_entry.port == 0) {
			break;
		}
		
		if (memberlist_set.count(to_string(updated_entry.id) + ":" + to_string(updated_entry.port)) == 0) {// new node, add into memberlist
			add_node_to_memberlist(updated_entry.id, updated_entry.port, msg->heartbeat, memberNode->heartbeat);
		} else {
			for (auto old_entry : memberNode->memberList) {
				if (updated_entry.id == old_entry.id && updated_entry.port == old_entry.port) {
					if (updated_entry.heartbeat > old_entry.heartbeat) {
						old_entry.heartbeat = updated_entry.heartbeat;
						old_entry.timestamp = memberNode->heartbeat;
					}
				}
			}
		}
	}
	
	free(msg);
    return;
}

/**
 * FUNCTION NAME: handle_message_MEMBERFAILURE
 *
 * DESCRIPTION: 
 */
void MP1Node::handle_message_MEMBERFAILURE(void *env, char *data, int size ) {

	/*
	 * Your code goes here (seems nothing has to been done)
	 */
	 /*
    MessageMemberFailure * mfail = (MessageMemberFailure *) data;
	Address addr = mfail->nodeaddr;
	size_t pos = addr.find(":");
	int id = stoi(addr.substr(0, pos));
	short port = (short)stoi(addr.substr(pos + 1, addr.size()-pos-1));
	
	*/
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
    memberNode->heartbeat += 1;
	
	// check suspected list, remove possibly dead nodes from it and membership list
	vector<int> remove_list;
	for (int index = 0; index < (int)suspected_list.size(); index ++) {
		if (suspected_list[index].timestamp + FAILURE_PERIOD < memberNode->heartbeat) {
			delete_node_from_memberlist(suspected_list[index].id, suspected_list[index].port);
			suspected_set.erase(to_string(suspected_list[index].id) + ":" + to_string(suspected_list[index].port));
			remove_list.push_back(index);
		}
	}
	
    for (int i = (int) remove_list.size() - 1; i >= 0; i --) {
	    suspected_list.erase(suspected_list.begin() + remove_list[i]);
    }
	
	// check membership list, move possible dead nodes to suspected list
	for (auto entry : memberNode->memberList) {
		string str = to_string(entry.id) + ":" + to_string(entry.port);
		if (suspected_set.count(str) == 0 && entry.timestamp + SUSPECTED_FAILURE_PERIOD < memberNode->heartbeat) {
			suspected_set.insert(str);
			MemberListEntry new_suspected_entry(entry.id, entry.port, entry.heartbeat, memberNode->heartbeat);
			suspected_list.push_back(new_suspected_entry);
		}
	}
	
	// send heartbeat message to members
	int num_memberlist_items = min((int)memberNode->memberList.size() - 1, NUM_MEMBERLIST_ENTRIES_COPY);
	MessageHEARTBEAT * msg = (MessageHEARTBEAT *) malloc(sizeof(MessageHEARTBEAT));
	msg->messageheader.msgType = HEARTBEAT;
	msg->nodeaddr = memberNode->addr;
	msg->heartbeat = memberNode->heartbeat;
	
	for (int index = 0; index < (int)memberNode->memberList.size(); index ++) {
		srand(time(NULL));
		int start = rand() % memberNode->memberList.size();
		
		start = (start == index)? ((start + 1) % memberNode->memberList.size()): start;
		for (int i = 0; i < num_memberlist_items; i ++) {
			msg->memberList[i] = memberNode->memberList[start];
			start = (start + 1) % memberNode->memberList.size();
		}
		
		for (int i = num_memberlist_items; i < NUM_MEMBERLIST_ENTRIES_COPY; i ++) { // clear these entries,
			msg->memberList[i].id = 0;
			msg->memberList[i].port = 0;
		}
		 
		Address toaddr = GetAddress(memberNode->memberList[index].id, memberNode->memberList[index].port);
		emulNet->ENsend(&memberNode->addr, &toaddr, (char *)msg, sizeof(MessageHEARTBEAT));
	}
	
	free(msg);
	
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

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
Address MP1Node::GetAddress(int id, short port)
{
    Address nodeaddr;

    memset(&nodeaddr, 0, sizeof(Address));
    *(int *)(&nodeaddr.addr) = id;
    *(short *)(&nodeaddr.addr[4]) = port;

    return nodeaddr;
}
