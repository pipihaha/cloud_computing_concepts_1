/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Header file of MP1Node class.
 **********************************/

#ifndef _MP1NODE_H_
#define _MP1NODE_H_

#include "stdincludes.h"
#include "Log.h"
#include "Params.h"
#include "Member.h"
#include "EmulNet.h"
#include "Queue.h"
#include <unordered_set>
#include <unordered_map>
/**
 * Macros
 */
 #define SELFDEBUG 1
 
 
#define TREMOVE 20
#define TFAIL 5
#define NUM_MEMBERLIST_ENTRIES_COPY 20
#define HB_INTERVAL_PING 5
#define SUSPECTED_FAILURE_PERIOD HB_INTERVAL_PING * 20
#define FAILURE_PERIOD  HB_INTERVAL_PING * 10
#define FAILURE_NODE_REJOIN_INTERBAL  SUSPECTED_FAILURE_PERIOD
/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Message Types
 */
enum MsgTypes{
    JOINREQ,
    JOINREP,
	LEAVENOTICE,
	HEARTBEAT,
	MEMBERFAILURE,
    DUMMYLASTMSGTYPE
};

/**
 * STRUCT NAME: MessageHdr
 *
 * DESCRIPTION: Header and content of a message
 */
typedef struct MessageHdr {
	enum MsgTypes msgType;
}MessageHdr;

/**
 * STRUCT NAME: MessageJoinReq
 *
 * DESCRIPTION: Message body of a join request
 */
typedef struct MessageJoinReq {
	MessageHdr messageheader; // message type, etc..
	Address nodeaddr; // address of the this node 
	long heartbeat; // hearrbeat of this node
}MessageJoinReq;


/**
 * STRUCT NAME: MessageJoinResp
 *
 * DESCRIPTION: Message body of a join request
 */
typedef struct MessageJoinResp {
	MessageHdr messageheader; // message type, etc..
	Address nodeaddr; // address of the node 
	long heartbeat; // hearrbeat of this node
	MemberListEntry memberList[NUM_MEMBERLIST_ENTRIES_COPY];
}MessageJoinResp;

/**
 * STRUCT NAME: MessageLeaveNotice
 *
 * DESCRIPTION: Message body of a leave notice
 */
typedef struct MessageLeaveNotice {
	MessageHdr messageheader; // message type, etc..
	Address nodeaddr; // address of the node that is welcomed
	MemberListEntry memberList[NUM_MEMBERLIST_ENTRIES_COPY];
}MessageLeaveNotice;

/**
 * STRUCT NAME: MessageMemberFailure
 *
 * DESCRIPTION: Message body of member failure 
 */
typedef struct MessageMemberFailure {
	MessageHdr messageheader; // message type, etc..
	Address nodeaddr; // address of the node that is welcomed
	//MemberListEntry memberList[NUM_MEMBERLIST_ENTRIES_COPY];
}MessageMemberFailure;

/**
 * STRUCT NAME: MessageHEARTBEAT
 *
 * DESCRIPTION: Message body of a HEARTBEAT
 */
typedef struct MessageHEARTBEAT {
	MessageHdr messageheader; // message type, etc..
	Address nodeaddr; // address of the node that is welcomed
	long heartbeat;
	MemberListEntry memberList[NUM_MEMBERLIST_ENTRIES_COPY];
}MessageHEARTBEAT;
/**
 * CLASS NAME: MP1Node
 *
 * DESCRIPTION: Class implementing Membership protocol functionalities for failure detection
 */
class MP1Node {
private:
	EmulNet *emulNet;
	Log *log;
	Params *par;
	Member *memberNode;
	char NULLADDR[6];
	bool booter;
    unordered_set<string> memberlist_set;
	vector<MemberListEntry> suspected_list;
	unordered_set<string> suspected_set;
	unordered_map<string, long> removednode_timestamp;
public:
	MP1Node(Member *, Params *, EmulNet *, Log *, Address *);
	Member * getMemberNode() {
		return memberNode;
	}
	int recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);
	void nodeStart(char *servaddrstr, short serverport);
	int initThisNode(Address *joinaddr);
	int introduceSelfToGroup(Address *joinAddress);
	int finishUpThisNode();
	void nodeLoop();
	void checkMessages();
	bool recvCallBack(void *env, char *data, int size);
	void nodeLoopOps();
	
	void add_node_to_memberlist(int id, short port, long heartbeat, long timestamp);
	void delete_node_from_memberlist(int id, short port);
	void handle_message_JOINREQ(void *env, char *data, int size);
	void handle_message_JOINREP(void *env, char *data, int size);
	void handle_message_LEAVENOTICE(void *env, char *data, int size);
	void handle_message_HEARTBEAT(void *env, char *data, int size);
	void handle_message_MEMBERFAILURE(void *env, char *data, int size);
	Address GetAddress(int id, short port);
	
	int isNullAddress(Address *addr);
	Address getJoinAddress();
	void initMemberListTable(Member *memberNode);
	void printAddress(Address *addr);
	virtual ~MP1Node();
};

#endif /* _MP1NODE_H_ */
