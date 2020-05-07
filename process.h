#include <stdint.h>

#define MAX_SERVERS 5
#define MAX_MSG_LEN 200
#define MAX_MSG 1000
#define MAX_MSG_UNCOMMITTED 10
#define ROOT_PORT 20000
#define MULTICAST_PORT 29999
#define MULTICAST_IP "225.0.0.1"
#define HEARTBEAT_TIMEOUT 6
#define ELECTION_TIMEOUT_MAX 10
#define ELECTION_TIMEOUT_MIN 6

/*----- Message Types -----*/
enum State {
    LEADER=0,
    FOLLOWER,
    CANDIDATE
} State;

typedef struct Message {
    int message_id;
    char message_text[MAX_MSG_LEN];
} Message;

enum RaftMessageType {
    REQUEST_VOTE=0,
    CAST_VOTE,
    APPEND_ENTRIES,
    APPEND_ENTRIES_ACK,
    FORWARD_MSG
} RaftMessageType;

typedef struct RaftMessage {
    enum RaftMessageType type;
    int term;
    int server_id;
    int commit_index;
    Message message_log[MAX_MSG_UNCOMMITTED];
    Message message;
} RaftMessage;

// Init
int start_server(int port);
int bind_socket(int port, int type);
int bind_multicast_socket();

// Socket read
int read(int fd);
void read_from_proxy(char *buffer);
void read_from_peer(char *buffer);

// RAFT messages
int send_request_vote();
int send_cast_vote(int candidate_id);
int send_append_entries();
int send_append_entries_ack();
int forward_message(Message *msg);

// RAFT logic
void match_leader_log(RaftMessage *raftMsg);

// Timeout
void reset_election_timeout(bool new_duration);
void reset_heartbeat_timeout();

// UDP helpers
int sendto_leader(RaftMessage *raftMsg);
int sendto_multicast(RaftMessage *raftMsg);
