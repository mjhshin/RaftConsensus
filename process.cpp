#include <fstream>
#include <iostream>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/time.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <sstream>
#include <algorithm>
#include <iterator>
#include <vector>
#include <queue>
#include <map>
#include <netinet/in.h>
#include <netdb.h>
#include <signal.h>
#include <boost/asio.hpp>
#include <string>
#include <mutex>
#include <condition_variable>
#include "process.h"

using std::cout;
using std::endl;
using std::ofstream;
using std::string;
using std::vector;
using std::queue;
using std::map;

// Arg and socket vars
int proxy_fd;
int peers_unicast_fd;
int peers_multicast_fd;
int server_id;
int num_servers;
bool is_proxy_connected = false;

// Persistent state
enum State state = FOLLOWER;
int current_term = 0;
int leader_id = -1; // -1 means no leader
int votes = 0;

vector<Message> message_log;
queue<Message> follower_buffer;
map<int, int> follower_commit_index;

// Volatile state
int commit_index = -1;
//int last_applied = -1;

// Volatile state on leaders (reinitialized after election)
//int next_index[MAX_SERVERS]; // init to leader last log index + 1
//int match_index[MAX_SERVERS]; // init to 0

// Threading and timer stuff
boost::asio::io_service background_service;
boost::asio::deadline_timer election_timer(background_service);
boost::asio::deadline_timer heartbeat_timer(background_service);
int election_timeout_duration = 0;

std::mutex mtx;
std::condition_variable cv;
bool ready = true;

int errno;
ofstream output_file;

int main(int argc, char* argv[])
{
    srand(time(NULL));
    message_log.reserve(MAX_MSG);
    output_file.open("output.txt");
    background_service.run(); // Will return immediately
    
    // Minimal input error checking for now
    if(argc != 4) {
        output_file << "Error: argc != 4" << endl;
        exit(EXIT_FAILURE);
    }
    
    server_id = atoi(argv[1]);
    num_servers = atoi(argv[2]);
    int port = atoi(argv[3]);
    
    output_file << "start command server_id: " << server_id << endl;
    start_server(port);
    
    return 0;
}

// Loop where we continually listen for inputs to select()
int start_server(int port)
{
    fd_set active_fd_set, read_fd_set;
    struct sockaddr_in client_addr;
    size_t size;
    
    // Bind to proxy socket via TCP
    int init_proxy_fd = bind_socket(port, SOCK_STREAM);
    
    // Bind to socket for peers via UDP
    peers_unicast_fd = bind_socket(ROOT_PORT + server_id, SOCK_DGRAM);
    peers_multicast_fd = bind_multicast_socket();
    
    // Track max file descriptor
    int max_peers_fd = std::max(peers_unicast_fd, peers_multicast_fd);
    int max_fd = std::max(init_proxy_fd, max_peers_fd);
    
    if(listen(init_proxy_fd, 1) < 0) {
        output_file << "Error: listen" << endl;
        exit(EXIT_FAILURE);
    }
    
    // Init active set of sockets
    FD_ZERO(&active_fd_set);
    FD_SET(init_proxy_fd, &active_fd_set);
    FD_SET(peers_unicast_fd, &active_fd_set);
    FD_SET(peers_multicast_fd, &active_fd_set);
    
    while(1) {
        read_fd_set = active_fd_set;
        
        if(select(max_fd + 1, &read_fd_set, NULL, NULL, NULL) < 0) {
            output_file << "Error: select " << errno << endl;
            exit(EXIT_FAILURE);
        }
        
        // Loop through all file descriptors
        for(int i = 0; i <= max_fd; i++) {
            if(!FD_ISSET(i, &read_fd_set)) {
                continue;
            }
            
            if(i == init_proxy_fd) {
                // Connection request from proxy socket
                size = sizeof(client_addr);
                int new_sock_fd = accept(i, (struct sockaddr *)&client_addr, (socklen_t *)&size);
                
                if(new_sock_fd < 0) {
                    output_file << "Error: accept()" << endl;
                    exit(EXIT_FAILURE);
                }
                
                // Update max FD
                if(new_sock_fd > max_fd) {
                    max_fd = new_sock_fd;
                }
                
                proxy_fd = new_sock_fd;
                
                output_file << "connected to proxy: " << inet_ntoa(client_addr.sin_addr)
                    << " port " <<  ntohs(client_addr.sin_port) << endl;
                
                FD_SET(new_sock_fd, &active_fd_set);
                is_proxy_connected = true;
                reset_election_timeout(true);
            }
            else {
                std::unique_lock<std::mutex> lck(mtx);
                ready = false;
                
                // Critical section - Data arrived on connected socket
                if(read(i) < 0) {
                    close(i);
                    FD_CLR(i, &active_fd_set);
                }
                ready = true;
                cv.notify_all();
            }
        }
    }
    
    return 0;
}

// Create socket, configure it, bind to it.
int bind_socket(int port, int type)
{
    struct addrinfo hints, *res;
    int fd;
    int rv;
    
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;      // IPv4
    hints.ai_socktype = type;       // UDP or TCP
    hints.ai_flags = AI_PASSIVE;    // Fill in IP (localhost)
    
    char port_str[6];
    sprintf(port_str, "%d", port);
    
    if((rv = getaddrinfo(NULL, port_str, &hints, &res)) != 0) {
        output_file << "Error: getaddrinfo " << errno << endl;
        exit(EXIT_FAILURE);
    }
    
    if((fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol)) < 0) {
        output_file << "Error: socket " << errno << endl;
        exit(EXIT_FAILURE);
    }
    
    if(bind(fd, res->ai_addr, res->ai_addrlen) < 0) {
        output_file << "Error: bind " << errno << endl;
        exit(EXIT_FAILURE);
    }
    
    freeaddrinfo(res);
    
    return fd;
}

int bind_multicast_socket()
{
    int fd;
    int enable = 1;
    struct sockaddr_in addr;
    
    /* set up socket */
    if ((fd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        output_file << "Error: socket " << errno << endl;
        exit(EXIT_FAILURE);
    }

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(MULTICAST_PORT);
    
    // Reuse address
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)) < 0) {
        output_file << "Error: setsockopt(SO_REUSEADDR) " << errno << endl;
        exit(EXIT_FAILURE);
    }
    
    // Reuse port (redundant with SO_REUSEADDR on most systems)
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &enable, sizeof(int)) < 0) {
        output_file << "Error: setsockopt(SO_REUSEADDR) " << errno << endl;
        exit(EXIT_FAILURE);
    }
    
    // Receive own multicast messages
    if (setsockopt(fd, IPPROTO_IP, IP_MULTICAST_LOOP, &enable, sizeof(int)) < 0) {
        output_file << "Error: setsockopt(IP_MULTICAST_LOOP) " << errno << endl;
        exit(EXIT_FAILURE);
    }
    
    // TTL
    char ttl = 32;
    if (setsockopt(fd, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(char)) < 0) {
        output_file << "Error: setsockopt(IP_MULTICAST_TTL) " << errno << endl;
        exit(EXIT_FAILURE);
    }
    
    // Subscribe to multicast group
    struct ip_mreq mreq;
    mreq.imr_multiaddr.s_addr = inet_addr(MULTICAST_IP);
    mreq.imr_interface.s_addr = htonl(INADDR_ANY);
    
    if (setsockopt(fd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) < 0) {
        output_file << "Error: setsockopt(IP_ADD_MEMBERSHIP) " << errno << endl;
        exit(EXIT_FAILURE);
    }
    
    if (bind(fd, (struct sockaddr *) &addr, sizeof(addr)) < 0) {
        output_file << "Error: bind " << errno << endl;
        exit(EXIT_FAILURE);
    }
    
    return fd;
}

int read(int fd)
{
    char buffer[MAX_MSG_LEN];
    int num_bytes;
    
    if(fd == proxy_fd) {
        // Received via TCP
        num_bytes = (int)read(fd, buffer, MAX_MSG_LEN);
    }
    else {
        // Received via UDP
        struct sockaddr_in addr;
        socklen_t len = sizeof(addr);
        num_bytes = (int)recvfrom(fd, buffer, MAX_MSG_LEN, 0, (struct sockaddr *)&addr, &len);
    }
    
    if(num_bytes < 0) {
        output_file << "Error: read" << endl;
        exit(EXIT_FAILURE);
    }
    else if(num_bytes == 0) {
        output_file << "Received EOF" << endl;
        return -1;
    }
    else { // Good to go. Process data.
        if(fd == proxy_fd) {
            read_from_proxy(buffer);
        }
        else {
            read_from_peer(buffer);
        }
        return 0;
    }
}

// Process commands from proxy
void read_from_proxy(char *buffer)
{
    vector<string> args;
    char buffer_copy[MAX_MSG_LEN];
    strcpy(buffer_copy, buffer);
    char *token = strtok(buffer_copy, " ");
    
    while(token != NULL) {
        args.push_back(string(token));
        token = strtok(NULL, " \n");
    }
    
    if(args[0] == "msg") { // msg command
        output_file << "PROXY sent: " << buffer << endl;
        Message message = {};
        message.message_id = atoi(args[1].c_str());
        strcpy(message.message_text, args[2].c_str());
        
        if(state == LEADER) {
            message_log.push_back(message);
            send_append_entries();
        }
        else {
            if(leader_id < 0) {
                // No leader. Buffer msg until leader elected.
                follower_buffer.push(message);
            }
            else {
                forward_message(&message);
            }
        }
    }
    else if(args.size() == 2 && args[0] == "get" && args[1] == "chatLog") { // get chatLog command
        output_file << "get chatLog command" << endl;
        
        string chatlog = "chatLog ";
        
        for(int i = 0; i <= commit_index; i++) {
            chatlog.append(message_log[i].message_text);
            chatlog.append(",");
        }
        chatlog = chatlog.substr(0, chatlog.size() - 1); // remove trailing comma
        chatlog.append("\n");
        output_file << "chatLog: " << chatlog << endl;
        send(proxy_fd, chatlog.c_str(), chatlog.length(), 0);
    }
    else if(args.size() == 1 && args[0] == "crash") { // crash command
        output_file << "crash command" << endl;
        output_file.close();
        abort();
    }
    else {
        output_file << "unknown command" << endl;
        exit(EXIT_FAILURE);
    }
}

// Process data from peer servers
void read_from_peer(char *buffer)
{
    //output_file << "read_from_peer" << endl;
    RaftMessage *raftMsg = (RaftMessage *)buffer;
    
    if(raftMsg->type == FORWARD_MSG) {
        output_file << "--> receive FORWARD_MSG: " << raftMsg->message.message_text << endl;
        
        if(state == LEADER) {
            message_log.push_back(raftMsg->message);
            send_append_entries();
        }
    }
    else if(raftMsg->type == REQUEST_VOTE) {
        if(raftMsg->term >= current_term && raftMsg->server_id != server_id) { // Ignore if receiving own broadcast
            output_file << "--> receive REQUEST_VOTE - term: " << raftMsg->term << " server_id: "<< raftMsg->server_id << endl;
            if(leader_id < 0) {
                current_term = raftMsg->term;
                leader_id = raftMsg->server_id;
                send_cast_vote(leader_id);
            }
        }
    }
    else if(raftMsg->type == CAST_VOTE) {
        if(raftMsg->term >= current_term && raftMsg->server_id == server_id) {
            output_file << "--> receive CAST_VOTE - term: " << raftMsg->term << " server_id: "<< raftMsg->server_id << endl;
            votes++;
            
            if (votes > num_servers / 2) {
                // Become leader
                output_file << "state == LEADER" << endl << endl;
                state = LEADER;
                leader_id = server_id;
                follower_commit_index.clear();
                reset_heartbeat_timeout();
                send_append_entries();
            }
        }
    }
    else if(raftMsg->type == APPEND_ENTRIES) {
        if(raftMsg->term >= current_term && raftMsg->server_id != server_id) { // Ignore if receiving own broadcast
            output_file << "--> receive APPEND_ENTRIES - term: " << raftMsg->term << " server_id: "<< raftMsg->server_id << endl;
            
            if(raftMsg->term > current_term || (state == CANDIDATE && raftMsg->term == current_term)) {
                output_file << "state == FOLLOWER" << endl << endl;
                state = FOLLOWER;
                leader_id = raftMsg->server_id;
                match_leader_log(raftMsg);
                
                while(follower_buffer.size() > 0) {
                    Message msg = follower_buffer.front();
                    forward_message(&msg);
                    follower_buffer.pop();
                }
            }
            
            if(state == FOLLOWER) {
                reset_election_timeout(false);
                match_leader_log(raftMsg);
                send_append_entries_ack();
            }
        }
    }
    else if(raftMsg->type == APPEND_ENTRIES_ACK) {
        if(raftMsg->term == current_term) {
            output_file << "--> receive APPEND_ENTRIES_ACK - term: " << raftMsg->term << " server_id: "<< raftMsg->server_id << endl;
            
            follower_commit_index[raftMsg->server_id] = raftMsg->commit_index;
            
            while(true) {
                int commit_count = 0;
                
                map<int, int>::iterator it;
                for (it = follower_commit_index.begin(); it != follower_commit_index.end(); it++) {
                    if(it->second > commit_index) {
                        commit_count++;
                    }
                }
                
                if(commit_count > num_servers / 2) {
                    commit_index++;
                    output_file << "leader commit index: " << commit_index << endl;
                    
                    // Respond to proxy with new committed msg's
                    std::stringstream ss;
                    ss << "ack " << message_log[commit_index].message_id << " " <<  commit_index << endl;
                    string ack_str = ss.str();

                    send(proxy_fd, ack_str.c_str(), ack_str.length(), 0);
                }
                else {
                    break;
                }
            }
        }
    }
}

void election_timeout_handler(const boost::system::error_code&)
{
    std::unique_lock<std::mutex> lck(mtx);
    while (!ready) cv.wait(lck);

    output_file << "*** election timeout" << endl;
    
    if(state != LEADER) {
        // No need to increase term if timed out as a candidate
        if(state == FOLLOWER) {
            current_term++;
        }
        
        output_file << "state == CANDIDATE" << endl << endl;
        state = CANDIDATE;
        leader_id = server_id;
        votes = 1; // Vote for yourself
        reset_election_timeout(true);
        send_request_vote();
    }
}

void heartbeat_timeout_handler(const boost::system::error_code&)
{
    std::unique_lock<std::mutex> lck(mtx);
    while (!ready) cv.wait(lck);
    
    output_file << "*** heartbeat timeout" << endl;
    
    if(state == LEADER) {
        reset_heartbeat_timeout();
        send_append_entries();
    }
}

void reset_election_timeout(bool new_duration)
{
    if(new_duration) {
        int range = ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN + 1;
        election_timeout_duration = rand() % range + ELECTION_TIMEOUT_MIN;
    }
    //output_file << "reset_election_timeout: " << election_timeout_duration << endl;
    
    boost::posix_time::seconds interval(election_timeout_duration);
    election_timer.expires_from_now(interval);
    election_timer.async_wait(election_timeout_handler);
    
    std::thread([]() {
        if(background_service.stopped()) {
            background_service.restart();
            background_service.run();
        }
    }).detach();
}

void reset_heartbeat_timeout()
{
    output_file << "reset_heartbeat_timeout" << endl;
    
    boost::posix_time::seconds interval(HEARTBEAT_TIMEOUT);
    heartbeat_timer.expires_from_now(interval);
    heartbeat_timer.async_wait(heartbeat_timeout_handler);
    
    std::thread([]() {
        if(background_service.stopped()) {
            background_service.restart();
            background_service.run();
        }
    }).detach();
}

void match_leader_log(RaftMessage *raftMsg)
{
    while(commit_index <= raftMsg->commit_index) {
        message_log.push_back(raftMsg->message_log[std::max(0, commit_index)]);
        commit_index++;
    }
}

int send_request_vote()
{
    output_file << "send REQUEST_VOTE" << endl;
    
    RaftMessage raftMsg = {};
    raftMsg.type = REQUEST_VOTE;
    raftMsg.term = current_term;
    raftMsg.server_id = server_id;
    
    if(sendto_multicast(&raftMsg) < 0) {
        output_file << "Error: broadcast REQUEST_VOTE" << endl;
        return -1;
    }

    return 0;
}

int send_cast_vote(int candidate_id)
{
    output_file << "send CAST_VOTE: " << candidate_id << endl;
    
    RaftMessage raftMsg = {};
    raftMsg.type = CAST_VOTE;
    raftMsg.term = current_term;
    raftMsg.server_id = candidate_id;
    return sendto_leader(&raftMsg);
}

int send_append_entries()
{
    output_file << "send APPEND_ENTRIES" << endl;
    
    RaftMessage raftMsg = {};
    raftMsg.type = APPEND_ENTRIES;
    raftMsg.term = current_term;
    raftMsg.server_id = server_id;
    raftMsg.commit_index = commit_index;
    
    if(message_log.size() > MAX_MSG_UNCOMMITTED) {
        std::copy(message_log.begin() + MAX_MSG_UNCOMMITTED, message_log.end(), raftMsg.message_log);
    }
    else {
        std::copy(message_log.begin(), message_log.end(), raftMsg.message_log);
    }
    
    if(sendto_multicast(&raftMsg) < 0) {
        output_file << "Error: broadcast APPEND_ENTRIES" << endl;
        return -1;
    }
    
    reset_heartbeat_timeout();
    return 0;
}

int send_append_entries_ack()
{
    output_file << "send APPEND_ENTRIES_ACK" << endl;
    
    RaftMessage raftMsg = {};
    raftMsg.type = APPEND_ENTRIES_ACK;
    raftMsg.term = current_term;
    raftMsg.server_id = server_id;
    raftMsg.commit_index = commit_index;
    return sendto_leader(&raftMsg);
}

int forward_message(Message *msg)
{
    output_file << "send FORWARD_MSG" << endl;
    
    RaftMessage raftMsg = {};
    raftMsg.type = FORWARD_MSG;
    raftMsg.message = *msg;
    return sendto_leader(&raftMsg);
}

// Send message via UDP
int sendto_leader(RaftMessage *raftMsg)
{
    int rv;
    struct addrinfo hints, *res;
    
    int dest_port = ROOT_PORT + leader_id;
    char port_str[6];
    sprintf(port_str, "%d", dest_port);
    
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;
    
    if((rv = getaddrinfo(NULL, port_str, &hints, &res)) != 0) {
        output_file << "Error: getaddrinfo " << errno << endl;
        exit(EXIT_FAILURE);
    }
    
    int num_bytes = (int)sendto(peers_unicast_fd, raftMsg, sizeof(RaftMessage), 0,
                                res->ai_addr, res->ai_addrlen);
    if(num_bytes < 0) {
        output_file << "Error: sendto " << errno << endl;
        exit(EXIT_FAILURE);
    }
    
    freeaddrinfo(res);
    
    return num_bytes;
}

int sendto_multicast(RaftMessage *raftMsg)
{
    struct sockaddr_in addr;
    
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;                      // host byte order
    addr.sin_port = htons(MULTICAST_PORT);          // short, network byte order
    addr.sin_addr.s_addr = inet_addr(MULTICAST_IP);
    
    int num_bytes = (int)sendto(peers_multicast_fd, raftMsg, sizeof(RaftMessage), 0,
                                (struct sockaddr *)&addr, sizeof addr);
    //output_file << "broadcast num_bytes: " << num_bytes << endl;

    if(num_bytes < 0) {
        output_file << "Error: sendto " << errno << endl;
        exit(EXIT_FAILURE);
    }

    return num_bytes;
}
