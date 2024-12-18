#include "gtstore.hpp"
#include <map>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <string.h>
#include <algorithm>
#include <pthread.h>

// buffer = num_vals\0val_0\0val_1\0...
static void parse_ports(char *encoded_ports, vector<int>& val) {
    // Get the length
    int len = atoi(encoded_ports);
#ifdef PRINT_TRACE
    cout << "Client: can talk to " << len << "nodes" << endl;
#endif
    encoded_ports += strlen(encoded_ports) + 1;
    // Get the values
    for (int i = 0; i < len; i++) {
        val.push_back(atoi(encoded_ports));
#ifdef PRINT_TRACE
        cout << "random port " << string(encoded_ports) << endl;
#endif
        encoded_ports += strlen(encoded_ports) + 1;
    }
}

static vector<int> get_known_storage_nodes(int manager_port) {
    message_t *message = (message_t *) malloc(sizeof(message_t) + 1);
    message->type = MESSAGE_CLIENT_STORAGE;
    message->size = 0;
    message_t *returned_message = (message_t *) send_message(message, "127.0.0.1", manager_port);
    vector<int> storage_node_ports;
    parse_ports(returned_message->buffer, storage_node_ports);
    free(returned_message);
    free(message);
    return storage_node_ports;
}

void GTStoreClient::init(int id) {
	
#ifdef PRINT_TRACE
	cout << "Inside GTStoreClient::init() for client " << id << "\n";
#endif
	this->client_id = id;
    this->manager_port = MANAGER_PORT;
    this->version = 0;
    this->known_storage_nodes = get_known_storage_nodes(MANAGER_PORT);
}

static int pick_random_node(vector<int> known_storage_nodes) {
    static int cursor = 0;
    int node = known_storage_nodes[cursor];
    cursor = (cursor + 1) % known_storage_nodes.size();
    return node;
}

val_t GTStoreClient::get(string key) {
	
#ifdef PRINT_TRACE
	cout << "Inside GTStoreClient::get() for client: " << client_id << " key: " << key << "\n";
#endif
	val_t value;
    message_t *get_message = create_get_message(key, this->version);
    int port = pick_random_node(this->known_storage_nodes);
    message_t *response = (message_t *) send_message(get_message, "127.0.0.1", port);
    if (response->type != MESSAGE_SUCCESS) {
        perror("Could not get value!");
    } else {
        parse_get_value(response->buffer, value);
    }
    free(get_message);
    free(response);
	return value;
}

bool GTStoreClient::put(string key, val_t value) {

	string print_value = "";
	for (uint i = 0; i < value.size(); i++) {
		print_value += value[i] + " ";
	}
#ifdef PRINT_TRACE
	cout << "Inside GTStoreClient::put() for client: " << client_id << " key: " << key << " value: " << print_value << "\n";
#endif
    this->version++;
    message_t *put_message = create_put_message(key, value, this->version);
    int port = pick_random_node(this->known_storage_nodes);
    message_t *response = (message_t *) send_message(put_message, "127.0.0.1", port);
    bool success = response->type == MESSAGE_SUCCESS;
    free(put_message);
    free(response);
	return success;
}

void GTStoreClient::finalize() {
	
#ifdef PRINT_TRACE
	cout << "Inside GTStoreClient::finalize() for client " << client_id << "\n";
#endif
}

/**
 * Sends a message and receives a message as a return value.
 *
 * Used https://www.geeksforgeeks.org/socket-programming-cc/ as reference.
 */
void *send_message(void *message_uncasted, string address, unsigned short port) {
    message_t *message = (message_t *) message_uncasted;
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd == -1) {
        perror("send_message:: Could not get sockfd!");
        return NULL;
    }
    struct sockaddr_in peer_address;
    peer_address.sin_family = AF_INET;
    peer_address.sin_port = htons(port);

    if (inet_pton(AF_INET, address.c_str(), &peer_address.sin_addr) == -1) {
        perror("send_message:: Invalid address!");
        close(sockfd);
        return NULL;
    }

    if (connect(sockfd, (struct sockaddr *) &peer_address, sizeof(peer_address)) == -1) {
        perror("send_message:: Could not connect to peer!");
#ifdef PRINT_TRACE
        cerr << "Peer: " << port << endl;
#endif
        close(sockfd);
        return NULL;
    }

    message_t *return_value = NULL;

#ifdef PRINT_TRACE
    cout << "\tsending message (" << message->type << ") to " << address << ":" << port << endl;
#endif

    // Not going to bother converting to network byte order because the target machine has the same byte order.
    if (send(sockfd, message, sizeof(message_t) + message->size, 0) == -1) {
        perror("send_message:: Could not send message!");
#ifdef PRINT_TRACE
        cerr << "Peer: " << port << endl;
#endif
    } else {
        // Get the return value
        long message_type = MESSAGE_INVALID;
        int message_size = -1;
        if (read(sockfd, &message_type, sizeof(long)) == -1 ||
            read(sockfd, &message_size, sizeof(int)) == -1 ||
            (return_value = (message_t *) malloc(message_size + sizeof(message_t))) == NULL ||
            read(sockfd, &return_value->buffer, message_size) == -1) {
            perror("send_message:: Could not read return message!");
#ifdef PRINT_TRACE
            cerr << "Peer: " << port << endl;
#endif
#ifdef PRINT_TRACE
            cerr << "Message type: " << message->type << endl;
#endif
            if (return_value) {
                free(return_value);
                return_value = NULL;
            }
        } else {
            return_value->type = message_type;
            return_value->size = message_size;
#ifdef PRINT_TRACE
            cout << "\treceived return value for (" << message->type << ") from " << address << ":" << port << endl;
#endif
        }
    }

    close(sockfd);
    return return_value;
}

typedef struct {
    int client_sockfd;
    int port;
    std::map<long, void *(*)(void *message, bool *accepting_connections, void *state)> *message_handlers;
    void *state;
    bool *accepting_connections;
} thread_arg_t;


void *general_message_handler(void *arg) {
    thread_arg_t *thread_args = (thread_arg_t *) arg;

    int client_sockfd = thread_args->client_sockfd;
    void *state = thread_args->state;
#ifdef PRINT_TRACE
    int port = thread_args->port;
#endif
    bool *accepting_connections = thread_args->accepting_connections;
    std::map<long, void *(*)(void *message, bool *accepting_connections, void *state)> *message_handlers = thread_args->message_handlers;

    free(thread_args);

    // read data and dispatch
    long message_type = MESSAGE_INVALID;
    int message_size = -1;
    message_t *message_buffer = NULL;
    if (read(client_sockfd, &message_type, sizeof(long)) == -1 ||
        read(client_sockfd, &message_size, sizeof(int)) == -1 ||
        (message_buffer = (message_t *) malloc(message_size + sizeof(message_t))) == NULL ||
        read(client_sockfd, &message_buffer->buffer, message_size) == -1) {
        perror("run_message_server:: Could not read message!");
        if (message_buffer) {
            free(message_buffer);
        }
    } else if (message_handlers->find(message_type) != message_handlers->end()) {
        message_buffer->type = message_type;
        message_buffer->size = message_size;
#ifdef PRINT_TRACE
        cout << "\t(" << port << ") accepted message of type " << message_buffer->type << endl;
#endif
        message_t *return_message = (message_t *) (*message_handlers).at(message_type)(message_buffer, accepting_connections, state);
        if (send(client_sockfd, return_message, sizeof(message_t) + return_message->size, 0) == -1) {
            perror("run_message_server:: Could not send return message!");
        }
        free(return_message);
        free(message_buffer);
    } else {
        perror("run_message_server:: Unknown message type!");
        message_buffer->type = message_type;
        message_buffer->size = message_size;
        message_t *return_message = (message_t *) message_handlers->at(MESSAGE_INVALID)(message_buffer, accepting_connections, state);
        if (send(client_sockfd, return_message, sizeof(message_t) + return_message->size, 0) == -1) {
            perror("run_message_server:: Could not send return message!");
        }
        free(return_message);
        free(message_buffer);
    }
    close(client_sockfd);

    return NULL;
}

bool run_message_server(unsigned short port, void *state,
                        std::map<long, void *(*)(void *message, bool *accepting_connections, void *state)> message_handlers,
                        void (*listen_callback)(void *state),
                        bool async_handlers) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    const int request_backlog_length = 1024;
    if (sockfd == -1) {
        perror("run_message_server:: Could not get sockfd!");
        return false;
    }

    int opt = 1;

    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt)) == -1) {
        perror("run_message_server:: Could not setsockopt");
        close(sockfd);
        return false;
    }

    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    if (bind(sockfd, (struct sockaddr *)&address, sizeof(address)) == -1) {
        perror("run_message_server:: Could not bind to socket!");
        close(sockfd);
        return false;
    }

    if (listen(sockfd, request_backlog_length) == -1) {
        perror("run_message_server:: Could not listen on socket!");
        close(sockfd);
        return false;
    }

    if (listen_callback) {
        listen_callback(state);
    }

    bool accepting_connections = true;

    size_t address_len = sizeof(address);

#ifdef PRINT_TRACE
    cout << "Successfully bound to port " << port << endl;
#endif

    while (accepting_connections) {
#ifdef PRINT_TRACE
        cout << "\t(" << port << ") waiting to accept next message" << endl;
#endif
        int client_sockfd = accept(sockfd, (struct sockaddr *)&address, (socklen_t *)&address_len);
#ifdef PRINT_TRACE
        cout << "\t(" << port << ") accepted message... beginning parse" << endl;
#endif
        if (client_sockfd == -1) {
            perror("run_message_server:: Could not accept connection!");
            accepting_connections = false;
        } else {
            thread_arg_t *thread_arg = (thread_arg_t *) malloc(sizeof(thread_arg_t));
            thread_arg->client_sockfd = client_sockfd;
            thread_arg->port = port;
            thread_arg->message_handlers = &message_handlers;
            thread_arg->state = state;
            thread_arg->accepting_connections = &accepting_connections;

            if (async_handlers) {
                pthread_t thread;
                pthread_create(&thread, NULL, general_message_handler, thread_arg);
                pthread_detach(thread);
            } else {
                general_message_handler(thread_arg);
            }
        }
    }

    close(sockfd);
    return true;
}

// buffer = num_vals\0val_0\0val_1\0...
void parse_get_value(char *encoded_val, val_t& val) {
    // Get the length
    int len = atoi(encoded_val);
    encoded_val += strlen(encoded_val) + 1;
    // Get the values
    for (int i = 0; i < len; i++) {
        val.push_back(string(encoded_val));
        encoded_val += strlen(encoded_val) + 1;
    }
}

// buffer = key\0version\0num_vals\0val_0\0val_1\0...
void parse_key_value(char *encoded_key_val, string& key, val_t& val, int &version) {
    // Get the key
    key = string(encoded_key_val);
    encoded_key_val += key.length() + 1;
    version = atoi(encoded_key_val);
    encoded_key_val += string(encoded_key_val).length() + 1;
    parse_get_value(encoded_key_val, val);
    // // Get the length
    // int len = atoi(encoded_key_val);
    // encoded_key_val += strlen(encoded_key_val) + 1;
    // // Get the values
    // for (int i = 0; i < len; i++) {
        // val.push_back(string(encoded_key_val));
        // encoded_key_val += strlen(encoded_key_val) + 1;
    // }
}

static void pack_value(val_t& val, char *buffer) {
    for (auto cur = val.begin(); cur != val.end(); cur++) {
        strcpy(buffer, (*cur).c_str());
        buffer += (*cur).length() + 1;
    }
}

// key\0version\0length\0val
static message_t *create_put_like_message(string key, val_t &val, int message_type, int version) {
    int len = key.length() + 1;
    for (auto cur = val.begin(); cur != val.end(); cur++) {
        len += (*cur).length() + 1;
    }
    string length_string = to_string(val.size());
    string version_string = to_string(version);
    len += length_string.length() + 1;
    len += version_string.length() + 1;
    message_t *message = (message_t *) malloc(sizeof(message_t) + len);
    strcpy(message->buffer, key.c_str());
    strcpy(message->buffer + key.length() + 1, version_string.c_str());
    strcpy(message->buffer + key.length() + version_string.length() + 2, length_string.c_str());
    pack_value(val, message->buffer + key.length() + length_string.length() + version_string.length() + 3);
    message->type = message_type;
    message->size = len;
    return message;
}

message_t *create_put_message(string key, val_t &val, int version) {
    return create_put_like_message(key, val, MESSAGE_PUT, version);
}

message_t *create_update_message(string key, val_t &val, int version) {
    return create_put_like_message(key, val, MESSAGE_UPDATE, version);
}

message_t *create_get_message(string key, int version) {
    string version_string = to_string(version);
    int len = key.length() + 1 + version_string.length() + 1;
    message_t *message = (message_t *) malloc(sizeof(message_t) + len);
    strcpy(message->buffer, key.c_str());
    strcpy(message->buffer + key.length() + 1, version_string.c_str());
    message->type = MESSAGE_GET;
    message->size = len;
    return message;
}

message_t *create_get_response_message(val_t &val) {
    int len = 0;
    for (auto cur = val.begin(); cur != val.end(); cur++) {
        len += (*cur).length() + 1;
    }
    string length_string = to_string(val.size());
    len += length_string.length() + 1;
    message_t *return_message = (message_t *) malloc(sizeof(message_t) + len);
    strcpy(return_message->buffer, length_string.c_str());
    pack_value(val, return_message->buffer + length_string.length() + 1);
    return_message->type = MESSAGE_SUCCESS;
    return_message->size = len;
    return return_message;
}

message_t *create_join_message(int port) {
    string port_string = to_string(port);
    int len = port_string.length() + 1;
    message_t *message = (message_t*) malloc(sizeof(message_t) + len);
    strcpy(message->buffer, port_string.c_str());
    message->type = MESSAGE_JOIN;
    message->size = len;
    return message;
}

// *slaps tummy* It fit many loop brother
static char *seek_past_n_strings(char *cursor, int n) {
    for (int i = 0; i < n; i++) {
#ifdef PRINT_TRACE
        cout << "seeking past " << string(cursor) << endl;
#endif
        while(*cursor != '\0') cursor++;
        cursor++;
    }
    return cursor;
}

static message_t *append_nodes_to_message(message_t *message, int *total_len, vector<pair<string, unsigned short>> nodes) {
    string nodes_size = to_string(nodes.size());
    int offset = *total_len;
    *total_len += nodes_size.length() + 1;
    message = (message_t *) realloc(message, sizeof(message_t) + *total_len);
    strcpy(message->buffer + offset, nodes_size.c_str());
    offset = *total_len;
#ifdef PRINT_TRACE
    cout << "appending " << nodes.size() << endl;
#endif
    for (auto n : nodes) {
        int preoffset = offset;
        /*
        string id = n.first;
        *total_len += id.length() + 1;
        message = (message_t *) realloc(message, sizeof(message_t) + *total_len);
        strcpy(message->buffer + offset, id.c_str());
        offset = *total_len;
        string port = to_string(n.second);
        *total_len += port.length() + 1;
        message = (message_t *) realloc(message, sizeof(message_t) + *total_len);
        strcpy(message->buffer + offset, port.c_str());
        */

        string id = n.first;
        string port = to_string(n.second);
        string id_port = id + "Q" + port;

        *total_len += id_port.length() + 1;
        message = (message_t *) realloc(message, sizeof(message_t) + *total_len);
        strcpy(message->buffer + offset, id_port.c_str());
        message->buffer[offset + id.length()] = '\0';
        offset = *total_len;

#ifdef PRINT_TRACE
        cout << "packed " << id << " " << port << endl;
#endif
        seek_past_n_strings(message->buffer + preoffset, 2);
    }

    return message;
}

// buffer = id\0prev_nei_id\0prev_nei_port\0next_nei_id\0next_nei_port\0num_backing_up\0backing_up_nodes\0num_backed_up_by\0backed_up_by_nodes\0cap_node
message_t *create_join_response(string id, pair<string, unsigned short> prev_neighbor, pair<string, unsigned short> next_neighbor, 
            vector<pair<string, unsigned short>> backing_up, vector<pair<string, unsigned short>> backed_up_by, string cap_id) {
    int len = 0;
    int type = MESSAGE_SUCCESS;

    len = id.length() + 1;
    int total_len = len;
    total_len += prev_neighbor.first.length() + 1;
    total_len += to_string(prev_neighbor.second).length() + 1;
    total_len += next_neighbor.first.length() + 1;
    total_len += to_string(next_neighbor.second).length() + 1;

    message_t *message = (message_t*) malloc(sizeof(message_t) + total_len);
    strcpy(message->buffer, id.c_str());
    strcpy(message->buffer + len, prev_neighbor.first.c_str());
    len += prev_neighbor.first.length() + 1;
    strcpy(message->buffer + len, to_string(prev_neighbor.second).c_str());
    len += to_string(prev_neighbor.second).length() + 1;
    strcpy(message->buffer + len, next_neighbor.first.c_str());
    len += next_neighbor.first.length() + 1;
    strcpy(message->buffer + len, to_string(next_neighbor.second).c_str());
    len += to_string(next_neighbor.second).length() + 1;
#ifdef PRINT_TRACE
    cout << "packing backing_up nodes" << endl;
#endif
    message = append_nodes_to_message(message, &total_len, backing_up);
#ifdef PRINT_TRACE
    cout << "packing backed_up_by nodes" << endl;
#endif
    message = append_nodes_to_message(message, &total_len, backed_up_by);

    int offset = total_len;
    total_len += cap_id.length() + 1;
    message = (message_t *) realloc(message, sizeof(message_t) + total_len);
    strcpy(message->buffer + offset, cap_id.c_str());

    message->size = total_len;
    message->type = type;
    return message;
}
// buffer = prev_nei_id\0prev_nei_port\0next_nei_id\0next_nei_port
message_t *create_neighbor_update_message(pair<string, unsigned short> prev_neighbor, pair<string, unsigned short> next_neighbor) {
    if (prev_neighbor.first != to_string(prev_neighbor.second)) {
        //cout << "&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&" << prev_neighbor.first << " " << prev_neighbor.second << endl;
    }
    if (next_neighbor.first != to_string(next_neighbor.second)) {
        //cout << "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@" << next_neighbor.first << " " << next_neighbor.second << endl;
    }
    int len = 0;
    int type = MESSAGE_UPDATE_NEIGHBORS;
    len = prev_neighbor.first.length() + 1;
    int total_len = len;
    total_len += to_string(prev_neighbor.second).length() + 1;
    total_len += next_neighbor.first.length() + 1;
    total_len += to_string(next_neighbor.second).length() + 1;
    message_t *message = (message_t*) malloc(sizeof(message_t) + total_len);

    strcpy(message->buffer, prev_neighbor.first.c_str());
    //cout << "Prev ID " << message->buffer << endl;
    strcpy(message->buffer + len, to_string(prev_neighbor.second).c_str());
    //cout << "Prev Port " << message->buffer + len << endl;
    len += to_string(prev_neighbor.second).length() + 1;
    strcpy(message->buffer + len, next_neighbor.first.c_str());
    //cout << "Next ID " << message->buffer + len << endl;
    len += next_neighbor.first.length() + 1;
    strcpy(message->buffer + len, to_string(next_neighbor.second).c_str());
    //cout << "Next Port " << message->buffer + len << endl;
    len += to_string(next_neighbor.second).length() + 1;

    message->size = len;
    message->type = type;
    return message;
}

pair<pair<string, unsigned short>, pair<string, unsigned short>> parse_neighbors(char *buf) {
    string prev_neighbor_id = string(buf);
    buf += prev_neighbor_id.length() + 1;
    unsigned short prev_neighbor_port = atoi(buf);
    buf += to_string(prev_neighbor_port).length() + 1;
    string next_neighbor_id = string(buf);
    buf += next_neighbor_id.length() + 1;
    unsigned short next_neighbor_port = atoi(buf);

    //cout << "(( Prev " << prev_neighbor_id << ":" << prev_neighbor_port << " Next " << next_neighbor_id << ":" << next_neighbor_port << " ))" << endl;

    pair<string, unsigned short> prev_neighbor = pair<string, unsigned short>(prev_neighbor_id, prev_neighbor_port);
    pair<string, unsigned short> next_neighbor = pair<string, unsigned short>(next_neighbor_id, next_neighbor_port);

    return pair<pair<string, unsigned short>, pair<string, unsigned short>>(prev_neighbor, next_neighbor);
}

int parse_port(char *buf) {
    return atoi(buf);
}

// buffer = num_nodes\0node_1_id\0node_1_port...
message_t *create_duplicate_update_message(vector<pair<string, unsigned short>> nodes, int type) {
#ifdef PRINT_TRACE
    cout << "Creating duplicate update message" << endl;
#endif
    int len = 0;
    size_t num_nodes = nodes.size();

    len += to_string(num_nodes).length() + 1;

    message_t *message = (message_t*) malloc(sizeof(message_t) + len);

    strcpy(message->buffer, to_string(num_nodes).c_str());


    for (auto node : nodes) {
        int id_len = node.first.length() + 1;
        int port_len = to_string(node.second).length() + 1;
        message = (message_t*) realloc(message, sizeof(message_t) + len + id_len + port_len);
        strcpy(message->buffer + len, node.first.c_str());
        strcpy(message->buffer + len + id_len, to_string(node.second).c_str());
        len += id_len + port_len;
    }

    message->size = len;
    message->type = type;

#ifdef PRINT_TRACE
    cout << "Finished Creating duplicate update message" << endl;
#endif
    return message;
}

message_t *create_cap_node_update_message(string cap_id) {
    int len = cap_id.length() + 1;
    int type = MESSAGE_CAP_UPDATE;
    message_t *message = (message_t*)malloc(sizeof(message_t) + len);
    strcpy(message->buffer, cap_id.c_str());
    message->size = len;
    message->type = type;
    return message;
}

vector<pair<string, unsigned short>> unpack_nodes(char *buf) {
    int num_nodes = atoi(buf);
    buf += to_string(num_nodes).length() + 1;


    vector<pair<string, unsigned short>> nodes;

    for (auto it = 0; it < num_nodes; it++) {
        string node_id = string(buf);
        buf += node_id.length() + 1;
        unsigned short port = atoi(buf);
        buf += to_string(port).length() + 1;

        pair<string, unsigned short> node = pair<string, unsigned short>(node_id, port);
        nodes.push_back(node);
    }
    return nodes;
}

pair<Node*, Node*> HashRing::get_neighbors(string node_id) {
	vector<Node>::iterator it = std::find(this->nodes.begin(), this->nodes.end(), Node(node_id));
	size_t index = std::distance(this->nodes.begin(), it);

	if (this->nodes.size() < 3) {
		// Case 1: First Node added, no neighbors
		if (this->nodes.size() == 1) {
			return pair<Node*, Node*>(nullptr, nullptr);
		} else if (index == 1) {
            vector<Node>::iterator prev_neighbor = prev(it);
			// Case 2 - Second Node added, 1 neighbor: prev
			return pair<Node*, Node*>(&(*prev_neighbor), nullptr);
		} else {
            vector<Node>::iterator next_neighbor = next(it);
			// Case 3 - Second Node added, 1 neighbor: next
			return pair<Node*, Node*>(nullptr, &(*next_neighbor));
		}
	} else {
		if (index == 0) {
			// Case 4 - At least 3 nodes added, node is beginning of vector
            vector<Node>::iterator next_neighbor = next(it);
			return pair<Node*, Node*>(&this->nodes.back(), &(*next_neighbor));
		} else if (index == this->nodes.size() - 1) {
            vector<Node>::iterator prev_neighbor = prev(it);
			return pair<Node*, Node*>(&(*prev_neighbor), &this->nodes.front());
		} else {
            vector<Node>::iterator prev_neighbor = prev(it);
            vector<Node>::iterator next_neighbor = next(it);
			return pair<Node*, Node*>(&(*prev_neighbor), &(*next_neighbor));
		}
	}
}

void HashRing::add_node(string id) {
#ifdef PRINT_TRACE
	//cerr << "add_node: adding node with id " << id << endl;
#endif
    Node node = Node(id);
    // cout << "add_node: nodeid: " << node.id << endl;
    // cout << "add_node: len: " << this->nodes.size() << endl;
    this->nodes.push_back(node);
	// uses implemented < operator, so sorts by hash
    // cout << "add_node: sorting " << id << endl;
    std::sort(this->nodes.begin(), this->nodes.end());
#ifdef PRINT_TRACE
    //cerr << "add_node: nodes: ";
    for (auto x : this->nodes) {
        cout << x.id << "," ;
    }
    //cerr << endl;
#endif
#ifdef PRINT_TRACE
    //cerr << "add_node: added node with id " << id << endl;
#endif
}

// Determine which node is mapped to this key.
string HashRing::get_node(string key) {
	// more or less binary search
	// hackily just instantiate a new Node for the comparison
	vector<Node>::iterator it = std::lower_bound(this->nodes.begin(), this->nodes.end(), Node(key));
	if (it == this->nodes.end()) {
		return this->nodes[0].id;
	}
	return it->id;
}

// Retrieves UP TO k nodes past the passed-in node. It may be less if there are less than k nodes in the system
vector<Node> HashRing::get_k_forward_nodes(string node_id, unsigned int k) {
    vector<Node> nodes;
    if (this->nodes.size() == 1) {
        return nodes;
    }
    vector<Node>::iterator it = std::find(this->nodes.begin(), this->nodes.end(), Node(node_id));
    size_t index = std::distance(this->nodes.begin(), it);
    unsigned int len = 0;
    index = (index + 1) % this->nodes.size();
    unsigned int num_iterations = std::min((size_t)k, this->nodes.size() - 1);
    while (len < num_iterations && this->nodes[index].id != node_id) {
        nodes.push_back(this->nodes[index]);
        len++;
        index = (index + 1) % this->nodes.size();
    }
#ifdef PRINT_TRACE
    cout << "Num forwards nodes found: " << nodes.size() << endl;
#endif
    return nodes;
}
// Retrieves UP TO k nodes before the passed-in node. It may be less if there are less than k nodes in the system
vector<Node> HashRing::get_k_backward_nodes(string node_id, unsigned int k) {
    vector<Node> nodes;
    if (this->nodes.size() == 1) {
        return nodes;
    }
    vector<Node>::iterator it = std::find(this->nodes.begin(), this->nodes.end(), Node(node_id));
    size_t index = std::distance(this->nodes.begin(), it);
#ifdef PRINT_TRACE
    cout << "initial index: " << index << endl;
#endif
    unsigned int len = 0;
    /*index = (index - 1) % this->nodes.size();
    index = index < 0 ? this->nodes.size() - 1 : index;*/
    index = index ? index - 1 : this->nodes.size() - 1;
    unsigned int num_iterations = std::min((size_t)k, this->nodes.size() - 1);
#ifdef PRINT_TRACE
    cout << "num_iterations " << num_iterations << endl;
#endif
    for (auto x : this->nodes) {
#ifdef PRINT_TRACE
        cout << x.id << ", " << endl;
#endif
    }
#ifdef PRINT_TRACE
    cout << "index " << index << endl;
#endif
    while (len < num_iterations && this->nodes[index].id != node_id) {
        nodes.push_back(this->nodes[index]);
        len++;
        index = index ? index - 1 : this->nodes.size() - 1;
    }
#ifdef PRINT_TRACE
    cout << "Num backwards nodes found: " << len << " (claimed) or (actual) " << nodes.size() << endl;
#endif
    return nodes;
}

string HashRing::get_cap_node_id(string node_id, unsigned int replication_factor) {
#ifdef PRINT_TRACE
    cout << "trying to get cap node id for node " << node_id << endl;
#endif
    if (replication_factor + 1 > this->nodes.size()) {
        return "none";
    }
#ifdef PRINT_TRACE
    cout << "Replication factor: " << replication_factor << endl;
#endif
#ifdef PRINT_TRACE
    cout << "Number of nodes in the system: " << this->nodes.size() << endl;
#endif
    vector<Node> prev_nodes = get_k_backward_nodes(node_id, replication_factor);
    return prev_nodes.back().id; 
}
