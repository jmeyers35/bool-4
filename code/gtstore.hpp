#ifndef GTSTORE
#define GTSTORE

#include <string>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <vector>
#include <unistd.h>
#include <sys/wait.h>
#include <map>
#include <unordered_map>
#include <tuple>
#include <queue>

#define PASS "\033[32;1m PASS \033[0m\n"
#define FAIL "\033[31;1m FAIL \033[0m\n"

#define MESSAGE_INVALID -1
#define MESSAGE_ERROR 0
#define MESSAGE_GET 1
#define MESSAGE_PUT 2
#define MESSAGE_JOIN 3
#define MESSAGE_UPDATE 4
#define MESSAGE_TEXT 5
#define MESSAGE_SHUTDOWN 6
#define MESSAGE_SUCCESS 7
#define MESSAGE_CLIENT_STORAGE 8
#define MESSAGE_UPDATE_NEIGHBORS 9
#define MESSAGE_BACKING_UP_UPDATE 10
#define MESSAGE_BACKED_UP_BY_UPDATE 11

#define MESSAGE_CAP_UPDATE 69

#define MANAGER_PORT 9800

using namespace std;

typedef vector<string> val_t;

// Represents a storage node for consistent hashing purposes
class Node {
public:
	string id;
	size_t hash;

	Node(string id) {
		this->id = id;
		this->hash = std::hash<string>{}(this->id);
	}

	// Node(const Node& other) : id(other.id), hash(other.hash) {}

    // Node& operator=(const Node& other) {
        // this->id = other.id;
        // this->hash = other.hash;
        // return *this;
    // }

	bool operator < (const Node& other) {
		return this->hash < other.hash;
	}

	bool operator == (const Node& other) {
		return this->hash == other.hash;
	}
};

// Class to implement consistent hashing.
// Manages mappings of keys to nodes and adding / removing nodes
class HashRing {
public:
	vector<Node> nodes;
	string get_node(string key);
	void add_node(string node_id);
	pair<Node*, Node*> get_neighbors(string node_id);
	vector<Node> get_k_forward_nodes(string node_id, unsigned int k);
	vector<Node> get_k_backward_nodes(string node_id, unsigned int k);
	string get_cap_node_id(string node_id, unsigned int k);

    HashRing() {
        nodes.reserve(1024);
    }

    ~HashRing() {
        nodes.shrink_to_fit();
    }
};

class GTStoreClient {
private:
	int client_id;
	val_t value;
	int version = 0;
    vector<int> known_storage_nodes;
    int manager_port;
public:
	void init(int id);
	void finalize();
	val_t get(string key);
	bool put(string key, val_t value);
};

class GTStoreManager {
public:
	HashRing hash_ring;
	unordered_map<string, unsigned short> node_to_port;
	int replication_factor;
	string create_id(int port);
	void send_neighbor_update_message(string node_id);
	void send_backing_up_update_message(string affected_node, vector<Node> new_nodes);
	void send_backed_up_by_update_message(string affected_node, vector<Node> new_nodes);
	void send_cap_node_update(string affected_node, string cap_node);
	void init(int replication_factor);
};

class GTStoreStorage {
public:
	string id;
	int port;
	size_t hash;
	pair<string, unsigned short> prev_neighbor;
	pair<string, unsigned short> next_neighbor;
	unordered_map<string, pair<val_t, int>> key_val_store;
	unordered_map<string, val_t> duplicate_data;
	vector<pair<string, unsigned short>> backing_up;
	vector<pair<string, unsigned short>> backed_up_by;
	string cap_node_id;
	unordered_map<string, int> version;
    queue<tuple<string, val_t, int>> replication_queue;
    pthread_mutex_t replication_queue_lock;
    pthread_cond_t replication_requests_available;
    bool is_shutting_down;
	pthread_mutex_t storage_node_lock;
	void init();
};

typedef struct {
    long type;
    int size;
    char buffer[1];
} message_t;

// Protocol stuff
void *send_message(void *message_uncasted, string address, unsigned short port);

bool run_message_server(unsigned short port, void *state,
                        std::map<long, void *(*)(void *message, bool *accepting_connections, void *state)> message_handlers,
                        void (*listen_callback)(void *state),
                        bool async_handlers);

message_t *create_put_message(string key, val_t &val, int version);
message_t *create_update_message(string key, val_t &val, int version);
void parse_key_value(char *encoded_key_val, string& key, val_t& val, int &version);
message_t *create_get_response_message(val_t &val);
message_t *create_get_message(string key, int version);
message_t *create_join_message(int port);
message_t *create_join_response(string id, pair<string, unsigned short> prev_neighbor, pair<string, unsigned short> next_neighbor, 
	vector<pair<string, unsigned short>> backing_up,  vector<pair<string, unsigned short>> backed_up_by, string cap_id);
message_t *create_neighbor_update_message(pair<string, unsigned short> prev_neighbor, pair<string, unsigned short> next_neighbor);
message_t *create_duplicate_update_message(vector<pair<string, unsigned short>> nodes, int type);
message_t *create_cap_node_update_message(string cap_id);
vector<pair<string, unsigned short>> unpack_nodes(char *buf);
pair<pair<string, unsigned short>, pair<string, unsigned short>> parse_neighbors(char *buf);
void parse_get_value(char *encoded_val, val_t& val);
int parse_port(char *buf);
#endif
