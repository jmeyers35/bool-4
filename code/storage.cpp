#include "gtstore.hpp"
#include <sys/socket.h>
#include <arpa/inet.h>
#include <string.h>
#include <pthread.h>
#include <assert.h>

static void *client_shutdown_handler(void *message_uncasted, bool *accepting_connections, void *state) {
    (void) message_uncasted;
    (void) state;
    *accepting_connections = false;
    message_t *return_message = (message_t *) malloc(sizeof(message_t));
    return_message->type = MESSAGE_SUCCESS;
    return_message->size = 0;
    return return_message;
}

static void *update_neighbors_handler(void *message_uncasted, bool *accepting_connections, void *state) {
	(void) accepting_connections;
	GTStoreStorage *storage_node = (GTStoreStorage*)state;
	message_t *message = (message_t*)message_uncasted;
	pair<pair<string, unsigned short>, pair<string, unsigned short>> new_neighbors = parse_neighbors(message->buffer);
#ifdef PRINT_TRACE
	cout << "Got neighbor update message for node " << storage_node->id << endl;
#endif
	storage_node->prev_neighbor = new_neighbors.first;
	storage_node->next_neighbor = new_neighbors.second;


	message_t *response = (message_t*) malloc(sizeof(message_t));
	response->type = MESSAGE_SUCCESS;
	response->size = 0;

	return response;
}

static void *print_state_handler(void *message_uncasted, bool *accepting_connections, void *state) {
	(void)message_uncasted;
	(void)accepting_connections;
	GTStoreStorage *storage = (GTStoreStorage*)state;
#ifdef PRINT_TRACE
	cout << "=================================" << endl;
#endif
#ifdef PRINT_TRACE
	cout << "Printing state for Storage Node with id " << storage->id << endl;
#endif
#ifdef PRINT_TRACE
	cout << "Previous Neighbor: Id " << storage->prev_neighbor.first << " Port " << storage->prev_neighbor.second << endl;
#endif
#ifdef PRINT_TRACE
	cout << "Id: " << storage->id << " Port " << storage->port << endl;
#endif
#ifdef PRINT_TRACE
	cout << "Next Neighbor: Id " << storage->next_neighbor.first << " Port " << storage->next_neighbor.second << endl;
#endif
#ifdef PRINT_TRACE
    cout << "Nodes Backing Up below:" << endl;
#endif
    for (auto node : storage->backing_up) {
#ifdef PRINT_TRACE
        cout << "Node id " << node.first << endl;
#endif
    }
#ifdef PRINT_TRACE
    cout << "Nodes Backed Up By below:" << endl;
#endif
    for (auto node : storage->backed_up_by) {
#ifdef PRINT_TRACE
        cout << "Node id " << node.first << endl;
#endif
    }
#ifdef PRINT_TRACE
    cout << "Cap Node: " << storage->cap_node_id << endl;
#endif
#ifdef PRINT_TRACE
    cout << "Key Value Store: " << endl;
#endif
    for (auto it = storage->key_val_store.begin(); it != storage->key_val_store.end(); it++) {
        string val = "[";
        for (auto item : it->second.first) {
            val += item + ",";
        }
        val += "]";
#ifdef PRINT_TRACE
        cout << "\t[" << it->first << ".v" << it->second.second << "]: " << val << endl;
#endif
    }
#ifdef PRINT_TRACE
	cout << "=================================" << endl;
#endif

	message_t *response = (message_t*) malloc(sizeof(message_t));
	response->type = MESSAGE_SUCCESS;
	response->size = 0;

	return response;
}

static string get_responsible_node(GTStoreStorage *storage, string key) {

    HashRing ring;

    ring.add_node(storage->id);
    if (storage->next_neighbor.first != "none") {
        ring.add_node(storage->next_neighbor.first);
    }
    if (storage->prev_neighbor.first != "none") {
        ring.add_node(storage->prev_neighbor.first);
    }

    // pair<Node *, Node *> neighbors = ring.get_neighbors(key);

    return ring.get_node(key);

    // if ((*(neighbors.second)).id == "none") {
        // return (*(neighbors.first)).id;
    // } else  {
        // return (*(neighbors.second)).id;
    // }
}

static bool is_replica_for(string id, string key, string cap_node_id) {

    HashRing ring;

    ring.add_node(id);
    if (cap_node_id != "none") {
        ring.add_node(cap_node_id);
    }

    return ring.get_node(key) == id;

}

static bool is_replica_for(GTStoreStorage *storage, string key) {

    return is_replica_for(storage->id, key, storage->cap_node_id);

    // HashRing ring;

    // ring.add_node(storage->id);
    // if (storage->cap_node_id != "none") {
        // ring.add_node(storage->cap_node_id);
    // }

    // return ring.get_node(key) == storage->id;

}


static void *storage_get_handler(void *message_uncasted, bool *accepting_connections, void *state) {
    (void) accepting_connections;
    GTStoreStorage *storage = (GTStoreStorage *)state;
    message_t *message = (message_t *) message_uncasted;
    string key = string(message->buffer);
    int version = atoi(message->buffer + key.length() + 1);
    pthread_mutex_lock(&storage->storage_node_lock);
    if (is_replica_for(storage, key)) {
        int num_retries = 2;
        for (int i = 0; i < num_retries; i++) {
            if (storage->key_val_store.find(key) != storage->key_val_store.end()) {
                pair<val_t, int> val = storage->key_val_store[message->buffer];
                if (version <= val.second) {
#ifdef PRINT_TRACE
                    cout << "Storage node " << storage->port << ": I have " << key << "... here you go" << endl;
#endif
                    pthread_mutex_unlock(&storage->storage_node_lock);
                    return create_get_response_message(val.first);
                }
            }
            // TODO make this a more reasonable time period
#ifdef PRINT_TRACE
            cout << "port " << storage->id << " " <<  key << " =================================================================================WHAT" << endl;
#endif
            pthread_mutex_unlock(&storage->storage_node_lock);
            sleep(1);
            pthread_mutex_lock(&storage->storage_node_lock);
        }
        string words = "Could not find key: " + string(message->buffer);
        message_t *return_message = (message_t *) malloc(sizeof(message_t) + words.length() + 1);
        return_message->type = MESSAGE_ERROR;
        return_message->size = words.length() + 1;
        strcpy(return_message->buffer, words.c_str());
        pthread_mutex_unlock(&storage->storage_node_lock);
        return return_message;
    } else {
        string responsible_node = get_responsible_node(storage, key);
        unsigned short port = responsible_node == storage->prev_neighbor.first ? storage->prev_neighbor.second : storage->next_neighbor.second;
#ifdef PRINT_TRACE
        cout << "Storage node: I am not responsible for " << key << "... asking my neghbor" << endl;
#endif
        pthread_mutex_unlock(&storage->storage_node_lock);
        return (message_t *) send_message(message_uncasted, "127.0.0.1", port);
    }

}

static void update_replicas(GTStoreStorage *storage, tuple<string, val_t, int> key_val_update) {
#ifdef PRINT_TRACE
    cout << "Submitting replica update request for " << get<0>(key_val_update) << endl;
#endif
    pthread_mutex_lock(&storage->replication_queue_lock);
#ifdef PRINT_TRACE
    cout << "got lock " << get<0>(key_val_update) << endl;
#endif
    storage->replication_queue.push(key_val_update);
    pthread_mutex_unlock(&storage->replication_queue_lock);
    pthread_cond_signal(&storage->replication_requests_available);
}

static void *key_val_update_handler(void *message_uncasted, bool *accepting_connections, void *state, bool recursive) {
    (void) accepting_connections;
    GTStoreStorage *storage = (GTStoreStorage *)state;
    message_t *message = (message_t *) message_uncasted;
    val_t value;
    string key;
    int version;
    parse_key_value(message->buffer, key, value, version);

    pthread_mutex_lock(&storage->storage_node_lock);
    if (is_replica_for(storage, key)) {
#ifdef PRINT_TRACE
        cout << "Storage node: PUT/UPDATE I am a replica for " << key << "... checking if I have it" << endl;
#endif
        bool version_is_newer = false;
        if (storage->key_val_store.find(key) != storage->key_val_store.end()) {
            pair<val_t, int> val = storage->key_val_store[message->buffer];
            if (version > val.second) {
                version_is_newer = true;
            }
        } else {
            version_is_newer = true;
        }
        if (version_is_newer) {
            storage->key_val_store[key] = make_pair(value, version);
            if (recursive) {
                update_replicas(storage, make_tuple(key, value, version));
            }
        }
        message_t *return_message = (message_t *) malloc(sizeof(message_t));
        return_message->type = MESSAGE_SUCCESS;
        return_message->size = 0;
        pthread_mutex_unlock(&storage->storage_node_lock);
        return return_message;
    } else {
#ifdef PRINT_TRACE
        cout << "Storage node: I am not responsible for " << key << "... asking my neghbor" << endl;
#endif
        string responsible_node = get_responsible_node(storage, key);
        unsigned short port = responsible_node == storage->prev_neighbor.first ? storage->prev_neighbor.second : storage->next_neighbor.second;
        pthread_mutex_unlock(&storage->storage_node_lock);
        return (message_t *) send_message(message_uncasted, "127.0.0.1", port);
    }
}

static void *storage_put_handler(void *message_uncasted, bool *accepting_connections, void *state) {
    return key_val_update_handler(message_uncasted, accepting_connections, state, true);
}

static void *storage_update_handler(void *message_uncasted, bool *accepting_connections, void *state) {
#ifdef PRINT_TRACE
    cout << "Updating!" << endl;
#endif
    return key_val_update_handler(message_uncasted, accepting_connections, state, false);
}

static void *update_backed_up_by_handler(void *message_uncasted, bool *accepting_connections, void *state) {
    (void) accepting_connections;
#ifdef PRINT_TRACE
    cout << "unpacking nodes" << endl;
#endif
    message_t *message = (message_t*) message_uncasted;
    GTStoreStorage *storage = (GTStoreStorage*)state;

    vector<pair<string, unsigned short>> new_backed_up_by = unpack_nodes(message->buffer);
#ifdef PRINT_TRACE
    cout << "unpacked nodes" << endl;
#endif
    pthread_mutex_lock(&storage->storage_node_lock);
    storage->backed_up_by = new_backed_up_by;
    pthread_mutex_unlock(&storage->storage_node_lock);

    message_t *return_message = (message_t*) malloc(sizeof(message_t));
    return_message->type = MESSAGE_SUCCESS;
    return_message->size = 0;
    return return_message;
}

static void *update_backing_up_handler(void *message_uncasted, bool *accepting_connections, void *state) {
    (void) accepting_connections;
    message_t *message = (message_t*) message_uncasted;
    GTStoreStorage *storage = (GTStoreStorage*)state;

    vector<pair<string, unsigned short>> new_backing_up = unpack_nodes(message->buffer);
    pthread_mutex_lock(&storage->storage_node_lock);
    storage->backing_up = new_backing_up;
    pthread_mutex_unlock(&storage->storage_node_lock);

    message_t *return_message = (message_t*) malloc(sizeof(message_t));
    return_message->type = MESSAGE_SUCCESS;
    return_message->size = 0;
    return return_message;
}

static void *update_cap_node_handler(void *message_uncasted, bool *accepting_connections, void *state) {
#ifdef PRINT_TRACE
    cout << "GOT A CAP UPDATE OKAY" << endl;
#endif
    (void) accepting_connections;
    message_t *message = (message_t*) message_uncasted;
    GTStoreStorage *storage = (GTStoreStorage*)state;
    pthread_mutex_lock(&storage->storage_node_lock);
    string new_cap = string(message->buffer);
    storage->cap_node_id = new_cap;
    pthread_mutex_unlock(&storage->storage_node_lock);

    message_t *return_message = (message_t*) malloc(sizeof(message_t));
    return_message->type = MESSAGE_SUCCESS;
    return_message->size = 0;
    return return_message;
}

void *replication_handler(void *arg) {
    GTStoreStorage *storage = (GTStoreStorage *) arg;

    while (!storage->is_shutting_down) {
        // Lock queue
        pthread_mutex_lock(&storage->replication_queue_lock);
        // If empty, release it, wait for it to be non-empty, then lock again
        while (storage->replication_queue.empty()) {
            pthread_cond_wait(&storage->replication_requests_available, &storage->replication_queue_lock);
#ifdef PRINT_TRACE
            cout << "replicator " << storage->port << ": waking up!" << endl;
#endif
        }

        // Get all pending requests and put them into a local queue quickly so that puts can go add more things
        vector<tuple<string, val_t, int>> updated_keys;
        while (!storage->replication_queue.empty()) {
            updated_keys.push_back(storage->replication_queue.front());
            storage->replication_queue.pop();
#ifdef PRINT_TRACE
            cout << "replicator " << storage->port << ": getting thing out!" << endl;
#endif
        }

#ifdef PRINT_TRACE
        cout << "replicator " << storage->port << ": Unlocking queue lock!" << endl;
#endif

        // Free the lock on the replication queue
        pthread_mutex_unlock(&storage->replication_queue_lock);

#ifdef PRINT_TRACE
        cout << "replicator " << storage->port << ": Unlocked queue lock!" << endl;
#endif

        // Send update messages to all the replicas
        for (auto update_request : updated_keys) {
            vector<pair<string, unsigned short>> replicas;
            for (auto potential_replica : storage->backing_up) {
                if (is_replica_for(potential_replica.first, get<0>(update_request), storage->cap_node_id)) {
                    replicas.push_back(potential_replica);
                }
            }
            int backed_up_by_replicas = storage->backing_up.size() - replicas.size();
            assert(backed_up_by_replicas >= 0);
            for (int i = 0; i < backed_up_by_replicas; i++) {
                replicas.push_back(storage->backed_up_by[i]);
            }
            for (auto replica : replicas) {
                message_t *update_message = create_update_message(get<0>(update_request), get<1>(update_request), get<2>(update_request));
#ifdef PRINT_TRACE
                cout << "replicator " << storage->port << ": sending replica update to " << replica.second << " for " << get<0>(update_request) << endl;
#endif
                message_t *status_message = (message_t *) send_message(update_message, "127.0.0.1", replica.second);
#ifdef PRINT_TRACE
                cout << "replicator " << storage->port << ": sent replica update to " << replica.second << endl;
#endif
                free(status_message);
                free(update_message);
            }
        }
#ifdef PRINT_TRACE
        cout << "replicator " << storage->port << ": done updating " << endl;
#endif

    }

    return NULL;
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

void join_handshake(void *state) {
    GTStoreStorage *storage = (GTStoreStorage *) state;
	// ping the server, ask for id and neighbors
	message_t *message = create_join_message(storage->port);
	message_t *response = (message_t*)send_message(message, "127.0.0.1", MANAGER_PORT);

	if (response->type == MESSAGE_SUCCESS) {
		// Set id and neighbors
		string id = string(response->buffer);
		storage->id = id;
#ifdef PRINT_TRACE
		cout << "Storage: recieved id " << storage->id << " successfully" << endl;
#endif
		storage->hash = std::hash<string>{}(storage->id);

		pair<pair<string, short>, pair<string, short>> neighbors = parse_neighbors(response->buffer + id.length() + 1);
        // Seek past neighbors
        char *cursor = seek_past_n_strings(response->buffer, 5);
#ifdef PRINT_TRACE
        cout << "join============================ num backing up: " << string(cursor) << endl;
#endif
        vector<pair<string, unsigned short>> backing_up = unpack_nodes(cursor);

        cursor = seek_past_n_strings(cursor, atoi(cursor) * 2 + 1);

#ifdef PRINT_TRACE
        cout << "join============================ num backed up by: " << string(cursor) << endl;
#endif
        vector<pair<string, unsigned short>> backed_up_by = unpack_nodes(cursor);

        cursor = seek_past_n_strings(cursor, atoi(cursor) * 2 + 1);

        storage->cap_node_id = string(cursor);
#ifdef PRINT_TRACE
        cout << "Received a cap id upon joining: " << storage->cap_node_id << endl;
#endif

        storage->backed_up_by = backed_up_by;
        storage->backing_up = backing_up;

		storage->prev_neighbor = neighbors.first;
		storage->next_neighbor = neighbors.second;
	}

    free(response);
    free(message);
}

void GTStoreStorage::init() {

ohno:
    pthread_mutex_init(&this->storage_node_lock, NULL);

	int sockfd = socket(AF_INET, SOCK_STREAM, 0);
	struct sockaddr_in address;
	address.sin_family = AF_INET;
	address.sin_port = 0;
	address.sin_addr.s_addr = INADDR_ANY;
	bind(sockfd, (struct sockaddr*)&address, sizeof(address));
	socklen_t len = sizeof(address);
	getsockname(sockfd, (struct sockaddr*)&address, &len);
	
	this->port = address.sin_port;
    if (this->port < 1024 || sockfd < 0) {
        close(sockfd);
        goto ohno;
    }
	//close(sockfd);
	std::map<long, void *(*)(void *message, bool *accepting_connections, void *state)> message_handlers;

	// // ping the server, ask for id and neighbors
	// message_t *message = create_join_message(this->port);
	// message_t *response = (message_t*)send_message(message, "127.0.0.1", MANAGER_PORT);

	// if (response->type == MESSAGE_SUCCESS) {
		// // Set id and neighbors
		// string id = string(message->buffer);
		// this->id = id;
        // #ifdef PRINT_TRACE
		// cout << "Storage: recieved id " << this->id << " successfully" << endl;
        // #endif
		// this->hash = std::hash<string>{}(this->id);

		// pair<pair<string, short>, pair<string, short>> neighbors = parse_neighbors(response->buffer + id.length() + 1);

		// this->prev_neighbor = neighbors.first;
		// this->next_neighbor = neighbors.second;
	// }

	message_handlers[MESSAGE_UPDATE_NEIGHBORS] = update_neighbors_handler;
	message_handlers[MESSAGE_TEXT] = print_state_handler;
	message_handlers[MESSAGE_SHUTDOWN] = client_shutdown_handler;
	message_handlers[MESSAGE_GET] = storage_get_handler;
	message_handlers[MESSAGE_PUT] = storage_put_handler;
	message_handlers[MESSAGE_UPDATE] = storage_update_handler;
    message_handlers[MESSAGE_BACKED_UP_BY_UPDATE] = update_backed_up_by_handler;
    message_handlers[MESSAGE_BACKING_UP_UPDATE] = update_backing_up_handler;
    message_handlers[MESSAGE_CAP_UPDATE] = update_cap_node_handler;

    pthread_mutex_init(&this->replication_queue_lock, NULL);
    pthread_cond_init(&this->replication_requests_available, NULL);

    pthread_t replication_handler_thread;

    this->is_shutting_down = false;

    pthread_create(&replication_handler_thread, NULL, replication_handler, this);

	run_message_server(this->port, this, message_handlers, join_handshake, true);

    this->is_shutting_down = true;

    pthread_cond_signal(&this->replication_requests_available);

    pthread_join(replication_handler_thread, NULL);
#ifdef PRINT_TRACE
	cout << "Storage Server " << this->id << " shut down!" << endl;
#endif
}

#ifndef UNIT_TESTING
int main(void) {

	GTStoreStorage storage;
	storage.init();
	
}
#endif
