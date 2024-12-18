#include <algorithm>
#include "gtstore.hpp"
#include <string.h>

static void *shutdown_system_handler(void *message_uncasted, bool *accepting_connections, void *state) {
    (void) message_uncasted;
    GTStoreManager *manager = (GTStoreManager*)state;

    message_t shutdown_message;
    shutdown_message.type = MESSAGE_SHUTDOWN;
    shutdown_message.size = 0;

    for (auto it = manager->node_to_port.begin(); it != manager->node_to_port.end(); it++) {
        void * return_message = send_message(&shutdown_message, "127.0.0.1", (*it).second);
        free(return_message);
    }

    *accepting_connections = false;

    message_t *return_message = (message_t *) malloc(sizeof(message_t));
    return_message->type = MESSAGE_SUCCESS;
    return_message->size = 0;
    return return_message;
}

static void *print_text_handler(void *message_uncasted, bool *accepting_connections, void *state) {
    (void) accepting_connections;
    (void) message_uncasted;
    GTStoreManager *manager = (GTStoreManager*)state;

#ifdef PRINT_TRACE
    cout << "=================================" << endl;
#endif
#ifdef PRINT_TRACE
	cout << "Printing state for Manager Node" << endl;
#endif
#ifdef PRINT_TRACE
	cout << "Hash Ring below" <<endl;
#endif
    if (!manager->hash_ring.nodes.size()) {
#ifdef PRINT_TRACE
        cout << "manager nodes is null!";
#endif
    }
    for (auto it = manager->hash_ring.nodes.begin(); it != manager->hash_ring.nodes.end(); it++) {
#ifdef PRINT_TRACE
        cout << "Node " << (*it).id <<  " with hash " << (*it).hash << " with port " << manager->node_to_port[(*it).id] << endl;
#endif
    }
#ifdef PRINT_TRACE
	cout << "=================================" << endl;
#endif

    message_t text_message;
    text_message.type = MESSAGE_TEXT;
    text_message.size = 0;

    sleep(1);

    for (auto it = manager->node_to_port.begin(); it != manager->node_to_port.end(); it++) {
        free(send_message(&text_message, "127.0.0.1", (*it).second));
    }

    message_t *return_message = (message_t *) malloc(sizeof(message_t));
    return_message->type = MESSAGE_SUCCESS;
    return_message->size = 0;
    return return_message;
}


static void *node_join_handler(void *message_uncasted, bool *accepting_connections, void *state) {
	(void) accepting_connections;
	GTStoreManager *manager = (GTStoreManager*) state;
	message_t *message = (message_t*) message_uncasted;
	int port = parse_port(message->buffer);
	string id = manager->create_id(port);
#ifdef PRINT_TRACE
	cout << "Manager: got node join request from port " << port << endl;
#endif
#ifdef PRINT_TRACE
	cout << "Assigned node id " << id << endl;
#endif
	manager->hash_ring.add_node(id);
	manager->node_to_port[id] = port;
#ifdef PRINT_TRACE
    cout << "Manager: Updating neighbors for node " << id << endl;
#endif
	pair<Node*, Node*> neighbors = manager->hash_ring.get_neighbors(id);
	Node* prev_neighbor = neighbors.first;
	Node* next_neighbor = neighbors.second;
	pair<string, unsigned short> prev_neighbor_pair;
	pair<string, unsigned short> next_neighbor_pair;
	if (prev_neighbor == nullptr) {
		prev_neighbor_pair = pair<string, unsigned short>("none", -1);
	} else {
		// Send update message to prev neighbor
#ifdef PRINT_TRACE
        cout << "Manager: Updating prev neighbor (" << neighbors.first->id << ") for node " << id << endl;
#endif
		prev_neighbor_pair = pair<string, unsigned short>(neighbors.first->id, manager->node_to_port[neighbors.first->id]);
		manager->send_neighbor_update_message(neighbors.first->id);
	}

	if (next_neighbor == nullptr) {
		next_neighbor_pair = pair<string, unsigned short>("none", -1);
	} else {
		// Send update message to next neighbor
#ifdef PRINT_TRACE
        cout << "Manager: Updating next neighbor (" << neighbors.second->id << ") for node " << id << endl;
#endif
		next_neighbor_pair = pair<string, unsigned short>(neighbors.second->id, manager->node_to_port[neighbors.second->id]);
		manager->send_neighbor_update_message(neighbors.second->id);
	}

    // Now we need to update duplicate information
    vector<Node> new_node_duplicates = manager->hash_ring.get_k_forward_nodes(id, manager->replication_factor - 1);

    // Send message to these nodes to update backing_up field
    for (auto node : new_node_duplicates) {
        if (node.id == id) {
#ifdef PRINT_TRACE
            cout << "Error! sending message to joining node! Deadlock imminent!" << endl;
#endif
        }
        manager->send_backing_up_update_message(node.id, manager->hash_ring.get_k_backward_nodes(node.id, manager->replication_factor - 1));
#ifdef PRINT_TRACE
        cout << "successfully sent backing_up message" << endl; 
#endif
    }
#ifdef PRINT_TRACE
    cout <<"finished sending all backing_up messages" << endl;
#endif

    // Send message to previous nodes to update backed_up_by field
    vector<Node> affected_nodes = manager->hash_ring.get_k_backward_nodes(id, manager->replication_factor - 1);
    for (auto node : affected_nodes) {
        if (node.id == id) {
#ifdef PRINT_TRACE
            cout << "Error! sending message to joining node! Deadlock imminent!" << endl;
#endif
        }
        manager->send_backed_up_by_update_message(node.id, manager->hash_ring.get_k_forward_nodes(node.id, manager->replication_factor - 1));
#ifdef PRINT_TRACE
        cout << "sent backed_up_by_update message successfully" << endl;
#endif
    }

    // Send cap node update messages
    vector<Node> caped_nodes = manager->hash_ring.get_k_forward_nodes(id, manager->replication_factor);
    for (auto node : caped_nodes) {
        string cap_id = manager->hash_ring.get_cap_node_id(node.id, manager->replication_factor);
        if (cap_id != "none") {
            manager->send_cap_node_update(node.id, cap_id);
        }
    }

#ifdef PRINT_TRACE
    cout << "Creating join response for node " << id << endl;
#endif
    //printf("(%s:%s) (current) (%s:%s)\n", prev_neighbor_pair.first.c_str(), to_string(prev_neighbor_pair.second).c_str(), next_neighbor_pair.first.c_str(), to_string(next_neighbor_pair.second).c_str());

    vector<pair<string, unsigned short>> backing_up, backed_up_by;

    for (auto node : affected_nodes) {
        backing_up.push_back(pair<string, unsigned short>(node.id, manager->node_to_port[node.id]));
    }

    for (auto node : new_node_duplicates) {
        backed_up_by.push_back(pair<string, unsigned short>(node.id, manager->node_to_port[node.id]));
    }

	message_t *response = create_join_response(id, prev_neighbor_pair, next_neighbor_pair, backing_up, backed_up_by, manager->hash_ring.get_cap_node_id(id, manager->replication_factor));
#ifdef PRINT_TRACE
    cout << "Created join response for node " << id << " successfully" << endl;
#endif
	return response;
}

void GTStoreManager::send_backing_up_update_message(string affected_node, vector<Node> new_nodes) {
#ifdef PRINT_TRACE
    cout << "creating backing up update message" << endl;
#endif
    vector<pair<string, unsigned short>> node_pairs;
    for (auto node : new_nodes) {
        node_pairs.push_back(pair<string, unsigned short>(node.id, this->node_to_port[node.id]));
    }
    message_t *message = create_duplicate_update_message(node_pairs, MESSAGE_BACKING_UP_UPDATE);
#ifdef PRINT_TRACE
    cout << "sending backing up update message" << endl;
#endif
    free(send_message(message, "127.0.0.1", this->node_to_port[affected_node]));
    free(message);
#ifdef PRINT_TRACE
    cout << "finished sending backing up update message" << endl;
#endif
}

void GTStoreManager::send_backed_up_by_update_message(string affected_node, vector<Node> new_nodes) {
#ifdef PRINT_TRACE
    cout << "creating backed up update message" << endl;
#endif
    vector<pair<string, unsigned short>> node_pairs;
    for (auto node : new_nodes) {
        node_pairs.push_back(pair<string, unsigned short>(node.id, this->node_to_port[node.id]));
    }
    message_t *message = create_duplicate_update_message(node_pairs, MESSAGE_BACKED_UP_BY_UPDATE);
#ifdef PRINT_TRACE
    cout << "sending backed up update message" << endl;
#endif
    free(send_message(message, "127.0.0.1", this->node_to_port[affected_node]));
    free(message);
#ifdef PRINT_TRACE
    cout << "finished sending backed up update message" << endl;
#endif
}

void GTStoreManager::send_cap_node_update(string affected_node, string cap_node) {
    message_t *message = create_cap_node_update_message(cap_node);
    free(send_message(message, "127.0.0.1", this->node_to_port[affected_node]));
    free(message);
}

static void *manager_invalid_handler(void *message_uncasted, bool *accepting_connections, void *state) {
    (void) accepting_connections;
    (void) state;
    message_t *message = (message_t *) message_uncasted;
    string words = "Invalid message type: " + message->type;
    message_t *return_message = (message_t *) malloc(sizeof(message_t) + words.length() + 1);
    return_message->type = MESSAGE_ERROR;
    return_message->size = words.length() + 1;
    strcpy(return_message->buffer, words.c_str());
    return return_message;
}

static void *manager_pick_random_storage_nodes(void *message_uncasted, bool *accepting_connections, void *state) {
    (void) accepting_connections;
    (void) message_uncasted;
    GTStoreManager *manager = (GTStoreManager *) state;
    // const char *manager_port_message = "1\09800";
	// unordered_map<string, unsigned short> node_to_port;
    vector<string> keys;
    for (auto kv: manager->node_to_port) {
        keys.push_back(kv.first);
    }

    std::random_shuffle(keys.begin(), keys.end());

    vector<string>::iterator it = keys.begin();

    int num_nodes = min(keys.size(), (size_t)4);
    string num_nodes_string = to_string(num_nodes);
    int len = 1 + num_nodes_string.length();

    for (auto i = 0; i < num_nodes; i++) {
        string port_string = to_string(manager->node_to_port[keys[i]]);
        len += 1 + port_string.length();
    }

    //auto selected_node = manager->node_to_port.begin();
    //unsigned short selected_port = (*selected_node).second;
    //string port_string = to_string(selected_port);
    //int len = 1 + 1 + port_string.length() + 1;
    message_t *return_message = (message_t *) malloc(sizeof(message_t) + len);
    return_message->type = MESSAGE_SUCCESS;
    return_message->size = len;
    strcpy(return_message->buffer, num_nodes_string.c_str());
    char *cursor = return_message->buffer + num_nodes_string.length() + 1;
    int cursor_len = 0;
    for (auto i = 0; i < num_nodes; i++) {
        unsigned short port = manager->node_to_port[*it];
        strcpy(cursor + cursor_len, to_string(port).c_str());
        cursor_len += to_string(port).length() + 1;
        it++;
    }
    return return_message;
}

void GTStoreManager::init(int replication_factor) {
    this->replication_factor = replication_factor;
    std::map<long, void *(*)(void *message, bool *accepting_connections, void *state)> message_handlers;
    message_handlers[MESSAGE_TEXT] = print_text_handler;
    message_handlers[MESSAGE_SHUTDOWN] = shutdown_system_handler;
    //message_handlers[MESSAGE_GET] = manager_temp_get_handler;
    //message_handlers[MESSAGE_PUT] = manager_temp_put_handler;
    message_handlers[MESSAGE_INVALID] = manager_invalid_handler;
    message_handlers[MESSAGE_CLIENT_STORAGE] = manager_pick_random_storage_nodes;
	message_handlers[MESSAGE_JOIN] = node_join_handler;
    run_message_server(MANAGER_PORT, this, message_handlers, NULL, false);
#ifdef PRINT_TRACE
    cout << "System shut down!" << endl;
#endif
}

string GTStoreManager::create_id(int port) {
	return to_string(port);
}

void GTStoreManager::send_neighbor_update_message(string node_id) {
#ifdef PRINT_TRACE
    cout << "Manager: Getting neighbor updates for " << node_id << endl;
#endif
	pair<Node*, Node*> neighbors = this->hash_ring.get_neighbors(node_id);
	Node* prev_neighbor = neighbors.first;
	Node* next_neighbor = neighbors.second;
	pair<string, unsigned short> prev_neighbor_pair;
	pair<string, unsigned short> next_neighbor_pair;
	if (prev_neighbor == nullptr) {
		prev_neighbor_pair = pair<string, unsigned short>("none", -1);
	} else {
		prev_neighbor_pair = pair<string, unsigned short>(neighbors.first->id, this->node_to_port[neighbors.first->id]);
	}

	if (next_neighbor == nullptr) {
		next_neighbor_pair = pair<string, unsigned short>("none", -1);
	} else {
		next_neighbor_pair = pair<string, unsigned short>(neighbors.second->id, this->node_to_port[neighbors.second->id]);
	}

#ifdef PRINT_TRACE
    cout << "Manager: Creating neighbor update message for " << node_id << endl;
#endif
	message_t *message = create_neighbor_update_message(prev_neighbor_pair, next_neighbor_pair);

#ifdef PRINT_TRACE
    cout << "Manager: Sending neighbor update message to " << node_id << endl;
#endif
	free(send_message(message, "127.0.0.1", this->node_to_port[node_id]));
    free(message);
}

#ifndef UNIT_TESTING
int main(void) {
    GTStoreManager manager;
	manager.init(3);
}
#endif
