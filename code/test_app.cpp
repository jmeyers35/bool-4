#include "gtstore.hpp"
#include <map>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>
#include <sys/time.h>

#ifdef UNIT_TESTING

void spawn_n_storage_nodes(int n) {
    for (int i = 0; i < n; i++) {
        pid_t pid1 = fork();
        if (!pid1) {
            GTStoreStorage storage;
            storage.init();
            exit(0);
        }
        // sleep(2);
    }
}

bool test_message_passing_small_message_correct = false;

void *check_message(void *message, bool *accepting_connections, void *state) {
    (void) accepting_connections;
    (void) state;
    string words = "incorrect!";
    message_t *return_message = (message_t *) malloc(sizeof(message_t) + words.length() + 1);
    return_message->type = 1;
    if(strcmp(((message_t *)message)->buffer, "Hello world!")) {
#ifdef PRINT_TRACE
        cout << "Wrong message! " << string(((message_t *)message)->buffer) << endl;
#endif
#ifdef PRINT_TRACE
        cout << FAIL;
#endif
    } else {
        words = "correct!";
    }
    strcpy(return_message->buffer, words.c_str());
    return_message->size = words.length() + 1;
    return return_message;
}

void *shutdown_server(void *message_uncasted, bool *accepting_connections, void *state) {
    (void) state;
    message_t *message = (message_t *) message_uncasted;
    // Setting this flag is the functionality we care about
    *accepting_connections = false;
    if (message->type != 1 || message->size != 1 + strlen("correct!") || strcmp(message->buffer, "correct!")) {
#ifdef PRINT_TRACE
        cout << "Message 2 was wrong!" << endl;
#endif
#ifdef PRINT_TRACE
        cout << message->buffer << endl;
#endif
#ifdef PRINT_TRACE
        cout << message->type << endl;
#endif
#ifdef PRINT_TRACE
        cout << message->size << endl;
#endif
#ifdef PRINT_TRACE
        cout << FAIL;
#endif
    } else {
        test_message_passing_small_message_correct = true;
    }
    string words = "never checked!";
    message_t *return_message = (message_t *) malloc(sizeof(message_t) + words.length() + 1);
    return_message->type = 1;
    return_message->size = words.length() + 1;
    strcpy(return_message->buffer, words.c_str());
    return return_message;
}

void test_message_passing_small_message(void) {
    pid_t pid = fork();
    if (pid) {
        std::map<long, void *(*)(void *message, bool *accepting_connections, void *state)> message_handlers;
        message_handlers[0] = check_message;
        message_handlers[1] = shutdown_server;
        run_message_server(MANAGER_PORT, NULL, message_handlers, NULL, false);
        if (test_message_passing_small_message_correct) {
#ifdef PRINT_TRACE
            cout << PASS;
#endif
        }
        exit(0);
    } else {
        string words = "Hello world!";
        message_t *message = (message_t *) malloc(sizeof(message_t) + words.length() + 1);
        message->type = 0;
        message->size = words.length() + 1;
        strcpy(message->buffer, words.c_str());
        sleep(1);
        message_t *returned_message = (message_t *) send_message(message, "127.0.0.1", MANAGER_PORT);
        free(message);
        if (returned_message->type != 1 || returned_message->size != strlen("correct!") + 1 || strcmp(returned_message->buffer, "correct!")) {
            free(returned_message);
#ifdef PRINT_TRACE
            cout << "Returned message was wrong!" << endl;
#endif
#ifdef PRINT_TRACE
            cout << returned_message->buffer << endl;
#endif
#ifdef PRINT_TRACE
            cout << returned_message->type << endl;
#endif
#ifdef PRINT_TRACE
            cout << returned_message->size << endl;
#endif
#ifdef PRINT_TRACE
            cout << FAIL;
#endif
            return;
        }
        // Sending this message is just making sure that the server shuts off correctly
        message_t *returned_message_2 = (message_t *) send_message(returned_message, "127.0.0.1", MANAGER_PORT);
        free(returned_message);
        free(returned_message_2);
    }
}

void test_basic_put_get(void) {
    pid_t pid = fork();
    if (pid) {
        GTStoreManager manager;
        manager.init(0);
        exit(0);
    } else {
        sleep(1);
        val_t expected_cart;
        expected_cart.push_back("50 gallon drum");
        expected_cart.push_back("knives");
        expected_cart.push_back("latex mask");
        message_t *message = create_put_message("bob's cart", expected_cart, 1);
        message_t *returned_message = (message_t *) send_message(message, "127.0.0.1", MANAGER_PORT);
        bool pass = true;
        if (returned_message->type != MESSAGE_SUCCESS) {
#ifdef PRINT_TRACE
            cout << "Returned message was wrong!" << endl;
#endif
#ifdef PRINT_TRACE
            cout << returned_message->buffer << endl;
#endif
#ifdef PRINT_TRACE
            cout << returned_message->type << endl;
#endif
#ifdef PRINT_TRACE
            cout << returned_message->size << endl;
#endif
#ifdef PRINT_TRACE
            cout << FAIL;
#endif
            pass = false;
        }
        free(returned_message);
        free(message);

        message = create_get_message("bob's cart", 1);
        returned_message = (message_t *) send_message(message, "127.0.0.1", MANAGER_PORT);
        val_t retrieved_cart;
        parse_get_value(returned_message->buffer, retrieved_cart);
        if (returned_message->type != MESSAGE_SUCCESS || retrieved_cart != expected_cart) {
#ifdef PRINT_TRACE
            cout << "Returned message was wrong!" << endl;
#endif
#ifdef PRINT_TRACE
            cout << returned_message->buffer << endl;
#endif
#ifdef PRINT_TRACE
            cout << returned_message->type << endl;
#endif
#ifdef PRINT_TRACE
            cout << returned_message->size << endl;
#endif
#ifdef PRINT_TRACE
            cout << FAIL;
#endif
            pass = false;
            return;
        }
        free(message);
        free(returned_message);
        // Sending this message is just making sure that the server shuts off correctly
        message_t shutdown_server_message;
        shutdown_server_message.type = MESSAGE_SHUTDOWN,
        shutdown_server_message.size = 1;
        returned_message = (message_t *) send_message(&shutdown_server_message, "127.0.0.1", MANAGER_PORT);
        if (returned_message->type != MESSAGE_SUCCESS) {
#ifdef PRINT_TRACE
            cout << "Returned message was wrong!" << endl;
#endif
#ifdef PRINT_TRACE
            cout << returned_message->buffer << endl;
#endif
#ifdef PRINT_TRACE
            cout << returned_message->type << endl;
#endif
#ifdef PRINT_TRACE
            cout << returned_message->size << endl;
#endif
#ifdef PRINT_TRACE
            cout << FAIL;
#endif
            pass = false;
            return;
        }
        free(returned_message);
        if (pass) {
#ifdef PRINT_TRACE
            cout << PASS;
#endif
        }
    }
}

void test_basic_put_get_client(void) {
    pid_t pid = fork();
    if (pid) {
        GTStoreManager manager;
        manager.init(3);
        exit(0);
    } else {
        sleep(1);
        spawn_n_storage_nodes(7);
        sleep(2);
        vector<val_t> expected_carts;
        val_t expected_cart;
        expected_cart.push_back("50 gallon drum");
        expected_cart.push_back("knives");
        expected_cart.push_back("latex mask");
        expected_carts.push_back(expected_cart);
        expected_cart.push_back("cleaning supplies");
        expected_cart.push_back("tarps");
        expected_carts.push_back(expected_cart);
        GTStoreClient bob;
        bob.init(55);
        vector<string> test_keys;
        test_keys.push_back("bob's cart");
        test_keys.push_back("sue's cart");
        test_keys.push_back("mary's cart");
        test_keys.push_back("tom's cart");
        test_keys.push_back("avagadro's cart");
        test_keys.push_back("ur mom's cart");
        test_keys.push_back("alfredo's cart");
        test_keys.push_back("pinata's cart");
        test_keys.push_back("jacob meyer's cart");
        test_keys.push_back("jim harris's cart");
        bool pass = true;
        for (unsigned int j = 0; j < expected_carts.size(); j++){
            for (unsigned int i = 0; i < test_keys.size(); i++) {
                bool status = bob.put(test_keys[i], expected_carts[j]);
                if (!status) {
#ifdef PRINT_TRACE
                    cout << "Could not put!" << endl;
#endif
#ifdef PRINT_TRACE
                    cout << FAIL;
#endif
                    pass = false;
                }

                val_t retrieved_cart = bob.get(test_keys[i]);

                if (retrieved_cart != expected_carts[j]) {
#ifdef PRINT_TRACE
                    cout << "Could not get cart!" << endl;
#endif
#ifdef PRINT_TRACE
                    cout << FAIL;
#endif
                    pass = false;
                }
            }
        }

#ifdef PRINT_TRACE
        cout << "Waiting for all updates to propagate before printing state" << endl;
#endif
        sleep(2);
        // Sending this message is just making sure that the server shuts off correctly
        message_t shutdown_server_message;
        shutdown_server_message.type = MESSAGE_TEXT;
        shutdown_server_message.size = 1;
        message_t *returned_message = (message_t *) send_message(&shutdown_server_message, "127.0.0.1", MANAGER_PORT);
        if (returned_message->type != MESSAGE_SUCCESS) {
#ifdef PRINT_TRACE
            cout << "Returned message was wrong!" << endl;
#endif
#ifdef PRINT_TRACE
            cout << returned_message->buffer << endl;
#endif
#ifdef PRINT_TRACE
            cout << returned_message->type << endl;
#endif
#ifdef PRINT_TRACE
            cout << returned_message->size << endl;
#endif
#ifdef PRINT_TRACE
            cout << FAIL;
#endif
            pass = false;
            return;
        }
        free(returned_message);
        shutdown_server_message.type = MESSAGE_SHUTDOWN;
        returned_message = (message_t *) send_message(&shutdown_server_message, "127.0.0.1", MANAGER_PORT);
        if (returned_message->type != MESSAGE_SUCCESS) {
#ifdef PRINT_TRACE
            cout << "Returned message was wrong!" << endl;
#endif
#ifdef PRINT_TRACE
            cout << returned_message->buffer << endl;
#endif
#ifdef PRINT_TRACE
            cout << returned_message->type << endl;
#endif
#ifdef PRINT_TRACE
            cout << returned_message->size << endl;
#endif
#ifdef PRINT_TRACE
            cout << FAIL;
#endif
            pass = false;
            return;
        }
        free(returned_message);
        if (pass) {
#ifdef PRINT_TRACE
            cout << PASS;
#endif
        }
    }
}

void* basic_client_thread(void *arg) {
    GTStoreClient bob;
    int id = *(int*)arg;
    free(arg);
    bob.init(id);
    string key = to_string(id);
    val_t val;
    val_t expected_val;
    struct timeval t0;
    struct timeval t1;
    long elapsed = 0;

    val.push_back("tendies");
    val.push_back("cookies");
    expected_val.push_back("tendies");
    expected_val.push_back("cookies");

    gettimeofday(&t0, 0);
    bool result = bob.put(key, val);
	gettimeofday(&t1, 0);
	elapsed = (t1.tv_sec-t0.tv_sec)*1000000 + t1.tv_usec-t0.tv_usec;
    cout << "PUT\t" << elapsed << endl;
    if (!result) {
#ifdef PRINT_TRACE
        cout << "Client " << id << " could not put!" << endl;
#endif
#ifdef PRINT_TRACE
        cout << FAIL;
#endif
        return NULL;
    }
    gettimeofday(&t0, 0);
    val_t actual_val = bob.get(key);
	gettimeofday(&t1, 0);
	elapsed = (t1.tv_sec-t0.tv_sec)*1000000 + t1.tv_usec-t0.tv_usec;
    cout << "GET\t" << elapsed << endl;

    if (actual_val != expected_val) {
#ifdef PRINT_TRACE
        cout << "Client " << id << " didn't get correct result!" << endl;
#endif
#ifdef PRINT_TRACE
        cout << FAIL;
#endif
        return NULL;
    }

    val.push_back("kanye west's hit album The College Dropout");
    expected_val.push_back("kanye west's hit album The College Dropout");
    gettimeofday(&t0, 0);
    result = bob.put(key, val);
	gettimeofday(&t1, 0);
	elapsed = (t1.tv_sec-t0.tv_sec)*1000000 + t1.tv_usec-t0.tv_usec;
    cout << "PUT\t" << elapsed << endl;
    if (!result) {
#ifdef PRINT_TRACE
        cout << "Client " << id << " could not put!" << endl;
#endif
#ifdef PRINT_TRACE
        cout << FAIL;
#endif
        return NULL;
    }
    gettimeofday(&t0, 0);
    actual_val = bob.get(key);
	gettimeofday(&t1, 0);
	elapsed = (t1.tv_sec-t0.tv_sec)*1000000 + t1.tv_usec-t0.tv_usec;
    cout << "GET\t" << elapsed << endl;

    if (actual_val != expected_val) {
#ifdef PRINT_TRACE
        cout << "Client " << id << " didn't get correct result!" << endl;
#endif
#ifdef PRINT_TRACE
        cout << FAIL;
#endif
        return NULL;
    }

    val.push_back("banana");
    expected_val.push_back("banana");
    gettimeofday(&t0, 0);
    result = bob.put(key, val);
	gettimeofday(&t1, 0);
	elapsed = (t1.tv_sec-t0.tv_sec)*1000000 + t1.tv_usec-t0.tv_usec;
    cout << "PUT\t" << elapsed << endl;
    if (!result) {
#ifdef PRINT_TRACE
        cout << "Client " << id << " could not put!" << endl;
#endif
#ifdef PRINT_TRACE
        cout << FAIL;
#endif
        return NULL;
    }
    gettimeofday(&t0, 0);
    actual_val = bob.get(key);
	gettimeofday(&t1, 0);
	elapsed = (t1.tv_sec-t0.tv_sec)*1000000 + t1.tv_usec-t0.tv_usec;
    cout << "GET\t" << elapsed << endl;

    if (actual_val != expected_val) {
#ifdef PRINT_TRACE
        cout << "Client " << id << " didn't get correct result!" << endl;
#endif
#ifdef PRINT_TRACE
        cout << FAIL;
#endif
        return NULL;
    }

#ifdef PRINT_TRACE
    cout << "Client " << id << ": " << PASS << endl;
#endif
    return NULL;
}

void test_concurrent_clients(int num_nodes, int num_clients, int replication_factor) {
    cout << "VALS\t" << num_nodes << "\t" << num_clients << "\t" << replication_factor << endl;
    pid_t pid = fork();
    if (pid) {
        GTStoreManager manager;
        manager.init(replication_factor);
        exit(0);
    } else {
        sleep(1);
        spawn_n_storage_nodes(num_nodes);
        sleep(5);

        pthread_t tids[num_clients];

        for(auto i = 0; i < num_clients; i++) {
            int *id = (int*)malloc(sizeof(int));
            *id = i;
            pthread_create(&tids[i], NULL, basic_client_thread, (void*)id);
        }
            for(auto i = 0; i < num_clients; i++) {
            pthread_join(tids[i], NULL);
        }
        sleep(3);
        message_t shutdown_server_message;
        shutdown_server_message.type = MESSAGE_SHUTDOWN;
        send_message(&shutdown_server_message, "127.0.0.1", MANAGER_PORT);
    }
}

void test_small_concurrent_clients() {
    test_concurrent_clients(10, 5, 4);
}

void test_BIG_concurrent_clients() {
    test_concurrent_clients(100, 50, 4);
}

void run_n_variance_experiments(void) {
    for (int n = 10; n <= 100; n += 5) {
        test_concurrent_clients(n, 50, 3);
    }
}

void run_c_variance_experiments(void) {
    for (int c = 10; c <= 100; c += 5) {
        test_concurrent_clients(20, c, 3);
    }
}

void run_k_variance_experiments(void) {
    for (int k = 1; k <= 4; k++) {
        test_concurrent_clients(20, 50, k);
    }
}

void test_basic_node_join() {
    // short node_port = 29170;
    pid_t pid = fork();
    GTStoreManager manager;
    GTStoreStorage storage;
    if(pid) {
        manager.init(0);
        exit(0);
    } else {
        pid_t pid1 = fork();
        if(pid1) {
            sleep(1);
            GTStoreStorage storage;
            storage.init();
            exit(0);
        } else {
            sleep(2);
            message_t print_node_state_message;
            print_node_state_message.type = MESSAGE_TEXT;
            print_node_state_message.size = 1;
            send_message(&print_node_state_message, "127.0.0.1", MANAGER_PORT);

            message_t shutdown_server_message;
            shutdown_server_message.type = MESSAGE_SHUTDOWN,
            shutdown_server_message.size = 1;
            send_message(&shutdown_server_message, "127.0.0.1", MANAGER_PORT);
            exit(0);
        }
        
    }
#ifdef PRINT_TRACE
    cout << "Test completed. Check printed state above for correctness!" << endl;
#endif
}

void add_nodes_test(int n) {
    pid_t pid = fork();
    if (!pid) {
        GTStoreManager manager;
        manager.init(4);
        exit(0);
    } else {
        sleep(1);
        spawn_n_storage_nodes(n);
        sleep(1);
        message_t print_node_state_message;
        print_node_state_message.type = MESSAGE_TEXT;
        print_node_state_message.size = 0;
        send_message(&print_node_state_message, "127.0.0.1", MANAGER_PORT);
        print_node_state_message.type = MESSAGE_SHUTDOWN;
#ifdef PRINT_TRACE
        cout << "Shutting down!" << endl;
#endif
        send_message(&print_node_state_message, "127.0.0.1", MANAGER_PORT);
    }
}

void test_add_second_node() {
    add_nodes_test(2);
}

void test_add_third_node() {
    add_nodes_test(3);
}

void test_add_seventh_node() {
    add_nodes_test(7);
}

void usage(string argv0, string error) {
#ifdef PRINT_TRACE
    cout << "ERROR:" << error << endl
         << "Usage:" << endl
         << "\t" << string(argv0) << " <test_case>" << endl;
#endif
    exit(1);
}

int main(int argc, char **argv) {
    std::map<std::string, void (*)(void)> test_case_names_to_functions;

    // test_case_names_to_functions.insert(pair<string, void (*)(void)>("single_set_get", single_set_get));
    test_case_names_to_functions["test_message_passing_small_message"] = test_message_passing_small_message;
    test_case_names_to_functions["test_basic_put_get"] = test_basic_put_get;
    test_case_names_to_functions["test_basic_put_get_client"] = test_basic_put_get_client;
    test_case_names_to_functions["test_basic_node_join"] = test_basic_node_join;
    test_case_names_to_functions["test_add_second_node"] = test_add_second_node;
    test_case_names_to_functions["test_add_third_node"] = test_add_third_node;
    test_case_names_to_functions["test_add_seventh_node"] = test_add_seventh_node;
    test_case_names_to_functions["test_small_concurrent_clients"] = test_small_concurrent_clients;
    test_case_names_to_functions["test_BIG_concurrent_clients"] = test_BIG_concurrent_clients;
    test_case_names_to_functions["run_n_variance_experiments"] = run_n_variance_experiments;
    test_case_names_to_functions["run_c_variance_experiments"] = run_c_variance_experiments;
    test_case_names_to_functions["run_k_variance_experiments"] = run_k_variance_experiments;

    if (argc != 2) {
        usage(string(argv[0]), "Missing test_case name!");
    }

    string test = string(argv[1]);

    if (test == "ALL") {
        for (auto& mapping : test_case_names_to_functions) {
#ifdef PRINT_TRACE
            cout << "========== RUNNING TEST CASE " << mapping.first << "==========" << endl;
#endif
            mapping.second();
        }
    } else if (test_case_names_to_functions.find(test) != test_case_names_to_functions.end()) {
        test_case_names_to_functions.at(test)();
    } else {
        string test_case_options = "ALL: Run all test cases";
        for (auto& mapping : test_case_names_to_functions) {
            test_case_options += "\n" + mapping.first;
        }
        usage(string(argv[0]), "Could not find test case! Options: \n" + test_case_options);
    }
}
#endif

#ifndef UNIT_TESTING
void single_set_get(int client_id) {
    cout << "Testing single set-get for GTStore by client " << client_id << ".\n";

    GTStoreClient client;
    client.init(client_id);
    
    string key = to_string(client_id);
    vector<string> value;
    value.push_back("phone");
    value.push_back("phone_case");
    
    client.put(key, value);
    val_t val = client.get(key);
    if (val != value) {
        cout << "Client " << client_id << " did not get expected cart!" << endl;
    } else {
        cout << "Client " << client_id << " successfully got expected cart!" << endl;
    }

    client.finalize();
}


int main(int argc, char **argv) {
    (void) argc;
    string test = string(argv[1]);
    int client_id = atoi(argv[2]);

    string test1 = "single_set_get";
    if (string(argv[1]) ==  test1) {
        single_set_get(client_id);
    }
}
#endif
