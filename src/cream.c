#include "cream.h"
#include "utils.h"
#include "csapp.c"
#include "queue.h"

hashmap_t *hashmap;
queue_t *queue;

void *worker_threads() {
    while(true) {
        int *cli_soct = dequeue(queue);
        int cli_soc = *cli_soct;
        request_header_t rht;
        if (rio_readn(cli_soc, &rht, sizeof(rht)) < 0) {
            printf("Recieve failed");
            exit(EXIT_FAILURE);
        }
        if (rht.request_code == PUT) {
            if (rht.key_size <= 0 || rht.value_size <= 0) {
                response_header_t rspt = {BAD_REQUEST, 0};
                if (rio_writen(cli_soc, &rspt, sizeof(rspt)) < 0) {
                    exit(EXIT_FAILURE);
                }
            }
            else {
                char* key = calloc(1, rht.key_size + 1);
                char* value = calloc(1, rht.value_size + 1);
                if (rio_readn(cli_soc, key, rht.key_size) < 0) {
                    printf("Recieve failed1");
                    exit(EXIT_FAILURE);
                }
                if (rio_readn(cli_soc, value, rht.value_size) < 0) {
                    printf("Recieve failed2");
                    exit(EXIT_FAILURE);
                }
                put(hashmap, MAP_KEY(key, rht.key_size), MAP_VAL(value, rht.value_size), true);
                response_header_t rspt = {OK, strlen(value)};
                if (rio_writen(cli_soc, &rspt, sizeof(rspt)) < 0) {
                    exit(EXIT_FAILURE);
                }
            }
        }
        else if (rht.request_code == GET) {
            if (rht.key_size <= 0) {
                response_header_t rspt = {BAD_REQUEST, 0};
                if (rio_writen(cli_soc, &rspt, sizeof(rspt)) < 0) {
                    exit(EXIT_FAILURE);
                }
            }
            else {
                char* key = calloc(1, rht.key_size + 1);
                if (rio_readn(cli_soc, key, rht.key_size) < 0) {
                    printf("Recieve failed1");
                    exit(EXIT_FAILURE);
                }
                map_val_t mvt = get(hashmap, MAP_KEY(key, rht.key_size));
                if (mvt.val_len == 0) {
                    response_header_t rspt = {NOT_FOUND, 0};
                    if (rio_writen(cli_soc, &rspt, sizeof(rspt)) < 0) {
                        exit(EXIT_FAILURE);
                    }
                }
                else {
                    response_header_t rspt = {OK, mvt.val_len};
                    if (rio_writen(cli_soc, &rspt, sizeof(rspt)) < 0) {
                        exit(EXIT_FAILURE);
                    }
                    if (rio_writen(cli_soc, mvt.val_base, mvt.val_len) < 0) {
                        exit(EXIT_FAILURE);
                    }
                }
            }
        }
        else if (rht.request_code == EVICT) {
            if (rht.key_size <= 0) {
                response_header_t rspt = {BAD_REQUEST, 0};
                if (rio_writen(cli_soc, &rspt, sizeof(rspt)) < 0) {
                    exit(EXIT_FAILURE);
                }
            }
            else {
                char* key = calloc(1, rht.key_size + 1);
                if (rio_readn(cli_soc, key, rht.key_size) < 0) {
                    printf("Recieve failed1");
                    exit(EXIT_FAILURE);
                }
                delete(hashmap, MAP_KEY(key, rht.key_size));
                response_header_t rspt = {OK, 0};
                    if (rio_writen(cli_soc, &rspt, sizeof(rspt)) < 0) {
                        exit(EXIT_FAILURE);
                }
            }
        }
        else if (rht.request_code == CLEAR) {
            clear_map(hashmap);
            response_header_t rspt = {OK, 0};
                if (rio_writen(cli_soc, &rspt, sizeof(rspt)) < 0) {
                    exit(EXIT_FAILURE);
            }
        }
        else {
            response_header_t rspt = {UNSUPPORTED, 0};
                if (rio_writen(cli_soc, &rspt, sizeof(rspt)) < 0) {
                    exit(EXIT_FAILURE);
            }
        }
    }
}

void destroy_fnc(map_key_t map_key, map_val_t map_val) {
    free(map_key.key_base);
    free(map_val.val_base);
}

int main(int argc, char *argv[]) {
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-h") == 0) {
            printf("./cream [-h] NUM_WORKERS PORT_NUMBER MAX_ENTRIES\n-h                 Displays this help menu and returns EXIT_SUCCESS.\nNUM_WORKERS        The number of worker threads used to service requests.\nPORT_NUMBER        Port number to listen on for incoming connections.\nMAX_ENTRIES        The maximum number of entries that can be stored in `cream`'s underlying data store.\n");
            exit(EXIT_SUCCESS);
        }
    }
    if (argc != 4) {
        printf("Invalid number of arguments\n");
        exit(EXIT_FAILURE);
    }
    int NUM_WORKERS = atoi(argv[1]);
    int PORT_NUMBER = atoi(argv[2]);
    int MAX_ENTRIES = atoi(argv[3]);

    hashmap = create_map(MAX_ENTRIES, jenkins_one_at_a_time_hash, destroy_fnc);
    queue = create_queue();
    pthread_t thread_ids[NUM_WORKERS];
    for(int index = 0; index < NUM_WORKERS; index++) {
        int *ptr = malloc(sizeof(int));
        *ptr = index;
        if(pthread_create(&thread_ids[index], NULL, worker_threads, ptr) != 0)
            exit(EXIT_FAILURE);
    }
    int cream_soc;
    if ((cream_soc = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        printf("Socket failed");
        exit(EXIT_FAILURE);
    }
    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT_NUMBER);
    if (bind(cream_soc, (struct sockaddr *)&address, sizeof(address)) < 0) {
        printf("Bind failed");
        exit(EXIT_FAILURE);
    }
    if (listen(cream_soc, 5) < 0) {
        printf("Listen failed");
        exit(EXIT_FAILURE);
    }
    while (true) {
        struct sockaddr_in cli_address;
        int cli_len = sizeof(cli_address);
        int cli_soc;
        if ((cli_soc = accept(cream_soc, (struct sockaddr *)&cli_address, (socklen_t*)&cli_len)) < 0) {
            printf("Accept failed");
            exit(EXIT_FAILURE);
        }
        enqueue(queue, &cli_soc);
    }
    for(int index = 0; index < NUM_WORKERS; index++) {
        pthread_join(thread_ids[index], NULL);
    }
    exit(EXIT_SUCCESS);
}
