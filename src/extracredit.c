#include "utils.h"
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#define MAP_KEY(base, len) (map_key_t) {.key_base = base, .key_len = len}
#define MAP_VAL(base, len) (map_val_t) {.val_base = base, .val_len = len}
#define MAP_NODE_D(key_arg, val_arg, tombstone_arg, created_arg) (map_node_t) {.key = key_arg, .val = val_arg, .tombstone = tombstone_arg, .created = created_arg}

hashmap_t *create_map(uint32_t capacity, hash_func_f hash_function, destructor_f destroy_function) {
    if (hash_function == NULL || destroy_function == NULL) {
        errno = EINVAL;
        return NULL;
    }
    hashmap_t *hmap = calloc(1, sizeof(*hmap));
    if (hmap == NULL) {
        return NULL;
    }
    hmap->invalid = false;
    hmap->capacity = capacity;
    hmap->nodes = calloc(capacity, sizeof(map_node_t));
    if (hmap->nodes == NULL) {
        return NULL;
    }
    hmap->hash_function = hash_function;
    hmap->destroy_function = destroy_function;
    if (pthread_mutex_init(&hmap->write_lock, NULL) != 0) {
        return NULL;
    }
    if (pthread_mutex_init(&hmap->fields_lock, NULL) != 0) {
        return NULL;
    }
    return hmap;
}

int get_noL(hashmap_t *self, map_key_t key) {
    uint32_t ind = self->hash_function(key) % self->capacity;
    bool end = false;
    for (uint32_t i = ind; i < self->capacity; i++) {
        if (self->nodes[i].key.key_base == NULL && !self->nodes[i].tombstone) {
            end = true;
        }
        if (end) {
            break;
        }
        if (self->nodes[i].key.key_base == NULL) {
        }
        else if (strcmp(((char*)self->nodes[i].key.key_base), ((char*)key.key_base)) == 0) {
            return i;
        }
    }
    for (uint32_t i = 0; i < ind; i++) {
        if (self->nodes[i].key.key_base == NULL && !self->nodes[i].tombstone) {
            end = true;
        }
        if (end) {
            break;
        }
        if (self->nodes[i].key.key_base == NULL) {
        }
        else if (strcmp(((char*)self->nodes[i].key.key_base), ((char*)key.key_base)) == 0) {
            return i;
        }
    }
    return -1;
}

bool put(hashmap_t *self, map_key_t key, map_val_t val, bool force) {
    if (self->invalid != 0 || key.key_base == NULL || key.key_len == 0 || val.val_base == NULL || val.val_len == 0) {
        errno = EINVAL;
        return false;
    }
    uint32_t ind = self->hash_function(key) % self->capacity;
    pthread_mutex_lock(&self->write_lock);
    if (get_noL(self, key) == -1) {
        if (self->size < self->capacity) {
            while (ind < self->capacity) {
                if (self->nodes[ind].val.val_len == 0) {
                    self->nodes[ind].key = key;
                    self->nodes[ind].val = val;
                    self->nodes[ind].created = time(0);
                    self->size++;
                    pthread_mutex_unlock(&self->write_lock);
                    return true;
                }
                ind++;
            }
            ind = 0;
            while (ind < self->capacity) {
                if (self->nodes[ind].val.val_len == 0) {
                    self->nodes[ind].key = key;
                    self->nodes[ind].val = val;
                    self->nodes[ind].created = time(0);
                    self->size++;
                    pthread_mutex_unlock(&self->write_lock);
                    return true;
                }
                ind++;
            }
        }
        else {
            if (force) {
                self->nodes[ind].key = key;
                self->nodes[ind].val = val;
                self->nodes[ind].created = time(0);
                pthread_mutex_unlock(&self->write_lock);
                return true;
            }
            else {
                pthread_mutex_unlock(&self->write_lock);
                errno = ENOMEM;
                return false;
            }
        }
    }
    else {
        self->nodes[get_noL(self, key)].key = key;
        self->nodes[get_noL(self, key)].val = val;
        self->nodes[ind].created = time(0);
        pthread_mutex_unlock(&self->write_lock);
        return true;
    }
    pthread_mutex_unlock(&self->write_lock);
    return false;
}


map_val_t get(hashmap_t *self, map_key_t key) {
    if (self->invalid == true || key.key_base == NULL || key.key_len == 0) {
        errno = EINVAL;
        return MAP_VAL(NULL, 0);
    }
    pthread_mutex_lock(&self->fields_lock);
    self->num_readers++;
    if (self->num_readers == 1) {
        pthread_mutex_lock(&self->write_lock);
    }
    pthread_mutex_unlock(&self->fields_lock);

    uint32_t ind = self->hash_function(key) % self->capacity;
    bool end = false;
    for (uint32_t i = ind; i < self->capacity; i++) {
        if (self->nodes[i].key.key_base == NULL && !self->nodes[i].tombstone) {
            end = true;
        }
        if (end) {
            break;
        }
        if (self->nodes[i].key.key_base == NULL) {

        }
        else if (strcmp(((char*)self->nodes[i].key.key_base), ((char*)key.key_base)) == 0) {
            pthread_mutex_lock(&self->fields_lock);
            self->num_readers--;
            if (self->num_readers == 0) {
                pthread_mutex_unlock(&self->write_lock);
            }
            pthread_mutex_unlock(&self->fields_lock);
            time_t now = time(0);
            if (difftime(now, self->nodes[i].created) > 10) {
                if (get_noL(self, key) == -1) {
                }
                else {
                    int i = get_noL(self, key);
                    self->nodes[i].key.key_base = NULL;
                    self->nodes[i].key.key_len = 0;
                    self->nodes[i].val.val_base = NULL;
                    self->nodes[i].val.val_len = 0;
                    self->nodes[i].tombstone = true;
                }
                return MAP_VAL(NULL, 0);
            }
            return MAP_VAL(self->nodes[i].val.val_base, self->nodes[i].val.val_len);
        }
    }
    for (uint32_t i = 0; i < ind; i++) {
        if (self->nodes[i].key.key_base == NULL && !self->nodes[i].tombstone) {
            end = true;
        }
        if (end) {
            break;
        }
        if (self->nodes[i].key.key_base == NULL) {

        }
        else if (strcmp(((char*)self->nodes[i].key.key_base), ((char*)key.key_base)) == 0) {
            pthread_mutex_lock(&self->fields_lock);
            self->num_readers--;
            if (self->num_readers == 0) {
                pthread_mutex_unlock(&self->write_lock);
            }
            pthread_mutex_unlock(&self->fields_lock);
            time_t now = time(0);
            if (difftime(now, self->nodes[i].created) > 10) {
                if (get_noL(self, key) == -1) {
                }
                else {
                    int i = get_noL(self, key);
                    self->nodes[i].key.key_base = NULL;
                    self->nodes[i].key.key_len = 0;
                    self->nodes[i].val.val_base = NULL;
                    self->nodes[i].val.val_len = 0;
                    self->nodes[i].tombstone = true;
                }
                return MAP_VAL(NULL, 0);
            }
            return MAP_VAL(self->nodes[i].val.val_base, self->nodes[i].val.val_len);
        }
    }
    pthread_mutex_lock(&self->fields_lock);
    self->num_readers--;
    if (self->num_readers == 0) {
        pthread_mutex_unlock(&self->write_lock);
    }
    pthread_mutex_unlock(&self->fields_lock);
    return MAP_VAL(NULL, 0);
}

map_node_t delete(hashmap_t *self, map_key_t key) {
    if (self->invalid == true || key.key_base == NULL || key.key_len == 0) {
        errno = EINVAL;
        return MAP_NODE(MAP_KEY(NULL, 0), MAP_VAL(NULL, 0), false);
    }
    pthread_mutex_lock(&self->write_lock);
    if (get_noL(self, key) == -1) {
        pthread_mutex_unlock(&self->write_lock);
        return MAP_NODE(MAP_KEY(NULL, 0), MAP_VAL(NULL, 0), false);
    }
    else {
        int i = get_noL(self, key);
        map_node_t mnt = MAP_NODE(MAP_KEY(self->nodes[i].key.key_base, self->nodes[i].key.key_len), MAP_VAL(self->nodes[i].val.val_base, self->nodes[i].val.val_len), self->nodes[i].tombstone);
        self->nodes[i].key.key_base = NULL;
        self->nodes[i].key.key_len = 0;
        self->nodes[i].val.val_base = NULL;
        self->nodes[i].val.val_len = 0;
        self->nodes[i].tombstone = true;
        pthread_mutex_unlock(&self->write_lock);
        return mnt;
    }
}

bool clear_map(hashmap_t *self) {
    if (self->invalid == true) {
        errno = EINVAL;
        return false;
    }
    for (uint32_t i = 0; i < self->capacity; i++) {
        if (self->nodes[i].key.key_base != NULL) {
            self->destroy_function(self->nodes[i].key, self->nodes[i].val);
        }
        self->nodes[i].key.key_base = NULL;
        self->nodes[i].key.key_len = 0;
        self->nodes[i].val.val_base = NULL;
        self->nodes[i].val.val_len = 0;
        self->nodes[i].tombstone = false;
    }
    return true;
}

bool invalidate_map(hashmap_t *self) {
    if (self->invalid == true) {
        errno = EINVAL;
        return false;
    }
    self->invalid = true;
    for (uint32_t i = 0; i < self->capacity; i++) {
        if (self->nodes[i].key.key_base != NULL) {
            self->destroy_function(self->nodes[i].key, self->nodes[i].val);
        }
    }
    free(self->nodes);
    return true;
}
