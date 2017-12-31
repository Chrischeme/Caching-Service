#include "queue.h"
#include <errno.h>

queue_t *create_queue(void) {
    queue_t *que = calloc(1, sizeof(*que));
    if (que == NULL) {
        return NULL;
    }
    que->front = NULL;
    que->rear = NULL;
    if (pthread_mutex_init(&que->lock, NULL) != 0) {
        return NULL;
    }
    if (sem_init(&que->items, 0, 0) == -1) {
        return NULL;
    }
    que->invalid = false;
    return que;
}

bool invalidate_queue(queue_t *self, item_destructor_f destroy_function) {
    if (!self->invalid || self == NULL || destroy_function == NULL) {
        self->invalid = true;
        while (self->front != NULL) {
            destroy_function(self->front->item);
            queue_node_t *temp = self->front;
            self->front = temp->next;
            free(temp);
        }
        return true;
    }
    errno = EINVAL;
    return false;
}

bool enqueue(queue_t *self, void *item) {
    if (!self->invalid && self != NULL && item != NULL) {
        queue_node_t *qnt = calloc(1, sizeof(*qnt));
        qnt->item = item;
        qnt->next = NULL;
        pthread_mutex_lock(&self->lock);
        if (self->rear == NULL) {
            self->front = qnt;
            self->rear = qnt;
        }
        else {
            self->rear->next = qnt;
            self->rear = qnt;
        }
        pthread_mutex_unlock(&self->lock);
        sem_post(&self->items);
        return true;
    }
    errno = EINVAL;
    return false;
}

void *dequeue(queue_t *self) {
    if (!self->invalid && self != NULL) {
        sem_wait(&self->items);
        pthread_mutex_lock(&self->lock);
        queue_node_t *temp = self->front;
        void * item = temp->item;
        self->front = temp->next;
        if(self->front == NULL) {
            self->rear = NULL;
        }
        free(temp);
        pthread_mutex_unlock(&self->lock);
        return item;
    }
    errno = EINVAL;
    return NULL;
}
