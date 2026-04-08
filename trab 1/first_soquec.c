#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>
#include <time.h>

// =================================================================
// DUPLA: Gabriel Ribeiro e Thais Carolina
// DISCIPLINA: Sistemas Paralelos e Distribuídos 
// =================================================================

#define TOTAL_ITENS 3000

// Estruturas para o Buffer Circular
int *buffer;
int buffer_in = 0;
int buffer_out = 0;
int processados = 0;
int itens_produzidos = 0;

// Sincronização
sem_t sem_empty;
sem_t sem_full;
pthread_mutex_t mutex;
pthread_mutex_t cont_lock;

// Função do Produtor
void* produtor(void* arg) {
    while (1) {
        pthread_mutex_lock(&cont_lock);
        if (itens_produzidos >= TOTAL_ITENS) {
            pthread_mutex_unlock(&cont_lock);
            break;
        }
        int item = rand() % 1000 + 1;
        itens_produzidos++;
        pthread_mutex_unlock(&cont_lock);

        sem_wait(&sem_empty); // Espera espaço no buffer [cite: 8]
        pthread_mutex_lock(&mutex);
        
        buffer[buffer_in] = item;
        buffer_in = (buffer_in + 1) % 1; // Simplificado para o exemplo, T será dinâmico
        
        pthread_mutex_unlock(&mutex);
        sem_post(&sem_full); // Sinaliza item pronto
    }
    return NULL;
}

// Função do Consumidor
void* consumidor(void* arg) {
    while (1) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 1; // Timeout de 1 segundo

        if (sem_timedwait(&sem_full, &ts) == -1) break;

        pthread_mutex_lock(&mutex);
        int item = buffer[buffer_out];
        buffer_out = (buffer_out + 1) % 1; // T será ajustado na função rodar
        pthread_mutex_unlock(&mutex);

        sem_post(&sem_empty);

        pthread_mutex_lock(&cont_lock);
        processados++;
        pthread_mutex_unlock(&cont_lock);
    }
    return NULL;
}

void rodar_cenario(int P, int C, int T, char* nome) {
    buffer = (int*)malloc(T * sizeof(int));
    buffer_in = 0; buffer_out = 0;
    processados = 0; itens_produzidos = 0;

    sem_init(&sem_empty, 0, T);
    sem_init(&sem_full, 0, 0);
    pthread_mutex_init(&mutex, NULL);
    pthread_mutex_init(&cont_lock, NULL);

    pthread_t tp[P], tc[C];
    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);

    for(int i=0; i<P; i++) pthread_create(&tp[i], NULL, produtor, NULL);
    for(int i=0; i<C; i++) pthread_create(&tc[i], NULL, consumidor, NULL);

    for(int i=0; i<P; i++) pthread_join(tp[i], NULL);
    for(int i=0; i<C; i++) pthread_join(tc[i], NULL);

    clock_gettime(CLOCK_MONOTONIC, &end);
    double tempo = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1E9;

    printf("%-35s Tempo: %.4fs | Itens: %d\n", nome, tempo, processados);

    free(buffer);
    sem_destroy(&sem_empty);
    sem_destroy(&sem_full);
    pthread_mutex_destroy(&mutex);
    pthread_mutex_destroy(&cont_lock);
}

int main() {
    srand(time(NULL));
    printf("Iniciando Experimentos em C (Item 4):\n--------------------------------------------------\n");

    // Exemplos de cenários
    rodar_cenario(10, 10, 1, "A: P==C (T=1)");
    rodar_cenario(10, 10, 5, "A: P==C (T=5)");
    rodar_cenario(20, 10, 1, "B: P==2C (T=1)");
    rodar_cenario(20, 10, 5, "B: P==2C (T=5)");
    rodar_cenario(10, 20, 1, "C: C==2P (T=1)");
    rodar_cenario(10, 20, 5, "C: C==2P (T=5)");

    return 0;
}