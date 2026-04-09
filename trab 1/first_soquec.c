#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>
#include <time.h>

// =================================================================
// DUPLA: Gabriel Ribeiro e Thais Carolina
// DISCIPLINA: Sistemas Paralelos e Distribuídos 
// FONTES DE CONSULTA: Documentação POSIX (pthreads), GeeksforGeeks (Produtor-Consumidor), 
// e base teórica do problema clássico de Dijkstra.
// =================================================================

#define TOTAL_ITENS 3000

// Estruturas e Variáveis Globais para o Buffer Circular
int *buffer;
int buffer_in = 0; // Índice de inserção do produtor
int buffer_out = 0; // Índice de remoção do consumidor
int processados = 0; // Contador de itens lidos
int itens_produzidos = 0; // Contador de itens gerados
int tamanho_buffer; // Receberá o valor de T configurável

// Variáveis de Sincronização
sem_t sem_empty; // Controla os espaços VAZIOS no buffer
sem_t sem_full;  // Controla os espaços CHEIOS no buffer
pthread_mutex_t mutex;     // Protege a região crítica (acesso ao array buffer)
pthread_mutex_t cont_lock; // Protege as variáveis contadoras globais

// Função da Thread Produtora
void* produtor(void* arg) {
    while (1) {
        // Verifica se já produzimos o total necessário
        pthread_mutex_lock(&cont_lock);
        if (itens_produzidos >= TOTAL_ITENS) {
            pthread_mutex_unlock(&cont_lock);
            break;
        }
        int item = rand() % 1000 + 1; // Gera dado aleatório
        itens_produzidos++;
        pthread_mutex_unlock(&cont_lock);
        
        // Simula o tempo gasto para buscar/gerar um dado (Ex: 2 milissegundos)
        usleep(2000);

        sem_wait(&sem_empty); // Decrementa espaços vazios (bloqueia se buffer estiver cheio)
        pthread_mutex_lock(&mutex); // Bloqueia outros acessos ao buffer
        
        buffer[buffer_in] = item; // Insere o item
        buffer_in = (buffer_in + 1) % tamanho_buffer; // Lógica circular
        
        pthread_mutex_unlock(&mutex); // Libera o acesso ao buffer
        sem_post(&sem_full); // Incrementa espaços cheios (avisa o consumidor)
    }
    return NULL;
}

// Função da Thread Consumidora
void* consumidor(void* arg) {
    while (1) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        
        // Cria um timeout de 0.1s no futuro. 
        // Isso evita que o consumidor fique preso para sempre quando a produção acaba.
        ts.tv_nsec += 100000000; 
        if (ts.tv_nsec >= 1000000000) {
            ts.tv_sec += 1;
            ts.tv_nsec -= 1000000000;
        }

        // Tenta consumir. Se der timeout (-1), significa que a produção acabou e o buffer está vazio.
        if (sem_timedwait(&sem_full, &ts) == -1) break;

        pthread_mutex_lock(&mutex); // Bloqueia outros acessos ao buffer
        int item = buffer[buffer_out]; // Retira o item
        buffer_out = (buffer_out + 1) % tamanho_buffer; // Lógica circular
        pthread_mutex_unlock(&mutex); // Libera o buffer

        sem_post(&sem_empty); // Incrementa espaços vazios (avisa o produtor)
        
        // Simula o tempo gasto para processar o dado
        usleep(3000);

        // Registra que o item foi processado com sucesso
        pthread_mutex_lock(&cont_lock);
        processados++;
        pthread_mutex_unlock(&cont_lock);
    }
    return NULL;
}

// Função para orquestrar os cenários propostos
void rodar_cenario(int P, int C, int T, char* nome) {
    tamanho_buffer = T;
    buffer = (int*)malloc(T * sizeof(int)); // Aloca a memória do buffer
    
    // Reseta as variáveis para o novo experimento
    buffer_in = 0; buffer_out = 0;
    processados = 0; itens_produzidos = 0;

    // Inicializa os semáforos e mutexes
    sem_init(&sem_empty, 0, T); // Começa com T espaços vazios
    sem_init(&sem_full, 0, 0);  // Começa com 0 espaços cheios
    pthread_mutex_init(&mutex, NULL);
    pthread_mutex_init(&cont_lock, NULL);

    pthread_t tp[P], tc[C];
    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start); // Inicia o cronômetro

    // Dispara as threads
    for(int i=0; i<P; i++) pthread_create(&tp[i], NULL, produtor, NULL);
    for(int i=0; i<C; i++) pthread_create(&tc[i], NULL, consumidor, NULL);

    // Aguarda todas as threads finalizarem
    for(int i=0; i<P; i++) pthread_join(tp[i], NULL);
    for(int i=0; i<C; i++) pthread_join(tc[i], NULL);

    clock_gettime(CLOCK_MONOTONIC, &end); // Para o cronômetro
    double tempo = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1E9;

    printf("%-35s Tempo: %.4fs | Itens: %d\n", nome, tempo, processados);

    // Limpa a memória e destroi os bloqueios
    free(buffer);
    sem_destroy(&sem_empty);
    sem_destroy(&sem_full);
    pthread_mutex_destroy(&mutex);
    pthread_mutex_destroy(&cont_lock);
}

int main() {
    srand(time(NULL));
    printf("Iniciando Experimentos em C (Item 4):\n--------------------------------------------------\n");

    // Executa os experimentos com P, C e T configuráveis
    rodar_cenario(10, 10, 1, "A: P==C (T=1)");
    rodar_cenario(10, 10, 5, "A: P==C (T=5)");
    printf("\n");
    rodar_cenario(20, 10, 1, "B: P==2C (T=1)");
    rodar_cenario(20, 10, 5, "B: P==2C (T=5)");
    printf("\n");
    rodar_cenario(10, 20, 1, "C: C==2P (T=1)");
    rodar_cenario(10, 20, 5, "C: C==2P (T=5)");

    return 0;
}