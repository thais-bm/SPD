import threading
import time
import random
import os

# =================================================================
# DUPLA: Gabriel Ribeiro e Thais Carolina
# FONTES DE CONSULTA: Documentação Oficial Python (threading), GeeksforGeeks.
# INSPIRAÇÃO: Problema Clássico de Sincronização de Dijkstra.
# GERADOR DE DADOS: Função gerar_arquivo_dados() inclusa abaixo.
# =================================================================

def gerar_arquivo_dados(n=500):
    """5.a - Gerador de arquivos de dados: Cria números aleatórios para o teste."""
    with open("dados_entrada.txt", "w") as f:
        for _ in range(n):
            # Correção do erro: f-string sem parênteses extras
            f.write(f"{random.randint(1, 1000)}\n")

def carregar_dados():
    """Lê os dados gerados para serem processados pelas threads."""
    if not os.path.exists("dados_entrada.txt"):
        gerar_arquivo_dados()
    with open("dados_entrada.txt", "r") as f:
        return [line.strip() for line in f.readlines()]

# Variáveis globais de controle e sincronização
buffer = []
mutex = threading.Lock() # 3. Sincronização para consistência
itens_restantes = []
processados = 0
cont_lock = threading.Lock()

def produtor(id_p, sem_empty, sem_full):
    global itens_restantes
    while True:
        with cont_lock:
            if not itens_restantes:
                break
            item = itens_restantes.pop(0)

        sem_empty.acquire() # 1.c - Controle de tamanho T do Buffer
        with mutex:
            buffer.append(item)
        sem_full.release() 

def consumidor(id_c, sem_empty, sem_full):
    global processados
    while True:
        # Timeout de 2s para encerrar caso a produção pare
        sucesso = sem_full.acquire(timeout=2)
        if not sucesso:
            break
            
        with mutex:
            if buffer:
                item = buffer.pop(0)
                with cont_lock:
                    processados += 1
        sem_empty.release()

def rodar_cenario(P, C, T, nome_cenario):
    """Executa os experimentos solicitados no item 4."""
    global buffer, itens_restantes, processados
    buffer = []
    itens_restantes = carregar_dados()
    processados = 0
    
    # 3. Solução de sincronização com Semáforos
    sem_empty = threading.Semaphore(T)
    sem_full = threading.Semaphore(0)
    
    threads = []
    inicio = time.time()

    # 1.a & 1.b - Threads configuráveis
    for i in range(P):
        t = threading.Thread(target=produtor, args=(i, sem_empty, sem_full))
        threads.append(t)
        t.start()

    for i in range(C):
        t = threading.Thread(target=consumidor, args=(i, sem_empty, sem_full))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    fim = time.time()
    print(f"{nome_cenario:.<35} Tempo: {fim-inicio:.4f}s | Itens: {processados}")

if __name__ == "__main__":
    print("Preparando ambiente e gerando arquivos de teste...")
    gerar_arquivo_dados(500) 
    
    print("\nIniciando Experimentos (Itens 4.a, 4.b, 4.c):\n" + "-"*70)
    
    # Lista de cenários conforme o enunciado
    experimentos = [
        # (P, C, T, "Descrição")
        (10, 10, 1, "A: P==C (T=1)"),
        (10, 10, 5, "A: P==C (T=5)"),
        (20, 10, 1, "B: P==2C (T=1)"),
        (20, 10, 5, "B: P==2C (T=5)"),
        (10, 20, 1, "C: C==2P (T=1)"),
        (10, 20, 5, "C: C==2P (T=5)"),
    ]

    for p, c, t, nome in experimentos:
        rodar_cenario(p, c, t, nome)
    print("-"*70)