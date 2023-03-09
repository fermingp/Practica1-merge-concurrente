"""
Práctica 1: Fermín González Pereiro

Enunciado:
    Implementar un merge concurrente:
- Tenemos NPROD procesos que producen números no negativos de forma
creciente. Cuando un proceso acaba de producir, produce un -1

- Hay un proceso merge que debe tomar los números y almacenarlos de
forma creciente en una única lista (o array). El proceso debe esperar a que
los productores tengan listo un elemento e introducir el menor de
ellos.

- Se debe crear listas de semáforos. Cada productor solo maneja los
sus semáforos para sus datos. El proceso merge debe manejar todos los
semáforos.

- OPCIONALMENTE: se puede hacer un búffer de tamaño fijo de forma que
los productores ponen valores en el búffer.
"""

"""
PARTE OPCIONAL: en esta parte sí incluimos funciones comunes por lo que hacemos uso del semáforo mutex para
la exclusividad
"""

"""
Consideración: la producción creciente de números no la consideramos estrictamente creciente por lo que
puede darse el caso en que aumente 0 unidades y se mantenga el mismo valor. Por eso incluimos 
el 0 en new_product = last_cons[ind] + randint(0,5)
"""

from multiprocessing import Process
from multiprocessing import Manager
from multiprocessing import Semaphore, Lock, BoundedSemaphore
from multiprocessing import current_process
from multiprocessing import Value, Array
from random import randint, random
from time import sleep


N = 50 #Vueltas
K = 5 #Capacidad búffer 
NPROD = 6 #número de productores

def delay(factor = 3):
    sleep(random()/factor)

#devolvemos el minimo del buffer y el productor al que corresponde
#el minimo siempre estará en la primera de las K posiciones de cada productor
def min_buffer(buffer, mutex): 
    mutex.acquire()
    try:
        n=len(buffer)
        result = []
        for i in range(0,n,K):
            if buffer[i]!=-1:
                result.append((buffer[i],i//K)) #metemos tupla (elem,productor)
        min_buf=min(result) #el minimo es el de primera componente menor
        pos_min=result[result.index(min_buf)][1]
    finally:
        mutex.release()
    return min_buf[0], pos_min


def add_data(buffer, ind, last_prod, last_ind, mutex):
    mutex.acquire()
    try:
        r = last_ind[ind] #este array indica en que posición tiene que producir el productor ind
        new_product = last_prod[ind] + randint(0,5)
        buffer[ind*K+r] = new_product
        last_prod[ind] = new_product
        last_ind[ind] = last_ind[ind] + 1
    finally:
        mutex.release()
    return new_product


def get_data(buffer, lista_merge, product, ind, last_ind, mutex):
    mutex.acquire()
    try:
        lista_merge.append(product)
        for i in range(ind*K, (ind+1)*K-1): #movemos los K elementos una posición hacia delante
            buffer[i] = buffer[i + 1]
        buffer[(ind+1)*K-1] = -2 #posición vacía
        if buffer[ind*K]==-1:
            for i in range(K): #proceso entero a -1 (no tiene valores a consumir)
                buffer[ind*K + i]=-1
        last_ind[ind] = last_ind[ind] - 1 
    finally:
        mutex.release()


#-1 en la primera posición disponible. Si no hay valores a consumir todos a -1
def fin_proceso(buffer, ind, last_ind, mutex): 
    mutex.acquire()
    try:
        buffer[last_ind[ind]+ ind*K] = -1 #(index+1)*K-1
        if buffer[ind*K]==-1:
            for i in range(K):
                buffer[ind*K + i]=-1   
    finally:
        mutex.release()
        
        
def producir(buffer, empty_sem_list, non_empty_sem_list, last_prod, last_ind, mutex):
    for i in range(N):
        print (f"productor {current_process().name} produciendo")
        k = int(current_process().name.split('_')[1])
        empty_sem_list[k].acquire()
        product = add_data(buffer, k, last_prod, last_ind, mutex)
        delay()
        non_empty_sem_list[k].release()
        print (f"productor {current_process().name} produción realizada {product}, buffer actual: ",buffer[:])
    k = int(current_process().name.split('_')[1])
    empty_sem_list[k].acquire()
    fin_proceso(buffer, k, last_ind, mutex)
    non_empty_sem_list[k].release()  
    print (f"productor {k} terminado, buffer actual: ", buffer[:])

#cuando un proceso acaba ponemos un -1 en la primera posición no ocupada y cuando se queda sin elementos
#que pueden ser consumidos, los K elementos del productor pasan a -1. Se puede consumir mientras haya algún 
#elemento distinto de -1
def se_puede_consumir(buffer, mutex): 
    mutex.acquire()
    try:
        j=0
        for i in range(K*NPROD):
            if buffer[i]==-1:
                j+=1
        return j<K*NPROD
    finally:
        mutex.release()

def merge(buffer, empty_sem_list, non_empty_sem_list, lista_merge, last_ind, mutex):
    for i in range(NPROD): #comprobamos que todos los productores han producido
        non_empty_sem_list[i].acquire()
    while se_puede_consumir(buffer, mutex): 
        producto, pos = min_buffer(buffer, mutex)
        get_data(buffer, lista_merge, producto, pos, last_ind, mutex)
        print (f"merge guardando {producto} de productor {pos}, buffer actual: ", buffer[:])
        empty_sem_list[pos].release()
        non_empty_sem_list[pos].acquire()
        

def main():
    buffer = Array('i', K*NPROD)
    for i in range(K*NPROD):
        buffer[i] = -2 #representamos con -2 que los procesos no han producido nada aún (vacíos)
    print ("Buffer inicial: ", buffer[:],"\n")
    
    last_prod = Array('i', NPROD)
    for i in range(NPROD): #array que guarda el último valor que ha producido cada productor
        last_prod[i] = 0 #lo inicializamos lleno de ceros
        
    last_ind = Array('i', NPROD) #cada posición indica el número de productos producidos de dicho productor
    for i in range(NPROD): 
        last_ind[i] = 0 #se inicializa lleno de ceros
    
    manager = Manager()
    lista_merge=manager.list()  #lista donde se van guardando las consumiciones
    
    empty_sem_list = [BoundedSemaphore(K) for i in range(NPROD)] #semáforo empty para cada productor
    non_empty_sem_list = [Semaphore(0) for i in range(NPROD)] #semáforo non_empty para cada productor
    mutex = Lock()
     
    prodlst = [ Process(target=producir,
                        name=f'prod_{i}',
                        args=(buffer, empty_sem_list, non_empty_sem_list, last_prod, last_ind, mutex))
                for i in range(NPROD) ]

    consumidor = Process(target=merge, name= "consumidor", 
                         args=(buffer, empty_sem_list, non_empty_sem_list, lista_merge, last_ind, mutex))

    for p in prodlst:
        p.start()
    consumidor.start()
    
    for p in prodlst:
        p.join()
    consumidor.join()
    
    print("\n")
    print("Buffer final: ", buffer[:])
    print("Lista final de consumiciones:", lista_merge)
    print("La lista es de longitud N*NPROD = ", len(lista_merge) )

if __name__ == '__main__':
    main()