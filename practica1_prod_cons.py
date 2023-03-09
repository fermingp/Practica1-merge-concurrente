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
PARTE OBLIGATORIA
"""

"""
Consideración: la producción creciente de números no la consideramos estrictamente creciente por lo que
puede darse el caso en que aumente 0 unidades y se mantenga el mismo valor. Por eso incluimos 
el 0 en new_product=last_cons[k] + randint(0,5)
"""

from multiprocessing import Process
from multiprocessing import Manager
from multiprocessing import Semaphore, Lock
from multiprocessing import current_process
from multiprocessing import Value, Array
from random import randint, random
from time import sleep


N = 50 #número de vueltas 
NPROD = 6 #número de productores


def delay(factor = 3):
    sleep(random()/factor)
    
    
def min_buffer(buffer):  #Función que devuelve el mínimo del buffer y el productor al que corresponde
    n=len(buffer)
    result = []
    for i in range(n):
        if buffer[i]!=-1:
            result.append((buffer[i],i)) #metemos tupla (elem,pos)
    min_buf=min(result) #el minimo es el de primera componente (elemento) menor
    pos_min=result[result.index(min_buf)][1]
    return min_buf[0], pos_min  
    
def producir(buffer, empty_sem_list, non_empty_sem_list, last_cons):
    for i in range(N):
        print (f"productor {current_process().name} produciendo")
        k = int(current_process().name.split('_')[1]) #índice del proceso en producción
        empty_sem_list[k].acquire()
        new_product=last_cons[k] + randint(0,5) #los números se producen de forma creciente
        buffer[k]=new_product
        delay()
        non_empty_sem_list[k].release()
        print (f"productor {current_process().name} producción realizada {new_product}, buffer actual: ",buffer[:])
    k = int(current_process().name.split('_')[1])
    empty_sem_list[k].acquire()
    buffer[k]=-1  #indicamos con -1 el final del proceso
    non_empty_sem_list[k].release()
    print (f"productor {k} terminado, buffer actual: ", buffer[:])
    
    
def se_puede_consumir(buffer): #mientras haya un elemento distinto de -1 el proceso merge puede consumir
    j=0
    for i in range(NPROD):
        if buffer[i]==-1:
            j+=1
    return j<NPROD

def merge(buffer, lista_merge, empty_sem_list, non_empty_sem_list, last_cons): 
    for i in range(NPROD): #así comprobamos que todos los productores hayan producido
        non_empty_sem_list[i].acquire()
    while se_puede_consumir(buffer):
        producto, pos = min_buffer(buffer)
        lista_merge.append(producto)
        buffer[pos]=-2 #indicamos con -2 que se ha quedado vacío esa posición del buffer
        last_cons[pos]=producto  #metemos el producto en la lista de últimas consumiciones
        print (f"merge guardando {producto} de productor {pos}, buffer actual: ", buffer[:])
        empty_sem_list[pos].release()
        non_empty_sem_list[pos].acquire()
        
def main():
    buffer = Array('i', NPROD)
    for i in range(NPROD):
        buffer[i] = -2 #representamos con -2 que los procesos no han producido nada aún (vacíos)
    print ("Buffer inicial: ", buffer[:],"\n")
    
    last_cons = Array('i', NPROD)
    for i in range(NPROD): #array que guarda el último valor que se ha consumido (o producido pues el tamaño es 1) de cada productor 
        last_cons[i] = 0 #lo inicializamos lleno de ceros
        
    empty_sem_list = [Lock() for i in range(NPROD)]  #semáforo empty para cada productor
    non_empty_sem_list = [Semaphore(0) for i in range(NPROD)]  #semáforo non_empty para cada productor
    
    manager = Manager()
    lista_merge=manager.list()  #lista donde se van guardando las consumiciones

    prodlst = [ Process(target=producir,
                        name=f'prod_{i}',
                        args=(buffer, empty_sem_list, non_empty_sem_list, last_cons))
                for i in range(NPROD) ]

    consumidor = Process(target=merge, name= "consumidor", 
                         args=(buffer, lista_merge, empty_sem_list, non_empty_sem_list, last_cons))
     
    for p in prodlst:
        p.start()
    consumidor.start()
    
    for p in prodlst:
        p.join()
    consumidor.join()
        
    print("\n")
    print("Buffer final: ", buffer[:])
    print("Lista final de consumiciones: ", lista_merge)
    print("La lista es de longitud N*NPROD = ", len(lista_merge) )
    
if __name__ == '__main__':
    main()
        

    
