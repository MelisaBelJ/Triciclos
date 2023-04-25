# Triciclos

*3-ciclos:* una relación entre 3 entidades donde cada una
 está relacionada con las otras dos.

## 1. Función _getTriciclos_
Programa paralelo que calcula los 3-ciclos de un grafo definido como lista de
 aristas. (Caso particular de _getTriciclosMult_)

Recibe como argumento el nombre del archivo del que lee dicha lista.

Produce como salida el número de 3 ciclos que hay en esa lista.

## 2. Función _getTriciclosMult_
Programa paralelo calcula los 3-ciclos de un grafo que se encuentra definido
 en múltiples ficheros de entrada.

Recibe como argumento una lista con los nombres de los ficheros.

Produce como salida el número de 3 ciclos.

## 3. Función _getTriciclosLocal_
Programa paralelo que calcula independientemente los 3-ciclos de cada uno de
 los ficheros de entrada.

(Recorre los ficheros y les aplica _getTriciclos_)

Recibe como argumento una lista con los nombres de los ficheros.

Produce como salida el número de 3 ciclos de cada uno.

def getTriciclos(file):
    return getTriciclosMult([file])
def getTriciclosMult (files):
    for file in files:
        ...
    return f'En {files} hay {x} triciclos'
def getTriciclos local(files):
    r = []
    for file in files:
        r.append(getTriciclos(file))
    return r
def formateaListaFicheros(files):
    for file in files:
        r += file+', '
    return r[0:-1]
