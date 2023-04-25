# Triciclos
Para ejecutar el archivo _Tricilos.py_ se da como primer argumento el nombre del fichero (o ficheros separados por comas: ej: nombre1,nombre2) del que leer los grafos y, de forma opcional, como segundo se escribe local si se quiere que los ficheros se evaluen como grafos independientes. Por defecto, considera que si hay más de un fichero todos forman parte del mismo grafo.

```console
User@#PATH#Triciclos~$ python3 Triciclos.py nombreFichero(s) (local)
```

*3-ciclos:* una relación entre 3 entidades donde cada una está relacionada con las otras dos.

Se han metido todas las funciones dentro de una clase _Triciclos_ como funciones estáticas por modularidad y para hacerlo más facilmente reutilizable.

## 1. Función _getTriciclos0_
Calcula los 3-ciclos de un grafo definido como lista de aristas. (Caso particular de _getTriciclosMult_)

Recibe como argumento el nombre del archivo del que lee dicha lista.

Produce como salida el número de 3 ciclos que hay en esa lista.

## 2. Función _getTriciclosMult_
Calcula los 3-ciclos de un grafo que se encuentra definido en múltiples ficheros de entrada.

Recibe como argumento una lista con los nombres de los ficheros.

Produce como salida el número de 3 ciclos.

## 3. Función _getTriciclosLocal_
Calcula independientemente los 3-ciclos de cada uno de los ficheros de entrada.

(Recorre los ficheros y les aplica _getTriciclos0_)

Recibe como argumento una lista con los nombres de los ficheros.

Produce como salida el número de 3 ciclos de cada uno.

## 4. Función _getTriciclos_
Recibe un fichero o una lista de ficheros y un booleano opcional, que se toma como False por defecto. El booleano como True indica que si se recibe una lista de ficheros
se deben tratar como si los ciclos que contuvieran fueran independientes. Aplica la función correspondiente para calcular la cantidad de triciclos.
