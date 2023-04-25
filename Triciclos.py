from pyspark import SparkContext
from itertools import combinations
import sys

class Triciclos():
    def __init__(self):
        pass
    
    """
    Obtiene las aristas del grafo con el menor número a la izquierda.    
    Si es un ciclo, devuelve None, pues no interesa.
    Recibe la arista como un string de la forma: valor1, valor2
    """
    @staticmethod
    def getBordes(line):
        return Triciclos.getBordesLista(line.split(","))
    
    """
    Devuelve las aristas del grafo con el menor número a la izquierda.
    Si es un ciclo, devuelve None, pues no interesa.
    Recibe la arista como iterable.
    """
    @staticmethod
    def getBordesLista(a):
        if a[0] != a[1]:
            r = a[0] < a[1]
            return a[(r+1)%2], a[r]
        else:
            return None
        
    """
    Recibe un iterable de la forma: valor, lista_de_adyacentes.
    Devuelve una lista ordenada de tuplas formadas por aristas y un segundo valor, que es:
      - True, si el arista es de la forma (valor, y) con y en lista_de_adyacentes.
      - valor, si el arista está formado por miembros distintos de lista_de_adyacentes.
    """
    @staticmethod
    def transforma(fila):
        valor, lista = fila[0], list(fila[1])
        r = []
        for y in lista:
            if valor != y:
                e = (valor, y), True
                if e not in r:
                    r.append(e)
        for a in combinations(lista, 2):
            aux = Triciclos.getBordesLista(a)
            if aux != None:
                e = aux, valor
                if e not in r:
                    r.append(e)
        return r

    """
    Devuelve True si y solo si alguno de los aristas tiene asociado True (está en el grafo)
    """
    @staticmethod
    def existe(fila):
        _, aristas = fila        
        return any(arista == True for arista in aristas)


    """
    Devuelve una lista con los triciclos en la fila
    """
    @staticmethod
    def encuentraTriciclos(fila):
        arista, valores = fila
        a2, a3 = arista
        return [(a1, a2, a3) for a1 in valores if not (a1 == True)]
    
    """
    Devuelve la cantidad de triciclos que hay en el/los fichero(s) que recibe:
    Tiene como parámetros 
        - el nombre del fichero o una lista con nombres de ficheros y
        - un booleano indicando si, de ser una lista, esta represeta un único grafo, o varios independientes.   
    """
    @staticmethod
    def getTriciclos(ficheros, noLocal = False):
        if type(ficheros) is list:
            if noLocal:
                print('Muchos ficheros, no locales')
                r = Triciclos.getTriciclosMult(ficheros)
            else:
                print('Muchos ficheros, locales')
                r = Triciclos.getTriciclosLocal(ficheros)
        else:
            print('Un fichero')
            r = Triciclos.getTriciclos0(ficheros)
        return r
    
    """
    Devuelve la cantidad de triciclos en elgrafo representado por el fichero que recibe como argumento.
    """
    @staticmethod
    def getTriciclos0(fichero):
        return Triciclos.getTriciclosMult([fichero])
    
    """
    Devuelve la cantidad de triciclos en el grafo representado por los ficheros de la lista que recibe como argumento.
    """
    @staticmethod
    def getTriciclosMult (ficheros):
        rdd = ''
        with SparkContext() as sc:
            for fichero in ficheros:
                print(fichero)
                data = sc.textFile(fichero)
                rdd = data if rdd=='' else sc.union([data, rdd])
            rdd = rdd.map(Triciclos.getBordes).filter(lambda x: x != None).groupByKey().flatMap(Triciclos.transforma).groupByKey().filter(Triciclos.existe).flatMap(Triciclos.encuentraTriciclos)
            x = f"    En {', '.join(ficheros)} hay {rdd.count()} triciclos"
        return x
    
    """
    Devuelve la cantidad de triciclos en cada uno de los grafos representados por los ficheros en la lista que recibe como argumento.
    """
    @staticmethod
    def getTriciclosLocal(ficheros):
        r = ''
        for fichero in ficheros:
            r += Triciclos.getTriciclos0(fichero) + '\n'
        return r
    
def main(ficheros = 'g0.txt', noLocal = False):   
    print('Resultado: \n'+Triciclos.getTriciclos(ficheros, noLocal))

if __name__ == "__main__":
    l = len(sys.argv)
    if l > 1:
        fichero, noLocal = sys.argv[1], l==2 or not 'local' in sys.argv[2]
        main(fichero.split(',') if ',' in fichero else fichero, noLocal)
    else:
        main()
