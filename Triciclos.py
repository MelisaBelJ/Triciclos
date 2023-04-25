from pyspark import SparkContext
from itertools import combinations
import sys

class Triciclos():
    def __init__(self):
        pass
    
    @staticmethod
    def getBordes(line):
        return Triciclos.getBordesLista(line.split(","))

    @staticmethod
    def getBordesLista(a):
        if a[0] != a[1]:
            r = a[0] < a[1]
            return a[(r+1)%2], a[r]
        else:
            return None
        
    @staticmethod
    def transforma(fila):
        x, lista = fila[0], list(fila[1])
        r = []
        for y in lista:
            if x != y:
                e = (x, y), True
                if e not in r:
                    r.append(e)
        for a in combinations(lista, 2):
            aux = Triciclos.getBordesLista(a)
            if aux != None:
                e = aux, x
                if e not in r:
                    r.append(e)
        return r

    @staticmethod
    def existe(fila):
        _, valores = fila
        x = False
        for valor in valores:
            if valor == True:
                x = True
                break
        return x

    @staticmethod
    def encuentraTriciclos(fila):
        clave, valores = fila
        a2, a3 = clave
        bordes = [(valor, a2, a3) for valor in valores if not (valor==True)]
        return bordes
    
    @staticmethod
    def getTriciclos(ficheros, noLocal):
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
    
    @staticmethod
    def getTriciclos0(fichero):
        return Triciclos.getTriciclosMult([fichero])
    
    @staticmethod
    def getTriciclosMult (ficheros):
        rdd = ''
        with SparkContext() as sc:
            for fichero in ficheros:
                print(fichero)
                data = sc.textFile(fichero)
                rdd = data if rdd=='' else sc.union([data, rdd])
            rdd = rdd.map(Triciclos.getBordes).filter(lambda x: x != None).groupByKey().flatMap(Triciclos.transforma).groupByKey().filter(Triciclos.existe).flatMap(Triciclos.encuentraTriciclos)
            x = f'    En {Triciclos.formateaListaFicheros(ficheros)} hay {rdd.count()} triciclos'
        return x
    
    @staticmethod
    def getTriciclosLocal(ficheros):
        r = ''
        for fichero in ficheros:
            r += Triciclos.getTriciclos0(fichero) + '\n'
        return r
    
    @staticmethod
    def formateaListaFicheros(ficheros):
        r = ''
        for fichero in ficheros:
            r += fichero + ', '
        return r[0:-2]
    
def main(ficheros = 'g0.txt', noLocal = False):   
    print('Resultado: \n'+Triciclos.getTriciclos(ficheros, noLocal))

if __name__ == "__main__":
    l = len(sys.argv)
    if l > 1:
        fichero, noLocal = sys.argv[1], l==2 or not 'local' in sys.argv[2]
        main(fichero.split(',') if ',' in fichero else fichero, noLocal)
    else:
        main()
