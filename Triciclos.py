from pyspark import SparkContext
import sys

class Triciclos():
    def __init__(self):
        pass
    
    @staticmethod
    def getBordes(line):
        a = line.split(",")
        if a[0] == a[1]:
            return None
        else:
            r = a[0] < a[1]
            return a[(r+1)%2], a[r]
        
    @staticmethod
    def transforma(fila):
        x, lista = fila[0], list(fila[1])
        r = []
        for y in lista:
            if x != y:
                e = (x, y), 'existe'
                if e not in r:
                    r.append(e)
        for a in combinations(lista, 2):
            if a[0] != a[1]:
                z = a[0] < a[1]:
                e = (a[(z+1)%2], a[z]), ('pendiente', x)
                if e not in r:
                    r.append(e)
        return r

    @staticmethod
    def hayExiste(fila):
        _, valores = fila
        x = False
        for valor in valores:
            if valor == "existe":
                x = True
                break
        return x

    @staticmethod
    def encuentraTriciclos(fila):
        clave, valores = fila
        bordes = []
        for valor in valores:
            if valor != "existe":
                _, a1 = valor
                a2, a3 = clave
                bordes.append((a1, a2, a3))
        return bordes
    
    @staticmethod
    def getTriciclos(ficheros, local):
        if type(ficheros) is list:
            if local:
                Triciclos.getTriciclosLocal(ficheros)
            else:
                Triciclos.getTriciclosMult(ficheros)
        else:
            Triciclos.getTriciclos0(ficheros)
    
    @staticmethod
    def getTriciclos0(fichero):
        return Triciclos.getTriciclosMult([fichero])
    
    @staticmethod
    def getTriciclosMult (ficheros):
        x = 0
        with SparkContext() as sc:
            for fichero in ficheros:
                data = sc.textFile(fname)
                rdd = data.map(Triciclos.getBordes).filter(lambda x: x != None).groupByKey().\
                flatMap(Triciclos.transforma).groupByKey().filter(Triciclos.hayExiste).flatMap(Triciclos.encuentraTriciclos)
                x = rdd.collect()
        return f'En {ficheros} hay {x} triciclos'
    
    @staticmethod
    def getTriciclosLocal(ficheros):
        r = []
        for fichero in ficheros:
            r.append(Triciclos.getTriciclos0(fichero) + '\n')
        return r
    
    @staticmethod
    def formateaListaFicheros(ficheros):
        r = ''
        for fichero in ficheros:
            r += fichero + ', '
        return r[0:-2]
    
def main(ficheros = 'triciclos.txt', local = True):   
    print(Triciclos.getTriciclos(ficheros, local))

if __name__ == "__main__":
    if len(sys.argv) == 2:
        main(sys.argv[1])
    elif len(sys.argv) == 3:
        main(sys.argv[1], sys.argv[2])
    else:
        main()
