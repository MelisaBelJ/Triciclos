from pyspark import SparkContext
import sys

class Triciclos():
    def __init__(self):
        pass
    
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
        for fichero in ficheros:
            pass
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