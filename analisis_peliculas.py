from pyspark import SparkContext
from pyspark.sql import SQLContext

def main():
    sc = SparkContext("local", "Analisi_peliculas")
    sqlContext = SQLContext(sc)
    
    rddPeliculas = sqlContext.read.csv("movie_metadata.csv", header=True).rdd
    rdd_kv = rddPeliculas.map(lambda x: (x[1], int(x[8], 1) if x[8] != None and x[8] != "" else (0, 1) ))
    rdd_reducido = rdd_kv.reduceByKey(lambda x,y: (x[0] + y[0]) / (x[1]+ y[1]) )
    rdd_ordenado = rdd_reducido.sortBy(lambda x: x[1], ascending=False)

    arr_kv = rdd_ordenado.take(1)
    print(arr_kv)

if __name__ == "__main__":
    main()
