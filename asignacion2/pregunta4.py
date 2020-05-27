from pyspark import SparkContext
from pyspark.sql import SQLContext

def main():
  sc = SparkContext("local", "Analisis_de_distritos")
  sqlContext = SQLContext(sc)
  rddData = sqlContext.read.csv("urbania_data.csv", header = True).rdd
  #rddReducido = rdd_kv.reduceByKey

  def FiltrarProvincia(registro):
    if registro[95] == "Lima" :
      return True
    else:
      return False

  rddProvLima = rddData.filter(FiltrarProvincia)
  rdd_kv = rddProvLima.map(lambda x: (x[94] , (float(x[15]),1) ))
  #print (rdd_kv.take(8))
  rdd_reducido = rdd_kv.reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1]))
  rdd_prec_prom = rdd_reducido.map(lambda x: (x[0], x[1][0]/x[1][1]))
  rdd_ordenado = rdd_prec_prom.sortBy(lambda x: x[1], ascending = False)
  print(rdd_ordenado.take(12))

if _name_ == "_main_":
  main()