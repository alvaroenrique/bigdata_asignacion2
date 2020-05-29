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
  
  def isLimaModerna(distrito):
    if distrito == 'SanMiguel' or distrito == 'PuebloLibre' or distrito == 'JesusMaria' or distrito == 'MagdalenaDelMar' or distrito == 'Lince' or distrito == 'SanIsidro' or distrito == 'Miraflores' or distrito == 'Surquillo' or distrito == 'SanBorja' or distrito == 'Barranco' or distrito == 'SantiagoDeSurco' or distrito == 'LaMolina':
      return 'Lima moderna'
    else:
      return 0


  # Calculo del precio promedio por distrito
  rddProvLima = rddData.filter(FiltrarProvincia)
  rdd_kv = rddProvLima.map(lambda x: (x[94] , (float(x[15]),1) ))
  rdd_reducido = rdd_kv.reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1]))
  rdd_prec_prom = rdd_reducido.map(lambda x: (x[0], x[1][0]/x[1][1], isLimaModerna(x[0])))
  
  rdd_ordenado = rdd_prec_prom.sortBy(lambda x: x[1], ascending = False)

  for i, d in enumerate(rdd_ordenado.collect()):
    print(f'{i + 1}: {d[0]} {d[1]} -> {d[2]}')

if __name__ == "__main__":
  main()
