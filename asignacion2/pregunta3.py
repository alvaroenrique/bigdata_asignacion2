from pyspark import SparkContext
from pyspark.sql import SQLContext

def checkIfIsFloat(element):
  try:
    float(element)
  except ValueError:
    return 1
  return float(element)

def Main():
  sc = SparkContext("local","asignacion2")
  sqlContext = SQLContext(sc)
  data = sqlContext.read.csv("urbania_data.csv",header=True).rdd

  def filtrarDatasetPatio(registro):
    if registro[95] == "Lima" and registro[34] == "1.0":
      return True
    else:
      return False

  def filtrarDatasetDeposito(registro):
    if registro[95] == "Lima" and registro[29] == "1.0":
      return True
    else:
      return False

  def filtrarDatasetPatioDeposito(registro):
    if registro[95] == "Lima" and registro[34] == "1.0"  and registro[29] == "1.0":
      return True
    else:
      return False

  # Se filtra por distrito y por si tiene patio, depósito o los 2
  data_lima_patio = data.filter(filtrarDatasetPatio).map(lambda x: ("lima_patio", (checkIfIsFloat(x[15]), 1)))
  data_lima_deposito = data.filter(filtrarDatasetDeposito).map(lambda x: ("lima_deposito", (checkIfIsFloat(x[15]), 1)))
  data_lima_deposito_patio = data.filter(filtrarDatasetPatioDeposito).map(lambda x: ("lima_deposito_patio", (checkIfIsFloat(x[15]), 1)))
  
  # Se calcula el precio promedio
  print(data_lima_patio.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).map(lambda x: (x[0], x[1][0] / x[1][1])).collect())
  print(data_lima_deposito.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).map(lambda x: (x[0], x[1][0] / x[1][1])).collect())
  print(data_lima_deposito_patio.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).map(lambda x: (x[0], x[1][0] / x[1][1])).collect())


if __name__ == "__main__":
  Main()