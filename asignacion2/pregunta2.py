from pyspark import SparkContext
from pyspark.sql import SQLContext

def checkIfIsFloat(element):
  try:
    float(element)
  except ValueError:
    return 1
  return float(element)

def main():
  sc = SparkContext("local", "asignación2")
  sqlContext = SQLContext(sc)
    
  data = sqlContext.read.csv("urbania_data.csv", header=True).rdd

  formated_data = data.map(lambda x: (x[94], checkIfIsFloat(x[15]) / checkIfIsFloat(x[26]))) # Distrito, precio por m2
    
  reduced_data = formated_data.groupByKey().map(lambda x : (x[0],sum(list(x[1])) / len(list(x[1])) )) # Calculo del promedio por distrito

  sorted_data = reduced_data.sortBy(lambda x: x[1], ascending=False).collect()

  for data in sorted_data:
    print(f'{data[0]} {data[1]}')

    
if __name__ == "__main__":
  main()