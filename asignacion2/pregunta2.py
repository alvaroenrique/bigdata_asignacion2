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

    data_dist_prov = data.map(lambda x: (x[94], checkIfIsFloat(x[15]) / checkIfIsFloat(x[26]))) # Provincia, precio, area total m2
    

    print(data_dist_prov.groupByKey().map(lambda x : (x[0],sum(list(x[1])) / len(list(x[1])) )).sortBy(lambda x: x[1], ascending=False).collect())
    #print(data_dist_prov.reduceByKey())

    
if __name__ == "__main__":
    main()