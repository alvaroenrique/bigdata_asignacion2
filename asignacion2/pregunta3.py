from pyspark import SparkContext 
from pyspark.sql import SQLContext  

def main():
  sc = SparkContext("local","asignacion2")
  sqlContext = SQLContext(sc)

  data = sqlContext.read.csv("urbania_data.csv",header=True).rdd 

  data_dist_prov = data.map(lambda x: (x[94], x[34], x[29], x[15], x[26], 1 if x[95] == 'Lima' else x[95])) #distrito, patio, deposito, pre$
  
  print(data_dist_prov.take(3))

if __name__ == "__main__":
  main()