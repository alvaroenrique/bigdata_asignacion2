from pyspark import SparkContext
from pyspark.sql import SQLContext

def main():
    sc = SparkContext("local", "asignación2")
    sqlContext = SQLContext(sc)
    
    data = sqlContext.read.csv("urbania_data.csv", header=True).rdd

    data_dist_prov = data.map(lambda x: (x[94], 1 if x[95] == 'Lima' else x[95]))
    
    data_lima = data_dist_prov.filter(lambda x: x[1] == 1).reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[1], ascending = False)

    for data in data_lima.collect():
        print(f'{data[0]} {data[1]}')
    

if __name__ == "__main__":
    main()
