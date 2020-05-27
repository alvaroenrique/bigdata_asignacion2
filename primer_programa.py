from pyspark import SparkContext

def main():
    sc = SparkContext("local", "Mi Primer Promgrama")
    arr_numeros = [1, 2, 3, 4]
    rdd = sc.parallelize(arr_numeros) 

    # Transformaciones
    # map
    rdd_elevado2 = rdd.map(lambda num : num*num)

    # Actions
    arr = rdd_elevado2.collect() # Nos devuelve un listado python (no RDD)
    for num in arr:
        print(num)

if __name__ == "__main__":
    main()
