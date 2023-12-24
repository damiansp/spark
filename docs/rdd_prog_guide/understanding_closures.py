from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('MyApp') 
sc = SparkContext(conf=conf)

counter = 0
rdd = sc.parallelize(data)


# DON'T DO THIS
def incr_counter(n):
    # each executor will have its own independent copy of counter
    global counter  
    counter += n


rdd.foreach(incr_counter)
print('Counter val:', counter)
