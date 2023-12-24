from pyspark import SparkConf, SparkContext


# Typically set master in spark-submit
conf = SparkConf().setAppName('MyApp')#.setMaster('local')
sc = SparkContext(conf=conf)

data = [1, 2, 3, 4, 5]
dist_data = sc.parallelize(data)

dist_file = sc.textFile('data.txt')


lines = sc.textFile('data.txt')
line_lens = lines.map(lambda s: len(s))
total_len = line_lens.reduce(lambda a, b: a + b)

# if needed for reuse
line_lens.persist()

