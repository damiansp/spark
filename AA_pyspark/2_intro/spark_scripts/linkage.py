prev = spark.read.csv('linkage')
print(prev.show()) # head

parsed = (spark.read.option('header', 'true')
          .option('nullValue', '?')
          .option('inferSchema', 'true')
          .csv('linkage'))
parsed.printSchema()


# Alternatives for known schemata:
'''
from pyspark.sql.types import *
schema = StructType([StructField('id_1', IntegerType(), False),
                     StructField('id_2', StringType(), False),
                     StructField('cmp_fname_c1', DoubleType(), False)])
spark.read.schema(schema).csv('...')
'''

'''
schema = 'id_1 INT, id_2 INT, cmp_fname_c1 DOUBLE'
'''

# Also
'''
d1 = spark.read.format('json').load('myfile.json')
d2 = spark.read.json('myfile.json')
'''
