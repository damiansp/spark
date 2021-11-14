from pyspark.sql.functions import avg, col, stddev


prev = spark.read.csv('linkage')
print(prev.show()) # head

parsed = (spark.read.option('header', 'true')
          .option('nullValue', '?')
          .option('inferSchema', 'true')
          .csv('linkage'))
parsed.printSchema()
parsed.cache()


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

# Also -- reads/writes
'''
d1 = spark.read.format('json').load('myfile.json')
d2 = spark.read.json('myfile.json')

d1.write.format('parquet').save('myfile.parquet')
d1.write.parquet('myfile.parquet')
d2.write.format('parquet').mode('overwrite').save('myfile.parquet')
'''

print(parsed.first)
parsed.show(5)
n = parsed.count()
print('n:', n)

parsed.groupBy('is_match').count().orderBy(col('count').desc()).show()
parsed.agg(avg('cmp_sex'), stddev('cmp_sex')).show()

parsed.createOrReplaceTempView('linkage') # allow to be treated as a SQL table
spark.sql('''
    SELECT is_match, COUNT(*) n
    FROM linkage
    GROUP BY is_match
    ORDER BY n DESC'''
).show()
