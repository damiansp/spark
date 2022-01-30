from pyspark.sql import SparkSession


DATA = '../../data'

spark = SparkSession.builder.appName('flights').getOrCreate()
data = (spark
        .read
        .option('inferScehma', 'true')
        .option('header', 'true')
        .csv(f'{DATA}/flight-data/csv/2015-summary.csv'))
print(data.take(3))
data.sort('count').explain()

spark.conf.set('spark.sql.shuffle.partitions', '5') # default is 200
print(data.sort('count').take(2))


# DataFrames and SQL
data.createOrReplaceTempView('flight_data')
sql_way = spark.sql('''
    SELECT DEST_COUNTRY_NAME, COUNT(1)
    FROM flight_data
    GROUP BY DEST_COUNTRY_NAME''')

df_way = data.groupBy('DEST_COUNTRY_NAME').count()

print('sql:\n')
sql_way.explain()
print('\ndf:\n')
df_way.explain()




