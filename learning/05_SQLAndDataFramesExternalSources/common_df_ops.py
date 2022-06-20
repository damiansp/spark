from pyspark.sql.functions import expr


delays_path = '/my/path/delays.csv'
airports_path = '/my/path/airports.csv'
airports_df = (
    spark
    .read
    .format('csv')
    .options(header='true', inferSchema='true', sep='\t')
    .load(airports_path))
airports_df.createOrReplaceTempView('airports')
delays_df = (
    spark
    .read
    .format('csv')
    .options(header='true')
    .load(delays_path))
delays_df = (
    delays_df
    .withColumn('delay', expr('CAST(delay AS INT) AS delay'))
    .withColumn('distance', expr('CAST(distance AS INT) AS distance')))
delays_df.createOrReplaceTempView('delays')
foo = delays.filter(
    expr(
        "origin == 'SEA' AND destination == 'SFO' "
        "AND date LIKE '01010%' "
        "AND delay > 0"))
foo.createOrReplaceTempView('foo')
spark.sql('SELECT * FROM foo').show()


# union
bar = delays.union(foo)
bar.createOrReplaceTempView('bar')
bar.filter(
    expr(
        "origin == 'SEA' AND destination == 'SFO' "
        "AND date LIKE '01010%' "
        "AND delay > 0")
).show()


# join
(foo
 .join(airports, airports.IATA == foo.origin)
 .select('City', 'State', 'date', 'delay', 'distance', 'destination')
 .show())


# window
# SQL:
'''
DROP TABLE IF EXISTS departureDelaysWindow;

CREATE TABLE departureDelaysWindow AS
SELECT origin, destination, SUM(delay) AS total_delays
FROM delays
WHERE origin IN ('SEA', 'SFO', 'JFK') 
  AND destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL')
GROUP BY origin, destination;

SELECT * FROM departureDelaysWindow
'''


spark.sql(
    'SELECT origin, destination, total_delays, rank '
    'FROM ('
    '  SELECT origin, destination total_delays, dense_rank()'
    '  OVER (PARTITION BY origin ORDER BY total_days DESC) AS rank'
    '  FROM departureDelayWindow'
    ') t '
    'WHERE rank <= 3'
).show()

foo2 = foo.withColumn(
    'status', expr("CASE WHEN delay <= 10 THEN 'on-time' ELSE 'delayed' END"))
foo3 = foo2.drop('delay')
foo4 = foo3.withColumnRenamed('status', 'flight_status')


# pivots
'''
SELECT destination, CAST(SUBSTRING(date, 0, 2) AS INT) AS month, delay
FROM delays
WHERE origin = 'SEA'
'''

'''
SELECT * 
FROM (
  SELECT destination, CAST(SUBSTRING(date, 0, 2) AS INT) AS month, delay
  FROM delays
  WHERE origin = 'SEA'
)
PIVOT (
  CAST(AVG(delay) AS DECIMAL(4, 2)) AS mean_delay, MAX(delay) AS max_delay
  FOR month IN (1 JAN, 2 FEB)
)
ORDER BY destination
'''
