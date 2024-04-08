from pyspark.sql import SparkSession
from pyspark.sql.functions import expr


DATA = '../../data/databricks-datasets/learning-spark-v2'
delays_path = f'{DATA}/flights/departuredelays.csv'
airports_path = f'{DATA}/flights/airport-codes-na.txt'
spark = SparkSession.builder.appName('Common Ops').getOrCreate()


def main():
    airports = load_airports()
    airports.createOrReplaceTempView('airports')
    delays = load_delays()
    delays = cast_types(delays)
    delays.createOrReplaceTempView('delays')
    mini = get_subset(delays, 'SEA', 'SFO', '01010%')
    mini.createOrReplaceTempView('mini')
    spark.sql('SELECT * FROM delays LIMIT 10').show()
    spark.sql('SELECT * FROM mini').show()
    (mini
     .join(airports, airports.IATA == mini.origin)
     .select('City', 'State', 'date', 'delay', 'distance', 'destination')
     .show())
    # Unsupported w/o Hive
    #make_departure_delays_window()
    #get_most_delays()
    #get_most_delays_windowed()
    add_col(mini)
    drop_col(mini)
    rename_col(mini)
    pivot(delays)
    
    
def load_airports():
    return (
        spark.read.format('csv')
        .options(header='true', infer_schema='true', sep='\t')
        .load(airports_path))


def load_delays():
    return spark.read.format('csv').options(header='true').load(delays_path)


def cast_types(delays):
    return (
        delays
        .withColumn('delay', expr('CAST(delay AS INT) AS delay'))
        .withColumn('distance', expr('CAST(distance AS INT) AS distance')))

def get_subset(delays, origin, dest, datelike):
    return (
        delays
        .filter(
            expr(
                f'''origin == '{origin}'
                  AND destination == '{dest}'
                  AND date LIKE '{datelike}'
                  AND delay > 0''')))

def make_departure_delays_window():
    spark.sql('DROP TABLE IF EXISTS departure_delays_window')
    spark.sql(
        '''CREATE TABLE departure_delays_window AS
        SELECT origin, destination, SUM(delay) AS total_delays
        FROM delays
        WHERE origin IN ('SEA', 'SFO', 'JFK')
          AND destination in ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL')
        GROUP BY origin, destination''')
    spark.sql('SELECT * FROM departure_delays_window').show()


def get_most_delays():
    spark.sql(
        '''SELECT origin, destination, SUM(total_delays) AS sum_total_delays
        FROM departure_delays_window
        WHERE origin = 'SEA'
        GROUP BY origin, destination
        ORDER BY sum_total_delays
        LIMIT 3'''
    ).show()


def get_most_delays_windowed():
    spark.sql(
        '''SELECT origin, destination, total_delays, rank
        FROM (

          SELECT
            origin,
            destination,
            total_delays,
            DENSE_RANK() OVER (
              PARTITION BY origin ORDER BY total_delays DESC
            ) AS rank
          FROM departure_delays_window
        
        ) t
        WHERE rank <= 3'''
    ).show()


def add_col(df):
    df.withColumn(
        'status',
        expr('CASE WHEN delay <= 10 THEN "on time" ELSE "delayed" END')
    ).show()


def drop_col(df):
    df.drop('delay').show()
    

def rename_col(df):
    df.withColumnRenamed('status', 'flight_status').show()


def pivot(delays):
    spark.sql(
        '''SELECT
          destination,
          CAST(SUBSTRING(date, 0, 2) AS INT) AS month,
          delay
        FROM delays
        WHERE origin = 'SEA' '''
    ).show()
    spark.sql(
        '''SELECT *
        FROM (

          SELECT
            destination,
            CAST(SUBSTRING(date, 0, 2) AS INT) AS month,
            delay
          FROM delays
          WHERE origin = 'SEA'

        )
        PIVOT(
          CAST(AVG(delay) AS DECIMAL(4, 2)) AS mean_delay,
          MAX(delay) AS max_delay
          FOR month IN (1 JAN, 2 FEB)
        )
        ORDER BY destination'''
    ).show()
    
    
if __name__ == '__main__':
    main()
