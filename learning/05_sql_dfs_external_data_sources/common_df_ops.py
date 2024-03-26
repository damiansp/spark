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
     .join(airports, airport.IATA == mini.origin)
     .select('City', 'State', 'date', 'delay', 'distance', 'destination')
     .show())
    

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


if __name__ == '__main__':
    main()
