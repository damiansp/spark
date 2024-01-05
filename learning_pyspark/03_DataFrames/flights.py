from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('Flights').getOrCreate()
DATA = '../data'
dd = f'{DATA}/departuredelays.csv'
codes = f'{DATA}/airport-codes-na.txt'


def main():
    airports = spark.read.csv(
        codes, header='true', inferSchema='true', sep='\t')
    airports.createOrReplaceTempView('airports')
    flights = spark.read.csv(dd, header='true')
    flights.createOrReplaceTempView('flights')
    flights.cache()
    spark.sql(
        '''
        SELECT a.City, f.origin, SUM(f.delay) as delays
        FROM flights f JOIN airports a ON a.IATA = f.origin
        WHERE a.State = 'WA'
        GROUP BY a.City, f.origin
        ORDER BY 3 DESC'''
    ).show()


if __name__ == '__main__':
    main()
