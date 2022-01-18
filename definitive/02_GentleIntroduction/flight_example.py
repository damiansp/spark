from pyspark.sql import SparkSession


DATA = '../../data'

spark = SparkSession.builder.appName('flights').getOrCreate()
flight_data_2015 = (spark
                    .read
                    .option('inferScehma', 'true')
                    .option('header', 'true')
                    .csv(f'{DATA}/flight-data/csv/2015-summary.csv'))
