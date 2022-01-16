from pyspark.sql.types import *


schema = StructType([StructField('author', StringType(), False),
                     StructField('title', StringType(), False),
                     StructField('pages', IntegerType(), False)])

# With DDL
schema = 'author STRING, title STRING, pages INT'
