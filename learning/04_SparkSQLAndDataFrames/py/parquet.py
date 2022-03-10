parquet_path = 'my/path/parquet_directory/'
df = spark.read.format('parquet').load(parquet_path)

# SQL
create_table = f'''
CREATE OR REPLACE TEMPORARY VIEW flights
USING parquet
OPTIONS (path "{parquet_path}");'''

spark.sql('SELECT * FROM flights').show()
