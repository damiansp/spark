# Parquet is default
df = spark.read.load('path/to/my_file.parquet')
df.select('name', 'color').write.save('path/to/favorite_colors.parquet')


# Specify options
df = spark.read.load('path/to/people.json', format='json')
df.select('name', 'age').write.save('path/to/people.parquet', format='parquet')
df = spark.read.load(
    'people.csv', format='csv', sep=';', inferSchema=True, header='true')


# Run SQL on files directly
df = spark.sql('SELECT * FROM parquet.`path/to/my_file.parquet`')
