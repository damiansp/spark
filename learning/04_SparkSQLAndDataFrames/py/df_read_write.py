from pyspark.ml import image

#(DataFrameWriter
# .format(args)
# .options(args)
# .bucketBy(args)
# .partitionBy(args)
# .save(path))

#DataFrameWriter.format(args).option(args).sortBy(args).saveAsTable(table)

#DataFrame.write
#DataFrame.writeStream

filepath = 'my/path/my_file.json'
df.write.format('json').mode('overwrite').save(filepath)

(df.write
 .format('parquet')
 .mode('overwrite')
 .option('compression', 'snappy')
 .save(filepath))

df.write.mode('overwrite').saveAsTable('flights')

df = spark.read.format('json').load(filepath)

# SQL
# CREATE OR REPLACE TEMPORARY VIEW flights
# USING json
# OPTIONS (path 'my/path/my_file.json');

spark.sql('SELECT * FROM flights').show()

df.write.format('json').mode('overwrite').save('/tmp/data/json/df.json')


# CSV
filepath = 'my/path/data/csv/*'
schema = 'DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count INT'
df = (
    spark.read.format('csv')
    .option('header', 'true')
    .schema(schema)
    .option('mode', 'FAILFAST')
    .option('nullValue', '')
    .load(filepath))

# SQL
# CREATE OR REPLACE TEMPORARY VIEW flights
# USING csv
# OPTIONS (
#   path "my/path/data/csv/*",
#   header "true",
#   inferSchema "true",
#   mode "FAILFAST");

spark.sql('SELECT * FROM flights').show(10)

df.write.format('csv').mode('overwrite').save('/tmp/path/csv')


# Images
img_dir = 'path/to/images'
img_df = spark.read.format('image').load(f'{img_dir}/')
img_df.print_schema()

img_df.select(
    'image.height', 'image.width', 'image.nChanels', 'image.mode', 'label'
).show(5)


# Binaries
binary_df = (
    spark.read.format('binaryFile')
    .option('pathGlobFilter', '*.jpg')
    .option('recrusiveFileLookup', 'true')
    .load('my/img/dir/'))
