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
