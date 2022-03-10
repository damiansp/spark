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
