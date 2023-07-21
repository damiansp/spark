csv_file = (
    spark
    .read
    .format('csv')
    .option('header', 'true')
    .option('mode', 'FAILFAST')
    .schema(my_schema)
    .load('my/path.csv'))
(csv_file
 .write
 .format('csv')
 .mode('overwrite')
 .option('sep', '\t')
 .save('my/path.csv'))
