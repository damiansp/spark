# Postgres
jdbc_df1 = (
    spark
    .read.format('jdbc')
    .option('url', 'jdbc:postgresql://my_server')
    .option('dbtable', 'my_schema.my_table')
    .option('user', 'my_username')
    .option('password', 'my_password')
    .load())
jdbc_df2 = spark.read.jdbc(
    'jdbc:postgresql://my_server',
    'my_schema.my_table',
    properties={'user': 'my_username', 'password': 'my_password'})
# write
(jdbc_df1
 .write.format('jdbc')
 .option('url', 'jdbc:postgresql://my_server')
 .option('dtable', 'my_schema.my_table')
 .option('user', 'my_username')
 .option('password', 'my_password')
 .save())
jdbc_df2.write.jdbc(
    'jdbc:postgresql://my_server',
    'my_schema.my_table',
    properties={'user': 'my_username', 'password': 'my_password'})


# MySQL
