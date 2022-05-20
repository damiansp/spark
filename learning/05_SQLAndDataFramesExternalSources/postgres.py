# spark-submit --jars postgresql-42.2.6.jar

df1 = (
    spark.read.format('jdbc')
    .option('url', 'jdbc:postgresql://my_server')
    .option('dbtable', 'schema.table')
    .option('user', 'my_username')
    .option('password', 'my_password')
    .load())
df2 = (
    spark.read.jdbc(
        'jdbc:postgresql://my_server',
        'schema.table_name',
        properties={'user': 'my_username', 'password': 'my_password'}))

(df2
 .write
 .format('jdbc')
 .option('url', 'jdbc:postgresql://my_server')
 .option('dbtable', 'schema.table_name')
 .option('user', 'user_name')
 .option('password', 'passy')
 .save())
df2.write.jdbc(
    'jdbc:postgresql:my_server',
    'schema.table_name',
    properties={'user': 'username', 'passwor', 'muh passwerd'})
