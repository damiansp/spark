from pyspark.sql.functions import col, to_date


df_with_date = df.withColumn(
    'date', to_date(col('InvoiceDate'), 'MM/d/yyyy H:mm'))
df_with_date.createOrReplaceTempView('dfWithDate')
