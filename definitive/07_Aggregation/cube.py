from pyspark.sql.functions import col, sum


(df_no_null
 .cube('Date', 'Country')
 .agg(sum(col('Quantity')))
 .select('Date', 'Country', 'sum(Quantity)')
 .orderBy('Date'))
