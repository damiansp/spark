from pyspark.sql.functions import col, date_format


prepped_df = (
    static_df
    .na.fill(0)
    .withColumn('day_of_week', date_format(col('InvoiceDate'), 'EEEE'))
    .coalesce(5))

train_df = prepped_df.where("InvoiceDate < '2011-07-01'")
test_df  = prepped_df.where("InvoiceDate >= '2011-07-01'")
print(trainDF.count())
print(trainDF.count())

