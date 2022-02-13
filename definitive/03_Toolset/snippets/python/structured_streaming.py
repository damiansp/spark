from pyspark.sql.functions import col, column, desc, window


# reasonable setting for local-mode (vs default of 200)
spark.conf.set('spark.sql.shuffle.partitions', '5') 


data_path = '/data/retail-data/by-day/*.csv'
static_df = (spark.read.format('csv')
             .option('header', 'true')
             .option('inferSchema', 'true')
             .load(data_path))
static_df.createOrReplaceTempView('retail')
static_schema = static_df.schema

(static_df
 .selectExpr(
     'CustomerId', '(UnitPrice * Quantity) AS total_cost', 'InvoiceDate')
 .groupBy(col('CustomerId'), window(col('InvoiceDate'), '1 day'))
 .sum('total_cost')
 .show(5))

streaming_df = (spark.readStream
                .schema(static_schema)
                .option('maxFilesPerTrigger', 1)
                .format('csv')
                .option('header', 'true')
                .load(data_path)
purchase_by_customer_per_hour = (
    streaming_df
    .selectExpr(
        'CustomerId', '(UnitPrice * Quantity) AS total_cost', 'InvoiceDate')
    .groupBy(col('CustomerId'), window(col('InvoiceDate'), '1 day'))
    .sum('total_cost')
