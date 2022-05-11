from pyspark.sql.functions import col, expr, instr, lit


df = (
    spark.read.format('csv')
    .option('header', 'true')
    .option('inferSchema', 'true')
    .load('my/path/to/file.csv'))
df.printSchema()
df.createOrReplaceTempView('table')

df.select(lit(5), lit('five'), lit(5.))

df.whhere(col('InvoiceNo') != 53356).select('InvoiceNo', 'Description').show(5)
df.where('InvoiceNo = 53545').show(5)
df.where('InvoiceNo <> 53545').show(5)

price_filter = col('UnitPrice') > 600
# 'POSTAGE' instr(ing) for col Description?
descrip_filter = instr(df.Description, 'POSTAGE') >= 1
df.where(df.StockCode.isin('DOT')).where(price_filter | descrip_filter).show()

# same as
'''
SELECT *
FROM dfTable
WHERE StockCode IN ("DOT") 
  AND (UnitPrice > 600 OR instr(Description, "POSTAGE") >= 1)
'''

DOT_code_filter = col('StockCode') == 'DOT'
price_filter = col('UnitPrice') > 600
descrip_filter = instr(col('Description'), 'POSTAGE') >= 1
(df
 .withColumn('isExpensive', DOT_code_filter & (price_filter | descrip_filter))
 .where('isExpensive')
 .select('UnitPrice', 'isExpensive')
 .show())

'''
SELECT 
  UnitPrice,
  (StockCode = 'DOT'
    AND (UnitPrice > 600 OR instr(Destription, "POSTAGE") >= 1)
  ) AS isExpensive
FROM dfTable
WHERE StockCode = 'DOT' 
  AND (UnitPrice > 600 OR instr(Description, "POSTAGE") >= 1)
'''

(df
 .withColumn('isExpensive', expr('NOT UnitPrice <= 250'))
 .where('isExpensive')
 .select('Description', 'UnitPrice')
 .show(5))

df.where(col('Description').eqNullSafe('hello')).show()
