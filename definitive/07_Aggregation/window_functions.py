from pyspark.sql.functions import (
    col, dense_rank, desc, max as mx, rank, to_date)
from pypsark.sql.window import Window


df_with_date = df.withColumn(
    'date', to_date(col('InvoiceDate'), 'MM/d/yyyy H:mm'))
df_with_date.createOrReplaceTempView('dfWithDate')

window_spec = (
    Window
    .partitionBy('id', 'date')
    .orderBy(desc('quantity'))
    .rowsBetween(Window.unboundedPreceding, Window.currentRow))
max_purchase_qty = mx(col('quantity')).over(window_spec)
purchase_dense_rank = dense_rank().over(window_spec)
purchase_rank = rank().over(window_spec)

(df_with_date
 .where('id IS NOT NULL')
 .orderBy('id')
 .select(
     col('id'),
     col('date'),
     col('quantity'),
     purchase_rank.alias('qty_rank'),
     purchase_density_rank.alias('qty_dense_rank'),
     max_purchase_qty.alias('max_purchase_qty'))
 .show())

df_no_null = df_with_date.drop()
df_no_null.createOrReplaceTempView('df_no_null')

rolled_up = (
    df_no_null
    .rollup('date', 'country')
    .agg(sum('quantity'))
    .selectExpr('date', 'country', '`SUM(quantity)` AS total_quantityy')
    .orderBy('date'))
rolled_up.show()

