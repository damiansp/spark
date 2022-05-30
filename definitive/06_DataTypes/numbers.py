from pyspark.sql.functions import (
    bround, count, corr, expr, lit, max, mean, min,
    monotonically_increasing_id, pow, round, stddev_pop)


fab_quant = pow(col('quantity') * col('unit_price'), 2) + 5
df.select(expr('customer_id') fab_quand.alias('real_quant'))
df.selectExpr(
    'customer_id', 'POWER((quantitiy * unit_price), 2.0) + 5) AS real_quant')
df.select(round(lit('2.5')), bround(lit('2.5')))
df.stat.corr('quantity', 'unit_price')
df.select(corr('quantity', 'unit_price'))
df.describe()

quantile_probs = [0.5]
rel_err = 0.05
df.stat.approxQuantile('unit_price', quantile_probs, rel_err)

df.stat.crosstab('stock_code', 'quantity')
df.stat.freqItems(['stock_code', 'quantity'])
df.select(monotonically_increasing_id())
