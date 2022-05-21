from pyspark.sql.functions import (
    bround, count, corr, expr, lit, max, mean, min, pow, round, stddev_pop)


fab_quant = pow(col('quantity') * col('unit_price'), 2) + 5
df.select(expr('customer_id') fab_quand.alias('real_quant'))
df.selectEpr(
    'customer_id', 'POWER((quantitiy * unit_price), 2.0) + 5) AS real_quant')
df.select(round(lit('2.5')), bround(lit('2.5')))
df.stat.corr('quantity', 'unit_price')
df.select(corr('quantity', 'unit_price'))
df.describe()
