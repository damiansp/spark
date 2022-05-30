from pyspark.sql.functions import (
    initcap, lit, lower, lpad, ltrim, rpad, rtrim, trim, upper)


df.select(initcap(col('description')))  # like title()
df.select(col('x'), lower(col('x')), upper(col('x')))
df.select(
    ltrim(lit('   hello   ').alias('ltrim')),  # 'hello   '
    rtrim(lit('   hello   ').alias('rtrim')),  # '   hello'
    trim(lit('   hello   ').alias('trim')),    # 'hello'
    lpad(lit('hello'), 3, ' ').alias('lp'),    # ' he'
    rpad(lit('hello'), 7, ' ').alias('rp'))    # 'hello  '
